"""
Fail-closed contract for the order-feed proxy (Shared order stream):
when the account relay dies mid-stream (seq gap or a stale heartbeat), the
execution client must NEVER assume flat, must raise/log loudly, must flip a
host-readable ``order_feed_degraded`` flag, and must force the REST reconcile
poll's cadence to the fast tier -- WITHIN ONE TICK, not waiting out the
300s-floored idle cadence -- while still going through the shared API budget
gate (never bypassing it).
"""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from nautilus_tradestation import execution as execution_module
from nautilus_tradestation.execution import TradeStationExecutionClient
from nautilus_tradestation.feed.tail_client import FeedGapError, FeedHandlerDeadError

from tests.test_execution_event_hardening import _make_harness


async def wait_until(predicate, timeout=5.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


class _KilledMidStream:
    """A fake proxy stream client: yields nothing, then raises the given
    feed error on the first ``__anext__`` -- "the relay died mid-test"."""

    def __init__(self, error: Exception):
        self._error = error
        self.reentries = 0

    async def stream_orders(self, account_id):
        self.reentries += 1
        if False:
            yield {}  # pragma: no cover -- makes this an async generator
        raise self._error


def _make_streaming_client(fake_stream_client) -> TradeStationExecutionClient:
    h = _make_harness()
    h.client._use_streaming = True
    h.client._stream_client = fake_stream_client
    return h.client


class TestNoImplicitFlatOnFeedError:
    async def test_gap_error_never_generates_a_synthetic_terminal_event(self, monkeypatch):
        fake = _KilledMidStream(FeedGapError("seq gap for SIMTEST"))
        client = _make_streaming_client(fake)
        client.generate_order_filled = MagicMock(side_effect=AssertionError("must not fill"))
        client.generate_order_canceled = MagicMock(
            side_effect=AssertionError("must not cancel")
        )
        client.generate_order_rejected = MagicMock(
            side_effect=AssertionError("must not reject")
        )
        assert client.order_feed_degraded is False
        task = asyncio.create_task(client._stream_order_fills())
        try:
            await wait_until(lambda: client.order_feed_degraded is True)
            # Give the (re-raising, backed-off) loop a moment more: it must
            # keep re-entering, never synthesize a terminal order event.
            await asyncio.sleep(0.05)
            client.generate_order_filled.assert_not_called()
            client.generate_order_canceled.assert_not_called()
            client.generate_order_rejected.assert_not_called()
            assert fake.reentries >= 1  # re-entered the stream, never gave up silently
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_handler_dead_error_also_flips_degraded_and_never_flat(self):
        fake = _KilledMidStream(FeedHandlerDeadError("heartbeat stale for SIMTEST"))
        client = _make_streaming_client(fake)
        client.generate_order_canceled = MagicMock(
            side_effect=AssertionError("must not cancel")
        )
        task = asyncio.create_task(client._stream_order_fills())
        try:
            await wait_until(lambda: client.order_feed_degraded is True)
            client.generate_order_canceled.assert_not_called()
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_healthy_event_after_degradation_clears_the_flag(self):
        # A recovered relay must clear the flag from a REAL event, never a timeout.
        class _RecoversAfterOneGap:
            def __init__(self):
                self.attempt = 0

            async def stream_orders(self, account_id):
                self.attempt += 1
                if self.attempt == 1:
                    raise FeedGapError("seq gap for SIMTEST")
                yield {"OrderID": "TS-1", "Status": "ACK", "FilledQuantity": "0",
                       "AveragePrice": "0"}
                await asyncio.sleep(10)  # hold the generator open

        fake = _RecoversAfterOneGap()
        client = _make_streaming_client(fake)
        task = asyncio.create_task(client._stream_order_fills())
        try:
            await wait_until(lambda: client.order_feed_degraded is True)
            await wait_until(lambda: client.order_feed_degraded is False, timeout=5.0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class TestCadenceFlipsWithinOneTick:
    async def test_degraded_flag_forces_fast_cadence_even_with_nothing_pending(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            execution_module._budget, "reserve_poll", lambda account_id: (True, "ok")
        )
        h = _make_harness()
        client = h.client
        client._check_order_statuses = AsyncMock()
        assert client._has_pending_orders() is False  # nothing tracked at all

        client.order_feed_degraded = True
        # NOTE: 0.05s (not an unrealistically tiny 0.02s) -- _interruptible_
        # sleep races two real asyncio tasks per tick (Canary
        # hardening), and a sub-30ms interval is fragile under real OS
        # scheduling jitter/load, independent of correctness. Production
        # ticks every 30s, where this overhead is immaterial.
        loop_task = asyncio.create_task(client._reconcile_order_statuses_loop(0.05))
        try:
            # FAST cadence (0.05s), not the 300s-floored idle cadence: the very
            # first tick after degrading must poll, and it must keep polling
            # -- proves the idle 300s floor never engaged while degraded.
            await wait_until(lambda: client._check_order_statuses.call_count >= 3, timeout=3.0)
        finally:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)

    async def test_reserve_poll_still_gates_the_degraded_fast_poll(self, monkeypatch):
        # Degraded cadence is FAST, never a bypass of the shared budget.
        calls = []

        def _reserve(account_id):
            calls.append(account_id)
            return False, "ceiling"

        monkeypatch.setattr(execution_module._budget, "reserve_poll", _reserve)
        h = _make_harness()
        client = h.client
        client._check_order_statuses = AsyncMock()
        client.order_feed_degraded = True
        loop_task = asyncio.create_task(client._reconcile_order_statuses_loop(0.02))
        try:
            await wait_until(lambda: len(calls) >= 1, timeout=1.0)
            await asyncio.sleep(0.05)
            client._check_order_statuses.assert_not_called()  # denied -> skipped, not bypassed
        finally:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)

    async def test_flip_mid_sleep_wakes_the_loop_within_35s_real_event_loop(
        self, monkeypatch
    ):
        """Canary: the 2026-09-09 canary's staged relay-kill found
        the reconcile loop's fast cadence never actually engaging during a
        7-minute outage. Root suspicion: a degrade landing MID-SLEEP waits
        out the rest of that sleep before the loop re-evaluates
        ``order_feed_degraded``. This proves the fix in a REAL event loop
        (no mocked/patched sleep): configure an artificially large
        ``interval_secs`` (300s -- far beyond any plausible test budget) so
        that WITHOUT the wake mechanism this test would have to wait a full
        5 minutes for the loop to even notice; flip the flag via the same
        ``_set_order_feed_degraded`` the real gap/dead-relay path uses WHILE
        the loop is genuinely asleep inside that 300s wait; and assert the
        resulting poll lands within 35s of the flip.
        """
        monkeypatch.setattr(
            execution_module._budget, "reserve_poll", lambda account_id: (True, "ok")
        )
        # The one-time startup jitter (up to interval_secs, see
        # _reconcile_initial_jitter_seconds) is deliberately NOT wake-
        # interruptible (see its call site's docstring) -- zero it out so
        # this test exercises the STEADY-STATE sleep this fix is actually
        # about, exactly like test_skips_subsequent_ticks_while_idle does.
        monkeypatch.setattr(
            "nautilus_tradestation.execution._reconcile_initial_jitter_seconds",
            lambda _interval: 0.0,
        )
        h = _make_harness()
        client = h.client
        client._check_order_statuses = AsyncMock()
        assert client._has_pending_orders() is False

        loop_task = asyncio.create_task(client._reconcile_order_statuses_loop(300.0))
        try:
            # Let the loop actually reach its 300s _interruptible_sleep
            # before flipping -- proves the wake interrupts a sleep already
            # IN PROGRESS, not merely a sleep not yet started.
            await asyncio.sleep(0.1)
            assert client._check_order_statuses.call_count == 0  # still asleep, nothing yet

            t0 = time.monotonic()
            client._set_order_feed_degraded(True, reason="test: simulated relay death")

            await wait_until(
                lambda: client._check_order_statuses.call_count >= 1, timeout=35.0
            )
            elapsed = time.monotonic() - t0
            assert elapsed < 35.0, f"poll took {elapsed:.1f}s after the flip (>= 35s bound)"
        finally:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)

    async def test_recovery_flip_wakes_the_loop_without_forcing_a_spurious_poll(
        self, monkeypatch
    ):
        # Symmetric proof for the RECOVERED direction: clearing the flag
        # while the loop is asleep must wake it promptly too (never silently
        # swallowed) -- but, unlike the degrade direction, recovering while
        # nothing is pending and a poll JUST happened must NOT force another
        # poll immediately (the idle 300s floor is real, not bypassed by
        # every wake).
        monkeypatch.setattr(
            execution_module._budget, "reserve_poll", lambda account_id: (True, "ok")
        )
        h = _make_harness()
        client = h.client
        client._check_order_statuses = AsyncMock()
        client.order_feed_degraded = True

        # Fast cadence first, to establish a REAL last_poll_time (the very
        # first tick always polls regardless of pending -- this run
        # establishes the baseline this test actually wants to probe).
        loop_task = asyncio.create_task(client._reconcile_order_statuses_loop(0.05))
        try:
            await wait_until(
                lambda: client._check_order_statuses.call_count >= 1, timeout=5.0
            )
            calls_before = client._check_order_statuses.call_count

            t0 = time.monotonic()
            client._set_order_feed_degraded(False, reason="test: simulated recovery")
            # The wake must be observed (loop stays alive/responsive)...
            await asyncio.sleep(0.2)
            assert not loop_task.done()
            assert time.monotonic() - t0 < 35.0
            # ...but must NOT force an extra poll: healthy + nothing pending
            # + a poll just happened -> the idle 300s floor correctly holds,
            # even though the wake fired.
            assert client._check_order_statuses.call_count == calls_before
        finally:
            loop_task.cancel()
            await asyncio.gather(loop_task, return_exceptions=True)


class TestDegradedRecoveredMirroredToStdout:
    """Canary: "the operator reads stdout" -- DEGRADED/RECOVERED
    lines must be visible in the node's raw captured stdout log regardless
    of nautilus's own internal logging routing/level filtering, since that
    is what an operator tailing the process's stdout log file actually
    sees."""

    async def test_degrade_prints_a_warning_tagged_line_with_the_account(self, capsys):
        h = _make_harness()
        client = h.client
        client._set_order_feed_degraded(True, reason="seq gap for SIM_FUTURES")
        out = capsys.readouterr().out
        assert "[ORDER-FEED-WARNING]" in out
        assert client._account_id in out
        assert "DEGRADED" in out
        assert "seq gap for SIM_FUTURES" in out

    async def test_recovery_prints_a_warning_tagged_line_too(self, capsys):
        h = _make_harness()
        client = h.client
        client._set_order_feed_degraded(True, reason="initial failure")
        capsys.readouterr()  # discard the DEGRADED line
        client._set_order_feed_degraded(False, reason="relay resumed delivering events")
        out = capsys.readouterr().out
        assert "[ORDER-FEED-WARNING]" in out
        assert "RECOVERED" in out
        assert "relay resumed delivering events" in out

    async def test_real_degrade_and_recover_via_stream_order_fills_both_print(
        self, capsys
    ):
        # End to end through the REAL _stream_order_fills path (not just a
        # direct _set_order_feed_degraded call): a gap error then a healthy
        # event must each print their own stdout line.
        class _GapThenHealthy:
            def __init__(self):
                self.attempt = 0

            async def stream_orders(self, account_id):
                self.attempt += 1
                if self.attempt == 1:
                    raise FeedGapError("seq gap for SIMTEST")
                yield {"OrderID": "TS-1", "Status": "ACK", "FilledQuantity": "0",
                       "AveragePrice": "0"}
                await asyncio.sleep(10)

        client = _make_streaming_client(_GapThenHealthy())
        task = asyncio.create_task(client._stream_order_fills())
        try:
            await wait_until(lambda: client.order_feed_degraded is True)
            await wait_until(lambda: client.order_feed_degraded is False, timeout=5.0)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        out = capsys.readouterr().out
        assert out.count("[ORDER-FEED-WARNING]") == 2
        assert "DEGRADED" in out
        assert "RECOVERED" in out
