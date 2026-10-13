"""
Ownership proof for the shared account-wide order feed: an account-wide feed changes where order
events come FROM, never WHO may act on them. Two hosts sharing ONE feed
handler's order mirror must each process ONLY the order it itself submitted
(``_ts_order_id_to_client_order_id`` is populated host-locally, per §1 of the
design doc) even though BOTH hosts receive the identical verbatim broadcast.

End-to-end through the real pieces: ``FeedHandler`` (the relay),
``FeedTailStreamClient`` (what each host tails), and TWO independent
``TradeStationExecutionClient`` instances standing in for two host processes
on the same SIM account.
"""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from nautilus_tradestation.feed.handler import FeedHandler

from tests.test_execution_event_hardening import _arm, _make_harness, _market_order


class FakeUpstreamOrders:
    """Fake TradeStation order-stream source (handler side)."""

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.calls: list[str] = []

    async def stream_orders(self, account_id):
        self.calls.append(account_id)
        while True:
            yield await self.queue.get()

    async def stream_bars(self, *args, **kwargs):  # pragma: no cover -- unused here
        return
        yield  # noqa: unreachable, makes this an async generator


def _order_ev(order_id: str, status: str) -> dict:
    return {
        "OrderID": order_id,
        "Status": status,
        "FilledQuantity": "1" if status == "FLL" else "0",
        "AveragePrice": "1900.0" if status == "FLL" else "0",
    }


async def wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


def _make_host(order_coid: str, ts_order_id: str):
    """One host process: a real execution client armed with ONE order it
    itself submitted, wired to a ``FeedTailStreamClient`` for the shared
    account mirror."""
    from nautilus_tradestation.feed.tail_client import FeedTailStreamClient

    h = _make_harness()
    h.client._use_streaming = True
    order = _market_order(order_coid)
    _arm(h, order, ts_order_id=ts_order_id)
    h.order = order
    return h


class TestTwoHostsOneFeedOwnershipHolds:
    async def test_host_a_never_emits_for_hosts_b_order(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        from nautilus_tradestation.feed.tail_client import FeedTailStreamClient

        fake_upstream = FakeUpstreamOrders()
        handler = FeedHandler(
            "SIMSHARED",
            tmp_path,
            fake_upstream,
            scan_secs=0.02,
            heartbeat_secs=0.02,
            manifest_flush_secs=0.05,
            alerts_dir=tmp_path / "alerts",
            enable_order_feed=True,  # Order-feed opt-in gate: this test IS about orders
        )
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMSHARED"
        try:
            await wait_until(lambda: fake_upstream.calls == ["SIMSHARED"])

            host_a = _make_host("O-A", "TS-AAA")
            host_b = _make_host("O-B", "TS-BBB")
            host_a.client._stream_client = FeedTailStreamClient(feed_dir, poll_ms=10)
            host_b.client._stream_client = FeedTailStreamClient(feed_dir, poll_ms=10)

            host_a.client.generate_order_filled = MagicMock()
            host_b.client.generate_order_filled = MagicMock()

            task_a = asyncio.create_task(host_a.client._stream_order_fills())
            task_b = asyncio.create_task(host_b.client._stream_order_fills())

            # Broadcast BOTH hosts' order fills onto the ONE shared mirror --
            # the SAME two events reach both tailing consumers verbatim.
            fake_upstream.queue.put_nowait(_order_ev("TS-AAA", "FLL"))
            fake_upstream.queue.put_nowait(_order_ev("TS-BBB", "FLL"))

            await wait_until(lambda: host_a.client.generate_order_filled.call_count >= 1)
            await wait_until(lambda: host_b.client.generate_order_filled.call_count >= 1)
            await asyncio.sleep(0.1)  # let any wrongly-owned second call surface

            # OWNERSHIP HOLDS: host A fired exactly once, for ITS OWN order,
            # never host B's -- and vice versa. A broken/removed ownership
            # guard (deleting the `if not client_order_id: return` check in
            # _process_order_event) would make each host fire TWICE (once per
            # broadcast event), which this call-count assertion catches.
            assert host_a.client.generate_order_filled.call_count == 1
            assert host_b.client.generate_order_filled.call_count == 1
            a_kwargs = host_a.client.generate_order_filled.call_args.kwargs
            b_kwargs = host_b.client.generate_order_filled.call_args.kwargs
            assert str(a_kwargs["client_order_id"]) == "O-A"
            assert str(b_kwargs["client_order_id"]) == "O-B"
            assert str(a_kwargs["venue_order_id"]) == "TS-AAA"
            assert str(b_kwargs["venue_order_id"]) == "TS-BBB"

            # ONE upstream subscription served both hosts (the whole point).
            assert fake_upstream.calls == ["SIMSHARED"]

            task_a.cancel()
            task_b.cancel()
            await asyncio.gather(task_a, task_b, return_exceptions=True)
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_host_with_no_matching_order_id_drops_the_event_silently(
        self, tmp_path, monkeypatch
    ):
        # A host that submitted NOTHING this session must simply drop every
        # broadcast event -- never raise, never guess an owner.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        from nautilus_tradestation.feed.tail_client import FeedTailStreamClient

        fake_upstream = FakeUpstreamOrders()
        handler = FeedHandler(
            "SIMSHARED",
            tmp_path,
            fake_upstream,
            scan_secs=0.02,
            heartbeat_secs=0.02,
            manifest_flush_secs=0.05,
            alerts_dir=tmp_path / "alerts",
            enable_order_feed=True,  # Order-feed opt-in gate: this test IS about orders
        )
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMSHARED"
        try:
            await wait_until(lambda: fake_upstream.calls == ["SIMSHARED"])
            bystander = _make_harness()
            bystander.client._use_streaming = True
            bystander.client._stream_client = FeedTailStreamClient(feed_dir, poll_ms=10)
            bystander.client.generate_order_filled = MagicMock(
                side_effect=AssertionError("a bystander host must never emit anything")
            )
            task = asyncio.create_task(bystander.client._stream_order_fills())
            fake_upstream.queue.put_nowait(_order_ev("TS-NOBODY", "FLL"))
            await asyncio.sleep(0.2)
            bystander.client.generate_order_filled.assert_not_called()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
