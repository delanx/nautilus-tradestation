"""
Tests for the stream-aware reconcile-poll cadence (API-quota safety).

Root cause: ``TS_STREAM_RECONCILE_POLL_SECS=30`` (when set process-wide)
made ``_check_order_statuses`` run every 30s per
node regardless of whether that node had anything outstanding to reconcile.
Many node processes sharing one TradeStation account drove a continuous
stream of REST calls against that account's rate-limit bucket, which no
bar-data refresher throttle counts (it is a completely separate poller).

This fix makes the poll stream-aware: fast (``TS_STREAM_RECONCILE_POLL_SECS``,
unchanged, default unset/off) while there is a non-terminal order to
reconcile, backed off to >=300s (``TS_STREAM_RECONCILE_POLL_IDLE_SECS``,
floored) once the client is quiescent. Also verifies the cadence's pure
decision function and its cross-process desync jitter.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.model.identifiers import AccountId

from nautilus_tradestation.execution import (
    _RECONCILE_IDLE_FLOOR_SECONDS,
    TradeStationExecutionClient,
    _reconcile_initial_jitter_seconds,
    _should_poll_now,
    _stream_reconcile_idle_secs,
)
from nautilus_tradestation.providers import TradeStationInstrumentProvider

from tests.test_execution_event_hardening import (
    _ACCOUNT,
    _TRADER_ID,
    _arm,
    _make_harness,
    _market_order,
    _stop_order,
)
from tests.test_kit import TSTestInstrumentStubs


class _OverridableCache(Cache):
    """Real ``nautilus_trader`` ``Cache`` subclass that lets a test force
    what ``order(client_order_id)`` returns for one order id.

    ``Cache`` is a Cython extension type: neither its instances (no
    ``__dict__`` -- "object has no attribute '__dict__'") nor its class
    (``TypeError: cannot set 'order' attribute of immutable type``) can be
    monkeypatched from Python, and ``TradeStationExecutionClient``'s
    ``cache=`` constructor argument is type-checked to require an actual
    ``Cache`` instance (a plain duck-typed fake raises
    ``TypeError: Argument 'cache' has incorrect type``). Subclassing and
    overriding the cpdef method IS the supported Cython mechanism, and a
    subclass instance satisfies the ``isinstance`` check -- so this is the
    real production ``Cache`` for everything except the one method under
    test's control.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._overrides: dict = {}

    def force_order(self, client_order_id_str: str, value) -> None:
        self._overrides[client_order_id_str] = value

    def order(self, client_order_id):
        key = str(client_order_id)
        if key in self._overrides:
            return self._overrides[key]
        return super().order(client_order_id)


def _make_overridable_harness() -> SimpleNamespace:
    loop = asyncio.get_event_loop()
    clock = LiveClock()
    msgbus = MessageBus(trader_id=_TRADER_ID, clock=clock)
    cache = _OverridableCache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())

    http = MagicMock()
    http.base_url = "https://mock.tradestation.com/v3"

    client = TradeStationExecutionClient(
        loop=loop,
        client=http,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=TradeStationInstrumentProvider(client=http),
        account_id=_ACCOUNT,
    )
    client._set_account_id(AccountId(f"TRADESTATION-{_ACCOUNT}"))
    return SimpleNamespace(client=client, cache=cache)


class TestStreamReconcileIdleSecsEnvParsing:
    def test_unset_defaults_to_floor(self, monkeypatch):
        monkeypatch.delenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", raising=False)
        assert _stream_reconcile_idle_secs() == _RECONCILE_IDLE_FLOOR_SECONDS
        assert _RECONCILE_IDLE_FLOOR_SECONDS == 300.0

    def test_junk_falls_back_to_floor(self, monkeypatch):
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", "not-a-number")
        assert _stream_reconcile_idle_secs() == _RECONCILE_IDLE_FLOOR_SECONDS

    def test_value_below_floor_is_floored(self, monkeypatch):
        """A fat-fingered small override must NEVER recreate the fast
        steady-state poll this cadence split exists to remove."""
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", "30")
        assert _stream_reconcile_idle_secs() == _RECONCILE_IDLE_FLOOR_SECONDS

    def test_value_above_floor_is_honored(self, monkeypatch):
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", "600")
        assert _stream_reconcile_idle_secs() == 600.0

    def test_negative_falls_back_to_floor(self, monkeypatch):
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", "-5")
        assert _stream_reconcile_idle_secs() == _RECONCILE_IDLE_FLOOR_SECONDS


class TestShouldPollNow:
    """Pure cadence decision function -- no real sleeping required."""

    def test_first_tick_always_polls(self):
        assert _should_poll_now(
            pending=False,
            last_poll_time=None,
            now=1000.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )
        assert _should_poll_now(
            pending=True,
            last_poll_time=None,
            now=1000.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )

    def test_pending_polls_at_fast_cadence(self):
        # 30s elapsed since last poll, pending -> due (fast interval reached).
        assert _should_poll_now(
            pending=True,
            last_poll_time=1000.0,
            now=1030.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )
        # Only 10s elapsed -> not due yet.
        assert not _should_poll_now(
            pending=True,
            last_poll_time=1000.0,
            now=1010.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )

    def test_idle_backs_off_to_slow_cadence(self):
        # 30s elapsed but NOT pending -> must NOT poll (idle cadence not reached).
        assert not _should_poll_now(
            pending=False,
            last_poll_time=1000.0,
            now=1030.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )
        # 299s elapsed, still not due.
        assert not _should_poll_now(
            pending=False,
            last_poll_time=1000.0,
            now=1299.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )
        # 300s elapsed -> due.
        assert _should_poll_now(
            pending=False,
            last_poll_time=1000.0,
            now=1300.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )

    def test_resumes_fast_cadence_the_instant_pending_reappears(self):
        # Only 30s since the last (idle-cadence) poll, but a new order just
        # went pending -> the very next tick must poll at the fast cadence,
        # not wait out the remaining ~270s of the idle window.
        assert _should_poll_now(
            pending=True,
            last_poll_time=1000.0,
            now=1030.0,
            fast_interval_secs=30.0,
            idle_interval_secs=300.0,
        )


class TestReconcileInitialJitter:
    def test_zero_interval_gives_zero_jitter(self):
        assert _reconcile_initial_jitter_seconds(0.0) == 0.0
        assert _reconcile_initial_jitter_seconds(-1.0) == 0.0

    def test_jitter_bounded_by_interval(self):
        for _ in range(200):
            j = _reconcile_initial_jitter_seconds(30.0)
            assert 0.0 <= j < 30.0

    def test_jitter_is_not_constant(self):
        # A real random draw, not silently degenerated to always-zero.
        draws = {_reconcile_initial_jitter_seconds(30.0) for _ in range(50)}
        assert len(draws) > 1


class TestHasPendingOrders:
    async def test_no_orders_tracked_is_not_pending(self):
        h = _make_harness()
        assert h.client._has_pending_orders() is False

    async def test_order_with_no_observed_status_is_pending(self):
        h = _make_harness()
        _arm(h, _market_order("O-1"), ts_order_id="TS-1")
        # Never observed by SSE or a poll yet -- fail toward "pending".
        assert h.client._has_pending_orders() is True

    async def test_order_with_terminal_status_and_closed_cache_is_not_pending(self):
        # Pins the two inputs _has_pending_orders actually reads
        # (_order_last_status + cached_order.is_closed) against the real
        # Cache type via _OverridableCache (see its docstring for why a
        # plain fake/mock cannot stand in here).
        h = _make_overridable_harness()
        _arm(h, _stop_order("O-1"), ts_order_id="TS-1")
        h.client._order_last_status["TS-1"] = "FLL"
        h.cache.force_order("O-1", _ClosedOrderStub())
        assert h.client._has_pending_orders() is False

    async def test_order_with_nonterminal_status_is_pending(self):
        h = _make_harness()
        _arm(h, _stop_order("O-1"), ts_order_id="TS-1")
        h.client._order_last_status["TS-1"] = "OPN"
        assert h.client._has_pending_orders() is True

    async def test_terminal_status_but_cache_says_still_open_is_pending(self):
        """Fail toward polling: a terminal last_status alone is not trusted
        if the cached order disagrees (defensive belt-and-suspenders)."""
        h = _make_overridable_harness()
        _arm(h, _stop_order("O-1"), ts_order_id="TS-1")
        h.client._order_last_status["TS-1"] = "FLL"
        h.cache.force_order("O-1", _OpenOrderStub())
        assert h.client._has_pending_orders() is True

    async def test_missing_cache_entry_is_pending(self):
        h = _make_overridable_harness()
        _arm(h, _stop_order("O-1"), ts_order_id="TS-1")
        h.client._order_last_status["TS-1"] = "FLL"
        h.cache.force_order("O-1", None)
        assert h.client._has_pending_orders() is True

    async def test_mixed_book_pending_if_any_order_outstanding(self):
        h = _make_overridable_harness()
        _arm(h, _stop_order("O-1"), ts_order_id="TS-1")
        _arm(h, _market_order("O-2"), ts_order_id="TS-2")
        h.client._order_last_status["TS-1"] = "FLL"
        h.cache.force_order("O-1", _ClosedOrderStub())
        h.cache.force_order("O-2", _OpenOrderStub())
        # TS-2 has no observed status at all yet -> pending.
        assert h.client._has_pending_orders() is True


class _ClosedOrderStub:
    is_closed = True


class _OpenOrderStub:
    is_closed = False


class TestReconcileLoopFastWhilePending:
    """Real-time integration slice against the actual production coroutine
    (``_reconcile_order_statuses_loop``), not a reimplementation. Jitter is
    pinned to zero so the tick count is exact."""

    async def test_calls_every_fast_tick_while_pending(self, monkeypatch):
        monkeypatch.delenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", raising=False)
        monkeypatch.setattr(
            "nautilus_tradestation.execution._reconcile_initial_jitter_seconds",
            lambda _interval: 0.0,
        )
        h = _make_harness()
        h.client._has_pending_orders = lambda: True
        calls = []

        # (REST budget): _check_order_statuses now
        # takes a `budget_kind` label so the REST-budget journal can tell its
        # three callers apart. This stub must accept it -- without the kwarg the
        # real loop raises TypeError into its own broad `except Exception`, never
        # reaches the test's CancelledError, and spins forever instead of failing.
        async def _fake_check(*, budget_kind=None):
            calls.append(h.client._loop.time())
            if len(calls) >= 3:
                raise asyncio.CancelledError

        h.client._check_order_statuses = _fake_check
        await h.client._reconcile_order_statuses_loop(0.01)
        assert len(calls) == 3

    async def test_skips_subsequent_ticks_while_idle(self, monkeypatch):
        """Not pending: the loop's own ``asyncio.sleep`` is faked to a no-op
        so a fixed, deterministic number of loop iterations can be exercised
        without waiting out (or racing) the real 300s idle floor. Only the
        first (unconditional) tick should reach ``_check_order_statuses``;
        the floor guarantees the next 3 ticks (well under 300s of virtual
        time at 0.01s/tick) are not due yet."""
        monkeypatch.delenv("TS_STREAM_RECONCILE_POLL_IDLE_SECS", raising=False)
        monkeypatch.setattr(
            "nautilus_tradestation.execution._reconcile_initial_jitter_seconds",
            lambda _interval: 0.0,
        )
        h = _make_harness()
        h.client._has_pending_orders = lambda: False
        calls = []

        # (REST budget): _check_order_statuses now
        # takes a `budget_kind` label so the REST-budget journal can tell its
        # three callers apart. This stub must accept it -- without the kwarg the
        # real loop raises TypeError into its own broad `except Exception`, never
        # reaches the test's CancelledError, and spins forever instead of failing.
        async def _fake_check(*, budget_kind=None):
            calls.append(1)

        h.client._check_order_statuses = _fake_check

        tick = {"n": 0}
        real_sleep = asyncio.sleep

        async def _fake_sleep(_secs):
            tick["n"] += 1
            if tick["n"] > 4:
                raise asyncio.CancelledError
            await real_sleep(0)  # yield control, no real delay

        monkeypatch.setattr(
            "nautilus_tradestation.execution.asyncio.sleep", _fake_sleep
        )
        await h.client._reconcile_order_statuses_loop(0.01)
        assert tick["n"] == 5
        assert calls == [1]
