"""
Tests for the order-event integrity hardening (brackets part 3).

Three env-gated fixes, each byte-identical when its flag is unset:

1. ``TS_VENUE_CONFIRMED_CANCELS=1`` — ``_cancel_order`` stops synthesizing
   ``OrderCanceled`` on a REST DELETE 200 (a cancel REQUEST ack).  The venue's
   own CAN event (SSE / status poll / one-shot query) makes the order terminal,
   so a fill racing the cancel can no longer be dropped by the ``is_closed``
   gate (the concurrent-fill race window).
2. ``TS_STREAM_FILL_HARDENING=1`` — ``_process_order_event`` /
   ``_check_order_statuses`` record ``_order_last_status`` only AFTER an event
   is successfully processed (the zero-price FLL skip no longer poisons the
   dedup gate), and the SSE fill path gains the poll path's 4th price fallback
   (the order's own trigger/limit price).
3. ``TS_STREAM_RECONCILE_POLL_SECS=<s>`` — a slow REST status poll runs
   behind SSE streaming as the reconciliation backstop the SSE path never had.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.model.enums import ContingencyType, OrderSide, TimeInForce, TriggerType
from nautilus_trader.model.events import OrderCanceled, OrderFilled
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    StrategyId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import MarketOrder, StopMarketOrder
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from nautilus_tradestation.execution import (
    TradeStationExecutionClient,
    _stream_reconcile_poll_secs,
)
from nautilus_tradestation.providers import TradeStationInstrumentProvider
from tests.test_kit import TSTestInstrumentStubs

_ACCOUNT = "SIM0000001F"
_TRADER_ID = TestIdStubs.trader_id()
_STRATEGY_ID = StrategyId("S-001")
_GC_ID = InstrumentId.from_str("GCJ26.TRADESTATION")


def _make_harness() -> SimpleNamespace:
    loop = asyncio.get_event_loop()
    clock = LiveClock()
    msgbus = MessageBus(trader_id=_TRADER_ID, clock=clock)
    cache = TestComponentStubs.cache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())

    http = MagicMock()
    http.base_url = "https://mock.tradestation.com/v3"
    http.cancel_order = AsyncMock(return_value={"OrderID": "TS-1", "Status": "Cancelled"})

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

    events: list = []
    msgbus.register(endpoint="ExecEngine.process", handler=events.append)
    return SimpleNamespace(client=client, http=http, cache=cache, events=events)


def _stop_order(coid: str = "O-SL", stop_price: float = 3300.0) -> StopMarketOrder:
    return StopMarketOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        instrument_id=_GC_ID,
        client_order_id=ClientOrderId(coid),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1),
        trigger_price=Price(stop_price, 1),
        trigger_type=TriggerType.DEFAULT,
        time_in_force=TimeInForce.GTC,
        init_id=UUID4(),
        ts_init=0,
        contingency_type=ContingencyType.NO_CONTINGENCY,
    )


def _market_order(coid: str = "O-MKT") -> MarketOrder:
    return MarketOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        instrument_id=_GC_ID,
        client_order_id=ClientOrderId(coid),
        order_side=OrderSide.SELL,
        quantity=Quantity.from_int(1),
        time_in_force=TimeInForce.DAY,
        init_id=UUID4(),
        ts_init=0,
    )


def _arm(h, order, ts_order_id: str = "TS-1") -> None:
    h.cache.add_order(order)
    h.client._client_order_id_to_ts_order_id[order.client_order_id] = ts_order_id
    h.client._ts_order_id_to_client_order_id[ts_order_id] = order.client_order_id


def _cancel_cmd(order) -> CancelOrder:
    return CancelOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("TS-1"),
        command_id=UUID4(),
        ts_init=0,
    )


@pytest.fixture
def flags_off(monkeypatch):
    monkeypatch.delenv("TS_VENUE_CONFIRMED_CANCELS", raising=False)
    monkeypatch.delenv("TS_STREAM_FILL_HARDENING", raising=False)
    monkeypatch.delenv("TS_STREAM_RECONCILE_POLL_SECS", raising=False)


@pytest.fixture
def hardening_on(monkeypatch):
    monkeypatch.setenv("TS_STREAM_FILL_HARDENING", "1")


class TestVenueConfirmedCancels:
    async def test_default_keeps_synthetic_cancel(self, flags_off):
        h = _make_harness()
        order = _stop_order()
        h.client._client_order_id_to_ts_order_id[order.client_order_id] = "TS-1"
        await h.client._cancel_order(_cancel_cmd(order))

        canceled = [e for e in h.events if isinstance(e, OrderCanceled)]
        assert len(canceled) == 1  # today's behavior byte-identical

    async def test_flag_suppresses_synthetic_cancel_on_rest_200(
        self, flags_off, monkeypatch
    ):
        monkeypatch.setenv("TS_VENUE_CONFIRMED_CANCELS", "1")
        h = _make_harness()
        order = _stop_order()
        h.client._client_order_id_to_ts_order_id[order.client_order_id] = "TS-1"
        await h.client._cancel_order(_cancel_cmd(order))

        # the DELETE went out, but NO terminal event was synthesized: the order
        # stays open in Nautilus until the venue's own CAN (or FLL) arrives, so
        # a racing real fill can never be dropped by the is_closed gate.
        h.http.cancel_order.assert_awaited_once_with(order_id="TS-1")
        assert not [e for e in h.events if isinstance(e, OrderCanceled)]

    async def test_flag_keeps_not_open_order_ambiguity_untouched(
        self, flags_off, monkeypatch
    ):
        monkeypatch.setenv("TS_VENUE_CONFIRMED_CANCELS", "1")
        h = _make_harness()
        order = _stop_order()
        h.client._client_order_id_to_ts_order_id[order.client_order_id] = "TS-1"
        h.http.cancel_order = AsyncMock(
            side_effect=Exception("Order TS-1 rejected: Not an open order")
        )
        await h.client._cancel_order(_cancel_cmd(order))

        assert not [e for e in h.events if isinstance(e, OrderCanceled)]

    async def test_venue_can_event_still_emits_canceled(self, flags_off, monkeypatch):
        monkeypatch.setenv("TS_VENUE_CONFIRMED_CANCELS", "1")
        h = _make_harness()
        order = _stop_order()
        _arm(h, order)
        await h.client._process_order_event({"OrderID": "TS-1", "Status": "CAN"})

        canceled = [e for e in h.events if isinstance(e, OrderCanceled)]
        assert len(canceled) == 1


class TestStreamFillHardening:
    _ZERO_PX_FLL = {
        "OrderID": "TS-1",
        "Status": "FLL",
        "AveragePrice": "0",
        "FilledQuantity": "1",
    }

    async def test_default_zero_price_fll_is_dropped_forever(self, flags_off):
        """Lock in today's defect so the flag's value is measurable: the dedup
        gate is poisoned BEFORE the zero-price skip, so the repeat is dropped."""
        h = _make_harness()
        _arm(h, _market_order("O-MKT"))  # market order: no 4th fallback either
        await h.client._process_order_event(dict(self._ZERO_PX_FLL))
        assert h.client._order_last_status["TS-1"] == "FLL"  # poisoned
        # the repeat (now WITH a price) is dropped by the dedup gate
        await h.client._process_order_event(
            {**self._ZERO_PX_FLL, "FilledPrice": "3300.0"}
        )
        assert not [e for e in h.events if isinstance(e, OrderFilled)]

    async def test_hardened_zero_price_fll_stays_retryable(
        self, flags_off, hardening_on
    ):
        h = _make_harness()
        _arm(h, _market_order("O-MKT"))
        await h.client._process_order_event(dict(self._ZERO_PX_FLL))
        # the skip did NOT poison the dedup gate
        assert "TS-1" not in h.client._order_last_status
        # the retry (price now resolvable) emits the fill
        await h.client._process_order_event(
            {**self._ZERO_PX_FLL, "FilledPrice": "3300.0"}
        )
        fills = [e for e in h.events if isinstance(e, OrderFilled)]
        assert len(fills) == 1
        assert float(fills[0].last_px) == 3300.0
        assert h.client._order_last_status["TS-1"] == "FLL"

    async def test_hardened_uses_orders_own_trigger_price_fourth_fallback(
        self, flags_off, hardening_on
    ):
        h = _make_harness()
        _arm(h, _stop_order("O-SL", stop_price=3300.0))
        await h.client._process_order_event(dict(self._ZERO_PX_FLL))

        fills = [e for e in h.events if isinstance(e, OrderFilled)]
        assert len(fills) == 1
        assert float(fills[0].last_px) == 3300.0  # the stop's own trigger price
        assert h.client._order_last_status["TS-1"] == "FLL"

    async def test_hardened_poll_path_retries_failed_fill_emission(
        self, flags_off, hardening_on
    ):
        h = _make_harness()
        _arm(h, _stop_order("O-SL", stop_price=3300.0))
        h.http.get_orders = AsyncMock(
            return_value=[
                {
                    "OrderID": "TS-1",
                    "Status": "FLL",
                    "AveragePrice": "3300.0",
                    "FilledQuantity": "1",
                }
            ]
        )
        # First pass: force the fill emission to fail.
        original = h.client.generate_order_filled
        h.client.generate_order_filled = MagicMock(side_effect=RuntimeError("boom"))
        await h.client._check_order_statuses()
        assert "TS-1" not in h.client._order_last_status  # not poisoned
        # Second pass: emission works -> the fill is recovered.
        h.client.generate_order_filled = original
        await h.client._check_order_statuses()
        fills = [e for e in h.events if isinstance(e, OrderFilled)]
        assert len(fills) == 1
        assert h.client._order_last_status["TS-1"] == "FLL"


class TestStreamReconcilePoll:
    def test_env_parsing(self, flags_off, monkeypatch):
        assert _stream_reconcile_poll_secs() == 0.0
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_SECS", "30")
        assert _stream_reconcile_poll_secs() == 30.0
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_SECS", "-5")
        assert _stream_reconcile_poll_secs() == 0.0
        monkeypatch.setenv("TS_STREAM_RECONCILE_POLL_SECS", "junk")
        assert _stream_reconcile_poll_secs() == 0.0

    async def test_loop_invokes_check_order_statuses(self, flags_off):
        h = _make_harness()
        calls = []

        async def _fake_check():
            calls.append(1)
            raise asyncio.CancelledError  # one iteration, then stop

        h.client._check_order_statuses = _fake_check
        await h.client._reconcile_order_statuses_loop(0.01)
        assert calls == [1]
