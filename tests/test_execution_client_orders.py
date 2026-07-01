"""
Tests for TradeStationExecutionClient order submission / cancel / replace —
brackets part 1 (resting SL/PT design).

The client is instantiated with real Nautilus components (msgbus, cache,
clock) and a mocked HTTP client (AsyncMock methods), so the full
``_submit_order`` / ``_submit_order_list`` / ``_modify_order`` /
``_cancel_order`` paths run without any network calls. Generated order
events are captured off the message bus.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    CancelOrder,
    ModifyOrder,
    SubmitOrder,
    SubmitOrderList,
)
from nautilus_trader.model.enums import (
    ContingencyType,
    OrderSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.events import (
    OrderAccepted,
    OrderCanceled,
    OrderRejected,
    OrderUpdated,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientOrderId,
    InstrumentId,
    OrderListId,
    StrategyId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import (
    LimitOrder,
    MarketOrder,
    OrderList,
    StopMarketOrder,
)
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from nautilus_tradestation.execution import TradeStationExecutionClient
from nautilus_tradestation.providers import TradeStationInstrumentProvider
from tests.test_kit import TSTestInstrumentStubs


_ACCOUNT = "SIM0000001F"
_TRADER_ID = TestIdStubs.trader_id()
_STRATEGY_ID = StrategyId("S-001")
_GC_ID = InstrumentId.from_str("GCJ26.TRADESTATION")
_AAPL_ID = InstrumentId.from_str("AAPL.TRADESTATION")


def _make_harness() -> SimpleNamespace:
    """Build an exec client wired to real components + a mocked HTTP client."""
    loop = asyncio.get_event_loop()
    clock = LiveClock()
    msgbus = MessageBus(trader_id=_TRADER_ID, clock=clock)
    cache = TestComponentStubs.cache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())
    cache.add_instrument(TSTestInstrumentStubs.aapl_equity())

    http = MagicMock()
    http.base_url = "https://mock.tradestation.com/v3"
    http.place_order = AsyncMock(return_value={"OrderID": "TS-1"})
    http.place_order_group = AsyncMock(
        return_value={
            "OrderGroupId": "GRP-001",
            "Orders": [{"OrderID": "TS-SL"}, {"OrderID": "TS-TP"}],
        }
    )
    http.replace_order = AsyncMock(return_value={"OrderID": "TS-NEW"})
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


def _uid():
    return UUID4()


def _stop_order(
    coid: str,
    side: OrderSide,
    stop_price: float,
    instrument_id: InstrumentId = _GC_ID,
    precision: int = 1,
    contingency: ContingencyType = ContingencyType.OCO,
    linked: list[str] | None = None,
    tags: list[str] | None = None,
    qty: int = 1,
) -> StopMarketOrder:
    return StopMarketOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(coid),
        order_side=side,
        quantity=Quantity.from_int(qty),
        trigger_price=Price(stop_price, precision),
        trigger_type=TriggerType.DEFAULT,
        time_in_force=TimeInForce.GTC,
        init_id=_uid(),
        ts_init=0,
        contingency_type=contingency,
        order_list_id=OrderListId("OL-001"),
        linked_order_ids=[ClientOrderId(c) for c in (linked or [])],
        tags=tags,
    )


def _limit_order(
    coid: str,
    side: OrderSide,
    price: float,
    instrument_id: InstrumentId = _GC_ID,
    precision: int = 1,
    contingency: ContingencyType = ContingencyType.OCO,
    linked: list[str] | None = None,
    tags: list[str] | None = None,
    qty: int = 1,
) -> LimitOrder:
    return LimitOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(coid),
        order_side=side,
        quantity=Quantity.from_int(qty),
        price=Price(price, precision),
        time_in_force=TimeInForce.GTC,
        init_id=_uid(),
        ts_init=0,
        contingency_type=contingency,
        order_list_id=OrderListId("OL-001"),
        linked_order_ids=[ClientOrderId(c) for c in (linked or [])],
        tags=tags,
    )


def _market_order(coid: str, side: OrderSide, qty: int = 1) -> MarketOrder:
    return MarketOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        instrument_id=_GC_ID,
        client_order_id=ClientOrderId(coid),
        order_side=side,
        quantity=Quantity.from_int(qty),
        time_in_force=TimeInForce.DAY,
        init_id=_uid(),
        ts_init=0,
    )


def _submit_order_cmd(order) -> SubmitOrder:
    return SubmitOrder(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        order=order,
        command_id=_uid(),
        ts_init=0,
    )


def _submit_order_list_cmd(orders: list) -> SubmitOrderList:
    return SubmitOrderList(
        trader_id=_TRADER_ID,
        strategy_id=_STRATEGY_ID,
        order_list=OrderList(order_list_id=OrderListId("OL-001"), orders=orders),
        command_id=_uid(),
        ts_init=0,
    )


def _oco_bracket_pair(side_to_close: str = "long") -> list:
    """Build the OCO pair: StopMarket SL + Limit TP, GTC.

    ``side_to_close="long"`` → SELL legs (SL below entry, TP above);
    ``"short"`` → BUY legs (SL above entry, TP below).
    """
    if side_to_close == "long":
        side = OrderSide.SELL
        sl_px, tp_px = 3300.0, 3500.0
        intent = "TS_INTENT:close_long"
    else:
        side = OrderSide.BUY
        sl_px, tp_px = 3500.0, 3300.0
        intent = "TS_INTENT:close_short"
    tags = [intent, "TS_BRACKET:pod_x:O-ENTRY"]
    sl = _stop_order("O-SL", side, sl_px, linked=["O-TP"], tags=tags)
    tp = _limit_order("O-TP", side, tp_px, linked=["O-SL"], tags=tags)
    return [sl, tp]


class TestSubmitRestingOrders:
    """Plain submission of the resting order types the bracket design uses."""

    async def test_stop_market_submission_params(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0,
                            contingency=ContingencyType.NO_CONTINGENCY)
        await h.client._submit_order(_submit_order_cmd(order))

        h.http.place_order.assert_awaited_once()
        kwargs = h.http.place_order.await_args.kwargs
        assert kwargs["order_type"] == "StopMarket"
        assert kwargs["stop_price"] == "3300.0"
        assert kwargs["trade_action"] == "Sell"
        assert kwargs["time_in_force"] == "GTC"
        assert "limit_price" not in kwargs

    async def test_limit_submission_params(self):
        h = _make_harness()
        order = _limit_order("O-TP", OrderSide.SELL, 3500.0,
                             contingency=ContingencyType.NO_CONTINGENCY)
        await h.client._submit_order(_submit_order_cmd(order))

        kwargs = h.http.place_order.await_args.kwargs
        assert kwargs["order_type"] == "Limit"
        assert kwargs["limit_price"] == "3500.0"
        assert kwargs["time_in_force"] == "GTC"
        assert "stop_price" not in kwargs

    async def test_submission_registers_ids_and_accepts(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0,
                            contingency=ContingencyType.NO_CONTINGENCY)
        await h.client._submit_order(_submit_order_cmd(order))

        assert h.client._client_order_id_to_ts_order_id[ClientOrderId("O-SL")] == "TS-1"
        assert h.client._ts_order_id_to_client_order_id["TS-1"] == ClientOrderId("O-SL")
        accepted = [e for e in h.events if isinstance(e, OrderAccepted)]
        assert len(accepted) == 1
        assert accepted[0].venue_order_id == VenueOrderId("TS-1")

    async def test_submission_failure_generates_rejected(self):
        h = _make_harness()
        h.http.place_order = AsyncMock(side_effect=Exception("boom"))
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0,
                            contingency=ContingencyType.NO_CONTINGENCY)
        await h.client._submit_order(_submit_order_cmd(order))

        rejected = [e for e in h.events if isinstance(e, OrderRejected)]
        assert len(rejected) == 1
        assert "boom" in rejected[0].reason


class TestSubmitOcoOrderList:
    """The OCO pair routes to POST /orderexecution/ordergroups."""

    async def test_oco_pair_routes_to_group_endpoint(self):
        h = _make_harness()
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        h.http.place_order_group.assert_awaited_once()
        h.http.place_order.assert_not_awaited()
        kwargs = h.http.place_order_group.await_args.kwargs
        assert kwargs["group_type"] == "OCO"
        assert len(kwargs["orders"]) == 2

    async def test_oco_payloads_have_stop_and_limit_legs(self):
        h = _make_harness()
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        payloads = h.http.place_order_group.await_args.kwargs["orders"]
        stop_payload = next(p for p in payloads if p["OrderType"] == "StopMarket")
        limit_payload = next(p for p in payloads if p["OrderType"] == "Limit")
        assert stop_payload["StopPrice"] == "3300.0"
        assert limit_payload["LimitPrice"] == "3500.0"
        assert all(p["TimeInForce"] == {"Duration": "GTC"} for p in payloads)
        assert all(p["AccountID"] == _ACCOUNT for p in payloads)

    async def test_oco_legs_registered_and_accepted(self):
        h = _make_harness()
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        assert h.client._client_order_id_to_ts_order_id[ClientOrderId("O-SL")] == "TS-SL"
        assert h.client._client_order_id_to_ts_order_id[ClientOrderId("O-TP")] == "TS-TP"
        accepted = [e for e in h.events if isinstance(e, OrderAccepted)]
        assert {str(e.venue_order_id) for e in accepted} == {"TS-SL", "TS-TP"}

    async def test_futures_oco_legs_keep_plain_trade_actions(self):
        # GCJ26 is a future — intent tags must NOT produce BuyToCover.
        h = _make_harness()
        await h.client._submit_order_list(
            _submit_order_list_cmd(_oco_bracket_pair(side_to_close="short"))
        )

        payloads = h.http.place_order_group.await_args.kwargs["orders"]
        assert [p["TradeAction"] for p in payloads] == ["Buy", "Buy"]

    async def test_equity_oco_cover_legs_use_buy_to_cover(self):
        # Equity short-cover rejection class: AAPL is an Equity in the cache, both
        # legs are tagged close_short BUYs → group payload must say BuyToCover.
        h = _make_harness()
        tags = ["TS_INTENT:close_short", "TS_BRACKET:pod_x:O-ENTRY"]
        sl = _stop_order("O-SL", OrderSide.BUY, 460.0, instrument_id=_AAPL_ID,
                         precision=2, linked=["O-TP"], tags=tags)
        tp = _limit_order("O-TP", OrderSide.BUY, 430.0, instrument_id=_AAPL_ID,
                          precision=2, linked=["O-SL"], tags=tags)
        await h.client._submit_order_list(_submit_order_list_cmd([sl, tp]))

        payloads = h.http.place_order_group.await_args.kwargs["orders"]
        assert [p["TradeAction"] for p in payloads] == ["BuyToCover", "BuyToCover"]

    async def test_group_http_failure_rejects_all_legs(self):
        h = _make_harness()
        h.http.place_order_group = AsyncMock(side_effect=Exception("group failed"))
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        rejected = [e for e in h.events if isinstance(e, OrderRejected)]
        assert {str(e.client_order_id) for e in rejected} == {"O-SL", "O-TP"}
        assert not h.client._client_order_id_to_ts_order_id

    async def test_unlinked_list_falls_back_to_individual_orders(self):
        h = _make_harness()
        h.http.place_order = AsyncMock(
            side_effect=[{"OrderID": "TS-1"}, {"OrderID": "TS-2"}]
        )
        o1 = _limit_order("O-1", OrderSide.BUY, 3400.0,
                          contingency=ContingencyType.NO_CONTINGENCY)
        o2 = _limit_order("O-2", OrderSide.SELL, 3450.0,
                          contingency=ContingencyType.NO_CONTINGENCY)
        await h.client._submit_order_list(_submit_order_list_cmd([o1, o2]))

        h.http.place_order_group.assert_not_awaited()
        assert h.http.place_order.await_count == 2


class TestGroupLegIdMapping:
    """An unmapped leg is an invisible order: fail loud."""

    async def test_leg_missing_order_id_is_rejected(self):
        h = _make_harness()
        h.http.place_order_group = AsyncMock(
            return_value={
                "OrderGroupId": "GRP-001",
                "Orders": [{"OrderID": "TS-SL"}, {"Message": "leg failed"}],
            }
        )
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        accepted = [e for e in h.events if isinstance(e, OrderAccepted)]
        rejected = [e for e in h.events if isinstance(e, OrderRejected)]
        assert [str(e.client_order_id) for e in accepted] == ["O-SL"]
        assert [str(e.client_order_id) for e in rejected] == ["O-TP"]
        assert "no OrderID" in rejected[0].reason

    async def test_short_response_rejects_missing_legs(self):
        h = _make_harness()
        h.http.place_order_group = AsyncMock(
            return_value={"OrderGroupId": "GRP-001", "Orders": [{"OrderID": "TS-SL"}]}
        )
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        accepted = [e for e in h.events if isinstance(e, OrderAccepted)]
        rejected = [e for e in h.events if isinstance(e, OrderRejected)]
        assert [str(e.client_order_id) for e in accepted] == ["O-SL"]
        assert [str(e.client_order_id) for e in rejected] == ["O-TP"]

    async def test_mapped_leg_still_registered_when_sibling_unmapped(self):
        h = _make_harness()
        h.http.place_order_group = AsyncMock(
            return_value={"OrderGroupId": "GRP-001", "Orders": [{"OrderID": "TS-SL"}]}
        )
        await h.client._submit_order_list(_submit_order_list_cmd(_oco_bracket_pair()))

        assert h.client._client_order_id_to_ts_order_id == {
            ClientOrderId("O-SL"): "TS-SL"
        }


class TestModifyOrder:
    """Cancel/replace — atomic PUT replace with ID re-mapping."""

    def _arm(self, h, order, ts_order_id: str = "TS-1") -> None:
        h.cache.add_order(order)
        h.client._client_order_id_to_ts_order_id[order.client_order_id] = ts_order_id
        h.client._ts_order_id_to_client_order_id[ts_order_id] = order.client_order_id

    def _modify_cmd(self, order, quantity=None, price=None, trigger_price=None):
        return ModifyOrder(
            trader_id=_TRADER_ID,
            strategy_id=_STRATEGY_ID,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId("TS-1"),
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            command_id=_uid(),
            ts_init=0,
        )

    async def test_replace_stop_price(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"])
        self._arm(h, order)
        await h.client._modify_order(
            self._modify_cmd(order, trigger_price=Price(3310.0, 1))
        )

        h.http.replace_order.assert_awaited_once()
        kwargs = h.http.replace_order.await_args.kwargs
        assert kwargs["order_id"] == "TS-1"
        assert kwargs["stop_price"] == "3310.0"
        assert kwargs["order_type"] == "StopMarket"

    async def test_replace_remaps_new_venue_order_id(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"])
        self._arm(h, order)
        await h.client._modify_order(
            self._modify_cmd(order, trigger_price=Price(3310.0, 1))
        )

        assert h.client._client_order_id_to_ts_order_id[ClientOrderId("O-SL")] == "TS-NEW"
        assert h.client._ts_order_id_to_client_order_id["TS-NEW"] == ClientOrderId("O-SL")
        assert "TS-1" not in h.client._ts_order_id_to_client_order_id
        updated = [e for e in h.events if isinstance(e, OrderUpdated)]
        assert len(updated) == 1
        assert updated[0].venue_order_id == VenueOrderId("TS-NEW")

    async def test_replace_honors_command_quantity(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"], qty=2)
        self._arm(h, order)
        await h.client._modify_order(
            self._modify_cmd(
                order, quantity=Quantity.from_int(1), trigger_price=Price(3310.0, 1)
            )
        )

        kwargs = h.http.replace_order.await_args.kwargs
        assert kwargs["quantity"] == "1"
        updated = [e for e in h.events if isinstance(e, OrderUpdated)]
        assert updated[0].quantity == Quantity.from_int(1)

    async def test_replace_defaults_to_order_quantity(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"], qty=2)
        self._arm(h, order)
        await h.client._modify_order(
            self._modify_cmd(order, trigger_price=Price(3310.0, 1))
        )

        kwargs = h.http.replace_order.await_args.kwargs
        assert kwargs["quantity"] == "2"

    async def test_replace_equity_cover_keeps_buy_to_cover(self):
        # Equity short-cover rejection class on the replace path: a tagged short-cover stop must
        # not flip back to plain 'Buy' when its price is replaced.
        h = _make_harness()
        order = _stop_order(
            "O-SL", OrderSide.BUY, 460.0, instrument_id=_AAPL_ID, precision=2,
            linked=["O-TP"], tags=["TS_INTENT:close_short"],
        )
        self._arm(h, order)
        await h.client._modify_order(
            self._modify_cmd(order, trigger_price=Price(455.0, 2))
        )

        kwargs = h.http.replace_order.await_args.kwargs
        assert kwargs["trade_action"] == "BuyToCover"

    async def test_replace_futures_keeps_plain_action(self):
        h = _make_harness()
        order = _stop_order(
            "O-SL", OrderSide.BUY, 3300.0, linked=["O-TP"],
            tags=["TS_INTENT:close_short"],
        )
        self._arm(h, order)
        await h.client._modify_order(
            self._modify_cmd(order, trigger_price=Price(3310.0, 1))
        )

        kwargs = h.http.replace_order.await_args.kwargs
        assert kwargs["trade_action"] == "Buy"

    async def test_modify_unknown_order_is_noop(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"])
        # NOT armed — no ID mapping
        await h.client._modify_order(
            self._modify_cmd(order, trigger_price=Price(3310.0, 1))
        )

        h.http.replace_order.assert_not_awaited()
        assert not h.events


class TestCancelOrder:
    """Cancel — including the 'Not an open order' fill-race ambiguity."""

    def _cancel_cmd(self, order) -> CancelOrder:
        return CancelOrder(
            trader_id=_TRADER_ID,
            strategy_id=_STRATEGY_ID,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=VenueOrderId("TS-1"),
            command_id=_uid(),
            ts_init=0,
        )

    async def test_cancel_calls_http_and_generates_canceled(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"])
        h.client._client_order_id_to_ts_order_id[order.client_order_id] = "TS-1"
        await h.client._cancel_order(self._cancel_cmd(order))

        h.http.cancel_order.assert_awaited_once_with(order_id="TS-1")
        canceled = [e for e in h.events if isinstance(e, OrderCanceled)]
        assert len(canceled) == 1
        assert canceled[0].venue_order_id == VenueOrderId("TS-1")

    async def test_cancel_not_open_order_generates_no_event(self):
        # The deliberate behavior: a concurrent fill must not be masked
        # by a synthetic cancel — the real fill event resolves the order.
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"])
        h.client._client_order_id_to_ts_order_id[order.client_order_id] = "TS-1"
        h.http.cancel_order = AsyncMock(
            side_effect=Exception("Order TS-1 rejected: Not an open order")
        )
        await h.client._cancel_order(self._cancel_cmd(order))

        canceled = [e for e in h.events if isinstance(e, OrderCanceled)]
        assert not canceled

    async def test_cancel_unknown_order_is_noop(self):
        h = _make_harness()
        order = _stop_order("O-SL", OrderSide.SELL, 3300.0, linked=["O-TP"])
        await h.client._cancel_order(self._cancel_cmd(order))

        h.http.cancel_order.assert_not_awaited()
        assert not h.events
