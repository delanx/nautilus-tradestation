"""
Parsing functions for TradeStation execution reports and order conversion.
"""
import logging
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import ContingencyType, LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce
from nautilus_trader.model.identifiers import AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.model.orders import LimitOrder, MarketOrder, Order, StopLimitOrder, StopMarketOrder


_log = logging.getLogger(__name__)


def convert_order_type(order: Order) -> str:
    """Convert a NautilusTrader order to a TradeStation order type string.

    Raises
    ------
    ValueError
        If the order type is not supported by TradeStation.
    """
    if isinstance(order, MarketOrder):
        return "Market"
    elif isinstance(order, LimitOrder):
        return "Limit"
    elif isinstance(order, StopMarketOrder):
        return "StopMarket"
    elif isinstance(order, StopLimitOrder):
        return "StopLimit"
    else:
        raise ValueError(f"Unsupported order type: {type(order)}")


def convert_time_in_force(tif: TimeInForce) -> str:
    """Convert a NautilusTrader TimeInForce to a TradeStation duration string.

    Unknown values default to 'DAY' (TradeStation rejects FOK; use DAY instead).
    """
    _MAP = {
        TimeInForce.DAY: "DAY",
        TimeInForce.GTC: "GTC",
        TimeInForce.IOC: "IOC",
        TimeInForce.FOK: "FOK",
    }
    return _MAP.get(tif, "DAY")


def intent_from_tags(order: Order) -> str | None:
    """Return the TS_INTENT tag value from an order's tags, if present.

    Submitting strategies tag orders with their intent (e.g.
    ``TS_INTENT:close_short``) so the adapter can choose the correct
    TradeStation TradeAction without inferring from cached position state
    (the SPY EQUITY-GROUP-ORDER 'boxed position' lesson — the cache can be wrong).

    Parameters
    ----------
    order : Order
        The order whose tags to inspect.

    Returns
    -------
    str | None
        The intent value (e.g. ``"close_short"``), or ``None`` when no
        ``TS_INTENT:`` tag is present.
    """
    for tag in order.tags or []:
        tag_str = str(tag)
        if tag_str.startswith("TS_INTENT:"):
            return tag_str.split(":", 1)[1]
    return None


def equity_trade_action_from_intent(order: Order) -> str | None:
    """Map an EQUITY order's TS_INTENT tag to a TradeStation TradeAction.

    TradeStation requires ``BuyToCover`` to close an equity short and plain
    ``Sell`` to close an equity long. Futures must NOT use this mapping —
    TradeStation rejects SellShort/BuyToCover on futures.

    Parameters
    ----------
    order : Order
        The equity order whose intent tag to map.

    Returns
    -------
    str | None
        ``"BuyToCover"`` for a tagged short-cover BUY, ``"Sell"`` for a
        tagged long-close SELL, or ``None`` when no intent tag applies
        (caller keeps its default Buy/Sell action).
    """
    intent = intent_from_tags(order)
    if intent == "close_short" and order.side == OrderSide.BUY:
        return "BuyToCover"
    if intent == "close_long" and order.side == OrderSide.SELL:
        return "Sell"
    return None


def convert_order_to_ts_format(order: Order, account_id: str) -> dict[str, Any]:
    """Convert a NautilusTrader order to the kwargs dict for TradeStationHttpClient.place_order.

    Parameters
    ----------
    order : Order
        The order to convert.
    account_id : str
        The TradeStation account ID.

    Returns
    -------
    dict[str, Any]
        Keyword arguments ready to pass to ``client.place_order(**result)``.
    """
    symbol = str(order.instrument_id.symbol)
    ts_order_type = convert_order_type(order)
    ts_trade_action = "Buy" if order.side == OrderSide.BUY else "Sell"
    ts_tif = convert_time_in_force(order.time_in_force)

    params: dict[str, Any] = {
        "account_id": account_id,
        "symbol": symbol,
        "quantity": str(order.quantity),
        "order_type": ts_order_type,
        "trade_action": ts_trade_action,
        "time_in_force": ts_tif,
    }

    if isinstance(order, LimitOrder):
        params["limit_price"] = str(order.price)
    elif isinstance(order, StopMarketOrder):
        params["stop_price"] = str(order.trigger_price)
    elif isinstance(order, StopLimitOrder):
        params["limit_price"] = str(order.price)
        params["stop_price"] = str(order.trigger_price)

    return params


def parse_order_status(ts_status: str) -> OrderStatus:
    """Parse a TradeStation order status string to NautilusTrader OrderStatus."""
    _MAP = {
        "ACK": OrderStatus.ACCEPTED,
        "OPN": OrderStatus.SUBMITTED,
        "FLL": OrderStatus.FILLED,
        "FLP": OrderStatus.PARTIALLY_FILLED,
        "OUT": OrderStatus.CANCELED,
        "REJ": OrderStatus.REJECTED,
        "CAN": OrderStatus.CANCELED,
        "EXP": OrderStatus.EXPIRED,
    }
    return _MAP.get(ts_status, OrderStatus.PENDING_UPDATE)


def parse_ts_order_type(ts_order_type: str) -> OrderType:
    """Parse a TradeStation order type string to NautilusTrader OrderType."""
    _MAP = {
        "Market": OrderType.MARKET,
        "Limit": OrderType.LIMIT,
        "StopMarket": OrderType.STOP_MARKET,
        "StopLimit": OrderType.STOP_LIMIT,
    }
    return _MAP.get(ts_order_type, OrderType.MARKET)


def parse_order_status_report(
    ts_order: dict,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    account_id: AccountId,
    ts_now: int,
) -> OrderStatusReport | None:
    """Parse a raw TradeStation order dict into an OrderStatusReport.

    Parameters
    ----------
    ts_order : dict
        Raw order dict from the TradeStation API.
    instrument_id : InstrumentId
        The instrument this order belongs to.
    client_order_id : ClientOrderId
        The NautilusTrader client order ID to assign.
    account_id : AccountId
        The NautilusTrader account ID.
    ts_now : int
        Current timestamp in nanoseconds (from clock).

    Returns
    -------
    OrderStatusReport or None
        Parsed report, or None if parsing fails.
    """
    try:
        ts_order_id = ts_order.get("OrderID")
        status = parse_order_status(ts_order.get("Status", ""))

        qty_ordered = Decimal(ts_order.get("Quantity") or "0")
        qty_filled = Decimal(ts_order.get("FilledQuantity") or "0")

        price_str = ts_order.get("LimitPrice") or ts_order.get("Price") or "0"
        price = Price.from_str(price_str) if price_str != "0" else None

        trade_action = ts_order.get("TradeAction", "Buy")
        side = OrderSide.BUY if trade_action in ("Buy", "BuyToCover") else OrderSide.SELL

        avg_px_str = ts_order.get("AveragePrice") or "0"
        avg_px = Price.from_str(avg_px_str) if avg_px_str != "0" else None

        return OrderStatusReport(
            account_id=account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=VenueOrderId(ts_order_id) if ts_order_id else None,
            order_side=side,
            order_type=parse_ts_order_type(ts_order.get("OrderType", "Market")),
            time_in_force=TimeInForce.DAY,
            order_status=status,
            price=price,
            quantity=Quantity.from_str(str(qty_ordered)),
            filled_qty=Quantity.from_str(str(qty_filled)),
            avg_px=avg_px,
            report_id=UUID4(),
            ts_accepted=ts_now,
            ts_last=ts_now,
            ts_init=ts_now,
        )

    except Exception as e:
        _log.error(f"Failed to parse order status report: {e}")
        return None


def parse_fill_report(
    ts_order: dict,
    instrument_id: InstrumentId,
    account_id: AccountId,
    ts_now: int,
    client_order_id: ClientOrderId | None = None,
) -> FillReport | None:
    """Parse a filled TradeStation order dict into a NautilusTrader FillReport.

    Only orders with status ``FLL`` (fully filled) should be passed here.

    Parameters
    ----------
    ts_order : dict
        Raw order dict from the TradeStation API (Status == 'FLL').
    instrument_id : InstrumentId
        The instrument this fill belongs to.
    account_id : AccountId
        The NautilusTrader account ID.
    ts_now : int
        Current timestamp in nanoseconds (used as ts_event/ts_init fallback).
    client_order_id : ClientOrderId, optional
        The NautilusTrader client order ID if known; otherwise derived from the
        TradeStation order ID.

    Returns
    -------
    FillReport or None
        Parsed report, or None if the fill price or quantity cannot be read.
    """
    try:
        ts_order_id = ts_order.get("OrderID", "")

        # Fill price: prefer AveragePrice, fall back to ExecutionPrice in Legs
        avg_px_str = ts_order.get("AveragePrice") or "0"
        if avg_px_str == "0":
            legs = ts_order.get("Legs", [])
            avg_px_str = legs[0].get("ExecutionPrice", "0") if legs else "0"
        if avg_px_str == "0":
            return None

        # Fill quantity
        qty_str = ts_order.get("FilledQuantity") or ts_order.get("Quantity") or "0"
        qty = Decimal(qty_str)
        if qty == 0:
            return None

        # Order side
        trade_action = ts_order.get("TradeAction", "Buy")
        order_side = OrderSide.BUY if trade_action in ("Buy", "BuyToCover") else OrderSide.SELL

        # Timestamp
        closed_str = ts_order.get("ClosedDateTime", "") or ts_order.get("OpenedDateTime", "")
        if closed_str:
            ts_event = dt_to_unix_nanos(pd.Timestamp(closed_str, tz="UTC"))
        else:
            ts_event = ts_now

        coid = client_order_id or ClientOrderId(f"TS-{ts_order_id}")

        return FillReport(
            account_id=account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(ts_order_id) if ts_order_id else None,
            trade_id=TradeId(f"FILL-{ts_order_id}"),
            order_side=order_side,
            last_qty=Quantity.from_str(str(qty)),
            last_px=Price.from_str(avg_px_str),
            commission=Money(0.0, USD),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            report_id=UUID4(),
            ts_event=ts_event,
            ts_init=ts_now,
            client_order_id=coid,
        )

    except Exception as e:
        _log.error(f"Failed to parse fill report: {e}")
        return None



def _group_type_for_order_list(orders: list[Order]) -> str | None:
    """Determine the TradeStation group type for an OrderList.

    Returns
    -------
    str | None
        ``"OCO"`` when all orders have OCO contingency,
        ``"BRK"`` when the list follows an OTO/bracket pattern (one OTO entry
        + two or more OCO exit legs), or ``None`` when the list doesn't match
        a recognised group pattern and should be submitted individually.
    """
    if not orders:
        return None

    contingencies = {o.contingency_type for o in orders}

    # Pure OCO: all orders cancel each other (e.g. two exit orders)
    if contingencies == {ContingencyType.OCO}:
        return "OCO"

    # Bracket: one OTO entry + OCO exits
    oto_orders = [o for o in orders if o.contingency_type == ContingencyType.OTO]
    oco_orders = [o for o in orders if o.contingency_type == ContingencyType.OCO]
    if len(oto_orders) == 1 and len(oco_orders) >= 2:
        return "BRK"

    return None


def convert_order_list_to_ts_group(
    orders: list[Order],
    account_id: str,
    is_equity: Callable[[Order], bool] | None = None,
) -> tuple[str, list[dict]] | None:
    """Convert an NT OrderList into a TradeStation group order payload.

    Parameters
    ----------
    orders : list[Order]
        The orders from the OrderList (must be 2+ orders with contingencies).
    account_id : str
        The TradeStation account ID.
    is_equity : Callable[[Order], bool], optional
        Predicate returning ``True`` when an order's instrument is an equity.
        When provided, equity legs honor ``TS_INTENT`` tags so short-cover
        legs go out as ``BuyToCover`` instead of plain ``Buy`` (the SPY EQUITY-GROUP-ORDER
        rejection class — design §13-1). When ``None`` (default) payloads are
        byte-identical to the previous behavior.

    Returns
    -------
    tuple[str, list[dict]] | None
        ``(group_type, orders_payload)`` if the list is a supported group
        pattern, or ``None`` if it should be submitted individually.
    """
    group_type = _group_type_for_order_list(orders)
    if group_type is None:
        return None

    order_payloads = []
    for order in orders:
        params = convert_order_to_ts_format(order, account_id)
        # convert_order_to_ts_format returns kwargs for place_order;
        # the group API uses the same fields but in a dict with TS-style keys.
        payload: dict = {
            "AccountID": params["account_id"],
            "Symbol": params["symbol"],
            "Quantity": params["quantity"],
            "OrderType": params["order_type"],
            "TradeAction": params["trade_action"],
            "TimeInForce": {"Duration": params["time_in_force"]},
        }
        # Equity legs honor TS_INTENT tags (EQUITY-GROUP-ORDER class, design §13-1):
        # without this an equity short-cover leg goes out plain 'Buy' and
        # is rejected by TradeStation. Futures keep plain Buy/Sell.
        if is_equity is not None and is_equity(order):
            intent_action = equity_trade_action_from_intent(order)
            if intent_action is not None:
                payload["TradeAction"] = intent_action
        if "limit_price" in params:
            payload["LimitPrice"] = params["limit_price"]
        if "stop_price" in params:
            payload["StopPrice"] = params["stop_price"]
        order_payloads.append(payload)

    return group_type, order_payloads
