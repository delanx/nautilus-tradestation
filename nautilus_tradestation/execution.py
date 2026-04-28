"""
TradeStation execution client implementation.
"""

import asyncio
import os
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_tradestation.http.client import TradeStationHttpClient
from nautilus_tradestation.parsing.execution import convert_order_to_ts_format
from nautilus_tradestation.parsing.execution import convert_order_type
from nautilus_tradestation.parsing.execution import convert_time_in_force
from nautilus_tradestation.parsing.execution import convert_order_list_to_ts_group
from nautilus_tradestation.parsing.execution import equity_trade_action_from_intent
from nautilus_tradestation.parsing.execution import parse_fill_report
from nautilus_tradestation.parsing.execution import parse_order_status
from nautilus_tradestation.parsing.execution import parse_order_status_report
from nautilus_tradestation.parsing.execution import parse_ts_order_type
from nautilus_tradestation.providers import TradeStationInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import StopLimitOrder
from nautilus_trader.model.orders import StopMarketOrder


def _venue_confirmed_cancels_enabled() -> bool:
    """TS_VENUE_CONFIRMED_CANCELS=1: a REST DELETE 200 is treated as a
    cancel REQUEST acknowledgment, not venue-terminal state.

    TradeStation's DELETE acknowledges the request; the order can still FILL
    inside the race window (exactly the fast tape a resting stop leg gets
    canceled in).  Synthesizing ``OrderCanceled`` on the 200 makes the order
    terminal in Nautilus, so the real SSE ``FLL`` that follows is dropped by
    the ``is_closed`` gate -- leg fill + a released market exit double-fill the
    position (DESIGN.md §7 / checklist V-3).  With the flag set
    the cancel only becomes terminal on the venue's own CAN event (SSE stream,
    status poll, or a one-shot order-status query).  Unset (the default) keeps
    today's synthetic-cancel behavior byte-identical.
    """
    return os.environ.get("TS_VENUE_CONFIRMED_CANCELS", "") in ("1", "true", "True")


def _stream_fill_hardening_enabled() -> bool:
    """TS_STREAM_FILL_HARDENING=1: never permanently lose a fill event.

    Two defects in the streaming/poll event processing (unset keeps both,
    byte-identical):

    1. ``_order_last_status`` was recorded BEFORE the zero-price ``FLL`` skip,
       so the dedup gate dropped any repeat of the same fill -- and the log
       line's promised "order status report will reconcile" does not exist in
       streaming mode.  Hardened: the status is recorded only AFTER the event
       was fully processed, so a skipped fill stays retryable.
    2. The SSE fill path lacked the poll path's 4th price fallback (the
       order's own trigger/limit price).  Hardened: mirrored.
    """
    return os.environ.get("TS_STREAM_FILL_HARDENING", "") in ("1", "true", "True")


def _stream_reconcile_poll_secs() -> float:
    """TS_STREAM_RECONCILE_POLL_SECS=<seconds>: slow REST status-poll
    reconciliation while SSE streaming is active.

    ``use_streaming=True`` historically ran NO status polling at all, so a
    fill the SSE path lost (zero-price skip, or the mapping race where the SSE
    ``FLL`` arrives before the REST submit response registers
    ``ts_order_id -> client_order_id``) had no backstop.  A value > 0 runs
    ``_check_order_statuses`` every that-many seconds; the shared
    ``_order_last_status`` dedup keeps SSE + poll from double-emitting.
    Unset/0 (the default) keeps today's streaming-only behavior byte-identical.
    """
    raw = os.environ.get("TS_STREAM_RECONCILE_POLL_SECS", "")
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


class TradeStationExecutionClient(LiveExecutionClient):
    """
    Provide an execution client for TradeStation.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : TradeStationHttpClient
        The TradeStation HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : TradeStationInstrumentProvider
        The instrument provider for the client.
    account_id : str
        The TradeStation account ID to use for trading.
    base_url_ws : str | None, optional
        The WebSocket base URL (for future streaming implementation).

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: TradeStationHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: TradeStationInstrumentProvider,
        account_id: str,
        base_url_ws: str | None = None,
        use_streaming: bool = False,
        streaming_reconnect_delay_secs: float = 5.0,
        extended_hours: bool = False,
        order_map_path: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId("TRADESTATION"),
            venue=None,  # Multi-venue support
            oms_type=OmsType.NETTING,  # Futures: one net position per instrument
            instrument_provider=instrument_provider,
            account_type=AccountType.MARGIN,
            base_currency=Currency.from_str("USD"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        self._client = client
        self._account_id = account_id
        self._base_url_ws = base_url_ws

        # Order tracking
        self._ts_order_id_to_client_order_id: dict[str, ClientOrderId] = {}
        self._client_order_id_to_ts_order_id: dict[ClientOrderId, str] = {}

        # Fill detection — polling or streaming
        self._order_last_status: dict[str, str] = {}  # ts_order_id → last seen status
        self._fill_poll_task: asyncio.Task | None = None
        self._fill_poll_interval: float = 5.0  # seconds between order status polls
        # Slow REST reconcile poll behind SSE streaming (gated by
        # TS_STREAM_RECONCILE_POLL_SECS; None when streaming is off or unset)
        self._status_reconcile_task: asyncio.Task | None = None

        # Streaming configuration
        self._use_streaming = use_streaming
        self._extended_hours = extended_hours
        self._stream_client: "TradeStationStreamClient | None" = None
        if use_streaming:
            from nautilus_tradestation.streaming.client import (
                TradeStationStreamClient,
            )

            self._stream_client = TradeStationStreamClient(
                access_token_provider=lambda: self._client.access_token,
                access_token_refresher=lambda force_refresh=False: self._client.get_access_token(
                    force_refresh=force_refresh
                ),
                base_url=self._client.base_url,
                reconnect_delay_secs=streaming_reconnect_delay_secs,
            )

        # Order-map persistence (T-2)
        self._order_map_path: str | None = order_map_path

        # Account setup
        self._account_id_nautilus = AccountId(f"{self.id}-{account_id}")


    def _persist_order_map(self) -> None:
        """Persist _ts_order_id_to_client_order_id to disk for cross-restart recovery."""
        if not self._order_map_path:
            return
        import json as _json, time as _time, os as _os
        try:
            now_s = _time.time()
            # Merge with existing file to preserve entries from both directions
            try:
                with open(self._order_map_path, "r") as _f:
                    existing = _json.load(_f)
            except Exception:
                existing = {}
            # Update with current in-memory entries (add submitted_ts if missing)
            for ts_id, coid in self._ts_order_id_to_client_order_id.items():
                existing[ts_id] = {
                    "client_order_id": str(coid),
                    "submitted_ts": existing.get(ts_id, {}).get("submitted_ts", now_s),
                }
            tmp = self._order_map_path + f".{_os.getpid()}.tmp"
            with open(tmp, "w") as _f:
                _json.dump(existing, _f)
            _os.replace(tmp, self._order_map_path)
        except Exception as exc:
            self._log.warning(f"[ORDER-MAP] Failed to persist order map: {exc}")

    def _load_order_map(self) -> None:
        """Load persisted order map at startup; prune entries older than 7 days."""
        if not self._order_map_path:
            return
        import json as _json, time as _time
        try:
            with open(self._order_map_path, "r") as _f:
                raw = _json.load(_f)
        except FileNotFoundError:
            return
        except Exception as exc:
            self._log.warning(f"[ORDER-MAP] Failed to load order map: {exc}")
            return
        cutoff = _time.time() - 7 * 86400
        loaded = pruned = 0
        for ts_id, entry in raw.items():
            if entry.get("submitted_ts", 0) < cutoff:
                pruned += 1
                continue
            try:
                coid = ClientOrderId(entry["client_order_id"])
                self._ts_order_id_to_client_order_id[ts_id] = coid
                self._client_order_id_to_ts_order_id[coid] = ts_id
                loaded += 1
            except Exception:
                pass
        self._log.info(f"[ORDER-MAP] Loaded {loaded} order ID mappings ({pruned} pruned >7d)")

    async def _connect(self) -> None:
        """Connect to TradeStation."""
        self._log.info("Connecting to TradeStation...")

        # Test authentication by fetching account info
        try:
            accounts = await self._client.get_accounts()
            self._log.info(
                f"Successfully authenticated. Found {len(accounts)} account(s)"
            )

            # Fetch initial balances and positions
            await self._update_account_state()

        except Exception as e:
            self._log.error(f"Failed to connect to TradeStation: {e}")
            raise

        # Start background fill detection (streaming or polling)
        if self._use_streaming and self._stream_client:
            self._fill_poll_task = self._loop.create_task(self._stream_order_fills())
            # BACKSTOP: retrieve any escaped exception (prevent the asyncio "Task
            # exception was never retrieved" escalation that can crash node.run())
            # and resubscribe the fill stream if it ever ends non-cancelled.
            self._fill_poll_task.add_done_callback(self._on_fill_task_done)
            self._log.info(
                "Started order fill detection via SSE streaming", LogColor.GREEN
            )
            reconcile_secs = _stream_reconcile_poll_secs()
            if reconcile_secs > 0:
                self._status_reconcile_task = self._loop.create_task(
                    self._reconcile_order_statuses_loop(reconcile_secs)
                )
                self._log.info(
                    "Started slow REST order-status reconcile poll "
                    f"(every {reconcile_secs:.0f}s) behind the SSE stream",
                    LogColor.GREEN,
                )
        else:
            self._fill_poll_task = self._loop.create_task(self._poll_order_fills())
            self._log.info(
                f"Started order fill polling (every {self._fill_poll_interval:.0f}s)",
                LogColor.GREEN,
            )

        self._load_order_map()
        self._log.info("Connected to TradeStation", LogColor.GREEN)

    async def _disconnect(self) -> None:
        """Disconnect from TradeStation."""
        self._log.info("Disconnecting from TradeStation...")

        # Stop fill polling
        if self._fill_poll_task:
            self._fill_poll_task.cancel()
            try:
                await self._fill_poll_task
            except asyncio.CancelledError:
                pass
            self._fill_poll_task = None

        # Stop the slow status reconcile poll (streaming backstop)
        if self._status_reconcile_task:
            self._status_reconcile_task.cancel()
            try:
                await self._status_reconcile_task
            except asyncio.CancelledError:
                pass
            self._status_reconcile_task = None

        await self._client.close()
        self._log.info("Disconnected from TradeStation", LogColor.GREEN)

    # -- EXECUTION REPORTS ----------------------------------------------------------------

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        """Generate an order status report for the given order."""
        instrument_id = command.instrument_id
        client_order_id = command.client_order_id
        venue_order_id = command.venue_order_id
        self._log.debug(
            f"Generating order status report for {client_order_id}"
            + (f" (venue_order_id={venue_order_id})" if venue_order_id else ""),
        )

        # Try to find TradeStation order ID
        ts_order_id = self._client_order_id_to_ts_order_id.get(client_order_id)

        if not ts_order_id and venue_order_id:
            ts_order_id = str(venue_order_id)

        if not ts_order_id:
            self._log.warning(
                f"Cannot find TradeStation order ID for {client_order_id}"
            )
            return None

        # Fetch order from TradeStation
        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )

            # Find matching order
            ts_order = None
            for order in orders:
                if order.get("OrderID") == ts_order_id:
                    ts_order = order
                    break

            if not ts_order:
                self._log.warning(f"Order {ts_order_id} not found at TradeStation")
                return None

            return self._parse_order_status_report(
                ts_order, instrument_id, client_order_id
            )

        except Exception as e:
            self._log.error(f"Failed to generate order status report: {e}")
            return None

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """Generate order status reports for all orders."""
        instrument_id = command.instrument_id
        open_only = command.open_only
        self._log.debug("Generating order status reports")

        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )

            reports = []
            for ts_order in orders:
                # Map to client order ID if we have it
                ts_order_id = ts_order.get("OrderID")
                client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)

                if not client_order_id:
                    # Create a client order ID for unknown orders
                    client_order_id = ClientOrderId(f"TS-{ts_order_id}")

                # Get instrument ID — TS futures orders don't have a top-level Symbol;
                # the symbol is inside Legs[0]['Symbol']. Fall back to Legs if needed.
                # Skip only if genuinely no symbol can be found anywhere.
                symbol = ts_order.get("Symbol", "")
                if not symbol:
                    legs = ts_order.get("Legs", [])
                    if legs:
                        symbol = legs[0].get("Symbol", "")
                if not symbol:
                    self._log.warning(
                        f"Skipping order with empty Symbol during reconciliation: {ts_order}"
                    )
                    continue
                order_instrument_id = instrument_id or InstrumentId.from_str(
                    f"{symbol}.TRADESTATION",
                )

                # Filter by instrument if specified
                if instrument_id and order_instrument_id != instrument_id:
                    continue

                # Filter by open only
                if open_only:
                    status = self._parse_order_status(ts_order.get("Status", ""))
                    if status not in (
                        OrderStatus.ACCEPTED,
                        OrderStatus.SUBMITTED,
                        OrderStatus.PARTIALLY_FILLED,
                    ):
                        continue

                report = self._parse_order_status_report(
                    ts_order,
                    order_instrument_id,
                    client_order_id,
                )
                if report:
                    reports.append(report)

            self._log.debug(f"Generated {len(reports)} order status reports")
            return reports

        except Exception as e:
            self._log.error(f"Failed to generate order status reports: {e}")
            return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        """Generate fill reports from filled orders.

        TradeStation has no dedicated fills endpoint — fills are embedded in
        order status. This method fetches the order list and builds a FillReport
        for every order with status FLL (fully filled).
        """
        instrument_id = command.instrument_id
        venue_order_id = command.venue_order_id
        start = command.start
        end = command.end

        self._log.debug("Generating fill reports")

        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )

            ts_now = self._clock.timestamp_ns()
            reports: list[FillReport] = []

            for ts_order in orders:
                # Only process fully-filled orders
                if ts_order.get("Status") != "FLL":
                    continue

                # Resolve instrument ID from symbol / Legs fallback
                symbol = ts_order.get("Symbol", "")
                if not symbol:
                    legs = ts_order.get("Legs", [])
                    symbol = legs[0].get("Symbol", "") if legs else ""
                if not symbol:
                    continue

                order_instrument_id = InstrumentId.from_str(f"{symbol}.TRADESTATION")

                # Filter by instrument if requested
                if instrument_id and order_instrument_id != instrument_id:
                    continue

                # Filter by venue_order_id if requested
                if venue_order_id and ts_order.get("OrderID") != str(venue_order_id):
                    continue

                # Filter by time range if requested
                if start or end:
                    closed_str = ts_order.get("ClosedDateTime", "")
                    if closed_str:
                        order_ts = dt_to_unix_nanos(pd.Timestamp(closed_str, tz="UTC"))
                        if start and order_ts < dt_to_unix_nanos(pd.Timestamp(start)):
                            continue
                        if end and order_ts > dt_to_unix_nanos(pd.Timestamp(end)):
                            continue

                # Look up client order ID from our tracking dict
                ts_order_id = ts_order.get("OrderID")
                coid = self._ts_order_id_to_client_order_id.get(ts_order_id)

                report = parse_fill_report(
                    ts_order,
                    instrument_id=order_instrument_id,
                    account_id=self._account_id_nautilus,
                    ts_now=ts_now,
                    client_order_id=coid,
                )
                if report:
                    reports.append(report)

            self._log.debug(f"Generated {len(reports)} fill report(s)")
            return reports

        except Exception as e:
            self._log.error(f"Failed to generate fill reports: {e}")
            return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        """Generate position status reports."""
        instrument_id = command.instrument_id

        self._log.debug("Generating position status reports")

        try:
            positions = await self._client.get_positions(
                account_keys=self._account_id,
            )

            reports = []
            for ts_position in positions:
                symbol = ts_position.get("Symbol", "")
                position_instrument_id = InstrumentId.from_str(f"{symbol}.TRADESTATION")

                # Filter by instrument if specified
                if instrument_id and position_instrument_id != instrument_id:
                    continue

                # Parse position
                quantity_str = ts_position.get("Quantity", "0")
                quantity = Decimal(quantity_str)

                if quantity == 0:
                    continue  # Skip zero positions

                # Determine side — PositionSide not OrderSide
                position_side = (
                    PositionSide.LONG if quantity > 0 else PositionSide.SHORT
                )

                avg_px_str = ts_position.get("AveragePrice") or "0"
                avg_px_open = Decimal(avg_px_str) if avg_px_str != "0" else None

                report = PositionStatusReport(
                    account_id=self._account_id_nautilus,
                    instrument_id=position_instrument_id,
                    position_side=position_side,
                    quantity=Quantity.from_str(str(abs(quantity))),
                    avg_px_open=avg_px_open,
                    report_id=UUID4(),
                    ts_last=self._clock.timestamp_ns(),
                    ts_init=self._clock.timestamp_ns(),
                )
                reports.append(report)

            self._log.debug(f"Generated {len(reports)} position status reports")
            return reports

        except Exception as e:
            self._log.error(f"Failed to generate position status reports: {e}")
            return []

    # -- COMMAND HANDLERS -----------------------------------------------------------------

    async def _submit_order(self, command: SubmitOrder) -> None:
        """Submit an order to TradeStation."""
        order = command.order

        self._log.info(f"Submitting order {order.client_order_id}")

        try:
            # Convert Nautilus order to TradeStation format
            ts_order_params = self._convert_order_to_ts_format(order)

            # Submit order
            response = await self._client.place_order(
                **ts_order_params,
            )

            # Extract order ID from response
            ts_order_id = response.get("OrderID") or response.get("Orders", [{}])[
                0
            ].get("OrderID")

            if not ts_order_id:
                raise ValueError(f"No OrderID in response: {response}")

            # Track order IDs
            self._ts_order_id_to_client_order_id[ts_order_id] = order.client_order_id
            self._client_order_id_to_ts_order_id[order.client_order_id] = ts_order_id
            self._persist_order_map()

            # Generate order accepted event
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=VenueOrderId(ts_order_id),
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order {order.client_order_id} accepted (TradeStation ID: {ts_order_id})",
                LogColor.GREEN,
            )

        except Exception as e:
            self._log.error(f"Failed to submit order {order.client_order_id}: {e}")
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        """Submit a list of orders as a TradeStation group order when possible.

        Detects OCO and bracket (OTO + OCO) patterns and routes them to
        ``POST /v3/orderexecution/ordergroups`` for atomic submission.
        Falls back to submitting orders individually when no group pattern
        is recognised (e.g. plain unlinked order lists).
        """
        orders = command.order_list.orders

        # Try group submission first. The equity predicate lets group legs
        # honor TS_INTENT tags (EQUITY-GROUP-ORDER class — design §13-1): an equity
        # short-cover leg must go out as BuyToCover, never plain Buy.
        group_result = convert_order_list_to_ts_group(
            orders,
            self._account_id,
            is_equity=self._is_equity_order,
        )
        if group_result is not None:
            group_type, order_payloads = group_result
            await self._submit_order_group(
                command=command,
                group_type=group_type,
                order_payloads=order_payloads,
            )
            return

        # Fallback: submit each order individually
        self._log.info(
            f"OrderList {command.order_list.id} has no recognised group pattern "
            "— submitting orders individually"
        )
        for order in orders:
            submit_order = SubmitOrder(
                trader_id=command.trader_id,
                strategy_id=command.strategy_id,
                order=order,
                command_id=command.id,
                ts_init=command.ts_init,
                position_id=command.position_id,
            )
            await self._submit_order(submit_order)

    async def _submit_order_group(
        self,
        command: SubmitOrderList,
        group_type: str,
        order_payloads: list[dict],
    ) -> None:
        """Submit an OCO or bracket group to TradeStation and register all order IDs."""
        orders = command.order_list.orders
        self._log.info(
            f"Submitting {group_type} group for OrderList {command.order_list.id} "
            f"({len(orders)} legs)"
        )
        try:
            response = await self._client.place_order_group(
                group_type=group_type,
                orders=order_payloads,
            )

            # Extract individual order IDs from response
            # TS returns: {"OrderGroupId": "...", "Orders": [{"OrderID": "...", ...}, ...]}
            ts_orders = response.get("Orders", [])
            if len(ts_orders) != len(orders):
                self._log.error(
                    f"Group response has {len(ts_orders)} orders but submitted {len(orders)} — "
                    "unmapped legs will be rejected"
                )

            for i, order in enumerate(orders):
                ts_order_resp = ts_orders[i] if i < len(ts_orders) else {}
                ts_order_id = ts_order_resp.get("OrderID")
                if not ts_order_id:
                    # REJECTION-PATH ALERT (design §13-4): an unmapped leg is
                    # an INVISIBLE resting order — its fill/cancel events would
                    # be silently dropped by the session ID-map gate. Fail loud
                    # so the strategy's rejection handling (retry-once →
                    # degraded engine fallback) takes over instead of trusting
                    # a protective order we cannot see.
                    reason = (
                        f"Group leg {i} has no OrderID in response "
                        f"(leg response: {ts_order_resp}) — "
                        "leg unmapped, treating as rejected"
                    )
                    self._log.error(
                        f"Order group leg {i} UNMAPPED for "
                        f"{order.client_order_id}: {reason}"
                    )
                    self.generate_order_rejected(
                        strategy_id=command.strategy_id,
                        instrument_id=order.instrument_id,
                        client_order_id=order.client_order_id,
                        reason=reason,
                        ts_event=self._clock.timestamp_ns(),
                    )
                    continue

                # Register in tracking dicts
                self._ts_order_id_to_client_order_id[ts_order_id] = (
                    order.client_order_id
                )
                self._client_order_id_to_ts_order_id[order.client_order_id] = (
                    ts_order_id
                )
                self._persist_order_map()

                # Generate accepted event for each leg
                self.generate_order_accepted(
                    strategy_id=command.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    ts_event=self._clock.timestamp_ns(),
                )
                self._log.info(
                    f"Order group leg {i} accepted: {order.client_order_id} "
                    f"(TS: {ts_order_id})"
                )

        except Exception as e:
            self._log.error(
                f"Failed to submit {group_type} order group for "
                f"OrderList {command.order_list.id}: {e}"
            )
            # Reject all legs
            for order in orders:
                self.generate_order_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=str(e),
                    ts_event=self._clock.timestamp_ns(),
                )

    async def _modify_order(self, command: ModifyOrder) -> None:
        """Modify (replace) an existing order price via TradeStation PUT endpoint."""
        client_order_id = command.client_order_id
        ts_order_id = self._client_order_id_to_ts_order_id.get(client_order_id)

        if not ts_order_id:
            self._log.error(
                f"Cannot modify {client_order_id}: no TradeStation order ID found"
            )
            return

        # NOTE: was `self.cache` — an AttributeError on every call (this client
        # only has `_cache`). Caught by the brackets part-1 test suite;
        # _modify_order had never been exercised before. (Upstream fixed the
        # same bug independently in 83dca1b.)
        order = self._cache.order(client_order_id)
        if order is None:
            self._log.error(f"Cannot modify {client_order_id}: not found in cache")
            return

        try:
            symbol = str(order.instrument_id.symbol)
            ts_order_type = self._convert_order_type(order)
            ts_tif = self._convert_time_in_force(order.time_in_force)
            ts_side = "Buy" if order.side == OrderSide.BUY else "Sell"
            # Equity covers must keep their TradeAction on replace (EQUITY-GROUP-ORDER
            # class): replacing a BuyToCover leg with plain 'Buy' would be
            # rejected by TradeStation. Futures keep plain Buy/Sell.
            if self._is_equity_order(order):
                intent_action = equity_trade_action_from_intent(order)
                if intent_action is not None:
                    ts_side = intent_action

            # Resolve new quantity from the command (None = unchanged)
            new_qty = (
                command.quantity if command.quantity is not None else order.quantity
            )

            # Resolve new prices from the command (None = unchanged)
            if command.trigger_price is not None:
                stop_price = str(command.trigger_price)
            elif hasattr(order, "trigger_price"):
                stop_price = str(order.trigger_price)
            else:
                stop_price = None

            if command.price is not None:
                limit_price = str(command.price)
            elif hasattr(order, "price"):
                limit_price = str(order.price)
            else:
                limit_price = None

            response = await self._client.replace_order(
                order_id=ts_order_id,
                account_id=self._account_id,
                symbol=symbol,
                quantity=str(new_qty),
                order_type=ts_order_type,
                trade_action=ts_side,
                time_in_force=ts_tif,
                limit_price=limit_price,
                stop_price=stop_price,
            )

            # TS returns a new OrderID when the order is replaced
            new_ts_order_id = response.get("OrderID") or response.get("Orders", [{}])[
                0
            ].get("OrderID")
            if new_ts_order_id and new_ts_order_id != ts_order_id:
                # Update our ID maps to point to the new venue order ID
                self._ts_order_id_to_client_order_id.pop(ts_order_id, None)
                self._ts_order_id_to_client_order_id[new_ts_order_id] = client_order_id
                self._client_order_id_to_ts_order_id[client_order_id] = new_ts_order_id
                self._persist_order_map()
                venue_order_id = VenueOrderId(new_ts_order_id)
            else:
                venue_order_id = VenueOrderId(ts_order_id)

            self.generate_order_updated(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                quantity=new_qty,
                price=command.price,
                trigger_price=command.trigger_price,
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order {client_order_id} modified → venue {venue_order_id}",
                LogColor.GREEN,
            )

        except Exception as e:
            err = str(e)
            if "HTTP 4" in err:
                # 4xx: broker definitively rejected the modify — safe to surface
                # as a rejection event so NT reverts PENDING_UPDATE and the
                # strategy can react (retry, cancel, hedge).
                self._log.error(
                    f"Modify rejected by broker for {client_order_id}: {err}"
                )
                self.generate_order_modify_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    reason=err[:150],
                    ts_event=self._clock.timestamp_ns(),
                )
            else:
                # 5xx / network error: modify may have already succeeded at the
                # broker before the error occurred. Emitting a rejection would
                # create false state drift. Log and leave PENDING_UPDATE in place;
                # the next reconciliation cycle will correct state.
                self._log.error(
                    f"Failed to modify order {client_order_id} "
                    f"(ambiguous — broker state unknown): {err}"
                )

    async def _cancel_order(self, command: CancelOrder) -> None:
        """Cancel an order."""
        self._log.info(f"Cancelling order {command.client_order_id}")

        ts_order_id = self._client_order_id_to_ts_order_id.get(command.client_order_id)

        if not ts_order_id:
            self._log.error(
                f"Cannot find TradeStation order ID for {command.client_order_id}"
            )
            return

        try:
            await self._client.cancel_order(
                order_id=ts_order_id,
            )

            if _venue_confirmed_cancels_enabled():
                # The DELETE 200 is a cancel REQUEST acknowledgment, not venue-
                # terminal state: the order can still FILL inside the race
                # window.  Synthesizing OrderCanceled here would close the
                # order in Nautilus and the racing real fill would be dropped
                # by the is_closed gate (double-fill class, design §7/V-3).
                # The venue's own CAN (SSE / status poll / one-shot query)
                # makes it terminal.
                self._log.info(
                    f"Cancel request for {command.client_order_id} "
                    f"({ts_order_id}) acknowledged — awaiting venue cancel event"
                )
            else:
                self.generate_order_canceled(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=command.client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    ts_event=self._clock.timestamp_ns(),
                )

                self._log.info(
                    f"Order {command.client_order_id} cancelled", LogColor.GREEN
                )

        except Exception as e:
            if "Not an open order" in str(e):
                # Order already gone at broker — two possible causes:
                # 1. DAY order expired at session close (no event sent by TS) — common
                # 2. Order filled exactly as we tried to cancel it — rare race condition
                #
                # We do NOT generate OrderCanceled here: if it was a fill, the SSE
                # stream will deliver the real OrderFilled event shortly. Generating
                # a synthetic cancel would put the order in a terminal state and cause
                # NT to silently drop the fill → orphan position at broker.
                #
                # The downside: NT keeps the order as "open" until restart. Strategies
                # will attempt cancel again next bar and get this warning again. This
                # is acceptable noise compared to the risk of masking a real fill.
                self._log.warning(
                    f"Order {command.client_order_id} ({ts_order_id}) not found at broker "
                    f"(expired DAY order or concurrent fill) — no cancel event generated"
                )
            else:
                self._log.error(
                    f"Failed to cancel order {command.client_order_id}: {e}"
                )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        """Cancel all orders for a specific instrument and strategy.

        Uses the NT cache to identify which orders belong to the requesting
        strategy, then cancels only those.  Orders from other strategies or
        processes sharing the same TS account are never touched.

        This follows the same pattern as the official Binance adapter.
        """
        self._log.info(
            f"Cancelling orders for {command.instrument_id} "
            f"(strategy={command.strategy_id})"
        )

        open_orders = self._cache.orders_open(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
            side=command.order_side,
        )
        inflight_orders = [
            o for o in self._cache.orders_inflight(
                instrument_id=command.instrument_id,
                strategy_id=command.strategy_id,
            )
            if o.status == OrderStatus.SUBMITTED
        ]

        all_orders = list(open_orders) + inflight_orders

        if command.order_side != OrderSide.NO_ORDER_SIDE:
            all_orders = [o for o in all_orders if o.side == command.order_side]

        if not all_orders:
            self._log.info("No matching orders to cancel")
            return

        self._log.info(f"Cancelling {len(all_orders)} order(s)")

        for order in all_orders:
            venue_order_id = order.venue_order_id
            if venue_order_id is None:
                self._log.warning(
                    f"Order {order.client_order_id} has no venue_order_id — skipping"
                )
                continue

            ts_order_id = venue_order_id.value
            try:
                await self._client.cancel_order(order_id=ts_order_id)
                # Same venue-confirmed-cancel semantics as _cancel_order: with
                # the flag set, the DELETE 200 is only a request ack — the
                # terminal OrderCanceled must come from the venue's own CAN
                # event (SSE/poll), never synthesized here (V-3 race class).
                if not _venue_confirmed_cancels_enabled():
                    self.generate_order_canceled(
                        strategy_id=command.strategy_id,
                        instrument_id=command.instrument_id,
                        client_order_id=order.client_order_id,
                        venue_order_id=venue_order_id,
                        ts_event=self._clock.timestamp_ns(),
                    )
                self._log.info(
                    f"Cancelled order {order.client_order_id} ({ts_order_id})"
                )
            except Exception as e:
                if "Not an open order" in str(e):
                    self._log.warning(
                        f"Order {order.client_order_id} ({ts_order_id}) "
                        f"not found at broker — skipping"
                    )
                else:
                    self._log.error(
                        f"Failed to cancel order {order.client_order_id}: {e}"
                    )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        """Cancel a batch of orders."""
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    # -- FILL DETECTION POLLING -----------------------------------------------------------

    async def _poll_order_fills(self) -> None:
        """Background task: poll TradeStation every N seconds for order status changes.

        Detects fills, cancellations, and rejections and generates the corresponding
        NautilusTrader events so strategies receive on_order_filled / on_order_canceled /
        on_order_rejected callbacks.
        """
        self._log.info("Order fill polling loop started")
        while True:
            try:
                await asyncio.sleep(self._fill_poll_interval)
                await self._check_order_statuses()
            except asyncio.CancelledError:
                self._log.info("Order fill polling loop stopped")
                break
            except Exception as e:
                self._log.error(f"Error in order fill polling: {e}")
                await asyncio.sleep(5.0)

    async def _reconcile_order_statuses_loop(self, interval_secs: float) -> None:
        """Slow REST reconciliation behind SSE streaming (gated by
        TS_STREAM_RECONCILE_POLL_SECS).

        Backstop for events the SSE path can permanently lose: the zero-price
        FLL skip, and the mapping race where the SSE fill arrives before the
        REST submit response registers ``ts_order_id``.  Reuses
        ``_check_order_statuses``; the shared ``_order_last_status`` map keeps
        the poll from re-emitting events the stream already delivered.
        """
        self._log.info("Order status reconcile poll started")
        while True:
            try:
                await asyncio.sleep(interval_secs)
                await self._check_order_statuses()
            except asyncio.CancelledError:
                self._log.info("Order status reconcile poll stopped")
                break
            except Exception as e:
                self._log.error(f"Error in order status reconcile poll: {e}")

    async def _check_order_statuses(self) -> None:
        """Fetch current orders and emit events for any status changes.

        Status flow:
          ACK / OPN → order is open
          FLL       → fully filled   → generate_order_filled
          CAN / UCN / OUT / EXP / DON → canceled/expired → generate_order_canceled
          REJ / BRO / LAT → rejected → generate_order_rejected
        """
        if not self._ts_order_id_to_client_order_id:
            return  # Nothing tracked yet

        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )
        except Exception as e:
            self._log.error(f"Fill poll: failed to fetch orders: {e}")
            return

        for ts_order in orders:
            ts_order_id = ts_order.get("OrderID")
            if not ts_order_id:
                continue

            client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)
            if not client_order_id:
                continue  # Not submitted by us in this session

            status = ts_order.get("Status", "")
            last_status = self._order_last_status.get(ts_order_id, "")

            if status == last_status:
                continue  # No change

            # Hardened (TS_STREAM_FILL_HARDENING): record the status only
            # AFTER the event was successfully processed, so a failed fill
            # emission stays retryable on the next poll instead of being
            # dedup-poisoned forever.  Default keeps today's record-first.
            hardened = _stream_fill_hardening_enabled()
            if not hardened:
                self._order_last_status[ts_order_id] = status

            # Look up the NautilusTrader order for metadata
            cached_order = self._cache.order(client_order_id)
            if cached_order is None:
                self._log.warning(
                    f"Fill poll: order {client_order_id} not found in cache (status={status})"
                )
                if hardened:
                    # A cache miss does not heal on retry; record to stop the spam.
                    self._order_last_status[ts_order_id] = status
                continue

            # Skip if NautilusTrader already considers the order closed (idempotency guard)
            if cached_order.is_closed:
                if hardened:
                    self._order_last_status[ts_order_id] = status
                continue

            venue_order_id = VenueOrderId(ts_order_id)
            ts_now = self._clock.timestamp_ns()

            if status == "FLL":
                # Fully filled
                avg_px_str = ts_order.get("AveragePrice", "0")
                filled_qty_str = ts_order.get(
                    "FilledQuantity", str(cached_order.quantity)
                )

                try:
                    fill_px_raw = float(avg_px_str) if avg_px_str else 0.0
                    if fill_px_raw == 0.0:
                        # AveragePrice is missing/zero — common in sim mode.
                        # Resolution order:
                        #   1. FilledPrice field (present for Market orders in TS sim)
                        #   2. Legs[0].ExecutionPrice (always present when filled)
                        #   3. Order's own trigger_price (StopMarket) or price (Limit)
                        filled_price = ts_order.get("FilledPrice", "") or ""
                        if not filled_price and ts_order.get("Legs"):
                            filled_price = (
                                ts_order["Legs"][0].get("ExecutionPrice", "") or ""
                            )
                        if filled_price and float(filled_price) > 0:
                            avg_px_str = filled_price
                        else:
                            # Last resort: use the order's own price (stop trigger or limit)
                            fallback = getattr(
                                cached_order, "trigger_price", None
                            ) or getattr(  # StopMarket
                                cached_order, "price", None
                            )  # Limit
                            avg_px_str = str(fallback) if fallback else "0"
                        self._log.warning(
                            f"AveragePrice=0 for filled order {client_order_id}; "
                            f"resolved to {avg_px_str} (raw ts_order={ts_order})."
                        )

                    fill_px = Price.from_str(avg_px_str)
                    filled_qty = Quantity.from_str(filled_qty_str)

                    self.generate_order_filled(
                        strategy_id=cached_order.strategy_id,
                        instrument_id=cached_order.instrument_id,
                        client_order_id=client_order_id,
                        venue_order_id=venue_order_id,
                        venue_position_id=None,
                        trade_id=TradeId(f"{ts_order_id}-{ts_now}"),
                        order_side=cached_order.side,
                        order_type=cached_order.order_type,
                        last_qty=filled_qty,
                        last_px=fill_px,
                        quote_currency=Currency.from_str("USD"),
                        commission=Money(Decimal("0"), Currency.from_str("USD")),
                        liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                        ts_event=ts_now,
                    )
                    self._log.info(
                        f"Order filled: {client_order_id} @ {fill_px} qty={filled_qty}",
                        LogColor.GREEN,
                    )
                    if hardened:
                        self._order_last_status[ts_order_id] = status
                except Exception as e:
                    # Hardened: status deliberately NOT recorded — the next
                    # poll retries this fill instead of dropping it forever.
                    self._log.error(
                        f"Fill poll: error generating fill event for {client_order_id}: {e}"
                    )

            elif status in ("CAN", "UCN", "OUT", "EXP", "DON"):
                # Canceled or expired
                self.generate_order_canceled(
                    strategy_id=cached_order.strategy_id,
                    instrument_id=cached_order.instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=ts_now,
                )
                self._log.info(f"Order canceled: {client_order_id} (status={status})")
                if hardened:
                    self._order_last_status[ts_order_id] = status

            elif status in ("REJ", "BRO", "LAT"):
                # Rejected
                reason = ts_order.get("RejectReason", f"TradeStation status: {status}")
                self.generate_order_rejected(
                    strategy_id=cached_order.strategy_id,
                    instrument_id=cached_order.instrument_id,
                    client_order_id=client_order_id,
                    reason=reason,
                    ts_event=ts_now,
                )
                self._log.error(f"Order rejected: {client_order_id} ({reason})")
                if hardened:
                    self._order_last_status[ts_order_id] = status

            else:
                # Non-terminal status (ACK/OPN/...): nothing to emit.
                if hardened:
                    self._order_last_status[ts_order_id] = status

    async def _stream_order_fills(self) -> None:
        """SSE streaming task: receive real-time order events and emit NT order events.

        Replaces ``_poll_order_fills`` when ``use_streaming=True``. Processes the same
        status codes (FLL → filled, CAN → canceled, REJ → rejected) but gets notified
        immediately rather than waiting up to ``_fill_poll_interval`` seconds.
        """
        self._log.info("Order fill SSE stream started")
        # SELF-HEALING SUPERVISION (Phase 0): the order-fill feed must NEVER die
        # from a stream drop -- if it does, submitted orders stop being confirmed
        # (fills/cancels/rejects are missed) and position tracking silently breaks.
        # Per-event processing is already guarded; this outer loop additionally
        # survives the SSE stream itself raising (reconnect exhaustion / peer close)
        # by logging, backing off, and RE-ENTERING the stream.
        retry_delay = 1.0
        while True:
            try:
                async for event in self._stream_client.stream_orders(self._account_id):
                    retry_delay = 1.0  # healthy event resets the backoff
                    try:
                        await self._process_order_event(event)
                    except Exception as e:
                        self._log.error(f"Error processing streamed order event: {e}")
            except asyncio.CancelledError:
                self._log.info("Order fill SSE stream stopped")
                return
            except Exception as exc:  # noqa: BLE001 -- supervise: never let fills die
                self._log.error(
                    f"Order fill SSE stream failed: {exc!r}; "
                    f"resubscribing in {retry_delay:.0f}s"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2.0, 60.0)

    def _on_fill_task_done(self, task: asyncio.Task) -> None:
        """Backstop for the order-fill stream task (see _on_bar_task_done)."""
        if task.cancelled():
            return
        exc = None
        try:
            exc = task.exception()
        except Exception:  # noqa: BLE001
            return
        if exc is None:
            return
        self._log.error(f"Order fill stream task ENDED with {exc!r}; resubscribing")
        if getattr(self, "_is_disconnecting", False):
            return
        try:
            self._fill_poll_task = self._loop.create_task(self._stream_order_fills())
            self._fill_poll_task.add_done_callback(self._on_fill_task_done)
        except Exception as e:  # noqa: BLE001
            self._log.error(f"Failed to resubscribe order fill stream: {e!r}")

    async def _process_order_event(self, ts_order: dict) -> None:
        """Process a single order event (from streaming or polling) and emit NT events."""
        ts_order_id = ts_order.get("OrderID")
        if not ts_order_id:
            return

        client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)
        if not client_order_id:
            return  # Not submitted by us in this session

        status = ts_order.get("Status", "")
        last_status = self._order_last_status.get(ts_order_id, "")

        if status == last_status:
            return  # No change

        # Hardened (TS_STREAM_FILL_HARDENING): record the status only AFTER
        # the event was successfully processed.  Recording it up front poisoned
        # the dedup gate before the zero-price FLL skip, so the fill was dropped
        # FOREVER in streaming mode (no status-poll backstop existed either).
        # Default keeps today's record-first behavior byte-identical.
        hardened = _stream_fill_hardening_enabled()
        if not hardened:
            self._order_last_status[ts_order_id] = status

        cached_order = self._cache.order(client_order_id)
        if cached_order is None or cached_order.is_closed:
            if hardened:
                self._order_last_status[ts_order_id] = status
            return

        venue_order_id = VenueOrderId(ts_order_id)
        ts_now = self._clock.timestamp_ns()

        if status == "FLL":
            avg_px_str = ts_order.get("AveragePrice", "0")
            filled_qty_str = ts_order.get("FilledQuantity", str(cached_order.quantity))

            # Price fallback chain (mirrors _check_order_statuses):
            #   1. AveragePrice  — standard TS fill field
            #   2. FilledPrice   — present for Market orders in TS sim
            #   3. Legs[0].ExecutionPrice — always present when filled
            #   4. (hardened) the order's own trigger/limit price
            fill_px_raw = float(avg_px_str) if avg_px_str else 0.0
            if fill_px_raw == 0.0:
                filled_price = ts_order.get("FilledPrice", "") or ""
                if not filled_price:
                    legs = ts_order.get("Legs", [])
                    if legs:
                        filled_price = legs[0].get("ExecutionPrice", "") or ""
                if filled_price:
                    fill_px_raw = float(filled_price)

            if fill_px_raw == 0.0 and hardened:
                # 4th fallback (poll-path parity): a StopMarket fills at ~its
                # trigger, a Limit at ~its limit — better than losing the fill.
                fallback = getattr(cached_order, "trigger_price", None) or getattr(
                    cached_order, "price", None
                )
                if fallback is not None:
                    fill_px_raw = float(fallback)
                    self._log.warning(
                        f"Stream: AveragePrice=0 for filled order {client_order_id}; "
                        f"using the order's own price {fill_px_raw} (4th fallback)"
                    )

            if fill_px_raw == 0.0:
                if hardened:
                    # Status deliberately NOT recorded: the reconcile poll (or
                    # a repeated stream event) retries this fill instead of the
                    # dedup gate dropping it forever.
                    self._log.warning(
                        f"Stream: AveragePrice=0 for filled order {client_order_id}; "
                        "fill deferred to the status reconcile poll"
                    )
                else:
                    self._log.warning(
                        f"Stream: AveragePrice=0 for filled order {client_order_id}; "
                        "fill event skipped — order status report will reconcile"
                    )
                return

            instrument = self._cache.instrument(cached_order.instrument_id)
            if not instrument:
                return
            prec = instrument.price_precision
            fill_px = Price(round(fill_px_raw, prec), prec)
            filled_qty = Quantity.from_str(filled_qty_str or str(cached_order.quantity))
            self.generate_order_filled(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=TradeId(f"{ts_order_id}-{ts_now}"),
                order_side=cached_order.side,
                order_type=cached_order.order_type,
                last_qty=filled_qty,
                last_px=fill_px,
                quote_currency=instrument.quote_currency,
                commission=Money(Decimal("0"), Currency.from_str("USD")),
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event=ts_now,
            )
            self._log.info(f"Stream: order filled: {client_order_id} @ {fill_px}")
            if hardened:
                self._order_last_status[ts_order_id] = status

        elif status in ("CAN", "UCN", "OUT", "EXP", "DON"):
            self.generate_order_canceled(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_now,
            )
            self._log.info(
                f"Stream: order canceled: {client_order_id} (status={status})"
            )
            if hardened:
                self._order_last_status[ts_order_id] = status

        elif status in ("REJ", "BRO", "LAT"):
            reason = ts_order.get("RejectReason", f"TradeStation status: {status}")
            self.generate_order_rejected(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                reason=reason,
                ts_event=ts_now,
            )
            self._log.error(f"Stream: order rejected: {client_order_id} ({reason})")
            if hardened:
                self._order_last_status[ts_order_id] = status

        else:
            # Non-terminal status (ACK/OPN/...): nothing to emit.
            if hardened:
                self._order_last_status[ts_order_id] = status

    # -- INTERNAL METHODS -----------------------------------------------------------------

    async def _update_account_state(self) -> None:
        """Update account state from TradeStation.

        Retries up to 3 times on transient errors (3 s between attempts).
        If all attempts fail the method returns without raising — trading
        continues and the broker enforces margin on every order. A prominent
        warning is logged so the operator is aware the local balance snapshot
        is unavailable until a subsequent successful call.
        """
        last_err: Exception | None = None
        for attempt in range(3):
            if attempt > 0:
                await asyncio.sleep(3.0)
                self._log.info(
                    f"Retrying account state fetch (attempt {attempt + 1}/3)"
                )
            try:
                balances_data = await self._client.get_balances(
                    account_keys=self._account_id,
                )

                cash_balance = Decimal(balances_data.get("CashBalance", "0"))
                equity = Decimal(balances_data.get("Equity", "0"))
                # MarketValue is negative for short positions — clamp to 0.
                # NautilusTrader requires non-negative MarginBalance values.
                # We don't enforce margin limits so 0 is safe when short.
                margin_used = max(
                    Decimal("0"), Decimal(balances_data.get("MarketValue", "0"))
                )

                balances = [
                    AccountBalance(
                        Money(cash_balance, Currency.from_str("USD")),
                        Money(0, Currency.from_str("USD")),
                        Money(cash_balance, Currency.from_str("USD")),
                    ),
                ]
                margins = [
                    MarginBalance(
                        initial=Money(margin_used, Currency.from_str("USD")),
                        maintenance=Money(margin_used, Currency.from_str("USD")),
                    ),
                ]

                if self.account_id is None:
                    self._set_account_id(self._account_id_nautilus)

                self.generate_account_state(
                    balances=balances,
                    margins=margins,
                    reported=True,
                    ts_event=self._clock.timestamp_ns(),
                    info={
                        "equity": str(equity),
                        "cash_balance": str(cash_balance),
                        "account_id": self._account_id,
                    },
                )
                return  # success — exit retry loop

            except Exception as e:
                last_err = e
                self._log.warning(
                    f"Account state fetch failed (attempt {attempt + 1}/3): {e}"
                )

        self._log.warning(
            f"Account state unavailable after 3 attempts ({last_err}). "
            "Trading will continue — the broker enforces margin on every order. "
            "Balance display will be stale until the next successful update."
        )

    def _is_equity_order(self, order: Order) -> bool:
        """Return True when the order's cached instrument is an Equity."""
        from nautilus_trader.model.instruments import Equity

        return isinstance(self._cache.instrument(order.instrument_id), Equity)

    def _convert_order_to_ts_format(self, order: Order) -> dict[str, Any]:
        """Convert Nautilus order to TradeStation format.

        For futures: always Buy/Sell (TS rejects SellShort/BuyToCover on futures).
        For equities: use SellShort when opening a short, BuyToCover when closing a short.
        """
        params = convert_order_to_ts_format(order, self._account_id)

        # Check if this is an equity instrument — if so, adjust TradeAction
        instrument = self._cache.instrument(order.instrument_id)
        if instrument is not None:
            from nautilus_trader.model.instruments import Equity

            if isinstance(instrument, Equity):
                # For equities, TS requires SellShort/BuyToCover for short positions.
                # AUTHORITY ORDER (bug: SPY EQUITY-GROUP-ORDER 'boxed position' 2026-06-10): the
                # submitting strategy KNOWS its intent and tags the order
                # (TS_INTENT:close_short / close_long / open). The cache inference
                # below stays as fallback only -- the Nautilus cache showed FLAT
                # while the account was short 100 SPY (a dropped/unparsed status
                # report), so a flatten BUY went out as plain 'Buy' and was
                # REJECTED, orphaning the short.
                intent_action = equity_trade_action_from_intent(order)
                if intent_action is not None:
                    params["trade_action"] = intent_action
                    if self._extended_hours:
                        params["time_in_force"] = "DYP"
                    return params
                # Calculate net position from open positions in cache (fallback)
                open_positions = self._cache.positions_open(
                    instrument_id=order.instrument_id,
                )
                net_pos = (
                    sum(p.signed_qty for p in open_positions) if open_positions else 0
                )

                if order.side == OrderSide.SELL:
                    if net_pos <= 0:
                        # No long position to close — this is opening a short
                        params["trade_action"] = "SellShort"
                    else:
                        # Closing an existing long
                        params["trade_action"] = "Sell"
                elif order.side == OrderSide.BUY:
                    if net_pos < 0:
                        # Closing an existing short
                        params["trade_action"] = "BuyToCover"
                    else:
                        # Opening a new long
                        params["trade_action"] = "Buy"

                # When extended_hours is enabled, use DYP (Day Plus) so equity
                # limit orders can fill during pre-market and after-hours sessions.
                # TS rejects Market orders with DYP — use Limit orders instead.
                if self._extended_hours:
                    params["time_in_force"] = "DYP"

        return params

    def _convert_order_type(self, order: Order) -> str:
        """Convert Nautilus order type to TradeStation format."""
        return convert_order_type(order)

    def _convert_time_in_force(self, tif: TimeInForce) -> str:
        """Convert Nautilus TimeInForce to TradeStation format."""
        return convert_time_in_force(tif)

    def _parse_order_status(self, ts_status: str) -> OrderStatus:
        """Parse TradeStation order status to Nautilus OrderStatus."""
        return parse_order_status(ts_status)

    def _parse_order_status_report(
        self,
        ts_order: dict[str, Any],
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
    ) -> OrderStatusReport | None:
        """Parse TradeStation order into OrderStatusReport.

        When the payload carries no quantity anywhere (some TS payload shapes
        omit top-level Quantity AND Legs quantities), the cached engine
        order's quantity is passed as the fallback so the status report is
        not silently dropped (bug ledger C1-PARSE — a dropped report can
        desync engine vs broker state).
        """
        fallback_quantity: Decimal | None = None
        try:
            cached_order = self._cache.order(client_order_id)
            if cached_order is not None:
                fallback_quantity = Decimal(str(cached_order.quantity))
        except Exception:  # noqa: BLE001 -- fallback only; never block the parse
            fallback_quantity = None
        return parse_order_status_report(
            ts_order,
            instrument_id,
            client_order_id,
            account_id=self._account_id_nautilus,
            ts_now=self._clock.timestamp_ns(),
            fallback_quantity=fallback_quantity,
        )

    def _parse_ts_order_type(self, ts_order_type: str) -> OrderType:
        """Parse TradeStation order type to Nautilus OrderType."""
        return parse_ts_order_type(ts_order_type)
