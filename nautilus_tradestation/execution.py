"""
TradeStation execution client implementation.
"""

import asyncio
import os
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_tradestation.http import budget as _budget
from nautilus_tradestation.http.client import RateLimitedError
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


RAW_ORDER_CANCEL_TOPIC = "events.execution.raw_order_cancel"
_RAW_CANCEL_REASON_FIELDS = (
    "StatusDescription",
    "Message",
    "CancelReason",
    "RejectReason",
    "Reason",
    "Error",
)


def _raw_cancel_reason(ts_order: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return the broker's verbatim terminal description and its source field."""
    for field in _RAW_CANCEL_REASON_FIELDS:
        value = ts_order.get(field)
        if value is not None and str(value).strip():
            return str(value), field
    return None, None


def _venue_confirmed_cancels_enabled() -> bool:
    """TS_VENUE_CONFIRMED_CANCELS=1: a REST DELETE 200 is treated as a
    cancel REQUEST acknowledgment, not venue-terminal state.

    TradeStation's DELETE acknowledges the request; the order can still FILL
    inside the race window (exactly the fast tape a resting stop leg gets
    canceled in).  Synthesizing ``OrderCanceled`` on the 200 makes the order
    terminal in Nautilus, so the real SSE ``FLL`` that follows is dropped by
    the ``is_closed`` gate -- leg fill + a released market exit double-fill the
    position (the concurrent-fill race window).  With the flag set
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
    ``_check_order_statuses`` every that-many seconds while this client has
    orders it has not yet seen reach a terminal status (see
    ``_stream_reconcile_idle_secs`` below for the cadence once it does not);
    the shared ``_order_last_status`` dedup keeps SSE + poll from
    double-emitting. Unset/0 (the default) keeps today's streaming-only
    behavior byte-identical.
    """
    raw = os.environ.get("TS_STREAM_RECONCILE_POLL_SECS", "")
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


# 2026-09-06 API-quota regression
# (the API budget review Finding 1): this poll is a SAFETY
# NET behind the SSE stream, not a primary fill path -- it only has real work
# to do while there is at least one order this client submitted that has not
# yet been observed reaching a terminal status. In a multi-process deployment, dozens
# of node processes can share ONE TradeStation account; polling every 30s regardless of
# whether there was anything outstanding to reconcile drove ~1.1-1.3 REST
# calls/second continuously against that one account's rate-limit bucket --
# comparable in magnitude to the pre-2026-08-16 problem the shared
# _OrderFlowRefresher cadence fix was built to solve, and never counted in
# that fix's math (that change only touched the host application's bar
# refresher, not this adapter-level order-status poll). A floor of 300s below
# is enforced regardless of misconfiguration -- the idle cadence must always
# be materially slower than the working cadence, never accidentally equal or
# faster.
_RECONCILE_IDLE_FLOOR_SECONDS = 300.0
# Statuses _check_order_statuses treats as terminal (see its own docstring);
# duplicated here (not imported) because this set is a poll-cadence policy
# concern, independent of the status-parsing implementation it happens to
# share a source file with today.
_TERMINAL_ORDER_STATUSES = frozenset({"FLL", "CAN", "OUT", "EXP", "REJ", "BRO", "LAT"})


def _stream_reconcile_idle_secs() -> float:
    """TS_STREAM_RECONCILE_POLL_IDLE_SECS=<seconds>: cadence for the same
    reconcile poll once this client has NO outstanding (non-terminal) orders
    to reconcile -- i.e. nothing the SSE stream could plausibly still be
    covering. Floored at ``_RECONCILE_IDLE_FLOOR_SECONDS`` (300s) even if an
    operator sets a smaller value, so a fat-fingered override cannot silently
    recreate the fast steady-state poll this cadence split exists to avoid.
    Unset defaults to the floor.
    """
    raw = os.environ.get("TS_STREAM_RECONCILE_POLL_IDLE_SECS", "")
    if not raw:
        return _RECONCILE_IDLE_FLOOR_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return _RECONCILE_IDLE_FLOOR_SECONDS
    return max(_RECONCILE_IDLE_FLOOR_SECONDS, value)


#: The env var the bar data client already reads
#: (``nautilus_tradestation/data.py``) and the host supervisor's
#: cell spawner already bakes into EVERY supervisor-launched cell's
#: env for bars.
_FEED_PROXY_ENV = "TS_FEED_PROXY"

#: Order-feed opt-in gate: the order-feed proxy's OWN, separate opt-in.
#:
#: ``TS_FEED_PROXY`` is already deployment-wide for bars -- reusing it to also
#: arm the order proxy would mean the INSTANT this vendor build lands, every
#: already-running supervisor-launched host flips into order-proxy mode,
#: before any feed-handler on the new build has even been restarted to write
#: an ``orders/`` mirror for it to tail. That is a deployment-wide accidental
#: cutover, not the one-account canary Stage 1 requires. So this is a
#: DIFFERENT variable, checked ONLY here, never derived from
#: ``TS_FEED_PROXY``:
#:
#:   unset / empty      -> order-feed proxy OFF, direct mode -- even when
#:                          TS_FEED_PROXY is armed for bars.
#:   "1" / "true"       -> ON, reusing the SAME root TS_FEED_PROXY points at
#:                          (the common case once a canary account's handler
#:                          has been restarted with order-ingest enabled).
#:   "jsonl:<path>"     -> ON, an explicit path (a differently-rooted canary,
#:                          or a test that wants total independence from
#:                          whatever TS_FEED_PROXY happens to be).
_ORDER_FEED_PROXY_ENV = "TS_ORDER_FEED_PROXY"


def _resolve_order_feed_proxy() -> str:
    """The ``jsonl:<path>`` value to hand ``FeedTailStreamClient.from_env``
    for the order channel, or ``""`` if the order-feed proxy is not armed for
    this process. See ``_ORDER_FEED_PROXY_ENV`` for the exact contract."""
    raw = os.environ.get(_ORDER_FEED_PROXY_ENV, "").strip()
    if not raw:
        return ""
    if raw in ("1", "true", "True"):
        return os.environ.get(_FEED_PROXY_ENV, "")
    return raw


#: Environment variable naming an OPTIONAL host hook that receives order-feed
#: health ("pkg.module:function"). The function is called as
#: ``function(account_id, proxy=<str|None>, degraded=<bool>)`` on every
#: connect / degrade / recover event, so a host process can surface the
#: order-feed state in its own heartbeat or monitoring. Unset (the default):
#: no hook, no import, nothing happens.
ORDER_FEED_HOOK_ENV = "TRADESTATION_ORDER_FEED_HOOK"

#: A configured hook that cannot be called (its module imports but the
#: function is missing, or the call raises) is warned ONCE per process (not
#: per call -- this runs on every connect/degrade/recover event), so a broken
#: hook -- most likely version skew between this adapter and the host -- is
#: diagnosable from the node's own stdout log instead of invisible.
_SEAM_WARNED = False


def _report_order_feed_state(account_id: str, *, proxy: str | None, degraded: bool) -> None:
    """Best-effort call into the host's order-feed hook (see
    ``ORDER_FEED_HOOK_ENV``) so the host can surface live order-feed health.
    Mirrors the ``nautilus_tradestation.http.budget`` optional-hook pattern --
    never raises, a no-op when no hook is configured or its module is not
    importable -- but unlike that pattern, a hook that is configured and
    importable yet fails is WARNED once (stdout), not swallowed silently: see
    ``_SEAM_WARNED``."""
    global _SEAM_WARNED
    target = os.environ.get(ORDER_FEED_HOOK_ENV, "").strip()
    if not target:
        return  # no host hook configured -- expected, silent
    module_name, _, func_name = target.partition(":")
    try:
        import importlib

        hook_module = importlib.import_module(module_name)
    except ImportError:
        return  # host module not importable in this process -- silent
    except Exception:  # noqa: BLE001
        hook_module = None
    try:
        if hook_module is None or not func_name:
            raise AttributeError(f"cannot resolve {target!r}")
        getattr(hook_module, func_name)(account_id, proxy=proxy, degraded=degraded)
    except Exception as exc:  # noqa: BLE001 -- fail open, but LOUD (see _SEAM_WARNED)
        if not _SEAM_WARNED:
            _SEAM_WARNED = True
            print(
                f"[ORDER-FEED-WARNING] order-feed state hook {target!r} failed for "
                f"account {account_id}: {exc!r}. The host will NOT see "
                "order-feed proxy/degraded state. Likely cause: version skew "
                "between this adapter and the host application. Trading "
                "itself is unaffected (this is telemetry only).",
                flush=True,
            )
        return


def _reconcile_initial_jitter_seconds(fast_interval_secs: float) -> float:
    """One-time random delay before the reconcile loop's first tick.

    WHY RANDOM, NOT A DETERMINISTIC HASH (contrast with the 2026-08-16
    host-side bar refresher's ``sha256(account_id + strategy_id)`` jitter): that fix needed a STABLE key because it
    coordinates a cross-process lease that must re-elect the same relative
    ordering across a respawn. This jitter has no such state to stay
    consistent with -- its only job is to stop a deployment-wide mass relaunch
    from starting every node's reconcile loop in the same instant and then
    staying in lockstep forever (a simple ``sleep(interval)`` loop never
    re-synchronizes on its own once offset). A fresh random draw per process
    start achieves that without threading a new stable per-node identity
    through the exec-client config/factory chain across two repositories.
    Bounded to ``[0, fast_interval_secs)`` -- large enough to desynchronize a
    mass relaunch, never larger than one working-cadence period.
    """
    if fast_interval_secs <= 0:
        return 0.0
    import random

    return random.uniform(0.0, fast_interval_secs)


# --------------------------------------------------------------------------- #
# Startup connect backoff (REST budget, deliverable 3)
# --------------------------------------------------------------------------- #
#: Total wall-clock a connect may spend waiting out 429s before giving up.
#: Bounded well under the host launcher's ``OBSERVE_SECONDS = 90``
#: launch-observation window, so a node that recovers still reports ALIVE to
#: the launcher rather than trading one failure mode (dies instantly) for
#: another (survives the burst but is graded dead anyway).
_STARTUP_CONNECT_MAX_WAIT_S = 60.0
_STARTUP_CONNECT_BACKOFF_S: tuple[float, ...] = (2.0, 4.0, 8.0, 16.0, 30.0)


def _startup_connect_schedule(
    max_wait_s: float = _STARTUP_CONNECT_MAX_WAIT_S,
    backoff_s: tuple[float, ...] = _STARTUP_CONNECT_BACKOFF_S,
    retry_after_s: float | None = None,
) -> list[float]:
    """The bounded sleep schedule a connect uses to ride out a 429 burst.

    Pure, so the bound is testable without waiting a real minute. Truncated
    the moment cumulative wait would exceed ``max_wait_s`` -- the retry budget
    is BOUNDED by construction, never "keep trying." A venue-supplied
    ``Retry-After`` replaces the first delay when it is larger, because the
    venue knows when its own bucket refills and we do not.
    """
    if max_wait_s <= 0:
        return []
    delays = list(backoff_s)
    if retry_after_s is not None and delays and retry_after_s > delays[0]:
        delays[0] = float(retry_after_s)
    out: list[float] = []
    total = 0.0
    for delay in delays:
        delay = max(0.0, float(delay))
        if total + delay > max_wait_s:
            remaining = max_wait_s - total
            if remaining > 0.5:
                out.append(remaining)
            break
        out.append(delay)
        total += delay
    return out


def _env_float_or(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default


def _should_poll_now(
    *,
    pending: bool,
    last_poll_time: float | None,
    now: float,
    fast_interval_secs: float,
    idle_interval_secs: float,
) -> bool:
    """Pure decision function for the reconcile-poll cadence state machine.

    Extracted out of ``_reconcile_order_statuses_loop`` so the cadence policy
    (fast while ``pending``, idle otherwise, always poll on the very first
    tick) is unit-testable with synthetic clock values -- no real
    ``asyncio.sleep`` waiting required to exercise the 300s idle branch.
    ``last_poll_time is None`` (nothing polled yet this loop) always polls,
    matching pre-existing behavior (the poll always fires on its first
    tick regardless of cadence).
    """
    if last_poll_time is None:
        return True
    required_interval = fast_interval_secs if pending else idle_interval_secs
    return (now - last_poll_time) >= required_interval


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
        scope_reconciliation_to_known_orders: bool = False,
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
        self._streaming_reconnect_delay_secs = streaming_reconnect_delay_secs
        self._stream_client: "TradeStationStreamClient | None" = None
        if use_streaming:
            self._stream_client = self._build_real_stream_client()

        # Shared order stream: readable by the host (the host application's
        # live_runner / process heartbeat) via ``_report_order_feed_state``.
        # ``order_feed_degraded`` is set True the moment ``_stream_order_fills``
        # observes FeedGapError/FeedHandlerDeadError from the relay and
        # cleared on the next healthy event -- NEVER treated as "flat" in
        # between; it only forces the REST reconcile poll to its fast cadence.
        self.order_feed_degraded: bool = False
        self._order_feed_proxy: str | None = None
        # Canary hardening: the 2026-09-09 canary found the reconcile
        # poll's REST calls staying at the idle ~300s cadence for most of a
        # 7-minute staged outage instead of engaging its fast ~30s cadence
        # promptly -- ``_reconcile_order_statuses_loop`` only re-evaluates
        # ``order_feed_degraded`` once per ``interval_secs`` sleep, so a flip
        # mid-sleep waited out the REST of that sleep before being noticed.
        # This event lets ``_set_order_feed_degraded`` interrupt that sleep
        # the instant the flag changes, bounding the wake latency to well
        # under ``interval_secs`` regardless of when in the cycle it flips.
        self._order_feed_wake = asyncio.Event()

        # Order-map persistence (T-2)
        self._order_map_path: str | None = order_map_path

        # Shared-account reconciliation scoping: shared-account topology scoping.
        # See TradeStationExecClientConfig.scope_reconciliation_to_known_orders for the
        # full rationale.
        self._scope_reconciliation_to_known_orders: bool = (
            scope_reconciliation_to_known_orders
        )

        # Account setup
        self._account_id_nautilus = AccountId(f"{self.id}-{account_id}")

    def _publish_raw_cancel_evidence(
        self,
        ts_order: dict[str, Any],
        cached_order: Order,
        *,
        source: str,
        ts_event: int,
        resolution_kind: str | None = None,
    ) -> None:
        """Publish raw venue terminal evidence before Nautilus collapses status.

        ``OrderCanceled`` intentionally has no broker-status/reason fields.  This
        side-channel preserves TradeStation's raw status without changing the
        Nautilus lifecycle event or its terminal-state semantics.  The host consumes
        the envelope synchronously, keyed by client order id, before the generated
        ``OrderCanceled`` reaches the strategy.
        """
        raw_status = str(ts_order.get("Status", "") or "")
        broker_reason, broker_reason_field = _raw_cancel_reason(ts_order)
        try:
            self._msgbus.publish(
                topic=RAW_ORDER_CANCEL_TOPIC,
                msg={
                    "client_order_id": str(cached_order.client_order_id),
                    "venue_order_id": str(ts_order.get("OrderID", "") or "") or None,
                    "strategy_id": str(cached_order.strategy_id),
                    "raw_status": raw_status or None,
                    "broker_reason": broker_reason,
                    "broker_reason_field": broker_reason_field,
                    "source": source,
                    "ts_event": int(ts_event),
                    **({"resolution_kind": resolution_kind} if resolution_kind else {}),
                },
                external_pub=False,
            )
        except Exception as exc:
            # Evidence capture must never alter or suppress the terminal event.
            self._log.warning(
                "Raw cancel evidence publish failed for "
                f"{cached_order.client_order_id} (status={raw_status}): {exc}"
            )

    def _handle_ambiguous_terminal_update(self, ts_order, *, source: str) -> bool:
        """One authority for ambiguous cancellation and legacy closed-cache recovery.

        True means this update was handled (or refused without fabricating facts).
        Ordinary existing open-order updates remain on their existing path.
        """
        if os.environ.get("TS_AMBIGUOUS_CANCEL_AUTHORITY", "0") != "1":
            return False
        oid = str(ts_order.get("OrderID") or "")
        cid = self._ts_order_id_to_client_order_id.get(oid)
        order = self._cache.order(cid) if cid else None
        if order is None:
            return False
        try:
            self._validate_broker_identity(ts_order, order, required=False)
        except ValueError as exc:
            self._log.error(f"Rejected mismatched broker update for {cid}: {exc}")
            return True
        pending = getattr(self, "_pending_recovery_fills", {})
        awaiting = pending.get(oid)
        if awaiting is not None:
            if order.filled_qty.as_decimal() < awaiting:
                return True  # ExecEngine queues events; wait for actual cache application
            pending.pop(oid, None)
        status = str(ts_order.get("Status") or "")
        last = self._order_last_status.get(oid)
        ambiguous = getattr(self, "_ambiguous_cancel_orders", None)
        if ambiguous is None:
            ambiguous = self._ambiguous_cancel_orders = set()
        if status in {"UCN", "DON"}:
            if order.status == OrderStatus.FILLED or (order.is_closed and last in _TERMINAL_ORDER_STATUSES):
                return True  # stale notice must not regress a confirmed terminal state
            ambiguous.add(oid)
            if last != status:
                self._publish_raw_cancel_evidence(
                    ts_order, order, source=source, ts_event=self._clock.timestamp_ns(),
                    resolution_kind="nonterminal_notice",
                )
            self._order_last_status[oid] = status
            return True  # never close the Nautilus order on a request/session notice
        recovering = order.status == OrderStatus.CANCELED or oid in ambiguous
        if status in {"FLL", "FLP"} and (recovering or order.filled_qty.as_decimal() > 0 or status == "FLP"):
            if not self._deliver_proven_fill_delta(ts_order, order):
                return True  # unverified execution stays retryable and unresolved
            self._order_last_status[oid] = status
            if status == "FLL":
                ambiguous.discard(oid)
            return True
        if status in {"CAN", "OUT", "EXP"} and recovering:
            if last == status and order.is_closed:
                return True
            # A broker cancellation may also carry a partial execution. Never
            # release the host's exit while an unaccounted fill is still unresolved.
            if not self._deliver_proven_fill_delta(ts_order, order):
                return True
            if order.status != OrderStatus.CANCELED:
                return False  # real open/partially-filled order needs the normal terminal event
            self._publish_raw_cancel_evidence(
                ts_order, order, source=source, ts_event=self._clock.timestamp_ns(),
                resolution_kind="confirmed_terminal",
            )
            self._order_last_status[oid] = status
            ambiguous.discard(oid)
            return True  # engine suppresses duplicate CANCELED events by design
        if order.status == OrderStatus.CANCELED and status not in _TERMINAL_ORDER_STATUSES:
            return True  # late ACK/unknown cannot reopen a legacy closed cache
        return False

    def _validate_broker_identity(self, ts_order, order, *, required: bool) -> None:
        """Authenticate supplied identity; recovery requires a complete broker row."""
        oid = str(ts_order.get("OrderID") or "")
        reverse = self._client_order_id_to_ts_order_id.get(order.client_order_id)
        if reverse != oid:
            raise ValueError("reverse venue mapping mismatch")
        if order.venue_order_id is not None and str(order.venue_order_id) != oid:
            raise ValueError("cached venue mismatch")
        legs = ts_order.get("Legs") or []
        if len(legs) > 1:
            raise ValueError("multiple legs")
        leg = legs[0] if legs else {}
        account = ts_order.get("AccountID")
        symbol = leg.get("Symbol") or ts_order.get("Symbol")
        side = leg.get("BuyOrSell")
        if required and not all((account, symbol, side)):
            raise ValueError("incomplete account/instrument/side identity")
        if account is not None and account != self._account_id:
            raise ValueError("account mismatch")
        if symbol and symbol != str(order.instrument_id.symbol):
            raise ValueError("instrument mismatch")
        expected_side = "Buy" if order.side == OrderSide.BUY else "Sell"
        if side and side != expected_side:
            raise ValueError("side mismatch")

    def _deliver_proven_fill_delta(self, ts_order, order) -> bool:
        """Deliver only a broker-evidenced, previously unapplied execution delta.

        No trigger-price or requested-quantity fallback. Cumulative averages may
        reconstruct a remaining delta only when both averages are available.
        """
        try:
            self._validate_broker_identity(ts_order, order, required=True)
            legs = ts_order.get("Legs") or []
            if len(legs) > 1:
                raise ValueError("multiple legs")
            leg = legs[0] if legs else {}
            if ts_order.get("AccountID") not in (None, self._account_id):
                raise ValueError("account mismatch")
            symbol = leg.get("Symbol") or ts_order.get("Symbol")
            if symbol and symbol != str(order.instrument_id.symbol):
                raise ValueError("instrument mismatch")
            side = leg.get("BuyOrSell")
            expected_side = "Buy" if order.side == OrderSide.BUY else "Sell"
            if side and side != expected_side:
                raise ValueError("side mismatch")
            raw_total = ts_order.get("FilledQuantity", leg.get("ExecQuantity"))
            if raw_total in (None, ""):
                raise ValueError("missing cumulative executed quantity")
            total = Decimal(str(raw_total))
            prior = order.filled_qty.as_decimal()
            if not total.is_finite() or total < prior or total > order.quantity.as_decimal():
                raise ValueError("invalid cumulative executed quantity")
            if ts_order.get("Status") == "FLL" and total != order.quantity.as_decimal():
                raise ValueError("FLL quantity is not complete")
            delta = total - prior
            if delta == 0:
                return True
            raw_average = ts_order.get("AveragePrice")
            if prior:
                if not raw_average or not order.avg_px:
                    raise ValueError("incremental fill requires both cumulative averages")
                px = (Decimal(str(raw_average)) * total - Decimal(str(order.avg_px)) * prior) / delta
            else:
                px = Decimal("0")
                for candidate in (raw_average, ts_order.get("FilledPrice"), leg.get("ExecutionPrice")):
                    if candidate in (None, ""):
                        continue
                    value = Decimal(str(candidate))
                    if value.is_finite() and value > 0:
                        px = value
                        break
            if not px.is_finite() or px <= 0:
                raise ValueError("missing/invalid actual execution price")
            instrument = self._cache.instrument(order.instrument_id)
            if instrument is None:
                raise ValueError("missing instrument")
            oid = str(ts_order["OrderID"])
            self.generate_order_filled(
                strategy_id=order.strategy_id, instrument_id=order.instrument_id,
                client_order_id=order.client_order_id, venue_order_id=VenueOrderId(oid),
                venue_position_id=None, trade_id=TradeId(f"{oid}-cum-{total}"),
                order_side=order.side, order_type=order.order_type,
                last_qty=Quantity.from_str(str(delta)),
                last_px=Price.from_str(f"{px:.{instrument.price_precision}f}"),
                quote_currency=instrument.quote_currency,
                commission=Money(Decimal("0"), instrument.quote_currency),
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event=self._clock.timestamp_ns(),
            )
            pending = getattr(self, "_pending_recovery_fills", None)
            if pending is None:
                pending = self._pending_recovery_fills = {}
            pending[oid] = total
            return False  # enqueued is not applied; a later broker poll certifies completion
        except Exception as exc:
            self._log.error(f"Unresolved broker execution for {order.client_order_id}: {exc}")
            return False

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

    def _build_real_stream_client(self):
        """Build a direct ``TradeStationStreamClient`` -- the pre-existing
        constructor logic, factored out so it can ALSO serve as the
        ``real_client_factory`` a proxy-mode ``FeedTailStreamClient`` uses for
        the streams it still delegates (quotes/market depth); this exec
        client only ever calls ``stream_orders`` on either object, which the
        proxy now serves for real (Shared order stream)."""
        from nautilus_tradestation.streaming.client import TradeStationStreamClient

        return TradeStationStreamClient(
            access_token_provider=lambda: self._client.access_token,
            access_token_refresher=lambda force_refresh=False: self._client.get_access_token(
                force_refresh=force_refresh
            ),
            base_url=self._client.base_url,
            reconnect_delay_secs=self._streaming_reconnect_delay_secs,
        )

    async def _connect_get_accounts_with_backoff(self) -> list:
        """``get_accounts()``, retried with bounded backoff on HTTP 429 only.

        WHY (REST budget, deliverable 3): on 2026-09-07
        16:58 ET a brand-new node (``another-strategy``, host 7248) died at
        start with ``Failed to connect to TradeStation: Get accounts failed:
        HTTP 429`` -> ``node_start_timeout``, ``strategies_started=0``. The
        same shape killed two of the three 2026-08-26 canary launch attempts
        (per the node launch logs). A node dying on ONE burst from a
        shared account's rate limiter is a capital-safety event: a strategy
        that never starts is a strategy whose live position nobody manages.

        Deliberately narrow:
          * 429 ONLY. Every other failure (bad credentials, 403, a network
            error) still fails immediately and loudly -- retrying those hides
            real faults, which is the band-aid Rule 7 forbids.
          * BOUNDED by ``_startup_connect_schedule`` and kept under the
            launcher's 90 s observation window; it never becomes "retry
            forever".
          * Each wait is LOGGED with its attempt number, so a slow start is
            visible in the node's own stderr rather than looking like a hang.
          * The startup reservation asks the shared budget for the
            startup-priority band, which polls are held out of by design --
            so a saturated account still has room to admit a node.
        """
        max_wait = _env_float_or(
            "TS_STARTUP_CONNECT_MAX_WAIT_S", _STARTUP_CONNECT_MAX_WAIT_S
        )
        attempt = 0
        last_error: Exception | None = None
        schedule: list[float] | None = None
        while True:
            granted, reason = _budget.reserve_startup(str(self._account_id))
            if not granted:
                self._log.warning(
                    f"Startup account call held by the shared API budget ({reason}) -- "
                    "proceeding anyway; startup traffic is never blocked, only reported"
                )
            try:
                return await self._client.get_accounts()
            except RateLimitedError as exc:
                last_error = exc
                if schedule is None:
                    schedule = _startup_connect_schedule(
                        max_wait_s=max_wait,
                        retry_after_s=exc.retry_after_seconds,
                    )
                if attempt >= len(schedule):
                    self._log.error(
                        f"Get accounts still rate-limited (429) after {attempt} "
                        f"retries over ~{sum(schedule):.0f}s -- giving up"
                    )
                    raise
                delay = schedule[attempt]
                attempt += 1
                self._log.warning(
                    f"Get accounts rate-limited (429) -- backing off {delay:.0f}s "
                    f"(startup retry {attempt}/{len(schedule)}; the account's REST "
                    "budget is saturated, see the host budget report)"
                )
                await asyncio.sleep(delay)
            except Exception:
                raise
        raise last_error  # pragma: no cover -- loop only exits via return/raise

    async def _connect(self) -> None:
        """Connect to TradeStation."""
        self._log.info("Connecting to TradeStation...")

        # REST budget: tell the shared per-account budget
        # which account THIS process draws down, BEFORE the first REST call,
        # so account-less URLs (this very `GET /brokerage/accounts`, and every
        # /marketdata fetch) are attributed to the right bucket instead of
        # landing in "unknown".
        _budget.set_default_account(str(self._account_id))

        # Test authentication by fetching account info
        try:
            accounts = await self._connect_get_accounts_with_backoff()
            self._log.info(
                f"Successfully authenticated. Found {len(accounts)} account(s)"
            )

            # Fetch initial balances and positions
            await self._update_account_state()

        except Exception as e:
            self._log.error(f"Failed to connect to TradeStation: {e}")
            raise

        # Shared order stream / Order-feed opt-in gate: if the
        # ACCOUNT'S OWN order-feed opt-in is armed (TS_ORDER_FEED_PROXY --
        # a SEPARATE variable from the bar proxy's TS_FEED_PROXY, see
        # _resolve_order_feed_proxy's docstring for why), tail the relay's
        # order mirror instead of opening this host's OWN SSE order
        # connection + this host's own reconcile-poll REST calls -- exactly
        # the 26-streams-and-26-pollers -> 1-and-1 reduction this workstream
        # exists to deliver. TS_FEED_PROXY being armed for bars does NOT, by
        # itself, arm this -- direct order mode stays the default even on a
        # fully bar-proxied host until the canary opt-in is set explicitly.
        feed_proxy = _resolve_order_feed_proxy() if self._use_streaming else ""
        if feed_proxy:
            from nautilus_tradestation.feed.tail_client import FeedTailStreamClient

            self._stream_client = FeedTailStreamClient.from_env(
                feed_proxy,
                poll_ms=int(
                    os.environ.get(
                        "TS_FEED_PROXY_POLL_MS", os.environ.get("TS_FEED_POLL_MS", "100")
                    )
                ),
                real_client_factory=self._build_real_stream_client,
            )
            self._order_feed_proxy = feed_proxy
            self._log.info(
                f"Order feed: tailing account relay mirror ({feed_proxy}) instead of "
                "opening a direct SSE order stream",
                LogColor.GREEN,
            )
            _report_order_feed_state(str(self._account_id), proxy=feed_proxy, degraded=False)

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
        if self._order_feed_proxy:
            _report_order_feed_state(
                str(self._account_id), proxy=self._order_feed_proxy, degraded=False
            )
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
                # Bracket cancel-timeout investigation (Gap A):
                # ``/orders`` only ever serves "today's current activity" --
                # an order that has aged off it (settled, canceled, filled)
                # can never be found there again, and a one-shot re-poll that
                # only calls ``get_orders`` is structurally blind to it (this
                # is EXACTLY the mechanism that let a 2026-09-10 UCN cancel
                # re-alert every 10 minutes for 15+ hours although
                # TradeStation's own record showed the order cleanly OUT
                # within 0.3s, and separately let a DON'd stop go unconfirmed
                # for 9+ hours until it filled for real -- see
                # the bracket cancel-timeout investigation). Fall back
                # to ``/historicalorders`` -- the SAME two-endpoint contract
                # the host application's order-history fetcher already proved
                # empirically (2026-07-10 ticket #30). Best-effort: a
                # fallback failure leaves the caller's own retry cadence in
                # place, exactly like the pre-existing not-found path below.
                try:
                    hist_since = (
                        pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
                    ).strftime("%Y-%m-%d")
                    hist_orders = await self._client.get_historical_orders(
                        account_keys=self._account_id,
                        since=hist_since,
                    )
                    for order in hist_orders:
                        if order.get("OrderID") == ts_order_id:
                            ts_order = order
                            break
                except Exception as hist_exc:  # noqa: BLE001 -- best-effort fallback
                    self._log.warning(
                        f"historicalorders fallback failed for {ts_order_id}: "
                        f"{hist_exc}"
                    )

            if not ts_order:
                self._log.warning(
                    f"Order {ts_order_id} not found at TradeStation "
                    "(/orders + /historicalorders)"
                )
                return None

            if self._handle_ambiguous_terminal_update(ts_order, source="status_query"):
                return None  # already delivered, or explicitly unresolved; no inference bypass
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
                    if self._scope_reconciliation_to_known_orders:
                        # Shared-account reconciliation scoping: on a shared account,
                        # get_orders() returns every OTHER process's orders too, with no
                        # broker-side field distinguishing them from this client's own.
                        # An order this client never submitted (or loaded from
                        # order_map_path) is provably not ours -- drop it rather than
                        # synthesizing a TS-{id} tag Nautilus would treat as this node's.
                        continue
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

                if coid is None and self._scope_reconciliation_to_known_orders:
                    # Shared-account reconciliation scoping: same shared-account leak
                    # as generate_order_status_reports -- a fill this client cannot
                    # attribute to one of its own orders belongs to another process.
                    continue

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

        if self._scope_reconciliation_to_known_orders:
            # Shared-account reconciliation scoping: get_positions() returns the
            # account's ONE netted line per symbol across every process/strategy
            # sharing this account -- there is no way to attribute a fraction of it
            # to this process's own strategies, so any non-empty report here is either
            # entirely another process's exposure or an inseparable mix. Report
            # nothing and let the caller's own historical-position policy / adoption
            # own the position belief (the host application's design), instead of letting
            # Nautilus's cross-zero auto-heal collide this client's cold cache against
            # the whole account's real net and abort the node.
            self._log.debug(
                "Position status reports suppressed "
                "(scope_reconciliation_to_known_orders=True)"
            )
            return []

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
        # honor TS_INTENT tags (equity short-cover rejection class): an equity
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
                    # REJECTION-PATH ALERT: an unmapped leg is
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
            # Equity covers must keep their TradeAction on replace (equity short-cover
            # rejection class): replacing a BuyToCover leg with plain 'Buy' would be
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
                # by the is_closed gate (double-fill class).
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
                # Proactively run _check_order_statuses so the fill event is processed
                # within seconds — before NT's ExecEngine times out and auto-generates
                # an OrderCanceled (which would drop any subsequent SSE fill event).
                self._log.warning(
                    f"Order {command.client_order_id} ({ts_order_id}) not found at broker "
                    f"(expired DAY order or concurrent fill) — checking order statuses now"
                )
                await self._check_order_statuses()
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
                # event (SSE/poll), never synthesized here (concurrent-fill race class).
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
                # REST budget: this loop is the PRIMARY
                # fill path when streaming is off, so it is never gated --
                # but its calls must still reach the shared per-account
                # window, or the ceiling would under-count the deployment by
                # exactly the traffic of every non-streaming node.
                _budget.flush(str(self._account_id))
                await self._check_order_statuses()
            except asyncio.CancelledError:
                self._log.info("Order fill polling loop stopped")
                break
            except Exception as e:
                self._log.error(f"Error in order fill polling: {e}")
                await asyncio.sleep(5.0)

    def _has_pending_orders(self) -> bool:
        """True iff this client submitted an order that has not yet been
        observed reaching a terminal status -- i.e. the reconcile poll still
        has real reconciliation work to do.

        Fails toward True (assume pending / keep polling fast) on anything
        ambiguous: an order with no ``_order_last_status`` entry yet (just
        submitted, not yet observed by either SSE or a poll) counts as
        pending, and so does one whose cache entry cannot be found (never
        silently assume "nothing to reconcile" from missing data -- the whole
        point of this backstop poll is to catch exactly that kind of gap).
        An order only counts as settled once BOTH signals agree it is closed
        (``_order_last_status`` reached a terminal code AND the cached
        ``Order.is_closed``) -- either one alone disagreeing keeps polling,
        belt-and-suspenders against the two ever silently diverging.
        ``_ts_order_id_to_client_order_id`` entries are never removed on
        terminal status (only replaced on an order-replace), so this scans
        the full submission history each call; that dict stays small (one
        entry per order this process has ever submitted this session) so the
        scan cost is negligible next to the REST call it gates.
        """
        if getattr(self, "_pending_recovery_fills", None):
            return True
        for ts_order_id, client_order_id in self._ts_order_id_to_client_order_id.items():
            last_status = self._order_last_status.get(ts_order_id)
            terminal_statuses = _TERMINAL_ORDER_STATUSES
            if os.environ.get("TS_AMBIGUOUS_CANCEL_AUTHORITY", "0") != "1":
                terminal_statuses = terminal_statuses | {"UCN", "DON"}
            status_terminal = last_status is not None and last_status in terminal_statuses
            try:
                cached_order = self._cache.order(client_order_id)
            except Exception:  # noqa: BLE001 -- cache lookup must never crash the gate
                return True
            cache_closed = cached_order is not None and cached_order.is_closed
            if not (status_terminal and cache_closed):
                return True
        return False

    async def _interruptible_sleep(self, secs: float) -> None:
        """``await asyncio.sleep(secs)``, but returns EARLY if
        ``self._order_feed_wake`` is set during the wait (see
        ``_set_order_feed_degraded``).

        Races the REAL ``asyncio.sleep(secs)`` call against the wake event
        via ``asyncio.wait`` rather than substituting a different primitive
        (e.g. ``asyncio.wait_for(event.wait(), timeout=secs)``), for two
        reasons: (1) it still literally calls ``asyncio.sleep(secs)`` at this
        exact call site, so a test that monkeypatches
        ``nautilus_tradestation.execution.asyncio.sleep`` (the pre-existing
        cadence tests) keeps intercepting it; (2) an exception raised BY that
        sleep call (a patched fake, or a real one) is re-raised here exactly
        as ``await asyncio.sleep(secs)`` would have raised it directly --
        never swallowed by the race.
        """
        sleep_task = asyncio.ensure_future(asyncio.sleep(secs))
        wake_task = asyncio.ensure_future(self._order_feed_wake.wait())
        try:
            done, _pending = await asyncio.wait(
                {sleep_task, wake_task}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            for task in (sleep_task, wake_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleep_task, wake_task, return_exceptions=True)
        self._order_feed_wake.clear()
        if sleep_task in done:
            sleep_task.result()  # re-raise whatever asyncio.sleep(secs) raised, if anything

    async def _reconcile_order_statuses_loop(self, interval_secs: float) -> None:
        """Slow REST reconciliation behind SSE streaming (gated by
        TS_STREAM_RECONCILE_POLL_SECS).

        Backstop for events the SSE path can permanently lose: the zero-price
        FLL skip, and the mapping race where the SSE fill arrives before the
        REST submit response registers ``ts_order_id``.  Reuses
        ``_check_order_statuses``; the shared ``_order_last_status`` map keeps
        the poll from re-emitting events the stream already delivered.

        STREAM-AWARE CADENCE (2026-09-06, Finding 1 of
        the API budget review): this is a safety net
        behind the SSE stream, not a primary path, so it only needs to run at
        the fast ``interval_secs`` cadence while ``_has_pending_orders()`` is
        True. Once this client is flat/quiescent (no order awaiting a
        terminal event), the poll backs off to ``_stream_reconcile_idle_secs()``
        (floored at 300s) -- still ticking every ``interval_secs`` to notice
        promptly when a new order needs the fast cadence again, but only
        issuing the actual REST GET at the slow cadence while idle. A
        one-time random jitter (see ``_reconcile_initial_jitter_seconds``)
        delays the very first tick so a deployment-wide mass relaunch does not
        start every node's loop in lockstep.
        """
        idle_interval_secs = _stream_reconcile_idle_secs()
        jitter = _reconcile_initial_jitter_seconds(interval_secs)
        if jitter > 0:
            self._log.info(
                f"Order status reconcile poll: initial jitter {jitter:.1f}s "
                "before first tick"
            )
            try:
                # NOT wake-interruptible, deliberately: this is a ONE-TIME
                # startup delay (bounded by interval_secs, ~30s in
                # production) whose only job is desync, not reconciliation --
                # interrupting it would just consume the wake signal without
                # following it with a poll evaluation (that only happens
                # after the MAIN loop's OWN sleep below), which would make a
                # relay death during jitter WORSE, not better: the one wake
                # it got would be wasted, and the main loop's first real
                # sleep would then run uninterrupted to completion. See
                # _interruptible_sleep's docstring for the interruptible
                # steady-state loop this coordinates with.
                await asyncio.sleep(jitter)
            except asyncio.CancelledError:
                self._log.info("Order status reconcile poll stopped")
                return
        self._log.info(
            "Order status reconcile poll started "
            f"(fast={interval_secs:.0f}s while pending, idle={idle_interval_secs:.0f}s otherwise)"
        )
        last_poll_time: float | None = None
        while True:
            try:
                # Canary hardening: the sleep is INTERRUPTIBLE by
                # ``self._order_feed_wake`` (set inside
                # ``_set_order_feed_degraded``) so a degrade/recover flip is
                # noticed within, at most, this one wait -- never waiting out
                # the REST of a stale ``interval_secs`` sleep the way a plain
                # ``await asyncio.sleep(interval_secs)`` would. This STILL
                # calls the real ``asyncio.sleep(interval_secs)`` (see
                # ``_interruptible_sleep``) rather than a timeout-based
                # substitute, so cadence is byte-identical when the feed is
                # never degraded, and any CancelledError raised by a patched/
                # test ``asyncio.sleep`` still propagates exactly as before.
                await self._interruptible_sleep(interval_secs)
                # Shared order stream: an order-feed relay in a
                # degraded state (gap/dead) forces this safety net to its
                # FAST cadence tier regardless of whether this client itself
                # currently has a pending order -- the relay cannot prove
                # continuity for ANY order right now, so "nothing pending"
                # cannot be trusted the way it can be when the feed is
                # healthy. Still gated by the shared budget below; never a
                # bypass of it, only a cadence choice.
                pending = self._has_pending_orders() or self.order_feed_degraded
                now = self._loop.time()
                if _should_poll_now(
                    pending=pending,
                    last_poll_time=last_poll_time,
                    now=now,
                    fast_interval_secs=interval_secs,
                    idle_interval_secs=idle_interval_secs,
                ):
                    # REST budget, deliverable 1/2: the
                    # shared per-account ceiling. The stream-aware cadence
                    # landed 2026-09-06 lowers THIS node's rate; it cannot
                    # bound the DEPLOYMENT's, because no per-node knob knows
                    # whether 1 node or 40 share the account. This
                    # reservation does: ~35 futures nodes draw on ONE shared
                    # window, so the total backstop-poll rate on SIM_FUTURES
                    # stays under the configured ceiling regardless of node
                    # count.
                    #
                    # THE GATE IS HERE, NOT INSIDE _check_order_statuses, and
                    # that placement is load-bearing: the same method is also
                    # called (a) proactively from _cancel_order when the SSE
                    # path reports an order missing at the broker -- a real
                    # fill-detection path where a skipped call risks NT
                    # auto-generating an OrderCanceled over a live fill -- and
                    # (b) as _poll_order_fills' PRIMARY fill detection when
                    # streaming is off. Throttling either of those would trade
                    # an API-budget problem for a missed-fill problem. Only
                    # this loop -- the safety net behind an ACTIVE SSE stream
                    # -- is safe to skip, and skipping it costs a delayed
                    # backstop, never a missed fill. Fail-open: absent a host budget module,
                    # absent config, or on any error the poll proceeds
                    # exactly as before.
                    granted, reason = _budget.reserve_poll(str(self._account_id))
                    if granted:
                        await self._check_order_statuses(
                            budget_kind=_budget.poll_kind()
                        )
                        last_poll_time = now
                    else:
                        self._log.info(
                            f"Order status reconcile poll: cycle skipped -- account "
                            f"{self._account_id} is at its shared REST budget ceiling "
                            f"({reason}); the SSE stream remains the primary fill path"
                        )
            except asyncio.CancelledError:
                self._log.info("Order status reconcile poll stopped")
                break
            except Exception as e:
                self._log.error(f"Error in order status reconcile poll: {e}")

    async def _check_order_statuses(self, *, budget_kind: str | None = None) -> None:
        """Fetch current orders and emit events for any status changes.

        ``budget_kind`` labels this call in the host's per-account REST budget
        journal (REST budget). It is a PARAMETER rather
        than a constant because this method serves three callers with three
        different risk profiles -- the SSE-proactive check from
        ``_cancel_order``, ``_poll_order_fills``' primary fill detection, and
        ``_reconcile_order_statuses_loop``' safety-net backstop -- and only
        the last is throttleable. A journal that could not tell them apart
        would make the report say "polls" when it was really watching a fill
        path.

        Status flow:
          ACK / OPN → order is open
          FLL       → fully filled   → generate_order_filled
          CAN / UCN / OUT / EXP / DON → canceled/expired → generate_order_canceled
          REJ / BRO / LAT → rejected → generate_order_rejected

        Second-canary hardening: the 2026-09-10 second staged canary found
        ZERO REST calls journaled in the 2+ minutes after a real relay kill
        (9 calls total since startup, none after) even though the reconcile
        loop's wake/cadence fix (Canary) was confirmed firing --
        the DEGRADED stdout line escalated correctly the whole time. Root
        cause: this early return skips the REST call ENTIRELY whenever
        nothing is currently tracked (a flat host with no open order this
        session -- exactly an idle node's overnight state), so the
        reconcile loop's cadence engaging correctly still produced no
        journal row to observe. While the feed is degraded, "nothing to
        reconcile" is not provable from silence -- a REST call is the only
        way to confirm the account is genuinely flat rather than the relay
        having also hidden the fact that something WAS submitted (a
        cross-process race: another host on this account, or this host's
        own in-flight submit whose REST response hasn't registered
        ts_order_id yet). So this early return now applies ONLY while the
        feed is healthy; a degraded feed always makes the REST call (still
        gated by api_budget.reserve_poll below via the caller), giving the
        api-budget journal (and hence an operator) real, observable proof
        the safety net is alive even when there is nothing to act on.
        """
        if not self._ts_order_id_to_client_order_id and not self.order_feed_degraded:
            return  # Nothing tracked, feed healthy: nothing to check

        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
                budget_kind=budget_kind,
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

            if self._handle_ambiguous_terminal_update(ts_order, source="status_poll"):
                continue
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
                self._publish_raw_cancel_evidence(
                    ts_order,
                    cached_order,
                    source="status_poll",
                    ts_event=ts_now,
                )
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

    def _set_order_feed_degraded(self, value: bool, *, reason: str = "") -> None:
        """Flip ``order_feed_degraded`` and WAKE the reconcile loop immediately.

        Canary hardening: the 2026-09-09 canary's staged relay-kill
        found the reconcile loop's fast cadence never actually engaging
        during a 7-minute outage (3 REST calls total) because
        ``_reconcile_order_statuses_loop`` only re-reads this flag once per
        ``interval_secs`` sleep -- a flip landing mid-sleep waited out the
        REST of that sleep unnoticed. Setting ``self._order_feed_wake`` here
        interrupts that sleep the instant the flag changes (see
        ``_reconcile_order_statuses_loop``), bounding the wake latency to
        well under ``interval_secs`` regardless of timing.

        ``reason`` is cosmetic (only used in the stdout mirror line, so an
        operator tailing raw stdout sees WHY without needing full log
        context -- see the Canary request for stdout mirroring).
        """
        self.order_feed_degraded = value
        self._order_feed_wake.set()
        tag = "DEGRADED" if value else "RECOVERED"
        # Mirrored to stdout at WARNING regardless of the logging framework's
        # own routing/level filtering -- an operator tailing the node's raw
        # captured stdout must see this even if nautilus's internal log sink
        # does not surface it there.
        print(
            f"[ORDER-FEED-WARNING] account={self._account_id} order feed {tag}"
            + (f": {reason}" if reason else ""),
            flush=True,
        )

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
        from nautilus_tradestation.feed.tail_client import (
            FeedGapError,
            FeedHandlerDeadError,
        )

        retry_delay = 1.0
        while True:
            try:
                async for event in self._stream_client.stream_orders(self._account_id):
                    retry_delay = 1.0  # healthy event resets the backoff
                    if self.order_feed_degraded:
                        # Recovery: a healthy event proves the relay is caught
                        # up again. Never inferred from silence -- only a real
                        # event clears the flag.
                        self._set_order_feed_degraded(
                            False, reason="relay resumed delivering events"
                        )
                        self._log.info(
                            "Order feed: relay recovered, resuming normal reconcile "
                            "cadence",
                            LogColor.GREEN,
                        )
                        _report_order_feed_state(
                            str(self._account_id),
                            proxy=self._order_feed_proxy,
                            degraded=False,
                        )
                    try:
                        await self._process_order_event(event)
                    except Exception as e:
                        self._log.error(f"Error processing streamed order event: {e}")
            except asyncio.CancelledError:
                self._log.info("Order fill SSE stream stopped")
                return
            except (FeedGapError, FeedHandlerDeadError) as exc:
                # Shared order stream: the relay lost or cannot prove
                # continuity of this account's order events. NEVER assume
                # flat -- log loudly, flip the degraded flag the host reads,
                # force the REST reconcile poll to its fast/degraded cadence
                # (still gated by the shared API budget), and re-enter the
                # stream exactly like any other drop (Nautilus reconciliation,
                # reconciliation_lookback_mins=1440, covers the gap on
                # reconnect; this flag exists so a human ALSO sees it live).
                self._log.error(
                    f"ORDER FEED DEGRADED for account {self._account_id}: {exc!r}; "
                    f"forcing fast reconcile-poll cadence and resubscribing in "
                    f"{retry_delay:.0f}s -- NOT assuming flat",
                )
                self._set_order_feed_degraded(True, reason=repr(exc))
                _report_order_feed_state(
                    str(self._account_id), proxy=self._order_feed_proxy, degraded=True
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2.0, 60.0)
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

        if self._handle_ambiguous_terminal_update(ts_order, source="order_stream"):
            return
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
            self._publish_raw_cancel_evidence(
                ts_order,
                cached_order,
                source="order_stream",
                ts_event=ts_now,
            )
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
                # AUTHORITY ORDER (a boxed-position bug): the
                # submitting strategy KNOWS its intent and tags the order
                # (TS_INTENT:close_short / close_long / open_short / open_long).
                # The cache inference
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
        not silently dropped (C1-PARSE — a dropped report can
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
