"""
TradeStation SSE streaming client.

TradeStation uses HTTP Server-Sent Events (SSE) rather than WebSocket for
real-time data. Each streaming endpoint returns a long-lived HTTP GET whose
body is a sequence of newline-delimited JSON objects.

Endpoints
---------
- GET /v3/marketdata/stream/quotes/{symbols}       → real-time quotes
- GET /v3/marketdata/stream/barcharts/{symbol}     → real-time bars
- GET /v3/brokerage/stream/accounts/{account}/orders → order status changes
- GET /v3/marketdata/stream/marketdepth/{symbol}   → level-2 order book

Usage
-----
::

    stream_client = TradeStationStreamClient(
        access_token_provider=lambda: http_client.access_token,
        base_url="https://sim-api.tradestation.com/v3",
        reconnect_delay_secs=5.0,
    )

    async for event in stream_client.stream_quotes("GCJ26"):
        print(event)  # dict with Bid, Ask, Last, etc.

    async for event in stream_client.stream_orders("SIM0000001F"):
        print(event)  # dict with OrderID, Status, etc.

"""

import asyncio
import json
import logging
import os
import time
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from typing import Callable

import httpx

_log = logging.getLogger(__name__)

# Heartbeat lines that TradeStation sends to keep the connection alive.
_HEARTBEAT_KEYS = {"Heartbeat", "heartbeat"}

# Stream idle deadline (seconds). An open stream that delivers NO line at all --
# not even a heartbeat -- for this long is treated as dead and reconnected.
# TradeStation sends a ``{"Heartbeat": n, "Timestamp": ...}`` line every ~5 s on
# quiet bar, quote and order streams (measured against the simulation API),
# so 30 s is ~6 missed heartbeats. Before this deadline existed the reader used
# ``timeout=None``: a connection that went quiet without closing hung forever
# inside ``aiter_lines()`` and the reconnect loop below never ran.
_STREAM_IDLE_TIMEOUT_ENV = "TS_STREAM_IDLE_TIMEOUT_SECS"
_DEFAULT_STREAM_IDLE_TIMEOUT_SECS = 30.0
# Connect / write / pool budgets for opening a stream (never the read side).
_STREAM_CONNECT_TIMEOUT_SECS = 30.0


class StreamIdleTimeout(Exception):
    """An open SSE stream delivered no line (data or heartbeat) within the deadline."""


def _stream_idle_timeout_secs() -> float:
    """Resolve the stream idle deadline from ``TS_STREAM_IDLE_TIMEOUT_SECS``.

    Fail-closed: an unset, unparsable, non-finite or non-positive value can never
    disable the deadline -- it falls back to the default.
    """
    raw = os.environ.get(_STREAM_IDLE_TIMEOUT_ENV, "").strip()
    if not raw:
        return _DEFAULT_STREAM_IDLE_TIMEOUT_SECS
    try:
        value = float(raw)
    except ValueError:
        value = float("nan")
    if not (0.0 < value < float("inf")):
        _log.warning(
            f"{_STREAM_IDLE_TIMEOUT_ENV}={raw!r} is not a positive number of seconds; "
            f"using the default {_DEFAULT_STREAM_IDLE_TIMEOUT_SECS:.0f}s"
        )
        return _DEFAULT_STREAM_IDLE_TIMEOUT_SECS
    return value


class TradeStationStreamClient:
    """
    Async SSE client for TradeStation streaming endpoints.

    Wraps httpx's async streaming API. Parses newline-delimited JSON events,
    silently drops heartbeats, and reconnects after errors with exponential
    back-off (capped at ``reconnect_delay_secs * 8``). A stream that goes
    silent -- no line at all, heartbeats included, for the idle deadline --
    is treated as an error and reconnected the same way.

    Parameters
    ----------
    access_token_provider : Callable[[], str | None]
        Zero-argument callable that returns the current OAuth access token.
        Called on each (re)connection so tokens refresh transparently.
    access_token_refresher : Callable[[bool], Awaitable[str | None]], optional
        Async callable used to obtain a valid token before opening an SSE
        connection. When supplied, 401 responses force-refresh through this
        callback before reconnecting.
    base_url : str
        TradeStation API base URL (sandbox or production).
    reconnect_delay_secs : float, default 5.0
        Initial delay before reconnecting after a stream error. Doubles on
        each successive failure, capped at 8× the initial value.
    stream_idle_timeout_secs : float, optional
        Idle deadline for an open stream: if no line (data or heartbeat)
        arrives for this many seconds the connection is dropped and
        reconnected. Default: ``TS_STREAM_IDLE_TIMEOUT_SECS`` when it is a
        positive number, else 30 s. It cannot be disabled.

    """

    def __init__(
        self,
        access_token_provider: Callable[[], str | None],
        base_url: str,
        reconnect_delay_secs: float = 5.0,
        access_token_refresher: Callable[[bool], Awaitable[str | None]] | None = None,
        stream_idle_timeout_secs: float | None = None,
    ) -> None:
        self._token_provider = access_token_provider
        self._token_refresher = access_token_refresher
        self._base_url = base_url.rstrip("/")
        self._reconnect_delay = reconnect_delay_secs
        self._max_delay = reconnect_delay_secs * 8
        if stream_idle_timeout_secs is None or not (
            0.0 < stream_idle_timeout_secs < float("inf")
        ):
            stream_idle_timeout_secs = _stream_idle_timeout_secs()
        self._idle_timeout = float(stream_idle_timeout_secs)

    def _headers(self) -> dict[str, str]:
        token = self._token_provider()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.tradestation.streams.v2+json",
        }

    async def _headers_async(self, *, force_refresh: bool = False) -> dict[str, str]:
        if self._token_refresher is not None:
            token = await self._token_refresher(force_refresh)
        else:
            token = self._token_provider()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.tradestation.streams.v2+json",
        }

    async def _stream(
        self, url: str, params: dict[str, str] | None = None,
    ) -> AsyncIterator[dict]:
        """Core SSE reader — yields parsed JSON dicts, reconnects on error."""
        delay = self._reconnect_delay
        idle = self._idle_timeout
        # read= bounds every socket read (including the wait for response
        # headers); the per-line deadline below bounds the line iterator itself.
        timeout = httpx.Timeout(
            connect=_STREAM_CONNECT_TIMEOUT_SECS,
            read=idle,
            write=_STREAM_CONNECT_TIMEOUT_SECS,
            pool=_STREAM_CONNECT_TIMEOUT_SECS,
        )
        while True:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    headers = await self._headers_async()
                    async with client.stream(
                        "GET", url, headers=headers, params=params,
                    ) as resp:
                        if resp.status_code == 401:
                            body = await resp.aread()
                            _log.warning(
                                f"SSE stream {url} returned 401; "
                                "forcing token refresh before reconnect. "
                                f"Body: {body.decode(errors='replace')[:200]}"
                            )
                            if self._token_refresher is not None:
                                try:
                                    await self._headers_async(force_refresh=True)
                                    delay = self._reconnect_delay
                                    continue
                                except Exception as e:
                                    _log.error(
                                        f"SSE token refresh failed ({url}): {e} "
                                        f"— reconnecting in {delay:.0f}s"
                                    )
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, self._max_delay)
                            continue
                        if resp.status_code != 200:
                            body = await resp.aread()
                            _log.error(
                                f"SSE stream {url} returned {resp.status_code}: "
                                f"{body.decode(errors='replace')[:200]}"
                            )
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, self._max_delay)
                            continue

                        _log.info(f"SSE stream connected: {url}")
                        delay = self._reconnect_delay  # reset on successful connect

                        lines = resp.aiter_lines().__aiter__()
                        while True:
                            # Idle deadline: ANY line (heartbeat included)
                            # re-arms it; silence raises StreamIdleTimeout.
                            try:
                                async with asyncio.timeout(idle):
                                    line = await lines.__anext__()
                            except StopAsyncIteration:
                                break
                            except TimeoutError:
                                raise StreamIdleTimeout(
                                    f"no data or heartbeat for {idle:g}s"
                                ) from None
                            received_ns = time.time_ns()
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                event = json.loads(line)
                            except json.JSONDecodeError:
                                _log.debug(f"Non-JSON SSE line (skipped): {line[:120]}")
                                continue

                            # Drop heartbeats silently
                            if any(k in event for k in _HEARTBEAT_KEYS):
                                continue

                            event["_ts_sse_received_ns"] = received_ns
                            yield event

            except asyncio.CancelledError:
                _log.info(f"SSE stream cancelled: {url}")
                return
            except (StreamIdleTimeout, httpx.ReadTimeout) as e:
                _log.warning(
                    f"SSE stream idle timeout ({url}): {str(e) or type(e).__name__} "
                    f"(deadline {idle:g}s) — reconnecting in {delay:.0f}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_delay)
            except Exception as e:
                _log.error(
                    f"SSE stream error ({url}): {e} — reconnecting in {delay:.0f}s"
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_delay)

    async def stream_quotes(self, symbols: str) -> AsyncIterator[dict]:
        """
        Stream real-time quotes for one or more symbols.

        Parameters
        ----------
        symbols : str
            Symbol or comma-separated list (e.g. ``"GCJ26"`` or ``"GCJ26,ESM26"``).

        Yields
        ------
        dict
            Quote event with keys: Symbol, Bid, Ask, BidSize, AskSize,
            Last, LastSize, Volume, TradeTime, etc.

        """
        url = f"{self._base_url}/marketdata/stream/quotes/{symbols}"
        async for event in self._stream(url):
            yield event

    async def stream_bars(
        self,
        symbol: str,
        interval: str,
        unit: str,
        session_template: str | None = None,
    ) -> AsyncIterator[dict]:
        """
        Stream real-time bar updates for a symbol.

        Parameters
        ----------
        symbol : str
            The contract symbol (e.g. ``"GCJ26"``).
        interval : str
            Bar interval string (e.g. ``"1"``, ``"5"``, ``"15"``).
        unit : str
            Bar unit: ``"Minute"`` or ``"Daily"``.
        session_template : str, optional
            TS session template. Use ``"USEQPreAndPost"`` for extended hours
            equity bars. Default ``None`` uses TS default (RTH only for equities).

        Yields
        ------
        dict
            Bar event with keys: Open, High, Low, Close, TotalVolume,
            TimeStamp, Status (``"Historical"`` or ``"RealTime"``).

        """
        url = f"{self._base_url}/marketdata/stream/barcharts/{symbol}"
        params: dict[str, str] = {"interval": interval, "unit": unit, "barsback": "1"}
        if session_template:
            params["sessiontemplate"] = session_template
        async for event in self._stream(url, params=params):
            # Pass all events through — including Historical seed bars.
            # The caller (_stream_bars) uses them for reconnection gap recovery.
            yield event

    async def stream_orders(self, account_id: str) -> AsyncIterator[dict]:
        """
        Stream real-time order status changes for an account.

        Parameters
        ----------
        account_id : str
            The TradeStation account ID (e.g. ``"SIM0000001F"``).

        Yields
        ------
        dict
            Order event with keys: OrderID, Status, FilledQuantity,
            AveragePrice, TradeAction, etc.

        """
        url = f"{self._base_url}/brokerage/stream/accounts/{account_id}/orders"
        async for event in self._stream(url):
            yield event

    async def stream_market_depth(self, symbol: str) -> AsyncIterator[dict]:
        """
        Stream level-2 market depth (order book) for a symbol.

        Parameters
        ----------
        symbol : str
            The contract symbol (e.g. ``"GCJ26"``).

        Yields
        ------
        dict
            Market depth event with keys: Side (``"Bid"``/``"Ask"``),
            Price, Size, OrderCount, etc.

        """
        url = f"{self._base_url}/marketdata/stream/marketdepth/{symbol}"
        async for event in self._stream(url):
            yield event
