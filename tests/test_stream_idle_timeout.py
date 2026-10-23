"""
Stream idle deadline: a TradeStation SSE connection that goes silent without
closing must be dropped and reconnected, not hang forever.

Root cause pinned here: bar streams went silent while the sockets stayed open;
``_stream`` used ``httpx.AsyncClient(timeout=None)`` and silently dropped
heartbeats without timing them, so the reader sat inside ``aiter_lines()``
forever and the reconnect loop never ran.

These tests drive the REAL ``TradeStationStreamClient._stream`` (the code every
bar, quote and order stream and the feed handler run through) over a fake
in-process HTTP transport -- no network.
"""

import asyncio
import json
import logging
import time

import httpx
import pytest

from nautilus_tradestation.streaming import client as stream_mod
from nautilus_tradestation.streaming.client import (
    TradeStationStreamClient,
    _DEFAULT_STREAM_IDLE_TIMEOUT_SECS,
    _STREAM_IDLE_TIMEOUT_ENV,
)

IDLE = 0.3
RECONNECT = 0.05


class _ScriptedStream(httpx.AsyncByteStream):
    """Response body that plays a script of ("line", text) / ("sleep", secs) /
    ("hang", None) steps. "hang" never returns (a silent, still-open socket)."""

    def __init__(self, script):
        self._script = script

    async def __aiter__(self):
        for kind, arg in self._script:
            if kind == "line":
                yield (arg + "\n").encode()
            elif kind == "sleep":
                await asyncio.sleep(arg)
            elif kind == "hang":
                await asyncio.Event().wait()

    async def aclose(self):
        return None


class _FakeServer:
    """One script per connection attempt; records every connection."""

    def __init__(self, scripts):
        self._scripts = list(scripts)
        self.connections: list[float] = []
        self.client_timeouts: list[httpx.Timeout] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.connections.append(time.monotonic())
        idx = len(self.connections) - 1
        script = self._scripts[idx] if idx < len(self._scripts) else [("hang", None)]
        if script == "read_timeout":
            raise httpx.ReadTimeout("simulated: no response headers", request=request)
        if script == "read_timeout_blank":
            raise httpx.ReadTimeout("", request=request)
        return httpx.Response(200, stream=_ScriptedStream(script))


@pytest.fixture
def fake_server(monkeypatch):
    holder = {}
    real_async_client = httpx.AsyncClient

    def install(scripts):
        server = _FakeServer(scripts)
        transport = httpx.MockTransport(server.handler)

        def factory(*args, **kwargs):
            server.client_timeouts.append(kwargs.get("timeout"))
            return real_async_client(*args, transport=transport, **kwargs)

        monkeypatch.setattr(stream_mod.httpx, "AsyncClient", factory)
        holder["server"] = server
        return server

    return install


def _client(idle=IDLE):
    return TradeStationStreamClient(
        access_token_provider=lambda: "tok",
        base_url="https://mock.tradestation.com/v3",
        reconnect_delay_secs=RECONNECT,
        stream_idle_timeout_secs=idle,
    )


def _hb(n):
    return json.dumps({"Heartbeat": n, "Timestamp": "2025-01-02T15:00:00Z"})


def _bar(ts, close):
    return json.dumps({"TimeStamp": ts, "Close": str(close), "Status": "RealTime"})


async def _collect(agen, n, timeout):
    out = []

    async def run():
        async for ev in agen:
            out.append((time.monotonic(), ev))
            if len(out) >= n:
                break

    await asyncio.wait_for(run(), timeout=timeout)
    return out


# (a) silence mid-stream -> reconnect within idle deadline + backoff ----------------
async def test_silent_stream_reconnects_within_deadline(fake_server, caplog):
    server = fake_server([
        [("line", _bar("2025-01-02T15:00:00Z", 1.0)), ("hang", None)],
        [("line", _bar("2025-01-02T15:01:00Z", 2.0)), ("hang", None)],
    ])
    caplog.set_level(logging.WARNING, logger=stream_mod.__name__)
    got = await _collect(_client().stream_bars("ESZ26", "1", "Minute"), 2, timeout=5.0)

    assert [e["Close"] for _, e in got] == ["1.0", "2.0"]
    assert len(server.connections) == 2
    gap = got[1][0] - got[0][0]
    assert IDLE <= gap < IDLE + RECONNECT + 1.0
    assert any(
        r.levelno == logging.WARNING and "idle timeout" in r.getMessage()
        for r in caplog.records
    )


async def test_silent_order_stream_also_reconnects(fake_server):
    """The order stream rides the same reader -- it must be covered too."""
    server = fake_server([
        [("hang", None)],
        [("line", json.dumps({"OrderID": "1", "Status": "FLL"})), ("hang", None)],
    ])
    got = await _collect(_client().stream_orders("ACCOUNT1"), 1, timeout=5.0)
    assert got[0][1]["OrderID"] == "1"
    assert len(server.connections) == 2


async def test_no_response_headers_read_timeout_reconnects(fake_server, caplog):
    server = fake_server([
        "read_timeout",
        [("line", _bar("2025-01-02T15:00:00Z", 1.0)), ("hang", None)],
    ])
    caplog.set_level(logging.WARNING, logger=stream_mod.__name__)
    got = await _collect(_client().stream_quotes("ESZ26"), 1, timeout=5.0)
    assert got[0][1]["Close"] == "1.0"
    assert len(server.connections) == 2
    msgs = [r.getMessage() for r in caplog.records if "idle timeout" in r.getMessage()]
    assert "no response headers" in msgs[0]  # the cause is never logged blank


async def test_blank_read_timeout_is_named_in_the_warning(fake_server, caplog):
    fake_server([
        "read_timeout_blank",
        [("line", _bar("2025-01-02T15:00:00Z", 1.0)), ("hang", None)],
    ])
    caplog.set_level(logging.WARNING, logger=stream_mod.__name__)
    await _collect(_client().stream_quotes("ESZ26"), 1, timeout=5.0)
    msgs = [r.getMessage() for r in caplog.records if "idle timeout" in r.getMessage()]
    assert msgs and "ReadTimeout" in msgs[0]


# (b) heartbeats alone keep the stream alive -- no reconnect ------------------------
async def test_heartbeats_only_stream_does_not_reconnect(fake_server, caplog):
    beats = []
    for i in range(12):  # 12 x 0.1 s = 1.2 s of heartbeats, 4x the idle deadline
        beats += [("line", _hb(i)), ("sleep", 0.1)]
    server = fake_server([
        beats + [("line", _bar("2025-01-02T15:00:00Z", 7.0)), ("hang", None)],
    ])
    caplog.set_level(logging.WARNING, logger=stream_mod.__name__)
    got = await _collect(_client().stream_bars("ESZ26", "1", "Minute"), 1, timeout=5.0)

    assert got[0][1]["Close"] == "7.0"
    assert len(server.connections) == 1
    assert not any("idle timeout" in r.getMessage() for r in caplog.records)


# (c) normal events unchanged ------------------------------------------------------
async def test_normal_events_unchanged(fake_server):
    events = [
        {"TimeStamp": "2025-01-02T15:00:00Z", "Close": "1", "Status": "Historical"},
        {"TimeStamp": "2025-01-02T15:01:00Z", "Close": "2", "Status": "RealTime"},
        {"TimeStamp": "2025-01-02T15:02:00Z", "Close": "3", "Status": "RealTime"},
    ]
    server = fake_server([
        [
            ("line", json.dumps(events[0])),
            ("line", _hb(1)),
            ("line", ""),
            ("line", "not json"),
            ("line", json.dumps({"heartbeat": 2})),
            ("line", json.dumps(events[1])),
            ("line", json.dumps(events[2])),
            ("hang", None),
        ],
    ])
    got = [e for _, e in await _collect(
        _client().stream_bars("ESZ26", "1", "Minute"), 3, timeout=5.0)]

    assert len(server.connections) == 1
    for ev, want in zip(got, events):
        assert isinstance(ev.pop("_ts_sse_received_ns"), int)
        assert ev == want


async def test_clean_server_close_reconnects_immediately_as_before(fake_server, caplog):
    server = fake_server([
        [("line", _bar("2025-01-02T15:00:00Z", 1.0))],
        [("line", _bar("2025-01-02T15:01:00Z", 2.0)), ("hang", None)],
    ])
    caplog.set_level(logging.WARNING, logger=stream_mod.__name__)
    got = await _collect(_client().stream_quotes("ESZ26"), 2, timeout=5.0)
    assert len(server.connections) == 2
    assert got[1][0] - got[0][0] < IDLE
    assert not any("idle timeout" in r.getMessage() for r in caplog.records)


async def test_slow_consumer_does_not_trip_the_deadline(fake_server, caplog):
    """The deadline times the socket, not the caller's processing time."""
    server = fake_server([
        [("line", _bar("t1", 1.0)), ("line", _bar("t2", 2.0)), ("hang", None)],
    ])
    caplog.set_level(logging.WARNING, logger=stream_mod.__name__)
    seen = []
    agen = _client().stream_bars("ESZ26", "1", "Minute")

    async def run():
        async for ev in agen:
            seen.append(ev)
            if len(seen) == 2:
                break
            await asyncio.sleep(IDLE * 2)  # consumer busy longer than the deadline

    await asyncio.wait_for(run(), timeout=5.0)
    assert len(server.connections) == 1
    assert not any("idle timeout" in r.getMessage() for r in caplog.records)


async def test_cancel_during_silence_still_stops_cleanly(fake_server):
    """Unchanged contract: a cancel while waiting on the socket ends the stream
    (the reader returns), it is never mistaken for an idle timeout."""
    server = fake_server([[("hang", None)]])
    agen = _client(idle=60.0).stream_bars("ESZ26", "1", "Minute")

    async def run():
        async for _ in agen:
            pass

    task = asyncio.create_task(run())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.CancelledError:
        pass
    assert task.done()
    assert len(server.connections) == 1


# Regression guards: the deadline is always on and reaches httpx ----------------------
async def test_http_client_is_never_built_without_a_read_timeout(fake_server):
    server = fake_server([[("line", _bar("t1", 1.0)), ("hang", None)]])
    await _collect(_client(idle=12.5).stream_bars("ESZ26", "1", "Minute"), 1, timeout=5.0)
    t = server.client_timeouts[0]
    assert isinstance(t, httpx.Timeout)
    assert t.read == 12.5
    assert t.connect is not None


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
        ("", _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
        ("45", 45.0),
        ("0", _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
        ("-5", _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
        ("abc", _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
        ("inf", _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
        ("nan", _DEFAULT_STREAM_IDLE_TIMEOUT_SECS),
    ],
)
def test_env_override_cannot_disable_the_deadline(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv(_STREAM_IDLE_TIMEOUT_ENV, raising=False)
    else:
        monkeypatch.setenv(_STREAM_IDLE_TIMEOUT_ENV, raw)
    c = TradeStationStreamClient(
        access_token_provider=lambda: "tok", base_url="https://x/v3",
    )
    assert c._idle_timeout == expected


def test_explicit_argument_wins_over_env(monkeypatch):
    monkeypatch.setenv(_STREAM_IDLE_TIMEOUT_ENV, "45")
    assert _client(idle=9.0)._idle_timeout == 9.0


def test_default_deadline_is_well_above_measured_heartbeat():
    # TradeStation heartbeat measured at ~5 s on bar/quote/order streams.
    # The default must tolerate several missed beats.
    assert _DEFAULT_STREAM_IDLE_TIMEOUT_SECS >= 3 * 5.0
