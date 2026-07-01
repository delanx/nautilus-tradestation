"""
G1 differential (LOAD-BEARING): a recorded upstream event sequence must
produce IDENTICAL emitted Bar lists whether fed

(a) directly into the data client's REAL bar state machine
    (_handle_bar_stream_event), or
(b) through the feed transport — the handler's close-detection +
    MirrorWriter -> FeedTailStreamClient -> the SAME unchanged state machine.

The sequence exercises all three Historical-seed branches (same-ts
correction, stale-ignore, gap-fill), a mid-sequence consumer reconnect
(resume replay), the late-subscriber fresh start (manifest seed +
tail-from-end), the STALE-START class (idle/pre-open subscribe, handler
restart, FeedGapError fresh start on an initialized machine — the proxy must
never emit a prior bar a direct node would not), and ts_init precedence
(_ts_bar_emit_ns stamped at the consumer wins over the handler's
_ts_sse_received_ns — I6).
"""
import asyncio
import time

import pytest

from nautilus_tradestation.data import TradeStationDataClient
from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.handler import FeedHandler
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.feed.tail_client import FeedGapError, FeedTailStreamClient
from nautilus_trader.model.data import BarSpecification, BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from tests.test_kit import TSTestInstrumentStubs

KEY = stream_key("ESM26", "15", "Minute", None)

T0 = "2026-06-10T13:45:00Z"  # completed bar before connect (the connect seed)
TA = "2026-06-10T14:00:00Z"
TB = "2026-06-10T14:15:00Z"
TC = "2026-06-10T14:30:00Z"
TD = "2026-06-10T14:45:00Z"
TE = "2026-06-10T15:00:00Z"
TF = "2026-06-10T15:15:00Z"


def _ev(ts, open_, high, low, close, volume, status="RealTime"):
    return {
        "TimeStamp": ts,
        "Open": open_,
        "High": high,
        "Low": low,
        "Close": close,
        "TotalVolume": volume,
        "Status": status,
        # Handler receipt stamp (streaming client); must NOT win ts_init.
        "_ts_sse_received_ns": 12345,
    }


# Recorded upstream sequence covering every state-machine branch.
SEQUENCE = [
    _ev(T0, "4110.00", "4118.25", "4108.50", "4117.75", "9100", status="Historical"),
    _ev(TA, "4118.00", "4120.50", "4117.50", "4120.25", "300"),
    _ev(TA, "4118.00", "4121.75", "4117.50", "4121.50", "640"),
    # Upstream reconnect within bar A: same-ts correction (data.py:382-387).
    _ev(TA, "4118.00", "4121.75", "4117.25", "4121.75", "655", status="Historical"),
    _ev(TB, "4121.75", "4122.25", "4121.00", "4122.00", "120"),  # closes A (corrected)
    _ev(TB, "4121.75", "4123.50", "4121.00", "4123.25", "480"),
    # Brief reconnect within bar B: stale seed, ignored (data.py:388-394).
    _ev(TA, "4118.00", "4121.75", "4117.25", "4121.75", "655", status="Historical"),
    _ev(TB, "4121.75", "4123.75", "4121.00", "4123.50", "510"),
    # Outage spanning B's close: newer seed -> gap-fill emits B + C (data.py:395-416).
    _ev(TC, "4123.50", "4125.00", "4122.75", "4124.00", "880", status="Historical"),
    _ev(TD, "4124.25", "4125.25", "4124.00", "4125.00", "210"),
    _ev(TD, "4124.25", "4126.00", "4124.00", "4125.75", "560"),
    _ev(TE, "4125.75", "4126.25", "4125.50", "4126.00", "90"),  # closes D
]

EXPECTED_CLOSES = ["4121.75", "4123.50", "4124.00", "4125.75"]


class _StubLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


class ConsumerHarness:
    """Runs the data client's REAL bar state machine + emit path.

    The methods are the genuine TradeStationDataClient functions (not copies);
    only _handle_data is captured. ``st`` persists across (re)subscribes,
    exactly like the supervise loop in _stream_bars.
    """

    _handle_bar_stream_event = TradeStationDataClient._handle_bar_stream_event
    _parse_bars = TradeStationDataClient._parse_bars
    _mark_bar_emit = staticmethod(TradeStationDataClient._mark_bar_emit)

    def __init__(self):
        instrument = TSTestInstrumentStubs.es_futures_contract()
        self.instrument = instrument
        self.bar_type = BarType(
            instrument_id=instrument.id,
            bar_spec=BarSpecification(
                step=15, aggregation=BarAggregation.MINUTE, price_type=PriceType.LAST
            ),
            aggregation_source=AggregationSource.EXTERNAL,
        )
        self._log = _StubLogger()
        self.st: dict = {"buffered_event": None, "buffered_ts": "", "initialized": False}
        self.emitted: list = []

    def _handle_data(self, bar):
        self.emitted.append(bar)

    def feed(self, event):
        self._handle_bar_stream_event(event, self.bar_type, self.instrument, self.st)


def run_direct(events) -> ConsumerHarness:
    harness = ConsumerHarness()
    for ev in events:
        harness.feed(ev)
    return harness


def make_mirror(tmp_path):
    """A FeedHandler driven inline (no loops): the REAL ingest close-detection
    writes the mirror exactly as the running handler would."""
    handler = FeedHandler("SIMTEST", tmp_path, None)
    feed_dir = handler.account_dir
    feed_dir.mkdir(parents=True, exist_ok=True)
    (feed_dir / protocol.HEARTBEAT_NAME).touch()
    st = handler._start_ingest(KEY, "ESM26", "15", "Minute", None)
    return handler, st, feed_dir


def ingest(handler, st, ev):
    seq, segment, offset = st.writer.append(ev)
    handler._update_close_detection(st, ev, seq, segment, offset)


async def collect(agen, n, timeout=5.0):
    out = []

    async def _run():
        async for ev in agen:
            out.append(ev)
            if len(out) >= n:
                return

    await asyncio.wait_for(_run(), timeout)
    return out


def assert_bars_identical(direct, proxy, t0_ns):
    assert [b.ts_event for b in proxy] == [b.ts_event for b in direct]
    for d, p in zip(direct, proxy):
        assert (str(p.open), str(p.high), str(p.low), str(p.close)) == (
            str(d.open),
            str(d.high),
            str(d.low),
            str(d.close),
        )
        assert p.volume == d.volume
    for bar in list(direct) + list(proxy):
        # I6: ts_init is the consumer's emit stamp (_ts_bar_emit_ns), never the
        # handler's _ts_sse_received_ns (12345) or ts_event.
        assert bar.ts_init >= t0_ns


class TestG1Differential:
    async def test_full_sequence_direct_vs_proxy(self, tmp_path, monkeypatch):
        """(a) vs (b) over the full recorded sequence: identical Bars."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        t0_ns = time.time_ns()
        direct = run_direct(SEQUENCE)
        assert [str(b.close) for b in direct.emitted] == EXPECTED_CLOSES  # not degenerate

        handler, st, feed_dir = make_mirror(tmp_path)
        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, len(SEQUENCE)))
        await asyncio.sleep(0.05)  # consumer takes its start on the EMPTY mirror
        for ev in SEQUENCE:
            ingest(handler, st, ev)
        got = await task
        await gen.aclose()

        assert got == SEQUENCE  # events cross the transport VERBATIM (I2)
        proxy = ConsumerHarness()
        for ev in got:
            proxy.feed(ev)
        assert_bars_identical(direct.emitted, proxy.emitted, t0_ns)

    async def test_consumer_reconnect_resume_no_missing_closes(self, tmp_path, monkeypatch):
        """Mid-sequence consumer drop + resume replay: zero missing closes."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        t0_ns = time.time_ns()
        direct = run_direct(SEQUENCE)

        handler, st, feed_dir = make_mirror(tmp_path)
        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        proxy = ConsumerHarness()

        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 5))
        await asyncio.sleep(0.05)
        for ev in SEQUENCE[:5]:
            ingest(handler, st, ev)
        first = await task
        await gen.aclose()  # consumer dies mid-stream; client cursor survives

        for ev in SEQUENCE[5:]:  # the rest arrives while disconnected
            ingest(handler, st, ev)

        # Re-entry (same client instance, like the supervise loop): RESUME
        # replays everything missed, exactly once, in order.
        gen2 = client.stream_bars("ESM26", "15", "Minute")
        rest = await collect(gen2, len(SEQUENCE) - 5)
        await gen2.aclose()

        assert first + rest == SEQUENCE
        for ev in first + rest:
            proxy.feed(ev)
        assert_bars_identical(direct.emitted, proxy.emitted, t0_ns)

    async def test_late_subscriber_seed_parity(self, tmp_path, monkeypatch):
        """A consumer connecting mid-session: the manifest seed + ONLY the events
        appended after attach drive the state machine exactly like a
        direct-mode connect (bar E's PRE-subscribe update is not replayed —
        a direct connect would not have it either)."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        t0_ns = time.time_ns()
        handler, st, feed_dir = make_mirror(tmp_path)
        for ev in SEQUENCE:
            ingest(handler, st, ev)

        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 3))
        await asyncio.sleep(0.05)  # seed yielded; consumer attached at the tail
        live_e = _ev(TE, "4125.75", "4126.75", "4125.50", "4126.25", "180")  # bar E update
        live_f = _ev(TF, "4126.25", "4126.75", "4126.00", "4126.50", "150")  # closes E
        ingest(handler, st, live_e)
        ingest(handler, st, live_f)
        got = await task
        await gen.aclose()

        # The synthesized seed is bar D's FINAL update, re-stamped Historical
        # (what TS barsback=1 would send a fresh direct connection).
        seed = got[0]
        assert seed["Status"] == "Historical"
        assert seed["_ts_feed_seed"] is True
        assert seed["TimeStamp"] == TD
        assert seed["Close"] == "4125.75"
        assert got[1:] == [live_e, live_f]  # bar E's pre-subscribe update NOT replayed

        # Direct equivalent: the same connect-time seed, then the same events.
        direct = run_direct([dict(seed), live_e, live_f])
        proxy = ConsumerHarness()
        for ev in got:
            proxy.feed(ev)
        # Both: seed skipped cold (initialized=False), bar E closes at TF.
        assert [str(b.close) for b in direct.emitted] == ["4126.25"]
        assert_bars_identical(direct.emitted, proxy.emitted, t0_ns)

    async def test_fresh_subscribe_during_idle_emits_no_stale_session_bar(
        self, tmp_path, monkeypatch
    ):
        """G1 at the fleet's mandated restart window: a consumer subscribing during
        an idle stretch (overnight/pre-open) must NOT replay the previous
        session's final bar M — the state machine would emit it as a live
        closed bar at the next session's first event, which a direct node
        connecting at the same instant never does."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        handler, st, feed_dir = make_mirror(tmp_path)
        session = [
            _ev(TA, "4118.00", "4120.50", "4117.50", "4120.25", "300"),
            _ev(TA, "4118.00", "4121.75", "4117.50", "4121.50", "640"),
            _ev(TB, "4121.75", "4123.50", "4121.00", "4123.25", "480"),  # final bar M...
            _ev(TB, "4121.75", "4123.75", "4121.00", "4123.50", "510"),  # ...close unconfirmed
        ]
        for ev in session:
            ingest(handler, st, ev)

        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 2))
        await asyncio.sleep(0.05)  # idle subscribe: seed yielded, attached at the tail
        nxt = _ev(TC, "4123.50", "4125.00", "4122.75", "4124.00", "880")  # next session opens
        ingest(handler, st, nxt)
        got = await task
        await gen.aclose()

        assert got[0]["_ts_feed_seed"] is True
        assert got[0]["TimeStamp"] == TA  # seed = last PROVEN-complete bar
        assert got[1:] == [nxt]  # bar M's updates were NOT replayed

        proxy = ConsumerHarness()
        for ev in got:
            proxy.feed(ev)
        # A direct node connecting at the same instant: connect seed + next event.
        direct = run_direct([dict(got[0]), nxt])
        assert direct.emitted == []  # nothing emitted for the prior session's bar
        assert proxy.emitted == []  # G1: the proxy matches

    async def test_handler_restart_idle_subscribe_with_reconnect_seed(
        self, tmp_path, monkeypatch
    ):
        """Pre-open handler restart: the restored manifest join points at the
        previous session's final bar M, and upstream's reconnect Historical
        seed Y (ts > M) lands in the stream. The proxy must emit NEITHER M nor
        Y — exactly a direct cold connect (seed skipped, next bar initializes)."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        handler, st, feed_dir = make_mirror(tmp_path)
        session = [
            _ev(TA, "4118.00", "4120.50", "4117.50", "4120.25", "300"),
            _ev(TB, "4121.75", "4123.50", "4121.00", "4123.25", "480"),  # bar M, unconfirmed
        ]
        for ev in session:
            ingest(handler, st, ev)
        st.writer.close()  # handler stops overnight

        handler2 = FeedHandler("SIMTEST", tmp_path, None)  # pre-open restart
        st2 = handler2._start_ingest(KEY, "ESM26", "15", "Minute", None)

        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 3))
        await asyncio.sleep(0.05)  # subscribed before upstream reconnects

        seed_y = _ev(TB, "4121.75", "4123.75", "4121.00", "4123.50", "520", status="Historical")
        ingest(handler2, st2, seed_y)  # rule (a): bar M completed during the idle
        nxt = _ev(TC, "4123.50", "4125.00", "4122.75", "4124.00", "880")
        ingest(handler2, st2, nxt)
        got = await task
        await gen.aclose()

        assert got[0]["_ts_feed_seed"] is True
        assert got[0]["TimeStamp"] == TA  # restored manifest seed (last proven close)
        assert got[1:] == [seed_y, nxt]  # M's RealTime updates were NOT replayed

        proxy = ConsumerHarness()
        for ev in got:
            proxy.feed(ev)
        direct = run_direct([dict(got[0]), seed_y, nxt])
        assert direct.emitted == []  # neither M nor Y emitted
        assert proxy.emitted == []
        st2.writer.close()

    async def test_gap_error_fresh_start_initialized_no_out_of_order(
        self, tmp_path, monkeypatch
    ):
        """FeedGapError recovery on an INITIALIZED state machine: the fresh
        start must deliver only (manifest seed + post-attach events) — no
        stale RealTime replay that would emit out of order. The loss bound is
        a direct-mode SSE outage: the buffered bar emits with its last SEEN
        update at the next bar's first event."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        t0_ns = time.time_ns()
        handler, st, feed_dir = make_mirror(tmp_path)
        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        proxy = ConsumerHarness()

        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 3))
        await asyncio.sleep(0.05)
        pre = [
            _ev(TA, "4118.00", "4120.50", "4117.50", "4120.25", "300"),
            _ev(TA, "4118.00", "4121.75", "4117.50", "4121.50", "640"),
            _ev(TB, "4121.75", "4123.50", "4121.00", "4123.25", "480"),  # bar B in progress
        ]
        for ev in pre:
            ingest(handler, st, ev)
        for ev in await task:
            proxy.feed(ev)  # the proxy is now INITIALIZED mid-bar B

        # A bar-B update never reaches the consumer intact: forge a seq gap.
        seg_path = st.writer.key_dir / protocol.segment_name(st.writer.active_segment)
        lost = _ev(TB, "4121.75", "4123.75", "4121.00", "4123.50", "510")
        with open(seg_path, "ab") as f:
            f.write(protocol.encode_event_line(st.writer.seq_highwater + 2, lost))
        with pytest.raises(FeedGapError):
            await gen.__anext__()
        await gen.aclose()

        # Supervise re-entry: FRESH START on the same (initialized) machine.
        gen2 = client.stream_bars("ESM26", "15", "Minute")
        task2 = asyncio.create_task(collect(gen2, 2))
        await asyncio.sleep(0.05)
        nxt = _ev(TC, "4123.50", "4125.00", "4122.75", "4124.00", "880")
        ingest(handler, st, nxt)
        got2 = await task2
        await gen2.aclose()

        assert got2[0]["_ts_feed_seed"] is True
        assert got2[0]["TimeStamp"] == TA  # seed = last proven-complete bar
        assert got2[1:] == [nxt]  # nothing stale replayed
        for ev in got2:
            proxy.feed(ev)

        # Direct node across the same outage: pre-gap events, reconnect seed,
        # next bar. The stale seed is ignored; bar B emits with its last SEEN
        # update; nothing emits out of order.
        direct = run_direct(pre + [dict(got2[0]), nxt])
        assert [str(b.close) for b in direct.emitted] == ["4121.50", "4123.25"]
        assert_bars_identical(direct.emitted, proxy.emitted, t0_ns)
