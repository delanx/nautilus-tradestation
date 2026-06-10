"""
G1 differential (LOAD-BEARING): a recorded upstream event sequence must
produce IDENTICAL emitted Bar lists whether fed

(a) directly into the data client's REAL bar state machine
    (_handle_bar_stream_event), or
(b) through the feed transport — the handler's close-detection +
    MirrorWriter -> FeedTailStreamClient -> the SAME unchanged state machine.

The sequence exercises all three Historical-seed branches (same-ts
correction, stale-ignore, gap-fill), a mid-sequence consumer reconnect
(resume replay), the late-subscriber fresh start (manifest seed + join),
and ts_init precedence (_ts_bar_emit_ns stamped at the CELL wins over the
handler's _ts_sse_received_ns — I6).
"""
import asyncio
import time

from nautilus_tradestation.data import TradeStationDataClient
from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.handler import FeedHandler
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.feed.tail_client import FeedTailStreamClient
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


class CellHarness:
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


def run_direct(events) -> CellHarness:
    harness = CellHarness()
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
        # I6: ts_init is the CELL's emit stamp (_ts_bar_emit_ns), never the
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
        proxy = CellHarness()
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
        proxy = CellHarness()

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
        """A cell connecting after history: the manifest seed + join replay
        drive the state machine exactly like a direct-mode connect seed."""
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        t0_ns = time.time_ns()
        handler, st, feed_dir = make_mirror(tmp_path)
        for ev in SEQUENCE:
            ingest(handler, st, ev)

        client = FeedTailStreamClient(feed_dir, poll_ms=10)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 2)  # seed + join replay of bar E's first update

        # The synthesized seed is bar D's FINAL update, re-stamped Historical
        # (what TS barsback=1 would send a fresh direct connection).
        seed = got[0]
        assert seed["Status"] == "Historical"
        assert seed["_ts_feed_seed"] is True
        assert seed["TimeStamp"] == TD
        assert seed["Close"] == "4125.75"
        assert got[1] == SEQUENCE[-1]  # bar E's first (and only) update so far

        live = _ev(TF, "4126.00", "4126.75", "4125.75", "4126.50", "150")
        ingest(handler, st, live)
        got += await collect(gen, 1)
        await gen.aclose()

        # Direct equivalent: the same connect-time seed, then the same events.
        direct = run_direct([dict(seed), SEQUENCE[-1], live])
        proxy = CellHarness()
        for ev in got:
            proxy.feed(ev)
        # Both: seed skipped cold (initialized=False), bar E closes at TF.
        assert [str(b.close) for b in direct.emitted] == ["4126.00"]
        assert_bars_identical(direct.emitted, proxy.emitted, t0_ns)
