"""
Tests for feed/handler.py — FeedHandler end-to-end with a fake SSE source:
publish -> subscribe -> replay -> dedup, request discovery, key-mismatch
quarantine, heartbeat, planned stop, retention sweep.
"""
import asyncio
import os
import time

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.handler import FeedHandler
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.feed.tail_client import FeedTailStreamClient

KEY = stream_key("ESM26", "15", "Minute", None)


def _ev(ts: str, close: str, status: str = "RealTime") -> dict:
    return {
        "TimeStamp": ts,
        "Open": "4119.0",
        "High": "4124.50",
        "Low": "4118.75",
        "Close": close,
        "TotalVolume": "1500",
        "Status": status,
    }


class FakeStreamClient:
    """Fake TradeStation SSE source: stream_bars yields pushed events forever."""

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.calls: list[tuple] = []

    async def stream_bars(self, symbol, interval, unit, session_template=None):
        self.calls.append((symbol, interval, unit, session_template))
        while True:
            yield await self.queue.get()


def write_request(feed_dir, symbol, interval, unit, session_template) -> str:
    requests_dir = feed_dir / protocol.REQUESTS_DIR
    requests_dir.mkdir(parents=True, exist_ok=True)
    key = stream_key(symbol, interval, unit, session_template)
    protocol.write_json_atomic(
        requests_dir / f"{key}{protocol.REQUEST_SUFFIX}",
        {
            "v": 1,
            "symbol": symbol,
            "interval": interval,
            "unit": unit,
            "session_template": session_template,
            "requested_by": 1234,
            "ts_utc": "2026-06-10T00:00:00+00:00",
        },
    )
    return key


async def wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def collect(agen, n, timeout=5.0):
    out = []

    async def _run():
        async for ev in agen:
            out.append(ev)
            if len(out) >= n:
                return

    await asyncio.wait_for(_run(), timeout)
    return out


def make_handler(tmp_path, fake, **kwargs) -> FeedHandler:
    defaults = dict(
        scan_secs=0.02,
        heartbeat_secs=0.02,
        manifest_flush_secs=0.05,
        alerts_dir=tmp_path / "alerts",
    )
    defaults.update(kwargs)
    return FeedHandler("SIMTEST", tmp_path, fake, **defaults)


class TestEndToEnd:
    async def test_publish_subscribe_replay_dedup(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            # Bar T (two updates), then T+1's first update (proves T completed).
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4120.0"))
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4121.5"))
            fake.queue.put_nowait(_ev("2026-06-10T14:15:00Z", "4122.0"))
            manifest_path = feed_dir / protocol.BARS_DIR / KEY / protocol.MANIFEST_NAME
            await wait_until(lambda: (protocol.read_json(manifest_path) or {}).get("join"))

            # SUBSCRIBE: two cells tail the same key.
            client_a = FeedTailStreamClient(feed_dir, poll_ms=10)
            client_b = FeedTailStreamClient(feed_dir, poll_ms=10)
            gen_a = client_a.stream_bars("ESM26", "15", "Minute")
            gen_b = client_b.stream_bars("ESM26", "15", "Minute")
            got_a = await collect(gen_a, 2)  # seed (T's final update) + T+1's first
            got_b = await collect(gen_b, 2)
            assert got_a == got_b
            assert got_a[0]["Status"] == "Historical"
            assert got_a[0]["_ts_feed_seed"] is True
            assert got_a[0]["TimeStamp"] == "2026-06-10T14:00:00Z"
            assert got_a[0]["Close"] == "4121.5"
            assert got_a[1] == _ev("2026-06-10T14:15:00Z", "4122.0")

            # PUBLISH live: a pushed update reaches both subscribers verbatim.
            live = _ev("2026-06-10T14:15:00Z", "4123.0")
            fake.queue.put_nowait(live)
            assert (await collect(gen_a, 1))[0] == live
            assert (await collect(gen_b, 1))[0] == live

            # DEDUP: three request drops (hand + two cells) -> ONE upstream stream.
            assert fake.calls == [("ESM26", "15", "Minute", None)]

            # REPLAY: kill A's generator, push more, re-enter -> exactly once.
            await gen_a.aclose()
            fake.queue.put_nowait(_ev("2026-06-10T14:30:00Z", "4124.0"))
            gen_a2 = client_a.stream_bars("ESM26", "15", "Minute")
            resumed = await collect(gen_a2, 1)
            assert resumed[0] == _ev("2026-06-10T14:30:00Z", "4124.0")
            assert "_ts_feed_seed" not in resumed[0]  # resume yields no seed
            await gen_a2.aclose()
            await gen_b.aclose()
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_key_mismatch_renamed_bad_never_silent(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            requests_dir = feed_dir / protocol.REQUESTS_DIR
            requests_dir.mkdir(parents=True, exist_ok=True)
            bogus = requests_dir / f"BOGUS{protocol.REQUEST_SUFFIX}"
            protocol.write_json_atomic(
                bogus,
                {
                    "v": 1,
                    "symbol": "ESM26",
                    "interval": "15",
                    "unit": "Minute",
                    "session_template": None,
                    "requested_by": 1234,
                    "ts_utc": "2026-06-10T00:00:00+00:00",
                },
            )
            await wait_until(lambda: not bogus.exists())
            assert bogus.with_name(f"{bogus.name}.bad").exists()
            assert not (feed_dir / protocol.BARS_DIR / "BOGUS").exists()
            assert fake.calls == []  # never ingested
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestLifecycle:
    async def test_heartbeat_mtime_advances_and_meta_written(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            heartbeat = feed_dir / protocol.HEARTBEAT_NAME
            await wait_until(heartbeat.exists)
            meta = protocol.read_json(feed_dir / protocol.META_NAME)
            assert meta["account"] == "SIMTEST"
            assert meta["pid"] == os.getpid()
            first = os.stat(heartbeat).st_mtime
            await asyncio.sleep(0.2)
            assert os.stat(heartbeat).st_mtime > first
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_planned_stop_marker_shuts_down_cleanly(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        await wait_until((feed_dir / protocol.META_NAME).exists)
        (feed_dir / protocol.STOP_MARKER_NAME).touch()
        await asyncio.wait_for(run_task, timeout=5.0)  # clean return, not a crash


class TestRetention:
    async def test_sweep_spares_active_and_join_and_tolerates_pins(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(
            tmp_path,
            fake,
            segment_max_bytes=1,  # every event lands in its own segment
            retention_hours=0,  # everything but active/join is immediately stale
            retention_sweep_secs=0.05,
        )
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        key_dir = feed_dir / protocol.BARS_DIR / KEY
        try:
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4120.0"))  # seg 1
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4121.5"))  # seg 2
            fake.queue.put_nowait(_ev("2026-06-10T14:15:00Z", "4122.0"))  # seg 3 = join
            fake.queue.put_nowait(_ev("2026-06-10T14:15:00Z", "4122.5"))  # seg 4 = active
            await wait_until(lambda: protocol.list_segments(key_dir) and
                             max(protocol.list_segments(key_dir)) == 4)
            # Segments 1-2 fall off retention; 3 (join) and 4 (active) are spared.
            await wait_until(lambda: protocol.list_segments(key_dir) == [3, 4])

            # Pin segment 4 with an open handle, then advance active to 5.
            pinned = open(key_dir / protocol.segment_name(4), "rb")
            try:
                fake.queue.put_nowait(_ev("2026-06-10T14:15:00Z", "4123.0"))  # seg 5
                await wait_until(lambda: 5 in protocol.list_segments(key_dir))
                # The pinned segment survives sweeps (deferred delete) + alert
                # written (pinned past 2x retention, which is 0 here).
                alerts = tmp_path / "alerts"
                await wait_until(
                    lambda: alerts.exists() and any(alerts.glob(f"feed_SIMTEST_{KEY}_*.json"))
                )
                assert 4 in protocol.list_segments(key_dir)
            finally:
                pinned.close()
            # Handle released -> the next sweep finally deletes it.
            await wait_until(lambda: protocol.list_segments(key_dir) == [3, 5])
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
