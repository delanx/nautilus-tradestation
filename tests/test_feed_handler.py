"""
Tests for feed/handler.py — FeedHandler end-to-end with a fake SSE source:
publish -> subscribe -> replay -> dedup, request discovery, key-mismatch and
payload-conflict quarantine, single-writer lock, ingest-health heartbeat,
key retirement, planned stop, retention sweep.
"""
import asyncio
import os
import time

import pytest

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

            # SUBSCRIBE: two cells tail the same key. Fresh start = seed +
            # tail-from-end: T+1's PRE-subscribe update is NOT replayed
            # (direct-connect parity; a stale replay would break G1).
            client_a = FeedTailStreamClient(feed_dir, poll_ms=10)
            client_b = FeedTailStreamClient(feed_dir, poll_ms=10)
            gen_a = client_a.stream_bars("ESM26", "15", "Minute")
            gen_b = client_b.stream_bars("ESM26", "15", "Minute")
            task_a = asyncio.create_task(collect(gen_a, 2))
            task_b = asyncio.create_task(collect(gen_b, 2))
            await asyncio.sleep(0.1)  # seeds yielded; both attached at the tail

            # PUBLISH live: a pushed update reaches both subscribers verbatim.
            live = _ev("2026-06-10T14:15:00Z", "4123.0")
            fake.queue.put_nowait(live)
            got_a = await task_a
            got_b = await task_b
            assert got_a == got_b
            assert got_a[0]["Status"] == "Historical"
            assert got_a[0]["_ts_feed_seed"] is True
            assert got_a[0]["TimeStamp"] == "2026-06-10T14:00:00Z"  # seed = T's final update
            assert got_a[0]["Close"] == "4121.5"
            assert got_a[1] == live  # T+1's pre-subscribe update was not replayed

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


class TestSingleWriterLock:
    async def test_second_handler_refused_and_lock_dies_with_owner(self, tmp_path, monkeypatch):
        # Two concurrent handlers would interleave appends with independent seq
        # counters (consumers silently drop the forks as dedup = missed bars).
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        h1 = make_handler(tmp_path, FakeStreamClient())
        t1 = asyncio.create_task(h1.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until((feed_dir / protocol.META_NAME).exists)
            h2 = make_handler(tmp_path, FakeStreamClient())
            with pytest.raises(RuntimeError, match="handler.lock"):
                await h2.run()
        finally:
            t1.cancel()
            await asyncio.gather(t1, return_exceptions=True)
        # The lock died with the first handler: a successor starts cleanly.
        h3 = make_handler(tmp_path, FakeStreamClient())
        t3 = asyncio.create_task(h3.run())
        try:
            heartbeat = feed_dir / protocol.HEARTBEAT_NAME
            before = os.stat(heartbeat).st_mtime
            await wait_until(lambda: os.stat(heartbeat).st_mtime > before)
            assert not t3.done()
        finally:
            t3.cancel()
            await asyncio.gather(t3, return_exceptions=True)


class TestIngestHealth:
    async def test_heartbeat_freezes_while_mirror_io_fails_then_recovers(
        self, tmp_path, monkeypatch
    ):
        # Disk-full class: appends fail but the process is alive. The heartbeat
        # must FREEZE (cells read silence as handler-dead, never quiet market)
        # and thaw on the next successful append.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4120.0"))
            await wait_until(lambda: KEY in handler._ingests and handler._ingests[KEY].cur_ts)
            st = handler._ingests[KEY]
            real_append = st.writer.append
            broken = {"on": True}

            def flaky_append(ev):
                if broken["on"]:
                    raise OSError("disk full")
                return real_append(ev)

            monkeypatch.setattr(st.writer, "append", flaky_append)
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4121.0"))
            await wait_until(lambda: KEY in handler._mirror_io_errors)
            heartbeat = feed_dir / protocol.HEARTBEAT_NAME
            frozen = os.stat(heartbeat).st_mtime
            await asyncio.sleep(0.2)  # many heartbeat cadences (0.02s each)
            assert os.stat(heartbeat).st_mtime == frozen  # FROZEN, not green
            # IO recovers: the next successful append thaws the heartbeat.
            broken["on"] = False
            fake.queue.put_nowait(_ev("2026-06-10T14:00:00Z", "4122.0"))
            await wait_until(lambda: KEY not in handler._mirror_io_errors, timeout=10.0)
            await wait_until(lambda: os.stat(heartbeat).st_mtime > frozen, timeout=10.0)
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestKeyRetirement:
    async def test_stale_lease_retires_ingest_and_resubscribes_on_return(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake, request_ttl_secs=0.2)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        req = feed_dir / protocol.REQUESTS_DIR / f"{KEY}{protocol.REQUEST_SUFFIX}"
        try:
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            await wait_until(lambda: KEY in handler._ingests)
            assert len(fake.calls) == 1
            # No cell re-stamps the lease: past the TTL the key is retired
            # (upstream subscription dropped, writer closed, lease deleted).
            old = time.time() - 5.0
            os.utime(req, (old, old))
            await wait_until(lambda: KEY not in handler._ingests)
            assert KEY not in handler._ingest_tasks
            assert not req.exists()
            # A cell coming back re-leases and gets a fresh ingest.
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            await wait_until(lambda: KEY in handler._ingests)
            await wait_until(lambda: len(fake.calls) == 2)  # upstream re-subscribed
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_stale_leftover_request_never_subscribed(self, tmp_path, monkeypatch):
        # Handler restart finding a retired pod's leftover lease: never re-open
        # an upstream TS SSE subscription for it.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        feed_dir = tmp_path / "SIMTEST"
        key = write_request(feed_dir, "ESM26", "15", "Minute", None)
        req = feed_dir / protocol.REQUESTS_DIR / f"{key}{protocol.REQUEST_SUFFIX}"
        old = time.time() - 5.0
        os.utime(req, (old, old))
        handler = make_handler(tmp_path, fake, request_ttl_secs=0.2)
        run_task = asyncio.create_task(handler.run())
        try:
            await wait_until(lambda: not req.exists())
            assert fake.calls == []
            assert key not in handler._ingests
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_conflicting_payload_for_running_key_quarantined(self, tmp_path, monkeypatch):
        # Defense in depth behind the key hash: a request whose key another
        # stream already owns is quarantined + alerted, never silently absorbed
        # (the conflicting subscriber would otherwise tail the WRONG symbol).
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        req = feed_dir / protocol.REQUESTS_DIR / f"{KEY}{protocol.REQUEST_SUFFIX}"
        try:
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            await wait_until(lambda: KEY in handler._ingests)
            # Tamper: same key file, different semantic payload.
            protocol.write_json_atomic(req, {
                "v": 1,
                "symbol": "NQM26",
                "interval": "15",
                "unit": "Minute",
                "session_template": None,
                "requested_by": 1234,
                "ts_utc": "2026-06-10T00:00:00+00:00",
            })
            await wait_until(lambda: req.with_name(f"{req.name}.bad").exists())
            alert = tmp_path / "alerts" / f"feed_request_conflict_SIMTEST_{KEY}.json"
            await wait_until(alert.exists)
            # The running ingest is untouched; the conflicting cell is NOT served.
            assert KEY in handler._ingests
            assert handler._ingests[KEY].symbol == "ESM26"
            assert fake.calls == [("ESM26", "15", "Minute", None)]
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
