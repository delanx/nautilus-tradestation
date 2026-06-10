"""
Tests for feed/tail_client.py — FeedTailStreamClient: string preservation,
fresh start (seed + join), resume replay, gap hard-error, heartbeat liveness,
segment rolls, subscribe timeout, delegation.
"""
import asyncio
import os
import time

import pytest

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.feed.mirror import MirrorWriter
from nautilus_tradestation.feed.tail_client import (
    FeedGapError,
    FeedHandlerDeadError,
    FeedSubscribeTimeoutError,
    FeedTailStreamClient,
)

KEY = stream_key("ESM26", "15", "Minute", None)


def _ev(ts: str, close: str = "4123.25", status: str = "RealTime", marker: int | None = None):
    ev = {
        "TimeStamp": ts,
        "Open": "4119.0",
        "High": "4124.50",
        "Low": "4118.75",
        "Close": close,
        "TotalVolume": "1500",
        "Status": status,
    }
    if marker is not None:
        ev["_marker"] = marker
    return ev


def touch_heartbeat(feed_dir, age_secs: float = 0.0) -> None:
    feed_dir.mkdir(parents=True, exist_ok=True)
    heartbeat = feed_dir / protocol.HEARTBEAT_NAME
    heartbeat.touch()
    if age_secs:
        stamp = time.time() - age_secs
        os.utime(heartbeat, (stamp, stamp))


def make_feed(tmp_path, **writer_kwargs):
    feed_dir = tmp_path / "SIMTEST"
    key_dir = feed_dir / protocol.BARS_DIR / KEY
    writer = MirrorWriter(key_dir, **writer_kwargs)
    touch_heartbeat(feed_dir)
    return feed_dir, key_dir, writer


def make_client(feed_dir, **kwargs) -> FeedTailStreamClient:
    defaults = dict(
        poll_ms=10,
        subscribe_timeout_secs=2.0,
        idle_check_secs=0.5,
        heartbeat_stale_secs=0.5,
    )
    defaults.update(kwargs)
    return FeedTailStreamClient(feed_dir, **defaults)


def write_manifest_raw(key_dir, *, active_segment=1, seq_highwater=0, seed_event=None,
                       join=None, last_close_ts=""):
    key_dir.mkdir(parents=True, exist_ok=True)
    protocol.write_json_atomic(
        key_dir / protocol.MANIFEST_NAME,
        {
            "v": 1,
            "key": key_dir.name,
            "active_segment": active_segment,
            "seq_highwater": seq_highwater,
            "seed_event": seed_event,
            "join": join,
            "last_close_ts": last_close_ts,
        },
    )


async def collect(agen, n, timeout=5.0):
    out = []

    async def _run():
        async for ev in agen:
            out.append(ev)
            if len(out) >= n:
                return

    await asyncio.wait_for(_run(), timeout)
    return out


class TestFromEnv:
    def test_unknown_scheme_is_hard_error(self):
        with pytest.raises(RuntimeError, match="unknown scheme"):
            FeedTailStreamClient.from_env("tcp:127.0.0.1:9999")

    def test_empty_path_is_hard_error(self):
        with pytest.raises(RuntimeError, match="requires a path"):
            FeedTailStreamClient.from_env("jsonl:")

    def test_non_localappdata_path_is_hard_error(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TS_FEED_ALLOW_ANY_DIR", raising=False)
        monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "AppData" / "Local"))
        with pytest.raises(RuntimeError, match="LOCALAPPDATA"):
            FeedTailStreamClient.from_env(r"jsonl:G:\My Drive\example\feed\SIM1")

    def test_localappdata_path_accepted(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TS_FEED_ALLOW_ANY_DIR", raising=False)
        local = tmp_path / "AppData" / "Local"
        monkeypatch.setenv("LOCALAPPDATA", str(local))
        client = FeedTailStreamClient.from_env(f"jsonl:{local / 'example' / 'feed' / 'SIM1'}")
        assert isinstance(client, FeedTailStreamClient)

    def test_allow_any_dir_escape(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "AppData" / "Local"))
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        client = FeedTailStreamClient.from_env(f"jsonl:{tmp_path / 'elsewhere'}")
        assert isinstance(client, FeedTailStreamClient)


class TestStringPreservation:
    async def test_events_cross_verbatim(self, tmp_path):
        # I2: TimeStamp/OHLCV remain the exact strings TS sent — never re-typed.
        feed_dir, key_dir, writer = make_feed(tmp_path)
        ev = _ev("2026-06-10T14:30:00Z", close="4123.25")
        seq, seg, off = writer.append(ev)
        writer.write_manifest(
            seed_event=None, join=(seg, off, seq), last_close_ts=""
        )
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = (await collect(gen, 1))[0]
        await gen.aclose()
        assert got == ev
        for field in ("TimeStamp", "Open", "High", "Low", "Close", "TotalVolume"):
            assert type(got[field]) is str
            assert got[field] == ev[field]
        assert "_ts_feed_seed" not in got
        writer.close()


class TestFreshStart:
    async def test_seed_yielded_once_then_join_tail(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        seed = _ev("2026-06-10T14:15:00Z", status="Historical")
        current = _ev("2026-06-10T14:30:00Z", marker=1)
        seq, seg, off = writer.append(current)
        writer.write_manifest(
            seed_event=seed, join=(seg, off, seq), last_close_ts=seed["TimeStamp"]
        )
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 2)
        await gen.aclose()
        assert got[0] == dict(seed, _ts_feed_seed=True)
        assert got[0]["Status"] == "Historical"
        assert got[1] == current
        writer.close()

    async def test_null_join_tails_from_end(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        writer.append(_ev("2026-06-10T14:00:00Z", marker=1))
        writer.append(_ev("2026-06-10T14:00:00Z", marker=2))
        writer.write_manifest(seed_event=None, join=None, last_close_ts="")
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 1))
        await asyncio.sleep(0.25)  # let the consumer attach at the tail
        late = _ev("2026-06-10T14:15:00Z", marker=3)
        writer.append(late)
        got = await task
        await gen.aclose()
        assert got == [late]  # pre-attach events were skipped, like direct mode
        writer.close()

    async def test_request_file_written(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        writer.write_manifest(seed_event=None, join=None, last_close_ts="")
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        task = asyncio.create_task(collect(gen, 1, timeout=0.5))
        await asyncio.sleep(0.1)
        request = protocol.read_json(
            feed_dir / protocol.REQUESTS_DIR / f"{KEY}{protocol.REQUEST_SUFFIX}"
        )
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await gen.aclose()
        assert request["symbol"] == "ESM26"
        assert request["interval"] == "15"
        assert request["unit"] == "Minute"
        assert request["session_template"] is None
        assert request["requested_by"] == os.getpid()
        writer.close()


class TestResume:
    async def test_resume_replays_exactly_once_in_order(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        events = [_ev("2026-06-10T14:00:00Z", marker=i) for i in range(1, 6)]
        positions = [writer.append(ev) for ev in events]
        seq, seg, off = positions[0]
        writer.write_manifest(seed_event=None, join=(seg, off, seq), last_close_ts="")
        client = make_client(feed_dir)

        gen1 = client.stream_bars("ESM26", "15", "Minute")
        first = await collect(gen1, 2)
        await gen1.aclose()  # kill the generator mid-stream
        assert first == events[:2]
        assert KEY in client._cursors

        gen2 = client.stream_bars("ESM26", "15", "Minute")
        rest = await collect(gen2, 3)
        assert rest == events[2:]  # every line replayed exactly once, in order
        extra = _ev("2026-06-10T14:15:00Z", marker=6)
        writer.append(extra)
        assert (await collect(gen2, 1)) == [extra]
        await gen2.aclose()
        assert all("_ts_feed_seed" not in ev for ev in first + rest)
        writer.close()

    async def test_resume_with_cursor_segment_gone_raises_gap(self, tmp_path):
        # Retention falloff: the cursor's segment was deleted while the
        # generator was down -> FeedGapError, cursor cleared, fresh start next.
        feed_dir, key_dir, writer = make_feed(tmp_path, segment_max_bytes=1)
        events = [_ev("2026-06-10T14:00:00Z", marker=i) for i in range(1, 4)]
        positions = [writer.append(ev) for ev in events]
        seq, seg, off = positions[0]
        seed = _ev("2026-06-10T13:45:00Z", status="Historical")
        writer.write_manifest(seed_event=seed, join=(seg, off, seq), last_close_ts="")
        client = make_client(feed_dir)

        gen1 = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen1, 3)  # seed + events 1-2; cursor lands in segment 2
        await gen1.aclose()
        assert got[0] == dict(seed, _ts_feed_seed=True)
        assert got[1:] == events[:2]

        (key_dir / protocol.segment_name(1)).unlink()
        (key_dir / protocol.segment_name(2)).unlink()
        # Cursor points into the (now deleted) segment 2.
        gen2 = client.stream_bars("ESM26", "15", "Minute")
        with pytest.raises(FeedGapError):
            await collect(gen2, 1)
        assert KEY not in client._cursors

        # Fresh-start fallback is loud but works: the seed is yielded again.
        gen3 = client.stream_bars("ESM26", "15", "Minute")
        got3 = await collect(gen3, 1)
        await gen3.aclose()
        assert got3[0] == dict(seed, _ts_feed_seed=True)
        writer.close()


class TestGapHardError:
    async def test_seq_gap_raises_and_clears_cursor(self, tmp_path):
        feed_dir = tmp_path / "SIMTEST"
        key_dir = feed_dir / protocol.BARS_DIR / KEY
        key_dir.mkdir(parents=True)
        touch_heartbeat(feed_dir)
        seed = _ev("2026-06-10T13:45:00Z", status="Historical")
        with open(key_dir / protocol.segment_name(1), "wb") as f:
            f.write(protocol.encode_event_line(1, _ev("2026-06-10T14:00:00Z", marker=1)))
            f.write(protocol.encode_event_line(2, _ev("2026-06-10T14:00:00Z", marker=2)))
            f.write(protocol.encode_event_line(5, _ev("2026-06-10T14:00:00Z", marker=5)))
        write_manifest_raw(
            key_dir,
            seq_highwater=5,
            seed_event=seed,
            join={"segment": 1, "offset": 0, "seq": 1},
        )
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 3)  # seed + seq 1 + seq 2
        assert [ev.get("_marker") for ev in got] == [None, 1, 2]
        with pytest.raises(FeedGapError):
            await gen.__anext__()
        assert KEY not in client._cursors
        # Next entry takes the fresh-start path (seed again) — never silent.
        gen2 = client.stream_bars("ESM26", "15", "Minute")
        got2 = await collect(gen2, 1)
        await gen2.aclose()
        assert got2[0] == dict(seed, _ts_feed_seed=True)

    async def test_duplicate_seq_is_dropped(self, tmp_path):
        feed_dir = tmp_path / "SIMTEST"
        key_dir = feed_dir / protocol.BARS_DIR / KEY
        key_dir.mkdir(parents=True)
        touch_heartbeat(feed_dir)
        with open(key_dir / protocol.segment_name(1), "wb") as f:
            f.write(protocol.encode_event_line(1, _ev("2026-06-10T14:00:00Z", marker=1)))
            f.write(protocol.encode_event_line(2, _ev("2026-06-10T14:00:00Z", marker=2)))
            f.write(protocol.encode_event_line(2, _ev("2026-06-10T14:00:00Z", marker=99)))
            f.write(protocol.encode_event_line(3, _ev("2026-06-10T14:00:00Z", marker=3)))
        write_manifest_raw(key_dir, seq_highwater=3, join={"segment": 1, "offset": 0, "seq": 1})
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 3)
        await gen.aclose()
        assert [ev["_marker"] for ev in got] == [1, 2, 3]  # the duplicate never surfaces

    async def test_bad_line_skipped_when_continuity_holds(self, tmp_path):
        feed_dir = tmp_path / "SIMTEST"
        key_dir = feed_dir / protocol.BARS_DIR / KEY
        key_dir.mkdir(parents=True)
        touch_heartbeat(feed_dir)
        with open(key_dir / protocol.segment_name(1), "wb") as f:
            f.write(protocol.encode_event_line(1, _ev("2026-06-10T14:00:00Z", marker=1)))
            f.write(b"this is not json\n")
            f.write(protocol.encode_event_line(2, _ev("2026-06-10T14:00:00Z", marker=2)))
        write_manifest_raw(key_dir, seq_highwater=2, join={"segment": 1, "offset": 0, "seq": 1})
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 2)
        await gen.aclose()
        assert [ev["_marker"] for ev in got] == [1, 2]

    async def test_bad_line_hiding_an_event_is_a_gap(self, tmp_path):
        feed_dir = tmp_path / "SIMTEST"
        key_dir = feed_dir / protocol.BARS_DIR / KEY
        key_dir.mkdir(parents=True)
        touch_heartbeat(feed_dir)
        with open(key_dir / protocol.segment_name(1), "wb") as f:
            f.write(protocol.encode_event_line(1, _ev("2026-06-10T14:00:00Z", marker=1)))
            f.write(b'{"seq": 2, "ev": {"TimeSta\n')  # a torn line that WAS event 2
            f.write(protocol.encode_event_line(3, _ev("2026-06-10T14:00:00Z", marker=3)))
        write_manifest_raw(key_dir, seq_highwater=3, join={"segment": 1, "offset": 0, "seq": 1})
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 1)
        assert got[0]["_marker"] == 1
        with pytest.raises(FeedGapError):
            await gen.__anext__()


class TestHeartbeatLiveness:
    async def test_stale_heartbeat_while_idle_raises_dead_and_keeps_cursor(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        seq, seg, off = writer.append(_ev("2026-06-10T14:00:00Z", marker=1))
        writer.write_manifest(seed_event=None, join=(seg, off, seq), last_close_ts="")
        touch_heartbeat(feed_dir, age_secs=100.0)
        client = make_client(feed_dir, idle_check_secs=0.15, heartbeat_stale_secs=0.3)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 1)
        assert got[0]["_marker"] == 1
        with pytest.raises(FeedHandlerDeadError):
            await asyncio.wait_for(gen.__anext__(), timeout=5.0)
        # Cursor KEPT: recovery after a handler restart is a resume-replay.
        assert KEY in client._cursors
        writer.close()

    async def test_fresh_bytes_reset_the_idle_clock(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        seq, seg, off = writer.append(_ev("2026-06-10T14:00:00Z", marker=0))
        writer.write_manifest(seed_event=None, join=(seg, off, seq), last_close_ts="")
        touch_heartbeat(feed_dir, age_secs=100.0)  # handler "dead" the whole time
        client = make_client(feed_dir, idle_check_secs=0.5, heartbeat_stale_secs=0.3)
        gen = client.stream_bars("ESM26", "15", "Minute")

        async def pump():
            for i in range(1, 9):
                writer.append(_ev("2026-06-10T14:00:00Z", marker=i))
                await asyncio.sleep(0.1)

        pump_task = asyncio.create_task(pump())
        got = await collect(gen, 9, timeout=5.0)  # flows for ~0.8s with no raise
        await pump_task
        assert [ev["_marker"] for ev in got] == list(range(9))
        with pytest.raises(FeedHandlerDeadError):  # then idle -> dead detected
            await asyncio.wait_for(gen.__anext__(), timeout=5.0)
        writer.close()

    async def test_missing_heartbeat_counts_as_dead(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path)
        writer.write_manifest(seed_event=None, join=None, last_close_ts="")
        (feed_dir / protocol.HEARTBEAT_NAME).unlink()
        client = make_client(feed_dir, idle_check_secs=0.15, heartbeat_stale_secs=0.3)
        gen = client.stream_bars("ESM26", "15", "Minute")
        with pytest.raises(FeedHandlerDeadError):
            await collect(gen, 1, timeout=5.0)
        writer.close()


class TestSegmentRolls:
    async def test_roll_followed_mid_tail(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path, segment_max_bytes=1)
        events = [_ev("2026-06-10T14:00:00Z", marker=i) for i in range(1, 5)]
        positions = [writer.append(ev) for ev in events]
        seq, seg, off = positions[0]
        writer.write_manifest(seed_event=None, join=(seg, off, seq), last_close_ts="")
        client = make_client(feed_dir)
        gen = client.stream_bars("ESM26", "15", "Minute")
        got = await collect(gen, 4)
        # Live roll while tailing:
        late = _ev("2026-06-10T14:15:00Z", marker=5)
        writer.append(late)
        got += await collect(gen, 1)
        await gen.aclose()
        assert got == events + [late]
        writer.close()

    async def test_roll_during_resume_replay(self, tmp_path):
        feed_dir, key_dir, writer = make_feed(tmp_path, segment_max_bytes=1)
        events = [_ev("2026-06-10T14:00:00Z", marker=i) for i in range(1, 5)]
        positions = [writer.append(ev) for ev in events]
        seq, seg, off = positions[0]
        writer.write_manifest(seed_event=None, join=(seg, off, seq), last_close_ts="")
        client = make_client(feed_dir)
        gen1 = client.stream_bars("ESM26", "15", "Minute")
        assert await collect(gen1, 1) == events[:1]
        await gen1.aclose()
        gen2 = client.stream_bars("ESM26", "15", "Minute")  # resume crosses 3 rolls
        assert await collect(gen2, 3) == events[1:]
        await gen2.aclose()
        writer.close()


class TestSubscribeTimeout:
    async def test_no_manifest_raises_loudly(self, tmp_path):
        feed_dir = tmp_path / "SIMTEST"
        touch_heartbeat(feed_dir)
        client = make_client(feed_dir, subscribe_timeout_secs=0.3)
        gen = client.stream_bars("ESM26", "15", "Minute")
        with pytest.raises(FeedSubscribeTimeoutError):
            await collect(gen, 1, timeout=5.0)
        # The request was still dropped for the handler to find.
        assert (feed_dir / protocol.REQUESTS_DIR / f"{KEY}{protocol.REQUEST_SUFFIX}").exists()


class TestDelegation:
    async def test_stream_quotes_delegates_lazily_once(self, tmp_path):
        calls = []

        class FakeReal:
            async def stream_quotes(self, symbols):
                yield {"Symbol": symbols, "Bid": "1.0"}

            async def stream_orders(self, account_id):
                yield {"AccountID": account_id}

        def factory():
            calls.append(1)
            return FakeReal()

        client = FeedTailStreamClient(tmp_path, poll_ms=10, real_client_factory=factory)
        assert calls == []  # lazy: not built until a delegated stream is used
        quotes = [ev async for ev in client.stream_quotes("GCJ26")]
        orders = [ev async for ev in client.stream_orders("SIM0000001F")]
        assert quotes == [{"Symbol": "GCJ26", "Bid": "1.0"}]
        assert orders == [{"AccountID": "SIM0000001F"}]
        assert calls == [1]  # built exactly once

    async def test_no_factory_is_loud(self, tmp_path):
        from nautilus_tradestation.feed.tail_client import FeedProxyError

        client = FeedTailStreamClient(tmp_path, poll_ms=10)
        with pytest.raises(FeedProxyError):
            async for _ in client.stream_quotes("GCJ26"):
                pass
