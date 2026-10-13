"""
Tests for feed/tail_client.py's ``stream_orders`` (Shared order stream):
poll-tail of the account-wide order/fill mirror. Contrasted with
``stream_bars`` throughout: no seed, no per-key subdirectory, no request
lease -- a fresh subscriber ALWAYS starts at the end (never replays history),
a visible seq gap is ``FeedGapError``, and a stale handler heartbeat observed
while idle is ``FeedHandlerDeadError``.
"""
import asyncio
import os
import time

import pytest

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.mirror import MirrorWriter
from nautilus_tradestation.feed.tail_client import (
    FeedGapError,
    FeedHandlerDeadError,
    FeedSubscribeTimeoutError,
    FeedTailStreamClient,
)


def _order_ev(order_id: str, status: str = "ACK", marker: int | None = None) -> dict:
    ev = {
        "OrderID": order_id,
        "Status": status,
        "FilledQuantity": "0",
        "AveragePrice": "0",
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


def make_orders_feed(tmp_path, **writer_kwargs):
    feed_dir = tmp_path / "SIMTEST"
    orders_dir = feed_dir / protocol.ORDERS_DIR
    writer = MirrorWriter(orders_dir, **writer_kwargs)
    writer.write_manifest(seed_event=None, join=None, last_close_ts="")
    touch_heartbeat(feed_dir)
    return feed_dir, orders_dir, writer


def make_client(feed_dir, **kwargs) -> FeedTailStreamClient:
    defaults = dict(
        poll_ms=10,
        subscribe_timeout_secs=2.0,
        idle_check_secs=0.5,
        heartbeat_stale_secs=0.5,
    )
    defaults.update(kwargs)
    return FeedTailStreamClient(feed_dir, **defaults)


async def collect(agen, n, timeout=5.0):
    out = []

    async def _run():
        async for ev in agen:
            out.append(ev)
            if len(out) >= n:
                return

    await asyncio.wait_for(_run(), timeout)
    return out


class TestOrdersFreshStartNeverReplays:
    async def test_no_seed_pre_attach_events_never_replayed(self, tmp_path):
        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        pre = _order_ev("1", marker=1)  # written before any subscriber attaches
        writer.append(pre)
        client = make_client(feed_dir)
        gen = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen, 1))
        await asyncio.sleep(0.15)  # consumer attached at the tail, no seed to wait on
        live = _order_ev("2", marker=2)
        writer.append(live)
        got = await task
        await gen.aclose()
        assert got == [live]  # the pre-attach event was never replayed
        assert all("_ts_feed_seed" not in ev for ev in got)  # orders never synthesize a seed
        writer.close()

    async def test_events_cross_verbatim(self, tmp_path):
        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        client = make_client(feed_dir)
        gen = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen, 1))
        await asyncio.sleep(0.1)
        ev = _order_ev("42", status="FLL")
        writer.append(ev)
        got = (await task)[0]
        await gen.aclose()
        assert got == ev
        for field in ("OrderID", "Status", "FilledQuantity", "AveragePrice"):
            assert type(got[field]) is str
            assert got[field] == ev[field]
        writer.close()


class TestOrdersResume:
    async def test_resume_replays_exactly_once_in_order(self, tmp_path):
        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        client = make_client(feed_dir)
        gen1 = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen1, 2))
        await asyncio.sleep(0.1)
        events = [_order_ev(str(i), marker=i) for i in range(1, 6)]
        for ev in events:
            writer.append(ev)
        first = await task
        await gen1.aclose()
        assert first == events[:2]
        assert client._order_cursor is not None

        gen2 = client.stream_orders("SIMTEST")
        rest = await collect(gen2, 3)
        assert rest == events[2:]  # every line replayed exactly once, in order
        await gen2.aclose()
        writer.close()

    async def test_resume_with_cursor_segment_gone_raises_gap(self, tmp_path):
        # Retention falloff: the cursor's segment was deleted while the
        # generator was down -> FeedGapError raised at resume, cursor cleared.
        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        client = make_client(feed_dir)
        gen1 = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen1, 2))
        await asyncio.sleep(0.1)
        events = [_order_ev(str(i), marker=i) for i in range(1, 3)]
        for ev in events:
            writer.append(ev)
        got = await task
        await gen1.aclose()
        assert got == events
        assert client._order_cursor[0] == 1  # both events landed in segment 1

        writer.close()  # release the writer's file handle before deleting it
        (orders_dir / protocol.segment_name(1)).unlink()
        gen2 = client.stream_orders("SIMTEST")
        with pytest.raises(FeedGapError):
            await collect(gen2, 1)
        assert client._order_cursor is None


class TestOrdersGapHardError:
    async def test_seq_gap_raises_and_clears_cursor(self, tmp_path):
        feed_dir = tmp_path / "SIMTEST"
        orders_dir = feed_dir / protocol.ORDERS_DIR
        orders_dir.mkdir(parents=True)
        touch_heartbeat(feed_dir)
        protocol.write_json_atomic(
            orders_dir / protocol.MANIFEST_NAME,
            {"v": 1, "key": "orders", "active_segment": 1, "seq_highwater": 0,
             "seed_event": None, "join": None, "last_close_ts": ""},
        )
        client = make_client(feed_dir)
        gen = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen, 2))
        await asyncio.sleep(0.1)  # attach on the still-empty orders dir
        with open(orders_dir / protocol.segment_name(1), "wb") as f:
            f.write(protocol.encode_event_line(1, _order_ev("1", marker=1)))
            f.write(protocol.encode_event_line(2, _order_ev("2", marker=2)))
            f.write(protocol.encode_event_line(5, _order_ev("5", marker=5)))
        got = await task
        assert [ev["_marker"] for ev in got] == [1, 2]
        with pytest.raises(FeedGapError):
            await gen.__anext__()
        assert client._order_cursor is None


class TestOrdersHeartbeatLiveness:
    async def test_stale_heartbeat_while_idle_raises_dead_and_keeps_cursor(self, tmp_path):


        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        client = make_client(feed_dir, idle_check_secs=0.15, heartbeat_stale_secs=0.3)
        gen = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen, 1))
        await asyncio.sleep(0.05)
        writer.append(_order_ev("1", marker=1))
        got = await task
        assert got[0]["_marker"] == 1
        touch_heartbeat(feed_dir, age_secs=100.0)  # handler dies; stream goes idle
        with pytest.raises(FeedHandlerDeadError):
            await asyncio.wait_for(gen.__anext__(), timeout=5.0)
        # Cursor KEPT: recovery after a handler restart is a resume-replay
        # with zero loss for everything that reached the mirror.
        assert client._order_cursor is not None
        writer.close()

    async def test_missing_heartbeat_counts_as_dead(self, tmp_path):


        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        (feed_dir / protocol.HEARTBEAT_NAME).unlink()
        client = make_client(feed_dir, idle_check_secs=0.15, heartbeat_stale_secs=0.3)
        gen = client.stream_orders("SIMTEST")
        with pytest.raises(FeedHandlerDeadError):
            await collect(gen, 1, timeout=5.0)
        writer.close()


class TestOrdersSegmentRolls:
    async def test_roll_followed_mid_tail(self, tmp_path):
        feed_dir, orders_dir, writer = make_orders_feed(tmp_path, segment_max_bytes=1)
        client = make_client(feed_dir)
        gen = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen, 4))
        await asyncio.sleep(0.1)
        events = [_order_ev(str(i), marker=i) for i in range(1, 5)]
        for ev in events:
            writer.append(ev)
        got = await task
        await gen.aclose()
        assert got == events
        writer.close()


class TestOrdersSubscribeTimeout:
    async def test_no_manifest_raises_loudly(self, tmp_path):


        feed_dir = tmp_path / "SIMTEST"
        touch_heartbeat(feed_dir)
        client = make_client(feed_dir, subscribe_timeout_secs=0.3)
        gen = client.stream_orders("SIMTEST")
        with pytest.raises(FeedSubscribeTimeoutError):
            await collect(gen, 1, timeout=5.0)


class TestOrdersNoLease:
    async def test_stream_orders_writes_no_request_file(self, tmp_path):
        # Orders are not request-gated: no lease file should ever appear.
        feed_dir, orders_dir, writer = make_orders_feed(tmp_path)
        client = make_client(feed_dir)
        gen = client.stream_orders("SIMTEST")
        task = asyncio.create_task(collect(gen, 1, timeout=0.3))
        await asyncio.sleep(0.1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await gen.aclose()
        requests_dir = feed_dir / protocol.REQUESTS_DIR
        assert not requests_dir.exists() or not list(requests_dir.glob("*"))
        writer.close()
