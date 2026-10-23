"""
Tests for feed/handler.py's order-stream ingest (Shared order stream):
the second ingest loop that mirrors ``stream_orders(account_id)`` verbatim to
``<account_dir>/orders/``, sharing the writer lock, heartbeat, alert path, and
retention sweep with the bar ingest.
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


def _bar_ev(ts: str, close: str, status: str = "RealTime") -> dict:
    return {
        "TimeStamp": ts,
        "Open": "4119.0",
        "High": "4124.50",
        "Low": "4118.75",
        "Close": close,
        "TotalVolume": "1500",
        "Status": status,
    }


def _order_ev(order_id: str, status: str = "ACK") -> dict:
    return {
        "OrderID": order_id,
        "Status": status,
        "FilledQuantity": "0",
        "AveragePrice": "0",
    }


class FakeStreamClient:
    """Fake TradeStation source with independent bar and order queues."""

    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.order_queue: asyncio.Queue = asyncio.Queue()
        self.calls: list[tuple] = []
        self.order_calls: list[str] = []

    async def stream_bars(self, symbol, interval, unit, session_template=None):
        self.calls.append((symbol, interval, unit, session_template))
        while True:
            yield await self.queue.get()

    async def stream_orders(self, account_id):
        self.order_calls.append(account_id)
        while True:
            yield await self.order_queue.get()


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
    # Order-feed opt-in gate: this whole test file is ABOUT the order-ingest
    # loop, so it opts in explicitly (enable_order_feed=True) rather than
    # relying on the TS_ORDER_FEED_INGEST env var -- the opt-in-by-default
    # OFF behavior with no override is covered by TestOrderFeedOptOutGate
    # below.
    defaults = dict(
        scan_secs=0.02,
        heartbeat_secs=0.02,
        manifest_flush_secs=0.05,
        alerts_dir=tmp_path / "alerts",
        enable_order_feed=True,
    )
    defaults.update(kwargs)
    return FeedHandler("SIMTEST", tmp_path, fake, **defaults)


class TestOrderIngestStartsUnconditionally:
    async def test_order_stream_opened_with_no_request_needed(self, tmp_path, monkeypatch):
        # Unlike bars, the order channel is not request-gated: it starts the
        # moment the handler runs, because every live host on the account
        # wants it for as long as the handler is alive.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until(lambda: fake.order_calls == ["SIMTEST"])
            manifest_path = feed_dir / protocol.ORDERS_DIR / protocol.MANIFEST_NAME
            await wait_until(lambda: protocol.read_json(manifest_path) is not None)
            manifest = protocol.read_json(manifest_path)
            # No seed/join for orders, by design.
            assert manifest["seed_event"] is None
            assert manifest["join"] is None
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestOrderPublishSubscribe:
    async def test_order_events_mirrored_and_tailed_verbatim(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until(lambda: fake.order_calls == ["SIMTEST"])
            # A pre-attach event: a fresh subscriber must NOT see it (tail
            # from the end only -- no seed/replay for orders).
            fake.order_queue.put_nowait(_order_ev("100001", "ACK"))
            await wait_until(
                lambda: protocol.list_segments(feed_dir / protocol.ORDERS_DIR)
            )
            await asyncio.sleep(0.1)

            client = FeedTailStreamClient(feed_dir, poll_ms=10)
            gen = client.stream_orders("SIMTEST")
            task = asyncio.create_task(collect(gen, 1))
            await asyncio.sleep(0.1)  # consumer attaches at the tail

            live = _order_ev("100002", "FLL")
            fake.order_queue.put_nowait(live)
            got = await task
            await gen.aclose()
            assert got == [live]  # verbatim, pre-attach event never replayed
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_two_hosts_share_one_upstream_order_stream(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until(lambda: fake.order_calls == ["SIMTEST"])
            client_a = FeedTailStreamClient(feed_dir, poll_ms=10)
            client_b = FeedTailStreamClient(feed_dir, poll_ms=10)
            gen_a = client_a.stream_orders("SIMTEST")
            gen_b = client_b.stream_orders("SIMTEST")
            task_a = asyncio.create_task(collect(gen_a, 1))
            task_b = asyncio.create_task(collect(gen_b, 1))
            await asyncio.sleep(0.1)
            live = _order_ev("200001", "FLL")
            fake.order_queue.put_nowait(live)
            got_a = await task_a
            got_b = await task_b
            await gen_a.aclose()
            await gen_b.aclose()
            assert got_a == got_b == [live]
            # ONE upstream subscription served both consumers.
            assert fake.order_calls == ["SIMTEST"]
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestOrderIngestHealthSharesHeartbeat:
    async def test_bar_only_and_order_only_mirror_io_failure_both_freeze_heartbeat(
        self, tmp_path, monkeypatch
    ):
        # REGRESSION GUARD: a bar-only mirror-IO failure and an
        # order-only mirror-IO failure must both freeze the SAME shared
        # heartbeat -- consumers of EITHER channel must read a dead handler
        # the same way, never just one channel going quietly dark.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        heartbeat = feed_dir / protocol.HEARTBEAT_NAME
        try:
            write_request(feed_dir, "ESM26", "15", "Minute", None)
            fake.queue.put_nowait(_bar_ev("2026-06-10T14:00:00Z", "4120.0"))
            await wait_until(lambda: KEY in handler._ingests and handler._ingests[KEY].cur_ts)
            await wait_until(lambda: fake.order_calls == ["SIMTEST"])

            # --- ORDER-ONLY failure freezes the heartbeat ---
            real_order_append = handler._order_writer.append
            broken = {"on": True}

            def flaky_order_append(ev):
                if broken["on"]:
                    raise OSError("disk full (orders)")
                return real_order_append(ev)

            monkeypatch.setattr(handler._order_writer, "append", flaky_order_append)
            fake.order_queue.put_nowait(_order_ev("300001"))
            await wait_until(lambda: "orders" in handler._mirror_io_errors)
            frozen = os.stat(heartbeat).st_mtime
            await asyncio.sleep(0.2)
            assert os.stat(heartbeat).st_mtime == frozen  # FROZEN by an orders-only failure

            broken["on"] = False
            fake.order_queue.put_nowait(_order_ev("300002"))
            await wait_until(lambda: "orders" not in handler._mirror_io_errors, timeout=10.0)
            await wait_until(lambda: os.stat(heartbeat).st_mtime > frozen, timeout=10.0)

            # --- BAR-only failure ALSO freezes the SAME heartbeat ---
            st = handler._ingests[KEY]
            real_bar_append = st.writer.append
            broken2 = {"on": True}

            def flaky_bar_append(ev):
                if broken2["on"]:
                    raise OSError("disk full (bars)")
                return real_bar_append(ev)

            monkeypatch.setattr(st.writer, "append", flaky_bar_append)
            fake.queue.put_nowait(_bar_ev("2026-06-10T14:00:00Z", "4121.0"))
            await wait_until(lambda: KEY in handler._mirror_io_errors)
            frozen2 = os.stat(heartbeat).st_mtime
            await asyncio.sleep(0.2)
            assert os.stat(heartbeat).st_mtime == frozen2  # FROZEN by a bar-only failure too

            broken2["on"] = False
            fake.queue.put_nowait(_bar_ev("2026-06-10T14:00:00Z", "4122.0"))
            await wait_until(lambda: KEY not in handler._mirror_io_errors, timeout=10.0)
            await wait_until(lambda: os.stat(heartbeat).st_mtime > frozen2, timeout=10.0)
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestOrderRetention:
    async def test_order_segments_swept_sparing_active(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(
            tmp_path,
            fake,
            segment_max_bytes=1,  # every event lands in its own segment
            retention_hours=0,  # everything but the active segment is immediately stale
            retention_sweep_secs=0.05,
        )
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        orders_dir = feed_dir / protocol.ORDERS_DIR
        try:
            await wait_until(lambda: fake.order_calls == ["SIMTEST"])
            fake.order_queue.put_nowait(_order_ev("1"))  # seg 1
            fake.order_queue.put_nowait(_order_ev("2"))  # seg 2 = active
            await wait_until(
                lambda: protocol.list_segments(orders_dir)
                and max(protocol.list_segments(orders_dir)) == 2
            )
            # Segment 1 falls off retention; 2 (active) is spared -- unlike a
            # bar key, orders have no "join" segment to also spare.
            await wait_until(lambda: protocol.list_segments(orders_dir) == [2])
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestOrderShutdown:
    async def test_planned_stop_flushes_and_closes_order_writer(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        fake = FakeStreamClient()
        handler = make_handler(tmp_path, fake)
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        await wait_until(lambda: fake.order_calls == ["SIMTEST"])
        fake.order_queue.put_nowait(_order_ev("1"))
        await wait_until(lambda: handler._order_writer.seq_highwater >= 1)
        (feed_dir / protocol.STOP_MARKER_NAME).touch()
        await asyncio.wait_for(run_task, timeout=5.0)  # clean return, not a crash
        manifest = protocol.read_json(
            feed_dir / protocol.ORDERS_DIR / protocol.MANIFEST_NAME
        )
        assert manifest["seq_highwater"] == 1


class TestOrderFeedOptOutGate:
    """Order-feed opt-in gate: without the explicit opt-in, a handler must be
    byte-identical to a pre-order-ingest handler -- no orders/
    directory, no ingest task, no stream_orders call at all. Proven both via
    the constructor default (reads TS_ORDER_FEED_INGEST) and via an explicit
    False override."""

    async def test_default_off_env_absent_never_touches_orders(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        monkeypatch.delenv("TS_ORDER_FEED_INGEST", raising=False)
        fake = FakeStreamClient()
        handler = FeedHandler(
            "SIMTEST", tmp_path, fake,
            scan_secs=0.02, heartbeat_secs=0.02, manifest_flush_secs=0.05,
            alerts_dir=tmp_path / "alerts",
        )  # enable_order_feed NOT passed -- resolves from (absent) env -> False
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until((feed_dir / protocol.META_NAME).exists)
            await asyncio.sleep(0.2)  # many scan/heartbeat/retention cadences
            assert fake.order_calls == []  # stream_orders was NEVER called
            assert not (feed_dir / protocol.ORDERS_DIR).exists()
            assert handler._order_writer is None
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_env_opt_in_starts_the_order_ingest(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        monkeypatch.setenv("TS_ORDER_FEED_INGEST", "1")
        fake = FakeStreamClient()
        handler = FeedHandler(
            "SIMTEST", tmp_path, fake,
            scan_secs=0.02, heartbeat_secs=0.02, manifest_flush_secs=0.05,
            alerts_dir=tmp_path / "alerts",
        )  # enable_order_feed NOT passed -- resolves from env "1" -> True
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until(lambda: fake.order_calls == ["SIMTEST"])
            assert (feed_dir / protocol.ORDERS_DIR).exists()
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)

    async def test_explicit_false_overrides_a_truthy_env(self, tmp_path, monkeypatch):
        # The constructor arg wins over env when given explicitly (tests, or
        # a caller with its own config source that must not be shadowed by
        # an ambient var it doesn't control).
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        monkeypatch.setenv("TS_ORDER_FEED_INGEST", "1")
        fake = FakeStreamClient()
        handler = FeedHandler(
            "SIMTEST", tmp_path, fake,
            scan_secs=0.02, heartbeat_secs=0.02, manifest_flush_secs=0.05,
            alerts_dir=tmp_path / "alerts",
            enable_order_feed=False,
        )
        run_task = asyncio.create_task(handler.run())
        feed_dir = tmp_path / "SIMTEST"
        try:
            await wait_until((feed_dir / protocol.META_NAME).exists)
            await asyncio.sleep(0.2)
            assert fake.order_calls == []
            assert handler._order_writer is None
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
