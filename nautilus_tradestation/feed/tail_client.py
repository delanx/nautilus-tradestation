"""
Consumer-side tail client for the feed transport.

``FeedTailStreamClient`` is a drop-in replacement for
``TradeStationStreamClient`` on the data path: ``stream_bars`` poll-tails the
per-key JSONL mirror written by the account's feed handler instead of opening
its own TradeStation SSE connection. Event dicts cross VERBATIM (values are the
exact strings TS sent). The other ``stream_*`` methods delegate to a lazily
built real client (no client code subscribes ticks; delegation keeps the
stream-client contract whole).

Feed exceptions are plain ``RuntimeError`` subclasses so the data client's
unchanged supervise loop catches them, marks the feed degraded, backs off, and
re-enters the stream.
"""

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.keys import stream_key

_log = logging.getLogger(__name__)

_TAIL_ALIGN_BYTES = 65_536


class FeedProxyError(RuntimeError):
    """Base error for feed-proxy streaming failures."""


class FeedGapError(FeedProxyError):
    """A seq gap was observed (hard error): the cursor is cleared so the next
    entry takes the fresh-start path (seed + join), bounding loss exactly like
    a direct-mode SSE outage."""


class FeedHandlerDeadError(FeedProxyError):
    """The handler heartbeat went stale while the stream was idle. The cursor
    is KEPT, so recovery after a handler restart is a resume-replay with zero
    loss for everything that reached the mirror."""


class FeedSubscribeTimeoutError(FeedProxyError):
    """No manifest appeared for the requested key within the subscribe timeout
    (loud — never a silent no-data feed)."""


class FeedTailStreamClient:
    """Poll-tails per-key JSONL segments written by the account feed handler.

    Cursors live on the CLIENT INSTANCE (like ``_last_bar_ts`` on the data
    client) because the supervise loop creates a NEW async generator per
    resubscribe; they are in-memory only — a restarted consumer process is
    cursor-less BY DESIGN (= direct-mode cold-connect parity).
    """

    def __init__(
        self,
        feed_dir: Path,
        *,
        poll_ms: int = 100,
        real_client_factory: Callable[[], object] | None = None,
        subscribe_timeout_secs: float = 30.0,
        idle_check_secs: float = 10.0,
        heartbeat_stale_secs: float = 15.0,
        request_refresh_secs: float = protocol.REQUEST_REFRESH_SECS,
    ) -> None:
        self._feed_dir = Path(feed_dir)
        self._poll_secs = max(int(poll_ms), 1) / 1000.0
        self._real_client_factory = real_client_factory
        self._real_client: object | None = None
        self._subscribe_timeout_secs = subscribe_timeout_secs
        self._idle_check_secs = idle_check_secs
        self._heartbeat_stale_secs = heartbeat_stale_secs
        self._request_refresh_secs = request_refresh_secs
        # key -> (segment, offset, last_seq); set once the first event of a
        # stream is consumed, cleared on a seq gap (fresh-start fallback).
        self._cursors: dict[str, tuple[int, int, int]] = {}

    @classmethod
    def from_env(
        cls,
        value: str,
        *,
        poll_ms: int = 100,
        real_client_factory: Callable[[], object] | None = None,
    ) -> "FeedTailStreamClient":
        """Build from the ``TS_FEED_PROXY`` value, e.g. ``jsonl:<abs path>``.

        Unknown scheme prefixes and feed dirs outside ``%LOCALAPPDATA%`` are
        hard errors at construction.
        """
        if not value.startswith("jsonl:"):
            raise RuntimeError(
                f"TS_FEED_PROXY has unknown scheme: {value!r} (expected 'jsonl:<abs path>')"
            )
        raw_path = value[len("jsonl:") :]
        if not raw_path:
            raise RuntimeError("TS_FEED_PROXY 'jsonl:' requires a path")
        feed_dir = protocol.validate_feed_dir(Path(raw_path))
        return cls(feed_dir, poll_ms=poll_ms, real_client_factory=real_client_factory)

    # -- BARS (proxied) --------------------------------------------------------

    async def stream_bars(
        self,
        symbol: str,
        interval: str,
        unit: str,
        session_template: str | None = None,
    ) -> AsyncIterator[dict]:
        """Stream bar events for a symbol from the local JSONL mirror.

        Same yield contract as ``TradeStationStreamClient.stream_bars``: every
        upstream event (Historical seeds AND in-progress RealTime updates), in
        order, verbatim. The manifest seed is yielded first on a fresh start
        with ``_ts_feed_seed: true`` added (the only synthesized yield).
        """
        key = stream_key(symbol, interval, unit, session_template)
        key_dir = self._feed_dir / protocol.BARS_DIR / key
        self._write_request(key, symbol, interval, unit, session_template)
        manifest = await self._await_manifest(key, key_dir)

        cursor = self._cursors.get(key)
        if cursor is not None:
            # RESUME: generator re-entered after a raise, consumer process still alive.
            segment, offset, last_seq = cursor
            try:
                size = os.stat(key_dir / protocol.segment_name(segment)).st_size
            except OSError:
                size = None
            if size is None or offset > size:
                self._cursors.pop(key, None)
                raise FeedGapError(
                    f"feed resume for {key}: cursor segment={segment} offset={offset} is gone "
                    f"(size={size}); next entry takes a fresh start"
                )
            _log.info(
                f"feed resume for {key}: segment={segment} offset={offset} last_seq={last_seq}"
            )
        else:
            # FRESH START: seed (if any) once, then tail from the end.
            seed = manifest.get("seed_event")
            if isinstance(seed, dict):
                yield dict(seed, _ts_feed_seed=True)
            segment, offset, last_seq = self._fresh_start_position(key, key_dir)

        idle_since = time.monotonic()
        last_lease = time.monotonic()
        while True:
            seg_path = key_dir / protocol.segment_name(segment)
            try:
                size = os.stat(seg_path).st_size
            except OSError:
                size = None

            if size is None:
                # Missing is legit while waiting for the first/just-rolled
                # segment; but with NEWER segments on disk it can never come
                # back (numbering is monotonic) — never poll it silently.
                newer = [n for n in protocol.list_segments(key_dir) if n > segment]
                if newer:
                    self._cursors.pop(key, None)
                    raise FeedGapError(
                        f"feed segment {segment} for {key} disappeared (newer segments "
                        f"{newer[:3]} exist); next entry takes a fresh start"
                    )

            progressed = False
            rolled = False
            if size is not None and size > offset:
                with open(seg_path, "rb") as f:
                    f.seek(offset)
                    data = f.read(size - offset)
                raw_lines, consumed = protocol.split_terminated(data)
                if consumed:
                    progressed = True
                    idle_since = time.monotonic()
                pos = offset
                for raw in raw_lines:
                    line_len = len(raw) + 1
                    decoded = protocol.decode_line(raw)
                    kind = decoded[0]
                    if kind == protocol.ROLL:
                        # Final line of the segment: hop without consulting the
                        # manifest (manifest.active_segment is recovery, not hot path).
                        segment = decoded[1]
                        offset = 0
                        if last_seq is not None:
                            self._cursors[key] = (segment, offset, last_seq)
                        rolled = True
                        break
                    if kind == protocol.EVENT:
                        seq, ev = decoded[1], decoded[2]
                        if last_seq is not None and seq <= last_seq:
                            # Transport dedup (at-least-once); layer 2 is the
                            # unchanged TimeStamp state machine in the data client.
                            offset = pos + line_len
                            self._cursors[key] = (segment, offset, last_seq)
                        elif last_seq is None or seq == last_seq + 1:
                            offset = pos + line_len
                            last_seq = seq
                            self._cursors[key] = (segment, offset, last_seq)
                            yield ev
                        else:
                            # I5: a visible seq gap is a HARD error. Clear the
                            # cursor so the next entry takes the fresh-start
                            # path (seed gap-fill, like a direct-mode outage).
                            self._cursors.pop(key, None)
                            raise FeedGapError(
                                f"feed seq gap for {key}: expected {last_seq + 1}, got {seq} "
                                f"(segment {segment} offset {pos})"
                            )
                    else:
                        _log.warning(
                            f"feed: skipping unparseable line for {key} at segment {segment} "
                            f"offset {pos} ({decoded[1]}); seq continuity check still applies"
                        )
                        offset = pos + line_len
                    pos += line_len
                if rolled:
                    continue

            if not progressed:
                if time.monotonic() - idle_since > self._idle_check_secs:
                    heartbeat = self._feed_dir / protocol.HEARTBEAT_NAME
                    try:
                        heartbeat_age = time.time() - os.stat(heartbeat).st_mtime
                    except OSError:
                        heartbeat_age = float("inf")
                    if heartbeat_age > self._heartbeat_stale_secs:
                        # Cursor KEPT: after the handler restarts, re-entry
                        # resume-replays everything that reached the mirror.
                        raise FeedHandlerDeadError(
                            f"feed handler heartbeat stale ({heartbeat_age:.0f}s) while "
                            f"{key} idle ({self._feed_dir})"
                        )
                    idle_since = time.monotonic()  # alive but quiet: re-check periodically
            if time.monotonic() - last_lease >= self._request_refresh_secs:
                # The request file is a lease: re-stamp it so the handler can
                # retire keys NO live consumer wants (protocol.REQUEST_TTL_SECS).
                self._write_request(key, symbol, interval, unit, session_template)
                last_lease = time.monotonic()
            await asyncio.sleep(self._poll_secs)

    # -- OTHER STREAMS (delegated to the real client) ---------------------------

    async def stream_quotes(self, symbols: str) -> AsyncIterator[dict]:
        async for event in self._real().stream_quotes(symbols):
            yield event

    async def stream_orders(self, account_id: str) -> AsyncIterator[dict]:
        # Defensive: the exec client owns its own stream client (per-consumer, direct).
        async for event in self._real().stream_orders(account_id):
            yield event

    async def stream_market_depth(self, symbol: str) -> AsyncIterator[dict]:
        async for event in self._real().stream_market_depth(symbol):
            yield event

    # -- INTERNAL ---------------------------------------------------------------

    def _real(self):
        if self._real_client is None:
            if self._real_client_factory is None:
                raise FeedProxyError(
                    "FeedTailStreamClient has no real_client_factory for delegated streams"
                )
            self._real_client = self._real_client_factory()
        return self._real_client

    def _write_request(
        self, key: str, symbol: str, interval: str, unit: str, session_template: str | None
    ) -> None:
        """Drop/refresh the subscription request (idempotent; cross-consumer
        collisions are benign — identical semantic content). Re-stamped every
        ``request_refresh_secs`` while streaming: the file doubles as the
        liveness lease the handler's key retirement checks."""
        requests_dir = self._feed_dir / protocol.REQUESTS_DIR
        requests_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "v": protocol.PROTOCOL_VERSION,
            "symbol": symbol,
            "interval": interval,
            "unit": unit,
            "session_template": session_template,
            "requested_by": os.getpid(),
            "ts_utc": datetime.now(timezone.utc).isoformat(),
        }
        protocol.write_json_atomic(requests_dir / f"{key}{protocol.REQUEST_SUFFIX}", payload)

    async def _await_manifest(self, key: str, key_dir: Path) -> dict:
        manifest_path = key_dir / protocol.MANIFEST_NAME
        deadline = time.monotonic() + self._subscribe_timeout_secs
        while True:
            manifest = protocol.read_json(manifest_path)
            if manifest is not None:
                return manifest
            if time.monotonic() >= deadline:
                raise FeedSubscribeTimeoutError(
                    f"no manifest for feed key {key} within "
                    f"{self._subscribe_timeout_secs:.0f}s ({manifest_path})"
                )
            await asyncio.sleep(self._poll_secs)

    def _fresh_start_position(self, key: str, key_dir: Path) -> tuple[int, int, int | None]:
        """Resolve the (segment, offset, last_seq) a fresh subscriber tails from.

        ALWAYS the end of the newest segment — exact direct-connect parity:
        the manifest seed (yielded by the caller) plays TS's barsback=1
        connect seed, and the unchanged bar state machine initializes on the
        first event that arrives AFTER subscribe, exactly like a direct SSE
        connect. ``manifest.join`` is deliberately NOT replayed: every SSE
        update is a full bar snapshot, so join replay adds nothing a direct
        connect would have — and a STALE join (subscribing during an idle
        stretch, e.g. overnight/pre-open, where the final bar's close was
        never confirmed) would replay the previous session's last bar into
        the state machine, which then emits it as a live closed bar at the
        next session's first event (a G1 break a direct node never shows).
        """
        segments = protocol.list_segments(key_dir)
        if not segments:
            return 1, 0, None
        segment = segments[-1]
        seg_path = key_dir / protocol.segment_name(segment)
        try:
            size = os.stat(seg_path).st_size
        except OSError:
            return segment, 0, None
        # Align to the last line terminator so we never attach mid-line.
        with open(seg_path, "rb") as f:
            f.seek(max(0, size - _TAIL_ALIGN_BYTES))
            chunk = f.read()
        base = size - len(chunk)
        cut = chunk.rfind(b"\n")
        if cut < 0:
            return segment, base, None
        # A pending roll line (writer crashed between the roll write and the
        # next segment's creation) must be followed, not attached BEHIND:
        # appends resume in the roll target, never in this segment.
        start = chunk.rfind(b"\n", 0, cut)
        decoded = protocol.decode_line(chunk[start + 1 : cut])
        if decoded[0] == protocol.ROLL:
            return decoded[1], 0, None
        return segment, base + cut + 1, None
