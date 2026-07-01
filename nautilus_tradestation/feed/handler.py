"""
Per-account feed handler for the feed transport.

``FeedHandler`` owns ALL TradeStation SSE bar subscriptions for one SIM
account's fleet, reusing the existing ``TradeStationStreamClient`` (its
infinite-reconnect loop is proven) for the SSE side, and appends every event
verbatim to the per-key JSONL mirror that consumers tail.

The handler synthesizes exactly ONE piece of state — ``manifest.seed_event``,
the final update of the most recently COMPLETED bar (what TS barsback=1 would
send a fresh direct connection). A seed comes from:

(a) an upstream ``Status="Historical"`` event (handler connect/reconnect seed),
    stored verbatim; or
(b) on close detection ONLY (an event with a NEW TimeStamp proves the previous
    bar completed): a copy of the previous bar's last update with ``Status``
    re-stamped to ``"Historical"`` and ``"_ts_feed_seed": true`` added.

HARD RULE: the handler must never place an in-progress snapshot in
``seed_event`` — a later TimeStamp is the only proof of completion.
"""

import asyncio
import logging
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.feed.mirror import MirrorWriter

_log = logging.getLogger(__name__)


@dataclass
class _IngestState:
    """Per-key ingest bookkeeping (close-detection METADATA; never alters the stream)."""

    key: str
    symbol: str
    interval: str
    unit: str
    session_template: str | None
    writer: MirrorWriter
    started: float = 0.0  # time.time() at ingest start (retirement grace)
    seed_event: dict | None = None
    join: tuple[int, int, int] | None = None  # (segment, offset, seq) of current bar's 1st event
    last_close_ts: str = ""
    cur_ts: str = ""
    last_event: dict | None = None


class FeedHandler:
    """One process per SIM account: ingest TS SSE bars, publish via JSONL mirror.

    Parameters
    ----------
    account_id : str
        The SIM account this handler serves (names the feed directory).
    feed_root : Path
        Feed root (``%LOCALAPPDATA%/<app>/feed``); the handler works under
        ``<feed_root>/<account_id>/``. Must be on local disk.
    stream_client : TradeStationStreamClient
        The upstream SSE client (or a compatible fake in tests); only its
        ``stream_bars`` is used.

    """

    def __init__(
        self,
        account_id: str,
        feed_root: Path,
        stream_client,
        *,
        scan_secs: float = 2.0,
        heartbeat_secs: float = 2.0,
        segment_max_bytes: int = protocol.SEGMENT_MAX_BYTES,
        retention_hours: int = 48,
        manifest_flush_secs: float = 5.0,
        retention_sweep_secs: float = 3600.0,
        request_ttl_secs: float = protocol.REQUEST_TTL_SECS,
        alerts_dir: Path | None = None,
    ) -> None:
        self._account_id = account_id
        self._account_dir = protocol.validate_feed_dir(Path(feed_root) / account_id)
        self._stream_client = stream_client
        self._scan_secs = scan_secs
        self._heartbeat_secs = heartbeat_secs
        self._segment_max_bytes = segment_max_bytes
        self._retention_hours = retention_hours
        self._manifest_flush_secs = manifest_flush_secs
        self._retention_sweep_secs = retention_sweep_secs
        self._request_ttl_secs = request_ttl_secs
        self._alerts_dir = (
            Path(alerts_dir)
            if alerts_dir is not None
            else Path(feed_root).parent / "supervisor" / "alerts"
        )
        self._ingests: dict[str, _IngestState] = {}
        self._ingest_tasks: dict[str, asyncio.Task] = {}
        self._stop_event: asyncio.Event | None = None
        # mirror-IO failure markers ("<key>" = append, "<key>:manifest" =
        # manifest flush): while ANY is set the heartbeat is frozen, so consumers
        # read total data loss as handler-dead, never as a quiet market.
        self._mirror_io_errors: set[str] = set()
        self._lock_fh = None  # exclusive single-writer lock, held for run()'s lifetime

    @property
    def account_dir(self) -> Path:
        return self._account_dir

    def stop(self) -> None:
        """Request a clean shutdown (same effect as the planned-stop marker)."""
        if self._stop_event is not None:
            self._stop_event.set()

    async def run(self) -> None:
        """Run until cancelled, ``stop()``, or the planned-stop marker appears."""
        self._stop_event = asyncio.Event()
        self._acquire_writer_lock()
        try:
            self._prepare()
            loops = [
                asyncio.create_task(self._scan_requests_loop()),
                asyncio.create_task(self._heartbeat_loop()),
                asyncio.create_task(self._manifest_flush_loop()),
                asyncio.create_task(self._retention_loop()),
            ]
            _log.info(f"feed-handler started for {self._account_id} at {self._account_dir}")
            try:
                await self._wait_for_stop()
            finally:
                tasks = loops + list(self._ingest_tasks.values())
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                for st in list(self._ingests.values()):
                    self._write_manifest(st)
                    st.writer.close()
                _log.info(f"feed-handler stopped for {self._account_id}")
        finally:
            self._release_writer_lock()

    # -- LIFECYCLE ---------------------------------------------------------------

    def _acquire_writer_lock(self) -> None:
        """Enforce ONE live handler per account dir (exclusive OS lock, held
        open for the process lifetime; the OS releases it on death, so a stale
        lock cannot outlive a crash). Two concurrent handlers would interleave
        appends into the same segments with independent seq counters — readers
        silently swallow the forked seqs as transport dedup (missed bars, no
        FeedGapError) and the audit mirror is corrupted. Refuse loudly."""
        self._account_dir.mkdir(parents=True, exist_ok=True)
        path = self._account_dir / protocol.LOCK_NAME
        fh = open(path, "a+b")
        try:
            fh.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            raise RuntimeError(
                f"another live feed-handler holds {path} — exactly one handler may "
                f"write the {self._account_id} mirror; refusing to start"
            ) from None
        self._lock_fh = fh

    def _release_writer_lock(self) -> None:
        fh, self._lock_fh = self._lock_fh, None
        if fh is None:
            return
        try:
            fh.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass  # the close (or process exit) releases it regardless
        fh.close()

    def _prepare(self) -> None:
        (self._account_dir / protocol.REQUESTS_DIR).mkdir(parents=True, exist_ok=True)
        (self._account_dir / protocol.BARS_DIR).mkdir(parents=True, exist_ok=True)
        meta = {
            "v": protocol.PROTOCOL_VERSION,
            "pid": os.getpid(),
            "account": self._account_id,
            "started_utc": datetime.now(timezone.utc).isoformat(),
        }
        protocol.write_json_atomic(self._account_dir / protocol.META_NAME, meta)
        self._touch_heartbeat()

    async def _wait_for_stop(self) -> None:
        stop_marker = self._account_dir / protocol.STOP_MARKER_NAME
        while not self._stop_event.is_set():
            if stop_marker.exists():
                _log.info(
                    f"feed-handler {self._account_id}: planned-stop marker found; shutting down"
                )
                return
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._scan_secs)
            except asyncio.TimeoutError:
                pass

    # -- REQUEST SCAN --------------------------------------------------------------

    async def _scan_requests_loop(self) -> None:
        while True:
            try:
                self._scan_requests_once()
            except Exception as exc:  # noqa: BLE001 -- the scan loop must never die
                _log.error(f"feed-handler request scan failed: {exc!r}")
            await asyncio.sleep(self._scan_secs)

    def _scan_requests_once(self) -> None:
        requests_dir = self._account_dir / protocol.REQUESTS_DIR
        now = time.time()
        for path in sorted(requests_dir.glob(f"*{protocol.REQUEST_SUFFIX}")):
            key = path.name[: -len(protocol.REQUEST_SUFFIX)]
            payload = protocol.read_json(path)
            if payload is None:
                continue  # mid-write or transiently unreadable -- next scan retries
            if key in self._ingests:
                self._verify_running_request(self._ingests[key], path, payload)
                continue
            expected = stream_key(
                payload.get("symbol", ""),
                payload.get("interval", ""),
                payload.get("unit", ""),
                payload.get("session_template"),
            )
            if expected != key:
                self._quarantine_request(
                    path,
                    f"recomputes to key {expected!r} != filename stem {key!r}",
                )
                continue
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if now - mtime > self._request_ttl_secs:
                # A retired pod's leftover: live consumers re-stamp their lease
                # every REQUEST_REFRESH_SECS (and on every stream re-entry),
                # so a lease this stale has no subscriber — never re-open an
                # upstream TS SSE subscription for it.
                _log.info(
                    f"feed-handler {self._account_id}: dropping stale leftover request "
                    f"{path.name} (lease {now - mtime:.0f}s old, no running ingest)"
                )
                try:
                    path.unlink()
                except OSError:
                    pass
                continue
            st = self._start_ingest(
                key,
                payload.get("symbol", ""),
                payload.get("interval", ""),
                payload.get("unit", ""),
                payload.get("session_template"),
            )
            task = asyncio.create_task(self._ingest(st))
            task.add_done_callback(lambda t, k=key: self._on_ingest_done(t, k))
            self._ingest_tasks[key] = task
            _log.info(f"feed-handler {self._account_id}: ingest started for {key}")
        self._retire_stale_ingests(requests_dir, now)

    def _verify_running_request(self, st: _IngestState, path: Path, payload: dict) -> None:
        """A request whose key is already ingesting must carry the SAME semantic
        tuple (defense in depth behind the key hash): otherwise a consumer would
        silently tail the WRONG symbol's stream. Quarantine + alert, loudly."""
        requested = (
            str(payload.get("symbol", "")),
            str(payload.get("interval", "")),
            str(payload.get("unit", "")),
            payload.get("session_template") or None,
        )
        running = (st.symbol, st.interval, st.unit, st.session_template or None)
        if requested == running:
            return
        self._quarantine_request(
            path,
            f"payload {requested!r} != running ingest {running!r} for key {st.key!r}",
        )
        self._write_alert(
            f"feed_request_conflict_{self._account_id}_{st.key}.json",
            {
                "kind": "feed_request_conflict",
                "key": st.key,
                "requested": list(requested),
                "running": list(running),
                "detail": (
                    "a request file recomputed to a key another stream already owns; "
                    "the conflicting subscriber is NOT being served"
                ),
            },
        )

    def _quarantine_request(self, path: Path, reason: str) -> None:
        bad = path.with_name(f"{path.name}.bad")
        _log.error(
            f"feed-handler {self._account_id}: request {path.name} {reason}; renaming to .bad"
        )
        try:
            os.replace(path, bad)
        except OSError as exc:
            _log.error(f"feed-handler: could not rename bad request {path.name}: {exc!r}")

    def _retire_stale_ingests(self, requests_dir: Path, now: float) -> None:
        """Retire keys no consumer leases anymore: live consumers re-stamp their
        request every REQUEST_REFRESH_SECS, so an ingest older than the TTL
        whose request is missing or stale past the TTL has zero subscribers —
        drop the upstream SSE subscription and close the writer. Mirror files
        stay for the retention sweep (the audit window is preserved; orphaned
        dirs are deleted there once past retention)."""
        for key, st in list(self._ingests.items()):
            if now - st.started <= self._request_ttl_secs:
                continue  # grace: a just-(re)started ingest is never retired
            req = requests_dir / f"{key}{protocol.REQUEST_SUFFIX}"
            try:
                mtime = req.stat().st_mtime
            except OSError:
                mtime = None
            if mtime is not None and now - mtime <= self._request_ttl_secs:
                continue
            task = self._ingest_tasks.pop(key, None)
            if task is not None:
                task.cancel()
            self._ingests.pop(key, None)
            self._write_manifest(st)
            st.writer.close()
            self._mirror_io_errors.discard(key)
            self._mirror_io_errors.discard(f"{key}:manifest")
            try:
                req.unlink()
            except OSError:
                pass
            _log.info(
                f"feed-handler {self._account_id}: retired {key} "
                f"(no consumer lease within {self._request_ttl_secs:.0f}s)"
            )

    def _start_ingest(
        self, key: str, symbol: str, interval: str, unit: str, session_template: str | None
    ) -> _IngestState:
        """Create the writer + state and write the initial manifest synchronously
        (a subscriber's 30s manifest wait must not depend on upstream data)."""
        key_dir = self._account_dir / protocol.BARS_DIR / key
        writer = MirrorWriter(key_dir, segment_max_bytes=self._segment_max_bytes)
        st = _IngestState(
            key=key,
            symbol=symbol,
            interval=interval,
            unit=unit,
            session_template=session_template,
            writer=writer,
            started=time.time(),
        )
        prior = protocol.read_json(key_dir / protocol.MANIFEST_NAME)
        if prior:
            # Handler restart: keep the prior seed/join — the mirrored files are
            # untouched, so the join offsets remain valid and the seed still
            # describes the last completed bar until upstream replaces it.
            seed = prior.get("seed_event")
            if isinstance(seed, dict):
                st.seed_event = seed
                st.last_close_ts = str(prior.get("last_close_ts") or "")
            join = prior.get("join")
            if isinstance(join, dict):
                try:
                    st.join = (int(join["segment"]), int(join["offset"]), int(join["seq"]))
                except (KeyError, TypeError, ValueError):
                    st.join = None
        self._ingests[key] = st
        self._write_manifest(st)
        return st

    # -- INGEST ---------------------------------------------------------------------

    async def _ingest(self, st: _IngestState) -> None:
        """Mirror one upstream bar stream. The upstream client never raises
        (infinite reconnect), but supervise anyway: an ingest task must never
        die silently — on unexpected exception log, back off, re-enter."""
        retry_delay = 1.0
        while True:
            try:
                async for ev in self._stream_client.stream_bars(
                    symbol=st.symbol,
                    interval=st.interval,
                    unit=st.unit,
                    session_template=st.session_template,
                ):
                    retry_delay = 1.0  # a healthy event resets the backoff
                    try:
                        seq, segment, offset = st.writer.append(ev)
                    except OSError:
                        # Mirror append failing (e.g. disk full) = total data
                        # loss for every consumer on this key. Freeze the heartbeat
                        # so consumers raise FeedHandlerDeadError (never read the
                        # silence as a quiet market) and the supervisor's
                        # stale-heartbeat check restarts/escalates us.
                        self._mirror_io_errors.add(st.key)
                        raise
                    self._mirror_io_errors.discard(st.key)
                    self._update_close_detection(st, ev, seq, segment, offset)
                # The real upstream client only ends on cancel; treat a clean
                # end (e.g. a fake/test source) like a drop.
                _log.warning(
                    f"feed-handler ingest stream for {st.key} ended; "
                    f"re-entering in {retry_delay:.0f}s"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2.0, 60.0)
            except asyncio.CancelledError:
                _log.info(f"feed-handler ingest stopped for {st.key}")
                return
            except Exception as exc:  # noqa: BLE001 -- supervise: never die silently
                _log.error(
                    f"feed-handler ingest for {st.key} failed: {exc!r}; "
                    f"re-entering in {retry_delay:.0f}s"
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2.0, 60.0)

    def _update_close_detection(
        self, st: _IngestState, ev: dict, seq: int, segment: int, offset: int
    ) -> None:
        """Track bar-completion METADATA for the manifest (never alters the stream)."""
        ev_ts = ev.get("TimeStamp", "")
        if not ev_ts:
            return
        seed_ts = (st.seed_event or {}).get("TimeStamp", "")
        if ev.get("Status") == "Historical":
            # Seed rule (a): upstream connect/reconnect seed, stored verbatim.
            # Newest TimeStamp wins; >= so the authoritative upstream copy of a
            # bar replaces a synthesized rule-(b) seed with the same TimeStamp.
            if ev_ts >= seed_ts:
                st.seed_event = dict(ev)
                st.last_close_ts = ev_ts
                self._write_manifest(st)
            return
        if ev_ts != st.cur_ts:
            # Seed rule (b): may ONLY fire on close detection — a NEW TimeStamp
            # is the proof the previous bar completed. Never an in-progress
            # snapshot; > keeps an upstream verbatim seed of the same bar.
            if st.cur_ts and st.last_event is not None and st.cur_ts > seed_ts:
                seed = dict(st.last_event)
                seed["Status"] = "Historical"
                seed["_ts_feed_seed"] = True
                st.seed_event = seed
                st.last_close_ts = st.cur_ts
            # join = position of the FIRST event of the now-current bar. Audit
            # metadata ONLY: fresh subscribers never replay it (a stale join is
            # the previous session's final bar — replaying it breaks G1); see
            # tail_client._fresh_start_position.
            st.join = (segment, offset, seq)
            st.cur_ts = ev_ts
            st.last_event = ev
            self._write_manifest(st)
        else:
            st.last_event = ev

    def _write_manifest(self, st: _IngestState) -> None:
        st.writer.write_manifest(
            seed_event=st.seed_event, join=st.join, last_close_ts=st.last_close_ts
        )

    def _on_ingest_done(self, task: asyncio.Task, key: str) -> None:
        """Backstop: retrieve an escaped exception; drop the dead ingest so the
        next request scan respawns it (request files persist on disk)."""
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception:  # noqa: BLE001 -- retrieval itself must never raise
            return
        if exc is None:
            return
        _log.error(f"feed-handler ingest task for {key} ENDED with {exc!r}; respawning on scan")
        self._ingest_tasks.pop(key, None)
        st = self._ingests.pop(key, None)
        if st is not None:
            st.writer.close()

    # -- HOUSEKEEPING -----------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                if self._mirror_io_errors:
                    # The heartbeat asserts "the mirror is being written", not
                    # bare process liveness: while any append/manifest IO is
                    # failing it stays FROZEN so consumers and the supervisor see
                    # a dead handler, never a quiet market.
                    _log.error(
                        f"feed-handler heartbeat FROZEN: mirror IO failing for "
                        f"{sorted(self._mirror_io_errors)}"
                    )
                else:
                    self._touch_heartbeat()
            except Exception as exc:  # noqa: BLE001
                _log.error(f"feed-handler heartbeat touch failed: {exc!r}")
            await asyncio.sleep(self._heartbeat_secs)

    def _touch_heartbeat(self) -> None:
        heartbeat = self._account_dir / protocol.HEARTBEAT_NAME
        try:
            os.utime(heartbeat, None)
        except FileNotFoundError:
            heartbeat.touch()

    async def _manifest_flush_loop(self) -> None:
        """Periodic flush so seq_highwater/active_segment stay current (and any
        contention-deferred manifest write is retried)."""
        while True:
            await asyncio.sleep(self._manifest_flush_secs)
            for st in list(self._ingests.values()):
                try:
                    self._write_manifest(st)
                except OSError as exc:
                    # Real IO failure (contention is retried inside
                    # write_json_atomic and never raises): freeze the heartbeat.
                    self._mirror_io_errors.add(f"{st.key}:manifest")
                    _log.error(f"feed-handler manifest flush failed for {st.key}: {exc!r}")
                except Exception as exc:  # noqa: BLE001
                    _log.error(f"feed-handler manifest flush failed for {st.key}: {exc!r}")
                else:
                    self._mirror_io_errors.discard(f"{st.key}:manifest")

    async def _retention_loop(self) -> None:
        while True:
            await asyncio.sleep(self._retention_sweep_secs)
            try:
                self._retention_sweep_once()
            except Exception as exc:  # noqa: BLE001
                _log.error(f"feed-handler retention sweep failed: {exc!r}")

    def _retention_sweep_once(self) -> None:
        """Delete segments older than retention EXCEPT the active segment and the
        join segment. A reader-pinned handle defers the delete; pinned past 2x
        retention writes an alert file. Orphaned key dirs (retired keys with no
        ingest, no lease, nothing newer than retention) are deleted whole."""
        now = time.time()
        cutoff = now - self._retention_hours * 3600.0
        pin_cutoff = now - 2.0 * self._retention_hours * 3600.0
        self._sweep_orphaned_key_dirs(cutoff)
        for st in list(self._ingests.values()):
            key_dir = st.writer.key_dir
            active = st.writer.active_segment
            join_segment = st.join[0] if st.join else None
            for number in protocol.list_segments(key_dir):
                if number == active or number == join_segment:
                    continue
                path = key_dir / protocol.segment_name(number)
                try:
                    mtime = path.stat().st_mtime
                except OSError:
                    continue
                if mtime >= cutoff:
                    continue
                try:
                    path.unlink()
                    _log.info(f"feed-handler retention: deleted {path}")
                except PermissionError:
                    # A lagging reader pins the handle — skip, retry next sweep.
                    if mtime < pin_cutoff:
                        self._write_pin_alert(st.key, number, mtime)
                except OSError as exc:
                    _log.warning(f"feed-handler retention: could not delete {path}: {exc!r}")

    def _sweep_orphaned_key_dirs(self, cutoff: float) -> None:
        """Delete mirror dirs of retired keys: no running ingest, no request
        lease, and nothing modified since the retention cutoff (the audit
        window is preserved until then)."""
        bars_dir = self._account_dir / protocol.BARS_DIR
        requests_dir = self._account_dir / protocol.REQUESTS_DIR
        try:
            key_dirs = [p for p in bars_dir.iterdir() if p.is_dir()]
        except OSError:
            return
        for key_dir in key_dirs:
            key = key_dir.name
            if key in self._ingests:
                continue
            if (requests_dir / f"{key}{protocol.REQUEST_SUFFIX}").exists():
                continue
            try:
                newest = max((p.stat().st_mtime for p in key_dir.iterdir()), default=0.0)
            except OSError:
                continue
            if newest >= cutoff:
                continue
            try:
                shutil.rmtree(key_dir)
                _log.info(f"feed-handler retention: deleted orphaned mirror dir {key_dir}")
            except OSError as exc:
                # A lagging reader pins a handle — retry next sweep.
                _log.warning(
                    f"feed-handler retention: could not delete orphaned {key_dir}: {exc!r}"
                )

    def _write_pin_alert(self, key: str, segment_number: int, mtime: float) -> None:
        self._write_alert(
            f"feed_{self._account_id}_{key}_{segment_number:08d}.json",
            {
                "kind": "feed_segment_pinned",
                "key": key,
                "segment": segment_number,
                "segment_mtime_utc": datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat(),
                "detail": "segment pinned by an open handle past 2x retention",
            },
        )

    def _write_alert(self, name: str, fields: dict) -> None:
        """Write one alert file (atomic; never raises — alerts must not kill loops)."""
        try:
            self._alerts_dir.mkdir(parents=True, exist_ok=True)
            alert = {
                "v": protocol.PROTOCOL_VERSION,
                "account": self._account_id,
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                **fields,
            }
            protocol.write_json_atomic(self._alerts_dir / name, alert)
            _log.error(
                f"feed-handler alert {fields.get('kind')}: written to {self._alerts_dir / name}"
            )
        except Exception as exc:  # noqa: BLE001
            _log.error(f"feed-handler: failed to write alert {name}: {exc!r}")
