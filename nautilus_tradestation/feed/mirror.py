"""
Handler-side JSONL mirror writer for the feed transport.

``MirrorWriter`` owns one stream key's segment files and manifest. Appends are
synchronous inline IO (local NTFS, sub-millisecond at TS bar-update rates) with
ONE ``write()`` + ``flush()`` per line and NO per-line fsync — same-machine
readers see flushed writes via the unified file cache; machine-crash tail loss
is backstopped by the upstream barsback=1 seed, not by fsync.
"""

import logging
import os
from pathlib import Path

from nautilus_tradestation.feed import protocol

_log = logging.getLogger(__name__)

_TAIL_SCAN_BYTES = 65_536


class MirrorWriter:
    """Append-only JSONL segment writer for one stream key (handler side).

    Startup performs torn-tail repair (a crash mid-append must not let the next
    append glue onto the fragment) and seq recovery: the next assigned seq is
    ``max(manifest.seq_highwater, max seq scanned from the segment tails) + 1``,
    so a crash between append and manifest flush cannot reuse a seq.
    """

    def __init__(
        self, key_dir: Path, *, segment_max_bytes: int = protocol.SEGMENT_MAX_BYTES
    ) -> None:
        self._key_dir = Path(key_dir)
        self._key = self._key_dir.name
        self._segment_max = segment_max_bytes
        self._key_dir.mkdir(parents=True, exist_ok=True)

        manifest = protocol.read_json(self._key_dir / protocol.MANIFEST_NAME) or {}
        try:
            manifest_active = int(manifest.get("active_segment") or 0)
        except (TypeError, ValueError):
            manifest_active = 0
        segments = protocol.list_segments(self._key_dir)
        # max() keeps segment numbering monotonic even if segment files were
        # removed out-of-band — a segment number is never reused.
        self._segment = max(segments[-1] if segments else 1, manifest_active, 1)

        path = self._segment_path(self._segment)
        path.touch()
        self._repair_torn_tail(path)
        roll_target = self._pending_roll_target(path)
        if roll_target is not None:
            # Crash between writing the roll line and creating the next segment:
            # appending here would hide events behind the roll line. Honor it.
            self._segment = roll_target
            path = self._segment_path(roll_target)
            path.touch()
            self._repair_torn_tail(path)

        self._seq = self.recover_seq(self._key_dir)
        self._fh = open(path, "ab")
        self._fh.seek(0, os.SEEK_END)
        self._offset = self._fh.tell()

    @property
    def key_dir(self) -> Path:
        return self._key_dir

    @property
    def active_segment(self) -> int:
        return self._segment

    @property
    def seq_highwater(self) -> int:
        return self._seq

    @property
    def offset(self) -> int:
        return self._offset

    def append(self, ev: dict) -> tuple[int, int, int]:
        """Append one event line; returns ``(seq, segment, line_start_offset)``.

        Rotates first when the active segment exceeds ``segment_max_bytes``
        (a final ``{"roll": N}`` line, then the next segment file).
        """
        if self._offset > self._segment_max:
            self._rotate()
        seq = self._seq + 1
        line = protocol.encode_event_line(seq, ev)
        start = self._offset
        self._fh.write(line)
        self._fh.flush()
        self._seq = seq
        self._offset += len(line)
        return seq, self._segment, start

    def write_manifest(
        self,
        *,
        seed_event: dict | None,
        join: tuple[int, int, int] | None,
        last_close_ts: str,
    ) -> bool:
        """Write the manifest atomically (tmp + ``os.replace``, contention-retried).

        ``join`` is ``(segment, offset, seq)`` of the first event of the current
        in-progress bar, or None. Returns False if the replace stayed blocked
        (caller retries on the periodic flush).
        """
        manifest = {
            "v": protocol.PROTOCOL_VERSION,
            "key": self._key,
            "active_segment": self._segment,
            "seq_highwater": self._seq,
            "seed_event": seed_event,
            "join": (
                {"segment": join[0], "offset": join[1], "seq": join[2]} if join else None
            ),
            "last_close_ts": last_close_ts,
        }
        return protocol.write_json_atomic(self._key_dir / protocol.MANIFEST_NAME, manifest)

    def close(self) -> None:
        try:
            self._fh.close()
        except OSError:
            pass

    @staticmethod
    def recover_seq(key_dir: Path) -> int:
        """Highest seq known for this key (0 if none); the next append is +1.

        ``max(manifest.seq_highwater, max seq scanned from the segment tails)``:
        the scan beats a stale manifest, and a manifest flushed ahead of a torn
        final line still cannot lead to seq reuse.
        """
        key_dir = Path(key_dir)
        high = 0
        manifest = protocol.read_json(key_dir / protocol.MANIFEST_NAME)
        if manifest:
            try:
                high = max(high, int(manifest.get("seq_highwater") or 0))
            except (TypeError, ValueError):
                pass
        for number in reversed(protocol.list_segments(key_dir)):
            seg_high = 0
            try:
                data = (key_dir / protocol.segment_name(number)).read_bytes()
            except OSError:
                continue
            lines, _ = protocol.split_terminated(data)
            for raw in lines:
                decoded = protocol.decode_line(raw)
                if decoded[0] == protocol.EVENT and decoded[1] > seg_high:
                    seg_high = decoded[1]
            if seg_high:
                return max(high, seg_high)
        return high

    def _segment_path(self, number: int) -> Path:
        return self._key_dir / protocol.segment_name(number)

    def _rotate(self) -> None:
        next_segment = self._segment + 1
        self._fh.write(protocol.encode_roll_line(next_segment))
        self._fh.flush()
        self._fh.close()
        self._segment = next_segment
        path = self._segment_path(next_segment)
        self._fh = open(path, "ab")
        self._fh.seek(0, os.SEEK_END)
        self._offset = self._fh.tell()
        _log.info(f"MirrorWriter[{self._key}]: rotated to segment {next_segment}")

    @staticmethod
    def _repair_torn_tail(path: Path) -> None:
        """If the last byte is not ``\\n``, append one so the next append cannot
        glue onto the torn fragment. Never truncates anything."""
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size == 0:
            return
        with open(path, "rb") as f:
            f.seek(size - 1)
            last = f.read(1)
        if last != b"\n":
            with open(path, "ab") as f:
                f.write(b"\n")
            _log.warning(f"MirrorWriter: repaired torn tail in {path} (terminated fragment)")

    @classmethod
    def _pending_roll_target(cls, path: Path) -> int | None:
        """Return the roll target if the segment's last complete line is a roll line."""
        last = cls._last_line(path)
        if last is None:
            return None
        decoded = protocol.decode_line(last)
        return decoded[1] if decoded[0] == protocol.ROLL else None

    @staticmethod
    def _last_line(path: Path) -> bytes | None:
        try:
            size = path.stat().st_size
        except OSError:
            return None
        if size == 0:
            return None
        with open(path, "rb") as f:
            f.seek(max(0, size - _TAIL_SCAN_BYTES))
            chunk = f.read()
        end = chunk.rfind(b"\n")
        if end < 0:
            return None
        start = chunk.rfind(b"\n", 0, end)
        return chunk[start + 1 : end]
