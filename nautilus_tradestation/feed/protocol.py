"""
On-disk wire protocol for the example feed transport (jsonl-tail).

One feed-handler process per SIM account owns all TradeStation SSE bar
subscriptions and appends every bar event VERBATIM to per-key JSONL segment
files; cells poll-tail those files. The JSONL audit mirror IS the transport:
no sockets, no server.

Layout (all under ``%LOCALAPPDATA%/example/feed/<account>/`` — LOCAL disk only):

    handler.heartbeat              touched (os.utime) every ~2s by the handler
    handler.meta.json              {"v":1,"pid":...,"account":...,"started_utc":...}
    handler.stop                   planned-stop marker (operator/supervisor)
    requests/<key>.req.json        subscription drop-dir (cells write, handler scans)
    bars/<key>/manifest.json       seed/join/seq highwater, atomic (tmp + os.replace)
    bars/<key>/00000001.jsonl ...  segments, monotonic 8-digit numbering, size-rotated

Segment line format (one JSON object per ``\\n``-terminated UTF-8 line):

    {"seq": <int>, "ev": {<verbatim upstream TS event dict>}}    event line
    {"roll": <int>}                                              final line before rotation

Torn-line rules (load-bearing; identical for every reader):

- accept only ``\\n``-terminated lines that ``json.loads`` cleanly into one of
  the two shapes above;
- an unterminated tail is "in progress": re-poll, never error;
- a terminated-but-unparseable line is skipped with a warning ONLY if the next
  valid line preserves seq continuity (the consumer's seq check enforces that;
  a visible seq gap is a HARD error, never silently passed).
"""

import json
import logging
import os
import time
from pathlib import Path

_log = logging.getLogger(__name__)

PROTOCOL_VERSION = 1

SEGMENT_MAX_BYTES = 10_000_000
SEGMENT_SUFFIX = ".jsonl"
MANIFEST_NAME = "manifest.json"
HEARTBEAT_NAME = "handler.heartbeat"
META_NAME = "handler.meta.json"
STOP_MARKER_NAME = "handler.stop"
REQUESTS_DIR = "requests"
BARS_DIR = "bars"
REQUEST_SUFFIX = ".req.json"

# decode_line result kinds
EVENT = "event"
ROLL = "roll"
BAD = "bad"

_REPLACE_RETRIES = 5
_REPLACE_RETRY_SECS = 0.05


def segment_name(number: int) -> str:
    """Return the segment file name for a segment number (8-digit, monotonic)."""
    return f"{number:08d}{SEGMENT_SUFFIX}"


def list_segments(key_dir: Path) -> list[int]:
    """Return the sorted segment numbers present in a key directory."""
    numbers = []
    for path in Path(key_dir).glob(f"*{SEGMENT_SUFFIX}"):
        try:
            numbers.append(int(path.stem))
        except ValueError:
            continue
    return sorted(numbers)


def encode_event_line(seq: int, ev: dict) -> bytes:
    """Encode one event line. ``ev`` crosses verbatim (values never re-typed)."""
    return json.dumps({"seq": seq, "ev": ev}, separators=(",", ":")).encode("utf-8") + b"\n"


def encode_roll_line(next_segment: int) -> bytes:
    """Encode the final line of a segment, pointing readers at the next one."""
    return json.dumps({"roll": next_segment}, separators=(",", ":")).encode("utf-8") + b"\n"


def decode_line(raw: bytes) -> tuple:
    """Decode one ``\\n``-terminated line (terminator stripped) per the torn-line rules.

    Returns ``(EVENT, seq, ev)`` | ``(ROLL, next_segment)`` | ``(BAD, reason)``.
    """
    if not raw.strip():
        return (BAD, "empty line")
    try:
        obj = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return (BAD, "unparseable line")
    if not isinstance(obj, dict):
        return (BAD, "not a JSON object")
    if "roll" in obj:
        target = obj["roll"]
        if isinstance(target, int) and target > 0 and len(obj) == 1:
            return (ROLL, target)
        return (BAD, "malformed roll line")
    seq = obj.get("seq")
    ev = obj.get("ev")
    if isinstance(seq, int) and seq > 0 and isinstance(ev, dict):
        return (EVENT, seq, ev)
    return (BAD, "malformed event line")


def split_terminated(data: bytes) -> tuple[list[bytes], int]:
    """Split ``data`` into complete (``\\n``-terminated) lines, terminators stripped.

    Returns ``(lines, bytes_consumed)``. An unterminated remainder is NOT
    consumed — it is "in progress" and must be re-polled, never an error.
    """
    end = data.rfind(b"\n")
    if end < 0:
        return [], 0
    return data[:end].split(b"\n"), end + 1


def write_json_atomic(path: Path, obj: dict) -> bool:
    """Write ``obj`` to ``path`` via tmp file + ``os.replace`` (atomic on NTFS).

    On ``PermissionError`` (a reader briefly holds the target open — Windows),
    retries with short backoff; returns False if all retries fail so the caller
    can try again next cycle (never raises for contention).
    """
    path = Path(path)
    tmp = path.with_name(f"{path.name}.tmp{os.getpid()}")
    tmp.write_bytes(json.dumps(obj, separators=(",", ":")).encode("utf-8"))
    for attempt in range(_REPLACE_RETRIES):
        try:
            os.replace(tmp, path)
            return True
        except PermissionError:
            time.sleep(_REPLACE_RETRY_SECS)
    _log.warning(f"write_json_atomic: os.replace blocked for {path}; deferring to next cycle")
    try:
        tmp.unlink()
    except OSError:
        pass
    return False


def read_json(path: Path) -> dict | None:
    """Read a JSON file in one open-read-close; retry once on transient failure.

    Returns None if the file is missing or unreadable (caller decides severity).
    """
    for attempt in range(2):
        try:
            with open(path, "rb") as f:
                data = f.read()
            obj = json.loads(data)
            return obj if isinstance(obj, dict) else None
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            if attempt == 0:
                time.sleep(0.02)
    return None


def validate_feed_dir(path: Path) -> Path:
    """Enforce that a feed directory lives under ``%LOCALAPPDATA%`` (local disk).

    Feed files must NEVER live on the Drive mount (per-bar file IO there causes
    fatal page errors). ``TS_FEED_ALLOW_ANY_DIR=1`` relaxes the check (tests
    only). Raises ``RuntimeError`` on violation.
    """
    resolved = Path(path).resolve()
    if os.environ.get("TS_FEED_ALLOW_ANY_DIR") == "1":
        return resolved
    local = os.environ.get("LOCALAPPDATA")
    if not local:
        raise RuntimeError("LOCALAPPDATA is not set; cannot validate feed dir location")
    local_norm = os.path.normcase(str(Path(local).resolve())).rstrip("\\/")
    path_norm = os.path.normcase(str(resolved))
    if path_norm != local_norm and not path_norm.startswith(local_norm + os.sep):
        raise RuntimeError(
            f"Feed dir {resolved} is not under %LOCALAPPDATA% ({local}) — feed files must "
            "live on local disk (TS_FEED_ALLOW_ANY_DIR=1 is the test-only escape)"
        )
    return resolved
