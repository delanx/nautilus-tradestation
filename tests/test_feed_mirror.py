"""
Tests for feed/mirror.py — MirrorWriter: append, rotation, torn-tail repair,
seq recovery, atomic manifest writes under Windows reader contention.
"""
import json
import threading
import time

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.mirror import MirrorWriter


def _ev(ts: str, close: str = "100.0", status: str = "RealTime") -> dict:
    return {
        "TimeStamp": ts,
        "Open": "99.0",
        "High": "101.5",
        "Low": "98.75",
        "Close": close,
        "TotalVolume": "1500",
        "Status": status,
    }


def _read_lines(path) -> list:
    lines, _ = protocol.split_terminated(path.read_bytes())
    return [protocol.decode_line(raw) for raw in lines]


class TestAppend:
    def test_seq_monotonic_from_one(self, tmp_path):
        writer = MirrorWriter(tmp_path / "k")
        results = [writer.append(_ev(f"2026-06-10T14:{i:02d}:00Z")) for i in range(3)]
        assert [r[0] for r in results] == [1, 2, 3]
        writer.close()

    def test_append_returns_segment_and_line_start_offset(self, tmp_path):
        writer = MirrorWriter(tmp_path / "k")
        seq1, seg1, off1 = writer.append(_ev("2026-06-10T14:00:00Z"))
        seq2, seg2, off2 = writer.append(_ev("2026-06-10T14:15:00Z"))
        assert (seg1, off1) == (1, 0)
        assert seg2 == 1 and off2 > 0
        # The offset is the line start: decoding from there gives event 2.
        data = (tmp_path / "k" / protocol.segment_name(1)).read_bytes()
        lines, _ = protocol.split_terminated(data[off2:])
        assert protocol.decode_line(lines[0])[1] == 2
        writer.close()

    def test_events_cross_verbatim(self, tmp_path):
        writer = MirrorWriter(tmp_path / "k")
        ev = _ev("2026-06-10T14:30:00Z", close="4123.25")
        writer.append(ev)
        decoded = _read_lines(tmp_path / "k" / protocol.segment_name(1))[0]
        assert decoded[0] == protocol.EVENT
        assert decoded[2] == ev
        assert type(decoded[2]["Close"]) is str and decoded[2]["Close"] == "4123.25"
        writer.close()


class TestRotation:
    def test_rotation_writes_roll_line_and_next_segment(self, tmp_path):
        writer = MirrorWriter(tmp_path / "k", segment_max_bytes=1)
        writer.append(_ev("2026-06-10T14:00:00Z"))  # segment 1
        writer.append(_ev("2026-06-10T14:15:00Z"))  # exceeds 1 byte -> rolls to 2
        writer.append(_ev("2026-06-10T14:30:00Z"))  # rolls to 3
        assert protocol.list_segments(tmp_path / "k") == [1, 2, 3]
        seg1 = _read_lines(tmp_path / "k" / protocol.segment_name(1))
        assert seg1[-1] == (protocol.ROLL, 2)
        seg2 = _read_lines(tmp_path / "k" / protocol.segment_name(2))
        assert seg2[0][1] == 2  # event seq 2
        assert seg2[-1] == (protocol.ROLL, 3)
        assert writer.active_segment == 3
        writer.close()

    def test_restart_after_roll_line_without_next_segment(self, tmp_path):
        # Crash between writing the roll line and creating the next file:
        # the restarted writer must honor the roll, never append behind it.
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        writer.close()
        with open(key_dir / protocol.segment_name(1), "ab") as f:
            f.write(protocol.encode_roll_line(2))
        writer2 = MirrorWriter(key_dir)
        assert writer2.active_segment == 2
        seq, segment, offset = writer2.append(_ev("2026-06-10T14:15:00Z"))
        assert (seq, segment, offset) == (2, 2, 0)
        writer2.close()


class TestTornTailRepair:
    def test_partial_line_is_terminated_once_and_skipped(self, tmp_path):
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        writer.append(_ev("2026-06-10T14:15:00Z"))
        writer.close()
        seg = key_dir / protocol.segment_name(1)
        with open(seg, "ab") as f:
            f.write(b'{"seq": 3, "ev": {"TimeStamp": "2026-06-10T14:3')  # torn, no \n
        writer2 = MirrorWriter(key_dir)
        data = seg.read_bytes()
        assert data.endswith(b'14:3\n')
        assert not data.endswith(b"\n\n")  # exactly ONE newline appended
        # Seq continuity: the torn seq-3 line never became valid, so the next
        # append takes seq 3, and readers see [1, 2, bad, 3].
        seq, _, _ = writer2.append(_ev("2026-06-10T14:30:00Z"))
        assert seq == 3
        decoded = _read_lines(seg)
        assert [d[1] for d in decoded if d[0] == protocol.EVENT] == [1, 2, 3]
        assert sum(1 for d in decoded if d[0] == protocol.BAD) == 1
        writer2.close()

    def test_repair_is_noop_on_clean_tail(self, tmp_path):
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        writer.close()
        before = (key_dir / protocol.segment_name(1)).read_bytes()
        writer2 = MirrorWriter(key_dir)
        writer2.close()
        assert (key_dir / protocol.segment_name(1)).read_bytes() == before


class TestRecoverSeq:
    def test_tail_scan_beats_stale_manifest(self, tmp_path):
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir)
        for i in range(4):
            writer.append(_ev(f"2026-06-10T14:{i:02d}:00Z"))
        # Stale manifest: crash-between-append-and-manifest.
        writer.write_manifest(seed_event=None, join=None, last_close_ts="")
        manifest_path = key_dir / protocol.MANIFEST_NAME
        manifest = json.loads(manifest_path.read_bytes())
        manifest["seq_highwater"] = 1
        manifest_path.write_bytes(json.dumps(manifest).encode())
        writer.close()
        assert MirrorWriter.recover_seq(key_dir) == 4
        writer2 = MirrorWriter(key_dir)
        assert writer2.append(_ev("2026-06-10T15:00:00Z"))[0] == 5
        writer2.close()

    def test_manifest_highwater_beats_shorter_scan(self, tmp_path):
        # A manifest ahead of the scannable tail must still prevent seq reuse.
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        writer.write_manifest(seed_event=None, join=None, last_close_ts="")
        manifest_path = key_dir / protocol.MANIFEST_NAME
        manifest = json.loads(manifest_path.read_bytes())
        manifest["seq_highwater"] = 10
        manifest_path.write_bytes(json.dumps(manifest).encode())
        writer.close()
        assert MirrorWriter.recover_seq(key_dir) == 10
        writer2 = MirrorWriter(key_dir)
        assert writer2.append(_ev("2026-06-10T15:00:00Z"))[0] == 11
        writer2.close()

    def test_scan_falls_back_past_empty_active_segment(self, tmp_path):
        # Crash right after rotation created an empty next segment.
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir, segment_max_bytes=1)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        writer.append(_ev("2026-06-10T14:15:00Z"))  # rotates to segment 2
        writer.close()
        (key_dir / protocol.segment_name(3)).touch()  # empty "active" segment
        assert MirrorWriter.recover_seq(key_dir) == 2

    def test_empty_dir_recovers_zero(self, tmp_path):
        assert MirrorWriter.recover_seq(tmp_path / "missing") == 0


class TestManifestContention:
    def test_replace_retries_while_reader_holds_manifest(self, tmp_path):
        # Windows: os.replace fails with PermissionError while a reader holds
        # the target open (CPython open() does not pass FILE_SHARE_DELETE).
        key_dir = tmp_path / "k"
        writer = MirrorWriter(key_dir)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        assert writer.write_manifest(seed_event=None, join=None, last_close_ts="") is True
        manifest_path = key_dir / protocol.MANIFEST_NAME

        holder = open(manifest_path, "rb")
        release = threading.Timer(0.12, holder.close)
        release.start()
        try:
            ok = writer.write_manifest(
                seed_event=_ev("2026-06-10T14:00:00Z", status="Historical"),
                join=(1, 0, 1),
                last_close_ts="2026-06-10T14:00:00Z",
            )
        finally:
            release.join()
            if not holder.closed:
                holder.close()
        assert ok is True
        manifest = json.loads(manifest_path.read_bytes())
        assert manifest["join"] == {"segment": 1, "offset": 0, "seq": 1}
        assert manifest["seed_event"]["Status"] == "Historical"
        assert manifest["seq_highwater"] == 1
        writer.close()

    def test_manifest_schema(self, tmp_path):
        key_dir = tmp_path / "MYKEY"
        writer = MirrorWriter(key_dir)
        writer.append(_ev("2026-06-10T14:00:00Z"))
        writer.write_manifest(seed_event=None, join=None, last_close_ts="")
        manifest = json.loads((key_dir / protocol.MANIFEST_NAME).read_bytes())
        assert manifest == {
            "v": 1,
            "key": "MYKEY",
            "active_segment": 1,
            "seq_highwater": 1,
            "seed_event": None,
            "join": None,
            "last_close_ts": "",
        }
        writer.close()
