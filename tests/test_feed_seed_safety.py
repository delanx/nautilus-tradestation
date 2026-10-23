"""
Seed-safety tests (the one piece of synthesized state — handle with care).

manifest.seed_event = the final update of the most recently COMPLETED bar.
HARD RULE: a synthesized seed may ONLY be installed on close detection (a later
TimeStamp is the proof of completion). An in-progress snapshot must NEVER
become the seed.
"""
import json

import pytest

from nautilus_tradestation.feed import protocol
from nautilus_tradestation.feed.handler import FeedHandler
from nautilus_tradestation.feed.keys import stream_key

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


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """A FeedHandler ingest state driven directly (the exact _ingest loop body)."""
    monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
    handler = FeedHandler("SIMTEST", tmp_path, stream_client=None)
    handler._prepare()
    st = handler._start_ingest(KEY, "ESM26", "15", "Minute", None)

    def push(ev: dict) -> None:
        seq, segment, offset = st.writer.append(ev)
        handler._update_close_detection(st, ev, seq, segment, offset)

    def manifest() -> dict:
        path = tmp_path / "SIMTEST" / protocol.BARS_DIR / KEY / protocol.MANIFEST_NAME
        return json.loads(path.read_bytes())

    yield handler, st, push, manifest
    st.writer.close()


class TestInProgressNeverSeeds:
    def test_updates_of_one_bar_never_seed(self, harness):
        handler, st, push, manifest = harness
        for close in ("4120.0", "4121.5", "4119.25"):
            push(_ev("2026-06-10T14:00:00Z", close))
            assert manifest()["seed_event"] is None  # in-progress snapshot NEVER seeds

    def test_first_event_of_next_bar_seeds_previous_final_update(self, harness):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))
        push(_ev("2026-06-10T14:00:00Z", "4121.5"))  # bar T's FINAL update
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))  # T+1 begins -> T completed
        seed = manifest()["seed_event"]
        assert seed["TimeStamp"] == "2026-06-10T14:00:00Z"  # byte-equal to T's last update
        assert seed["Close"] == "4121.5"
        assert seed["Status"] == "Historical"
        assert seed["_ts_feed_seed"] is True
        assert manifest()["last_close_ts"] == "2026-06-10T14:00:00Z"

    def test_later_updates_of_current_bar_keep_previous_seed(self, harness):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))
        push(_ev("2026-06-10T14:15:00Z", "4123.75"))  # still in-progress
        seed = manifest()["seed_event"]
        assert seed["TimeStamp"] == "2026-06-10T14:00:00Z"
        assert seed["Close"] == "4120.0"

    def test_join_points_at_first_event_of_current_bar(self, harness):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))
        push(_ev("2026-06-10T14:00:00Z", "4121.5"))
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))  # first event of the current bar
        push(_ev("2026-06-10T14:15:00Z", "4122.5"))
        join = manifest()["join"]
        assert join["seq"] == 3
        # Replaying from the join offset yields exactly the current bar's updates.
        seg_path = st.writer.key_dir / protocol.segment_name(join["segment"])
        lines, _ = protocol.split_terminated(seg_path.read_bytes()[join["offset"]:])
        decoded = [protocol.decode_line(raw) for raw in lines]
        assert [d[1] for d in decoded] == [3, 4]
        assert decoded[0][2]["TimeStamp"] == "2026-06-10T14:15:00Z"


class TestUpstreamHistoricalSeeds:
    def test_upstream_seed_stored_verbatim(self, harness):
        handler, st, push, manifest = harness
        upstream = _ev("2026-06-10T14:00:00Z", "4121.0", status="Historical")
        push(upstream)
        seed = manifest()["seed_event"]
        assert seed == upstream  # verbatim: no _ts_feed_seed, no re-stamping
        assert "_ts_feed_seed" not in seed

    def test_newer_upstream_seed_wins(self, harness):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))  # closes T -> synthesized seed
        newer = _ev("2026-06-10T14:15:00Z", "4123.0", status="Historical")
        push(newer)  # reconnect seed for the SAME bar the buffer holds
        assert manifest()["seed_event"] == newer

    def test_stale_upstream_seed_ignored(self, harness):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))  # seed = T (14:00)
        stale = _ev("2026-06-10T13:45:00Z", "4100.0", status="Historical")
        push(stale)
        seed = manifest()["seed_event"]
        assert seed["TimeStamp"] == "2026-06-10T14:00:00Z"
        assert seed["Close"] == "4120.0"

    def test_synthesized_seed_does_not_clobber_verbatim_same_bar(self, harness):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))  # in-progress T (stale update)
        authoritative = _ev("2026-06-10T14:00:00Z", "4121.0", status="Historical")
        push(authoritative)  # T actually completed during an outage
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))  # close detection for T fires
        # The verbatim upstream copy of T must survive (rule (b) is strict-newer).
        assert manifest()["seed_event"] == authoritative


class TestHandlerRestartKeepsSeed:
    def test_prior_manifest_seed_and_join_survive_restart(self, harness, tmp_path, monkeypatch):
        handler, st, push, manifest = harness
        push(_ev("2026-06-10T14:00:00Z", "4120.0"))
        push(_ev("2026-06-10T14:15:00Z", "4122.0"))
        before = manifest()
        st.writer.close()

        handler2 = FeedHandler("SIMTEST", tmp_path, stream_client=None)
        handler2._prepare()
        st2 = handler2._start_ingest(KEY, "ESM26", "15", "Minute", None)
        after = manifest()
        assert after["seed_event"] == before["seed_event"]
        assert after["join"] == before["join"]
        assert after["last_close_ts"] == before["last_close_ts"]
        assert after["seq_highwater"] == before["seq_highwater"]
        st2.writer.close()
