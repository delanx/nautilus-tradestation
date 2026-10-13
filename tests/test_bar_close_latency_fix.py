"""Regression: the SSE bar stream must emit a completed bar on TradeStation's
explicit ``BarStatus == "Closed"`` event, NOT wait for the *next* bar's first tick.

Before the fix the adapter only emitted a buffered bar when a new TimeStamp arrived
(`_handle_bar_stream_event`'s timestamp-change branch). On a quiet instrument (e.g.
overnight Treasuries) the next tick can be SECONDS away, so completed bars sat
un-emitted -- the 1.5-10 s live receive transit. TradeStation sends BarStatus=="Closed"
~150 ms after the period ends, so emitting on it restores ~150 ms delivery.

These call the REAL ``_handle_bar_stream_event`` (the existing tests re-implement the
buffer logic inline, so they could not catch this).
"""
from nautilus_tradestation.data import TradeStationDataClient

H = TradeStationDataClient._handle_bar_stream_event


class _Log:
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass


def _fake_client(emitted: list):
    class FakeClient:
        _log = _Log()

        def _parse_bars(self, raws, bar_type, instrument):
            return [{"ts": raws[0].get("TimeStamp")}]

        def _handle_data(self, bar):
            emitted.append(bar)

        def _mark_bar_emit(self, raw):
            return raw

    return FakeClient()


def _st():
    return {"buffered_ts": "", "buffered_event": None, "initialized": False}


def test_emits_on_closed_status_without_waiting_for_next_bar():
    emitted: list = []
    fc, st = _fake_client(emitted), _st()
    # forming bar -> buffered, not emitted
    H(fc, {"TimeStamp": "T1", "BarStatus": "Open"}, "BT", "INST", st)
    assert emitted == []
    # explicit Closed for the SAME bar -> emit NOW (the fix)
    H(fc, {"TimeStamp": "T1", "BarStatus": "Closed"}, "BT", "INST", st)
    assert len(emitted) == 1, "completed bar must emit on BarStatus=='Closed'"
    # next bar starts -> must NOT re-emit T1 (de-dup via emitted_ts)
    H(fc, {"TimeStamp": "T2", "BarStatus": "Open"}, "BT", "INST", st)
    assert len(emitted) == 1, "must not re-emit a bar already emitted on Closed"


def test_fallback_emits_on_timestamp_change_when_no_closed_seen():
    """If TradeStation never sends an explicit Closed, the old timestamp-change
    behavior must still emit the bar (no regression for that path)."""
    emitted: list = []
    fc, st = _fake_client(emitted), _st()
    H(fc, {"TimeStamp": "T1", "BarStatus": "Open"}, "BT", "INST", st)
    assert emitted == []
    H(fc, {"TimeStamp": "T2", "BarStatus": "Open"}, "BT", "INST", st)  # new ts
    assert len(emitted) == 1, "timestamp-change fallback must still emit the prior bar"
