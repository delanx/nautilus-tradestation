"""
Tests for feed/keys.py — stream-key encoding (single shared source, writer + reader).
"""
import hashlib

import pytest

from nautilus_tradestation.common.enums import TradeStationBarUnit
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.parsing.data import bar_spec_to_ts_params
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.enums import BarAggregation, PriceType


def _h(symbol: str, interval: str, unit: str, session: str) -> str:
    # Independent reimplementation pinning the hash contract: sha1[:8] over the
    # RAW (unsanitized) component tuple joined with \x1f.
    raw = "\x1f".join((symbol, interval, unit, session))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]


class TestStreamKeyFormat:
    def test_basic_format(self):
        expected = f"ESM26__15Minute__default__{_h('ESM26', '15', 'Minute', 'default')}"
        assert stream_key("ESM26", "15", "Minute", None) == expected

    def test_session_template_in_key(self):
        key = stream_key("AAPL", "15", "Minute", "USEQPreAndPost")
        expected = (
            f"AAPL__15Minute__USEQPreAndPost__{_h('AAPL', '15', 'Minute', 'USEQPreAndPost')}"
        )
        assert key == expected

    def test_extended_hours_and_default_never_collide(self):
        rth = stream_key("AAPL", "15", "Minute", None)
        ext = stream_key("AAPL", "15", "Minute", "USEQPreAndPost")
        assert rth != ext

    def test_none_session_is_default(self):
        assert stream_key("CLN26", "30", "Minute") == stream_key("CLN26", "30", "Minute", None)

    def test_empty_session_is_default(self):
        # The data client passes None for non-equities; "" must not split namespaces.
        assert stream_key("CLN26", "30", "Minute", "") == stream_key("CLN26", "30", "Minute", None)


class TestStreamKeySanitization:
    def test_at_prefix_sanitized(self):
        key = stream_key("@ES", "15", "Minute", None)
        assert key.startswith("_ES__15Minute__default__")

    def test_sanitization_collisions_disambiguated_by_hash(self):
        # '@ES' and '=ES' both sanitize to '_ES'; the raw-tuple hash keeps the
        # keys distinct so two cells can never silently share a stream.
        assert stream_key("@ES", "15", "Minute", None) != stream_key("=ES", "15", "Minute", None)
        assert stream_key("AAPL", "15", "Minute", "US/EQ") != stream_key(
            "AAPL", "15", "Minute", "US EQ"
        )

    def test_path_hostile_characters_sanitized(self):
        key = stream_key('E/S\\M:2*6?"<>|', "15", "Minute", None)
        assert "/" not in key and "\\" not in key and ":" not in key
        assert key.startswith("E_S_M_2_6")

    def test_allowed_characters_preserved(self):
        key = stream_key("ES.M-2_6", "15", "Minute", None)
        assert key.startswith("ES.M-2_6__15Minute__default__")

    def test_session_sanitized(self):
        key = stream_key("AAPL", "15", "Minute", "US EQ/Pre")
        assert key.startswith("AAPL__15Minute__US_EQ_Pre__")


class TestStreamKeyBarSpecParams:
    """Keys for bar_spec_to_ts_params outputs, mirroring the data.py call site
    (interval str + unit.value)."""

    @pytest.mark.parametrize(
        "spec,expected",
        [
            (BarSpecification(15, BarAggregation.MINUTE, PriceType.LAST), "15Minute"),
            (BarSpecification(1, BarAggregation.HOUR, PriceType.LAST), "60Minute"),
            (BarSpecification(1, BarAggregation.DAY, PriceType.LAST), "1Daily"),
        ],
    )
    def test_bar_spec_keys(self, spec, expected):
        interval, unit = bar_spec_to_ts_params(spec)
        assert stream_key("ESM26", interval, unit.value, None).startswith(
            f"ESM26__{expected}__default__"
        )

    def test_enum_unit_equals_value_string(self):
        # Defensive: a str-Enum unit encodes identically to its .value.
        assert stream_key("ESM26", "15", TradeStationBarUnit.MINUTE) == stream_key(
            "ESM26", "15", "Minute"
        )
