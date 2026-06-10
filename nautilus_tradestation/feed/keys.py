"""
Stream-key encoding for the example feed transport.

The single shared source for both sides of the transport (handler = writer,
cells = readers) so key drift is impossible. Inputs are exactly the four
arguments of ``TradeStationStreamClient.stream_bars``.
"""

import re

_SANITIZE = re.compile(r"[^A-Za-z0-9._-]")


def _component(value) -> str:
    # Accept str-Enum inputs (e.g. TradeStationBarUnit) by taking .value, like
    # the data client's call site does.
    value = getattr(value, "value", value)
    return _SANITIZE.sub("_", str(value))


def stream_key(symbol: str, interval: str, unit: str, session_template: str | None = None) -> str:
    """Encode one bar stream's identity as a filesystem-safe key.

    Format: ``<SYMBOL>__<INTERVAL><UNIT>__<SESSION>``; SESSION is
    ``session_template`` or ``"default"``; every component is sanitized
    ``[^A-Za-z0-9._-] -> "_"``.

    ``session_template`` MUST be part of the key: USEQPreAndPost and RTH
    subscribers must never share a stream.
    """
    session = session_template if session_template else "default"
    return f"{_component(symbol)}__{_component(interval)}{_component(unit)}__{_component(session)}"
