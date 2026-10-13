"""
Stream-key encoding for the feed transport.

The single shared source for both sides of the transport (handler = writer,
consumers = readers) so key drift is impossible. Inputs are exactly the four
arguments of ``TradeStationStreamClient.stream_bars``.
"""

import hashlib
import re

_SANITIZE = re.compile(r"[^A-Za-z0-9._-]")


def _raw(value) -> str:
    # Accept str-Enum inputs (e.g. TradeStationBarUnit) by taking .value, like
    # the data client's call site does.
    return str(getattr(value, "value", value))


def _component(value) -> str:
    return _SANITIZE.sub("_", _raw(value))


def stream_key(symbol: str, interval: str, unit: str, session_template: str | None = None) -> str:
    """Encode one bar stream's identity as a filesystem-safe key.

    Format: ``<SYMBOL>__<INTERVAL><UNIT>__<SESSION>__<HASH8>``; SESSION is
    ``session_template`` or ``"default"``; every readable component is
    sanitized ``[^A-Za-z0-9._-] -> "_"``. The trailing hash is taken over the
    RAW (unsanitized) component tuple, so two distinct raw inputs whose
    sanitized text collides (e.g. ``"@ES"`` vs ``"=ES"`` -> ``"_ES"``) can
    never share a stream.

    ``session_template`` MUST be part of the key: USEQPreAndPost and RTH
    subscribers must never share a stream.
    """
    session = session_template if session_template else "default"
    raw = "\x1f".join((_raw(symbol), _raw(interval), _raw(unit), _raw(session)))
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return (
        f"{_component(symbol)}__{_component(interval)}{_component(unit)}"
        f"__{_component(session)}__{digest}"
    )
