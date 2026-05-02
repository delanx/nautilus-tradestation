"""Roll watcher: detect when the front contract changes during an open trade.

Strategies that hold positions across days/weeks (swing trades) need to be
warned if the front contract rolls forward while a position is open. The
watcher does not auto-roll the position — that's a strategy decision —
but it emits a structured event so the strategy can decide.

For pure intraday strategies that flatten by session close, the watcher
is a no-op.
"""
from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from .resolver import FrontContract, FrontContractResolver


@dataclass(frozen=True)
class RollEvent:
    root: str
    old_contract: str
    new_contract: str
    old_expiration_utc: _dt.datetime
    new_expiration_utc: _dt.datetime
    detected_at_utc: _dt.datetime
    position_open: bool
    """If True, the strategy held a non-zero position when the front shifted —
    this is the "swing trade roll" warning case."""


class RollWatcher:
    """Periodically asks the resolver for the front contract and emits a
    RollEvent if it changed since the last check.

    Wire this into an Actor that calls `check()` on a schedule (e.g. once
    per session start), passing a callable that returns whether the
    strategy currently holds a position.
    """

    def __init__(
        self,
        resolver: FrontContractResolver,
        root: str,
        *,
        on_event: Optional[Callable[[RollEvent], Awaitable[None]]] = None,
        log: Optional[logging.Logger] = None,
    ):
        self._resolver = resolver
        self._root = root
        self._on_event = on_event
        self._log = log or logging.getLogger(__name__)
        self._last_front: Optional[FrontContract] = None

    async def check(self, *, has_open_position: bool = False) -> Optional[RollEvent]:
        """Re-resolve the front; emit + return a RollEvent if it changed.

        Returns None if no change.
        """
        current = await self._resolver.front_contract(self._root)

        if self._last_front is None:
            self._last_front = current
            return None

        if current.contract_symbol == self._last_front.contract_symbol:
            return None

        event = RollEvent(
            root=self._root,
            old_contract=self._last_front.contract_symbol,
            new_contract=current.contract_symbol,
            old_expiration_utc=self._last_front.expiration_utc,
            new_expiration_utc=current.expiration_utc,
            detected_at_utc=_dt.datetime.now(_dt.timezone.utc),
            position_open=has_open_position,
        )

        level = logging.WARNING if has_open_position else logging.INFO
        self._log.log(
            level,
            "front-contract roll: %s -> %s (root=%s, position_open=%s)",
            event.old_contract, event.new_contract, event.root, event.position_open,
        )

        self._last_front = current
        if self._on_event is not None:
            await self._on_event(event)
        return event
