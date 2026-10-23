"""Continuous-contract resolver for TradeStation futures.

TradeStation trades only specific dated contracts (e.g. NQH26, NQM26).
Strategies that operate on a "continuous" notion (e.g. @NQ) need to be
mapped to the current front-month contract. This package handles:

  - Front-contract resolution (which dated contract is "now")
  - Mid-trade roll detection (warn the strategy if the front changes
    while a position is open — relevant for swing trades that span
    expirations)

Usage:
    from nautilus_tradestation.continuous_contract import (
        FrontContractResolver, RollWatcher,
    )
    resolver = FrontContractResolver(http_client)
    front = await resolver.front_contract("@NQ")  # -> "NQH26"

This module is designed to be PR-able to ltamagnone/nautilus-tradestation.
"""
from .resolver import FrontContractResolver, FrontContract
from .roll_watcher import RollWatcher, RollEvent

__all__ = ["FrontContractResolver", "FrontContract", "RollWatcher", "RollEvent"]
