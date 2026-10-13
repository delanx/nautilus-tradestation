"""Resolve a continuous futures symbol (@NQ) to the current front-month contract.

TradeStation's futures contract notation:
    Root + MonthCode + 2-digit Year, e.g. NQH26 = NQ March 2026

CME futures month codes:
    F=Jan G=Feb H=Mar J=Apr K=May M=Jun
    N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec

Front-contract logic:
    1. Query TS for all currently-listed contracts with the given root
    2. Filter to those with expiration date > today
    3. Pick the one with the earliest expiration

Roll dates:
    Industry convention is to roll a few days BEFORE expiration to avoid
    last-day liquidity issues. Default: roll 8 calendar days before
    expiry (configurable via `roll_buffer_days`).
"""
from __future__ import annotations

import datetime as _dt
import re
from dataclasses import dataclass
from typing import Optional

import pandas as pd


_MONTH_CODES = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}
_INVERSE_MONTH_CODES = {v: k for k, v in _MONTH_CODES.items()}


@dataclass(frozen=True)
class FrontContract:
    """Resolved front-month contract for a continuous symbol."""
    root: str                      # e.g. "@NQ" or "NQ"
    contract_symbol: str           # e.g. "NQH26"
    contract_month: int            # 1-12
    contract_year: int             # full year (2026)
    expiration_utc: _dt.datetime   # contract last trade date (UTC)
    roll_date_utc: _dt.datetime    # date at which we should roll forward
    resolved_at_utc: _dt.datetime


class FrontContractResolver:
    """Resolves continuous-contract roots to current front-month contracts.

    Stateless except for an internal cache of contract listings (to avoid
    hammering the TS API). Cache TTL defaults to 1 hour.
    """

    def __init__(
        self,
        http_client,                  # TradeStationHttpClient
        *,
        roll_buffer_days: int = 8,
        cache_ttl_seconds: int = 3600,
    ):
        self._http = http_client
        self._roll_buffer_days = roll_buffer_days
        self._cache_ttl = cache_ttl_seconds
        self._cache: dict[str, tuple[_dt.datetime, list[dict]]] = {}

    async def front_contract(
        self,
        root: str,
        *,
        as_of: Optional[_dt.datetime] = None,
    ) -> FrontContract:
        """Return the front-month contract for the given root symbol.

        Parameters
        ----------
        root : str
            Continuous symbol like "@NQ" or just "NQ". The leading "@" is
            stripped if present.
        as_of : datetime, optional
            Treat this as "now" for front-month determination. Defaults to
            current UTC. Mostly useful for testing.

        Raises
        ------
        ValueError
            If no current or future-dated contract is found for the root.
        """
        as_of = as_of or _dt.datetime.now(_dt.timezone.utc)
        clean_root = root.lstrip("@").upper()

        contracts = await self._list_contracts(clean_root)
        active = []
        for c in contracts:
            sym = c["Symbol"]
            parsed = _parse_contract_symbol(sym, clean_root)
            if parsed is None:
                continue
            month, year = parsed
            expiry = _expiration_estimate(clean_root, month, year)
            roll_date = expiry - _dt.timedelta(days=self._roll_buffer_days)
            # Use roll_date (not expiry) as the cutover point
            if roll_date > as_of:
                active.append((roll_date, expiry, sym, month, year))

        if not active:
            raise ValueError(
                f"no active future-dated contracts found for root {root!r} "
                f"(checked {len(contracts)} listings)"
            )

        active.sort(key=lambda t: t[0])
        roll_date, expiry, sym, month, year = active[0]
        return FrontContract(
            root=root,
            contract_symbol=sym,
            contract_month=month,
            contract_year=year,
            expiration_utc=expiry,
            roll_date_utc=roll_date,
            resolved_at_utc=as_of,
        )

    async def _list_contracts(self, root: str) -> list[dict]:
        """Cached call to TS symbol search filtered to FUTURE category."""
        now = _dt.datetime.now(_dt.timezone.utc)
        cached = self._cache.get(root)
        if cached and (now - cached[0]).total_seconds() < self._cache_ttl:
            return cached[1]

        results = await self._http.search_symbols(root, category="Future")
        contracts = []
        for r in results:
            sym = r.get("Symbol", "")
            if _parse_contract_symbol(sym, root) is not None:
                contracts.append(r)
        self._cache[root] = (now, contracts)
        return contracts


_CONTRACT_RE = re.compile(r"^([A-Z]{1,3})([FGHJKMNQUVXZ])(\d{2})$")


def _parse_contract_symbol(sym: str, expected_root: str) -> Optional[tuple[int, int]]:
    """Parse a contract symbol like 'NQH26' -> (month=3, year=2026).

    Returns None if the symbol does not match the expected root or if it is
    not a valid futures contract symbol.
    """
    m = _CONTRACT_RE.match(sym.strip().upper())
    if not m:
        return None
    root, mc, yy = m.group(1), m.group(2), m.group(3)
    if root != expected_root:
        return None
    month = _MONTH_CODES[mc]
    year_int = int(yy)
    # 2-digit year: assume 20YY for YY >= 0..89, else 19YY (TS uses near-future
    # contracts only, so 2-digit year is unambiguous in practice)
    year = 2000 + year_int if year_int < 90 else 1900 + year_int
    return month, year


def _expiration_estimate(root: str, month: int, year: int) -> _dt.datetime:
    """Estimate contract expiration date based on standard CME conventions.

    This is a simplified estimate — actual expiration varies by product.
    Consumers should prefer the TS API's GetSymbolDetails for an exact
    expiration when accuracy matters. This estimate is used only for
    front-month rollover ordering.

    Defaults:
      - Equity index (NQ, ES, RTY): 3rd Friday of contract month
      - Energy (CL): ~3 business days before 25th of month preceding contract
      - Metals (GC, SI): last business day of month preceding contract
      - Currencies (EC, others): 2nd business day before 3rd Wednesday
      - Default fallback: 3rd Friday of contract month
    """
    if root in ("NQ", "ES", "RTY", "MNQ", "MES"):
        return _third_friday(year, month).replace(tzinfo=_dt.timezone.utc)
    if root in ("CL", "MCL"):
        # CL expires ~3 business days before 25th of prior month
        prior = _add_months(_dt.date(year, month, 25), -1)
        return _shift_business_days(prior, -3).replace(
            hour=0, tzinfo=_dt.timezone.utc
        ) if isinstance(prior, _dt.datetime) else _dt.datetime(
            *_shift_business_days(prior, -3).timetuple()[:3], tzinfo=_dt.timezone.utc
        )
    if root in ("GC", "SI", "MGC"):
        prior = _add_months(_dt.date(year, month, 1), -1)
        last = _last_business_day(prior.year, prior.month)
        return _dt.datetime(last.year, last.month, last.day, tzinfo=_dt.timezone.utc)
    if root in ("NG", "MNG"):
        prior = _add_months(_dt.date(year, month, 1), -1)
        last = _last_business_day(prior.year, prior.month)
        return _dt.datetime(last.year, last.month, last.day, tzinfo=_dt.timezone.utc)
    if root in ("EC",):
        third_wed = _nth_weekday_of_month(year, month, weekday=2, n=3)
        exp = _shift_business_days(third_wed, -2)
        return _dt.datetime(exp.year, exp.month, exp.day, tzinfo=_dt.timezone.utc)
    return _third_friday(year, month).replace(tzinfo=_dt.timezone.utc)


def _third_friday(year: int, month: int) -> _dt.datetime:
    return _dt.datetime(year, month, 1) + pd.tseries.offsets.WeekOfMonth(
        week=2, weekday=4
    )


def _nth_weekday_of_month(year: int, month: int, *, weekday: int, n: int) -> _dt.date:
    """weekday: Monday=0..Sunday=6"""
    d = _dt.date(year, month, 1)
    delta = (weekday - d.weekday()) % 7
    first = d + _dt.timedelta(days=delta)
    return first + _dt.timedelta(weeks=n - 1)


def _last_business_day(year: int, month: int) -> _dt.date:
    if month == 12:
        next_first = _dt.date(year + 1, 1, 1)
    else:
        next_first = _dt.date(year, month + 1, 1)
    d = next_first - _dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= _dt.timedelta(days=1)
    return d


def _shift_business_days(d: _dt.date, n: int) -> _dt.date:
    step = -1 if n < 0 else 1
    remaining = abs(n)
    while remaining > 0:
        d += _dt.timedelta(days=step)
        if d.weekday() < 5:
            remaining -= 1
    return d


def _add_months(d: _dt.date, n: int) -> _dt.date:
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    return _dt.date(y, m, min(d.day, 28))
