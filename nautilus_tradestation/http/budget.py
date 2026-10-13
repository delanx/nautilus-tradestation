"""Optional host hook for a per-account REST budget.

WHY A HOOK AND NOT A DEPENDENCY
Every REST call this adapter makes funnels through
``TradeStationHttpClient._request`` -- that method is the single choke point
where an account's true call rate can be measured. But when many processes
share one TradeStation account, the account-wide view of that rate has to
live in the host application that runs those processes (its metrics store,
its reports, its alerting). Duplicating that accounting here would create
two uncoordinated rate-limit layers, each blind to the other.

So the adapter calls an OPTIONAL host-supplied budget module, named by an
environment variable:

    TRADESTATION_BUDGET_MODULE="pkg.module"

The module is imported once per process. When the variable is unset (the
default), or the module cannot be imported, every hook below is a no-op --
the adapter stays usable on its own, with no new dependency and no behavior
change.

HOST MODULE INTERFACE (all optional to call, all must exist):
  * ``record_call(*, url, method, status, headers, duration_ms, kind)``
  * ``set_default_account(account_id)``
  * ``flush_if_due(account_id)``
  * ``reserve(account_id, *, kind) -> obj`` with ``.granted`` (bool) and
    ``.reason`` (str)
  * constants ``KIND_POLL``, ``KIND_STARTUP``, ``KIND_CRITICAL``

CONTRACT
  * ``record(...)`` never raises and never blocks. It is called on the REST
    hot path, INCLUDING the order-submission path, so the host's
    ``record_call`` must add nothing but a cheap append.
  * ``reserve(...)`` returns ``True`` (proceed) unless the host budget module
    is present, enforcement is on, and the account is genuinely over its
    ceiling for that kind of traffic. Absent module, absent config, or on ANY
    error it returns ``True``. Fail-open is not a convenience here: an
    instrument that can stop a trading node from working is worse than no
    instrument.
  * Order submission/replace/cancel is recorded but is NEVER passed through
    ``reserve``. See ``_request``'s call sites.
"""

from __future__ import annotations

import importlib
import os
from typing import Any

#: Environment variable naming the host budget module ("pkg.module").
BUDGET_MODULE_ENV = "TRADESTATION_BUDGET_MODULE"

_MODULE: Any = None
_RESOLVED = False


def _module() -> Any:
    """The host budget module named by ``TRADESTATION_BUDGET_MODULE``, or
    ``None``.

    Resolved once per process. A failed resolution is cached too: a process
    that has no host module must not pay an import attempt on every single
    REST call."""
    global _MODULE, _RESOLVED
    if _RESOLVED:
        return _MODULE
    _RESOLVED = True
    name = os.environ.get(BUDGET_MODULE_ENV, "").strip()
    if not name:
        _MODULE = None
        return _MODULE
    try:
        _MODULE = importlib.import_module(name)
    except Exception:  # noqa: BLE001 -- no host module: hooks become no-ops
        _MODULE = None
    return _MODULE


def available() -> bool:
    return _module() is not None


def record(
    *,
    url: str,
    method: str,
    status: int | None,
    headers: Any = None,
    duration_ms: float | None = None,
    kind: str | None = None,
) -> None:
    """Journal one REST call. Silent no-op when no host budget module is configured."""
    mod = _module()
    if mod is None:
        return
    try:
        mod.record_call(
            url=url,
            method=method,
            status=status,
            headers=headers,
            duration_ms=duration_ms,
            kind=kind,
        )
    except Exception:  # noqa: BLE001
        return


def set_default_account(account_id: str | None) -> None:
    """Tell the budget which account THIS process draws down, so calls whose
    URL carries no account id (the connect-time ``GET /brokerage/accounts``,
    every ``/marketdata/*`` fetch) are still attributed to the right bucket."""
    mod = _module()
    if mod is None:
        return
    try:
        mod.set_default_account(account_id)
    except Exception:  # noqa: BLE001
        return


def reserve_poll(account_id: str | None) -> tuple[bool, str]:
    """Ask the shared per-account ceiling whether a background POLL may issue
    its REST call now.

    Returns ``(granted, reason)``. ``granted=False`` means SKIP THIS CYCLE --
    the caller is a periodic safety net that will come back on its own clock,
    never a primary fill path. Always ``True`` when no host budget module is configured."""
    mod = _module()
    if mod is None or not account_id:
        return True, "budget_unavailable"
    try:
        reservation = mod.reserve(str(account_id), kind=mod.KIND_POLL)
        return bool(reservation.granted), str(reservation.reason)
    except Exception as exc:  # noqa: BLE001 -- fail OPEN
        return True, f"fallback_{type(exc).__name__}"


def reserve_startup(account_id: str | None) -> tuple[bool, str]:
    """Startup-priority reservation for a node's connect-time account calls.

    Draws on the FULL window capacity rather than the poll band, so a new node
    can still connect into an account whose pollers have used their share --
    the 2026-09-07 16:58 ET ``node_start_timeout`` (``another-strategy``, host
    7248) that this priority split exists to prevent."""
    mod = _module()
    if mod is None or not account_id:
        return True, "budget_unavailable"
    try:
        reservation = mod.reserve(str(account_id), kind=mod.KIND_STARTUP)
        return bool(reservation.granted), str(reservation.reason)
    except Exception as exc:  # noqa: BLE001 -- fail OPEN
        return True, f"fallback_{type(exc).__name__}"


def startup_kind() -> str | None:
    """The reservation-kind label for a node's connect-time account calls, or
    ``None`` when no host budget module is configured (an unlabelled call is classified from its URL
    instead, which is correct but coarser)."""
    mod = _module()
    if mod is None:
        return None
    try:
        return str(mod.KIND_STARTUP)
    except Exception:  # noqa: BLE001
        return None


def poll_kind() -> str | None:
    """The reservation-kind label for a background safety-net poll."""
    mod = _module()
    if mod is None:
        return None
    try:
        return str(mod.KIND_POLL)
    except Exception:  # noqa: BLE001
        return None


def critical_kind() -> str | None:
    """The reservation-kind label for order-path traffic (recorded, never
    gated), or ``None`` when no host budget module is configured."""
    mod = _module()
    if mod is None:
        return None
    try:
        return str(mod.KIND_CRITICAL)
    except Exception:  # noqa: BLE001
        return None


def flush(account_id: str | None) -> None:
    """Fold this process's pending call counts into the shared per-account
    window. A process that only SUBMITS orders never calls ``reserve``, so
    without this its traffic would be invisible to the ceiling and the
    account-wide count would silently under-report."""
    mod = _module()
    if mod is None or not account_id:
        return
    try:
        mod.flush_if_due(str(account_id))
    except Exception:  # noqa: BLE001
        return
