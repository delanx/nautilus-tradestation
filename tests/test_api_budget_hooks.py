"""Tests for the optional per-account REST budget hooks.

Three behaviors are pinned here, each against
the REAL coroutines -- ``_request``, ``_connect_get_accounts_with_
backoff``, ``_reconcile_order_statuses_loop`` -- never a reimplementation:

  1. INSTRUMENT. Every REST call funnels through ``_request``, so every REST
     call is journaled with its endpoint, status and the vendor's own
     ``X-RateLimit-*`` headers. That is the single choke point to measure
     rather than guess.
  2. CEILING. The order-status reconcile poll -- and ONLY that poll -- asks
     the shared per-account budget before its REST call, so the aggregate
     backstop-poll rate is bounded regardless of node count.
  3. STARTUP BACKOFF. A connect that meets a 429 waits and retries within a
     bounded budget instead of killing the node (``Get accounts failed: HTTP 429`` ->
     a node start timeout with no strategies started).

The hooks reach the host through ``TRADESTATION_BUDGET_MODULE``
(``nautilus_tradestation/http/budget.py``). This repo's suite sets no host module, so the hooks are
inert here by construction -- which is itself one of the properties under
test (a standalone checkout must keep working). Tests that need the hooks
ACTIVE install a fake budget module, exercising the same call signatures the
real one exposes.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_tradestation.execution import (
    _STARTUP_CONNECT_BACKOFF_S,
    _STARTUP_CONNECT_MAX_WAIT_S,
    _startup_connect_schedule,
)
from nautilus_tradestation.http import budget as budget_mod
from nautilus_tradestation.http.client import (
    RateLimitedError,
    TradeStationHttpClient,
    _retry_after_from_headers,
)

from tests.test_execution_event_hardening import _make_harness


class _FakeBudget:
    """Stands in for a host budget module with the same surface the hook
    uses. Records what it was asked, and answers reservations from a queue so
    a test can drive "granted, then denied"."""

    KIND_CRITICAL = "critical"
    KIND_STARTUP = "startup"
    KIND_POLL = "poll"

    def __init__(self, answers=None):
        self.calls = []
        self.reservations = []
        self.flushes = []
        self.default_account = None
        self._answers = list(answers or [])

    def record_call(self, **kwargs):
        self.calls.append(kwargs)

    def set_default_account(self, account_id):
        self.default_account = account_id

    def flush_if_due(self, account_id):
        self.flushes.append(account_id)

    def reserve(self, account_id, *, kind="poll"):
        self.reservations.append((account_id, kind))
        granted = self._answers.pop(0) if self._answers else True
        return SimpleNamespace(granted=granted, reason="test" if granted else "ceiling_exceeded")


@pytest.fixture
def fake_budget(monkeypatch):
    fake = _FakeBudget()
    monkeypatch.setattr(budget_mod, "_MODULE", fake)
    monkeypatch.setattr(budget_mod, "_RESOLVED", True)
    yield fake


# --------------------------------------------------------------------------- #
# the seam itself
# --------------------------------------------------------------------------- #
class TestSoftImportSeam:
    def test_hooks_are_inert_without_a_host_module(self, monkeypatch):
        """A standalone checkout of this adapter must keep working with no
        host module configured -- the hook adds no dependency."""
        monkeypatch.setattr(budget_mod, "_MODULE", None)
        monkeypatch.setattr(budget_mod, "_RESOLVED", True)
        assert budget_mod.available() is False
        budget_mod.record(url="https://x/v3/brokerage/accounts", method="GET", status=200)
        budget_mod.set_default_account("SIM1")
        budget_mod.flush("SIM1")
        assert budget_mod.reserve_poll("SIM1") == (True, "budget_unavailable")
        assert budget_mod.reserve_startup("SIM1") == (True, "budget_unavailable")
        assert budget_mod.poll_kind() is None

    def test_reserve_fails_open_when_the_budget_module_raises(self, monkeypatch):
        """Fail-open is the contract: an instrument that can stop a trading
        node is worse than no instrument."""

        class _Exploding:
            KIND_POLL = "poll"
            KIND_STARTUP = "startup"

            def reserve(self, *a, **k):
                raise RuntimeError("budget state unreadable")

        monkeypatch.setattr(budget_mod, "_MODULE", _Exploding())
        monkeypatch.setattr(budget_mod, "_RESOLVED", True)
        granted, reason = budget_mod.reserve_poll("SIM1")
        assert granted is True
        assert reason.startswith("fallback_")

    def test_record_never_raises_when_the_budget_module_raises(self, monkeypatch):
        class _Exploding:
            def record_call(self, **k):
                raise RuntimeError("disk full")

        monkeypatch.setattr(budget_mod, "_MODULE", _Exploding())
        monkeypatch.setattr(budget_mod, "_RESOLVED", True)
        budget_mod.record(url="https://x", method="GET", status=200)  # must not raise


# --------------------------------------------------------------------------- #
# TRADESTATION_BUDGET_MODULE resolution
# --------------------------------------------------------------------------- #
class TestBudgetModuleEnv:
    def _unresolved(self, monkeypatch):
        monkeypatch.setattr(budget_mod, "_MODULE", None)
        monkeypatch.setattr(budget_mod, "_RESOLVED", False)

    def test_env_name_is_the_documented_one(self):
        assert budget_mod.BUDGET_MODULE_ENV == "TRADESTATION_BUDGET_MODULE"

    def test_unset_env_resolves_to_no_module(self, monkeypatch):
        self._unresolved(monkeypatch)
        monkeypatch.delenv("TRADESTATION_BUDGET_MODULE", raising=False)
        assert budget_mod.available() is False
        assert budget_mod.reserve_poll("SIM1") == (True, "budget_unavailable")

    def test_env_names_a_host_module_that_receives_the_calls(self, monkeypatch):
        import sys
        import types

        fake = _FakeBudget(answers=[False])
        host = types.ModuleType("fake_host_app_budget")
        for name in ("KIND_CRITICAL", "KIND_STARTUP", "KIND_POLL"):
            setattr(host, name, getattr(fake, name))
        for name in ("record_call", "set_default_account", "flush_if_due", "reserve"):
            setattr(host, name, getattr(fake, name))
        monkeypatch.setitem(sys.modules, "fake_host_app_budget", host)
        self._unresolved(monkeypatch)
        monkeypatch.setenv("TRADESTATION_BUDGET_MODULE", "fake_host_app_budget")

        assert budget_mod.available() is True
        budget_mod.record(url="https://x/v3/brokerage/accounts", method="GET", status=200)
        budget_mod.set_default_account("SIM1")
        budget_mod.flush("SIM1")
        assert budget_mod.reserve_poll("SIM1") == (False, "ceiling_exceeded")
        assert budget_mod.poll_kind() == "poll"
        assert budget_mod.startup_kind() == "startup"
        assert budget_mod.critical_kind() == "critical"
        assert fake.calls[0]["url"] == "https://x/v3/brokerage/accounts"
        assert fake.default_account == "SIM1"
        assert fake.flushes == ["SIM1"]
        assert fake.reservations == [("SIM1", "poll")]

    def test_unimportable_module_fails_open_and_is_resolved_once(self, monkeypatch):
        self._unresolved(monkeypatch)
        monkeypatch.setenv("TRADESTATION_BUDGET_MODULE", "no_such_host_module_xyz")
        assert budget_mod.available() is False
        assert budget_mod._RESOLVED is True
        assert budget_mod.reserve_startup("SIM1") == (True, "budget_unavailable")


# --------------------------------------------------------------------------- #
# 1. the instrument
# --------------------------------------------------------------------------- #
def _client() -> TradeStationHttpClient:
    return TradeStationHttpClient(
        client_id="id", client_secret="secret", refresh_token="tok", use_sandbox=True
    )


class TestRequestJournalsEveryCall:
    async def test_success_is_recorded_with_the_vendor_headers(self, monkeypatch, fake_budget):
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        response = MagicMock()
        response.status_code = 200
        response.headers = {"X-RateLimit-Limit": "120", "X-RateLimit-Remaining": "97"}
        monkeypatch.setattr(client._httpx, "get", AsyncMock(return_value=response))

        await client._request("GET", f"{client.base_url}/brokerage/accounts/SIM1/orders")

        assert len(fake_budget.calls) == 1
        recorded = fake_budget.calls[0]
        assert recorded["status"] == 200
        assert recorded["method"] == "GET"
        assert recorded["headers"]["X-RateLimit-Limit"] == "120"
        assert recorded["duration_ms"] is not None

    async def test_every_429_retry_attempt_is_recorded_not_just_the_last(
        self, monkeypatch, fake_budget
    ):
        """A 429 burst is the exact thing the journal exists to count. If only
        the final attempt were recorded the instrument would systematically
        under-report the true call rate -- the same blind spot that
        made three prior fixes aim at the wrong consumer."""
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        throttled = MagicMock()
        throttled.status_code = 429
        throttled.headers = {"Retry-After": "0"}
        monkeypatch.setattr(client._httpx, "get", AsyncMock(return_value=throttled))
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())

        await client._request("GET", f"{client.base_url}/brokerage/accounts/SIM1/orders")

        assert len(fake_budget.calls) == client._max_retries
        assert all(c["status"] == 429 for c in fake_budget.calls)

    async def test_a_transport_failure_is_recorded_as_status_zero_and_reraised(
        self, monkeypatch, fake_budget
    ):
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        monkeypatch.setattr(
            client._httpx, "get", AsyncMock(side_effect=ConnectionError("socket died"))
        )
        with pytest.raises(ConnectionError):
            await client._request("GET", f"{client.base_url}/brokerage/accounts/SIM1/orders")
        assert fake_budget.calls[0]["status"] == 0

    async def test_budget_kind_is_stripped_before_the_http_call(self, monkeypatch, fake_budget):
        """``budget_kind`` is our label, not a TradeStation parameter -- it
        must never reach httpx as a query/keyword argument."""
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        response = MagicMock()
        response.status_code = 200
        response.headers = {}
        getter = AsyncMock(return_value=response)
        monkeypatch.setattr(client._httpx, "get", getter)

        await client._request("GET", f"{client.base_url}/x", budget_kind="poll")

        assert "budget_kind" not in getter.call_args.kwargs
        assert fake_budget.calls[0]["kind"] == "poll"

    async def test_order_submission_is_recorded_but_never_reserved(
        self, monkeypatch, fake_budget
    ):
        """The single most important property in this repo's half of the fix:
        the order path gains an append, and nothing else. No reservation, no
        lock, no wait."""
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        response = MagicMock()
        response.status_code = 200
        response.headers = {}
        monkeypatch.setattr(client._httpx, "post", AsyncMock(return_value=response))

        await client._request("POST", f"{client.base_url}/orderexecution/orders", json={})

        assert len(fake_budget.calls) == 1
        assert fake_budget.reservations == []


class TestRateLimitedError:
    async def test_get_accounts_raises_the_typed_error_on_429(self, monkeypatch, fake_budget):
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        throttled = MagicMock()
        throttled.status_code = 429
        throttled.headers = {"Retry-After": "12"}
        throttled.text = "slow down"
        monkeypatch.setattr(client._httpx, "get", AsyncMock(return_value=throttled))
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())

        with pytest.raises(RateLimitedError) as excinfo:
            await client.get_accounts()
        assert excinfo.value.status_code == 429
        assert excinfo.value.retry_after_seconds == 12.0
        # Message text unchanged so existing log greps still match.
        assert "Get accounts failed: HTTP 429" in str(excinfo.value)

    async def test_other_failures_still_raise_a_plain_exception(self, monkeypatch, fake_budget):
        """Narrowness matters: only 429 becomes retryable. A 403 must keep
        failing immediately and loudly, never be swallowed by a retry loop."""
        client = _client()
        monkeypatch.setattr(client, "_get_headers", AsyncMock(return_value={}))
        forbidden = MagicMock()
        forbidden.status_code = 403
        forbidden.headers = {}
        forbidden.text = "nope"
        monkeypatch.setattr(client._httpx, "get", AsyncMock(return_value=forbidden))

        with pytest.raises(Exception) as excinfo:
            await client.get_accounts()
        assert not isinstance(excinfo.value, RateLimitedError)

    @pytest.mark.parametrize(
        "headers,expected",
        [
            ({"Retry-After": "5"}, 5.0),
            ({"Retry-After": ""}, None),
            ({}, None),
            (None, None),
            ({"Retry-After": "junk"}, None),
        ],
    )
    def test_retry_after_parsing(self, headers, expected):
        assert _retry_after_from_headers(headers) == expected


# --------------------------------------------------------------------------- #
# 3. the startup backoff
# --------------------------------------------------------------------------- #
class TestStartupConnectSchedule:
    def test_schedule_is_bounded_by_the_max_wait(self):
        schedule = _startup_connect_schedule()
        assert sum(schedule) <= _STARTUP_CONNECT_MAX_WAIT_S
        assert schedule, "a bounded budget is not a zero budget"

    def test_schedule_stays_under_the_launcher_observation_window(self):
        """A process launcher may grade a launch at +90s. A retry budget
        that outlasts it would trade "dies instantly" for "survives the burst
        but is graded dead anyway" -- no gain."""
        assert sum(_startup_connect_schedule()) < 90.0

    def test_a_zero_budget_disables_retrying_entirely(self):
        assert _startup_connect_schedule(max_wait_s=0.0) == []

    def test_a_venue_retry_after_replaces_the_first_delay_when_larger(self):
        schedule = _startup_connect_schedule(retry_after_s=25.0)
        assert schedule[0] == 25.0
        assert sum(schedule) <= _STARTUP_CONNECT_MAX_WAIT_S

    def test_a_small_retry_after_does_not_shorten_our_own_backoff(self):
        assert _startup_connect_schedule(retry_after_s=0.1)[0] == _STARTUP_CONNECT_BACKOFF_S[0]

    def test_a_tight_budget_truncates_rather_than_overrunning(self):
        schedule = _startup_connect_schedule(max_wait_s=7.0)
        assert sum(schedule) <= 7.0


class TestConnectBackoff:
    async def test_a_429_burst_no_longer_kills_the_node(self, monkeypatch, fake_budget):
        """The startup 429 failure, inverted into a guarantee: two
        429s in a row are ridden out and the node connects."""
        h = _make_harness()
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        h.http.get_accounts = AsyncMock(
            side_effect=[
                RateLimitedError("Get accounts failed: HTTP 429"),
                RateLimitedError("Get accounts failed: HTTP 429"),
                [{"AccountID": "SIM1"}],
            ]
        )
        accounts = await h.client._connect_get_accounts_with_backoff()
        assert accounts == [{"AccountID": "SIM1"}]
        assert h.http.get_accounts.await_count == 3

    async def test_it_gives_up_after_a_bounded_number_of_retries(self, monkeypatch, fake_budget):
        """Bounded, not infinite: a permanently saturated account must still
        surface as a failed connect rather than a node that hangs forever
        looking alive."""
        h = _make_harness()
        monkeypatch.setattr(asyncio, "sleep", AsyncMock())
        h.http.get_accounts = AsyncMock(
            side_effect=RateLimitedError("Get accounts failed: HTTP 429")
        )
        with pytest.raises(RateLimitedError):
            await h.client._connect_get_accounts_with_backoff()
        expected = len(_startup_connect_schedule()) + 1
        assert h.http.get_accounts.await_count == expected

    async def test_non_429_failures_are_not_retried(self, monkeypatch, fake_budget):
        """Retrying a credentials failure would hide a real fault, which only
        masks the underlying problem."""
        h = _make_harness()
        slept = AsyncMock()
        monkeypatch.setattr(asyncio, "sleep", slept)
        h.http.get_accounts = AsyncMock(side_effect=ValueError("bad credentials"))
        with pytest.raises(ValueError):
            await h.client._connect_get_accounts_with_backoff()
        assert h.http.get_accounts.await_count == 1
        assert slept.await_count == 0

    async def test_startup_uses_the_priority_band_and_is_never_blocked(
        self, monkeypatch, fake_budget
    ):
        """Startup asks for the reserved band, but a denial only WARNS -- the
        connect proceeds. A budget that could refuse a node its startup calls
        would cause the exact failure it exists to prevent."""
        h = _make_harness()
        fake_budget._answers = [False]
        h.http.get_accounts = AsyncMock(return_value=[{"AccountID": "SIM1"}])
        accounts = await h.client._connect_get_accounts_with_backoff()
        assert accounts == [{"AccountID": "SIM1"}]
        assert fake_budget.reservations == [(str(h.client._account_id), "startup")]


# --------------------------------------------------------------------------- #
# 2. the ceiling, applied to the reconcile poll ONLY
# --------------------------------------------------------------------------- #
class TestReconcilePollCeiling:
    async def test_a_denied_reservation_skips_the_cycle(self, monkeypatch, fake_budget):
        monkeypatch.setattr(
            "nautilus_tradestation.execution._reconcile_initial_jitter_seconds",
            lambda _interval: 0.0,
        )
        h = _make_harness()
        h.client._has_pending_orders = lambda: True
        fake_budget._answers = [False, False, True]
        calls = []

        async def _fake_check(*, budget_kind=None):
            calls.append(budget_kind)
            raise asyncio.CancelledError

        h.client._check_order_statuses = _fake_check
        await h.client._reconcile_order_statuses_loop(0.01)

        # Two ticks denied, the third admitted -- the REST call happened once.
        assert calls == ["poll"]
        assert [k for _a, k in fake_budget.reservations] == ["poll", "poll", "poll"]

    async def test_a_granted_reservation_polls_as_before(self, monkeypatch, fake_budget):
        monkeypatch.setattr(
            "nautilus_tradestation.execution._reconcile_initial_jitter_seconds",
            lambda _interval: 0.0,
        )
        h = _make_harness()
        h.client._has_pending_orders = lambda: True
        calls = []

        async def _fake_check(*, budget_kind=None):
            calls.append(budget_kind)
            if len(calls) >= 3:
                raise asyncio.CancelledError

        h.client._check_order_statuses = _fake_check
        await h.client._reconcile_order_statuses_loop(0.01)
        assert calls == ["poll", "poll", "poll"]

    async def test_the_sse_proactive_fill_check_is_never_gated(self, monkeypatch, fake_budget):
        """Load-bearing placement test. ``_check_order_statuses`` is also
        called (a) proactively from ``_cancel_order`` when SSE reports an
        order missing at the broker, and (b) as ``_poll_order_fills``' PRIMARY
        fill detection when streaming is off. Gating either would trade an
        API-budget problem for a MISSED FILL. Only the backstop loop reserves;
        a direct call must consult no budget at all."""
        h = _make_harness()
        h.client._ts_order_id_to_client_order_id = {"TS-1": MagicMock()}
        h.http.get_orders = AsyncMock(return_value=[])

        await h.client._check_order_statuses()

        assert fake_budget.reservations == [], (
            "a direct _check_order_statuses call consulted the budget -- the gate has "
            "leaked out of the backstop loop onto a real fill-detection path"
        )
        assert h.http.get_orders.await_args.kwargs["budget_kind"] is None

    async def test_the_non_streaming_fill_loop_flushes_but_is_never_gated(
        self, monkeypatch, fake_budget
    ):
        """With streaming off this loop IS the fill path, so it is never
        denied -- but its calls must still reach the shared window, or the
        ceiling would under-count the total by every non-streaming node."""
        h = _make_harness()
        h.client._fill_poll_interval = 0.01
        calls = []

        async def _fake_check(*, budget_kind=None):
            calls.append(budget_kind)
            raise asyncio.CancelledError

        h.client._check_order_statuses = _fake_check
        await h.client._poll_order_fills()

        assert fake_budget.flushes == [str(h.client._account_id)]
        assert fake_budget.reservations == []
