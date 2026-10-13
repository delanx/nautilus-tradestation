"""
Shared fixtures for TradeStation adapter integration tests.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_tradestation.constants import TRADESTATION_VENUE
from nautilus_tradestation.providers import TradeStationInstrumentProvider
from nautilus_trader.model.identifiers import Venue
from tests.mock_http_client import MockTradeStationHttpClient
from tests.test_kit import TSTestInstrumentStubs


@pytest.fixture
def venue() -> Venue:
    """Return the TradeStation venue."""
    return TRADESTATION_VENUE


@pytest.fixture
def mock_http_client() -> MagicMock:
    """Return a mock HTTP client for provider tests.

    test_providers.py sets .return_value / .side_effect on individual methods.
    Since the client is now async, get_symbol_details must be an AsyncMock so
    ``await client.get_symbol_details(...)`` works in the provider's load_async.
    """
    client = MagicMock()
    client.get_symbol_details = AsyncMock()
    return client


@pytest.fixture
def stub_http_client() -> MockTradeStationHttpClient:
    """Return the concrete MockTradeStationHttpClient with pre-loaded stub data."""
    return MockTradeStationHttpClient()


@pytest.fixture
def instrument_provider(mock_http_client) -> TradeStationInstrumentProvider:
    """Create a TradeStationInstrumentProvider backed by the mock client."""
    return TradeStationInstrumentProvider(client=mock_http_client)


@pytest.fixture
def instrument():
    """Return a GC futures contract for testing."""
    return TSTestInstrumentStubs.gc_futures_contract()


@pytest.fixture
def data_client():
    """Placeholder — not used in provider tests."""
    return None


@pytest.fixture
def exec_client():
    """Placeholder — not used in provider tests."""
    return None


@pytest.fixture
def account_state():
    """Placeholder — not used in provider tests."""
    return None


@pytest.fixture(autouse=True)
def _api_budget_hooks_inert_by_default(monkeypatch):
    """Keep the optional host hooks INERT for this repo's whole suite unless a
    test opts in.

    ``nautilus_tradestation/http/budget.py`` imports the module named by
    ``TRADESTATION_BUDGET_MODULE`` and ``execution._report_order_feed_state``
    calls the function named by ``TRADESTATION_ORDER_FEED_HOOK``. If either
    variable happens to be set in the environment that runs these tests, an
    unguarded suite would resolve the REAL host module and every mocked REST
    call would be journaled into that host's production measurements.

    Clearing both variables and pinning ``_MODULE = None`` makes the
    documented standalone behavior -- "no host module, every hook is a
    no-op" -- the suite's default; ``tests/test_api_budget_hooks.py`` and
    ``tests/test_order_feed_seam.py`` install their own fakes where they need
    the hooks live.
    """
    monkeypatch.delenv("TRADESTATION_BUDGET_MODULE", raising=False)
    monkeypatch.delenv("TRADESTATION_ORDER_FEED_HOOK", raising=False)
    from nautilus_tradestation.http import budget as _budget_mod

    monkeypatch.setattr(_budget_mod, "_MODULE", None)
    monkeypatch.setattr(_budget_mod, "_RESOLVED", True)
