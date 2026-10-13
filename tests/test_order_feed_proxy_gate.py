"""
Order-feed opt-in gate: the order-feed proxy has its OWN opt-in
(``TS_ORDER_FEED_PROXY``), separate from the bar proxy's
``TS_FEED_PROXY``. ``TS_FEED_PROXY`` is already baked into every
supervisor-launched cell's env for bars, so if the order proxy reused it,
the instant this vendor build lands every already-running host would flip
into order-proxy mode -- a deployment-wide accidental cutover, not the one-
account canary Stage 1 requires.

These tests pin the decoupling at BOTH the pure resolver
(``_resolve_order_feed_proxy``) and the real ``_connect()`` wiring: with
``TS_FEED_PROXY`` set but ``TS_ORDER_FEED_PROXY`` absent, the client must
build the DIRECT ``TradeStationStreamClient`` for orders, never a
``FeedTailStreamClient``.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs

from nautilus_tradestation.execution import (
    TradeStationExecutionClient,
    _resolve_order_feed_proxy,
)
from nautilus_tradestation.feed.tail_client import FeedTailStreamClient
from nautilus_tradestation.providers import TradeStationInstrumentProvider
from nautilus_tradestation.streaming.client import TradeStationStreamClient
from tests.test_kit import TSTestInstrumentStubs

_ACCOUNT = "SIM_FUTURES"
_TRADER_ID = TestIdStubs.trader_id()


def _make_streaming_harness() -> SimpleNamespace:
    """A real ``TradeStationExecutionClient`` with ``use_streaming=True`` --
    unlike ``test_execution_event_hardening._make_harness`` (streaming off),
    this exercises the REAL ``_connect()`` proxy-wiring branch, with the
    account-state REST calls mocked out."""
    loop = asyncio.get_event_loop()
    clock = LiveClock()
    msgbus = MessageBus(trader_id=_TRADER_ID, clock=clock)
    cache = TestComponentStubs.cache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())

    http = MagicMock()
    http.base_url = "https://mock.tradestation.com/v3"

    client = TradeStationExecutionClient(
        loop=loop,
        client=http,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=TradeStationInstrumentProvider(client=http),
        account_id=_ACCOUNT,
        use_streaming=True,
    )
    client._set_account_id(AccountId(f"TRADESTATION-{_ACCOUNT}"))
    client._connect_get_accounts_with_backoff = AsyncMock(return_value=[])
    client._update_account_state = AsyncMock()
    return SimpleNamespace(client=client, http=http)


async def _connect_and_cleanup(client: TradeStationExecutionClient) -> None:
    """Run the real _connect(), then cancel the background tasks it starts
    before they get a chance to touch the fake/absent upstream."""
    await client._connect()
    for task in (client._fill_poll_task, client._status_reconcile_task):
        if task is not None:
            task.cancel()
    for task in (client._fill_poll_task, client._status_reconcile_task):
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


class TestResolveOrderFeedProxyPure:
    """Pure resolver -- no client, no event loop needed."""

    def test_absent_is_off_even_with_bar_proxy_armed(self, monkeypatch):
        monkeypatch.setenv("TS_FEED_PROXY", r"jsonl:D:\feed\SIM_FUTURES")
        monkeypatch.delenv("TS_ORDER_FEED_PROXY", raising=False)
        assert _resolve_order_feed_proxy() == ""

    def test_absent_is_off_with_no_bar_proxy_either(self, monkeypatch):
        monkeypatch.delenv("TS_FEED_PROXY", raising=False)
        monkeypatch.delenv("TS_ORDER_FEED_PROXY", raising=False)
        assert _resolve_order_feed_proxy() == ""

    def test_one_reuses_the_bar_proxy_root(self, monkeypatch):
        monkeypatch.setenv("TS_FEED_PROXY", r"jsonl:D:\feed\SIM_FUTURES")
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", "1")
        assert _resolve_order_feed_proxy() == r"jsonl:D:\feed\SIM_FUTURES"

    def test_true_string_also_reuses_the_bar_proxy_root(self, monkeypatch):
        monkeypatch.setenv("TS_FEED_PROXY", r"jsonl:D:\feed\SIM_FUTURES")
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", "true")
        assert _resolve_order_feed_proxy() == r"jsonl:D:\feed\SIM_FUTURES"

    def test_one_with_no_bar_proxy_resolves_to_empty_not_a_crash(self, monkeypatch):
        # "1" means "reuse TS_FEED_PROXY" -- if that is itself unset, there is
        # nothing to reuse: OFF, never a malformed jsonl: value.
        monkeypatch.delenv("TS_FEED_PROXY", raising=False)
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", "1")
        assert _resolve_order_feed_proxy() == ""

    def test_explicit_path_is_independent_of_the_bar_proxy(self, monkeypatch):
        monkeypatch.setenv("TS_FEED_PROXY", r"jsonl:D:\feed\SIM_FUTURES")
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", r"jsonl:D:\feed\SIM_CANARY_ONLY")
        assert _resolve_order_feed_proxy() == r"jsonl:D:\feed\SIM_CANARY_ONLY"

    def test_explicit_path_works_with_no_bar_proxy_at_all(self, monkeypatch):
        monkeypatch.delenv("TS_FEED_PROXY", raising=False)
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", r"jsonl:D:\feed\SIM_CANARY_ONLY")
        assert _resolve_order_feed_proxy() == r"jsonl:D:\feed\SIM_CANARY_ONLY"


class TestConnectWiringHonorsTheSeparateGate:
    async def test_bar_proxy_armed_alone_builds_direct_stream_client_for_orders(
        self, tmp_path, monkeypatch
    ):
        # THE regression this change request exists to prevent: TS_FEED_PROXY
        # set (as EVERY supervisor-launched cell already has it, for bars)
        # but TS_ORDER_FEED_PROXY absent -> orders stay fully direct.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        monkeypatch.setenv("TS_FEED_PROXY", f"jsonl:{tmp_path / 'SIM_FUTURES'}")
        monkeypatch.delenv("TS_ORDER_FEED_PROXY", raising=False)
        h = _make_streaming_harness()
        # A pre-existing direct stream client was built at __init__ time
        # (use_streaming=True, no proxy yet resolved) -- _connect() must
        # leave it in place for orders.
        pre_connect_client = h.client._stream_client
        assert isinstance(pre_connect_client, TradeStationStreamClient)
        await _connect_and_cleanup(h.client)
        assert h.client._stream_client is pre_connect_client
        assert isinstance(h.client._stream_client, TradeStationStreamClient)
        assert not isinstance(h.client._stream_client, FeedTailStreamClient)
        assert h.client._order_feed_proxy is None

    async def test_both_armed_builds_the_tail_client_for_orders(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        feed_dir = tmp_path / "SIM_FUTURES"
        monkeypatch.setenv("TS_FEED_PROXY", f"jsonl:{feed_dir}")
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", "1")
        h = _make_streaming_harness()
        await _connect_and_cleanup(h.client)
        assert isinstance(h.client._stream_client, FeedTailStreamClient)
        assert h.client._order_feed_proxy == f"jsonl:{feed_dir}"

    async def test_neither_armed_is_fully_direct(self, monkeypatch):
        monkeypatch.delenv("TS_FEED_PROXY", raising=False)
        monkeypatch.delenv("TS_ORDER_FEED_PROXY", raising=False)
        h = _make_streaming_harness()
        pre_connect_client = h.client._stream_client
        await _connect_and_cleanup(h.client)
        assert h.client._stream_client is pre_connect_client
        assert isinstance(h.client._stream_client, TradeStationStreamClient)

    async def test_explicit_order_only_path_works_with_no_bar_proxy(
        self, tmp_path, monkeypatch
    ):
        # A canary account could, in principle, arm ONLY the order proxy
        # (e.g. bars staying direct while orders are canaried) via an
        # explicit jsonl: path independent of TS_FEED_PROXY.
        monkeypatch.setenv("TS_FEED_ALLOW_ANY_DIR", "1")
        monkeypatch.delenv("TS_FEED_PROXY", raising=False)
        feed_dir = tmp_path / "SIM_FUTURES"
        monkeypatch.setenv("TS_ORDER_FEED_PROXY", f"jsonl:{feed_dir}")
        h = _make_streaming_harness()
        await _connect_and_cleanup(h.client)
        assert isinstance(h.client._stream_client, FeedTailStreamClient)
