"""Installed LiveExecutionEngine queue integration, with mocked broker transport."""
import asyncio
import pytest
from nautilus_trader.common.component import LiveClock
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.live.config import LiveExecEngineConfig
from nautilus_trader.model.events import OrderCanceled, OrderFilled
from tests.test_ambiguous_terminal_state import harness, broker_row
from nautilus_tradestation.execution import RAW_ORDER_CANCEL_TOPIC


@pytest.mark.parametrize('terminal', ['FLL', 'OUT'])
async def test_real_engine_queue_applies_recovery_before_following_terminal(monkeypatch, terminal):
    monkeypatch.setenv('TS_AMBIGUOUS_CANCEL_AUTHORITY', '1')
    h, order = harness(True, quantity=2)
    h.msgbus.deregister(endpoint='ExecEngine.process', handler=h.events.append)
    engine = LiveExecutionEngine(loop=asyncio.get_running_loop(), msgbus=h.msgbus,
        cache=h.cache, clock=LiveClock(), config=LiveExecEngineConfig(reconciliation=False))
    applied = []
    h.msgbus.subscribe(topic='events.order.*', handler=applied.append)
    partial = broker_row('FLP', 1)
    final = broker_row(terminal, 2 if terminal == 'FLL' else 1)
    await h.client._process_order_event(partial)
    await h.client._process_order_event(final)
    await asyncio.sleep(0)  # enqueue uses call_soon_threadsafe
    assert engine.evt_qsize() == 1 and float(order.filled_qty) == 0
    assert not applied and not h.evidence
    consumer = asyncio.create_task(engine._run_evt_queue())
    try:
        for _ in range(20):
            if applied: break
            await asyncio.sleep(0)
        assert len(applied) == 1 and isinstance(applied[0], OrderFilled)
        assert float(order.filled_qty) == 1
        await h.client._process_order_event(final)
        assert len(applied) == 1  # enqueue is not synchronous application
        for _ in range(20):
            if len(applied) == 2: break
            await asyncio.sleep(0)
        assert len(applied) == 2
        assert isinstance(applied[1], OrderFilled if terminal == 'FLL' else OrderCanceled)
        assert order.status.name == ('FILLED' if terminal == 'FLL' else 'CANCELED')
        assert float(order.filled_qty) == (2 if terminal == 'FLL' else 1)
        await h.client._process_order_event(final)
        assert engine.evt_qsize() == 0 and len(applied) == 2
    finally:
        consumer.cancel()
        await consumer
