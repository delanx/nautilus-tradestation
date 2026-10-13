"""Real-cache regressions for UCN/DON split state; all transport is mocked."""
from unittest.mock import AsyncMock
import pytest
from nautilus_trader.model.events import OrderCanceled, OrderFilled
from nautilus_trader.model.identifiers import VenueOrderId
from tests.test_execution_event_hardening import _make_harness, _stop_order, _arm
from nautilus_tradestation.execution import RAW_ORDER_CANCEL_TOPIC
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.objects import Quantity


@pytest.fixture(autouse=True)
def enable_authority(monkeypatch):
    monkeypatch.setenv('TS_AMBIGUOUS_CANCEL_AUTHORITY', '1')


def harness(poison=False, quantity=1):
    h = _make_harness()
    template = _stop_order()
    order = type(template)(trader_id=template.trader_id, strategy_id=template.strategy_id,
        instrument_id=template.instrument_id, client_order_id=template.client_order_id,
        order_side=template.side, quantity=Quantity.from_int(quantity),
        trigger_price=template.trigger_price, trigger_type=template.trigger_type,
        time_in_force=template.time_in_force, init_id=UUID4(), ts_init=0)
    _arm(h, order)
    h.evidence=[];h.msgbus.subscribe(topic=RAW_ORDER_CANCEL_TOPIC,handler=h.evidence.append)
    if poison:
        h.client.generate_order_canceled(strategy_id=order.strategy_id,instrument_id=order.instrument_id,client_order_id=order.client_order_id,venue_order_id=VenueOrderId('TS-1'),ts_event=1)
        order.apply(h.events[-1]);h.events.clear();h.client._order_last_status['TS-1']='UCN'
        assert order.is_closed
    return h,order


@pytest.mark.parametrize('raw',['UCN','DON'])
async def test_ambiguous_notice_never_closes_nautilus_cache(raw):
    h,order=harness()
    await h.client._process_order_event({'OrderID':'TS-1','Status':raw})
    for event in h.events: order.apply(event)
    assert not order.is_closed
    assert not any(isinstance(e,OrderCanceled) for e in h.events)
    assert h.evidence[-1]['raw_status']==raw
    assert h.client._has_pending_orders()


async def test_poisoned_cache_real_out_reaches_resolution_channel_once():
    h,order=harness(True)
    row={'AccountID':'SIM0000001F','Legs':[{'Symbol':'GCJ26','BuyOrSell':'Sell'}],'OrderID':'TS-1','Status':'OUT','Quantity':'1','FilledQuantity':'0'}
    await h.client._process_order_event(row)
    await h.client._process_order_event(row)
    assert len(h.evidence)==1
    assert h.evidence[0]['resolution_kind']=='confirmed_terminal'
    assert h.evidence[0]['raw_status']=='OUT'
    assert not h.events  # never synthesize a second NT canceled event


async def test_poisoned_cache_real_fill_is_not_skipped_or_duplicated():
    h,order=harness(True)
    row={'AccountID':'SIM0000001F','Legs':[{'Symbol':'GCJ26','BuyOrSell':'Sell'}],'OrderID':'TS-1','Status':'FLL','Quantity':'1','FilledQuantity':'1','AveragePrice':'3301.0'}
    await h.client._process_order_event(row)
    fills=[e for e in h.events if isinstance(e,OrderFilled)]
    assert len(fills)==1
    order.apply(fills[0])
    await h.client._process_order_event(row)
    assert len([e for e in h.events if isinstance(e,OrderFilled)])==1
    assert str(order.status.name) == "FILLED"


@pytest.mark.parametrize('status', ['ACK', 'UNKNOWN'])
async def test_poisoned_cache_nonterminal_status_cannot_release_or_reopen(status):
    h, order = harness(True)
    await h.client._process_order_event({'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': status})
    await h.client._process_order_event({'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': status})
    assert order.is_closed and not h.events and not h.evidence
    assert h.client._has_pending_orders()


@pytest.mark.parametrize('bad', [
    {'AveragePrice': '0'}, {'AveragePrice': 'NaN'}, {'FilledQuantity': None},
    {'FilledQuantity': '2'}, {'AccountID': 'WRONG'},
    {'Legs': [{'Symbol': 'WRONG'}]}, {'Legs': [{'BuyOrSell': 'Buy'}]},
])
async def test_poisoned_fill_requires_actual_matching_execution(bad):
    h, order = harness(True)
    row = {'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': 'FLL', 'FilledQuantity': '1', 'AveragePrice': '3301'}
    row.update(bad)
    await h.client._process_order_event(row)
    assert not h.events and not h.evidence and order.is_closed
    assert h.client._order_last_status['TS-1'] == 'UCN'


async def test_actual_filled_price_can_replace_zero_average_without_trigger_fallback():
    h, order = harness(True)
    await h.client._process_order_event({'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': 'FLL',
        'FilledQuantity': '1', 'AveragePrice': '0', 'FilledPrice': '3302'})
    assert len(h.events) == 1 and float(h.events[0].last_px) == 3302


@pytest.mark.parametrize('status', ['OUT', 'FLL'])
async def test_historical_query_repairs_poisoned_cache_without_report_inference(status):
    h, order = harness(True)
    row = {'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': status, 'FilledQuantity': '0' if status == 'OUT' else '1', 'AveragePrice': '3301'}
    h.http.get_orders = AsyncMock(return_value=[])
    h.http.get_historical_orders = AsyncMock(return_value=[row])
    command = GenerateOrderStatusReport(instrument_id=order.instrument_id,
        client_order_id=order.client_order_id, venue_order_id=VenueOrderId('TS-1'),
        command_id=UUID4(), ts_init=0)
    assert await h.client.generate_order_status_report(command) is None
    for event in h.events: order.apply(event)
    assert await h.client.generate_order_status_report(command) is None
    assert len(h.evidence) == (1 if status == 'OUT' else 0)
    assert len(h.events) == (1 if status == 'FLL' else 0)
    assert h.http.get_historical_orders.await_count == 2


async def test_partial_then_final_fill_delivers_only_cumulative_delta():
    h, order = harness(True, quantity=2)
    partial = {'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': 'FLP', 'FilledQuantity': '1', 'AveragePrice': '3301'}
    await h.client._process_order_event(partial)
    order.apply(h.events[-1])
    assert order.status.name == 'PARTIALLY_FILLED'
    await h.client._process_order_event(partial)
    assert len(h.events) == 1
    final = dict(partial, Status='FLL', FilledQuantity='2', AveragePrice='3302')
    await h.client._process_order_event(final)
    assert len(h.events) == 2
    assert float(h.events[-1].last_qty) == 1 and float(h.events[-1].last_px) == 3303
    order.apply(h.events[-1])
    await h.client._process_order_event(final)
    assert len(h.events) == 2 and order.status.name == 'FILLED'
    assert float(order.avg_px) == 3302


async def test_confirmed_cancel_with_unproven_partial_execution_stays_unresolved():
    h, order = harness(True, quantity=2)
    await h.client._process_order_event({'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': 'OUT', 'FilledQuantity': '1'})
    assert not h.events and not h.evidence
    assert h.client._order_last_status['TS-1'] == 'UCN'


async def test_poll_uses_same_authority_and_late_ambiguous_notice_cannot_regress_out():
    h, order = harness(True)
    h.http.get_orders = AsyncMock(return_value=[{'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': 'OUT', 'FilledQuantity': '0'}])
    await h.client._check_order_statuses()
    await h.client._process_order_event({'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}], 'OrderID': 'TS-1', 'Status': 'UCN'})
    assert len(h.evidence) == 1 and not h.events
    assert h.client._order_last_status['TS-1'] == 'OUT'


def broker_row(status, qty):
    return {'AccountID': 'SIM0000001F', 'Legs': [{'Symbol': 'GCJ26', 'BuyOrSell': 'Sell'}],
            'OrderID': 'TS-1', 'Status': status, 'FilledQuantity': str(qty), 'AveragePrice': '3301'}


@pytest.mark.parametrize('next_status', ['FLL', 'OUT'])
async def test_undrained_engine_queue_defers_later_fill_and_terminal_proof(next_status):
    h, order = harness(True, quantity=2)
    partial = broker_row('FLP', 1)
    final = broker_row(next_status, 2 if next_status == 'FLL' else 1)
    await h.client._process_order_event(partial)
    await h.client._process_order_event(partial)
    await h.client._process_order_event(final)
    assert len(h.events) == 1 and not h.evidence
    assert float(h.events[0].last_qty) == 1
    assert h.client._has_pending_orders()
    order.apply(h.events[0])  # model the engine draining its event queue
    await h.client._process_order_event(final)
    assert len(h.events) == 2
    if next_status == 'FLL':
        assert isinstance(h.events[1], OrderFilled) and float(h.events[1].last_qty) == 1
    else:
        assert isinstance(h.events[1], OrderCanceled)
    order.apply(h.events[1])
    await h.client._process_order_event(final)
    assert len(h.events) == 2
    await h.client._process_order_event(broker_row('FLP', 0))  # stale lower cumulative row
    assert len(h.events) == 2 and float(order.filled_qty) == (2 if next_status == 'FLL' else 1)


@pytest.mark.parametrize('field', ['AccountID', 'Symbol', 'BuyOrSell'])
async def test_recovery_requires_complete_identity(field):
    h, order = harness(True)
    row = broker_row('FLL', 1)
    if field == 'AccountID': row.pop(field)
    else: row['Legs'][0].pop(field)
    await h.client._process_order_event(row)
    assert not h.events and not h.evidence


@pytest.mark.parametrize('fault', ['account', 'reverse_map'])
async def test_ambiguous_notice_rejects_supplied_conflicting_identity(fault):
    h, order = harness()
    row = broker_row('UCN', 0)
    if fault == 'account': row['AccountID'] = 'WRONG'
    else: h.client._client_order_id_to_ts_order_id[order.client_order_id] = 'OTHER'
    await h.client._process_order_event(row)
    assert not h.events and not h.evidence


async def test_default_off_is_explicit_baseline_rollback(monkeypatch):
    monkeypatch.delenv('TS_AMBIGUOUS_CANCEL_AUTHORITY')
    h, order = harness()
    await h.client._process_order_event(broker_row('UCN', 0))
    assert len(h.events) == 1 and isinstance(h.events[0], OrderCanceled)
    assert 'resolution_kind' not in h.evidence[0]


async def test_out_with_execution_waits_for_fill_application_before_cancellation():
    h, order = harness(True, quantity=2)
    row = broker_row('OUT', 1)
    await h.client._process_order_event(row)
    await h.client._process_order_event(row)
    assert len(h.events) == 1 and isinstance(h.events[0], OrderFilled)
    assert not h.evidence  # no cancellation proof can overtake the queued fill
    order.apply(h.events[0])
    await h.client._process_order_event(row)
    assert len(h.events) == 2 and isinstance(h.events[1], OrderCanceled)
    order.apply(h.events[1])
    await h.client._process_order_event(row)
    assert len(h.events) == 2

