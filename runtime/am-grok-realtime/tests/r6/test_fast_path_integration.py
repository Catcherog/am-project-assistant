"""Actual intake/action/runtime components with synthetic host I/O only."""
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r5'))
from test_assistant_coordinator import ALICE, OWNER, message, rig
from am_gateway.phase1.service import RuntimeService
from integration.assistant_queries import StudioProjectQueries


def snapshot(rig, *, progress='材料已到，预计周一开始测试。', version='v1', rows=True):
    return {'version': version, 'source_revision': version, 'source': 'synthetic:DEMO',
        'observed_at': rig.clock[0], 'complete': True, 'stale': False, 'refresh_error': None,
        'cache_hit': True, 'rows': [{'key': 'row-hd', 'project_ref': 'row-hd',
            'business_id': 'QI-2099-007', 'identity_status': 'verified',
            'title': '示例平台', 'aliases': ['示例平台'], '项目进展': progress,
            '待办': '核对测试计划，待报告回读。', '关键日期': '预计11月20日汇报。',
            '人员与职责': '合成负责人'}] if rows else []}


class Snapshots:
    def __init__(self, rig, value=None, *, automatic=True):
        self.clock = lambda: rig.clock[0]
        self.value = value
        self.automatic = automatic
        self.next_value = None
        self.future = None
        self.reads = 0

    def get(self, *, wait=False):
        assert wait is False, 'the foreground lane may not block on source I/O'
        if self.value is None:
            raise RuntimeError('SOURCE_LOADING')
        return deepcopy(self.value)

    def refresh(self):
        if self.future is not None and not self.future.done():
            return self.future
        self.reads += 1
        self.future = Future()
        if self.automatic:
            if self.next_value is not None:
                self.value = self.next_value
            if self.value is None:
                self.future.set_exception(RuntimeError('source unavailable'))
            else:
                self.future.set_result(deepcopy(self.value))
        return self.future

    def complete(self, value):
        self.value = value
        self.future.set_result(deepcopy(value))


def bind(rig, snapshots=None):
    snap = snapshots or Snapshots(rig, snapshot(rig))
    reader = StudioProjectQueries(NS(am_self=NS(snapshots=snap)), rig.authority,
                                 {'ledger_project_refs': {'HD': 'row-hd'}})
    rig.c.project_reader = reader
    return snap, reader


def ask(rig, text='请帮我看看示例平台的进度', *, mid='om_fast', group=False, **kwargs):
    event = message(rig, text, mid=mid, projects=(),
        chat='oc_hd' if group else 'oc_alice', channel='group' if group else 'bot_dm',
        mentions=(rig.cfg.bot_id,) if group else (), **kwargs)
    rig.c.capture(event)
    return event, rig.c.intake_once()


@pytest.mark.parametrize('group', [False, True])
def test_fact_query_reaches_existing_sender_without_model_or_harness_task(rig, group):
    bind(rig)
    event, outcome = ask(rig, group=group)
    assert outcome['route'] == 'ledger_direct'
    assert outcome['retrieval_status'] == 'found'
    assert rig.store.rows('SELECT * FROM h1_tasks') == []
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 1
    assert '预计周一' in rig.sends[0]['payload']['body']
    assert rig.sends[0]['payload']['target']['channel'] == event.channel
    assert rig.c.trace.local_elapsed(event.item_id) is not None
    assert rig.c.local_delivery_once() == 0


def test_cold_read_yields_and_other_message_can_complete(rig):
    snapshots = Snapshots(rig, automatic=False)
    bind(rig, snapshots)
    event, state = ask(rig, mid='om_first')
    assert state['state'] == 'source_wait'
    assert rig.store.one('SELECT state FROM r5_intake WHERE item_id=?', (event.item_id,))['state'] == 'source_wait'
    _, greeting = ask(rig, '你好', mid='om_other')
    assert greeting['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    snapshots.complete(snapshot(rig))
    rig.clock[0] += .3
    assert rig.c.intake_once()['retrieval_status'] == 'found'
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 2
    assert snapshots.reads == 1


def test_cache_miss_refreshes_once_and_finds_new_source_record(rig):
    snap = Snapshots(rig, snapshot(rig, rows=False))
    snap.next_value = snapshot(rig, version='v2')
    bind(rig, snap)
    _, first = ask(rig)
    assert first['state'] == 'source_wait'
    rig.clock[0] += .3
    result = rig.c.intake_once()
    assert result['retrieval_status'] == 'found'
    assert rig.c.local_delivery_once() == 1
    assert snap.reads == 1


def test_source_wait_deadline_reports_unavailability_not_absence(rig):
    snap = Snapshots(rig, automatic=False)
    bind(rig, snap)
    event, first = ask(rig)
    assert first['state'] == 'source_wait'
    rig.clock[0] += 13
    assert rig.c.intake_once()['retrieval_status'] == 'unavailable'
    assert rig.c.local_delivery_once() == 1
    assert '无法可靠核对' in rig.sends[0]['payload']['body']
    assert '没查到' not in rig.sends[0]['payload']['body']
    assert rig.store.rows('SELECT * FROM h1_tasks') == []


def test_source_update_revises_same_action_before_one_send(rig):
    snap, _ = bind(rig)
    ask(rig)
    initial = rig.store.rows('SELECT action_id FROM r5_local_replies')[0]['action_id']
    snap.value = snapshot(rig, progress='已标记完成，仍待核验。', version='v2')
    assert rig.c.local_delivery_once() == 1
    assert '已标记完成，仍待核验' in rig.sends[0]['payload']['body']
    assert rig.store.rows('SELECT action_id FROM r5_local_replies') == [{'action_id': initial}]
    assert json.loads(rig.store.one('SELECT result_json FROM r6_reply_sources WHERE action_id=?',
        (initial,))['result_json'])['source_version'] == 'v2'


def test_grant_revoked_between_preparation_and_send_removes_project_text(rig):
    bind(rig)
    ask(rig, group=True)
    rig.directory.project_ids_for = lambda actor: ()
    assert rig.c.local_delivery_once() == 1
    body = rig.sends[0]['payload']['body']
    assert '示例平台' not in body and '材料已到' not in body
    assert '绑定' in body


def test_source_change_at_executor_boundary_does_not_send_old_or_duplicate_reply(rig):
    snap, reader = bind(rig)
    calls = [0]
    def changing(event, actor, projects, **kwargs):
        calls[0] += 1
        if calls[0] == 3:
            snap.value = snapshot(rig, progress='测试已开始，报告仍待回读。', version='v2')
        return reader(event, actor, projects, **kwargs)
    rig.c.project_reader = changing
    ask(rig)
    assert rig.c.local_delivery_once() == 0
    assert rig.sends == []
    assert rig.c.local_delivery_once() == 1
    assert '测试已开始' in rig.sends[0]['payload']['body']
    assert len(rig.sends) == 1


def test_duplicate_input_and_recall_preserve_one_action_or_zero_effects(rig):
    bind(rig)
    event, _ = ask(rig)
    assert rig.c.capture(event)['duplicate']
    assert rig.c.intake_once() is None
    recalled = replace(event, is_deleted=True, version_time=event.version_time + 1)
    rig.c.revoke(recalled)
    assert rig.c.local_delivery_once() == 0
    assert rig.sends == []


def test_crash_after_prepare_reuses_stable_action_even_if_source_changed(rig):
    snap, reader = bind(rig)
    event = message(rig, '示例平台进展如何', projects=(), mid='om_crash')
    rig.c.capture(event)
    item = rig.c._claim_intake()
    actor = rig.directory.resolve(ALICE)
    result = reader(event, actor, ('HD',))
    event = replace(event, project_ids=tuple(result['project_ids']))
    rig.c._store_project_reply(event, actor, result)
    # Process dies before _finish_intake. The existing row and source binding
    # committed atomically; lease recovery derives a new body for the same ID.
    rig.clock[0] += 121
    snap.value = snapshot(rig, progress='新来源：报告已回读。', version='v2')
    recovered = rig.rebuild()
    recovered.project_reader = reader
    rig.c = recovered
    assert recovered.intake_once()['state'] == 'local_ready'
    assert len(rig.store.rows('SELECT * FROM r5_local_replies')) == 1
    assert len(rig.store.rows('SELECT * FROM r6_reply_sources')) == 1
    assert recovered.local_delivery_once() == 1
    assert len(rig.sends) == 1 and '新来源' in rig.sends[0]['payload']['body']


def test_telemetry_failure_after_success_never_turns_applied_into_unknown(rig, monkeypatch):
    bind(rig)
    def broken(*args, **kwargs):
        raise OSError('synthetic diagnostic write failure')
    monkeypatch.setattr(rig.c.trace, '_write', broken)
    event, outcome = ask(rig)
    assert outcome['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 1
    assert rig.c.actions.list()[0]['state'] == 'applied'
    assert rig.c.trace.local_elapsed(event.item_id) is None


def test_exact_reply_reference_carries_focus_only_inside_same_actor_chat(rig):
    bind(rig)
    ask(rig)
    rig.c.local_delivery_once()
    parent = 'om_synthetic_1'
    _, outcome = ask(rig, '下一步呢', mid='om_followup', parent_id=parent)
    assert outcome['retrieval_status'] == 'found'
    assert rig.c.local_delivery_once() == 1
    assert '核对测试计划' in rig.sends[-1]['payload']['body']
    other = message(rig, '下一步呢', mid='om_wrong_chat', projects=(), chat='oc_other', parent_id=parent)
    assert rig.c._reply_focus(other) is None


def test_compound_request_stays_intact_for_existing_agent_path(rig):
    bind(rig)
    rig.c.settings['project_names'] = {'HD': ['示例平台']}
    event, result = ask(rig, '示例平台进展如何，再做一份报告')
    assert result['state'] == 'admitted'
    task = rig.store.rows('SELECT event_json FROM gw_items WHERE item_id=?', (event.item_id,))[0]
    assert json.loads(task['event_json'])['body'] == event.body
    assert rig.store.rows('SELECT * FROM r5_local_replies') == []


def test_live_runtime_intake_and_send_are_not_blocked_by_background(rig):
    gate, entered, delivered = threading.Event(), threading.Event(), threading.Event()
    actual_executor = rig.c.action_executor
    def slow_background():
        entered.set()
        gate.wait(3)
    def sender(frozen):
        result = actual_executor(frozen)
        delivered.set()
        return result
    rig.c.background_tick = slow_background
    rig.c.action_executor = sender
    service = RuntimeService(NS(gateway=rig.h, scheduler=None), dispatch_enabled=False)
    service.assistant = rig.c
    try:
        service.start()
        assert entered.wait(1)
        rig.c.capture(message(rig, '你好', mid='om_while_background'))
        assert delivered.wait(2), 'foreground reply was blocked by the background lane'
        names = {t.name for t in service.threads}
        assert 'am-r4-assistant-intake' in names
        assert 'am-r4-assistant-improvement' in names
    finally:
        gate.set()
        assert service.stop()['stopped']
    assert len(rig.sends) == 1


def test_feedback_capture_is_local_and_analysis_is_separate(rig):
    event, _ = ask(rig, '你刚才回答不准确，而且太长了', mid='om_feedback')
    feedback = rig.store.rows('SELECT payload_json FROM r6_improvement_feedback')
    assert len(feedback) == 1 and event.body in feedback[0]['payload_json']
    assert rig.c.improvement.list_proposals() == []
    proposal = rig.c.improvement_tick()
    assert proposal['state'] == 'candidate_needs_replay'
    assert not proposal['business_facts_changed']


def test_refresh_finishing_after_snapshot_read_retries_new_version(rig):
    from integration.assistant_queries import FastReadPending
    old = snapshot(rig)
    old['rows'][0]['title'], old['rows'][0]['aliases'] = '旧名称', ['旧名称']
    snap = Snapshots(rig, old, automatic=False)
    _, reader = bind(rig, snap)
    event = message(rig, '示例平台项目进展如何', projects=())
    actor = rig.directory.resolve(ALICE)
    with pytest.raises(FastReadPending):
        reader(event, actor, ('HD',))
    ordinary_get = snap.get
    def racing_get(**kwargs):
        value = ordinary_get(**kwargs)
        snap.complete(snapshot(rig, version='v2'))
        snap.get = ordinary_get
        return value
    snap.get = racing_get
    assert reader(event, actor, ('HD',))['status'] == 'found'
    assert snap.reads == 1


def test_source_edit_between_intake_check_and_prepare_cannot_bind_old_answer_to_new_input(rig):
    bind(rig)
    old = message(rig, '示例平台进展如何', mid='om_edited', projects=())
    new = replace(old, body='示例平台下一步呢', version_time=old.version_time + 1)
    actual = rig.c._store_project_reply
    def edit_then_prepare(event, actor, result):
        rig.c.capture(new)
        rig.c._store_project_reply = actual
        return actual(event, actor, result)
    rig.c._store_project_reply = edit_then_prepare
    rig.c.capture(old)
    assert rig.c.intake_once()['state'] == 'superseded'
    assert rig.c.local_delivery_once() == 0
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 1
    assert '下一步／待办' in rig.sends[0]['payload']['body']


def test_identical_source_refresh_keeps_approval_digest_and_sends_no_extra_card(rig):
    snap, _ = bind(rig)
    # A protected person's name legitimately requires the existing group review.
    ask(rig, '示例平台负责人是谁，示例主管乙呢', group=True)
    action = rig.c.actions.list()[0]
    assert action['state'] == 'awaiting_approval'
    version, digest = action['version'], action['digest']
    report_count = len(rig.store.rows('SELECT * FROM ar5_outbox'))
    rig.clock[0] += 65
    snap.value = snapshot(rig, version='v2')
    assert rig.c.local_delivery_once() == 0
    latest = rig.c.actions.get(action['action_id'])
    assert (latest['version'], latest['digest']) == (version, digest)
    assert len(rig.store.rows('SELECT * FROM ar5_outbox')) == report_count


def test_twenty_recalled_local_rows_do_not_starve_later_valid_reply(rig):
    for n in range(20):
        event, _ = ask(rig, '你好', mid='om_recalled_' + str(n))
        rig.c.revoke(replace(event, is_deleted=True, version_time=event.version_time + 1))
    ask(rig, '谢谢', mid='om_valid_after_recalled')
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 1


def test_unknown_deliveries_do_not_starve_new_reply_and_are_not_replayed(rig):
    original = rig.c.action_executor
    calls = []
    rig.c.action_executor = lambda frozen: calls.append(frozen) or {'status': 'unknown'}
    for n in range(20):
        ask(rig, '你好', mid='om_unknown_' + str(n))
    assert rig.c.local_delivery_once() == 0
    assert len(calls) == 20
    rig.c.action_executor = original
    ask(rig, '谢谢', mid='om_valid_after_unknown')
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 1 and len(calls) == 20


def test_cold_identity_only_blocks_its_conversation_not_all_intake_workers(rig):
    gate, entered, delivered = threading.Event(), threading.Event(), threading.Event()
    actual_resolve, actual_executor = rig.directory.resolve, rig.c.action_executor
    def slow_resolve(uid):
        if uid == ALICE:
            entered.set()
            gate.wait(3)
        return actual_resolve(uid)
    def sender(frozen):
        outcome = actual_executor(frozen)
        if frozen['payload']['target']['chat_id'] == 'oc_owner':
            delivered.set()
        return outcome
    rig.directory.resolve, rig.c.action_executor = slow_resolve, sender
    service = RuntimeService(NS(gateway=rig.h, scheduler=None), dispatch_enabled=False)
    service.assistant = rig.c
    try:
        rig.c.capture(message(rig, '你好', mid='om_cold_peer'))
        service.start()
        assert entered.wait(1)
        rig.c.capture(message(rig, '你好', mid='om_owner_other_scope', actor=OWNER, chat='oc_owner'))
        assert delivered.wait(2), 'cold identity serialised unrelated intake'
    finally:
        gate.set()
        assert service.stop()['stopped']


def test_processing_claims_serialize_same_conversation_but_allow_another(rig):
    a = message(rig, '你好', mid='om_a')
    b = message(rig, '谢谢', mid='om_b')
    c = message(rig, '你好', mid='om_c', actor=OWNER, chat='oc_owner')
    for event in (a, b, c):
        rig.c.capture(event)
    first, second = rig.c._claim_intake(), rig.c._claim_intake()
    first_event = json.loads(first['event_json'])
    second_event = json.loads(second['event_json'])
    assert first_event['sender_id'] != second_event['sender_id']
    assert rig.c._claim_intake() is None


def test_timing_edit_does_not_reuse_previous_generation(rig):
    trace = rig.c.trace
    trace.mark('same-item', 'ingress_received', at=1, clock_domain='test', details={'request_version': 'a'})
    trace.mark('same-item', 'delivery_readback_observed', at=3, clock_domain='test', details={'request_version': 'a'})
    assert trace.local_elapsed('same-item') == 2
    trace.mark('same-item', 'ingress_received', at=10, clock_domain='test', details={'request_version': 'b'})
    assert trace.local_elapsed('same-item') is None
    trace.mark('same-item', 'delivery_readback_observed', at=17, clock_domain='test', details={'request_version': 'b'})
    assert trace.local_elapsed('same-item') == 7


def test_natural_followup_uses_delivered_focus_without_forcing_reply_button(rig):
    bind(rig)
    ask(rig)
    assert rig.c.local_delivery_once() == 1
    _, outcome = ask(rig, '那下一步呢', mid='om_natural_followup')
    assert outcome['retrieval_status'] == 'found'
    assert rig.c.local_delivery_once() == 1
    assert '核对测试计划' in rig.sends[-1]['payload']['body']


def test_attached_material_prevents_ledger_only_shortcut(rig):
    _, reader = bind(rig)
    event = message(rig, '示例平台进展如何', attachments=({'name': 'synthetic.txt'},))
    assert reader(event, rig.directory.resolve(ALICE), ('HD',)) is None


def test_automatic_focus_does_not_cross_a_new_topic(rig):
    bind(rig)
    ask(rig)
    assert rig.c.local_delivery_once() == 1
    rig.c.settings['project_names'] = {'HD': ['示例平台']}
    _, topic = ask(rig, '把示例平台项目的进展整理成一份报告', mid='om_new_topic')
    assert topic.get('route') != 'ledger_direct'
    followup = message(rig, '那下一步呢', mid='om_after_topic', projects=())
    rig.c.capture(followup)
    assert rig.c._reply_focus(followup) is None


def test_other_conversation_does_not_clear_delivered_project_focus(rig):
    bind(rig)
    ask(rig)
    assert rig.c.local_delivery_once() == 1
    rig.c.capture(message(rig, '你好', mid='om_other_topic', actor=OWNER, chat='oc_owner'))
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    _, followup = ask(rig, '那下一步呢', mid='om_same_scope_followup')
    assert followup['retrieval_status'] == 'found'


def test_newer_shared_snapshot_satisfies_an_earlier_request_refresh(rig):
    from integration.assistant_queries import FastReadPending
    snap = Snapshots(rig, snapshot(rig))
    _, reader = bind(rig, snap)
    actor = rig.directory.resolve(ALICE)
    first = message(rig, 'PT-2099-006项目进展', mid='om_missing_a', projects=())
    second = message(rig, 'PT-2099-005项目进展', mid='om_missing_b', projects=())
    snap.next_value = snapshot(rig, version='v2')
    with pytest.raises(FastReadPending):
        reader(first, actor, ('HD',))
    snap.next_value = snapshot(rig, version='v3')
    with pytest.raises(FastReadPending):
        reader(second, actor, ('HD',))
    answer = reader(first, actor, ('HD',))
    assert answer['status'] == 'no_match'
    assert answer['source_version'] == 'v3'
    assert snap.reads == 2


def test_expired_completed_refresh_can_refresh_again_before_delivery(rig):
    from integration.assistant_queries import FastReadPending
    snap = Snapshots(rig, snapshot(rig, rows=False))
    snap.next_value = snapshot(rig, version='v2')
    _, reader = bind(rig, snap)
    event = message(rig, '示例平台项目进展', projects=())
    actor = rig.directory.resolve(ALICE)
    with pytest.raises(FastReadPending):
        reader(event, actor, ('HD',))
    assert reader(event, actor, ('HD',))['status'] == 'found'
    rig.clock[0] += 61
    snap.next_value = snapshot(rig, version='v3', progress='测试报告待验收。')
    with pytest.raises(FastReadPending):
        reader(event, actor, ('HD',))
    answer = reader(event, actor, ('HD',))
    assert answer['status'] == 'found' and answer['source_version'] == 'v3'
    assert '测试报告待验收' in answer['body']
    assert snap.reads == 2


def test_new_named_project_never_falls_back_to_delivered_old_focus(rig):
    bind(rig)
    ask(rig)
    assert rig.c.local_delivery_once() == 1
    _, result = ask(rig, '那示例测量器项目进展呢', mid='om_new_project_name')
    if result.get('state') == 'source_wait':
        rig.clock[0] += .3
        result = rig.c.intake_once()
    assert result.get('retrieval_status') != 'found'
    rig.c.local_delivery_once()
    assert '示例平台' not in rig.sends[-1]['payload']['body']
