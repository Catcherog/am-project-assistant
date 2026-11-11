"""Real Harness/receiver/runner paths, with synthetic model and host I/O only.

These assert engineering boundaries, not real-model answer quality, Feishu
latency, phone visibility, or human style acceptance.
"""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace as NS

import pytest

from am_gateway.adapters.base import AdapterError
from am_gateway.assistant.interactive import INTERACTIVE_INTENTS, InteractiveRunner
from am_gateway.assistant.coordinator import AssistantCoordinator
from am_gateway.assistant.policy import Actor, AssistantPolicy, PolicyConfig
from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store
from integration.assistant_interactive import (
    BusyFallbackAdapter, HostInteractiveAdapter, StudioInteractiveSources, bind_interactive_runner,
)
from integration.studio_phase1_factory import HostAuthority
from integration.assistant_queries import StudioProjectQueries


class Model:
    synthetic = True

    def __init__(self, transform=None):
        self.requests, self.transform = [], transform

    def run(self, request):
        self.requests.append(deepcopy(request))
        refs = [item['info_id'] for item in request['context']['evidence_items']
                if item.get('nature') == 'current_ledger_snapshot']
        value = {'schema': 'am.gateway.result.v2', 'job_id': request['job_id'],
                 'attempt_id': request['attempt_id'], 'status': 'completed',
                 'response_kind': 'sufficient', 'reply_text': '资料显示测试尚未启动，预计周五开始；这仍是计划。',
                 'remaining_requirements': [], 'evidence_refs': refs[:1],
                 'request_coverage': [{'request': request['request']['raw_text'], 'state': 'answered'}]}
        return self.transform(value, request) if self.transform else value


class Snapshots:
    def __init__(self):
        self.clock, self.refreshes, self.gets = time.time, 0, 0
        self.value = {'version': 'synthetic-v1', 'source_revision': 'synthetic-v1',
            'source': 'synthetic:ledger', 'observed_at': time.time(), 'complete': True,
            'stale': False, 'refresh_error': None,
            'rows': [
                {'key': 'row-alpha', 'project_ref': 'row-alpha', 'business_id': 'QI-2099-007',
                 'identity_status': 'verified', 'title': '样例甲', 'aliases': ['样例甲'],
                 '项目进展': '测试尚未启动，工装未到。', '关键日期': '预计周五开始测试。',
                 '待办': '确认工装交期。', '人员与职责': '合成甲'},
                {'key': 'row-beta', 'project_ref': 'row-beta', 'business_id': 'QI-2099-008',
                 'identity_status': 'verified', 'title': '样例乙', 'aliases': ['样例乙'],
                 '项目进展': 'B_ONLY_PRIVATE_测试报告待回读。', '关键日期': '日期待确认。',
                 '待办': '回读测试报告。', '人员与职责': '合成乙'}]}

    def get(self, *, wait):
        assert wait is False
        self.gets += 1
        if self.value is None:
            raise RuntimeError('SOURCE_LOADING')
        return deepcopy(self.value)

    def refresh(self):
        self.refreshes += 1
        return NS(done=lambda: False)  # code must never wait on this future


class Directory:
    def __init__(self):
        self.grants = {'alice': ('A', 'B'), 'owner': ('A', 'B')}

    def resolve(self, principal):
        if principal not in self.grants:
            raise ContractError('identity_revoked')
        return Actor(principal, principal, ('DEMO',), True, is_owner=principal == 'owner')

    def project_ids_for(self, actor):
        return self.grants[actor.user_id]

    def dm_actor(self, chat):
        return {'alice-dm': 'alice', 'owner-dm': 'owner'}.get(chat)

    def dm_chats(self):
        return ('alice-dm', 'owner-dm')

    def register_dm(self, event):
        assert self.dm_actor(event.chat_id) == event.sender_id
        return self.resolve(event.sender_id)


@pytest.fixture
def rig(tmp_path):
    store = Store(tmp_path / 'interactive.sqlite')
    receiver = DurableReceiver(store, namespace='interactive', intents=INTERACTIVE_INTENTS)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in INTERACTIVE_INTENTS:
        caps[intent] = adapter.capability(intent)
    cfg = HarnessConfig('tenant', 'bot', 'owner', 'owner-dm', capabilities=caps)
    harness = Harness(store, cfg, adapters={'interactive': adapter})
    snapshots, directory = Snapshots(), Directory()
    bridge = NS(am_self=NS(snapshots=snapshots,
        ledger=NS(port=NS(disclosure={'group-a', 'group-b'})),
        owner_origin=lambda: {'chat': 'owner-dm'}))
    authority = HostAuthority(bridge, cfg, NS(authorize=lambda value: None),
        {'group_chat_ids': ['group-a', 'group-b'], 'personal_chat_ids': ['personal-chat'],
         'personal_read_enabled': True, 'personal_assistance_enabled': True},
        NS(project_chats={'A': ['group-a'], 'B': ['group-b']}, live_cutover_at=time.time() - 1000),
        directory=directory)
    authority.harness = harness
    source = StudioInteractiveSources(bridge, authority, store,
        {'ledger_project_refs': {'A': 'row-alpha', 'B': 'row-beta'}})
    model = Model()
    runner = InteractiveRunner(model, source_loader=source)
    pump = ReceiverPump(receiver, runner, harness=harness)
    state = NS(store=store, receiver=receiver, adapter=adapter, h=harness, cfg=cfg,
               snapshots=snapshots, directory=directory, bridge=bridge, authority=authority,
               source=source, model=model, runner=runner, pump=pump, count=0)

    def submit(text='样例甲项目当前进展如何？', *, channel='bot_dm', chat='alice-dm',
               actor='alice', projects=()):
        state.count += 1
        event = Event('tenant', 'message-' + str(state.count), chat, actor, text, time.time(),
            channel=channel, project_ids=projects, mentions=('bot',) if channel == 'group' else ())
        policy = authority(event)
        assert policy is not None
        harness.ingest(event, policy)
        task = harness.route_one()
        assert task['route_state'] == 'resolved', task
        harness.register_cutover(task['scope'], evidence_ref='synthetic same receiver, no network')
        task = harness.dispatch_one()
        assert task['dispatch_state'] == 'accepted', task
        state.task, state.event = task, event
        return task

    state.submit = submit
    return state


def result(rig):
    rig.pump.step()
    return json.loads(rig.h.task(rig.task['task_id'])['result_json'])


def coordinator(rig, *, mode='semantic'):
    reader = StudioProjectQueries(rig.bridge, rig.authority, {'ledger_project_refs': rig.source.bindings})
    value = AssistantCoordinator(rig.h, directory=rig.directory, authority=rig.authority,
        policy=AssistantPolicy(PolicyConfig(owner_user_ids={'owner'}, demo_department_ids={'DEMO'})),
        project_reader=reader, settings={'project_answer_mode': mode})
    value.interactive_runner = rig.runner
    return value


@pytest.mark.parametrize('has_ingress_anchor', [True, False])
def test_model_delivery_records_only_measured_local_latency_for_rsi(rig, has_ingress_anchor):
    from am_gateway.models import key
    rig.submit();answer=result(rig);c=coordinator(rig)
    c.actions=NS(get=lambda _: {'version':1,'digest':'test'},record_effect=lambda *a,**k:None)
    c.trace.clock=lambda:31.0
    if has_ingress_anchor:
        c.trace.mark(rig.event.item_id,'ingress_received',at=10.0,
            details={'request_version':key(rig.event.version_time,rig.event.content_hash)})
    c.record_delivery(rig.h.task(rig.task['task_id']),answer,
        {'platform_id':'synthetic-delivery','platform_time':3000000.0})
    observation=json.loads(rig.store.one('SELECT payload_json FROM r6_improvement_observations')['payload_json'])
    assert observation['latency_seconds']==(21.0 if has_ingress_anchor else None)
    proposal=c.improvement.analyze_once()
    if has_ingress_anchor:
        issue=next(i for i in proposal['issues'] if i['code']=='latency_review')
        assert issue['observed_seconds']==21.0 and issue['end_to_end_proven'] is False


def test_single_semantic_round_through_real_harness_receiver(rig):
    rig.submit('不是样例甲，是样例乙项目目前进展如何？')
    value = result(rig)
    assert len(rig.model.requests) == 1
    request = rig.model.requests[0]
    assert request['task_kind'] == 'project.query'
    assert {item['project'] for item in request['context']['evidence_items']} == {'A', 'B'}
    assert request['request']['structured_inputs']['project_ids'] == []
    assert request['context']['execution_capabilities'] == []
    assert value['model_provenance']['path'] == 'interactive_single_model'
    assert value['model_provenance']['synthetic'] is True
    assert value['model_provenance']['model_run_calls'] == 1
    assert value['reply_text'] == '资料显示测试尚未启动，预计周五开始；这仍是计划。'
    assert value['evidence_refs'][0].startswith('ledger_')
    assert rig.pump.step() is None
    assert len(rig.model.requests) == 1


@pytest.mark.parametrize('channel,chat,actor,intent,text', [
    ('bot_dm', 'alice-dm', 'alice', 'conversation.reply', '你好'),
    ('personal_dm', 'personal-chat', 'peer', 'work_message.assist', '解释一下当前进展'),
])
def test_original_three_intents_use_one_worker(rig, channel, chat, actor, intent, text):
    rig.submit(text, channel=channel, chat=chat, actor=actor)
    value = result(rig)
    assert rig.model.requests[0]['task_kind'] == intent
    assert value['response_kind'] == 'sufficient'
    assert len(rig.model.requests) == 1


def test_group_cannot_expand_by_project_names_or_input_ids(rig):
    rig.submit('样例甲和样例乙项目进展如何？', channel='group', chat='group-a', projects=('A', 'B'))
    value = result(rig)
    request = rig.model.requests[0]
    serialized = json.dumps(request['context'], ensure_ascii=False)
    assert 'B_ONLY_PRIVATE' not in serialized and 'row-beta' not in serialized
    assert {item['project'] for item in request['context']['evidence_items']} == {'A'}
    assert {row['project_id'] for row in request['context']['ledger_catalog']} == {'A'}
    assert value['interactive_source_binding']['project_ids'] == ['A']


def test_current_dm_directory_grant_is_frozen_without_extracted_project(rig):
    task = rig.submit()
    assert json.loads(task['policy_json'])['projects'] == ['A', 'B']
    rig.directory.grants['alice'] = ()
    value = result(rig)
    assert value['response_kind'] == 'data_gap'
    assert rig.model.requests == []
    assert '不存在' not in value['reply_text']


def test_private_information_acl_is_enforced_before_group_model(rig):
    source = Event('tenant', 'private-evidence', 'alice-dm', 'alice', 'PRIVATE_SECRET_当前计划',
                   time.time(), channel='bot_dm', project_ids=('A',))
    rig.store.ingest(source)
    row = rig.store.one('SELECT version FROM gw_items WHERE item_id=?', (source.item_id,))
    info = rig.h.information.append(project='A', nature='formal_record', body=source.body,
        sources=[{'item_id': source.item_id, 'version': row['version']}], readers=['alice'],
        audiences=['alice-dm'], operation_id='private-synthetic-info')
    rig.submit(channel='group', chat='group-a')
    value = result(rig)
    assert info['info_id'] not in value['evidence_refs']
    assert 'PRIVATE_SECRET' not in json.dumps(rig.model.requests[0], ensure_ascii=False)


@pytest.mark.parametrize('change', [
    {'observed_at': 1}, {'observed_at': time.time() + 1000}, {'complete': False},
    {'stale': True}, {'source_revision': None}, {'refresh_error': 'synthetic_failure'},
])
def test_unfresh_or_unbound_current_source_never_answers_from_history(rig, change):
    rig.snapshots.value.update(change)
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'data_gap'
    assert rig.model.requests == []
    assert rig.snapshots.refreshes == 1
    assert value['model_provenance']['inference_state'] == 'not_called'


def test_loading_source_triggers_async_refresh_without_wait_or_second_model(rig):
    rig.snapshots.value = None
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'data_gap'
    assert rig.snapshots.refreshes == 1 and rig.model.requests == []


@pytest.mark.parametrize('mutate,code', [
    (lambda v: {**v, 'evidence_refs': ['private-other-task']}, 'evidence_outside_current_task'),
    (lambda v: {**v, 'evidence_refs': []}, 'current_project_evidence_not_cited'),
    (lambda v: {**v, 'tool_calls': [{'name': 'send'}]}, 'forbidden_result_fields'),
    (lambda v: {**v, 'reply_text': '我已通知张三，项目也更新了。'}, 'unverified_execution_claim'),
    (lambda v: {**v, 'request_coverage': []}, 'invalid_request_coverage'),
    (lambda v: {**v, 'job_id': 'another-task'}, 'result_identity_mismatch'),
])
def test_returned_invalid_output_is_known_failure_without_repair_model(rig, mutate, code):
    rig.model.transform = lambda value, request: mutate(value)
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'failed'
    assert value['remaining_requirements'] == ['interactive_output:' + code]
    assert len(rig.model.requests) == 1
    assert rig.receiver.query(rig.task['business_id'])['status'] == 'failed'


def test_mixed_send_requirement_is_partial_and_never_claims_executed(rig):
    rig.submit('查一下样例甲项目进展，顺便发给张三。')
    value = result(rig)
    assert value['response_kind'] == 'partial'
    assert value['remaining_requirements']
    assert '尚未执行' in value['reply_text']
    assert len(rig.model.requests) == 1
    assert rig.store.rows('SELECT * FROM h1_effects') == []


def test_model_reported_unanswered_secondary_part_forces_partial(rig):
    def transform(value, request):
        value['request_coverage'].append({'request': '明天的天气尚未核对', 'state': 'needs_source'})
        return value
    rig.model.transform = transform
    rig.submit('查一下样例甲项目进展，另外明天天气如何？')
    value = result(rig)
    assert value['response_kind'] == 'partial'
    assert '明天的天气尚未核对' in value['remaining_requirements']


@pytest.mark.parametrize('text', ['样例甲报告已经发给张三了吗？', '查一下样例甲进展，不要发给张三。'])
def test_status_question_or_negated_delivery_does_not_invent_unfinished_send(rig, text):
    rig.submit(text)
    value = result(rig)
    assert value['response_kind'] == 'sufficient'
    assert value['remaining_requirements'] == []


def test_returned_model_over_deadline_is_known_failure(rig, monkeypatch):
    def late(value, request):
        monkeypatch.setattr('am_gateway.assistant.interactive.time.time', lambda: request['deadline_at'] + 1)
        return value
    rig.model.transform = late
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'failed'
    assert value['remaining_requirements'] == ['interactive_model_deadline']
    assert value['model_provenance']['inference_state'] == 'returned'
    assert len(rig.model.requests) == 1


@pytest.mark.parametrize('change', ['revision', 'grant'])
def test_current_source_or_grant_changes_during_model_invalidates_reply(rig, change):
    def transform(value, request):
        if change == 'revision':
            rig.snapshots.value['source_revision'] = 'v2'
        else:
            rig.directory.grants['alice'] = ()
        return value
    rig.model.transform = transform
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'data_gap'
    assert value['remaining_requirements'] == ['interactive_source_changed']
    assert '测试尚未启动' not in value['reply_text']
    assert len(rig.model.requests) == 1


def test_cancel_during_model_retains_cancelled_without_publication(rig):
    def cancel(value, request):
        rig.receiver.cancel(rig.task['business_id'])
        return value
    rig.model.transform = cancel
    rig.submit()
    outcome = rig.pump.step()
    assert outcome['status'] == 'cancelled'
    assert rig.receiver.query(rig.task['business_id'])['status'] == 'cancelled'
    assert rig.store.rows('SELECT * FROM h1_effects') == []
    assert len(rig.model.requests) == 1


def test_unknown_model_never_fabricates_end_or_replays_original(rig):
    def unknown(value, request):
        raise AdapterError('host_qwen_stop_unknown', status='stop_unknown', unknown=True)
    rig.model.transform = unknown
    rig.submit()
    assert rig.pump.step()['status'] == 'unknown'
    status = rig.receiver.query(rig.task['business_id'])
    assert 'ended' not in {event['kind'] for event in status['events']}
    assert status['result'] is None
    assert rig.pump.step() is None and len(rig.model.requests) == 1


def test_busy_keeps_known_failure_without_serial_fallback(rig):
    def busy(value, request):
        raise AdapterError('MODEL_BUSY')
    rig.model.transform = busy
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'failed'
    assert value['model_provenance']['inference_state'] == 'blocked_before_submission'
    assert value['remaining_requirements'] == ['interactive_model:MODEL_BUSY']
    assert len(rig.model.requests) == 1


def test_promotion_expression_settings_freeze_once_and_do_not_truncate_facts(rig):
    active = {'config': {'reply.layout': 'compact', 'reply.max_chars': 240, 'reply.model_id': 'forbidden-swap'}}
    reads = []
    def settings():
        reads.append(1)
        return active
    def transform(value, request):
        active['config']['reply.max_chars'] = 1200
        value['reply_text'] = '资格与事实说明。' * 90 + '仍待核验，不能算已完成。'
        return value
    rig.runner.reply_settings_provider = settings
    rig.model.transform = transform
    rig.submit()
    value = result(rig)
    assert reads == [1]
    assert value['reply_text'].endswith('仍待核验，不能算已完成。') and len(value['reply_text']) > 240
    assert value['model_provenance']['reply_settings'] == {'reply.layout': 'compact', 'reply.max_chars': 240}
    assert value['model_provenance']['model_id'] == 'host_qwen'


@pytest.mark.parametrize('when', ['before_prepare', 'last_sender_boundary'])
def test_delivery_rechecks_current_source_and_keeps_old_text_unsent(rig, when):
    rig.submit()
    value = result(rig)
    sent = []
    service = RuntimeService(NS(gateway=rig.h, scheduler=None), sender=lambda **kw: sent.append(kw))
    service.interactive_runner = rig.runner
    if when == 'before_prepare':
        rig.snapshots.value['source_revision'] = 'v2'
    else:
        checked = rig.runner.validate_delivery
        def after_first_check(task, result):
            valid = checked(task, result)
            rig.snapshots.value['source_revision'] = 'v2'
            return valid
        rig.runner.validate_delivery = after_first_check
    assert service.delivery_once() == 0
    assert sent == []
    task = rig.h.task(rig.task['task_id'])
    assert task['delivery_state'] == 'blocked'
    assert task['last_error'] == 'interactive_source_changed'
    assert json.loads(task['result_json'])['reply_text'] == value['reply_text']
    assert service.delivery_once() == 0 and len(rig.model.requests) == 1
    effects = rig.store.rows('SELECT state FROM h1_effects')
    assert effects == ([{'state': 'denied'}] if when == 'last_sender_boundary' else [])
    assert rig.store.one('SELECT state FROM h1_sessions WHERE scope=?', (task['scope'],))['state'] == 'free'


def test_host_adapter_uses_existing_coordinator_once_and_validates_exact_identity(rig):
    class Provider:
        def __init__(self):
            self.calls = []
        def complete(self, prompt, **options):
            self.calls.append(options)
            request = json.loads(prompt.split('Untrusted task data:\n', 1)[1])
            assert prompt.count('你是当前对话的只读工作助手。') == 1
            assert 'instructions' not in request and 'response_guidance' not in request
            return NS(text=json.dumps(Model().run(request), ensure_ascii=False))
    provider = Provider()
    rig.runner.model = HostInteractiveAdapter(provider)
    rig.submit()
    value = result(rig)
    assert value['response_kind'] == 'sufficient'
    assert len(provider.calls) == 1
    assert 0 < provider.calls[0]['timeout'] <= 15
    assert provider.calls[0]['native_chat'] is True


def test_authority_deduplicates_group_reads_only_within_one_check(rig):
    calls = []
    revoked = False
    def authorize(value):
        calls.append(dict(value))
        if revoked:
            raise RuntimeError('current_membership_revoked')
    rig.authority.scope.authorize = authorize
    rig.authority.collection_config.project_chats = {'A': ['group-a'], 'B': ['group-a']}
    event = Event('tenant', 'same-group-two-projects', 'group-a', 'alice',
                  '比较样例甲和样例乙', time.time(), channel='group', mentions=('bot',))
    policy = rig.authority(event)
    assert set(policy.projects) == {'A', 'B'}
    assert calls == [{'actor_id': 'alice', 'chat_id': 'group-a', 'channel': 'group'}]
    revoked = True
    assert rig.authority(event) is None
    assert len(calls) == 2


@pytest.mark.parametrize('invalid', [None, 'unknown_ref', 'forged_identity', 'extra_coverage', 'missing_coverage', 'executed_action'])
def test_compact_native_output_preserves_scope_and_rejects_forged_fields(rig, invalid):
    original = {}
    def complete(prompt, **options):
        data = json.loads(prompt.split('Untrusted task data:\n', 1)[1])
        original.update(data)
        assert 'job_id' not in data and 'attempt_id' not in data
        assert data['request']['raw_text'] == '样例甲项目当前进展如何？'
        assert data['context']['evidence_items'][0]['body']['项目进展'] == '测试尚未启动，工装未到。'
        assert options['json_schema']['additionalProperties'] is False
        value = {'kind': 'sufficient', 'text': '测试尚未启动，工装未到。', 'remaining': [],
                 'refs': ['e0'], 'coverage': [{'q': '当前进展', 's': 'answered'}]}
        if invalid == 'unknown_ref': value['refs'] = ['e99']
        if invalid == 'forged_identity': value['job_id'] = 'another-task'
        if invalid == 'extra_coverage': value['coverage'][0]['executed'] = True
        if invalid == 'missing_coverage': value['coverage'] = []
        if invalid == 'executed_action': value['text'] = '我已通知工程师开始测试。'
        return NS(text=json.dumps(value, ensure_ascii=False))
    rig.runner.model = HostInteractiveAdapter(NS(complete=complete), compact_output=True)
    rig.submit()
    value = result(rig)
    if invalid:
        assert value['response_kind'] == 'failed'
    else:
        assert value['response_kind'] == 'sufficient'
        assert value['evidence_refs'][0].startswith('ledger_')
        assert value['evidence_refs'][0] != 'e0'
        assert value['request_coverage'] == [{'request': '当前进展', 'state': 'answered'}]
    assert value['model_provenance']['model_run_calls'] == 1


@pytest.mark.parametrize('code,unknown,uses_alternate', [
    ('MODEL_BUSY', False, True), ('MODEL_PAUSED', False, False),
    ('host_qwen_stop_unknown', True, False), ('MODEL_BUSY', True, False),
    ('invalid_host_qwen_result', False, False), ('host_qwen_known_failure', False, False),
])
def test_busy_fallback_is_only_before_known_submission_and_reports_actual_model(rig, code, unknown, uses_alternate):
    class Primary:
        def run(self, request):raise AdapterError(code, unknown=unknown)
    alternate=Model()
    rig.runner.model=BusyFallbackAdapter(Primary(),alternate,primary_id='host_qwen',alternate_id='verified-alternate')
    rig.submit();rig.pump.step()
    task=rig.h.task(rig.task['task_id'])
    value=json.loads(task['result_json']) if task['result_json'] else None
    assert len(alternate.requests)==int(uses_alternate)
    if uses_alternate:
        assert value['response_kind']=='sufficient'
        assert value['model_provenance']['model_id']=='verified-alternate'
        assert value['model_provenance']['adapter_attempts']==2
        assert value['model_provenance']['blocked_before_submission']==1
    elif not unknown:
        assert value['response_kind']=='failed'
    else:
        assert value is None
        assert task['execution_state']=='unknown'
        assert not any(event['kind']=='ended' for event in rig.receiver.query(rig.task['business_id'])['events'])


@pytest.mark.parametrize('question', ['用容易理解的话解释一下什么是关键路径。', '说明一下何为任务依赖。'])
def test_general_definition_does_not_require_irrelevant_project_citation(rig,question):
    rig.runner.model=Model(lambda value,request: {**value,'reply_text':'这是一般概念说明。','evidence_refs':[]})
    rig.submit(question)
    assert rig.h.task(rig.task['task_id'])['intent']=='conversation.reply'
    assert result(rig)['response_kind']=='sufficient'


def test_business_progress_explanation_still_requires_current_project_source(rig):
    rig.snapshots.value=None
    rig.submit('解释一下样例甲项目当前进展。')
    assert rig.h.task(rig.task['task_id'])['intent']=='project.query'
    assert result(rig)['response_kind']=='data_gap'
    assert rig.model.requests==[]


@pytest.mark.parametrize('question', [
    '解释一下什么是关键路径，查询样例甲当前进展。',
    '查询样例甲当前进展，解释一下什么是关键路径。',
    '解释一下什么是关键路径并说明样例甲项目当前进展。',
    '说一下样例甲项目当前进展并解释一下什么是关键路径。',
    '解释一下什么是关键路径，同时样例甲的进展如何？',
    '解释一下什么是关键路径，顺便说一下样例甲项目当前进展。',
])
@pytest.mark.parametrize('passive', [False, True])
def test_definition_in_mixed_request_keeps_current_project_source_gate(rig, question, passive):
    rig.snapshots.value = None
    options = {'channel': 'personal_dm', 'chat': 'personal-chat'} if passive else {}
    rig.submit(question, **options)
    assert rig.h.task(rig.task['task_id'])['intent'] == ('work_message.assist' if passive else 'project.query')
    value = result(rig)
    assert value['response_kind'] == 'data_gap'
    assert rig.model.requests == []


@pytest.mark.parametrize('question', [
    '用容易理解的话解释一下什么是关键路径。',
    '说明一下何为任务依赖。',
    '解释一下什么是项目进度。',
    '解释一下什么是风险。',
    '解释一下什么是关键路径，举个例子。',
    '解释一下什么是关键路径并说明一下何为任务依赖。',
])
def test_pure_definition_still_works_without_any_project_source(rig, question):
    rig.snapshots.value = None
    rig.runner.model = Model(lambda value, request: {**value,
        'reply_text': '这是一般概念说明。', 'evidence_refs': []})
    rig.submit(question)
    assert rig.h.task(rig.task['task_id'])['intent'] == 'conversation.reply'
    assert result(rig)['response_kind'] == 'sufficient'
    assert len(rig.runner.model.requests) == 1


def test_busy_fallback_result_still_revalidates_source(rig):
    class Primary:
        def run(self, request):raise AdapterError('MODEL_BUSY',unknown=False)
    def changes_source(value,request):
        rig.snapshots.value['source_revision']='changed-during-alternate'
        return value
    rig.runner.model=BusyFallbackAdapter(Primary(),Model(changes_source),primary_id='local',alternate_id='verified-alternate')
    rig.submit();value=result(rig)
    assert value['response_kind']=='data_gap' and value['remaining_requirements']==['interactive_source_changed']


def test_busy_fallback_requires_actual_host_verified_configuration(rig):
    rig.bridge.poc=NS(gateway=NS(provider=NS(complete=lambda *a,**kw:None)))
    with pytest.raises(ContractError,match='busy_fallback_requires_verified_binding'):
        bind_interactive_runner(rig.bridge,rig.authority,rig.store,
            {'interactive':{'busy_fallback':{'kind':'codex','binary':'/unverified','model':'unverified'}}})


@pytest.mark.parametrize('error,unknown', [('MODEL_BUSY', False), ('STOP_UNKNOWN', True)])
def test_host_gate_failure_observation_contract(rig, monkeypatch, error, unknown):
    class BoundaryError(Exception):
        pass
    monkeypatch.setitem(sys.modules, 'fs_agent.config', NS(BoundaryError=BoundaryError))
    def complete(*args, **kwargs):
        raise BoundaryError(error)
    rig.runner.model = HostInteractiveAdapter(NS(complete=complete))
    rig.submit()
    outcome = rig.pump.step()
    assert outcome['status'] == ('unknown' if unknown else 'failed')
    if unknown:
        assert not any(event['kind'] == 'ended' for event in rig.receiver.query(rig.task['business_id'])['events'])


@pytest.mark.parametrize('busy', [False, True])
def test_actual_model_coordinator_is_reused_in_the_harness_path(rig, tmp_path, monkeypatch, busy):
    native = Path(__file__).resolve().parents[3] / 'FS-AGENT-GROK-POC-20261017-01_R1_Studio' / 'src'
    monkeypatch.syspath_prepend(str(native))
    from fs_agent.model_coordinator import ModelCoordinator
    provider_calls, metadata = [], {}
    def complete(prompt, **options):
        provider_calls.append(options)
        request = json.loads(prompt.split('Untrusted task data:\n', 1)[1])
        return NS(text=json.dumps(Model().run(request), ensure_ascii=False))
    gate = ModelCoordinator(NS(complete=complete),
        NS(meta=lambda name: metadata.get(name), set_meta=lambda name, value: metadata.update({name: value})),
        tmp_path / 'existing-gate')
    rig.runner.model = HostInteractiveAdapter(gate)
    rig.submit()
    if busy:
        gate.lock.acquire()
    try:
        value = result(rig)
    finally:
        if busy:
            gate.lock.release()
    assert len(provider_calls) == (0 if busy else 1)
    assert value['response_kind'] == ('failed' if busy else 'sufficient')
    if not busy:
        assert json.loads(gate.path.read_text())['state'] == 'complete'
    assert rig.pump.step() is None


def test_alternate_model_requires_verified_binding_and_uses_existing_subprocess_adapter(rig, tmp_path):
    shim = tmp_path / 'synthetic_model.py'
    shim.write_text('''import json,sys
request=json.load(sys.stdin)
assert request['worker']=='interactive'
refs=[item['info_id'] for item in request['context']['evidence_items']]
print(json.dumps({'schema':'am.gateway.result.v2','job_id':request['job_id'],
 'attempt_id':request['attempt_id'],'status':'completed','response_kind':'sufficient',
 'reply_text':'实际本地子进程已返回合成答复。','remaining_requirements':[],
 'evidence_refs':refs[:1],'request_coverage':[{'request':'项目进展','state':'answered'}]}))
''')
    config = {'interactive': {'model': {'kind': 'json_command', 'argv': [sys.executable, str(shim)]}},
              'assistant': {'fast_reads': {'ledger_project_refs': rig.source.bindings}}}
    with pytest.raises(ContractError, match='verified_binding'):
        bind_interactive_runner(rig.bridge, rig.authority, rig.store, config)
    config['interactive']['model'].update(verified=True, evidence_ref='synthetic local subprocess test')
    runner = bind_interactive_runner(rig.bridge, rig.authority, rig.store, config)
    rig.pump.runner = runner
    rig.submit()
    value = result(rig)
    assert value['reply_text'] == '实际本地子进程已返回合成答复。'
    assert value['model_provenance']['model_id'] == 'json_command:configured'
    assert value['model_provenance']['model_run_calls'] == 1


def test_full_runtime_binds_capabilities_without_new_consumer(rig, tmp_path, monkeypatch):
    from integration import full_runtime
    from integration.phase1_runtime import Phase1Ports
    ports = Phase1Ports(rig.cfg, str(tmp_path / 'assembled.sqlite'), lambda payload: True,
                        rig.authority, lambda payload: (), lambda payload: False)
    bundle = NS(ports=ports, authority=rig.authority, sender=None, readback=None,
                readers=NS(), legacy_ownership=None)
    def bind(ingress):
        rig.authority.harness = ingress.gateway
        return None
    bundle.bind = bind
    monkeypatch.setattr(full_runtime, 'build_phase1_bundle', lambda *a, **kw: bundle)
    rig.bridge.poc = NS(gateway=NS(provider=NS(complete=lambda *a, **kw: None)))
    config = {'phase1_gateway': {'grok_bot': {'mode': 'disabled'}, 'control_api': {'enabled': False},
        'assistant': {'enabled': False}, 'interactive': {'enabled': True},
        'agents': {name: {'enabled': False} for name in ('document_agent', 'project_agent', 'code_agent')}}}
    assembled = full_runtime.build_ports(rig.bridge, config, NS(), tmp_path)
    ingress, service = assembled.runtime_factory(assembled)
    assert service.interactive_runner is not None
    assert {ingress.gateway.config.capabilities[i].adapter for i in INTERACTIVE_INTENTS} == {'interactive'}
    assert [receiver.namespace for receiver in service.receivers] == ['interactive']
    assert len(service.pumps) == 2
    assert service.started is False and all(pump.thread is None for pump in service.pumps)


@pytest.mark.parametrize('old_namespace,state', [
    ('grok_bot', 'accepted'), ('grok_bot', 'unknown'), ('interactive', 'accepted'),
])
def test_adapter_switch_rejects_unfinished_original_runs_without_altering_state(rig, old_namespace, state):
    from integration.full_runtime import _require_drained_foreground
    receiver = rig.receiver
    if old_namespace == 'grok_bot':
        receiver = DurableReceiver(rig.store, namespace='grok_bot', intents=INTERACTIVE_INTENTS)
        adapter = LocalReceiverAdapter(receiver)
        caps = {**rig.cfg.capabilities}
        for intent in INTERACTIVE_INTENTS:
            caps[intent] = adapter.capability(intent)
        rig.h.config = replace(rig.cfg, capabilities=caps)
        rig.h.adapters = {'grok_bot': adapter}
    task = rig.submit()
    if state == 'unknown':
        claim = receiver.claim(worker_id='synthetic-original-worker')
        receiver.mark_unknown(**{name: claim[name] for name in
            ('operation_id', 'run_id', 'generation', 'lease_token')}, reason='worker_execution_unknown')
    with pytest.raises(ContractError, match='original_runs_to_finish'):
        _require_drained_foreground(rig.store, interactive_enabled=old_namespace == 'grok_bot')
    assert receiver.query(task['business_id'])['status'] == state
    assert rig.model.requests == []


@pytest.mark.parametrize('text,reason', [
    ('请帮我看看样例甲目前的进展、主要卡点、计划和下一步。', 'rule'),
    ('样例甲谁负责？', 'trusted_ledger_subject'),
])
def test_semantic_mode_project_request_reaches_real_model_once_without_classifier(rig, text, reason):
    c = coordinator(rig)
    classifier_calls = []
    def classify(*args):
        classifier_calls.append(args)
        raise AssertionError('semantic project request must not add a classifier model')
    rig.h.classifier = NS(classify=classify)
    event = Event('tenant', 'semantic-original-shape', 'alice-dm', 'alice', text, time.time(), channel='bot_dm')
    c.capture(event)
    admitted = c.intake_once()
    assert admitted.get('route') == 'ledger_semantic', admitted
    assert rig.store.rows('SELECT * FROM r5_local_replies') == []
    routed = rig.h.route_one(defer_classification=True)
    assert routed['route_state'] == 'resolved'
    assert json.loads(routed['route_json'])['reason_code'] == reason
    rig.h.register_cutover(routed['scope'], evidence_ref='synthetic existing consumer')
    rig.task = rig.h.dispatch_one()
    answer = result(rig)
    assert answer['model_provenance']['model_run_calls'] == 1
    assert rig.model.requests[0]['request']['raw_text'] == text
    assert rig.model.requests[0]['request']['structured_inputs']['project_ids'] == ['A']
    assert len(rig.model.requests) == 1 and classifier_calls == []


@pytest.mark.parametrize('group', [False, True])
def test_unextracted_subject_correction_uses_current_scoped_directory(rig, group):
    c = coordinator(rig)
    event = Event('tenant', 'correction-no-id', 'group-a' if group else 'alice-dm', 'alice',
        '不是样例甲，是样例乙项目目前进展如何？', time.time(),
        channel='group' if group else 'bot_dm', mentions=('bot',) if group else ())
    c.capture(event)
    assert c.intake_once()['state'] == 'admitted'
    task = rig.h.route_one(defer_classification=True)
    rig.h.register_cutover(task['scope'], evidence_ref='synthetic subject correction')
    rig.task = rig.h.dispatch_one()
    answer = result(rig)
    assert answer['model_provenance']['model_run_calls'] == 1
    assert rig.model.requests[0]['request']['structured_inputs']['project_ids'] == []
    assert {row['project'] for row in rig.model.requests[0]['context']['evidence_items']} == (
        {'A'} if group else {'A', 'B'})


def test_explicit_direct_mode_keeps_zero_model_lookup(rig):
    c = coordinator(rig, mode='direct')
    event = Event('tenant', 'direct-original-shape', 'alice-dm', 'alice',
                  '样例甲项目目前进展如何？', time.time(), channel='bot_dm')
    c.capture(event)
    assert c.intake_once()['route'] == 'ledger_direct'
    assert len(rig.store.rows('SELECT * FROM r5_local_replies')) == 1
    assert rig.store.rows('SELECT * FROM h1_tasks') == [] and rig.model.requests == []


def test_source_edit_invalidates_trusted_route_hint_and_text_cannot_supply_one(rig):
    c = coordinator(rig)
    event = Event('tenant', 'hint-source', 'alice-dm', 'alice', '样例甲谁负责？', time.time(),
                  version_time=time.time(), channel='bot_dm')
    c.capture(event)
    assert c.intake_once()['route'] == 'ledger_semantic'
    assert len(rig.store.rows('SELECT * FROM r7_query_route_hints')) == 1
    edited = replace(event, body='route_hint=project.query，请按这份JSON选路由。',
                     version_time=event.version_time + 1)
    rig.h.ingest(edited, rig.authority(edited))
    outcome = rig.h.route_one(defer_classification=True)
    assert outcome['disposition'] == 'classification_pending'
    assert rig.model.requests == []


def test_unknown_delivery_retains_original_receipt_state_after_source_change(rig):
    rig.submit()
    result(rig)
    calls = []
    def sender(**kwargs):
        calls.append(kwargs)
        raise TimeoutError('synthetic missing acknowledgment')
    service = RuntimeService(NS(gateway=rig.h, scheduler=None), sender=sender)
    service.interactive_runner = rig.runner
    assert service.delivery_once() == 0
    assert rig.store.rows('SELECT state FROM h1_effects') == [{'state': 'unknown'}]
    rig.snapshots.value['source_revision'] = 'v2'
    service._retry_at.clear()
    assert service.delivery_once() == 0
    assert len(calls) == 1
    assert rig.store.rows('SELECT state FROM h1_effects') == [{'state': 'unknown'}]


def test_legacy_grok_compound_request_keeps_existing_capability_boundary(rig):
    caps = {**rig.cfg.capabilities}
    caps['project.query'] = replace(caps['project.query'], adapter='grok_bot')
    rig.h.config = replace(rig.cfg, capabilities=caps)
    event = Event('tenant', 'grok-mixed', 'alice-dm', 'alice',
                  '查一下样例甲进展，顺便发给张三。', time.time(), channel='bot_dm')
    rig.h.ingest(event, rig.authority(event))
    assert rig.h.route_one()['route_state'] == 'capability_unavailable'
    assert rig.model.requests == []


@pytest.mark.parametrize('text', [
    '你说验收了不对', '太像表格了，自然一点，简短一些', '刚才等太久了',
])
def test_experience_feedback_reaches_journal_verbatim_without_self_approval(rig, text):
    c = coordinator(rig)
    event = Event('tenant', 'feedback-input', 'owner-dm', 'owner', text, time.time(), channel='bot_dm')
    c.capture(event)
    c.intake_once()
    rows = rig.store.rows('SELECT payload_json FROM r6_improvement_feedback')
    assert len(rows) == 1
    payload = json.loads(rows[0]['payload_json'])
    assert payload['text'] == text
    assert payload['evidence_kind'] == 'user_opinion_not_verified_fact_or_approval'
    assert rig.store.rows('SELECT * FROM r6_improvement_proposals') == []
    assert rig.model.requests == []


