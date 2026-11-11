"""Business flows through the real assistant state machines and Harness.

Only host identity, current source authority and transport I/O are synthetic.
No model, network connection or real message is used by these tests.
"""
from datetime import datetime
from dataclasses import replace
import json
from types import SimpleNamespace as NS

import pytest

from am_gateway.assistant.coordinator import AssistantCoordinator
from am_gateway.assistant.policy import Actor, AssistantPolicy, PolicyConfig
from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.store import Store


OWNER, ALICE, LEADER = 'ou_owner', 'ou_alice', 'ou_leader'
DEMO = 'od_demo'


class SyntheticDirectory:
    def __init__(self):
        self.actors = {
            OWNER: Actor(OWNER, '示例负责人', (), True, is_owner=True),
            ALICE: Actor(ALICE, 'DEMO同事', (DEMO,), True),
            LEADER: Actor(LEADER, '示例主管乙', (DEMO,), True, is_leader=True),
        }
        self.calls, self.registrations = [], []
        self.chats = {'oc_owner': OWNER, 'oc_alice': ALICE, 'oc_leader': LEADER}

    def resolve(self, user_id):
        self.calls.append(user_id)
        return self.actors[user_id]

    def project_ids_for(self, actor):
        return ('HD',)

    def register_dm(self, event):
        known = self.chats.get(event.chat_id)
        if known and known != event.sender_id:
            raise ContractError('synthetic DM member mismatch')
        self.chats[event.chat_id] = event.sender_id
        self.registrations.append((event.chat_id, event.sender_id))

    def dm_actor(self, chat_id):
        return self.chats.get(chat_id)


class SyntheticAuthority:
    def __init__(self, cutover):
        self.collection_config = NS(live_cutover_at=cutover)
        self.group_chats = ('oc_hd',)
        self.calls = []

    def projects_for_chat(self, chat):
        return ('HD',) if chat == 'oc_hd' else ()

    def __call__(self, event):
        self.calls.append(event.item_id)
        return Policy(event.sender_id, 'synthetic-current-source-authority', event.chat_id,
                      (event.chat_id,), event.project_ids, allow_reply=True,
                      allow_supplement=event.sender_id == OWNER)


@pytest.fixture
def rig(tmp_path):
    clock = [datetime.fromisoformat('2026-11-10T10:00:00+08:00').timestamp()]
    store = Store(tmp_path / 'gateway.sqlite', clock=lambda: clock[0])
    receiver = DurableReceiver(store)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent)
    cfg = HarnessConfig('tenant', 'ou_am_bot', OWNER, 'oc_owner', capabilities=caps)
    harness = Harness(store, cfg, adapters={'grok_bot': adapter})
    policy = AssistantPolicy(PolicyConfig(owner_user_ids=frozenset({OWNER}),
        demo_department_ids=frozenset({DEMO}), leader_user_ids=frozenset({LEADER}),
        protected_person_ids=frozenset({LEADER})))
    directory = SyntheticDirectory()
    authority = SyntheticAuthority(clock[0] - 1000)
    sends, reports = [], []

    def executor(execution):
        sends.append(execution)
        message_id = 'om_synthetic_' + str(len(sends))
        return {'status': 'applied', 'evidence_ref': 'feishu:message:' + message_id,
                'readback_evidence_ref': 'feishu:independent-readback:' + message_id}

    def report_sender(report):
        reports.append(report)
        return {'status': 'sent', 'evidence_ref': 'synthetic-owner-report:' + report['report_id']}

    def new_coordinator():
        return AssistantCoordinator(harness, directory=directory, policy=policy, authority=authority,
                                    action_executor=executor, report_sender=report_sender)

    coordinator = new_coordinator()
    return NS(c=coordinator, h=harness, store=store, receiver=receiver, cfg=cfg,
              clock=clock, directory=directory, authority=authority,
              sends=sends, owner_reports=reports, rebuild=new_coordinator)


def message(rig, text='HD 项目进展怎么样了？', *, mid='om_1', actor=ALICE,
            chat='oc_alice', projects=('HD',), **changes):
    values = dict(tenant_id=rig.cfg.tenant_id, native_id=mid, chat_id=chat, sender_id=actor,
                  body=text, original_time=rig.clock[0], version_time=rig.clock[0],
                  channel='bot_dm', project_ids=projects)
    values.update(changes)
    return Event(**values)


def admit(rig, event=None, **capture):
    event = event or message(rig)
    assert rig.c.capture(event, **capture)['captured']
    outcome = rig.c.intake_once()
    assert outcome['state'] == 'admitted', outcome
    return outcome['task_id'], event


def finish_execution(rig, *, answer='原计划不变，当前等待测试报告。'):
    # Real routing, receiver acceptance, worker claim, result and lifecycle.
    rig.h.route_one()
    task = rig.store.rows('SELECT * FROM h1_tasks ORDER BY created,task_id')[-1]
    rig.h.register_cutover(task['scope'], evidence_ref='synthetic-unique-consumer')
    rig.h.dispatch_one()
    result = {'response_kind': 'sufficient', 'reply_text': answer,
              'remaining_requirements': [], 'artifacts': []}
    pump = ReceiverPump(rig.receiver, lambda envelope, context: dict(result), harness=rig.h)
    pump.step()
    rig.receiver.reconcile(rig.h)
    task = rig.h.task(task['task_id'])
    assert task['execution_state'] == 'succeeded'
    return task, json.loads(task['result_json'])


def approve(rig, action):
    return rig.c.handle('POST', '/v1/assistant/actions/' + action['action_id'] + '/decision',
                        {'version': action['version'], 'digest': action['digest'], 'approve': True}, actor=OWNER)


def plan_followup(rig, work_task_id):
    return rig.c.handle('POST', '/v1/assistant/followups', {
        'task_id': work_task_id, 'event_id': 'followup-report-1', 'recipient_id': ALICE,
        'target_chat': 'oc_alice', 'due_at': rig.clock[0], 'text': '请反馈报告进展。',
        'source_ref': 'approved-plan:HD-report',
    }, actor=OWNER)


def test_capture_is_durable_local_only_until_background_intake(rig):
    event = message(rig)
    first = rig.c.capture(event)
    second = rig.c.capture(event)
    assert first['captured'] and not first['duplicate'] and second['duplicate']
    assert rig.directory.calls == [] and rig.authority.calls == []
    assert rig.store.rows('SELECT * FROM h1_tasks') == []
    assert rig.store.rows('SELECT state FROM r5_intake') == [{'state': 'pending'}]


def test_demo_background_intake_creates_traceable_workbook_and_harness_input(rig):
    work_id, event = admit(rig)
    task = rig.c.book.get_task(work_id, principal_id=OWNER, include_events=True)
    assert task['state'] == 'in_progress' and task['requester_id'] == ALICE
    assert task['project_id'] == 'HD' and task['source_ref'].endswith(event.native_id)
    assert rig.directory.registrations == [('oc_alice', ALICE)]
    assert rig.store.one('SELECT item_id FROM gw_items WHERE item_id=?', (event.item_id,))


@pytest.mark.parametrize('origin', ['gpt_dot', 'grok_bot'])
def test_owner_cli_results_are_readable_from_shared_task_without_feishu_send(rig, origin):
    submitted = rig.c.handle('POST', '/v1/assistant/tasks', {
        'origin': origin, 'event_id': 'owner-query-1', 'text': 'HD 项目进展怎么样了？', 'project_id': 'HD',
    }, actor=OWNER)
    assert submitted['state'] == 'accepted'
    work = rig.c.intake_once()
    assert work['state'] == 'admitted'
    task, result = finish_execution(rig)
    assert rig.c.prepare_reply(task, result) is False
    view = rig.c.handle('GET', '/v1/assistant/tasks/' + work['task_id'], {}, actor=OWNER)
    assert view['runs'][0]['result']['reply_text'] == result['reply_text']
    assert any(e['kind'] == 'result_available' for e in view['task']['events'])
    assert rig.sends == [] and rig.owner_reports == []
    assert view['task']['state'] != 'completed'


def test_owner_cli_event_replay_and_cross_entry_continuation_keep_one_task(rig):
    body = {'origin': 'gpt_dot', 'event_id': 'owner-1', 'text': 'HD 项目进展怎么样了？', 'project_id': 'HD'}
    first = rig.c.handle('POST', '/v1/assistant/tasks', body, actor=OWNER)
    assert rig.c.handle('POST', '/v1/assistant/tasks', body, actor=OWNER)['duplicate']
    task = rig.c.intake_once()
    continued = {**body, 'origin': 'grok_bot', 'event_id': 'owner-2', 'task_id': task['task_id'],
                 'text': 'HD 项目现在有什么卡点？'}
    rig.c.handle('POST', '/v1/assistant/tasks', continued, actor=OWNER)
    assert rig.c.intake_once()['task_id'] == task['task_id']
    assert len(rig.c.book.owner_brief(principal_id=OWNER)['tasks']) == 1
    with pytest.raises(ContractError):
        rig.c.handle('POST', '/v1/assistant/tasks', {**body, 'text': '同一个ID换了要求'}, actor=OWNER)
    assert first['item_id']


def test_client_payload_cannot_claim_owner_or_department(rig):
    with pytest.raises(ContractError):
        rig.c.handle('POST', '/v1/assistant/tasks', {'origin': 'gpt_dot', 'event_id': 'forged',
            'text': '帮我查资料', 'actor_id': OWNER, 'department_ids': [DEMO]}, actor=OWNER)
    with pytest.raises(ContractError):
        rig.c.handle('GET', '/v1/assistant/tasks', {}, actor=ALICE)


def test_ordinary_greeting_then_explicit_close_reports_sent_conversation_once(rig):
    rig.c.capture(message(rig, '你好'))
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    rig.clock[0] += 1
    rig.c.capture(message(rig, '先这样', mid='om_close'))
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    reports = rig.c.reports.list_reports(actor_id=OWNER, kind='conversation')
    assert len(reports) == 1
    assert '你好' in reports[0]['body'] and 'AM 已送达原文' in reports[0]['body']
    assert reports[0]['payload']['task_completion_inferred'] is False
    assert rig.c.reports_once() == 1
    assert rig.c.local_delivery_once() == 0 and len(rig.sends) == 2
    assert len(rig.owner_reports) == 1
    assert rig.store.rows('SELECT * FROM h1_tasks') == []


def test_direct_leader_reply_waits_for_exact_owner_approval(rig):
    rig.c.capture(message(rig, '你好', actor=LEADER, chat='oc_leader'))
    assert rig.c.intake_once()['state'] == 'local_ready'
    actions = rig.c.actions.list()
    assert len(actions) == 1 and actions[0]['state'] == 'awaiting_approval'
    assert rig.c.local_delivery_once() == 0 and rig.sends == []
    with pytest.raises(ContractError):
        rig.c.handle('POST', '/v1/assistant/actions/' + actions[0]['action_id'] + '/decision',
                     {'version': actions[0]['version'], 'digest': 'stale', 'approve': True}, actor=OWNER)
    approve(rig, actions[0])
    assert rig.c.local_delivery_once() == 1
    assert rig.sends[0]['payload']['body'] == actions[0]['payload']['body']
    assert rig.c.local_delivery_once() == 0


@pytest.mark.parametrize('protected_name', ['示例主管甲', '示例主管乙'])
def test_protected_person_in_original_request_requires_approval_of_model_reply(rig, protected_name):
    _, event = admit(rig, message(rig, 'HD 项目进展怎么样了？' + protected_name + '想了解。'))
    task, result = finish_execution(rig, answer='当前计划仍为本周，测试报告待补。')
    assert rig.c.prepare_reply(task, result) is False
    action = rig.c.actions.list()[0]
    assert action['state'] == 'awaiting_approval'
    assert action['payload']['body'] == result['reply_text']
    approve(rig, action)
    assert rig.c.prepare_reply(task, result) is True
    assert rig.sends == []  # existing Harness owns this eventual send
    assert event.body.startswith('HD')


def test_trusted_protected_person_mention_requires_approval_without_display_name(rig):
    admit(rig, message(rig, 'HD 项目进展怎么样了？请向 @_user_1 说明。', chat='oc_hd',
                       channel='group', mentions=(rig.cfg.bot_id, LEADER)))
    task, result = finish_execution(rig)
    assert rig.c.prepare_reply(task, result) is False
    assert rig.c.actions.list()[0]['state'] == 'awaiting_approval'


def test_unrelated_peer_request_is_only_recorded_and_referred(rig):
    rig.c.capture(message(rig, '替我处理一个不属于示例负责人工作的个人事项', projects=()))
    result = rig.c.intake_once()
    assert result['state'] == 'referred'
    task = rig.c.book.get_task(result['task_id'], principal_id=OWNER)
    assert task['state'] == 'awaiting_owner' and not task['related']
    assert rig.store.rows('SELECT * FROM h1_tasks') == []
    assert rig.sends == []
    assert any('需要你判断' in r['body'] for r in rig.c.reports.list_reports(actor_id=OWNER))


def test_owner_acceptance_of_referral_resumes_same_task_without_referral_loop(rig):
    rig.c.capture(message(rig, '帮同事核对额外资料', projects=()))
    referred = rig.c.intake_once()
    rig.c.handle('POST', '/v1/assistant/tasks/' + referred['task_id'] + '/decision',
                 {'accept': True, 'source_ref': 'owner:accept-extra-task'}, actor=OWNER)
    resumed = rig.c.intake_once()
    assert resumed['state'] == 'admitted' and resumed['task_id'] == referred['task_id']
    assert len(rig.c.book.owner_brief(principal_id=OWNER)['tasks']) == 1


def test_followup_observation_never_sends_until_owner_approves(rig):
    work_id, _ = admit(rig)
    plan_followup(rig, work_id)
    rig.c.followups_once()
    action = rig.c.actions.list()[0]
    assert action['state'] == 'awaiting_approval'
    assert rig.sends == []
    rig.clock[0] += 86400 * 30
    rig.c.followups_once()
    assert rig.sends == []
    assert len(rig.c.actions.list()) == 1
    approve(rig, rig.c.actions.list()[0])
    rig.c.followups_once()
    assert len(rig.sends) == 1
    assert rig.sends[0]['payload']['body'] == '请反馈报告进展。'
    assert rig.c.book.due_followups(rig.clock[0], principal_id=OWNER) == []
    rig.c.followups_once()
    assert len(rig.sends) == 1


def test_restart_and_reply_before_send_cancel_pending_followup(rig):
    work_id, _ = admit(rig)
    plan_followup(rig, work_id)
    rig.c.followups_once()
    approve(rig, rig.c.actions.list()[0])
    rig.c = rig.rebuild()
    rig.clock[0] += 1
    reply = message(rig, '报告正在补充', mid='om_feedback')
    rig.c.capture(reply, work_task_id=work_id)
    outcome = rig.c.intake_once()
    assert outcome['task_id'] == work_id
    rig.c.followups_once()
    assert rig.sends == []
    assert rig.c.book.due_followups(rig.clock[0], principal_id=OWNER) == []
    assert len(rig.c.book.owner_brief(principal_id=OWNER)['tasks']) == 1


def test_feedback_claim_of_completion_stays_reported_until_verified(rig):
    work_id, _ = admit(rig)
    plan_followup(rig, work_id)
    rig.clock[0] += 1
    rig.c.capture(message(rig, '测试已完成，报告待补。', mid='om_done'), work_task_id=work_id)
    rig.c.intake_once()
    task = rig.c.book.get_task(work_id, principal_id=OWNER, include_events=True)
    assert task['state'] == 'reported_complete'
    assert task['verification_ref'] == ''
    assert any(e['kind'] == 'reported_complete' for e in task['events'])
    assert rig.c.book.due_followups(rig.clock[0], principal_id=OWNER) == []


@pytest.mark.parametrize('text', [
    '测试还没完成，预计明天完成。',
    '测试完成了吗？',
    '如果测试已完成，我会发报告。',
])
def test_negative_planned_or_conditional_feedback_is_not_reported_completion(rig, text):
    work_id, _ = admit(rig)
    rig.clock[0] += 1
    rig.c.capture(message(rig, text, mid='om_status_feedback'), work_task_id=work_id)
    rig.c.intake_once()
    task = rig.c.book.get_task(work_id, principal_id=OWNER, include_events=True)
    assert task['state'] not in {'reported_complete', 'completed'}
    assert task['verification_ref'] == ''
    assert not any(e['kind'] == 'reported_complete' for e in task['events'])


def test_restarted_coordinator_does_not_replay_sent_local_reply(rig):
    event = message(rig, '你好')
    rig.c.capture(event)
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    rig.c = rig.rebuild()
    assert rig.c.capture(event)['duplicate']
    assert rig.c.intake_once() is None
    assert rig.c.local_delivery_once() == 0
    assert len(rig.sends) == 1


def test_private_owner_task_accepts_exact_followup_reply_without_disclosing_task(rig):
    submitted = rig.c.handle('POST', '/v1/assistant/tasks', {
        'origin': 'gpt_dot', 'event_id': 'private-owner-1', 'text': 'HD 私有预算判断，请先整理。', 'project_id': 'HD',
    }, actor=OWNER)
    assert rig.c.intake_once()['state'] == 'admitted'
    work_id = submitted['task_id']
    plan_followup(rig, work_id)
    rig.c.followups_once()
    approve(rig, rig.c.actions.list()[0])
    rig.c.followups_once()
    rig.clock[0] += 1
    reply = message(rig, '测试完成了，报告晚些补充。', mid='om_private_feedback', parent_id='om_synthetic_1')
    rig.c.capture(reply)
    outcome = rig.c.intake_once()
    assert outcome == {'state': 'feedback_recorded', 'task_id': work_id}
    task = rig.c.book.get_task(work_id, principal_id=OWNER, include_events=True)
    assert task['state'] == 'reported_complete'
    assert any(e['source_ref'].endswith('om_private_feedback') for e in task['events'])
    assert rig.store.rows('SELECT state FROM a5_wb_followups') == [{'state': 'resolved'}]
    from am_gateway.assistant.workbook import AccessDenied
    with pytest.raises(AccessDenied):
        rig.c.book.get_task(work_id, principal_id=ALICE)
    assert rig.store.one('SELECT 1 FROM gw_items WHERE item_id=?', (reply.item_id,)) is None
    assert rig.c.local_delivery_once() == 1
    assert '预算' not in rig.sends[-1]['payload']['body']


def test_recall_before_intake_never_starts_model_or_local_send(rig):
    event = message(rig, '你好')
    rig.c.capture(event)
    rig.c.revoke(replace(event, is_deleted=True, observed_via='recall', version_time=event.version_time + 1))
    assert rig.c.intake_once() is None
    assert rig.c.capture(event)['duplicate']  # stale live/poll replay cannot resurrect
    assert rig.c.local_delivery_once() == 0
    assert rig.store.rows('SELECT * FROM h1_tasks') == []


def test_recall_invalidates_pending_exact_approval(rig):
    event = message(rig, '你好', actor=LEADER, chat='oc_leader')
    rig.c.capture(event)
    rig.c.intake_once()
    action = rig.c.actions.list()[0]
    rig.c.revoke(replace(event, is_deleted=True, observed_via='recall', version_time=event.version_time + 1))
    with pytest.raises(ContractError, match='source changed'):
        approve(rig, action)
    rig.c.local_delivery_once()
    assert rig.sends == []


def test_edited_request_stops_previous_model_reply_before_background_intake(rig):
    _, event = admit(rig)
    task, result = finish_execution(rig)
    assert rig.c.prepare_reply(task, result)
    rig.c.capture(replace(event, body='HD 请等我更新范围，暂时不要继续。', version_time=event.version_time + 1))
    assert rig.c.prepare_reply(task, result) is False
    assert rig.sends == []


def test_shared_harness_boundary_blocks_stale_source_before_background_refresh(rig):
    _, event = admit(rig)
    task, result = finish_execution(rig)
    rig.c.capture(replace(event, body='HD 范围改变，暂缓原来的操作。', version_time=event.version_time + 1))
    sent = []
    with pytest.raises(ContractError, match='source changed'):
        rig.h.deliver_recorded_result(task['task_id'], lambda **kw: sent.append(kw))
    assert sent == []


def test_local_reply_uses_original_source_stamp_after_project_resolution(rig):
    event = message(rig, '你好', projects=('NOT_ALLOWED',))
    rig.c.capture(event)
    assert rig.c.intake_once()['state'] == 'local_ready'
    assert rig.c.local_delivery_once() == 1
    assert len(rig.sends) == 1


def test_cli_result_is_projected_by_tick_without_live_sender(rig):
    rig.c.report_sender = rig.c.action_executor = None
    submitted = rig.c.handle('POST', '/v1/assistant/tasks', {
        'origin': 'grok_bot', 'event_id': 'offline-cli', 'text': 'HD 项目进展怎么样了？', 'project_id': 'HD',
    }, actor=OWNER)
    rig.c.intake_once()
    finish_execution(rig)
    rig.c.tick()
    task = rig.c.book.get_task(submitted['task_id'], principal_id=OWNER, include_events=True)
    assert task['state'] == 'waiting_feedback'
    assert len([e for e in task['events'] if e['kind'] == 'result_available']) == 1
    assert rig.sends == []


def test_generated_but_unsent_answer_keeps_task_in_progress(rig):
    work_id, _ = admit(rig)
    finish_execution(rig)
    rig.c.observe_results_once()
    task = rig.c.book.get_task(work_id, principal_id=OWNER)
    assert task['state'] == 'in_progress'
    assert rig.c.reports.list_reports(actor_id=OWNER, kind='conversation') == []


def test_verified_delivery_report_recovers_after_crash_without_resending(rig):
    _, event = admit(rig)
    task, result = finish_execution(rig)
    assert rig.c.prepare_reply(task, result)
    sent = []
    def sender(**kwargs):
        sent.append(kwargs)
        return {'platform_id': 'om_verified_recovery', 'platform_time': rig.clock[0] + 1}
    rig.h.deliver_recorded_result(task['task_id'], sender)
    rig.h.verify_delivery(task['task_id'], {
        'platform_id': 'om_verified_recovery', 'platform_time': rig.clock[0] + 1,
        'target': event.chat_id, 'body': result['reply_text'], 'sender_id': rig.cfg.bot_id,
    })
    # Process died after Harness verification but before assistant projection.
    rig.c = rig.rebuild()
    rig.c.observe_results_once()
    rig.c.observe_results_once()
    rig.c.reports.close_conversation(event.chat_id, closed_at=rig.clock[0] + 5, actor_id=ALICE)
    reports = rig.c.reports.list_reports(actor_id=OWNER, kind='conversation')
    assert len(reports) == 1 and result['reply_text'] in reports[0]['body']
    assert 'AM 已送达原文' in reports[0]['body']
    assert len(sent) == 1


def test_feedback_between_followup_selection_and_effect_prevents_send(rig):
    work_id, _ = admit(rig)
    plan_followup(rig, work_id)
    rig.c.followups_once()
    approve(rig, rig.c.actions.list()[0])
    original_execute = rig.c.actions.execute
    def interleave(*args, **kwargs):
        rig.c.book.record_reply(work_id, 'arrived-before-effect', 'feishu', ALICE,
            '报告正在补充', source_ref='feishu:message:om_race_reply', at=rig.clock[0] + 1)
        return original_execute(*args, **kwargs)
    rig.c.actions.execute = interleave
    rig.c.followups_once()
    assert rig.sends == []
    assert rig.c.actions.list()[0]['state'] == 'not_applied'


def test_recall_after_followup_approval_prevents_external_contact(rig):
    work_id, event = admit(rig)
    plan_followup(rig, work_id)
    rig.c.followups_once()
    action = rig.c.actions.list()[0]
    approve(rig, action)
    recalled = replace(event, is_deleted=True, observed_via='recall', version_time=event.version_time + 1)
    rig.c.revoke(recalled)
    rig.h.ingest(recalled)
    rig.c.followups_once()
    assert rig.sends == []
    with pytest.raises(ContractError, match='source changed'):
        approve(rig, action)


def test_changed_source_requires_new_followup_preview_after_background_refresh(rig):
    work_id, event = admit(rig)
    plan_followup(rig, work_id)
    rig.c.followups_once()
    previous = rig.c.actions.list()[0]
    approve(rig, previous)
    rig.clock[0] += 1
    rig.c.capture(replace(event, body='HD 项目进展怎么样了？范围加上样件。', version_time=rig.clock[0]))
    rig.c.followups_once()
    assert rig.sends == []
    assert rig.c.intake_once()['state'] == 'admitted'
    rig.c.followups_once()
    current = rig.c.actions.get(previous['action_id'])
    assert current['state'] == 'awaiting_approval' and current['digest'] != previous['digest']
    assert rig.sends == []


def test_peer_risk_is_quoted_once_and_kept_separate_from_verified_completion(rig):
    work_id, _ = admit(rig, message(rig, 'HD 测试卡住，有风险需要关注。'))
    rig.c.risks_once()
    rig.clock[0] += 5
    rig.c.risks_once()
    reports = rig.c.reports.list_reports(actor_id=OWNER, kind='risk')
    assert len(reports) == 1 and '尚未独立核验' in reports[0]['body']
    assert rig.c.book.get_task(work_id, principal_id=OWNER)['state'] == 'in_progress'


def test_referral_card_digest_invalidates_when_request_is_edited(rig):
    event = message(rig, '额外的工作请求', projects=())
    rig.c.capture(event)
    outcome = rig.c.intake_once()
    notice = rig.c.reports.list_reports(actor_id=OWNER, kind='referral')[0]
    digest = notice['payload']['metadata']['scope_digest']
    rig.c.capture(replace(event, body='修改后的额外工作请求', version_time=event.version_time + 1))
    with pytest.raises(ContractError, match='source changed'):
        rig.c.handle('POST', '/v1/assistant/tasks/' + outcome['task_id'] + '/decision',
            {'accept': True, 'source_ref': 'owner:old-card', 'scope_digest': digest}, actor=OWNER)


def test_entry_id_cannot_be_reused_to_change_task_scope(rig):
    body = {'origin': 'gpt_dot', 'event_id': 'scope-identity', 'text': '查询资料', 'project_id': 'HD'}
    rig.c.handle('POST', '/v1/assistant/tasks', body, actor=OWNER)
    with pytest.raises(ContractError, match='scope'):
        rig.c.handle('POST', '/v1/assistant/tasks', {**body, 'task_id': 'wt_other'}, actor=OWNER)


def test_owner_feishu_can_explicitly_continue_cli_private_task(rig):
    submitted = rig.c.handle('POST', '/v1/assistant/tasks', {
        'origin': 'gpt_dot', 'event_id': 'cross-feishu', 'text': 'HD 项目进展怎么样了？', 'project_id': 'HD',
    }, actor=OWNER)
    rig.c.intake_once()
    event = message(rig, '继续任务 ' + submitted['task_id'] + '，请补充项目风险。',
                    actor=OWNER, chat='oc_owner', mid='om_owner_continue', projects=())
    rig.c.capture(event)
    outcome = rig.c.intake_once()
    assert outcome['state'] == 'admitted' and outcome['task_id'] == submitted['task_id']
    assert len(rig.c.book.owner_brief(principal_id=OWNER)['tasks']) == 1


def test_owner_book_api_preserves_plans_feedback_and_completion_evidence(rig):
    work_id, _ = admit(rig)
    step = rig.c.handle('POST', '/v1/assistant/tasks/' + work_id + '/steps',
        {'event_id': 'step-1', 'title': '整理报告资料', 'source_ref': 'owner:plan'}, actor=OWNER)
    assert step['state'] == 'pending'
    fact = rig.c.handle('POST', '/v1/assistant/facts', {
        'project_id': 'HD', 'field': '测试情况', 'value': '报告待补', 'event_id': 'fact-1',
        'source_ref': 'feishu:message:om_feedback', 'status': 'reported',
    }, actor=OWNER)
    assert fact['status'] == 'reported'
    from am_gateway.assistant.workbook import WorkbookError
    with pytest.raises(WorkbookError):
        rig.c.handle('POST', '/v1/assistant/tasks/' + work_id + '/state',
            {'state': 'completed', 'source_ref': 'owner:close'}, actor=OWNER)
    result = rig.c.handle('POST', '/v1/assistant/tasks/' + work_id + '/state',
        {'state': 'completed', 'source_ref': 'owner:close', 'verification_ref': 'test:verified-report'}, actor=OWNER)
    assert result['state'] == 'completed'


def test_risk_api_is_idempotent_and_rejects_changed_content(rig):
    data = {'event_id': 'owner-risk-1', 'text': '报告截止时间需关注', 'source_ref': 'owner:plan'}
    first = rig.c.handle('POST', '/v1/assistant/risks', data, actor=OWNER)
    rig.clock[0] += 10
    assert rig.c.handle('POST', '/v1/assistant/risks', data, actor=OWNER) == first
    with pytest.raises(ContractError, match='different content'):
        rig.c.handle('POST', '/v1/assistant/risks', {**data, 'text': '换了内容'}, actor=OWNER)
