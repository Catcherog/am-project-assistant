"""Exercise actual dispatch/result channels and bounded task-context disclosure."""
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace as NS

import pytest

from am_gateway.assistant.continuity import validate_work_context
from am_gateway.models import ContractError, Event, dumps
from am_gateway.phase1 import Harness
from am_gateway.phase1.agents import AgentRunner, ArtifactStore
from am_gateway.phase1.execution import ReceiverHTTPServer, ReceiverPump
from am_gateway.phase1.service import RuntimeService
from test_assistant_coordinator import rig, OWNER, ALICE, message, admit, finish_execution


def submit(rig, number, *, task_id=None, origin='gpt_dot', text=None):
    data = {'origin': origin, 'event_id': 'fix1-' + str(number),
            'text': text or 'HD 项目进展怎么样了？第' + str(number) + '次查询。', 'project_id': 'HD'}
    if task_id:
        data['task_id'] = task_id
    result = rig.c.handle('POST', '/v1/assistant/tasks', data, actor=OWNER)
    assert rig.c.intake_once()['state'] == 'admitted'
    return result


def completed_cli(rig, number=1, *, task_id=None, origin='gpt_dot', answer='draft-marker', text=None):
    submitted = submit(rig, number, task_id=task_id, origin=origin, text=text)
    task, result = finish_execution(rig, answer=answer)
    assert rig.c.prepare_reply(task, result) is False
    fetched = rig.c.handle('GET', '/v1/assistant/tasks/' + submitted['task_id'], {}, actor=OWNER)
    assert any(run['result'] and run['result']['reply_text'] == answer for run in fetched['runs'])
    rig.clock[0] += 1
    return submitted, task


def dispatch_current(rig):
    task = rig.h.route_one()
    assert task['route_state'] == 'resolved'
    rig.h.information.project_sources()
    rig.h.register_cutover(task['scope'], evidence_ref='synthetic-unique-consumer')
    dispatched = rig.h.dispatch_one()
    assert dispatched['dispatch_state'] == 'accepted'
    return dispatched


def test_more_than_32_cli_results_release_capacity_without_feishu_or_completion(rig):
    for number in range(36):
        completed_cli(rig, number)
    rig.h.owner_results_available()
    states = rig.store.rows("SELECT DISTINCT delivery_state,verification_state FROM h1_tasks")
    assert states == [{'delivery_state': 'available_to_owner', 'verification_state': 'pending'}]
    assert rig.store.rows('SELECT * FROM h1_effects') == []
    assert all(task['state'] != 'completed' for task in rig.c.book.owner_brief(principal_id=OWNER)['tasks'])
    restarted = Harness(rig.store, rig.h.config, adapters=rig.h.adapters)
    rig.c.harness = rig.h = restarted
    _, event = admit(rig, message(rig, actor=OWNER, chat='oc_owner', mid='om_owner_after_36'))
    assert rig.h.route_one()['route_state'] == 'resolved'
    assert len(rig.store.rows("SELECT * FROM h1_tasks WHERE delivery_state='available_to_owner'")) == 36


@pytest.mark.parametrize('uncertain', ['sending', 'unknown', 'held'])
def test_available_cli_does_not_release_unresolved_real_effect_or_held_scope(rig, uncertain):
    _, first = completed_cli(rig)
    rig.h.owner_results_available()
    rig.h.config = replace(rig.h.config, queue_limit=1)
    with rig.store.transaction() as db:
        if uncertain == 'held':
            db.execute("UPDATE h1_sessions SET task_id=?,state='held' WHERE scope=?", (first['task_id'], first['scope']))
        else:
            db.execute('INSERT INTO h1_effects VALUES(?,?,?,?,?,?,?,?,?)',
                       ('synthetic-effect', first['task_id'], first['generation'], 'frozen', 'oc_owner',
                        uncertain, None, rig.clock[0], None))
    submit(rig, 2)
    assert rig.h.route_one()['route_state'] == 'queue_full'


def test_delivery_limit_is_applied_after_excluding_cli_results(rig):
    for number in range(52):
        completed_cli(rig, number)
    admit(rig, message(rig, mid='om_peer_after_52'))
    task, _ = finish_execution(rig)
    sends = []

    def sender(**request):
        sends.append(request)
        return {'platform_id': 'om_peer_answer', 'platform_time': rig.clock[0]}

    def readback(task_id):
        effect = rig.store.one('SELECT * FROM h1_effects WHERE task_id=?', (task_id,))
        return {'platform_id': effect['platform_id'], 'target': effect['target'],
                'body': effect['body'], 'sender_id': rig.cfg.bot_id, 'platform_time': effect['platform_time']}

    service = RuntimeService(NS(gateway=rig.h, scheduler=None), sender=sender, readback=readback)
    service.assistant = rig.c
    assert service.delivery_once() == 1
    assert len(sends) == 1 and sends[0]['target'] == 'oc_alice'
    assert rig.h.task(task['task_id'])['delivery_state'] == 'delivered'


def test_three_entries_receive_same_task_previous_output_and_steps(rig):
    first, _ = completed_cli(rig, answer='FIRST-PRIVATE-MARKER')
    rig.c.book.add_step(first['task_id'], 'step-a', 'CHECK-REPORT-MARKER', principal_id=OWNER,
                        source_ref='owner-plan:approved-1')
    submit(rig, 2, task_id=first['task_id'], origin='grok_bot', text='HD 项目进展怎么样了？请沿用刚才的草稿。')
    second = dispatch_current(rig)
    observed = []

    def runner(envelope, context):
        observed.append(envelope)
        assert 'FIRST-PRIVATE-MARKER' in dumps(envelope['context'])
        assert 'CHECK-REPORT-MARKER' in dumps(envelope['context'])
        return {'response_kind': 'sufficient', 'reply_text': 'SECOND-PRIVATE-MARKER',
                'remaining_requirements': [], 'artifacts': []}

    ReceiverPump(rig.receiver, runner, harness=rig.h).step()
    rig.c.prepare_reply(rig.h.task(second['task_id']), json.loads(rig.h.task(second['task_id'])['result_json']))
    rig.clock[0] += 1
    event = message(rig, '继续任务 ' + first['task_id'] + '，查询进展并保留前稿。',
                    actor=OWNER, chat='oc_owner', mid='om_owner_continuation', projects=())
    assert admit(rig, event)[0] == first['task_id']
    third = dispatch_current(rig)
    claim = rig.receiver.claim(worker_id='synthetic-native')
    snapshot = claim['envelope']['context']['work_task']
    assert snapshot['task_id'] == first['task_id']
    assert 'FIRST-PRIVATE-MARKER' in dumps(snapshot) and 'SECOND-PRIVATE-MARKER' in dumps(snapshot)
    assert {entry['availability'] for entry in snapshot['entries'] if entry['kind'] == 'result'} == {'available_to_owner'}
    assert all(entry.get('business_completion_verified') is not True for entry in snapshot['entries'])
    assert third['delivery_state'] == 'not_delivered'


def test_group_continuation_does_not_include_private_additions_or_unsent_drafts(rig):
    group = message(rig, 'HD 公开项目进展怎么样了？PUBLIC-REQUEST', actor=OWNER,
                    chat='oc_hd', mid='om_group_start', channel='group', mentions=(rig.cfg.bot_id,))
    work_id, _ = admit(rig, group)
    finish_execution(rig, answer='UNSENT-GROUP-DRAFT')
    rig.clock[0] += 1
    completed_cli(rig, 2, task_id=work_id, answer='PRIVATE-OUTPUT', text='HD 项目进展怎么样了？PRIVATE-INPUT')
    group_next = message(rig, '继续任务 ' + work_id + '，查询进展。', actor=OWNER,
                         chat='oc_hd', mid='om_group_next', channel='group', mentions=(rig.cfg.bot_id,))
    admit(rig, group_next)
    task = dispatch_current(rig)
    rendered = dumps(json.loads(task['context_json']).get('work_task', {}))
    assert 'PUBLIC-REQUEST' in rendered
    assert 'PRIVATE-INPUT' not in rendered and 'PRIVATE-OUTPUT' not in rendered
    assert 'UNSENT-GROUP-DRAFT' not in rendered


@pytest.mark.parametrize('change', ['recall', 'edit', 'policy_revoke'])
def test_source_change_after_snapshot_blocks_worker_disclosure(rig, change):
    first, previous = completed_cli(rig, answer='OLD-OUTPUT-MARKER')
    submit(rig, 2, task_id=first['task_id'])
    current = dispatch_current(rig)
    assert 'OLD-OUTPUT-MARKER' in current['context_json']
    event = Event.from_dict(json.loads(previous['request_json']))
    if change == 'recall':
        rig.c.revoke(replace(event, is_deleted=True, version_time=rig.clock[0] + 1))
    elif change == 'edit':
        rig.c.capture(replace(event, body='修改后的请求', version_time=rig.clock[0] + 1), origin='gpt_dot', work_task_id=first['task_id'])
    else:
        from am_gateway.phase1 import Policy
        rig.h.update_policy(previous['task_id'], replace(Policy.parse(previous['policy_json']), revoked=True, version=2))
    with rig.store.transaction() as db:
        with pytest.raises(ContractError, match='continuation'):
            rig.h._authorize(db, current['task_id'], current['generation'])
    observed = []
    ReceiverPump(rig.receiver, lambda envelope, context: observed.append(envelope), harness=rig.h).step()
    assert observed == []


def test_small_budget_tracks_step_source_even_when_request_text_is_omitted(rig):
    rig.h.config = replace(rig.h.config, context_bytes=2048)
    first, previous = completed_cli(rig, answer='长结果' * 3000,
                                    text='HD 项目进展怎么样了？' + '长请求' * 3000)
    event = Event.from_dict(json.loads(previous['request_json']))
    rig.c.book.add_step(first['task_id'], 'bounded-step', '短步骤', principal_id=OWNER,
                        source_ref=rig.c.source_ref(event))
    submit(rig, 2, task_id=first['task_id'])
    current = dispatch_current(rig)
    context = json.loads(current['context_json'])
    assert len(dumps(context).encode()) <= 2048
    assert context['work_task']['truncated'] is True
    assert any(entry['kind'] == 'step' for entry in context['work_task']['entries'])
    assert context['work_task']['bindings']['sources']
    rig.c.revoke(replace(event, is_deleted=True, version_time=rig.clock[0] + 1))
    with rig.store.transaction() as db:
        with pytest.raises(ContractError, match='continuation'):
            rig.h._authorize(db, current['task_id'], current['generation'])


def test_native_cli_and_internal_agent_model_request_keep_work_context(rig, tmp_path):
    first, _ = completed_cli(rig, answer='NATIVE-CONTINUITY-MARKER')
    submit(rig, 2, task_id=first['task_id'])
    dispatch_current(rig)
    server = ReceiverHTTPServer(rig.receiver, harness=rig.h, admin_token='test-admin', worker_token='test-worker')
    server.start()
    config = tmp_path / 'worker.json'
    config.write_text(dumps({'receiver_url': server.base_url, 'worker_token': 'test-worker', 'worker_id': 'test-native'}))
    config.chmod(0o600)
    tool = Path(__file__).resolve().parents[2] / 'tools/native_worker_bridge.py'
    try:
        claim = subprocess.run([sys.executable, str(tool), '--config', str(config), 'claim'],
                               capture_output=True, text=True, timeout=8)
        assert claim.returncode == 0, claim.stdout
        data = json.loads(claim.stdout)
        assert 'NATIVE-CONTINUITY-MARKER' in dumps(data)
        assert 'lease_token' not in claim.stdout and 'test-worker' not in claim.stdout
        task_input = data['task']['envelope']
        runner = AgentRunner('document.process', ArtifactStore(tmp_path / 'artifacts'))
        request = runner._model_request(task_input, {})
        assert 'NATIVE-CONTINUITY-MARKER' in dumps(request['context'])
        response = {'status': 'succeeded', 'result': {'response_kind': 'sufficient', 'reply_text': 'done',
                    'remaining_requirements': [], 'artifacts': []}, 'stopped_evidence': 'synthetic native returned'}
        completed = subprocess.run([sys.executable, str(tool), '--config', str(config), 'result',
                                    '--claim-handle', data['claim_handle']], input=dumps(response),
                                   capture_output=True, text=True, timeout=8)
        assert completed.returncode == 0, completed.stdout
    finally:
        server.stop()
