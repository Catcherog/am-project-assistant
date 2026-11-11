import copy
from dataclasses import replace
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import business_fingerprint, registry
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store


def envelope(mid='m1', *, tenant='tenant', chat='chat', principal='owner', generation=1, now=1000):
    text = '项目进度如何 ' + mid
    inputs = {'project_ids': ['A']}
    policy = Policy(principal, 'auth', chat, (chat,), ('A',), allow_reply=True)
    return {'schema_version': '1.0', 'identity': {
        'task_id': 'task_' + mid, 'request_revision': 1, 'business_operation_id': 'op_' + mid,
        'idempotency_key': 'op_' + mid, 'dispatch_attempt_id': 'attempt_' + mid,
        'business_fingerprint': business_fingerprint(text=text, intent='project.query',
            principal=principal, reply_target=chat, attachments=[], inputs=inputs)},
        'routing': {'intent': 'project.query', 'adapter': 'grok_bot'},
        'source': {'tenant_id': tenant, 'native_id': mid, 'chat_id': chat, 'thread_id': '', 'sender_id': principal},
        'payload': {'raw_text': text, 'attachment_refs': [], 'structured_inputs': inputs},
        'request_context': {'acting_principal': principal, 'requester_id': principal},
        'policy': json.loads(policy.json()), 'context': {'evidence_items': []},
        'execution': {'scope': 'test', 'generation': generation, 'accept_deadline': now + 60,
                      'completion_deadline': now + 120}, 'output': {'reply_target': chat}}


@pytest.fixture
def receiver(tmp_path):
    clock = [1000.]
    store = Store(tmp_path / 'receiver.sqlite', clock=lambda: clock[0])
    return DurableReceiver(store, worker_lease_seconds=10), clock


def complete(receiver, claim, reply='当前完成测试，下一步等待实际验收。'):
    return receiver.complete(**{k: claim[k] for k in ('operation_id', 'run_id', 'generation', 'lease_token')},
        result={'reply_text': reply, 'response_kind': 'sufficient', 'remaining_requirements': [], 'artifacts': []})


def seed_delivered_turn(receiver, operation_id):
    # Synthetic delivered-history fixture for the isolation tests below. The
    # integration test exercises the real send/readback promotion gate.
    with receiver.store.transaction() as db:
        receiver._publish_memory(db, receiver._row(db, operation_id))


def test_acceptance_survives_restart_and_poll_replay(receiver):
    r, _ = receiver
    first = r.accept(envelope())
    restarted = DurableReceiver(Store(r.store.path, clock=r.store.clock))
    assert restarted.accept(envelope()) == first
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda _: restarted.claim(worker_id='bot'), range(2)))
    assert len([c for c in claims if c is not None]) == 1
    assert restarted.health()['states'] == {'running': 1}
    assert first['payload']['accepted_by'] == 'gateway_receiver'


def test_lost_worker_is_unknown_without_duplicate_execution(receiver):
    r, clock = receiver
    r.accept(envelope())
    claim = r.claim(worker_id='native-worker')
    clock[0] += 11
    assert r.claim(worker_id='recovery-poller') is None
    assert r.query('op_m1')['status'] == 'unknown'
    # The original worker can reconcile its execution after the lease expired.
    assert complete(r, claim)['status'] == 'succeeded'
    assert r.claim(worker_id='recovery-poller') is None


def test_changed_generation_and_business_content_do_not_reexecute(receiver):
    r, _ = receiver
    r.accept(envelope())
    changed = envelope(generation=2)
    with pytest.raises(ContractError, match='different identity'):
        r.accept(changed)
    changed = envelope()
    changed['payload']['raw_text'] = '替换工作内容'
    with pytest.raises(ContractError, match='fingerprint'):
        r.accept(changed)


def test_running_cancel_waits_for_worker_return(receiver):
    r, _ = receiver
    r.accept(envelope())
    claim = r.claim(worker_id='bot')
    assert r.cancel('op_m1')['stopped'] is False
    assert r.claim(worker_id='other') is None
    outcome = complete(r, claim)
    assert outcome['status'] == 'cancelled' and outcome['result'] is None
    assert complete(r, claim)['status'] == 'cancelled'  # duplicate callback


def test_cancel_before_claim_is_stopped_without_model_call(receiver):
    r, _ = receiver
    r.accept(envelope())
    assert r.cancel('op_m1')['stopped'] is True
    assert r.claim(worker_id='bot') is None
    kinds = [x['kind'] for x in r.query('op_m1')['events']]
    assert kinds == ['accepted', 'ended']


@pytest.mark.parametrize('changes', [{'tenant': 'other'}, {'chat': 'other'}, {'principal': 'other'}])
def test_memory_isolated_by_tenant_chat_and_user(receiver, changes):
    r, _ = receiver
    r.accept(envelope())
    first = r.claim(worker_id='bot')
    complete(r, first, reply='本会话私密内容')
    seed_delivered_turn(r, first['operation_id'])
    r.accept(envelope('m2', **changes))
    other = r.claim(worker_id='bot')
    assert other['session_key'] != first['session_key']
    assert other['history'] == []


def test_memory_one_day_and_authorization_change(receiver):
    r, clock = receiver
    r.accept(envelope())
    complete(r, r.claim(worker_id='bot'))
    seed_delivered_turn(r, 'op_m1')
    r.accept(envelope('m2'))
    c = r.claim(worker_id='bot')
    assert len(c['history']) == 2
    complete(r, c)
    seed_delivered_turn(r, 'op_m2')
    narrowed = envelope('m3')
    narrowed['policy']['projects'] = []
    narrowed['policy']['version'] = 2
    r.accept(narrowed)
    assert r.claim(worker_id='bot')['history'] == []
    clock[0] += 86401
    r.accept(envelope('m4', now=clock[0]))
    assert r.claim(worker_id='bot')['history'] == []


def test_worker_token_cannot_complete_other_task_or_add_authority(receiver):
    r, _ = receiver
    r.accept(envelope())
    r.accept(envelope('m2'))
    first, second = r.claim(worker_id='bot'), r.claim(worker_id='bot')
    wrong = dict(second, lease_token=first['lease_token'])
    with pytest.raises(ContractError, match='token'):
        complete(r, wrong)
    with pytest.raises(ContractError, match='authority'):
        r.complete(**{k: first[k] for k in ('operation_id', 'run_id', 'generation', 'lease_token')},
            result={'response_kind': 'sufficient', 'remaining_requirements': [], 'artifacts': [],
                    'reply_text': 'send', 'target': 'new-group'})


def test_receiver_rejects_forged_principal_for_session(receiver):
    r, _ = receiver
    forged = envelope()
    forged['request_context']['acting_principal'] = 'another-user'
    with pytest.raises(ContractError, match='principal'):
        r.accept(forged)


def test_full_harness_receiver_execution_delivery_readback(tmp_path):
    clock = [1000.]
    store = Store(tmp_path / 'full.sqlite', clock=lambda: clock[0])
    receiver = DurableReceiver(store)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent, scope='grok-local')
    harness = Harness(store, HarnessConfig('tenant', 'bot', 'owner', 'chat', capabilities=caps),
                      adapters={'grok_bot': adapter})
    harness.register_cutover('grok-local', evidence_ref='test shared receiver ledger')
    event = Event('tenant', 'message1', 'chat', 'owner', 'A 项目现在进展如何？', 999,
                  channel='bot_dm', project_ids=('A',))
    harness.ingest(event, Policy('owner', 'host-auth', 'chat', ('chat',), ('A',), allow_reply=True))
    routed = harness.route_one()
    dispatched = harness.dispatch_one()
    assert dispatched['dispatch_state'] == 'accepted'
    assert dispatched['execution_state'] != 'running'
    assert receiver.health()['status'] == 'awaiting_worker'
    calls = []
    def runner(envelope, context):
        calls.append(context['run_id'])
        assert context['history'] == []
        return {'response_kind': 'sufficient', 'reply_text': '当前已进入测试阶段。',
                'remaining_requirements': [], 'artifacts': []}
    pump = ReceiverPump(receiver, runner, harness=harness)
    assert pump.step()['status'] == 'succeeded'
    task_id = dispatched['task_id']
    assert harness.task(task_id)['execution_state'] == 'succeeded'
    assert receiver.publish_delivered_memory(harness) == 0
    sends = []
    def sender(**kwargs):
        sends.append(kwargs)
        return {'platform_id': 'actual-output-id', 'platform_time': 1001.}
    sent = harness.deliver_recorded_result(task_id, sender)
    harness.deliver_recorded_result(task_id, sender)
    assert receiver.publish_delivered_memory(harness) == 0
    assert len(sends) == len(calls) == 1
    harness.verify_delivery(task_id, {'platform_id': 'actual-output-id', 'target': 'chat',
        'body': '当前已进入测试阶段。', 'sender_id': 'bot', 'platform_time': 1001.})
    assert harness.task(task_id)['verification_state'] == 'sufficient'
    assert receiver.publish_delivered_memory(harness) == 1
    assert len(store.rows('SELECT * FROM h4_receiver_memory')) == 2
    assert receiver.reconcile(harness) == 0


def test_long_worker_heartbeat_renews_harness_without_reexecuting(tmp_path):
    clock = [1000.]
    store = Store(tmp_path / 'heartbeat.sqlite', clock=lambda: clock[0])
    receiver = DurableReceiver(store, worker_lease_seconds=10)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent, scope='long-run')
    harness = Harness(store, HarnessConfig('tenant', 'bot', 'owner', 'chat', capabilities=caps),
                      adapters={'grok_bot': adapter})
    harness.register_cutover('long-run', evidence_ref='synthetic shared receiver')
    harness.ingest(Event('tenant', 'm1', 'chat', 'owner', 'A 项目现在进展如何？', 999,
        channel='bot_dm', project_ids=('A',)), Policy('owner', 'auth', 'chat', ('chat',), ('A',), allow_reply=True))
    harness.route_one()
    task = harness.dispatch_one()
    claim = receiver.claim(worker_id='slow-original-worker')
    credentials = {k: claim[k] for k in ('operation_id', 'run_id', 'generation', 'lease_token')}
    receiver.reconcile(harness)
    for _ in range(20):
        clock[0] += 5
        receiver.heartbeat(**credentials)
        receiver.reconcile(harness)
        harness.expire()
        assert harness.task(task['task_id'])['execution_state'] == 'running'
    assert harness.dispatch_one() is None
    clock[0] += 11
    harness.expire()
    receiver.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'unknown'
    # Polling the control plane alone cannot renew model execution.
    receiver.query(claim['operation_id'])
    receiver.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'unknown'
    receiver.heartbeat(**credentials)
    receiver.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'running'
    complete(receiver, claim)
    receiver.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'succeeded'


@pytest.mark.parametrize('failure', ['runner_error', 'no_worker'])
def test_failure_is_visible_and_releases_capacity_after_delivery(tmp_path, failure):
    clock = [1000.]
    store = Store(tmp_path / 'failure.sqlite', clock=lambda: clock[0])
    receiver = DurableReceiver(store)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent, scope='one-capacity')
    harness = Harness(store, HarnessConfig('tenant', 'bot', 'owner', 'chat', capabilities=caps, queue_limit=1),
                      adapters={'grok_bot': adapter})
    harness.register_cutover('one-capacity', evidence_ref='synthetic shared receiver')
    def failing_runner(env, ctx):
        raise RuntimeError('sensitive-provider-error-not-for-user')
    pumps = [ReceiverPump(receiver, failing_runner, harness=harness)] if failure == 'runner_error' else []
    sent = []
    def sender(**kwargs):
        sent.append(kwargs)
        return {'platform_id': 'platform-' + str(len(sent)), 'platform_time': clock[0]}
    def readback(task_id):
        effect = store.one('SELECT * FROM h1_effects WHERE task_id=?', (task_id,))
        return {name: effect[name] for name in ('platform_id', 'target', 'body', 'platform_time')} | {'sender_id': 'bot'}
    service = RuntimeService(SimpleNamespace(gateway=harness, scheduler=None), receivers=[receiver],
                             pumps=pumps, sender=sender, readback=readback)
    policy = Policy('owner', 'auth', 'chat', ('chat',), ('A',), allow_reply=True)
    harness.ingest(Event('tenant', 'm1', 'chat', 'owner', 'A 项目进展如何？', 999,
                  channel='bot_dm', project_ids=('A',)), policy)
    service.step()
    if failure == 'no_worker':
        clock[0] += 181
        service.step()  # no worker claim occurs at any point
    task = store.one('SELECT * FROM h1_tasks')
    assert task['execution_state'] == 'failed'
    assert task['delivery_state'] == 'delivered'
    assert task['verification_state'] == 'sufficient'
    assert 'full_completed_at' not in json.loads(task['stamps'])
    assert len(sent) == 1 and '未完成' in sent[0]['body']
    assert 'sensitive-provider-error' not in sent[0]['body']
    assert json.loads(task['result_json'])['response_kind'] in {'failed', 'unavailable'}
    harness.ingest(Event('tenant', 'm2', 'chat', 'owner', 'A 项目进展如何？', clock[0],
                  channel='bot_dm', project_ids=('A',)), policy)
    assert harness.route_one()['route_state'] == 'resolved'
