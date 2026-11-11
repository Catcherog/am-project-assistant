"""Persistent h1 delivery claims with actual Harness/effect/attachment states.

The transport is controlled and synthetic. These tests measure concurrency and
non-replay, not real Feishu latency or phone visibility.
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import io
import json
import threading
import time
from types import SimpleNamespace as NS

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.agents import ArtifactStore
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store
from integration.phase1_artifacts import ArtifactDelivery


@pytest.fixture
def rig(tmp_path):
    now = [time.time()]
    store = Store(tmp_path / 'delivery.sqlite', clock=lambda: now[0])
    receiver = DurableReceiver(store, intents=('project.query', 'conversation.reply',
        'work_message.assist', 'document.process'))
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent)
    cfg = HarnessConfig('tenant', 'bot', 'owner', 'owner-dm', capabilities=caps)
    harness = Harness(store, cfg, adapters={'grok_bot': adapter})
    artifacts = ArtifactStore(tmp_path / 'artifacts')
    values, policies, calls, completed, observations = {}, {}, [], [], []

    def runner(envelope, context):
        return values[envelope['source']['native_id']]

    pump = ReceiverPump(receiver, runner, harness=harness)

    def sender(**kwargs):
        calls.append(kwargs)
        completed.append(kwargs['body'])
        return {'platform_id': 'sent-' + kwargs['idempotency_key'], 'platform_time': now[0]}

    def readback(task_id):
        effect = store.one('SELECT * FROM h1_effects WHERE task_id=?', (task_id,))
        return {'platform_id': effect['platform_id'], 'target': effect['target'],
                'body': effect['body'], 'sender_id': 'bot', 'platform_time': now[0]}

    state = NS(store=store, now=now, receiver=receiver, adapter=adapter, cfg=cfg,
               h=harness, artifacts=artifacts, pump=pump, calls=calls, completed=completed,
               observations=observations, policies=policies, values=values,
               sender=sender, readback=readback)

    def add(mid, *, chat='chat-a', actor='alice', thread='', channel='bot_dm', finish=True,
            file=False, intent='project.query'):
        now[0] += .01
        target = 'owner-dm' if channel == 'personal_dm' else chat
        principal = 'owner' if channel == 'personal_dm' else actor
        text = ('生成一份 Word 文档 ' if intent == 'document.process' else 'A 项目进展如何？ ') + mid
        event = Event('tenant', mid, chat, actor, text, now[0],
            channel=channel, thread_id=thread, mentions=('bot',) if channel == 'group' else (),
            project_ids=('A',))
        policy = Policy(principal, 'synthetic-host', target, tuple(sorted({chat, target})),
                        ('A',), allow_reply=True)
        policies[mid] = policy
        harness.ingest(event, policy)
        routed = harness.route_one()
        assert routed['route_state'] == 'resolved'
        assert routed['intent'] == ('work_message.assist' if channel == 'personal_dm' else intent)
        harness.register_cutover(routed['scope'], evidence_ref='synthetic original consumer')
        task = harness.dispatch_one()
        assert task is not None and task['dispatch_state'] == 'accepted'
        files = []
        if file:
            files.append(artifacts.write(task_id=task['task_id'], business_id=task['business_id'],
                name=mid + '.md', data=('synthetic complete content ' + mid).encode(), media_type='text/markdown'))
        values[mid] = {'response_kind': 'sufficient', 'reply_text': 'answer:' + mid,
                       'remaining_requirements': [], 'artifacts': files}
        if finish:
            assert pump.step()['status'] == 'succeeded'
        return harness.task(task['task_id'])

    def service(**kwargs):
        options = {'sender': sender, 'readback': readback, **kwargs}
        value = RuntimeService(NS(gateway=harness, scheduler=None), **options)
        return value

    def record(service):
        service.assistant = NS(prepare_reply=lambda *args: True,
            record_delivery=lambda task, result, evidence: observations.append(task['task_id']))
        return service

    state.add, state.service, state.record = add, service, record
    return state


def effects(rig):
    return rig.store.rows('SELECT task_id,state,platform_id FROM h1_effects ORDER BY created,effect_id')


def finish_claim(rig, claim):
    credentials = {name: claim[name] for name in ('operation_id', 'run_id', 'generation', 'lease_token')}
    native_id = claim['envelope']['source']['native_id']
    rig.receiver.complete(**credentials, result=rig.values[native_id], status='succeeded',
                          stopped_evidence='synthetic runner actually returned')
    rig.receiver.reconcile(rig.h)


@pytest.mark.parametrize('blocking_stage', ['sender', 'readback'])
def test_other_conversation_finishes_while_network_stage_is_blocked(rig, blocking_stage):
    first = rig.add('slow-a')
    rig.add('after-a')
    other = rig.add('fast-b', chat='chat-b')
    entered, release = threading.Event(), threading.Event()

    def sender(**kwargs):
        if blocking_stage == 'sender' and kwargs['body'] == 'answer:slow-a':
            entered.set()
            assert release.wait(5)
        return rig.sender(**kwargs)

    def readback(task_id):
        if blocking_stage == 'readback' and task_id == first['task_id']:
            entered.set()
            assert release.wait(5)
        return rig.readback(task_id)

    service = rig.service(sender=sender, readback=readback)
    with ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(service.delivery_once)
        try:
            assert entered.wait(2)
            assert pool.submit(service.delivery_once).result(timeout=2) == 1
            assert rig.h.task(other['task_id'])['verification_state'] == 'sufficient'
            assert 'answer:after-a' not in rig.completed
        finally:
            release.set()
        assert slow.result(timeout=3) == 2
    assert rig.completed.index('answer:slow-a') < rig.completed.index('answer:after-a')
    assert len(rig.calls) == len({call['idempotency_key'] for call in rig.calls}) == 3


def test_same_group_thread_orders_different_senders_and_other_thread_can_progress(rig):
    first = rig.add('group-first', chat='group', actor='alice', channel='group', thread='thread-a')
    rig.add('group-second', chat='group', actor='bob', channel='group', thread='thread-a')
    other = rig.add('thread-b', chat='group', actor='carol', channel='group', thread='thread-b')
    entered, release = threading.Event(), threading.Event()
    def readback(task_id):
        if task_id == first['task_id']:
            entered.set()
            assert release.wait(5)
        return rig.readback(task_id)
    service = rig.service(readback=readback)
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(service.delivery_once)
        try:
            assert entered.wait(2)
            assert service.delivery_once() == 1
            assert rig.h.task(other['task_id'])['verification_state'] == 'sufficient'
            assert 'answer:group-second' not in rig.completed
        finally:
            release.set()
        slow.result(timeout=3)
    assert rig.completed == ['answer:group-first', 'answer:thread-b', 'answer:group-second']


def test_actual_owner_dm_target_serializes_passive_source_and_direct_request(rig):
    first = rig.add('passive', chat='personal-source', actor='peer', channel='personal_dm', thread='source-thread')
    rig.add('owner-direct', chat='owner-dm', actor='owner')
    entered, release = threading.Event(), threading.Event()
    def readback(task_id):
        if task_id == first['task_id']:
            entered.set()
            assert release.wait(5)
        return rig.readback(task_id)
    service = rig.service(readback=readback)
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(service.delivery_once)
        try:
            assert entered.wait(2)
            assert service.delivery_once() == 0
            assert rig.completed == ['answer:passive']
        finally:
            release.set()
        slow.result(timeout=3)
    assert rig.completed == ['answer:passive', 'answer:owner-direct']
    assert all(call['target'] == 'owner-dm' for call in rig.calls)


def test_earlier_running_model_in_group_prevents_later_ready_reply_overtaking(rig):
    first = rig.add('model-still-running', chat='group', actor='alice', channel='group', finish=False)
    claim = rig.receiver.claim(worker_id='synthetic-long-model')
    rig.receiver.reconcile(rig.h)
    assert rig.h.task(first['task_id'])['execution_state'] == 'running'
    rig.add('ready-later', chat='group', actor='bob', channel='group')
    other = rig.add('independent', chat='other', actor='carol')
    service = rig.service()
    assert service.delivery_once() == 1
    assert rig.h.task(other['task_id'])['verification_state'] == 'sufficient'
    assert rig.completed == ['answer:independent']
    finish_claim(rig, claim)
    assert service.delivery_once() == 2
    assert rig.completed == ['answer:independent', 'answer:model-still-running', 'answer:ready-later']


def test_long_document_model_does_not_block_short_query_in_the_same_chat(rig):
    document = rig.add('long-document-model', intent='document.process', finish=False)
    claim = rig.receiver.claim(worker_id='synthetic-document-model')
    rig.receiver.reconcile(rig.h)
    short = rig.add('short-query', actor='bob')
    service = rig.service()
    assert service.delivery_once() == 1
    assert rig.h.task(document['task_id'])['execution_state'] == 'running'
    assert rig.h.task(short['task_id'])['verification_state'] == 'sufficient'
    assert rig.completed == ['answer:short-query']
    finish_claim(rig, claim)
    assert service.delivery_once() == 1
    assert rig.completed == ['answer:short-query', 'answer:long-document-model']


def test_two_runtime_instances_share_one_durable_send_claim(rig):
    rig.add('once')
    entered, release = threading.Event(), threading.Event()
    def sender(**kwargs):
        entered.set()
        assert release.wait(5)
        return rig.sender(**kwargs)
    one, two = rig.service(sender=sender), rig.service(sender=sender)
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(one.delivery_once)
        try:
            assert entered.wait(2)
            assert two.delivery_once() == 0
        finally:
            release.set()
        assert running.result(timeout=3) == 1
    assert two.delivery_once() == 0
    assert len(rig.calls) == len(effects(rig)) == 1


def test_expired_preparation_claim_is_recovered_and_stale_token_cannot_release_new_claim(rig):
    task = rig.add('recover')
    one, two = rig.service(), rig.service()
    old = one._claim_delivery()
    assert old['task_id'] == task['task_id']
    rig.now[0] += one.delivery_lease_seconds + 1
    current = two._claim_delivery()
    assert current['task_id'] == old['task_id'] and current['token'] != old['token']
    assert not one._renew_delivery_claim(old)
    one._release_delivery_claim(old)
    assert two._renew_delivery_claim(current)
    with pytest.raises(ContractError):
        rig.h.deliver_recorded_result(task['task_id'], rig.sender,
            claim_guard=lambda db: one._renew_delivery_claim(old, db=db))
    assert effects(rig) == [] and rig.calls == []
    two._release_delivery_claim(current)
    assert two.delivery_once() == 1
    assert len(rig.calls) == 1


def test_expired_lease_never_replays_inflight_text_or_overtakes_same_chat(rig):
    rig.add('inflight')
    rig.add('later-same-chat')
    rig.add('other-chat', chat='other')
    entered, release = threading.Event(), threading.Event()
    def sender(**kwargs):
        if kwargs['body'] == 'answer:inflight':
            entered.set()
            assert release.wait(5)
        return rig.sender(**kwargs)
    one, two = rig.service(sender=sender), rig.service(sender=sender)
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(one.delivery_once)
        try:
            assert entered.wait(2)
            assert effects(rig)[0]['state'] == 'sending'
            rig.now[0] += one.delivery_lease_seconds + 1
            assert two.delivery_once() == 1
            assert rig.completed == ['answer:other-chat']
        finally:
            release.set()
        assert slow.result(timeout=3) == 2
    assert rig.completed == ['answer:other-chat', 'answer:inflight', 'answer:later-same-chat']
    assert len({call['idempotency_key'] for call in rig.calls}) == 3


def test_unknown_text_survives_restart_and_scope_stays_blocked_without_replay(rig):
    first = rig.add('unknown')
    rig.add('later')
    rig.add('other', chat='other')
    calls = []
    def sender(**kwargs):
        calls.append(kwargs)
        if kwargs['body'] == 'answer:unknown':
            raise TimeoutError('synthetic lost acknowledgement')
        return rig.sender(**kwargs)
    one = rig.service(sender=sender)
    assert one.delivery_once() == 1
    assert rig.h.task(first['task_id'])['delivery_state'] == 'unknown'
    rig.now[0] += one.delivery_lease_seconds + 1
    assert rig.service(sender=sender).delivery_once() == 0
    assert [call['body'] for call in calls] == ['answer:unknown', 'answer:other']
    assert rig.h.task(first['task_id'])['delivery_state'] == 'unknown'


def test_earlier_revoked_answer_without_effect_does_not_block_later_same_lane(rig):
    first = rig.add('revoked-before-send')
    later = rig.add('still-authorized')
    rig.h.update_policy(first['task_id'], replace(rig.policies['revoked-before-send'], version=2, revoked=True))
    service = rig.service()
    assert service.delivery_once() == 1
    assert rig.completed == ['answer:still-authorized']
    assert rig.h.task(later['task_id'])['verification_state'] == 'sufficient'
    assert rig.store.rows('SELECT * FROM h1_effects WHERE task_id=?', (first['task_id'],)) == []


def test_expired_readback_can_resume_but_old_worker_cannot_publish_again(rig):
    task = rig.add('readback')
    entered, release = threading.Event(), threading.Event()
    count = [0]
    def readback(task_id):
        count[0] += 1
        if count[0] == 1:
            entered.set()
            assert release.wait(5)
        return rig.readback(task_id)
    one = rig.record(rig.service(readback=readback))
    two = rig.record(rig.service(readback=readback))
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(one.delivery_once)
        try:
            assert entered.wait(2)
            rig.now[0] += one.delivery_lease_seconds + 1
            assert two.delivery_once() == 1
            assert rig.observations == [task['task_id']]
        finally:
            release.set()
        assert slow.result(timeout=3) == 0
    assert rig.observations == [task['task_id']]
    assert len(rig.calls) == 1 and rig.h.task(task['task_id'])['verification_state'] == 'sufficient'


def artifact_port(rig, task, *, block=None, unknown=None):
    """Actual ArtifactDelivery and store; only the SDK/read transport is fake."""
    entered, release = threading.Event(), threading.Event()
    counts = {'upload': 0, 'send': 0, 'resource': 0}
    data = rig.artifacts.read(rig.values[json.loads(task['request_json'])['native_id']]['artifacts'][0]['artifact_id'])
    event = json.loads(task['request_json'])
    def response(**data):
        return NS(success=lambda: True, data=NS(**data))
    def upload(request):
        counts['upload'] += 1
        if block == 'upload' and counts['upload'] == 1:
            entered.set()
            assert release.wait(5)
        return response(file_key='' if unknown == 'upload' else 'file_upload')
    def send(request):
        counts['send'] += 1
        if block == 'send' and counts['send'] == 1:
            entered.set()
            assert release.wait(5)
        return response(message_id='' if unknown == 'send' else 'file-message')
    def resource(request):
        counts['resource'] += 1
        if block == 'resource' and counts['resource'] == 1:
            entered.set()
            assert release.wait(5)
        return NS(success=lambda: True, file=io.BytesIO(data))
    def read_message(source, message_id):
        row = rig.store.one('SELECT name,target FROM h4_artifact_effects WHERE task_id=?', (task['task_id'],))
        return {'message_id': message_id, 'chat_id': row['target'], 'deleted': False, 'msg_type': 'file',
                'parent_id': event['native_id'], 'thread_id': event['thread_id'],
                'create_time': str(int(rig.now[0] * 1000)),
                'sender': {'id': 'bot', 'sender_type': 'app'},
                'body': {'content': json.dumps({'file_key': 'file_resource', 'file_name': row['name']})}}
    client = NS(im=NS(v1=NS(file=NS(create=upload), message=NS(reply=send, create=send),
                            message_resource=NS(get=resource))))
    port = ArtifactDelivery(rig.h, rig.artifacts, client,
        lambda source: rig.policies[source.native_id], NS(read_message=read_message), send_live=True)
    return NS(port=port, counts=counts, entered=entered, release=release)


@pytest.mark.parametrize('stage', ['upload', 'send'])
def test_slow_artifact_keeps_its_scope_and_other_chat_progresses_after_lease_expiry(rig, stage):
    first = rig.add('artifact-slow', file=True)
    rig.add('after-artifact')
    rig.add('other', chat='other')
    bound = artifact_port(rig, first, block=stage)
    one = rig.service(artifact_delivery=bound.port)
    two = rig.service(artifact_delivery=bound.port)
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(one.delivery_once)
        try:
            # The actual SDK's first import may dominate this synthetic setup;
            # no latency acceptance is inferred from this synchronization wait.
            assert bound.entered.wait(10)
            rig.now[0] += one.delivery_lease_seconds + 1
            assert two.delivery_once() == 1
            assert rig.completed == ['answer:artifact-slow', 'answer:other']
            assert bound.counts[stage] == 1
        finally:
            bound.release.set()
        assert slow.result(timeout=3) == 2
    assert bound.counts['upload'] == bound.counts['send'] == 1
    assert rig.store.one('SELECT state FROM h4_artifact_effects')['state'] == 'verified'
    assert rig.completed[-1] == 'answer:after-artifact'


@pytest.mark.parametrize('stage', ['upload', 'send'])
def test_unknown_artifact_is_never_reuploaded_or_resent_after_restart(rig, stage):
    first = rig.add('artifact-unknown', file=True)
    rig.add('later')
    rig.add('other', chat='other')
    bound = artifact_port(rig, first, unknown=stage)
    one = rig.service(artifact_delivery=bound.port)
    assert one.delivery_once() == 1
    rig.now[0] += one.delivery_lease_seconds + 1
    assert rig.service(artifact_delivery=bound.port).delivery_once() == 0
    assert rig.store.one('SELECT state FROM h4_artifact_effects')['state'] == stage + '_unknown'
    assert bound.counts['upload'] == 1
    assert bound.counts['send'] == (1 if stage == 'send' else 0)
    assert rig.completed == ['answer:artifact-unknown', 'answer:other']


def test_late_artifact_readback_cannot_overwrite_new_claims_verified_result(rig):
    first = rig.add('artifact-readback', file=True)
    bound = artifact_port(rig, first, block='resource')
    one = rig.record(rig.service(artifact_delivery=bound.port))
    two = rig.record(rig.service(artifact_delivery=bound.port))
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(one.delivery_once)
        try:
            assert bound.entered.wait(10)
            rig.now[0] += one.delivery_lease_seconds + 1
            assert two.delivery_once() == 1
            assert rig.observations == [first['task_id']]
        finally:
            bound.release.set()
        assert slow.result(timeout=3) == 0
    assert rig.store.one('SELECT state FROM h4_artifact_effects')['state'] == 'verified'
    assert bound.counts['upload'] == bound.counts['send'] == 1
    assert rig.observations == [first['task_id']]


def test_long_document_upload_does_not_block_same_chat_short_query(rig):
    first = rig.add('long-document-file', intent='document.process', file=True)
    rig.add('second-document', intent='document.process')
    short = rig.add('short-during-upload')
    bound = artifact_port(rig, first, block='upload')
    one, two = rig.service(artifact_delivery=bound.port), rig.service(artifact_delivery=bound.port)
    with ThreadPoolExecutor(max_workers=1) as pool:
        slow = pool.submit(one.delivery_once)
        try:
            assert bound.entered.wait(10)
            assert two.delivery_once() == 1
            assert rig.h.task(short['task_id'])['verification_state'] == 'sufficient'
            assert rig.completed == ['answer:long-document-file', 'answer:short-during-upload']
        finally:
            bound.release.set()
        assert slow.result(timeout=3) == 2
    assert bound.counts['upload'] == bound.counts['send'] == 1
    assert rig.completed[-1] == 'answer:second-document'


def test_default_four_runtime_delivery_workers_start_without_new_ingress(rig):
    first = rig.add('worker-slow')
    fast = rig.add('worker-fast', chat='other')
    entered, release, done = threading.Event(), threading.Event(), threading.Event()
    def readback(task_id):
        if task_id == first['task_id']:
            entered.set()
            assert release.wait(5)
        if task_id == fast['task_id']:
            done.set()
        return rig.readback(task_id)
    service = rig.service(readback=readback)
    assert service.delivery_workers == 4
    service.start()
    try:
        assert entered.wait(2)
        assert done.wait(2)
        assert len([thread for thread in service.threads if 'delivery' in thread.name]) == 4
    finally:
        release.set()
        assert service.stop()['stopped']
    assert len(rig.calls) == 2

