"""Keep uncertain protected executions open until original correlated evidence."""
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] /
                      'FS-AGENT-GROK-POC-20261017-01_R1_Studio' / 'src'))

from fs_agent.config import BoundaryError
from am_gateway.adapters.base import AdapterError
from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.agents import AgentRunner, ArtifactStore
from am_gateway.phase1.contracts import business_fingerprint, registry
from am_gateway.phase1.execution import (
    HTTPReceiverAdapter, HTTPWorkerClient, ReceiverHTTPServer, ReceiverPump)
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.store import Store
from integration.studio_agent_models import HostQwenDraftAdapter
from test_receiver import envelope


def model_request():
    return {'schema': 'am.gateway.job.v2', 'job_id': 'job', 'attempt_id': 'attempt',
            'task_kind': 'document.process', 'worker': 'document_agent', 'session_key': 'session',
            'conversation': {}, 'context': {}, 'output_mode': 'internal',
            'deadline_at': time.time() + 30, 'instructions': 'Prepare a synthetic draft.',
            'request': {'text': 'synthetic'}}


def result():
    return {'response_kind': 'sufficient', 'reply_text': '已取得原运行的明确结果。',
            'remaining_requirements': [], 'artifacts': []}


def identity(claim):
    return {k: claim[k] for k in ('operation_id', 'run_id', 'generation', 'lease_token')}


def capture_claim(receiver, monkeypatch):
    claims, original = [], receiver.claim
    def claim(**kwargs):
        value = original(**kwargs)
        if value is not None:
            claims.append(value)
        return value
    monkeypatch.setattr(receiver, 'claim', claim)
    return claims


@pytest.mark.parametrize('error', [
    BoundaryError('STOP_UNKNOWN'), RuntimeError('private provider error text')])
def test_host_provider_uncertainty_matches_coordinator_without_exposing_error(error):
    def complete(*args, **kwargs):
        raise error
    with pytest.raises(AdapterError) as caught:
        HostQwenDraftAdapter(NS(complete=complete)).run(model_request())
    assert caught.value.unknown and caught.value.status == 'stop_unknown'
    assert str(caught.value) == 'host_qwen_stop_unknown'
    assert 'private provider' not in str(caught.value)


def test_known_host_rejection_is_not_uncertain_execution():
    def complete(*args, **kwargs):
        raise BoundaryError('MODEL_BUSY')
    with pytest.raises(BoundaryError, match='MODEL_BUSY'):
        HostQwenDraftAdapter(NS(complete=complete)).run(model_request())


@pytest.mark.parametrize('text', ['not JSON', json.dumps({
    'schema': 'am.gateway.result.v2', 'job_id': 'another-job', 'attempt_id': 'attempt',
    'status': 'completed'})])
def test_normal_host_return_with_invalid_output_is_known_failure(text):
    provider = NS(complete=lambda *args, **kwargs: NS(text=text))
    with pytest.raises(AdapterError) as caught:
        HostQwenDraftAdapter(provider).run(model_request())
    assert caught.value.unknown is False
    assert str(caught.value) == 'invalid_host_qwen_result'


@pytest.mark.parametrize('cancel_before_error', [False, True])
@pytest.mark.parametrize('error', [BoundaryError('STOP_UNKNOWN'), RuntimeError('private exception')])
def test_real_host_adapter_pump_keeps_unknown_and_cancellation_pending(
        tmp_path, monkeypatch, cancel_before_error, error):
    env = envelope(now=time.time())
    env['routing'] = {'intent': 'document.process', 'adapter': 'document_agent'}
    env['identity']['business_fingerprint'] = business_fingerprint(
        text=env['payload']['raw_text'], intent='document.process', principal='owner',
        reply_target='chat', attachments=[], inputs=env['payload']['structured_inputs'])
    path = tmp_path / 'receiver.sqlite'
    receiver = DurableReceiver(Store(path), namespace='document_agent', intents=('document.process',))
    receiver.accept(env)
    claims = capture_claim(receiver, monkeypatch)
    def complete(*args, **kwargs):
        if cancel_before_error:
            receiver.cancel('op_m1')
        raise error
    runner = AgentRunner('document.process', ArtifactStore(tmp_path / 'artifacts'),
                         model=HostQwenDraftAdapter(NS(complete=complete)))
    assert ReceiverPump(receiver, runner).step()['status'] == 'unknown'
    credentials = identity(claims[0])
    state = receiver.query('op_m1')
    assert state['ended_at'] is None and state['lease_until'] is None and state['result'] is None
    assert state['last_error'] == 'host_qwen_stop_unknown'
    assert state['cancel_requested'] is cancel_before_error
    assert [entry['kind'] for entry in state['events']] == ['accepted', 'running', 'unknown']
    assert receiver.heartbeat(**credentials)['status'] == 'unknown'
    assert receiver.cancel('op_m1')['stopped'] is False
    assert receiver.heartbeat(**credentials) == {'status': 'unknown', 'cancel_requested': True}
    restarted = DurableReceiver(Store(path), namespace='document_agent', intents=('document.process',))
    assert restarted.claim(worker_id='restarted-worker') is None
    assert restarted.query('op_m1')['status'] == 'unknown'
    # Explicit original-worker stop evidence can resolve the existing operation.
    ended = restarted.complete(**credentials, status='cancelled',
                               stopped_evidence='correlated_original_inference_stop_verified')
    assert ended['status'] == 'cancelled' and ended['ended_at'] is not None
    assert ended['last_error'] == ''


def test_unknown_entry_requires_original_credentials_and_accepts_later_result(tmp_path):
    receiver = DurableReceiver(Store(tmp_path / 'receiver.sqlite', clock=lambda: 1000.))
    receiver.accept(envelope())
    claim = receiver.claim(worker_id='original')
    credentials = identity(claim)
    for invalid in ({'lease_token': 'forged'}, {'run_id': 'another-run'}, {'generation': 2}):
        with pytest.raises(ContractError, match='does not own'):
            receiver.mark_unknown(**(credentials | invalid))
    with pytest.raises(ContractError, match='recognized'):
        receiver.mark_unknown(**credentials, reason='private free-form error')
    receiver.mark_unknown(**credentials)
    receiver.mark_unknown(**credentials)
    assert [e['kind'] for e in receiver.query('op_m1')['events']].count('unknown') == 1
    ended = receiver.complete(**credentials, result=result(),
                              stopped_evidence='original_response_and_stop_observed')
    assert ended['status'] == 'succeeded'
    assert receiver.mark_unknown(**credentials)['status'] == 'succeeded'
    assert receiver.query('op_m1')['events'][-1]['kind'] == 'ended'


def test_unknown_receipt_reconciles_without_releasing_session(tmp_path, monkeypatch):
    clock = [1000.]
    store = Store(tmp_path / 'harness.sqlite', clock=lambda: clock[0])
    receiver = DurableReceiver(store)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent, scope='unknown-hold')
    harness = Harness(store, HarnessConfig('tenant', 'bot', 'owner', 'chat', capabilities=caps),
                      adapters={'grok_bot': adapter})
    harness.register_cutover('unknown-hold', evidence_ref='synthetic shared receiver')
    policy = Policy('owner', 'auth', 'chat', ('chat',), ('A',), allow_reply=True)
    harness.ingest(Event('tenant', 'm1', 'chat', 'owner', 'A 项目进展如何？', 999,
                        channel='bot_dm', project_ids=('A',)), policy)
    harness.route_one()
    task = harness.dispatch_one()
    claims = capture_claim(receiver, monkeypatch)
    def runner(env, context):
        raise AdapterError('synthetic_stop_unknown', status='stop_unknown', unknown=True)
    assert ReceiverPump(receiver, runner, harness=harness).step()['status'] == 'unknown'
    credentials = identity(claims[0])
    receiver.cancel(task['business_id'])
    receiver.heartbeat(**credentials)
    receiver.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'unknown'
    assert harness.query_execution(task['task_id'])['status'] == 'unknown'
    assert store.one('SELECT task_id FROM h1_sessions WHERE scope=?',
                     (task['scope'],))['task_id'] == task['task_id']
    harness.ingest(Event('tenant', 'm2', 'chat', 'owner', 'A 项目有哪些卡点？', 999,
                        channel='bot_dm', project_ids=('A',)), policy)
    harness.route_one()
    assert harness.dispatch_one() is None
    restarted = DurableReceiver(Store(store.path, clock=lambda: clock[0]))
    clock[0] += 600
    assert restarted.claim(worker_id='restart') is None
    restarted.reconcile(harness)
    harness.expire()
    assert harness.task(task['task_id'])['execution_state'] == 'unknown'
    assert store.one('SELECT task_id FROM h1_sessions WHERE scope=?',
                     (task['scope'],))['task_id'] == task['task_id']
    restarted.complete(**credentials, status='cancelled',
                       stopped_evidence='original_execution_stop_independently_verified')
    restarted.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'cancelled'
    assert store.one('SELECT task_id FROM h1_sessions WHERE scope=?',
                     (task['scope'],))['task_id'] is None


def test_http_worker_records_unknown_with_same_lease_and_can_reconcile(tmp_path, monkeypatch):
    receiver = DurableReceiver(Store(tmp_path / 'receiver.sqlite', clock=lambda: 1000.))
    server = ReceiverHTTPServer(receiver, admin_token='admin-test', worker_token='worker-test')
    server.start()
    try:
        admin = HTTPReceiverAdapter(server.base_url, 'admin-test')
        admin.accept(envelope())
        client = HTTPWorkerClient(server.base_url, 'worker-test')
        claims = capture_claim(client, monkeypatch)
        def runner(env, context):
            raise AdapterError('private-adapter-code', unknown=True)
        assert ReceiverPump(client, runner).step()['status'] == 'unknown'
        credentials = identity(claims[0])
        assert admin.query('op_m1')['last_error'] == 'adapter_execution_unknown'
        assert client.heartbeat(**credentials)['status'] == 'unknown'
        assert admin.cancel('op_m1')['stopped'] is False
        with pytest.raises(AdapterError):
            client.mark_unknown(**(credentials | {'lease_token': 'forged'}))
        with pytest.raises(AdapterError):
            HTTPWorkerClient(server.base_url, 'admin-test').mark_unknown(**credentials)
        assert client.complete(**credentials, status='cancelled',
            stopped_evidence='native_execution_stop_verified')['status'] == 'cancelled'
    finally:
        server.stop()


@pytest.mark.parametrize('supports_unknown', [False, True])
def test_unknown_reporting_unavailable_never_falls_back_to_completion(
        tmp_path, monkeypatch, supports_unknown):
    clock = [1000.]
    receiver = DurableReceiver(Store(tmp_path / 'receiver.sqlite', clock=lambda: clock[0]))
    receiver.accept(envelope())
    completed = []
    transport = NS(namespace=receiver.namespace, worker_lease_seconds=10,
                   claim=receiver.claim, heartbeat=receiver.heartbeat,
                   complete=lambda **kwargs: completed.append(kwargs))
    if supports_unknown:
        def unavailable(**kwargs):
            raise AdapterError('http_rejected')
        transport.mark_unknown = unavailable
    def runner(env, context):
        raise AdapterError('runner_uncertain', unknown=True)
    with pytest.raises(AdapterError) as caught:
        ReceiverPump(transport, runner).step()
    assert caught.value.unknown is True and completed == []
    assert receiver.query('op_m1')['ended_at'] is None
    clock[0] += 61
    assert receiver.claim(worker_id='recovery-poller') is None
    assert receiver.query('op_m1')['status'] == 'unknown'
    assert not any(e['kind'] == 'ended' for e in receiver.query('op_m1')['events'])
