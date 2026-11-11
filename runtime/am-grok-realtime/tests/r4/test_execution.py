import time

import pytest

from am_gateway.adapters.base import AdapterError, Deadline
from am_gateway.adapters.http import request_json
from am_gateway.models import Event
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.execution import HTTPReceiverAdapter, HTTPWorkerClient, ReceiverHTTPServer, ReceiverPump
from am_gateway.store import Store
from test_receiver import envelope


@pytest.fixture
def network_receiver(tmp_path):
    receiver = DurableReceiver(Store(tmp_path / 'receiver.sqlite', clock=lambda: 1000.))
    server = ReceiverHTTPServer(receiver, admin_token='admin-test-token', worker_token='worker-test-token')
    server.start()
    try:
        yield receiver, server
    finally:
        server.stop()


def test_http_accept_claim_execute_and_query_native_worker(network_receiver):
    receiver, server = network_receiver
    adapter = HTTPReceiverAdapter(server.base_url, 'admin-test-token')
    assert not adapter.capability('project.query').available
    receipt = adapter.accept(envelope())
    assert receipt['payload']['accepted_by'] == 'gateway_receiver'
    assert adapter.health()['status'] == 'awaiting_worker'
    client = HTTPWorkerClient(server.base_url, 'worker-test-token')
    seen = []
    def runner(envelope, context):
        seen.append(envelope['identity']['business_operation_id'])
        assert context['cancelled']() is False
        return {'response_kind': 'sufficient', 'reply_text': '已核对实际消息。',
                'remaining_requirements': [], 'artifacts': []}
    worker = ReceiverPump(client, runner)
    assert worker.step()['status'] == 'succeeded'
    assert worker.step() is None
    queried = adapter.query('op_m1', receipt['payload']['run_id'], 1)
    assert queried['result']['reply_text'] == '已核对实际消息。'
    assert [r['kind'] for r in queried['events']] == ['accepted', 'running', 'result', 'ended']
    assert seen == ['op_m1']
    assert adapter.health()['external_bot_boundary'] == 'unverified'


def test_worker_credential_cannot_submit_or_query_other_tasks(network_receiver):
    _, server = network_receiver
    adapter = HTTPReceiverAdapter(server.base_url, 'worker-test-token')
    with pytest.raises(AdapterError, match='rejected'):
        adapter.accept(envelope())
    with pytest.raises(AdapterError, match='rejected'):
        adapter.query('op_m1')
    wrong_client = HTTPWorkerClient(server.base_url, 'admin-test-token')
    with pytest.raises(AdapterError, match='rejected'):
        wrong_client.claim(worker_id='not-worker')
    with pytest.raises(AdapterError, match='rejected'):
        request_json(server.base_url + '/v1/worker/shell', method='POST', payload={'command': 'whoami'},
                     deadline=Deadline(time.monotonic() + 2), bearer_token='worker-test-token')


def test_worker_result_requires_original_lease_token(network_receiver):
    _, server = network_receiver
    HTTPReceiverAdapter(server.base_url, 'admin-test-token').accept(envelope())
    client = HTTPWorkerClient(server.base_url, 'worker-test-token')
    claim = client.claim(worker_id='native')
    identity = {k: claim[k] for k in ('operation_id', 'run_id', 'generation', 'lease_token')}
    identity['lease_token'] = 'forged'
    with pytest.raises(AdapterError, match='rejected'):
        client.complete(**identity, status='succeeded', result={'response_kind': 'sufficient',
            'reply_text': 'forged', 'remaining_requirements': [], 'artifacts': []})


def test_wake_failure_keeps_original_durable_operation(network_receiver):
    receiver, _ = network_receiver
    class FailingWake:
        def notify(self):
            raise TimeoutError('synthetic wake timeout')
    adapter = LocalReceiverAdapter(receiver, wake=FailingWake())
    first = adapter.accept(envelope())
    assert adapter.accept(envelope()) == first
    assert receiver.claim(worker_id='five-minute-recovery-poller') is not None
    assert receiver.claim(worker_id='realtime-worker') is None


def test_lost_ack_reconciles_original_remote_operation(tmp_path, network_receiver):
    receiver, server = network_receiver
    adapter = HTTPReceiverAdapter(server.base_url, 'admin-test-token',
                                  verified_boundary=True, evidence_ref='synthetic loopback receiver fixture')
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent, scope='remote-test')
    store = Store(tmp_path / 'harness.sqlite', clock=lambda: 1000.)
    harness = Harness(store, HarnessConfig('tenant', 'bot', 'owner', 'chat', capabilities=caps),
                      adapters={'grok_bot': adapter})
    harness.register_cutover('remote-test', evidence_ref='synthetic shared claims')
    harness.ingest(Event('tenant', 'm1', 'chat', 'owner', 'A 项目现在进展如何？', 999,
        channel='bot_dm', project_ids=('A',)), Policy('owner', 'auth', 'chat', ('chat',), ('A',), allow_reply=True))
    harness.route_one()
    original = adapter.accept
    def lose_ack(env):
        original(env)
        raise TimeoutError('synthetic response dropped after persistence')
    adapter.accept = lose_ack
    task = harness.dispatch_one()
    assert task['dispatch_state'] == 'unknown'
    assert receiver.health()['states'] == {'accepted': 1}
    adapter.accept = original
    assert adapter.reconcile(harness) == 1
    assert harness.task(task['task_id'])['dispatch_state'] == 'accepted'
    worker = ReceiverPump(HTTPWorkerClient(server.base_url, 'worker-test-token'),
        lambda env, context: {'response_kind': 'sufficient', 'reply_text': '单次实际运行结果',
                             'remaining_requirements': [], 'artifacts': []})
    worker.step()
    adapter.reconcile(harness)
    assert harness.task(task['task_id'])['execution_state'] == 'succeeded'
    assert harness.dispatch_one() is None
    assert receiver.health()['states'] == {'succeeded': 1}


def test_cancel_endpoint_reports_pending_until_native_worker_returns(network_receiver):
    _, server = network_receiver
    adapter = HTTPReceiverAdapter(server.base_url, 'admin-test-token')
    adapter.accept(envelope())
    client = HTTPWorkerClient(server.base_url, 'worker-test-token')
    claim = client.claim(worker_id='bot')
    identity = {k: claim[k] for k in ('operation_id', 'run_id', 'generation', 'lease_token')}
    assert adapter.cancel('op_m1', claim['run_id'], 1)['stopped'] is False
    assert client.heartbeat(**identity)['cancel_requested'] is True
    result = client.complete(**identity, status='cancelled', stopped_evidence='native worker actually stopped')
    assert result['status'] == 'cancelled'


def test_receiver_http_restart_keeps_endpoint_and_original_operation(network_receiver):
    _, server = network_receiver
    adapter = HTTPReceiverAdapter(server.base_url, 'admin-test-token')
    receipt = adapter.accept(envelope())
    address = server.base_url
    assert server.stop()['stopped']
    server.start()
    assert server.base_url == address
    assert adapter.query('op_m1')['run_id'] == receipt['payload']['run_id']
    assert adapter.accept(envelope()) == receipt
