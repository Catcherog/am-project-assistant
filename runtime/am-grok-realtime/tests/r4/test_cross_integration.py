"""Actual R4 modules together; Feishu SDK I/O ends at an in-memory transport.

No live Feishu, Grok Bot, model, real credentials or user approval is exercised.
"""
from dataclasses import replace
import json
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'FS-AGENT-GROK-POC-20261017-01_R1_Studio' / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'local-qwen3.8' / 'feishu-bridge'))

from am_gateway.collectors import SourceSpec
from am_gateway.models import ContractError, dumps
from am_gateway.phase1.agents import AgentRunner, ArtifactStore, ProposalRepository
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.control_api import ControlAPI
from am_gateway.phase1.execution import ReceiverPump, HTTPWorkerClient, ReceiverHTTPServer
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store
from integration.phase1_runtime import install
from integration.studio_phase1_factory import build_phase1_bundle


class MemorySDK:
    """Consume actual SDK request builders; only transport responses are synthetic."""
    def __init__(self, body='项目 P 进度如何？'):
        self.messages, self.sent, self.reads = {}, [], []
        self.add('om_input', 'oc_owner', 'ou_owner', body)
        self.im = NS(v1=NS(message=NS(reply=self.reply, create=self.create)))

    def add(self, mid, chat, author, text, *, parent=''):
        created = str(int(time.time() * 1000))
        self.messages[mid] = {'message_id': mid, 'chat_id': chat, 'create_time': created,
            'update_time': created, 'deleted': False, 'msg_type': 'text', 'thread_id': '',
            'parent_id': parent, 'root_id': '', 'mentions': [],
            'sender': {'id': author, 'id_type': 'open_id', 'sender_type': 'app' if author == 'ou_bot' else 'user',
                       'tenant_key': 'tenant'}, 'body': {'content': dumps({'text': text})}}

    def request(self, request):
        self.reads.append(request.uri)
        if request.uri.endswith('/chats'):
            items = []
        elif request.uri == '/open-apis/im/v1/messages':
            query = dict(request.queries)
            items = [value for value in self.messages.values() if value['chat_id'] == query['container_id']]
        else:
            items = [self.messages[request.uri.rsplit('/', 1)[-1]]]
        return NS(raw=NS(content=dumps({'code': 0, 'data': {'items': items, 'has_more': False}}).encode()))

    def reply(self, request):
        parent = request.message_id
        return self._send(request.request_body, self.messages[parent]['chat_id'], parent)

    def create(self, request):
        return self._send(request.request_body, request.request_body.receive_id)

    def _send(self, body, chat, parent=''):
        self.sent.append({'chat': chat, 'content': json.loads(body.content), 'uuid': body.uuid})
        mid = 'om_output_' + str(len(self.sent))
        self.add(mid, chat, 'ou_bot', json.loads(body.content)['text'], parent=parent)
        return NS(success=lambda: True, data=NS(message_id=mid))


def system(tmp_path, *, intent='project.query', body=None, runner=None):
    database = tmp_path / 'gateway-r4.sqlite3'
    namespace = 'grok_bot' if intent == 'project.query' else intent.split('.')[0] + '_agent'
    store = Store(database)
    receiver = DurableReceiver(store, namespace=namespace, intents=[intent])
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    caps[intent] = adapter.capability(intent)
    owner = NS(owner='ou_owner', owner_origin=lambda: {'chat': 'oc_owner'}, query_settings={},
               query_identity=NS(verify=lambda actor: True), ledger=NS(port=NS(disclosure=set())))
    bridge = NS(config={'app_id':'cli_synthetic'}, user_allowed=lambda actor: actor == 'ou_owner',
                am_self=owner, poc=NS(config=NS(tenant_key='tenant',bot_open_id='ou_bot')))
    sdk = MemorySDK(body or '项目 P 进度如何？')
    old_cutover = store.one("SELECT value FROM gw_meta WHERE key='live_cutover_at'")
    settings = {'live_cutover_at': float(old_cutover['value']) if old_cutover else time.time()-300, 'group_chat_ids': [],
                'project_chats': {'P':['oc_owner']}, 'send_live': True}
    bundle = build_phase1_bundle(bridge, {'phase1_gateway':settings}, sdk, tmp_path,
        capabilities=caps, adapters={namespace:adapter}, legacy_control_port=lambda payload: False)
    ingress, _ = install(bundle.ports, activate=False)
    collector = bundle.bind(ingress)
    h = ingress.gateway
    calls = []
    if runner is None:
        def runner(envelope, context):
            calls.append(envelope['identity']['business_operation_id'])
            return {'response_kind':'sufficient', 'reply_text':'测试资料仅记录原计划，最新执行结果仍待核对。',
                    'remaining_requirements':[], 'artifacts':[]}
    pump = ReceiverPump(receiver, runner, harness=h)
    service = RuntimeService(ingress, receivers=[receiver], pumps=[pump], collector=collector,
                             sender=bundle.sender, readback=bundle.readback,
                             ownership=bundle.legacy_ownership, auto_scopes=False)
    return NS(h=h, bundle=bundle, sdk=sdk, service=service, collector=collector,
              receiver=receiver, adapter=adapter, pump=pump, calls=calls)


def collect_and_route(s):
    source = SourceSpec('tenant', 'oc_owner', identity='am_bot', channel='bot_dm', project_ids=('P',))
    assert s.collector.collectors.collect_one(source.source_id, 'live')['status'] == 'completed'
    task = s.h.route_one()
    assert task and task['route_state'] == 'resolved'
    s.bundle.legacy_ownership.ensure_scope(task['scope'])
    return task


def test_full_collection_route_receiver_reply_and_exact_sdk_readback(tmp_path):
    s = system(tmp_path)
    task = collect_and_route(s)
    assert s.h.store.one('SELECT state FROM h1_inbox')['state'] == 'done'
    outcome = s.service.step()
    result = s.h.task(task['task_id'])
    assert result['execution_state'] == 'succeeded'
    assert result['delivery_state'] == 'delivered'
    assert result['verification_state'] == 'sufficient'
    assert outcome['delivered'] == 1
    assert len(s.calls) == len(s.sdk.sent) == 1
    assert s.sdk.sent[0]['chat'] == 'oc_owner'
    assert any(path.endswith('/om_output_1') for path in s.sdk.reads)
    stamps = json.loads(result['stamps'])
    assert all(name in stamps for name in ('t_i','t_r','t_a','t_p','t_o'))
    # Re-ingestion from the other channel and repeated runtime steps do not
    # create a second task or a second platform effect.
    source = SourceSpec('tenant','oc_owner',identity='am_bot',channel='bot_dm',project_ids=('P',))
    s.service.ingress.ingest_feishu_snapshot(s.sdk.messages['om_input'], source=source)
    for _ in range(3):
        s.service.step()
    assert len(s.sdk.sent) == len(s.calls) == 1
    assert len(s.h.store.rows('SELECT * FROM h1_tasks')) == 1


def test_accepted_receiver_task_survives_reconstruction_before_worker_runs(tmp_path):
    first = system(tmp_path)
    task = collect_and_route(first)
    first.service.dispatch_once()
    saved = first.h.task(task['task_id'])
    assert saved['dispatch_state'] == 'accepted'
    assert first.calls == first.sdk.sent == []
    # New service/receiver instances use the existing SQLite operation; no
    # resend or second accept is needed to discover the pending work.
    resumed = system(tmp_path)
    resumed.service.step()
    result = resumed.h.task(task['task_id'])
    assert result['business_id'] == saved['business_id']
    assert result['run_id'] == saved['run_id']
    assert result['execution_state'] == 'succeeded'
    assert result['verification_state'] == 'sufficient'
    assert len(resumed.calls) == len(resumed.sdk.sent) == 1
    assert len(resumed.h.store.rows('SELECT * FROM h4_receiver_runs')) == 1


def test_real_project_runner_to_control_confirmation_and_readback_evidence(tmp_path):
    class Model:
        def run(self, request):
            return {'schema':'am.gateway.result.v2','job_id':request['job_id'],
                'attempt_id':request['attempt_id'],'status':'completed','evidence_refs':[],
                'proposal':{'project_id':'P','target':{'type':'work_item','id':'item1'},
                    'action':'update_fields','changes':{'status':'completed'},'reason':'用户请求'}}
    artifacts = ArtifactStore(tmp_path/'artifacts')
    repository = ProposalRepository(Store(tmp_path/'gateway-r4.sqlite3'),'ou_owner')
    runner = AgentRunner('project.execute', artifacts, model=Model(), proposals=repository)
    s = system(tmp_path, intent='project.execute', body='把项目 P 状态改成完成', runner=runner)
    task = collect_and_route(s)
    s.service.step()
    answer = json.loads(s.h.task(task['task_id'])['result_json'])
    assert answer['response_kind'] == 'partial'
    proposal = repository.get(answer['proposal_id'])
    assert proposal['source_version'] == s.h.task(task['task_id'])['source_version']
    calls = []
    def execute(payload, *, idempotency_key):
        calls.append((payload,idempotency_key))
        return {'status':'applied','evidence_ref':'synthetic-project-readback:item1:v2'}
    control = ControlAPI(s.service,token='synthetic-owner-token-0123456789',proposals=repository,
                         executor=execute,artifacts=artifacts)
    assert control.handle('GET','/v1/proposals',{})['proposals'][0]['status'] == 'draft'
    route = '/v1/proposals/'+proposal['proposal_id']+'/confirm'
    data = {'version':proposal['version'],'digest':proposal['digest']}
    assert control.handle('POST',route,data)['status'] == 'applied'
    assert control.handle('POST',route,data)['status'] == 'applied'
    assert len(calls) == 1


def test_source_revision_invalidates_project_confirmation_before_executor(tmp_path):
    class Model:
        def run(self, request):
            return {'schema':'am.gateway.result.v2','job_id':request['job_id'],
                'attempt_id':request['attempt_id'],'status':'completed','evidence_refs':[],
                'proposal':{'project_id':'P','target':{'type':'work_item','id':'item1'},
                    'action':'update_fields','changes':{'status':'completed'}}}
    repository = ProposalRepository(Store(tmp_path/'gateway-r4.sqlite3'),'ou_owner')
    runner = AgentRunner('project.execute', ArtifactStore(tmp_path/'artifacts'), model=Model(),proposals=repository)
    s = system(tmp_path,intent='project.execute',body='把项目 P 状态改成完成',runner=runner)
    task = collect_and_route(s)
    s.service.step()
    proposal = repository.list()[0]
    raw = s.sdk.messages['om_input']
    raw['update_time'] = str(int(raw['update_time']) + 5000)
    raw['body'] = {'content':dumps({'text':'撤销修改要求'})}
    s.service.ingress.ingest_feishu_snapshot(raw,source=SourceSpec('tenant','oc_owner',identity='am_bot',channel='bot_dm',project_ids=('P',)))
    calls = []
    control=ControlAPI(s.service,token='synthetic-owner-token-0123456789',proposals=repository,
                       executor=lambda *a,**kw:calls.append(1))
    with pytest.raises(ContractError):
        control.handle('POST','/v1/proposals/'+proposal['proposal_id']+'/confirm',
                       {'version':proposal['version'],'digest':proposal['digest']})
    assert calls == []


def test_real_host_hook_starts_full_runtime_and_http_worker_without_second_consumer(tmp_path,monkeypatch):
    from integration.r2_runtime import install as install_host
    monkeypatch.setenv('AM_GATEWAY_RECEIVER_ADMIN_TOKEN','synthetic-admin-0123456789012345')
    monkeypatch.setenv('AM_GATEWAY_BOT_WORKER_TOKEN','synthetic-worker-0123456789012345')
    owner = NS(owner='ou_owner',owner_origin=lambda:{'chat':'oc_owner'},query_settings={},
               query_identity=NS(verify=lambda actor:True),ledger=NS(port=NS(disclosure=set())))
    bridge = NS(config={'app_id':'cli_synthetic'},user_allowed=lambda actor:actor=='ou_owner',
                am_self=owner,poc=NS(config=NS(tenant_key='tenant',bot_open_id='ou_bot')))
    owner.ux_collection_at_receipt=owner.ux_collection_in_chat=lambda row:False
    owner.s=NS(rows=lambda *a:[],clock=time.time)
    owner.scope=lambda row:'owner-session'
    bridge.poc._mode=lambda row:'disabled'
    bridge.poc.config.reminders_enabled=False
    bridge.poc.store=NS(rows=lambda *a:[])
    bridge.mutex=threading.RLock()
    bridge.db=NS(execute=lambda *a:NS(fetchone=lambda:None))
    bridge.group_ledger=None
    settings = {'enabled':True,'implementation':'phase1','host_factory':'integration.full_runtime:build_ports',
                'live_cutover_at':time.time()-300,'project_chats':{'P':['oc_owner']},'send_live':True,
                'control_api':{'enabled':False},'grok_bot':{'mode':'local_pull','worker_api':{'port':0,
                    'admin_token_env':'AM_GATEWAY_RECEIVER_ADMIN_TOKEN','worker_token_env':'AM_GATEWAY_BOT_WORKER_TOKEN'}},
                'agents':{name:{'enabled':False} for name in ['document_agent','project_agent','code_agent']}}
    sdk=MemorySDK()
    ingress,service=install_host(bridge,{'multi_ai_gateway':settings},sdk,tmp_path)
    calls=[]
    def runner(envelope,context):
        calls.append(context['run_id'])
        return {'response_kind':'sufficient','reply_text':'独立HTTP worker处理完成的测试回复。',
                'remaining_requirements':[],'artifacts':[]}
    server=next(server for server in service.servers if isinstance(server,ReceiverHTTPServer))
    worker=ReceiverPump(HTTPWorkerClient(server.base_url,'synthetic-worker-0123456789012345'),runner)
    try:
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            worker.step()
            rows=ingress.gateway.store.rows('SELECT verification_state FROM h1_tasks')
            if rows and rows[0]['verification_state']=='sufficient':
                break
            time.sleep(.02)
        assert rows[0]['verification_state']=='sufficient'
        assert len(calls)==len(sdk.sent)==1
        assert ingress.scheduler is service
        assert bridge.multi_ai_legacy_ownership is service.bundle.legacy_ownership
    finally:
        stopped=service.stop()
    assert stopped['stopped']
