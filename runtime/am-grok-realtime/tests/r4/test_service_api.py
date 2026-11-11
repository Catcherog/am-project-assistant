from dataclasses import replace
import json
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from am_gateway.models import Event, ContractError
from am_gateway.phase1 import HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.control_api import ControlAPI
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store
from integration.phase1_collection import LegacyOwnership
from integration.phase1_runtime import Phase1Ports, install


def rig(tmp_path, *, sender_unknown=False):
    store = Store(tmp_path/'gateway.sqlite')
    receiver = DurableReceiver(store)
    adapter = LocalReceiverAdapter(receiver)
    caps = registry()
    for intent in receiver.intents:
        caps[intent] = adapter.capability(intent)
    cfg = HarnessConfig('tenant','bot','owner','owner-dm',capabilities=caps)
    policy = Policy('owner','trusted-host','owner-dm',('owner-dm',),('A',),allow_reply=True,allow_supplement=True)
    ports = Phase1Ports(cfg,store.path,lambda p:True,lambda e:policy,lambda p:('A',),lambda p:False,
                        adapters={'grok_bot':adapter})
    ingress,_=install(ports)
    h=ingress.gateway
    seen=[]
    def runner(envelope,context):
        seen.append((envelope,context))
        return {'response_kind':'sufficient','reply_text':'计划仍为周五，当前待供应商反馈。',
                'remaining_requirements':[],'artifacts':[]}
    pump=ReceiverPump(receiver,runner,harness=h)
    sent=[]
    def sender(**kw):
        sent.append(kw)
        return {} if sender_unknown else {'platform_id':'out-'+kw['idempotency_key']}
    def readback(task_id):
        e=h.store.one('SELECT * FROM h1_effects WHERE task_id=?',(task_id,))
        return {'platform_id':e['platform_id'],'target':e['target'],'body':e['body'],
                'sender_id':'bot','platform_time':time.time()}
    service=RuntimeService(ingress,receivers=[receiver],pumps=[pump],sender=sender,readback=readback,
                           ownership=LegacyOwnership(h),auto_scopes=True)
    ingress.scheduler=service
    return h,service,policy,seen,sent


def submit(h,policy,mid='message',text='A 项目进展怎么样了？'):
    e=Event('tenant',mid,'owner-dm','owner',text,time.time(),channel='bot_dm',project_ids=('A',))
    h.ingest(e,policy)
    return e


def test_complete_service_step_routes_executes_sends_and_independently_reads(tmp_path):
    h,s,p,seen,sent=rig(tmp_path)
    submit(h,p)
    s.step()
    task=h.store.rows('SELECT * FROM h1_tasks')[0]
    assert task['execution_state']=='succeeded'
    assert task['delivery_state']=='delivered' and task['verification_state']=='sufficient'
    assert task['scope']=='grok_bot:tenant:owner-dm:owner'
    assert len(seen)==len(sent)==1
    s.step();s.step()
    assert len(sent)==1
    submit(h,p,'second')
    s.step()
    assert len(seen[1][1]['history'])==2


def test_unknown_send_survives_runtime_reconstruction_without_replay(tmp_path):
    h,s,p,seen,sent=rig(tmp_path,sender_unknown=True)
    submit(h,p)
    s.step()
    assert h.store.rows('SELECT delivery_state FROM h1_tasks')[0]['delivery_state']=='unknown'
    rebuilt=RuntimeService(s.ingress,receivers=s.receivers,pumps=s.pumps,sender=s.sender,readback=s.readback)
    rebuilt.delivery_once()
    assert len(sent)==1
    assert h.store.rows('SELECT * FROM h4_receiver_memory')==[]


def test_live_service_threads_and_stop_use_same_original_ingress(tmp_path):
    h,s,p,seen,sent=rig(tmp_path)
    s.start()
    try:
        submit(h,p)
        s.notify()
        deadline=time.monotonic()+4
        while time.monotonic()<deadline and not sent:
            time.sleep(.03)
        assert len(sent)==1
        assert s.health()['service_running']
    finally:
        assert s.stop()['stopped']


def test_control_http_auth_and_real_review_mutation(tmp_path):
    h,s,p,_,_=rig(tmp_path)
    submit(h,p,text='帮我处理一下')
    s.step()
    task=h.store.rows('SELECT * FROM h1_tasks')[0]
    api=ControlAPI(s,token='operator-secret-'+'a'*30)
    api.start()
    try:
        with pytest.raises(HTTPError) as denied:
            urlopen(api.url+'/v1/tasks',timeout=3)
        assert denied.value.code==401
        headers={'Authorization':'Bearer '+api.token,'Content-Type':'application/json'}
        data=json.load(urlopen(Request(api.url+'/v1/tasks',headers=headers),timeout=3))
        assert data['tasks'][0]['route_state']=='classification_error'
        request=Request(api.url+'/v1/review',headers=headers,data=json.dumps({
            'task_id':task['task_id'],'action':'ignore','actor':'untrusted-body-value'}).encode())
        assert json.load(urlopen(request,timeout=3))['route_state']=='ignored'
        page=urlopen(api.url+'/',timeout=3).read().decode()
        assert 'AM · 任务与待审' in page
    finally:
        api.stop()


def test_activation_requires_real_assembly_not_evidence_flags(tmp_path):
    h,s,p,_,_=rig(tmp_path)
    ports=s.ingress.ports
    with pytest.raises(ContractError):
        install(ports,activate=True)
    ports.runtime_factory=lambda supplied:(s.ingress,s)
    try:
        actual,runtime=install(ports,activate=True)
        assert actual is s.ingress and runtime is s and s.started
    finally:
        s.stop()


def test_scopes_for_different_chats_do_not_share_serial_execution_lock(tmp_path):
    h,s,p,_,_=rig(tmp_path)
    submit(h,p)
    other=Event('tenant','other','group','owner','A项目进展怎么样了？',time.time(),
                channel='group',mentions=('bot',),project_ids=('A',))
    h.ingest(other,replace(p,reply_target='group',source_chats=('group',)))
    h.route_one();h.route_one()
    s._bind_ready_scopes()
    one=h.claim(require_cutover=True)
    two=h.claim(require_cutover=True)
    assert one and two and one['execution']['scope']!=two['execution']['scope']


def test_dispatch_pause_honors_existing_persisted_cutover(tmp_path):
    h,s,p,_,_=rig(tmp_path)
    submit(h,p)
    h.route_one()
    s._bind_ready_scopes()
    s.dispatch_enabled=False
    assert s.dispatch_once() is None
    assert h.store.rows('SELECT dispatch_state FROM h1_tasks')[0]['dispatch_state']=='pending'


def test_recorded_cancel_without_effect_releases_queue_capacity(tmp_path):
    h,s,p,_,_=rig(tmp_path)
    h.config=replace(h.config,queue_limit=1)
    submit(h,p)
    h.route_one();s.dispatch_once()
    task=h.store.rows('SELECT * FROM h1_tasks')[0]
    h.request_cancel(task['task_id']);s.receipts_once()
    assert h.task(task['task_id'])['execution_state']=='cancelled'
    submit(h,p,'next')
    assert h.route_one()['route_state']=='resolved'


def test_full_factory_can_load_into_existing_studio_hook_without_starting(tmp_path,monkeypatch):
    import integration.full_runtime as full
    h,s,p,_,_=rig(tmp_path)
    class Bundle:
        ports=s.ingress.ports
        sender=None
        readback=None
        legacy_ownership=None
        authority=None
        readers=None
        def bind(self,ingress):
            self.legacy_ownership=LegacyOwnership(ingress.gateway)
            return None
    bundle=Bundle()
    monkeypatch.setattr(full,'build_phase1_bundle',lambda *a,**kw:bundle)
    class Bridge:pass
    settings={'phase1_gateway':{'grok_bot':{'mode':'disabled'},'control_api':{'enabled':False},
              'send_live':False},'multi_ai_gateway':{'enabled':False}}
    ports=full.build_ports(Bridge(),settings,object(),tmp_path)
    assert callable(ports.runtime_factory) and not s.started
    ingress,service=ports.runtime_factory(ports)
    assert not service.started
    assert len(service.pumps)==3
    assert service.harness.config.capabilities['document.process'].available
    assert not service.harness.config.capabilities['conversation.reply'].available
