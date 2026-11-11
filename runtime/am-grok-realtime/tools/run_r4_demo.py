#!/usr/bin/env python3
"""Exercise all four worker branches with real local components and synthetic I/O.

This demonstration never contacts Feishu, a model service or a real Grok Bot.
It starts one loopback receiver, creates actual files, and uses the same durable
queue, lifecycle, approval and delivery code as the Studio integration.
"""
import argparse
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT)]

from am_gateway.models import Event, dumps
from am_gateway.phase1 import HarnessConfig, Policy
from am_gateway.phase1.agents import ArtifactStore, ProposalRepository, build_agent_runners
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.control_api import ControlAPI
from am_gateway.phase1.execution import HTTPWorkerClient, ReceiverHTTPServer, ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.service import RuntimeService
from am_gateway.store import Store
from integration.phase1_collection import LegacyOwnership
from integration.phase1_runtime import Phase1Ports, install


class SyntheticDraftModel:
    def run(self, request):
        result={'schema':'am.gateway.result.v2','job_id':request['job_id'],
                'attempt_id':request['attempt_id'],'status':'completed','evidence_refs':[]}
        if request['task_kind']=='document.process':
            result['document']={'title':'项目进展示例','format':'docx','content':'原计划周五完成。当前等待供应商反馈。实际完成仍待核验。'}
        elif request['task_kind']=='project.execute':
            result['proposal']={'project_id':'A','target':{'type':'work_item','id':'demo-item'},
                                'action':'update_fields','changes':{'status':'in_progress'},'reason':'示例明确要求'}
        else:
            result['files']=[{'path':'example.py','content':"def greeting():\n    return 'hello'\n"}]
        return result


class SyntheticFileDelivery:
    def __init__(self,harness,artifacts):self.harness,self.artifacts=harness,artifacts
    def deliver_task(self,tid):
        result=json.loads(self.harness.task(tid)['result_json'])
        for ref in result['artifacts']:
            self.artifacts.read(ref['artifact_id'],task_id=tid)
        return {'complete':True,'artifacts':result['artifacts'],'synthetic_platform':True,
                'completed_platform_time':time.time()}


def run(directory):
    directory.mkdir(parents=True,exist_ok=True)
    if (directory/'gateway.sqlite').exists():
        raise ValueError('use a fresh output directory to keep demonstration evidence unambiguous')
    store=Store(directory/'gateway.sqlite')
    receivers,adapters,caps=[],{},registry()
    mapping={'grok_bot':('project.query','conversation.reply','work_message.assist'),
             'document_agent':('document.process',),'project_agent':('project.execute',),'code_agent':('code.execute',)}
    for name,intents in mapping.items():
        receiver=DurableReceiver(store,namespace=name,intents=intents)
        adapter=LocalReceiverAdapter(receiver)
        receivers.append(receiver);adapters[name]=adapter
        for intent in intents:caps[intent]=adapter.capability(intent)
    cfg=HarnessConfig('demo-tenant','demo-bot','demo-owner','demo-owner-dm',capabilities=caps)
    policy=Policy('demo-owner','synthetic-owner-policy','demo-owner-dm',('demo-owner-dm',),('A',),
                  allow_reply=True,allow_supplement=True)
    ports=Phase1Ports(cfg,store.path,lambda p:True,lambda e:policy,lambda p:('A',),lambda p:False,adapters=adapters)
    ingress,_=install(ports)
    h=ingress.gateway
    models={name:SyntheticDraftModel() for name in mapping if name!='grok_bot'}
    runners=build_agent_runners(artifact_root=directory/'artifacts',store=store,owner_id=cfg.owner_id,models=models)
    pumps=[ReceiverPump(r,runners[r.namespace],harness=h) for r in receivers if r.namespace!='grok_bot']
    bot_receiver=receivers[0]
    server=ReceiverHTTPServer(bot_receiver,admin_token='synthetic-administrator-token',
                              worker_token='synthetic-worker-token',harness=h)
    server.start()
    client=HTTPWorkerClient(server.base_url,'synthetic-worker-token')
    def dialogue(envelope,context):
        return {'response_kind':'sufficient','reply_text':'原计划周五完成；当前等待供应商反馈。',
                'remaining_requirements':[],'artifacts':[]}
    pumps.append(ReceiverPump(client,dialogue,worker_id='synthetic-external-bot'))
    sent={}
    def sender(**request):
        mid='synthetic-'+request['idempotency_key']
        sent[mid]={**request,'platform_id':mid,'platform_time':time.time()}
        return {'platform_id':mid}
    def readback(tid):
        effect=h.store.one('SELECT * FROM h1_effects WHERE task_id=?',(tid,))
        record=sent[effect['platform_id']]
        return {'platform_id':record['platform_id'],'target':record['target'],'body':record['body'],
                'sender_id':cfg.bot_id,'platform_time':record['platform_time']}
    artifacts=ArtifactStore(directory/'artifacts')
    service=RuntimeService(ingress,receivers=receivers,pumps=pumps,sender=sender,readback=readback,
        artifact_delivery=SyntheticFileDelivery(h,artifacts),ownership=LegacyOwnership(h),auto_scopes=True)
    try:
        for index,text in enumerate(('A项目进展怎么样了？','请生成项目报告文档','更新项目状态','编写Python代码')):
            h.ingest(Event(cfg.tenant_id,'input-'+str(index),cfg.owner_chat_id,cfg.owner_id,text,
                           time.time(),channel='bot_dm',project_ids=('A',)),policy)
        for _ in range(8):service.step()
        proposals=ProposalRepository(store,cfg.owner_id)
        applied=[]
        def executor(proposal,*,idempotency_key):
            applied.append(idempotency_key)
            target=directory/'synthetic-business-state.json'
            target.write_text(dumps(proposal['payload']))
            assert json.loads(target.read_text())==proposal['payload']
            return {'status':'applied','evidence_ref':'synthetic-local-file-readback'}
        api=ControlAPI(service,token='synthetic-operator-token-24-chars',proposals=proposals,executor=executor)
        p=proposals.list()[0]
        approved=api.handle('POST','/v1/proposals/'+p['proposal_id']+'/confirm',
                            {'version':p['version'],'digest':p['digest']})
        tasks=h.store.rows('SELECT task_id,intent,execution_state,delivery_state,verification_state FROM h1_tasks ORDER BY created')
        assert len(tasks)==4 and all(t['execution_state']=='succeeded' and t['verification_state']=='sufficient' for t in tasks)
        outcome={'mode':'synthetic_external_io','actual_components':['SQLite','Harness','loopback_HTTP',
                 'receiver','runner','file_generation','proposal_confirmation','delivery','readback'],
                 'not_exercised':['real_Grok_Bot','real_model','real_Feishu','mobile','human_acceptance'],
                 'tasks':tasks,'unique_text_sends':len(sent),'business_apply_calls':len(applied),
                 'proposal_status':approved['status'],'metrics':service.metrics(),
                 'artifacts':[a for t in h.store.rows('SELECT result_json FROM h1_tasks')
                              for a in json.loads(t['result_json']).get('artifacts',[])]}
        (directory/'demo-result.json').write_text(json.dumps(outcome,ensure_ascii=False,indent=2)+'\n')
        return outcome
    finally:server.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    outcome=run(args.output.resolve())
    print(json.dumps({'mode':outcome['mode'],'tasks':len(outcome['tasks']),
                      'artifacts':len(outcome['artifacts']),'business_apply_calls':outcome['business_apply_calls']},ensure_ascii=False))
