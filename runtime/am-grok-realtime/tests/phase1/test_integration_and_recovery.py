from dataclasses import replace
import json
import threading

import pytest

from am_gateway.models import ContractError, Event
from am_gateway.phase1 import Harness, Policy
from am_gateway.phase1.contracts import validate_envelope
from integration.phase1_runtime import Phase1Ports, ReviewArea, install
from conftest import admit,receipt


def ports_for(h):
    return Phase1Ports(h.config,h.store.path,lambda p:True,
        lambda e:Policy(e.sender_id,'trusted-host',e.chat_id,(e.chat_id,),('A',),allow_reply=True),
        lambda p:('A',),lambda p:False)


def payload(mid,text):
    return {'header':{'tenant_key':'tenant'},'event':{'sender':{'sender_id':{'open_id':'owner'}},
        'message':{'message_id':mid,'chat_id':'owner-dm','chat_type':'p2p','create_time':'1000000',
                   'message_type':'text','content':json.dumps({'text':text})}}}


def test_existing_ingress_and_review_projection_without_live_activation(rig):
    h,_=rig
    ports=ports_for(h)
    ingress,scheduler=install(ports)
    # Candidate is not a second connection and does not silently start a background worker.
    assert scheduler.thread is None
    a=ingress.ingest(payload('direct','帮我处理一下'))
    assert a.claimed and a.captured
    task=ingress.gateway.route_one()
    assert ingress.review.items()[0]['task_id']==task['task_id']
    with pytest.raises(ContractError):install(ports,activate=True)
    # Even setting all four evidence strings cannot bypass the missing real adapter.
    with pytest.raises(ContractError):install(replace(ports,receiver_evidence='x',result_channel_evidence='x',
        memory_boundary_evidence='x',polling_exclusion_evidence='x'),activate=True)


def test_existing_control_remains_unclaimed(rig):
    h,_=rig
    ingress,_=install(replace(ports_for(h),legacy_control=lambda p:True))
    assert not ingress.ingest(payload('control','/doc 帮助')).claimed
    assert ingress.gateway.route_one()['disposition']=='collection_only'


def test_review_actor_comes_from_host_not_action_payload(rig):
    h,_=rig
    task=admit(h,'unclear','帮我处理一下')
    ingress,_=install(replace(ports_for(h),authenticated_actor=lambda p:'stranger'))
    forged={'event':{'action':{'value':{'kind':'harness_review','task_id':task['task_id'],
             'action':'ignore','actor':'owner'}}}}
    with pytest.raises(ContractError):ingress.card_callback(forged)
    assert ingress.recall({})['status']=='unsupported'


def test_semantic_clarification_uses_explicit_task_identity(rig):
    h,_=rig
    a=admit(h,'a','帮我处理一下');b=admit(h,'b','帮我处理附件')
    review=ReviewArea(h)
    review.apply(authenticated_actor='owner',task_id=a['task_id'],action='ignore')
    assert h.task(a['task_id'])['route_state']=='ignored'
    assert h.task(b['task_id'])['route_state']=='classification_error'


def test_context_snapshot_frozen_after_acceptance_and_policy_can_only_narrow(rig):
    h,_=rig
    task=admit(h);env=h.claim()
    receipt(h,env,'accepted',{'run_id':'r','durable_record':'receiver-row'})
    before=h.task(task['task_id'])['context_json']
    admit(h,'supplement','补充：新的客户要求')
    assert h.envelope(h.task(task['task_id']))['context']==json.loads(before)
    p=Policy.parse(task['policy_json'])
    h.update_policy(task['task_id'],replace(p,version=2,projects=()))
    with pytest.raises(ContractError):h.update_policy(task['task_id'],replace(p,version=3,source_chats=('owner-dm','other')))


def test_edit_between_route_and_claim_never_uses_stale_request(rig):
    h,_=rig
    task=admit(h)
    event=Event.from_dict(json.loads(task['request_json']))
    h.ingest(replace(event,body='把项目改成完成',version_time=1002),Policy.parse(task['policy_json']))
    # Do not wait for the route worker to see the edit.
    assert h.claim() is None
    assert h.task(task['task_id'])['dispatch_state']=='source_changed'


def test_native_revision_only_change_preserves_old_evidence(rig):
    h,_=rig
    e=Event('tenant','source','owner-dm','owner','同样文字',999,channel='bot_dm',revision='v1',version_time=999)
    p=Policy('owner','auth','owner-dm',('owner-dm',))
    h.ingest(e,p)
    h.ingest(replace(e,revision='v2',version_time=1001,attachments=({'id':'changed'},)),p)
    assert h.store.get_event({'item_id':e.item_id,'version':1}).revision=='v1'
    assert h.store.get_event({'item_id':e.item_id,'version':2}).revision=='v2'
    assert h.store.one('SELECT version FROM gw_items WHERE item_id=?',(e.item_id,))['version']==2


def test_unknown_permission_or_changed_payload_rejected_in_envelope(rig):
    h,_=rig
    admit(h);env=h.claim();validate_envelope(env)
    forged=json.loads(json.dumps(env));forged['policy']['allow_all_writes']=True
    with pytest.raises((ContractError,TypeError)):validate_envelope(forged)
    forged=json.loads(json.dumps(env));forged['payload']['raw_text']='different operation'
    with pytest.raises(ContractError):validate_envelope(forged)
    forged=json.loads(json.dumps(env));forged['schema_version']='future'
    with pytest.raises(ContractError):validate_envelope(forged)


def test_accepted_lost_response_preserves_valid_callback_not_unknown(rig):
    h,_=rig
    h.register_cutover('grok_bot:shared',evidence_ref='synthetic-isolation')
    class Receiver:
        def accept(self,env):
            receipt(h,env,'accepted',{'run_id':'r','durable_record':'saved'})
            raise TimeoutError('ACK response lost')
    h.adapters={'grok_bot':Receiver()}
    task=admit(h)
    assert h.dispatch_one()['dispatch_state']=='accepted'
    assert h.claim() is None


def test_scope_not_released_while_executor_send_is_uncertain(rig):
    h,_=rig
    admit(h,'a');admit(h,'b');env=h.claim()
    started,finish=threading.Event(),threading.Event()
    def sender(**kw):
        started.set();assert finish.wait(3)
        raise TimeoutError('platform outcome uncertain')
    thread=threading.Thread(target=lambda:h.tool_action(env['identity']['task_id'],env['execution']['generation'],
        'reply',target='owner-dm',body='answer',sender=sender))
    thread.start();assert started.wait(3)
    receipt(h,env,'ended',{'status':'succeeded','run_id':'r','stopped_evidence':'exit'})
    assert h.claim() is None
    finish.set();thread.join(3)
    assert h.claim() is None


def test_background_projection_is_bounded_extractive_and_keeps_unknown_association(rig):
    h,_=rig
    e=Event('tenant','discussion','group','owner','等待供应商反馈',999,project_ids=('A',))
    p=Policy('owner','auth','group',('group',),('A',))
    h.ingest(e,p)
    assert not h.route_one().get('task_id')
    assert h.information.project_sources()==1
    assert h.information.project_sources()==0
    snapshot=h.information.snapshot(p)
    assert snapshot['evidence_items'][0]['nature']=='feedback'
    h.ingest(replace(e,native_id='unassociated',project_ids=()),p)
    h.route_one();assert h.information.project_sources()==0


def test_lost_send_receipt_recovery_requires_exact_mapping_and_independent_read(rig):
    h,_=rig
    admit(h,'a');admit(h,'b');env=h.claim();tid=env['identity']['task_id']
    effect=h.tool_action(tid,env['execution']['generation'],'reply',target='owner-dm',body='answer',sender=lambda **kw:{})
    receipt(h,env,'ended',{'status':'succeeded','run_id':'r','stopped_evidence':'exit'})
    assert h.claim() is None
    h.record_send_receipt(task_id=tid,effect_id=effect['effect_id'],platform_id='known-output')
    assert h.claim() is None  # mapping alone is not the independent readback
    h.verify_delivery(tid,{'platform_id':'known-output','target':'owner-dm','body':'answer',
                          'sender_id':'bot','platform_time':1001})
    assert h.claim() is not None


@pytest.mark.parametrize('channel,chat',[('group','allowed-group'),('bot_dm','owner-dm')])
def test_active_entry_target_is_bound_once_before_ack(rig,channel,chat):
    h,_=rig
    task=admit(h,channel=channel,chat=chat)
    env=h.claim()
    assert env['output']['reply_target']==chat
    assert env['source']['chat_id']==chat
    assert h.claim() is None


def test_dispatch_claim_only_selects_scope_with_verified_cutover(rig):
    h,_=rig
    caps=dict(h.config.capabilities)
    caps['conversation.reply']=replace(caps['conversation.reply'],execution_scope='not-cut-over')
    h=Harness(h.store,replace(h.config,capabilities=caps))
    hello=admit(h,'hello','你好');query=admit(h,'query')
    h.register_cutover('grok_bot:shared',evidence_ref='test-scoped-exclusion')
    chosen=h.claim(require_cutover=True)
    assert chosen['identity']['task_id']==query['task_id']
    assert h.task(hello['task_id'])['dispatch_state']=='pending'


def test_supplement_edit_updates_information_without_second_business_operation(rig):
    h,_=rig
    task=admit(h,'supplement','补充：客户要求周四')
    e=Event.from_dict(json.loads(task['request_json']));p=Policy.parse(task['policy_json'])
    h.ingest(replace(e,body='补充：更正为客户要求周三',version_time=1002),p)
    h.route_one();h.information.project_sources()
    info=h.information.snapshot(p)['evidence_items']
    assert len(info)==1 and info[0]['body']=='补充：更正为客户要求周三'
    assert info[0]['nature']=='pending_requirement'
    assert h.task(task['task_id'])['business_id']==task['business_id']
