import json
import time
from copy import deepcopy
import pytest

from am_gateway.models import ContractError
from integration.meegle_executor import MeegleOperationBridge


@pytest.fixture
def rig(tmp_path):
    state={'value':'original','writes':0,'identity':'owner','timeout':False}
    grant={'project_id':'test-project','project_key':'space','target_id':'100','target_name':'测试项目',
           'work_item_type':'project-type','before':{'description':'original'},'after':{'description':'original\nprobe'},
           'field_types':{'description':'multi-text'},'authorization_ref':'explicit-test-authorization','expires_at':time.time()+60}
    config={'host':'project.feishu.cn','user_key':'owner','grants':[grant]}
    def invoke(args):
        if args[:2]==['auth','status']:return {'authenticated':True,'host':'project.feishu.cn'}
        if args[:2]==['user','me']:return {'user_key':state['identity']}
        if args[:2]==['workitem','get']:
            return {'work_item_attribute':{'work_item_id':'100','work_item_name':'测试项目','owned_project':{'key':'space'},
                    'work_item_type':{'key':'project-type'}},'work_item_fields':[{'key':'description','value':state['value']}]}
        if args[:2]==['workitem','meta-fields']:return {'list':[{'field_key':'description','field_type':'multi-text'}]}
        assert args[:2]==['workitem','update']
        state['writes']+=1
        fields=json.loads(args[args.index('--fields')+1]);assert fields[0]['field_key']=='description'
        state['value']=fields[0]['field_value']
        if state['timeout']:raise TimeoutError('response lost after platform applied')
        return {}
    bridge=MeegleOperationBridge(config,tmp_path/'operations.sqlite',invoke=invoke)
    request={'operation':'apply','schema':'am.approved.operation.v1','idempotency_key':'effect_test',
             'payload':{'project_id':'test-project','target':{'type':'work_item','id':'100'},
                        'action':'update_fields','changes':{'description':'original\nprobe'}}}
    return bridge,request,state


def test_write_and_independent_readback_are_exact_and_duplicate_is_not_replayed(rig):
    bridge,request,state=rig
    assert bridge.handle(request)['status']=='submitted'
    assert bridge.handle(request)['status']=='unknown'
    assert state['writes']==1
    read=bridge.handle({**request,'operation':'readback'})
    assert read['status']=='applied' and read['changes']==request['payload']['changes']
    assert read['evidence_ref']
    assert bridge.handle(request)['status']=='applied' and state['writes']==1


def test_lost_write_response_is_read_back_without_repeating_effect(rig):
    bridge,request,state=rig;state['timeout']=True
    assert bridge.handle(request)['status']=='unknown'
    assert bridge.handle({**request,'operation':'readback'})['status']=='applied'
    assert state['writes']==1


def test_current_value_change_prevents_old_preview_write(rig):
    bridge,request,state=rig;state['value']='parallel human edit'
    assert bridge.handle(request)['status']=='not_applied'
    assert state['writes']==0 and state['value']=='parallel human edit'


@pytest.mark.parametrize('change',['target','field','actor','payload'])
def test_unbound_target_field_actor_and_changed_operation_cannot_write(rig,change):
    bridge,request,state=rig;request=deepcopy(request)
    if change=='target':request['payload']['target']['id']='other'
    elif change=='field':request['payload']['changes']={'status':'completed'}
    elif change=='actor':state['identity']='other'
    else:
        assert bridge.handle(request)['status']=='submitted'
        request['payload']['reason']='different frozen payload'
    with pytest.raises(ContractError):bridge.handle(request)
    assert state['writes']==(1 if change=='payload' else 0)
