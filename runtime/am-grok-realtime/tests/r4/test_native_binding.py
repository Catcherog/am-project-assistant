"""Native first installation and existing model coordinator integration."""
import json
import time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from am_gateway.models import ContractError
from integration.studio_phase1_factory import build_phase1_bundle


def host(provider=None):
    owner = NS(owner='ou_owner', owner_origin=lambda: {'chat': 'oc_owner'},
               query_settings={}, query_identity=NS(verify=lambda actor: None),
               ledger=NS(port=NS(disclosure={})))
    return NS(config={'app_id': 'cli_synthetic'}, am_self=owner,
              user_allowed=lambda actor: actor == 'ou_owner',
              poc=NS(config=NS(tenant_key='synthetic', bot_open_id='ou_bot'),
                     gateway=NS(provider=provider)))


def test_first_null_cutover_is_persisted_and_does_not_replay_history(tmp_path):
    bridge = host()
    settings = {'phase1_gateway': {'live_cutover_at': None}}
    start = time.time()
    first = build_phase1_bundle(bridge, settings, Mock(), tmp_path)
    cutoff = first.authority.collection_config.live_cutover_at
    assert start <= cutoff <= time.time()
    second = build_phase1_bundle(bridge, settings, Mock(), tmp_path)
    assert second.authority.collection_config.live_cutover_at == cutoff
    with pytest.raises(ContractError, match='persisted_cutover_mismatch'):
        build_phase1_bundle(bridge, {'phase1_gateway': {'live_cutover_at': cutoff - 1}}, Mock(), tmp_path)


def request():
    return {'schema': 'am.gateway.job.v2', 'job_id': 'native-job', 'attempt_id': 'native-attempt',
            'task_kind': 'document.process', 'worker': 'document_agent', 'session_key': 'synthetic:owner',
            'conversation': {'tenant_id': 'synthetic', 'chat_id': 'oc_owner', 'actor_id': 'ou_owner'},
            'context': {}, 'output_mode': 'internal', 'deadline_at': time.time() + 30,
            'instructions': 'Produce an unapproved document draft.', 'request': {'text': 'synthetic'}}


def test_host_model_binding_uses_existing_coordinator_and_validates_identity(tmp_path):
    from integration.studio_agent_models import bind_host_models
    data = {'schema': 'am.gateway.result.v2', 'job_id': 'native-job', 'attempt_id': 'native-attempt',
            'status': 'completed', 'evidence_refs': [], 'document': {'content': 'draft'}}
    provider = NS(complete=Mock(return_value=NS(text=json.dumps(data))))
    models = bind_host_models(host(provider), {'document_agent': {'model': {'kind': 'host_qwen'}}})
    assert models['document_agent'].run(request()) == data
    args, kwargs = provider.complete.call_args
    assert 'unapproved document draft' in args[0]
    assert 0 < kwargs['timeout'] <= 30
    assert kwargs['native_chat'] is True and kwargs['max_tokens'] == 4096
    assert len(kwargs['run_id']) == 32
    data['job_id'] = 'unrelated-task'
    provider.complete.return_value = NS(text=json.dumps(data))
    with pytest.raises(Exception):
        models['document_agent'].run(request())


def test_missing_host_provider_is_rejected_before_start():
    from integration.studio_agent_models import bind_host_models
    with pytest.raises(ContractError, match='existing_host_qwen_provider_required'):
        bind_host_models(host(None), {'document_agent': {'model': {'kind': 'host_qwen'}}})


def test_history_system_notice_advances_with_audit_without_fabricated_actor(tmp_path):
    from am_gateway.collectors import SourceSpec
    from integration.phase1_runtime import install
    sdk=Mock()
    sdk.request.return_value=NS(raw=NS(content=json.dumps({'code':0,'data':{'items':[
        {'message_id':'om_system','chat_id':'oc_group','msg_type':'system',
         'sender':{'id':'','id_type':'','sender_type':'','tenant_key':''}}
    ],'has_more':True,'page_token':'next-page'}}).encode()))
    bridge=host();bridge.am_self.ledger.port.disclosure={'oc_group':['P']}
    bundle=build_phase1_bundle(bridge,{'phase1_gateway':{'group_chat_ids':['oc_group'],
        'project_chats':{'P':['oc_group']}}},sdk,tmp_path)
    ingress,_=install(bundle.ports);collector=bundle.bind(ingress).collectors
    source=SourceSpec('synthetic','oc_group',identity='am_bot',project_ids=('P',))
    assert collector.collect_one(source.source_id,'history')['status']=='page'
    assert not ingress.gateway.store.rows('SELECT * FROM gw_items')
    assert not ingress.gateway.store.rows('SELECT * FROM h1_tasks')
    assert ingress.gateway.store.one("SELECT page_token FROM gw_source_scans WHERE source_id=? AND lane='history'",(source.source_id,))['page_token']=='next-page'
    assert ingress.gateway.store.one("SELECT detail FROM gw_audit WHERE kind='platform_system_excluded'")


@pytest.mark.parametrize('app_id,accepted',[('cli_synthetic',True),('cli_other',False)])
def test_exact_get_maps_only_verified_self_bot_mentions(tmp_path,app_id,accepted):
    from am_gateway.collectors import SourceSpec
    from am_gateway.feishu_snapshot import normalize_feishu_snapshot
    from integration.studio_host import ScopedReadTransport
    from am_gateway.phase1 import HarnessConfig
    now=str(int(time.time()*1000))
    raw={'message_id':'om_known','chat_id':'oc_group','create_time':now,'update_time':now,
         'msg_type':'text','deleted':False,'body':{'content':json.dumps({'text':'@_user_1 当前进度'})},
         'sender':{'id':'ou_owner','id_type':'open_id','sender_type':'user','tenant_key':'synthetic'},
         'mentions':[{'id':app_id,'id_type':'app_id','key':'@_user_1'}]}
    sdk=Mock();sdk.request.return_value=NS(raw=NS(content=json.dumps({'code':0,'data':{'items':[raw]}}).encode()))
    reader=ScopedReadTransport(sdk,['oc_group'],app_id='cli_synthetic',bot_id='ou_bot',tenant_id='synthetic')
    source=SourceSpec('synthetic','oc_group',identity='am_bot')
    message=reader.read_message(source,'om_known')
    cfg=HarnessConfig('synthetic','ou_bot','ou_owner','oc_owner')
    if accepted:
        event=normalize_feishu_snapshot(message,cfg,source)
        assert event.mentions==('ou_bot',)
        raw['mentions'][0].update(id='ou_bot',id_type='open_id')
        assert event.revision==normalize_feishu_snapshot(raw,cfg,source).revision
    else:
        with pytest.raises(ContractError,match='mention id type'):
            normalize_feishu_snapshot(message,cfg,source)
