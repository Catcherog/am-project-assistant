"""Native binding boundaries with the installed SDK and synthetic events."""
from pathlib import Path
from types import SimpleNamespace as NS
import json
import re
import sqlite3
import sys
import time
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'local-qwen3.8' / 'feishu-bridge'))
sys.path.insert(0, str(ROOT / 'FS-AGENT-GROK-POC-20261017-01_R1_Studio' / 'src'))

from am_gateway.models import Config, Event, dumps
from am_gateway.store import Store
from am_gateway.core import Gateway
from am_gateway.collectors import FeishuMessageAdapter, SourceSpec, ScanRequest
from integration.am_multi_ai_host import ExistingSender
from integration.studio_host import (BindingError, HostScope, StudioSender, ScopedReadTransport,
                                     artifact_resolver, cutover_for, CoordinatedQwen, build_ports,
                                     ApprovedGroupDiscovery, legacy_control)
from am_gateway.discovery import DiscoverySpec
from am_gateway_r2_host import install


def config():
    return Config('tenant', 'ou_bot', 'ou_owner', 'oc_owner', 0)


def event(**changes):
    message = {'message_id': 'om_test', 'chat_id': 'oc_group', 'chat_type': 'group',
               'message_type': 'text', 'content': '{"text":"合成消息"}',
               'create_time': str(int(time.time() * 1000))}
    message.update(changes)
    return {'header': {'app_id': 'cli_app', 'tenant_key': 'tenant', 'event_type': 'im.message.receive_v1'},
            'event': {'sender': {'sender_type': 'user', 'sender_id': {'open_id': 'ou_owner'},
                                  'tenant_key': 'tenant'}, 'message': message}}


def bridge():
    return NS(config={'app_id': 'cli_app'}, user_allowed=lambda u: u.startswith('ou_'),
              am_self=NS(query_settings={'identity_cache_seconds': 600},
                         query_identity=NS(verify=Mock()), ledger=NS(port=NS(guard=Mock()))))


def action(aid='final', **changes):
    value = {'action_id': aid, 'action_type': 'message', 'body': '合成回复', 'payload_json': '{}',
             'target_json': dumps({'chat_id': 'oc_group', 'actor_id': 'ou_owner', 'channel': 'group',
                                   'thread_id': 'omt_thread', 'reply_to_message_id': 'om_test'})}
    value.update(changes)
    return value


def sdk():
    reply = Mock(return_value=NS(success=lambda: True, data=NS(message_id='om_sent')))
    create = Mock(return_value=NS(success=lambda: True, data=NS(message_id='om_created')))
    get = Mock()
    return NS(im=NS(v1=NS(message=NS(reply=reply, create=create, get=get))))


def test_disabled_loader_does_not_touch_host(tmp_path):
    assert install(None, {}, None, tmp_path) == (None, None)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('old', ['grok_routing', 'grok_realtime'])
def test_dual_route_activation_rejected(tmp_path, old):
    with pytest.raises(RuntimeError, match='OLD_GROK_ROUTE'):
        install(None, {'multi_ai_gateway': {'enabled': True}, old: {'enabled': True}}, None, tmp_path)


def test_factory_requires_real_bot_and_memory_evidence(tmp_path):
    b = bridge(); b.poc = NS()
    with pytest.raises(BindingError, match='grok_bot_endpoint'):
        build_ports(b, {'multi_ai_gateway': {}, 'am_self': {'ledger_write': False}}, None, tmp_path)


def test_ingress_is_local_and_scope_restricted():
    b = bridge(); scope = HostScope(b, config(), ['oc_group'])
    assert scope.verified(event())
    assert not scope.verified(event(chat_id='oc_unapproved'))
    forged = event(); forged['header']['app_id'] = 'cli_other'
    assert not scope.verified(forged)
    forged = event(); forged['event']['sender']['tenant_key'] = 'other'
    assert not scope.verified(forged)
    assert not scope.verified(event(create_time='0'))
    b.am_self.ledger.port.guard.assert_not_called()
    b.am_self.query_identity.verify.assert_not_called()


def test_dispatch_reuses_identity_lease_and_honors_revocation():
    b = bridge(); scope = HostScope(b, config(), ['oc_group'])
    conv = {'actor_id': 'ou_owner', 'chat_id': 'oc_owner', 'channel': 'bot_dm'}
    scope.authorize(conv)
    b.am_self.query_identity.verify.assert_called_once_with('ou_owner')
    b.am_self.query_settings['denied_users'] = ['ou_owner']
    with pytest.raises(BindingError, match='actor_not_authorized'):
        scope.authorize(conv)


def test_history_bot_app_id_cannot_become_inbound_dialogue(tmp_path):
    raw = {'code': 0, 'data': {'has_more': False, 'items': [{
        'message_id': 'om_old_bot', 'chat_id': 'oc_owner', 'create_time': str(int(time.time()*1000)),
        'msg_type': 'text', 'body': {'content': '{"text":"历史机器人回复"}'},
        'sender': {'id': 'cli_app', 'id_type': 'app_id', 'sender_type': 'app', 'tenant_key': 'tenant'}}]}}
    client = NS(request=Mock(return_value=NS(raw=NS(content=json.dumps(raw).encode()))))
    read = ScopedReadTransport(client, ['oc_owner'], app_id='cli_app', bot_id='ou_bot', tenant_id='tenant')
    adapter = FeishuMessageAdapter(read, bot_id='ou_bot')
    page = adapter.read_page(SourceSpec('tenant', 'oc_owner', channel='bot_dm', identity='am_bot'),
                             ScanRequest('live', 0, time.time()))
    assert page.events[0].is_self and page.events[0].direction == 'outbound'
    gw = Gateway(Store(str(tmp_path/'gateway.db')), config())
    gw.ingest(page.events[0]); gw.route()
    assert gw.store.rows('SELECT * FROM gw_jobs') == []
    assert read.normalized_self_messages == 1


@pytest.mark.parametrize('url,params,identity', [
    ('https://wrong.example/open-apis/im/v1/messages', {'container_id_type':'chat','container_id':'oc_group'}, 'am_bot'),
    ('https://open.feishu.cn/open-apis/im/v1/messages', {'container_id_type':'chat','container_id':'oc_other'}, 'am_bot'),
    ('https://open.feishu.cn/open-apis/im/v1/messages', {'container_id_type':'thread','container_id':'omt_unknown'}, 'am_bot'),
    ('https://open.feishu.cn/open-apis/im/v1/messages', {'container_id_type':'chat','container_id':'oc_group'}, 'user'),
])
def test_reader_rejects_unknown_identity_origin_and_scope(url, params, identity):
    client = NS(request=Mock())
    read = ScopedReadTransport(client, ['oc_group'], app_id='cli_app', bot_id='ou_bot', tenant_id='tenant')
    with pytest.raises(BindingError): read(url, params, identity)
    client.request.assert_not_called()


def test_closed_sender_never_calls_platform(tmp_path):
    client = sdk(); scope = HostScope(bridge(), config(), ['oc_group'])
    sender = StudioSender(client, scope, tmp_path/'send.db')
    assert sender.send(action()).detail == 'send_live_disabled'
    client.im.v1.message.reply.assert_not_called()


def test_native_send_uses_per_action_uuid_and_original_thread(tmp_path):
    client = sdk(); scope = HostScope(bridge(), config(), ['oc_group'])
    sender = StudioSender(client, scope, tmp_path/'send.db', True)
    a, b = action('ack'), action('final')
    assert sender.send(a).status == sender.send(b).status == 'sent'
    calls = client.im.v1.message.reply.call_args_list
    assert calls[0].args[0].request_body.uuid != calls[1].args[0].request_body.uuid
    assert calls[1].args[0].request_body.uuid == ExistingSender.send_uuid(b)
    assert calls[1].args[0].message_id == 'om_test'
    assert calls[1].args[0].request_body.reply_in_thread is True


def test_uncertain_send_is_not_repeated(tmp_path):
    client = sdk(); client.im.v1.message.reply.side_effect = TimeoutError()
    sender = StudioSender(client, HostScope(bridge(), config(), ['oc_group']), tmp_path/'send.db', True)
    assert sender.send(action()).status == 'unknown'
    assert sender.send(action()).status == 'unknown'
    assert client.im.v1.message.reply.call_count == 1


def test_artifact_delivery_cannot_be_reported_as_sent(tmp_path):
    client = sdk()
    sender = StudioSender(client, HostScope(bridge(), config(), ['oc_group']), tmp_path/'send.db', True)
    result = sender.send(action(payload_json=dumps({'artifacts':[{'name':'x.docx','uri':'file:///x.docx'}]})))
    assert result.status == 'failed' and result.detail == 'artifact_delivery_not_bound'
    client.im.v1.message.reply.assert_not_called()


def test_readback_checks_author_target_parent_and_body(tmp_path):
    client = sdk(); scope = HostScope(bridge(), config(), ['oc_group'])
    sender = StudioSender(client, scope, tmp_path/'send.db', True)
    sender.send(action())
    msg = NS(message_id='om_sent', chat_id='oc_group', deleted=False, msg_type='text', thread_id='omt_thread', parent_id='om_test',
             sender=NS(open_bot_id=None, id='cli_app', sender_type='app'), body=NS(content='{"text":"合成回复"}'))
    client.im.v1.message.get.return_value = NS(success=lambda:True, data=NS(items=[msg]))
    assert sender.lookup(action()).status == 'sent'
    for name, value in [('chat_id','oc_other'), ('parent_id','om_other'), ('thread_id','omt_other')]:
        old=getattr(msg,name);setattr(msg,name,value)
        assert sender.lookup(action()).status == 'unknown'
        setattr(msg,name,old)
    msg.body.content='{"text":"别的内容"}'
    assert sender.lookup(action()).status == 'unknown'


def test_artifacts_stay_in_output_root(tmp_path):
    allowed=tmp_path/'outputs';allowed.mkdir();file=allowed/'report.docx';file.write_bytes(b'synthetic')
    outside=tmp_path/'private.docx';outside.write_bytes(b'not an output')
    resolve=artifact_resolver(allowed)
    assert resolve({'uri':file.as_uri()})['delivery']=='local_only'
    with pytest.raises(BindingError):resolve({'uri':outside.as_uri()})
    link=allowed/'link.docx';link.symlink_to(outside)
    with pytest.raises(BindingError):resolve({'uri':link.as_uri()})
    with pytest.raises(BindingError):resolve({'uri':'https://example.com/report.docx'})


def test_cutover_is_reused_without_clock_reset(tmp_path):
    path=tmp_path/'gateway.db'
    Gateway(Store(str(path)), config())
    assert cutover_for(path,None)==0
    with pytest.raises(BindingError,match='mismatch'):cutover_for(path,10)
    with pytest.raises(BindingError,match='explicit'):cutover_for(tmp_path/'new.db',None)


def test_qwen_uses_existing_coordinator():
    request={'schema':'am.gateway.job.v2','job_id':'j','attempt_id':'a','worker':'qwen',
             'task_kind':'tag_candidates','session_key':'s','conversation':{},'context':{},
             'output_mode':'internal','deadline_at':time.time()+20}
    result={'schema':'am.gateway.result.v2','job_id':'j','attempt_id':'a','status':'completed','candidates':[]}
    provider=NS(complete=Mock(return_value=NS(text=json.dumps(result))))
    assert CoordinatedQwen(provider).run(request)==result
    assert re.fullmatch(r'[a-f0-9]{32}', provider.complete.call_args.kwargs['run_id'])


def test_group_directory_does_not_expand_approved_sources():
    raw={'code':0,'data':{'items':[{'chat_id':'oc_group','name':'Allowed'},
                                  {'chat_id':'oc_private','name':'Outside scope'}], 'has_more':False}}
    discovery=ApprovedGroupDiscovery(lambda *_:raw,['oc_group'],{'project-p':['oc_group']})
    page=discovery.discover_page(DiscoverySpec('tenant','groups','approved_groups',scope='approved_joined_groups'),'')
    assert [s.native_id for s in page.sources]==['oc_group']
    assert page.sources[0].project_ids==('project-p',)


def legacy_bridge():
    b=bridge();b.mutex=__import__('threading').RLock();b.db=sqlite3.connect(':memory:')
    b.db.execute('CREATE TABLE inbox(id TEXT PRIMARY KEY)')
    b.poc=NS(store=NS(rows=Mock(return_value=[])), _mode=lambda _: 'chat',config=NS(reminders_enabled=False))
    b.group_ledger=None
    b.am_self.s=NS(rows=Mock(return_value=[]),clock=time.time)
    b.am_self.scope=lambda row: dumps([row['tenant'],row['chat'],row['user']])
    b.am_self.ux_collection_at_receipt=Mock(return_value=None)
    b.am_self.ux_collection_in_chat=Mock(return_value=None)
    return b


@pytest.mark.parametrize('text',['/doc 帮助','停止当前任务','我的跟进','项目查询'])
def test_existing_control_paths_remain_owned_by_am(text):
    b=legacy_bridge();p=event(chat_type='p2p',chat_id='oc_owner',content=dumps({'text':text}))
    assert legacy_control(b,config(),p)
    b.db.close()


def test_ordinary_chat_is_not_claimed_by_legacy_catch_all():
    b=legacy_bridge();p=event(chat_type='p2p',chat_id='oc_owner',content=dumps({'text':'解释一下齿轮的原理'}))
    assert not legacy_control(b,config(),p)
    b.am_self.ux_collection_at_receipt.return_value={'token':'synthetic-editor'}
    assert legacy_control(b,config(),p)
    b.db.close()


def test_active_original_document_session_is_preserved():
    b=legacy_bridge();b.poc._mode=lambda _: 'poc'
    p=event(chat_type='p2p',chat_id='oc_owner',content=dumps({'text':'请采用第二稿'}))
    assert legacy_control(b,config(),p)
    b.db.close()


def test_old_group_message_is_not_reowned_by_r2():
    b=legacy_bridge();db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE ledger_inbox(mid TEXT,chat TEXT,user TEXT,answer_mid TEXT)')
    db.execute("INSERT INTO ledger_inbox VALUES('om_test','oc_group','ou_owner','om_old_reply')")
    b.group_ledger=NS(mutex=__import__('threading').RLock(),db=db)
    assert legacy_control(b,config(),event())
    b.db.close();db.close()


def test_factory_attaches_collectors_to_same_gateway_store(tmp_path):
    b=bridge()
    b.poc=NS(config=NS(tenant_key='tenant',bot_open_id='ou_bot'),gateway=NS(provider=Mock()))
    b.am_self.owner='ou_owner';b.am_self.owner_origin=lambda:{'chat':'oc_owner'}
    b.am_self.ledger.port.disclosure={'oc_group':['project-p']}
    b.information=None
    settings={'enabled':True,'send_live':True,'live_cutover_at':0,'group_chat_ids':['oc_group'],
              'project_chats':{'project-p':['oc_group']},
              'grok_bot':{'url':'http://127.0.0.1:12345/test-only','native_memory_verified':True},
              'grok_cli':{'argv':[sys.executable,'-c','print(0)'],'isolation_verified':True}}
    ports=build_ports(b,{'multi_ai_gateway':settings,'am_self':{'ledger_write':False}},sdk(),tmp_path)
    gateway=Gateway(Store(ports.database_path),ports.config,ports.adapters)
    service=ports.collectors_factory(gateway)
    assert service.collectors.store is gateway.store
    assert len(gateway.store.rows('SELECT * FROM gw_sources'))==2
    assert 'grok_bot' in ports.adapters and 'qwen' in ports.adapters
    assert 'codex_cli' not in ports.adapters
