import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


TOOL = Path(__file__).resolve().parents[2] / 'tools' / 'feishu_cli_read_bridge.py'
spec = importlib.util.spec_from_file_location('feishu_cli_read_bridge', TOOL)
bridge_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge_module)
Bridge, Error = bridge_module.FeishuCLIReadBridge, bridge_module.ReadBridgeError


def setup(tmp_path, *, failure=False):
    fake = tmp_path / 'verified_cli.py'
    fake.write_text('''import json,sys
mode, identity, container, message, page = sys.argv[1:]
assert identity=='owner_cli'
if mode=='fail':
 sys.stderr.write('secret-token-should-not-escape');sys.exit(4)
item={'message_id':message or 'om_1','chat_id':'oc_allowed','thread_id':'omt_1',
      'body':{'content':json.dumps({'text':'真实本地进程结果 '+page},ensure_ascii=False)}}
print(json.dumps({'code':0,'data':{'items':[item],'has_more':False}},ensure_ascii=False))
''')
    argv = [sys.executable,str(fake),'fail' if failure else 'list','{identity}','{container_id}','{message_id}','{page_token}']
    config = {'identity':'owner_cli','allowed_chats':['oc_allowed'],
              'commands':{'list':{'argv':argv},'get':{'argv':argv}},'timeout_seconds':3}
    return config, Bridge(config,state_path=tmp_path/'state.sqlite')


def request(*, path='/open-apis/im/v1/messages', params=None, identity='owner_cli'):
    return {'operation':'feishu_read','url':'https://open.feishu.cn'+path,'identity':identity,
            'params':params if params is not None else {'container_id_type':'chat','container_id':'oc_allowed'}}


def test_list_and_exact_get_run_actual_configured_read_command(tmp_path):
    _, bridge = setup(tmp_path)
    assert bridge(request())['data']['items'][0]['message_id']=='om_1'
    result = bridge(request(path='/open-apis/im/v1/messages/om_exact',params={'user_id_type':'open_id'}))
    assert result['data']['items'][0]['message_id']=='om_exact'


def test_observed_thread_binding_persists_and_unobserved_thread_is_rejected(tmp_path):
    config, bridge = setup(tmp_path)
    thread = request(params={'container_id_type':'thread','container_id':'omt_1'})
    with pytest.raises(Error,match='not_observed'):
        bridge(thread)
    bridge(request())
    restarted = Bridge(config,state_path=tmp_path/'state.sqlite')
    assert restarted(thread)['code']==0


@pytest.mark.parametrize('bad', [
    request(identity='am_bot'),
    request(params={'container_id_type':'chat','container_id':'oc_private'}),
    request(path='/open-apis/im/v1/messages/om_1/reply'),
    {**request(),'url':'https://example.com/open-apis/im/v1/messages'},
])
def test_wrong_identity_scope_and_write_endpoint_never_start_cli(tmp_path,monkeypatch,bad):
    _, bridge=setup(tmp_path)
    calls=[]
    monkeypatch.setattr(bridge_module,'_run',lambda *a,**kw:calls.append(1))
    with pytest.raises(Error):
        bridge(bad)
    assert calls==[]


def test_argv_data_cannot_become_shell_command(tmp_path):
    _, bridge=setup(tmp_path)
    marker=tmp_path/'must-not-exist'
    token='$(touch '+str(marker)+'); echo unsafe'
    result=bridge(request(params={'container_id_type':'chat','container_id':'oc_allowed','page_token':token}))
    assert token in result['data']['items'][0]['body']['content']
    assert not marker.exists()


def test_cli_failure_does_not_surface_private_stderr(tmp_path):
    config,_=setup(tmp_path,failure=True)
    cfg=tmp_path/'read-config.json';cfg.write_text(json.dumps(config))
    process=subprocess.run([sys.executable,str(TOOL),'--config',str(cfg)],
        input=json.dumps(request()),text=True,capture_output=True,timeout=5)
    assert process.returncode==1
    assert json.loads(process.stdout)['error']=='cli_read_failed'
    assert 'secret-token' not in process.stdout+process.stderr


def test_main_stdin_contract_works_without_llm(tmp_path):
    config,_=setup(tmp_path)
    cfg=tmp_path/'read-config.json';cfg.write_text(json.dumps(config))
    process=subprocess.run([sys.executable,str(TOOL),'--config',str(cfg)],
        input=json.dumps(request()),text=True,capture_output=True,timeout=5)
    assert process.returncode==0
    assert json.loads(process.stdout)['data']['items'][0]['chat_id']=='oc_allowed'


@pytest.mark.parametrize('identity,ok,accepted', [('user',True,True),('bot',True,False),('user',False,False)])
def test_installed_cli_wrapper_requires_successful_user_identity(tmp_path,monkeypatch,identity,ok,accepted):
    config,_=setup(tmp_path)
    config['commands']['list']['response_format']='lark_cli_v1'
    response={'ok':ok,'identity':identity,'data':{'items':[{'message_id':'om_1','chat_id':'oc_allowed'}]}}
    monkeypatch.setattr(bridge_module,'_run',lambda *a,**kw:json.dumps(response).encode())
    bridge=Bridge(config,state_path=tmp_path/'user-cli.sqlite')
    if accepted:
        assert bridge(request())=={'code':0,'data':response['data']}
    else:
        with pytest.raises(Error,match='verified_user_cli_response_required'):
            bridge(request())
