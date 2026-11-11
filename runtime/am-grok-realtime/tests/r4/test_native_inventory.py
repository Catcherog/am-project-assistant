import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

TOOLS=Path(__file__).resolve().parents[2]/'tools';sys.path.insert(0,str(TOOLS))
spec=importlib.util.spec_from_file_location('native_inventory',TOOLS/'lark_personal_inventory.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


@pytest.mark.parametrize('identity,accepted',[('user',True),('bot',False)])
def test_inventory_checks_user_identity_and_only_returns_approved_chats(tmp_path,identity,accepted):
    binary=tmp_path/'verified-cli';binary.write_bytes(b'synthetic')
    cfg={'binary':str(binary),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),
         'profile':'verified','app_id':'cli_synthetic','owner_id':'ou_owner','tenant_id':'tenant',
         'identity':'owner_cli','allowed_chats':['oc_work']}
    request={'operation':'discover_page','discovery':{'tenant_id':'tenant','identity':'owner_cli','scope':'personal_dm'},'page_token':''}
    calls=[]
    def run(argv,**kw):
        calls.append(argv)
        if argv[1]=='whoami':
            return json.dumps({'identity':identity,'available':True,'profile':'verified','appId':'cli_synthetic','onBehalfOf':{'openId':'ou_owner'}}).encode()
        return json.dumps({'ok':True,'identity':'user','data':{'has_more':False,'chats':[
            {'chat_id':c,'tenant_key':'tenant','p2p_target_id':'ou_colleague'} for c in ['oc_work','oc_unrelated']]}}).encode()
    if accepted:
        result=module.discover(cfg,request,run)
        assert [x['native_id'] for x in result['sources']]==['oc_work']
        assert result['has_more'] is False and result['complete_for_scope'] is True
    else:
        with pytest.raises(module.ReadBridgeError,match='identity_changed'):module.discover(cfg,request,run)
        assert len(calls)==1
