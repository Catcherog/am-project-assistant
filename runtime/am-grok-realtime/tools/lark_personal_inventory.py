"""Read native user P2P inventory, intersecting an explicit work-chat allowlist."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

from feishu_cli_read_bridge import _run, ReadBridgeError


def discover(config, request, run=_run):
    spec=request.get('discovery',{})
    if (request.get('operation')!='discover_page' or spec.get('scope')!='personal_dm'
            or spec.get('identity')!=config['identity'] or spec.get('tenant_id')!=config['tenant_id']):
        raise ReadBridgeError('personal_inventory_binding_mismatch')
    binary=Path(config['binary'])
    if hashlib.sha256(binary.read_bytes()).hexdigest()!=config['binary_sha256']:
        raise ReadBridgeError('personal_cli_binary_changed')
    def call(argv):
        return json.loads(run(argv,timeout=25,output_limit=8000000))
    identity=call([str(binary),'whoami','--profile',config['profile']])
    if (identity.get('identity')!='user' or identity.get('available') is not True
            or identity.get('profile')!=config['profile'] or identity.get('appId')!=config['app_id']
            or identity.get('onBehalfOf',{}).get('openId')!=config['owner_id']):
        raise ReadBridgeError('personal_cli_identity_changed')
    command=[str(binary),'im','+chat-list','--types=p2p','--sort','active_time','--page-size','50',
             '--as','user','--profile',config['profile']]
    token=request.get('page_token','')
    if not isinstance(token,str):raise ReadBridgeError('invalid_inventory_cursor')
    if token:command+=['--page-token',token]
    response=call(command)
    if response.get('ok') is not True or response.get('identity')!='user':
        raise ReadBridgeError('user_inventory_failed')
    data=response.get('data',{})
    if not isinstance(data.get('chats'),list) or type(data.get('has_more')) is not bool:
        raise ReadBridgeError('invalid_user_inventory')
    next_token=data.get('page_token','')
    if data['has_more'] and (not isinstance(next_token,str) or not next_token or next_token==token):
        raise ReadBridgeError('invalid_inventory_next_cursor')
    allowed=set(config['allowed_chats']);sources=[]
    for chat in data['chats']:
        if chat.get('chat_id') in allowed:
            if chat.get('tenant_key')!=config['tenant_id'] or not chat.get('p2p_target_id'):
                raise ReadBridgeError('personal_inventory_source_mismatch')
            sources.append({'tenant_id':config['tenant_id'],'native_id':chat['chat_id'],
                'identity':config['identity'],'channel':'personal_dm','adapter':'personal_messages'})
    return {'status':'ok','sources':sources,'has_more':data['has_more'],
            'page_token':next_token if data['has_more'] else '', 'complete_for_scope':True}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True);a=parser.parse_args()
    try:
        raw=sys.stdin.buffer.read(1048577)
        if len(raw)>1048576:raise ReadBridgeError('inventory_request_limit')
        result=discover(json.loads(a.config.read_text()),json.loads(raw))
        print(json.dumps(result,ensure_ascii=False));return 0
    except (ReadBridgeError,ValueError,OSError,KeyError,TypeError) as exc:
        print(json.dumps({'status':'error','error':str(exc) if isinstance(exc,ReadBridgeError) else type(exc).__name__}));return 1


if __name__=='__main__':raise SystemExit(main())
