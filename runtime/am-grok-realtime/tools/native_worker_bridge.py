"""Four local worker actions for a native Bot; credentials stay in private files.

This is a host protocol adapter, not a Grok product API or proof of product-side
tool/memory isolation. No arbitrary command, model, business write or send exists.
"""
import argparse
import fcntl
import hmac
import json
import os
from pathlib import Path
import secrets
import stat
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT)]
from am_gateway.phase1.execution import HTTPWorkerClient


class ClaimBindingError(ValueError):
    """Only fixed, source-free diagnostic codes may be exposed to the worker."""


def private_json(path):
    info=path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
        raise ValueError('private_owner_file_required')
    return json.loads(path.read_text())


def bound_credentials(saved, claim_handle):
    # A delayed command must name its original claim. Never infer ownership from
    # whichever operation happens to occupy the mutable current-state file.
    if not claim_handle:
        raise ClaimBindingError('claim_handle_required')
    expected=saved.get('claim_handle')
    if not isinstance(expected,str) or not expected:
        raise ClaimBindingError('legacy_claim_requires_resolution')
    if not hmac.compare_digest(expected.encode(),claim_handle.encode()):
        raise ClaimBindingError('stale_claim_handle')
    return saved['credentials']


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--claim-handle',help='The opaque handle returned by this task\'s original claim')
    parser.add_argument('action',choices=['claim','heartbeat','read_information','result'])
    args=parser.parse_args()
    cfg=private_json(args.config.resolve())
    client=HTTPWorkerClient(cfg['receiver_url'],cfg['worker_token'])
    state=args.config.resolve().with_suffix('.claim.json')
    lock=args.config.resolve().with_suffix('.lock')
    fd=os.open(lock,os.O_CREAT|os.O_RDWR,0o600)
    with os.fdopen(fd,'r+') as handle:
        fcntl.flock(handle,fcntl.LOCK_EX)
        if args.action=='claim':
            if args.claim_handle:
                raise ClaimBindingError('claim_does_not_accept_handle')
            # Do not create another execution when the previous result is unknown.
            if state.exists():
                claim=private_json(state)
                status=client.heartbeat(**claim['credentials'])
                if status['status'] in {'succeeded','failed','cancelled'}:
                    state.unlink()  # Terminal effects remain in the receiver's durable ledger.
                else:
                    # A second wake/poll must not receive the same task as new
                    # work. Only the original processor keeps its task context.
                    state_name=('already_claimed' if claim.get('claim_handle')
                                else 'legacy_claim_requires_resolution')
                    result={'status':state_name,'execution_state':status['status'],'same_claim':True}
            if not state.exists():
                claim=client.claim(worker_id=cfg['worker_id'])
                if claim is None:result={'status':'empty'}
                else:
                    credentials={k:claim[k] for k in ['operation_id','run_id','generation','lease_token']}
                    task={k:claim[k] for k in ['envelope','session_key','history']}
                    claim_handle='claim_'+secrets.token_urlsafe(32)
                    saved={'claim_handle':claim_handle,'credentials':credentials,'task':task}
                    fd=os.open(state,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
                    with os.fdopen(fd,'w') as output:json.dump(saved,output,ensure_ascii=False)
                    result={'status':'claimed','claim_handle':claim_handle,'task':task}
        else:
            if not args.claim_handle:
                raise ClaimBindingError('claim_handle_required')
            saved=private_json(state)
            credentials=bound_credentials(saved,args.claim_handle)
            if args.action=='heartbeat':result=client.heartbeat(**credentials)
            else:
                raw=sys.stdin.buffer.read(262145)
                if len(raw)>262144:raise ValueError('worker_input_limit')
                data=json.loads(raw)
                if args.action=='read_information':
                    if set(data)!={'info_id'}:raise ValueError('info_id_only')
                    result=client.read_information(**credentials,info_id=data['info_id'])
                else:
                    if set(data)-{'status','result','stopped_evidence','reason'}:raise ValueError('result_fields_only')
                    if data.get('status')=='unknown':
                        result=client.mark_unknown(**credentials,reason='worker_execution_unknown')
                    else:
                        result=client.complete(**credentials,**data)
        print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    try:main()
    except ClaimBindingError as exc:
        print(json.dumps({'status':'error','error_type':type(exc).__name__,
                          'error_code':str(exc)}));raise SystemExit(1)
    except Exception as exc:
        # Source bodies, tokens and HTTP diagnostic response text are private.
        print(json.dumps({'status':'error','error_type':type(exc).__name__}));raise SystemExit(1)
