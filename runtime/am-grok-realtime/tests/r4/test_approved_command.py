import json
from pathlib import Path
import sys

import pytest

from am_gateway.models import ContractError
from integration.approved_command import ApprovedCommandExecutor


def executor(tmp_path):
    script=tmp_path/'host.py'
    script.write_text('''import json,sys
r=json.load(sys.stdin)
p=r['payload']
if r['operation']=='apply':
 print(json.dumps({'idempotency_key':r['idempotency_key'],'status':'applied'}))
else:
 print(json.dumps({'idempotency_key':r['idempotency_key'],'status':'applied','project_id':p['project_id'],
 'target':p['target'],'changes':p['changes'],'evidence_ref':'local-host-readback'}))
''')
    return ApprovedCommandExecutor(argv=[sys.executable,str(script)],readback_argv=[sys.executable,str(script)],
        targets={'P':{'work_item':['item-1']}},actions=['update_fields'])


def proposal():
    return {'proposal_id':'proposal','version':1,'digest':'frozen',
            'payload':{'project_id':'P','target':{'type':'work_item','id':'item-1'},
                       'action':'update_fields','changes':{'text':'literal $(never execute) `or this`'}}}


def test_approved_json_command_uses_independent_exact_readback(tmp_path):
    result=executor(tmp_path)(proposal(),idempotency_key='one-effect')
    assert result['status']=='applied' and result['evidence_ref']=='local-host-readback'
    assert result['readback']['changes']==proposal()['payload']['changes']


def test_business_command_rejects_unbound_target_before_execution(tmp_path):
    p=proposal();p['payload']['target']['id']='outside'
    with pytest.raises(ContractError,match='not configured'):
        executor(tmp_path)(p,idempotency_key='one-effect')
