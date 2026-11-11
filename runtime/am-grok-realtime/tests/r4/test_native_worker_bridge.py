import json
import os
from pathlib import Path
import subprocess
import sys

from am_gateway.phase1.receiver import DurableReceiver
from am_gateway.phase1.execution import ReceiverHTTPServer
from am_gateway.store import Store
from test_receiver import envelope

TOOL=Path(__file__).resolve().parents[2]/'tools/native_worker_bridge.py'


def test_local_native_commands_keep_credentials_private_and_resume_original_claim(tmp_path):
    receiver=DurableReceiver(Store(tmp_path/'receiver.sqlite',clock=lambda:1000.))
    receiver.accept(envelope())
    server=ReceiverHTTPServer(receiver,admin_token='synthetic-admin',worker_token='synthetic-worker');server.start()
    cfg=tmp_path/'worker.json';cfg.write_text(json.dumps({'receiver_url':server.base_url,'worker_token':'synthetic-worker','worker_id':'native-test'}));cfg.chmod(0o600)
    def call(action,data=None,*,claim_handle=None):
        argv=[sys.executable,str(TOOL),'--config',str(cfg),action]
        if claim_handle is not None:argv+=['--claim-handle',claim_handle]
        result=subprocess.run(argv,
            input='' if data is None else json.dumps(data),capture_output=True,text=True,timeout=5)
        assert result.returncode==0,result.stdout+result.stderr
        assert 'synthetic-worker' not in result.stdout and 'lease_token' not in result.stdout
        return json.loads(result.stdout)
    try:
        claimed=call('claim')
        assert claimed['status']=='claimed'
        claim_handle=claimed['claim_handle']
        duplicate=call('claim')
        assert duplicate['same_claim'] is True and 'task' not in duplicate
        assert 'claim_handle' not in duplicate
        assert call('heartbeat',claim_handle=claim_handle)['status']=='running'
        assert call('result',{'status':'unknown'},claim_handle=claim_handle)['status']=='unknown'
        assert call('heartbeat',claim_handle=claim_handle)['status']=='unknown'
        assert call('result',{'status':'succeeded','result':{'response_kind':'sufficient','reply_text':'synthetic',
            'remaining_requirements':[],'artifacts':[]},'stopped_evidence':'synthetic definite completion'},
            claim_handle=claim_handle)['status']=='succeeded'
        assert len(receiver.store.rows('SELECT * FROM h4_receiver_runs'))==1
        receiver.accept(envelope(mid='m2'))
        assert call('claim')['status']=='claimed'
        assert len(receiver.store.rows('SELECT * FROM h4_receiver_runs'))==2
    finally:server.stop()
