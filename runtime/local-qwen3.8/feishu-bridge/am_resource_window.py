"""Explicit private diagnostic requests executed by the original bridge process.

This port has no training operation. It uses the live Store and ModelCoordinator,
keeps message reception running, and never creates a second business consumer.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

REPO=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(REPO/'am-integrated-r6'))
sys.path.insert(0,str(REPO/'am-style-r3'))
from am_training_window.host import ExistingCoordinatorHost, StoreMaintenanceFence
from am_training_window.original_r4 import OriginalCoordinatorLock
from am_training_window.ollama import OllamaLifecycle
from am_training_window.window import HostBinding, HostObservation, Journal, ModelIdentity, TrainingWindow, WindowError
from fs_agent.provider import atomic_json


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class NativeResourceWindow:
    def __init__(self, bridge):
        self.bridge=bridge
        self.coordinator=bridge.poc.gateway.provider
        self.store=bridge.poc.store
        self.root=bridge.root/'resource-window-r6'
        self.root.mkdir(mode=0o700,exist_ok=True)
        if self.root.is_symlink(): raise WindowError('resource_directory_symlink')
        self.root.chmod(0o700)
        self.thread=None

    def tick(self):
        if self.thread is not None and self.thread.is_alive(): return False
        requests=sorted(self.root.glob('*.request.json'))
        if not requests:return False
        p=requests[0];st=p.lstat()
        if p.is_symlink() or st.st_uid!=os.getuid() or st.st_mode&0o077 or st.st_size>16000:return False
        claimed=p.with_suffix('.claimed')
        try:p.rename(claimed)
        except FileNotFoundError:return False
        self.thread=threading.Thread(target=self.run,args=(claimed,),name='am-resource-diagnostic',daemon=True)
        self.thread.start()
        return True

    def queue_counts(self):
        now=self.store.clock()
        counts={}
        for name,sql in {
            'harness':"SELECT count(*) AS n FROM am_harness_runs WHERE state IN ('queued','running')",
            'am':"SELECT count(*) AS n FROM am_inbox WHERE state IN ('received','processing','harness_admitting','source_wait') AND expires>?",
            'tasks':"SELECT count(*) AS n FROM tasks WHERE state IN ('queued','running','received')",
            'outbox':"SELECT count(*) AS n FROM outbox WHERE state IN ('pending','sending') AND expires>?",
        }.items():counts[name]=self.store.rows(sql,(now,) if '?' in sql else ())[0]['n']
        with self.bridge.mutex:
            counts['ordinary_chat']=self.bridge.db.execute("SELECT count(*) FROM inbox WHERE state IN ('queued','processing','sending')").fetchone()[0]
        ledger=self.bridge.group_ledger
        if ledger is not None:
            with ledger.mutex:
                counts['group_ledger']=ledger.db.execute("SELECT count(*) FROM ledger_inbox WHERE state IN ('queued','processing','sending')").fetchone()[0]
        info=self.bridge.information
        if info is not None:
            db=info.root/'information.sqlite3'
            with sqlite3.connect(db.resolve().as_uri()+'?mode=ro',uri=True) as c:
                counts['information_model_jobs']=c.execute("SELECT count(*) FROM jobs WHERE kind='extract' AND state IN ('pending','leased')").fetchone()[0]
        # Running futures cover claim-to-model gaps and complete multi-step work.
        futures=[getattr(self.bridge,n,None) for n in ('group_future','chat_future')]
        futures+=list(getattr(self.bridge.am_self,'io_futures',{}).values())
        # An idle ordinary-chat polling future is covered by the DB and model lock.
        counts['active_io']=sum(f is not None and not f.done() for f in futures if f is not getattr(self.bridge,'chat_future',None))
        return counts

    def setup(self):
        manifest_path=REPO/'AM-HARNESS-20261031-01/R4/private/mlx-base.json'
        manifest=json.loads(manifest_path.read_text())
        from training.mlx_r4_resources import validate_service_paths
        validate_service_paths(manifest)
        # Reuse the already installed R4 Python only to read psutil metrics.
        plan=REPO/'AM-HARNESS-20261031-01/R5/private/feedback-003/pilot-plan-001/plan.json'
        python=json.loads(plan.read_text())['python_interpreter']['path']
        if not Path(python).is_file():raise WindowError('original_metrics_interpreter_missing')
        plist=Path.home()/'Library/LaunchAgents/local.qwen38.ollama.plist'
        service=plistlib.loads(plist.read_bytes())
        argv=service.get('ProgramArguments')
        if not isinstance(argv,list) or len(argv)!=2 or argv[1]!='serve':raise WindowError('manager_not_original_ollama')
        manager=subprocess.check_output(['/usr/sbin/lsof','-nP','-iTCP:11434','-sTCP:LISTEN','-t'],text=True).split()
        if len(manager)!=1:raise WindowError('manager_not_unique')
        lifecycle=OllamaLifecycle(manager_binary=argv[0],manager_pid=int(manager[0]),version='0.40.5')
        protected=dict(manifest['service']['protected_config_files'])
        protected[str(plist)]=sha(plist)
        fingerprint=hashlib.sha256(json.dumps(protected,sort_keys=True).encode()).hexdigest()
        model=ModelIdentity(manifest['service']['expected_model'],manifest['service']['expected_digest'])
        instance_path=self.bridge.root/'instance.lock'
        instance_stat=instance_path.stat()
        instance=f'original-bridge:{instance_stat.st_dev}:{instance_stat.st_ino}'
        binding=HostBinding(instance,fingerprint,model,'llamacpp')

        def observation():
            current_instance=instance_path.stat()
            if (current_instance.st_dev,current_instance.st_ino)!=(instance_stat.st_dev,instance_stat.st_ino):
                raise WindowError('original_instance_lock_changed')
            for path,expected in protected.items():
                if sha(path)!=expected:raise WindowError('protected_config_changed')
            lifecycle.verify_identity(binding)
            rows=subprocess.check_output(['/bin/ps','-axo','pid=,command='],text=True).splitlines()
            consumers=[]
            for line in rows:
                parts=line.strip().split(None,1)
                if len(parts)==2 and Path(parts[1].split(' ',1)[0]).name.startswith('python') and '/feishu-bridge/bridge.py --config ' in parts[1]:consumers.append(int(parts[0]))
            connection=json.loads((self.bridge.root/'connection-status.json').read_text())
            heartbeat=json.loads((self.bridge.root/'heartbeat-status.json').read_text())
            counts=self.queue_counts()
            self.last_queues=counts
            loaded=tuple(ModelIdentity(m['name'],m['digest']) for m in lifecycle.request('/api/ps')['models'])
            return HostObservation(binding,len(consumers),
                consumers==[os.getpid()] and connection['state']=='connected' and time.time()-heartbeat['time']<120,
                True,int(self.coordinator.lock.locked()),sum(counts.values()),loaded,None)

        def probe():
            code=('import sys,json;sys.path[:0]=sys.argv[1:3];'
                  'from am_training_window.original_r4 import read_original_metrics;'
                  'v=read_original_metrics(json.load(open(sys.argv[3])));'
                  'v["mlx_imported"]=any(n=="mlx" or n.startswith("mlx.") for n in sys.modules);print(json.dumps(v))')
            done=subprocess.run([python,'-c',code,str(REPO/'am-integrated-r6'),str(REPO/'am-style-r3'),str(manifest_path)],
                capture_output=True,text=True,timeout=15,check=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
            value=json.loads(done.stdout)
            if value['mlx_imported']:raise WindowError('diagnostic_imported_training_runtime')
            value['all_queue_counts']=self.queue_counts()
            value['worker']=lifecycle.workers()
            value['manager_pid']=lifecycle.pid
            return value

        host=ExistingCoordinatorHost(coordinator=self.coordinator,store=self.store,read_observation=observation,
                                     lifecycle=lifecycle,drain_timeout_seconds=30)
        return host,binding,lifecycle,lambda:OriginalCoordinatorLock(manifest),probe

    def run(self, path):
        result={'ok':False,'trainer_start_attempts':0,'real_platform_sends':0}
        try:
            request=json.loads(path.read_text())
            if set(request)-{'operation','expires_at','journal'} or request.get('expires_at',0)<=time.time():
                raise WindowError('resource_request_invalid_or_expired')
            if request.get('operation') not in ('diagnostic','recover'):raise WindowError('diagnostic_port_has_no_trainer')
            host,binding,lifecycle,lock,probe=self.setup()
            if request['operation']=='recover':
                journal_path=(self.root/request['journal']).resolve()
                if journal_path.parent!=self.root.resolve() or journal_path.suffix!='.json':raise WindowError('journal_scope')
                journal=Journal.load(journal_path)
                window=TrainingWindow(host,binding,lock,probe,journal)
                output=window.recover()
            else:
                initial=probe()
                result['initial_observation']=initial
                # An idle model may already have expired. Establish the same Q8
                # residency in an owned, drained window before measuring removal.
                if not host.inspect().loaded_models:
                    if sum(self.queue_counts().values()) or self.coordinator.lock.locked():
                        raise WindowError('preload_business_not_drained')
                    preload=Journal.create(self.root/(uuid.uuid4().hex+'.journal.json'),'diagnostic',binding)
                    owner=preload.data['window_id'];fence=StoreMaintenanceFence(self.store)
                    preload.write('preload_fencing')
                    fence.claim(owner)
                    try:
                        host.drain(owner)
                        with lock():
                            preload.write('preloading',suspend_attempted=True)
                            lifecycle.restore(binding)
                        if host.inspect().loaded_models!=(binding.model,):raise WindowError('warmup_identity_unverified')
                    except BaseException:
                        result['preload_journal']=preload.path.name
                        if preload.data['suspend_attempted']:
                            result['preload_recovery']=TrainingWindow(host,binding,lock,probe,preload).recover()
                        else:
                            fence.release(owner)
                            preload.write('failed_before_load',status='failed_restored')
                        raise
                    else:
                        preload.write('preload_verified',restore_verified=True)
                        fence.release(owner)
                        preload.write('completed',status='completed')
                    result['preloaded_original_model']=True
                journal=Journal.create(self.root/(uuid.uuid4().hex+'.journal.json'),'diagnostic',binding)
                def sampled_probe():
                    samples=[probe()]
                    if not samples[0]['loaded_models']:
                        for _ in range(2):time.sleep(2);samples.append(probe())
                    value=dict(samples[-1]);value['stability_samples']=samples
                    return value
                window=TrainingWindow(host,binding,lock,sampled_probe,journal)
                output=window.diagnostic()
            result.update(ok=output['status'] in ('completed','recovered'),window=output,
                          lifecycle=lifecycle.events,final_observation=probe())
        except BaseException as exc:
            result['error']=str(exc) if isinstance(exc,WindowError) else type(exc).__name__
        result['finished_at']=time.time()
        atomic_json(path.with_name(path.name.replace('.request.claimed','.result.json')),result)
