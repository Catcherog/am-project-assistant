"""Synthetic experiments use the original live consumer, resource window and stop proof.
The user explicitly authorized experimental synthetic admission; gold paths are unchanged.
"""
import hashlib
import os
from pathlib import Path
import subprocess
import time

from .native_r5 import OriginalPilotExecutor, group_gone
from .process_guard import BoundProcessGuard, digest, private_json
from .window import GateObservation, ExecutionObservation, WindowError

class SyntheticInputGate:
    def __init__(self,path):self.path=Path(path)
    def __call__(self):
        from training.common import read_json
        from training.mlx_synthetic_contracts import validate_envelope
        plan=read_json(self.path)
        validate_envelope(plan)
        return GateObservation(True,plan['sha256'])

class SyntheticExecutor(OriginalPilotExecutor):
    @staticmethod
    def command(plan_path,plan):
        roots=plan['runtime_source_binding']['roots']
        root=Path(roots['training'])
        env=dict(os.environ,PYTHONPATH=os.pathsep.join((roots['training'],roots['am_style'])),
                 PYTHONDONTWRITEBYTECODE='1',HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1')
        argv=[plan['python_interpreter']['path'],'-m','training.mlx_synthetic','--plan',str(plan_path),'--execute']
        return argv,env,root

    def __call__(self):
        if self.process is not None or self.cli_start_attempts:raise WindowError('pilot_callback_already_used')
        self.check_request_binding()
        SyntheticInputGate(self.plan)()  # Missing inputs do not even spawn a CLI.
        self.check_request_binding()
        from training.common import read_json
        plan=read_json(self.plan)
        self.run_directory=Path(plan['run_directory'])
        if self.run_directory.exists():raise WindowError('original_run_directory_already_exists')
        python=plan['python_interpreter']['path']
        self.evidence.mkdir(mode=0o700,parents=True,exist_ok=True)
        if self.evidence.is_symlink():raise WindowError('execution_evidence_symlink')
        self.evidence.chmod(0o700)
        reason=self.supervise()
        if reason:raise WindowError('execution_cancelled_before_cli:'+reason)
        self.check_request_binding()
        log=self.evidence/'original-cli.log'
        command,env,package_root=self.command(self.plan,plan)
        commands={'cli':command}
        for phase in ('fit','reload','eval'):
            role='mlx_worker' if phase=='eval' else phase
            commands[role]=[python,'-m','training.mlx_synthetic','--worker',phase,
                             '--request',str(self.run_directory/'request.json'),'--execute']
        self.guard=BoundProcessGuard.create(self.evidence/'processes.json',python=python,
            commands=commands,request_id=self.request_id,plan_path=self.plan,run_directory=self.run_directory,
            sources={'runtime':plan['runtime_source_binding'],
                     'synthetic_entry':digest(package_root/'training/mlx_synthetic.py'),'native_synthetic.py':digest(__file__),
                     'process_guard.py':digest(Path(__file__).with_name('process_guard.py'))})
        started=time.time()
        deadline=min(self.expires_at or started+660,started+660)
        stop_reason=None;code=None
        fd=os.open(log,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        try:
            with os.fdopen(fd,'wb') as stream:
                self.cli_start_attempts+=1
                self.process=subprocess.Popen(command,cwd=package_root,env=env,stdin=subprocess.DEVNULL,
                    stdout=stream,stderr=subprocess.STDOUT,start_new_session=True,shell=False)
                self.guard.refresh()
                while self.process.poll() is None:
                    self.guard.refresh()
                    stop_reason=self.supervise()
                    if time.time()>=deadline:stop_reason='request_or_total_window_expired'
                    if stop_reason:
                        self.guard.stop(stop_reason,reap=self.process.poll)
                        break
                    time.sleep(.25)
                code=self.process.poll()
        except BaseException as exc:
            stop_reason='supervision_exception:'+type(exc).__name__
            if self.process is not None:
                self.guard.stop(stop_reason,reap=self.process.poll)
                code=self.process.poll()
            if isinstance(exc,(KeyboardInterrupt,SystemExit)):raise
        result_path=self.run_directory/'result.json'
        result_hash=None
        if result_path.is_file() and not result_path.is_symlink() and result_path.stat().st_mtime>=started:
            self.result=read_json(result_path)
            if self.result.get('schema')!='am-synthetic-mlx-result/1':raise WindowError('original_r4_result_schema')
            result_hash=hashlib.sha256(result_path.read_bytes()).hexdigest()
            self.guard.report_result_groups(self.result.get('phases',[]))
        stopped=self.stopped()
        completed=(not stop_reason and code==0 and stopped and self.result is not None and self.result.get('complete') is True
                   and [p.get('phase') for p in self.result['phases']]==(['fit','reload'] if plan['mode']=='train' else ['eval'])
                   and all(p.get('attempted') is True and p.get('status')=='completed' for p in self.result['phases']))
        validated=None
        if completed:
            from training.mlx_synthetic_contracts import validate_completed_run
            try:
                validated=validate_completed_run(plan,self.run_directory)
                if read_json(self.run_directory/'request.json')['config']!=read_json(plan['config']['path']):
                    raise WindowError('actual_request_differs_from_bound_plan')
            except Exception as exc:
                completed=False;stop_reason='completed_result_validation:'+str(exc)
        process_snapshot=self.evidence/'processes-at-return.json'
        private_json(process_snapshot,self.guard.data,exclusive=True)
        evidence={'schema':'am-synthetic-execution-observation/1','started':started,'finished':time.time(),
            'request_id':self.request_id,'plan_sha256':digest(self.plan),'command':command,
            'source_environment':{'PYTHONPATH':env['PYTHONPATH']},'mode':plan['mode'],
            'process_ledger_sha256':digest(process_snapshot),'process_ledger_snapshot':process_snapshot.name,
            'stop_reason':stop_reason,'validated_pilot':validated,
            'cli_pid':self.process.pid,'cli_returncode':code,'cli_start_attempts':self.cli_start_attempts,
            'original_r4_result_sha256':result_hash,'original_phases':None if self.result is None else self.result.get('phases'),
            'original_cli_group_gone':group_gone(self.process.pid),'independent_worker_groups_gone':stopped,
            'status':'completed' if completed else 'blocked_or_failed','log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
        path=self.evidence/'execution.json'
        private_json(path,evidence,exclusive=True)
        return ExecutionObservation(evidence['status'],stopped,digest(path))
