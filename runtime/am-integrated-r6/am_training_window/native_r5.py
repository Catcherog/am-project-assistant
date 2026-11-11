"""Actual original R5 CLI callback and independent original R4 stop proof.

This is supplied to TrainingWindow.train, after its maintenance lock is released.
The diagnostic control port never imports or invokes it. No R4 argv is cached.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from .process_guard import BoundProcessGuard, digest, private_json
from .r5_gate import OriginalR5InputGate
from .window import ExecutionObservation, WindowError


def group_gone(pid):
    if type(pid) is not int or pid<=1:return False
    try:os.killpg(pid,0)
    except ProcessLookupError:return True
    except PermissionError:return False
    return False


def phase_stop_proof(result, check_group=group_gone):
    phases=result.get('phases')
    if not isinstance(phases,list) or not phases:return False
    names=[p.get('phase') for p in phases]
    if names!=['fit','reload']:return False
    if any(p.get('attempted') is not True and not (p.get('phase')=='reload'
            and p.get('attempted') is False and p.get('status')=='skipped_fit_not_completed_or_stop_unconfirmed') for p in phases):return False
    attempted=[p for p in phases if p.get('attempted') is True]
    if not attempted:return False
    return all(p.get('stop_confirmed') is True and check_group(p.get('pid')) for p in attempted)


class OriginalPilotExecutor:
    def __init__(self, plan_path, evidence_directory, *, request_id=None, supervise=None,
                 expires_at=None, expected_plan_sha256=None):
        self.plan=Path(plan_path)
        self.evidence=Path(evidence_directory)
        if not self.plan.is_absolute() or not self.evidence.is_absolute():raise ValueError('absolute original paths required')
        self.process=None;self.run_directory=None;self.result=None;self.cli_start_attempts=0
        self.request_id=request_id or str(uuid.uuid4())
        uuid.UUID(self.request_id)
        self.supervise=supervise or (lambda: None)
        self.expires_at=expires_at
        self.expected_plan_sha256=expected_plan_sha256
        self.guard=None

    def check_request_binding(self):
        if self.expires_at is not None and time.time() >= self.expires_at:
            raise WindowError('execution_request_expired')
        if self.expected_plan_sha256 is not None and digest(self.plan) != self.expected_plan_sha256:
            raise WindowError('execution_request_plan_changed')

    def stopped(self):
        # Called independently by TrainingWindow before any Q8 restoration.
        if self.process is None:return self.cli_start_attempts==0
        self.process.poll()
        return self.guard is not None and self.guard.stopped()

    @staticmethod
    def command(plan_path, plan):
        from am_style_r5 import pilot
        root=Path(pilot.__file__).resolve().parents[2]
        script=root/'run.py'
        if not script.is_file():raise WindowError('original_r5_run_entry_missing')
        roots=plan['runtime_source_binding']['roots']
        env=dict(os.environ, AM_R5_HOST_SRC=roots['am_style'],
                 AM_R5_TRAINING_ROOT=roots['training'],
                 PYTHONPATH=os.pathsep.join((str(root/'src'),roots['am_style'],roots['training'])),
                 PYTHONDONTWRITEBYTECODE='1')
        argv=[plan['python_interpreter']['path'],str(script),'pilot-execute','--plan',str(plan_path),'--execute']
        return argv,env,root

    def __call__(self):
        if self.process is not None or self.cli_start_attempts:raise WindowError('pilot_callback_already_used')
        self.check_request_binding()
        OriginalR5InputGate(self.plan)()  # Missing inputs do not even spawn a CLI.
        self.check_request_binding()
        from am_style_r5 import pilot
        from am_style_r5.common import read_json
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
        commands={'cli':command,'r4_parent':[python,'-m','training.mlx_r4','--manifest',plan['manifest']['path'],
            '--config',plan['config']['path'],'--output',str(self.run_directory),'--execute']}
        for phase in ('fit','reload'):
            commands[phase]=[python,'-m','training.mlx_r4','--worker',phase,
                             '--request',str(self.run_directory/'request.json'),'--execute']
        self.guard=BoundProcessGuard.create(self.evidence/'processes.json',python=python,
            commands=commands,request_id=self.request_id,plan_path=self.plan,run_directory=self.run_directory,
            sources={'runtime':plan['runtime_source_binding'],
                     'run.py':digest(package_root/'run.py'),'native_r5.py':digest(__file__),
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
            if self.result.get('schema')!='am-style-mlx-r4-result/1':raise WindowError('original_r4_result_schema')
            result_hash=hashlib.sha256(result_path.read_bytes()).hexdigest()
            self.guard.report_result_groups(self.result.get('phases',[]))
        stopped=self.stopped()
        completed=(not stop_reason and code==0 and stopped and self.result is not None and self.result.get('mechanics_closed') is True
                   and [p.get('phase') for p in self.result['phases']]==['fit','reload']
                   and all(p.get('attempted') is True and p.get('status')=='completed' for p in self.result['phases']))
        validated=None
        if completed:
            from am_style_r5.runners import validate_sft_run
            try:
                validated=validate_sft_run(self.run_directory,read_json(plan['manifest']['path']))
                if read_json(self.run_directory/'request.json')['config']!=read_json(plan['config']['path']):
                    raise WindowError('actual_request_differs_from_bound_plan')
            except Exception as exc:
                completed=False;stop_reason='completed_result_validation:'+str(exc)
        process_snapshot=self.evidence/'processes-at-return.json'
        private_json(process_snapshot,self.guard.data,exclusive=True)
        evidence={'schema':'am-original-r5-execution-observation/1','started':started,'finished':time.time(),
            'request_id':self.request_id,'plan_sha256':digest(self.plan),'command':command,
            'source_environment':{k:env[k] for k in ('AM_R5_HOST_SRC','AM_R5_TRAINING_ROOT')},
            'process_ledger_sha256':digest(process_snapshot),'process_ledger_snapshot':process_snapshot.name,
            'stop_reason':stop_reason,'validated_pilot':validated,
            'cli_pid':self.process.pid,'cli_returncode':code,'cli_start_attempts':self.cli_start_attempts,
            'original_r4_result_sha256':result_hash,'original_phases':None if self.result is None else self.result.get('phases'),
            'original_cli_group_gone':group_gone(self.process.pid),'independent_worker_groups_gone':stopped,
            'status':'completed' if completed else 'blocked_or_failed','log_sha256':hashlib.sha256(log.read_bytes()).hexdigest()}
        path=self.evidence/'execution.json'
        private_json(path,evidence,exclusive=True)
        return ExecutionObservation(evidence['status'],stopped,digest(path))
