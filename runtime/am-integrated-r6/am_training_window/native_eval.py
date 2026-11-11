"""Same fenced memory/stop window for the original R5 MLX evaluation runner."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import time

from .native_r5 import OriginalPilotExecutor
from .process_guard import BoundProcessGuard, digest, private_json
from .window import ExecutionObservation, GateObservation, WindowError


class OriginalEvaluationGate:
    def __init__(self, plan, pilot_plan, arm, output):
        self.plan, self.pilot_plan, self.output = map(Path, (plan, pilot_plan, output))
        self.arm = arm

    def checked(self):
        from am_style_r5.common import read_json, check_seal, object_hash
        from am_style_r5.evaluation import validate_plan
        from am_style_r5.holdout_gate import verify_holdout
        from am_style_r5.pilot import _source_binding, _interpreter_record
        from am_style_r5.runners import (validate_sft_run, _mlx_identity, _fresh_output,
                                        _execution_sources, MLX_SETTINGS)
        pilot = check_seal(read_json(self.pilot_plan))
        evaluation = validate_plan(read_json(self.plan))
        held = verify_holdout(pilot)
        if (evaluation['mode'] != 'real' or evaluation['case_manifest_sha256'] != held['case_manifest_sha256']
                or self.arm not in ('mlx_base', 'mlx_sft')
                or evaluation['arms'][self.arm]['generation_settings'] != MLX_SETTINGS):
            raise WindowError('evaluation_not_bound_to_actual_pilot_holdout')
        sources = _source_binding(pilot['runtime_source_binding']['roots'])
        if sources != pilot['runtime_source_binding'] or _interpreter_record(pilot['python_interpreter']['path']) != pilot['python_interpreter']:
            raise WindowError('evaluation_runtime_changed')
        if digest(pilot['manifest']['path']) != pilot['manifest']['sha256']:
            raise WindowError('evaluation_manifest_changed')
        manifest = read_json(pilot['manifest']['path'])
        selected = validate_sft_run(pilot['run_directory'], manifest)
        _mlx_identity(evaluation, 'mlx_sft', manifest, selected)
        _mlx_identity(evaluation, 'mlx_base', manifest, None)
        _fresh_output(self.output, manifest, additional_roots=(pilot['run_directory'],))
        binding = object_hash({'evaluation': evaluation['sha256'], 'pilot': pilot['sha256'],
            'selected': selected, 'output': str(self.output), 'arm': self.arm,
            'execution_sources': _execution_sources()})
        return pilot, evaluation, binding

    def __call__(self):
        return GateObservation(True, self.checked()[2])


class OriginalMlxEvalExecutor(OriginalPilotExecutor):
    def __init__(self, plan, pilot_plan, arm, output, evidence, **options):
        super().__init__(plan, evidence, **options)
        self.gate = OriginalEvaluationGate(plan, pilot_plan, arm, output)
        self.arm = arm
        self.run_directory = Path(output)

    def __call__(self):
        if self.process is not None or self.cli_start_attempts:
            raise WindowError('evaluation_callback_already_used')
        self.check_request_binding()
        pilot, evaluation, binding = self.gate.checked()
        self.check_request_binding()
        from am_style_r5 import pilot as pilot_module
        from am_style_r5.common import read_json
        from am_style_r5.runners import _validate_worker_completion, _parent_interruption
        package = Path(pilot_module.__file__).resolve().parents[2]
        _, env, _ = self.command(self.plan, pilot)
        python = pilot['python_interpreter']['path']
        argv = [python, str(package/'run.py'), 'eval-mlx', '--plan', str(self.plan),
                '--manifest', pilot['manifest']['path'], '--arm', self.arm,
                '--output', str(self.run_directory), '--worker-timeout', '300', '--execute']
        if self.arm == 'mlx_sft':
            argv += ['--sft-run', pilot['run_directory']]
        commands = {'cli': argv, 'mlx_worker': [python, '-m', 'am_style_r5.runners',
                    '--mlx-worker', str(self.run_directory/'request.json'), '--execute']}
        self.evidence.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.evidence.is_symlink():
            raise WindowError('evaluation_evidence_symlink')
        self.evidence.chmod(0o700)
        reason = self.supervise()
        if reason:
            raise WindowError('evaluation_cancelled_before_cli:' + reason)
        self.check_request_binding()
        self.guard = BoundProcessGuard.create(self.evidence/'processes.json', python=python,
            commands=commands, request_id=self.request_id, plan_path=self.plan,
            run_directory=self.run_directory, sources={'runtime': pilot['runtime_source_binding'],
                'run.py': digest(package/'run.py'), 'native_eval.py': digest(__file__)})
        started = time.time(); deadline = min(self.expires_at or started+360, started+360)
        reason = None; code = None
        log = self.evidence/'original-cli.log'
        try:
            fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                self.cli_start_attempts += 1
                self.process = subprocess.Popen(argv, cwd=package, env=env, stdin=subprocess.DEVNULL,
                    stdout=stream, stderr=subprocess.STDOUT, start_new_session=True, shell=False)
                self.guard.refresh()
                while self.process.poll() is None:
                    self.guard.refresh(); reason = self.supervise()
                    if time.time() >= deadline:
                        reason = 'evaluation_request_or_window_expired'
                    if reason:
                        self.guard.stop(reason, reap=self.process.poll); break
                    time.sleep(.25)
                code = self.process.poll()
        except BaseException as exc:
            reason = 'evaluation_supervision_exception:' + type(exc).__name__
            if self.process is not None:
                self.guard.stop(reason, reap=self.process.poll); code = self.process.poll()
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
        parent_path = self.run_directory/'parent-result.json'
        if parent_path.is_file() and not parent_path.is_symlink():
            parent = read_json(parent_path)
            if parent.get('plan_sha256') != evaluation['sha256'] or parent.get('arm') != self.arm:
                raise WindowError('evaluation_parent_binding')
            self.guard.report_result_groups([{'attempted': True, 'pid': parent.get('pid')}])
        stopped = self.stopped()
        complete = code == 0 and stopped and reason is None
        request_path = self.run_directory/'request.json'
        if complete:
            _validate_worker_completion(self.run_directory, read_json(request_path))
        elif stopped and request_path.is_file() and not (self.run_directory/'parent-interruption-record.json').exists():
            # Preserve any full outputs and the first interrupted attempt. The
            # original collector retains all remaining cases as missing.
            _parent_interruption(self.run_directory, read_json(request_path),
                'cancelled' if reason else 'failed', reason or 'original_runner_failed', stopped=True)
        process_snapshot = self.evidence/'processes-at-return.json'
        private_json(process_snapshot, self.guard.data, exclusive=True)
        result = {'schema': 'am-r7-mlx-window-execution/1', 'request_id': self.request_id,
                  'plan_sha256': digest(self.plan), 'binding_sha256': binding, 'arm': self.arm,
                  'command': argv, 'started': started, 'finished': time.time(),
                  'cli_pid': self.process.pid if self.process else None, 'cli_returncode': code,
                  'stop_confirmed': stopped, 'stop_reason': reason,
                  'status': 'completed' if complete else 'blocked_or_failed',
                  'process_ledger_sha256': digest(process_snapshot),
                  'process_ledger_snapshot': process_snapshot.name, 'log_sha256': digest(log)}
        path = self.evidence/'execution.json'; private_json(path, result, exclusive=True)
        return ExecutionObservation(result['status'], stopped, digest(path))
