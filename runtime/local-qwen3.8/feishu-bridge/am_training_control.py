"""Private R7 training requests executed inside the original live consumer.

The diagnostic port retains its diagnostic/recover contract. This separate
port accepts only a sealed local pilot plan, a unique request/run identity and
an expiry. Cancellation and restart recovery never start or resume training.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import uuid

from am_resource_window import NativeResourceWindow, REPO
from am_training_window.native_r5 import OriginalPilotExecutor
from am_training_window.native_eval import OriginalEvaluationGate, OriginalMlxEvalExecutor
from am_training_window.process_guard import BoundProcessGuard, digest, private_json
from am_training_window.r5_gate import OriginalR5InputGate
from am_training_window.native_synthetic import SyntheticInputGate, SyntheticExecutor
from am_training_window.window import Journal, TrainingWindow, WindowError, _check_host


class NativeTrainingControl(NativeResourceWindow):
    def __init__(self, bridge):
        super().__init__(bridge)
        self.root = bridge.root / 'training-window-r7'
        self.root.mkdir(mode=0o700, exist_ok=True)
        if self.root.is_symlink():
            raise WindowError('training_control_directory_symlink')
        self.root.chmod(0o700)
        self.plan_scope = (REPO / 'AM-HARNESS-20261031-01/R7/private').resolve()

    def information_demand(self, info):
        """Reuse the extraction worker's exact deterministic/model boundary.

        A job named 'extract' can be a trusted field mapping with no model call.
        Inspect only routing metadata (manual provenance when needed), never
        change a job state, and retain malformed/unknown jobs as demand.
        """
        from am_info_sync.models import Source
        from am_info_sync.facts import _structured_enabled, _trusted_manual_actor
        result = {'model_required': 0, 'deterministic_io': 0, 'unknown': 0}
        db = info.root/'information.sqlite3'
        with sqlite3.connect(db.resolve().as_uri()+'?mode=ro', uri=True, timeout=.3) as connection:
            connection.execute('BEGIN')
            for (raw,) in connection.execute("SELECT payload_json FROM jobs WHERE kind='extract' AND state IN ('pending','leased')"):
                try:
                    job = json.loads(raw)
                    row = connection.execute('SELECT config_json FROM sources WHERE source_id=?', (job['source_id'],)).fetchone()
                    source = Source.from_dict(json.loads(row[0]))
                    deterministic = _structured_enabled(source) and bool(source.settings.get('fact_mappings'))
                    if source.kind == 'manual':
                        row = connection.execute('SELECT payload_json FROM observations WHERE source_id=? AND object_id=? AND version=?',
                            (job['source_id'], job['object_id'], job['version'])).fetchone()
                        payload = json.loads(row[0])
                        deterministic = _trusted_manual_actor(source, payload) and bool(payload.get('structured_claims'))
                    key = 'deterministic_io' if deterministic and not source.settings.get('extract_text', False) else 'model_required'
                    result[key] += 1
                except (ValueError, TypeError, KeyError, IndexError):
                    result['unknown'] += 1
            connection.rollback()
        return result

    def queue_counts(self):
        """Classify with the host's own routing; preserve pure-I/O work."""
        now = self.store.clock()
        counts = {'harness': self.store.rows(
            "SELECT count(*) AS n FROM am_harness_runs WHERE state IN ('queued','running')")[0]['n']}
        rows = self.store.rows("SELECT * FROM am_inbox WHERE state IN "
            "('received','processing','harness_admitting','source_wait') AND expires>?", (now,))
        counts['am_model_demand'] = sum(row['state'] != 'source_wait' and
            self.bridge.am_self.training_requires_model(dict(row)) for row in rows)
        counts['source_wait'] = sum(row['state'] == 'source_wait' and
            self.bridge.am_self.training_requires_model(dict(row)) for row in rows)
        counts['tasks'] = self.store.rows("SELECT count(*) AS n FROM tasks WHERE state NOT IN "
            "('done','completed','expired','timed_out','failed','cancelled','canceled','rejected')")[0]['n']
        with self.bridge.mutex:
            chat_rows = self.bridge.db.execute("SELECT text FROM inbox WHERE state NOT IN "
                "('done','completed','expired','timed_out','failed','cancelled','canceled','rejected','sending')").fetchall()
        counts['ordinary_chat'] = sum(self.bridge.ordinary_chat_requires_model(row[0]) for row in chat_rows)
        ledger = self.bridge.group_ledger
        if ledger is not None:
            with ledger.mutex:
                group_rows = ledger.db.execute("SELECT * FROM ledger_inbox WHERE state IN ('queued','processing')").fetchall()
            counts['group_ledger_model'] = sum(not ledger.can_process_without_model(dict(row)) for row in group_rows)
        info = self.bridge.information
        information = {'model_required': 0, 'deterministic_io': 0, 'unknown': 0}
        if info is not None:
            information = self.information_demand(info)
            counts['information_extract'] = information['model_required'] + information['unknown']
        conversation = getattr(self.bridge.am_self, 'conversation', None)
        finalizer = getattr(getattr(conversation, 'agent', None), 'output_finalizer', None)
        if finalizer is not None:
            counts['original_probe_queue'] = len(list((finalizer.directory/'requests').glob('*.json')))
            future = getattr(finalizer, 'future', None)
            counts['original_probe_active'] = int(future is not None and not future.done())
        counts['conversation_futures'] = sum(not f.done() for f in
            list(getattr(self.bridge.am_self, 'conversation_futures', ())))
        self.last_queue_classification = {
            'model_demand': counts.copy(),
            'information_jobs': information,
            'pure_io_am_rows': sum(not self.bridge.am_self.training_requires_model(dict(r)) for r in rows),
            'delivery_outbox': self.store.rows("SELECT count(*) AS n FROM outbox WHERE state IN "
                "('pending','sending') AND expires>?", (now,))[0]['n'],
            'policy': 'original route classification; source_wait retained; deliveries and source I/O continue',
        }
        return counts

    def validate_request(self, request):
        operation = request.get('operation')
        keys = {'operation', 'request_id', 'expires_at'}
        if operation in ('train', 'eval-mlx', 'train-synthetic', 'eval-synthetic'):
            keys |= {'plan', 'plan_sha256', 'run_id'}
        if operation == 'eval-mlx':
            keys |= {'pilot_plan', 'pilot_plan_sha256', 'arm'}
        if set(request) != keys or operation not in ('train', 'eval-mlx', 'train-synthetic', 'eval-synthetic', 'recover'):
            raise WindowError('training_request_schema')
        request_id = request['request_id']
        if str(uuid.UUID(request_id)) != request_id:
            raise WindowError('training_request_id_not_canonical')
        expiry = request['expires_at']
        if type(expiry) not in (float, int) or not math.isfinite(expiry) or not time.time() < expiry <= time.time()+3600:
            raise WindowError('training_request_expired_or_unbounded')
        if operation in ('train', 'eval-mlx', 'train-synthetic', 'eval-synthetic'):
            path = Path(request['plan'])
            if (not path.is_absolute() or path.is_symlink() or not path.resolve().is_relative_to(self.plan_scope)
                    or path.name != 'plan.json' or digest(path) != request['plan_sha256']):
                raise WindowError('training_plan_scope_or_digest')
            plan = json.loads(path.read_text())
            if operation in ('train-synthetic','eval-synthetic') and plan.get('mode') != ('train' if operation=='train-synthetic' else 'evaluation'):
                raise WindowError('synthetic_operation_plan_mismatch')
            if operation == 'eval-mlx':
                original = Path(request['pilot_plan'])
                if (not original.is_absolute() or original.is_symlink()
                        or not original.resolve().is_relative_to(self.plan_scope)
                        or digest(original) != request['pilot_plan_sha256']
                        or request['arm'] not in ('mlx_base', 'mlx_sft')):
                    raise WindowError('evaluation_pilot_or_arm_binding')
                plan = json.loads(original.read_text())
            if (plan['runtime_source_binding']['roots'] != {
                    'am_style': str(REPO/'am-conversation-r1/src'), 'training': str(REPO/'am-style-r3')}
                    or Path(plan['manifest']['path']) != REPO/'AM-HARNESS-20261031-01/R4/private/mlx-base.json'):
                raise WindowError('training_requires_current_original_runtime')
            if not isinstance(request['run_id'], str) or re.fullmatch('[A-Za-z0-9_-]{1,100}', request['run_id']) is None:
                raise WindowError('training_run_identity_invalid')
            run = Path(plan['run_directory']) if operation in ('train','train-synthetic','eval-synthetic') else self.plan_scope/request['run_id']
            if not run.is_absolute() or not run.resolve().is_relative_to(self.plan_scope) or run.name != request['run_id']:
                raise WindowError('training_run_binding')
        return request_id

    def cancelled(self, request):
        path = self.root / (request['request_id'] + '.cancel.json')
        if not path.exists():
            return False
        st = path.lstat()
        if path.is_symlink() or st.st_mode & 0o077 or st.st_uid != os.getuid() or st.st_size > 16000:
            raise WindowError('invalid_private_cancel')
        value = json.loads(path.read_text())
        if (set(value) != {'request_id', 'plan_sha256', 'expires_at'}
                or value['request_id'] != request['request_id'] or value['plan_sha256'] != request['plan_sha256']
                or type(value['expires_at']) not in (float, int) or not math.isfinite(value['expires_at'])
                or value['expires_at'] <= time.time()):
            raise WindowError('cancel_binding_or_expiry')
        return True

    def run(self, path):
        result = {'ok': False, 'trainer_start_attempts': 0, 'real_platform_sends': 0}
        executor = None
        try:
            sys.path[:0] = [str(REPO/'am-style-r5/src'), str(REPO/'am-conversation-r1/src'), str(REPO/'am-style-r3')]
            request = json.loads(path.read_text())
            request_id = self.validate_request(request)
            receipt = self.root / (request_id + '.receipt.json')
            journal_path = self.root / (request_id + '.journal.json')
            evidence = self.root / (request_id + '.evidence')
            if request['operation'] != 'recover':
                # Durable idempotency before any lifecycle or launch action.
                private_json(receipt, request, exclusive=True)
                run_marker = self.root / ('run-' + request['run_id'] + '.receipt.json')
                raw_gate = (SyntheticInputGate(request['plan']) if request['operation'] in ('train-synthetic','eval-synthetic') else
                        OriginalR5InputGate(request['plan']) if request['operation'] == 'train' else
                        OriginalEvaluationGate(request['plan'], request['pilot_plan'], request['arm'],
                                               self.plan_scope/request['run_id']))
                def gate():
                    self.validate_request(request)
                    value = raw_gate()
                    self.validate_request(request)
                    return value
                gate()
                if self.cancelled(request):
                    raise WindowError('cancelled_before_window')
                private_json(run_marker, {'request_id': request_id, 'plan_sha256': request['plan_sha256']}, exclusive=True)
            else:
                original_request = json.loads(receipt.read_text())
                if original_request['request_id'] != request_id:
                    raise WindowError('recovery_request_binding')
            host, binding, lifecycle, lock, probe = self.setup()
            if request['operation'] == 'recover':
                journal = Journal.load(journal_path)
                stop_check = None
                if journal.data['trainer_invoked']:
                    guard = BoundProcessGuard.load(evidence/'processes.json')
                    if guard.data['request_id'] != request_id or guard.data['plan_sha256'] != original_request['plan_sha256']:
                        raise WindowError('recovery_process_binding')
                    guard.stop('explicit_restart_recovery')
                    stop_check = guard.stopped
                output = TrainingWindow(host, binding, lock, probe, journal).recover(stop_check)
            else:
                journal = Journal.create(journal_path, 'training', binding)
                journal.write('created', request_id=request_id, plan=request['plan'],
                              operation=request['operation'],
                              plan_sha256=request['plan_sha256'], run_id=request['run_id'],
                              expires_at=request['expires_at'], evidence_directory=str(evidence))
                history = []; initial_swap = None; last_metrics = 0

                def supervise():
                    nonlocal initial_swap, last_metrics
                    if self.cancelled(request):
                        return 'explicit_cancel'
                    if time.time() >= request['expires_at']:
                        return 'request_expired'
                    observed = host.inspect()
                    _check_host(observed, binding, journal.data['window_id'], model='absent')
                    if observed.pending_business or observed.active_inference:
                        return 'new_business_model_demand'
                    if time.monotonic()-last_metrics >= 1:
                        from training.mlx_r4_resources import check_metrics
                        metrics = probe(); last_metrics = time.monotonic()
                        if initial_swap is None:
                            initial_swap = metrics['swap_used_GiB']
                        metrics['queue_classification'] = self.last_queue_classification
                        history.append(metrics)
                        private_json(evidence/'supervision.json', history)
                        check_metrics(metrics, before_load=False, initial_swap_gib=initial_swap)
                    return None

                options = dict(request_id=request_id, supervise=supervise, expires_at=request['expires_at'],
                               expected_plan_sha256=request['plan_sha256'])
                if request['operation'] in ('train-synthetic','eval-synthetic'):
                    journal.write('created',execution_kind='synthetic_SFT' if request['operation']=='train-synthetic' else 'synthetic_evaluation')
                    executor = SyntheticExecutor(request['plan'], evidence, **options)
                elif request['operation'] == 'train':
                    executor = OriginalPilotExecutor(request['plan'], evidence, **options)
                else:
                    executor = OriginalMlxEvalExecutor(request['plan'], request['pilot_plan'], request['arm'],
                        self.plan_scope/request['run_id'], evidence, **options)
                output = TrainingWindow(host, binding, lock, probe, journal).train(
                    gate, executor, executor.stopped)
            result.update(ok=output['status'] in ('completed', 'recovered'), window=output,
                          lifecycle=lifecycle.events, final_observation=probe(),
                          queue_classification=self.last_queue_classification,
                          recover_request={'operation': 'recover', 'request_id': request_id,
                                           'expires_at': 'set to current UTC epoch seconds + 900'})
        except BaseException as exc:
            result['error'] = str(exc) if isinstance(exc, (WindowError, FileExistsError)) else type(exc).__name__
        if executor is not None:
            result['cli_start_attempts'] = executor.cli_start_attempts
            result['trainer_start_attempts'] = sum(p['role']=='fit' for p in
                (executor.guard.data['processes'] if executor.guard else []))
            if executor.result is not None:
                result['trainer_start_attempts'] = max(result['trainer_start_attempts'],
                    sum(p.get('phase')=='fit' and p.get('attempted') is True for p in executor.result.get('phases', [])))
        result['finished_at'] = time.time()
        private_json(path.with_name(path.name.replace('.request.claimed', '.result.json')), result)
