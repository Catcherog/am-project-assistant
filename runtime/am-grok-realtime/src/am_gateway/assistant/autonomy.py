"""Persistent RSI scheduling in the existing AM improvement lane.

Feedback creates a bounded job, not a new chat or a Feishu request. A process
lock covers host/CLI concurrency. Only the engine can promote configurations;
code jobs return isolated candidates and never edit the running source tree.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import fcntl
import json
import math
from pathlib import Path
import threading
import time
from zoneinfo import ZoneInfo

from ..models import ContractError, dumps, key
from .evolution import ReplayBudget


@dataclass(frozen=True)
class AutonomyBudget:
    max_jobs_per_day: int = 4
    max_active_seconds_per_day: float = 3600
    min_idle_seconds: float = 30
    cooldown_seconds: float = 600
    max_job_seconds: float = 1800
    max_deferrals: int = 100
    timezone: str = 'Asia/Shanghai'

    def validate(self):
        for field, minimum, maximum in [('max_jobs_per_day', 1, 100), ('max_deferrals', 1, 1000)]:
            value = getattr(self, field)
            if type(value) is not int or not minimum <= value <= maximum:
                raise ContractError('rsi_invalid_' + field)
        for field, minimum, maximum in [('max_active_seconds_per_day', 1, 86400),
                ('min_idle_seconds', 0, 3600), ('cooldown_seconds', 0, 86400),
                ('max_job_seconds', 1, 86400)]:
            value = getattr(self, field)
            if type(value) not in (int, float) or not math.isfinite(value) or not minimum <= value <= maximum:
                raise ContractError('rsi_invalid_' + field)
        ZoneInfo(self.timezone)
        return self


class AutonomousRSI:
    def __init__(self, engine, coordinator, *, scope, development_ref, holdout_ref=None,
                 budget=None, replay_budget=None, code_worker=None, clock=time.time,
                 monotonic=time.monotonic, foreground_busy=None):
        self.engine, self.coordinator, self.scope = engine, coordinator, scope
        self.store, self.owner_id = coordinator.store, coordinator.owner_id
        if self.store.path != engine.store.path or self.owner_id != engine.owner_id:
            raise ContractError('rsi_host_identity_mismatch')
        self.development_ref, self.holdout_ref = development_ref, holdout_ref
        self.budget = AutonomyBudget(**(budget or {})).validate()
        self.replay_budget = ReplayBudget(**(replay_budget or {})).validate()
        self.code_worker, self.clock, self.monotonic = code_worker, clock, monotonic
        self.foreground_busy = foreground_busy or self._host_busy
        self.runtime_consumers, self.last = [], {'state': 'not_started'}
        self._idle_since, self._thread_lock = None, threading.Lock()
        self.lock_path = Path(self.store.path + '.rsi-' + key(self.owner_id, scope)[:16] + '.lock')
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r8_rsi_control(
                owner_id TEXT NOT NULL,scope TEXT NOT NULL,paused INTEGER NOT NULL DEFAULT 0,
                next_at REAL NOT NULL DEFAULT 0,updated REAL NOT NULL,
                PRIMARY KEY(owner_id,scope))''')
            db.execute('''CREATE TABLE IF NOT EXISTS r8_rsi_jobs(
                job_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,scope TEXT NOT NULL,kind TEXT NOT NULL,
                proposal_id TEXT NOT NULL,state TEXT NOT NULL,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,
                result_json TEXT NOT NULL DEFAULT '',result_hash TEXT NOT NULL DEFAULT '',
                created REAL NOT NULL,updated REAL NOT NULL,started_day TEXT NOT NULL DEFAULT '',
                active_seconds REAL NOT NULL DEFAULT 0,deferrals INTEGER NOT NULL DEFAULT 0,
                UNIQUE(owner_id,scope,kind,proposal_id))''')
            db.execute('''CREATE TABLE IF NOT EXISTS r8_rsi_usage(
                owner_id TEXT NOT NULL,scope TEXT NOT NULL,day TEXT NOT NULL,
                jobs_started INTEGER NOT NULL DEFAULT 0,active_seconds REAL NOT NULL DEFAULT 0,
                PRIMARY KEY(owner_id,scope,day))''')
            db.execute('INSERT OR IGNORE INTO r8_rsi_control VALUES(?,?,0,0,?)',
                       (self.owner_id, scope, self.clock()))
        # The engine checks this before EACH replay attempt. Existing default
        # engines ignore this field until upgraded; startup validates capability.
        if not hasattr(engine, 'should_defer'):
            raise ContractError('rsi_checkpoint_capable_engine_required')
        engine.should_defer = self.should_yield

    def _host_busy(self):
        present = {r['name'] for r in self.store.rows("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'r5_intake' in present:
            row = self.store.one("SELECT count(*) AS n FROM r5_intake WHERE state IN ('pending','processing','source_wait')")
            if row['n']:
                return True
        if 'h1_tasks' in present:
            row = self.store.one("""SELECT count(*) AS n FROM h1_tasks
                WHERE dispatch_state IN ('pending','dispatching','accepted','unknown')
                AND execution_state NOT IN ('succeeded','failed','cancelled')""")
            if row['n']:
                return True
        return False

    def paused(self):
        return bool(self.store.one('SELECT paused FROM r8_rsi_control WHERE owner_id=? AND scope=?',
                                  (self.owner_id, self.scope))['paused'])

    def set_paused(self, paused):
        if type(paused) is not bool:
            raise ContractError('rsi_boolean_pause_required')
        with self.store.transaction() as db:
            db.execute('UPDATE r8_rsi_control SET paused=?,updated=? WHERE owner_id=? AND scope=?',
                       (int(paused), self.clock(), self.owner_id, self.scope))
        return {'state': 'paused' if paused else 'resumed', 'running_call_stop_proven': False}

    def should_yield(self, context=None):
        busy = self.paused() or bool(self.foreground_busy())
        if busy:
            self._idle_since = None
        return busy

    def _day(self):
        return datetime.fromtimestamp(self.clock(), ZoneInfo(self.budget.timezone)).date().isoformat()

    def _usage(self):
        return self.store.one('SELECT * FROM r8_rsi_usage WHERE owner_id=? AND scope=? AND day=?',
                             (self.owner_id, self.scope, self._day())) or {'jobs_started': 0, 'active_seconds': 0}

    def _enqueue(self):
        active = self.engine.active_config(self.scope)
        if not active:
            raise ContractError('rsi_baseline_missing')
        proposals = self.coordinator.improvement.list_proposals(limit=200)
        with self.store.transaction() as db:
            for proposal in reversed(proposals):
                codes = {item['code'] for item in proposal.get('issues', [])}
                kinds = ['config']
                if self.code_worker and codes.intersection({'latency_review', 'latency_feedback',
                    'accuracy_feedback', 'possible_false_no_match', 'source_unavailable', 'execution_failed'}):
                    kinds.insert(0, 'code')
                for kind in kinds:
                    payload = {'proposal_id': proposal['proposal_id'], 'proposal_hash': key(proposal),
                        'kind': kind, 'runtime_revision': self.engine.runtime_revision,
                        'parent_version': active['version'], 'scope': self.scope,
                        'development_ref': self.development_ref, 'holdout_ref': self.holdout_ref,
                        'authorization': 'user_cli_autonomy_2026-11-11',
                        'production_deployment_authorized': False}
                    job_id = 'rsi_' + key(self.owner_id, self.scope, kind, proposal['proposal_id'])[:40]
                    db.execute('''INSERT OR IGNORE INTO r8_rsi_jobs
                        (job_id,owner_id,scope,kind,proposal_id,state,payload_json,payload_hash,created,updated)
                        VALUES(?,?,?,?,?,'pending',?,?,?,?)''', (job_id, self.owner_id, self.scope, kind,
                            proposal['proposal_id'], dumps(payload), key(payload), self.clock(), self.clock()))

    @staticmethod
    def _payload(row):
        payload = json.loads(row['payload_json'])
        if key(payload) != row['payload_hash']:
            raise ContractError('rsi_job_integrity_error')
        return payload

    def _run_config(self, job, payload):
        # Reuse a checkpointed candidate. Do not spend another CLI proposal call.
        candidates = self.store.rows('SELECT * FROM r7_evo_candidates WHERE owner_id=? AND scope=? ORDER BY created',
                                     (self.owner_id, self.scope))
        existing = next((r for r in candidates if json.loads(r['payload_json']).get('proposal_id') == job['proposal_id']
                         and json.loads(r['payload_json']).get('runtime_revision') == self.engine.runtime_revision
                         and json.loads(r['payload_json']).get('parent_version') == payload['parent_version']), None)
        if existing:
            candidate = {'candidate_id': existing['candidate_id'], **self.engine._decoded(existing)}
            if existing['state'] in {'active', 'rolled_back', 'superseded'}:
                return {'state': 'already_resolved', 'candidate_id': existing['candidate_id']}
            if existing['state'] == 'eligible':
                run = self.store.one('SELECT * FROM r7_evo_runs WHERE run_id=?', (existing['active_run_id'],))
                report = self.engine._decoded(run, payload='report_json', digest='report_hash')
                if self.should_yield():
                    return {**report, 'state': 'deferred', 'reason': 'activation_waits_for_idle'}
                report['activation'] = self.engine.activate(candidate['candidate_id'],
                    expected_version=candidate['parent_version'])
                return report
        else:
            candidate = self.engine.propose_from_journal(self.coordinator.improvement,
                scope=self.scope, development_ref=self.development_ref, holdout_ref=self.holdout_ref,
                budget=self.replay_budget, proposal_id=job['proposal_id'])
        if not candidate.get('candidate_id'):
            receipt = getattr(self.engine.proposer, 'last_result', None)
            if candidate.get('state') == 'proposal_failed' and isinstance(receipt, dict):
                if receipt.get('state') == 'cancelled' and receipt.get('local_process_stopped') is True:
                    return {'state': 'deferred', 'reason': 'proposal_preempted', 'proposal_attempt': candidate,
                            'local_process_stopped': True, 'provider_cancellation_proven': False,
                            'next_attempt_uses_new_workspace': True}
                if receipt.get('local_process_stopped') is False:
                    return {'state': 'unknown', 'reason': 'proposal_stop_unresolved', 'proposal_attempt': candidate}
            return candidate
        report = self.engine.replay(candidate['candidate_id'])
        if report.get('eligible') and not self.should_yield():
            report['activation'] = self.engine.activate(candidate['candidate_id'],
                expected_version=candidate['parent_version'])
        elif report.get('eligible'):
            report = {**report, 'state': 'deferred', 'reason': 'activation_waits_for_idle'}
        return report

    def _result(self, job, result, elapsed, day):
        pending = result.get('state') in {'deferred', 'busy', 'paused'}
        state = 'deferred' if pending else 'completed'
        if result.get('state') in {'failed', 'unknown', 'interrupted', 'stop_unknown', 'interrupted_unknown'}:
            state = 'unknown' if 'unknown' in result['state'] or result['state'] == 'interrupted' else 'failed'
        if 'local_process_stopped' in result and result['local_process_stopped'] is not True:
            state = 'unknown'
        if pending and job['deferrals'] + 1 >= self.budget.max_deferrals:
            state = 'deferred_limit'
        summary = {'job_id': job['job_id'], 'kind': job['kind'], 'proposal_id': job['proposal_id'],
            'state': state, 'result': result, 'elapsed_seconds_this_step': elapsed,
            'business_effects_applied_by_scheduler': False, 'production_deployment_performed': False,
            'report_delivery': 'local_record_only'}
        with self.store.transaction() as db:
            db.execute('''UPDATE r8_rsi_jobs SET state=?,result_json=?,result_hash=?,updated=?,
                active_seconds=active_seconds+?,deferrals=deferrals+? WHERE job_id=?''',
                (state, dumps(summary), key(summary), self.clock(), elapsed, int(pending), job['job_id']))
            db.execute('''UPDATE r8_rsi_usage SET active_seconds=active_seconds+?
                WHERE owner_id=? AND scope=? AND day=?''', (elapsed, self.owner_id, self.scope, day))
            db.execute('UPDATE r8_rsi_control SET next_at=?,updated=? WHERE owner_id=? AND scope=?',
                (self.clock() + (0 if pending else self.budget.cooldown_seconds), self.clock(),
                 self.owner_id, self.scope))
        return summary

    def step(self, *, force=False):
        # force means inspect pending work now; it never bypasses pause, idle,
        # daily budgets, evaluation, a version conflict or an unknown attempt.
        if not self._thread_lock.acquire(blocking=False):
            return {'state': 'busy'}
        try:
            with self.lock_path.open('a') as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return {'state': 'busy'}
                try:
                    self.last = self._step_locked(force=force)
                    return self.last
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)
        finally:
            self._thread_lock.release()

    def _step_locked(self, *, force):
        if self.should_yield():
            self._idle_since = None
            return {'state': 'paused' if self.paused() else 'deferred_for_interactive_work'}
        now = self.monotonic()
        if self._idle_since is None:
            self._idle_since = now
        if now - self._idle_since < self.budget.min_idle_seconds:
            return {'state': 'waiting_for_idle_window'}
        with self.store.transaction() as db:
            # flock proves no other scheduler owns this scope. A previous
            # running job lost its scheduler; no blind replay of external work.
            db.execute("UPDATE r8_rsi_jobs SET state='unknown',updated=? WHERE owner_id=? AND scope=? AND state='running'",
                       (self.clock(), self.owner_id, self.scope))
        if self.store.one("SELECT job_id FROM r8_rsi_jobs WHERE owner_id=? AND scope=? AND state='unknown' LIMIT 1",
                          (self.owner_id, self.scope)):
            return {'state': 'needs_reconciliation', 'automatic_retry': False}
        control = self.store.one('SELECT * FROM r8_rsi_control WHERE owner_id=? AND scope=?', (self.owner_id, self.scope))
        if not force and self.clock() < control['next_at']:
            return {'state': 'cooldown'}
        usage = self._usage()
        if usage['active_seconds'] >= self.budget.max_active_seconds_per_day:
            return {'state': 'daily_time_budget_reached'}
        self._enqueue()
        job = self.store.one("""SELECT * FROM r8_rsi_jobs WHERE owner_id=? AND scope=?
            AND state IN ('pending','deferred') ORDER BY CASE state WHEN 'deferred' THEN 0 ELSE 1 END,
            created,CASE kind WHEN 'code' THEN 0 ELSE 1 END,job_id LIMIT 1""", (self.owner_id, self.scope))
        if not job:
            return {'state': 'idle', 'reason': 'no_new_improvement_signal'}
        if not job['started_day'] and usage['jobs_started'] >= self.budget.max_jobs_per_day:
            return {'state': 'daily_job_budget_reached'}
        payload = self._payload(job)
        active = self.engine.active_config(self.scope)
        if payload['runtime_revision'] != self.engine.runtime_revision or active['version'] != payload['parent_version']:
            with self.store.transaction() as db:
                db.execute("UPDATE r8_rsi_jobs SET state='stale_baseline',updated=? WHERE job_id=?", (self.clock(), job['job_id']))
            return {'state': 'stale_baseline', 'job_id': job['job_id'], 'automatic_rebase': False}
        remaining = min(self.budget.max_job_seconds - job['active_seconds'],
                        self.budget.max_active_seconds_per_day - usage['active_seconds'])
        if remaining <= 0:
            with self.store.transaction() as db:
                db.execute("UPDATE r8_rsi_jobs SET state='budget_exhausted',updated=? WHERE job_id=?",
                           (self.clock(), job['job_id']))
            return {'state': 'job_time_budget_reached', 'job_id': job['job_id']}
        day = self._day()
        with self.store.transaction() as db:
            db.execute('INSERT OR IGNORE INTO r8_rsi_usage VALUES(?,?,?,0,0)', (self.owner_id, self.scope, day))
            db.execute('UPDATE r8_rsi_usage SET jobs_started=jobs_started+? WHERE owner_id=? AND scope=? AND day=?',
                       (int(not job['started_day']), self.owner_id, self.scope, day))
            db.execute("UPDATE r8_rsi_jobs SET state='running',started_day=CASE WHEN started_day='' THEN ? ELSE started_day END,updated=? WHERE job_id=?",
                       (day, self.clock(), job['job_id']))
        began = self.monotonic()
        timed_out = lambda: self.monotonic() - began >= remaining
        old_defer = self.engine.should_defer
        old_remaining = getattr(self.engine, 'host_remaining_seconds', None)
        self.engine.should_defer = lambda context=None: self.should_yield() or timed_out()
        self.engine.host_remaining_seconds = lambda: max(0, remaining - (self.monotonic() - began))
        try:
            if job['kind'] == 'config':
                result = self._run_config(job, payload)
            else:
                proposal = next(p for p in self.coordinator.improvement.list_proposals(200)
                                if p['proposal_id'] == job['proposal_id'])
                if key(proposal) != payload['proposal_hash']:
                    raise ContractError('rsi_proposal_changed')
                result = self.code_worker.run(reason=proposal,
                    cancelled=lambda: self.should_yield() or timed_out())
            if timed_out() and result.get('state') == 'deferred':
                result = {**result, 'state': 'budget_exhausted'}
        except Exception as exc:
            result = {'state': 'failed', 'error_type': type(exc).__name__}
        finally:
            self.engine.should_defer = old_defer
            self.engine.host_remaining_seconds = old_remaining
        return self._result(job, result, max(0, self.monotonic() - began), day)

    def status(self):
        states = self.store.rows('''SELECT state,count(*) AS n FROM r8_rsi_jobs
            WHERE owner_id=? AND scope=? GROUP BY state''', (self.owner_id, self.scope))
        return {**self.engine.status(), 'autonomous': True, 'paused': self.paused(),
            'scope': self.scope, 'jobs': {r['state']: r['n'] for r in states},
            'daily_usage': self._usage(), 'budget': asdict(self.budget),
            'last_cycle': self.last, 'runtime_consumers': self.runtime_consumers,
            'code_worker_bound': self.code_worker is not None,
            'production_deployment_performed': False}

    def reports(self, limit=20):
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ContractError('rsi_invalid_report_limit')
        rows = self.store.rows('''SELECT result_json,result_hash FROM r8_rsi_jobs
            WHERE owner_id=? AND scope=? AND result_json<>'' ORDER BY updated DESC LIMIT ?''',
            (self.owner_id, self.scope, limit))
        result = []
        for row in rows:
            report = json.loads(row['result_json'])
            if key(report) != row['result_hash']:
                raise ContractError('rsi_report_integrity_error')
            result.append(report)
        return result
