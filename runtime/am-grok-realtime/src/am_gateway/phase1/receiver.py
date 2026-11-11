"""Durable task receiver shared by realtime wakes and recovery polling.

An acceptance acknowledges this SQLite receiver, not a third-party Bot run.
Only a successful worker claim records ``running``. Abandoned running work is
kept unknown until its original worker reconciles; it is never blindly rerun.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import math
import secrets
import uuid

from ..models import ContractError, dumps, key
from .contracts import ANSWER_KINDS, Capability, INTENTS, TERMINAL, validate_envelope


_SCHEMA = (
    """CREATE TABLE IF NOT EXISTS h4_receiver_runs(
        namespace TEXT NOT NULL, operation_id TEXT NOT NULL, task_id TEXT NOT NULL,
        run_id TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL, generation INTEGER NOT NULL,
        attempt_id TEXT NOT NULL, envelope_json TEXT NOT NULL, session_key TEXT NOT NULL,
        memory_key TEXT NOT NULL, state TEXT NOT NULL, accepted_at REAL NOT NULL,
        started_at REAL, ended_at REAL, lease_until REAL, token_hash TEXT NOT NULL DEFAULT '',
        cancel_requested INTEGER NOT NULL DEFAULT 0, result_json TEXT,
        worker_id TEXT NOT NULL DEFAULT '', last_error TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(namespace, operation_id))""",
    """CREATE TABLE IF NOT EXISTS h4_receiver_events(
        seq INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT NOT NULL,
        operation_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE,
        task_id TEXT NOT NULL, generation INTEGER NOT NULL, kind TEXT NOT NULL,
        payload_json TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0)""",
    """CREATE INDEX IF NOT EXISTS h4_receiver_queue
        ON h4_receiver_runs(namespace,state,accepted_at)""",
    """CREATE TABLE IF NOT EXISTS h4_receiver_memory(
        seq INTEGER PRIMARY KEY AUTOINCREMENT, namespace TEXT NOT NULL,
        memory_key TEXT NOT NULL, operation_id TEXT NOT NULL, created REAL NOT NULL,
        role TEXT NOT NULL, body TEXT NOT NULL,
        UNIQUE(namespace,operation_id,role))""",
    """CREATE INDEX IF NOT EXISTS h4_memory_lookup
        ON h4_receiver_memory(namespace,memory_key,created)""",
    """CREATE TABLE IF NOT EXISTS h4_receiver_workers(
        namespace TEXT NOT NULL, worker_id TEXT NOT NULL, seen_at REAL NOT NULL,
        PRIMARY KEY(namespace,worker_id))""",
)


def session_identity(envelope):
    source = envelope['source']
    principal = envelope['request_context']['acting_principal']
    return 'session_' + key(source['tenant_id'], source['chat_id'], principal)[:40]


def _memory_identity(envelope):
    # Revoked/narrowed authorizations never reuse a broader conversation memory.
    return key(session_identity(envelope), envelope['policy'])


def validate_result(result, run_id):
    if not isinstance(result, dict) or result.get('response_kind') not in ANSWER_KINDS:
        raise ContractError('receiver result kind required')
    if result.get('run_id', run_id) != run_id:
        raise ContractError('receiver result execution mismatch')
    if not isinstance(result.get('reply_text', ''), str):
        raise ContractError('receiver reply must be text')
    if not isinstance(result.get('remaining_requirements'), list) or not isinstance(result.get('artifacts'), list):
        raise ContractError('receiver result requirements and artifacts required')
    if len(result.get('reply_text', '').encode()) > 30000 or len(dumps(result).encode()) > 524288:
        raise ContractError('receiver result too large')
    # A worker result is data. It cannot change recipient, policy or tool rights.
    if set(result) & {'target', 'targets', 'recipients', 'policy', 'authorization_ref', 'tool_calls'}:
        raise ContractError('worker result cannot grant actions or authority')
    normalized = {**result, 'run_id': run_id}
    if normalized['response_kind'] in {'failed', 'unavailable'} and not normalized.get('reply_text', '').strip():
        normalized['reply_text'] = ('这次任务未完成：执行端运行出错。已保留原任务记录。'
                                    if normalized['response_kind'] == 'failed' else
                                    '这次任务未完成：执行端暂时无法处理。已保留原任务记录。')
    return normalized


def reconcile_worker_lease(harness, *, task_id, operation_id, run_id, generation,
                           status, lease_until):
    """A live, correlated worker heartbeat can renew only its existing execution."""
    if status not in {'running', 'cancel_requested'} or isinstance(lease_until, bool) or not isinstance(lease_until, (int, float)) or not math.isfinite(lease_until):
        return False
    now = harness.store.clock()
    if lease_until <= now:
        return False
    with harness.store.transaction() as db:
        task = harness.task(task_id, db)
        if (task['business_id'], task['run_id'], task['generation']) != (operation_id, run_id, generation):
            return False
        if task['execution_state'] in TERMINAL or task['execution_deadline'] <= now:
            return False
        db.execute('''UPDATE h1_tasks SET execution_state='running',lease_until=?,
            last_error=CASE WHEN last_error='execution_status_required' THEN '' ELSE last_error END
            WHERE task_id=?''', (min(lease_until, task['execution_deadline']), task_id))
        return True


class DurableReceiver:
    def __init__(self, store, *, namespace='grok_bot', intents=None,
                 memory_ttl_seconds=86400, history_limit=20, worker_lease_seconds=60):
        if not namespace or not 1 <= history_limit <= 100 or memory_ttl_seconds <= 0 or worker_lease_seconds <= 0:
            raise ContractError('invalid receiver configuration')
        self.store, self.namespace = store, namespace
        self.intents = tuple(intents if intents is not None else INTENTS[:3])
        if not self.intents or not set(self.intents) <= set(INTENTS):
            raise ContractError('receiver intent scope required')
        self.memory_ttl_seconds = memory_ttl_seconds
        self.history_limit = history_limit
        self.worker_lease_seconds = worker_lease_seconds
        with store.transaction() as db:
            for statement in _SCHEMA:
                db.execute(statement)

    def _row(self, db, operation_id):
        row = db.execute('SELECT * FROM h4_receiver_runs WHERE namespace=? AND operation_id=?',
                         (self.namespace, operation_id)).fetchone()
        if row is None:
            raise ContractError('unknown receiver operation')
        return dict(row)

    def _event(self, db, row, kind, payload):
        event_id = 'receiver_' + key(self.namespace, row['operation_id'], row['generation'], kind, payload)[:48]
        db.execute('''INSERT OR IGNORE INTO h4_receiver_events
            (namespace,operation_id,event_id,task_id,generation,kind,payload_json)
            VALUES(?,?,?,?,?,?,?)''', (self.namespace, row['operation_id'], event_id,
                row['task_id'], row['generation'], kind, dumps(payload)))
        return {'event_id': event_id, 'kind': kind, 'payload': payload}

    def _accepted(self, db, row):
        return self._event(db, row, 'accepted', {
            'run_id': row['run_id'], 'durable_record': 'sqlite:' + row['run_id'],
            'accepted_at': row['accepted_at'], 'accepted_by': 'gateway_receiver',
        })

    def accept(self, envelope):
        validate_envelope(envelope)
        policy, request_context = envelope['policy'], envelope['request_context']
        if (request_context.get('acting_principal'), request_context.get('requester_id')) != (
                policy['principal'], policy['principal']) or envelope['source']['chat_id'] not in policy['source_chats']:
            raise ContractError('receiver principal or source scope mismatch')
        if envelope['routing']['intent'] not in self.intents or envelope['routing']['adapter'] != self.namespace:
            raise ContractError('receiver adapter or intent mismatch')
        identity, execution = envelope['identity'], envelope['execution']
        operation_id = identity['business_operation_id']
        now = self.store.clock()
        encoded = dumps(envelope)
        if len(encoded.encode()) > 1048576:
            raise ContractError('receiver envelope too large')
        with self.store.transaction() as db:
            old = db.execute('SELECT * FROM h4_receiver_runs WHERE namespace=? AND operation_id=?',
                             (self.namespace, operation_id)).fetchone()
            if old:
                row = dict(old)
                if (row['task_id'], row['fingerprint'], row['generation'], row['session_key']) != (
                        identity['task_id'], identity['business_fingerprint'], execution['generation'],
                        session_identity(envelope)):
                    raise ContractError('same receiver operation has different identity or generation')
                return self._accepted(db, row)
            if execution['accept_deadline'] <= now:
                raise ContractError('receiver acceptance deadline passed')
            run_id = 'run_' + uuid.uuid4().hex
            db.execute('''INSERT INTO h4_receiver_runs(namespace,operation_id,task_id,run_id,
                fingerprint,generation,attempt_id,envelope_json,session_key,memory_key,state,accepted_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,'accepted',?)''',
                       (self.namespace, operation_id, identity['task_id'], run_id,
                        identity['business_fingerprint'], execution['generation'],
                        identity['dispatch_attempt_id'], encoded, session_identity(envelope),
                        _memory_identity(envelope), now))
            return self._accepted(db, self._row(db, operation_id))

    def _worker_seen(self, db, worker_id):
        if not isinstance(worker_id, str) or not worker_id or len(worker_id) > 128:
            raise ContractError('worker identity required')
        db.execute('''INSERT INTO h4_receiver_workers VALUES(?,?,?)
            ON CONFLICT(namespace,worker_id) DO UPDATE SET seen_at=excluded.seen_at''',
                   (self.namespace, worker_id, self.store.clock()))

    def expire_unclaimed(self, *, limit=100):
        """Finish only operations that provably never reached a worker.

        This runs during reconciliation too, so an absent Bot cannot strand the
        queue forever waiting for a claim call that will never arrive.
        """
        count, now = 0, self.store.clock()
        with self.store.transaction() as db:
            rows = db.execute('''SELECT * FROM h4_receiver_runs WHERE namespace=?
                AND state='accepted' ORDER BY accepted_at LIMIT ?''', (self.namespace, limit)).fetchall()
            for raw in rows:
                row = dict(raw)
                envelope = json.loads(row['envelope_json'])
                if envelope['execution']['completion_deadline'] > now:
                    continue
                result = {'response_kind': 'unavailable',
                    'reply_text': '这次任务未完成：执行端未能在规定时间内开始处理。已保留原任务记录。',
                    'remaining_requirements': ['worker_not_started_before_deadline'],
                    'artifacts': [], 'run_id': row['run_id']}
                self._finish(db, row, result, 'failed', 'deadline_before_worker_start')
                count += 1
        return count

    def claim(self, *, worker_id):
        self.expire_unclaimed()
        now = self.store.clock()
        with self.store.transaction() as db:
            self._worker_seen(db, worker_id)
            # An expired run is observable uncertainty, not permission to repeat it.
            expired = db.execute('''SELECT * FROM h4_receiver_runs
                WHERE namespace=? AND state IN ('running','cancel_requested') AND lease_until<?''',
                                 (self.namespace, now)).fetchall()
            for old in expired:
                row = dict(old)
                db.execute("UPDATE h4_receiver_runs SET state='unknown',last_error='worker_lease_expired' WHERE run_id=?", (row['run_id'],))
                self._event(db, row, 'unknown', {'run_id': row['run_id'], 'reason': 'worker_lease_expired'})
            row = db.execute('''SELECT * FROM h4_receiver_runs WHERE namespace=? AND state='accepted'
                ORDER BY accepted_at,operation_id LIMIT 1''', (self.namespace,)).fetchone()
            if row is None:
                return None
            row = dict(row)
            envelope = json.loads(row['envelope_json'])
            if envelope['execution']['completion_deadline'] <= now:
                result = {'response_kind': 'unavailable',
                          'reply_text': '这次任务未完成：执行端未能在规定时间内开始处理。已保留原任务记录。',
                          'remaining_requirements': ['worker_not_started_before_deadline'], 'artifacts': [],
                          'run_id': row['run_id']}
                self._finish(db, row, result, 'failed', 'deadline_before_worker_start')
                return None
            token = secrets.token_urlsafe(32)
            db.execute('''UPDATE h4_receiver_runs SET state='running',worker_id=?,started_at=?,
                lease_until=?,token_hash=? WHERE run_id=?''',
                       (worker_id, now, now + self.worker_lease_seconds,
                        hashlib.sha256(token.encode()).hexdigest(), row['run_id']))
            self._event(db, row, 'running', {'run_id': row['run_id'], 'started_at': now})
            db.execute('DELETE FROM h4_receiver_memory WHERE namespace=? AND created<?',
                       (self.namespace, now - self.memory_ttl_seconds))
            history = db.execute('''SELECT role,body,operation_id FROM h4_receiver_memory WHERE namespace=?
                AND memory_key=? ORDER BY seq DESC LIMIT ?''',
                                 (self.namespace, row['memory_key'], self.history_limit)).fetchall()
            return {'operation_id': row['operation_id'], 'run_id': row['run_id'],
                    'generation': row['generation'], 'lease_token': token,
                    'session_key': row['session_key'], 'envelope': envelope,
                    'history': [dict(item) for item in reversed(history)],
                    'lease_seconds': self.worker_lease_seconds}

    def _worker_authorize(self, db, operation_id, run_id, generation, token):
        row = self._row(db, operation_id)
        token_hash = hashlib.sha256(str(token).encode()).hexdigest()
        if (row['run_id'], row['generation']) != (run_id, generation) or not row['token_hash'] or not hmac.compare_digest(row['token_hash'], token_hash):
            raise ContractError('worker token does not own operation')
        return row

    def worker_context(self, *, operation_id, run_id, generation, lease_token):
        with self.store.transaction() as db:
            row = self._worker_authorize(db, operation_id, run_id, generation, lease_token)
            if row['state'] in TERMINAL or row['cancel_requested']:
                raise ContractError('worker operation already ended')
            return json.loads(row['envelope_json'])

    def heartbeat(self, *, operation_id, run_id, generation, lease_token):
        with self.store.transaction() as db:
            row = self._worker_authorize(db, operation_id, run_id, generation, lease_token)
            if row['state'] in TERMINAL or row['state'] == 'unknown':
                # A control thread can remain alive after the protected model's
                # completion became unknown. Its heartbeat cannot clear that.
                return {'status': row['state'], 'cancel_requested': bool(row['cancel_requested'])}
            state = 'cancel_requested' if row['cancel_requested'] else 'running'
            db.execute('UPDATE h4_receiver_runs SET state=?,lease_until=? WHERE run_id=?',
                       (state, self.store.clock() + self.worker_lease_seconds, run_id))
            self._worker_seen(db, row['worker_id'])
            return {'status': state, 'cancel_requested': bool(row['cancel_requested'])}

    def mark_unknown(self, *, operation_id, run_id, generation, lease_token,
                     reason='worker_execution_unknown'):
        """Retain an original execution whose stopping could not be observed.

        No result, ended receipt or replacement claim is created. Only the
        original worker credentials can later reconcile a result/stop evidence.
        """
        if not isinstance(reason, str) or reason not in {'host_qwen_stop_unknown',
                'adapter_execution_unknown', 'worker_execution_unknown'}:
            raise ContractError('recognized execution uncertainty code required')
        with self.store.transaction() as db:
            row = self._worker_authorize(db, operation_id, run_id, generation, lease_token)
            if row['state'] in TERMINAL:
                return self._status(row)  # A delayed report cannot regress an end.
            if row['state'] not in {'running', 'cancel_requested', 'unknown'}:
                raise ContractError('only a claimed execution can become unknown')
            db.execute("""UPDATE h4_receiver_runs SET state='unknown',lease_until=NULL,last_error=?
                WHERE run_id=?""", (reason, run_id))
            self._event(db, row, 'unknown', {'run_id': run_id, 'reason': reason})
            return self._status(self._row(db, operation_id))

    def _finish(self, db, row, result, status, stopped_evidence):
        now = self.store.clock()
        if result is not None:
            self._event(db, row, 'result', result)
        self._event(db, row, 'ended', {'run_id': row['run_id'], 'status': status,
            'stopped_evidence': stopped_evidence, 'ended_at': now})
        db.execute('''UPDATE h4_receiver_runs SET state=?,ended_at=?,lease_until=NULL,last_error='',
            result_json=? WHERE run_id=?''', (status, now, dumps(result) if result is not None else None, row['run_id']))

    def _publish_memory(self, db, row):
        envelope, result = json.loads(row['envelope_json']), json.loads(row['result_json'])
        for role, body in (('user', envelope['payload']['raw_text']), ('assistant', result['reply_text'])):
            db.execute('''INSERT OR IGNORE INTO h4_receiver_memory
                (namespace,memory_key,operation_id,created,role,body) VALUES(?,?,?,?,?,?)''',
                       (self.namespace, row['memory_key'], row['operation_id'], self.store.clock(), role, body))

    def publish_delivered_memory(self, harness, *, limit=100):
        """Promote conversation turns only after exact send and independent readback.

        An execution result can be a draft or an unknown send. Neither is an
        assistant utterance that the next model may assume the user has seen.
        """
        rows = self.store.rows('''SELECT r.* FROM h4_receiver_runs r WHERE r.namespace=?
            AND r.state='succeeded' AND r.ended_at>=?
            AND NOT EXISTS(SELECT 1 FROM h4_receiver_memory m
                WHERE m.namespace=r.namespace AND m.operation_id=r.operation_id)
            ORDER BY r.ended_at DESC LIMIT ?''',
                              (self.namespace, self.store.clock() - self.memory_ttl_seconds, limit))
        count = 0
        for row in rows:
            task = harness.store.one('SELECT * FROM h1_tasks WHERE task_id=?', (row['task_id'],))
            if not task or task['business_id'] != row['operation_id'] or task['generation'] != row['generation']:
                continue
            if task['delivery_state'] != 'delivered' or task['verification_state'] != 'sufficient':
                continue
            result = json.loads(row['result_json'] or '{}')
            effect = harness.store.one('SELECT * FROM h1_effects WHERE task_id=?', (row['task_id'],))
            if not result.get('reply_text') or not effect or not effect['platform_id'] or effect['body'] != result['reply_text']:
                continue
            try:
                with harness.store.transaction() as db:
                    harness._authorize(db, row['task_id'], row['generation'], require_running=False)
            except ContractError:
                continue
            with self.store.transaction() as db:
                self._publish_memory(db, row)
            count += 1
        return count

    def complete(self, *, operation_id, run_id, generation, lease_token, result=None,
                 status='succeeded', stopped_evidence='worker_returned'):
        if status not in TERMINAL or not isinstance(stopped_evidence, str) or not stopped_evidence:
            raise ContractError('worker completion needs stopped execution evidence')
        if status != 'cancelled' and result is None:
            raise ContractError('finished worker must return a result')
        normalized = validate_result(result, run_id) if result is not None else None
        with self.store.transaction() as db:
            row = self._worker_authorize(db, operation_id, run_id, generation, lease_token)
            if row['cancel_requested']:
                status, normalized = 'cancelled', None
            if row['state'] in TERMINAL:
                if row['state'] != status or row['result_json'] != (dumps(normalized) if normalized is not None else None):
                    raise ContractError('conflicting receiver completion')
                return self._status(row)
            # Cancellation can only be asserted once the original worker returned.
            self._finish(db, row, normalized, status, stopped_evidence)
            return self._status(self._row(db, operation_id))

    def cancel(self, operation_id, run_id='', generation=None):
        with self.store.transaction() as db:
            row = self._row(db, operation_id)
            self._match_status(row, run_id, generation)
            if row['state'] == 'accepted':
                db.execute('UPDATE h4_receiver_runs SET cancel_requested=1 WHERE run_id=?', (row['run_id'],))
                self._finish(db, row, None, 'cancelled', 'cancelled_before_worker_start')
            elif row['state'] == 'unknown':
                # Cancellation expresses intent, not newly observed execution.
                db.execute('UPDATE h4_receiver_runs SET cancel_requested=1 WHERE run_id=?', (row['run_id'],))
            elif row['state'] not in TERMINAL:
                db.execute("UPDATE h4_receiver_runs SET cancel_requested=1,state='cancel_requested' WHERE run_id=?", (row['run_id'],))
            updated = self._row(db, operation_id)
            return {**self._status(updated), 'stopped': updated['state'] in TERMINAL}

    @staticmethod
    def _match_status(row, run_id, generation):
        if run_id and row['run_id'] != run_id or generation is not None and row['generation'] != generation:
            raise ContractError('receiver status execution mismatch')

    @staticmethod
    def _status(row):
        return {'status': row['state'], 'run_id': row['run_id'],
                'operation_id': row['operation_id'], 'generation': row['generation'],
                'accepted_at': row['accepted_at'], 'started_at': row['started_at'],
                'ended_at': row['ended_at'], 'cancel_requested': bool(row['cancel_requested']),
                'lease_until': row['lease_until'],
                'result': json.loads(row['result_json']) if row['result_json'] else None,
                'last_error': row['last_error'], 'accepted_by': 'gateway_receiver'}

    def query(self, operation_id, run_id='', generation=None):
        self.expire_unclaimed()
        with self.store.transaction() as db:
            row = self._row(db, operation_id)
            self._match_status(row, run_id, generation)
            result = self._status(row)
            result['events'] = [self._receipt(dict(item)) for item in db.execute(
                'SELECT * FROM h4_receiver_events WHERE namespace=? AND operation_id=? ORDER BY seq',
                (self.namespace, operation_id))]
            return result

    @staticmethod
    def _receipt(row):
        return {'event_id': row['event_id'], 'task_id': row['task_id'],
                'business_id': row['operation_id'], 'generation': row['generation'],
                'kind': row['kind'], 'payload': json.loads(row['payload_json'])}

    def reconcile(self, harness, *, limit=100):
        self.expire_unclaimed(limit=limit)
        rows = self.store.rows('''SELECT * FROM h4_receiver_events WHERE namespace=?
            AND acknowledged=0 ORDER BY seq LIMIT ?''', (self.namespace, limit))
        count = 0
        for row in rows:
            harness.record_receipt(**self._receipt(row))
            with self.store.transaction() as db:
                db.execute('UPDATE h4_receiver_events SET acknowledged=1 WHERE seq=?', (row['seq'],))
            count += 1
        live = self.store.rows('''SELECT * FROM h4_receiver_runs WHERE namespace=?
            AND state IN ('running','cancel_requested') AND lease_until>?''',
                               (self.namespace, self.store.clock()))
        for row in live:
            reconcile_worker_lease(harness, task_id=row['task_id'], operation_id=row['operation_id'],
                run_id=row['run_id'], generation=row['generation'], status=row['state'], lease_until=row['lease_until'])
        self.publish_delivered_memory(harness, limit=limit)
        return count

    def health(self):
        workers = self.store.rows('SELECT worker_id,seen_at FROM h4_receiver_workers WHERE namespace=?', (self.namespace,))
        now = self.store.clock()
        counts = self.store.rows('SELECT state,COUNT(*) AS count FROM h4_receiver_runs WHERE namespace=? GROUP BY state', (self.namespace,))
        recent = any(row['seen_at'] >= now - max(60, self.worker_lease_seconds * 2) for row in workers)
        return {'namespace': self.namespace, 'status': 'worker_seen' if recent else 'awaiting_worker',
                'receiver': 'durable_sqlite', 'worker_recently_seen': recent,
                'receiver_boundary_enforced': True, 'external_bot_boundary': 'unverified',
                'states': {row['state']: row['count'] for row in counts}}


class LocalReceiverAdapter:
    """Harness adapter. ``wake`` only prompts a worker to inspect this ledger."""
    def __init__(self, receiver, *, wake=None):
        self.receiver, self.wake = receiver, wake

    def accept(self, envelope):
        receipt = self.receiver.accept(envelope)
        if self.wake is not None:
            # A wake failure must not erase the durable acceptance. Polling is the
            # independent recovery path over exactly the same operation record.
            try:
                self.wake.notify()
            except Exception:
                pass
        return receipt

    def query(self, operation_id, run_id='', generation=None):
        return self.receiver.query(operation_id, run_id, generation)

    def cancel(self, operation_id, run_id='', generation=None):
        return self.receiver.cancel(operation_id, run_id, generation)

    def capability(self, intent, *, scope=None):
        if intent not in self.receiver.intents:
            raise ContractError('capability outside receiver intent set')
        return Capability(intent, self.receiver.namespace,
            scope or self.receiver.namespace + ':{tenant_id}:{chat_id}:{principal}',
            available=True, durable_acceptance=True, status_query=True, cancellation=True,
            isolation=True, enforced_tools=True, evidence_ref='implementation:durable_receiver_v1')

    def reconcile(self, harness, *, limit=100):
        return self.receiver.reconcile(harness, limit=limit)
