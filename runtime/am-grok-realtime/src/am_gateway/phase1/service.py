"""Run the whole gateway without opening a second Feishu event connection.

Routing, model classification, execution, collection and delivery have separate
bounded workers. A slow model or API never runs inside the message callback.
"""
from __future__ import annotations

import json
from contextlib import nullcontext
import threading
import time
import uuid

from ..models import ContractError, key
from ..assistant.continuity import owner_cli_sql
from .contracts import Policy
from .metrics import report


class RuntimeService:
    def __init__(self, ingress, *, receivers=(), pumps=(), collector=None,
                 sender=None, readback=None, artifact_delivery=None, servers=(), ownership=None,
                 auto_scopes=False, dispatch_enabled=True, tick_seconds=.2, delivery_workers=4):
        self.ingress, self.harness = ingress, ingress.gateway
        self.routing_scheduler = ingress.scheduler
        self.receivers, self.pumps = list(receivers), list(pumps)
        self.collector, self.sender, self.readback = collector, sender, readback
        self.artifact_delivery = artifact_delivery
        self.assistant = None
        self.interactive_runner = None
        self.servers, self.ownership = list(servers), ownership
        self.auto_scopes = auto_scopes
        self.dispatch_enabled = dispatch_enabled
        if not .02 <= tick_seconds <= 5:
            raise ContractError('invalid runtime tick')
        self.tick_seconds = tick_seconds
        self.stopped = threading.Event()
        self.wakeup = threading.Event()
        self.threads = []
        self._lifecycle = threading.RLock()
        if type(delivery_workers) is not int or not 1 <= delivery_workers <= 8:
            raise ContractError('runtime delivery_workers must be between 1 and 8')
        self.delivery_workers = delivery_workers
        self.delivery_lease_seconds = 120.0
        self._retry_at = {}
        self._errors = {}
        self.started = False
        with self.harness.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r7_h1_delivery_scopes(
                task_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,scope TEXT NOT NULL,
                source_version INTEGER NOT NULL,scope_version INTEGER NOT NULL DEFAULT 2)''')
            if 'scope_version' not in {row[1] for row in db.execute('PRAGMA table_info(r7_h1_delivery_scopes)')}:
                db.execute('ALTER TABLE r7_h1_delivery_scopes ADD COLUMN scope_version INTEGER NOT NULL DEFAULT 1')
            db.execute('''CREATE INDEX IF NOT EXISTS r7_h1_delivery_scope
                ON r7_h1_delivery_scopes(owner_id,scope)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r7_h1_delivery_claims(
                owner_id TEXT NOT NULL,scope TEXT NOT NULL,task_id TEXT NOT NULL,
                token TEXT NOT NULL,lease_until REAL NOT NULL,PRIMARY KEY(owner_id,scope))''')

    def notify(self):
        self.wakeup.set()
        if self.routing_scheduler:
            self.routing_scheduler.notify()

    def _error(self, lane, exc, object_id='runtime'):
        # Audit the exception type; remote bodies may contain private source text.
        marker = (lane, object_id, type(exc).__name__)
        now = time.monotonic()
        if now - self._errors.get(marker, -1000) >= 10:
            with self.harness.store.transaction() as db:
                self.harness.store.audit(db, 'r4_' + lane, object_id, type(exc).__name__)
            self._errors[marker] = now

    def _bind_ready_scopes(self):
        if not self.auto_scopes:
            return 0
        if self.ownership is None:
            raise ContractError('scope ownership port is required')
        rows = self.harness.store.rows("""SELECT DISTINCT scope FROM h1_tasks
            WHERE dispatch_state='pending' AND scope!=''""")
        count = 0
        for row in rows:
            old = self.harness.store.one('SELECT state FROM h1_cutover WHERE scope=?', (row['scope'],))
            if old:
                continue  # a paused or rolled-back scope cannot be re-enabled by a timer
            self.ownership.ensure_scope(row['scope'])
            count += 1
        return count

    def dispatch_once(self):
        if not self.dispatch_enabled:
            return None
        self._bind_ready_scopes()
        return self.harness.dispatch_one()

    def receipts_once(self):
        total = 0
        for receiver in self.receivers:
            try:
                value = receiver.reconcile(self.harness, limit=100)
                total += value if isinstance(value, int) else 0
            except Exception as exc:
                self._error('receipt_reconcile', exc)
        # Remote adapters are allowed to replay their durable event log after a
        # lost callback. A status lookup never creates a new operation.
        for name, adapter in self.harness.adapters.items():
            if not hasattr(adapter, 'reconcile') or getattr(adapter, 'receiver', None) in self.receivers:
                continue
            try:
                adapter.reconcile(self.harness, limit=100)
            except Exception as exc:
                self._error('remote_status', exc, name)
        self.harness.owner_results_available()
        return total

    def _interactive_delivery_allowed(self, task, result):
        if not result.get('interactive_source_binding'):
            return True
        runner = self.interactive_runner
        try:
            return bool(runner is not None and runner.validate_delivery(task, result))
        except Exception:
            return False

    def _block_interactive_delivery(self, task, *, effect_id=None):
        """Retain the exact result for review; never rerun its model or send it.

        A denied wrapper has not called the sender, so its just-created effect
        is known not sent. A previous unknown transport outcome is untouched.
        """
        with self.harness.store.transaction() as db:
            if effect_id is not None:
                db.execute("UPDATE h1_effects SET state='denied' WHERE effect_id=? AND platform_id IS NULL",
                           (effect_id,))
            db.execute("UPDATE h1_tasks SET delivery_state='blocked',verification_state='conflict',"
                       "last_error='interactive_source_changed' WHERE task_id=?", (task['task_id'],))
            unresolved = db.execute("SELECT 1 FROM h1_effects WHERE task_id=? AND state IN ('sending','unknown')",
                                    (task['task_id'],)).fetchone()
            if not unresolved and task['execution_state'] in {'succeeded', 'failed'}:
                db.execute("UPDATE h1_sessions SET state='free',task_id=NULL WHERE task_id=? AND generation=?",
                           (task['task_id'], task['generation']))
            self.harness.store.audit(db, 'r7_interactive_delivery_blocked', task['task_id'],
                                     'current_source_or_scope_changed')

    def _sync_delivery_scopes(self, db):
        cfg = self.harness.config
        rows = db.execute('''SELECT t.task_id,t.request_json,t.policy_json,t.source_version,t.intent
            FROM h1_tasks t LEFT JOIN r7_h1_delivery_scopes s USING(task_id)
            WHERE s.task_id IS NULL OR s.source_version!=t.source_version OR s.owner_id!=? OR s.scope_version!=2''',
            (cfg.owner_id,)).fetchall()
        for row in rows:
            event, policy = json.loads(row['request_json']), Policy.parse(row['policy_json'])
            # Passive source chats converge on the actual owner DM. Different
            # group senders share one delivery sequence inside the same thread.
            # Background documents/code keep their own ordered output lane so
            # a long artifact does not block a ready interactive answer.
            thread = event.get('thread_id', '') if policy.reply_target == event['chat_id'] else ''
            lane = ('foreground' if row['intent'] in {'project.query', 'conversation.reply', 'work_message.assist'}
                    else 'background')
            scope = key(event['tenant_id'], policy.reply_target, thread, lane)
            db.execute('INSERT OR REPLACE INTO r7_h1_delivery_scopes VALUES(?,?,?,?,2)',
                       (row['task_id'], cfg.owner_id, scope, row['source_version']))

    def _claim_delivery(self, attempted=()):
        if self.sender is None:
            return None
        cfg, now = self.harness.config, self.harness.store.clock()
        excluded = tuple(dict.fromkeys([*attempted, *(tid for tid, when in list(self._retry_at.items())
                                                     if time.monotonic() < when)]))
        exclusion = ('AND t.task_id NOT IN (' + ','.join('?' for _ in excluded) + ')') if excluded else ''
        with self.harness.store.transaction() as db:
            self._sync_delivery_scopes(db)
            artifact_block = ''
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='h4_artifact_effects'").fetchone():
                artifact_block = '''AND NOT EXISTS(SELECT 1 FROM h4_artifact_effects f
                    JOIN r7_h1_delivery_scopes fs ON fs.task_id=f.task_id
                    WHERE fs.owner_id=s.owner_id AND fs.scope=s.scope
                      AND f.state IN ('uploading','sending','upload_unknown','send_unknown'))'''
            row = db.execute(f'''SELECT t.*,s.scope AS delivery_scope FROM h1_tasks t
                JOIN r7_h1_delivery_scopes s USING(task_id)
                WHERE s.owner_id=? AND t.execution_state IN ('succeeded','failed')
                  AND t.result_json IS NOT NULL AND coalesce(json_extract(t.result_json,'$.reply_text'),'')!=''
                  AND json_extract(t.policy_json,'$.allow_reply')=1
                  AND coalesce(json_extract(t.policy_json,'$.revoked'),0)=0
                  AND t.verification_state!='conflict' AND t.last_error!='source_changed'
                  AND NOT (t.delivery_state='delivered' AND t.verification_state='sufficient')
                  AND NOT {owner_cli_sql()}
                  AND NOT EXISTS(SELECT 1 FROM r7_h1_delivery_claims c
                    WHERE c.owner_id=s.owner_id AND c.scope=s.scope AND c.lease_until>?)
                  AND NOT EXISTS(SELECT 1 FROM h1_effects e JOIN r7_h1_delivery_scopes es USING(task_id)
                    WHERE es.owner_id=s.owner_id AND es.scope=s.scope AND e.state IN ('sending','unknown'))
                  {artifact_block}
                  AND NOT EXISTS(SELECT 1 FROM h1_tasks p JOIN r7_h1_delivery_scopes ps USING(task_id)
                    WHERE ps.owner_id=s.owner_id AND ps.scope=s.scope
                      AND (p.created<t.created OR (p.created=t.created AND p.task_id<t.task_id))
                      AND p.execution_state!='cancelled' AND p.verification_state!='conflict'
                      AND p.last_error!='source_changed' AND json_extract(p.policy_json,'$.allow_reply')=1
                      AND coalesce(json_extract(p.policy_json,'$.revoked'),0)=0
                      AND NOT (p.delivery_state='delivered' AND p.verification_state='sufficient')
                      AND NOT {owner_cli_sql('p')}
                      AND ((p.execution_state IN ('succeeded','failed')
                            AND coalesce(json_extract(p.result_json,'$.reply_text'),'')!='')
                           OR (p.execution_state NOT IN ('succeeded','failed','cancelled')
                               AND p.dispatch_state IN ('pending','dispatching','accepted','unknown'))))
                  {exclusion}
                ORDER BY t.created,t.task_id LIMIT 1''',
                (cfg.owner_id, cfg.owner_id, cfg.owner_chat_id, cfg.owner_id, now,
                 cfg.owner_id, cfg.owner_chat_id, cfg.owner_id, *excluded)).fetchone()
            if row is None:
                return None
            task, token = dict(row), uuid.uuid4().hex
            scope = task.pop('delivery_scope')
            db.execute('''INSERT INTO r7_h1_delivery_claims VALUES(?,?,?,?,?)
                ON CONFLICT(owner_id,scope) DO UPDATE SET task_id=excluded.task_id,
                token=excluded.token,lease_until=excluded.lease_until''',
                (cfg.owner_id, scope, task['task_id'], token, now + self.delivery_lease_seconds))
            return {'owner_id': cfg.owner_id, 'scope': scope, 'task_id': task['task_id'],
                    'token': token, 'task': task}

    def _renew_delivery_claim(self, claim, db=None):
        with (self.harness.store.transaction() if db is None else nullcontext(db)) as connection:
            return bool(connection.execute('''UPDATE r7_h1_delivery_claims SET lease_until=?
                WHERE owner_id=? AND scope=? AND task_id=? AND token=?''',
                (self.harness.store.clock() + self.delivery_lease_seconds, claim['owner_id'],
                 claim['scope'], claim['task_id'], claim['token'])).rowcount)

    def _release_delivery_claim(self, claim):
        with self.harness.store.transaction() as db:
            return bool(db.execute('''DELETE FROM r7_h1_delivery_claims
                WHERE owner_id=? AND scope=? AND task_id=? AND token=?''',
                (claim['owner_id'], claim['scope'], claim['task_id'], claim['token'])).rowcount)

    def delivery_once(self):
        if self.sender is None:
            return 0
        completed, attempted = 0, []
        for _ in range(50):
            claim = self._claim_delivery(attempted)
            if claim is None:
                break
            attempted.append(claim['task_id'])
            stopped = threading.Event()

            def heartbeat(current=claim, signal=stopped):
                while not signal.wait(max(.05, self.delivery_lease_seconds / 3)):
                    try:
                        if not self._renew_delivery_claim(current):
                            break
                    except Exception:
                        break

            worker = threading.Thread(target=heartbeat, daemon=True, name='runtime-delivery-heartbeat')
            worker.start()
            try:
                completed += self._deliver_claim(claim)
            except Exception as exc:
                self._error('delivery', exc, claim['task_id'])
                self._retry_at[claim['task_id']] = time.monotonic() + 2
            finally:
                stopped.set()
                worker.join(1)
                self._release_delivery_claim(claim)
        return completed

    def _deliver_claim(self, claim):
        task, tid = claim['task'], claim['task_id']
        result = json.loads(task['result_json'])
        guard = lambda db=None: self._renew_delivery_claim(claim, db)
        if not guard():
            return 0
        old_effect = self.harness.store.one('SELECT * FROM h1_effects WHERE task_id=?', (tid,))
        if old_effect is None and not self._interactive_delivery_allowed(task, result):
            self._block_interactive_delivery(task)
            return 0
        if self.assistant is not None and not self.assistant.prepare_reply(task, result):
            self._retry_at[tid] = time.monotonic() + 1
            return 0
        if not guard():
            return 0
        denied_before_send = []

        def checked_sender(**kwargs):
            if not self._interactive_delivery_allowed(task, result):
                denied_before_send.append(kwargs['idempotency_key'])
                raise ContractError('interactive_source_changed_before_send')
            return self.sender(**kwargs)

        # The claim check and h1 effect insertion share one SQLite transaction.
        # Once an effect is sending, its scope cannot be reclaimed on timeout.
        effect = self.harness.deliver_recorded_result(tid, checked_sender, claim_guard=guard)
        if denied_before_send:
            self._block_interactive_delivery(task, effect_id=denied_before_send[0])
            return 0
        if not guard():
            return 0
        completed = 0
        if effect.get('platform_id') and self.readback:
            evidence = self.readback(tid)
            if not guard():
                return 0
            if evidence:
                attachments = {}
                if result.get('artifacts'):
                    if self.artifact_delivery is None:
                        self._retry_at[tid] = time.monotonic() + 5
                        return 0
                    attachments = self.artifact_delivery.deliver_task(tid, claim_guard=guard)
                    if not guard() or not attachments.get('complete'):
                        self._retry_at[tid] = time.monotonic() + 5
                        return 0
                verified = self.harness.verify_delivery(tid, evidence, claim_guard=guard)
                if not guard():
                    return 0
                if self.assistant is not None:
                    self.assistant.record_delivery(task, result, evidence)
                file_time = attachments.get('completed_platform_time')
                if file_time and result.get('response_kind') == 'sufficient' and not result.get('remaining_requirements'):
                    stamps = json.loads(verified['stamps'])
                    stamps['full_completed_at'] = max(stamps.get('full_completed_at', 0), file_time)
                    with self.harness.store.transaction() as db:
                        if guard(db):
                            db.execute('UPDATE h1_tasks SET stamps=? WHERE task_id=?', (json.dumps(stamps), tid))
                completed = 1
        self._retry_at[tid] = time.monotonic() + 2
        return completed

    def collection_once(self):
        if self.collector is None:
            return None
        return self.collector.run_due()

    def step(self, *, classify=True, collect=False):
        """Deterministic embedded/test entry; it exercises the actual components."""
        if self.assistant is not None:
            self.assistant.tick()
        routed = self.harness.route_one(defer_classification=True)
        classified = self.harness.classify_one() if classify else None
        self.harness.information.project_sources()
        dispatched = self.dispatch_once()
        for pump in self.pumps:
            pump.step()
        reconciled = self.receipts_once()
        delivered = self.delivery_once()
        self.harness.expire()
        collected = self.collection_once() if collect else None
        return dict(routed=routed, classified=classified, dispatched=dispatched,
                    reconciled=reconciled, delivered=delivered, collected=collected)

    def _loop(self, lane, action, interval):
        while not self.stopped.is_set():
            try:
                action()
            except Exception as exc:
                self._error(lane, exc)
            self.stopped.wait(interval)

    def start(self):
        with self._lifecycle:
            if self.started:
                return
            self.stopped.clear()
            started = []
            try:
                for server in self.servers:
                    server.start()
                    started.append(server)
                if self.routing_scheduler:
                    self.routing_scheduler.start()
                for pump in self.pumps:
                    pump.start()
                lanes = [('dispatch', self.dispatch_once, self.tick_seconds),
                         ('receipts', self.receipts_once, self.tick_seconds),
                         ('delivery', self.delivery_once, self.tick_seconds)]
                lanes.extend(('delivery-' + str(n), self.delivery_once, self.tick_seconds)
                             for n in range(2, self.delivery_workers + 1))
                if self.collector:
                    lanes.append(('collection', self.collection_once, 1.0))
                if self.assistant is not None:
                    if callable(getattr(self.assistant, 'foreground_tick', None)):
                        lanes.extend([
                            ('assistant-intake', self.assistant.foreground_tick, .05),
                            ('assistant-local-delivery', self.assistant.local_delivery_once, .05),
                            ('assistant-background', self.assistant.background_tick, .5),
                            ('assistant-improvement', self.assistant.improvement_tick, 1.0),
                        ])
                        lanes.extend(('assistant-intake-' + str(n), self.assistant.foreground_tick, .05)
                                     for n in range(2, self.assistant.intake_workers + 1))
                        # The assistant claims one durable conversation at a
                        # time; independent chats need not await a slow sender.
                        lanes.extend(('assistant-local-delivery-' + str(n), self.assistant.local_delivery_once, .05)
                                     for n in range(2, self.assistant.delivery_workers + 1))
                    else:
                        lanes.append(('assistant', self.assistant.tick, .2))
                for name, action, interval in lanes:
                    worker = threading.Thread(target=self._loop, args=(name, action, interval),
                                              name='am-r4-' + name, daemon=True)
                    self.threads.append(worker)
                    worker.start()
                self.started = True
                self.notify()
            except BaseException:
                self.stopped.set()
                for server in reversed(started):
                    server.stop()
                if self.routing_scheduler:
                    self.routing_scheduler.stop()
                for pump in self.pumps:
                    pump.stop()
                raise

    def stop(self):
        with self._lifecycle:
            self.stopped.set()
            self.wakeup.set()
            routing = self.routing_scheduler.stop() if self.routing_scheduler else {'stopped': True}
            pumps = [pump.stop() for pump in self.pumps]
            deadline = time.monotonic() + 5
            for worker in self.threads:
                worker.join(max(0, deadline - time.monotonic()))
            for server in reversed(self.servers):
                server.stop()
            done = not any(t.is_alive() for t in self.threads)
            pump_done = all(v is None or v is True or (isinstance(v, dict) and v.get('stopped')) for v in pumps)
            self.started = not (done and routing.get('stopped') and pump_done)
            return {'stopped': not self.started, 'routing': routing,
                    'pumps': pumps, 'runtime_workers_stopped': done}

    def health(self):
        states = self.harness.store.rows("""SELECT execution_state,delivery_state,count(*) AS count
            FROM h1_tasks GROUP BY execution_state,delivery_state""")
        return {'service_running': self.started, 'queues': states,
                'delivery_workers': self.delivery_workers,
                'dispatch_enabled': self.dispatch_enabled,
                'receivers': [r.health() for r in self.receivers],
                'collector_bound': self.collector is not None,
                'sender_bound': self.sender is not None,
                'readback_bound': self.readback is not None,
                'artifact_delivery_bound': self.artifact_delivery is not None,
                'capabilities': {i: {'available': c.available, 'adapter': c.adapter}
                                 for i, c in self.harness.config.capabilities.items()},
                'assistant': self.assistant.status() if self.assistant is not None else None,
                'real_business_verified': False}

    def metrics(self):
        return report(self.harness.store)
