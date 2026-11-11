"""Trusted receiver evidence; duplicate/late events cannot regress terminal states."""
import json
import math

from ..models import ContractError, dumps
from .contracts import ANSWER_KINDS, TERMINAL


class Lifecycle:
    def record_receipt(self, *, event_id, task_id, business_id, generation, kind, payload):
        if kind not in {'accepted', 'running', 'ended', 'result', 'unknown'} or not event_id or not isinstance(payload, dict):
            raise ContractError('invalid receipt')
        encoded = dumps(payload)
        with self.store.transaction() as db:
            task = self.task(task_id, db)
            if task['business_id'] != business_id:
                raise ContractError('receipt operation mismatch')
            old = db.execute('SELECT * FROM h1_receipts WHERE event_id=?', (event_id,)).fetchone()
            if old:
                if (old['task_id'],old['generation'],old['kind'],old['payload']) != (task_id,generation,kind,encoded):
                    raise ContractError('receipt identity payload conflict')
                return task
            db.execute('INSERT INTO h1_receipts VALUES(?,?,?,?,?,?)',
                       (event_id,task_id,generation,kind,encoded,self.store.clock()))
            if generation != task['generation']:
                self.store.audit(db, 'stale_receipt', task_id, str(generation))
                return task
            stamps = json.loads(task['stamps'])
            terminal = task['execution_state'] in TERMINAL
            if kind == 'accepted':
                if not payload.get('durable_record') or not payload.get('run_id'):
                    raise ContractError('webhook acceptance is not durable task acceptance')
                if task['run_id'] and payload['run_id'] != task['run_id']:
                    raise ContractError('conflicting execution instance')
                stamps.setdefault('t_a', self.store.clock())
                stamps.setdefault('accepted_at', payload.get('accepted_at'))
                db.execute("UPDATE h1_tasks SET dispatch_state='accepted',run_id=? WHERE task_id=?", (payload['run_id'],task_id))
                db.execute("UPDATE h1_dispatch SET state='accepted' WHERE task_id=?", (task_id,))
            elif kind == 'running':
                if not payload.get('run_id') or (task['run_id'] and task['run_id'] != payload['run_id']):
                    raise ContractError('execution instance required')
                if not terminal:
                    db.execute("UPDATE h1_tasks SET execution_state='running',run_id=? WHERE task_id=?", (payload['run_id'],task_id))
                    stamps.setdefault('execution_started_observed', self.store.clock())
                    stamps.setdefault('execution_started_at', payload.get('started_at'))
            elif kind == 'unknown':
                if not terminal:
                    db.execute("UPDATE h1_tasks SET execution_state='unknown' WHERE task_id=?", (task_id,))
            elif kind == 'ended':
                state = payload.get('status')
                if state not in TERMINAL or not payload.get('stopped_evidence') or not payload.get('run_id'):
                    raise ContractError('end requires correlated evidence that protected execution stopped')
                if task['run_id'] and task['run_id'] != payload['run_id']:
                    raise ContractError('end instance mismatch')
                if terminal and task['execution_state'] != state:
                    db.execute("UPDATE h1_tasks SET verification_state='conflict',last_error='contradictory_terminal_evidence' WHERE task_id=?", (task_id,))
                else:
                    db.execute('UPDATE h1_tasks SET execution_state=?,run_id=?,lease_until=NULL WHERE task_id=?', (state,payload['run_id'],task_id))
                    stamps.setdefault('execution_ended_observed', self.store.clock())
                    if not db.execute("SELECT 1 FROM h1_effects WHERE task_id=? AND state IN ('sending','unknown')",(task_id,)).fetchone():
                        db.execute("UPDATE h1_sessions SET task_id=NULL,state='free' WHERE scope=? AND task_id=? AND generation=?",
                                   (task['scope'],task_id,generation))
            elif kind == 'result':
                if payload.get('response_kind') not in ANSWER_KINDS:
                    raise ContractError('result kind required')
                if not isinstance(payload.get('remaining_requirements'), list) or not isinstance(payload.get('artifacts'), list):
                    raise ContractError('result must declare requirements and artifacts')
                if not payload.get('run_id') or (task['run_id'] and payload['run_id'] != task['run_id']):
                    raise ContractError('result instance mismatch')
                if task['result_json'] and task['result_json'] != encoded:
                    db.execute("UPDATE h1_tasks SET verification_state='conflict',last_error='conflicting_result' WHERE task_id=?", (task_id,))
                else:
                    # A result may arrive before ACK or progress. Bind its execution
                    # instance, but do not invent durable acceptance or a stopped run.
                    db.execute('UPDATE h1_tasks SET result_json=?,run_id=? WHERE task_id=?',
                               (encoded,payload['run_id'],task_id))
                    stamps.setdefault('result_observed', self.store.clock())
            db.execute('UPDATE h1_tasks SET stamps=? WHERE task_id=?', (dumps(stamps),task_id))
            return self.task(task_id, db)

    def dispatch_one(self):
        # No trigger can be invoked without an explicit cutover ledger entry.
        enabled = self.store.rows("SELECT scope FROM h1_cutover WHERE state='harness'")
        if not enabled:
            return None
        envelope = self.claim(require_cutover=True)
        if envelope is None:
            return None
        task_id = envelope['identity']['task_id']
        scope = envelope['execution']['scope']
        if scope not in {r['scope'] for r in enabled}:
            raise ContractError('scope cutover not verified')
        adapter = self.adapters.get(envelope['routing']['adapter'])
        try:
            if adapter is None:
                raise ContractError('adapter unbound')
            receipt = adapter.accept(envelope)
            if not isinstance(receipt, dict) or receipt.get('kind') != 'accepted':
                raise ContractError('receiver has not durably accepted')
            self.record_receipt(event_id=receipt['event_id'], task_id=task_id,
                business_id=envelope['identity']['business_operation_id'],
                generation=envelope['execution']['generation'], kind='accepted', payload=receipt['payload'])
        except Exception as exc:
            with self.store.transaction() as db:
                current = self.task(task_id, db)
                identity, execution = envelope['identity'], envelope['execution']
                if (current['business_id'],current['generation'],current['attempt']) != (
                        identity['business_operation_id'],execution['generation'],identity['dispatch_attempt_id']):
                    # A delayed transport failure belongs to its own attempt. It
                    # cannot alter an execution reconciled under a newer identity.
                    self.store.audit(db,'stale_dispatch_failure',task_id,type(exc).__name__)
                    return current
                self.store.audit(db,'dispatch_transport_error',task_id,type(exc).__name__)
                if current['dispatch_state'] != 'accepted':
                    # The ACK channel and execution evidence are independent. A
                    # running/ended callback can commit before accept() times out.
                    # Preserve that evidence and any recorded result or conflict;
                    # a result alone still does not establish execution completion.
                    state = current['execution_state']
                    if state not in TERMINAL and state != 'running':
                        state = 'unknown'
                    db.execute("UPDATE h1_tasks SET dispatch_state='unknown',execution_state=?,last_error=? WHERE task_id=?",
                               (state,current['last_error'] or type(exc).__name__,task_id))
                    db.execute("UPDATE h1_dispatch SET state='unknown' WHERE task_id=?", (task_id,))
        return self.task(task_id)

    def query_execution(self, task_id):
        task = self.task(task_id)
        cap = self.config.capabilities[task['intent']]
        adapter = self.adapters.get(cap.adapter)
        if not cap.status_query or adapter is None:
            return {'status':'unknown','reason':'status_interface_unavailable'}
        # Receiver status lookup must use stable operation identity, not a new dispatch.
        return adapter.query(task['business_id'], task['run_id'], task['generation'])

    def request_cancel(self, task_id):
        task = self.task(task_id)
        cap = self.config.capabilities[task['intent']]
        adapter = self.adapters.get(cap.adapter)
        if not cap.cancellation or adapter is None:
            return {'status':'unsupported','stopped':False}
        return adapter.cancel(task['business_id'], task['run_id'], task['generation'])
