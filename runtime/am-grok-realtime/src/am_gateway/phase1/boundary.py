"""Task-scoped tool port. Only effective when ALL executor tools are routed here.

There is no assertion that the external Grok Bot currently uses this port.
"""
import json

from ..models import ContractError, dumps, key
from ..assistant.continuity import validate_work_context
from .contracts import Policy


class ToolBoundary:
    def _authorize(self, db, task_id, generation, *, require_running=True):
        task = self.task(task_id, db)
        policy = Policy.parse(task['policy_json'])
        if policy.revoked or generation != task['generation']:
            raise ContractError('revoked policy or old generation')
        if task['last_error'] == 'source_changed' or task['verification_state'] == 'conflict':
            raise ContractError('source or evidence changed')
        source = db.execute("""SELECT i.version,i.is_deleted,i.chat_id,i.legacy_owned,b.accepted
            FROM gw_items i JOIN h1_inbox b USING(item_id) WHERE item_id=?""", (task['item_id'],)).fetchone()
        if (not source or source['is_deleted'] or source['legacy_owned'] or not source['accepted']
                or source['version'] != task['source_version'] or source['chat_id'] not in policy.source_chats):
            raise ContractError('request source is no longer usable')
        # R5's durable receive queue observes edits/recalls before the slow
        # background lane can refresh gw_items. They already invalidate an old
        # tool read, manual proposal execution or send at this shared boundary.
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='r5_intake'").fetchone():
            intake = db.execute('SELECT state,event_json FROM r5_intake WHERE item_id=?', (task['item_id'],)).fetchone()
            if intake:
                latest, request = json.loads(intake['event_json']), json.loads(task['request_json'])
                fields = ('body', 'is_deleted', 'sender_id', 'chat_id', 'thread_id', 'parent_id',
                          'root_id', 'mentions', 'attachments', 'revision', 'version_time')
                if (intake['state'] in {'recalled', 'referred', 'needs_owner'} or latest.get('is_deleted')
                        or any(latest.get(name) != request.get(name) for name in fields)):
                    raise ContractError('assistant request source changed before background refresh')
        session = db.execute('SELECT * FROM h1_sessions WHERE scope=?', (task['scope'],)).fetchone()
        if require_running and (not session or session['task_id'] != task_id or session['generation'] != generation or session['state'] != 'held'):
            raise ContractError('executor no longer owns this scope')
        context = json.loads(task['context_json'] or '{}')
        # Revocation and revision changes invalidate an already accepted snapshot.
        for item in context.get('evidence_items', []):
            row = db.execute('SELECT * FROM h1_info WHERE info_id=?', (item['info_id'],)).fetchone()
            if not row or not self.information.allowed(db, row, policy):
                raise ContractError('accepted context is no longer usable')
        validate_work_context(self, db, task, policy)
        return task, policy

    def read_information(self, task_id, generation, info_id):
        with self.store.transaction() as db:
            task, policy = self._authorize(db, task_id, generation)
            row = db.execute('SELECT * FROM h1_info WHERE info_id=?', (info_id,)).fetchone()
            if not row or not self.information.allowed(db, row, policy):
                raise ContractError('read outside task scope')
            return {n: row[n] for n in ('info_id','project','nature','body')}

    def tool_action(self, task_id, generation, action, *, target='', body='', sender=None):
        if action != 'reply':
            raise ContractError('business writes and arbitrary tools are not connected')
        return self._reply(task_id, generation, target, body, sender, executor=True)

    def deliver_recorded_result(self, task_id, sender, *, claim_guard=None):
        """Trusted delivery component, not an executor override for expired epochs."""
        task = self.task(task_id)
        result = json.loads(task['result_json'] or '{}')
        if not result.get('reply_text') or task['execution_state'] not in {'succeeded','failed'}:
            raise ContractError('no finished recorded answer')
        return self._reply(task_id, task['generation'], Policy.parse(task['policy_json']).reply_target,
                           result['reply_text'], sender, executor=False, claim_guard=claim_guard)

    def _reply(self, task_id, generation, target, body, sender, *, executor, claim_guard=None):
        if not isinstance(body, str) or not body.strip() or len(body.encode()) > 30000 or sender is None:
            raise ContractError('bounded answer and bound sender required')
        with self.store.transaction() as db:
            if claim_guard is not None and not claim_guard(db):
                raise ContractError('delivery claim no longer owns the scope')
            task, policy = self._authorize(db, task_id, generation, require_running=executor)
            if not policy.allow_reply or target != policy.reply_target:
                raise ContractError('delivery target is not authorized')
            effect_id = 'send_' + key(task['business_id'], 'answer')[:32]
            old = db.execute('SELECT * FROM h1_effects WHERE effect_id=?', (effect_id,)).fetchone()
            if old:
                if (old['body'], old['target']) != (body, target):
                    raise ContractError('same delivery identity different body or target')
                return dict(old)  # including unknown: never resend blindly
            db.execute("INSERT INTO h1_effects VALUES(?,?,?,?,?,'sending',NULL,?,NULL)",
                       (effect_id,task_id,generation,body,target,self.store.clock()))
        # Sender is the trusted host port and must not expose a credential to the Agent.
        # Immediate recheck closes the ordinary queued/revoked case; upstream SDK must
        # preserve this authorization point and stable effect ID in cross-process use.
        denied = False
        with self.store.transaction() as db:
            try:
                if claim_guard is not None and not claim_guard(db):
                    raise ContractError('delivery claim changed before send')
                _, current_policy = self._authorize(db, task_id, generation, require_running=executor)
                if not current_policy.allow_reply or target != current_policy.reply_target:
                    raise ContractError('delivery authority changed before send')
            except ContractError:
                db.execute("UPDATE h1_effects SET state='denied' WHERE effect_id=?",(effect_id,))
                denied = True
        if denied:
            raise ContractError('authorization changed before send')
        try:
            receipt = sender(target=target, body=body, idempotency_key=effect_id)
            state = 'sent' if isinstance(receipt, dict) and receipt.get('platform_id') else 'unknown'
        except Exception:
            receipt, state = {}, 'unknown'
        with self.store.transaction() as db:
            db.execute('UPDATE h1_effects SET state=?,platform_id=?,platform_time=? WHERE effect_id=?',
                       (state,receipt.get('platform_id'),receipt.get('platform_time'),effect_id))
            db.execute('UPDATE h1_tasks SET delivery_state=? WHERE task_id=?',
                       ('delivered' if state == 'sent' else 'unknown', task_id))
            current = self.task(task_id,db)
            if state=='sent' and current['execution_state'] in {'succeeded','failed','cancelled'}:
                db.execute("UPDATE h1_sessions SET state='free',task_id=NULL WHERE task_id=? AND generation=?",(task_id,generation))
            return dict(db.execute('SELECT * FROM h1_effects WHERE effect_id=?', (effect_id,)).fetchone())

    def record_send_receipt(self, *, task_id, effect_id, platform_id):
        """Trusted sender/receiver supplies the exact effect-to-message mapping.

        This is not a content/thread search and cannot be called by a source
        message. Independent platform readback still follows verify_delivery.
        """
        if not isinstance(platform_id,str) or not platform_id:
            raise ContractError('platform message identity required')
        with self.store.transaction() as db:
            effect=db.execute('SELECT * FROM h1_effects WHERE effect_id=? AND task_id=?',(effect_id,task_id)).fetchone()
            if not effect or effect['state'] not in {'sending','unknown','sent'}:
                raise ContractError('no unresolved or sent effect')
            if effect['platform_id'] and effect['platform_id']!=platform_id:
                raise ContractError('conflicting platform message mapping')
            db.execute("UPDATE h1_effects SET state='sent',platform_id=? WHERE effect_id=?",(platform_id,effect_id))
            db.execute("UPDATE h1_tasks SET delivery_state='delivered' WHERE task_id=?",(task_id,))

    def verify_delivery(self, task_id, evidence, *, claim_guard=None):
        """Evidence comes from an independently bound host reader, never model text."""
        with self.store.transaction() as db:
            if claim_guard is not None and not claim_guard(db):
                raise ContractError('delivery claim changed during readback')
            task = self.task(task_id, db)
            effect = db.execute('SELECT * FROM h1_effects WHERE task_id=?', (task_id,)).fetchone()
            if not effect or not effect['platform_id']:
                raise ContractError('exact output mapping missing')
            if any(evidence.get(k) != v for k,v in {
                    'platform_id':effect['platform_id'], 'target':effect['target'],
                    'body':effect['body'], 'sender_id':self.config.bot_id}.items()):
                raise ContractError('output evidence mismatch; thread match is insufficient')
            platform_time = evidence.get('platform_time')
            if not isinstance(platform_time, (int,float)) or isinstance(platform_time, bool) or platform_time <= 0:
                raise ContractError('verified platform generation time required')
            stamps = json.loads(task['stamps'])
            stamps.setdefault('t_p', platform_time)
            stamps.setdefault('t_o', self.store.clock())
            result = json.loads(task['result_json'] or '{}')
            full = result.get('response_kind') == 'sufficient' and not result.get('remaining_requirements')
            if full:
                stamps.setdefault('full_completed_at', platform_time)
            if task['verification_state'] != 'conflict':
                db.execute("UPDATE h1_tasks SET verification_state='sufficient',stamps=? WHERE task_id=?", (dumps(stamps),task_id))
            if task['execution_state'] in {'succeeded','failed','cancelled'}:
                db.execute("UPDATE h1_sessions SET state='free',task_id=NULL WHERE task_id=? AND generation=?",
                           (task_id,task['generation']))
            return self.task(task_id, db)
