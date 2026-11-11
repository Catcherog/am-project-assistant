"""Converge the original sender's durable results without another network send.

The native occurrence is the existing sender's record, not a second outbox.
``sent/verified`` plus an exact adoption is a durable pending reconciliation:
it remains discoverable if the process exits between the two local commits.
Only local bookkeeping is retried here; no message search or resend is allowed.
"""
import hashlib
import json
from datetime import datetime, timezone

from am_followup.followup import FollowupError, ReadbackEvidence, _draft_load
from am_followup.rendering import feishu_post
from fs_agent.project_catalog import digest


class FollowupRecovery:
    def __init__(self, host):
        self.host, self.s = host, host.s
        with self.s.lock:
            self.s.conn.executescript('''
            CREATE TABLE IF NOT EXISTS followup_result_retries(
              occurrence_id TEXT PRIMARY KEY,attempts INTEGER NOT NULL,
              next_attempt REAL NOT NULL,error TEXT NOT NULL,updated REAL NOT NULL);
            ''')

    def _proof(self, ident, c):
        h = self.host
        o = h.scheduler.occurrence(ident, c)
        mapping = c.execute('SELECT * FROM followup_adoptions WHERE occurrence_id=?', (ident,)).fetchone()
        if not mapping:
            return None, None
        i = c.execute('SELECT * FROM followup_intents WHERE tenant_id=? AND intent_id=?',
                      (h.tenant, mapping['intent_id'])).fetchone()
        if not i:
            raise FollowupError('recovery_intent_missing')
        if o['state'] != 'sent' or o['readback_state'] != 'verified' or not o['message_id']:
            return None, i
        b = c.execute('SELECT * FROM followup_bindings WHERE preview_id=?', (i['preview_id'],)).fetchone()
        payload = json.loads(o['payload'])
        receipt = json.loads(o['receipt'] or '{}')
        draft = _draft_load(i['draft_json'])
        if (not b or mapping['request_uuid'] != i['request_uuid']
                or o['request_key'] != i['request_uuid'] or mapping['plan_id'] != o['plan_id']
                or mapping['item_id'] != o['item_id'] or b['item_id'] != o['item_id']
                or payload.get('item_id') != o['item_id'] or payload.get('followup_intent') != i['intent_id']
                or digest(payload) != o['hash'] or payload.get('body') != draft.body
                or payload.get('target') != json.loads(b['native_target'])
                or receipt.get('message_id') != o['message_id']):
            raise FollowupError('recovery_native_mapping_changed')
        target = payload['target']
        if target['kind'] == 'group':
            target_id = target['payload']['group']['id']
            mentions = tuple(p['id'] for p in target['payload']['people'])
            if payload.get('post') != feishu_post(payload['body'], mentions) or payload.get('card'):
                raise FollowupError('recovery_native_content_changed')
            attention = mentions
        elif target['kind'] == 'recipient_p2p':
            target_id, mentions = target['user'], ()
            attention = (target_id,)
            if payload.get('post') or payload.get('card'):
                raise FollowupError('recovery_native_content_changed')
        else:
            raise FollowupError('recovery_native_target_changed')
        if (target['kind'] != draft.target.kind or target_id != draft.target.target_id
                or tuple(sorted(attention)) != tuple(sorted(draft.target.attention_ids))
                or tuple(sorted(mentions)) != tuple(sorted(draft.target.mention_ids))):
            raise FollowupError('recovery_native_target_changed')
        proof = ReadbackEvidence(o['request_key'], ident, o['message_id'], target['kind'], target_id,
            hashlib.sha256(payload['body'].encode()).hexdigest(), mentions,
            'send_response_message_id', 'native:' + ident,
            datetime.fromtimestamp(self.s.clock(), timezone.utc))
        return proof, i

    def readback(self, ident):
        with self.s.tx() as c:
            proof, _ = self._proof(ident, c)
            return proof

    def repair_native_states(self):
        """Unknown admissions are never left in the ready-to-send selection."""
        with self.s.tx() as c:
            return bool(c.execute('''
                UPDATE reminder_occurrences SET state='unknown',readback_state='pending',
                  error='FOLLOWUP_ADMISSION_UNKNOWN'
                WHERE state IN ('ready','confirmed_not_sent','sending') AND id IN (
                  SELECT a.occurrence_id FROM followup_adoptions a
                  JOIN followup_intents i ON i.intent_id=a.intent_id
                  WHERE i.tenant_id=? AND i.state='unknown')
                ''', (self.host.tenant,)).rowcount)

    def settle(self, ident):
        """Apply only an already verified exact receipt, including after expiry.

        Expired approval blocks a new send, not acknowledgement of a historical
        send. A verified result can fence its own stale sending claim; this does
        not touch another worker/intent or allocate another request UUID.
        """
        try:
            with self.s.tx() as c:
                proof, i = self._proof(ident, c)
                if proof is None or i['state'] == 'confirmed':
                    return False
                if i['state'] not in ('unknown', 'sending'):
                    raise FollowupError('recovery_intent_state_mismatch')
                intent_id = i['intent_id']
                if i['state'] == 'sending':
                    c.execute("UPDATE followup_intents SET state='unknown',generation=generation+1,"
                              "claim_token='',reason='verified_native_result_pending',updated_at=? "
                              "WHERE tenant_id=? AND intent_id=? AND state='sending'",
                              (self.s.clock(), self.host.tenant, intent_id))
                    c.execute('INSERT INTO followup_audit(tenant_id,object_id,event,detail,recorded_at) VALUES(?,?,?,?,?)',
                              (self.host.tenant, intent_id, 'unknown', 'verified_native_result_pending', self.s.clock()))
            result = self.host.coordinator.reconcile(self.host.tenant, intent_id, proof)
            if result.state != 'confirmed':
                raise FollowupError('recovery_result_unsettled')
            self.s.execute('DELETE FROM followup_result_retries WHERE occurrence_id=?', (ident,))
            return True
        except Exception as exc:
            # The pending condition remains durable even if writing this retry
            # hint also fails. Error strings never contain raw provider content.
            try:
                with self.s.tx() as c:
                    row = c.execute('SELECT attempts FROM followup_result_retries WHERE occurrence_id=?', (ident,)).fetchone()
                    attempts = (row[0] if row else 0) + 1
                    delay = min(300, 5 * 2 ** min(attempts - 1, 6))
                    c.execute('''INSERT INTO followup_result_retries VALUES(?,?,?,?,?)
                        ON CONFLICT(occurrence_id) DO UPDATE SET attempts=excluded.attempts,
                          next_attempt=excluded.next_attempt,error=excluded.error,updated=excluded.updated''',
                        (ident, attempts, self.s.clock() + delay,
                         exc.code if isinstance(exc, FollowupError) else type(exc).__name__, self.s.clock()))
            except Exception:
                pass
            raise

    def recover(self):
        changed = self.repair_native_states()
        rows = self.s.rows('''
            SELECT a.occurrence_id FROM followup_adoptions a
            JOIN followup_intents i ON i.intent_id=a.intent_id
            JOIN reminder_occurrences o ON o.id=a.occurrence_id
            LEFT JOIN followup_result_retries r ON r.occurrence_id=o.id
            WHERE i.tenant_id=? AND i.state IN ('unknown','sending')
              AND o.state='sent' AND o.readback_state='verified'
              AND COALESCE(r.next_attempt,0)<=?
            ORDER BY COALESCE(r.next_attempt,0),o.scheduled_at,o.rowid LIMIT 50
            ''', (self.host.tenant, self.s.clock()))
        for row in rows:
            try:
                changed = self.settle(row['occurrence_id']) or changed
            except Exception:
                # One bad or temporarily unavailable mapping must not block
                # another result, the native scheduler, or an unrelated item.
                continue
        return changed
