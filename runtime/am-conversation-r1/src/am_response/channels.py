"""Protected observations in legacy chat databases; no replacement sender."""
import json
from am_outcome.types import OutcomeEnvelope, VERSION, fingerprint, encode


class ProtectedChannelJournal:
    def __init__(self, db):
        self.db = db
        db.execute('''CREATE TABLE IF NOT EXISTS response_channel_v2 (
            event_id TEXT PRIMARY KEY, presentation_id TEXT NOT NULL,
            scope_key TEXT NOT NULL, semantic_hash TEXT NOT NULL,
            outcome_json TEXT NOT NULL, state TEXT NOT NULL,
            message_id TEXT, expires REAL NOT NULL)''')
        db.execute("UPDATE response_channel_v2 SET state='unknown' WHERE state='prepared'")

    def prepare(self, event_id, scope, family, text, expires, *, source_metadata=None):
        scope = list(scope)
        digest = fingerprint(text)
        ident = fingerprint([scope, event_id, 'final'])
        envelope = OutcomeEnvelope.freeze({'schema_version': VERSION,
            'binding': {'kind': 'event', 'scope': scope, 'origin_event_id': event_id,
                        'engine_event_id': event_id}, 'intent': {'goal_set_complete': False},
            'families': [family], 'completion': 'blocked', 'work_units': [], 'claims': [],
            'protected_blocks': [{'id': 'legacy-body', 'role': 'legacy', 'required': True,
                                  'text': text, 'semantic_hash': digest}],
            'dependencies': {'policy': 'original_channel_guards', 'complete': False,
                             'source_metadata': source_metadata or {}},
            'allowed_next_steps': [], 'pending_actions': [], 'expires_at': expires,
            'selected_source': 'legacy_protected', 'business_completion': 'unclassified',
            'generation_adopted': False, 'presentation_calls': 0})
        old = self.db.execute('SELECT scope_key,semantic_hash FROM response_channel_v2 WHERE event_id=?', (event_id,)).fetchone()
        if old is not None:
            if tuple(old) != (fingerprint(scope), digest): raise ValueError('CHANNEL_PRESENTATION_CONFLICT')
            return ident
        self.db.execute('INSERT INTO response_channel_v2 VALUES(?,?,?,?,?,\'prepared\',NULL,?)',
                        (event_id, ident, fingerprint(scope), digest, envelope.serialized, expires))
        return ident

    def receipt(self, event_id, *, message_id=None, unknown=False):
        # HTTP acceptance is not independent readback. Never label verified.
        self.db.execute('UPDATE response_channel_v2 SET state=?,message_id=? WHERE event_id=?',
                        ('unknown' if unknown else 'accepted', message_id if isinstance(message_id, str) else None, event_id))

    def cleanup(self, now):
        self.db.execute('DELETE FROM response_channel_v2 WHERE expires<=?', (now,))
