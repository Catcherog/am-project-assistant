"""Local latency evidence. Platform time and process monotonic time never mix."""
from contextlib import contextmanager
import json
import math
import time
import uuid

from ..models import dumps


PROCESS_CLOCK = 'host_monotonic:' + uuid.uuid4().hex


class AssistantTrace:
    def __init__(self, store, owner_id, *, clock=time.monotonic):
        self.store, self.owner_id, self.clock = store, owner_id, clock
        with store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r6_trace(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,owner_id TEXT NOT NULL,
                item_id TEXT NOT NULL,name TEXT NOT NULL,clock_domain TEXT NOT NULL,
                started REAL NOT NULL,ended REAL NOT NULL,details_json TEXT NOT NULL)''')
            db.execute('CREATE INDEX IF NOT EXISTS r6_trace_request ON r6_trace(owner_id,item_id,seq)')

    def mark(self, item_id, name, *, at=None, clock_domain=None, details=None):
        point = self.clock() if at is None else at
        try:
            self._write(item_id, name, point, point, clock_domain or PROCESS_CLOCK, details or {})
        except Exception:
            # Missing telemetry is never evidence of non-delivery and must not
            # change a successful send into an unknown business outcome.
            return False
        return True

    def _write(self, item_id, name, start, end, domain, details):
        if (not all(isinstance(x, (int, float)) and math.isfinite(x) for x in (start, end))
                or end < start):
            raise ValueError('invalid trace interval')
        with self.store.transaction() as db:
            db.execute('INSERT INTO r6_trace(owner_id,item_id,name,clock_domain,started,ended,details_json) VALUES(?,?,?,?,?,?,?)',
                       (self.owner_id, item_id, name, domain, start, end, dumps(details)))

    @contextmanager
    def span(self, item_id, name):
        start = self.clock()
        outcome = 'returned'
        try:
            yield
        except BaseException:
            outcome = 'raised'
            raise
        finally:
            try:
                self._write(item_id, name, start, self.clock(), PROCESS_CLOCK, {'outcome': outcome})
            except Exception:
                pass

    def read(self, item_id):
        try:
            rows = self.store.rows('SELECT * FROM r6_trace WHERE owner_id=? AND item_id=? ORDER BY seq',
                                   (self.owner_id, item_id))
            return [{**row, 'details': json.loads(row['details_json'])} for row in rows]
        except Exception:
            return []

    def local_elapsed(self, item_id):
        rows = self.read(item_id)
        starts = [r for r in rows if r['name'] == 'ingress_received']
        ends = [r for r in rows if r['name'] == 'delivery_readback_observed']
        # Editing the same platform message creates a new request generation.
        # Never recycle the first successful generation's duration.
        for start in reversed(starts):
            generation = start['details'].get('request_version')
            valid = [r for r in ends if r['clock_domain'] == start['clock_domain']
                     and r['ended'] >= start['started']
                     and r['details'].get('request_version') == generation]
            if valid:
                return min(r['ended'] for r in valid) - start['started']
            return None
        return None
