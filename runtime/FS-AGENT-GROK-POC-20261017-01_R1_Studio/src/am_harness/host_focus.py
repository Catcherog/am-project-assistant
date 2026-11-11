"""One event-ordered, expiring focus for quick reads and durable conversation."""
import json
from .fastpath import FocusSnapshot
from .history_adapter import _ENGINE_EVENT
from .store import Scope, StoreError


class HostFocus:
    def __init__(self, host):
        self.host = host
        with host.s.tx() as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS am_harness_focus(scope_key TEXT PRIMARY KEY, project_id TEXT, event_order INTEGER NOT NULL, event_id TEXT NOT NULL, engine_event TEXT, permission TEXT NOT NULL, expires REAL NOT NULL)')

    def _identity(self, ctx):
        from am_conversation.legacy import _hash
        h = self.host
        current = h.bridge._current(ctx)
        rows = h.s.rows('SELECT rowid AS event_order,created,expires FROM am_inbox WHERE id=?', (current['row']['id'],))
        if len(rows) != 1:
            raise RuntimeError('HARNESS_SOURCE_MISSING')
        permission = _hash([ctx.scope, sorted(ctx.allowed_project_ids), current['grant'], ctx.is_owner, ctx.source_access_version])
        return current, rows[0], permission

    def get(self, ctx):
        h = self.host
        current, source, permission = self._identity(ctx)
        rows = h.s.rows('SELECT * FROM am_harness_focus WHERE scope_key=?', (ctx.session_id,))
        if not rows:
            return None
        row = rows[0]
        if (row['event_order'] > source['event_order'] or row['permission'] != permission
                or row['expires'] <= h.s.clock() or not row['project_id'] or not ctx.can_access_project(row['project_id'])):
            return None
        if row['engine_event']:
            match = _ENGINE_EVENT.fullmatch(row['engine_event'])
            if not match:
                return None
            try:
                run = h.tasks.get(Scope.from_context(ctx), match[1])
            except StoreError:
                return None
            if run.revision != int(match[2]) or run.purged or run.state not in ('queued','running','succeeded','waiting_review'):
                return None
        return FocusSnapshot(ctx.session_id, row['project_id'], row['expires'])

    def project(self, ctx):
        focus = self.get(ctx)
        return focus.project_id if focus else None

    def remember(self, ctx, project_id, event_id=None):
        if project_id is not None and not ctx.can_access_project(project_id):
            raise RuntimeError('PROJECT_ACCESS_DENIED')
        h = self.host
        current, source, permission = self._identity(ctx)
        engine = current.get('engine_event_id')
        expires = min(source['created']+600, source['expires'], ctx.content_expires_at)
        with h.s.tx() as conn:
            fence = h.hooks.fence
            if engine:
                if fence is None:
                    raise RuntimeError('HARNESS_UNBOUND_RUN')
                h.tasks.check_in_transaction(conn, fence, allowed_states=('running',))
            conn.execute("""INSERT INTO am_harness_focus VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(scope_key) DO UPDATE SET project_id=excluded.project_id,
                event_order=excluded.event_order,event_id=excluded.event_id,engine_event=excluded.engine_event,
                permission=excluded.permission,expires=excluded.expires
                WHERE excluded.event_order>=am_harness_focus.event_order""",
                (ctx.session_id,project_id,source['event_order'],current['row']['id'],engine,permission,expires))

    def invalidate(self, ctx):
        self.remember(ctx, None)
