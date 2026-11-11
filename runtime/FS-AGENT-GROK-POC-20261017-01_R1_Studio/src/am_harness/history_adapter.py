"""Filter stale harness revisions out of the ORIGINAL AM conversation history.

ConversationStore commits history in a different transaction from publication.
Cancellation just after its finish_event() can therefore leave an unsent answer
in that database. This adapter preserves original writes and source retention,
but excludes stale/failed task revisions on reads. It does not delete receipts.
"""
from __future__ import annotations

import json
import re
import sqlite3

from .store import DurableStore, Scope, StoreError


_ENGINE_EVENT = re.compile(r"harness:(run_[a-f0-9]{40}):v([1-9][0-9]*)")


class FencedHistoryStore:
    """A history-only facade over the reviewed AM ConversationStore.

    Install at host initialization by assigning the same facade to
    bridge.store and bridge.agent.store. All other methods delegate unchanged;
    existing tool closures may retain the original store, which is still the
    sole owner of actions, notes, messages and event deduplication.

    This adapter is intentionally tied to the reviewed conversation_messages
    schema and its _access_fingerprint helper. Recheck it on a newer AM tree.
    Structured source-version filtering still belongs to ContextBuilder.
    """
    def __init__(self, original, tasks: DurableStore):
        if not callable(getattr(original, "_connection", None)) or not callable(getattr(original, "_now", None)):
            raise ValueError("HISTORY_STORE_CONTRACT_UNAVAILABLE")
        self.original, self.tasks = original, tasks

    def __getattr__(self, name):
        return getattr(self.original, name)

    def _visible_assistant(self, run) -> bool:
        # A generated answer is not proof the user received it. Require the
        # original publisher's verified receipt mirror before using it as an
        # assistant statement in a subsequent harness turn.
        with sqlite3.connect(str(self.tasks.path)) as conn:
            try:
                return conn.execute("""SELECT 1 FROM am_harness_outbox
                    WHERE run_id=? AND scope_key=? AND revision=? AND cancel_epoch=?
                    AND lease_generation=? AND state='sent' AND verified=1 LIMIT 1""",
                    (run.id,run.scope.key,run.revision,run.cancel_epoch,run.lease_generation)).fetchone() is not None
            except sqlite3.OperationalError as exc:
                if "no such table" in str(exc): return False
                raise

    def history_records(self, ctx, limit=20):
        from am_conversation.store import HISTORY_RETENTION, _access_fingerprint, _derived_fingerprint, _limit
        _limit(limit,HISTORY_RETENTION)
        with self.original._connection() as conn:
            rows=conn.execute("""SELECT id,event_id,role,content_json,content_expires FROM conversation_messages
                WHERE scope_key=? AND ((role='user' AND access_fingerprint=?) OR (role!='user' AND access_fingerprint=?)) AND content_expires>?
                ORDER BY id DESC LIMIT ?""",
                (ctx.session_id,_access_fingerprint(ctx),_derived_fingerprint(ctx),self.original._now(),HISTORY_RETENTION)).fetchall()
        scope=Scope.from_context(ctx); snapshots={}; visibility={}; kept=[]
        for row in rows:
            event=row["event_id"] or ""
            if event.startswith("harness:"):
                match=_ENGINE_EVENT.fullmatch(event)
                if match is None: continue
                run_id,revision=match[1],int(match[2])
                if run_id not in snapshots:
                    try: snapshots[run_id]=self.tasks.get(scope,run_id)
                    except StoreError: snapshots[run_id]=None
                run=snapshots[run_id]
                if (run is None or run.purged or run.revision!=revision
                        or run.state not in ("queued","running","succeeded","waiting_review")):
                    continue
                if row["role"]=="assistant":
                    if run.state not in ("succeeded","waiting_review"): continue
                    if run_id not in visibility: visibility[run_id]=self._visible_assistant(run)
                    if not visibility[run_id]: continue
            kept.append(dict(row))
            if len(kept)>=limit: break
        return list(reversed(kept))


    def history(self, ctx, limit=20):
        return [{'role': row['role'], 'content': json.loads(row['content_json'])}
                for row in self.history_records(ctx, limit)]
