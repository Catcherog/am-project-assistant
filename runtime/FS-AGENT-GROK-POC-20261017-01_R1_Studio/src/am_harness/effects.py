"""Fences for the ORIGINAL AM outbox, using the same SQLite transaction.

This module contains no sender, approval engine or retry policy. The host keeps
its existing grant checks, payload hash/version checks, transport, verified
receipts, and unknown-outcome reconciliation. SQL callbacks must not do I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import sqlite3
from typing import Callable, Any

from .store import DurableStore, Fence, RunSnapshot, StoreError


@dataclass(frozen=True)
class Suppression:
    suppressed: int = 0
    needs_reconciliation: int = 0
    already_sent: int = 0


class FencedOutbox:
    """Add revision bindings to existing ``outbox`` rows without duplicating them.

    Production must put DurableStore in the original AM SQLite database, use
    finish(on_commit=...) to bind newly enqueued replies, and invoke
    before_dispatch() inside the original pending->sending transaction. Merely
    checking a fence before a network call is not an atomic cancellation guard.
    """
    def __init__(self, store: DurableStore, *, binding_verifier=None):
        self.store = store
        if binding_verifier is None:
            from .legacy_binding import verify_legacy_binding
            binding_verifier = verify_legacy_binding
        if not callable(binding_verifier):
            raise ValueError("OUTBOX_ORIGIN_VERIFIER_REQUIRED")
        self.binding_verifier = binding_verifier
        with sqlite3.connect(str(store.path)) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS am_harness_outbox(
                outbox_id TEXT PRIMARY KEY, run_id TEXT NOT NULL,
                scope_key TEXT NOT NULL, revision INTEGER NOT NULL,
                cancel_epoch INTEGER NOT NULL, lease_generation INTEGER NOT NULL,
                payload_hash TEXT NOT NULL, state TEXT NOT NULL,
                receipt_id TEXT, verified INTEGER NOT NULL DEFAULT 0,
                UNIQUE(outbox_id,run_id,revision,cancel_epoch))""")

    def _transaction(self, conn: sqlite3.Connection) -> None:
        if not conn.in_transaction:
            raise RuntimeError("OUTBOX_TRANSACTION_REQUIRED")
        # A similarly named table in a different database is not the same gate.
        from pathlib import Path
        databases = conn.execute("PRAGMA database_list").fetchall()
        main = next((r[2] for r in databases if r[1] == "main"), "")
        if not main or Path(main).resolve() != self.store.path.resolve():
            raise RuntimeError("OUTBOX_MUST_SHARE_RUN_DATABASE")

    @staticmethod
    def _row(conn, query, params=()):
        cursor = conn.execute(query, params)
        row = cursor.fetchone()
        return None if row is None else dict(zip([d[0] for d in cursor.description], row))

    @staticmethod
    def _payload_hash(row) -> str:
        value = {k: row.get(k) for k in ("app", "tenant", "user", "chat", "chat_type", "kind", "body", "expires")}
        return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest()

    def enqueue(self, conn: sqlite3.Connection, run: RunSnapshot,
                original_enqueue: Callable[[sqlite3.Connection, RunSnapshot], str]) -> str:
        """Call only the trusted original-AM local outbox insertion callback."""
        self._transaction(conn)
        self.store.check_in_transaction(conn, run.fence, allowed_states=("running",))
        outbox_id = original_enqueue(conn, run)
        self.bind(conn, run, outbox_id)
        return outbox_id

    def bind(self, conn: sqlite3.Connection, run: RunSnapshot, outbox_id: str) -> None:
        self._transaction(conn)
        self.store.check_in_transaction(conn, run.fence, allowed_states=("running",))
        if not isinstance(outbox_id, str) or not outbox_id:
            raise ValueError("OUTBOX_ID_INVALID")
        row = self._row(conn, "SELECT * FROM outbox WHERE id=?", (outbox_id,))
        if row is None or row["state"] != "pending":
            raise RuntimeError("OUTBOX_NOT_PENDING")
        expected = (run.scope.tenant_id, run.scope.app_id, run.scope.actor_id, run.scope.chat_id)
        if tuple(row.get(k) for k in ("tenant", "app", "user", "chat")) != expected:
            raise RuntimeError("OUTBOX_SCOPE_MISMATCH")
        if row.get("expires", 0) > run.expires_at or row.get("expires", 0) <= self.store.clock():
            raise RuntimeError("OUTBOX_TTL_MISMATCH")
        self.binding_verifier(conn, run, outbox_id)
        existing = self._row(conn, "SELECT * FROM am_harness_outbox WHERE outbox_id=?", (outbox_id,))
        binding = (run.id, run.fence.scope_key, run.revision, run.cancel_epoch)
        if existing:
            if tuple(existing[k] for k in ("run_id", "scope_key", "revision", "cancel_epoch")) != binding:
                raise RuntimeError("OUTBOX_BINDING_CONFLICT")
            return
        conn.execute("INSERT INTO am_harness_outbox(outbox_id,run_id,scope_key,revision,cancel_epoch,lease_generation,payload_hash,state) VALUES(?,?,?,?,?,?,?,?)",
                     (outbox_id, *binding, run.fence.generation, self._payload_hash(row), "pending"))

    def before_dispatch(self, conn: sqlite3.Connection, outbox_id: str) -> bool:
        """Call after existing authorization, before original claim, in ONE tx.

        If this returns True for a bound row, the host must mark that original
        outbox row sending in this transaction. The network call follows commit.
        A later cancel reports that dispatch may already have started; it does
        not promise to retract the remote side effect.
        """
        self._transaction(conn)
        binding = self._row(conn, "SELECT * FROM am_harness_outbox WHERE outbox_id=?", (outbox_id,))
        if binding is None:
            return True  # Non-harness AM outbox rows keep their original policy.
        row = self._row(conn, "SELECT * FROM outbox WHERE id=?", (outbox_id,))
        if row is None or row["state"] != "pending" or binding["state"] != "pending":
            return False
        fence = Fence(binding["run_id"], binding["scope_key"], binding["revision"], binding["cancel_epoch"], binding["lease_generation"])
        try:
            run = self.store.check_in_transaction(conn, fence, allowed_states=("succeeded", "waiting_review"),
                                                   enforce_execution_deadline=False)
            self.binding_verifier(conn, run, outbox_id)
            if self._payload_hash(row) != binding["payload_hash"]:
                raise RuntimeError("OUTBOX_PAYLOAD_CHANGED")
        except (StoreError, RuntimeError, ValueError):
            conn.execute("UPDATE outbox SET state='rejected',error='HARNESS_FENCE_REJECTED' WHERE id=? AND state='pending'", (outbox_id,))
            conn.execute("UPDATE am_harness_outbox SET state='suppressed' WHERE outbox_id=?", (outbox_id,))
            return False
        conn.execute("UPDATE am_harness_outbox SET state='dispatching' WHERE outbox_id=?", (outbox_id,))
        return True

    def suppress_pending(self, conn: sqlite3.Connection, old_fence: Fence) -> Suppression:
        """Pass as DurableStore.cancel/revise(on_change=...). Never retries."""
        self._transaction(conn)
        rows = conn.execute("""SELECT b.outbox_id,o.state FROM am_harness_outbox b
            JOIN outbox o ON o.id=b.outbox_id
            WHERE b.run_id=? AND b.scope_key=? AND b.revision=? AND b.cancel_epoch=?""",
            (old_fence.run_id, old_fence.scope_key, old_fence.revision, old_fence.cancel_epoch)).fetchall()
        counts = {"suppressed": 0, "needs_reconciliation": 0, "already_sent": 0}
        for outbox_id, state in rows:
            if state == "pending":
                conn.execute("UPDATE outbox SET state='rejected',error='HARNESS_CANCELLED' WHERE id=? AND state='pending'", (outbox_id,))
                label = "suppressed"
                counts[label] += 1
            elif state == "sent":
                label = "sent"
                counts["already_sent"] += 1
            elif state in ("sending", "unknown", "delivery_unknown"):
                label = "needs_reconciliation"
                counts[label] += 1
            else:
                label = "suppressed"
            conn.execute("UPDATE am_harness_outbox SET state=? WHERE outbox_id=?", (label, outbox_id))
        return Suppression(**counts)

    def observe_receipt(self, conn: sqlite3.Connection, outbox_id: str, receipt_id: str, *, verified: bool) -> None:
        """Mirror a host-verified receipt, including one arriving after cancel.

        This does not acknowledge a preview or grant approval. Those remain the
        existing LegacyConversationBridge / accepted_hash responsibilities.
        """
        self._transaction(conn)
        if type(verified) is not bool or not isinstance(receipt_id, str) or not receipt_id:
            raise ValueError("RECEIPT_INVALID")
        row = self._row(conn, "SELECT * FROM outbox WHERE id=?", (outbox_id,))
        if row is None or row.get("state") != "sent" or row.get("message_id") != receipt_id:
            raise RuntimeError("HOST_RECEIPT_MISMATCH")
        conn.execute("UPDATE am_harness_outbox SET state=?,receipt_id=?,verified=? WHERE outbox_id=?",
                     ("sent" if verified else "needs_reconciliation", receipt_id, int(verified), outbox_id))
