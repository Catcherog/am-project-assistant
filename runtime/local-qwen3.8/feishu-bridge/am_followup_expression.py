"""Read native follow-up state and reuse am_style's final expression presenter.

This bridge owns no scheduler, model, approval, outbox, or business database.
Default mode is off. Exact previews and confirmation commands never enter it.
"""
from dataclasses import dataclass
from hashlib import sha256
import json

from am_style.contracts import ContractError, digest
from am_style.followup import FollowupSpeech, project_followup, render_followup
from am_style.runtime import TerminalPresenter, StaleFrame, recheck_before_send
from fs_agent.config import BoundaryError
from am_followup.followup import FollowupError


@dataclass(frozen=True)
class FollowupPrepared:
    snapshot: FollowupSpeech
    response: object
    row_json: str
    item_id: str


class NativeFollowupExpression:
    """Opt-in host seam. Use shadow before considering owner-only enforce.

    Enqueue must use this object, not call notify(prepared.response.text)
    directly: the final read and original outbox commit share the host fence.
    """
    def __init__(self, host, presenter=None):
        self.host = host
        self.presenter = presenter or TerminalPresenter(mode="off", wall_clock=host.s.clock)
        with host.s.lock:
            host.s.conn.execute("CREATE TABLE IF NOT EXISTS followup_expression_outputs("
                "outbox_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,row_json TEXT NOT NULL,"
                "snapshot_hash TEXT NOT NULL,text_hash TEXT NOT NULL,expires REAL NOT NULL,"
                "frame_hash TEXT NOT NULL,metrics_json TEXT NOT NULL,delivery_state TEXT NOT NULL)")

    def _authorize(self, row):
        self.host.actor(row)  # existing owner identity and feature checks
        if row.get("_response_kind") in {"followup_text_preview", "followup_text_confirm"}:
            raise ContractError("followup_exact_preview_not_rewritable")

    def _capture_locked(self, row, item_id, native_db, information_db):
        h = self.host
        if row["expires"] <= h.s.clock():
            raise StaleFrame("followup_request_expired")
        if (row.get("app"), row.get("tenant"), row.get("user"), row.get("chat_type")) != (h.app, h.tenant, h.owner, "p2p"):
            raise StaleFrame("followup_owner_scope_changed")
        source_revision = str(h.sources.info.store.access_version())
        expected_source = row.get("_source_access_version")
        if expected_source is not None and str(expected_source) != source_revision:
            raise StaleFrame("followup_source_access_changed")
        item = h.am.items.get(item_id, native_db)
        if item["owner"] != h.owner:
            raise StaleFrame("followup_item_owner_changed")
        sem = native_db.execute("SELECT * FROM action_semantics WHERE item_id=?", (item_id,)).fetchone()
        if sem is None:
            raise ContractError("followup_semantics_unbound")
        occurrences = native_db.execute("SELECT o.*,a.intent_id FROM reminder_occurrences o "
            "LEFT JOIN followup_adoptions a ON a.occurrence_id=o.id WHERE o.item_id=? "
            "ORDER BY o.scheduled_at DESC,o.rowid DESC LIMIT 1", (item_id,)).fetchall()
        delivery = dict.fromkeys(("occurrence_id", "intent_id", "request_uuid", "message_id", "body_sha256", "target_sha256", "receipt"))
        delivery["state"] = "none"
        feedback = []
        job = None
        if not occurrences and json.loads(sem["legacy_history"]):
            raise ContractError("followup_legacy_history_requires_original_view")
        if occurrences:
            o = dict(occurrences[0])
            if not o.get("intent_id"):
                raise ContractError("followup_legacy_occurrence_requires_original_view")
            mapping = native_db.execute("SELECT * FROM followup_adoptions WHERE occurrence_id=?", (o["id"],)).fetchone()
            intent = native_db.execute("SELECT * FROM followup_intents WHERE tenant_id=? AND intent_id=?",
                (h.tenant, o["intent_id"])).fetchone()
            if intent is None or mapping["request_uuid"] != o["request_key"] or intent["request_uuid"] != o["request_key"]:
                raise StaleFrame("followup_native_identity_changed")
            payload = json.loads(o["payload"])
            if payload.get("followup_intent") != o["intent_id"] or payload.get("item_id") != item_id or digest(payload) != o["hash"]:
                raise StaleFrame("followup_native_payload_changed")
            body = payload.get("body")
            if type(body) is not str or not body:
                raise ContractError("followup_native_body_unavailable")
            target = payload.get("target")
            if type(target) is not dict or not target:
                raise ContractError("followup_native_target_unavailable")
            state = {"ready": "queued", "confirmed_not_sent": "queued", "sending": "sending",
                     "unknown": "unknown", "sent": "unknown", "cancelled": "cancelled",
                     "failed": "failed", "expired": "failed"}.get(o["state"])
            if state is None:
                raise ContractError("followup_native_state_unsupported")
            if state == "queued" and intent["state"] not in ("queued", "claimed"):
                raise ContractError("followup_queue_not_executable")
            delivery.update(state=state, occurrence_id=o["id"], intent_id=o["intent_id"],
                request_uuid=o["request_key"], message_id=o.get("message_id"),
                body_sha256=sha256(body.encode()).hexdigest(), target_sha256=digest(target))
            if o["state"] == "sent" and o["readback_state"] == "verified":
                # Reuses all original payload/target/receipt checks; no boolean
                # from the model and no fuzzy message search can prove success.
                proof, _ = h.recovery._proof(o["id"], native_db)
                if proof is None or proof.message_id != o["message_id"]:
                    raise StaleFrame("followup_receipt_unavailable")
                delivery["state"] = "verified"
                delivery["receipt"] = {k: delivery[k] for k in ("occurrence_id", "intent_id", "request_uuid", "message_id", "body_sha256", "target_sha256")}
            feedback = [dict(r) for r in native_db.execute(
                "SELECT message_id,occurrence_id,actor,kind FROM followup_reply_links WHERE item_id=? AND occurrence_id=? ORDER BY message_id",
                (item_id, o["id"]))]
            assignees = set(json.loads(item["assignees"]))
            if any(f["actor"] not in assignees for f in feedback):
                raise StaleFrame("followup_feedback_assignee_changed")
            # This predicate is the native scheduler's durable readback queue,
            # restricted further to a known exact message ID. It does not create
            # another job. Sending or exhausted jobs do not get an 'I will check'.
            if (delivery["state"] == "unknown" and o["state"] in ("unknown", "sent")
                    and o.get("message_id") and o.get("readback_state") != "verified"
                    and type(o.get("reconcile_attempts")) is int and o["reconcile_attempts"] < 6
                    and type(o.get("next_reconcile")) in (int, float)
                    and o["next_reconcile"] < row["expires"]
                    and getattr(h.am, "scheduler", None) is h.scheduler):
                # Original consumer calls scheduler.step even with new sends
                # disabled. It must still be the active original consumer.
                if getattr(h.am, "followup", None) is h and getattr(h.scheduler, "followup", None) is h:
                    job = {k: delivery[k] for k in ("occurrence_id", "request_uuid", "body_sha256", "target_sha256", "message_id")}
                    job.update(job_id="native-readback/" + o["id"], kind="original_scheduler_readback",
                        state="queued", not_before=o["next_reconcile"], expires_at=row["expires"])
        closed = item["state"] == "closed"
        value = {"schema_version": "am-followup-speech/1",
            "binding": {"turn_id": row["id"], "scope": [row[k] for k in ("tenant", "app", "chat", "user")],
                "source_revision": source_revision, "item_version": item["version"], "semantic_version": sem["version"],
                "cancel_epoch": item["decision_version"], "owner_id": h.owner, "requester_id": row["user"]},
            "expires_at": row["expires"], "question": row["body"],
            "item": {"id": item_id, "label": item["description"], "state": item["state"], "paused": bool(item["paused"]),
                "verifier": item["verifier"], "closed_by": item["closed_by"] if closed else None,
                "close_evidence_sha256": sha256(item["close_evidence"].encode()).hexdigest() if closed and item["close_evidence"] else None},
            "delivery": delivery, "feedback": feedback, "readback_job": job}
        return FollowupSpeech.freeze(value)

    def capture(self, row, item_id, *, authorize=True):
        if authorize:
            self._authorize(row)
        with self.host.state_transaction() as (native_db, information_db):
            return self._capture_locked(row, item_id, native_db, information_db)

    def prepare(self, row, item_id, *, legacy_text=None, deadline=None, fastpath=True):
        if self.presenter.mode == "off":
            return None  # original path remains the owner of its output
        from am_style.contracts import canonical
        self._authorize(row)
        row = dict(row, expires=min(row["expires"], self.host.s.clock() + 600))
        snapshot = self.capture(row, item_id, authorize=False)
        row["_source_access_version"] = snapshot.data["binding"]["source_revision"]
        frame = project_followup(snapshot)
        if legacy_text is None:
            # A new status entry has no older free-text answer to preserve.
            # Its deterministic baseline must use this exact captured snapshot.
            legacy_text = render_followup(snapshot)
        def current(_):
            if self.capture(row, item_id, authorize=False).fingerprint != snapshot.fingerprint:
                raise StaleFrame("followup_outcome_changed_during_expression")
        response = self.presenter.prepare(frame, legacy_text=legacy_text, guard=current,
            task_deadline_monotonic=deadline, fastpath=fastpath)
        return FollowupPrepared(snapshot, response, canonical(row), item_id)

    def enqueue(self, prepared):
        """Local fresh-state fence and the existing outbox transaction only."""
        row = json.loads(prepared.row_json)
        self._authorize(row)
        h = self.host
        with h.state_transaction() as (native_db, information_db):
            current = self._capture_locked(row, prepared.item_id, native_db, information_db)
            if current.fingerprint != prepared.snapshot.fingerprint:
                raise StaleFrame("followup_outcome_changed_before_enqueue")
            frame = project_followup(current)
            text = recheck_before_send(prepared.response, frame, lambda _: None, wall_clock=h.s.clock)
            # Source version has native type for existing strict equality.
            row["_source_access_version"] = h.sources.info.store.access_version()
            outbox_id = h.am.notify(row, text=text, kind="followup_expression", object_id=prepared.item_id,
                version=current.data["binding"]["item_version"], hash=current.fingerprint,
                conn=native_db, event_key="followup-expression/" + row["id"])
            native_db.execute("INSERT OR IGNORE INTO followup_expression_outputs VALUES(?,?,?,?,?,?,?,?,?)",
                (outbox_id, prepared.item_id, prepared.row_json, current.fingerprint,
                 sha256(text.encode()).hexdigest(), row["expires"], frame.fingerprint,
                 prepared.response.metrics_json, "queued"))
            return outbox_id

    def valid_notification(self, outbox_row):
        """Called by the original gateway before delivery; stale text is dropped.

        No re-generation or business replay is attempted here. Missing metadata
        is a refusal for this kind, while unrelated outputs return None.
        """
        h = self.host
        records = h.s.rows("SELECT * FROM followup_expression_outputs WHERE outbox_id=?", (outbox_row["id"],))
        if not records:
            notes = h.s.rows("SELECT kind FROM am_notifications WHERE outbox_id=?", (outbox_row["id"],))
            return False if any(r["kind"] == "followup_expression" for r in notes) else None
        record = records[0]
        if record["expires"] <= h.s.clock() or self.presenter.mode == "off":
            return False
        row = json.loads(record["row_json"])
        try:
            self._authorize(row)
            with h.state_transaction() as (native_db, information_db):
                current = self._capture_locked(row, record["item_id"], native_db, information_db)
                if current.fingerprint != record["snapshot_hash"]:
                    return False
                body = outbox_row.get("body")
                if outbox_row.get("kind") != "text" or type(body) is not str or sha256(body.encode()).hexdigest() != record["text_hash"]:
                    return False
        except (StaleFrame, ContractError, BoundaryError, FollowupError):
            return False
        return True

    def cleanup(self):
        self.host.s.execute("DELETE FROM followup_expression_outputs WHERE expires<=?", (self.host.s.clock(),))

    def delivered(self, native_db, outbox_row, message_id):
        # Called by the original delivery callback, never by model generation.
        # native_sent is not an independently verified platform readback.
        if message_id:
            native_db.execute("UPDATE followup_expression_outputs SET delivery_state='native_sent' WHERE outbox_id=?",
                (outbox_row["id"],))
