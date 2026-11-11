"""Trusted host entry points; this module never opens a Feishu long connection."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
import uuid
from typing import Any, Callable, Mapping

from .models import Observation, Principal, ScopeViolation, IntegrityConflict, LeaseLost, digest
from .policy import require_operator
from .store import Store


class EventBridge:
    """Call ONLY after the existing SDK/gateway verified the incoming event.

    This is not an HTTP webhook verifier. app/tenant checks are defense in depth,
    not a replacement for the existing signed/encrypted transport verification.
    """
    def __init__(self, store: Store, expected_app_id: str, expected_tenant_id: str):
        if not expected_app_id or not expected_tenant_id:
            raise ValueError("Explicit app and tenant identity are required")
        self.store = store
        self.app_id, self.tenant_id = expected_app_id, expected_tenant_id

    def accept_verified(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        header = envelope.get("header", {})
        if header.get("app_id") != self.app_id or header.get("tenant_key") != self.tenant_id:
            raise ScopeViolation("Incoming event app/tenant does not match the configured ingress")
        event_type = header.get("event_type")
        if event_type not in {"im.message.receive_v1", "im.message.recalled_v1"}:
            return {"accepted": False, "reason": "unsupported_event_type"}
        event = envelope.get("event", {})
        message = event.get("message", event)
        chat_id = message.get("chat_id") or event.get("chat_id")
        if not chat_id or not header.get("event_id"):
            raise ValueError("Registered message events require a chat and event identifier")
        sources = [s for s in self.store.list_sources() if s.enabled and s.tenant_id == self.tenant_id
                   and s.kind == "im_chat" and s.remote_id == chat_id]
        if not sources:
            # Do not retain unapproved message content just to classify it later.
            return {"accepted": False, "reason": "source_not_registered"}
        if len(sources) != 1:
            raise IntegrityConflict("Multiple active sources point to the same chat")
        if event_type == "im.message.receive_v1":
            from .adapters.messages import sender_in_scope
            if not sender_in_scope(sources[0], dict(message, sender=event.get('sender', {}))):
                return {'accepted': False, 'reason': 'sender_outside_source_scope'}
            try:
                created_at = int(message["create_time"])
            except (KeyError, ValueError, TypeError):
                raise ValueError("Message creation time is required before capture") from None
            if created_at < sources[0].start_ms:
                return {"accepted": False, "reason": "outside_authorized_history"}
        payload = {"event": deepcopy(event), "event_type": event_type,
                   "event_time": header.get("create_time")}
        return self.store.accept_event(sources[0].source_id, header["event_id"], event_type, payload)

    def capture_then_route(self, envelope: Mapping[str, Any], route: Callable[[], Any]) -> Any:
        self.accept_verified(envelope)
        # ALWAYS execute original routing, including on duplicate capture. Otherwise
        # an earlier routing failure would be suppressed on the delivery retry.
        return route()


def _event_ms(value: Any, fallback: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    if number > 10**17:
        return number // 1_000_000
    if number > 10**14:
        return number // 1000
    if number < 10**11:
        return number * 1000
    return number


class EventWorker:
    def __init__(self, store: Store):
        self.store = store

    def run_once(self) -> bool:
        job = self.store.claim_job(("event",))
        if job is None:
            return False
        args = json.loads(job["payload_json"])
        try:
            saved = self.store.get_event(args["event_key"])
            if saved is None:
                self.store.open_gap(args["source_id"], "", "event_expired_before_processing", "Raw event expired; use an authorized history reread")
                self.store.fail_job(job["job_id"], job["lease_token"], "event_expired", max_attempts=1)
                return True
            content = json.loads(saved["payload_json"])
            source = self.store.get_source(saved["source_id"])
            event = content["event"]
            message = deepcopy(event.get("message", event))
            if saved["event_type"] == "im.message.receive_v1":
                from .adapters.messages import normalize_message
                message.setdefault("sender", event.get("sender", {}))
                obs, discoveries = normalize_message(source, message)
            else:
                object_id = message.get("message_id") or event.get("message_id")
                if not object_id:
                    raise ValueError("Recall is missing its message identity")
                previous = self.store.get_observation(source.source_id, object_id)
                occurred = _event_ms(event.get("recall_time") or content.get("event_time"), saved["received_at_ms"])
                payload = {"recall": True}
                if previous:
                    payload["created_at_ms"] = previous.payload.get("created_at_ms", previous.occurred_at_ms)
                obs = Observation(source.source_id, object_id, "recall:" + str(occurred), occurred,
                                  payload=payload, deleted=True)
                discoveries = ()
            with self.store.transaction() as db:
                self.store._check_job(db, job["job_id"], job["lease_token"])
                self.store._record(db, obs)
                for item in discoveries:
                    db.execute("INSERT OR IGNORE INTO discoveries VALUES(?,?,?,?,?,?,'pending',?)",
                               (digest([source.source_id, item.kind, item.remote_id]), source.source_id,
                                item.kind, item.remote_id, item.reason[:300], json.dumps(item.settings), self.store.clock()))
                db.execute("UPDATE jobs SET state='done',finished_at_ms=?,lease_until_ms=NULL WHERE job_id=?",
                           (self.store.clock(), job["job_id"]))
        except Exception as exc:
            try:
                self.store.fail_job(job["job_id"], job["lease_token"], getattr(exc, "code", type(exc).__name__),
                                    retry_after_seconds=min(300, 2 ** min(job["attempts"], 8)))
            except LeaseLost:
                pass
        return True


def record_offline(store: Store, source_id: str, principal: Principal, text: str,
                   subject_id: str, structured_claims: list[dict[str, Any]] | None = None,
                   idempotency_key: str | None = None) -> dict[str, Any]:
    """Trusted owner path. `confirmed` claims require explicit owner intent upstream.

    Never map a model-produced `confirmed=true` into this argument. An ordinary
    offline message is recorded without structured claims and stays reported.
    """
    source = store.get_source(source_id)
    require_operator(source, principal)
    if source.kind != "manual":
        raise ScopeViolation("Offline input requires a registered manual source")
    if not text.strip() or not subject_id:
        raise ValueError("Text and a stable subject identity are required")
    if len(text) > 200_000:
        raise ValueError("Offline record exceeds the supported size")
    trusted = source.settings.get("trusted_actors", [source.owner_user_id])
    if principal.user_id not in trusted:
        raise ScopeViolation("Actor is not registered for this offline source")
    claims = deepcopy(structured_claims or [])
    for claim in claims:
        if not isinstance(claim, dict) or not claim.get("quote") or claim["quote"] not in text:
            raise ValueError("Offline claims require an exact quote from the owner's record")
    object_id = "offline:" + (idempotency_key or uuid.uuid4().hex)
    payload = {"actor_id": principal.user_id, "trusted_actor": True, "subject_id": subject_id,
               "structured_claims": claims}
    version = digest([text, payload])
    with store._lock:
        previous = store.conn.execute("SELECT current_version,occurred_at_ms FROM objects WHERE source_id=? AND object_id=?",
                                      (source_id, object_id)).fetchone()
    if previous and previous["current_version"] != version:
        raise IntegrityConflict("Offline idempotency key reused with different content")
    timestamp = previous["occurred_at_ms"] if previous else store.clock()
    observation = Observation(source_id, object_id, version, timestamp, text, payload)
    with store.transaction() as db:
        result = store._record(db, observation, authoritative=True)
        db.execute("UPDATE sources SET availability='ok',last_checked_ms=?,last_error=NULL,updated_at_ms=? WHERE source_id=?",
                   (store.clock(), store.clock(), source_id))
    return {"source_id": source_id, "object_id": object_id, "version": version, "status": result,
            "fact_status": "pending_extraction", "external_actions": 0}


def signal_source_change(store: Store, source_id: str, verified_event_id: str) -> str:
    """Host-verified project/docs notification: refresh a registered source.

    No notification text or model argument can register a new source or authorize
    a write. Project webhook verification remains in the existing host gateway.
    """
    source = store.get_source(source_id)
    if not source.enabled or not verified_event_id:
        raise ScopeViolation("Enabled source and verified event identity are required")
    return store.enqueue("sync", digest([source_id, "dirty", verified_event_id]),
                         {"source_id": source_id, "mode": "reconcile"})
