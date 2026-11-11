"""Small foreground path: commit once, wake dispatcher, never await a model.

Transport, sender, latest-scope guard and AM handoff are separate trusted ports.
The Bot is not given an API for setting approval or business completion state.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import re
import threading
import time
from typing import Any

from .config import GatewayConfig
from .models import (
    Admission, BotResult, BotTransport, BusinessHandoff, ContractError,
    DispatchReceipt, GuardDecision, InboundMessage, ReplySender, ResultGuard,
    SendReceipt, bounded_text, canonical_json, finite_time,
)
from .store import Store


def is_acknowledgement(text: str) -> bool:
    compact = re.sub(r"[\s，。！!,.：:…]+", "", text).lower()
    return compact in {
        "收到", "好的", "已收到", "处理中", "正在处理", "收到正在处理", "收到处理中",
        "好的正在处理", "请稍候", "已受理", "已加入队列", "收到稍后回复",
        "processing", "received", "workingonit", "ok", "ack",
    }


class ScopeGuard:
    """Mandatory local checks, with a host-owned current-policy/content check.

    Without current_check this guard denies output. A result's own citations or
    claim of correctness never stand in for the host's authority.
    """
    def __init__(self, current_check=None, *, current_authorize=None, clock=time.time):
        self.current_check = current_check
        self.current_authorize = current_authorize
        self.clock = clock

    def authorize(self, message: InboundMessage) -> GuardDecision:
        if not message.auth.verified or message.auth.expires_at <= self.clock():
            return GuardDecision(False, reason="IDENTITY_EXPIRED")
        if self.current_authorize is None:
            return GuardDecision(False, reason="HOST_AUTHORIZATION_UNBOUND")
        result = self.current_authorize(message)
        return result if isinstance(result, GuardDecision) else GuardDecision(False, reason="INVALID_AUTHORIZATION")

    def validate(self, message: InboundMessage, result: BotResult) -> GuardDecision:
        authorized = self.authorize(message)
        if not authorized.allowed:
            return authorized
        if self.current_check is None:
            return GuardDecision(False, reason="HOST_GUARD_UNBOUND")
        snapshots = {s["source_id"]: s["revision"] for s in message.context.get("sources", [])}
        for source in result.sources:
            sid = source["source_id"]
            if sid not in message.auth.allowed_source_ids or snapshots.get(sid) != source["revision"]:
                return GuardDecision(False, reason="SOURCE_BINDING_MISMATCH")
        if message.auth.disclosure == "general_only" and result.sources:
            return GuardDecision(False, reason="DISCLOSURE_DENIED")
        decision = self.current_check(message, result)
        if not isinstance(decision, GuardDecision):
            return GuardDecision(False, reason="INVALID_HOST_GUARD")
        return GuardDecision(decision.allowed, decision.useful and not is_acknowledgement(result.text),
                             decision.reason)


class Gateway:
    def __init__(
        self, config: GatewayConfig, store: Store, transport: BotTransport,
        sender: ReplySender, guard: ResultGuard, handoff: BusinessHandoff | None = None,
        *, clock=time.time,
    ):
        config.validate()
        self.config, self.store, self.transport = config, store, transport
        self.sender, self.guard, self.handoff, self.clock = sender, guard, handoff, clock
        self._wake, self._stop = threading.Event(), threading.Event()
        self._thread = None
        self._dispatch_pool = self._send_pool = None
        self._dispatch_futures, self._send_futures = set(), set()
        self.last_worker_error = ""
        self._lifecycle_lock = threading.Lock()

    def admit(self, message: InboundMessage) -> Admission:
        if not self.config.enabled:
            return Admission(False, reason="disabled")
        message.validate()
        now = self.clock()
        if message.sender_is_bot or not message.auth.verified:
            return Admission(False, reason="identity_rejected")
        if message.auth.expires_at <= now:
            return Admission(False, reason="identity_expired")
        if self.config.allowed_sender_ids and message.sender_id not in self.config.allowed_sender_ids:
            return Admission(False, reason="sender_outside_scope")
        if message.chat_type == "group":
            if not message.mentions_bot or message.chat_id not in self.config.allowed_chat_ids:
                return Admission(False, reason="group_outside_scope")
        elif not self.config.allow_private:
            return Admission(False, reason="private_disabled")
        # The host resolves current business controls before calling this method.
        if message.text.lstrip().startswith("/") or message.context.get("route_hint") == "am_business":
            return Admission(False, reason="existing_am_workflow")
        if now - message.event_created_at > self.config.max_message_age_seconds or message.event_created_at > now + 5:
            return Admission(False, reason="event_outside_time_window")
        row, created = self.store.admit(message, ttl=self.config.request_ttl_seconds,
                                        max_pending=self.config.max_pending)
        self._wake.set()
        return Admission(True, row["request_id"], row["status"] if created else "duplicate",
                         row["error_code"])

    def _make_request(self, row: dict) -> dict[str, Any]:
        message = InboundMessage.from_dict(json.loads(row["message_json"]))
        context = dict(message.context)
        if "recent_messages" not in context:
            context["recent_messages"] = self.store.history(message)
        return {
            "schema": "am.grok.request.v1",
            "request_id": row["request_id"], "attempt_id": row["attempt_id"],
            "conversation_id": message.conversation_id,
            "deadline_at": row["deadline_at"], "created_at": row["created_at"],
            "backend_profile": self.config.backend_profile,
            "priority": "interactive" if message.origin == "realtime" else "reconcile",
            "message": {
                "tenant_id": message.tenant_id, "app_id": message.app_id,
                "message_id": message.message_id, "chat_id": message.chat_id,
                "chat_type": message.chat_type, "sender_id": message.sender_id,
                "text": message.text, "event_created_at": message.event_created_at,
            },
            "context": context,
            "policy": {
                "policy_version": message.auth.policy_version,
                "disclosure": message.auth.disclosure,
                "allowed_source_ids": list(message.auth.allowed_source_ids),
                "side_effects": "propose_only",
                "direct_feishu_send": False,
                "reply_target": "bound_original_message",
                "use_realtime_request_not_full_poll": True,
            },
            "callback_url": self.config.callback_url,
        }

    def _dispatch_row(self, row):
        try:
            message = InboundMessage.from_dict(json.loads(row["message_json"]))
            if message.auth.expires_at <= self.clock():
                self.store.block_dispatch(row["request_id"], row["attempt_id"], "IDENTITY_EXPIRED")
                return
            decision = self.guard.authorize(message)
            if not isinstance(decision, GuardDecision) or not decision.allowed:
                self.store.block_dispatch(row["request_id"], row["attempt_id"], "CURRENT_AUTHORIZATION_DENIED")
                return
            if message.auth.expires_at <= self.clock() or not self.store.dispatch_current(row["request_id"], row["attempt_id"]):
                self.store.block_dispatch(row["request_id"], row["attempt_id"], "DISPATCH_NO_LONGER_CURRENT")
                return
        except Exception:
            self.store.block_dispatch(row["request_id"], row["attempt_id"], "AUTHORIZATION_UNAVAILABLE")
            return
        try:
            request = self._make_request(row)
            if not self.store.mark_submission(row["request_id"], row["attempt_id"]):
                return
            receipt = self.transport.submit(request)
            if not isinstance(receipt, DispatchReceipt):
                receipt = DispatchReceipt("unknown", detail="INVALID_TRANSPORT_RECEIPT")
        except Exception:
            receipt = DispatchReceipt("unknown", detail="TRANSPORT_EXCEPTION")
        self.store.dispatch_receipt(row["request_id"], row["attempt_id"], receipt.status, receipt.run_id)
        self._wake.set()

    def dispatch_once(self) -> bool:
        if not self.config.enabled:
            return False
        self.store.expire()
        row = self.store.claim_dispatch(self.config.max_inflight, self.config.max_reconcile_inflight)
        if row is None:
            return False
        self._dispatch_row(row)
        return True

    def accept_result(self, raw: dict) -> dict:
        result = BotResult.parse(raw)
        decision = self.store.accept_result(result)
        self._wake.set()
        return decision

    def mark_started(self, raw: dict) -> dict:
        if not isinstance(raw, dict):
            raise ContractError("invalid started callback")
        fields = set(raw)
        if fields not in ({"request_id", "attempt_id"}, {"schema", "request_id", "attempt_id"}):
            raise ContractError("invalid started callback")
        if "schema" in raw and raw["schema"] != "am.grok.started.v1":
            raise ContractError("invalid started callback")
        rid = bounded_text(raw["request_id"], "request_id", 128)
        attempt = bounded_text(raw["attempt_id"], "attempt_id", 128)
        return self.store.mark_started(rid, attempt)

    def cancel(self, conversation_id: str, sender_id: str, request_id: str | None = None) -> dict:
        result = self.store.cancel(conversation_id, sender_id, request_id)
        self._wake.set()
        return result

    def _send_row(self, row):
        rid, lease = row["request_id"], row["lease"]
        port_started = False
        try:
            message = InboundMessage.from_dict(json.loads(row["message_json"]))
            result = BotResult.parse(json.loads(row["result_json"]))
            if message.auth.expires_at <= self.clock():
                self.store.finish_output(rid, lease, "blocked", error_code="IDENTITY_EXPIRED")
                return
            decision = self.guard.validate(message, result)
            if not isinstance(decision, GuardDecision) or not decision.allowed:
                self.store.finish_output(rid, lease, "blocked",
                                         error_code=decision.reason if isinstance(decision, GuardDecision) else "INVALID_GUARD")
                return
            # Recheck after any host lookup/guard, immediately before invoking the port.
            if message.auth.expires_at <= self.clock() or not self.store.output_current(rid, lease):
                self.store.finish_output(rid, lease, "cancelled", error_code="OUTPUT_NO_LONGER_CURRENT")
                return
            if row["output_kind"] == "handoff":
                if self.handoff is None:
                    self.store.finish_output(rid, lease, "blocked", error_code="BUSINESS_PORT_UNBOUND")
                    return
                port_started = True
                receipt = self.handoff.submit(message, result, row["idempotency_key"])
                # A handoff receipt proves registration, never actual business completion.
                status = "handed_off" if isinstance(receipt, SendReceipt) and receipt.status == "sent" and receipt.message_id else (
                    "failed" if isinstance(receipt, SendReceipt) and receipt.status == "failed" else "unknown")
                self.store.finish_output(rid, lease, status, message_id=getattr(receipt, "message_id", ""))
                return
            port_started = True
            receipt = self.sender.send(message, result.text, row["idempotency_key"])
            if not isinstance(receipt, SendReceipt):
                receipt = SendReceipt("unknown", detail="INVALID_SENDER_RECEIPT")
            readback = receipt.readback_at
            if readback is not None:
                finite_time(readback, "readback_at")
                if readback < row["created_at"] or readback > self.clock() + 5:
                    readback = None
            self.store.finish_output(
                rid, lease, receipt.status, message_id=receipt.message_id, readback_at=readback,
                useful=decision.useful and row["result_origin"] == "bot" and not is_acknowledgement(result.text),
                error_code="" if receipt.status == "sent" else "SEND_" + receipt.status.upper(),
            )
        except Exception:
            # A guard failure precedes any side effect. A thrown port error is ambiguous.
            self.store.finish_output(rid, lease, "unknown" if port_started else "blocked",
                                     error_code="OUTPUT_EXCEPTION" if port_started else "GUARD_OR_INPUT_UNAVAILABLE")
        finally:
            self._wake.set()

    def send_once(self) -> bool:
        if not self.config.enabled:
            return False
        self.store.expire()
        row = self.store.claim_output()
        if row is None:
            return False
        self._send_row(row)
        return True

    def start(self):
        with self._lifecycle_lock:
            if not self.config.enabled or (self._thread and self._thread.is_alive()):
                return
            self.store.recover()
            self.store.expire()
            self._stop.clear()
            self._dispatch_pool = ThreadPoolExecutor(max_workers=self.config.max_inflight, thread_name_prefix="grok-dispatch")
            self._send_pool = ThreadPoolExecutor(max_workers=self.config.reply_workers, thread_name_prefix="grok-output")
            self._thread = threading.Thread(target=self._run, name="grok-gateway", daemon=True)
            self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.store.expire()
                self._dispatch_futures = {f for f in self._dispatch_futures if not f.done()}
                self._send_futures = {f for f in self._send_futures if not f.done()}
                while len(self._send_futures) < self.config.reply_workers:
                    row = self.store.claim_output()
                    if row is None:
                        break
                    self._send_futures.add(self._send_pool.submit(self._send_row, row))
                while len(self._dispatch_futures) < self.config.max_inflight:
                    row = self.store.claim_dispatch(self.config.max_inflight, self.config.max_reconcile_inflight)
                    if row is None:
                        break
                    self._dispatch_futures.add(self._dispatch_pool.submit(self._dispatch_row, row))
                self.last_worker_error = ""
            except Exception as exc:
                # Do not kill all lanes or print payloads/credentials on an error.
                self.last_worker_error = type(exc).__name__
            self._wake.wait(self.config.tick_seconds)
            self._wake.clear()

    def stop(self, timeout: float = 2.0) -> dict:
        with self._lifecycle_lock:
            self._stop.set()
            self._wake.set()
            if self._thread:
                self._thread.join(timeout=timeout)
            if self._dispatch_pool:
                self._dispatch_pool.shutdown(wait=False, cancel_futures=True)
            if self._send_pool:
                self._send_pool.shutdown(wait=False, cancel_futures=True)
            pending = sum(not f.done() for f in self._dispatch_futures | self._send_futures)
            # Ports must have real network deadlines. A Python thread is not killed here.
            return {"scheduler_stopped": not bool(self._thread and self._thread.is_alive()),
                    "ports_in_flight": pending, "provider_stopped": False}
