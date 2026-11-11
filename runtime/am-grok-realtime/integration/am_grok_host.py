"""Host adapter for the audited R10 AM consumer, with no Feishu websocket.

The host supplies authority; Bot results cannot create it.  In particular,
``snapshot`` and ``business_active`` must be bounded local reads.  The existing
DEMO identity cache alone does not prove group membership, disclosure, current
source revisions, or a factual answer.  Those checks belong in a deliberately
configured HostBindings profile.  Missing bindings never become allow-all.

The bridge hook runs BEFORE AmSelfService.ingest: that method can otherwise
capture every ordinary message.  Active business state is fenced first.  A
durable gateway receipt always wins over reclassification of a replay.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter
import hashlib
import hmac
import importlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from am_grok_gateway.config import GatewayConfig
from am_grok_gateway.core import Gateway, ScopeGuard
from am_grok_gateway.http_server import CallbackServer
from am_grok_gateway.models import (
    Admission, AuthContext, BotResult, ContractError, GuardDecision,
    InboundMessage, SendReceipt, canonical_json,
)
from am_grok_gateway.store import Store
from am_grok_gateway.transport import WebhookTransport, is_loopback_host, secret_bytes


@dataclass(frozen=True)
class HostEnvelope:
    tenant_id: str
    app_id: str
    event_id: str
    message_id: str
    chat_id: str
    chat_type: str
    sender_id: str
    text: str
    event_created_at: float
    received_at: float
    mentions_bot: bool
    thread_id: str = ""
    parent_id: str = ""
    root_id: str = ""

    def legacy_row(self) -> dict[str, Any]:
        return {
            "id": self.message_id, "message_id": self.message_id,
            "app": self.app_id, "tenant": self.tenant_id, "user": self.sender_id,
            "chat": self.chat_id, "chat_type": self.chat_type,
            "thread": self.thread_id, "parent": self.parent_id,
            "_source_meta": {"thread": self.thread_id, "parent": self.parent_id,
                             "root": self.root_id},
        }


@dataclass(frozen=True)
class HostSnapshot:
    auth: AuthContext
    context: dict[str, Any] = field(default_factory=dict)
    measurement_class: str = "ordinary"


@dataclass(frozen=True)
class HostBindings:
    """Trusted on-device profile. These are functions, not payload claims.

    snapshot: local cached identity + group membership/disclosure + context.
    current_authorize: recheck policy/identity/source-read grants before dispatch
        and again before output; may do bounded I/O on the dedicated pool.
    check_result: host-owned content/source/freshness check, including unsupported
        business facts; return useful=True only for a useful, validated answer.
    business_active: extra local business/control fences beyond the R10 fence.
    handoff: optional candidate-registration port; never executes Bot instructions
        as approved AM actions, and never re-ingests synthesized user messages.
    """
    snapshot: Callable[[HostEnvelope], HostSnapshot | None]
    current_authorize: Callable[[InboundMessage], GuardDecision]
    check_result: Callable[[InboundMessage, BotResult], GuardDecision]
    business_active: Callable[[HostEnvelope], bool]
    handoff: Any = None

    def validate(self) -> None:
        if not all(callable(f) for f in (
            self.snapshot, self.current_authorize, self.check_result, self.business_active,
        )):
            raise ContractError("HOST_PROFILE_UNBOUND")


def _receipt(store: Store, tenant: str, app: str, mid: str) -> dict | None:
    """Indexed read against the version-1 gateway schema; never scan all history."""
    dedupe = hashlib.sha256(canonical_json([tenant, app, mid]).encode()).hexdigest()
    uri = Path(store.path).resolve().as_uri() + "?mode=ro"
    db = sqlite3.connect(uri, uri=True, timeout=0.05)
    db.row_factory = sqlite3.Row
    try:
        row = db.execute(
            "SELECT request_id,conversation_id,sender_id,status,message_json "
            "FROM gateway_requests WHERE dedupe_key=?", (dedupe,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        db.close()


class OriginalMessageSender:
    """One final response through R10's original reply(mid, text) closure.

    That closure has uuid5(app_id + '/' + original_mid).  Do not enable a separate
    acknowledgement using it.  Exceptions are ambiguous, so never auto-retry.
    A real reply ID proves API acceptance, not mobile visibility or readback.
    """
    def __init__(self, reply: Callable[[str, str], str]):
        if not callable(reply):
            raise ContractError("HOST_REPLY_UNBOUND")
        self.reply = reply

    def send(self, message: InboundMessage, text: str, idempotency_key: str) -> SendReceipt:
        safe_text = re.sub(r"<(/?)at(?=[\s>])", lambda m: "＜" + m.group(1) + "at",
                           text, flags=re.IGNORECASE)
        try:
            mid = self.reply(message.message_id, safe_text)
        except Exception:
            return SendReceipt("unknown", detail="HOST_REPLY_EXCEPTION")
        if not isinstance(mid, str) or not re.fullmatch(r"om_[A-Za-z0-9_]{1,160}", mid):
            return SendReceipt("unknown", detail="HOST_REPLY_RECEIPT_UNVERIFIED")
        return SendReceipt("sent", message_id=mid)


class R10BusinessFence:
    """Read-only checks against the pinned R10 bridge, with no model/network call.

    An active AM focus is kept on AM conservatively.  This may retain an old
    query conversation on the legacy path until it expires or is cleared; it
    never means that conversation met the Grok latency target.
    """
    def __init__(self, bridge: Any, *, explicit_text: Callable[[str], bool] | None = None):
        self.bridge = bridge
        self.explicit_text = explicit_text or _explicit_am_text

    def __call__(self, message: HostEnvelope) -> bool:
        bridge, row = self.bridge, message.legacy_row()
        if self.explicit_text(message.text):
            return True
        with bridge.mutex:
            if bridge.db.execute("SELECT 1 FROM inbox WHERE id=?", (message.message_id,)).fetchone():
                return True
        poc = getattr(bridge, "poc", None)
        if poc is not None:
            if poc.store.rows("SELECT 1 FROM inbox WHERE app=? AND tenant=? AND message_id=?",
                              (message.app_id, message.tenant_id, message.message_id)):
                return True
            if poc._mode(row) == "poc":
                return True
            # Reminder selection/follow-up is owned by the existing reminder
            # service. wants() is a local classifier/state read in this baseline.
            if getattr(poc.config, "reminders_enabled", False) and message.chat_type == "p2p":
                if poc.gateway.reminders.wants(row, message.text):
                    return True
        am = getattr(bridge, "am_self", None)
        if am is not None:
            if am.s.rows("SELECT 1 FROM am_inbox WHERE id=?", (message.message_id,)):
                return True
            if message.chat_type == "p2p" and message.sender_id == am.owner:
                if am.ux_collection_at_receipt(row) or am.ux_collection_in_chat(row):
                    return True
            if am.s.rows(
                "SELECT 1 FROM am_sessions WHERE scope=? AND expires>? "
                "AND (pending IS NOT NULL OR focus IS NOT NULL)",
                (am.scope(row), am.s.clock()),
            ):
                return True
            if message.parent_id:
                if am.s.rows(
                    "SELECT 1 FROM am_followup_text_actions t JOIN am_cards c ON c.nonce=t.nonce "
                    "WHERE c.message_id=? AND t.actor=? AND t.chat=?",
                    (message.parent_id, message.sender_id, message.chat_id),
                ):
                    return True
                if am.s.rows(
                    "SELECT 1 FROM reminder_occurrences o JOIN followup_adoptions a "
                    "ON a.occurrence_id=o.id WHERE o.message_id=?", (message.parent_id,),
                ):
                    return True
                # Explicit replies to another existing AM card remain on AM,
                # where that flow performs its original owner/scope checks.
                if am.s.rows("SELECT 1 FROM am_cards WHERE message_id=? AND chat=?",
                             (message.parent_id, message.chat_id)):
                    return True
        group = getattr(bridge, "group_ledger", None)
        if group is not None and message.chat_type == "group":
            with group.mutex:
                if group.db.execute("SELECT 1 FROM ledger_inbox WHERE mid=?",
                                    (message.message_id,)).fetchone():
                    return True
                if group.db.execute(
                    "SELECT 1 FROM query_conversations WHERE scope=? AND expires>? "
                    "AND (pending IS NOT NULL OR focus IS NOT NULL)",
                    (group.scope(row), group.clock()),
                ).fetchone():
                    return True
                if message.parent_id and group.db.execute(
                    "SELECT 1 FROM ledger_inbox WHERE chat=? AND user=? AND (mid=? OR answer_mid=?)",
                    (message.chat_id, message.sender_id, message.parent_id, message.parent_id),
                ).fetchone():
                    return True
        return False


def _explicit_am_text(text: str) -> bool:
    value = text.strip()
    if value.startswith("/"):
        return True
    # The existing harness owns stop/cancel/revise speech and its independent
    # control pool. It must remain reachable even without a pending AM session.
    from am_harness.controls import parse_control
    if parse_control(value) is not None:
        return True
    if value in {"我的跟进", "我的跟进？", "我的跟进?", "我的跟进。", "项目", "项目查询", "项目列表", "查看项目"}:
        return True
    if value.startswith(("今天我需要跟进什么", "我的待办", "查看变化", "项目变化", "变化简报",
                         "订阅简报", "取消订阅", "新增跟进", "查看跟进", "更新需求", "更新项目")):
        return True
    if value.startswith("哪些项目") and re.search("待确认|未明确|缺少|资料", value):
        return True
    # Unlike am_self.wants(), these functions do not contain a catch-all chat
    # classifier. Import the installed R10 definitions rather than rewriting its
    # business-command grammar. Natural send/cancel speech also stays on AM.
    from am_followup_text import wants_text
    return bool(wants_text(value))


class HostRuntime:
    def __init__(
        self, gateway: Gateway, bindings: HostBindings,
        *, app_id: str, tenant_id: str, bot_open_id: str,
        normalize: Callable[..., Any], business_fence: Callable[[HostEnvelope], bool],
        callbacks: CallbackServer | None = None, clock=time.time,
    ):
        bindings.validate()
        self.gateway, self.bindings = gateway, bindings
        self.app_id, self.tenant_id, self.bot_open_id = app_id, tenant_id, bot_open_id
        if not callable(normalize) or not callable(business_fence):
            raise ContractError("HOST_INGRESS_BINDING_UNBOUND")
        self.normalize, self.business_fence = normalize, business_fence
        self.callbacks, self.clock = callbacks, clock
        self.last_route_reason = "not_started"
        self._metrics_lock = threading.Lock()
        self._routing_counts: Counter = Counter()
        self._legacy_reasons: Counter = Counter()

    def _skip(self, reason: str) -> Admission:
        self.last_route_reason = reason
        return Admission(False, reason=reason)

    def ingest(self, payload: dict, *, origin: str = "realtime") -> Admission:
        """Return handled=True only when the old AM path must stop.

        Do not catch persistence errors and fall back to the old path: an SDK
        retry must find the receipt if its commit succeeded before an error.
        ``origin='reconcile'`` is for a trusted on-host polling adapter only.
        """
        with self._metrics_lock:
            self._routing_counts["ingress_calls"] += 1
        try:
            decision = self._ingest(payload, origin=origin)
        except Exception:
            with self._metrics_lock:
                self._routing_counts["errors"] += 1
            raise
        with self._metrics_lock:
            if not decision.handled:
                self._routing_counts["legacy_path"] += 1
                self._legacy_reasons[decision.reason or "unspecified"] += 1
            elif decision.status == "duplicate":
                self._routing_counts["duplicates"] += 1
            else:
                self._routing_counts["admitted"] += 1
        return decision

    def routing_metrics(self) -> dict:
        """No content/IDs; process-local event attempts, reset after restart.

        These counts diagnose routing coverage. They are not the gateway's
        durable unique-request denominator or an end-to-end latency metric.
        Events that fail before this hook are outside this counter's scope.
        """
        with self._metrics_lock:
            return {
                "schema": "am.grok.host-routing.v1", "persistent": False,
                "denominator": "event_deliveries_reaching_host_hook_since_start",
                **{key: self._routing_counts[key] for key in (
                    "ingress_calls", "admitted", "duplicates", "legacy_path", "errors",
                )},
                "legacy_reasons": dict(self._legacy_reasons),
            }

    def _ingest(self, payload: dict, *, origin: str) -> Admission:
        if not self.gateway.config.enabled:
            return self._skip("disabled")
        try:
            h, event = payload["header"], payload["event"]
            raw, sender = event["message"], event["sender"]
            if (h["event_type"] != "im.message.receive_v1" or h["app_id"] != self.app_id
                    or h["tenant_key"] != self.tenant_id or sender["sender_type"] != "user"
                    or sender.get("tenant_key", self.tenant_id) != self.tenant_id):
                return self._skip("host_identity_mismatch")
            user, mid, chat = sender["sender_id"]["open_id"], raw["message_id"], raw["chat_id"]
            if user == self.bot_open_id:
                return self._skip("bot_echo")
            for value, prefix in ((user, "ou"), (mid, "om"), (chat, "oc")):
                if not isinstance(value, str) or not re.fullmatch(prefix + r"_[A-Za-z0-9_]{1,160}", value):
                    return self._skip("invalid_host_identity")
            chat_type = raw["chat_type"]
            if chat_type not in {"group", "p2p"}:
                return self._skip("unsupported_chat_type")
        except (KeyError, TypeError):
            return self._skip("invalid_host_event")
        # Receipt ownership is immutable even when a duplicate changes content,
        # a business flow becomes active, or the identity cache later expires.
        prior = _receipt(self.gateway.store, self.tenant_id, self.app_id, mid)
        if prior:
            self.last_route_reason = "gateway_receipt_exists"
            return Admission(True, prior["request_id"], "duplicate", self.last_route_reason)
        cfg = self.gateway.config
        if cfg.allowed_sender_ids and user not in cfg.allowed_sender_ids:
            return self._skip("sender_outside_scope")
        if (chat_type == "group" and chat not in cfg.allowed_chat_ids) or (chat_type == "p2p" and not cfg.allow_private):
            return self._skip("chat_outside_scope")
        if raw.get("message_type") not in {"text", "post"}:
            return self._skip("existing_non_text_flow")
        try:
            normalized = self.normalize(raw, bot_open_id=self.bot_open_id, max_chars=6000)
            text, mentioned = normalized.text.strip(), normalized.bot_mentioned
            if not text or (chat_type == "group" and not mentioned):
                return self._skip("not_bot_question")
            created = int(raw["create_time"]) / 1000
            now = self.clock()
            if not now - cfg.max_message_age_seconds < created <= now + 5:
                return self._skip("event_outside_time_window")
            envelope = HostEnvelope(
                self.tenant_id, self.app_id, h["event_id"], mid, chat, chat_type,
                user, text, created, now, bool(mentioned),
                raw.get("thread_id") or "", raw.get("parent_id") or "", raw.get("root_id") or "",
            )
            if self.business_fence(envelope) or self.bindings.business_active(envelope):
                return self._skip("existing_am_workflow")
            snapshot = self.bindings.snapshot(envelope)
            if not isinstance(snapshot, HostSnapshot):
                return self._skip("host_snapshot_unavailable_legacy_path")
            if not snapshot.auth.verified or snapshot.auth.expires_at <= now:
                return self._skip("host_snapshot_expired_legacy_path")
            # Freeze trusted context before durable admission. No HTTP or Bot
            # data supplies the identity, permissions, or reply target.
            context = json.loads(canonical_json(snapshot.context))
            message = InboundMessage(
                tenant_id=envelope.tenant_id, app_id=envelope.app_id,
                event_id=envelope.event_id, message_id=envelope.message_id,
                chat_id=envelope.chat_id, chat_type=envelope.chat_type,
                sender_id=envelope.sender_id, text=envelope.text,
                event_created_at=envelope.event_created_at, received_at=envelope.received_at,
                auth=snapshot.auth, origin=origin, mentions_bot=envelope.mentions_bot,
                context=context, measurement_class=snapshot.measurement_class,
            )
            message.validate()
        except Exception:
            # A failed local eligibility check is explicitly a legacy path;
            # no Grok latency/success claim is made for it.
            return self._skip("host_preflight_unavailable_legacy_path")
        decision = self.gateway.admit(message)
        self.last_route_reason = decision.reason or decision.status
        return decision

    def recall(self, payload: dict) -> dict:
        """Only the existing verified SDK recalled-event handler calls this."""
        try:
            h, event = payload["header"], payload["event"]
            if (h["event_type"] != "im.message.recalled_v1" or h["app_id"] != self.app_id
                    or h["tenant_key"] != self.tenant_id):
                return {"handled": False, "reason": "recall_scope_mismatch"}
            mid = event["message_id"]
            if not isinstance(mid, str) or not re.fullmatch(r"om_[A-Za-z0-9_]{1,160}", mid):
                return {"handled": False, "reason": "invalid_recall"}
            prior = _receipt(self.gateway.store, self.tenant_id, self.app_id, mid)
            if not prior:
                return {"handled": False, "reason": "no_gateway_receipt"}
            original = json.loads(prior["message_json"])
            if event.get("chat_id") and event["chat_id"] != original["chat_id"]:
                return {"handled": False, "reason": "recall_chat_mismatch"}
            result = self.gateway.cancel(prior["conversation_id"], prior["sender_id"], prior["request_id"])
            return {"handled": True, **result}
        except (KeyError, TypeError, ValueError):
            return {"handled": False, "reason": "invalid_recall"}

    def start(self) -> None:
        if self.callbacks is not None:
            self.callbacks.start()
        try:
            self.gateway.start()
        except Exception:
            if self.callbacks is not None:
                self.callbacks.stop()
            raise

    def stop(self) -> dict:
        result = self.gateway.stop()
        if self.callbacks is not None:
            self.callbacks.stop()
        return result


def _callback_url(value: Any, *, allow_loopback_http: bool) -> str:
    if not isinstance(value, str) or not value or len(value) > 8192 or any(c.isspace() or ord(c) < 32 for c in value):
        raise ContractError("GROK_CALLBACK_URL_UNCONFIGURED")
    try:
        parsed = urlsplit(value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ContractError("INVALID_CALLBACK_URL") from None
    if (not host or parsed.username is not None or parsed.password is not None or parsed.fragment
            or parsed.query or parsed.path != "/v1/results" or port == 0):
        raise ContractError("INVALID_CALLBACK_URL")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and allow_loopback_http and is_loopback_host(host)
    ):
        raise ContractError("CALLBACK_REQUIRES_CONFIGURED_HTTPS_ROUTE")
    return value


def create_host_runtime(
    *, bridge: Any, config: Mapping[str, Any], reply: Callable[[str, str], str],
    bindings: HostBindings | None, normalize: Callable[..., Any],
    env: Mapping[str, str] | None = None, clock=time.time,
) -> HostRuntime | None:
    """Construct without starting listeners/workers; absent config is disabled.

    The externally reachable callback URL is supplied, never inferred from the
    callback listener address. A reverse proxy/tunnel must be configured and
    independently checked on the real host. No credential file is read here.
    """
    settings = config.get("grok_realtime", {})
    if not isinstance(settings, Mapping):
        raise ContractError("INVALID_GROK_SETTINGS")
    if type(settings.get("enabled", False)) is not bool:
        raise ContractError("INVALID_GROK_ENABLE_FLAG")
    if not settings.get("enabled", False):
        return None
    if not isinstance(bindings, HostBindings):
        raise ContractError("HOST_PROFILE_UNBOUND")
    bindings.validate()
    poc = getattr(bridge, "poc", None)
    if poc is None or not getattr(poc.config, "tenant_key", "") or not getattr(poc.config, "bot_open_id", ""):
        raise ContractError("VERIFIED_HOST_BOT_IDENTITY_UNBOUND")
    if poc.config.app_id != config.get("app_id"):
        raise ContractError("HOST_APP_IDENTITY_MISMATCH")
    environment = os.environ if env is None else env
    dispatch_key = secret_bytes(environment.get("AM_GROK_DISPATCH_KEY", ""))
    callback_key = secret_bytes(environment.get("AM_GROK_CALLBACK_KEY", ""))
    if hmac.compare_digest(dispatch_key, callback_key):
        raise ContractError("GROK_DIRECTION_KEYS_MUST_DIFFER")
    loopback = settings.get("allow_loopback_http", False)
    if type(loopback) is not bool:
        raise ContractError("INVALID_LOOPBACK_SETTING")
    callback_url = _callback_url(settings.get("callback_url", ""), allow_loopback_http=loopback)
    for name in ("allowed_chat_ids", "allowed_sender_ids"):
        values = settings.get(name, ())
        if not isinstance(values, (list, tuple)) or any(not isinstance(value, str) or not value for value in values):
            raise ContractError("INVALID_GROK_ALLOWLIST")
    cfg = GatewayConfig(
        enabled=True, allow_private=settings.get("allow_private", False),
        allowed_chat_ids=tuple(settings.get("allowed_chat_ids", ())),
        allowed_sender_ids=tuple(settings.get("allowed_sender_ids", ())),
        max_pending=settings.get("max_pending", 128),
        max_inflight=settings.get("max_inflight", 4),
        max_reconcile_inflight=settings.get("max_reconcile_inflight", 1),
        reply_workers=settings.get("reply_workers", 2),
        request_ttl_seconds=settings.get("request_ttl_seconds", 30.0),
        max_message_age_seconds=settings.get("max_message_age_seconds", 600.0),
        tick_seconds=settings.get("tick_seconds", 0.025),
        callback_url=callback_url, backend_profile="grok-bot-interactive",
    )
    cfg.validate()
    transport = WebhookTransport(
        settings.get("webhook_url", ""), dispatch_key,
        timeout=settings.get("transport_timeout_seconds", 5.0),
        max_inflight=cfg.max_inflight, allow_loopback_http=loopback,
    )
    store = Store(Path(config["state_directory"]) / "grok-realtime.sqlite3", clock=clock)
    guard = ScopeGuard(bindings.check_result, current_authorize=bindings.current_authorize, clock=clock)
    gateway = Gateway(cfg, store, transport, OriginalMessageSender(reply), guard,
                      bindings.handoff, clock=clock)
    callbacks = CallbackServer(
        on_result=gateway.accept_result, on_started=gateway.mark_started,
        replay_guard=store.claim_nonce, callback_key=callback_key,
        host=settings.get("callback_bind", "127.0.0.1"),
        port=settings.get("callback_port", 8789), clock=clock,
    )
    return HostRuntime(
        gateway, bindings, app_id=config["app_id"], tenant_id=poc.config.tenant_key,
        bot_open_id=poc.config.bot_open_id, normalize=normalize,
        business_fence=R10BusinessFence(bridge), callbacks=callbacks, clock=clock,
    )


def install(bridge: Any, config: Mapping[str, Any], reply: Callable[[str, str], str]) -> HostRuntime | None:
    """Called once by the original bridge; imports an explicitly trusted profile.

    Example host_profile: ``my_am_grok_profile:build_bindings``. That module is
    host deployment code, not a value accepted from Feishu or from the Bot. Its
    callable receives bridge/config and must return HostBindings. This package
    deliberately supplies no fabricated allow-all production profile.
    """
    settings = config.get("grok_realtime", {})
    if not isinstance(settings, Mapping) or type(settings.get("enabled", False)) is not bool:
        raise ContractError("INVALID_GROK_SETTINGS")
    if not settings.get("enabled", False):
        return None
    profile = settings.get("host_profile", "")
    if not isinstance(profile, str) or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", profile):
        raise ContractError("HOST_PROFILE_UNBOUND")
    module, function = profile.split(":")
    builder = getattr(importlib.import_module(module), function)
    bindings = builder(bridge=bridge, config=config)
    from fs_agent.message_text import normalize_message
    runtime = create_host_runtime(bridge=bridge, config=config, reply=reply, bindings=bindings,
                                  normalize=normalize_message)
    if runtime is not None:
        runtime.start()
    return runtime
