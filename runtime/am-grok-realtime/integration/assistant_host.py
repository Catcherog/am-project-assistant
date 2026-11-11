"""Bind the R5 assistant to the existing Studio consumer and SDK transport."""
from __future__ import annotations

from dataclasses import asdict
import json
import re
import time

from am_gateway.assistant.coordinator import AssistantCoordinator
from am_gateway.assistant.policy import AssistantPolicy, PolicyConfig
from am_gateway.collectors import SourceSpec
from am_gateway.models import Event, dumps, key
from integration.assistant_identity import StudioAssistantDirectory, DEMOScope
from integration.assistant_queries import StudioProjectQueries
from integration.assistant_intelligence import bind_reply_intelligence
from integration.studio_host import BindingError, StudioSender


def _required(value, code):
    if not isinstance(value, str) or not value:
        raise BindingError(code)
    return value


def _card(title, body, *, approval=None, scope_decision=None, mention_ids=()):
    # These are the same card primitives and mention format used by
    # fs_agent.business_port.BusinessPort.prepare on the original host.
    from fs_agent.card_ui import bind_rendered_card, button, card, paragraph
    elements = []
    if mention_ids:
        if any(not re.fullmatch(r"ou_[A-Za-z0-9_-]+", user_id) for user_id in mention_ids):
            raise BindingError("assistant_verified_mention_ids_required")
        elements.append({"tag": "markdown", "content": " ".join(
            '<at id="' + user_id + '"></at>' for user_id in dict.fromkeys(mention_ids))})
    elements.append(paragraph(body))
    if approval is not None:
        elements.extend([
            button("同意本次执行", {**approval, "approve": True}),
            button("不同意", {**approval, "approve": False}),
        ])
    if scope_decision is not None:
        elements.extend([
            button("承接此项", {**scope_decision, "accept": True}),
            button("暂不承接", {**scope_decision, "accept": False}),
        ])
    return bind_rendered_card(card(title, elements))


class StudioAssistantDelivery:
    """Frozen assistant actions through StudioSender with exact SDK readback.

    No timestamp is invented here. Coordinator observation time is distinct
    from an SDK platform create_time and from a user-visible latency measure.
    """

    def __init__(self, sender, directory, store, cfg):
        self.sender, self.directory, self.store, self.cfg = sender, directory, store, cfg

    def _send_verified(self, action):
        timings = {'clock_domain': 'host_monotonic', 'phone_visibility_measured': False}
        began = time.monotonic()
        try:
            return self._send_verified_measured(action, timings)
        finally:
            timings['total_seconds'] = time.monotonic() - began
            try:
                with self.store.transaction() as db:
                    self.store.audit(db, 'assistant_delivery_timing', action['action_id'], dumps(timings))
            except Exception:
                pass  # Optional timing cannot turn verified delivery into a retry.

    def _send_verified_measured(self, action, timings):
        began = time.monotonic()
        receipt = self.sender.send(action)
        timings['send_call_seconds'] = time.monotonic() - began
        timings['send_status'] = receipt.status
        if receipt.status != "sent" or not receipt.platform_id:
            if receipt.status == "failed":
                with self.store.transaction() as db:
                    self.store.audit(db, "assistant_studio_send_rejected", action["action_id"],
                                     receipt.detail or "send_rejected")
                    audit_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                return {"status": "not_applied", "evidence_ref": "gateway:audit:" + str(audit_id),
                        "reason": receipt.detail or "send_rejected"}
            return {"status": "unknown", "reason": receipt.detail or "send_unconfirmed"}
        began = time.monotonic()
        verified = self.sender.lookup(action)
        timings['independent_readback_seconds'] = time.monotonic() - began
        timings['readback_status'] = verified.status
        evidence_ref = "feishu:message:" + receipt.platform_id
        if verified.status != "sent" or verified.platform_id != receipt.platform_id:
            return {"status": "unknown", "evidence_ref": evidence_ref,
                    "reason": verified.detail or "readback_unconfirmed"}
        return {"status": "applied", "platform_id": receipt.platform_id,
                "evidence_ref": evidence_ref,
                "readback_evidence_ref": "feishu:readback:" + receipt.platform_id}

    def report(self, report):
        target = {"chat_id": self.cfg.owner_chat_id, "actor_id": self.cfg.owner_id, "channel": "bot_dm"}
        payload = report.get("payload", {})
        if report.get("target") != target or payload.get("target") != target:
            raise BindingError("assistant_report_owner_target_mismatch")
        body = _required(report.get("body"), "assistant_report_body_required")
        report_id = _required(report.get("report_id"), "assistant_report_id_required")
        metadata = payload.get("metadata", {})
        if not isinstance(metadata, dict):
            raise BindingError("assistant_report_metadata_invalid")
        approval, scope_decision = None, None
        if report.get("kind") == "approval" or metadata.get("kind") == "approval":
            aid = _required(metadata.get("action_id"), "assistant_approval_action_required")
            digest, version = metadata.get("digest"), metadata.get("version")
            if (not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)
                    or type(version) is not int or version < 1):
                raise BindingError("assistant_exact_approval_binding_required")
            approval = {"kind": "assistant_action", "action_id": aid, "version": version, "digest": digest}
        elif metadata.get("kind") == "referral" and metadata.get("task_id"):
            task_id = _required(metadata.get("task_id"), "assistant_scope_task_required")
            scope_digest = metadata.get("scope_digest")
            if not isinstance(scope_digest, str) or not re.fullmatch(r"[a-f0-9]{64}", scope_digest):
                raise BindingError("assistant_exact_scope_binding_required")
            scope_decision = {"kind": "assistant_scope", "task_id": task_id, "scope_digest": scope_digest}
        titles = {"approval": "AM · 请确认本次操作", "conversation": "AM · 沟通汇报",
                  "digest": "AM · 工作简报", "risk": "AM · 需要关注"}
        title = payload.get("title") or titles.get(report.get("kind"), "AM · 项目助理")
        rendered = _card(title, body, approval=approval, scope_decision=scope_decision)
        attempt = report.get("attempt_no")
        current = self.store.one("SELECT attempt_no FROM ar5_outbox WHERE owner_id=? AND report_id=?",
                                 (self.cfg.owner_id, report_id))
        if type(attempt) is not int or attempt < 1 or not current or current["attempt_no"] != attempt:
            raise BindingError("assistant_report_attempt_not_claimed")
        # A new outbox attempt exists only after confirmed non-delivery. Keep
        # every previous transport receipt, including uncertain attempts.
        action = {"action_id": "assistant_report:" + report_id + ":" + str(attempt) + ":" + key(body, rendered, target),
                  "action_type": "approval_card", "body": body,
                  "payload_json": dumps({"card": rendered}), "target_json": dumps(target)}
        outcome = self._send_verified(action)
        if outcome["status"] != "applied":
            return {**outcome, "status": "unknown"}
        return {**outcome, "status": "sent", "delivery_ref": outcome["evidence_ref"],
                "evidence_ref": outcome["readback_evidence_ref"]}

    def __call__(self, frozen):
        payload, context = frozen.get("payload", {}), frozen.get("context", {})
        kind = context.get("action_kind")
        if kind not in {"reply", "proactive_followup", "send_message"}:
            raise BindingError("assistant_message_action_kind_not_bound")
        aid = _required(frozen.get("action_id"), "assistant_action_id_required")
        digest = frozen.get("digest")
        if (not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)
                or context.get("action_id") != aid):
            raise BindingError("assistant_frozen_action_binding_invalid")
        body, target = payload.get("body"), payload.get("target")
        if not isinstance(body, str) or body != context.get("content_text") or not isinstance(target, dict):
            raise BindingError("assistant_frozen_payload_mismatch")
        if (not isinstance(target.get("chat_id"), str) or not target["chat_id"].startswith("oc_")
                or target.get("channel") not in {"bot_dm", "group"}):
            raise BindingError("assistant_exact_message_target_required")
        if payload.get("artifacts") or payload.get("attachments"):
            raise BindingError("assistant_message_attachment_port_required")
        recipients = context.get("recipients", [])
        if not isinstance(recipients, list) or not recipients:
            raise BindingError("assistant_frozen_recipients_required")
        recipient_ids = []
        for expected in recipients:
            if not isinstance(expected, dict) or expected.get("verified") is not True:
                raise BindingError("assistant_frozen_recipient_unverified")
            actor = self.directory.resolve(expected.get("user_id"))
            actual = asdict(actor)
            fields = ("user_id", "verified", "is_owner", "is_leader")
            if (any(actual[field] != expected.get(field) for field in fields)
                    or set(actual["department_ids"]) != set(expected.get("department_ids", ()))):
                raise BindingError("assistant_recipient_identity_changed")
            self.sender.scope.authorize({**target, "actor_id": actor.user_id})
            recipient_ids.append(actor.user_id)
        if target.get("actor_id") not in recipient_ids:
            raise BindingError("assistant_target_recipient_mismatch")
        attempt = frozen.get("attempt_count")
        current = self.store.one("SELECT version,digest,attempt_count FROM a5_actions WHERE owner_id=? AND action_id=?",
                                 (self.cfg.owner_id, aid))
        if (type(attempt) is not int or attempt < 1 or not current
                or (current["version"], current["digest"], current["attempt_count"])
                != (frozen.get("version"), digest, attempt)):
            raise BindingError("assistant_action_attempt_not_claimed")
        action = {"action_id": "assistant_action:" + aid + ":" + digest + ":" + str(attempt),
                  "action_type": "text", "body": body,
                  "payload_json": "{}", "target_json": dumps(target)}
        if kind == "proactive_followup" and target["channel"] == "group":
            rendered = _card("AM · 事项跟进", body, mention_ids=recipient_ids)
            action["action_type"], action["payload_json"] = "approval_card", dumps({"card": rendered})
        elif kind == "reply":
            rendered = _card("AM", body)
            action["action_type"], action["payload_json"] = "approval_card", dumps({"card": rendered})
        return self._send_verified(action)


def bind_assistant(service, bundle, bridge, settings, client, root, executor=None):
    """Wire existing ports only; never start a receiver, thread or websocket."""
    authority, harness = bundle.authority, service.harness
    cfg, store = harness.config, harness.store
    assistant = settings.get("assistant", {})
    if not isinstance(assistant, dict):
        raise BindingError("assistant_settings_must_be_object")
    directory, scope = getattr(authority, "directory", None), getattr(authority, "scope", None)
    if not isinstance(directory, StudioAssistantDirectory):
        raise BindingError("assistant_verified_directory_required")
    if not isinstance(scope, DEMOScope) or scope.directory is not directory or scope.bridge is not bridge:
        raise BindingError("assistant_demo_scope_required")
    if any(getattr(directory.config, name) != getattr(cfg, name)
           for name in ("tenant_id", "bot_id", "owner_id", "owner_chat_id")):
        raise BindingError("assistant_directory_config_mismatch")
    readers = getattr(bundle.readers, "readers", {})
    bot_reader = readers.get("am_bot")
    collectors = getattr(getattr(service, "collector", None), "collectors", None)
    if (bot_reader is None or getattr(bot_reader, "identity", None) != "am_bot"
            or not hasattr(bot_reader, "chats") or collectors is None
            or not callable(getattr(collectors, "register_source", None))):
        raise BindingError("assistant_existing_collection_ports_required")
    aliases = assistant.get("protected_name_aliases", ())
    if isinstance(aliases, (str, bytes)) or not isinstance(aliases, (list, tuple, set, frozenset)):
        raise BindingError("assistant_protected_aliases_must_be_names")
    policy = AssistantPolicy(PolicyConfig(
        owner_user_ids={cfg.owner_id},
        demo_department_ids=assistant.get("demo_department_ids", ()),
        leader_user_ids=assistant.get("leader_user_ids", ()),
        protected_person_ids=assistant.get("protected_person_ids", ()),
        protected_name_aliases={"示例主管甲", "示例主管乙", *aliases},
    ))

    def on_dm_bound(event):
        if not isinstance(event, Event):
            raise BindingError("assistant_verified_dm_event_required")
        event.validate()
        if (event.tenant_id != cfg.tenant_id or event.channel != "bot_dm" or event.kind != "message"
                or event.direction != "inbound" or event.is_self or event.is_deleted
                or directory.dm_actor(event.chat_id) != event.sender_id):
            raise BindingError("assistant_registered_dm_required")
        actor = directory.resolve(event.sender_id)
        if not actor.verified or actor.user_id != event.sender_id:
            raise BindingError("assistant_verified_dm_actor_required")
        source = SourceSpec(cfg.tenant_id, event.chat_id, identity="am_bot", channel="bot_dm",
            project_ids=tuple(authority.projects_for_chat(event.chat_id)),
            poll_seconds=settings.get("poll_seconds", 60))
        collectors.register_source(source, discovered_by="assistant_verified_dm")
        bot_reader.chats = frozenset({*bot_reader.chats, event.chat_id})

    delivery = None
    if settings.get("send_live") is True:
        sender = StudioSender(client, scope, store.path, send_live=True)
        delivery = StudioAssistantDelivery(sender, directory, store, cfg)
    coordinator = AssistantCoordinator(harness, directory=directory, policy=policy, authority=authority,
        on_dm_bound=on_dm_bound, report_sender=delivery.report if delivery else None,
        action_executor=delivery,
        project_reader=bind_reply_intelligence(
            StudioProjectQueries(bridge, authority, assistant.get('fast_reads', {})),
            bridge, store, cfg.owner_id, assistant),
        settings={**assistant, "project_chats": settings.get("project_chats", {}),
                  "group_chat_ids": settings.get("group_chat_ids", ())})
    coordinator.maintenance = None
    try:
        from integration.assistant_maintenance import RoutineMaintenance
    except ModuleNotFoundError as exc:
        if exc.name != "integration.assistant_maintenance":
            raise
    else:
        proposals = getattr(service, "proposals", None)
        if proposals is None:
            raise BindingError("assistant_proposal_repository_required")
        coordinator.maintenance = RoutineMaintenance(harness, proposals, directory, policy,
            coordinator.actions, executor if settings.get("send_live") is True else None,
            assistant.get("routine_fields", {}))
    from integration.assistant_evolution import bind_evolution
    coordinator.evolution = bind_evolution(coordinator, bridge, assistant, root,
        interactive_runner=getattr(service, 'interactive_runner', None))
    service.assistant = coordinator
    service.ingress.assistant = coordinator
    collectors.assistant = coordinator
    return coordinator
