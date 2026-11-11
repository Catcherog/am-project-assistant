"""Apply sourced routine progress/todo proposals through the existing executor.

The standing R5 permission is recorded as policy authority, never as a human
approval.  The h4 draft is claimed in the same transaction as current Harness
authorization.  The a5 action and external host call follow after commit.  A
crash between either ledger cannot replay the claimed h4 operation.

Field mapping does not grant a new target, command or CLI capability.  The host
executor must already bind the target and support the exact operation; a Meegle
executor with frozen before/after grants still requires a matching real grant.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
import unicodedata

from am_gateway.assistant.policy import ActionContext, Actor, SourceEvidence
from am_gateway.models import ContractError, Event, dumps, key


POLICY_AUTHORITY = "policy:R5-standing-routine"
_SEMANTICS = {"progress", "todo"}
_FORMAL_WORDS = frozenset({"status", "state", "stage", "date", "deadline", "due", "milestone",
                          "owner", "assignee", "approval", "approve", "acceptance", "accepted",
                          "priority", "budget", "schedule", "responsibility", "start", "end",
                          "close", "closed", "closure", "complete", "completed", "completion",
                          "finish", "finished", "review", "reviewer", "approved", "rejected"})
_FORMAL_CHINESE = re.compile(r"状态|阶段|日期|截止|里程碑|负责人|责任人|审批|批准|验收|结项|优先级|预算|资源承诺")
_SENSITIVE_TEXT = (
    (re.compile(r"审批|批准|\bapprov(?:e|al|ed)\b", re.I), "approval"),
    (re.compile(r"验收|结项|\bacceptance\b|close[ _-]?project", re.I), "acceptance"),
    (re.compile(r"里程碑|milestone", re.I), "milestone_change"),
    (re.compile(r"(?:变更|调整|更换|改派).{0,8}(?:负责人|责任人)|change[ _-]?owner", re.I), "responsibility_change"),
    (re.compile(r"(?:承诺|保证|正式确定).{0,12}(?:完成|交付|截止|日期)|formal[ _-]?commitment", re.I), "formal_commitment"),
    (re.compile(r"(?:变更|修改|推迟|提前|顺延).{0,8}(?:正式日期|交付日期|计划日期|截止日期)"), "milestone_change"),
    (re.compile(r"领导|董事长|总经理"), "restricted_information"),
)


class _Hold(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


def _formal_field(name):
    value = re.sub(r"([a-z])([A-Z])", r"\1_\2", unicodedata.normalize("NFKC", name)).casefold()
    return bool(set(re.findall(r"[a-z]+", value)) & _FORMAL_WORDS or _FORMAL_CHINESE.search(value))


def current_r5_intake_hash(db, source_event):
    """Bind current ingress before the background Harness has caught up.

    R4-only sources have no R5 row and retain their existing authority checks.
    Once captured by R5, only the same admitted real event is usable.  A pending
    edit, local answer, recall, or a newer reliable revision invalidates it even
    while gw_items / h1_tasks still contain the earlier admitted request.
    """
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='r5_intake'").fetchone():
        return None
    row = db.execute("SELECT state,event_json,source_hash FROM r5_intake WHERE item_id=?",
                     (source_event.item_id,)).fetchone()
    if row is None:
        return None
    try:
        intake = Event.from_dict(json.loads(row["event_json"]))
    except (KeyError, ValueError, TypeError, ContractError):
        raise ContractError("r5_intake_source_changed") from None
    identity = ("tenant_id", "native_id", "kind", "chat_id", "sender_id", "direction")
    changed = (row["state"] != "admitted" or intake.is_deleted or intake.is_self
               or any(getattr(intake, name) != getattr(source_event, name) for name in identity)
               or intake.body != source_event.body or intake.content_hash != source_event.content_hash)
    if (intake.revision or source_event.revision) and intake.revision != source_event.revision:
        changed = True
    if (intake.version_time or source_event.version_time) and intake.version_time != source_event.version_time:
        changed = True
    if intake.original_time != source_event.original_time:
        changed = True
    if changed or not isinstance(row["source_hash"], str) or not row["source_hash"]:
        raise ContractError("r5_intake_source_changed")
    return row["source_hash"]


class RoutineMaintenance:
    def __init__(self, harness, proposals, directory, policy, actions, executor, routine_fields):
        self.harness, self.store = harness, harness.store
        self.proposals, self.directory, self.policy = proposals, directory, policy
        self.actions, self.executor = actions, executor
        if (Path(proposals.store.path).resolve() != Path(self.store.path).resolve()
                or Path(actions.store.path).resolve() != Path(self.store.path).resolve()
                or proposals.owner_id != harness.config.owner_id or actions.owner_id != harness.config.owner_id
                or actions.policy is not policy):
            raise ContractError("routine maintenance requires the same owner, store and assistant policy")
        self.routine_fields = json.loads(dumps(routine_fields)) if isinstance(routine_fields, dict) else {}
        self.mapping_error = self._mapping_error(routine_fields)
        self._cursor = 0
        with self.store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS r5_routine_maintenance(
                proposal_id TEXT PRIMARY KEY,version INTEGER NOT NULL,digest TEXT NOT NULL,
                action_id TEXT NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL,
                source_json TEXT NOT NULL,effect_id TEXT NOT NULL,receipt_json TEXT NOT NULL,
                created REAL NOT NULL,updated REAL NOT NULL)""")

    @staticmethod
    def _mapping_error(mapping):
        if not isinstance(mapping, dict) or not mapping:
            return "routine_fields_unconfigured"
        for project, fields in mapping.items():
            if not isinstance(project, str) or not project or not isinstance(fields, dict) or not fields:
                return "routine_field_mapping_invalid"
            for field, semantic in fields.items():
                if not isinstance(field, str) or not field or semantic not in _SEMANTICS:
                    return "routine_field_mapping_invalid"
                if _formal_field(field):
                    return "formal_field_cannot_be_routine"
        return ""

    def _binding_reason(self, payload=None):
        if self.mapping_error:
            return self.mapping_error
        if self.executor is None or not callable(self.executor):
            return "business_executor_unbound"
        if getattr(self.executor,"routine_live",True) is not True:
            return "routine_send_live_disabled"
        if hasattr(self.executor, "routine_targets") and not self.executor.routine_targets:
            return "routine_grants_unconfigured"
        targets = getattr(self.executor, "targets", None)
        if not isinstance(targets, dict) or "update_fields" not in getattr(self.executor, "actions", ()):
            return "executor_target_or_action_binding_missing"
        if payload is not None:
            target = payload.get("target", {})
            if target.get("id") not in targets.get(payload.get("project_id"), {}).get(target.get("type"), ()):
                return "executor_target_not_bound"
            grants = getattr(self.executor, "routine_targets", None)
            if grants is not None:
                bound = grants.get(payload.get("project_id"), {}).get(target.get("id"), {})
                configured = self.routine_fields.get(payload.get("project_id"), {})
                if any(field not in bound or bound[field] != configured.get(field) for field in payload.get("changes", {})):
                    return "routine_grant_target_or_fields_unbound"
        return ""

    def _record(self, db, proposal, *, status, reason, source=None, action_id="", effect_id="", receipt=None):
        source, receipt = source or {}, receipt or {}
        encoded, receipt_json = dumps(source), dumps(receipt)
        old = db.execute("SELECT * FROM r5_routine_maintenance WHERE proposal_id=?", (proposal["proposal_id"],)).fetchone()
        now = self.store.clock()
        values = (proposal["version"], proposal["digest"], action_id, status, reason, encoded, effect_id, receipt_json)
        if old and tuple(old[name] for name in ("version", "digest", "action_id", "status", "reason", "source_json", "effect_id", "receipt_json")) == values:
            return
        db.execute("""INSERT INTO r5_routine_maintenance VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(proposal_id) DO UPDATE SET version=excluded.version,digest=excluded.digest,
            action_id=excluded.action_id,status=excluded.status,reason=excluded.reason,
            source_json=excluded.source_json,effect_id=excluded.effect_id,receipt_json=excluded.receipt_json,
            updated=excluded.updated""", (proposal["proposal_id"], *values, now, now))
        self.store.audit(db, "assistant_routine." + status, proposal["proposal_id"],
            dumps({"reason": reason, "version": proposal["version"], "digest": proposal["digest"],
                   "action_id": action_id, "source": source, "authority": POLICY_AUTHORITY,
                   "effect_id": effect_id}))

    def _validated(self, db, proposal, actor):
        if proposal["kind"] != "project.execute":
            raise _Hold("awaiting_approval", "non_project_operation")
        payload = proposal["payload"]
        if proposal["digest"] != key(proposal["kind"], payload):
            raise _Hold("awaiting_review", "proposal_frozen_digest_mismatch")
        if (not isinstance(payload, dict) or payload.get("action") != "update_fields"
                or not isinstance(payload.get("target"), dict) or set(payload["target"]) != {"type", "id"}
                or not all(isinstance(value, str) and value for value in payload["target"].values())
                or not isinstance(payload.get("project_id"), str)
                or not isinstance(payload.get("changes"), dict) or not payload["changes"]):
            raise _Hold("awaiting_approval", "non_routine_proposal_contract")
        reason = self._binding_reason(payload)
        if reason:
            raise _Hold("awaiting_binding", reason)
        project, changes = payload["project_id"], payload["changes"]
        fields = self.routine_fields.get(project)
        if not fields:
            raise _Hold("awaiting_binding", "project_routine_fields_unconfigured")
        if any(field not in fields or _formal_field(field) for field in changes):
            raise _Hold("awaiting_approval", "changes_include_non_routine_field")
        if any(not isinstance(value, str) or not value.strip() for value in changes.values()):
            raise _Hold("awaiting_approval", "routine_text_required_no_blanket_clear")
        try:
            task, authorization = self.harness._authorize(db, proposal["task_id"], proposal["generation"],
                                                         require_running=False)
        except ContractError:
            raise _Hold("awaiting_review", "current_source_or_authority_unusable") from None
        if (proposal["business_id"], proposal["generation"], proposal["source_version"]) != (
                task["business_id"], task["generation"], task["source_version"]):
            raise _Hold("awaiting_review", "proposal_source_or_execution_changed")
        if project not in authorization.projects:
            raise _Hold("awaiting_approval", "project_outside_current_authority")
        source = db.execute("SELECT * FROM gw_items WHERE item_id=?", (task["item_id"],)).fetchone()
        version = db.execute("SELECT * FROM gw_item_versions WHERE item_id=? AND version=?",
                             (task["item_id"], task["source_version"])).fetchone()
        if (not source or not version or source["is_deleted"] or source["is_self"]
                or source["version"] != task["source_version"] or source["content_hash"] != version["content_hash"]):
            raise _Hold("awaiting_review", "verified_source_version_unavailable")
        try:
            event = Event.from_dict(json.loads(version["event_json"]))
            requested = Event.from_dict(json.loads(task["request_json"]))
        except (ValueError, TypeError, KeyError, ContractError):
            raise _Hold("awaiting_review", "source_record_invalid") from None
        if (event.item_id != task["item_id"] or event.is_deleted or event.is_self or event.direction != "inbound"
                or event.kind != "message" or event.sender_id == self.harness.config.bot_id
                or event.tenant_id != self.harness.config.tenant_id or requested.item_id != event.item_id
                or requested.sender_id != event.sender_id or authorization.principal != event.sender_id
                or not isinstance(actor, Actor) or not actor.verified or actor.user_id != event.sender_id):
            raise _Hold("awaiting_review", "source_identity_not_verified")
        try:
            intake_hash = current_r5_intake_hash(db, event)
        except ContractError:
            raise _Hold("awaiting_review", "r5_intake_source_changed") from None
        action_id = "routine_" + key(proposal["proposal_id"], proposal["version"], proposal["digest"])[:40]
        source_id = "gw_item:" + event.item_id + ":v" + str(task["source_version"])
        stamp = {"proposal_id": proposal["proposal_id"], "proposal_version": proposal["version"],
                 "proposal_digest": proposal["digest"], "task_id": task["task_id"],
                 "business_id": task["business_id"], "generation": task["generation"],
                 "source_version": task["source_version"], "source_item_id": event.item_id,
                 "source_content_hash": source["content_hash"], "source_ref": source_id,
                 "policy_hash": key(task["policy_json"]), "project_id": project,
                 "target": payload["target"], "field_semantics": {field: fields[field] for field in changes}}
        if intake_hash is not None:
            stamp["r5_intake_hash"] = intake_hash
        text = "\n".join((actor.display_name, event.body, *changes.values()))
        sensitivity = {tag for pattern, tag in _SENSITIVE_TEXT if pattern.search(text)}
        mentioned = tuple(sorted(set(event.mentions) | {event.sender_id}))
        if set(mentioned) & self.policy.config.leader_user_ids:
            sensitivity.add("restricted_information")
        semantics = sorted({fields[field] for field in changes})
        contexts = []
        for semantic in semantics:
            context = ActionContext(action_id, "update_" + semantic, actor, project_scope="demo",
                source_evidence=(SourceEvidence(source_id, kind="message", verified=True,
                                                project_id=project, subject_id=payload["target"]["id"]),),
                sensitivity=frozenset(sensitivity), mentioned_person_ids=mentioned,
                content_text=text, external_visibility=True, project_id=project,
                subject_id=payload["target"]["id"])
            decision = self.policy.evaluate(context)
            if not decision.allowed:
                raise _Hold("awaiting_approval" if decision.requires_approval else "awaiting_review",
                            "policy:" + ",".join(decision.reason_codes))
            contexts.append(context)
        return contexts[0], stamp

    def _preflight_actor(self, proposal):
        task = self.store.one("SELECT item_id FROM h1_tasks WHERE task_id=?", (proposal["task_id"],))
        source = self.store.one("SELECT sender_id FROM gw_items WHERE item_id=?", (task["item_id"],)) if task else None
        if not source:
            raise _Hold("awaiting_review", "source_record_missing")
        try:
            return self.directory.resolve(source["sender_id"])
        except Exception:
            raise _Hold("awaiting_identity", "trusted_directory_unavailable") from None

    def _effect(self, proposal, effect_id):
        """Adapt the host's separate apply/readback evidence to a5's effect shape."""
        raw = self.executor(json.loads(dumps(proposal)), idempotency_key=effect_id)
        if not isinstance(raw, dict):
            return {"status": "unknown", "reason": "invalid_host_receipt"}
        receipt = raw.get("receipt")
        if not isinstance(receipt, dict) or receipt.get("idempotency_key") != effect_id:
            return {"status": "unknown", "reason": "host_write_receipt_identity_missing"}
        if receipt.get("status") not in {"submitted", "applied", "not_applied", "unknown"}:
            return {"status": "unknown", "reason": "invalid_host_write_status"}
        write = {"status": receipt["status"], "idempotency_key": effect_id}
        write_ref = "host-write-receipt:sha256:" + key(write)
        if raw.get("status") == "not_applied" and receipt.get("status") == "not_applied":
            return {"status": "not_applied", "evidence_ref": write_ref, "write_receipt": write}
        if raw.get("status") != "applied":
            return {"status": "unknown", "evidence_ref": write_ref, "write_receipt": write,
                    "reason": "host_result_unconfirmed"}
        observed, payload = raw.get("readback"), proposal["payload"]
        valid = (isinstance(observed, dict) and observed.get("status") == "applied"
                 and observed.get("idempotency_key") == effect_id
                 and observed.get("project_id") == payload["project_id"]
                 and observed.get("target") == payload["target"]
                 and observed.get("changes") == payload["changes"]
                 and isinstance(observed.get("evidence_ref"), str) and bool(observed["evidence_ref"].strip()))
        if not valid:
            return {"status": "applied", "evidence_ref": write_ref, "write_receipt": write,
                    "reason": "independent_readback_missing_or_mismatched"}
        return {"status": "applied", "evidence_ref": write_ref,
                "readback_evidence_ref": observed["evidence_ref"], "write_receipt": write,
                "readback": {"idempotency_key": effect_id, "project_id": payload["project_id"],
                             "target": payload["target"], "changes_digest": key(payload["changes"]),
                             "evidence_ref": observed["evidence_ref"]}}

    def _one(self, selected):
        try:
            actor = self._preflight_actor(selected)
        except _Hold as held:
            with self.store.transaction() as db:
                self._record(db, selected, status=held.status, reason=held.reason)
            return {"proposal_id": selected["proposal_id"], "status": held.status, "reason": held.reason}
        with self.store.transaction() as db:
            proposal = self.proposals._decode(db.execute("SELECT * FROM h4_agent_proposals WHERE proposal_id=?",
                                                         (selected["proposal_id"],)).fetchone())
            if proposal["status"] != "draft":
                return {"proposal_id": proposal["proposal_id"], "status": proposal["status"], "reason": "already_claimed"}
            try:
                context, stamp = self._validated(db, proposal, actor)
            except _Hold as held:
                self._record(db, proposal, status=held.status, reason=held.reason)
                return {"proposal_id": proposal["proposal_id"], "status": held.status, "reason": held.reason}
            effect_id = "effect_" + key(proposal["proposal_id"], proposal["version"], proposal["digest"])[:40]
            db.execute("""UPDATE h4_agent_proposals SET status='executing',approved_by=?,updated=?
                WHERE proposal_id=? AND status='draft' AND version=? AND digest=?""",
                       (POLICY_AUTHORITY, self.store.clock(), proposal["proposal_id"], proposal["version"], proposal["digest"]))
            proposal["status"], proposal["approved_by"] = "executing", POLICY_AUTHORITY
            self._record(db, proposal, status="executing", reason="standing_routine_authority_claimed",
                         source=stamp, action_id=context.action_id, effect_id=effect_id)
        host_receipt = {}

        def execute(frozen):
            # Recheck any source/policy mutation committed after the first claim.
            # No network operation occurs while this transaction is held.
            with self.store.transaction() as db:
                current = self.proposals._decode(db.execute("SELECT * FROM h4_agent_proposals WHERE proposal_id=?",
                                                            (proposal["proposal_id"],)).fetchone())
                try:
                    _, current_stamp = self._validated(db, current, actor)
                    if (current["status"] != "executing" or current["approved_by"] != POLICY_AUTHORITY
                            or current_stamp != stamp or frozen["source_stamp"] != stamp
                            or frozen["payload"] != proposal["payload"]):
                        raise _Hold("not_applied", "claimed_source_or_payload_changed")
                except _Hold as held:
                    proof = "routine-preflight-rejection:sha256:" + key(effect_id, held.reason)
                    host_receipt.update(status="not_applied", evidence_ref=proof, reason=held.reason)
                    self.store.audit(db, "assistant_routine.preflight_rejected", proposal["proposal_id"],
                                     dumps({"reason": held.reason, "evidence_ref": proof, "effect_id": effect_id}))
                    return dict(host_receipt)
            host_receipt.update(self._effect(proposal, effect_id))
            return dict(host_receipt)

        try:
            action = self.actions.prepare(context, proposal["payload"], stamp)
            outcome = self.actions.execute(context.action_id, action["digest"], stamp, context, execute)
            status = outcome["state"] if outcome.get("executed") else "not_applied"
            if status not in {"applied", "applied_unverified", "not_applied", "unknown"}:
                status = "unknown"
            reason = (host_receipt.get("reason") or outcome.get("last_error")
                      or ("independent_readback_verified" if status == "applied" else "action_not_executed"))
        except Exception:
            status, reason = "unknown", "routine_execution_exception"
        safe_receipt = {"status": status, "reason": reason, "idempotency_key": effect_id,
                        "action_id": context.action_id, "authority": POLICY_AUTHORITY,
                        "source_ref": stamp["source_ref"], **host_receipt}
        safe_receipt["status"] = status
        with self.store.transaction() as db:
            current = db.execute("SELECT * FROM h4_agent_proposals WHERE proposal_id=?", (proposal["proposal_id"],)).fetchone()
            if (current["status"] == "executing" and current["approved_by"] == POLICY_AUTHORITY
                    and current["version"] == proposal["version"] and current["digest"] == proposal["digest"]):
                db.execute("UPDATE h4_agent_proposals SET status=?,receipt=?,updated=? WHERE proposal_id=?",
                           (status, dumps(safe_receipt), self.store.clock(), proposal["proposal_id"]))
                self._record(db, proposal, status=status, reason=reason, source=stamp,
                             action_id=context.action_id, effect_id=effect_id, receipt=safe_receipt)
            else:
                self.store.audit(db, "assistant_routine.late_result_retained", proposal["proposal_id"],
                                 dumps({"action_id": context.action_id, "effect_id": effect_id, "result_status": status}))
                status = current["status"]
        return {"proposal_id": proposal["proposal_id"], "action_id": context.action_id,
                "status": status, "reason": reason, "source_ref": stamp["source_ref"]}

    def step(self, limit=20):
        """Inspect drafts, applying at most one external operation per call."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ContractError("invalid routine scan limit")
        binding = self._binding_reason()
        if binding:
            return {"status": "awaiting_binding", "reason": binding, "results": []}
        drafts = self.proposals.list("draft")
        if not drafts:
            return {"status": "idle", "results": []}
        start = self._cursor % len(drafts)
        selected = (drafts[start:] + drafts[:start])[:limit]
        results = []
        for proposal in selected:
            result = self._one(proposal)
            results.append(result)
            self._cursor = (self._cursor + 1) % len(drafts)
            if result["status"] in {"applied", "applied_unverified", "not_applied", "unknown", "executing"}:
                break
        return {"status": results[-1]["status"] if results else "idle", "results": results}

    def status(self):
        reason = self._binding_reason()
        rows = self.store.rows("SELECT proposal_id,version,action_id,status,reason,source_json,effect_id,updated FROM r5_routine_maintenance ORDER BY updated DESC LIMIT 100")
        for row in rows:
            row["source"] = json.loads(row.pop("source_json"))
        return {"status": "awaiting_binding" if reason else "bound", "reason": reason,
                "authority": POLICY_AUTHORITY, "routine_fields": self.routine_fields,
                "records": rows, "unresolved": [row for row in rows if row["status"] in {"executing", "unknown", "applied_unverified"}]}
