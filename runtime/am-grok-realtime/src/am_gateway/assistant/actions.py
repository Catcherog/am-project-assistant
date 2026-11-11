"""Exact-action approvals and durable execution, independent of any transport.

The host supplies authenticated Actors, resolved destinations and fresh source
versions. It must use the frozen payload when sending through another idempotent
transport. Message text and worker output never establish those bindings.
"""
from __future__ import annotations

from contextlib import closing
from dataclasses import asdict
import hashlib
import json
import uuid

from ..models import ContractError
from ..store import Store
from .policy import ActionContext, Actor, AssistantPolicy, OUTBOUND_ACTIONS, SourceEvidence


_SCHEMA = """
CREATE TABLE IF NOT EXISTS a5_actions(
 owner_id TEXT NOT NULL,action_id TEXT NOT NULL,version INTEGER NOT NULL,
 fingerprint TEXT NOT NULL,digest TEXT NOT NULL,snapshot_json TEXT NOT NULL,
 decision TEXT NOT NULL,reasons_json TEXT NOT NULL,state TEXT NOT NULL,
 approval_actor TEXT NOT NULL DEFAULT '',approval_version INTEGER,
 approval_digest TEXT NOT NULL DEFAULT '',approval_at REAL,
 execution_token TEXT NOT NULL DEFAULT '',attempt_count INTEGER NOT NULL DEFAULT 0,
 evidence_ref TEXT NOT NULL DEFAULT '',readback_evidence_ref TEXT NOT NULL DEFAULT '',
 last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL,
 PRIMARY KEY(owner_id,action_id)
);
CREATE TABLE IF NOT EXISTS a5_action_versions(
 owner_id TEXT NOT NULL,action_id TEXT NOT NULL,version INTEGER NOT NULL,
 fingerprint TEXT NOT NULL,digest TEXT NOT NULL,snapshot_json TEXT NOT NULL,
 decision TEXT NOT NULL,reasons_json TEXT NOT NULL,created_at REAL NOT NULL,
 PRIMARY KEY(owner_id,action_id,version)
);
"""
_INITIAL_STATES = {"allow": "ready", "approval_required": "awaiting_approval",
                   "record_and_refer": "referred", "deny": "denied"}
_LOCKED_STATES = frozenset({"executing", "applied", "applied_unverified", "unknown"})
_EXECUTABLE_STATES = frozenset({"ready", "approved", "not_applied"})
_ALL_STATES = _LOCKED_STATES | _EXECUTABLE_STATES | frozenset(_INITIAL_STATES.values()) | {"rejected"}


def _json(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ContractError("action data must be finite JSON") from None


def _object(value):
    if not isinstance(value, dict):
        raise ContractError("action payload and source stamp must be JSON objects")

    def check(item):
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise ContractError("action JSON keys must be strings")
            for part in item.values():
                check(part)
        elif isinstance(item, list):
            for part in item:
                check(part)
        elif item is not None and type(item) not in (str, int, float, bool):
            raise ContractError("action data contains an unsupported JSON value")
    try:
        check(value)
    except RecursionError:
        raise ContractError("action data must be finite JSON") from None
    return json.loads(_json(value))


def _plain(value):
    if isinstance(value, dict):
        return {key: _plain(part) for key, part in value.items()}
    if isinstance(value, (set, frozenset)):
        return sorted(_plain(part) for part in value)
    if isinstance(value, (list, tuple)):
        return [_plain(part) for part in value]
    return value


def _context_data(context):
    if not isinstance(context, ActionContext):
        raise ContractError("a trusted ActionContext is required")
    return _plain(asdict(context))


def _context_from(data):
    value = dict(data)
    value["initiator"] = Actor(**value["initiator"])
    for field in ("recipients", "group_audience"):
        value[field] = tuple(Actor(**actor) for actor in value[field])
    value["source_evidence"] = tuple(SourceEvidence(**source) for source in value["source_evidence"])
    return ActionContext(**value)


def _sha(value):
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


class AssistantActions:
    def __init__(self, store: Store, owner_id, policy: AssistantPolicy):
        if not isinstance(store, Store) or not isinstance(policy, AssistantPolicy):
            raise ContractError("actions require a Store and AssistantPolicy")
        if not isinstance(owner_id, str) or owner_id not in policy.config.owner_user_ids:
            raise ContractError("the action owner must be a configured owner identity")
        self.store, self.owner_id, self.policy = store, owner_id, policy
        with closing(store.connect()) as db:
            db.executescript(_SCHEMA)

    def _row(self, action_id, db=None):
        if not isinstance(action_id, str) or not action_id:
            raise ContractError("an exact action id is required")
        sql = "SELECT * FROM a5_actions WHERE owner_id=? AND action_id=?"
        args = (self.owner_id, action_id)
        row = db.execute(sql, args).fetchone() if db is not None else self.store.one(sql, args)
        if row is None:
            raise ContractError("action not found")
        return dict(row)

    @staticmethod
    def _metadata(row):
        return {"action_id": row["action_id"], "version": row["version"],
                "digest": row["digest"], "decision": row["decision"],
                "reasons": json.loads(row["reasons_json"]),
                "reason_codes": json.loads(row["reasons_json"]), "state": row["state"],
                "attempt_count": row["attempt_count"], "evidence_ref": row["evidence_ref"],
                "readback_evidence_ref": row["readback_evidence_ref"],
                "last_error": row["last_error"]}

    def _audit(self, db, event, row, **details):
        self.store.audit(db, "assistant_action." + event,
                         self.owner_id + ":" + row["action_id"],
                         _json({"version": row["version"], "digest": row["digest"], **details}))

    def _owner(self, actor):
        if (not isinstance(actor, Actor) or not actor.verified or actor.user_id != self.owner_id
                or actor.user_id not in self.policy.config.owner_user_ids):
            raise ContractError("a verified owner identity is required")

    @staticmethod
    def _binding(row, version, digest):
        if type(version) is not int or version != row["version"] or digest != row["digest"]:
            raise ContractError("the exact action version and digest are required")

    @staticmethod
    def _approved(row):
        return bool(row["approval_actor"]) and row["approval_version"] == row["version"] \
            and row["approval_digest"] == row["digest"]

    def prepare(self, context: ActionContext, payload: dict, source_stamp: dict):
        context_data = _context_data(context)
        payload, source_stamp = _object(payload), _object(source_stamp)
        if context.action_kind in OUTBOUND_ACTIONS:
            if not isinstance(payload.get("body"), str) or payload["body"] != context.content_text:
                raise ContractError("the outgoing body must equal the policy-reviewed content")
            if not isinstance(payload.get("target"), (str, dict)) or not payload["target"]:
                raise ContractError("an exact resolved outgoing target is required")
        decision = self.policy.evaluate(context)
        snapshot = {"owner_id": self.owner_id, "action_id": context.action_id,
                    "context": context_data, "payload": payload, "source_stamp": source_stamp,
                    "policy": {"outcome": decision.outcome, "reasons": list(decision.reason_codes)}}
        fingerprint = _sha(snapshot)
        with self.store.transaction() as db:
            old = db.execute("SELECT * FROM a5_actions WHERE owner_id=? AND action_id=?",
                             (self.owner_id, context.action_id)).fetchone()
            if old is not None and old["fingerprint"] == fingerprint:
                return self._metadata(old)
            if old is not None and old["state"] in _LOCKED_STATES:
                raise ContractError("an executed or unresolved action cannot be replaced")
            version = old["version"] + 1 if old is not None else 1
            digest = _sha({"version": version, "fingerprint": fingerprint})
            now, raw, reasons = self.store.clock(), _json(snapshot), _json(list(decision.reason_codes))
            state = _INITIAL_STATES[decision.outcome]
            db.execute("""INSERT INTO a5_actions(owner_id,action_id,version,fingerprint,digest,
                snapshot_json,decision,reasons_json,state,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(owner_id,action_id) DO UPDATE SET version=excluded.version,
                fingerprint=excluded.fingerprint,digest=excluded.digest,snapshot_json=excluded.snapshot_json,
                decision=excluded.decision,reasons_json=excluded.reasons_json,state=excluded.state,
                approval_actor='',approval_version=NULL,approval_digest='',approval_at=NULL,
                execution_token='',attempt_count=0,evidence_ref='',readback_evidence_ref='',last_error='',
                updated_at=excluded.updated_at""",
                       (self.owner_id, context.action_id, version, fingerprint, digest, raw,
                        decision.outcome, reasons, state, now, now))
            db.execute("INSERT INTO a5_action_versions VALUES(?,?,?,?,?,?,?,?,?)",
                       (self.owner_id, context.action_id, version, fingerprint, digest, raw,
                        decision.outcome, reasons, now))
            row = self._row(context.action_id, db)
            self._audit(db, "prepared", row, state=state)
            return self._metadata(row)

    def snapshot(self, action_id):
        """Return a detached frozen-input copy for the trusted execution adapter."""
        return self._snapshot(self._row(action_id))

    @classmethod
    def _snapshot(cls, row):
        snapshot = json.loads(row["snapshot_json"])
        return {**cls._metadata(row), "context": snapshot["context"],
                "payload": snapshot["payload"], "source_stamp": snapshot["source_stamp"]}

    get = snapshot

    def list(self, state=None, limit=100):
        """Owner-scoped snapshots; only authenticated host endpoints expose these."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ContractError("action list limit must be between 1 and 1000")
        if state is not None and state not in _ALL_STATES:
            raise ContractError("unknown action state")
        sql, args = "SELECT * FROM a5_actions WHERE owner_id=?", [self.owner_id]
        if state is not None:
            sql += " AND state=?"
            args.append(state)
        rows = self.store.rows(sql + " ORDER BY updated_at DESC,action_id LIMIT ?", (*args, limit))
        return [self._snapshot(row) for row in rows]

    def decide(self, action_id, version, digest, actor: Actor, approve=True):
        self._owner(actor)
        if type(approve) is not bool:
            raise ContractError("approve must be a boolean")
        with self.store.transaction() as db:
            row = self._row(action_id, db)
            self._binding(row, version, digest)
            if row["state"] in _LOCKED_STATES:
                raise ContractError("an executed or unresolved action cannot be reapproved")
            snapshot = json.loads(row["snapshot_json"])
            decision = self.policy.evaluate(_context_from(snapshot["context"]))
            if approve and (row["decision"] == "deny" or decision.outcome == "deny"):
                raise ContractError("a denied action cannot be approved")
            if approve and {"outcome": decision.outcome, "reasons": list(decision.reason_codes)} != snapshot["policy"]:
                raise ContractError("policy changed; prepare a current action before approval")
            state = "approved" if approve else "rejected"
            if row["state"] == state and (not approve or self._approved(row)):
                return self._metadata(row)
            db.execute("""UPDATE a5_actions SET state=?,approval_actor=?,approval_version=?,
                approval_digest=?,approval_at=?,updated_at=? WHERE owner_id=? AND action_id=?""",
                       (state, actor.user_id if approve else "", version if approve else None,
                        digest if approve else "", self.store.clock(), self.store.clock(), self.owner_id, action_id))
            row = self._row(action_id, db)
            self._audit(db, "approved" if approve else "rejected", row, actor_id=actor.user_id)
            return self._metadata(row)

    def _gate(self, row, digest, source_stamp, context):
        reasons = []
        if digest != row["digest"]:
            reasons.append("action_digest_changed")
        snapshot = json.loads(row["snapshot_json"])
        if _json(source_stamp) != _json(snapshot["source_stamp"]):
            reasons.append("source_stamp_changed")
        if _json(_context_data(context)) != _json(snapshot["context"]):
            reasons.append("action_context_changed")
        decision = self.policy.evaluate(context)
        if row["decision"] == "deny" or decision.outcome == "deny":
            reasons.append("policy_denied")
        elif {"outcome": decision.outcome, "reasons": list(decision.reason_codes)} != snapshot["policy"]:
            reasons.append("policy_changed")
        if row["state"] not in _EXECUTABLE_STATES:
            reasons.append("execution_in_progress" if row["state"] == "executing"
                           else "no_automatic_replay" if row["state"] in _LOCKED_STATES
                           else "action_not_ready")
        if decision.outcome != "allow" and not self._approved(row):
            reasons.append("exact_owner_approval_required")
        result = self._metadata(row)
        return {**result, "allowed": not reasons, "gate_reasons": list(dict.fromkeys(reasons))}

    def can_execute(self, action_id, digest, current_source_stamp, current_context):
        """Read-only check; the existing Harness may own send idempotency itself."""
        stamp = _object(current_source_stamp)
        return self._gate(self._row(action_id), digest, stamp, current_context)

    @staticmethod
    def _effect(result):
        if not isinstance(result, dict):
            return "unknown", "", "", "invalid_executor_result"
        status = result.get("status")
        evidence = result.get("evidence_ref", "")
        readback = result.get("readback_evidence_ref", "")
        evidence = evidence if isinstance(evidence, str) and evidence.strip() else ""
        readback = readback if isinstance(readback, str) and readback.strip() else ""
        if status == "applied" and evidence:
            verified = bool(readback) and readback != evidence
            return "applied" if verified else "applied_unverified", evidence, readback, ""
        if status == "not_applied" and evidence:
            return "not_applied", evidence, readback, ""
        return "unknown", evidence, readback, "" if status == "unknown" else "execution_evidence_missing"

    def _write_effect(self, db, row, effect, *, event="effect_recorded"):
        state, evidence, readback, error = effect
        db.execute("""UPDATE a5_actions SET state=?,execution_token='',evidence_ref=?,
            readback_evidence_ref=?,last_error=?,updated_at=? WHERE owner_id=? AND action_id=?""",
                   (state, evidence, readback, error, self.store.clock(), self.owner_id, row["action_id"]))
        updated = self._row(row["action_id"], db)
        self._audit(db, event, updated, state=state, evidence_ref=evidence,
                    readback_evidence_ref=readback, error=error)
        return updated

    def execute(self, action_id, digest, current_source_stamp, current_context, executor):
        if not callable(executor):
            raise ContractError("a callable action executor is required")
        stamp = _object(current_source_stamp)
        with self.store.transaction() as db:
            row = self._row(action_id, db)
            gate = self._gate(row, digest, stamp, current_context)
            if not gate["allowed"]:
                return {**gate, "executed": False}
            token = uuid.uuid4().hex
            db.execute("""UPDATE a5_actions SET state='executing',execution_token=?,
                attempt_count=attempt_count+1,evidence_ref='',readback_evidence_ref='',last_error='',
                updated_at=? WHERE owner_id=? AND action_id=?""",
                       (token, self.store.clock(), self.owner_id, action_id))
            row = self._row(action_id, db)
            frozen = json.loads(row["snapshot_json"])
            execution = {**self._metadata(row), "payload": frozen["payload"],
                         "context": frozen["context"], "source_stamp": frozen["source_stamp"],
                         "execution_token": token}
            self._audit(db, "execution_claimed", row, attempt_count=row["attempt_count"])
        try:
            effect = self._effect(executor(execution))
        except Exception:
            effect = ("unknown", "", "", "executor_exception")
        with self.store.transaction() as db:
            current = self._row(action_id, db)
            if current["execution_token"] != token or current["state"] != "executing":
                self._audit(db, "late_result_ignored", current)
                return {**self._metadata(current), "executed": True, "late_result_ignored": True}
            current = self._write_effect(db, current, effect)
            return {**self._metadata(current), "executed": True}

    def record_effect(self, action_id, version, digest, status="applied", evidence_ref=None,
                      readback_evidence_ref=None):
        """Record a trusted Harness outcome after its own atomic send operation."""
        effect = self._effect({"status": status, "evidence_ref": evidence_ref,
                               "readback_evidence_ref": readback_evidence_ref})
        with self.store.transaction() as db:
            row = self._row(action_id, db)
            self._binding(row, version, digest)
            current_effect = (row["state"], row["evidence_ref"], row["readback_evidence_ref"], row["last_error"])
            if current_effect == effect:
                return self._metadata(row)
            if (row["state"] == "applied_unverified" and effect[0] == "applied"
                    and row["evidence_ref"] == effect[1]):
                return self._metadata(self._write_effect(db, row, effect, event="readback_verified"))
            if row["state"] not in _EXECUTABLE_STATES:
                raise ContractError("an existing or unresolved effect cannot be overwritten")
            if row["decision"] == "deny" or (row["decision"] != "allow" and not self._approved(row)):
                raise ContractError("the action did not have execution authority")
            db.execute("""UPDATE a5_actions SET attempt_count=attempt_count+1
                WHERE owner_id=? AND action_id=?""", (self.owner_id, action_id))
            return self._metadata(self._write_effect(db, row, effect))

    def reconcile(self, action_id, version, digest, actor: Actor, status, evidence_ref,
                  readback_evidence_ref=None):
        self._owner(actor)
        if status not in {"applied", "not_applied", "unknown"}:
            raise ContractError("reconciliation requires an explicit effect status")
        if not isinstance(evidence_ref, str) or not evidence_ref.strip():
            raise ContractError("reconciliation requires a specific evidence reference")
        effect = self._effect({"status": status, "evidence_ref": evidence_ref,
                               "readback_evidence_ref": readback_evidence_ref})
        with self.store.transaction() as db:
            row = self._row(action_id, db)
            self._binding(row, version, digest)
            current_effect = (row["state"], row["evidence_ref"], row["readback_evidence_ref"], row["last_error"])
            if current_effect == effect:
                return self._metadata(row)
            if row["state"] not in {"executing", "unknown", "applied_unverified"}:
                raise ContractError("only an unresolved action may be reconciled")
            updated = self._write_effect(db, row, effect, event="owner_reconciled")
            self._audit(db, "reconciliation_authority", updated, actor_id=actor.user_id)
            return self._metadata(updated)
