"""Evidence-bound fact projection. Model output never grants business authority."""
from __future__ import annotations

from collections import defaultdict
import json
import math
import re
import sqlite3
from typing import Any, Iterable

from .models import Observation, Principal, Source, canonical, digest, now_ms
from .policy import can_read


ASSERTION_TYPES = frozenset({"reported", "forecast", "proposal"})
FORMAL_FIELDS = frozenset({"plan", "review", "node", "decision", "progress", "owner", "risk", "ledger_record", "system_record"})
_FIELD_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_CANDIDATE_KEYS = frozenset({
    "source_id", "object_id", "version", "subject_id", "field", "value", "quote", "assertion_type"
})


class FactValidationError(ValueError):
    """Safe machine-readable rejection; messages never contain source content."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def validate_value(value: Any, *, depth: int = 0) -> None:
    if depth > 8:
        raise FactValidationError("value_too_deep")
    if value is None or type(value) in {str, bool, int, float}:
        if type(value) is str and len(value) > 8000:
            raise FactValidationError("value_too_large")
        if type(value) is float and not math.isfinite(value):
            raise FactValidationError("non_finite_value")
        if type(value) is int and value.bit_length() > 128:
            raise FactValidationError("number_too_large")
    elif type(value) is list:
        if len(value) > 100:
            raise FactValidationError("value_too_large")
        for item in value:
            validate_value(item, depth=depth + 1)
    elif type(value) is dict:
        if len(value) > 100 or any(type(k) is not str or len(k) > 128 for k in value):
            raise FactValidationError("invalid_value_object")
        for item in value.values():
            validate_value(item, depth=depth + 1)
    else:
        raise FactValidationError("invalid_value_type")
    if depth == 0 and len(canonical(value).encode("utf-8")) > 16384:
        raise FactValidationError("value_too_large")


def validate_candidate(candidate: Any, observation: Observation) -> dict[str, Any]:
    """Require an exact, nonempty quote and the complete evidence identity."""
    if type(candidate) is not dict or set(candidate) != _CANDIDATE_KEYS:
        raise FactValidationError("candidate_schema")
    for key in ("source_id", "object_id", "version", "subject_id", "field", "quote", "assertion_type"):
        if type(candidate[key]) is not str or not candidate[key].strip():
            raise FactValidationError("candidate_schema")
    if any(candidate[key] != getattr(observation, key) for key in ("source_id", "object_id", "version")):
        raise FactValidationError("evidence_identity_mismatch")
    if len(candidate["subject_id"]) > 256 or not _FIELD_RE.fullmatch(candidate["field"]):
        raise FactValidationError("invalid_subject_or_field")
    if candidate["assertion_type"] not in ASSERTION_TYPES:
        raise FactValidationError("model_authority_forbidden")
    if len(candidate["quote"]) > 4000 or candidate["quote"] not in observation.text:
        raise FactValidationError("quote_not_verbatim")
    if candidate["assertion_type"] == "forecast" and candidate["field"] == "plan":
        raise FactValidationError("forecast_must_use_forecast_field")
    validate_value(candidate["value"])
    return dict(candidate)


def _path(value: Any, path: Any) -> Any:
    if type(path) is not list or not path or len(path) > 12:
        raise FactValidationError("invalid_mapping_path")
    result = value
    try:
        for key in path:
            if type(key) is str and type(result) is dict:
                result = result[key]
            elif type(key) is int and type(result) is list and key >= 0:
                result = result[key]
            else:
                raise KeyError(key)
    except (KeyError, IndexError):
        raise FactValidationError("mapping_value_missing") from None
    return result


def _structured_enabled(source: Source) -> bool:
    # A configuration switch cannot turn a chat, an unknown adapter, or a manual
    # payload into a formal API. A docx block mapping must be approved explicitly.
    return (source.kind in {"project_snapshot", "task_snapshot", "ledger_snapshot", "tasks_collection", "approval_snapshot", "workitem_snapshot", "bitable_snapshot", "sheet_snapshot"}
            or (source.kind == "docx" and source.settings.get("trusted_structured") is True))


def _mapping_hash(source: Source) -> str:
    return digest({"kind": source.kind, "trusted_structured": source.settings.get("trusted_structured"),
                   "fact_mappings": source.settings.get("fact_mappings", {})})


def _trusted_manual_actor(source: Source, payload: dict[str, Any]) -> bool:
    actors = source.settings.get("trusted_actors", [])
    return (source.kind == "manual" and payload.get("trusted_actor") is True
            and type(payload.get("actor_id")) is str
            and (payload["actor_id"] == source.owner_user_id
                 or (type(actors) in {list, tuple} and payload["actor_id"] in actors)))


class FactEngine:
    """Derived facts can be rebuilt; original source records remain authoritative."""

    def __init__(self, store: Any):
        self.store = store
        self.clock = getattr(store, "clock", now_ms)
        with store.transaction() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS fact_candidates (
                fact_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL, object_id TEXT NOT NULL, version TEXT NOT NULL,
                subject_id TEXT NOT NULL, field TEXT NOT NULL, value_json TEXT NOT NULL,
                quote TEXT NOT NULL, assertion_type TEXT NOT NULL, authority TEXT NOT NULL,
                protected INTEGER NOT NULL DEFAULT 0, origin TEXT NOT NULL,
                provenance_json TEXT NOT NULL, extractor_version TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            )""")
            conn.execute("CREATE INDEX IF NOT EXISTS facts_subject ON fact_candidates(subject_id)")
            conn.execute("""CREATE TABLE IF NOT EXISTS extraction_issues (
                source_id TEXT NOT NULL, object_id TEXT NOT NULL, version TEXT NOT NULL,
                code TEXT NOT NULL, created_at_ms INTEGER NOT NULL,
                PRIMARY KEY(source_id, object_id, version, code)
            )""")

    def process(self, observation: Observation, candidates: Iterable[dict[str, Any]],
                extractor_version: str = "injected-v1", *, job_id: str | None = None,
                lease_token: str | None = None) -> dict[str, Any]:
        if type(extractor_version) is not str or not extractor_version or len(extractor_version) > 128:
            raise FactValidationError("invalid_extractor_version")
        if type(candidates) not in {list, tuple} or len(candidates) > 100:
            raise FactValidationError("too_many_candidates")
        # Never accept a caller's altered copy of a stored observation.
        persisted = self.store.get_observation(observation.source_id, observation.object_id, observation.version)
        if persisted is None:
            raise FactValidationError("evidence_unavailable")
        if canonical(persisted.to_dict()) != canonical(observation.to_dict()):
            raise FactValidationError("evidence_body_mismatch")
        if observation.deleted or observation.completeness != "complete":
            raise FactValidationError("evidence_incomplete")
        source = self.store.get_source(observation.source_id)
        prepared: list[dict[str, Any]] = []
        for candidate in candidates:
            item = validate_candidate(candidate, observation)
            self._check_subject(source, item["subject_id"])
            item.update(authority="unconfirmed", protected=False, origin="model", provenance={})
            prepared.append(item)
        prepared.extend(self._mapped_facts(source, observation))
        prepared.extend(self._manual_facts(source, observation))
        ids: list[str] = []
        inserted = 0
        with self.store.transaction() as conn:
            if (job_id is None) != (lease_token is None):
                raise FactValidationError("incomplete_job_identity")
            if job_id is not None:
                job = self.store._check_job(conn, job_id, lease_token)
                identity = {"source_id": observation.source_id, "object_id": observation.object_id,
                            "version": observation.version}
                if job["kind"] != "extract" or json.loads(job["payload_json"]) != identity:
                    raise FactValidationError("extraction_job_identity_mismatch")
            current = conn.execute("SELECT current_version,state FROM objects WHERE source_id=? AND object_id=?",
                                   (observation.source_id, observation.object_id)).fetchone()
            registered = conn.execute("SELECT config_json,availability FROM sources WHERE source_id=?",
                                      (observation.source_id,)).fetchone()
            if not registered or registered["availability"] == "denied":
                raise FactValidationError("source_unavailable")
            current_source = Source.from_dict(json.loads(registered["config_json"]))
            if not current_source.enabled or current_source.to_dict() != source.to_dict():
                raise FactValidationError("source_configuration_changed")
            if not current or current["state"] != "active" or current["current_version"] != observation.version:
                raise FactValidationError("evidence_superseded")
            raw = conn.execute("SELECT raw_expires_at_ms,purged FROM observations WHERE source_id=? AND object_id=? AND version=?",
                               (observation.source_id, observation.object_id, observation.version)).fetchone()
            if not raw or raw["purged"] or raw["raw_expires_at_ms"] <= self.clock():
                raise FactValidationError("evidence_unavailable")
            for item in prepared:
                identity = {k: item[k] for k in ("source_id", "object_id", "version", "subject_id", "field",
                                                 "value", "quote", "assertion_type", "authority", "origin", "provenance")}
                fact_id = digest(identity)
                ids.append(fact_id)
                cursor = conn.execute("""INSERT OR IGNORE INTO fact_candidates
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                    fact_id, item["source_id"], item["object_id"], item["version"], item["subject_id"], item["field"],
                    canonical(item["value"]), item["quote"], item["assertion_type"], item["authority"],
                    int(item["protected"]), item["origin"], canonical(item["provenance"]), extractor_version,
                    self.clock(),
                ))
                inserted += cursor.rowcount
            if job_id is not None:
                conn.execute("UPDATE jobs SET state='done',finished_at_ms=?,lease_until_ms=NULL WHERE job_id=?",
                             (self.clock(), job_id))
                conn.execute("UPDATE gaps SET state='resolved',last_seen_ms=? WHERE source_id=? AND object_id=? AND kind='analysis_context_changed'",
                             (self.clock(),observation.source_id,observation.object_id))
        return {"inserted": inserted, "duplicates": len(ids) - inserted, "fact_ids": ids}

    @staticmethod
    def _check_subject(source: Source, subject_id: str) -> None:
        fixed = source.settings.get("subject_id")
        allowed = source.settings.get("subject_ids")
        if fixed is not None and subject_id != fixed:
            raise FactValidationError("subject_outside_source_scope")
        if allowed is not None and (type(allowed) not in {list, tuple} or subject_id not in allowed):
            raise FactValidationError("subject_outside_source_scope")

    def _mapped_facts(self, source: Source, observation: Observation) -> list[dict[str, Any]]:
        mappings = source.settings.get("fact_mappings", {})
        if not mappings or not _structured_enabled(source):
            return []
        if type(mappings) is not dict or len(mappings) > 40:
            raise FactValidationError("invalid_fact_mappings")
        result = []
        for field, mapping in mappings.items():
            if field not in FORMAL_FIELDS or type(mapping) is not dict:
                raise FactValidationError("invalid_fact_mapping")
            subject = mapping.get("subject_id", source.settings.get("subject_id"))
            if "subject_path" in mapping:
                subject = _path(observation.payload, mapping["subject_path"])
            quote = _path(observation.payload, mapping.get("quote_path"))
            value = _path(observation.payload, mapping.get("value_path"))
            item = validate_candidate({"source_id": observation.source_id, "object_id": observation.object_id,
                "version": observation.version, "subject_id": subject, "field": field, "value": value,
                "quote": quote, "assertion_type": "reported"}, observation)
            self._check_subject(source, subject)
            item.update(authority="source_confirmed", protected=False, origin="structured",
                        provenance={"mapping_hash": _mapping_hash(source), "value_path": mapping["value_path"]})
            result.append(item)
        return result

    def _manual_facts(self, source: Source, observation: Observation) -> list[dict[str, Any]]:
        if not _trusted_manual_actor(source, observation.payload):
            return []
        claims = observation.payload.get("structured_claims", [])
        if type(claims) is not list or len(claims) > 40:
            raise FactValidationError("invalid_manual_claims")
        result = []
        for claim in claims:
            if type(claim) is not dict or not {"field", "value", "quote"} <= set(claim):
                raise FactValidationError("invalid_manual_claim")
            if set(claim) - {"field", "value", "quote", "confirmed", "supersedes_object_id"}:
                raise FactValidationError("invalid_manual_claim")
            subject = observation.payload.get("subject_id")
            item = validate_candidate({"source_id": observation.source_id, "object_id": observation.object_id,
                "version": observation.version, "subject_id": subject, "field": claim["field"],
                "value": claim["value"], "quote": claim["quote"], "assertion_type": "reported"}, observation)
            self._check_subject(source, subject)
            confirmed = claim.get("confirmed") is True
            supersedes = claim.get("supersedes_object_id")
            if supersedes is not None and (not confirmed or type(supersedes) is not str or not supersedes
                                           or len(supersedes) > 256 or supersedes == observation.object_id):
                raise FactValidationError("invalid_supersession")
            item.update(authority="owner_confirmed" if confirmed else "unconfirmed", protected=confirmed,
                        origin="manual", provenance={"actor_id": observation.payload["actor_id"],
                                                       "supersedes_object_id": supersedes})
            result.append(item)
        return result

    def purge_expired_quotes(self) -> dict[str, int]:
        """Erase expired excerpts without erasing opted-in durable business facts.

        Call outside Store.purge_expired_raw()'s transaction. Source policy may
        instead forbid retained derived records, in which case remove them too.
        """
        cleared = removed = 0
        with self.store.transaction() as conn:
            rows = conn.execute("""SELECT f.fact_id,f.quote,s.config_json
                FROM fact_candidates f
                JOIN sources s ON s.source_id=f.source_id
                JOIN observations r ON r.source_id=f.source_id AND r.object_id=f.object_id AND r.version=f.version
                WHERE r.purged=1 OR r.raw_expires_at_ms<=?""", (self.clock(),)).fetchall()
            for row in rows:
                source = Source.from_dict(json.loads(row["config_json"]))
                if source.settings.get("retain_derived", True) is not True:
                    removed += conn.execute("DELETE FROM fact_candidates WHERE fact_id=?", (row["fact_id"],)).rowcount
                elif row["quote"]:
                    cleared += conn.execute("UPDATE fact_candidates SET quote='' WHERE fact_id=?", (row["fact_id"],)).rowcount
        return {"quotes_cleared": cleared, "facts_removed": removed}

    def query(self, subject_id: str, principal: Principal, *,
              connection: sqlite3.Connection | None = None) -> dict[str, Any]:
        """Apply the same fact policy in an owned or caller-fenced transaction.

        The optional connection must belong to this Store's active transaction;
        query never commits or renews that caller's transaction. Existing callers
        retain the original independent transaction behavior.
        """
        if type(subject_id) is not str or not subject_id or len(subject_id) > 256:
            raise FactValidationError("invalid_subject")
        now = self.clock()
        facts: list[dict[str, Any]] = []
        source_info: dict[str, dict[str, Any]] = {}
        unavailable: set[str] = set()
        scope = (self.store.transaction() if connection is None
                 else self.store.read_in_transaction(connection))
        with scope as conn:
            rows = conn.execute("""SELECT f.*, s.config_json, s.availability, s.last_checked_ms,
                s.collected_through_ms, o.current_version, o.state,
                r.text AS evidence_text, r.received_at_ms, r.raw_expires_at_ms, r.purged,
                r.completeness, r.deleted, r.url, r.occurred_at_ms
                FROM fact_candidates f
                JOIN sources s ON s.source_id=f.source_id
                JOIN objects o ON o.source_id=f.source_id AND o.object_id=f.object_id
                JOIN observations r ON r.source_id=f.source_id AND r.object_id=f.object_id AND r.version=f.version
                WHERE f.subject_id=? ORDER BY f.created_at_ms,f.fact_id""", (subject_id,)).fetchall()
            for row in rows:
                source = Source.from_dict(json.loads(row["config_json"]))
                # Filter before producing counts, conflicts, freshness summaries, or excerpts.
                if not source.enabled or row["availability"] == "denied" or not can_read(source, principal):
                    continue
                try:
                    self._check_subject(source, subject_id)
                except FactValidationError:
                    continue
                if row["state"] != "active" or row["current_version"] != row["version"] or row["deleted"]:
                    continue
                raw_available = not row["purged"] and row["raw_expires_at_ms"] > now
                if not raw_available:
                    unavailable.add("raw_evidence_expired")
                    if source.settings.get("retain_derived", True) is not True:
                        continue
                if row["completeness"] != "complete":
                    unavailable.add("evidence_incomplete")
                    continue
                if raw_available and (not row["quote"] or row["quote"] not in (row["evidence_text"] or "")):
                    unavailable.add("evidence_quote_unavailable")
                    continue
                provenance = json.loads(row["provenance_json"])
                if row["origin"] == "structured" and (not _structured_enabled(source)
                        or provenance.get("mapping_hash") != _mapping_hash(source)):
                    unavailable.add("mapping_changed_requires_reprocessing")
                    continue
                if row["origin"] == "manual":
                    actor = provenance.get("actor_id")
                    if not _trusted_manual_actor(source, {"actor_id": actor, "trusted_actor": True}):
                        continue
                freshness_seconds = source.settings.get("freshness_seconds", max(120, source.poll_seconds * 2))
                if type(freshness_seconds) not in {int, float} or freshness_seconds < 1:
                    freshness_seconds = max(120, source.poll_seconds * 2)
                stale = (source.kind != "manual" and (row["availability"] != "ok" or row["last_checked_ms"] is None
                         or now - row["last_checked_ms"] > freshness_seconds * 1000))
                source_info[source.source_id] = {
                    "source_id": source.source_id, "availability": row["availability"],
                    "last_checked_ms": row["last_checked_ms"], "collected_through_ms": row["collected_through_ms"],
                    "stale": stale,
                }
                facts.append({"fact_id": row["fact_id"], "subject_id": subject_id, "field": row["field"],
                    "value": json.loads(row["value_json"]), "assertion_type": row["assertion_type"],
                    "authority": row["authority"], "protected": bool(row["protected"]), "stale": stale,
                    "evidence": {"source_id": row["source_id"], "object_id": row["object_id"],
                        "version": row["version"], "quote": row["quote"] if raw_available else None,
                        "raw_available": raw_available, "raw_expires_at_ms": row["raw_expires_at_ms"],
                        "verification": "verbatim_at_extraction", "url": row["url"],
                        "occurred_at_ms": row["occurred_at_ms"], "received_at_ms": row["received_at_ms"]},
                    "_supersedes": provenance.get("supersedes_object_id")})
        # A newer observation alone never replaces a confirmed decision. An explicit
        # trusted correction can replace another current manual decision of the same field.
        replaced: set[str] = set()
        for new in facts:
            if new["authority"] != "owner_confirmed" or not new["_supersedes"]:
                continue
            for old in facts:
                if (old["authority"] == "owner_confirmed" and old["field"] == new["field"]
                        and old["evidence"]["source_id"] == new["evidence"]["source_id"]
                        and old["evidence"]["object_id"] == new["_supersedes"]
                        and old["evidence"]["occurred_at_ms"] < new["evidence"]["occurred_at_ms"]):
                    replaced.add(old["fact_id"])
        facts = [item for item in facts if item["fact_id"] not in replaced]
        for item in facts:
            item.pop("_supersedes")
        by_field: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in facts:
            if item["assertion_type"] != "proposal":
                by_field[item["field"]].append(item)
        conflicts = []
        effective = []
        for field, items in sorted(by_field.items()):
            if field in {'system_record','ledger_record'}:
                # A collection contains independent rows/nodes/deliverables.
                # Distinct record values are not conflicting singleton claims.
                for item in items:
                    if item['authority']!='unconfirmed':
                        effective.append({'field':field,'value':item['value'],
                            'fact_ids':[item['fact_id']],'stale':item['stale'],
                            'object_id':item['evidence']['object_id']})
                continue
            confirmed = [item for item in items if item["authority"] != "unconfirmed"]
            if len({canonical(item["value"]) for item in items}) > 1:
                kind = "confirmed_conflict" if len({canonical(item["value"]) for item in confirmed}) > 1 else "unconfirmed_discrepancy"
                conflicts.append({"field": field, "kind": kind, "fact_ids": [item["fact_id"] for item in items]})
            if confirmed and len({canonical(item["value"]) for item in confirmed}) == 1:
                effective.append({"field": field, "value": confirmed[0]["value"],
                                  "fact_ids": [item["fact_id"] for item in confirmed],
                                  "stale": any(item["stale"] for item in confirmed)})
        return {"subject_id": subject_id, "facts": facts, "effective": effective, "conflicts": conflicts,
                "pending_confirmation": [item["fact_id"] for item in facts if item["authority"] == "unconfirmed"],
                "freshness": {"as_of_ms": now, "sources": list(source_info.values())},
                "incomplete_reasons": sorted(unavailable)}
