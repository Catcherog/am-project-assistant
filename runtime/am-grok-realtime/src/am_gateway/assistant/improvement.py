"""Local evidence journal and replay proposals; never self-approved improvement.

The host records an immutable observation per stage/attempt. A later success
must use another event ID and cannot replace an earlier failure. Recording is
independent of analysis; the host runs ``analyze_once`` in its background lane.
This module has no model, network, business write, training or deployment port.
"""
from __future__ import annotations

import json
import math
import re
import uuid

from ..models import ContractError, dumps, key


ANALYZER_VERSION = "r6.candidate-review.v1"
MAX_TEXT_BYTES = 1024 * 1024
MAX_ID_BYTES = 4096
ANALYSIS_LEASE_SECONDS = 30.0
SLOW_REVIEW_SECONDS = 10.0
LONG_REVIEW_CHARACTERS = 1800

_NO_PROJECT = re.compile(
    r"(?:没查到|未查到|未找到|没有找到|不存在|找不到).{0,50}(?:项目|需求)"
    r"|(?:项目|需求).{0,30}(?:不存在|没找到|未找到)"
    r"|\bno\s+(?:matching\s+)?projects?\s+(?:found|available)\b", re.I)
_SOURCE_UNAVAILABLE = re.compile(
    r"来源不可用|资料不可用|读取失败|无法读取|未能读取|权限不足|SOURCE_(?:UNAVAILABLE|PARTIAL)", re.I)
_STYLE_MARKER = re.compile(r"作为(?:一个)?\s*AI|as an ai|claim_handle|trace_id|校验标记", re.I)
_FEEDBACK_RULES = (
    ("accuracy_feedback", re.compile(r"不对|错误|答错|查错|漏了|遗漏|没查到|没有这个|答非所问")),
    ("latency_feedback", re.compile(r"太慢|很慢|好慢|等太久|耗时|卡住|速度")),
    ("brevity_feedback", re.compile(r"太长|啰嗦|太多|简短|简洁|少说")),
    ("style_feedback", re.compile(r"风格|语气|生硬|官腔|自然一点|不像|AI\s*味", re.I)),
)
_SUGGESTIONS = {
    "possible_false_no_match": "用相同问题与完整授权来源快照回放名称、别名和编号检索；核对空结果含义。",
    "source_unavailable": "回放来源失败与部分读取，检查答复是否准确说明未能读取而非断言项目不存在。",
    "latency_review": "按相同入口分解排队、检索、模型、投递时间，比较正确完整答复的耗时。",
    "response_length_review": "用同一事实核对较简洁的候选答复，保留用户要求的信息和状态限定。",
    "style_review": "回放同一事实下的表达，检查称呼、自然程度及无关诊断文字。",
    "execution_failed": "保留首次失败，回放原输入并记录修复后的独立尝试。",
    "outcome_unknown": "核对实际回执与完成状态；未知结果不能按成功计分或自动重放外部效果。",
    "accuracy_feedback": "把反馈与原问题、完整来源逐项核对；用户意见本身不替代事实依据。",
    "latency_feedback": "关联反馈对应的实际请求和同一时钟的完成时间，测量候选路径。",
    "brevity_feedback": "在不漏掉要求和事实限定的前提下回放精简表达。",
    "style_feedback": "按该反馈准备表达候选并回放；不把一次偏好自动推广为全局规则。",
    "feedback_review": "保留反馈原文，关联实际观察后判断是否需要修改；不得自动成为批准或训练金标。",
}


def _text(value, name, *, maximum=MAX_TEXT_BYTES, required=False):
    if not isinstance(value, str) or required and not value.strip():
        raise ContractError("improvement " + name + " must be text")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ContractError("improvement " + name + " must be valid UTF-8") from exc
    if size > maximum:
        raise ContractError("improvement " + name + " exceeds local journal limit")
    return value


class ImprovementJournal:
    """Append-only inputs and a leased, one-item-at-a-time local analysis queue.

    Each request/response/feedback text field accepts up to 1 MiB, far above
    normal message sizes. There is no pending-count admission limit. One call
    processes at most one new record, with idle calls doing no analysis. Raw
    text stays in this store and is never an instruction to execute.
    """

    def __init__(self, store, owner_id):
        self.store = store
        self.owner_id = _text(owner_id, "owner_id", maximum=MAX_ID_BYTES, required=True)
        statements = (
            """CREATE TABLE IF NOT EXISTS r6_improvement_observations(
                observation_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,event_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,created REAL NOT NULL,
                UNIQUE(owner_id,event_id))""",
            """CREATE TABLE IF NOT EXISTS r6_improvement_feedback(
                owner_id TEXT NOT NULL,feedback_id TEXT NOT NULL,payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL,created REAL NOT NULL,PRIMARY KEY(owner_id,feedback_id))""",
            """CREATE TABLE IF NOT EXISTS r6_improvement_jobs(
                job_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,kind TEXT NOT NULL,
                object_id TEXT NOT NULL,payload_hash TEXT NOT NULL,state TEXT NOT NULL,
                created REAL NOT NULL,available_at REAL NOT NULL,token TEXT NOT NULL DEFAULT '',
                lease_until REAL,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',
                proposal_id TEXT NOT NULL DEFAULT '',UNIQUE(owner_id,kind,object_id))""",
            """CREATE INDEX IF NOT EXISTS r6_improvement_queue
                ON r6_improvement_jobs(owner_id,state,available_at,created,job_id)""",
            """CREATE TABLE IF NOT EXISTS r6_improvement_proposals(
                proposal_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,job_id TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,created REAL NOT NULL)""",
        )
        with self.store.transaction() as db:
            for statement in statements:
                db.execute(statement)

    def _enqueue(self, db, kind, object_id, digest, now):
        job_id = "ij_" + key(self.owner_id, kind, object_id)[:40]
        db.execute("""INSERT INTO r6_improvement_jobs
            (job_id,owner_id,kind,object_id,payload_hash,state,created,available_at)
            VALUES(?,?,?,?,?,'pending',?,?)""",
            (job_id, self.owner_id, kind, object_id, digest, now, now))

    def record_observation(self, event_id, *, request, response, channel, actor_id,
                           source_version="", status="generated", latency_seconds=None, evidence_ref=""):
        event_id = _text(event_id, "event_id", maximum=MAX_ID_BYTES, required=True)
        if latency_seconds is not None and (isinstance(latency_seconds, bool)
                or not isinstance(latency_seconds, (int, float))
                or not math.isfinite(latency_seconds) or latency_seconds < 0):
            raise ContractError("improvement latency_seconds must be finite and nonnegative")
        payload = {
            "event_id": event_id, "request": _text(request, "request"),
            "response": _text(response, "response"),
            "channel": _text(channel, "channel", maximum=256, required=True),
            "actor_id": _text(actor_id, "actor_id", maximum=MAX_ID_BYTES, required=True),
            "source_version": _text(source_version, "source_version", maximum=MAX_ID_BYTES),
            "status": _text(status, "status", maximum=256, required=True),
            "latency_seconds": float(latency_seconds) if latency_seconds is not None else None,
            "evidence_ref": _text(evidence_ref, "evidence_ref", maximum=MAX_ID_BYTES),
            "evidence_kind": "host_observation_not_business_acceptance",
        }
        encoded, digest = dumps(payload), key(payload)
        observation_id = "io_" + key(self.owner_id, event_id)[:40]
        with self.store.transaction() as db:
            old = db.execute("SELECT * FROM r6_improvement_observations WHERE owner_id=? AND event_id=?",
                             (self.owner_id, event_id)).fetchone()
            if old:
                if old["payload_json"] != encoded or old["payload_hash"] != digest:
                    raise ContractError("improvement observation ID conflict; first observation retained")
                return old["observation_id"]
            now = self.store.clock()
            db.execute("INSERT INTO r6_improvement_observations VALUES(?,?,?,?,?,?)",
                (observation_id, self.owner_id, event_id, encoded, digest, now))
            self._enqueue(db, "observation", observation_id, digest, now)
        return observation_id

    def record_feedback(self, feedback_id, *, actor_id, text, observation_id="", source_ref=""):
        feedback_id = _text(feedback_id, "feedback_id", maximum=MAX_ID_BYTES, required=True)
        payload = {
            "feedback_id": feedback_id,
            "actor_id": _text(actor_id, "actor_id", maximum=MAX_ID_BYTES, required=True),
            "text": _text(text, "feedback", required=True),
            "observation_id": _text(observation_id, "observation_id", maximum=MAX_ID_BYTES),
            "source_ref": _text(source_ref, "source_ref", maximum=MAX_ID_BYTES),
            "evidence_kind": "user_opinion_not_verified_fact_or_approval",
        }
        encoded, digest = dumps(payload), key(payload)
        with self.store.transaction() as db:
            old = db.execute("SELECT * FROM r6_improvement_feedback WHERE owner_id=? AND feedback_id=?",
                             (self.owner_id, feedback_id)).fetchone()
            if old:
                if old["payload_json"] != encoded or old["payload_hash"] != digest:
                    raise ContractError("improvement feedback ID conflict; first feedback retained")
                return feedback_id
            # An unlinked/late feedback remains useful. A supplied foreign
            # observation, however, must not be imported into this owner's work.
            linked = db.execute("SELECT owner_id FROM r6_improvement_observations WHERE observation_id=?",
                                (observation_id,)).fetchone() if observation_id else None
            if linked and linked["owner_id"] != self.owner_id:
                raise ContractError("improvement observation outside owner journal")
            now = self.store.clock()
            db.execute("INSERT INTO r6_improvement_feedback VALUES(?,?,?,?,?)",
                       (self.owner_id, feedback_id, encoded, digest, now))
            self._enqueue(db, "feedback", feedback_id, digest, now)
        return feedback_id

    def _claim(self):
        now, token = self.store.clock(), uuid.uuid4().hex
        with self.store.transaction() as db:
            # Analysis has no external effects, so an expired local lease is
            # safe to replay. Tokens prevent the earlier worker committing late.
            db.execute("""UPDATE r6_improvement_jobs SET state='pending',token='',lease_until=NULL,
                last_error='local_analysis_lease_expired' WHERE owner_id=? AND state='processing'
                AND lease_until<=?""", (self.owner_id, now))
            row = db.execute("""SELECT * FROM r6_improvement_jobs WHERE owner_id=? AND state='pending'
                AND available_at<=? ORDER BY created,job_id LIMIT 1""", (self.owner_id, now)).fetchone()
            if row is None:
                return None
            db.execute("""UPDATE r6_improvement_jobs SET state='processing',token=?,lease_until=?,
                attempts=attempts+1 WHERE job_id=?""", (token, now + ANALYSIS_LEASE_SECONDS, row["job_id"]))
            return {**dict(row), "token": token}

    def _payload(self, job):
        if job["kind"] == "observation":
            row = self.store.one("SELECT * FROM r6_improvement_observations WHERE owner_id=? AND observation_id=?",
                                 (self.owner_id, job["object_id"]))
        else:
            row = self.store.one("SELECT * FROM r6_improvement_feedback WHERE owner_id=? AND feedback_id=?",
                                 (self.owner_id, job["object_id"]))
        if not row or row["payload_hash"] != job["payload_hash"]:
            raise ContractError("improvement immutable input unavailable")
        payload = json.loads(row["payload_json"])
        if key(payload) != row["payload_hash"]:
            raise ContractError("improvement immutable input integrity mismatch")
        return payload

    @staticmethod
    def _observation_issues(payload):
        answer, issues = payload["response"], []
        if _NO_PROJECT.search(answer):
            issues.append({"code": "possible_false_no_match", "basis": "negative_project_lookup_phrase",
                           "truth": "unverified_until_source_replay"})
        if _SOURCE_UNAVAILABLE.search(answer) or payload["status"] == "source_unavailable":
            issues.append({"code": "source_unavailable", "basis": "source_failure_phrase_or_host_status"})
        latency = payload["latency_seconds"]
        if latency is not None and latency > SLOW_REVIEW_SECONDS:
            issues.append({"code": "latency_review", "basis": "caller_supplied_stage_latency",
                           "observed_seconds": latency, "review_threshold_seconds": SLOW_REVIEW_SECONDS,
                           "end_to_end_proven": False, "sla_failure_inferred": False})
        if len(answer) > LONG_REVIEW_CHARACTERS:
            issues.append({"code": "response_length_review", "basis": "long_response_candidate",
                           "characters": len(answer), "requested_length_checked": False})
        if _STYLE_MARKER.search(answer):
            issues.append({"code": "style_review", "basis": "expression_or_diagnostic_marker",
                           "context_may_be_legitimate": True})
        if payload["status"] in {"failed", "not_applied", "error"}:
            issues.append({"code": "execution_failed", "basis": "host_status"})
        if payload["status"] in {"unknown", "send_unknown", "delivery_unknown", "applied_unverified"}:
            issues.append({"code": "outcome_unknown", "basis": "host_status"})
        return issues

    def _analyze(self, job):
        payload = self._payload(job)
        feedback_ids, observations, issues = [], [], []
        if job["kind"] == "observation":
            observations.append((job["object_id"], payload))
            issues.extend(self._observation_issues(payload))
        else:
            feedback_ids.append(payload["feedback_id"])
            issues.extend({"code": code, "basis": "user_feedback_not_verified_fact"}
                          for code, pattern in _FEEDBACK_RULES if pattern.search(payload["text"]))
            if not issues:
                issues.append({"code": "feedback_review", "basis": "user_feedback_not_verified_fact"})
            if payload["observation_id"]:
                linked = self.store.one("SELECT * FROM r6_improvement_observations WHERE owner_id=? AND observation_id=?",
                                        (self.owner_id, payload["observation_id"]))
                if linked:
                    observed = json.loads(linked["payload_json"])
                    if key(observed) != linked["payload_hash"]:
                        raise ContractError("improvement linked observation integrity mismatch")
                    observations.append((linked["observation_id"], observed))
        if not issues:
            return None
        observation_ids = [oid for oid, _ in observations]
        source_versions = list(dict.fromkeys(p["source_version"] for _, p in observations if p["source_version"]))
        return {
            "proposal_id": "ip_" + key(self.owner_id, job["job_id"], job["payload_hash"], ANALYZER_VERSION)[:40],
            "state": "candidate_needs_replay", "analyzer_version": ANALYZER_VERSION,
            "trigger": {"kind": job["kind"], "id": job["object_id"], "payload_hash": job["payload_hash"]},
            "issues": issues,
            "suggested_changes": list(dict.fromkeys(_SUGGESTIONS[item["code"]] for item in issues)),
            "replay_plan": {
                "observation_ids": observation_ids, "feedback_ids": feedback_ids,
                "source_versions": source_versions, "source_snapshot_required": True,
                "missing_observation": not bool(observations),
                "preserve_first_attempt": True,
                "compare": ["factual_accuracy", "requested_content_coverage", "style_fit", "complete_response_latency"],
                "acceptance": "replay_evidence_required_before_any_improvement_claim",
            },
            "evidence_refs": list(dict.fromkeys(
                [p["evidence_ref"] for _, p in observations if p["evidence_ref"]]
                + ([payload["source_ref"]] if job["kind"] == "feedback" and payload["source_ref"] else []))),
            "analysis_kind": "deterministic_local_candidate_review",
            "business_facts_changed": False, "execution_approved": False,
            "training_gold": False, "automatic_changes_applied": False,
            "improvement_verified": False,
        }

    def analyze_once(self):
        job = self._claim()
        if job is None:
            return None
        try:
            proposal = self._analyze(job)
        except Exception as exc:
            with self.store.transaction() as db:
                db.execute("""UPDATE r6_improvement_jobs SET state='pending',token='',lease_until=NULL,
                    available_at=?,last_error=? WHERE owner_id=? AND job_id=? AND state='processing' AND token=?""",
                    (self.store.clock() + 1, type(exc).__name__, self.owner_id, job["job_id"], job["token"]))
            raise
        with self.store.transaction() as db:
            current = db.execute("SELECT * FROM r6_improvement_jobs WHERE owner_id=? AND job_id=?",
                                 (self.owner_id, job["job_id"])).fetchone()
            if (not current or current["state"] != "processing" or current["token"] != job["token"]
                    or current["payload_hash"] != job["payload_hash"]):
                return None
            now = self.store.clock()
            if current["lease_until"] <= now:
                db.execute("UPDATE r6_improvement_jobs SET state='pending',token='',lease_until=NULL WHERE job_id=?",
                           (job["job_id"],))
                return None
            proposal_id = ""
            if proposal is not None:
                proposal["created_at"] = now
                proposal_id = proposal["proposal_id"]
                db.execute("INSERT INTO r6_improvement_proposals VALUES(?,?,?,?,?)",
                           (proposal_id, self.owner_id, job["job_id"], dumps(proposal), now))
            db.execute("""UPDATE r6_improvement_jobs SET state='done',token='',lease_until=NULL,
                proposal_id=?,last_error='' WHERE job_id=?""", (proposal_id, job["job_id"]))
        return proposal

    def list_proposals(self, limit=30):
        if type(limit) is not int or limit < 1:
            raise ContractError("improvement proposal limit must be a positive integer")
        rows = self.store.rows("""SELECT payload_json FROM r6_improvement_proposals WHERE owner_id=?
            ORDER BY created DESC,proposal_id DESC LIMIT ?""", (self.owner_id, min(limit, 200)))
        return [json.loads(row["payload_json"]) for row in rows]
