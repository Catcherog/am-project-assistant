"""Typed follow-up outcomes for the existing terminal presenter.

Only native AM records author this snapshot. It is not an LLM tool schema,
approval, send command, new task store, or a replacement for the R4 renderer.
"""
from dataclasses import dataclass
import json
import math

from .contracts import ContractError, SpeechFrame, canonical, digest
from .render import unit, render_canonical


SCHEMA = "am-followup-speech/1"
SEND_STATES = {"none", "queued", "sending", "unknown", "verified", "cancelled", "failed"}
ITEM_STATES = {"waiting", "in_progress", "blocked", "submitted", "closed", "cancelled"}


def _text(value, name, maximum=4096):
    if type(value) is not str or not value.strip() or len(value) > maximum:
        raise ContractError("followup_invalid_" + name)
    return value


def _sha(value, name):
    if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ContractError("followup_invalid_" + name)
    return value


def _number(value, name):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ContractError("followup_invalid_" + name)
    return value


@dataclass(frozen=True)
class FollowupSpeech:
    serialized: str

    @property
    def data(self):
        return json.loads(self.serialized)

    @property
    def fingerprint(self):
        return digest(self.data)

    @classmethod
    def freeze(cls, value):
        d = json.loads(canonical(value))
        if set(d) != {"schema_version", "binding", "expires_at", "question", "item", "delivery", "feedback", "readback_job"} or d["schema_version"] != SCHEMA:
            raise ContractError("followup_speech_schema")
        b = d["binding"]
        if set(b) != {"turn_id", "scope", "source_revision", "item_version", "semantic_version", "cancel_epoch", "owner_id", "requester_id"}:
            raise ContractError("followup_speech_binding")
        for key in ("turn_id", "source_revision", "owner_id", "requester_id"):
            _text(b[key], key)
        if b["requester_id"] != b["owner_id"]:
            raise ContractError("followup_owner_scope_required")
        if type(b["scope"]) is not list or len(b["scope"]) != 4 or any(type(v) is not str or not v for v in b["scope"]) or b["scope"][-1] != b["requester_id"]:
            raise ContractError("followup_scope_unbound")
        for key in ("item_version", "semantic_version", "cancel_epoch"):
            if type(b[key]) is not int or b[key] < 0:
                raise ContractError("followup_invalid_fence")
        _number(d["expires_at"], "expiry")
        _text(d["question"], "question")
        item = d["item"]
        if set(item) != {"id", "label", "state", "paused", "verifier", "closed_by", "close_evidence_sha256"}:
            raise ContractError("followup_item_schema")
        _text(item["id"], "item_id"); _text(item["label"], "item_label")
        if item["state"] not in ITEM_STATES or type(item["paused"]) is not bool:
            raise ContractError("followup_item_state")
        _text(item["verifier"], "verifier")
        if item["state"] == "closed":
            if item["closed_by"] != item["verifier"]:
                raise ContractError("followup_close_without_verifier")
            _sha(item["close_evidence_sha256"], "closure_evidence")
        elif item["closed_by"] is not None or item["close_evidence_sha256"] is not None:
            raise ContractError("followup_closure_state_mismatch")
        delivery = d["delivery"]
        required = {"state", "occurrence_id", "intent_id", "request_uuid", "message_id", "body_sha256", "target_sha256", "receipt"}
        if set(delivery) != required or delivery["state"] not in SEND_STATES:
            raise ContractError("followup_delivery_schema")
        if delivery["state"] == "none":
            if any(delivery[k] is not None for k in required - {"state"}):
                raise ContractError("followup_absent_delivery_has_identity")
        else:
            for key in ("occurrence_id", "intent_id", "request_uuid"):
                _text(delivery[key], key)
            for key in ("body_sha256", "target_sha256"):
                _sha(delivery[key], key)
        if delivery["state"] == "verified":
            receipt = delivery["receipt"]
            expected = {k: delivery[k] for k in ("occurrence_id", "intent_id", "request_uuid", "message_id", "body_sha256", "target_sha256")}
            _text(delivery["message_id"], "message_id")
            if type(receipt) is not dict or receipt != expected:
                raise ContractError("followup_receipt_not_exact")
        elif delivery["receipt"] is not None:
            raise ContractError("followup_unverified_has_receipt")
        feedback = d["feedback"]
        if type(feedback) is not list or len(feedback) > 100:
            raise ContractError("followup_feedback_schema")
        seen = set()
        for f in feedback:
            if set(f) != {"message_id", "occurrence_id", "actor", "kind"} or f["kind"] not in {"acknowledged", "reported_update"}:
                raise ContractError("followup_feedback_schema")
            for key in ("message_id", "occurrence_id", "actor"):
                _text(f[key], key)
            if f["message_id"] in seen or f["occurrence_id"] != delivery["occurrence_id"]:
                raise ContractError("followup_feedback_not_exact")
            seen.add(f["message_id"])
        job = d["readback_job"]
        if job is not None:
            keys = {"job_id", "occurrence_id", "request_uuid", "body_sha256", "target_sha256", "message_id", "kind", "state", "not_before", "expires_at"}
            if type(job) is not dict or set(job) != keys:
                raise ContractError("followup_readback_job_schema")
            if delivery["state"] != "unknown" or job["kind"] != "original_scheduler_readback" or job["state"] != "queued":
                raise ContractError("followup_readback_job_state")
            for k in ("occurrence_id", "request_uuid", "body_sha256", "target_sha256", "message_id"):
                if job[k] != delivery[k]:
                    raise ContractError("followup_readback_job_not_exact")
            _text(job["job_id"], "readback_job_id")
            _text(job["message_id"], "readback_message_id")
            _number(job["not_before"], "readback_not_before")
            if _number(job["expires_at"], "readback_expiry") > d["expires_at"] or job["not_before"] >= job["expires_at"]:
                raise ContractError("followup_readback_job_lifetime")
        return cls(canonical(d))


def project_followup(snapshot):
    """Build bounded variants without calling a model or performing any action.

    Acknowledgements and self-reports never change the independent business
    completion clause. Full send previews intentionally use a different path.
    """
    d = snapshot.data
    # Revalidate even a manually constructed dataclass at the trust boundary.
    FollowupSpeech.freeze(d)
    b, item, delivery = d["binding"], d["item"], d["delivery"]
    state = delivery["state"]
    label = item["label"]
    variants = {
        "none": [f"“{label}”还没有本轮 AM 跟进发送记录。"],
        "queued": [f"“{label}”的本次发送已确认，正在等原 AM 执行。"],
        "sending": [f"“{label}”已进入发送流程，结果还没核实，暂时不会重发。"],
        "unknown": [f"“{label}”的发送结果还没核实，暂时不会重发。"],
        "verified": [f"“{label}”的跟进已发出，消息记录已核对。", f"“{label}”已经发出跟进，发送记录核对过了。"],
        "cancelled": [f"“{label}”的本次跟进已停止；已进入发送过程的历史结果仍保留核对。"],
        "failed": [f"“{label}”的本次发送未完成。"],
    }[state]
    if d["readback_job"] is not None:
        variants = [f"“{label}”是否送达还没确认。我确定一下，暂时不会重发。",
                    f"“{label}”的发送结果尚未确认，已安排核对，暂时不会重发。"]
    units = [unit("delivery", variants)]
    feedback = d["feedback"]
    if any(f["kind"] == "reported_update" for f in feedback):
        # Owner may be the test recipient; never invent a second person/leader.
        actors = {f["actor"] for f in feedback if f["kind"] == "reported_update"}
        who = "你" if actors == {b["requester_id"]} else "负责人"
        units.append(unit("feedback", [f"已收到{who}的自报进展，内容还待核对。", f"{who}已经回复进展，内容仍待核对。 ".strip()]))
    elif feedback:
        who = "你" if {f["actor"] for f in feedback} == {b["requester_id"]} else "负责人"
        units.append(unit("feedback", [f"{who}已回应；这只表示收到回复。", f"已收到{who}的回应。 ".strip()]))
    elif state == "verified":
        units.append(unit("feedback", ["目前还没有关联到这次跟进的回复。"] ))
    business = {
        "waiting": "事项仍在跟进中，尚未核验完成。",
        "in_progress": "事项处于处理中，尚未核验完成。",
        "blocked": "事项目前受阻，尚未核验完成。",
        "submitted": "事项已提交，正在等待验收。",
        "closed": "事项已由指定验收人凭核验依据关闭。",
        "cancelled": "事项已取消，这不表示业务完成。",
    }[item["state"]]
    units.append(unit("business", [business]))
    if item["paused"] and item["state"] not in {"closed", "cancelled"}:
        units.append(unit("paused", ["此事项的后续跟进目前已暂停。"] ))
    binding = {"turn_id": b["turn_id"], "scope_id": digest(b["scope"]),
               "outcome_hash": snapshot.fingerprint, "source_revision": b["source_revision"],
               "revision": b["item_version"], "cancel_epoch": b["cancel_epoch"]}
    return SpeechFrame.freeze({"schema_version": "am-style-frame/1", "binding": binding,
        "expires_at": d["expires_at"], "question": d["question"], "family": "followup_status",
        "units": units, "canonical_units": [u["id"] for u in units],
        "obligations": [u["id"] for u in units], "sources_requested": False,
        "context": [{"requester_is_owner": True, "readback_is_read_only": d["readback_job"] is not None}],
        "provenance": {"native_followup": d}})


def render_followup(snapshot):
    return render_canonical(project_followup(snapshot))
