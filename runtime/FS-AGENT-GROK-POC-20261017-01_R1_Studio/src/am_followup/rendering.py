"""Small, grounded natural-language drafts and code-owned Feishu mentions.

No model call is required for read views or sending. A model may select allowed
fact IDs, purpose and tone BEFORE approval. This bounded composer cannot perform
general semantic entailment; arbitrary model prose is deliberately not accepted.
The existing Qwen extraction path can supply proposals to the evidence layer.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Iterable


class RenderError(ValueError):
    pass


PURPOSES = {
    "confirm_start": "想确认一下，测试目前是否已经开始？如果还没开始，主要卡点和预计开测时间是什么？",
    "confirm_release": "想确认一下，目前是否已经正式发布？如果已经发布，请补充发布记录；如果还没有，预计什么时候完成？",
    "confirm_external_version": "想确认一下，目前是否已有明确可对外使用的版本？如果还没有，预计什么时候能提供？",
    "request_estimate": "请帮忙补充这项工作的预计投入和计划处理时间，先给粗略估计也可以。",
    "clarify_conditions": "请帮忙确认正式执行前的条件是否已落实，并补充仍缺少的条件或意见。",
    "request_progress": "想确认一下这项工作的当前进展、主要卡点和下一步安排。",
}
TONE_PREFIX = {"neutral": "", "warm": "辛苦看一下。", "brief": ""}


def _plain(value: str, *, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise RenderError("TEXT_OUT_OF_BOUNDS")
    # Text nodes are the output boundary; control characters and raw mention
    # syntax are additionally rejected so later transport conversion stays safe.
    if re.search(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", value) or re.search(r"<\s*/?\s*at\b|@all\b", value, re.I):
        raise RenderError("CONTROL_OR_MENTION_IN_TEXT")
    return value.strip()


@dataclass(frozen=True)
class DisclosedFact:
    fact_id: str
    text: str
    evidence_ref: str
    evidence_version: str
    policy_version: str
    allowed_audiences: tuple[str, ...]
    authority: str = "verified"

    def __post_init__(self) -> None:
        _plain(self.text, max_length=300)
        if any(not isinstance(s, str) or not s for s in
               (self.fact_id, self.evidence_ref, self.evidence_version, self.policy_version)):
            raise RenderError("FACT_PROVENANCE_REQUIRED")
        if self.authority not in {"verified", "reported"}:
            raise RenderError("UNCONFIRMED_FACT_NOT_A_SENDABLE_ASSERTION")
        if not self.allowed_audiences or len(set(self.allowed_audiences)) != len(self.allowed_audiences):
            raise RenderError("EXPLICIT_DISCLOSURE_SCOPE_REQUIRED")


@dataclass(frozen=True)
class DraftSelection:
    purpose: str
    fact_ids: tuple[str, ...] = ()
    tone: str = "neutral"


def parse_selection(raw: str) -> DraftSelection:
    """Strict tool-free output contract; source text cannot set actions/targets."""
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 8192:
        raise RenderError("MODEL_OUTPUT_TOO_LARGE")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise RenderError("DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RenderError("MODEL_OUTPUT_NOT_JSON") from exc
    if type(value) is not dict or set(value) != {"purpose", "fact_ids", "tone"}:
        raise RenderError("MODEL_OUTPUT_SCHEMA")
    if type(value["purpose"]) is not str or value["purpose"] not in PURPOSES:
        raise RenderError("PURPOSE_NOT_ALLOWED")
    if type(value["tone"]) is not str or value["tone"] not in TONE_PREFIX:
        raise RenderError("TONE_NOT_ALLOWED")
    ids = value["fact_ids"]
    if (type(ids) is not list or len(ids) > 3 or
            any(type(x) is not str or not x or len(x) > 256 for x in ids) or len(set(ids)) != len(ids)):
        raise RenderError("FACT_IDS_INVALID")
    return DraftSelection(value["purpose"], tuple(ids), value["tone"])


@dataclass(frozen=True)
class ComposedDraft:
    body: str
    fact_ids: tuple[str, ...]
    dependencies: tuple[tuple[str, str, str], ...]
    purpose: str
    audience_key: str

    @property
    def body_hash(self) -> str:
        return hashlib.sha256(self.body.encode("utf-8")).hexdigest()


def compose(project_title: str, action_title: str, audience_key: str,
            purpose: str, facts: Iterable[DisclosedFact] = (),
            selection: DraftSelection | None = None) -> ComposedDraft:
    """Titles must also be approved for this audience by the caller's policy gate.

    The argument purpose comes from the task, NOT from the LLM. A changed model
    purpose is rejected rather than silently changing what is being requested.
    """
    project_title = _plain(project_title, max_length=80)
    action_title = _plain(action_title, max_length=160)
    if purpose not in PURPOSES or not audience_key:
        raise RenderError("PURPOSE_OR_AUDIENCE_REQUIRED")
    available = tuple(facts)
    by_id = {f.fact_id: f for f in available}
    if len(by_id) != len(available):
        raise RenderError("DUPLICATE_FACT_ID")
    selection = selection or DraftSelection(purpose)
    if selection.purpose != purpose or selection.tone not in TONE_PREFIX or len(selection.fact_ids) > 3:
        raise RenderError("MODEL_CHANGED_PURPOSE")
    chosen = []
    for fact_id in selection.fact_ids:
        fact = by_id.get(fact_id)
        if fact is None or audience_key not in fact.allowed_audiences:
            raise RenderError("FACT_NOT_DISCLOSABLE")
        chosen.append(fact)
    lines = [f"关于{project_title}的{action_title}，"]
    for fact in chosen:
        lines.append(("目前收到的反馈是：" if fact.authority == "reported" else "") + fact.text.rstrip("。") + "。")
    lines.append(TONE_PREFIX[selection.tone] + PURPOSES[purpose])
    body = "".join(lines)
    return ComposedDraft(body, tuple(f.fact_id for f in chosen),
                         tuple((f.evidence_ref, f.evidence_version, f.policy_version) for f in chosen),
                         purpose, audience_key)


def feishu_post(body: str, mention_ids: tuple[str, ...] = ()) -> dict:
    """Build a post with actual code-owned @ nodes; no @all, attachments or cards.

    Input IDs must be resolved open_ids for THIS app; structural validation here
    is not directory authorization. The recipient gate must prove membership.
    """
    body = _plain(body, max_length=4000)
    if len(mention_ids) > 20 or len(set(mention_ids)) != len(mention_ids):
        raise RenderError("MENTION_LIST_INVALID")
    if any(not isinstance(x, str) or not re.fullmatch(r"ou_[A-Za-z0-9_-]{1,128}", x) for x in mention_ids):
        raise RenderError("VERIFIED_OPEN_IDS_REQUIRED")
    content = []
    if mention_ids:
        content.append([{"tag": "at", "user_id": uid} for uid in mention_ids])
    content.extend([[{"tag": "text", "text": line}] for line in body.splitlines()])
    return {"zh_cn": {"title": "", "content": content}}


def describe_receipt(state: str, message_id: str | None = None) -> str:
    if state == "confirmed" and message_id:
        return "已发出并核对消息记录，正在等对方反馈。"
    if state in {"unknown", "sending"}:
        return "发送结果尚未核实，正在核对，暂时不会重发。"
    if state in {"queued", "approved", "pending"}:
        return "已确认，等待到达发送时间。"
    if state in {"paused", "revoked", "cancelled", "blocked"}:
        return "尚未发送的跟进已停止；已发出的消息会保留记录。"
    return "暂未确认发送成功。"

