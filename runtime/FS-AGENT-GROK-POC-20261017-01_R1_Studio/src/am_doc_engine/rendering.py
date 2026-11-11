"""Deterministic document previews and normalized, reviewable Feishu write plans.

Plans are NOT Feishu API payloads and never execute operations. The host must map slots
against the copied native template, reauthorize, compare revision, execute and read back.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Any, Mapping

from .models import ContractError, DraftDocument, FactSnapshot, TemplateProfile, fingerprint, to_dict

STATE_LABELS = {"draft_complete": "草稿内容已齐", "draft_with_gaps": "草稿待补充", "ready_for_review": "待业务审阅"}


def _check_binding(document: DraftDocument, profile: TemplateProfile) -> None:
    if (document.tenant_id, document.template_id, document.template_revision, document.template_hash) != (
            profile.tenant_id, profile.template_id, profile.revision, profile.content_hash):
        raise ContractError("文档与所选模板快照不一致")
    if document.doc_type != profile.doc_type or document.rule_version != profile.rule_version:
        raise ContractError("文档类型或填写规则版本与模板快照不一致")
    if tuple(document.sections) != tuple(profile.sections):
        raise ContractError("文档章节、顺序或固定内容与模板快照不一致")
    if document.state not in STATE_LABELS:
        raise ContractError("渲染器不接受批准、发布等外部业务状态")
    if len({f.field_id for f in document.fields}) != len(document.fields):
        raise ContractError("文档包含重复字段")
    actual_fields = {f.field_id for f in document.fields}
    expected_fields = {f.field_id for f in profile.fields}
    if actual_fields != expected_fields:
        raise ContractError("文档字段与模板不完整对应；缺项须保留为 missing 或 not_applicable 字段")


def _md(text: str) -> str:
    """Keep external content plain, including links/HTML supplied by a model."""
    text = html.escape(text, quote=False)
    return re.sub(r"([\\`*_{}\[\]()#+.!|>~-])", r"\\\1", text)


def render_markdown(document: DraftDocument, profile: TemplateProfile, *, include_report: bool = False) -> str:
    _check_binding(document, profile)
    fields = {f.field_id: f for f in document.fields}
    lines = [f"# {_md(document.title)}", "", f"> {STATE_LABELS[document.state]} · 信息截止 {_md(document.as_of)}", ""]
    for section in profile.sections:
        lines.extend([f"## {_md(section.title)}", ""])
        if section.fixed_text:
            lines.extend([_md(section.fixed_text), ""])
        for rule in profile.fields:
            if rule.section_id != section.section_id:
                continue
            value = fields.get(rule.field_id)
            text = value.text if value is not None else "待补：该字段尚未生成。"
            lines.extend([f"### {_md(rule.label)}", "", _md(text), ""])
    if include_report:
        lines.extend(["---", "", "## 生成记录", "",
                      f"模板：{_md(document.template_id)} / {_md(document.template_revision)}", "",
                      f"规则版本：{_md(document.rule_version)}；文档修订：{document.revision}", ""])
        for item in document.issues:
            lines.extend([f"- {_md(item.code)}：{_md(item.message)}", ""])
        for item in document.questions:
            lines.extend([f"- 待补充：{_md(item)}", ""])
    return "\n".join(lines).rstrip() + "\n"


def render_html(document: DraftDocument, profile: TemplateProfile, *, include_report: bool = False) -> str:
    _check_binding(document, profile)
    escape = lambda s: html.escape(str(s), quote=True)
    fields = {f.field_id: f for f in document.fields}
    parts = ["<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">",
             "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
             "<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'\">",
             f"<title>{escape(document.title)}</title>",
             "<style>body{margin:0;background:#f6f4ef;color:#24342d;font:16px/1.8 system-ui,sans-serif}"
             "main{max-width:900px;margin:36px auto;padding:32px 40px;background:white;border:1px solid #e1e5df;border-radius:12px}"
             "h1{font-size:30px;line-height:1.35}h2{font-size:23px;margin-top:32px;border-bottom:1px solid #dce3dc;padding-bottom:8px}"
             "h3{font-size:17px;color:#345745}p{white-space:pre-wrap;overflow-wrap:anywhere}.meta{color:#63736a;font-size:14px}"
             ".missing,.conflict{border-left:3px solid #b68a3d;padding-left:12px}details{margin-top:32px;border-top:1px solid #dce3dc;padding-top:16px}"
             "li{overflow-wrap:anywhere}code{white-space:pre-wrap}@media(max-width:600px){main{margin:0;padding:20px;border-radius:0}}"
             "@media print{body{background:white}main{margin:0;border:0;padding:0;max-width:none}h2,h3{break-after:avoid}details{display:block}}"
             "</style></head><body><main>",
             f"<h1>{escape(document.title)}</h1><p class=\"meta\">{STATE_LABELS[document.state]} · 信息截止 {escape(document.as_of)}</p>"]
    for section in profile.sections:
        parts.append(f"<section><h2>{escape(section.title)}</h2>")
        if section.fixed_text:
            parts.append(f"<p>{escape(section.fixed_text)}</p>")
        for rule in profile.fields:
            if rule.section_id != section.section_id:
                continue
            value = fields.get(rule.field_id)
            text = value.text if value is not None else "待补：该字段尚未生成。"
            css = value.status if value is not None and value.status in {"missing", "conflict"} else "filled"
            parts.append(f"<div class=\"{css}\"><h3>{escape(rule.label)}</h3><p>{escape(text)}</p></div>")
        parts.append("</section>")
    if include_report:
        parts.append("<details><summary>生成记录与待补事项</summary>")
        parts.append(f"<p class=\"meta\">模板 {escape(document.template_id)} / {escape(document.template_revision)}；规则 {escape(document.rule_version)}</p><ul>")
        for issue in document.issues:
            parts.append(f"<li>{escape(issue.code)}：{escape(issue.message)}</li>")
        for question in document.questions:
            parts.append(f"<li>待补充：{escape(question)}</li>")
        parts.append("</ul></details>")
    parts.append("</main></body></html>")
    return "\n".join(parts)


def build_report(document: DraftDocument, profile: TemplateProfile, snapshot: FactSnapshot | None = None) -> dict[str, Any]:
    _check_binding(document, profile)
    evidence_map = {}
    if snapshot is not None:
        if (snapshot.snapshot_id, snapshot.content_hash) != (document.snapshot_id, document.snapshot_hash):
            raise ContractError("事实追溯报告与文档采用的快照不一致")
        used = {eid for f in document.fields for eid in f.evidence_ids}
        evidence_map = {e.evidence_id: to_dict(e) for e in snapshot.evidence if e.evidence_id in used}
        if used - set(evidence_map):
            raise ContractError("追溯快照缺少文档已引用的证据")
    return {"document_id": document.document_id, "state": document.state,
            "business_approval": "not_performed", "template_id": document.template_id,
            "template_revision": document.template_revision, "template_hash": document.template_hash,
            "rule_version": document.rule_version, "snapshot_id": document.snapshot_id,
            "snapshot_hash": document.snapshot_hash, "as_of": document.as_of,
            "field_evidence": {f.field_id: list(f.evidence_ids) for f in document.fields},
            "evidence_sources": evidence_map,
            "evidence_trace_complete": snapshot is not None,
            "issues": to_dict(document.issues), "questions": list(document.questions),
            "sources": [{"source_id": s.source_id, "role": s.role, "uri": s.uri,
                         "revision": s.revision, "read_state": s.read_state, "content_hash": s.content_hash}
                        for s in profile.assets],
            "rendering_limitations": ["HTML/Markdown 是内容预览；原生飞书模板版式需真实映射、写入与回读验收"]}


@dataclass(frozen=True)
class FieldSlot:
    block_id: str
    expected_text_hash: str
    template_hash: str


def build_feishu_write_plan(document: DraftDocument, profile: TemplateProfile, *,
                            document_token: str, expected_revision: str | int,
                            slots: Mapping[str, FieldSlot | Mapping[str, str]],
                            current_blocks: Mapping[str, str],
                            human_block_ids: tuple[str, ...] = ()) -> dict[str, Any]:
    """Produce a conservative whole-slot replacement plan against an already copied template.

    ``current_blocks`` is a fresh host-read snapshot. Each slot is a dedicated text block;
    multi-cell/inline/rich-text slots require a host renderer and are intentionally rejected.
    No unmapped or human-modified block is changed. A revision/hash mismatch blocks execution.
    """
    _check_binding(document, profile)
    errors: list[dict[str, str]] = []
    operations: list[dict[str, str]] = []
    preserved = set(current_blocks)
    # Missing facts may be saved as explicitly incomplete drafts. Inconsistent or
    # unvalidated revisions must not escape into an executable mapping plan.
    incomplete_draft_codes = {"missing_required", "missing_optional", "required_input_missing",
                              "field_missing", "field_conflict", "fact_conflict"}
    for issue in document.issues:
        if issue.severity == "error" and issue.code not in incomplete_draft_codes:
            errors.append({"code": "document_validation_incomplete", "message": f"文档需先完成核对: {issue.code}"})
    if not isinstance(document_token, str) or not document_token:
        errors.append({"code": "missing_target", "message": "缺少宿主实读的目标文档标识"})
    if (isinstance(expected_revision, bool) or not isinstance(expected_revision, (str, int))
            or str(expected_revision).strip().lower() in {"", "-1", "latest", "current", "*"}
            or isinstance(expected_revision, int) and expected_revision < 0):
        errors.append({"code": "missing_revision_guard", "message": "需指定实际读取的版本，不能使用 latest/-1"})
    by_rule = {rule.field_id: rule for rule in profile.fields}
    if set(slots) - set(by_rule):
        errors.append({"code": "unknown_slot", "message": "槽位引用了模板未定义字段"})
    used_blocks: set[str] = set()
    for field in document.fields:
        slot_value = slots.get(field.field_id)
        if slot_value is None:
            errors.append({"code": "missing_slot", "message": f"未映射字段: {field.field_id}"})
            continue
        try:
            slot = slot_value if isinstance(slot_value, FieldSlot) else FieldSlot(**slot_value)
        except (TypeError, ValueError) as exc:
            raise ContractError("槽位映射结构不匹配") from exc
        if slot.block_id in used_blocks:
            errors.append({"code": "duplicate_slot", "message": "多个字段不能覆盖同一个整块槽位"})
            continue
        used_blocks.add(slot.block_id)
        if slot.template_hash != profile.content_hash:
            errors.append({"code": "template_mapping_stale", "message": f"槽位对应模板版本已变化: {field.field_id}"})
            continue
        existing = current_blocks.get(slot.block_id)
        if not isinstance(existing, str):
            errors.append({"code": "slot_unreadable", "message": f"未读取槽位原值: {field.field_id}"})
            continue
        if fingerprint(existing) != slot.expected_text_hash:
            errors.append({"code": "slot_changed", "message": f"槽位原文已被修改: {field.field_id}"})
            continue
        if existing == field.text:
            continue
        if slot.block_id in human_block_ids or field.field_id in document.human_modified_fields:
            errors.append({"code": "human_content_conflict", "message": f"保留人工内容，需先合并: {field.field_id}"})
            continue
        operations.append({"operation": "replace_dedicated_text_slot", "field_id": field.field_id,
                           "block_id": slot.block_id, "expected_text_hash": slot.expected_text_hash,
                           "text": field.text})
        preserved.discard(slot.block_id)
    if len(document.fields) != len(profile.fields):
        errors.append({"code": "incomplete_field_mapping", "message": "文档字段未覆盖完整模板，需明确不适用项后再生成写入计划"})
    plan = {"schema_version": "am.native-slot-plan.v1", "document_token": document_token,
            "purpose": "save_draft", "document_state": document.state,
            "tenant_id": document.tenant_id, "project_id": document.project_id,
            "expected_revision": expected_revision, "template_hash": profile.content_hash,
            "document_id": document.document_id, "document_revision": document.revision,
            "ready_for_execution": not errors, "executes_external_action": False,
            "host_authorization_required": True, "recheck_revision_before_write": True,
            "readback_required": True, "business_approval": "not_performed",
            "operations": operations if not errors else [], "errors": errors,
            "preserved_block_ids": sorted(preserved if not errors else set(current_blocks)),
            "limitations": ["仅支持已映射的独立纯文本块；表格单元格、内联富文本和版式由宿主适配",
                            "此结构为 AM 内部计划，不能直接作为飞书 API 请求体"]}
    plan["plan_hash"] = fingerprint(plan)
    return plan
