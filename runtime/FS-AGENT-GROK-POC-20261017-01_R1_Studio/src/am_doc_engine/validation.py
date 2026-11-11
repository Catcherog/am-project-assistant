"""Deterministic document checks, deliberately separate from semantic/business review.

These checks catch structural and lexical errors. A correct citation identifier cannot
prove that prose is entailed by its source. Narrative output therefore retains a
``semantic_review_required`` issue for the host's independent review step.
"""
from __future__ import annotations

import math
import re
from datetime import date
from typing import Any, Iterable

from .content_checks import check_content
from .models import (
    DraftField, DraftRequest, FieldResolution, Issue, TemplateProfile,
)

_PLACEHOLDER = re.compile(r"^(?:待补(?:充)?|待确认|待核实|待提供|未知|暂无资料|TBD|TODO|[—\-]|\{\{[^}]+\}\})[。.!！\s]*$", re.I)


def dedupe_issues(issues: Iterable[Issue]) -> tuple[Issue, ...]:
    result, seen = [], set()
    for item in issues:
        key = (item.code, item.severity, item.field_id, item.message, item.evidence_ids)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(result)


def _valid_value(kind: str, value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and _PLACEHOLDER.fullmatch(value.strip()):
        return False
    if kind == "any":
        return not isinstance(value, str) or bool(value.strip())
    if kind == "string":
        return isinstance(value, str) and bool(value.strip())
    if kind == "number":
        return type(value) is int or type(value) is float and math.isfinite(value)
    if kind == "list":
        # An explicitly confirmed empty list can mean "no remaining actions".
        # Missing evidence is represented by missing_keys/None, never inferred from 0/[]/False.
        return isinstance(value, (list, tuple))
    if kind == "date":
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return False
        try:
            date.fromisoformat(value)
            return True
        except ValueError:
            return False
    return False


def validate_document(
    profile: TemplateProfile,
    request: DraftRequest,
    document_fields: tuple[DraftField, ...],
    resolutions: tuple[FieldResolution, ...],
) -> tuple[Issue, ...]:
    """Check one draft without accessing external services or changing any state.

    All errors block readiness. Warnings retain explicit limitations. In particular,
    the host must resolve narrative semantic review before preparing a submission.
    No result of this function constitutes professional approval or project closure.
    """
    issues: list[Issue] = []

    def add(code: str, message: str, field_id: str = "", evidence_ids=(), severity: str = "error") -> None:
        issues.append(Issue(code, severity, message, field_id, tuple(evidence_ids)))

    if (profile.tenant_id, profile.doc_type) != (request.tenant_id, request.doc_type):
        add("profile_scope_mismatch", "模板与请求的租户或文档类型不一致")
    if "*" not in profile.project_types and request.project_type not in profile.project_types:
        add("profile_project_type_mismatch", "模板不适用于该项目类型")
    if "*" not in profile.stages and request.stage not in profile.stages:
        add("profile_stage_mismatch", "模板不适用于该阶段")
    section_ids = {section.section_id for section in profile.sections}
    field_rules = {rule.field_id: rule for rule in profile.fields}
    if len(field_rules) != len(profile.fields) or len(section_ids) != len(profile.sections):
        add("profile_duplicate_structure", "模板包含重复章节或字段身份")

    resolved: dict[str, FieldResolution] = {}
    for resolution in resolutions:
        if resolution.field_id in resolved:
            add("duplicate_resolution", "同一字段存在多份事实解析结果", resolution.field_id)
        else:
            resolved[resolution.field_id] = resolution
        issues.extend(resolution.issues)
        if resolution.field_id not in field_rules:
            add("unknown_resolution_field", "事实解析结果引用未知字段", resolution.field_id)

    fields: dict[str, DraftField] = {}
    for item in document_fields:
        if item.field_id in fields:
            add("duplicate_field", "文档中字段身份重复", item.field_id)
        else:
            fields[item.field_id] = item
        if item.field_id not in field_rules:
            add("unknown_field", "生成结果包含模板未定义的字段", item.field_id)

    for rule in profile.fields:
        fid = rule.field_id
        if rule.section_id not in section_ids:
            add("unknown_section", "字段所属章节不存在", fid)
        item = fields.get(fid)
        required = rule.is_required(request) and rule.applies(request)
        if item is None:
            if rule.applies(request):
                add("field_absent", "适用字段没有生成结果", fid, severity="error" if required else "warning")
            continue
        if not isinstance(item.text, str):
            add("invalid_text_type", "字段文本必须为字符串", fid)
            continue
        if item.origin not in {"extractive", "model", "human"}:
            add("invalid_origin", "未知的字段内容来源", fid)
        if item.status not in {"filled", "missing", "conflict", "not_applicable"}:
            add("invalid_field_status", "未知的字段状态", fid)
        if len(item.text) > rule.max_chars:
            add("field_too_long", "字段超过模板允许的长度", fid)
        if isinstance(item.evidence_ids, str) or any(not isinstance(eid, str) for eid in item.evidence_ids):
            add("invalid_evidence_ids", "证据引用必须为字符串集合", fid)
            continue
        if len(item.evidence_ids) != len(set(item.evidence_ids)):
            add("duplicate_evidence_reference", "字段引用了重复的证据身份", fid)
        if not rule.applies(request):
            if item.status != "not_applicable" or item.evidence_ids or item.text.strip() not in {"", "不适用"}:
                add("inapplicable_field_filled", "不适用字段被填写了业务内容", fid)
            continue
        if item.status == "not_applicable":
            add("incorrect_not_applicable", "适用条件已满足，字段不能标记不适用", fid)
            continue
        if item.status in {"missing", "conflict"}:
            add("field_" + item.status, "该字段仍有缺口或冲突", fid, severity="error" if required else "warning")
            if item.evidence_ids:
                # Continue to validate references even in a partial draft.
                pass
            else:
                continue
        if not item.text.strip() or _PLACEHOLDER.fullmatch(item.text.strip()):
            add("placeholder_or_empty", "字段尚未形成有效内容", fid, severity="error" if required else "warning")
        resolution = resolved.get(fid)
        if resolution is None:
            add("resolution_absent", "字段没有对应的事实解析记录", fid)
            continue
        if resolution.missing_keys:
            add("required_input_missing", "缺少字段所需资料: " + "、".join(resolution.missing_keys), fid, severity="error" if required else "warning")
        ev_by_id = {ev.evidence_id: ev for ev in resolution.evidence}
        if len(ev_by_id) != len(resolution.evidence):
            add("duplicate_resolution_evidence", "字段解析结果包含重复证据身份", fid)
        unknown = set(item.evidence_ids) - set(ev_by_id)
        if unknown:
            add("foreign_evidence_reference", "字段引用了本字段事实集合以外的证据", fid, sorted(unknown))
        selected = tuple(ev_by_id[eid] for eid in item.evidence_ids if eid in ev_by_id)
        for ev in selected:
            if ev.source_read_state == "unavailable":
                add("evidence_source_unavailable", "事实来源不可读，不能作为已填写内容的有效依据", fid, (ev.evidence_id,))
            elif ev.source_read_state == "partial":
                add("evidence_source_partial", "事实来源仅部分读取，需保留资料覆盖限制", fid, (ev.evidence_id,), severity="warning")
            if ev.role != "project_fact":
                add("non_fact_as_fact", "标准、模板和范例不能充当当前项目事实", fid, (ev.evidence_id,))
            if (ev.tenant_id, ev.project_id) != (request.tenant_id, request.project_id):
                add("evidence_scope_mismatch", "引用证据不属于当前租户和项目", fid, (ev.evidence_id,))
            if ev.key not in rule.fact_keys or ev.state not in rule.accepted_states or ev.semantic not in rule.semantics:
                add("evidence_rule_mismatch", "证据的事实键、状态或语义不满足填写规则", fid, (ev.evidence_id,))
            if not _valid_value(rule.value_type, ev.value):
                add("invalid_evidence_value", f"事实值不满足 {rule.value_type} 类型要求", fid, (ev.evidence_id,))
        if item.status != "filled":
            # A missing/conflicting field is deliberately incomplete; do not run prose
            # checks over its explanatory placeholder as if it were a factual claim.
            continue
        if not selected:
            add("uncited_content", "已填写的事实字段缺少有效依据", fid)
        if selected and not resolution.missing_keys:
            missing_covered = set(rule.fact_keys) - {ev.key for ev in selected}
            if missing_covered:
                add("evidence_coverage_missing", "填写内容未引用所有必需事实键: " + "、".join(sorted(missing_covered)), fid)

        from .adapters import extractive_text
        exact_extraction = (
            item.origin == "extractive"
            and item.text == extractive_text(resolution.evidence)
            and set(item.evidence_ids) == set(ev_by_id)
        )
        # A program-controlled line such as "正式计划：通过验收" is an attributed
        # baseline, not an assertion that acceptance happened. Rephrased/model text
        # remains subject to the conservative conclusion checks below.
        content_check = check_content(item.text, selected, check_conclusions=not exact_extraction)
        if content_check.unsupported_numbers:
            add("unsupported_number_or_date", "正文出现来源中不存在的数字或日期: " + "、".join(content_check.unsupported_numbers), fid)
        if content_check.unsupported_claims:
            add("unsupported_business_conclusion", "正文的通过、批准、结项或风险关闭表述缺少明确已确认依据: " + "、".join(content_check.unsupported_claims), fid)

        if rule.mode == "extractive" or item.origin == "extractive":
            if not exact_extraction:
                add("extractive_value_changed", "受控字段未完整保留解析出的原值与证据", fid)
        if item.origin in {"model", "human"} or item.origin == "extractive" and not exact_extraction:
            add("semantic_review_required", "叙述字段需按填写要求和证据独立核对；引用有效不代表内容已得到证实", fid, severity="warning")
    return dedupe_issues(issues)
