"""Versioned, JSON-serializable contracts. Constructors reject malformed trust boundaries.

Only the host's authenticated service constructs AccessContext and Evidence. Model output
never constructs permissions, effective template status, or business approvals.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

Json = dict[str, Any]
ROLES = {"standard", "template", "instruction", "example", "project_fact"}
SEMANTICS = {"baseline", "forecast", "actual", "decision", "proposal"}
READ_STATES = {"complete", "partial", "unavailable"}
FACT_STATES = {"confirmed", "reported", "proposed", "retracted"}


class ContractError(ValueError):
    pass


class AccessDenied(PermissionError):
    pass


class SelectionError(ContractError):
    pass


class RevisionConflict(ContractError):
    pass


class IdempotencyConflict(ContractError):
    pass


class RunBusy(RuntimeError):
    pass


def canonical(value: Any) -> str:
    return json.dumps(to_dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def to_dict(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return {f.name: to_dict(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {k: to_dict(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_dict(v) for v in value]
    return value


def parse_time(value: str) -> datetime:
    if not isinstance(value, str):
        raise ContractError("时间必须为包含时区的 ISO 8601 字符串")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"无效时间: {value}") from exc
    if dt.tzinfo is None:
        raise ContractError("时间必须包含时区")
    return dt.astimezone(timezone.utc)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def identifier(value: str, label: str = "id") -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,191}", value):
        raise ContractError(f"{label} 需为稳定的 ASCII 标识，长度 1—192")


def text_tuple(value: Any, label: str, *, nonempty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(v, str) or not v for v in value):
        raise ContractError(f"{label} 必须为字符串数组，不能传单个字符串")
    if nonempty and not value:
        raise ContractError(f"{label} 不能为空")
    if len(set(value)) != len(value):
        raise ContractError(f"{label} 不能包含重复项")
    return tuple(value)


def require_keys(data: Json, allowed: set[str], required: set[str]) -> None:
    if not isinstance(data, dict):
        raise ContractError("需要 JSON 对象")
    unknown, missing = set(data) - allowed, required - set(data)
    if unknown or missing:
        raise ContractError(f"字段不匹配: unknown={sorted(unknown)}, missing={sorted(missing)}")


def strict_json(text: str) -> Any:
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ContractError(f"重复 JSON 字段: {key}")
            out[key] = value
        return out
    def reject(value):
        raise ContractError(f"JSON 不接受 {value}")
    try:
        return json.loads(text, object_pairs_hook=pairs, parse_constant=reject)
    except (ValueError, TypeError) as exc:
        raise ContractError("无法读取严格 JSON") from exc


@dataclass(frozen=True)
class AccessContext:
    tenant_id: str
    actor_id: str
    project_ids: tuple[str, ...]
    scopes: tuple[str, ...] = ("document:draft",)
    app_id: str = "standalone"
    chat_id: str = "standalone"

    def __post_init__(self):
        identifier(self.tenant_id, "tenant_id")
        identifier(self.actor_id, "actor_id")
        identifier(self.app_id, "app_id")
        identifier(self.chat_id, "chat_id")
        for name in ["project_ids", "scopes"]:
            object.__setattr__(self, name, text_tuple(getattr(self, name), name))

    def require(self, tenant_id: str, project_id: str, scope: str = "document:draft") -> None:
        if self.tenant_id != tenant_id or project_id not in self.project_ids or scope not in self.scopes:
            raise AccessDenied("当前身份未获准访问目标项目或执行该文档动作")


@dataclass(frozen=True)
class SourceAsset:
    source_id: str
    role: str
    uri: str
    revision: str
    content: str
    read_state: str = "complete"
    title: str = ""

    def __post_init__(self):
        identifier(self.source_id, "source_id")
        if self.role not in ROLES or self.read_state not in READ_STATES:
            raise ContractError("无效资料角色或读取状态")
        if not isinstance(self.content, str) or not isinstance(self.revision, str) or not self.revision or not isinstance(self.uri, str) or not self.uri:
            raise ContractError("资料需有正文、版本及来源 URI")

    @property
    def content_hash(self) -> str:
        return fingerprint(self.content)


@dataclass(frozen=True)
class SectionSpec:
    section_id: str
    title: str
    fixed_text: str = ""

    def __post_init__(self):
        identifier(self.section_id, "section_id")
        if not self.title:
            raise ContractError("章节标题不能为空")


@dataclass(frozen=True)
class FieldRule:
    field_id: str
    section_id: str
    label: str
    fact_keys: tuple[str, ...]
    instruction: str
    requirement_source_ids: tuple[str, ...]
    required: bool = True
    critical: bool = False
    required_when: dict[str, tuple[str, ...]] = field(default_factory=dict)
    applies_when: dict[str, tuple[str, ...]] = field(default_factory=dict)
    accepted_states: tuple[str, ...] = ("confirmed",)
    semantics: tuple[str, ...] = ("actual", "decision", "baseline")
    mode: str = "extractive"  # extractive: program-controlled values; narrative: optional model
    value_type: str = "any"  # any, string, number, date, list
    max_chars: int = 3000

    def __post_init__(self):
        identifier(self.field_id, "field_id")
        identifier(self.section_id, "section_id")
        for name in ["fact_keys", "requirement_source_ids", "accepted_states", "semantics"]:
            object.__setattr__(self, name, text_tuple(getattr(self, name), name, nonempty=True))
        if not self.label or not self.instruction or not self.fact_keys or not self.requirement_source_ids:
            raise ContractError("字段必须提供标签、填写要求、事实键和规则依据")
        if set(self.accepted_states) - (FACT_STATES - {"retracted"}) or not self.accepted_states:
            raise ContractError("无效事实确认状态")
        if set(self.semantics) - SEMANTICS or not self.semantics:
            raise ContractError("无效事实语义")
        if self.mode not in {"extractive", "narrative"} or self.value_type not in {"any", "string", "number", "date", "list"}:
            raise ContractError("无效字段生成模式或值类型")
        if type(self.required) is not bool or type(self.critical) is not bool or type(self.max_chars) is not int or not 1 <= self.max_chars <= 20000:
            raise ContractError("无效字段限制")
        for conditions in [self.required_when, self.applies_when]:
            if not isinstance(conditions, dict):
                raise ContractError("条件必须为对象")
            if set(conditions) - {"project_type", "stage", "mode"}:
                raise ContractError("条件只允许 project_type/stage/mode")
            for values in conditions.values():
                text_tuple(values, "条件值", nonempty=True)

    def applies(self, request: DraftRequest) -> bool:
        return all(getattr(request, k) in values for k, values in self.applies_when.items())

    def is_required(self, request: DraftRequest) -> bool:
        return self.required or bool(self.required_when) and all(getattr(request, k) in v for k, v in self.required_when.items())


@dataclass(frozen=True)
class TemplateProfile:
    tenant_id: str
    template_id: str
    revision: str
    rule_version: str
    doc_type: str
    title: str
    project_types: tuple[str, ...]
    stages: tuple[str, ...]
    sections: tuple[SectionSpec, ...]
    fields: tuple[FieldRule, ...]
    assets: tuple[SourceAsset, ...]
    status: str = "candidate"  # candidate is never automatically selected as effective
    effective_at: str = ""
    activation_ref: str = ""  # host-supplied ledger effective-state evidence
    allowed_actor_ids: tuple[str, ...] = ("*",)

    def __post_init__(self):
        for key in ["tenant_id", "template_id", "doc_type"]:
            identifier(getattr(self, key), key)
        for name in ["project_types", "stages", "allowed_actor_ids"]:
            object.__setattr__(self, name, text_tuple(getattr(self, name), name, nonempty=name != "allowed_actor_ids"))
        if not self.revision or not self.rule_version or not self.title or not self.project_types or not self.stages:
            raise ContractError("模板版本、类型、标题及适用范围不能为空")
        if self.status not in {"candidate", "effective", "retired"}:
            raise ContractError("模板状态无效")
        if self.status == "effective":
            parse_time(self.effective_at)
            if not self.activation_ref:
                raise ContractError("有效模板需有台账有效状态的依据")
        for collection, attr in [(self.sections, "section_id"), (self.fields, "field_id"), (self.assets, "source_id")]:
            ids = [getattr(x, attr) for x in collection]
            if len(set(ids)) != len(ids):
                raise ContractError(f"重复 {attr}")
        section_ids = {x.section_id for x in self.sections}
        sources = {x.source_id: x for x in self.assets}
        if not self.sections or not self.fields or not any(x.role == "template" for x in self.assets):
            raise ContractError("模板需包含章节、字段以及模板原件")
        for rule in self.fields:
            if rule.section_id not in section_ids:
                raise ContractError("字段引用了不存在的章节")
            for sid in rule.requirement_source_ids:
                if sid not in sources or sources[sid].role not in {"standard", "template", "instruction"}:
                    raise ContractError("硬性填写要求只能依据标准、模板或填写说明")

    @property
    def content_hash(self) -> str:
        return fingerprint(self)


@dataclass(frozen=True)
class Evidence:
    evidence_id: str
    tenant_id: str
    project_id: str
    key: str
    value: Any
    semantic: str
    state: str
    source_id: str
    source_uri: str
    source_revision: str
    locator: str
    observed_at: str
    role: str = "project_fact"
    allowed_actor_ids: tuple[str, ...] = ("*",)
    supersedes: tuple[str, ...] = ()
    source_read_state: str = "complete"
    qualifiers: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        for key in ["evidence_id", "tenant_id", "project_id", "key", "source_id"]:
            identifier(getattr(self, key), key)
        for name in ["allowed_actor_ids", "supersedes"]:
            object.__setattr__(self, name, text_tuple(getattr(self, name), name))
        if self.semantic not in SEMANTICS or self.state not in FACT_STATES or self.role not in ROLES:
            raise ContractError("无效事实语义、状态或来源角色")
        if self.source_read_state not in READ_STATES:
            raise ContractError("事实来源读取状态无效")
        if not self.source_uri or not self.source_revision or not self.locator:
            raise ContractError("事实需要可定位的来源、版本和位置")
        parse_time(self.observed_at)
        canonical(self.value)
        if not isinstance(self.qualifiers, dict):
            raise ContractError("事实限定信息必须为对象")
        # A detached JSON copy prevents a caller from changing the bound
        # attribution, polarity, or business state after snapshot construction.
        object.__setattr__(self, "qualifiers", json.loads(canonical(self.qualifiers)))
        if self.evidence_id in self.supersedes:
            raise ContractError("事实不能覆盖自身")


@dataclass(frozen=True)
class FactSnapshot:
    snapshot_id: str
    as_of: str
    evidence: tuple[Evidence, ...]

    def __post_init__(self):
        identifier(self.snapshot_id, "snapshot_id")
        cutoff = parse_time(self.as_of)
        ids = [x.evidence_id for x in self.evidence]
        if len(ids) != len(set(ids)):
            raise ContractError("重复 evidence_id")
        if any(parse_time(x.observed_at) > cutoff for x in self.evidence):
            raise ContractError("快照包含截止时间之后的事实")

    @property
    def content_hash(self) -> str:
        return fingerprint(self)


@dataclass(frozen=True)
class DraftRequest:
    request_id: str
    tenant_id: str
    project_id: str
    doc_type: str
    project_type: str
    stage: str
    mode: str = "draft"  # submit means preparing an approval draft, never actually submitting
    template_id: str | None = None
    template_revision: str | None = None
    instructions: str = ""

    def __post_init__(self):
        for key in ["request_id", "tenant_id", "project_id", "doc_type"]:
            identifier(getattr(self, key), key)
        if self.mode not in {"draft", "submit"} or not self.project_type or not self.stage:
            raise ContractError("无效撰写用途")
        if self.template_revision and not self.template_id:
            raise ContractError("指定模板版本时必须指定模板身份")
        if len(self.instructions) > 10000:
            raise ContractError("补充要求过长")


@dataclass(frozen=True)
class Issue:
    code: str
    severity: str  # error blocks readiness; warning leaves visible limitation
    message: str
    field_id: str = ""
    evidence_ids: tuple[str, ...] = ()

    def __post_init__(self):
        if self.severity not in {"error", "warning"}:
            raise ContractError("无效检查等级")


@dataclass(frozen=True)
class FieldResolution:
    field_id: str
    evidence: tuple[Evidence, ...]
    issues: tuple[Issue, ...] = ()
    missing_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class DraftField:
    field_id: str
    text: str
    evidence_ids: tuple[str, ...]
    status: str = "filled"  # filled, missing, conflict, not_applicable
    origin: str = "extractive"  # extractive, model, human


@dataclass(frozen=True)
class DraftDocument:
    document_id: str
    tenant_id: str
    project_id: str
    doc_type: str
    title: str
    template_id: str
    template_revision: str
    template_hash: str
    rule_version: str
    snapshot_id: str
    snapshot_hash: str
    as_of: str
    revision: int
    sections: tuple[SectionSpec, ...]
    fields: tuple[DraftField, ...]
    issues: tuple[Issue, ...]
    questions: tuple[str, ...]
    state: str  # draft_complete, draft_with_gaps, ready_for_review; never approved
    created_at: str
    human_modified_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class SemanticReview:
    basis_hash: str
    reviewed_field_ids: tuple[str, ...]
    issues: tuple[Issue, ...]
    reviewer: str


def profile_from_dict(data: Json) -> TemplateProfile:
    require_keys(data, {f.name for f in dataclasses.fields(TemplateProfile)},
                 {"tenant_id", "template_id", "revision", "rule_version", "doc_type", "title", "project_types", "stages", "sections", "fields", "assets"})
    values = dict(data)
    for key, cls in [("sections", SectionSpec), ("assets", SourceAsset)]:
        values[key] = tuple(cls(**item) for item in values.get(key, []))
    fields = []
    for item in values.get("fields", []):
        rule = dict(item)
        for key in ["fact_keys", "requirement_source_ids", "accepted_states", "semantics"]:
            if key in rule:
                rule[key] = text_tuple(rule[key], key)
        for key in ["required_when", "applies_when"]:
            if key in rule:
                if not isinstance(rule[key], dict):
                    raise ContractError(f"{key} 必须为条件对象")
                rule[key] = {k: text_tuple(v, k, nonempty=True) for k, v in rule[key].items()}
        fields.append(FieldRule(**rule))
    values["fields"] = tuple(fields)
    for key in ["project_types", "stages", "allowed_actor_ids"]:
        if key in values:
            values[key] = text_tuple(values[key], key)
    try:
        return TemplateProfile(**values)
    except TypeError as exc:
        raise ContractError(f"模板结构不匹配: {exc}") from exc


def snapshot_from_dict(data: Json) -> FactSnapshot:
    require_keys(data, {"snapshot_id", "as_of", "evidence"}, {"snapshot_id", "as_of", "evidence"})
    entries = []
    for item in data["evidence"]:
        values = dict(item)
        for key in ["allowed_actor_ids", "supersedes"]:
            if key in values:
                values[key] = text_tuple(values[key], key)
        entries.append(Evidence(**values))
    return FactSnapshot(data["snapshot_id"], data["as_of"], tuple(entries))


def document_from_dict(data: Json) -> DraftDocument:
    values = dict(data)
    values["sections"] = tuple(SectionSpec(**v) for v in values["sections"])
    values["fields"] = tuple(DraftField(**{**v, "evidence_ids": tuple(v["evidence_ids"])}) for v in values["fields"])
    values["issues"] = tuple(Issue(**{**v, "evidence_ids": tuple(v.get("evidence_ids", []))}) for v in values["issues"])
    values["questions"] = tuple(values["questions"])
    values["human_modified_fields"] = tuple(values.get("human_modified_fields", []))
    return DraftDocument(**values)
