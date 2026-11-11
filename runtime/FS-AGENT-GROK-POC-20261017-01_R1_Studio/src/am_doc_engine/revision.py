"""Pure three-way revision plans; preserving human work precedes regeneration.

Plans are reviewable values, not writes. A host must apply its own live revision guard,
assemble a combined evidence snapshot and revalidate before persisting/publishing a
changed document. This module never promotes a document to approved or published.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable

from .models import DraftDocument, DraftField, Issue, RevisionConflict, TemplateProfile
from .validation import dedupe_issues


@dataclass(frozen=True)
class FieldChange:
    field_id: str
    before: DraftField | None
    after: DraftField | None
    reason: str


@dataclass(frozen=True)
class MergeConflict:
    field_id: str
    code: str
    message: str
    base: DraftField | None = None
    current: DraftField | None = None
    proposed: DraftField | None = None


@dataclass(frozen=True)
class RevisionPlan:
    merged: DraftDocument
    changes: tuple[FieldChange, ...]
    conflicts: tuple[MergeConflict, ...]
    affected_field_ids: tuple[str, ...]
    expected_current_revision: int
    proposed_snapshot_id: str
    proposed_snapshot_hash: str
    requires_validation: bool
    ignored_field_ids: tuple[str, ...] = ()
    migration_ref: str = ""


def _document_identity(doc: DraftDocument):
    return doc.document_id, doc.tenant_id, doc.project_id, doc.doc_type


def _template_identity(doc: DraftDocument):
    return doc.template_id, doc.template_revision, doc.template_hash, doc.rule_version


def _fields(doc: DraftDocument) -> dict[str, DraftField]:
    result = {item.field_id: item for item in doc.fields}
    if len(result) != len(doc.fields):
        raise RevisionConflict("文档含重复字段身份")
    return result


def _check_revisions(base: DraftDocument, current: DraftDocument, proposed: DraftDocument, expected: int) -> None:
    if any(doc.state not in {"draft_complete", "draft_with_gaps", "ready_for_review"} for doc in [base, current, proposed]):
        raise RevisionConflict("本接口只处理草稿；已提交或已批准成果需先建立独立修订稿")
    if any(type(doc.revision) is not int or doc.revision < 1 for doc in [base, current, proposed]):
        raise RevisionConflict("文档修订号必须为正整数")
    if current.revision != expected:
        raise RevisionConflict("当前文档版本已变化，请重新读取后生成修改计划")
    if base.revision > current.revision or proposed.revision != base.revision + 1:
        raise RevisionConflict("候选修订必须显式基于 base.revision + 1，且基线不得新于当前稿")
    if len({_document_identity(doc) for doc in [base, current, proposed]}) != 1:
        raise RevisionConflict("三份文档的文档、租户、项目或类型身份不一致")


def _merge_field(
    field_id: str, base: DraftField, current: DraftField, proposed: DraftField,
    human_protected: bool,
) -> tuple[DraftField, MergeConflict | None]:
    if proposed == current or proposed == base:
        return current, None
    if human_protected:
        return current, MergeConflict(field_id, "human_content_protected", "该字段有人工修改，已保留当前内容", base, current, proposed)
    if current != base:
        return current, MergeConflict(field_id, "concurrent_field_change", "当前稿与候选稿同时改变了该字段，已保留当前内容", base, current, proposed)
    return proposed, None


def _review_issues(conflicts: Iterable[MergeConflict], changed: bool) -> tuple[Issue, ...]:
    result = [Issue(item.code, "error", item.message, item.field_id) for item in conflicts]
    if changed:
        result.append(Issue("revision_requires_revalidation", "error", "修改计划需绑定合并后的事实快照并重新检查，才能作为完整草稿交付"))
    return tuple(result)


def plan_revision(
    base: DraftDocument,
    current: DraftDocument,
    proposed: DraftDocument,
    requested_field_ids: Iterable[str],
    *,
    expected_current_revision: int,
) -> RevisionPlan:
    """Merge only requested fields. Changed candidates always require revalidation.

    ``proposed.revision`` must equal ``base.revision + 1``. This is a candidate revision,
    not an assertion that a remote write took place. Live remote revisions belong to the
    host's publication adapter. An explicit template migration uses the separate API.
    """
    _check_revisions(base, current, proposed, expected_current_revision)
    if len({_template_identity(doc) for doc in [base, current, proposed]}) != 1:
        raise RevisionConflict("模板身份、版本或规则变动需要显式模板迁移")
    if base.sections != current.sections or base.sections != proposed.sections:
        raise RevisionConflict("章节结构变动需要显式模板迁移")
    before, live, candidate = (_fields(doc) for doc in [base, current, proposed])
    if set(before) != set(live) or set(before) != set(candidate):
        raise RevisionConflict("字段增删需要显式模板迁移")
    requested = tuple(dict.fromkeys(requested_field_ids))
    if set(requested) - set(before):
        raise RevisionConflict("修改范围包含不存在的字段")
    output = dict(live)
    conflicts: list[MergeConflict] = []
    changes: list[FieldChange] = []
    for fid in requested:
        result, conflict = _merge_field(
            fid, before[fid], live[fid], candidate[fid],
            fid in current.human_modified_fields or live[fid].origin == "human",
        )
        if conflict:
            conflicts.append(conflict)
        if result != live[fid]:
            output[fid] = result
            changes.append(FieldChange(fid, live[fid], result, "requested_field_update"))
    ignored = tuple(fid for fid in before if fid not in requested and candidate[fid] != before[fid])
    issues = dedupe_issues((*current.issues, *_review_issues(conflicts, bool(changes))))
    mutated = bool(changes or conflicts)
    merged = replace(
        current,
        fields=tuple(output[item.field_id] for item in current.fields),
        issues=issues,
        state="draft_with_gaps" if mutated else current.state,
        revision=current.revision + 1 if mutated else current.revision,
    )
    return RevisionPlan(
        merged, tuple(changes), tuple(conflicts), tuple(requested), expected_current_revision,
        proposed.snapshot_id, proposed.snapshot_hash, bool(changes or conflicts), ignored,
    )


def plan_template_migration(
    base: DraftDocument,
    current: DraftDocument,
    proposed: DraftDocument,
    new_profile: TemplateProfile,
    *,
    expected_current_revision: int,
    migration_ref: str,
    field_map: dict[str, str] | None = None,
    accepted_removed_field_ids: Iterable[str] = (),
) -> RevisionPlan:
    """Explicit, atomic migration plan with a recorded task/decision reference.

    Unmapped fields common to both versions retain their identity. Deleted fields must
    be listed explicitly; removing human text or conflicting concurrent changes blocks
    the whole migration. The original document remains intact in the plan on conflict.
    A successful migration remains a draft requiring a combined snapshot/revalidation.
    """
    _check_revisions(base, current, proposed, expected_current_revision)
    if _template_identity(base) != _template_identity(current) or base.sections != current.sections:
        raise RevisionConflict("当前稿已发生模板迁移，请重新选定基线")
    if not isinstance(migration_ref, str) or not migration_ref.strip():
        raise RevisionConflict("模板迁移需有明确的任务或决定依据")
    if new_profile.status != "effective":
        raise RevisionConflict("只能迁移到有有效状态依据的模板")
    expected_template = (new_profile.template_id, new_profile.revision, new_profile.content_hash, new_profile.rule_version)
    if _template_identity(proposed) != expected_template or proposed.sections != new_profile.sections:
        raise RevisionConflict("候选稿与指定新模板的身份、内容指纹或章节不一致")
    if (new_profile.tenant_id, new_profile.doc_type) != (current.tenant_id, current.doc_type):
        raise RevisionConflict("新模板不属于当前租户或文档类型")
    before, live, candidate = (_fields(doc) for doc in [base, current, proposed])
    if set(before) != set(live):
        raise RevisionConflict("当前稿字段集合变化，请重新读取")
    if set(candidate) != {rule.field_id for rule in new_profile.fields}:
        raise RevisionConflict("候选稿字段集合与新模板不一致")
    mapping = dict(field_map or {})
    if set(mapping) - set(live) or set(mapping.values()) - set(candidate):
        raise RevisionConflict("迁移映射引用了不存在的旧字段或新字段")
    for fid in live:
        if fid in candidate and fid not in mapping and fid not in mapping.values():
            mapping[fid] = fid
    if len(set(mapping.values())) != len(mapping):
        raise RevisionConflict("多个旧字段不能静默合并到一个新字段")
    removed = set(live) - set(mapping)
    accepted = set(accepted_removed_field_ids)
    if accepted - removed:
        raise RevisionConflict("批准删除清单包含未删除的字段")
    output = dict(candidate)
    conflicts: list[MergeConflict] = []
    changes: list[FieldChange] = []
    for fid in sorted(removed):
        protected = fid in current.human_modified_fields or live[fid].origin == "human"
        if fid not in accepted or protected or live[fid] != before[fid]:
            conflicts.append(MergeConflict(fid, "migration_removal_requires_resolution", "删除字段涉及未确认范围、人工内容或并发修改，原稿已保留", before[fid], live[fid], None))
        else:
            changes.append(FieldChange(fid, live[fid], None, "explicit_template_field_removal"))
    for old_id, new_id in mapping.items():
        old_base = replace(before[old_id], field_id=new_id)
        old_current = replace(live[old_id], field_id=new_id)
        result, conflict = _merge_field(new_id, old_base, old_current, candidate[new_id], old_id in current.human_modified_fields or live[old_id].origin == "human")
        output[new_id] = result
        if conflict:
            conflicts.append(conflict)
        if old_id != new_id or result != live[old_id]:
            changes.append(FieldChange(new_id, live[old_id], result, "explicit_template_field_migration"))
    for fid in candidate:
        if fid not in mapping.values():
            changes.append(FieldChange(fid, None, candidate[fid], "new_template_field"))
    affected = tuple(dict.fromkeys([*sorted(removed), *[item.field_id for item in changes], *[item.field_id for item in conflicts]]))
    if conflicts:
        merged = replace(current, issues=dedupe_issues((*current.issues, *_review_issues(conflicts, False))), state="draft_with_gaps")
        return RevisionPlan(merged, (), tuple(conflicts), affected, expected_current_revision, proposed.snapshot_id, proposed.snapshot_hash, True, migration_ref=migration_ref)
    human_fields = tuple(mapping[fid] for fid in current.human_modified_fields if fid in mapping)
    merged = replace(
        proposed,
        fields=tuple(output[item.field_id] for item in proposed.fields),
        revision=current.revision + 1,
        issues=dedupe_issues((*proposed.issues, *_review_issues((), True))),
        human_modified_fields=human_fields,
        state="draft_with_gaps",
    )
    return RevisionPlan(merged, tuple(changes), (), affected, expected_current_revision, proposed.snapshot_id, proposed.snapshot_hash, True, migration_ref=migration_ref)
