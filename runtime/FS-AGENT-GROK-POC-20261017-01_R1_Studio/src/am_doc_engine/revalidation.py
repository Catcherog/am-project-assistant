"""Bind a merged draft to a real combined fact snapshot before preparing a write plan."""
from __future__ import annotations

from dataclasses import replace
from typing import Callable

from .catalog import TemplateCatalog
from .engine import review_basis
from .evidence import resolve_fields, scoped_snapshot
from .models import (AccessContext, ContractError, DraftDocument, DraftRequest, FactSnapshot,
                     Issue, RevisionConflict, SemanticReview, TemplateProfile, now_iso)
from .revision import RevisionPlan
from .validation import dedupe_issues, validate_document


def revalidate_revision(plan: RevisionPlan, profile: TemplateProfile, request: DraftRequest,
                        access: AccessContext, snapshot: FactSnapshot, *,
                        reviewer: Callable[..., SemanticReview] | None = None,
                        ensure_current: Callable[[], object] | None = None) -> DraftDocument:
    def guard():
        if ensure_current is not None and ensure_current() is False:
            raise RevisionConflict("宿主当前版本已改变")
    guard()
    if plan.conflicts:
        raise RevisionConflict("修改计划仍有未解决的人工或并发冲突")
    document = plan.merged
    if (document.tenant_id, document.project_id, document.doc_type) != (request.tenant_id, request.project_id, request.doc_type):
        raise RevisionConflict("修改稿与复核请求不属于同一业务对象")
    selection = TemplateCatalog([profile]).select(request, access, as_of=now_iso())
    if (document.template_hash, document.rule_version, document.sections) != (profile.content_hash, profile.rule_version, profile.sections):
        raise RevisionConflict("修改稿尚未完成与目标模板的绑定")
    safe = scoped_snapshot(snapshot, request, access)
    resolutions = resolve_fields(profile, request, safe, access)
    # Recompute rather than deleting blockers and retaining an old 'pass'.
    issues = list(selection.issues) + list(validate_document(profile, request, document.fields, resolutions))
    model_ids = {f.field_id for f in document.fields if f.origin in {"model", "human"} and f.status == "filled"}
    if model_ids and reviewer is not None:
        guard()
        review = reviewer(profile, request, document.fields, resolutions)
        if not isinstance(review, SemanticReview) or review.basis_hash != review_basis(profile, request, document.fields, resolutions) or set(review.reviewed_field_ids) != model_ids:
            raise ContractError("修订复核没有覆盖当前内容与来源版本")
        issues = [i for i in issues if not (i.code == "semantic_review_required" and i.field_id in model_ids)]
        issues.extend(review.issues)
    issues = list(dedupe_issues(issues))
    pending = any(i.severity == "error" or i.code == "semantic_review_required" for i in issues)
    state = "draft_with_gaps" if pending else "ready_for_review" if request.mode == "submit" else "draft_complete"
    questions = tuple(i.message for i in issues if i.severity == "error")[:3]
    result = replace(document, snapshot_id=safe.snapshot_id, snapshot_hash=safe.content_hash,
                     as_of=safe.as_of, issues=tuple(issues), questions=questions, state=state)
    guard()
    return result
