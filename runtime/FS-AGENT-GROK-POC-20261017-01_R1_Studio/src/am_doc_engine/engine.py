"""Deterministic orchestration around optional model calls.

Draft creation has no remote side effects. `submit` prepares a reviewable draft;
publication and business approval stay with authenticated AM host actions.
"""
from __future__ import annotations

from typing import Callable, Protocol

from .catalog import TemplateCatalog
from .evidence import resolve_fields, scoped_snapshot
from .models import (AccessContext, AccessDenied, ContractError, DraftDocument, DraftField,
                     DraftRequest, FactSnapshot, FieldResolution, FieldRule, Issue,
                     RevisionConflict, SemanticReview, TemplateProfile, fingerprint, now_iso)
from .storage import SQLiteRunStore


class Composer(Protocol):
    def compose(self, rule: FieldRule, resolution: FieldResolution,
                request: DraftRequest, profile: TemplateProfile) -> DraftField: ...


def review_basis(profile: TemplateProfile, request: DraftRequest,
                 fields: tuple[DraftField, ...], resolutions: tuple[FieldResolution, ...]) -> str:
    return fingerprint({"template_hash": profile.content_hash, "request": request,
                        "fields": fields, "resolutions": resolutions})


def _unique_issues(issues):
    seen = set()
    out = []
    for item in issues:
        key = fingerprint(item)
        if key not in seen:
            seen.add(key)
            out.append(item)
    return tuple(out)


class DocumentEngine:
    def __init__(self, catalog: TemplateCatalog, store: SQLiteRunStore, *,
                 composer: Composer | None = None,
                 reviewer: Callable[..., SemanticReview] | None = None,
                 ensure_current: Callable[[], None] | None = None):
        from .adapters import ExtractiveComposer
        self.catalog = catalog
        self.store = store
        self.composer = composer or ExtractiveComposer()
        self.reviewer = reviewer
        self.ensure_current = ensure_current or (lambda: None)

    def _guard(self):
        if self.ensure_current() is False:
            raise RevisionConflict("宿主作业已取消、过期或版本改变，拒绝迟到结果")

    def generate(self, request: DraftRequest, access: AccessContext,
                 snapshot: FactSnapshot) -> DraftDocument:
        from .adapters import ExtractiveComposer
        from .validation import validate_document
        self._guard()
        access.require(request.tenant_id, request.project_id)
        safe = scoped_snapshot(snapshot, request, access)
        selection = self.catalog.select(request, access, as_of=now_iso())
        profile = selection.profile
        resolutions = resolve_fields(profile, request, safe, access)
        identity = {"tenant": access.tenant_id, "actor": access.actor_id,
                    "app": access.app_id, "chat": access.chat_id,
                    "project": request.project_id, "request_id": request.request_id}
        run_key = fingerprint(identity)
        input_hash = fingerprint({"request": request, "access": access,
                                  "template": profile.content_hash, "snapshot": safe.content_hash,
                                  "composer": type(self.composer).__qualname__,
                                  "reviewer": type(self.reviewer).__qualname__ if self.reviewer else None,
                                  "engine_contract": "1.0"})
        lease = self.store.claim(run_key, input_hash)
        if lease.cached:
            self._guard()
            return lease.cached
        try:
            self.store.pin_template(profile)
            self.store.pin_snapshot(run_key, safe)
            issues = list(selection.issues)
            fields = []
            by_id = {r.field_id: r for r in resolutions}
            extractive = ExtractiveComposer()
            batch_output = {}
            batch_error = None
            batch_pairs = [(rule, by_id[rule.field_id]) for rule in profile.fields
                           if rule.mode == "narrative" and rule.applies(request)
                           and not by_id[rule.field_id].missing_keys
                           and not any(i.severity == "error" for i in by_id[rule.field_id].issues)]
            batch_method = getattr(self.composer, "compose_many", None)
            if batch_pairs and callable(batch_method):
                self._guard()
                try:
                    output = tuple(batch_method(batch_pairs, request, profile))
                    expected_ids = {r.field_id for r, _ in batch_pairs}
                    if any(not isinstance(item, DraftField) for item in output) or len(output) != len(expected_ids) or {item.field_id for item in output} != expected_ids:
                        raise ContractError("批量生成没有准确覆盖指定字段")
                    batch_output = {item.field_id: item for item in output}
                except (AccessDenied, RevisionConflict):
                    raise
                except (ContractError, TimeoutError, RuntimeError) as exc:
                    batch_error = type(exc).__name__
                self._guard()
            for rule in profile.fields:
                self._guard()
                resolution = by_id[rule.field_id]
                issues.extend(resolution.issues)
                if not rule.applies(request):
                    fields.append(DraftField(rule.field_id, "不适用", (), "not_applicable"))
                    continue
                if any(i.code == "fact_conflict" for i in resolution.issues):
                    fields.append(DraftField(rule.field_id, "待核对：本项存在尚未解决的事实冲突。",
                                             tuple(e.evidence_id for e in resolution.evidence), "conflict"))
                    continue
                composer = self.composer if rule.mode == "narrative" else extractive
                try:
                    if rule.field_id in batch_output:
                        item = batch_output[rule.field_id]
                    elif rule.mode == "narrative" and batch_error is not None and any(rule.field_id == r.field_id for r, _ in batch_pairs):
                        raise ContractError(f"批量生成失败: {batch_error}")
                    else:
                        item = composer.compose(rule, resolution, request, profile)
                    if not isinstance(item, DraftField):
                        raise ContractError("生成器必须返回 DraftField")
                except (AccessDenied, RevisionConflict):
                    raise
                except (ContractError, TimeoutError, RuntimeError) as exc:
                    # Preserve useful, sourced content, but report the failed generation.
                    item = extractive.compose(rule, resolution, request, profile)
                    issues.append(Issue("generation_failed", "error", f"{rule.label} 生成未完成，已保留有据内容；错误类型 {type(exc).__name__}", rule.field_id))
                fields.append(item)
                self._guard()
            fields_tuple = tuple(fields)
            issues.extend(validate_document(profile, request, fields_tuple, resolutions))
            narrative_ids = {f.field_id for f in fields if f.origin == "model" and f.status == "filled"}
            if narrative_ids and self.reviewer is not None:
                self._guard()
                try:
                    review = self.reviewer(profile, request, fields_tuple, resolutions)
                    if not isinstance(review, SemanticReview) or review.basis_hash != review_basis(profile, request, fields_tuple, resolutions):
                        raise ContractError("语义检查没有绑定当前内容与依据版本")
                    if set(review.reviewed_field_ids) != narrative_ids:
                        raise ContractError("语义检查没有完整覆盖本次模型生成字段")
                    issues = [i for i in issues if not (i.code == "semantic_review_required" and i.field_id in narrative_ids)]
                    issues.extend(review.issues)
                except (AccessDenied, RevisionConflict):
                    raise
                except (ContractError, TimeoutError, RuntimeError) as exc:
                    issues.append(Issue("semantic_review_failed", "error", f"语义核对未完成: {type(exc).__name__}"))
                self._guard()
            issues_tuple = _unique_issues(issues)
            questions = []
            priority = sorted(profile.fields, key=lambda r: (not r.critical, not r.is_required(request)))
            for rule in priority:
                relevant = [i for i in issues_tuple if i.field_id == rule.field_id and i.severity == "error"]
                if relevant:
                    questions.append(f"请核对“{rule.label}”：{relevant[0].message}")
                if len(questions) == 3:
                    break
            incomplete = any(i.severity == "error" or i.code == "semantic_review_required" for i in issues_tuple)
            state = "draft_with_gaps" if incomplete else "ready_for_review" if request.mode == "submit" else "draft_complete"
            document = DraftDocument(
                document_id="doc-" + run_key[:24], tenant_id=request.tenant_id,
                project_id=request.project_id, doc_type=request.doc_type,
                title=f"{request.project_id}｜{profile.title}",
                template_id=profile.template_id, template_revision=profile.revision,
                template_hash=profile.content_hash, rule_version=profile.rule_version,
                snapshot_id=safe.snapshot_id, snapshot_hash=safe.content_hash, as_of=safe.as_of,
                revision=1, sections=profile.sections, fields=fields_tuple,
                issues=issues_tuple, questions=tuple(questions), state=state, created_at=now_iso())
            self._guard()
            self.store.complete(lease, document)
            return document
        except BaseException as exc:
            self.store.fail(lease, type(exc).__name__)
            raise

