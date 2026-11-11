"""Integration seam for the existing AM LogicService, without importing or replacing it.

The host supplies trusted identities, template exports, facts and cancellation guards.
This module neither retrieves credentials nor writes a remote document or ledger.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from .adapters import EvidenceBoundModelComposer, ProviderSession
from .catalog import TemplateCatalog
from .engine import DocumentEngine
from .evidence import scoped_snapshot
from .models import (AccessContext, ContractError, DraftDocument, DraftRequest, FactSnapshot,
                     RevisionConflict, SourceAsset, TemplateProfile)
from .rendering import build_report, render_markdown
from .review import EvidenceSemanticReviewer
from .storage import SQLiteRunStore


@dataclass(frozen=True)
class BridgeResult:
    document: DraftDocument
    content: str
    reply_text: str
    model: str
    elapsed: float
    report: dict[str, Any]


class _MeteredProvider:
    def __init__(self, original):
        self.original = original
        self.models = []
        self.elapsed = 0.0

    def complete(self, prompt, **kwargs):
        result = self.original.complete(prompt, **kwargs)
        model = getattr(result, "model", "host-provider")
        if model not in self.models:
            self.models.append(model)
        self.elapsed += float(getattr(result, "elapsed", 0.0))
        return result


def prepare_am_document(*, task: dict[str, Any], request: DraftRequest,
                        access: AccessContext, profiles: tuple[TemplateProfile, ...],
                        snapshot: FactSnapshot, store: SQLiteRunStore, provider: Any,
                        ensure_current: Callable[[], object],
                        deadline_monotonic: float, on_start: Callable | None = None,
                        use_model: bool = True,max_tokens: int = 4096,
                        review_reserve_seconds: float = 0) -> BridgeResult:
    """Call from document branch; preserve original precheck/adopt/mount paths.

Pass the existing job's deadline, NOT a new 120-second budget on every retry.
The host must call its original late-result guard again while committing the result.
"""
    if not isinstance(task.get("run_id"), str):
        raise ContractError("缺少既有 AM task.run_id")
    def guard():
        if ensure_current() is False:
            raise RevisionConflict("原 AM 草稿已取消、过期或发生版本变化")
        access.require(request.tenant_id, request.project_id)
    guard()
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("宿主任务原有期限已耗尽")
    metered = _MeteredProvider(provider)
    session = ProviderSession(metered, run_id=task["run_id"], timeout=min(120, remaining),
                              deadline_monotonic=deadline_monotonic, on_start=on_start,
                              ensure_current=guard, max_tokens=max_tokens)
    if not 0<=review_reserve_seconds<=35:raise ContractError('核对保留时间无效')
    composition_session=ProviderSession(metered,run_id=task['run_id'],timeout=min(120,remaining),
        deadline_monotonic=deadline_monotonic-review_reserve_seconds,on_start=on_start,
        ensure_current=guard,max_tokens=max_tokens) if review_reserve_seconds else session
    engine = DocumentEngine(TemplateCatalog(profiles), store,
                            composer=EvidenceBoundModelComposer(session=composition_session) if use_model else None,
                            reviewer=EvidenceSemanticReviewer(session) if use_model else None,
                            ensure_current=guard)
    document = engine.generate(request, access, snapshot)
    profile = next(p for p in profiles if p.content_hash == document.template_hash)
    content = render_markdown(document, profile)
    if document.state == "draft_with_gaps":
        pending = document.questions or tuple(i.message for i in document.issues if i.severity == "error" or i.code == "semantic_review_required")[:3]
        reply = "草稿已整理，现有内容已保留。\n" + "\n".join(pending)
    elif document.state == "ready_for_review":
        reply = "已按适用模板形成待审稿，内容与填写要求已完成本次核对。"
    else:
        reply = "已按适用模板形成草稿，可继续补充或指定修改段落。"
    guard()
    return BridgeResult(document, content, reply, "/".join(metered.models) or "deterministic",
                        metered.elapsed, build_report(document, profile, scoped_snapshot(snapshot, request, access)))


def source_asset_from_legacy(source: dict[str, Any], *, role: str, source_uri: str) -> SourceAsset:
    """Require explicit source role; old provenance is NOT a standard/template/fact role."""
    status = {"complete": "complete", "partial": "partial", "failed": "unavailable"}.get(source.get("read_status"))
    if status is None:
        raise ContractError("旧资料读取状态未知，不能推定完整")
    segments = source.get("segments", [])
    if not isinstance(segments, list) or any(not isinstance(s, dict) or not isinstance(s.get("original"), str) for s in segments):
        raise ContractError("旧资料片段结构不匹配")
    content = "\n".join(s["original"] for s in segments)
    if not content.strip():
        status = "unavailable"
    return SourceAsset(source_id=source["id"], role=role, uri=source_uri,
                       revision=source.get("version") or "unknown", content=content,
                       read_state=status, title=source.get("name", ""))
