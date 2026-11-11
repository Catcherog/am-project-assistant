"""Controlled selection from a current ledger export; never chooses 'latest upload'."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .models import (AccessContext, DraftRequest, Issue, RevisionConflict,
                     SelectionError, TemplateProfile, parse_time)


@dataclass(frozen=True)
class TemplateSelection:
    profile: TemplateProfile
    issues: tuple[Issue, ...] = ()


class TemplateCatalog:
    def __init__(self, profiles: Iterable[TemplateProfile]):
        self._profiles: dict[tuple[str, str, str], TemplateProfile] = {}
        for profile in profiles:
            key = (profile.tenant_id, profile.template_id, profile.revision)
            previous = self._profiles.get(key)
            if previous and previous.content_hash != profile.content_hash:
                raise RevisionConflict("同一个模板身份与技术版本出现不同内容")
            self._profiles[key] = profile

    def select(self, request: DraftRequest, access: AccessContext, *, as_of: str) -> TemplateSelection:
        access.require(request.tenant_id, request.project_id)
        cutoff = parse_time(as_of)
        candidates = []
        for profile in self._profiles.values():
            if profile.tenant_id != request.tenant_id or profile.doc_type != request.doc_type:
                continue
            if access.actor_id not in profile.allowed_actor_ids and "*" not in profile.allowed_actor_ids:
                continue
            if profile.status != "effective" or parse_time(profile.effective_at) > cutoff:
                continue
            if request.project_type not in profile.project_types and "*" not in profile.project_types:
                continue
            if request.stage not in profile.stages and "*" not in profile.stages:
                continue
            if request.template_id and request.template_id != profile.template_id:
                continue
            if request.template_revision and request.template_revision != profile.revision:
                continue
            candidates.append(profile)
        if not candidates:
            raise SelectionError("没有匹配且当前有效的可访问模板；请检查台账映射和模板生效状态")
        if len(candidates) > 1:
            labels = ", ".join(f"{x.template_id}@{x.revision}" for x in candidates)
            raise SelectionError(f"模板匹配有歧义，需要明确选择: {labels}")
        profile = candidates[0]
        issues = []
        for source in profile.assets:
            if source.role in {"standard", "template", "instruction"}:
                if source.read_state != "complete" or not source.content.strip():
                    raise SelectionError(f"必需资料未完整读入: {source.source_id} ({source.read_state})")
            elif source.read_state != "complete":
                issues.append(Issue("reference_partial", "warning", f"参考资料 {source.source_id} 未完整读取，不作为必需依据"))
        return TemplateSelection(profile, tuple(issues))

