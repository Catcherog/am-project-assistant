"""Scope projection, explicit supersession, and field-level fact resolution."""
from __future__ import annotations

from collections import defaultdict

from .models import (AccessContext, ContractError, DraftRequest, Evidence, FactSnapshot,
                     FieldResolution, Issue, TemplateProfile, canonical)


def scoped_snapshot(snapshot: FactSnapshot, request: DraftRequest, access: AccessContext) -> FactSnapshot:
    access.require(request.tenant_id, request.project_id)
    visible = tuple(e for e in snapshot.evidence
                    if e.tenant_id == request.tenant_id and e.project_id == request.project_id
                    and (access.actor_id in e.allowed_actor_ids or "*" in e.allowed_actor_ids))
    return FactSnapshot(snapshot.snapshot_id, snapshot.as_of, visible)


def _live_facts(snapshot: FactSnapshot) -> tuple[Evidence, ...]:
    facts = {e.evidence_id: e for e in snapshot.evidence
             if e.role == "project_fact" and e.source_read_state != "unavailable"}
    graph: dict[str, tuple[str, ...]] = {}
    for eid, entry in facts.items():
        if entry.supersedes and entry.state not in {"confirmed", "retracted"}:
            # A report or suggestion cannot revoke an already confirmed baseline.
            raise ContractError("只有明确确认或撤回的事实可以声明覆盖旧事实")
        for target in entry.supersedes:
            if target not in facts:
                continue  # old historical source may be outside this materialized snapshot
            old = facts[target]
            if (entry.key, entry.semantic) != (old.key, old.semantic):
                raise ContractError("事实覆盖必须保持同一字段及同一计划/预测/实际语义")
        graph[eid] = tuple(x for x in entry.supersedes if x in facts)
    visiting, visited = set(), set()

    def visit(eid):
        if eid in visiting:
            raise ContractError("事实覆盖关系存在循环")
        if eid in visited:
            return
        visiting.add(eid)
        for target in graph[eid]:
            visit(target)
        visiting.remove(eid)
        visited.add(eid)
    for eid in graph:
        visit(eid)
    superseded = {target for targets in graph.values() for target in targets}
    return tuple(e for eid, e in facts.items() if eid not in superseded and e.state != "retracted")


def resolve_fields(profile: TemplateProfile, request: DraftRequest, snapshot: FactSnapshot,
                   access: AccessContext) -> tuple[FieldResolution, ...]:
    safe = scoped_snapshot(snapshot, request, access)
    live = _live_facts(safe)
    resolutions = []
    for rule in profile.fields:
        if not rule.applies(request):
            resolutions.append(FieldResolution(rule.field_id, ()))
            continue
        selected = [e for e in live if e.key in rule.fact_keys
                    and e.semantic in rule.semantics and e.state in rule.accepted_states
                    and e.value is not None and e.value != ""]
        issues = []
        grouped = defaultdict(list)
        for entry in selected:
            grouped[(entry.key, entry.semantic)].append(entry)
            if entry.source_read_state == "partial":
                issues.append(Issue("source_partial", "warning", "仅使用已读到的原文片段，不能据此推定未出现的信息不存在",
                                    rule.field_id, (entry.evidence_id,)))
        for (key, semantic), group in grouped.items():
            if len({canonical(e.value) for e in group}) > 1:
                issues.append(Issue("fact_conflict", "error", f"{rule.label} 的 {key}/{semantic} 存在未解决的事实冲突",
                                    rule.field_id, tuple(e.evidence_id for e in group)))
        found = {e.key for e in selected}
        missing = tuple(k for k in rule.fact_keys if k not in found)
        if missing:
            issues.append(Issue("missing_required" if rule.is_required(request) else "missing_optional",
                                "error" if rule.is_required(request) else "warning",
                                f"{rule.label} 缺少符合状态与语义要求的依据: {', '.join(missing)}", rule.field_id))
        resolutions.append(FieldResolution(rule.field_id, tuple(selected), tuple(issues), missing))
    return tuple(resolutions)

