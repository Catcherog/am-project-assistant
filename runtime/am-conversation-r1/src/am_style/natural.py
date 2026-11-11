"""Opt-in surface grammar for exact typed propositions, not an LLM judge.

This extension never erases words from model output. It enumerates finite
equivalent surfaces from host-verified typed values and effect receipts, then
matches complete non-overlapping obligations. Unknown language stays unknown.
Historical frames, gold reviews and their hashes are never rewritten.
"""
from __future__ import annotations

import re

from .contracts import ContractError, SpeechFrame
from .render import fact_unit, effect_unit
from .verify import Verdict, _normalize


SURFACE_VERSION = "natural-typed-surfaces/4.0.1"


def _fact_surfaces(fact):
    original = fact_unit(fact)
    kind = fact.get("kind", "statement")
    subject, value = str(fact.get("subject", "")).strip(), str(fact.get("value", "")).strip()
    texts = []
    if kind == "owner":
        texts = [f"{subject}的负责人为{value}。", f"负责{subject}的是{value}。"]
    elif kind == "date":
        texts = [f"{subject}的时间为{value}。"]
    # No generic replacements in statement values: they may contain quoted
    # names, negation, dates, status qualifications or comparison operators.
    modality = fact.get("modality", "recorded")
    if modality == "estimated":
        texts = ["预计" + t for t in texts]
    elif modality == "unverified":
        texts = [t.rstrip("。") + "，这点还没有确认。" for t in texts]
    return original, texts


def _effect_surfaces(effect):
    # Validate receipts, retry jobs, cancellation and schedules with the same
    # original host contract before allowing any new surface.
    original = effect_unit(effect)
    obj, kind, state = effect["object"], effect["kind"], effect["state"]
    texts = []
    if state == "succeeded":
        patterns = {
            "send_message": ["{o}已经发出去了。", "{o}已经发送了。"],
            "create_followup": ["{o}的跟进已经建好了。", "已经为{o}建立了跟进。"],
            "save_note": ["{o}已经记下了。"],
            "record_knowledge": ["{o}已经保存了。"],
            "generate_document": ["{o}已经生成了。"],
            "publish_document": ["{o}已经发布了。"],
            "mount_document": ["{o}已经挂载了。"],
            "cancel": ["{o}已经取消了。"],
            "update": ["{o}已经更新了。"],
        }
        texts = [p.format(o=obj) for p in patterns.get(kind, [])]
        if kind == "schedule_reminder":
            texts = [f"{obj}的提醒已经设好了，时间是{effect['schedule_text']}。"]
    elif state == "cancelled":
        texts = [f"{obj}后续处理已停止。", f"{obj}的后续处理已经停止了。"]
    return original, texts


def surface_choices(frame):
    """Create alternatives only for a typed value matching the frozen unit.

    A frame can contain provenance and separately authored surfaces. Require
    every reconstructed original variant to be present in that unit before
    treating provenance as the authority for additional expressions.
    """
    d = frame.data
    choices = [(u, list(u["variants"])) for u in d["units"]]
    by_id = {u["id"]: (u, variants) for u, variants in choices}
    provenance = d.get("provenance", {})
    for records, builder in ((provenance.get("facts", []), _fact_surfaces),
                             (provenance.get("effects", []), _effect_surfaces)):
        if not isinstance(records, list):
            raise ContractError("invalid_surface_provenance")
        for record in records:
            old, extra = builder(record)
            item = by_id.get(old["id"])
            if item is None:
                continue
            unit, variants = item
            if unit["authority"] != "typed_host" or unit["covers"] != old["covers"]:
                continue
            if not set(old["variants"]) <= set(unit["variants"]):
                continue
            variants.extend(t for t in extra if t not in variants)
    return choices


def with_natural_surfaces(frame):
    """New prompt experiment only; does not mutate/relabel an old frozen frame.

    The caller must generate a new run with the returned frame and bind its new
    hash. Old model outputs and human approvals remain bound to the old frame.
    """
    d = frame.data
    expanded = {u["id"]: variants for u, variants in surface_choices(frame)}
    for unit in d["units"]:
        if unit["authority"] != "exact_preview":
            unit["variants"] = expanded[unit["id"]][:12]
    d["surface_profile"] = SURFACE_VERSION
    d["parent_frame_sha256"] = frame.fingerprint
    return SpeechFrame.freeze(d)


class NaturalSurfaceVerifier:
    """Additional evidence grammar; opt in with CompositeVerifier.

    It can join complete sentences with punctuation or additive transitions.
    It never inserts causal/contrastive claims, resolves pronouns, fills a
    missing subject, drops modality or infers that a fact is current.
    """
    version = SURFACE_VERSION

    def verify(self, frame, text, policy):
        if not isinstance(text, str) or not text.strip() or len(text) > policy.max_chars:
            return Verdict("reject", "output_budget", (), self.version)
        d, normalized = frame.data, _normalize(text)
        for u in d["units"]:
            if u["authority"] == "exact_preview" and set(u["covers"]) & set(d["obligations"]):
                if not all(v in text for v in u["variants"]):
                    return Verdict("reject", "protected_text_changed", (), self.version)
        try:
            units = surface_choices(frame)
        except (ContractError, KeyError, TypeError, ValueError):
            return Verdict("indeterminate", "unusable_typed_provenance", (), self.version)
        choices = []
        for u, variants in units:
            for value in variants:
                surface = _normalize(value)
                ids = frozenset(u["covers"])
                exact = u["authority"] == "exact_preview"
                choices.append((surface, ids))
                if not exact and surface.endswith("。"):  # replace only the terminal sentence separator
                    choices.append((surface[:-1], ids))
        obligations = frozenset(d["obligations"])
        states, visited = {(0, frozenset())}, set()
        while states:
            pos, covered = states.pop()
            if (pos, covered) in visited:
                continue
            visited.add((pos, covered))
            if len(visited) > 4096:
                return Verdict("indeterminate", "grammar_state_budget", (), self.version)
            if pos == len(normalized) and obligations <= covered:
                return Verdict("pass", "typed_surface_composition", tuple(sorted(covered)), self.version)
            starts = {pos}
            marker = re.match(r"(?:[-*•]\s+|[1-9][0-9]?[.)、]\s+)", normalized[pos:])
            if marker:
                starts.add(pos + marker.end())
            if covered:
                boundary = re.match(r"[。，；;]?(?:另外[，,]|此外[，,])?", normalized[pos:])
                if boundary and boundary.end():
                    starts.add(pos + boundary.end())
            for start in starts:
                for surface, ids in choices:
                    if surface and not covered & ids and normalized.startswith(surface, start):
                        states.add((start + len(surface), covered | ids))
        return Verdict("indeterminate", "unrecognized_or_incomplete_semantics", (), self.version)
