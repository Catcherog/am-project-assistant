"""Bounded deterministic checks, with a separately versioned semantic extension.

Unknown language is not called false, but it is not automatically approved.
"""
from dataclasses import dataclass
import json
import re

from .contracts import canonical, digest


@dataclass(frozen=True)
class Verdict:
    decision: str
    reason: str
    covered: tuple = ()
    verifier: str = "bounded-surfaces/1"


def parse_candidate(raw):
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if (type(obj) is not dict or set(obj) != {"text"}
            or type(obj["text"]) is not str or not obj["text"].strip()):
        return None
    return obj["text"].strip()


def _normalize(text):
    # Only layout changes are ignored. Punctuation inside a semantic phrase is retained.
    lines = []
    for line in text.splitlines():
        lines.append(line.strip())
    return "".join(lines)


class BoundedVerifier:
    """Accept compositions of host surfaces with complete, non-duplicated coverage."""
    version = "bounded-surfaces/1"

    def verify(self, frame, text, policy):
        if not text.strip() or len(text) > policy.max_chars:
            return Verdict("reject", "output_budget")
        d = frame.data
        normalized = _normalize(text)
        # Preview bytes are exact; layout normalization must never alter an approval.
        for u in d["units"]:
            if u["authority"] == "exact_preview" and set(u["covers"]) & set(d["obligations"]):
                if not all(v in text for v in u["variants"]):
                    return Verdict("reject", "protected_text_changed")
        choices = []
        for u in d["units"]:
            for v in u["variants"]:
                choices.append((_normalize(v), frozenset(u["covers"])))
        obligations = frozenset(d["obligations"])
        # State bound prevents combinatorial ambiguity from becoming a DoS.
        states = {(0, frozenset())}
        seen = set()
        while states:
            pos, covered = states.pop()
            if (pos, covered) in seen:
                continue
            seen.add((pos, covered))
            if len(seen) > 4096:
                return Verdict("indeterminate", "grammar_state_budget")
            if pos == len(normalized):
                if obligations <= covered:
                    return Verdict("pass", "host_surface_composition", tuple(sorted(covered)))
                continue
            starts = [pos]
            marker = re.match(r"(?:[-*•]\s+|[1-9][0-9]?[.)、]\s+)", normalized[pos:])
            if marker:
                starts.append(pos + marker.end())
            for surface, ids in choices:
                for start in starts:
                    if not (covered & ids) and normalized.startswith(surface, start):
                        states.add((start + len(surface), covered | ids))
        return Verdict("indeterminate", "unrecognized_or_incomplete_semantics")


class ReviewedResponseVerifier:
    """Optional exact-frame human approvals. Does not generalize reviews to new facts."""
    version = "reviewed-response/1"

    def __init__(self, approvals=()):
        self.approvals = tuple(approvals)

    def verify(self, frame, text, policy):
        for row in self.approvals:
            if (row.get("frame_sha256") == frame.fingerprint
                    and row.get("text_sha256") == digest(text)
                    and row.get("policy_sha256") == digest(policy.as_dict())
                    and row.get("facts_pass") is True
                    and row.get("acceptable") is True
                    and row.get("reviewer") and row.get("reviewed_at")):
                return Verdict("pass", "exact_frame_human_review",
                               tuple(frame.data["obligations"]), self.version)
        return Verdict("indeterminate", "no_bound_human_review", (), self.version)


class CompositeVerifier:
    """Host plug-in verifiers must be validated separately before production use."""
    def __init__(self, *verifiers):
        self.verifiers = (BoundedVerifier(),) + verifiers

    def verify(self, frame, text, policy):
        last = None
        for verifier in self.verifiers:
            verdict = verifier.verify(frame, text, policy)
            if verdict.decision == "reject":
                return verdict
            if verdict.decision == "pass":
                if not set(frame.data["obligations"]) <= set(verdict.covered):
                    return Verdict("reject", "verifier_omitted_required_coverage")
                return verdict
            last = verdict
        return last
