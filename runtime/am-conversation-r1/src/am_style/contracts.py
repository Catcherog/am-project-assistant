"""Immutable host-authored speech contract. Model output never creates this object."""
from dataclasses import dataclass
import hashlib
import json
import math


class ContractError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StylePolicy:
    version: str = "owner-style/3.0-draft.1"
    max_chars: int = 900
    max_questions: int = 3
    show_sources: str = "on_request"
    inference: str = "grounded_and_qualified"
    # These two choices are proposed defaults, not newly confirmed user preferences.
    clarification: str = "key_two_or_three"
    latency: str = "host_ack_then_final"
    realization: str = "bounded"

    def __post_init__(self):
        if not 100 <= self.max_chars <= 6000 or not 1 <= self.max_questions <= 6:
            raise ContractError("invalid_policy_budget")
        if self.show_sources not in ("on_request", "always"):
            raise ContractError("invalid_source_policy")
        if self.realization not in ("bounded", "free"):
            raise ContractError("invalid_realization")

    def as_dict(self):
        from dataclasses import asdict
        return asdict(self)


@dataclass(frozen=True)
class SpeechFrame:
    """Store a string; never share mutable nested values across model calls."""
    serialized: str

    @classmethod
    def freeze(cls, value):
        d = json.loads(canonical(value))
        if d.get("schema_version") != "am-style-frame/1":
            raise ContractError("frame_schema")
        binding = d.get("binding", {})
        for key in ("turn_id", "scope_id", "outcome_hash", "source_revision"):
            if not isinstance(binding.get(key), str) or not binding[key]:
                raise ContractError("missing_binding:" + key)
        if len(binding["outcome_hash"]) != 64 or any(c not in "0123456789abcdef" for c in binding["outcome_hash"]):
            raise ContractError("outcome_hash")
        for key in ("revision", "cancel_epoch"):
            if type(binding.get(key)) is not int or binding[key] < 0:
                raise ContractError("invalid_fence:" + key)
        if not isinstance(d.get("expires_at"), (int, float)) or not math.isfinite(d["expires_at"]):
            raise ContractError("source_expiry")
        if not isinstance(d.get("question"), str) or not d["question"]:
            raise ContractError("question")
        units = d.get("units", [])
        if not isinstance(units, list) or not units or len(units) > 100:
            raise ContractError("unit_count")
        ids = [u.get("id") for u in units]
        if any(not isinstance(i, str) or not i for i in ids) or len(ids) != len(set(ids)):
            raise ContractError("unit_id_collision")
        obligations = d.get("obligations")
        if not isinstance(obligations, list) or any(not isinstance(x, str) for x in obligations):
            raise ContractError("obligations")
        if len(obligations) != len(set(obligations)):
            raise ContractError("obligation_collision")
        covered = set()
        for unit in units:
            if not isinstance(unit.get("covers"), list) or not unit["covers"]:
                raise ContractError("empty_coverage")
            if len(unit["covers"]) != len(set(unit["covers"])):
                raise ContractError("duplicate_unit_coverage")
            variants = unit.get("variants")
            if not isinstance(variants, list) or not variants or len(variants) > 12:
                raise ContractError("variants")
            if any(not isinstance(x, str) or not x.strip() or len(x) > 6000 for x in variants):
                raise ContractError("variant_text")
            # These are authored by the trusted projector, never by the response LLM.
            if unit.get("authority") not in ("typed_host", "exact_preview", "reviewed_surface"):
                raise ContractError("untrusted_surface")
            covered.update(unit["covers"])
        if not set(obligations) <= covered:
            raise ContractError("uncovered_obligation")
        chosen = d.get("canonical_units", [])
        if not isinstance(chosen, list) or not chosen or any(i not in ids for i in chosen):
            raise ContractError("canonical_selection")
        canon_coverage = set()
        index = {u["id"]: u for u in units}
        for i in chosen:
            if canon_coverage & set(index[i]["covers"]):
                raise ContractError("duplicate_canonical_coverage")
            canon_coverage.update(index[i]["covers"])
        if not set(obligations) <= canon_coverage:
            raise ContractError("canonical_omits_obligation")
        # Equal visible strings must not be used as evidence for distinct entities.
        surfaces = {}
        for u in units:
            for text in u["variants"]:
                if text in surfaces and surfaces[text] != sorted(u["covers"]):
                    raise ContractError("ambiguous_surface_coverage")
                surfaces[text] = sorted(u["covers"])
        return cls(canonical(d))

    @property
    def data(self):
        return json.loads(self.serialized)

    @property
    def fingerprint(self):
        return hashlib.sha256(self.serialized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ModelRequest:
    messages_json: str
    frame_fingerprint: str
    policy_fingerprint: str
    timeout_seconds: float
    max_output_tokens: int = 650

    @property
    def messages(self):
        return json.loads(self.messages_json)


@dataclass(frozen=True)
class ModelResult:
    text: str
    provider: str
    model: str
    duration_ms: float
    metadata_json: str = "{}"


@dataclass(frozen=True)
class PreparedResponse:
    text: str
    frame_fingerprint: str
    selected: str
    reason: str
    candidate: str | None
    metrics_json: str
    # Preparation is deliberately not a delivery receipt.
    delivery: str = "prepared"

    @property
    def metrics(self):
        return json.loads(self.metrics_json)
