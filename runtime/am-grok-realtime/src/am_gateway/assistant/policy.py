"""Pure business permissions for the R5 assistant.

Identity flags and risk tags come from trusted adapters, never from an inbound
message or a worker's requested permission. An approval is consumed elsewhere
against the exact proposed action; this policy never grants one itself.
"""

from dataclasses import dataclass
from typing import Literal
import unicodedata


Outcome = Literal["allow", "approval_required", "record_and_refer", "deny"]

ROUTINE_ACTIONS = frozenset({
    "reply", "query", "summarize", "draft_document", "update_progress",
    "update_todo", "mark_complete", "record_request", "notify_owner",
})
OUTBOUND_ACTIONS = frozenset({"reply", "proactive_followup", "send_message"})
APPROVAL_ACTIONS = {
    "approval": "business_approval",
    "formal_commitment": "formal_commitment",
    "change_milestone": "milestone_change",
    "change_owner": "responsibility_change",
    "acceptance": "acceptance_or_closure",
    "close_project": "acceptance_or_closure",
}
APPROVAL_TAGS = frozenset({
    "direct_leader_request", "direct_leader_contact", "approval",
    "formal_commitment", "milestone_change", "responsibility_change",
    "acceptance", "closure", "resource_commitment", "priority_conflict",
    "restricted_information", "uncertain_risk",
})
PROTECTED_TAGS = frozenset({
    "protected_person_related", "reviewer_a_related", "reviewer_b_related",
})
COMPLETION_EVIDENCE_KINDS = frozenset({
    "completion_proof", "verified_result", "acceptance_record",
})
PERMISSION_CHANGE_ACTIONS = frozenset({
    "change_policy", "self_grant_permission", "remove_approval_requirement",
})


def _strings(value, *, field, ordered=False):
    if isinstance(value, (str, bytes)):
        raise ValueError(f"{field} must be a collection of strings")
    values = tuple(value)
    if any(not isinstance(item, str) or not item.strip() for item in values):
        raise ValueError(f"{field} contains an invalid value")
    return tuple(dict.fromkeys(values)) if ordered else frozenset(values)


def _bools(instance, *fields):
    if any(type(getattr(instance, field)) is not bool for field in fields):
        raise ValueError("boolean fields require bool values")


def _normalized_name(value):
    return "".join(unicodedata.normalize("NFKC", value).casefold().split())


@dataclass(frozen=True)
class Actor:
    user_id: str
    display_name: str
    department_ids: tuple[str, ...]
    verified: bool
    is_owner: bool = False
    is_leader: bool = False

    def __post_init__(self):
        if not isinstance(self.user_id, str) or not isinstance(self.display_name, str):
            raise ValueError("actor identity must use strings")
        _bools(self, "verified", "is_owner", "is_leader")
        if self.verified and not self.user_id.strip():
            raise ValueError("a verified actor needs a user_id")
        object.__setattr__(self, "department_ids", _strings(
            self.department_ids, field="department_ids", ordered=True))


@dataclass(frozen=True)
class SourceEvidence:
    """Verified means the source was checked, not that its claim was proved.

    A reported completion can be recorded from a verified message. Claiming
    verified completion additionally needs a completion proof for subject_id.
    """

    source_id: str
    kind: str = "message"
    verified: bool = False
    project_id: str | None = None
    subject_id: str | None = None

    def __post_init__(self):
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise ValueError("source evidence needs an exact source_id")
        if not isinstance(self.kind, str) or not self.kind.strip():
            raise ValueError("source evidence needs a kind")
        _bools(self, "verified")
        for field in ("project_id", "subject_id"):
            value = getattr(self, field)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{field} must be a nonempty string or None")


@dataclass(frozen=True)
class PolicyConfig:
    owner_user_ids: frozenset[str] = frozenset()
    demo_department_ids: frozenset[str] = frozenset()
    leader_user_ids: frozenset[str] = frozenset()
    protected_person_ids: frozenset[str] = frozenset()
    protected_name_aliases: frozenset[str] = frozenset({"示例主管甲", "示例主管乙"})

    def __post_init__(self):
        for field in (
            "owner_user_ids", "demo_department_ids", "leader_user_ids",
            "protected_person_ids", "protected_name_aliases",
        ):
            object.__setattr__(self, field, _strings(getattr(self, field), field=field))


@dataclass(frozen=True)
class ActionContext:
    action_id: str
    action_kind: str
    initiator: Actor
    recipients: tuple[Actor, ...] = ()
    project_scope: str = "unknown"
    source_evidence: tuple[SourceEvidence, ...] = ()
    sensitivity: frozenset[str] = frozenset()
    mentioned_person_ids: tuple[str, ...] = ()
    mentioned_names: tuple[str, ...] = ()
    uses_owner_account: bool = False
    claims_verified_complete: bool = False
    passive_reply: bool = True
    group_audience: tuple[Actor, ...] = ()
    content_text: str = ""
    external_visibility: bool = False
    subject_id: str | None = None
    project_id: str | None = None

    def __post_init__(self):
        for field in ("action_id", "action_kind", "project_scope"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must be a nonempty string")
        if not isinstance(self.initiator, Actor):
            raise ValueError("initiator must be an Actor")
        for field in ("recipients", "group_audience"):
            values = tuple(getattr(self, field))
            if any(not isinstance(actor, Actor) for actor in values):
                raise ValueError(f"{field} must contain Actors")
            object.__setattr__(self, field, values)
        evidence = tuple(self.source_evidence)
        if any(not isinstance(item, SourceEvidence) for item in evidence):
            raise ValueError("source_evidence must contain SourceEvidence")
        object.__setattr__(self, "source_evidence", evidence)
        object.__setattr__(self, "sensitivity", _strings(self.sensitivity, field="sensitivity"))
        for field in ("mentioned_person_ids", "mentioned_names"):
            object.__setattr__(self, field, _strings(
                getattr(self, field), field=field, ordered=True))
        _bools(self, "uses_owner_account", "claims_verified_complete",
               "passive_reply", "external_visibility")
        if not isinstance(self.content_text, str):
            raise ValueError("content_text must be a string")
        for field in ("subject_id", "project_id"):
            value = getattr(self, field)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{field} must be a nonempty string or None")


@dataclass(frozen=True)
class Decision:
    outcome: Outcome
    reason_codes: tuple[str, ...]
    action_id: str = ""

    @property
    def allowed(self):
        return self.outcome == "allow"

    @property
    def requires_approval(self):
        return self.outcome == "approval_required"


class AssistantPolicy:
    """Evaluate one exact action without sending, changing state, or using time."""

    def __init__(self, config: PolicyConfig):
        if not isinstance(config, PolicyConfig):
            raise ValueError("config must be PolicyConfig")
        self.config = config

    def _owner(self, actor):
        return actor.verified and actor.user_id in self.config.owner_user_ids

    def _leader(self, actor):
        return actor.verified and (
            actor.is_leader or actor.user_id in self.config.leader_user_ids)

    def _demo(self, actor):
        return actor.verified and bool(
            set(actor.department_ids) & self.config.demo_department_ids)

    def _protected(self, context):
        if context.sensitivity & PROTECTED_TAGS:
            return True
        mentioned_ids = set(context.mentioned_person_ids)
        mentioned_ids.update(actor.user_id for actor in context.recipients)
        if mentioned_ids & self.config.protected_person_ids:
            return True
        aliases = tuple(_normalized_name(name) for name in self.config.protected_name_aliases)
        texts = [context.content_text, *context.mentioned_names]
        texts.extend(actor.display_name for actor in context.recipients)
        return any(alias in _normalized_name(value)
                   for value in texts for alias in aliases if alias)

    def _usable_sources(self, context):
        return tuple(source for source in context.source_evidence
                     if source.verified and not (
                         context.project_id and source.project_id
                         and source.project_id != context.project_id))

    def _completion_proven(self, context):
        return bool(context.subject_id) and any(
            source.kind in COMPLETION_EVIDENCE_KINDS
            and source.subject_id == context.subject_id
            for source in self._usable_sources(context))

    def evaluate(self, context: ActionContext) -> Decision:
        if not isinstance(context, ActionContext):
            raise ValueError("context must be ActionContext")

        def decide(outcome, *reasons):
            return Decision(outcome, tuple(dict.fromkeys(reasons)), context.action_id)

        kind = context.action_kind
        if kind in PERMISSION_CHANGE_ACTIONS or "policy_change" in context.sensitivity:
            return decide("deny", "permission_change_forbidden")
        if kind == "record_request":
            if context.recipients or context.group_audience or context.external_visibility:
                return decide("deny", "record_cannot_send")
            return decide("allow", "intake_record_only")
        if not self.config.owner_user_ids:
            return decide("deny", "owner_identity_unconfigured")

        audience = (*context.recipients, *context.group_audience)
        owner_only = bool(context.recipients) and all(self._owner(actor) for actor in audience)
        outward = (context.external_visibility or "external_disclosure" in context.sensitivity
                   or any(not self._owner(actor) for actor in audience))
        if kind == "notify_owner":
            if not owner_only or outward:
                return decide("deny", "owner_report_target_invalid")
            if context.uses_owner_account:
                return decide("approval_required", "owner_account")
            if context.claims_verified_complete and not self._completion_proven(context):
                return decide("record_and_refer", "unverified_completion_claim")
            return decide("allow", "owner_report")

        if kind not in ROUTINE_ACTIONS | OUTBOUND_ACTIONS | APPROVAL_ACTIONS.keys():
            return decide("record_and_refer", "unknown_action_kind")

        if not context.initiator.verified:
            return decide("record_and_refer", "unverified_initiator")
        is_owner = self._owner(context.initiator)
        is_leader = self._leader(context.initiator)
        if not is_owner:
            if context.project_scope != "demo":
                return decide("record_and_refer", "project_out_of_scope"
                              if context.project_scope == "unrelated" else "project_scope_unknown")
            if not self._demo(context.initiator) and not is_leader:
                return decide("record_and_refer", "requester_outside_demo")

        if kind in OUTBOUND_ACTIONS and not context.recipients:
            return decide("deny", "missing_recipients")
        if any(not actor.verified for actor in context.recipients):
            return decide("record_and_refer", "unverified_recipient")

        reasons = []
        if context.uses_owner_account:
            reasons.append("owner_account")
        if is_leader and not is_owner:
            reasons.append("direct_leader_request")
        if any(self._leader(actor) and not self._owner(actor) for actor in context.recipients):
            reasons.append("direct_leader_contact")
        if outward and self._protected(context):
            reasons.append("protected_person_external_information")
        if kind in APPROVAL_ACTIONS:
            reasons.append(APPROVAL_ACTIONS[kind])
        reasons.extend(sorted(context.sensitivity & APPROVAL_TAGS))
        unknown_tags = context.sensitivity - APPROVAL_TAGS - PROTECTED_TAGS - {"external_disclosure"}
        if unknown_tags:
            reasons.append("unclassified_sensitivity")
        if kind in {"proactive_followup", "send_message"} or (
                kind == "reply" and not context.passive_reply):
            reasons.append("observation_followup_requires_owner")
        if kind == "reply" and context.passive_reply and any(
                actor.user_id != context.initiator.user_id and not self._owner(actor)
                for actor in context.recipients):
            reasons.append("additional_recipient")
        if reasons:
            return decide("approval_required", *reasons)

        if any(not self._owner(actor) and not self._demo(actor) for actor in context.recipients):
            return decide("record_and_refer", "recipient_outside_demo")
        if kind not in ROUTINE_ACTIONS:
            return decide("record_and_refer", "unknown_action_kind")
        sources = self._usable_sources(context)
        if kind in {"update_progress", "update_todo", "mark_complete"} and not sources:
            return decide("record_and_refer", "no_verified_source")
        if context.claims_verified_complete or kind == "mark_complete":
            if not self._completion_proven(context):
                return decide("record_and_refer", "unverified_completion_claim")
        return decide("allow", "routine_owner_action" if is_owner else "routine_demo_action")
