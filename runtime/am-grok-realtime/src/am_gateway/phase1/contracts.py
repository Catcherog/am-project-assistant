"""Trusted contracts. Source text and classifier output never supply authority."""
from dataclasses import asdict, dataclass, field
import math

from ..models import ContractError, dumps, key

INTENTS = ("project.query", "conversation.reply", "work_message.assist",
           "document.process", "project.execute", "code.execute", "context.supplement")
ANSWER_KINDS = {"sufficient", "partial", "clarification", "data_gap", "unavailable", "failed"}
TERMINAL = {"succeeded", "failed", "cancelled"}


@dataclass(frozen=True)
class Capability:
    name: str
    adapter: str
    execution_scope: str
    available: bool = False
    schema_version: str = "1.0"
    reply_owner: str = "adapter"
    durable_acceptance: bool = False
    status_query: bool = False
    cancellation: bool = False
    isolation: bool = False
    enforced_tools: bool = False
    evidence_ref: str = ""


def registry():
    # Deliberately unavailable: a webhook 200 is NOT a durable receiver ACK.
    result = {i: Capability(i, "grok_bot", "grok_bot:shared") for i in INTENTS[:3]}
    result.update({i: Capability(i, i.split('.')[0] + "_agent", i) for i in INTENTS[3:6]})
    result["context.supplement"] = Capability("context.supplement", "harness", "harness", True,
                                              reply_owner="harness", enforced_tools=True)
    return result


@dataclass(frozen=True)
class HarnessConfig:
    tenant_id: str
    bot_id: str
    owner_id: str
    owner_chat_id: str
    route_version: str = "2026-11-09.1"
    capabilities: dict = field(default_factory=registry)
    queue_limit: int = 32
    dispatch_deadline_seconds: float = 120.0
    execution_deadline_seconds: float = 180.0
    lease_seconds: float = 30.0
    classification_budget_seconds: float = 6.0
    dispatch_reserve_seconds: float = 1.0
    delta_limit: int = 40
    context_bytes: int = 16384

    def validate(self):
        if not all((self.tenant_id, self.bot_id, self.owner_id, self.owner_chat_id)):
            raise ContractError("identity bindings required")
        for value in (self.dispatch_deadline_seconds, self.execution_deadline_seconds,
                      self.lease_seconds, self.classification_budget_seconds, self.dispatch_reserve_seconds):
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ContractError("finite positive budgets required")
        if not 1 <= self.queue_limit <= 1000 or not 1 <= self.delta_limit <= 1000 or not 1024 <= self.context_bytes <= 65536:
            raise ContractError("invalid bounded queue/context configuration")
        if set(self.capabilities) != set(INTENTS):
            raise ContractError("registered intent set differs from schema")
        for name, cap in self.capabilities.items():
            if cap.name != name or cap.schema_version != "1.0":
                raise ContractError("unsupported capability schema")
            if cap.available and cap.adapter != "harness" and not (cap.durable_acceptance and cap.evidence_ref):
                raise ContractError("available adapter needs durable acceptance evidence")


@dataclass(frozen=True)
class Policy:
    """Produced by the host authorization port, never the model or incoming JSON."""
    principal: str
    authorization_ref: str
    reply_target: str
    source_chats: tuple[str, ...]
    projects: tuple[str, ...] = ()
    business_operations: tuple[str, ...] = ()
    allow_reply: bool = False
    allow_supplement: bool = False
    version: int = 1
    revoked: bool = False

    def validate(self):
        if not self.principal or not self.authorization_ref or not self.reply_target:
            raise ContractError("policy identity and target required")
        if self.version < 1 or not isinstance(self.source_chats, tuple) or not isinstance(self.projects, tuple):
            raise ContractError("invalid policy")
        # Phase one intentionally does not implement business writes.
        if self.business_operations:
            raise ContractError("business write capability is not connected")
        dumps(asdict(self))

    def json(self):
        self.validate()
        return dumps(asdict(self))

    @classmethod
    def parse(cls, value):
        import json
        data = json.loads(value) if isinstance(value, str) else dict(value)
        for name in ("source_chats", "projects", "business_operations"):
            data[name] = tuple(data.get(name, ()))
        policy = cls(**data)
        policy.validate()
        return policy


def business_fingerprint(*, text, intent, principal, reply_target, attachments, inputs):
    """Frozen semantics; route version, attempt, epoch, evidence and policy version excluded."""
    return key({"text": text, "intent": intent, "principal": principal,
                "reply_target": reply_target, "attachments": attachments, "inputs": inputs})


def validate_envelope(envelope):
    if envelope.get('schema_version') != '1.0':
        raise ContractError('unsupported envelope version')
    policy = Policy.parse(envelope['policy'])
    identity, execution = envelope['identity'], envelope['execution']
    required = {'task_id','request_revision','business_operation_id','idempotency_key',
                'dispatch_attempt_id','business_fingerprint'}
    if set(identity) != required or identity['idempotency_key'] != identity['business_operation_id']:
        raise ContractError('invalid operation identity')
    if any(not isinstance(identity[n],str) or not identity[n] for n in required-{'request_revision'}):
        raise ContractError('identity strings required')
    for value in (identity['request_revision'],execution['generation']):
        if not isinstance(value,int) or isinstance(value,bool) or value<1:
            raise ContractError('positive revision and epoch required')
    for name in ('accept_deadline','completion_deadline'):
        value=execution[name]
        if not isinstance(value,(int,float)) or isinstance(value,bool) or not math.isfinite(value) or value<=0:
            raise ContractError('finite deadline required')
    payload=envelope['payload']
    if envelope['routing']['intent'] not in INTENTS or envelope['output']['reply_target']!=policy.reply_target:
        raise ContractError('routing/target contract mismatch')
    expected=business_fingerprint(text=payload['raw_text'],intent=envelope['routing']['intent'],
        principal=policy.principal,reply_target=policy.reply_target,attachments=payload['attachment_refs'],
        inputs=payload['structured_inputs'])
    if expected!=identity['business_fingerprint']:
        raise ContractError('business input fingerprint mismatch')
    return envelope
