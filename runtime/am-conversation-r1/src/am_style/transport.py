"""Bind logical tasks to the actual provider input without conflating roles.

These fingerprints prove content consistency, not the authenticity of a remote
model call. Codex stdin does not include its preserved system/AGENTS context.
Never pass ``wire_payload`` to an adapter which already applies that transport.
"""
from __future__ import annotations

from copy import deepcopy
import re

from .contracts import SpeechFrame, StylePolicy, digest as object_hash
from .data import DataError, canonical, validate_messages
from .prompt import PROMPT_VERSION, build_messages, coordinator_messages

TRANSPORT_SCHEMA = "am-style-transport/1"
DIRECT_CHAT = "direct-chat/1"
COORDINATOR_NATIVE = "coordinator-native-envelope/1"
CODEX_STDIN = "codex-canonical-messages-stdin/1"
PROFILES = {DIRECT_CHAT, COORDINATOR_NATIVE, CODEX_STDIN}
PURPOSES = {"producer", "judge", "student"}
SHA = re.compile(r"[0-9a-f]{64}")


def _sha(value, name):
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise DataError("transport: invalid " + name)


def bind_prompt(logical_messages, *, profile, purpose, frame_sha256, policy_sha256):
    """Describe the exact adapter input; no invocation, approval, or migration.

    ``logical_messages`` is always the value passed to ``ModelRequest``. Native
    Coordinator wraps it once; Codex writes its canonical JSON bytes to stdin.
    Only a chat profile can be a student target for the Qwen training entries.
    """
    validate_messages(logical_messages)
    if profile not in PROFILES or purpose not in PURPOSES:
        raise DataError("transport: unsupported profile/purpose")
    if purpose == "student" and profile == CODEX_STDIN:
        raise DataError("transport: Codex stdin is not a Qwen student chat prompt")
    _sha(frame_sha256, "frame SHA"); _sha(policy_sha256, "policy SHA")
    messages = deepcopy(logical_messages)
    if profile == CODEX_STDIN:
        wire = {"kind": "stdin_utf8", "text": canonical(messages)}
        scope = "provider_stdin_only; retained system/AGENTS context not captured"
    else:
        wire = {"kind": "chat_messages", "messages": coordinator_messages(messages)
                if profile == COORDINATOR_NATIVE else deepcopy(messages)}
        scope = "adapter_chat_messages; tokenizer/chat-template identity recorded separately"
    binding = {
        "schema": TRANSPORT_SCHEMA, "profile": profile, "purpose": purpose,
        "logical_prompt_version": PROMPT_VERSION,
        "frame_sha256": frame_sha256, "policy_sha256": policy_sha256,
        "logical_messages": messages, "logical_prompt_sha256": object_hash(messages),
        "wire_payload": wire, "wire_sha256": object_hash(wire), "context_scope": scope,
    }
    binding["binding_sha256"] = object_hash(binding)
    return binding


def validate_binding(binding, *, purpose=None, logical_messages=None,
                     frame_sha256=None, policy_sha256=None):
    if not isinstance(binding, dict):
        raise DataError("transport: explicit binding is required")
    try:
        expected = bind_prompt(binding["logical_messages"], profile=binding["profile"],
            purpose=binding["purpose"], frame_sha256=binding["frame_sha256"],
            policy_sha256=binding["policy_sha256"])
    except (KeyError, TypeError) as exc:
        raise DataError("transport: incomplete binding") from exc
    if binding != expected:
        raise DataError("transport: binding bytes/profile/hash changed")
    if purpose is not None and binding["purpose"] != purpose:
        raise DataError("transport: producer/judge binding cannot replace student target (wrong purpose)")
    for name, expected_value in (("logical_messages", logical_messages),
                                 ("frame_sha256", frame_sha256), ("policy_sha256", policy_sha256)):
        if expected_value is not None and binding[name] != expected_value:
            raise DataError("transport: " + name + " differs from the frozen task")
    return binding


def runtime_binding(frame, policy, *, profile=COORDINATOR_NATIVE, purpose="student"):
    frozen = frame if isinstance(frame, SpeechFrame) else SpeechFrame.freeze(frame)
    policy = policy if isinstance(policy, StylePolicy) else StylePolicy(**policy)
    return bind_prompt(build_messages(frozen, policy), profile=profile, purpose=purpose,
                       frame_sha256=frozen.fingerprint, policy_sha256=object_hash(policy.as_dict()))


def student_prompt(binding):
    """The only prompt that may become a row's target Qwen training input."""
    validate_binding(binding, purpose="student")
    if binding["wire_payload"]["kind"] != "chat_messages":
        raise DataError("transport: student requires actual chat messages")
    return deepcopy(binding["wire_payload"]["messages"])


def student_contract_version(binding):
    validate_binding(binding, purpose="student")
    # Preserve both historical contract strings. No R3 gold bytes are changed.
    return (PROMPT_VERSION if binding["profile"] == DIRECT_CHAT else
            PROMPT_VERSION + "+" + binding["profile"])


def validate_student_record(record):
    policy = StylePolicy(**record["style_policy"])
    frame = SpeechFrame.freeze(record["frame"])
    binding = validate_binding(record.get("target_binding"), purpose="student",
        logical_messages=build_messages(frame, policy), frame_sha256=frame.fingerprint,
        policy_sha256=object_hash(policy.as_dict()))
    if record.get("prompt") != student_prompt(binding):
        raise DataError("transport: actual record prompt differs from student wire prompt")
    if record.get("host_contract_version") != student_contract_version(binding):
        raise DataError("transport: host contract differs from student profile")
    if record.get("runtime_prompt_alignment") != "aligned":
        raise DataError("transport: runtime alignment is required")
    return binding
