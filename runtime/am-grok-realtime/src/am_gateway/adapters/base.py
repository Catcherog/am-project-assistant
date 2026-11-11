"""Small, dependency-free worker transport contracts.

Transports are host-configured. They do not select recipients, grant capabilities,
retry submissions, or persist model memory.
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any


class AdapterError(RuntimeError):
    """A sanitized failure suitable for the gateway job ledger.

    ``unknown`` means work may have run. The caller must reconcile the original
    attempt instead of submitting another one automatically.
    """

    def __init__(self, code: str, *, status: str = "failed", unknown: bool = False):
        super().__init__(code)
        self.code = code
        self.status = status
        self.unknown = unknown


@dataclass(frozen=True)
class Deadline:
    end: float

    @classmethod
    def from_request(cls, request: dict[str, Any]) -> "Deadline":
        deadline = request.get("deadline_at")
        if isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
            raise AdapterError("invalid_deadline")
        remaining = float(deadline) - time.time()
        if remaining <= 0:
            raise AdapterError("deadline_expired_before_submission")
        return cls(time.monotonic() + remaining)

    def remaining(self, *, submitted: bool = False) -> float:
        remaining = self.end - time.monotonic()
        if remaining <= 0:
            raise AdapterError(
                "deadline_expired", status="timeout_unknown" if submitted else "failed", unknown=submitted
            )
        return remaining


def validate_request(request: dict[str, Any]) -> None:
    if not isinstance(request, dict) or request.get("schema") != "am.gateway.job.v2":
        raise AdapterError("invalid_request_schema")
    for key in ("job_id", "attempt_id", "task_kind", "worker", "session_key"):
        if not isinstance(request.get(key), str) or not request[key]:
            raise AdapterError("invalid_request_" + key)
    if not isinstance(request.get("conversation"), dict) or not isinstance(request.get("context"), dict):
        raise AdapterError("invalid_request_context")
    if request.get("output_mode") not in ("internal", "reply"):
        raise AdapterError("invalid_output_mode")


def json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise AdapterError("invalid_json_payload") from exc


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def parse_json(data: bytes | str, *, submitted: bool = True) -> dict[str, Any]:
    try:
        value = json.loads(data, object_pairs_hook=_pairs, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not isinstance(value, dict):
            raise ValueError("object required")
        return value
    except (ValueError, UnicodeError, TypeError) as exc:
        raise AdapterError("invalid_json_result", status="unknown" if submitted else "failed", unknown=submitted) from exc


def validate_result(result: Any, request: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict) or result.get("schema") != "am.gateway.result.v2":
        raise AdapterError("invalid_result_schema", status="unknown", unknown=True)
    for key in ("job_id", "attempt_id"):
        if result.get(key) != request[key]:
            raise AdapterError("result_identity_mismatch", status="unknown", unknown=True)
    if result.get("status") not in ("completed", "failed"):
        raise AdapterError("invalid_result_status", status="unknown", unknown=True)
    return result
