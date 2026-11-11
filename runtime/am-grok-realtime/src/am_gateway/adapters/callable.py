"""Trusted synchronous integration hook for an existing deterministic executor."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .base import AdapterError, Deadline, json_bytes, parse_json, validate_request, validate_result


class CallableAdapter:
    """In-process trusted function; use only for short deterministic operations.

    Python callables cannot be forcibly preempted safely. A host requiring hard
    deadlines must put its executor behind SubprocessJsonAdapter or HTTP.
    """

    def __init__(self, function: Callable[[dict[str, Any]], dict[str, Any]]):
        if not callable(function):
            raise ValueError("function must be callable")
        self.function = function

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        validate_request(request)
        deadline = Deadline.from_request(request)
        # Prevent accidental mutation of the gateway-owned request object.
        detached = parse_json(json_bytes(request), submitted=False)
        deadline.remaining()
        try:
            result = self.function(detached)
        except AdapterError:
            raise
        except Exception as exc:
            raise AdapterError("callable_execution_failed", status="unknown", unknown=True) from exc
        deadline.remaining(submitted=True)
        return validate_result(result, request)
