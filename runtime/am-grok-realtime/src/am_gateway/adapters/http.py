"""HTTP JSON adapters for administrator-owned worker shims.

The asynchronous envelope below is this gateway's integration contract. It is
not an assertion that Grok or any other vendor exposes such an endpoint.
"""
from __future__ import annotations

import http.client
import re
import socket
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import quote, urlsplit

from .base import AdapterError, Deadline, json_bytes, parse_json, validate_request, validate_result


def _validate_url(url: str) -> None:
    try:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password or parts.fragment:
            raise ValueError()
        _ = parts.port
    except (ValueError, TypeError) as exc:
        raise ValueError("worker URL must be a configured HTTP(S) URL without credentials or fragment") from exc


def request_json(
    url: str,
    *,
    method: str,
    payload: dict[str, Any] | None,
    deadline: Deadline,
    bearer_token: str | None = None,
    max_response_bytes: int = 1_048_576,
    max_request_bytes: int = 1_048_576,
    already_submitted: bool = False,
) -> dict[str, Any]:
    """Send one request; never follow redirects or replay a POST."""
    _validate_url(url)
    parts = urlsplit(url)
    body = None if payload is None else json_bytes(payload)
    if body is not None and len(body) > max_request_bytes:
        raise AdapterError("request_too_large")
    remaining = deadline.remaining(submitted=already_submitted)
    conn_class = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = conn_class(parts.hostname, parts.port, timeout=remaining)
    submitted = already_submitted
    response = None
    try:
        # A connection failure here proves that this call did not transmit a body.
        conn.connect()
        if conn.sock is not None:
            conn.sock.settimeout(deadline.remaining(submitted=submitted))
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if bearer_token:
            headers["Authorization"] = "Bearer " + bearer_token
        path = parts.path or "/"
        if parts.query:
            path += "?" + parts.query
        # From this point a failure cannot prove whether the server accepted work.
        submitted = True
        conn.request(method, path, body=body, headers=headers)
        if conn.sock is not None:
            conn.sock.settimeout(deadline.remaining(submitted=True))
        response = conn.getresponse()
        if response.status >= 500:
            raise AdapterError("http_server_error", status="unknown", unknown=True)
        if response.status >= 400:
            raise AdapterError("http_request_rejected", status="unknown" if already_submitted else "failed", unknown=already_submitted)
        if response.status < 200 or response.status >= 300:
            raise AdapterError("http_redirect_or_unexpected_status", status="unknown", unknown=True)
        content_length = response.getheader("Content-Length")
        if content_length:
            try:
                if int(content_length) > max_response_bytes:
                    raise AdapterError("response_too_large", status="unknown", unknown=True)
            except ValueError as exc:
                raise AdapterError("invalid_response_length", status="unknown", unknown=True) from exc
        chunks: list[bytes] = []
        size = 0
        while True:
            remaining = deadline.remaining(submitted=True)
            # HTTP/1.0 / Connection:close detaches conn.sock; the response still
            # owns the same socket via its buffered file.
            sock = conn.sock
            if sock is None and response.fp is not None:
                sock = getattr(getattr(response.fp, "raw", None), "_sock", None)
            if sock is not None:
                sock.settimeout(remaining)
            chunk = response.read1(min(65536, max_response_bytes + 1 - size))
            if not chunk:
                break
            size += len(chunk)
            if size > max_response_bytes:
                raise AdapterError("response_too_large", status="unknown", unknown=True)
            chunks.append(chunk)
        return parse_json(b"".join(chunks))
    except AdapterError:
        raise
    except (TimeoutError, socket.timeout) as exc:
        raise AdapterError("http_timeout", status="timeout_unknown" if submitted else "failed", unknown=submitted) from exc
    except (OSError, http.client.HTTPException, ValueError) as exc:
        raise AdapterError("http_transport_failure", status="unknown" if submitted else "failed", unknown=submitted) from exc
    finally:
        if response is not None:
            response.close()
        conn.close()


class HTTPJSONAdapter:
    """Configured synchronous worker endpoint, optionally with status polling.

    ``status_url`` is a host configuration template containing ``{run_id}``.
    Submission response URLs are deliberately ignored. Reconciliation after an
    interrupted run belongs to the host; ``run`` always performs a new POST.
    """

    def __init__(
        self,
        url: str,
        *,
        bearer_token: str | None = None,
        status_url: str | None = None,
        poll_interval: float = 0.25,
        max_response_bytes: int = 1_048_576,
        max_request_bytes: int = 1_048_576,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ):
        _validate_url(url)
        if bearer_token is not None and (not isinstance(bearer_token, str) or "\r" in bearer_token or "\n" in bearer_token):
            raise ValueError("invalid bearer token")
        if status_url is not None:
            if status_url.count("{run_id}") != 1:
                raise ValueError("status_url must contain one {run_id}")
            _validate_url(status_url.replace("{run_id}", "run"))
            # Do not forward this endpoint's bearer credential to another origin.
            first, second = urlsplit(url), urlsplit(status_url.replace("{run_id}", "run"))
            if (first.scheme, first.netloc) != (second.scheme, second.netloc):
                raise ValueError("submit/status URLs must share an origin")
        if not 0 < poll_interval <= 60 or max_response_bytes <= 0 or max_request_bytes <= 0:
            raise ValueError("invalid HTTP limits")
        self.url = url
        self.bearer_token = bearer_token
        self.status_url = status_url
        self.poll_interval = poll_interval
        self.max_response_bytes = max_response_bytes
        self.max_request_bytes = max_request_bytes
        if on_progress is not None and not callable(on_progress):
            raise ValueError("on_progress must be callable")
        self.on_progress = on_progress

    def _request(self, url: str, method: str, payload: dict | None, deadline: Deadline, submitted: bool = False) -> dict:
        return request_json(
            url, method=method, payload=payload, deadline=deadline, bearer_token=self.bearer_token,
            max_response_bytes=self.max_response_bytes, max_request_bytes=self.max_request_bytes,
            already_submitted=submitted,
        )

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        validate_request(request)
        deadline = Deadline.from_request(request)
        result = self._request(self.url, "POST", request, deadline)
        if result.get("schema") == "am.gateway.result.v2":
            return validate_result(result, request)
        run_id: str | None = None
        while True:
            if result.get("schema") != "am.gateway.async.v1":
                raise AdapterError("invalid_async_envelope", status="unknown", unknown=True)
            if result.get("job_id") != request["job_id"] or result.get("attempt_id") != request["attempt_id"]:
                raise AdapterError("result_identity_mismatch", status="unknown", unknown=True)
            current_id = result.get("run_id")
            if not isinstance(current_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", current_id):
                raise AdapterError("invalid_async_run_id", status="unknown", unknown=True)
            if run_id is not None and run_id != current_id:
                raise AdapterError("async_run_id_changed", status="unknown", unknown=True)
            run_id = current_id
            state = result.get("status")
            if state not in ("accepted", "running", "completed", "failed"):
                raise AdapterError("invalid_async_status", status="unknown", unknown=True)
            if self.on_progress is not None:
                try:
                    self.on_progress({
                        "job_id": request["job_id"], "attempt_id": request["attempt_id"],
                        "provider_run_id": run_id, "state": state, "observed_at": time.time(),
                    })
                except Exception as exc:
                    error = AdapterError("progress_persistence_failed", status="unknown", unknown=True)
                    error.provider_run_id = run_id
                    raise error from exc
            if state in ("completed", "failed"):
                final = validate_result(result.get("result"), request)
                if final["status"] != state:
                    raise AdapterError("async_terminal_status_mismatch", status="unknown", unknown=True)
                return final
            if self.status_url is None:
                raise AdapterError("async_status_endpoint_required", status="submitted", unknown=True)
            remaining = deadline.remaining(submitted=True)
            time.sleep(min(self.poll_interval, remaining))
            deadline.remaining(submitted=True)
            result = self._request(self.status_url.replace("{run_id}", quote(run_id, safe="")), "GET", None, deadline, True)


class GrokBotAdapter(HTTPJSONAdapter):
    """Realtime dialogue shim. Scheduled and business work is refused."""

    background_jobs = False
    global_periodic_scan = False

    def __init__(self, url: str, *, background_jobs: bool = False, global_periodic_scan: bool = False, **kwargs: Any):
        if background_jobs is not False or global_periodic_scan is not False:
            raise ValueError("Grok Bot cannot execute background or periodic jobs")
        super().__init__(url, **kwargs)

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        validate_request(request)
        if request["task_kind"] != "dialogue_reply" or request["worker"] != "grok_bot" or request["output_mode"] != "reply":
            raise AdapterError("grok_bot_dialogue_only")
        # The host must actually use a fresh, memory-free native session. This
        # field cannot erase a remote product's shared or account-level memory.
        if request.get("memory_mode", "stateless") != "stateless":
            raise AdapterError("grok_bot_stateless_required")
        request = {**request, "memory_mode": "stateless"}
        result = super().run(request)
        forbidden = {"fact_updates", "target", "targets", "recipients", "authorization_ref"}
        if forbidden.intersection(result):
            raise AdapterError("grok_bot_forbidden_output", status="failed")
        return result
