"""Bounded HTTP adapter and HMAC helpers for an explicitly configured Bot shim.

These are our adapter contracts, not an assertion about a Grok product API.
Credentials remain in the trusted shim / gateway, outside the model prompt.
"""
from __future__ import annotations

import hashlib
import hmac
import http.client
import ipaddress
import json
import math
import queue
import re
import secrets
import socket
import ssl
import threading
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from .models import ContractError, DispatchReceipt, canonical_json


TIMESTAMP_HEADER = "X-AM-Timestamp"
NONCE_HEADER = "X-AM-Nonce"
SIGNATURE_HEADER = "X-AM-Signature"
_NONCE = re.compile(r"[A-Za-z0-9_-]{16,128}\Z")
_TIMESTAMP = re.compile(r"[0-9]{1,12}\Z")
_SIGNATURE = re.compile(r"v1=[0-9a-f]{64}\Z")


def secret_bytes(key: str | bytes) -> bytes:
    """A 32+ byte dedicated key. No Feishu credential is needed here."""
    raw = key.encode("utf-8") if isinstance(key, str) else key
    if not isinstance(raw, bytes) or not 32 <= len(raw) <= 8192:
        raise ContractError("signing key must contain at least 32 bytes")
    return raw


def _signature_input(method: str, target: str, body: bytes, timestamp: str, nonce: str) -> bytes:
    if method != "POST" or not isinstance(target, str) or not target.startswith("/"):
        raise ContractError("invalid signature target")
    if len(target) > 8192 or any(ord(c) < 32 or ord(c) == 127 for c in target):
        raise ContractError("invalid signature target")
    if not isinstance(body, bytes) or not _TIMESTAMP.fullmatch(timestamp) or not _NONCE.fullmatch(nonce):
        raise ContractError("invalid signature parameters")
    return ("AM-GROK-HMAC-V1\n" + method + "\n" + target + "\n" + timestamp + "\n" + nonce + "\n"
            + hashlib.sha256(body).hexdigest()).encode("utf-8")


def sign_headers(
    key: str | bytes,
    method: str,
    target: str,
    body: bytes,
    *,
    timestamp: int | None = None,
    nonce: str | None = None,
) -> dict[str, str]:
    """Sign the exact wire bytes; target includes path and any query string.

    The sender serializes JSON once, signs those bytes, and transmits unchanged.
    The receiver never reserializes JSON before authenticating it.
    """
    raw_key = secret_bytes(key)
    instant = int(time.time()) if timestamp is None else timestamp
    if isinstance(instant, bool) or not isinstance(instant, int) or instant < 0:
        raise ContractError("invalid signature timestamp")
    stamp = str(instant)
    token = secrets.token_urlsafe(24) if nonce is None else nonce
    digest = hmac.new(raw_key, _signature_input(method, target, body, stamp, token), hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json; charset=utf-8",
        TIMESTAMP_HEADER: stamp,
        NONCE_HEADER: token,
        SIGNATURE_HEADER: "v1=" + digest,
    }


def verify_signature(
    key: str | bytes,
    method: str,
    target: str,
    body: bytes,
    headers: Mapping[str, str],
    *,
    now: float | None = None,
    max_clock_skew: float = 300,
) -> tuple[str, float]:
    """Authenticate first; the caller must then atomically claim the nonce.

    Returns (nonce, replay-expiry). All failures use static messages so that a
    caller can return an error without exposing a request or credential.
    """
    raw_key = secret_bytes(key)
    if not isinstance(max_clock_skew, (int, float)) or not math.isfinite(max_clock_skew) or not 0 < max_clock_skew <= 3600:
        raise ContractError("invalid signature window")
    values = {k.lower(): v for k, v in headers.items()}
    stamp = values.get(TIMESTAMP_HEADER.lower(), "")
    nonce = values.get(NONCE_HEADER.lower(), "")
    received = values.get(SIGNATURE_HEADER.lower(), "")
    if not isinstance(stamp, str) or not _TIMESTAMP.fullmatch(stamp):
        raise ContractError("invalid signature")
    if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
        raise ContractError("invalid signature")
    if not isinstance(received, str) or not _SIGNATURE.fullmatch(received):
        raise ContractError("invalid signature")
    current = time.time() if now is None else now
    if not isinstance(current, (int, float)) or not math.isfinite(current) or abs(current - int(stamp)) > max_clock_skew:
        raise ContractError("expired signature")
    expected = "v1=" + hmac.new(raw_key, _signature_input(method, target, body, stamp, nonce), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(received, expected):
        raise ContractError("invalid signature")
    return nonce, float(int(stamp) + max_clock_skew)


def is_loopback_host(host: str) -> bool:
    # No arbitrary hostname is resolved merely to permit insecure HTTP.
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _reject_constant(_value: str) -> None:
    raise ContractError("invalid JSON number")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError("duplicate JSON key")
        result[key] = value
    return result


def strict_json(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8"), parse_constant=_reject_constant, object_pairs_hook=_unique_object)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ContractError("invalid JSON body") from exc


class WebhookTransport:
    """One signed POST per submission, no redirects, proxies, or retry loop.

    `accepted` means HTTP 2xx, not inference complete. An ambiguous error is
    `unknown` and must not be replayed automatically. Dedicated bounded daemon
    workers keep submit() bounded even when OS DNS resolution stalls. Stalled
    workers hold their permit, so repeated calls cannot create unlimited threads.
    """

    def __init__(
        self,
        url: str,
        signing_key: str | bytes,
        *,
        timeout: float = 5.0,
        max_response_bytes: int = 65536,
        max_request_bytes: int = 131072,
        max_inflight: int = 4,
        allow_loopback_http: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(url, str) or len(url) > 8192 or any(c.isspace() or ord(c) < 32 for c in url):
            raise ContractError("invalid webhook URL")
        try:
            parsed = urlsplit(url)
            host, port = parsed.hostname, parsed.port
        except ValueError as exc:
            raise ContractError("invalid webhook URL") from exc
        if not host or parsed.username is not None or parsed.password is not None or parsed.fragment:
            raise ContractError("invalid webhook URL")
        if parsed.scheme != "https" and not (parsed.scheme == "http" and allow_loopback_http and is_loopback_host(host)):
            raise ContractError("webhook requires HTTPS")
        if not isinstance(timeout, (float, int)) or isinstance(timeout, bool) or not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise ContractError("invalid transport timeout")
        for value in (max_response_bytes, max_request_bytes):
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1048576:
                raise ContractError("invalid transport body limit")
        if isinstance(max_inflight, bool) or not isinstance(max_inflight, int) or not 1 <= max_inflight <= 32:
            raise ContractError("invalid transport concurrency")
        self._host = "127.0.0.1" if parsed.scheme == "http" and host.lower() == "localhost" else host
        self._scheme = parsed.scheme
        self._port = port
        self._target = (parsed.path or "/") + (("?" + parsed.query) if parsed.query else "")
        self._key = secret_bytes(signing_key)
        self._timeout = float(timeout)
        self._max_response = max_response_bytes
        self._max_request = max_request_bytes
        self._clock = clock
        self._permits = threading.BoundedSemaphore(max_inflight)
        self._tls = ssl.create_default_context() if parsed.scheme == "https" else None

    def submit(self, request: dict[str, Any]) -> DispatchReceipt:
        if not isinstance(request, dict) or request.get("schema") != "am.grok.request.v1":
            return DispatchReceipt("rejected", detail="invalid_request_contract")
        try:
            body = canonical_json(request).encode("utf-8")
            if len(body) > self._max_request:
                return DispatchReceipt("rejected", detail="request_too_large")
            headers = sign_headers(self._key, "POST", self._target, body, timestamp=int(self._clock()))
        except (ContractError, TypeError, ValueError, OverflowError):
            return DispatchReceipt("rejected", detail="invalid_request_contract")
        if not self._permits.acquire(blocking=False):
            return DispatchReceipt("rejected", detail="transport_capacity_exhausted")
        result: queue.Queue[DispatchReceipt] = queue.Queue(maxsize=1)
        cancelled = threading.Event()
        state: dict[str, Any] = {"connection": None, "status": None}
        state_lock = threading.Lock()
        worker = threading.Thread(
            target=self._run,
            args=(body, headers, result, cancelled, state, state_lock),
            name="am-grok-http-submit",
            daemon=True,
        )
        try:
            worker.start()
        except RuntimeError:
            self._permits.release()
            return DispatchReceipt("rejected", detail="transport_worker_unavailable")
        try:
            return result.get(timeout=self._timeout)
        except queue.Empty:
            cancelled.set()
            with state_lock:
                connection = state["connection"]
                status = state["status"]
            self._abort(connection)
            if isinstance(status, int) and 200 <= status < 300:
                return DispatchReceipt("accepted", detail=f"http_{status}_ack_body_unavailable")
            return DispatchReceipt("unknown", detail="transport_timeout")

    @staticmethod
    def _abort(connection: http.client.HTTPConnection | None) -> None:
        if connection is None:
            return
        try:
            if connection.sock is not None:
                connection.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            connection.close()
        except OSError:
            pass

    def _run(
        self,
        body: bytes,
        headers: dict[str, str],
        result: queue.Queue[DispatchReceipt],
        cancelled: threading.Event,
        state: dict[str, Any],
        state_lock: threading.Lock,
    ) -> None:
        connection: http.client.HTTPConnection | None = None
        receipt = DispatchReceipt("unknown", detail="transport_error")
        status: int | None = None
        try:
            if self._scheme == "https":
                connection = http.client.HTTPSConnection(self._host, self._port, timeout=self._timeout, context=self._tls)
            else:
                connection = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
            with state_lock:
                state["connection"] = connection
            # http.client has no redirect handler and does not use environment proxies.
            connection.connect()
            if cancelled.is_set():
                return
            connection.request("POST", self._target, body=body, headers={
                **headers, "Accept": "application/json", "Connection": "close", "User-Agent": "AM-Grok-Gateway/0.1",
            })
            response = connection.getresponse()
            status = response.status
            with state_lock:
                state["status"] = status
            if 200 <= status < 300:
                receipt = DispatchReceipt("accepted", detail=f"http_{status}")
                content_length = response.getheader("Content-Length")
                if content_length is not None and (not content_length.isdigit() or int(content_length) > self._max_response):
                    receipt = DispatchReceipt("accepted", detail=f"http_{status}_ack_body_omitted")
                else:
                    raw = response.read(self._max_response + 1)
                    if len(raw) > self._max_response:
                        receipt = DispatchReceipt("accepted", detail=f"http_{status}_ack_body_omitted")
                    elif raw:
                        try:
                            data = strict_json(raw)
                            run_id = data.get("run_id", "") if isinstance(data, dict) else ""
                            if not isinstance(run_id, str) or len(run_id) > 256 or any(ord(c) < 32 for c in run_id):
                                run_id = ""
                            receipt = DispatchReceipt("accepted", run_id=run_id, detail=f"http_{status}")
                        except ContractError:
                            receipt = DispatchReceipt("accepted", detail=f"http_{status}_ack_not_json")
            elif 400 <= status < 500:
                receipt = DispatchReceipt("rejected", detail=f"http_{status}")
            elif 300 <= status < 400:
                receipt = DispatchReceipt("unknown", detail="redirect_blocked")
            else:
                receipt = DispatchReceipt("unknown", detail=f"http_{status}")
        except (OSError, http.client.HTTPException, ValueError, OverflowError):
            if status is not None and 200 <= status < 300:
                receipt = DispatchReceipt("accepted", detail=f"http_{status}_ack_body_unavailable")
        finally:
            self._abort(connection)
            try:
                result.put_nowait(receipt)
            except queue.Full:
                pass
            self._permits.release()
