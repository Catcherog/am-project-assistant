"""Small, injectable, read-only Feishu HTTP client.

Credentials never enter Source.settings, URLs, exceptions or observations. The
default urllib transport refuses every redirect before forwarding Authorization.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import re
import socket
from typing import Any, Callable, Mapping, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ..models import (InvalidCursor, PermissionDenied, RetryableError,
                      ScopeViolation, SnapshotChanged, Source, SyncError)


class AuthenticationExpired(SyncError):
    code = "authentication_expired"


class RemoteAPIError(SyncError):
    code = "remote_api_error"

    def __init__(self, api_code: int, status: int = 200):
        # Do not echo a remote msg: it can contain sensitive request details.
        self.api_code = api_code
        self.status = status
        super().__init__(f"Remote API failed (HTTP {status}, code {api_code})")


class InvalidResponse(SyncError):
    code = "invalid_response"


@dataclass(frozen=True)
class HttpRequest:
    url: str
    headers: Mapping[str, str] = field(repr=False)
    timeout: float = 15.0
    method: str = "GET"


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes | str | Mapping[str, Any] = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict)
    final_url: str | None = None


class Transport(Protocol):
    def __call__(self, request: HttpRequest) -> HttpResponse: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class UrllibTransport:
    """Only GET; no automatic redirects; bounded response memory."""

    def __init__(self, max_response_bytes: int = 16 * 1024 * 1024):
        if max_response_bytes < 1:
            raise ValueError("Invalid response size limit")
        self.max_response_bytes = max_response_bytes
        self.opener = build_opener(_NoRedirect())

    def __call__(self, request: HttpRequest) -> HttpResponse:
        if request.method != "GET":
            raise ScopeViolation("Read-only transport accepts GET only")
        req = Request(request.url, headers=dict(request.headers), method="GET")
        try:
            response = self.opener.open(req, timeout=request.timeout)
        except HTTPError as error:
            # HTTPError is a response object, including rejected redirects.
            response = error
        except (URLError, TimeoutError, socket.timeout, OSError):
            raise RetryableError("Network unavailable or request timed out") from None
        with response:
            body = response.read(self.max_response_bytes + 1)
            if len(body) > self.max_response_bytes:
                raise InvalidResponse("Response exceeded configured size limit")
            return HttpResponse(response.code, body, dict(response.headers), response.geturl())


_PATH = re.compile(
    r"/open-apis/(?:im/v1/messages(?:/[A-Za-z0-9_-]{1,256})?"
    r"|docx/v1/documents/[A-Za-z0-9_-]{1,256}(?:/blocks)?)\Z"
)
_AUTH_CODES = {99991661, 99991663, 99991664, 99991665, 99991668,
               99991669, 99991671, 20005, 20006}
_PERMISSION_CODES = {99991662, 99991672, 99991679, 230002, 230013, 230027,
                     230073, 231203, 231204, 1770032}
_RATE_CODES = {99991400}


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
    if value is None:
        return None
    try:
        return max(0.0, min(float(value), 86400.0))
    except (TypeError, ValueError):
        try:
            at = parsedate_to_datetime(value)
            if at.tzinfo is None:
                at = at.replace(tzinfo=timezone.utc)
            return max(0.0, min((at - datetime.now(timezone.utc)).total_seconds(), 86400.0))
        except (TypeError, ValueError, OverflowError):
            return None


class FeishuClient:
    """token_provider(Source) -> token; transport(HttpRequest) -> HttpResponse.

    A custom transport is a trusted integration dependency and must not follow
    redirects. Source configuration cannot override the two official API hosts.
    """

    def __init__(self, token_provider: Callable[[Source], str],
                 transport: Transport | None = None, *,
                 host: str = "open.feishu.cn", timeout: float = 15.0):
        if host not in {"open.feishu.cn", "open.larksuite.com"}:
            raise ScopeViolation("Only official Feishu/Lark API hosts are accepted")
        if not 0 < timeout <= 120:
            raise ValueError("Timeout must be between 0 and 120 seconds")
        self.token_provider = token_provider
        self.transport = transport or UrllibTransport()
        self.host = host
        self.timeout = timeout

    def get(self, source: Source, path: str,
            params: Mapping[str, str | int] | None = None) -> dict[str, Any]:
        if not source.enabled:
            raise ScopeViolation("Source is disabled")
        if not _PATH.fullmatch(path):
            raise ScopeViolation("Endpoint is outside the read-only adapter allowlist")
        try:
            token = self.token_provider(source)
        except Exception:
            raise AuthenticationExpired("Token provider failed") from None
        if not isinstance(token, str) or not token or any(c.isspace() for c in token):
            raise AuthenticationExpired("Token provider returned no usable credential")
        url = f"https://{self.host}{path}"
        if params:
            url += "?" + urlencode(params)
        request = HttpRequest(url, {"Authorization": f"Bearer {token}",
                                   "Accept": "application/json"}, self.timeout)
        try:
            response = self.transport(request)
        except SyncError:
            raise
        except Exception:
            raise RetryableError("HTTP transport failed") from None
        if not isinstance(response, HttpResponse):
            raise InvalidResponse("Transport must return HttpResponse")
        if response.final_url is not None and response.final_url != url:
            raise ScopeViolation("Redirected responses are not accepted")
        if 300 <= response.status < 400:
            raise ScopeViolation("HTTP redirects are not allowed")
        if response.status == 401:
            raise AuthenticationExpired("Remote credential expired or invalid")
        if response.status == 403:
            raise PermissionDenied("Remote source permission denied")
        if response.status == 429:
            raise RetryableError("Remote rate limit", _retry_after(response.headers))
        if response.status >= 500:
            raise RetryableError("Remote server unavailable", _retry_after(response.headers))
        try:
            result = dict(response.body) if isinstance(response.body, Mapping) else json.loads(response.body)
        except (TypeError, ValueError, UnicodeError):
            raise InvalidResponse("Remote response was not valid JSON") from None
        if not isinstance(result, dict) or type(result.get("code")) is not int:
            raise InvalidResponse("Remote response has no integer API code")
        code = result["code"]
        if code in _AUTH_CODES:
            raise AuthenticationExpired("Remote credential expired or invalid")
        if code in _PERMISSION_CODES:
            raise PermissionDenied(f"Remote source permission denied (code {code})")
        if code in _RATE_CODES:
            raise RetryableError("Remote rate limit", _retry_after(response.headers))
        if code in {100001, 1770022, 230001} and params and "page_token" in params:
            raise InvalidCursor("Remote pagination cursor rejected")
        if code in {95017, 95023, 1770021}:
            raise SnapshotChanged("Requested document revision is unavailable")
        if code != 0 or not 200 <= response.status < 300:
            raise RemoteAPIError(code, response.status)
        data = result.get("data")
        if not isinstance(data, dict):
            raise InvalidResponse("Successful API response has no data object")
        return data


def remote_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", value):
        raise ScopeViolation("Invalid remote object ID")
    return value


def page_items(data: dict[str, Any], old_cursor: str | None = None) -> tuple[list[dict[str, Any]], str | None]:
    items = data.get("items")
    more = data.get("has_more")
    if not isinstance(items, list) or any(not isinstance(x, dict) for x in items):
        raise InvalidResponse("Page items must be a list of objects")
    if type(more) is not bool:
        raise InvalidResponse("Page has_more flag is missing or invalid")
    token = data.get("page_token") if more else None
    if more and (not isinstance(token, str) or not token or token == old_cursor):
        raise InvalidCursor("Remote pagination did not advance")
    return items, token
