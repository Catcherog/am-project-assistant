"""Bounded authenticated callback receiver. No Feishu ingress or write API.

Use loopback behind an explicitly provisioned relay/TLS endpoint when the Bot
runs elsewhere. A remote Bot cannot reach Studio's localhost by implication.
"""
from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import math
import socket
import threading
import time
from typing import Any, Callable

from .models import BotResult, ContractError, bounded_text, canonical_json
from .transport import (
    NONCE_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, is_loopback_host,
    secret_bytes, strict_json, verify_signature,
)


class _BoundedServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    allow_reuse_address = True
    request_queue_size = 16

    def __init__(self, address: tuple[str, int], owner: CallbackServer) -> None:
        self.owner = owner
        self._slots = threading.BoundedSemaphore(owner.max_handlers)
        self._connections: set[socket.socket] = set()
        self._connection_lock = threading.Lock()
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, _Handler)

    def process_request(self, request: socket.socket, client_address: tuple[Any, ...]) -> None:
        if not self._slots.acquire(blocking=False):
            try:
                request.settimeout(0.05)
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        with self._connection_lock:
            self._connections.add(request)
        try:
            super().process_request(request, client_address)
        except Exception:
            with self._connection_lock:
                self._connections.discard(request)
            self._slots.release()
            self.shutdown_request(request)

    def process_request_thread(self, request: socket.socket, client_address: tuple[Any, ...]) -> None:
        # This is an absolute connection lifetime, not a timeout reset by each
        # byte. It bounds slow headers / bodies even if the peer keeps dripping.
        timer = threading.Timer(self.owner.request_timeout, self._abort_socket, args=(request,))
        timer.daemon = True
        timer.start()
        try:
            try:
                request.settimeout(self.owner.request_timeout)
            except OSError:
                # stop() may close an accepted socket before its handler thread
                # starts. It has no usable request to process; finish cleanup
                # here, within the same slot/connection ownership scope.
                self.shutdown_request(request)
                return
            super().process_request_thread(request, client_address)
        finally:
            timer.cancel()
            with self._connection_lock:
                self._connections.discard(request)
            self._slots.release()

    @staticmethod
    def _abort_socket(request: socket.socket) -> None:
        try:
            request.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            request.close()
        except OSError:
            pass

    def abort_connections(self) -> None:
        with self._connection_lock:
            sockets = list(self._connections)
        for request in sockets:
            self._abort_socket(request)

    def handle_error(self, request: socket.socket, client_address: tuple[Any, ...]) -> None:
        # Default traceback logging can include callback data. The gateway's
        # business record owns audit; this transport emits no payload logs.
        pass


class _Handler(BaseHTTPRequestHandler):
    server: _BoundedServer
    protocol_version = "HTTP/1.1"
    server_version = "AMCallback/1"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def parse_request(self) -> bool:
        result = super().parse_request()
        if result and (len(self.raw_requestline) > 2048 or len(self.headers) > 64
                       or sum(len(k) + len(v) for k, v in self.headers.items()) > 16384):
            self._reply(431, {"error": "headers_too_large"})
            return False
        return result

    def send_error(self, code: int, message: str | None = None, explain: str | None = None) -> None:
        self._reply(code, {"error": "invalid_http_request"})

    def _reply(self, code: int, data: dict[str, Any]) -> None:
        self.close_connection = True
        try:
            body = canonical_json(data).encode("utf-8")
            if len(body) > 8192:
                code, body = 500, b'{"error":"callback_response_too_large"}'
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except (OSError, ValueError, ContractError):
            pass

    def do_GET(self) -> None:
        self._reply(405, {"error": "method_not_allowed"})

    do_HEAD = do_GET
    do_PUT = do_GET
    do_PATCH = do_GET
    do_DELETE = do_GET
    do_OPTIONS = do_GET

    def do_POST(self) -> None:
        owner = self.server.owner
        if self.path not in {"/v1/results", "/v1/started"}:
            self._reply(404, {"error": "unknown_endpoint"})
            return
        if self.headers.get("Transfer-Encoding") is not None:
            self._reply(400, {"error": "transfer_encoding_not_supported"})
            return
        lengths = self.headers.get_all("Content-Length", [])
        if not lengths:
            self._reply(411, {"error": "content_length_required"})
            return
        if len(lengths) != 1 or not lengths[0].isdigit() or len(lengths[0]) > 8:
            self._reply(400, {"error": "invalid_content_length"})
            return
        size = int(lengths[0])
        if size <= 0 or size > owner.max_body_bytes:
            self._reply(413, {"error": "body_too_large"})
            return
        content_types = self.headers.get_all("Content-Type", [])
        if len(content_types) != 1 or content_types[0].split(";", 1)[0].strip().lower() != "application/json":
            self._reply(415, {"error": "json_required"})
            return
        if self.headers.get("Content-Encoding", "identity").lower() != "identity":
            self._reply(415, {"error": "content_encoding_not_supported"})
            return
        for header in (TIMESTAMP_HEADER, NONCE_HEADER, SIGNATURE_HEADER):
            if len(self.headers.get_all(header, [])) != 1:
                self._reply(401, {"error": "unauthorized"})
                return
        try:
            body = self.rfile.read(size)
            if len(body) != size:
                self._reply(400, {"error": "incomplete_body"})
                return
            nonce, expires_at = verify_signature(
                owner._key, "POST", self.path, body, dict(self.headers.items()),
                now=owner.clock(), max_clock_skew=owner.max_clock_skew,
            )
        except ContractError:
            self._reply(401, {"error": "unauthorized"})
            return
        except OSError:
            self._reply(408, {"error": "request_timeout"})
            return
        try:
            data = strict_json(body)
            if not isinstance(data, dict):
                raise ContractError("JSON object required")
            if self.path == "/v1/results":
                BotResult.parse(data)
            else:
                if set(data) != {"schema", "request_id", "attempt_id"} or data["schema"] != "am.grok.started.v1":
                    raise ContractError("invalid start receipt")
                bounded_text(data["request_id"], "request_id", 128)
                bounded_text(data["attempt_id"], "attempt_id", 128)
        except (ContractError, KeyError, TypeError):
            self._reply(400, {"error": "invalid_contract"})
            return
        try:
            claimed = owner.replay_guard("grok-callback:" + nonce, expires_at)
        except Exception:
            self._reply(503, {"error": "replay_store_unavailable"})
            return
        if claimed is not True:
            self._reply(409, {"error": "replayed_nonce"})
            return
        try:
            callback = owner.on_result if self.path == "/v1/results" else owner.on_started
            response = callback(data)
            if not isinstance(response, dict):
                raise TypeError("callback response must be an object")
            self._reply(200, response)
        except ContractError:
            self._reply(400, {"error": "invalid_contract"})
        except Exception:
            # A callback may have committed before throwing. The caller retries
            # only with a new nonce and the SAME request/attempt/result identity.
            self._reply(503, {"error": "callback_outcome_unknown"})


class CallbackServer:
    """Explicit lifecycle; construction/import does not start a network server.

    `replay_guard(key, expires_at)` must atomically persist a first-use claim and
    return exactly True once. Use Store.claim_nonce rather than process memory in
    production. Callbacks must be short, durable local operations, not model or
    Feishu calls. A remote deployment needs an independently configured TLS relay.
    """

    def __init__(
        self,
        *,
        on_result: Callable[[dict[str, Any]], dict[str, Any]],
        on_started: Callable[[dict[str, Any]], dict[str, Any]],
        replay_guard: Callable[[str, float], bool],
        callback_key: str | bytes,
        host: str = "127.0.0.1",
        port: int = 0,
        allow_non_loopback: bool = False,
        max_body_bytes: int = 65536,
        max_handlers: int = 16,
        request_timeout: float = 5.0,
        max_clock_skew: float = 300,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not isinstance(host, str) or not host or (not allow_non_loopback and not is_loopback_host(host)):
            raise ContractError("non-loopback callback binding requires explicit configuration")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ContractError("invalid callback port")
        if isinstance(max_body_bytes, bool) or not isinstance(max_body_bytes, int) or not 1 <= max_body_bytes <= 65536:
            raise ContractError("invalid callback body limit")
        if isinstance(max_handlers, bool) or not isinstance(max_handlers, int) or not 1 <= max_handlers <= 64:
            raise ContractError("invalid callback concurrency")
        for value in (request_timeout, max_clock_skew):
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
                raise ContractError("invalid callback time limit")
        if request_timeout > 60 or max_clock_skew > 3600:
            raise ContractError("invalid callback time limit")
        if not all(callable(f) for f in (on_result, on_started, replay_guard, clock)):
            raise ContractError("callback ports must be callable")
        self.on_result, self.on_started = on_result, on_started
        self.replay_guard = replay_guard
        self._key = secret_bytes(callback_key)
        self._host = "127.0.0.1" if host.lower() == "localhost" else host
        self._port = port
        self.max_body_bytes, self.max_handlers = max_body_bytes, max_handlers
        self.request_timeout, self.max_clock_skew = float(request_timeout), float(max_clock_skew)
        self.clock = clock
        self._server: _BoundedServer | None = None
        self._thread: threading.Thread | None = None
        self._lifecycle_lock = threading.Lock()

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None:
            raise RuntimeError("callback server is not running")
        address = self._server.server_address
        return str(address[0]), int(address[1])

    def start(self) -> CallbackServer:
        with self._lifecycle_lock:
            if self._server is not None:
                return self
            server = _BoundedServer((self._host, self._port), self)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, name="am-grok-callback", daemon=True)
            try:
                thread.start()
            except RuntimeError:
                server.server_close()
                raise
            self._server, self._thread = server, thread
        return self

    def stop(self) -> None:
        with self._lifecycle_lock:
            server, thread = self._server, self._thread
            if server is None:
                return
            self._server, self._thread = None, None
            server.abort_connections()
            server.shutdown()
            server.server_close()
            if thread is not None:
                thread.join(timeout=1.0)

    def __enter__(self) -> CallbackServer:
        return self.start()

    def __exit__(self, *args: Any) -> None:
        self.stop()
