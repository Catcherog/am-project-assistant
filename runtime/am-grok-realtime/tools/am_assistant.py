#!/usr/bin/env python3
"""Owner-only CLI for GPT dot / Grok bot to use the AM operator API.

Configuration contains only ``base_url`` and ``token_env``.  The token is read
from that environment variable, never from CLI arguments or message content.
Use a separate owner process: do not pass this credential to the native Grok
worker handling colleagues' conversations.  This client never calls Feishu or
executes an approved action directly; it only submits the exact API decision.
"""
from __future__ import annotations

import argparse
from http.client import HTTPException
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


PREFIX = "/v1/assistant"
JSON_COMMAND_FIELDS = {
    "decide-task": ("accept", "source_ref", "scope_digest"),
    "task-state": ("state", "source_ref", "verification_ref"),
    "add-step": ("event_id", "title", "assignee_id", "source_ref"),
    "fact": ("project_id", "field", "value", "event_id", "source_ref", "status", "category",
             "verification_ref", "observed_at", "supersedes_fact_id"),
    "risk": ("event_id", "text", "source_ref", "task_id", "priority", "needs_owner"),
}
TASK_STATES = {"pending", "in_progress", "waiting_feedback", "awaiting_owner", "reported_complete",
               "completed", "blocked", "cancelled"}


class ClientError(Exception):
    def __init__(self, category, code=2):
        self.category, self.code = category, code
        super().__init__(category)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def _config(path, environ):
    try:
        config = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ClientError("config_error") from None
    if not isinstance(config, dict) or set(config) != {"base_url", "token_env"}:
        raise ClientError("config_error")
    base, token_env = config["base_url"], config["token_env"]
    if (not isinstance(base, str) or not isinstance(token_env, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token_env)
            or any(ord(char) <= 32 or char == "\\" for char in base)):
        raise ClientError("config_error")
    try:
        parsed = urlsplit(base)
        hostname, port = parsed.hostname, parsed.port
    except ValueError:
        raise ClientError("config_error") from None
    if (parsed.scheme not in {"http", "https"} or not hostname or parsed.username is not None
            or parsed.password is not None or parsed.query or parsed.fragment
            or parsed.path not in {"", "/"} or port == 0):
        raise ClientError("config_error")
    if parsed.scheme == "http":
        try:
            loopback = ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            loopback = hostname.lower() == "localhost"
        if not loopback:
            raise ClientError("insecure_non_loopback_url")
    token = environ.get(token_env, "")
    if not isinstance(token, str) or not token or any(ord(char) <= 32 or ord(char) > 126 for char in token):
        raise ClientError("missing_or_invalid_token")
    return base.rstrip("/"), token


def _identifier(value):
    if (not isinstance(value, str) or not value or value in {".", ".."}
            or any(char in value for char in "/\\?#")
            or any(ord(char) < 32 for char in value)):
        raise ClientError("invalid_identifier")
    return quote(value, safe="")


def _parser():
    parser = argparse.ArgumentParser(description="Use AM through the owner operator API.")
    parser.add_argument("--config", required=True, help="JSON with base_url and token_env only")
    parser.add_argument("--timeout", type=float, default=30, help="HTTP timeout in seconds (default: 30)")
    parser.add_argument("--json-stdin", action="store_true", help="Read a JSON request body from stdin")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "tasks", "reports", "actions"):
        commands.add_parser(name)
    for name in ("task", "project"):
        command = commands.add_parser(name)
        command.add_argument("id")
    submit = commands.add_parser("submit")
    submit.add_argument("--origin", choices=("gpt_dot", "grok_bot"))
    for field in ("event-id", "text", "project-id", "task-id"):
        submit.add_argument("--" + field)
    submit.add_argument("--json-stdin", action="store_true", default=argparse.SUPPRESS)
    for name in ("approve", "reject"):
        command = commands.add_parser(name)
        command.add_argument("id")
        command.add_argument("--version", type=int)
        command.add_argument("--digest")
        command.add_argument("--json-stdin", action="store_true", default=argparse.SUPPRESS)
    followup = commands.add_parser("followup")
    for field in ("task-id", "recipient-id", "target-chat", "text", "event-id", "source-ref"):
        followup.add_argument("--" + field)
    followup.add_argument("--due-at", type=float)
    followup.add_argument("--json-stdin", action="store_true", default=argparse.SUPPRESS)
    for name in JSON_COMMAND_FIELDS:
        command = commands.add_parser(name, help="Read this operation's fields from JSON stdin only")
        if name in {"decide-task", "task-state", "add-step"}:
            command.add_argument("id")
        command.add_argument("--json-stdin", action="store_true", default=argparse.SUPPRESS)
    return parser


def _reject_nonfinite(_):
    raise ValueError("non-finite JSON number")


def _input_body(args, stdin, fields):
    body = {}
    if args.json_stdin:
        try:
            body = json.load(stdin, parse_constant=_reject_nonfinite)
        except (ValueError, UnicodeError, OSError, RecursionError):
            raise ClientError("invalid_json_input") from None
        if not isinstance(body, dict) or set(body) - set(fields):
            raise ClientError("unsupported_request_fields")
    for field in fields:
        value = getattr(args, field, None)
        if value is not None:
            if field in body and body[field] != value:
                raise ClientError("conflicting_request_fields")
            body[field] = value
    return body


def _required_text(body, names):
    for name in names:
        if not isinstance(body.get(name), str) or not body[name].strip():
            raise ClientError("missing_or_invalid_" + name)


def _optional_text(body, names, *, nullable=()):
    for name in names:
        if name in body and not isinstance(body[name], str) and not (name in nullable and body[name] is None):
            raise ClientError("invalid_" + name)


def _choice(body, name, choices, default=None):
    value = body.get(name, default)
    if not isinstance(value, str) or value not in choices:
        raise ClientError("missing_or_invalid_" + name)


def _finite_nonnegative(value):
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _json_request(args, stdin):
    command = args.command
    if not args.json_stdin:
        raise ClientError("json_stdin_required")
    body = _input_body(args, stdin, JSON_COMMAND_FIELDS[command])
    _required_text(body, ("source_ref",))
    if command == "decide-task":
        if type(body.get("accept")) is not bool:
            raise ClientError("missing_or_invalid_accept")
        if "scope_digest" in body:
            _required_text(body, ("scope_digest",))
        suffix = "/tasks/" + _identifier(args.id) + "/decision"
    elif command == "task-state":
        _choice(body, "state", TASK_STATES)
        _optional_text(body, ("verification_ref",))
        if body["state"] == "completed":
            _required_text(body, ("verification_ref",))
        suffix = "/tasks/" + _identifier(args.id) + "/state"
    elif command == "add-step":
        _required_text(body, ("event_id", "title"))
        _optional_text(body, ("assignee_id",))
        suffix = "/tasks/" + _identifier(args.id) + "/steps"
    elif command == "fact":
        _required_text(body, ("project_id", "field", "event_id"))
        if "value" not in body:
            raise ClientError("missing_value")
        _choice(body, "status", {"plan", "reported", "verified", "conflict"}, "reported")
        _choice(body, "category", {"plan", "progress", "blocker", "todo", "risk"}, "progress")
        _optional_text(body, ("verification_ref", "supersedes_fact_id"), nullable=("supersedes_fact_id",))
        if body.get("status") == "verified":
            _required_text(body, ("verification_ref",))
        if body.get("category") == "plan" and body.get("status", "reported") not in {"plan", "conflict"}:
            raise ClientError("invalid_plan_status")
        if body.get("observed_at") is not None:
            timestamp = body["observed_at"]
            if not _finite_nonnegative(timestamp):
                raise ClientError("invalid_observed_at")
        suffix = "/facts"
    else:
        _required_text(body, ("event_id", "text"))
        _optional_text(body, ("task_id", "needs_owner"))
        if "priority" in body:
            _choice(body, "priority", {"P0", "P1", "P2", "P3"})
        suffix = "/risks"
    return "POST", PREFIX + suffix, body


def _request(args, stdin):
    command = args.command
    if command in JSON_COMMAND_FIELDS:
        return _json_request(args, stdin)
    if command in {"status", "tasks", "reports", "actions", "task", "project"}:
        if args.json_stdin:
            raise ClientError("read_command_has_no_request_body")
        suffix = {"task": "tasks", "project": "projects"}.get(command, command)
        path = PREFIX + "/" + suffix
        if command in {"task", "project"}:
            path += "/" + _identifier(args.id)
        return "GET", path, None
    if command == "submit":
        body = _input_body(args, stdin, ("origin", "event_id", "text", "project_id", "task_id"))
        _required_text(body, ("origin", "event_id", "text"))
        if body["origin"] not in {"gpt_dot", "grok_bot"}:
            raise ClientError("invalid_origin")
        for optional in ("project_id", "task_id"):
            if optional in body and not isinstance(body[optional], str):
                raise ClientError("invalid_" + optional)
        return "POST", PREFIX + "/tasks", body
    if command in {"approve", "reject"}:
        body = _input_body(args, stdin, ("version", "digest"))
        if isinstance(body.get("version"), bool) or not isinstance(body.get("version"), int) or body["version"] < 1:
            raise ClientError("missing_or_invalid_version")
        _required_text(body, ("digest",))
        body["approve"] = command == "approve"
        return "POST", PREFIX + "/actions/" + _identifier(args.id) + "/decision", body
    if command == "followup":
        fields = ("task_id", "recipient_id", "target_chat", "text", "due_at", "event_id", "source_ref")
        body = _input_body(args, stdin, fields)
        _required_text(body, tuple(field for field in fields if field != "due_at"))
        due = body.get("due_at")
        if not _finite_nonnegative(due):
            raise ClientError("missing_or_invalid_due_at")
        return "POST", PREFIX + "/followups", body
    raise ClientError("unsupported_command")


def _redact(value, token):
    if isinstance(value, str):
        return value.replace(token, "[redacted]")
    if isinstance(value, list):
        return [_redact(item, token) for item in value]
    if isinstance(value, dict):
        return {_redact(key, token): _redact(item, token) for key, item in value.items()}
    return value


def main(argv=None, *, stdin=None, stdout=None, stderr=None, environ=None):
    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr
    environ = os.environ if environ is None else environ
    args = _parser().parse_args(argv)
    try:
        if not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ClientError("invalid_timeout")
        base, token = _config(args.config, environ)
        method, path, body = _request(args, stdin)
        try:
            data = None if body is None else json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise ClientError("invalid_json_input") from None
        request = Request(base + path, data=data, method=method,
                          headers={"Authorization": "Bearer " + token, "Accept": "application/json",
                                   "Content-Type": "application/json; charset=utf-8"})
        # A redirect must never forward the operator credential to another origin.
        # Local owner calls also bypass environment-configured HTTP proxies.
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        try:
            with opener.open(request, timeout=args.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            exc.close()
            raise ClientError("http_error:" + str(exc.code), 3) from None
        except (URLError, HTTPException, TimeoutError, socket.timeout, ConnectionError, OSError, UnicodeError):
            raise ClientError("network_error", 3) from None
        try:
            answer = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            raise ClientError("invalid_api_response", 4) from None
        print(json.dumps(_redact(answer, token), ensure_ascii=False, indent=2), file=stdout)
        return 0
    except ClientError as exc:
        print(exc.category, file=stderr)
        return exc.code


if __name__ == "__main__":
    raise SystemExit(main())
