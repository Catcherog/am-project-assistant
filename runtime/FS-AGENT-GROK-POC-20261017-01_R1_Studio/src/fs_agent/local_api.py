"""Text-only local Ollama API. No proxies, redirects, credentials, tools or retries."""
from __future__ import annotations

import http.client
import json
import re
from urllib.parse import urlsplit

from .config import BoundaryError

MAX_RESPONSE = 2 * 1024 * 1024


def validate_endpoint(base_url: str) -> int:
    try:
        value = urlsplit(base_url)
        port = value.port
        if (value.scheme != "http" or value.hostname != "127.0.0.1" or not port
                or value.username or value.password or value.path != "/v1" or value.query or value.fragment):
            raise ValueError
        return port
    except (TypeError, ValueError):
        raise BoundaryError("LOCAL_LOOPBACK_API_REQUIRED") from None


class LocalQwenClient:
    def __init__(self, base_url, model, digest):
        self.base_url, self.port = base_url, validate_endpoint(base_url)
        self.model, self.digest = model, digest

    def request(self, path, body=None, *, timeout=10, phase=None):
        if timeout <= 0:
            raise BoundaryError("CONTENT_EXPIRED")
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            if phase:
                phase("in_flight")
            connection.request("GET" if body is None else "POST", path,
                               body=None if body is None else json.dumps(body, ensure_ascii=False).encode(),
                               headers={"Content-Type": "application/json", "Connection": "close"})
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE:
                raise BoundaryError("API_OUTPUT_LIMIT")
            if phase:
                phase("response_complete")
            if response.status != 200:
                error = {401: "AUTH_FAILED", 403: "AUTH_FAILED", 429: "RATE_LIMITED"}.get(
                    response.status, "LOCAL_API_REJECTED")
                raise BoundaryError(error)
            try:
                result = json.loads(raw)
            except ValueError:
                raise BoundaryError("API_OUTPUT_SCHEMA_UNVERIFIED") from None
            if not isinstance(result, dict):
                raise BoundaryError("API_OUTPUT_SCHEMA_UNVERIFIED")
            return result
        except ConnectionRefusedError:
            if phase:
                phase("not_dispatched")
            raise BoundaryError("LOCAL_API_UNAVAILABLE") from None
        except (OSError, http.client.HTTPException):
            raise BoundaryError("LOCAL_API_CONNECTION_LOST") from None
        finally:
            connection.close()

    def fingerprint(self):
        version = self.request("/api/version", timeout=5).get("version", "")
        models = self.request("/api/tags", timeout=5).get("models", [])
        found = [m for m in models if m.get("name") == self.model]
        if len(found) != 1:
            raise BoundaryError("LOCAL_MODEL_MISSING")
        model = found[0]
        if not re.fullmatch(r"[0-9a-f]{64}", self.digest) or model.get("digest") != self.digest:
            raise BoundaryError("LOCAL_MODEL_CHANGED")
        if not re.fullmatch(r"[0-9A-Za-z.+-]{1,80}", version):
            raise BoundaryError("API_VERSION_UNVERIFIED")
        return {"provider": "local_qwen_api", "base_url": self.base_url, "model": self.model,
                "model_digest": self.digest, "server_version": version}

    def complete(self, prompt, *, timeout=120, max_tokens=1024, phase=None, json_schema=None, native_chat=False):
        self.last_usage = None
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 30000:
            raise BoundaryError("PROMPT_INVALID")
        if native_chat:
            body = {"model": self.model, "messages": [{"role": "user", "content": prompt}],
                    "stream": False, "think": False, "tools": [], "keep_alive": "30m",
                    "options": {"num_ctx": 16384, "num_predict": max_tokens, "temperature": 0}}
            if json_schema is not None:
                if not isinstance(json_schema, dict) or len(json.dumps(json_schema)) > 20000:
                    raise BoundaryError("RESPONSE_SCHEMA_INVALID")
                body["format"] = json_schema
            data = self.request("/api/chat", body, timeout=timeout, phase=phase)
            try:
                message = data["message"]
                text = message["content"]
                if data["model"] != self.model:
                    raise BoundaryError("MODEL_CHANGED")
                if data.get("done_reason") == "length":
                    raise BoundaryError("MODEL_OUTPUT_LIMIT")
                if (data.get("done") is not True or data.get("done_reason") != "stop"
                        or message.get("role") != "assistant" or message.get("tool_calls")
                        or message.get("thinking") or not isinstance(text, str) or not text.strip()
                        or len(text) > 12000 or data.get("eval_count", 0) <= 0):
                    raise BoundaryError("API_RESULT_REJECTED")
            except (KeyError, TypeError, AttributeError):
                raise BoundaryError("API_OUTPUT_SCHEMA_UNVERIFIED") from None
            self.last_usage={'input_tokens':data.get('prompt_eval_count'),'output_tokens':data.get('eval_count'),
                             'total_duration_ns':data.get('total_duration'),'load_duration_ns':data.get('load_duration'),
                             'input_duration_ns':data.get('prompt_eval_duration'),'output_duration_ns':data.get('eval_duration')}
            return text.strip()
        body = {
            "model": self.model, "messages": [{"role": "user", "content": prompt}],
            "stream": False, "reasoning_effort": "none", "max_tokens": max_tokens,
            "temperature": 0, "tool_choice": "none", "tools": [],
        }
        if json_schema is not None:
            if not isinstance(json_schema, dict) or len(json.dumps(json_schema)) > 20000:
                raise BoundaryError("RESPONSE_SCHEMA_INVALID")
            body["response_format"] = {"type": "json_schema", "json_schema": {
                "name": "group_response", "strict": True, "schema": json_schema}}
        data = self.request("/v1/chat/completions", body, timeout=timeout, phase=phase)
        try:
            choices = data["choices"]
            choice = choices[0]
            message = choice["message"]
            text = message["content"]
            if data["model"] != self.model:
                raise BoundaryError("MODEL_CHANGED")
            if choice.get("finish_reason") == "length":
                raise BoundaryError("MODEL_OUTPUT_LIMIT")
            if (len(choices) != 1 or choice.get("finish_reason") != "stop"
                    or message.get("role") != "assistant" or message.get("tool_calls")
                    or not isinstance(text, str) or not text.strip() or len(text) > 12000
                    or data.get("usage", {}).get("completion_tokens", 0) <= 0):
                raise BoundaryError("API_RESULT_REJECTED")
        except (KeyError, TypeError, IndexError, AttributeError):
            raise BoundaryError("API_OUTPUT_SCHEMA_UNVERIFIED") from None
        self.last_usage={'input_tokens':data.get('usage',{}).get('prompt_tokens'),'output_tokens':data.get('usage',{}).get('completion_tokens')}
        return text.strip()
