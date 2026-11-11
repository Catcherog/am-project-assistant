from __future__ import annotations

import json
import re
import shutil
import sys
import time
import uuid
from pathlib import Path

from .config import BoundaryError, Config
from .local_api import LocalQwenClient
from .provider import ProcessRunner, Result, atomic_json
from .store import CODE_PROGRESS_MODEL


def build_doc_prompt(source: str, history: list[dict]) -> str:
    """Project only document content; the gateway owns scope and absolute TTL checks."""
    if not isinstance(source, str) or not source.strip() or len(source) > 2000:
        raise BoundaryError("DOC_INPUT_INVALID")
    if not isinstance(history, list) or len(history) > 8:
        raise BoundaryError("DOC_CONTEXT_INVALID")
    context = []
    for item in history:
        if (not isinstance(item, dict) or item.get("role") not in ("user", "assistant")
                or not isinstance(item.get("body"), str)):
            raise BoundaryError("DOC_CONTEXT_INVALID")
        context.append({"role": item["role"], "body": item["body"]})
    if sum(len(item["body"]) for item in context) > 20000:
        raise BoundaryError("DOC_CONTEXT_LIMIT")
    # Tools, routing, destinations and the Markdown template remain code-owned.
    # JSON preserves quoted/newline content without copying task or identity metadata.
    prompt = "用中文概括当前合成资料；历史仅参考，不新增事实。\n" + json.dumps(
        {"history": context, "source": source}, ensure_ascii=False, separators=(",", ":"))
    if len(prompt) > 30000:
        raise BoundaryError("PROMPT_INVALID")
    return prompt


def synthetic_progress_result() -> Result:
    """Render the existing fixed sample without contacting or claiming a model."""
    started = time.monotonic()
    project = "Aurora"
    total, completed, in_progress, blocked = 5, 3, 2, 0
    completion_percent = completed * 100 // total
    text = (f"合成项目 {project}：总计 {total} 项，已完成 {completed} 项，"
            f"进行中 {in_progress} 项，阻塞 {blocked} 项，完成率 {completion_percent}%。")
    return Result(text, CODE_PROGRESS_MODEL, time.monotonic() - started)


def api_request_unknown(directory: Path) -> bool:
    path = directory / "api-state.json"
    if not path.exists():
        return False  # The worker records in_flight before it can send the request.
    try:
        return json.loads(path.read_text()).get("state") not in ("response_complete", "not_dispatched")
    except (OSError, ValueError, AttributeError):
        return True


def discard_payload(directory: Path):
    # Keep only body-free process/request markers when the server's completion is unknown.
    for name in ("request.json", "stdout.tmp", "stderr.tmp"):
        (directory / name).unlink(missing_ok=True)


class LocalQwenProvider:
    def __init__(self, config: Config):
        self.config = config
        self.client = LocalQwenClient(config.api_base_url, config.api_model, config.api_model_digest)
        self.runner = ProcessRunner()

    def fingerprint(self):
        self.config.check_service()
        return self.client.fingerprint()

    def pin(self):
        path = self.config.root / "state" / "provider-pin.json"
        if not path.exists():
            raise BoundaryError("P1_REQUIRED")
        pin = json.loads(path.read_text())
        current = self.fingerprint()
        if not pin.get("real_p1_success") or any(pin.get(k) != v for k, v in current.items()):
            raise BoundaryError("LOCAL_PROVIDER_PIN_CHANGED")
        return pin

    def complete(self, prompt, *, run_id, timeout=120, on_start=None, discover=False, json_schema=None, max_tokens=None, native_chat=False):
        started = time.monotonic()
        self.last_timing=[]
        self.config.check_service()
        if not re.fullmatch(r"[a-f0-9]{32}", run_id):
            raise BoundaryError("INVALID_RUN_ID")
        if self.config.concurrency != 1:
            raise BoundaryError("QWEN_SINGLE_LANE_REQUIRED")
        if max_tokens is not None and (type(max_tokens) is not int or not 1 <= max_tokens <= 4096):
            raise BoundaryError("MODEL_OUTPUT_LIMIT_INVALID")
        pin_started=time.monotonic()
        try:self.fingerprint() if discover else self.pin()
        finally:self.last_timing.append({'stage':'provider_pin','seconds':time.monotonic()-pin_started})
        remaining = min(120, timeout) - (time.monotonic() - started)
        if remaining <= 0:
            raise BoundaryError("CONTENT_EXPIRED")
        directory = self.config.root / "tasks" / run_id
        directory.mkdir(mode=0o700)
        for name in ("home", "work", "tmp"):
            (directory / name).mkdir(mode=0o700)
        request = {
            "base_url": self.config.api_base_url, "model": self.config.api_model,
            "model_digest": self.config.api_model_digest, "max_tokens": max_tokens or self.config.api_max_tokens,
            "prompt": prompt, "timeout": remaining,
        }
        if json_schema is not None:
            request["json_schema"] = json_schema
        if native_chat:
            request["native_chat"] = True
        atomic_json(directory / "request.json", request)
        env = {"PATH": "/usr/bin:/bin", "HOME": str(directory / "home"),
               "TMPDIR": str(directory / "tmp"), "LANG": "en_US.UTF-8"}
        try:
            code, out, _ = self.runner.run(
                [sys.executable, "-I", "-m", "fs_agent.api_worker", str(directory / "request.json")],
                cwd=directory / "work", env=env, directory=directory, timeout=remaining, on_start=on_start)
            if api_request_unknown(directory):
                raise BoundaryError("STOP_UNKNOWN")
            data = json.loads(out)
            if code:
                error = data.get("error", "LOCAL_API_WORKER_FAILED")
                if not re.fullmatch(r"[A-Z_]{1,80}", error):
                    error = "LOCAL_API_WORKER_FAILED"
                raise BoundaryError(error)
            if data.get("model") != self.config.api_model or not isinstance(data.get("text"), str):
                raise BoundaryError("API_OUTPUT_SCHEMA_UNVERIFIED")
            return Result(data["text"], data["model"], time.monotonic() - started,data.get('usage'))
        finally:
            self.last_timing.extend(getattr(self.runner,'last_timing',[]))
            journal = directory / "process.json"
            self.last_timing.append({'stage':'stop_observation','process_stopped':
                journal.exists() and json.loads(journal.read_text()).get('state')=='stopped',
                'server_response_unknown':api_request_unknown(directory)})
            if journal.exists() and json.loads(journal.read_text()).get("state") != "stopped":
                raise BoundaryError("STOP_UNKNOWN")
            if api_request_unknown(directory):
                discard_payload(directory)
                # A killed HTTP client is not proof that shared-server inference has ended.
                raise BoundaryError("STOP_UNKNOWN")
            shutil.rmtree(directory)

    def p1(self):
        fingerprint = self.fingerprint()
        path = self.config.root / "state" / "provider-pin.json"
        if path.exists():
            self.pin()
        run_id = uuid.uuid4().hex
        result = self.complete("合成验证：2+3 等于多少？只回复数字 5，不使用工具。", run_id=run_id,
                               discover=not path.exists())
        if result.text != "5":
            raise BoundaryError("P1_SYNTHETIC_ASSERTION_FAILED")
        receipt = dict(fingerprint, run_id=run_id, elapsed=result.elapsed,
                       real_p1_success=True, completed_at=time.time())
        atomic_json(path, receipt)
        return receipt
