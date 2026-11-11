"""Local Grok CLI backend. No Feishu client and no shared-model pause."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


class GrokError(RuntimeError):
    """Safe code only. Do not attach CLI stderr or prompt text."""


class GrokBackend:
    def __init__(self, binary=None, *, timeout=90):
        self.binary = str(binary or Path.home() / ".local/bin/grok")
        self.timeout = timeout

    def complete(self, prompt: str) -> str:
        if os.environ.get("MOCK_GROK") == "1":
            return self._mock(prompt)
        if not isinstance(prompt, str) or not prompt.strip():
            raise GrokError("GROK_UNAVAILABLE")
        binary = self.binary
        if not os.access(binary, os.X_OK):
            raise GrokError("GROK_UNAVAILABLE")
        work = tempfile.mkdtemp(prefix="grok-route-")
        command = [
            binary, "-p", prompt[:50000],
            "--disable-web-search", "--no-subagents",
            "--max-turns", "1", "--output-format", "plain",
        ]
        try:
            try:
                proc = subprocess.run(
                    command, capture_output=True, text=True, timeout=self.timeout,
                    cwd=work, check=False,
                )
            except subprocess.TimeoutExpired:
                raise GrokError("GROK_TIMEOUT") from None
            except OSError:
                raise GrokError("GROK_UNAVAILABLE") from None
        finally:
            shutil.rmtree(work, ignore_errors=True)
        if proc.returncode != 0 or not (proc.stdout or "").strip():
            raise GrokError("GROK_UNAVAILABLE")
        return proc.stdout.strip()

    @staticmethod
    def _mock(prompt: str) -> str:
        mode = os.environ.get("MOCK_GROK_MODE", "ok")
        if mode == "error":
            raise GrokError("GROK_UNAVAILABLE")
        if mode == "need_owner":
            payload = {"status": "need_owner", "reply_text": "这个需要和示例负责人确认", "actions": []}
        elif mode == "user":
            payload = {
                "status": "need_confirm",
                "reply_text": "先不发给对方",
                "actions": [{
                    "type": "reply_message",
                    "as": "user",
                    "text": "明天下午前给结论",
                    "prefix_user_p2p": True,
                }],
            }
        elif mode == "bad":
            return "not-json"
        else:
            payload = {
                "status": "ok",
                "reply_text": "MOCK_GROK_OK",
                "actions": [{"type": "reply_message", "as": "bot", "text": "MOCK_GROK_OK"}],
            }
        # Touch the prompt so a mock run still proves the caller built one.
        if not isinstance(prompt, str):
            raise GrokError("GROK_UNAVAILABLE")
        return json.dumps(payload, ensure_ascii=False)
