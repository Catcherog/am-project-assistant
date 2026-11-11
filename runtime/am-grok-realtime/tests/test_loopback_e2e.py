"""Exercise signed HTTP callbacks, durable core, duplicate ingress and sender."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


DEMO = Path(__file__).resolve().parents[1] / "tools" / "run_loopback_demo.py"
spec = importlib.util.spec_from_file_location("am_grok_loopback_demo", DEMO)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)


@pytest.mark.parametrize("chat_type", ["p2p", "group"])
def test_loopback_end_to_end_keeps_effects_unique_and_metrics_synthetic(chat_type):
    report = module.run_demo(chat_type=chat_type)
    assert report["result"] == "PASS"
    assert report["simulation"] is True
    assert report["real_grok_calls"] == report["real_feishu_sends"] == 0
    assert report["counts"] == {
        "ingress_attempts": 2, "accepted_unique_requests": 1, "duplicate_ingress": 1,
        "signed_webhook_posts": 1, "signed_callback_posts": 3, "synthetic_reply_sends": 1,
    }
    assert all(report["checks"].values())
    assert report["temporary_database_removed"] is True
    assert report["phone_visible_at"] is None
    assert report["stop_report"]["scheduler_stopped"] is True
    assert report["stop_report"]["ports_in_flight"] == 0
    group = report["latency_report"]["groups"]["synthetic"]
    assert group["latency_verdict"] == "SIMULATION_ONLY"
    assert group["verified_visible_answers"] == 0
    assert group["event_to_visible_p95_ms"] is None
    assert all(value >= 0 for value in report["observed_local_milliseconds"].values())


def test_loopback_cli_writes_report_without_persisting_database_or_keys(tmp_path):
    target = tmp_path / "demo.json"
    result = subprocess.run([sys.executable, str(DEMO), "--output", str(target)],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(target.read_text())
    assert report["result"] == "PASS" and report["temporary_database_removed"] is True
    assert list(tmp_path.iterdir()) == [target]
    assert "X-AM-Signature" not in target.read_text()
    assert "signing_key" not in target.read_text()
    assert "callback_key" not in target.read_text()
    assert "localhost" not in target.read_text()
