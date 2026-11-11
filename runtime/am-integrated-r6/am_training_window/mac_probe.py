"""Read-only, secret-minimizing macOS memory diagnostics.

Admission remains the original R4 resource gate inside its coordination lock.
The page estimate here is diagnostic only: it must never become a launch gate.
No inference endpoint, process signal, purge, launchctl, or environment dump is
used. Process command arguments are deliberately not collected.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import re
import subprocess
import sys
from typing import Callable, Sequence


SCHEMA = "am.resource-diagnostic.v1"
COMMANDS: dict[str, tuple[str, ...]] = {
    "physical": ("/usr/sbin/sysctl", "-n", "hw.memsize"),
    "swap": ("/usr/sbin/sysctl", "-n", "vm.swapusage"),
    "pressure": ("/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"),
    "boot": ("/usr/sbin/sysctl", "-n", "kern.boottime"),
    "pages": ("/usr/bin/vm_stat",),
    "processes": ("/bin/ps", "-axo", "pid=,ppid=,uid=,rss=,comm="),
}
PROVIDER_NAMES = {"llama-server", "ollama", "ollama_llama_server"}


class ProbeError(RuntimeError):
    """A diagnostic failed; callers must not reinterpret it as a zero value."""


def run_readonly(argv: Sequence[str]) -> str:
    args = tuple(argv)
    if args not in COMMANDS.values():
        raise ProbeError("command_not_allowlisted")
    try:
        result = subprocess.run(
            args, shell=False, capture_output=True, text=True,
            check=False, timeout=5, env={"LC_ALL": "C", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ProbeError(type(exc).__name__) from exc
    if result.returncode:
        raise ProbeError(f"command_exit_{result.returncode}")
    if len(result.stdout) > 4 * 1024 * 1024:
        raise ProbeError("unexpected_output_size")
    return result.stdout


def parse_pages(text: str) -> dict[str, int]:
    match = re.search(r"page size of\s+(\d+)\s+bytes", text)
    if not match or int(match.group(1)) <= 0:
        raise ProbeError("vm_stat_page_size_missing")
    page_size = int(match.group(1))
    counters: dict[str, int] = {}
    for line in text.splitlines():
        match = re.fullmatch(r'\s*([^:]+):\s*(\d+)\.?\s*', line)
        if match:
            counters[match.group(1).strip().strip('"')] = int(match.group(2))
    required = ("Pages free", "Pages inactive", "Pages speculative")
    if any(key not in counters for key in required):
        raise ProbeError("vm_stat_required_counter_missing")
    result = {"page_size_bytes": page_size}
    fields = {
        "Pages free": "free_bytes",
        "Pages inactive": "inactive_bytes",
        "Pages speculative": "speculative_bytes",
        "Pages active": "active_bytes",
        "Pages wired down": "wired_bytes",
        "Pages purgeable": "purgeable_bytes",
        "Pages occupied by compressor": "compressor_physical_bytes",
        "Pages stored in compressor": "compressor_logical_bytes",
        "File-backed pages": "file_backed_bytes",
    }
    for counter, field in fields.items():
        if counter in counters:
            result[field] = counters[counter] * page_size
    result["diagnostic_free_inactive_speculative_bytes"] = sum(counters[k] for k in required) * page_size
    # These counters describe events since boot, not currently resident bytes.
    for key in ("Pageouts", "Swapins", "Swapouts"):
        if key in counters:
            result[key.lower() + "_pages_since_boot"] = counters[key]
    return result


def parse_swap(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    units = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    for key in ("total", "used", "free"):
        match = re.search(rf"\b{key}\s*=\s*(\d+(?:\.\d+)?)\s*([BKMGT])\b", text)
        if not match:
            raise ProbeError("swap_field_missing:" + key)
        result[key + "_bytes"] = int(float(match.group(1)) * units[match.group(2)])
    return result


def parse_processes(text: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        values = line.split(None, 4)
        if len(values) != 5:
            raise ProbeError("ps_row_shape_invalid")
        try:
            pid, ppid, uid, rss_kib = [int(v) for v in values[:4]]
        except ValueError as exc:
            raise ProbeError("ps_number_invalid") from exc
        if pid <= 0 or min(ppid, uid, rss_kib) < 0:
            raise ProbeError("ps_number_out_of_range")
        executable = Path(values[4]).name
        # comm can contain a path. Only its basename leaves this probe.
        rows.append({"pid": pid, "parent_pid": ppid, "uid": uid,
                     "rss_bytes": rss_kib * 1024, "executable": executable,
                     "provider_candidate": executable in PROVIDER_NAMES})
    return sorted(rows, key=lambda r: int(r["rss_bytes"]), reverse=True)


def collect_snapshot(
    runner: Callable[[Sequence[str]], str] = run_readonly,
    *, system: str | None = None, captured_at: datetime | None = None,
) -> dict[str, object]:
    now = captured_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("captured_at must have timezone")
    report: dict[str, object] = {
        "schema": SCHEMA, "captured_at": now.isoformat(),
        "system": system or platform.system(), "status": "not_collected",
        "resource_gate_status": "not_checked",
        "admission_authority": "original_R4_locked_resource_gate",
        "model_identity_verified": False, "commands": {}, "errors": {},
        "limitations": [
            "RSS is not a recoverable-memory estimate and is not additive across processes.",
            "A process name alone does not identify the deployed model or service owner.",
            "The diagnostic page estimate does not authorize training.",
            "Capture original R4 metrics before and after a separately authorized unload.",
        ],
    }
    if report["system"] != "Darwin":
        report["status"] = "unsupported_platform"
        return report
    raw: dict[str, str] = {}
    errors: dict[str, str] = {}
    for key, argv in COMMANDS.items():
        try:
            raw[key] = runner(argv)
            report["commands"][key] = {"exit": 0, "stdout_sha256": hashlib.sha256(raw[key].encode()).hexdigest()}
        except Exception as exc:
            # Do not write arbitrary exception strings (which may include local data).
            errors[key] = type(exc).__name__
            report["commands"][key] = {"exit": "failed"}
    parsers = {"physical": lambda s: int(s.strip()), "swap": parse_swap,
               "pressure": lambda s: int(s.strip()), "pages": parse_pages,
               "processes": parse_processes}
    parsed: dict[str, object] = {}
    for key, parser in parsers.items():
        if key in raw:
            try:
                parsed[key] = parser(raw[key])
            except Exception as exc:
                errors[key] = type(exc).__name__
    if "physical" in parsed and int(parsed["physical"]) <= 0:
        errors["physical"] = "out_of_range"
        del parsed["physical"]
    report["physical_bytes"] = parsed.get("physical")
    report["swap"] = parsed.get("swap")
    report["pressure_level"] = parsed.get("pressure")
    report["memory"] = parsed.get("pages")
    if "boot" in raw:
        boot = re.search(r"sec\s*=\s*(\d+)", raw["boot"])
        if boot:
            report["boot_identity_sha256"] = hashlib.sha256(boot.group(1).encode()).hexdigest()
        else:
            errors["boot"] = "parse_error"
    rows = parsed.get("processes", [])
    report["top_processes"] = rows[:15]
    report["provider_candidates"] = [r for r in rows if r["provider_candidate"]]
    report["errors"] = errors
    report["status"] = "partial" if errors else "complete"
    return report


def compare_snapshots(before: dict, after: dict) -> dict[str, object]:
    """Return observations without attributing memory changes to a process."""
    if any(r.get("schema") != SCHEMA or r.get("status") != "complete" for r in (before, after)):
        raise ProbeError("complete_matching_schema_snapshots_required")
    if before.get("physical_bytes") != after.get("physical_bytes") or not before.get("boot_identity_sha256") or before["boot_identity_sha256"] != after.get("boot_identity_sha256"):
        raise ProbeError("machine_or_boot_changed")
    b_time, a_time = (datetime.fromisoformat(r["captured_at"]) for r in (before, after))
    if b_time.tzinfo is None or a_time.tzinfo is None or a_time <= b_time:
        raise ProbeError("snapshot_order_invalid")
    b_keys = {(r["pid"], r["executable"]) for r in before["provider_candidates"]}
    a_keys = {(r["pid"], r["executable"]) for r in after["provider_candidates"]}
    key = "diagnostic_free_inactive_speculative_bytes"
    return {
        "schema": "am.resource-delta.v1", "elapsed_seconds": (a_time - b_time).total_seconds(),
        "diagnostic_page_estimate_change_bytes": after["memory"][key] - before["memory"][key],
        "swap_used_change_bytes": after["swap"]["used_bytes"] - before["swap"]["used_bytes"],
        "pressure_before": before["pressure_level"], "pressure_after": after["pressure_level"],
        "provider_rows_disappeared": [list(v) for v in sorted(b_keys - a_keys)],
        "provider_rows_appeared": [list(v) for v in sorted(a_keys - b_keys)],
        "pid_identity_note": "PID plus basename is observational only; PID reuse and model identity are not resolved.",
        "resource_gate_status": "not_checked", "causal_attribution": "not_established",
        "instruction": "Use original R4 locked metrics to determine launch eligibility; do not add RSS to available memory.",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    snap = sub.add_parser("snapshot", help="Read-only local macOS snapshot")
    snap.add_argument("--output", type=Path)
    compare = sub.add_parser("compare", help="Compare two saved read-only snapshots")
    compare.add_argument("before", type=Path)
    compare.add_argument("after", type=Path)
    compare.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action == "snapshot":
            result = collect_snapshot()
        else:
            result = compare_snapshots(json.loads(args.before.read_text()), json.loads(args.after.read_text()))
        encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
        if args.output:
            # A report file is the only write operation; never overwrite evidence.
            with args.output.open("x", encoding="utf-8") as output:
                output.write(encoded)
        else:
            sys.stdout.write(encoded)
        return 0 if result.get("status", "complete") == "complete" else 2
    except (OSError, ValueError, ProbeError) as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

