"""Verbatim R10 parser declarations for isolated host compatibility tests.

Not production code; production imports am_harness.controls from the host.
Source SHA256: 25b8458ecffb59b99bf8bfa1034933310bb6871d9614138e754d148c87ea08a7
"""
from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class ControlCommand:
    operation: str
    replacement: str = ""
    target_run_id: str | None = None


_CANCEL_CURRENT = re.compile(r"(?:停止当前任务|取消当前任务|停止当前生成|停止生成)[。！!\s]*")


_CANCEL_ID = re.compile(r"(?:停止任务|取消任务)\s+([A-Za-z0-9_-]{1,128})[。！!\s]*")


_REVISE = re.compile(r"(?:更正当前任务|修改当前任务|当前任务改为)[：:]\s*(.+)", re.DOTALL)


_REVISE_ID = re.compile(r"(?:更正任务|修改任务)\s+([A-Za-z0-9_-]{1,128})[：:]\s*(.+)", re.DOTALL)


def parse_control(text: str) -> ControlCommand | None:
    if not isinstance(text, str) or not text.strip() or len(text) > 6200:
        return None
    value = text.strip()
    if _CANCEL_CURRENT.fullmatch(value):
        return ControlCommand("cancel")
    explicit = _CANCEL_ID.fullmatch(value)
    if explicit:
        return ControlCommand("cancel", target_run_id=explicit[1])
    revised = _REVISE.fullmatch(value)
    if revised and revised[1].strip() and len(revised[1].strip()) <= 6000:
        return ControlCommand("revise", revised[1].strip())
    explicit_revision = _REVISE_ID.fullmatch(value)
    if explicit_revision and explicit_revision[2].strip() and len(explicit_revision[2].strip()) <= 6000:
        return ControlCommand("revise", explicit_revision[2].strip(), explicit_revision[1])
    return None
