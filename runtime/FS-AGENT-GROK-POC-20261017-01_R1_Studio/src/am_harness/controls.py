"""Authenticated out-of-band control; never waits for the conversation/model lock.

The host must validate ingress identity and current permissions before invoking
this module. Bare '取消' retains its existing AM draft-action meaning.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable, Any

from .store import DurableStore, Scope, RunSnapshot, StoreError


@dataclass(frozen=True)
class ControlCommand:
    operation: str
    replacement: str = ""
    target_run_id: str | None = None


@dataclass(frozen=True)
class ControlReply:
    operation: str
    status: str
    text: str
    run_id: str | None = None
    revision: int | None = None
    cancel_epoch: int | None = None


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


class IngressController:
    """Only authenticated ingress may construct Scope and invoke handle().

    authorize(scope, operation) must raise on denial. on_change runs inside the
    same SQLite transaction as cancellation/revision, and may only do local SQL
    (e.g. reject pending original-AM outbox rows); it must not send anything.
    """
    def __init__(self, store: DurableStore, *, authorize: Callable[[Scope, str], None],
                 on_change: Callable[..., Any] | None = None,
                 execution_seconds: float = 120.0):
        if not callable(authorize) or not 0 < execution_seconds <= 120:
            raise ValueError("CONTROL_CONFIGURATION_INVALID")
        self.store, self.authorize = store, authorize
        self.on_change, self.execution_seconds = on_change, execution_seconds

    def handle(self, scope: Scope, event_id: str, text: str) -> ControlReply | None:
        command = parse_control(text)
        if command is None:
            return None
        self.authorize(scope, "control")
        noop = self.store.replay_control_noop(scope, event_id, command.operation,
                    command.replacement, target_run_id=command.target_run_id)
        if noop:
            return self._noop(command.operation, noop)
        previous = self.store.replay_control(scope, event_id, command.operation,
                    command.replacement, target_run_id=command.target_run_id)
        if previous:
            return ControlReply(command.operation, "already_handled", "这条控制消息已处理，不会重复改变任务。",
                                previous.id, previous.revision, previous.cancel_epoch)
        if command.target_run_id:
            run = self.store.get(scope, command.target_run_id)
        else:
            current = self.store.active(scope)
            if not current:
                self.store.record_control_noop(scope, event_id, command.operation,
                    command.replacement, reason="no_active_run")
                return self._noop(command.operation, "no_active_run")
            if len(current) != 1:
                self.store.record_control_noop(scope, event_id, command.operation,
                    command.replacement, reason="clarification")
                return self._noop(command.operation, "clarification")
            run = current[0]
        if command.operation == "cancel":
            updated = self.store.cancel(scope, event_id, run.id, on_change=self.on_change)
            text = "已停止这项任务的后续处理，旧结果将不再进入待发送队列。已经提交的操作仍需按原记录核对。"
        else:
            # Corrections are new immutable task revisions. Original source TTL
            # is never extended by a correction or a summary.
            deadline = min(run.expires_at, self.store.clock() + self.execution_seconds)
            updated = self.store.revise(scope, event_id, run.id, command.replacement,
                                        deadline=deadline, on_change=self.on_change)
            text = "已按更正后的要求重新排队，旧版本的后续结果将被忽略。"
        return ControlReply(command.operation, updated.state, text, updated.id,
                            updated.revision, updated.cancel_epoch)

    @staticmethod
    def _noop(operation, status):
        pattern = "更正任务 任务编号：新要求" if operation == "revise" else "取消任务 任务编号"
        text = ("当前没有正在排队或处理的任务。" if status == "no_active_run" else
                "当前有多个任务，请使用“" + pattern + "”指定要处理的任务。")
        return ControlReply(operation, status, text)
