"""Adapt the real AM ToolRegistry to optional bounded read batches.

The caller supplies a trusted AccessContext and original AM authorizer. Tool
schemas and project checks stay in the existing registry; no model can supply
parallel-safety declarations, identity, approval, or elevated permissions.
"""
from __future__ import annotations

from .tool_policy import ToolSpec


class AmRegistryAdapter:
    def __init__(self, registry, ctx, event_id: str, *, authorize,
                 independent_read_tools: frozenset[str] = frozenset(), max_result_bytes: int = 16384):
        if not callable(authorize) or not isinstance(event_id, str) or not event_id:
            raise ValueError("AM_TOOL_ADAPTER_INVALID")
        self.registry, self.ctx, self.event_id, self.authorizer = registry, ctx, event_id, authorize
        self.parallel = frozenset(independent_read_tools)
        self.max_result_bytes = max_result_bytes

    def resolve(self, name, arguments):
        tool = self.registry.resolve(self.ctx, name, dict(arguments))
        if name in self.parallel and tool.effect != "read":
            raise ValueError("PARALLEL_DECLARATION_NOT_READ_ONLY")
        def handler(call):
            # Resolve again at actual execution, so registry/policy mutation
            # cannot turn an earlier read declaration into a different effect.
            current = self.registry.resolve(self.ctx, call.name, dict(call.arguments))
            if current is not tool:
                raise ValueError("TOOL_REGISTRATION_CHANGED")
            return current.handler(self.ctx, dict(call.arguments), self.event_id)
        return ToolSpec(tool.name, tool.effect, handler,
                        parallel_safe=tool.name in self.parallel,
                        independent=tool.name in self.parallel,
                        max_result_bytes=self.max_result_bytes)

    def authorize(self, spec, call) -> bool:
        tool = self.registry.resolve(self.ctx, call.name, dict(call.arguments))
        if tool.effect != spec.effect:
            return False
        operation = "read" if tool.effect == "read" else "manage" if tool.owner_only else "report"
        self.authorizer(self.ctx, operation, dict(call.arguments))
        return True
