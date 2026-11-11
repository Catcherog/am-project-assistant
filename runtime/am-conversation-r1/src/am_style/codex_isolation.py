"""Read-only Codex isolation diagnosis; never creates a successful capability proof."""
from __future__ import annotations

import json
from .contracts import canonical, digest
from .providers import FIXED_OVERRIDES


def _expand(node, root, seen=()):
    if not isinstance(node, dict):
        return []
    ref = node.get("$ref")
    if ref:
        if not isinstance(ref, str) or not ref.startswith("#/") or ref in seen:
            return []
        resolved = root
        try:
            for part in ref[2:].split("/"):
                resolved = resolved[part.replace("~1", "/").replace("~0", "~")]
        except (KeyError, TypeError):
            return []
        return _expand(resolved, root, seen + (ref,))
    nodes = [node]
    for key in ("anyOf", "oneOf", "allOf"):
        for child in node.get(key, []):
            nodes.extend(_expand(child, root, seen))
    return nodes


def explicit_schema_path(schema, dotted_key):
    """An arbitrary additionalProperties entry is not named support evidence."""
    nodes = _expand(schema, schema)
    for part in dotted_key.split("."):
        children = []
        for node in nodes:
            child = node.get("properties", {}).get(part)
            if child is not None:
                children.extend(_expand(child, schema))
        nodes = children
    return bool(nodes)


def propose_isolation(*, cli_version, model, schema, previous_observation):
    if not cli_version or not model or not isinstance(schema, dict):
        raise ValueError("explicit CLI version, model and actual schema are required")
    relevant = ["features.code_mode.enabled", "features.multi_agent", "features.apps",
                "features.memories", "tools.view_image"]
    support = {key: explicit_schema_path(schema, key) for key in relevant}
    previous_tools = previous_observation.get("observed_tool_names")
    if not isinstance(previous_tools, list):
        raise ValueError("previous observed tool inventory is required")
    return {
        "schema": "am-codex-isolation-plan/4", "cli_version": cli_version, "model": model,
        "schema_sha256": digest(schema), "known_extra_switch_support": support,
        "candidate_overrides": dict(FIXED_OVERRIDES),
        "candidate_switches_may_be_probed": all(support.values()),
        "previous_observed_tools": previous_tools,
        "effective_inventory_verified": False, "sufficient_for_generation": False,
        "required_next_evidence": [
            "actual CLI version, binary and effective configuration hashes",
            "same selected model and authoritative model-catalog hash",
            "request captured before sending with actual provider and no provider override",
            "complete empty tool inventory, not only absence of tool-call events",
            "fresh proof bound to new overrides, request and model; original policy preserved",
        ],
        "note": "A local mock, configuration schema or this plan cannot certify the real tool surface.",
    }


def inspect_tool_inventory(payload):
    """Report complete supplied tool definitions. Unknown tools remain visible."""
    if not isinstance(payload, dict) or not isinstance(payload.get("tools"), list):
        raise ValueError("request must explicitly contain its complete tools array")
    names = []
    def visit(tool, prefix=""):
        if not isinstance(tool, dict):
            raise ValueError("unrecognized tool definition")
        kind, name = tool.get("type"), tool.get("name")
        if kind == "namespace":
            if not isinstance(name, str) or not isinstance(tool.get("tools"), list):
                raise ValueError("unrecognized namespace")
            if not tool["tools"]: names.append(prefix + name + ".<empty namespace>")
            for child in tool["tools"]: visit(child, prefix + name + ".")
        elif isinstance(name, str):
            names.append(prefix + name)
        elif isinstance(tool.get("function"), dict) and isinstance(tool["function"].get("name"), str):
            names.append(prefix + tool["function"]["name"])
        else:
            names.append(prefix + str(kind or "<unknown-tool>"))
    for tool in payload["tools"]:
        visit(tool)
    # A managed agent/harness can add more tools outside this array. This helper
    # cannot certify its absence, and therefore never emits a ready capability.
    return {"declared_tool_names": names, "declared_tool_count": len(payload["tools"]),
            "request_sha256": digest(payload), "full_effective_surface_verified": False,
            "has_uninspected_harness": any(k in payload for k in ("agent", "harness", "environment")),
            "sufficient_for_generation": False}
