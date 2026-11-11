"""Strict, read-only validation of a configured Meegle document target.

Only the response shapes listed here are supported. Configuration supplies an
expectation; platform attributes, field metadata and field values must prove it.
Unknown schemas are a diagnostic, never permission to guess another target.
"""
from __future__ import annotations

import json

from .config import BoundaryError
from .doc_logic import digest
from .project_catalog import business_ids


DOCUMENT_TYPES = frozenset(("charter", "change", "closure", "test"))
DOCUMENT_NAMES = {"项目章程": "charter", "项目更改申请": "change", "项目变更申请": "change",
                  "结项报告": "closure", "项目结项报告": "closure", "测试报告": "test"}
# These are deliberately narrow adapter contracts, not claims that every Meegle
# installation uses this vocabulary. An unsupported native schema stays blocked.
FIELD_TYPES = {
    "document_kind_field": frozenset(("text", "select")),
    "document_link_field": frozenset(("file",)),
    "project_field": frozenset(("work_item",)),
    "node_field": frozenset(("text", "select")),
    "approval_field": frozenset(("select",)),
    "delivery_mode_field": frozenset(("select",)),
}


class TargetValidationError(BoundaryError):
    """A content-free, reviewable reason why a target could not be proven."""

    def __init__(self, code, **diagnostic):
        super().__init__(code)
        self.diagnostic = diagnostic


def fingerprint(value):
    return digest(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def binding_config(value):
    if not isinstance(value, dict):
        raise TargetValidationError("BUSINESS_TARGET_CONFIG_INVALID")
    required = ("space", "project", "project_type", "project_code", "node", "deliverable", "type",
                "document_type", "document_kind_field", "document_link_field", "approval_field",
                "node_field", "project_field", "delivery_mode_field", "unreviewed_value", "url")
    missing = [key for key in required if not isinstance(value.get(key), str)
               or not value[key].strip() or value[key] != value[key].strip()]
    if missing:
        raise TargetValidationError("BUSINESS_TARGET_CONFIG_INCOMPLETE", missing_config=missing)
    if (value["document_type"] not in DOCUMENT_TYPES
            or business_ids(value["project_code"]) != [value["project_code"]]):
        raise TargetValidationError("BUSINESS_TARGET_CONFIG_INVALID", check="expected_identity")
    # Legacy aliases may agree with the new explicit fields, but never supply
    # their missing meaning and never silently override them.
    for legacy, current in (("document_field", "document_kind_field"),
                            ("attachment_field", "document_link_field")):
        if legacy in value and value[legacy] != value[current]:
            raise TargetValidationError("BUSINESS_TARGET_FIELD_CONFLICT", fields=[legacy, current])
    fields = [value[key] for key in FIELD_TYPES]
    if len(set(fields)) != len(fields):
        raise TargetValidationError("BUSINESS_TARGET_FIELD_CONFLICT", check="distinct_field_roles")
    mappings = value.get("document_type_bindings", {})
    if not isinstance(mappings, dict) or any(not isinstance(k, str) or not k or not isinstance(v, str) or v not in DOCUMENT_TYPES
                                           for k, v in mappings.items()):
        raise TargetValidationError("BUSINESS_TARGET_CONFIG_INVALID", check="document_type_bindings")
    return dict(value, attachment_field=value["document_link_field"])


def _scalar(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or str(value) == "":
        return None
    return str(value)


def _definitions(value):
    # The existing metadata reader allows grouped field definitions. A field
    # row itself is a leaf; do not mistake its options for more field rows.
    if isinstance(value, dict):
        if "field_key" in value:
            yield value
        else:
            for item in value.values():
                yield from _definitions(item)
    elif isinstance(value, list):
        for item in value:
            yield from _definitions(item)


def validate_metadata(binding, rows):
    wanted = {binding[role]: role for role in FIELD_TYPES}
    found = {}
    for field in _definitions(rows):
        key = field.get("field_key")
        if not isinstance(key, str) or key not in wanted:
            continue
        if key in found:
            raise TargetValidationError("BUSINESS_FIELD_METADATA_AMBIGUOUS", field_key=key)
        found[key] = field
    missing = sorted(set(wanted) - set(found))
    if missing:
        raise TargetValidationError("BUSINESS_FIELD_METADATA_MISSING", missing_fields=missing)
    verified = {}
    for key, role in wanted.items():
        field = found[key]
        typ = field.get("field_type")
        if not isinstance(typ, str) or typ not in FIELD_TYPES[role]:
            raise TargetValidationError("BUSINESS_FIELD_TYPE_UNSUPPORTED", field_key=key, role=role,
                                        observed_type=typ if isinstance(typ, str) else "unrecognized",
                                        supported_types=sorted(FIELD_TYPES[role]))
        definition = {"field_key": key, "field_type": typ}
        if typ == "select":
            options = field.get("options")
            if not isinstance(options, list) or not options or len(options) > 2000:
                raise TargetValidationError("BUSINESS_FIELD_CANDIDATES_UNVERIFIED", field_key=key)
            candidates = {}
            for option in options:
                value = _scalar(option.get("value")) if isinstance(option, dict) else None
                label = option.get("label") if isinstance(option, dict) else None
                if value is None or not isinstance(label, str) or not label.strip() or value in candidates:
                    raise TargetValidationError("BUSINESS_FIELD_CANDIDATES_UNVERIFIED", field_key=key)
                candidates[value] = label
            definition["candidates"] = candidates
        verified[key] = definition
    return verified


def field_value(fields, metadata, key):
    if key not in fields:
        raise TargetValidationError("BUSINESS_TARGET_FIELD_MISSING", field_key=key)
    raw = fields[key]
    if metadata[key]["field_type"] == "text":
        if not isinstance(raw, str) or not raw.strip():
            raise TargetValidationError("BUSINESS_FIELD_VALUE_UNVERIFIED", field_key=key)
        return raw, raw
    if metadata[key]["field_type"] == "select":
        value = _scalar(raw.get("value") if isinstance(raw, dict) else raw)
        candidates = metadata[key]["candidates"]
        if value not in candidates:
            raise TargetValidationError("BUSINESS_FIELD_CANDIDATE_MISMATCH", field_key=key)
        label = candidates[value]
        if isinstance(raw, dict) and "label" in raw and raw["label"] != label:
            raise TargetValidationError("BUSINESS_FIELD_CANDIDATE_MISMATCH", field_key=key)
        return value, label
    raise TargetValidationError("BUSINESS_FIELD_TYPE_UNSUPPORTED", field_key=key)


def project_identity(binding, project):
    attr = project.get("work_item_attribute", {})
    if (not isinstance(attr, dict) or _scalar(attr.get("work_item_id")) != binding["project"]
            or not isinstance(attr.get("owned_project"), dict)
            or attr["owned_project"].get("key") != binding["space"]
            or not isinstance(attr.get("work_item_type"), dict)
            or attr["work_item_type"].get("key") != binding["project_type"]):
        raise TargetValidationError("BUSINESS_PROJECT_IDENTITY_CHANGED", check="project_attributes")
    title = attr.get("work_item_name")
    codes = business_ids(title) if isinstance(title, str) else []
    if len(codes) != 1:
        raise TargetValidationError("BUSINESS_PROJECT_CODE_UNVERIFIED", check="single_project_code")
    if codes[0] != binding["project_code"]:
        raise TargetValidationError("BUSINESS_PROJECT_CODE_MISMATCH", check="project_code")
    return attr


def target_identity(binding, fields, metadata):
    project_key = binding["project_field"]
    if project_key not in fields:
        raise TargetValidationError("BUSINESS_PROJECT_RELATION_MISSING", field_key=project_key)
    relation = fields[project_key]
    if not isinstance(relation, dict) or _scalar(relation.get("id")) is None:
        raise TargetValidationError("BUSINESS_PROJECT_RELATION_UNVERIFIED", field_key=project_key)
    if _scalar(relation["id"]) != binding["project"]:
        raise TargetValidationError("BUSINESS_TARGET_CHANGED", check="project_relation", field_key=project_key)
    node, _ = field_value(fields, metadata, binding["node_field"])
    if node != binding["node"]:
        raise TargetValidationError("BUSINESS_TARGET_CHANGED", check="node_relation", field_key=binding["node_field"])
    kind, label = field_value(fields, metadata, binding["document_kind_field"])
    mapped = binding.get("document_type_bindings", {}).get(kind)
    named = DOCUMENT_NAMES.get(label)
    if mapped and named and mapped != named:
        raise TargetValidationError("BUSINESS_DOCUMENT_KIND_MISMATCH", check="candidate_label_binding")
    observed = mapped or named
    if observed is None:
        raise TargetValidationError("BUSINESS_DOCUMENT_IDENTITY_UNVERIFIED", field_key=binding["document_kind_field"])
    if observed != binding["document_type"]:
        raise TargetValidationError("BUSINESS_DOCUMENT_KIND_MISMATCH", field_key=binding["document_kind_field"])
    # These selections affect protected-write semantics too. Unknown values or
    # stale labels must not get past target resolution.
    for role in ("approval_field", "delivery_mode_field"):
        field_value(fields, metadata, binding[role])
    return {"project_code": binding["project_code"], "doc_type": observed}, kind


def immutable_binding(binding, metadata, identity, kind):
    keys = ("space", "project", "project_type", "node", "deliverable", "type", "url",
            "unreviewed_value", *FIELD_TYPES)
    return fingerprint({"binding": {key: binding[key] for key in keys},
                        "metadata": metadata, "identity": identity, "kind": kind,
                        "document_type_bindings": binding.get("document_type_bindings", {})})
