"""Verify a harness publication against the ORIGINAL AM ingress/outbox records.

This is a local SQL consistency check, not a permission refresher or an approval
receipt. The original dispatcher must still authorize the current user/project,
and DurableStore must check the current fence and content expiry in the same
transaction. Execution deadlines are intentionally not checked here: completing
the model turn and delivering its already committed result are different phases.

For a correction, keep the actual control message in am_inbox unchanged. Store
that real event id in am_conversation_outputs.event_id / am_outbox_events; store
the internal revision-specific engine id only in reply.event_id. Never invent an
inbox event or overwrite the original message body to make these checks pass.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from typing import Any, Mapping

from .am_adapter import engine_event_id
from .controls import parse_control
from .store import Scope, canonical


def _fail(code: str) -> None:
    raise RuntimeError(code)


def _hash(value: Any) -> str:
    # Same canonical JSON hash as am_conversation.legacy._hash, including the
    # quotes around a string. It is NOT sha256(body.encode()).
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _row(conn: sqlite3.Connection, query: str, parameters: tuple = ()) -> dict[str, Any] | None:
    cursor = conn.execute(query, parameters)
    value = cursor.fetchone()
    return None if value is None else dict(zip((column[0] for column in cursor.description), value))


def _rows(conn: sqlite3.Connection, query: str, parameters: tuple = ()) -> list[dict[str, Any]]:
    cursor = conn.execute(query, parameters)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _json(value: Any, kind: type, code: str) -> Any:
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        _fail(code)
    if type(decoded) is not kind:
        _fail(code)
    return decoded


def _finite_time(value: Any) -> bool:
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _strings(value: Any, *, nonempty: bool = False) -> bool:
    return (type(value) is list and (bool(value) or not nonempty)
            and all(isinstance(item, str) and item and len(item) <= 256 for item in value)
            and len(set(value)) == len(value))


def _thread(metadata: Mapping[str, Any]) -> str:
    thread = metadata.get("thread")
    if thread is None:
        return ""
    if not isinstance(thread, str) or len(thread) > 256:
        _fail("LEGACY_SOURCE_THREAD_INVALID")
    return thread


def _source(conn: sqlite3.Connection, event_id: str, scope: Scope) -> tuple[dict[str, Any], dict[str, Any]]:
    source = _row(conn, "SELECT * FROM am_inbox WHERE id=?", (event_id,))
    if source is None:
        _fail("LEGACY_SOURCE_EVENT_MISSING")
    meta = _row(conn, "SELECT metadata FROM am_source_meta WHERE id=?", (event_id,))
    metadata = {} if meta is None else _json(meta["metadata"], dict, "LEGACY_SOURCE_META_INVALID")
    identity = (source.get("tenant"), source.get("app"), source.get("user"), source.get("chat"), _thread(metadata))
    if identity != scope.values:
        _fail("LEGACY_SOURCE_SCOPE_MISMATCH")
    if source.get("chat_type") not in {"p2p", "group"} or not isinstance(source.get("body"), str) or not source["body"].strip():
        _fail("LEGACY_SOURCE_INVALID")
    if not _finite_time(source.get("created")) or not _finite_time(source.get("expires")) or source["created"] >= source["expires"]:
        _fail("LEGACY_SOURCE_TTL_INVALID")
    for key in ("parent", "root"):
        if metadata.get(key) is not None and (not isinstance(metadata[key], str) or len(metadata[key]) > 256):
            _fail("LEGACY_SOURCE_META_INVALID")
    return source, metadata


def deterministic_publication_key(run: Any) -> str:
    """Use as original notify(event_key=...); retain origin ids separately."""
    if (not isinstance(run.id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,180}", run.id)
            or type(run.revision) is not int or run.revision < 1
            or type(run.cancel_epoch) is not int or run.cancel_epoch < 0):
        _fail("LEGACY_PUBLICATION_KEY_INVALID")
    return f"harness:{run.id}:v{run.revision}:e{run.cancel_epoch}"


def verify_legacy_binding(conn: sqlite3.Connection, run: Any, outbox_id: str) -> None:
    """Fail closed unless this exact outbox is backed by the real AM event.

    Required baseline tables: am_harness_runs/events, am_conversation_outputs,
    am_inbox, am_source_meta, am_outbox_events, am_query_deliveries and outbox.
    Group replies also require am_group_delivery and, when quoting, am_group_refs.
    The same function is suitable before binding and again before dispatch.
    Missing deployment glue is an error, not a reason to skip verification.
    """
    if not conn.in_transaction:
        _fail("LEGACY_BINDING_TRANSACTION_REQUIRED")
    if not isinstance(outbox_id, str) or not outbox_id or len(outbox_id) > 256:
        _fail("LEGACY_OUTBOX_ID_INVALID")
    try:
        _verify(conn, run, outbox_id)
    except sqlite3.Error:
        _fail("LEGACY_BINDING_SCHEMA_UNAVAILABLE")


def _verify(conn: sqlite3.Connection, run: Any, outbox_id: str) -> None:
    if not isinstance(run.scope, Scope) or not isinstance(run.input_text, str):
        _fail("LEGACY_RUN_INPUT_UNAVAILABLE")
    deterministic_publication_key(run)
    current = _row(conn, "SELECT * FROM am_harness_runs WHERE id=? AND scope_key=?", (run.id, run.scope.key))
    if (current is None or current["event_id"] != run.event_id or current["revision"] != run.revision
            or current["cancel_epoch"] != run.cancel_epoch or current["lease_generation"] != run.lease_generation
            or current["input_hash"] != _hash(run.input_text) or current["purged"]):
        _fail("LEGACY_RUN_BINDING_MISMATCH")

    saved = _row(conn, "SELECT * FROM am_conversation_outputs WHERE outbox_id=?", (outbox_id,))
    outbox = _row(conn, "SELECT * FROM outbox WHERE id=?", (outbox_id,))
    if saved is None or outbox is None:
        _fail("LEGACY_OUTPUT_RECORD_REQUIRED")
    expected_four = run.scope.values[:4]
    if ((saved.get("tenant"), saved.get("app")) != expected_four[:2]
            or tuple(outbox.get(k) for k in ("tenant", "app", "user", "chat")) != expected_four
            or saved.get("scope") != run.scope.key):
        _fail("LEGACY_OUTPUT_SCOPE_MISMATCH")

    context = _json(saved.get("context"), dict, "LEGACY_CONTEXT_INVALID")
    context_fields = {"tenant_id", "app_id", "actor_id", "chat_id", "thread_id", "allowed_project_ids",
                      "all_projects", "is_owner", "content_expires_at", "reference_project_ids", "source_access_version"}
    if set(context) != context_fields:
        _fail("LEGACY_CONTEXT_SCHEMA_MISMATCH")
    context_scope = tuple(context.get(k) for k in ("tenant_id", "app_id", "actor_id", "chat_id", "thread_id"))
    if context_scope != run.scope.values or _hash(context_scope) != saved["scope"]:
        _fail("LEGACY_CONTEXT_SCOPE_MISMATCH")
    if (not _strings(context["allowed_project_ids"]) or not _strings(context["reference_project_ids"])
            or type(context["all_projects"]) is not bool or type(context["is_owner"]) is not bool):
        _fail("LEGACY_CONTEXT_INVALID")
    if not context["all_projects"] and not set(context["reference_project_ids"]).issubset(context["allowed_project_ids"]):
        _fail("LEGACY_REFERENCE_SCOPE_MISMATCH")

    source_version = context['source_access_version']
    if source_version is not None:
        if not isinstance(source_version, str) or not source_version or len(source_version) > 128:
            _fail('LEGACY_SOURCE_ACCESS_VERSION_INVALID')
        bound = _row(conn, 'SELECT source_access_version FROM am_source_deliveries WHERE outbox_id=?', (outbox_id,))
        if bound is None or bound['source_access_version'] != source_version:
            _fail('LEGACY_SOURCE_ACCESS_BINDING_MISMATCH')

    original, original_metadata = _source(conn, run.event_id, run.scope)
    submit = _row(conn, """SELECT * FROM am_harness_events WHERE scope_key=? AND event_id=?
        AND run_id=? AND operation='submit' AND applied_revision=1""", (run.scope.key, run.event_id, run.id))
    if submit is None or submit["request_hash"] != _hash({"input_text": original["body"], "priority": current["priority"]}):
        _fail("LEGACY_ORIGINAL_INPUT_HASH_MISMATCH")

    if run.revision == 1:
        source_event_id = run.event_id
        if original["body"] != run.input_text:
            _fail("LEGACY_RUN_INPUT_MISMATCH")
        source, metadata = original, original_metadata
    else:
        revisions = _rows(conn, """SELECT * FROM am_harness_events WHERE scope_key=? AND run_id=?
            AND operation='revise' AND applied_revision=?""", (run.scope.key, run.id, run.revision))
        if len(revisions) != 1:
            _fail("LEGACY_REVISION_EVENT_REQUIRED")
        revision_event = revisions[0]
        if revision_event["applied_cancel_epoch"] != run.cancel_epoch:
            _fail("LEGACY_REVISION_EPOCH_MISMATCH")
        source_event_id = revision_event["event_id"]
        source, metadata = _source(conn, source_event_id, run.scope)
        control = parse_control(source["body"])
        if (control is None or control.operation != "revise" or control.replacement != run.input_text
                or control.target_run_id not in (None, run.id)
                or revision_event["request_hash"] != _hash({"target": run.id, "input_text": run.input_text})):
            _fail("LEGACY_REVISION_INPUT_MISMATCH")

    if saved.get("event_id") != source_event_id:
        _fail("LEGACY_OUTPUT_EVENT_MISMATCH")
    link = _row(conn, "SELECT event_id FROM am_outbox_events WHERE outbox_id=?", (outbox_id,))
    if link is None or link["event_id"] != source_event_id:
        _fail("LEGACY_OUTBOX_EVENT_MISMATCH")
    if saved.get("input_hash") != _hash(source["body"]):
        _fail("LEGACY_SOURCE_INPUT_HASH_MISMATCH")
    reply = _json(saved.get("reply"), dict, "LEGACY_REPLY_INVALID")
    if reply.get("event_id") != engine_event_id(run):
        _fail("LEGACY_ENGINE_EVENT_MISMATCH")
    if outbox.get("chat_type") != source["chat_type"] or outbox.get("origin_chat_type", source["chat_type"]) != source["chat_type"]:
        _fail("LEGACY_DELIVERY_ROUTE_MISMATCH")
    if source["chat_type"] == "group" and context["is_owner"]:
        _fail("LEGACY_CONTEXT_OWNER_MISMATCH")

    ttl = (outbox.get("expires"), saved.get("expires"), context["content_expires_at"],
           run.expires_at, original["expires"], source["expires"])
    if (not all(_finite_time(value) for value in ttl) or not ttl[0] == ttl[1] == ttl[2]
            or ttl[0] > min(ttl[3:]) or current["expires_at"] != run.expires_at):
        _fail("LEGACY_OUTPUT_TTL_MISMATCH")
    grant = _json(saved.get("grant_json"), list, "LEGACY_GRANT_BINDING_INVALID")
    notice = _row(conn, "SELECT * FROM am_query_deliveries WHERE outbox_id=?", (outbox_id,))
    if (not _strings(grant, nonempty=True) or notice is None or notice.get("denial") != 0
            or notice.get("scope_hash") != _hash(grant)):
        _fail("LEGACY_GRANT_BINDING_INVALID")

    route_table = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='am_group_delivery'").fetchone()
    route = _row(conn, "SELECT * FROM am_group_delivery WHERE outbox_id=?", (outbox_id,)) if route_table else None
    if source["chat_type"] == "group":
        if (route is None or route.get("source_chat") != run.scope.chat_id or route.get("mode") != "public"
                or route.get("reply_to") != source_event_id or _thread(route) != run.scope.thread_id):
            _fail("LEGACY_GROUP_THREAD_BINDING_MISMATCH")
    elif route is not None:
        # A real private route derived from an authorized group page must still
        # remain private. Never allow a hidden reply_to to redirect a DM result.
        if (route.get("mode") != "private" or route.get("reply_to") is not None
                or not metadata.get("query_group") or route.get("source_chat") != metadata["query_group"]
                or _thread(route) != run.scope.thread_id):
            _fail("LEGACY_PRIVATE_ROUTE_MISMATCH")

    parent = metadata.get("parent")
    if source["chat_type"] == "group" and parent:
        quoted = _row(conn, """SELECT * FROM am_group_refs WHERE message_id=? AND app=? AND tenant=? AND chat=?""",
                      (parent, run.scope.app_id, run.scope.tenant_id, run.scope.chat_id))
        if quoted is None or not _finite_time(quoted.get("expires")) or quoted["expires"] < ttl[0]:
            _fail("LEGACY_QUOTED_SOURCE_UNAVAILABLE")
        projects = _json(quoted.get("projects"), list, "LEGACY_QUOTED_PROJECTS_INVALID")
        if not _strings(projects):
            _fail("LEGACY_QUOTED_PROJECTS_INVALID")
        expected_references = [p for p in projects if context["all_projects"] or p in context["allowed_project_ids"]]
        if context["reference_project_ids"] != expected_references:
            _fail("LEGACY_QUOTED_REFERENCE_MISMATCH")
    elif context["reference_project_ids"]:
        _fail("LEGACY_UNPROVEN_REFERENCE_PROJECTS")
