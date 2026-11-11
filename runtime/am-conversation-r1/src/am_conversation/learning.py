"""Bounded JSONL knowledge import with explicit governance and honest reports.

The file supplies knowledge data only.  Identity, application, tenant, project
permissions and approval capability always come from the trusted AccessContext
and KnowledgeStore configuration supplied by the host.
"""
from __future__ import annotations

from collections.abc import Iterable
import json
from pathlib import Path
from typing import Any

from .knowledge import KnowledgeStore
from .types import AccessContext, ConversationError, canonical


_FIELDS = frozenset({"item_key", "kind", "content", "source", "scope", "project_id"})
_REQUIRED = frozenset({"item_key", "kind", "content", "source"})


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ConversationError("IMPORT_DUPLICATE_JSON_FIELD")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise ConversationError("IMPORT_INVALID_JSON")


class KnowledgeImporter:
    """Import JSONL into a KnowledgeStore without trusting file-supplied roles.

    ``import_jsonl(ctx, text_or_lines, namespace='default', approve=False,
    dry_run=False)`` returns counts plus an individual result for every nonblank
    line.  ``import_file`` reads a local UTF-8 file with bounded physical lines.
    Approval is an explicit host argument and is still checked by KnowledgeStore.

    An item_key identifies one logical object inside tenant/app/namespace.  The
    key cannot silently change kind or visibility.  Importing content that has
    already appeared in its history does not create another version or restore
    a superseded version.  New content creates a candidate; the previously
    approved version remains active unless explicit approval succeeds.

    A dry-run performs SELECTs only on the live database.  It simulates earlier
    lines in memory, including duplicate items in the same file.  Reported plans
    are not reservations and can change if another process writes afterwards.
    """

    def __init__(self, store: KnowledgeStore, *, max_line_bytes: int = 65536,
                 max_content_chars: int = 20000, max_items: int = 1000) -> None:
        if (type(max_line_bytes) is not int or not 1024 <= max_line_bytes <= 1048576 or
                type(max_content_chars) is not int or not 1 <= max_content_chars <= 100000 or
                type(max_items) is not int or not 1 <= max_items <= 10000):
            raise ConversationError("INVALID_IMPORT_LIMITS")
        self.store = store
        self.max_line_bytes = max_line_bytes
        self.max_content_chars = max_content_chars
        self.max_items = max_items

    def import_file(self, ctx: AccessContext, path: str | Path, *, namespace: str = "default",
                    approve: bool = False, dry_run: bool = False) -> dict[str, Any]:
        try:
            with Path(path).open("rb") as stream:
                return self.import_jsonl(ctx, self._bounded_lines(stream), namespace=namespace,
                                         approve=approve, dry_run=dry_run)
        except OSError as exc:
            raise ConversationError("IMPORT_FILE_UNAVAILABLE") from exc

    def _bounded_lines(self, stream):
        while True:
            first = stream.readline(self.max_line_bytes + 1)
            if not first:
                return
            # Drain an oversized physical line without holding it in memory.
            if len(first) > self.max_line_bytes and not first.endswith(b"\n"):
                tail = first
                while tail and not tail.endswith(b"\n"):
                    tail = stream.readline(self.max_line_bytes + 1)
            yield first

    def import_jsonl(self, ctx: AccessContext, text_or_lines: str | bytes | Iterable,
                     *, namespace: str = "default", approve: bool = False,
                     dry_run: bool = False) -> dict[str, Any]:
        if (not isinstance(namespace, str) or not namespace.strip() or len(namespace) > 128 or
                any(ord(char) < 32 for char in namespace) or
                type(approve) is not bool or type(dry_run) is not bool):
            raise ConversationError("INVALID_IMPORT_OPTIONS")
        lines = text_or_lines.splitlines() if isinstance(text_or_lines, (str, bytes)) else text_or_lines
        try:
            iterator = iter(lines)
        except TypeError as exc:
            raise ConversationError("INVALID_IMPORT_INPUT") from exc
        report: dict[str, Any] = {
            "ok": True, "complete": True, "dry_run": dry_run,
            "approve_requested": approve, "namespace": namespace,
            "counts": {"created": 0, "updated": 0, "unchanged": 0, "approved": 0,
                       "errors": 0, "warnings": 0, "skipped_blank": 0},
            "planned": {"created": 0, "updated": 0, "approved": 0}, "results": [],
        }
        shadow: dict[str, dict[str, Any] | None] = {}
        line_number = 0
        while True:
            try:
                raw = next(iterator)
            except StopIteration:
                break
            except Exception:
                report["complete"] = False
                self._error(report, line_number + 1, None, "IMPORT_STREAM_FAILED")
                break
            line_number += 1
            item_key = None
            if len(report["results"]) >= self.max_items:
                if isinstance(raw, (str, bytes)) and not raw.strip() and len(raw) <= self.max_line_bytes:
                    report["counts"]["skipped_blank"] += 1
                    continue
                report["complete"] = False
                self._error(report, line_number, None, "IMPORT_ITEM_LIMIT")
                break
            try:
                parsed = self._parse(raw, first_line=line_number == 1)
                if parsed is None:
                    report["counts"]["skipped_blank"] += 1
                    continue
                item_key = parsed["item_key"]
                self.store._validate_submission(ctx, kind=parsed["kind"], content=parsed["content"],
                                                source=parsed["source"], scope=parsed["scope"],
                                                project_id=parsed["project_id"])
                if approve:
                    self.store._require_owner(ctx)
                if dry_run:
                    result = self._simulate(ctx, parsed, namespace, approve, shadow)
                else:
                    result = self._apply(ctx, parsed, namespace, approve)
                result.update({"line": line_number, "item_key": item_key, "ok": True})
                report["results"].append(result)
                action = result["action"]
                if action == "would_create":
                    report["planned"]["created"] += 1
                elif action == "would_update":
                    report["planned"]["updated"] += 1
                else:
                    report["counts"][action] += 1
                if result["approval_changed"]:
                    report["planned" if dry_run else "counts"]["approved"] += 1
                report["counts"]["warnings"] += len(result["warnings"])
            except ConversationError as exc:
                self._error(report, line_number, item_key, str(exc))
                if str(exc) == "IMPORT_ITEM_LIMIT":
                    break
            except Exception:
                # Do not echo database paths, file content or raw exception text.
                self._error(report, line_number, item_key, "IMPORT_STORAGE_FAILED")
        report["ok"] = report["complete"] and report["counts"]["errors"] == 0
        report["processed"] = len(report["results"])
        return report

    @staticmethod
    def _error(report, line, item_key, code):
        report["counts"]["errors"] += 1
        report["results"].append({"line": line, "item_key": item_key, "ok": False,
                                  "action": "error", "error": code, "persisted": False})

    def _parse(self, raw, *, first_line: bool):
        if isinstance(raw, bytes):
            if len(raw) > self.max_line_bytes:
                raise ConversationError("IMPORT_LINE_TOO_LARGE")
            try:
                raw = raw.decode("utf-8-sig" if first_line else "utf-8")
            except UnicodeDecodeError as exc:
                raise ConversationError("IMPORT_INVALID_UTF8") from exc
        elif isinstance(raw, str):
            try:
                if len(raw.encode("utf-8")) > self.max_line_bytes:
                    raise ConversationError("IMPORT_LINE_TOO_LARGE")
            except UnicodeEncodeError as exc:
                raise ConversationError("IMPORT_INVALID_UTF8") from exc
            if first_line:
                raw = raw.removeprefix("\ufeff")
        else:
            raise ConversationError("IMPORT_INVALID_LINE")
        if not raw.strip():
            return None
        try:
            item = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        except (ValueError, RecursionError) as exc:
            raise ConversationError("IMPORT_INVALID_JSON") from exc
        if not isinstance(item, dict) or not _REQUIRED.issubset(item) or set(item) - _FIELDS:
            raise ConversationError("IMPORT_INVALID_FIELDS")
        key = item["item_key"]
        if not isinstance(key, str) or not key.strip() or len(key) > 256 or any(ord(c) < 32 for c in key):
            raise ConversationError("IMPORT_INVALID_ITEM_KEY")
        if not isinstance(item["content"], str) or not item["content"].strip():
            raise ConversationError("INVALID_KNOWLEDGE_CONTENT")
        if len(item["content"]) > self.max_content_chars:
            raise ConversationError("IMPORT_CONTENT_TOO_LARGE")
        # Escaped lone surrogates are legal in Python's JSON decoder but cannot
        # be stored in UTF-8 SQLite text.  Reject them as data, before any writes.
        try:
            canonical(item).encode("utf-8")
        except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as exc:
            raise ConversationError("IMPORT_INVALID_JSON") from exc
        return {**item, "scope": item.get("scope", "private"), "project_id": item.get("project_id")}

    def _ensure_index(self):
        self.store._db.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_import_items (
                tenant_id TEXT NOT NULL,
                app_id TEXT NOT NULL,
                namespace TEXT NOT NULL,
                item_key TEXT NOT NULL,
                logical_id TEXT NOT NULL,
                PRIMARY KEY (tenant_id, app_id, namespace, item_key)
            )
        """)

    def _load(self, ctx, namespace, item_key):
        exists = self.store._db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='knowledge_import_items'"
        ).fetchone()
        if exists is None:
            return None
        mapping = self.store._db.execute(
            "SELECT logical_id FROM knowledge_import_items WHERE tenant_id=? AND app_id=? "
            "AND namespace=? AND item_key=?", (ctx.tenant_id, ctx.app_id, namespace, item_key),
        ).fetchone()
        if mapping is None:
            return None
        rows = self.store._db.execute(
            "SELECT id FROM knowledge_records WHERE tenant_id=? AND app_id=? AND logical_id=? "
            "ORDER BY version DESC", (ctx.tenant_id, ctx.app_id, mapping["logical_id"]),
        ).fetchall()
        if not rows:
            raise ConversationError("IMPORT_INDEX_INVALID")
        # Reuse the same visibility check as ordinary reads, including private
        # sessions and permissions that have been reduced since a prior import.
        history = [self.store._record(self.store._visible_row(ctx, row["id"])) for row in rows]
        return {"history": history}

    @staticmethod
    def _plan(item, state, approve):
        history = state["history"] if state else []
        latest = history[0] if history else None
        if latest and (latest["kind"], latest["scope"], latest["project_id"]) != (
                item["kind"], item["scope"], item["project_id"]):
            raise ConversationError("IMPORT_ITEM_DEFINITION_CHANGED")
        match = next((record for record in history if record["content"] == item["content"]), None)
        if match:
            historical = match["version"] != latest["version"]
            if approve and match["status"] != "approved":
                if historical:
                    raise ConversationError("IMPORT_HISTORICAL_VERSION")
                if match["status"] != "candidate":
                    raise ConversationError("KNOWLEDGE_NOT_CANDIDATE")
            return {"action": "unchanged", "record": match, "latest": latest,
                    "historical": historical,
                    "approval_changed": approve and match["status"] == "candidate"}
        return {"action": "updated" if latest else "created", "latest": latest,
                "historical": False, "approval_changed": approve}

    @staticmethod
    def _result(item, plan, record, *, dry_run, new_record=False):
        warnings = []
        if plan["historical"]:
            warnings.append("IMPORT_HISTORICAL_CONTENT_NOT_REAPPLIED")
        if record["source"] != item["source"]:
            warnings.append("IMPORT_UNCHANGED_SOURCE_RETAINED")
        if record["status"] in {"rejected", "superseded"}:
            warnings.append("IMPORT_RECORD_NOT_ACTIVE")
        action = plan["action"]
        if dry_run and action in {"created", "updated"}:
            action = "would_create" if action == "created" else "would_update"
        return {"action": action, "record_id": record["id"], "logical_id": record["logical_id"],
                "version": record["version"], "status": None if dry_run and (record["id"] is None or new_record or plan["approval_changed"]) else record["status"],
                "planned_status": record["status"] if dry_run else None,
                "persisted": not dry_run, "approval_changed": plan["approval_changed"],
                "historical": plan["historical"], "warnings": warnings}

    def _apply(self, ctx, item, namespace, approve):
        # One transaction covers the stable import mapping, version submission
        # and any explicit approval.  A failed row cannot leave half an import.
        with self.store._transaction():
            self._ensure_index()
            state = self._load(ctx, namespace, item["item_key"])
            plan = self._plan(item, state, approve)
            if plan["action"] == "unchanged":
                record = plan["record"]
            else:
                latest = plan["latest"]
                record = self.store.submit(
                    ctx, kind=item["kind"], content=item["content"], source=item["source"],
                    scope=item["scope"], project_id=item["project_id"],
                    logical_id=latest["logical_id"] if latest else None,
                    expected_version=latest["version"] if latest else None,
                )
                if latest is None:
                    self.store._db.execute(
                        "INSERT INTO knowledge_import_items (tenant_id,app_id,namespace,item_key,logical_id) "
                        "VALUES (?,?,?,?,?)", (ctx.tenant_id, ctx.app_id, namespace, item["item_key"], record["logical_id"]),
                    )
            if plan["approval_changed"]:
                record = self.store.approve(ctx, record["id"], record["version"])
            return self._result(item, plan, record, dry_run=False)

    def _simulate(self, ctx, item, namespace, approve, shadow):
        key = item["item_key"]
        if key not in shadow:
            with self.store._lock:
                self.store._ensure_open()
                shadow[key] = self._load(ctx, namespace, key)
        state = shadow[key]
        plan = self._plan(item, state, approve)
        history = [dict(record) for record in state["history"]] if state else []
        new_record = plan["action"] != "unchanged"
        if new_record:
            latest = plan["latest"]
            record = {"id": None, "logical_id": latest["logical_id"] if latest else None,
                      "version": latest["version"] + 1 if latest else 1, "kind": item["kind"],
                      "content": item["content"], "source": item["source"], "scope": item["scope"],
                      "project_id": item["project_id"], "status": "candidate"}
            history.insert(0, record)
        else:
            record = next(record for record in history if record["version"] == plan["record"]["version"])
        if plan["approval_changed"]:
            for older in history:
                if older["version"] < record["version"] and older["status"] in {"approved", "candidate"}:
                    older["status"] = "superseded"
            record["status"] = "approved"
        shadow[key] = {"history": history}
        return self._result(item, plan, record, dry_run=True, new_record=new_record)
