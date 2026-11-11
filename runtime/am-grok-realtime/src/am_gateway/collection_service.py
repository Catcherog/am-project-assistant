"""Scheduler-facing collection service and read-only coverage projection.

Diagnostics never instantiate CollectorEngine or DiscoveryEngine, because their
constructors create tables. Saved progress is evidence of earlier reads; it is
not evidence that a transport is currently bound in a running process.
"""
from __future__ import annotations

from collections import Counter
import json

from .discovery import resource_coverage


_BLOCKED = {"error", "unreadable", "unbound", "partial", "not_initialized"}


class CollectionService:
    def __init__(self, collectors, discovery=None, *, discovery_pages: int = 2, collector_pages: int = 8):
        for budget in (discovery_pages, collector_pages):
            if isinstance(budget, bool) or not isinstance(budget, int) or budget < 1:
                raise ValueError("positive page budgets required")
        self.collectors, self.discovery = collectors, discovery
        self.discovery_pages, self.collector_pages = discovery_pages, collector_pages
        self.last_run = None

    @staticmethod
    def _run(engine, budget):
        if engine is None:
            return {"status": "not_configured", "results": []}
        try:
            results = engine.run_due(max_pages=budget)
            if not isinstance(results, list):
                raise TypeError("engine results must be a list")
            state = ("partial" if any(row.get("status") in _BLOCKED for row in results)
                     else "processed" if results else "idle")
            return {"status": state, "results": results}
        except Exception as exc:
            # Exceptions can contain tokens or original message bodies.
            return {"status": "error", "error_type": type(exc).__name__, "results": []}

    def run_due(self) -> dict:
        # Discovery runs with a smaller bounded budget. Its errors must not skip
        # already-known sources; collector errors must not suppress its evidence.
        discovery = self._run(self.discovery, self.discovery_pages)
        collectors = self._run(self.collectors, self.collector_pages)
        stages = (discovery, collectors)
        status = ("partial" if any(s["status"] in {"error", "partial"} for s in stages)
                  else "ran" if any(s["results"] for s in stages) else "idle")
        self.last_run = {"status": status, "discovery": discovery, "collectors": collectors}
        return self.last_run

    def coverage(self) -> dict:
        errors = []
        collectors = _engine_coverage(self.collectors, "collectors", errors)
        discovery = _engine_coverage(self.discovery, "discovery", errors)
        # Source scope is persisted by the trusted registration path; enrich the
        # engine projection so generic 'chat' sources can be separated from DMs.
        store = getattr(self.collectors, "store", None)
        if store is not None:
            try:
                labels = _source_labels(store, getattr(getattr(self.collectors, "config", None), "tenant_id", None))
                for row in collectors:
                    row.update(labels.get(row["source_id"], {}))
            except Exception as exc:
                errors.append({"stage": "source_labels", "error_type": type(exc).__name__})
        report = _compose(collectors, discovery, errors, binding_verification="runtime")
        report["last_run"] = self.last_run
        return report


def _engine_coverage(engine, stage, errors):
    if engine is None:
        return []
    try:
        rows = engine.coverage()
        if not isinstance(rows, list):
            raise TypeError("coverage must be a list")
        return [dict(row) for row in rows]
    except Exception as exc:
        errors.append({"stage": stage, "error_type": type(exc).__name__})
        return []


def _source_labels(store, tenant_id=None):
    sql = "SELECT source_id,spec_json FROM gw_sources"
    args = ()
    if tenant_id is not None:
        sql += " WHERE tenant_id=?"
        args = (tenant_id,)
    labels = {}
    for row in store.rows(sql, args):
        spec = json.loads(row["spec_json"])
        labels[row["source_id"]] = {
            "channel": spec.get("channel", ""),
            "resource_domain": spec.get("metadata", {}).get("resource_domain", ""),
        }
    return labels


def _domain(row):
    explicit = row.get("resource_domain")
    if explicit:
        return explicit
    if row.get("container_type") == "thread" or row.get("source_kind") == "thread":
        return "threads"
    channel = row.get("channel")
    if channel in {"bot_dm", "personal_dm"}:
        return channel
    if channel == "group":
        return "joined_groups"
    kind = row.get("source_kind", "")
    if kind in {"doc", "docx", "wiki", "drive", "document"}:
        return "docs_wiki_drive"
    if kind in {"meegle", "task", "bitable", "calendar"}:
        return "meegle_tasks_bitable_calendar"
    if kind in {"attachment", "file", "image"}:
        return "attachments"
    return "unclassified"


def _history_done(row):
    return row.get("lane") == "history" and (row.get("state") == "complete" or
        (row.get("completed_until") is not None and row.get("window_start") is None))


def _compose(collectors, discovery, errors, *, binding_verification):
    resources = resource_coverage()
    for resource in resources:
        domain = resource["domain"]
        rows = [row for row in collectors if _domain(row) == domain]
        directories = [row for row in discovery if row.get("scope") == domain]
        source_ids = {row["source_id"] for row in rows}
        history_done = {row["source_id"] for row in rows if _history_done(row)}
        blocked = any(row.get("state") in _BLOCKED for row in rows + directories)
        observed = any((row.get("page_count") or 0) > 0 for row in rows + directories)
        if not rows and not directories:
            status = "not_connected"
        elif blocked:
            status = "partial"
        elif observed:
            status = "observed_for_scope"
        else:
            status = "pending"
        resource["observed"] = {
            "status": status,
            "registered_sources": len(source_ids),
            "history_completed_sources": len(history_done),
            "discovery_scopes": len(directories),
            "bound_adapter_rows": (sum(row.get("adapter_bound") is True for row in rows + directories)
                                   if binding_verification == "runtime" else None),
            "unbound_adapter_rows": (sum(row.get("adapter_bound") is False for row in rows + directories)
                                     if binding_verification == "runtime" else None),
        }
    states = Counter(row.get("state", "unknown") for row in collectors)
    directory_states = Counter(row.get("state", "unknown") for row in discovery)
    missing = [row["domain"] for row in resources if row["observed"]["status"] == "not_connected"]
    overall = ("error" if errors and not collectors and not discovery else
               "not_connected" if not collectors and not discovery else
               "partial" if errors or missing or any(k in _BLOCKED for k in states) or
               any(k in _BLOCKED for k in directory_states) else "observed_for_scope")
    return {
        "status": overall,
        "scope": "configured identities and sources only; no tenant-wide completeness claim",
        "binding_verification": binding_verification,
        "summary": {
            "registered_sources": len({row["source_id"] for row in collectors}),
            "source_lane_states": dict(states),
            "history_completed_sources": len({row["source_id"] for row in collectors if _history_done(row)}),
            "discovery_states": dict(directory_states),
            "not_connected_resource_domains": missing,
            "unclassified_sources": len({row["source_id"] for row in collectors if _domain(row) == "unclassified"}),
        },
        "collectors": collectors,
        "discovery": discovery,
        "resources": resources,
        "errors": errors,
        "last_run": None,
    }


def read_coverage(store, *, tenant_id=None) -> dict:
    """Read saved coverage using only SELECT; accepts an existing read-only store.

    No engine initialization, schema creation, checkpoint mutation or adapter
    invocation occurs. adapter_bound=None explicitly means not verifiable here.
    """
    collectors, discovery, errors = [], [], []
    try:
        tables = {row["name"] for row in store.rows("SELECT name FROM sqlite_master WHERE type='table'")}
    except Exception as exc:
        return _compose([], [], [{"stage": "schema", "error_type": type(exc).__name__}],
                        binding_verification="unavailable_readonly")
    if "gw_sources" in tables:
        try:
            where, args = (" WHERE tenant_id=?", (tenant_id,)) if tenant_id is not None else ("", ())
            sources = store.rows("SELECT source_id,spec_json,active,discovered_by FROM gw_sources" + where, args)
            scans = store.rows("SELECT * FROM gw_source_scans") if "gw_source_scans" in tables else []
            by_source = {}
            for scan in scans:
                by_source.setdefault(scan["source_id"], []).append(scan)
            for source in sources:
                spec = json.loads(source["spec_json"])
                scope = {"source_id": source["source_id"], "active": source["active"],
                         "discovered_by": source["discovered_by"], "source_kind": spec["source_kind"],
                         "native_id": spec["native_id"], "identity": spec["identity"],
                         "adapter": spec["adapter"], "adapter_bound": None, "channel": spec["channel"],
                         "container_type": spec["container_type"], "history_policy": "all_available_history",
                         "resource_domain": spec.get("metadata", {}).get("resource_domain", ""),
                         "coverage_meaning": ("visible full traversal; no thread time filter" if spec["container_type"] == "thread"
                                              else "fixed-window traversal for this identity")}
                saved = by_source.get(source["source_id"], [])
                if not saved:
                    collectors.append({**scope, "lane": "unknown", "state": "not_initialized"})
                for scan in saved:
                    fields = {name: scan[name] for name in (
                        "lane", "state", "completed_until", "window_start", "window_end", "page_count",
                        "item_count", "restart_count", "last_error", "last_attempt")}
                    collectors.append({**scope, **fields})
        except Exception as exc:
            errors.append({"stage": "collectors", "error_type": type(exc).__name__})
    if "gw_discovery" in tables:
        try:
            where, args = (" WHERE tenant_id=?", (tenant_id,)) if tenant_id is not None else ("", ())
            for row in store.rows("SELECT * FROM gw_discovery" + where, args):
                spec = json.loads(row["spec_json"])
                fields = {name: row[name] for name in ("discovery_id", "state", "generation", "completed_at",
                                                      "page_count", "source_count", "last_attempt", "last_error")}
                discovery.append({**fields, "scope": spec["scope"], "identity": spec["identity"],
                                  "adapter": spec["adapter"], "adapter_bound": None})
        except Exception as exc:
            errors.append({"stage": "discovery", "error_type": type(exc).__name__})
    return _compose(collectors, discovery, errors, binding_verification="unavailable_readonly")
