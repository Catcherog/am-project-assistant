import json
import sqlite3
import sys

import pytest

from am_gateway.__main__ import ReadOnlyStore, main
from am_gateway.collection_service import CollectionService, read_coverage
from am_gateway.collectors import CollectorEngine, Page, SourceSpec
from am_gateway.discovery import DiscoveryEngine, DiscoveryPage, DiscoverySpec
from am_gateway.models import Config
from am_gateway.store import Store


class Engine:
    def __init__(self, name, calls, *, fail=False, rows=None):
        self.name, self.calls, self.fail, self.rows = name, calls, fail, rows or []
    def run_due(self, max_pages):
        self.calls.append((self.name, max_pages))
        if self.fail:
            raise RuntimeError("secret token should not be persisted")
        return [{"status": "completed"}]
    def coverage(self):
        if self.fail:
            raise RuntimeError("secret token should not be persisted")
        return self.rows


@pytest.mark.parametrize("failing", ["discovery", "collectors"])
def test_failure_in_one_stage_does_not_skip_other(failing):
    calls = []
    service = CollectionService(Engine("collectors", calls, fail=failing == "collectors"),
                                Engine("discovery", calls, fail=failing == "discovery"))
    result = service.run_due()
    assert calls == [("discovery", 2), ("collectors", 8)]
    assert result["status"] == "partial"
    assert result[failing]["error_type"] == "RuntimeError"
    assert "secret token" not in json.dumps(result)
    assert "secret token" not in json.dumps(service.coverage())


def test_optional_discovery_and_result_errors_remain_visible():
    class Blocked:
        def run_due(self, max_pages): return [{"status": "unbound", "source_id": "source"}]
        def coverage(self): return []
    service = CollectionService(Blocked())
    result = service.run_due()
    assert result["discovery"]["status"] == "not_configured"
    assert result["collectors"]["status"] == "partial"
    assert result["collectors"]["results"][0]["source_id"] == "source"


def test_coverage_readonly_uses_saved_evidence_without_binding_claim(tmp_path):
    path = tmp_path / "gateway.db"
    store = Store(path, clock=lambda: 200.0)
    config = Config("tenant", "bot", "owner", "ownerchat", 100)
    class Reader:
        def read_page(self, source, scan): return Page()
    collectors = CollectorEngine(store, config, {"feishu_messages": Reader()})
    source = collectors.register_source(SourceSpec("tenant", "chat"))
    collectors.collect_one(source, "history")
    class PartialInventory:
        def discover_page(self, spec, cursor): return DiscoveryPage(complete_for_scope=False)
    discovery = DiscoveryEngine(collectors, {"inventory": PartialInventory()})
    discovery.register(DiscoverySpec("tenant", "groups", "inventory"))
    discovery.run_due()
    live = CollectionService(collectors, discovery).coverage()
    saved = read_coverage(ReadOnlyStore(path))
    assert live.keys() == saved.keys()
    assert live["summary"] == saved["summary"]
    assert saved["summary"]["history_completed_sources"] == 1
    assert saved["summary"]["discovery_states"] == {"partial": 1}
    assert saved["status"] == "partial"
    assert saved["binding_verification"] == "unavailable_readonly"
    assert all(row["adapter_bound"] is None for row in saved["collectors"])
    assert all(row["adapter_bound"] is True for row in live["collectors"])
    assert "docs_wiki_drive" in saved["summary"]["not_connected_resource_domains"]


def test_cli_empty_database_remains_byte_identical(tmp_path, monkeypatch, capsys):
    path = tmp_path / "empty.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE unrelated(value TEXT)")
        db.execute("INSERT INTO unrelated VALUES('preserve')")
    before = path.read_bytes()
    monkeypatch.setattr(sys, "argv", ["am_gateway", "coverage", "--db", str(path)])
    main()
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "not_connected"
    assert result["collectors"] == result["discovery"] == []
    assert path.read_bytes() == before
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("unrelated",)]


def test_readonly_database_does_not_create_missing_file(tmp_path):
    path = tmp_path / "missing.db"
    result = read_coverage(ReadOnlyStore(path))
    assert result["status"] == "error"
    assert result["errors"] == [{"stage": "schema", "error_type": "OperationalError"}]
    assert not path.exists()
