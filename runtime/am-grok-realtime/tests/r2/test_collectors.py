from dataclasses import replace
import json

import pytest

from am_gateway.collectors import (
    CollectorEngine, CursorExpired, FeishuMessageAdapter, JsonPageAdapter,
    Page, ScanRequest, SourceSpec, SourceUnreadable,
)
from am_gateway.discovery import (
    DiscoveryEngine, DiscoveryPage, DiscoverySpec, FeishuGroupsDiscovery,
    JsonDiscoveryAdapter, resource_coverage,
)
from am_gateway.models import Config, Event
from am_gateway.store import Store


class Clock:
    def __init__(self): self.value = 200.0
    def __call__(self): return self.value


class Adapter:
    def __init__(self, function): self.function, self.calls = function, []
    def read_page(self, source, scan):
        self.calls.append((source, scan))
        return self.function(source, scan)


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    store = Store(tmp_path / "gateway.db", clock=clock)
    config = Config("tenant", "bot", "owner", "ownerchat", 100.0)
    return store, config, clock


def event(name="m1", *, chat="chat", ts=150.0, **kwargs):
    return Event("tenant", name, chat, "user", "progress " + name, ts, **kwargs)


def state(store, source_id, lane="live"):
    return store.one("SELECT * FROM gw_source_scans WHERE source_id=? AND lane=?", (source_id, lane))


def test_fixed_window_last_page_only_and_independent_history(setup):
    store, config, clock = setup
    adapter = Adapter(lambda s, r: Page((event("m" + (r.page_token or "0")),),
                                       not r.page_token, "p2" if not r.page_token else ""))
    collector = CollectorEngine(store, config, {"feishu_messages": adapter})
    source = collector.register_source(SourceSpec("tenant", "chat"))
    assert collector.collect_one(source, "live")["status"] == "page"
    first = state(store, source)
    assert first["completed_until"] is None
    assert (first["window_start"], first["window_end"], first["page_token"]) == (100.0, 200.0, "p2")
    # Event delivery does not touch scan state, even if it carries a newer timestamp.
    store.ingest(event("eventonly", ts=900.0))
    assert state(store, source) == first
    clock.value = 250.0
    assert collector.collect_one(source, "live")["status"] == "completed"
    assert adapter.calls[-1][1].window_end == 200.0
    assert state(store, source)["completed_until"] == 200.0
    assert state(store, source, "history")["completed_until"] is None
    collector.collect_one(source, "history")
    assert adapter.calls[-1][1].window_start == 0.0
    assert adapter.calls[-1][1].window_end == 100.0


def test_expired_token_restarts_same_window_and_deduplicates(setup):
    store, config, clock = setup
    expired = False
    def read(s, r):
        nonlocal expired
        if r.page_token and not expired:
            expired = True
            raise CursorExpired()
        return Page((event("m2" if r.page_token else "m1"),), not r.page_token,
                    "p2" if not r.page_token else "")
    adapter = Adapter(read)
    collector = CollectorEngine(store, config, {"feishu_messages": adapter}, retry_seconds=0)
    source = collector.register_source(SourceSpec("tenant", "chat"))
    collector.collect_one(source, "live")
    clock.value = 900
    assert collector.collect_one(source, "live")["status"] == "cursor_restarted"
    assert state(store, source)["page_token"] == ""
    collector.collect_one(source, "live")
    collector.collect_one(source, "live")
    assert all(r.window_end == 200 for _, r in adapter.calls)
    assert len(store.rows("SELECT * FROM gw_items")) == 2
    assert state(store, source)["restart_count"] == 1
    assert state(store, source)["completed_until"] == 200


def test_page_items_and_checkpoint_rollback_together(setup, monkeypatch):
    store, config, _ = setup
    adapter = Adapter(lambda s, r: Page((event("first"), event("second")), True, "next"))
    collector = CollectorEngine(store, config, {"feishu_messages": adapter}, retry_seconds=0)
    source = collector.register_source(SourceSpec("tenant", "chat"))
    original = store.ingest_in_tx
    def fail_second(db, e):
        if e.native_id == "second": raise RuntimeError("injected crash")
        return original(db, e)
    monkeypatch.setattr(store, "ingest_in_tx", fail_second)
    assert collector.collect_one(source, "live")["status"] == "error"
    assert store.rows("SELECT * FROM gw_items") == []
    checkpoint = state(store, source)
    assert checkpoint["page_token"] == "" and checkpoint["page_count"] == 0
    assert checkpoint["completed_until"] is None
    monkeypatch.setattr(store, "ingest_in_tx", original)
    collector.collect_one(source, "live")
    assert len(store.rows("SELECT * FROM gw_items")) == 2
    assert state(store, source)["page_token"] == "next"


def test_all_same_timestamp_pages_are_drained(setup):
    store, config, _ = setup
    def read(s, r):
        page = int(r.page_token or 0)
        return Page((event(str(page), ts=150),), page < 5, str(page + 1) if page < 5 else "")
    collector = CollectorEngine(store, config, {"feishu_messages": Adapter(read)})
    source = collector.register_source(SourceSpec("tenant", "chat"))
    for _ in range(6): collector.collect_one(source, "live")
    assert len(store.rows("SELECT * FROM gw_items")) == 6
    assert state(store, source)["completed_until"] == 200


def test_fair_rotation_even_when_clock_does_not_advance(setup):
    store, config, _ = setup
    adapter = Adapter(lambda s, r: Page((), True, str(int(r.page_token or 0) + 1)))
    collector = CollectorEngine(store, config, {"feishu_messages": adapter})
    for name in ("a", "b", "c"): collector.register_source(SourceSpec("tenant", name))
    collector.run_due(12)
    counts = {}
    for s, r in adapter.calls: counts[s.source_id, r.lane] = counts.get((s.source_id, r.lane), 0) + 1
    assert len(counts) == 6
    assert set(counts.values()) == {2}


def test_unbound_unreadable_and_cross_scope_are_visible(setup):
    store, config, _ = setup
    def denied(s, r): raise SourceUnreadable()
    collector = CollectorEngine(store, config, {"denied": Adapter(denied),
                                "bad": Adapter(lambda s, r: Page((event(chat="secret"),)))})
    for name, adapter in (("a", "missing"), ("b", "denied"), ("c", "bad")):
        source = collector.register_source(SourceSpec("tenant", name, adapter=adapter))
        expected = {"missing": "unbound", "denied": "unreadable", "bad": "error"}[adapter]
        assert collector.collect_one(source, "live")["status"] == expected
        assert state(store, source)["completed_until"] is None
    assert store.rows("SELECT * FROM gw_items") == []
    assert {r["state"] for r in collector.coverage()} >= {"unbound", "unreadable", "error"}


def test_superseded_lease_cannot_commit_old_page(setup):
    store, config, clock = setup
    collector = CollectorEngine(store, config, lease_seconds=1)
    source = collector.register_source(SourceSpec("tenant", "chat"))
    def old_read(s, r):
        clock.value += 2
        collector.adapters["feishu_messages"] = Adapter(lambda s, r: Page((event("new"),)))
        assert collector.collect_one(source, "live")["status"] == "completed"
        return Page((event("old"),))
    collector.adapters["feishu_messages"] = Adapter(old_read)
    assert collector.collect_one(source, "live")["status"] == "superseded"
    assert [r["native_id"] for r in store.rows("SELECT * FROM gw_items")] == ["new"]


def message(native="m1", **kwargs):
    return {"message_id": native, "chat_id": "chat", "sender": {"id": "user", "id_type": "open_id"},
            "msg_type": "text", "body": {"content": json.dumps({"text": "hello"})},
            "create_time": "150000", "update_time": "151000", **kwargs}


def test_feishu_message_parameters_thread_scope_and_thread_discovery():
    calls = []
    def request(url, params, identity):
        calls.append((url, params, identity))
        return {"code": 0, "data": {"items": [message(thread_id="thread1")], "has_more": False}}
    adapter = FeishuMessageAdapter(request, bot_id="bot")
    source = SourceSpec("tenant", "chat")
    page = adapter.read_page(source, ScanRequest("live", 100, 200, "p1"))
    params = calls[-1][1]
    assert params["start_time"] == "100" and params["end_time"] == "200"
    assert params["sort_type"] == "ByCreateTimeAsc" and params["page_size"] == 50
    assert params["page_token"] == "p1"
    assert len(page.discovered_sources) == 1
    thread = page.discovered_sources[0]
    adapter.read_page(thread, ScanRequest("history", 0, 100))
    assert "start_time" not in calls[-1][1] and "end_time" not in calls[-1][1]
    assert calls[-1][1]["container_id_type"] == "thread"
    assert page.events[0].version_time == 151
    assert page.events[0].observed_via == "poll"


def test_empty_or_nonadvancing_response_cannot_claim_completion(setup):
    store, config, _ = setup
    for raw in ({"code": 0, "data": {}}, {"code": 0, "data": {"items": [], "has_more": True}}):
        adapter = FeishuMessageAdapter(lambda *a: raw, bot_id="bot")
        collector = CollectorEngine(store, config, {"feishu_messages": adapter}, retry_seconds=0)
        source = collector.register_source(SourceSpec("tenant", "chat"))
        assert collector.collect_one(source, "live")["status"] == "error"
        assert state(store, source)["completed_until"] is None


def test_discovery_every_page_and_partial_scope_are_separate(setup):
    store, config, _ = setup
    collectors = CollectorEngine(store, config)
    class Inventory:
        def discover_page(self, spec, cursor):
            return DiscoveryPage((SourceSpec("tenant", "b" if cursor else "a"),), not cursor,
                                 "next" if not cursor else "", bool(cursor))
    discovery = DiscoveryEngine(collectors, {"inventory": Inventory()})
    ident = discovery.register(DiscoverySpec("tenant", "groups", "inventory"))
    assert discovery.discover_one(ident)["status"] == "scanning"
    assert discovery.coverage()[0]["completed_at"] is None
    assert discovery.discover_one(ident)["status"] == "partial"
    assert len(store.rows("SELECT * FROM gw_sources")) == 2
    assert discovery.coverage()[0]["completed_at"] is None
    assert len(collectors.coverage()) == 4


def test_discovery_source_registration_and_checkpoint_atomic(setup):
    store, config, _ = setup
    collectors = CollectorEngine(store, config)
    class Inventory:
        def discover_page(self, spec, cursor):
            return DiscoveryPage((SourceSpec("tenant", "good"), SourceSpec("wrong_tenant", "bad")), True, "next")
    discovery = DiscoveryEngine(collectors, {"inventory": Inventory()})
    ident = discovery.register(DiscoverySpec("tenant", "groups", "inventory"))
    assert discovery.discover_one(ident)["status"] == "error"
    assert store.rows("SELECT * FROM gw_sources") == []
    assert store.one("SELECT page_token FROM gw_discovery")["page_token"] == ""


def test_group_discovery_is_joined_groups_only_and_normalized_transport_contract():
    calls = []
    def request(url, params, identity):
        calls.append((url, params, identity))
        return {"code": 0, "data": {"items": [{"chat_id": "chat", "name": "Team"}], "has_more": True, "page_token": "next"}}
    adapter = FeishuGroupsDiscovery(request)
    page = adapter.discover_page(DiscoverySpec("tenant", "groups", "groups"), "old")
    assert page.has_more and page.next_page_token == "next"
    assert calls[-1][1]["page_token"] == "old"
    assert page.sources[0].channel == "group"
    with pytest.raises(ValueError):
        adapter.discover_page(DiscoverySpec("tenant", "dms", "groups", scope="all_personal_dm"), "")
    raw = {"status": "ok", "has_more": False, "items": [event().to_dict()]}
    assert JsonPageAdapter(lambda p: raw).read_page(SourceSpec("tenant", "chat"), ScanRequest("live", 0, 200)).events[0] == event()
    missing_completeness = {"status": "ok", "sources": [], "has_more": False}
    with pytest.raises(RuntimeError):
        JsonDiscoveryAdapter(lambda p: missing_completeness).discover_page(DiscoverySpec("tenant", "x", "json"), "")
    catalog = {r["domain"]: r for r in resource_coverage()}
    assert catalog["docs_wiki_drive"]["host_binding"] == "unbound"
    assert catalog["personal_dm"]["host_binding"] == "unbound"


@pytest.mark.parametrize("msg_type,body", [
    ("post", {"zh_cn": {"title": "title", "content": [[{"tag": "text", "text": "A"}, {"tag": "text", "text": "B"}]]}}),
    ("file", {"file_key": "resource1", "file_name": "test.docx"}),
])
def test_event_poll_shared_normalization_does_not_create_false_edits(setup, msg_type, body):
    from am_gateway.normalization import normalized_content
    store, config, _ = setup
    text, attachments, status = normalized_content(body, msg_type)
    incoming = event("normalized", ts=150, attachments=tuple(attachments), content_status=status)
    incoming = replace(incoming, body=text, version_time=151)
    store.ingest(incoming)
    adapter = FeishuMessageAdapter(lambda *a: {"code": 0, "data": {"has_more": False, "items": [
        message("normalized", msg_type=msg_type, body={"content": json.dumps(body)})]}}, bot_id="bot")
    collector = CollectorEngine(store, config, {"feishu_messages": adapter})
    source = collector.register_source(SourceSpec("tenant", "chat"))
    assert collector.collect_one(source, "live")["status"] == "completed"
    assert store.one("SELECT version FROM gw_items")["version"] == 1
    assert len(store.rows("SELECT * FROM gw_item_versions")) == 1


def test_discovery_completed_marker_requires_successful_final_page(setup):
    store, config, clock = setup
    collectors = CollectorEngine(store, config)
    class Inventory:
        def discover_page(self, spec, cursor):
            if not cursor:
                return DiscoveryPage((SourceSpec("tenant", "chat"),), True, "next")
            raise SourceUnreadable()
    discovery = DiscoveryEngine(collectors, {"inventory": Inventory()}, retry_seconds=0)
    ident = discovery.register(DiscoverySpec("tenant", "groups", "inventory"))
    discovery.discover_one(ident)
    assert discovery.discover_one(ident)["status"] == "unreadable"
    assert discovery.coverage()[0]["completed_at"] is None
    assert len(store.rows("SELECT * FROM gw_sources")) == 1
    class Final:
        def discover_page(self, spec, cursor):
            assert cursor == "next"
            return DiscoveryPage()
    discovery.adapters["inventory"] = Final()
    assert discovery.discover_one(ident)["status"] == "complete_for_scope"
    assert discovery.coverage()[0]["completed_at"] == clock.value


def test_inventory_refresh_preserves_explicit_project_binding(setup):
    store, config, _ = setup
    collector = CollectorEngine(store, config)
    source = SourceSpec("tenant", "chat", project_ids=("project1",), poll_seconds=15,
                        metadata={"binding": "host_config"})
    collector.register_source(source)
    with store.transaction() as db:
        collector.register_in_tx(db, SourceSpec("tenant", "chat", metadata={"name": "renamed"}),
                                 discovered_by="directory")
    persisted = SourceSpec.from_dict(json.loads(store.one("SELECT spec_json FROM gw_sources")["spec_json"]))
    assert persisted.project_ids == ("project1",)
    assert persisted.poll_seconds == 15
    assert persisted.metadata == {"binding": "host_config", "name": "renamed"}
