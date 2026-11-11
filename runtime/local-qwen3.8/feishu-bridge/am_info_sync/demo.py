"""Executable synthetic scenario. Never contacts Feishu, Qwen, or a real person."""
from __future__ import annotations

import json
from pathlib import Path

from .models import Source, Observation, Principal, Page, RetryableError
from .store import Store
from .runtime import InformationService
from .extraction import DemoDirectiveExtractor


class DemoClock:
    def __init__(self):
        self.value = 1793347200000  # 2026-10-30 16:00 Asia/Shanghai, synthetic time.

    def __call__(self) -> int:
        return self.value


class DemoProjectAdapter:
    def fetch_page(self, source, start_ms, end_ms, cursor):
        text = "正式计划完成：2026-11-06；交付物审核：未审核；节点：进行中。"
        return Page((Observation(source.source_id, "project-1", "revision-1", end_ms, text,
            {"plan_end": "2026-11-06", "review": "未审核", "node": "进行中",
             "evidence": {"plan": "正式计划完成：2026-11-06", "review": "交付物审核：未审核", "node": "节点：进行中"}}),),
            time_coverage=False)


class DemoMessageAdapter:
    """Fails once on the second page to demonstrate a saved fixed-window retry."""
    def __init__(self):
        self.failed = False

    def fetch_page(self, source, start_ms, end_ms, cursor):
        if cursor is None:
            return Page((Observation(source.source_id, "msg-1", "v1", end_ms - 2000,
                "DEMO|P-DEMO|forecast|2026-11-10"),), next_cursor="page-2", time_coverage=True)
        if not self.failed:
            self.failed = True
            raise RetryableError("Synthetic page failure")
        return Page((Observation(source.source_id, "msg-2", "v1", end_ms - 1000,
            "DEMO|P-DEMO|progress|设备需要协调"),), time_coverage=True)


def run_demo(output: str | Path | None = None) -> dict:
    clock = DemoClock()
    store = Store(":memory:", clock=clock)
    try:
        shared = dict(tenant_id="demo-tenant", owner_user_id="owner", readers=("group:DEMO",),
                      output_chats=("demo-group",), enabled=True, start_ms=clock() - 3600_000)
        mapping = {field: {"subject_id": "P-DEMO", "value_path": [value], "quote_path": ["evidence", field]}
                   for field, value in (("plan", "plan_end"), ("review", "review"), ("node", "node"))}
        store.register_source(Source("project-demo", "project_snapshot", "demo-project", **shared,
                                     settings={"subject_id": "P-DEMO", "fact_mappings": mapping}))
        store.register_source(Source("forecast-chat", "im_chat", "demo-chat", **shared,
                                     settings={"subject_id": "P-DEMO"}))
        store.register_source(Source("owner-offline", "manual", "owner-notes", "demo-tenant", "owner",
                                     enabled=True, start_ms=clock() - 3600_000,
                                     settings={"subject_id": "P-DEMO", "trusted_actors": ["owner"]}))
        service = InformationService(store, {"project_snapshot": DemoProjectAdapter(), "im_chat": DemoMessageAdapter()},
                                     DemoDirectiveExtractor())
        service.sync_engine.run_source("project-demo")
        failed_once = False
        try:
            service.sync_engine.run_source("forecast-chat")
        except RetryableError:
            failed_once = True
        checkpoint_before = store.source_status("forecast-chat")["collected_through_ms"]
        resumed = service.sync_engine.run_source("forecast-chat")
        owner = Principal("owner", "demo-tenant")
        service.offline("owner-offline", owner, "内部先协调设备，暂不通知客户。", "P-DEMO",
            [{"field": "decision", "value": "内部先协调设备，暂不通知客户", "quote": "内部先协调设备，暂不通知客户", "confirmed": True}],
            idempotency_key="demo-meeting-decision")
        for _ in range(30):
            if service.analyze_once() is None:
                break
        private = service.query("P-DEMO", owner)
        public = service.query("P-DEMO", Principal("engineer", "demo-tenant", ("DEMO",), "demo-group"))
        private_fields = {fact["field"] for fact in private["facts"]}
        public_fields = {fact["field"] for fact in public["facts"]}
        if not {"plan", "forecast", "review", "node", "decision"} <= private_fields:
            raise AssertionError("Demo did not produce the required independent facts")
        if "decision" in public_fields:
            raise AssertionError("Private offline decision escaped into a group answer")
        before_count = len(private["facts"])
        clock.value += 86401_000
        maintenance = service.maintenance()
        after_retention = service.query("P-DEMO", owner)
        if len(after_retention["facts"]) != before_count:
            raise AssertionError("Raw retention incorrectly removed durable business facts")
        if any(f["evidence"].get("quote") for f in after_retention["facts"]):
            raise AssertionError("Expired raw quotations remain exposed")
        result = {"scenario": "synthetic_offline_only", "network_calls": 0, "external_actions": 0,
                  "model": "deterministic_demo_directive_parser_not_real_Qwen",
                  "recovery": {"failed_on_second_page": failed_once,
                               "coverage_not_advanced_after_failure": checkpoint_before is None,
                               "resumed_same_window_pages": resumed["page_count"]},
                  "owner_view": private, "department_group_view": public,
                  "retention": {"facts_before": before_count, "facts_after": len(after_retention["facts"]),
                                "raw_quotes_visible_after_expiry": False, "maintenance": maintenance},
                  "health": service.diagnostics()}
        if output is not None:
            path = Path(output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result
    finally:
        store.close()
