"""Local operator CLI. Never expose its identity arguments as public bot tools."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys

from .models import Source, Principal, ScopeViolation
from .store import Store
from .runtime import InformationService


def _json_file(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _principal(args):
    return Principal(args.user, args.tenant, tuple(getattr(args, "group", ()) or ()), getattr(args, "output_chat", None))


def _adapters(allowed):
    if not allowed:
        raise ScopeViolation("Network polling requires the explicit --allow-network operator flag")
    from .adapters import FeishuClient, MessageAdapter, DocxAdapter

    def token_provider(source):
        name = source.settings.get("token_env", "")
        if not re.fullmatch(r"AM_SYNC_[A-Z0-9_]+", name):
            raise ValueError("Register an AM_SYNC_ credential environment variable name")
        return os.environ.get(name, "")

    client = FeishuClient(token_provider)
    return {"im_chat": MessageAdapter(client), "im_thread": MessageAdapter(client), "docx": DocxAdapter(client)}


def parser():
    root = argparse.ArgumentParser(description="AM information sync: local operator commands")
    root.add_argument("--db", default=".am-sync/state.sqlite3")
    sub = root.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    load = sub.add_parser("load-sources"); load.add_argument("file")
    sub.add_parser("sources")
    sub.add_parser("health")
    poll = sub.add_parser("poll")
    poll.add_argument("source_id"); poll.add_argument("--allow-network", action="store_true")
    poll.add_argument("--reconcile", action="store_true"); poll.add_argument("--max-pages", type=int, default=20)
    work = sub.add_parser("work")
    work.add_argument("--max-jobs", type=int, default=100)
    query = sub.add_parser("query"); query.add_argument("subject_id")
    query.add_argument("--user", required=True); query.add_argument("--tenant", required=True)
    query.add_argument("--group", action="append"); query.add_argument("--output-chat")
    offline = sub.add_parser("offline"); offline.add_argument("source_id")
    offline.add_argument("--user", required=True); offline.add_argument("--tenant", required=True)
    offline.add_argument("--subject", required=True); offline.add_argument("--text-file", required=True)
    offline.add_argument("--claims-file"); offline.add_argument("--idempotency-key", required=True)
    retry = sub.add_parser("retry"); retry.add_argument("job_id")
    sub.add_parser("purge")
    backup = sub.add_parser("backup"); backup.add_argument("path")
    demo = sub.add_parser("demo"); demo.add_argument("--output")
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    store = None
    try:
        if args.command == "demo":
            from .demo import run_demo
            full = run_demo(args.output)
            result = {"scenario": full["scenario"], "recovery": full["recovery"],
                      "owner_fact_count": len(full["owner_view"]["facts"]),
                      "group_fact_count": len(full["department_group_view"]["facts"]),
                      "retention": full["retention"], "network_calls": 0, "external_actions": 0,
                      "output": args.output}
        else:
            store = Store(args.db)
            service = InformationService(store)
            if args.command == "init":
                result = {"initialized": True,
                          "enabled_sources": sum(s.enabled for s in store.list_sources()),
                          "network_started": False}
            elif args.command == "load-sources":
                values = _json_file(args.file)
                if not isinstance(values, list):
                    raise ValueError("Expected a JSON array of source registrations")
                sources = [Source.from_dict(value) for value in values]
                for source in sources:
                    store.register_source(source)
                result = {"registered": [s.source_id for s in sources], "network_started": False}
            elif args.command == "sources":
                result = [s.to_dict() for s in store.list_sources()]
            elif args.command == "health":
                result = service.diagnostics()
            elif args.command == "poll":
                from .sync import SyncEngine
                result = SyncEngine(store, _adapters(args.allow_network)).run_source(
                    args.source_id, "reconcile" if args.reconcile else "incremental", args.max_pages)
            elif args.command == "work":
                if not 1 <= args.max_jobs <= 10000:
                    raise ValueError("max-jobs out of bounds")
                result = {"events": 0, "analysis": [], "model_configured": False}
                for _ in range(args.max_jobs):
                    event = service.event_worker.run_once()
                    analysis = service.analyze_once()
                    result["events"] += int(event)
                    if analysis:
                        result["analysis"].append(analysis)
                    if not event and not analysis:
                        break
            elif args.command == "query":
                result = service.query(args.subject_id, _principal(args))
            elif args.command == "offline":
                result = service.offline(args.source_id, _principal(args), Path(args.text_file).read_text(encoding="utf-8"),
                                         args.subject, _json_file(args.claims_file) if args.claims_file else None,
                                         args.idempotency_key)
            elif args.command == "retry":
                store.retry_dead(args.job_id); result = {"requested_retry": args.job_id}
            elif args.command == "purge":
                result = service.maintenance()
            elif args.command == "backup":
                service.maintenance(); store.backup(args.path); result = {"backup": args.path}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        # No credential values, private payloads or remote error bodies in CLI errors.
        print(json.dumps({"error": getattr(exc, "code", type(exc).__name__)}, ensure_ascii=False), file=sys.stderr)
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    raise SystemExit(main())
