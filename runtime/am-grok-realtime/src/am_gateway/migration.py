"""Read/import settled R1 receipts without touching the old database."""
import json
from pathlib import Path
import sqlite3

from .models import ContractError, Event


def inspect_r1(path):
    p=Path(path).resolve()
    if not p.exists(): return {"exists":False,"settled":[],"unresolved":[]}
    db=sqlite3.connect(p.as_uri()+"?mode=ro",uri=True)
    db.row_factory=sqlite3.Row
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='gateway_requests'").fetchone():
            return {"exists":True,"settled":[],"unresolved":[],"r1_schema":False}
        rows=[dict(r) for r in db.execute("SELECT * FROM gateway_requests")]
    finally: db.close()
    settled=[]
    unresolved=[]
    for row in rows:
        if row["output_message_id"] or row["status"] in {"cancelled","failed","rejected","expired","no_reply","sent","handed_off"}:
            settled.append(row)
        else:
            unresolved.append({"request_id":row["request_id"],"status":row["status"]})
    return {"exists":True,"r1_schema":True,"settled":settled,"unresolved":unresolved}


def import_r1_settled(path,gateway):
    plan=inspect_r1(path)
    count=0
    # This is receipt import only, never a claim that unresolved R1 work finished.
    for row in plan["settled"]:
        raw=json.loads(row["message_json"])
        ctx=raw.get("context",{})
        event=Event(raw["tenant_id"],raw["message_id"],raw["chat_id"],raw["sender_id"],raw["text"],
            raw["event_created_at"],channel="bot_dm" if raw["chat_type"]=="p2p" else "group",
            thread_id=ctx.get("thread_id",""),version_time=raw["event_created_at"],
            observed_via="r1_receipt",mentions=(gateway.config.bot_id,) if raw.get("mentions_bot") else ())
        gateway.ingest(event,legacy_owned=True)
        count+=1
    return {"imported":count,"unresolved":plan["unresolved"],"old_database_modified":False,
            "can_finish_cutover":not plan["unresolved"]}
