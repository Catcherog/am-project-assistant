"""Scoped, bounded per-request context; no native global conversation memory."""
from __future__ import annotations
import json
from contextlib import closing

from .models import dumps


class Context:
    def __init__(self, store, facts, config):
        self.store,self.facts,self.config=store,facts,config

    def build(self, job):
        c=self.config
        conv=json.loads(job["conversation_json"])
        with closing(self.store.connect()) as db:
            db.execute("BEGIN")
            refs=json.loads(job["input_refs"])
            inputs=[]
            for ref in refs:
                event=self.store.get_event(ref,db)
                inputs.append({**event.to_dict(),"item_id":ref["item_id"],"version":ref["version"]})
            snapshots=self.facts.visible(db,conv,job["project_id"])
            recent=[]
            if job["task_kind"]=="dialogue_reply":
                rows=db.execute("""SELECT * FROM gw_items WHERE tenant_id=? AND chat_id=? AND thread_id=?
                    AND original_time>=? AND is_deleted=0
                    AND ((sender_id=? AND channel=? AND dialogue_claimed IS NOT NULL)
                     OR (is_self=1 AND recipient_actor=?))
                    ORDER BY original_time DESC,received_at DESC LIMIT ?""",
                    (c.tenant_id,conv["chat_id"],conv["thread_id"],self.store.clock()-c.conversation_ttl_seconds,
                     conv["actor_id"],conv["channel"],conv["actor_id"],c.recent_limit+len(refs))).fetchall()
                current={r["item_id"] for r in refs}
                for row in rows:
                    if row["item_id"] in current:
                        continue
                    e=json.loads(row["event_json"])
                    if row["is_self"] and not db.execute("SELECT 1 FROM gw_outbox WHERE platform_id=? AND state='sent'",
                                                         (row["native_id"],)).fetchone():
                        continue
                    recent.append({"item_id":row["item_id"],"role":"assistant" if row["is_self"] else "user",
                                   "text":e["body"],"at":row["original_time"]})
                    if len(recent)==c.recent_limit:
                        break
                recent.reverse()
            context={"input_items":inputs,"recent_messages":recent,"project_snapshot":snapshots,
                     "as_of":self.store.clock(),"scope":conv,
                     "memory_policy":{"native_session":"stateless","fact_write":job["worker"]=="grok_cli",
                                      "background_scan":False,"untrusted_input":True}}
            if job["task_kind"]=="dialogue_reply" and inputs[-1].get("parent_id"):
                anchor=db.execute("""SELECT i.event_json FROM gw_items i JOIN gw_outbox o ON o.platform_id=i.native_id
                    WHERE i.tenant_id=? AND i.native_id=? AND i.chat_id=? AND i.thread_id=?
                    AND i.is_self=1 AND i.is_deleted=0 AND o.state='sent' AND o.action_type='message'""",
                    (c.tenant_id,inputs[-1]["parent_id"],conv["chat_id"],conv["thread_id"])).fetchone()
                if anchor:
                    anchor_event=json.loads(anchor["event_json"])
                    context["reply_anchor"]={"native_id":anchor_event["native_id"],
                                             "text":anchor_event["body"][:4000],"role":"assistant",
                                             "truncated":len(anchor_event["body"])>4000}
            # Retain full source bodies in gw_items. References explain every
            # shortened input; long documents are fetched as separate tasks.
            budget=c.context_char_budget
            for item in context["input_items"]:
                cap=max(256,min(4000,budget//max(1,len(inputs))))
                if len(item["body"])>cap:
                    item["body"]=item["body"][:cap]
                    item["body_truncated"]=True
                    item["full_text_ref"]={"item_id":item["item_id"],"version":item["version"]}
            while len(dumps(context))>budget and context["recent_messages"]:
                context["recent_messages"].pop(0)
            while len(dumps(context))>budget and context["project_snapshot"]:
                context["project_snapshot"].pop()
                context["more_facts_available"]=True
            if len(dumps(context))>budget:
                # Structured payloads can be large too; keep immutable references
                # and mark them explicitly instead of silently dropping raw data.
                for item in context["input_items"]:
                    item["attachments"]=[{"resource_ref":a.get("resource_ref") or a.get("resource_key") or a.get("file_key") or a.get("image_key"),
                                          "content_status":"indexed"} for a in item["attachments"]]
                    item["body"]=item["body"][:256]
                    item["body_truncated"]=True
                    item["full_text_ref"]={"item_id":item["item_id"],"version":item["version"]}
            if len(dumps(context))>budget:
                context.pop("reply_anchor",None)
                context["input_items"]=[{"item_id":x["item_id"],"version":x["version"],
                                         "full_text_ref":{"item_id":x["item_id"],"version":x["version"]},
                                         "body_truncated":True} for x in context["input_items"]]
                context["input_bodies_deferred"]=True
            if len(dumps(context))>budget:
                # Do not claim a complete payload after silently dropping inputs.
                raise ValueError("context budget too small for input references")
        with self.store.transaction() as db:
            db.execute("UPDATE gw_jobs SET context_json=? WHERE job_id=? AND attempt_id=?",
                       (dumps(context),job["job_id"],job["attempt_id"]))
        return context
