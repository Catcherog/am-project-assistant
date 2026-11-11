"""Code-only routing; the Bot is never the classifier for incoming business data."""
from __future__ import annotations
import json

from .models import Config, Event, dumps, key


INTERNAL_KINDS = {"personal_context_update", "project_state_update", "tag_candidates", "normalize_text"}
BUSINESS_REQUESTS = {"project_analysis","followup_draft","document_content","build_document","code_task"}


class Router:
    def __init__(self, store, config: Config):
        self.store, self.config = store, config

    def is_dialogue(self, db, event: Event) -> bool:
        c = self.config
        if event.is_self or event.is_deleted or event.kind != "message":
            return False
        if event.channel == "bot_dm":
            return True
        if event.channel != "group":
            return False
        if c.bot_id in event.mentions:
            return True
        # A matching thread alone is insufficient. The anchor must be a SENT
        # message from this gateway, in the same chat and exact thread.
        if event.parent_id:
            row = db.execute("""SELECT event_json FROM gw_items WHERE tenant_id=? AND native_id=?
                AND kind='message' AND chat_id=? AND thread_id=? AND is_self=1""",
                (event.tenant_id,event.parent_id,event.chat_id,event.thread_id)).fetchone()
            return bool(row and db.execute("""SELECT 1 FROM gw_outbox
                WHERE platform_id=? AND state='sent' AND action_type='message'""",
                (event.parent_id,)).fetchone())
        return False

    def conversation(self, event):
        return {"tenant_id":event.tenant_id,"chat_id":event.chat_id,"thread_id":event.thread_id,
                "actor_id":event.sender_id,"channel":event.channel,"parent_message_id":event.native_id,
                "project_ids":list(event.project_ids)}

    def session(self, event):
        # Preserve AM's existing group + user isolation, and additionally isolate topics.
        return key(event.tenant_id,event.chat_id,event.thread_id,event.sender_id)

    def add_job(self, db, *, refs, event, kind, project_id="", request_id=None,
                parent_id=None, output_mode="internal", priority=20, logical=None, batching=False):
        now, c = self.store.clock(), self.config
        worker = c.worker_map[kind]
        logical = logical or key(refs,kind,project_id,request_id)
        jid = "job_" + key(logical)[:32]
        existing = db.execute("SELECT job_id FROM gw_job_routes WHERE logical_task_key=?", (logical,)).fetchone()
        if existing:
            return existing["job_id"]
        batch_key = key(event.tenant_id,kind,project_id,event.chat_id,event.channel)
        if batching:
            batch = db.execute("""SELECT * FROM gw_jobs WHERE batch_key=? AND state='queued'
                AND output_mode='internal' AND parent_job_id IS NULL AND available_at>?
                ORDER BY created_at LIMIT 1""", (batch_key,now)).fetchone()
            if batch:
                old_refs = json.loads(batch["input_refs"])
                chars = sum(len(self.store.get_event(r,db).body) for r in old_refs)
                added_chars = sum(len(self.store.get_event(r,db).body) for r in refs)
                if len(old_refs)+len(refs) <= c.batch_max_items and chars+added_chars <= c.batch_max_characters:
                    combined = old_refs+refs
                    due = now if len(combined) == c.batch_max_items or chars+added_chars == c.batch_max_characters else batch["available_at"]
                    db.execute("UPDATE gw_jobs SET input_refs=?,available_at=? WHERE job_id=?",
                               (dumps(combined),due,batch["job_id"]))
                    db.execute("INSERT INTO gw_job_routes VALUES(?,?)",(logical,batch["job_id"]))
                    return batch["job_id"]
        rid = request_id or jid
        db.execute("""INSERT INTO gw_jobs(job_id,request_id,parent_job_id,logical_task_key,task_kind,
            worker,input_refs,conversation_json,session_key,project_id,batch_key,priority,state,
            output_mode,reply_owner,created_at,available_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (jid,rid,parent_id,logical,kind,worker,dumps(refs),dumps(self.conversation(event)),self.session(event),
             project_id,batch_key,priority,"queued",output_mode,jid,now,now+c.batch_wait_seconds if batching else now))
        db.execute("INSERT INTO gw_job_routes VALUES(?,?)",(logical,jid))
        return jid

    def route_pending(self, limit=200):
        routed = []
        with self.store.transaction() as db:
            pending = db.execute("SELECT * FROM gw_items WHERE routed_version<version ORDER BY received_at LIMIT ?",
                                 (limit,)).fetchall()
            for row in pending:
                event = Event.from_dict(json.loads(row["event_json"]))
                refs = [{"item_id":row["item_id"],"version":row["version"]}]
                if event.tenant_id != self.config.tenant_id:
                    raise ValueError("tenant mismatch")
                if not event.is_self and not event.is_deleted and event.content_status!="draft_edit":
                    if (not row["legacy_owned"] and not row["dialogue_claimed"]
                        and event.original_time >= self.config.live_cutover_at and self.is_dialogue(db,event)):
                        jid = self.add_job(db,refs=refs,event=event,kind="dialogue_reply",
                            output_mode="reply",priority=0,logical=key(event.item_id,"dialogue"))
                        db.execute("UPDATE gw_items SET dialogue_claimed=? WHERE item_id=?", (jid,event.item_id))
                        routed.append(jid)
                    # Same event, independent internal job. Private facts stay private
                    # even if they refer to a known project.
                    if event.channel == "personal_dm":
                        routed.append(self.add_job(db,refs=refs,event=event,kind="personal_context_update",batching=True))
                    elif event.project_ids:
                        for project in event.project_ids:
                            routed.append(self.add_job(db,refs=refs,event=event,kind="project_state_update",
                                                       project_id=project,batching=True))
                    elif event.channel != "bot_dm":
                        routed.append(self.add_job(db,refs=refs,event=event,kind="tag_candidates",batching=True))
                db.execute("UPDATE gw_items SET routed_version=version WHERE item_id=?", (event.item_id,))
                if event.is_deleted:
                    # Retraction cancels unsent work for this message only.
                    self._retract(db, row["item_id"])
        return routed

    def _retract(self, db, item_id):
        for job in db.execute("SELECT * FROM gw_jobs WHERE state NOT IN ('cancelled','failed')").fetchall():
            refs=json.loads(job["input_refs"])
            if any(r["item_id"]==item_id for r in refs):
                remaining=[r for r in refs if r["item_id"]!=item_id]
                if remaining and job["output_mode"]=="internal" and job["parent_job_id"] is None:
                    if job["state"]=="queued":
                        db.execute("UPDATE gw_jobs SET input_refs=? WHERE job_id=?",(dumps(remaining),job["job_id"]))
                        continue
                    if job["state"] in {"running","submitted","unknown"}:
                        event=self.store.get_event(remaining[-1],db)
                        self.add_job(db,refs=remaining,event=event,kind=job["task_kind"],project_id=job["project_id"],
                                     logical=key("retraction_remainder",job["job_id"],remaining))
                db.execute("UPDATE gw_jobs SET state='cancelled',last_error='source_retracted' WHERE job_id=?", (job["job_id"],))
                db.execute("""UPDATE gw_outbox SET state='cancelled',last_error='source_retracted'
                    WHERE request_id=? AND state IN ('ready','pending_approval')""", (job["request_id"],))
        for fact in db.execute("SELECT fact_id,source_refs FROM gw_facts").fetchall():
            if any(r["item_id"]==item_id for r in json.loads(fact["source_refs"])):
                db.execute("UPDATE gw_facts SET status='withdrawn',version=version+1 WHERE fact_id=?", (fact["fact_id"],))
