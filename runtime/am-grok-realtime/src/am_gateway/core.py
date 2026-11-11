"""Durable orchestration of independent dialogue, business and output."""
from __future__ import annotations
import json
import uuid

from .context import Context
from .facts import Facts
from .models import Admission, Config, ContractError, Event, dumps, key
from .outbox import Outbox
from .policy import decision
from .router import BUSINESS_REQUESTS, Router


BOT_FIELDS={"schema","job_id","attempt_id","status","decision_mode","reply_text","hold_text",
            "approval_meta","task_requests","source_item_ids","answer_kind","cited_fact_ids",
            "action_intents","claims_current","error"}


class Gateway:
    def __init__(self, store, config: Config, adapters=None, *, artifact_resolver=None):
        config.validate()
        self.store,self.config=store,config
        self.adapters=dict(adapters or {})
        self.artifact_resolver=artifact_resolver
        self.facts=Facts(store,config)
        self.router=Router(store,config)
        self.context=Context(store,self.facts,config)
        self.outbox=Outbox(store,self.facts,config)
        for adapter in self.adapters.values():
            if hasattr(adapter,"on_progress"):
                adapter.on_progress=self.record_provider_progress
        with store.transaction() as db:
            current=db.execute("SELECT value FROM gw_meta WHERE key='tenant_id'").fetchone()
            if current and current[0]!=config.tenant_id:
                raise ContractError("database belongs to another tenant")
            cutover=db.execute("SELECT value FROM gw_meta WHERE key='live_cutover_at'").fetchone()
            if cutover and float(cutover[0])!=config.live_cutover_at:
                raise ContractError("reuse the persisted live_cutover_at on restart; changing it requires an explicit migration")
            db.execute("INSERT OR IGNORE INTO gw_meta VALUES('tenant_id',?)",(config.tenant_id,))
            db.execute("INSERT OR IGNORE INTO gw_meta VALUES('live_cutover_at',?)",(str(config.live_cutover_at),))

    def ingest(self, event: Event, *, legacy_owned=False) -> Admission:
        if event.tenant_id!=self.config.tenant_id:
            raise ContractError("tenant mismatch")
        with self.store.transaction() as db:
            admission=self.store.ingest_in_tx(db,event)
            if legacy_owned:
                db.execute("UPDATE gw_items SET legacy_owned=1 WHERE item_id=?", (event.item_id,))
            existing=db.execute("SELECT dialogue_claimed,legacy_owned FROM gw_items WHERE item_id=?", (event.item_id,)).fetchone()
            # This bounded lookup tells the host whether the OLD catch-all must
            # stop. Task creation remains on the route thread after durable ingest.
            handled=bool(existing["dialogue_claimed"]) or (
                not existing["legacy_owned"] and event.original_time>=self.config.live_cutover_at
                and self.router.is_dialogue(db,event))
            return Admission(admission.item_id,admission.inserted,admission.changed,handled)

    def route(self, limit=200):
        return self.router.route_pending(limit)

    def claim(self,workers):
        lease=max([self.config.job_lease_seconds]+[
            self.config.worker_timeouts.get(w,self.config.worker_timeout_seconds)+30 for w in workers])
        return self.store.claim_job(workers,lease)

    def record_provider_progress(self,event):
        with self.store.transaction() as db:
            row=db.execute("SELECT * FROM gw_jobs WHERE job_id=?",(event["job_id"],)).fetchone()
            if not row or row["attempt_id"]!=event["attempt_id"] or row["state"] not in {"running","submitted","unknown"}:
                return
            state=event["state"]
            now=self.store.clock()
            if state in {"accepted","submitted"}:
                db.execute("""UPDATE gw_jobs SET provider_run_id=?,provider_accepted_at=coalesce(provider_accepted_at,?),
                    state=CASE WHEN provider_started_at IS NULL THEN 'submitted' ELSE state END WHERE job_id=?""",
                    (event["provider_run_id"],now,event["job_id"]))
            elif state=="running":
                db.execute("""UPDATE gw_jobs SET provider_run_id=?,provider_started_at=coalesce(provider_started_at,?),
                    state='running' WHERE job_id=?""",(event["provider_run_id"],now,event["job_id"]))

    def read_input(self,job_id,worker,item_id,*,offset=0,limit=4000):
        job=self.store.one("SELECT * FROM gw_jobs WHERE job_id=?",(job_id,))
        if not job or job["worker"]!=worker:
            raise ContractError("job not owned by worker")
        context=json.loads(job["context_json"] or "{}")
        allowed={r["item_id"]:r for r in json.loads(job["input_refs"])}
        for fact in context.get("project_snapshot",[]):
            allowed.update({r["item_id"]:r for r in fact.get("source_refs",[])})
        if item_id not in allowed or not isinstance(offset,int) or offset<0 or not 1<=limit<=8000:
            raise ContractError("source outside job scope or invalid window")
        source=self.store.get_event(allowed[item_id])
        return {"item_id":item_id,"version":allowed[item_id]["version"],"body":source.body[offset:offset+limit],
                "offset":offset,"next_offset":offset+limit if offset+limit<len(source.body) else None,
                "total_characters":len(source.body),"attachments":list(source.attachments)}

    def request(self, job):
        context=self.context.build(job)
        return {"schema":"am.gateway.job.v2","job_id":job["job_id"],"attempt_id":job["attempt_id"],
                "task_kind":job["task_kind"],"worker":job["worker"],"session_key":job["session_key"],
                "conversation":json.loads(job["conversation_json"]),"context":context,
                "output_mode":job["output_mode"],"deadline_at":self.store.clock()+self.config.worker_timeouts.get(job["worker"],self.config.worker_timeout_seconds),
                "instruction":json.loads(job["conversation_json"]).get("instruction","")}

    def run_one(self, workers):
        job=self.claim(workers)
        if job:
            self.run_claimed(job)
        return job

    def run_claimed(self, job):
        try:
            request=self.request(job)
            adapter=self.adapters.get(job["worker"])
            if adapter is None:
                raise ContractError("adapter_unconfigured:"+job["worker"])
            result=adapter.run(request)
            if not isinstance(result,dict):
                raise ContractError("worker result must be JSON object")
            self.complete(job["job_id"],job["attempt_id"],result)
        except Exception as exc:
            # Known process failures and malformed outputs can be marked failed;
            # uncertain remote execution is retained as unknown, never replayed.
            status=getattr(exc,"status","failed")
            uncertain=status in {"unknown","submitted","timeout_unknown"} or getattr(exc,"unknown",False)
            self.fail(job["job_id"],job["attempt_id"],type(exc).__name__+":"+str(exc)[:180],unknown=uncertain)

    def maintenance(self):
        recovered=self.store.expire_leases()
        with self.store.transaction() as db:
            for row in db.execute("SELECT job_id FROM gw_jobs WHERE task_kind='aggregate' AND state='waiting'").fetchall():
                self._aggregate(db,row["job_id"])
        return recovered

    def complete(self, job_id, attempt_id, result):
        if (result.get("schema")!="am.gateway.result.v2" or result.get("job_id")!=job_id
            or result.get("attempt_id")!=attempt_id):
            raise ContractError("result correlation mismatch")
        if result.get("artifacts"):
            if not self.artifact_resolver or not isinstance(result["artifacts"],list):
                raise ContractError("artifact_resolver_unconfigured")
            # This port validates/registers local files idempotently. It does not
            # upload, send or mark delivery complete. Call it outside SQLite.
            result={**result,"artifacts":[self.artifact_resolver(a) for a in result["artifacts"]]}
            if any(not isinstance(a,dict) or not isinstance(a.get("name"),str) or not a["name"]
                   or not isinstance(a.get("uri"),str) or not a["uri"] for a in result["artifacts"]):
                raise ContractError("artifact resolver must return a verified name and resource uri")
        if result.get("status") not in {"completed","failed"}:
            raise ContractError("result status must be completed or failed")
        if result["status"]=="failed":
            return self.fail(job_id,attempt_id,str(result.get("error","worker_failed")))
        with self.store.transaction() as db:
            raw=db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not raw:
                raise ContractError("unknown job")
            job=dict(raw)
            if job["attempt_id"]!=attempt_id:
                raise ContractError("stale attempt")
            if job["state"]=="completed":
                if job["result_json"]!=dumps(result):
                    raise ContractError("conflicting duplicate result")
                return job
            if job["state"] not in {"running","submitted","unknown"}:
                raise ContractError("job not accepting results")
            if job["task_kind"]=="build_document" and not result.get("artifacts"):
                raise ContractError("completed document task requires a verified artifact")
            context=json.loads(job["context_json"] or "{}")
            allowed_sources={r["item_id"] for r in json.loads(job["input_refs"])}
            allowed_sources.update(sid for f in context.get("project_snapshot",[]) for sid in f["source_item_ids"])
            cited_sources=result.get("source_item_ids",[])
            if not isinstance(cited_sources,list) or any(sid not in allowed_sources for sid in cited_sources):
                raise ContractError("result source outside context")
            conv=json.loads(job["conversation_json"])
            owner=conv["channel"]=="bot_dm" and conv["actor_id"]==self.config.owner_id
            if job["worker"]=="grok_bot":
                if set(result)-BOT_FIELDS:
                    raise ContractError("unexpected Bot result fields")
                mode,reason=decision(result,context,owner=owner)
                if mode=="submit_task":
                    self._handoff(db,job,result)
                else:
                    if result.get("task_requests"):
                        raise ContractError("task requests require submit_task")
                    body=result.get("reply_text","")
                    if not body.strip():
                        raise ContractError("empty dialogue answer")
                    deps={f["fact_id"]:f["fingerprint"] for f in context.get("project_snapshot",[])
                          if f["fact_id"] in result.get("cited_fact_ids",[])}
                    self.outbox.final(db,job,body=body,require_approval=mode=="require_approval",
                                      dependencies=deps,reason=reason)
                    if mode=="require_approval" and self.config.send_hold and conv["channel"]=="group":
                        self.outbox.enqueue(db,job,purpose="hold",body="收到，这项内容我先跟示例负责人核对下口径，确认后回复。")
            else:
                # Internal jobs cannot create an outbox action, regardless of
                # display_text, decision_mode or forged targets in their result.
                if any(k in result for k in ("target","recipients","authorization_ref","task_requests","reply_text")):
                    raise ContractError("background result tried to route or authorize")
                # Persist only validated child outputs, so a malformed early
                # child cannot poison a later aggregation transaction.
                decision(result,context,owner=owner)
                self.facts.apply(db,job,result)
            db.execute("""UPDATE gw_jobs SET state='completed',completed_at=?,result_json=?,lease_until=NULL
                WHERE job_id=?""", (self.store.clock(),dumps(result),job_id))
            self.store.audit(db,"job_completed",job_id)
            if job["parent_job_id"]:
                self._aggregate(db,job["parent_job_id"])
            return dict(db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (job_id,)).fetchone())

    def _handoff(self, db, job, result):
        tasks=result.get("task_requests")
        if not isinstance(tasks,list) or not 1<=len(tasks)<=8:
            raise ContractError("one to eight task requests required")
        for task in tasks:
            if not isinstance(task,dict) or set(task)!={"task_kind","instruction"}:
                raise ContractError("task request contains unsupported fields")
            if task["task_kind"] not in BUSINESS_REQUESTS or not isinstance(task["instruction"],str) or not task["instruction"].strip():
                raise ContractError("invalid business request")
            if len(task["instruction"])>8000:
                raise ContractError("business request too long")
        bundle_id="bundle_"+key(job["request_id"])[:32]
        db.execute("""INSERT INTO gw_jobs(job_id,request_id,parent_job_id,logical_task_key,task_kind,worker,
            input_refs,conversation_json,session_key,priority,state,output_mode,reply_owner,created_at,
            available_at,context_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (bundle_id,job["request_id"],job["job_id"],key(job["job_id"],"aggregate"),"aggregate",
             "existing_executor",job["input_refs"],job["conversation_json"],job["session_key"],10,
             "waiting","reply",bundle_id,self.store.clock(),self.store.clock(),job["context_json"]))
        refs=json.loads(job["input_refs"])
        event=self.store.get_event(refs[-1],db)
        for i,task in enumerate(tasks):
            child=self.router.add_job(db,refs=refs,event=event,kind=task["task_kind"],
                request_id=job["request_id"],parent_id=bundle_id,output_mode="internal",
                priority=10,logical=key(job["job_id"],"child",i))
            conv=json.loads(job["conversation_json"])
            conv["instruction"]=task["instruction"]
            db.execute("UPDATE gw_jobs SET conversation_json=? WHERE job_id=?", (dumps(conv),child))
        db.execute("UPDATE gw_jobs SET reply_owner=? WHERE job_id=?", (bundle_id,job["request_id"]))
        self.outbox.enqueue(db,job,purpose="ack",body=f"已受理，任务编号 {bundle_id}。完成后我会把结果发回这里。")

    def _aggregate(self, db, bundle_id):
        bundle=db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (bundle_id,)).fetchone()
        if not bundle or bundle["task_kind"]!="aggregate" or bundle["state"]!="waiting":
            return
        children=db.execute("SELECT * FROM gw_jobs WHERE parent_job_id=? ORDER BY created_at,job_id",
                            (bundle_id,)).fetchall()
        if not children or any(c["state"] not in {"completed","failed","cancelled"} for c in children):
            return
        body=[]
        needs_review=False
        dependencies={}
        artifacts=[]
        for child in children:
            if child["state"]!="completed":
                body.append(f"{child['task_kind']}：{'结果待核对' if child['state']=='unknown' else '未完成'}（{child['last_error']}）。")
                continue
            result=json.loads(child["result_json"] or "{}")
            context=json.loads(child["context_json"] or "{}")
            mode,reason=decision(result,context)
            needs_review=needs_review or mode=="require_approval" or child["task_kind"]=="followup_draft"
            display=result.get("display_text","")
            if display:
                body.append(display)
            elif result.get("artifacts"):
                body.append("已生成："+ "、".join(a["name"] for a in result["artifacts"]))
            else:
                body.append(f"{child['task_kind']}：处理完成，未提供可展示正文。")
            artifacts.extend(result.get("artifacts",[]))
            dependencies.update({f["fact_id"]:f["fingerprint"] for f in context.get("project_snapshot",[])
                                 if f["fact_id"] in result.get("cited_fact_ids",[])})
        text="\n\n".join(body)
        action=self.outbox.final(db,dict(bundle),body=text,require_approval=needs_review,
                                dependencies=dependencies,reason="后台任务产出需核对的动作草稿" if needs_review else "")
        if artifacts:
            db.execute("UPDATE gw_outbox SET payload_json=? WHERE action_id=?",
                       (dumps({"artifacts":artifacts}),action["action_id"]))
        db.execute("UPDATE gw_jobs SET state='completed',completed_at=?,result_json=? WHERE job_id=?",
                   (self.store.clock(),dumps({"display_text":text,"artifacts":artifacts}),bundle_id))

    def fail(self, job_id, attempt_id, error, *, unknown=False):
        with self.store.transaction() as db:
            raw=db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not raw or raw["attempt_id"]!=attempt_id or raw["state"] in {"completed","cancelled"}:
                return None
            job=dict(raw)
            db.execute("UPDATE gw_jobs SET state=?,last_error=?,completed_at=?,lease_until=NULL WHERE job_id=?",
                       ("unknown" if unknown else "failed",error,self.store.clock(),job_id))
            self.store.audit(db,"job_unknown" if unknown else "job_failed",job_id,error)
            if job["output_mode"]=="reply":
                if unknown:
                    self.outbox.enqueue(db,job,purpose="status",body="这次处理结果还在核对中，任务已保留。")
                else:
                    self.outbox.final(db,job,body="这次处理尚未得到可确认的结果，已记录任务状态。")
            if job["parent_job_id"]:
                self._aggregate(db,job["parent_job_id"])
            return {"job_id":job_id,"state":"unknown" if unknown else "failed","error":error}
