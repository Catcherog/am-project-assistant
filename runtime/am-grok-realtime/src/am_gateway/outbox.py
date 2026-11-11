"""One sending path. Approval freezes exact content; no model runs after approval."""
from __future__ import annotations
import json
import uuid

from .models import ContractError, Event, SendReceipt, dumps


class Outbox:
    def __init__(self, store, facts, config):
        self.store,self.facts,self.config=store,facts,config

    def enqueue(self, db, job, *, purpose, body, state="ready", dependencies=None,
                reason="", payload=None, target=None, action_type="message"):
        conv=json.loads(job["conversation_json"])
        if target is None:
            target={"chat_id":conv["chat_id"],"thread_id":conv["thread_id"],
                    "reply_to_message_id":conv["parent_message_id"],"actor_id":conv["actor_id"],
                    "channel":conv["channel"]}
        aid="out_"+uuid.uuid4().hex
        db.execute("""INSERT OR IGNORE INTO gw_outbox(action_id,job_id,request_id,reply_owner,purpose,
            target_json,action_type,body,payload_json,state,dependencies,reason,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (aid,job["job_id"],job["request_id"],job["job_id"],purpose,dumps(target),action_type,body,
             dumps(payload or {}),state,dumps(dependencies or {}),reason,self.store.clock()))
        row=dict(db.execute("SELECT * FROM gw_outbox WHERE request_id=? AND purpose=?",
                            (job["request_id"],purpose)).fetchone())
        return row

    def final(self, db, job, *, body, require_approval=False, dependencies=None, reason=""):
        root=db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (job["request_id"],)).fetchone()
        if job["output_mode"]!="reply" or not root or root["reply_owner"]!=job["job_id"]:
            raise ContractError("job does not own this reply")
        row=self.enqueue(db,job,purpose="final",body=body,
                         state="pending_approval" if require_approval else "ready",
                         dependencies=dependencies,reason=reason)
        if row["state"]=="pending_approval":
            self._notice(db,row)
        return row

    def _notice(self, db, action):
        job=dict(db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (action["job_id"],)).fetchone())
        target=json.loads(action["target_json"])
        refs=json.loads(job["input_refs"])
        question="\n".join(self.store.get_event(r,db).body for r in refs)
        context=json.loads(job["context_json"] or "{}")
        old={f["fact_id"]:f for f in context.get("project_snapshot",[])}
        evidence=[]
        for fid in json.loads(action["dependencies"]):
            live=db.execute("SELECT * FROM gw_facts WHERE fact_id=?",(fid,)).fetchone()
            before=old.get(fid)
            evidence.append({"fact_id":fid,"before":before,
                "current":None if not live else {"project_id":live["project_id"],"field":live["field"],
                    "value":json.loads(live["value_json"]),"status":live["status"],
                    "source_refs":json.loads(live["source_refs"])}})
        def button(label, choice, typ="default"):
            return {"tag":"button","text":{"tag":"plain_text","content":label},"type":typ,
                    "value":{"gateway":"r2","action_id":action["action_id"],
                             "draft_version":action["draft_version"],"decision":choice}}
        card={"config":{"wide_screen_mode":True},
              "header":{"title":{"tag":"plain_text","content":"AM · 请核对本次回复"}},
              "elements":[
                {"tag":"markdown","content":f"**提问人**：{target['actor_id']}\n**原会话**：{target['chat_id']}\n**原话**：\n{question[:6000]}"},
                {"tag":"markdown","content":f"**核对原因**：{action['reason']}\n**拟回复**：\n{action['body']}"},
                {"tag":"markdown","content":"**依据（草拟时／现在）**：\n"+("\n".join(dumps(f) for f in evidence) or "以提问原话与当前草稿为准；未引用项目事实。")},
                {"tag":"action","actions":[button("确认发送","approve","primary"),button("修改草稿","edit"),button("忽略","ignore")]}
              ]}
        self.enqueue(db,job,purpose=f"approval_notice:{action['action_id']}:{action['draft_version']}",
                     body="请核对本次回复",payload={"card":card,"related_action_id":action["action_id"]},
                     target={"chat_id":self.config.owner_chat_id,"thread_id":"","reply_to_message_id":"",
                             "actor_id":self.config.owner_id,"channel":"bot_dm"},action_type="approval_card")

    def review(self, action_id, *, actor_id, draft_version, decision, edited_text=None):
        with self.store.transaction() as db:
            return self.review_in_transaction(db,action_id,actor_id=actor_id,
                draft_version=draft_version,decision=decision,edited_text=edited_text)

    def review_in_transaction(self, db, action_id, *, actor_id, draft_version, decision, edited_text=None):
        """Host editing atomically records input, changes draft and consumes session."""
        if actor_id!=self.config.owner_id:
            raise ContractError("only the bound owner may review")
        raw=db.execute("SELECT * FROM gw_outbox WHERE action_id=?", (action_id,)).fetchone()
        if not raw or raw["purpose"]!="final":
            raise ContractError("unknown review action")
        action=dict(raw)
        if action["draft_version"]!=draft_version:
            raise ContractError("draft changed; use the current preview")
        if action["state"] in {"ready","sending","sent","unknown","ignored"}:
            # Duplicate taps return the durable state and never enqueue again.
            return action
        if action["state"]!="pending_approval":
            raise ContractError("action is not reviewable")
        if decision=="edit":
            if edited_text is None:
                return {**action,"edit_required":True,"edit_action_id":action_id}
            if not isinstance(edited_text,str) or not edited_text.strip() or len(edited_text)>16000:
                raise ContractError("invalid edited draft")
            db.execute("""UPDATE gw_outbox SET body=?,draft_version=draft_version+1,
                authorization_ref='' WHERE action_id=?""", (edited_text,action_id))
            revised=dict(db.execute("SELECT * FROM gw_outbox WHERE action_id=?", (action_id,)).fetchone())
            self._notice(db,revised)
            return revised
        if decision=="ignore":
            db.execute("UPDATE gw_outbox SET state='ignored' WHERE action_id=?", (action_id,))
        elif decision=="approve":
            changed=self.facts.changed_dependencies(db,json.loads(action["dependencies"]))
            if changed:
                return self._refresh(db,action,changed)
            db.execute("""UPDATE gw_outbox SET state='ready',authorization_ref=?
                WHERE action_id=? AND state='pending_approval'""",
                (f"owner:{actor_id}:draft:{draft_version}",action_id))
        else:
            raise ContractError("unknown review decision")
        return dict(db.execute("SELECT * FROM gw_outbox WHERE action_id=?", (action_id,)).fetchone())

    def _refresh(self, db, action, changed):
        deps=json.loads(action["dependencies"])
        deps.update(changed)
        reason="本草稿实际引用的依据发生变化，请核对后确认。变化字段："+", ".join(changed)
        db.execute("""UPDATE gw_outbox SET state='pending_approval',draft_version=draft_version+1,
            dependencies=?,authorization_ref='',reason=? WHERE action_id=?""",
            (dumps(deps),reason,action["action_id"]))
        updated=dict(db.execute("SELECT * FROM gw_outbox WHERE action_id=?", (action["action_id"],)).fetchone())
        self._notice(db,updated)
        return updated

    def send_one(self, sender):
        with self.store.transaction() as db:
            row=db.execute("SELECT * FROM gw_outbox WHERE state='ready' ORDER BY created_at,action_id LIMIT 1").fetchone()
            if not row:
                return None
            action=dict(row)
            if action["purpose"]=="final":
                root=db.execute("SELECT * FROM gw_jobs WHERE job_id=?", (action["request_id"],)).fetchone()
                if not root or root["reply_owner"]!=action["reply_owner"] or root["state"]=="cancelled":
                    db.execute("UPDATE gw_outbox SET state='cancelled',last_error='reply_owner_changed' WHERE action_id=?",
                               (action["action_id"],))
                    return {"state":"cancelled","action_id":action["action_id"]}
                for ref in json.loads(root["input_refs"]):
                    source=db.execute("SELECT is_deleted FROM gw_items WHERE item_id=?",(ref["item_id"],)).fetchone()
                    if source and source["is_deleted"]:
                        db.execute("UPDATE gw_outbox SET state='cancelled',last_error='source_retracted' WHERE action_id=?",
                                   (action["action_id"],))
                        return {"state":"cancelled","action_id":action["action_id"]}
                changed=self.facts.changed_dependencies(db,json.loads(action["dependencies"]))
                if changed:
                    return self._refresh(db,action,changed)
            db.execute("UPDATE gw_outbox SET state='sending',lease_until=? WHERE action_id=?",
                       (self.store.clock()+self.config.job_lease_seconds,action["action_id"]))
        # The action ID is the idempotency key. It differs for hold/ack/final/cards.
        try:
            receipt=sender.send(action)
            if not isinstance(receipt,SendReceipt) or receipt.status not in {"sent","failed","unknown"}:
                receipt=SendReceipt("unknown",detail="invalid_sender_receipt")
            if receipt.status=="sent" and not receipt.platform_id:
                receipt=SendReceipt("unknown",detail="missing_platform_id")
        except Exception as exc:
            receipt=SendReceipt("unknown",detail=type(exc).__name__)
        return self.record_receipt(action["action_id"],receipt)

    def record_receipt(self, action_id, receipt: SendReceipt):
        with self.store.transaction() as db:
            raw=db.execute("SELECT * FROM gw_outbox WHERE action_id=?", (action_id,)).fetchone()
            if not raw:
                raise ContractError("unknown action")
            action=dict(raw)
            if action["state"]=="sent":
                if receipt.status=="sent" and receipt.platform_id!=action["platform_id"]:
                    raise ContractError("receipt conflicts with recorded platform_id")
                return action
            if action["state"] not in {"sending","unknown"}:
                raise ContractError("receipt without a send attempt")
            if receipt.status=="sent" and not receipt.platform_id:
                raise ContractError("sent receipt requires platform_id")
            now=self.store.clock()
            db.execute("""UPDATE gw_outbox SET state=?,platform_id=?,sent_at=?,last_error=?,lease_until=NULL
                WHERE action_id=?""", (receipt.status,receipt.platform_id,now if receipt.status=="sent" else None,
                                      receipt.detail,action_id))
            if receipt.status=="sent" and action["action_type"]=="message":
                target=json.loads(action["target_json"])
                event=Event(tenant_id=self.config.tenant_id,native_id=receipt.platform_id,
                    chat_id=target["chat_id"],sender_id=self.config.bot_id,body=action["body"],original_time=now,
                    channel=target["channel"],thread_id=target["thread_id"],
                    parent_id=target["reply_to_message_id"],version_time=now,
                    observed_via="outbox",is_self=True,direction="outbound")
                self.store.ingest_in_tx(db,event)
                db.execute("""UPDATE gw_items SET recipient_actor=?,request_id=?,routed_version=version
                    WHERE item_id=?""", (target["actor_id"],action["request_id"],event.item_id))
            return dict(db.execute("SELECT * FROM gw_outbox WHERE action_id=?", (action_id,)).fetchone())

    def reconcile_unknown(self, sender, limit=20):
        """lookup(action) must independently confirm the same platform send."""
        results=[]
        if not callable(getattr(sender,"lookup",None)):
            return results
        for action in self.store.rows("SELECT * FROM gw_outbox WHERE state='unknown' ORDER BY created_at LIMIT ?", (limit,)):
            try:
                receipt=sender.lookup(action)
            except Exception:
                continue
            if isinstance(receipt,SendReceipt) and receipt.status=="sent" and receipt.platform_id:
                results.append(self.record_receipt(action["action_id"],receipt))
        return results
