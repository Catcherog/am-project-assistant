"""R2 ports for the existing Bridge. This module opens no Feishu connection."""
from __future__ import annotations

from dataclasses import dataclass
import json
import time
from typing import Callable
import uuid

from am_gateway.models import ContractError, Event, SendReceipt, dumps
from am_gateway.normalization import normalized_content


def platform_seconds(value):
    number=float(value or 0)
    return number/1000 if number>=100000000000 else number


def normalize_message(payload, config, *, observed_via="event", channel=None, project_ids=()):
    envelope=payload.get("event",payload)
    msg=envelope.get("message",envelope)
    sender=envelope.get("sender",msg.get("sender",{}))
    sid=sender.get("sender_id",sender.get("id",{}))
    sender_id=sid.get("open_id","") if isinstance(sid,dict) else sid
    if not sender_id:
        raise ContractError("sender open_id missing; normalize ID type in host adapter")
    content=msg.get("content",msg.get("body",{}).get("content",""))
    mentions=[]
    for mention in msg.get("mentions",[]):
        mid=mention.get("id",{})
        mid=mid.get("open_id","") if isinstance(mid,dict) else mid
        if mid: mentions.append(mid)
    # The application's verified receive event knows whether p2p is addressed
    # to THIS bot. A user-identity history page must explicitly pass personal_dm.
    if channel is None:
        channel="bot_dm" if msg.get("chat_type")=="p2p" and observed_via=="event" else "group"
    text,attachments,content_status=normalized_content(content,msg.get("message_type",msg.get("msg_type","")))
    header=payload.get("header",{})
    tenant=header.get("tenant_key") or envelope.get("tenant_key") or config.tenant_id
    when=platform_seconds(msg.get("create_time") or msg.get("created_at"))
    event=Event(tenant_id=tenant,native_id=msg.get("message_id",""),chat_id=msg.get("chat_id",""),
        sender_id=sender_id,body=text,original_time=when,channel=channel,
        thread_id=msg.get("thread_id",""),parent_id=msg.get("parent_id",""),root_id=msg.get("root_id",""),
        mentions=tuple(mentions),project_ids=tuple(project_ids),attachments=tuple(attachments),
        version_time=platform_seconds(msg.get("update_time")) or when,observed_via=observed_via,
        is_self=sender_id==config.bot_id,is_deleted=bool(msg.get("deleted",False)),
        content_status=content_status)
    event.validate()
    return event


@dataclass(frozen=True)
class HostAdmission:
    captured: bool
    claimed: bool
    dialogue: bool
    item_id: str
    duplicate: bool


class BridgeIngress:
    def __init__(self,gateway,*,legacy_control:Callable[[dict],bool],
                 verified_scope:Callable[[dict],bool],projects_for:Callable[[dict],tuple],
                 scheduler=None,archive=None):
        # Use current host auth/cache and explicit pending-action parser. Never
        # pass AmSelfService.wants here: its catch-all can capture every message.
        self.gateway=gateway
        self.legacy_control=legacy_control
        self.verified_scope=verified_scope
        self.projects_for=projects_for
        self.scheduler=scheduler
        self.archive=archive
        with gateway.store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS gw_edit_sessions(
                actor_id TEXT PRIMARY KEY,action_id TEXT NOT NULL,draft_version INTEGER NOT NULL,
                expires_at REAL NOT NULL)""")

    def ingest(self,payload):
        if not self.verified_scope(payload):
            return HostAdmission(False,False,False,"",False)
        event=normalize_message(payload,self.gateway.config,project_ids=self.projects_for(payload))
        if event.tenant_id!=self.gateway.config.tenant_id:
            raise ContractError("tenant mismatch")
        if event.channel=="bot_dm" and event.sender_id==self.gateway.config.owner_id:
            with self.gateway.store.transaction() as db:
                old=db.execute("SELECT event_json FROM gw_items WHERE item_id=?",(event.item_id,)).fetchone()
                if old and json.loads(old["event_json"]).get("content_status")=="draft_edit":
                    return HostAdmission(True,True,False,event.item_id,True)
                editing=db.execute("SELECT * FROM gw_edit_sessions WHERE actor_id=? AND expires_at>?",
                                   (event.sender_id,self.gateway.store.clock())).fetchone()
                if editing:
                    # The input, exact replacement and session consumption commit
                    # together. Duplicate SDK deliveries cannot edit a second time.
                    from dataclasses import replace
                    event=replace(event,content_status="draft_edit")
                    result=self.gateway.store.ingest_in_tx(db,event)
                    db.execute("UPDATE gw_items SET legacy_owned=1 WHERE item_id=?",(event.item_id,))
                    if event.body.strip()!="取消修改":
                        self.gateway.outbox.review_in_transaction(db,editing["action_id"],actor_id=event.sender_id,
                            draft_version=editing["draft_version"],decision="edit",edited_text=event.body)
                    db.execute("DELETE FROM gw_edit_sessions WHERE actor_id=?",(event.sender_id,))
            if editing:
                if self.scheduler: self.scheduler.wakeup.set()
                return HostAdmission(True,True,False,result.item_id,not result.inserted)
        legacy=bool(self.legacy_control(payload))
        result=self.gateway.ingest(event,legacy_owned=legacy)
        if self.archive:
            self.archive(payload)  # bounded local archive; no model or API fetch
        if self.scheduler:
            self.scheduler.wakeup.set()
        return HostAdmission(True,not legacy,result.handled,result.item_id,not result.inserted and not result.changed)

    def review(self,verified_actor,action_value,edited_text=None):
        if action_value.get("gateway")!="r2":
            return None
        result=self.gateway.outbox.review(action_value["action_id"],actor_id=verified_actor,
            draft_version=int(action_value["draft_version"]),decision=action_value["decision"],
            edited_text=edited_text)
        if result.get("edit_required"):
            with self.gateway.store.transaction() as db:
                db.execute("""INSERT INTO gw_edit_sessions VALUES(?,?,?,?)
                    ON CONFLICT(actor_id) DO UPDATE SET action_id=excluded.action_id,
                    draft_version=excluded.draft_version,expires_at=excluded.expires_at""",
                    (verified_actor,result["action_id"],result["draft_version"],self.gateway.store.clock()+600))
                job=dict(db.execute("SELECT * FROM gw_jobs WHERE job_id=?",(result["job_id"],)).fetchone())
                self.gateway.outbox.enqueue(db,job,purpose=f"edit_prompt:{result['action_id']}:{result['draft_version']}",
                    body="请在这条私聊直接发送修改后的完整草稿；发“取消修改”可退出。修改后会重新展示预览。",
                    target={"chat_id":self.gateway.config.owner_chat_id,"thread_id":"","reply_to_message_id":"",
                            "actor_id":self.gateway.config.owner_id,"channel":"bot_dm"})
        return result

    def card_callback(self,payload):
        e=payload.get("event",payload)
        action=e.get("action",{})
        value=action.get("value",{})
        if not isinstance(value,dict) or value.get("gateway")!="r2":
            return None
        actor=e.get("operator",{}).get("open_id")
        if not actor:
            raise ContractError("verified card operator missing")
        result=self.review(actor,value)
        state=result["state"]
        text="请在 AM 私聊发送修改后的完整草稿。" if result.get("edit_required") else {
            "ready":"已确认，正在发送。","sent":"本次内容已发送。","pending_approval":"依据已变化，请查看更新后的预览。",
            "ignored":"已忽略本次草稿。","unknown":"发送结果正在核对。","sending":"正在发送。"
        }.get(state,"本次状态已记录。")
        return {"toast":{"type":"info","content":text}}

    def recall(self,payload):
        event=payload.get("event",payload)
        mid=event.get("message_id") or event.get("message",{}).get("message_id")
        row=self.gateway.store.one("SELECT event_json FROM gw_items WHERE tenant_id=? AND native_id=? AND kind='message'",
                                  (self.gateway.config.tenant_id,mid))
        if not row:
            return None
        from dataclasses import replace
        original=Event.from_dict(json.loads(row["event_json"]))
        recalled=replace(original,is_deleted=True,observed_via="recall",
                         version_time=max(self.gateway.store.clock(),original.version_time))
        result=self.gateway.ingest(recalled)
        if self.scheduler: self.scheduler.wakeup.set()
        return result


class ExistingSender:
    """Bind the existing Feishu executor's per-action send, card and readback APIs.

    send_message(target, text, uuid, artifacts) -> platform_id
    send_card(target, card, uuid) -> platform_id
    lookup_message(action, uuid) -> platform_id | None

    Do NOT bind the R1 reply(original_mid,text) closure: its UUID was based only
    on original_mid, so hold/ack would collide with final.
    """
    def __init__(self,send_message,send_card,*,lookup_message=None):
        self.send_message,self.send_card,self.lookup_message=send_message,send_card,lookup_message

    @staticmethod
    def send_uuid(action):
        return str(uuid.uuid5(uuid.NAMESPACE_URL,"am-gateway-r2/"+action["action_id"]))

    def send(self,action):
        target=json.loads(action["target_json"])
        payload=json.loads(action["payload_json"])
        send_id=self.send_uuid(action)
        try:
            if action["action_type"]=="approval_card":
                mid=self.send_card(target,payload["card"],send_id)
            else:
                mid=self.send_message(target,action["body"],send_id,payload.get("artifacts",[]))
        except Exception as exc:
            return SendReceipt("unknown",detail=type(exc).__name__)
        return SendReceipt("sent",mid) if isinstance(mid,str) and mid else SendReceipt("unknown",detail="missing_platform_id")

    def lookup(self,action):
        if self.lookup_message is None:
            return SendReceipt("unknown",detail="readback_unconfigured")
        mid=self.lookup_message(action,self.send_uuid(action))
        return SendReceipt("sent",mid) if mid else SendReceipt("unknown",detail="not_confirmed")


class ExistingStateProjection:
    """Idempotent bridge to existing INTERNAL state; never writes an external ledger.

    The host maps fact_id to an existing project/task identifier and preserves it.
    save_internal(fact, idempotency_key) must upsert rather than append a new todo.
    No host binding means the gateway's internal materialized view remains usable.
    """
    def __init__(self,gateway,save_internal):
        self.gateway,self.save_internal=gateway,save_internal
        with gateway.store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS gw_projection_receipts(
                fact_id TEXT PRIMARY KEY,version INTEGER NOT NULL)""")

    def sync(self,limit=100):
        rows=self.gateway.store.rows("""SELECT f.* FROM gw_facts f
            LEFT JOIN gw_projection_receipts p ON p.fact_id=f.fact_id
            WHERE p.version IS NULL OR p.version<f.version ORDER BY f.updated_at LIMIT ?""",(limit,))
        saved=[]
        for row in rows:
            self.save_internal(row,f"{row['fact_id']}:{row['version']}")
            with self.gateway.store.transaction() as db:
                db.execute("""INSERT INTO gw_projection_receipts VALUES(?,?)
                    ON CONFLICT(fact_id) DO UPDATE SET version=max(version,excluded.version)""",
                    (row["fact_id"],row["version"]))
            saved.append(row["fact_id"])
        return saved
