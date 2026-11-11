"""Bot-only, separately scoped reminder egress through the original SDK client."""
from __future__ import annotations
import hashlib
from pathlib import Path

SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
import json
import re
import time
import lark_oapi as lark
from lark_oapi.api.im.v1 import (CreateMessageRequest,CreateMessageRequestBody,GetChatRequest,
    GetChatMembersRequest,IsInChatChatMembersRequest,GetMessageRequest,ListMessageRequest)
from .config import BoundaryError
from .feishu import SendRejected,SendUnknown,classify_send_failure
from .reminder_content import canonical,verify_message

class ReminderPort:
    def __init__(self, config, client):
        self.config,self.client=config,client

    def policy(self):
        path=self.config.root/"config/group-reminder.json"
        if not path.exists(): return {"outbound_group_ids":[],"reconcile_own_attempts":False}
        try:value=json.loads(path.read_text())
        except (OSError,ValueError):raise BoundaryError("REMINDER_POLICY_INVALID") from None
        if (set(value)!={"outbound_group_ids","reconcile_own_attempts"}
                or not isinstance(value["outbound_group_ids"],list)
                or len(value["outbound_group_ids"])>10
                or not all(isinstance(x,str) and re.fullmatch(r"oc_[A-Za-z0-9_-]+",x) for x in value["outbound_group_ids"])
                or type(value["reconcile_own_attempts"]) is not bool):
            raise BoundaryError("REMINDER_POLICY_INVALID")
        return value

    def allowed(self, group_id):return group_id in self.policy()["outbound_group_ids"]

    def _data(self,response):
        if not response.success() or response.data is None:
            raise BoundaryError("REMINDER_PLATFORM_READ_"+str(response.code))
        return response.data

    def bot(self):
        request=(lark.BaseRequest.builder().http_method(lark.HttpMethod.GET).uri("/open-apis/bot/v3/info")
                 .token_types({lark.AccessTokenType.TENANT}).build())
        response=self.client.request(request)
        value=json.loads(response.raw.content)
        bot=value.get("bot",{})
        if value.get("code")!=0 or bot.get("open_id")!=self.config.bot_open_id or not bot.get("app_name"):
            raise BoundaryError("REMINDER_BOT_IDENTITY_UNVERIFIED")
        return {"id":bot["open_id"],"app_id":self.config.app_id,"name":bot["app_name"]}

    def group(self, group_id):
        if not self.allowed(group_id):raise BoundaryError("REMINDER_EGRESS_NOT_CONFIGURED")
        data=self._data(self.client.im.v1.chat.get(GetChatRequest.builder().chat_id(group_id).user_id_type("open_id").build()))
        member=self._data(self.client.im.v1.chat_members.is_in_chat(IsInChatChatMembersRequest.builder().chat_id(group_id).build()))
        if member.is_in_chat is not True:raise BoundaryError("REMINDER_BOT_NOT_IN_GROUP")
        if data.tenant_key!=self.config.tenant_key or data.external is True or data.chat_mode!="group" or data.chat_status!="normal":
            raise BoundaryError("REMINDER_GROUP_SCOPE_UNVERIFIED")
        # The actual send still checks platform permission; membership does not
        # imply that a restricted group allows this bot to speak.
        return {"id":group_id,"name":data.name,"tenant":data.tenant_key,"bot_member":True,
                "posting_policy":data.moderation_permission,"mode":data.chat_mode}

    def groups(self, hint):
        ids=self.policy()["outbound_group_ids"]
        explicit=re.search(r"oc_[A-Za-z0-9_-]+",hint or "")
        if explicit:
            return [self.group(explicit.group())] if explicit.group() in ids else []
        result=[self.group(g) for g in ids]
        exact=[g for g in result if g["name"]==hint]
        return exact or [g for g in result if hint and hint in g["name"]]

    def members(self, group_id):
        if not self.allowed(group_id):raise BoundaryError("REMINDER_EGRESS_NOT_CONFIGURED")
        result=[];token=None;seen=set()
        for _ in range(20):
            builder=GetChatMembersRequest.builder().chat_id(group_id).member_id_type("open_id").page_size(100)
            if token:builder.page_token(token)
            data=self._data(self.client.im.v1.chat_members.get(builder.build()))
            if data.trigger_security_conf_limit:raise BoundaryError("REMINDER_MEMBERS_TRUNCATED")
            for m in data.items or []:
                if m.member_id_type!="open_id" or not m.member_id or not m.member_id.startswith("ou_") or m.tenant_key!=self.config.tenant_key:
                    continue
                result.append({"id":m.member_id,"name":m.name})
            if not data.has_more:return result
            if not data.page_token or data.page_token in seen:break
            token=data.page_token;seen.add(token)
        raise BoundaryError("REMINDER_MEMBERS_TRUNCATED")

    def refresh(self,payload):
        bot=self.bot();group=self.group(payload["group"]["id"])
        members={m["id"]:m for m in self.members(group["id"])}
        people=[members.get(p["id"]) for p in payload["people"]]
        if any(p is None for p in people):raise BoundaryError("REMINDER_MEMBER_LEFT_GROUP")
        if bot!=payload["bot"] or group!=payload["group"] or people!=payload["people"]:
            raise BoundaryError("REMINDER_TARGET_CHANGED")

    def send(self,payload,request_key,guard):
        if not self.allowed(payload["group"]["id"]) or not guard():raise SendRejected("REMINDER_SEND_GUARD")
        request=(CreateMessageRequest.builder().receive_id_type("chat_id").request_body(
            CreateMessageRequestBody.builder().receive_id(payload["group"]["id"]).msg_type("post")
            .content(canonical(payload["content"])).uuid(request_key).build()).build())
        try:response=self.client.im.v1.message.create(request)
        except Exception:raise SendUnknown("REMINDER_SEND_UNKNOWN") from None
        if not response.success():
            classify_send_failure(response,"REMINDER_PLATFORM")
        if not response.data or not response.data.message_id:raise SendUnknown("REMINDER_RECEIPT_MISSING")
        return {"message_id":response.data.message_id,"sent_at":time.time()}

    def _message(self,m):
        sender=m.sender
        # Feishu may report the bot by app_id, with open_bot_id separately.
        actual=sender.open_bot_id or sender.id
        if actual==self.config.app_id and sender.sender_type in ("app","bot"):actual=self.config.bot_open_id
        return {"message_id":m.message_id,"chat_id":m.chat_id,"sender_id":actual,
                "sender_type":sender.sender_type,"msg_type":m.msg_type,"content":m.body.content,
                "mentions":[{"key":x.key,"id":x.id,"name":x.name} for x in m.mentions or []],
                "created_at":int(m.create_time)/1000,"link":m.message_app_link or None,
                "parent_id":getattr(m,"parent_id",None),"root_id":getattr(m,"root_id",None),
                "deleted":bool(m.deleted)}

    def read_card(self,message_id):
        request=GetMessageRequest.builder().message_id(message_id).user_id_type("open_id").card_msg_content_type('raw_card_content').build()
        data=self._data(self.client.im.v1.message.get(request))
        if len(data.items or [])!=1:raise BoundaryError('REMINDER_READBACK_MISSING')
        message=self._message(data.items[0])
        if message['deleted']:raise BoundaryError('REMINDER_READBACK_DELETED')
        return message

    def read(self,message_id):
        data=self._data(self.client.im.v1.message.get(GetMessageRequest.builder().message_id(message_id).user_id_type("open_id").build()))
        if len(data.items or [])!=1:raise BoundaryError("REMINDER_READBACK_MISSING")
        message=self._message(data.items[0])
        if message["deleted"]:raise BoundaryError("REMINDER_READBACK_DELETED")
        return message

    def reconcile(self,payload,request_key,started,message_id=None):
        if message_id:
            message=self.read(message_id)
            if not verify_message(payload,message):raise BoundaryError("REMINDER_READBACK_MISMATCH")
            return message
        if not self.policy()["reconcile_own_attempts"]:return None
        if not self.allowed(payload["group"]["id"]):return None
        # Bounded lookup for this one confirmed attempt. Unrelated message bodies
        # are discarded here and never persisted, returned or passed to the model.
        token=None;matches=[]
        for _ in range(3):
            builder=(ListMessageRequest.builder().container_id_type("chat").container_id(payload["group"]["id"])
                     .start_time(str(int(started)-2)).end_time(str(int(started)+125)).page_size(50))
            if token:builder.page_token(token)
            data=self._data(self.client.im.v1.message.list(builder.build()))
            for item in data.items or []:
                m=self._message(item)
                if not m["deleted"] and verify_message(payload,m):matches.append(m)
            if not data.has_more:return matches[0] if len(matches)==1 else None
            token=data.page_token
            if not token:break
        return None

class MissingReminderPort:
    def policy(self):return {"outbound_group_ids":[],"reconcile_own_attempts":False}
    def __getattr__(self,name):
        def unavailable(*a,**kw):raise BoundaryError("REMINDER_PLATFORM_UNAVAILABLE")
        return unavailable
