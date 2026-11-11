from __future__ import annotations

import base64
import http
import json
import time
from pathlib import Path

import lark_oapi as lark
from lark_oapi.api.contact.v3 import GetUserRequest
from lark_oapi.api.im.v1 import (CreateFileRequest, CreateFileRequestBody, CreateMessageRequest,
                                CreateMessageRequestBody, GetMessageResourceRequest)
from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse
from lark_oapi.ws import client as ws

from .config import BoundaryError, Config
from . import request_timing


class SendRejected(BoundaryError):
    pass


class SendUnknown(BoundaryError):
    pass


def classify_send_failure(response, prefix="FEISHU"):
    # Only documented definite rejections permit another attempt. 230049 is
    # "being sent"; a 5xx or unknown code cannot establish non-delivery.
    rejected={230001,230002,230006,230013,230015,230017,230018,230019,230020,230022,230025,230027,230028,230029,230034,230035,230036,230038,230053,230054,230055,230075,230099,232009}
    if response.code in rejected and getattr(response.raw,"status_code",200)<500:
        raise SendRejected(prefix+"_"+str(response.code))
    raise SendUnknown(prefix+"_OUTCOME_UNKNOWN_"+str(response.code))


class CardAwareClient(ws.Client):
    """Narrow lark-oapi 1.7.5 CARD-frame fix; no payload or exception-body logging."""

    async def _handle_data_frame(self, frame):
        hs = frame.headers
        msg_id = ws._get_by_key(hs, ws.HEADER_MESSAGE_ID)
        count = int(ws._get_by_key(hs, ws.HEADER_SUM))
        seq = int(ws._get_by_key(hs, ws.HEADER_SEQ))
        kind = ws.MessageType(ws._get_by_key(hs, ws.HEADER_TYPE))
        payload = frame.payload
        if count > 1:
            payload = self._combine(msg_id, count, seq, payload)
            if payload is None:
                return
        if kind not in (ws.MessageType.EVENT, ws.MessageType.CARD):
            return
        response = ws.Response(code=http.HTTPStatus.OK)
        try:
            start = time.monotonic()
            result = self._event_handler._do_without_validation(payload)
            header = hs.add()
            header.key = ws.HEADER_BIZ_RT
            header.value = str(int((time.monotonic() - start) * 1000))
            if result is not None:
                response.data = base64.b64encode(lark.JSON.marshal(result).encode())
        except Exception:
            # A failed commit is not acknowledged as accepted; Feishu may redeliver.
            response = ws.Response(code=http.HTTPStatus.INTERNAL_SERVER_ERROR)
        frame.payload = lark.JSON.marshal(response).encode()
        await self._write_message(frame.SerializeToString())


class Feishu:
    def __init__(self, config: Config, *, client=None):
        self.config = config
        if client is not None:
            self.client = client
            self.secret = None
            return
        self.secret = config.read_secret(config.app_secret_file)
        self.client = (lark.Client.builder().app_id(config.app_id).app_secret(self.secret)
                       .timeout(10).log_level(lark.LogLevel.CRITICAL).build())

    def authorized(self, row) -> bool:
        c = self.config
        origin_type = row.get("origin_chat_type", row["chat_type"])
        if (row["app"] != c.app_id or row["tenant"] != c.tenant_key or row["user"] not in c.users
                or row["chat_type"] not in ("p2p", "group")
                or (origin_type == "group" and row["chat"] not in c.chats)):
            return False
        try:
            request = (GetUserRequest.builder().user_id(row["user"]).user_id_type("open_id")
                       .department_id_type("open_department_id").build())
            with request_timing.remote('remote_identity'):
                response = self.client.contact.v3.user.get(request)
            if not response.success() or not response.data or not response.data.user:
                return False
            user = response.data.user
            if user.open_id != row["user"]:
                return False
            status = user.status
            if not status or status.is_activated is not True or any(
                    getattr(status, key, False) for key in ("is_resigned", "is_frozen", "is_exited")):
                return False
            departments = set(user.department_ids or [])
            allowed = set(c.departments)
            return bool(departments & allowed) if c.department_match == "any_exact" else bool(departments) and departments <= allowed
        except Exception:
            return False

    def send(self, row, guard):
        kind = row["kind"]
        if not guard():
            raise SendRejected("DELIVERY_AUTH_OR_TTL")
        if kind == "file":
            path = Path(row["artifact"])
            expected = self.config.root / "artifacts" / f"{row['task_id']}.md"
            if path != expected or path.is_symlink() or not path.is_file():
                raise SendRejected("ARTIFACT_PATH_INVALID")
            try:
                with path.open("rb") as stream:
                    body = (CreateFileRequestBody.builder().file_type("stream").file_name(path.name).file(stream).build())
                    response = self.client.im.v1.file.create(CreateFileRequest.builder().request_body(body).build())
            except Exception:
                # No message send was attempted. An orphan upload contains only synthetic data.
                raise SendRejected("FILE_UPLOAD_FAILED") from None
            if not response.success():
                raise SendRejected(f"FILE_UPLOAD_{response.code}")
            content = json.dumps({"file_key": response.data.file_key})
        elif kind == "interactive":
            content = row["body"]
        else:
            content = json.dumps({"text": row["body"]}, ensure_ascii=False)
        # Contact/TTL checked again after uploads and immediately before publishing.
        if not guard():
            raise SendRejected("DELIVERY_AUTH_OR_TTL")
        if row.get('_group_reply_to'):
            from lark_oapi.api.im.v1 import ReplyMessageRequest,ReplyMessageRequestBody
            body=(ReplyMessageRequestBody.builder().msg_type(kind).content(content).uuid(row['id'])
                  .reply_in_thread(bool(row.get('_reply_in_thread'))).build())
            try:
                with request_timing.remote('remote_publish'):
                    response=self.client.im.v1.message.reply(ReplyMessageRequest.builder().message_id(row['_group_reply_to']).request_body(body).build())
            except Exception:raise SendUnknown('GROUP_REPLY_RECEIPT_UNKNOWN') from None
            if not response.success():classify_send_failure(response,'GROUP_REPLY')
            if not response.data or not response.data.message_id:raise SendUnknown('GROUP_REPLY_RECEIPT_MISSING')
            return response.data.message_id
        target_type = "open_id" if row["chat_type"] == "p2p" else "chat_id"
        target = row["user"] if target_type == "open_id" else row["chat"]
        request = (CreateMessageRequest.builder().receive_id_type(target_type).request_body(
            CreateMessageRequestBody.builder().receive_id(target).msg_type(kind)
            .content(content).uuid(row["id"]).build()).build())
        try:
            with request_timing.remote('remote_publish'):
                response = self.client.im.v1.message.create(request)
        except Exception:
            raise SendUnknown("SEND_RECEIPT_UNKNOWN") from None
        if not response.success():
            classify_send_failure(response)
        if not response.data or not response.data.message_id:
            raise SendUnknown("SEND_RECEIPT_MISSING")
        if row['chat_type']=='p2p' and getattr(response.data,'chat_id',None):row['_delivered_chat_id']=response.data.chat_id
        return response.data.message_id

    def update_card(self,row,guard):
        from lark_oapi.api.im.v1 import PatchMessageRequest,PatchMessageRequestBody
        if row['chat_type']!='p2p' or row['kind']!='interactive' or not guard():raise SendRejected('UPDATE_SCOPE_INVALID')
        request=PatchMessageRequest.builder().message_id(row['update_message_id']).request_body(PatchMessageRequestBody.builder().content(row['body']).build()).build()
        if not guard():raise SendRejected('DELIVERY_AUTH_OR_TTL')
        try:
            with request_timing.remote('remote_publish'):
                response=self.client.im.v1.message.patch(request)
        except Exception:raise SendUnknown('UPDATE_RECEIPT_UNKNOWN') from None
        if not response.success():
            if self.navigation_target_unavailable(row['update_message_id'],row):raise SendRejected('UPDATE_TARGET_DELETED')
            classify_send_failure(response,'UPDATE')
        return row['update_message_id']

    def verify_card(self,message_id,row):
        from lark_oapi.api.im.v1 import GetMessageRequest
        guard=row.get('_query_readback_guard')
        if not (guard() if callable(guard) else self.authorized(row)):return False
        request=GetMessageRequest.builder().message_id(message_id).card_msg_content_type('raw_card_content').build()
        try:
            with request_timing.remote('remote_verify'):
                response=self.client.im.v1.message.get(request)
            if not response.success() or not response.data or len(response.data.items or [])!=1:return False
            message=response.data.items[0];sender=message.sender
            author=sender.open_bot_id or sender.id
            if author not in (self.config.bot_open_id,self.config.app_id) or sender.sender_type not in ('app','bot'):return False
            if message.chat_id!=row['chat'] or message.deleted or message.msg_type!='interactive':return False
            actual=json.loads(message.body.content);expected=json.loads(row['body'])
            from .card_ui import card_readback_matches
            return card_readback_matches(actual,expected)
        except Exception:return False

    def verify_text(self,message_id,row):
        """Exact text/body, bot and conversation readback for text approvals."""
        from lark_oapi.api.im.v1 import GetMessageRequest
        guard=row.get('_query_readback_guard')
        if not (guard() if callable(guard) else self.authorized(row)):return False
        try:
            with request_timing.remote('remote_verify'):
                response=self.client.im.v1.message.get(GetMessageRequest.builder().message_id(message_id).build())
            if not response.success() or not response.data or len(response.data.items or [])!=1:return False
            m=response.data.items[0];sender=m.sender
            return (m.chat_id==row['chat'] and not m.deleted and m.msg_type=='text' and sender.sender_type in ('app','bot')
                and (sender.open_bot_id or sender.id) in (self.config.bot_open_id,self.config.app_id)
                and json.loads(m.body.content)=={'text':row['body']})
        except Exception:return False

    def navigation_target_unavailable(self,message_id,row):
        """Only a positively identified deleted bot message permits recreation.

        Not-found, permission, timeout, malformed and temporary rejection are
        not proof. This method is only consulted for rejected navigation PATCH.
        """
        from lark_oapi.api.im.v1 import GetMessageRequest
        guard=row.get('_query_readback_guard')
        if not (guard() if callable(guard) else self.authorized(row)):return False
        try:
            response=self.client.im.v1.message.get(GetMessageRequest.builder().message_id(message_id).build())
            if not response.success() or not response.data or len(response.data.items or [])!=1:return False
            message=response.data.items[0];sender=message.sender
            return bool(message.message_id==message_id and message.chat_id==row['chat'] and message.deleted
                        and sender.sender_type in ('app','bot') and (sender.open_bot_id or sender.id) in (self.config.bot_open_id,self.config.app_id))
        except Exception:return False

    def read_file(self, row, file_key, guard, *, limit):
        if not guard():
            raise BoundaryError("FILE_AUTH_DENIED")
        request = (GetMessageResourceRequest.builder().message_id(row["message_id"])
                   .file_key(file_key).type("file").build())
        try:
            response = self.client.im.v1.message_resource.get(request)
            if not response.success() or not response.file:
                raise BoundaryError("FILE_READ_FAILED")
            data = response.file.read(limit + 1)
        except BoundaryError:
            raise
        except Exception:
            raise BoundaryError("FILE_READ_FAILED") from None
        if not guard():
            raise BoundaryError("FILE_AUTH_DENIED")
        if len(data) > limit:
            raise BoundaryError("FILE_READ_LIMIT")
        return data

    def socket(self, gateway):
        if self.config.event_transport == "existing_bridge":
            # AM221D221 already has a live consumer. Integration must dispatch
            # through that process instead of opening a competing connection.
            raise BoundaryError("EXISTING_BRIDGE_REQUIRED")
        def receive(data):
            gateway.ingest(json.loads(lark.JSON.marshal(data)))

        def card(data):
            accepted = gateway.ingest(json.loads(lark.JSON.marshal(data)))
            return P2CardActionTriggerResponse({"toast": {
                "type": "info", "content": "已记录，正在核验权限" if accepted else "已处理或不在测试范围"
            }})

        handler = (lark.EventDispatcherHandler.builder("", "", lark.LogLevel.CRITICAL)
                   .register_p2_im_message_receive_v1(receive).register_p2_card_action_trigger(card).build())
        return CardAwareClient(self.config.app_id, self.secret, event_handler=handler, log_level=lark.LogLevel.CRITICAL)
