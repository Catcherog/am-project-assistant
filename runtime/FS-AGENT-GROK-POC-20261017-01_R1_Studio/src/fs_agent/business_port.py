"""Original bot transport for scheduled messages and bound feedback cards."""
import json
import re
import uuid
import time
from .config import BoundaryError
from .feishu import SendRejected,SendUnknown,classify_send_failure
from .project_catalog import canonical
from .reminder_content import normalized_payload
from .card_ui import card as ui_card,paragraph,button,field,bind_rendered_card,card_readback_matches


class BusinessPort:
    def __init__(self,gateway,ledger):self.g,self.ledger=gateway,ledger;self.reminder=gateway.reminders.port

    def prepare(self,payload,occurrence_id):
        if payload['target']['kind']=='recipient_p2p':
            return payload
        if payload.get('followup_intent') and payload['target']['kind']=='group':
            from am_followup.rendering import feishu_post
            return dict(payload,post=feishu_post(payload['body'],tuple(p['id'] for p in payload['target']['payload']['people'])))
        if payload['target']['kind']=='owner':
            if payload['target'].get('conversation_text'):return payload
            if 'briefing_data' not in payload:return payload
            nonce=uuid.uuid5(uuid.NAMESPACE_URL,'briefing/'+occurrence_id).hex
            view=ui_card('本人变化简报',[paragraph(payload['body']),button('完整变化与待办',{'flow':'am_self','briefing':occurrence_id,'nonce':nonce})])
            return dict(payload,card=bind_rendered_card(view),briefing_nonce=nonce)
        source=payload['target']['payload']
        if payload['target'].get('conversation_text'):
            locale=source['content']['zh_cn']
            post={'zh_cn':dict(locale,content=[locale['content'][0]]+[[{'tag':'text','text':line}] for line in payload['body'].split('\n')]+[locale['content'][-1]])}
            return dict(payload,post=post)
        nonce=uuid.uuid5(uuid.NAMESPACE_URL,'feedback/'+occurrence_id).hex
        people=source['people']
        mentions=' '.join('<at id="'+p['id']+'"></at>' for p in people)
        # Only the code-owned mention line uses Markdown. The user body is a
        # plain_text div and cannot synthesize an @all or another button.
        base={'flow':'am_self','feedback':occurrence_id,'nonce':nonce}
        inputs=field('kind','反馈类型',kind='select_static',options=[('eta','预计时间'),('help','需要协助'),('submitted','提交材料'),('not_mine','非本人负责')])
        inputs+=field('eta','预计时间（仅预计反馈必填）',kind='picker_datetime',required=False)
        inputs+=field('detail','说明 / 材料链接',required=True)
        inputs.append(button('提交本次反馈',dict(base,action='feedback_form'),name='feedback_submit',submit=True))
        card=ui_card('AM 跟进事项',[
            {'tag':'markdown','content':mentions},paragraph(payload['body']),
            paragraph('收到只记录响应；提交材料仍需核验。'),
            button('已收到',dict(base,action='received')),
            {'tag':'form','name':'feedback_form','elements':inputs}])
        # This card is shared by the authorized group. Per-person form values
        # are callback data only; no personal navigation is written to the card.
        card['config']['update_multi']=True
        return dict(payload,card=bind_rendered_card(card),feedback_nonce=nonce)

    def refresh(self,payload):
        target=payload['target']
        if target['kind']=='owner':
            if target['user']!=self.g.config.owner:raise BoundaryError('OWNER_TARGET_CHANGED')
            self.ledger.port.authorize_user(target['user']);return
        if target['kind']=='recipient_p2p':
            resolver=getattr(self,'followup_resolver',None)
            if resolver is None:raise BoundaryError('RECIPIENT_RESOLVER_UNAVAILABLE')
            if resolver(target)!=target:raise BoundaryError('RECIPIENT_SNAPSHOT_CHANGED')
            return
        if target['kind']!='group':raise BoundaryError('EXACT_TARGET_REQUIRED')
        self.reminder.refresh(target['payload'])

    def send(self,payload,key,guard,*,before_send=None):
        from lark_oapi.api.im.v1 import CreateMessageRequest,CreateMessageRequestBody
        self.refresh(payload)
        if not guard():raise SendRejected('SCHEDULER_SEND_GUARD')
        t=payload['target'];private=t['kind'] in ('owner','recipient_p2p')
        target=t['user'] if private else t['payload']['group']['id']
        kind='interactive' if payload.get('card') else 'post' if payload.get('post') else 'text'
        content=payload['card'] if payload.get('card') else payload['post'] if payload.get('post') else {'text':payload['body']}
        req=(CreateMessageRequest.builder().receive_id_type('open_id' if private else 'chat_id').request_body(
            CreateMessageRequestBody.builder().receive_id(target).msg_type(kind).content(canonical(content)).uuid(key).build()).build())
        if not guard():raise SendRejected('SCHEDULER_SEND_GUARD')
        # A one-shot claim must never be composed into the repeatable guard.
        if before_send is not None:before_send()
        try:r=self.g.api.client.im.v1.message.create(req)
        except Exception:raise SendUnknown('SCHEDULED_SEND_UNKNOWN') from None
        if not r.success():classify_send_failure(r,'SCHEDULED')
        if not r.data or not r.data.message_id:raise SendUnknown('SCHEDULED_RECEIPT_MISSING')
        return {'message_id':r.data.message_id,'sent_at':time.time()}

    def verify(self,payload,m):
        if m['deleted'] or m['sender_id']!=self.g.config.bot_open_id or m['sender_type'] not in ('app','bot'):return False
        t=payload['target'];private=t['kind'] in ('owner','recipient_p2p')
        if not private and m['chat_id']!=t['payload']['group']['id']:return False
        if private:
            # The exact owner chat is bound when the subscription is created.
            if m['chat_id']!=t['chat']:return False
        try:content=json.loads(m['content']) if isinstance(m['content'],str) else m['content']
        except ValueError:return False
        if payload.get('post'):
            from .reminder_content import semantic_content
            try:return m['msg_type']=='post' and semantic_content(content,m.get('mentions',[]))==semantic_content(payload['post'])
            except (KeyError,ValueError,TypeError):return False
        return m['msg_type']==('interactive' if payload.get('card') else 'text') and (card_readback_matches(content,payload['card']) if payload.get('card') else content=={'text':payload['body']})

    def reconcile(self,payload,key,started,message_id=None):
        if not message_id:
            if payload.get('followup_intent') or payload['target']['kind']=='recipient_p2p':return None
            # A bounded bot-originated lookup is permitted only for an already
            # approved group. No synthetic probe and no inference from absence.
            if payload['target']['kind']=='owner' or not self.reminder.policy()['reconcile_own_attempts']:return None
            from lark_oapi.api.im.v1 import ListMessageRequest
            group=payload['target']['payload']['group']['id']
            if not self.reminder.allowed(group):return None
            request=(ListMessageRequest.builder().container_id_type('chat').container_id(group)
                     .start_time(str(int(started)-2)).end_time(str(int(started)+125)).page_size(50).build())
            data=self.reminder._data(self.g.api.client.im.v1.message.list(request))
            if data.has_more:return None
            matches=[self.reminder._message(m) for m in data.items or []]
            matches=[m for m in matches if self.verify(payload,m)]
            if len(matches)!=1:return None
            message=matches[0]
        else:
            reader=getattr(self.reminder,'read_card',self.reminder.read) if payload.get('card') else self.reminder.read
            message=reader(message_id)
        if message_id and message.get('message_id')!=message_id:return None
        if not self.verify(payload,message):return None
        return {'verified':True,'message_id':message['message_id'],'sent_at':message['created_at']}
