"""AM command and card wiring for items and the one persistent scheduler."""
import json
import re
from datetime import datetime
from fs_agent.action_items import ActionItems
from fs_agent.scheduler import Scheduler,parse_time,TZ
from fs_agent.business_port import BusinessPort
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical,digest
from fs_agent.reminder_content import normalized_payload
from group_ledger import select_projects

LABELS={'waiting':'待反馈','in_progress':'处理中','blocked':'受阻','submitted':'已提交待核验','closed':'已关闭','cancelled':'已取消'}
ITEM_HELP=('跟进事项：\n/item 新增 项目编号｜事项描述｜准确群名｜负责人姓名（多人用、分隔）｜交付要求\n'
 '/item 查看 事项ID\n/item 处理中|暂停|恢复|取消 事项ID v版本\n/item 关闭 事项ID v版本｜核验依据\n/item 检查 事项ID v版本｜明天 09:00\n'
 '创建只纳入 AM 跟进，不发消息、不改正式台账。')
PLAN_HELP=('计划：\n/plan 一次 事项ID｜明天 09:00｜完整提醒正文\n'
 '/plan 每天 事项ID｜09:00｜2026-11-30 18:00｜完整正文｜逐次确认（可明确选择固定规则）\n'
 '/plan 工作日 事项ID｜09:00｜2026-11-30 18:00｜完整正文｜逐次确认\n'
 '/plan 查看|暂停|恢复|取消 计划ID v版本\n/plan 对账 发送实例ID\n'
 '窗口默认60秒，预览中显示。工作日计划依赖已核实日历；不默认周一至周五。')


class BusinessCommands:
    def init_business(self,calendar=None):
        self.items=ActionItems(self.s,self.owner)
        self.scheduler=Scheduler(self.s,self.items,self.owner,BusinessPort(self.g,self.ledger),calendar=calendar,
                                 enabled=lambda:self.flags['scheduler'],notify=self.business_notification,briefing=self.briefing_payload)
        from am_followup_host import FollowupHost
        self.followup=FollowupHost(self)

    def owner_origin(self):
        rows=self.s.rows("SELECT * FROM am_inbox WHERE user=? AND chat_type='p2p' ORDER BY created DESC LIMIT 1",(self.owner,))
        if not rows:
            rows=self.s.rows("SELECT * FROM inbox WHERE user=? AND chat_type='p2p' ORDER BY received DESC LIMIT 1",(self.owner,))
        if not rows:return None
        row=rows[0]
        # A new business notification is not a revival of the original chat.
        return dict(row,expires=self.s.clock()+86400)

    def business_notification(self,kind,ident):
        row=self.owner_origin()
        if not row:return
        o=self.scheduler.occurrence(ident)
        if kind=='occurrence_preview':
            body=self.occurrence_text(o)
            self.card(row,object_id=ident,version=o['plan_version'],snapshot={'hash':o['hash'],'payload':json.loads(o['payload']),'deadline':o['deadline']},
                      title='待确认本次提醒',body=body,actions=[('approve','确认本次发送')],flow='occurrence',expires=o['deadline'])
        else:self.notify(row,self.occurrence_text(o))

    def occurrence_text(self,o):
        if json.loads(o['payload']).get('followup_intent'):return self.followup.status(o)
        p=json.loads(o['payload']);target=p['target'];who=target['user'] if target['kind']=='owner' else target['payload']['group']['name']+'；'+'、'.join(x['name'] for x in target['payload']['people'])
        return f"发送实例 {o['id']}\n准确目标：{who}\n正文：{p['body']}\n计划时刻：{datetime.fromtimestamp(o['scheduled_at'],TZ):%Y-%m-%d %H:%M:%S}\n执行截止：{datetime.fromtimestamp(o['deadline'],TZ):%Y-%m-%d %H:%M:%S}\n状态：{o['state']}；回读：{o['readback_state'] or '尚未发送'}\n消息发送成功不代表事项完成。"

    def plan_preview_text(self,p):
        target=json.loads(p['target']);rule=json.loads(p['rule'])
        who=('本人私聊' if target['kind']=='owner' else target['name']+'（机器人私聊）' if target['kind']=='recipient_p2p'
             else target['payload']['group']['name']+'；'+'、'.join(x['name'] for x in target['payload']['people']))
        body=self.scheduler.render(p,self.items.get(p['item_id']))['body'] if p['item_id'] else ''
        content='项目变化及待办' if 'todos' in self.briefing_types(p) else '项目变化'
        lines=['目标：'+who,'完整正文：\n'+(body or content+'（每次读取本人授权范围内记录）'),
               '类型：'+{'once':'一次','daily':'每天','workdays':'工作日','briefing':'本人简报'}[p['kind']],
               '下一次：'+__import__('am_views').stamp(p['next_at'])+'（北京时间）',
               '规则时刻：'+str(rule.get('time') or __import__('am_views').stamp(rule.get('at'))),
               '批准方式：'+{'preview_each':'每次发送前确认','fixed_rule':'此完整固定规则','owner_subscription':'本人订阅固定规则'}[p['policy']],
               '有效至：'+__import__('am_views').stamp(p['valid_until']),f"执行窗口：{p['window_seconds']}秒，错过不补发。"]
        if rule.get('calendar_version'):lines.append('日历版本：'+str(rule['calendar_version']))
        lines+=['时间策略：'+('含休息日，按明确时刻执行。' if rule.get('quiet_policy')=='exact' else '按已核实工作日历及普通工作时间顺延。' if rule.get('quiet_policy')=='business_hours' or rule.get('workdays') else '按所示时刻执行。'),
                '停止条件：事项暂停、关闭、取消；计划停用；权限或目标变化；批准失效。']
        allowed=json.loads(p['allowed_variables'])
        if allowed:lines.append('允许随事项变化的字段：'+'、'.join(allowed))
        return '\n'.join(lines)

    def plan_card(self,row,p):
        self.card(row,object_id=p['id'],version=p['version'],snapshot={'hash':p['hash'],'plan':p},title='计划完整预览',body=self.plan_preview_text(p),
                  actions=[('approve','确认所示计划与批准方式'),('cancel','取消计划')],flow='plan',expires=p['valid_until'])
        return None

    def item_text(self,item):
        history=self.items.history(item['id'])
        details='\n'.join(f"{h['actor']}｜{h['kind']}｜{'有效反馈' if h['applied'] else '迟到留证'}｜{datetime.fromtimestamp(h['created'],TZ).isoformat()}\n内容/材料：{h['body'] or '未补充'}\n来源：{h['source']}" for h in history)
        return (f"事项 {item['id']} v{item['version']}\n{item['description']}\n交付要求：{item['requirements'] or '待确认'}\n"
                f"状态：{LABELS[item['state']]}；执行控制：{'暂停' if item['paused'] else '继续'}\n日期：{item['deadline'] or '未设'}（{item['deadline_kind']}）\n"
                f"最新反馈：{item['latest_feedback'] or '暂无'}\n反馈记录数：{len(history)}\n{details}\n核验关闭依据：{item['close_evidence'] or '尚未核验关闭'}\n仅影响 AM 跟进，不改变正式项目状态。")

    def handle_business_card(self,row,c,action):
        if row['user']!=self.owner:raise BoundaryError('OWNER_ONLY')
        if c['flow']=='followup':return self.followup.card_action(row,c,action)
        if c['flow']=='followup_source_review':return self.followup.review_sources_action(row,c,action)
        if c['flow']=='followup_import':
            if action!='adopt':raise BoundaryError('CARD_ACTION_NOT_SUPPORTED')
            self.followup.verify_owner_review_delivery(row,c)
            snapshot=json.loads(c['snapshot']);current=self.s.rows('SELECT payload FROM followup_candidates WHERE id=?',(c['object_id'],))
            if not current or snapshot.get('candidate',snapshot)!=json.loads(current[0]['payload']):raise BoundaryError('FOLLOWUP_IMPORT_CHANGED')
            recoveries=self.followup.recovery_rows(c['object_id'])
            if ('recovery_hash' in snapshot and snapshot['recovery_hash']!=digest(recoveries)) or recoveries and 'recovery_hash' not in snapshot:
                raise BoundaryError('FOLLOWUP_IMPORT_RECOVERY_CHANGED')
            item=self.followup.adopt_candidate(row,c['object_id'],expected_payload_hash=digest(snapshot.get('candidate',snapshot)),
                expected_recovery_hash=snapshot.get('recovery_hash',digest([])))
            return self.ux_item(row,item['id'])
        snapshot=json.loads(c['snapshot'])
        if c['flow']=='retention' and action=='clean':
            result=self.items.apply_cleanup(row['user'],'cleanup/'+c['nonce'],snapshot['candidates'],snapshot['days'])
            return '已清理终结记录正文：'+str(result['archived_count'])+'项；保留身份、状态、版本及防重墓碑。'
        if c['flow']=='mapping' and action=='associate':
            self.projects(row)
            with self.ledger.mutex:self.ledger.catalog.confirm_mapping(snapshot['source_ref'],snapshot['target_ref'],snapshot['fingerprint'],row['user'])
            self.projects(row)
            return '已按本次明确映射保留原项目身份与变化历史。'
        if c['flow']=='plan':
            if action=='approve':p=self.scheduler.approve_plan(row['user'],c['object_id'],c['version'],snapshot['hash']);return '计划已确认：'+p['id']+'。'+('到期仍需逐次确认。' if p['policy']=='preview_each' else '仅在所示固定范围内执行。')
            if action=='cancel':self.scheduler.control(row['user'],row['id'],c['object_id'],c['version'],'cancel');return '计划已取消；已开始的外部请求另行核实。'
        if c['flow']=='occurrence' and action=='approve':
            o=self.scheduler.approve_occurrence(row['user'],c['object_id'],snapshot['hash']);return self.occurrence_text(o)
        if c['flow']=='item' and action=='create':
            item=self.items.create(row['user'],'card/'+c['nonce'],**snapshot);return self.ux_item(row,item['id'])
        if hasattr(self,'handle_update_card'):return self.handle_update_card(row,c,action)
        raise BoundaryError('CARD_ACTION_NOT_SUPPORTED')

    def handle_feedback(self,row,body):
        value=body['value'];o=self.scheduler.occurrence(value.get('feedback'))
        payload=json.loads(o['payload']);target=payload['target']
        if (target['kind']!='group' or row['chat']!=target['payload']['group']['id'] or value.get('nonce')!=payload.get('feedback_nonce')
            or not o['message_id'] or body['source_message']!=o['message_id']):raise BoundaryError('FEEDBACK_BINDING_INVALID')
        if self.s.clock()>o['scheduled_at']+86400:raise BoundaryError('FEEDBACK_CARD_EXPIRED')
        self.ledger.port.guard(row['chat'],row['user'])
        if row['user'] not in json.loads(self.items.get(o['item_id'])['assignees']):raise BoundaryError('NOT_ASSIGNEE')
        # Set only after the original message, occurrence, group and actor have
        # all been checked. Error handling may then send this actor a private form.
        row['_feedback_context']={'occurrence':o['id'],'nonce':payload['feedback_nonce'],'source_message':o['message_id'],'group':row['chat']}
        action=value.get('action');detail=body.get('detail','')
        if action=='feedback_form':
            from am_ux import card_time
            values=body.get('form_value')
            if not isinstance(values,dict) or set(values)-{'kind','eta','detail'}:raise BoundaryError('FORM_INVALID')
            action=values.get('kind');detail=values.get('detail')
            if action not in ('eta','help','submitted','not_mine') or not isinstance(detail,str) or not detail.strip() or len(detail)>3000:raise BoundaryError('FORM_REQUIRED')
            if action=='eta':detail='预计时间：'+datetime.fromtimestamp(card_time(values.get('eta')),TZ).isoformat()+'\n'+detail
        if action in ('eta','help','submitted') and not detail:
            # A button without a date/material does not invent one.
            detail={'eta':'预计完成时间待补充','help':'协助内容待补充','submitted':'材料位置待补充'}[action]
        op='feedback/'+digest([o['id'],row['user'],action,detail])
        result=self.items.feedback(row['user'],op,o['item_id'],o['item_version'],action,detail,{'message_id':o['message_id'],'occurrence':o['id']},
                                   submission_key=row.get('_feedback_submission') or digest([row['app'],row['tenant'],row['id']]))
        event=result.get('business_event_id',op)
        self.notify(row,('已记录本次反馈；'+('提交材料仍待核验。' if action=='submitted' else '尚未标记事项完成。')) if result['applied'] else '已保留迟到反馈，未覆盖负责人后续决定。',kind='feedback_ack',object_id=o['id'],event_key=event)
        owner=self.owner_origin()
        if owner:
            names={p['id']:p['name'] for p in target['payload']['people']}
            label={'received':'已收到','processing':'处理中','eta':'预计时间','help':'需要协助','submitted':'提交材料','not_mine':'非本人负责'}[action]
            summary=names.get(row['user'],'授权负责人')+'反馈：'+label+'。'
            if detail and len(detail)<=120:summary+='\n'+detail
            elif detail:summary+='\n完整反馈已保存，请打开事项查看。'
            summary+='\n'+('负责人自报，仍需核验。' if result['applied'] else '迟到留证，未覆盖新状态。')
            self.ux_feedback_notice(owner,result['item'],summary,event)

        return None

    def feedback_error(self,row,code):
        context=row.get('_feedback_context')
        if not context:return None
        from fs_agent.card_ui import card,paragraph,button,field,bind_rendered_card
        body=json.loads(row['body']);incoming=body.get('form_value') or {}
        if not isinstance(incoming,dict):incoming={}
        defaults={k:v for k,v in incoming.items() if k in ('kind','eta','detail') and isinstance(v,str) and len(v)<=3000}
        if code not in ('FORM_TIME_REQUIRED','FORM_REQUIRED','FORM_INVALID'):
            message='这张补填卡已提交过其他内容；请回到原事项卡发起新反馈。' if code=='OPERATION_ID_REUSED' else '本次反馈的处理结果尚未确认，请从原事项卡核对后重试；已有记录保留。'
            return self.notify(row,message,kind='feedback_ack',object_id=context['occurrence'],event_key=digest(['feedback-error',context,row['user'],code,defaults]))
        # A correction belongs to its user and original occurrence. Duplicate
        # invalid submissions reuse the same private prompt, never the group card.
        nonce=digest(['feedback-correction',context,row['user'],code,defaults])
        if self.s.rows('SELECT 1 FROM am_cards WHERE nonce=?',(nonce,)):return None
        snapshot={'context':context,'defaults':defaults,'app':row['app'],'tenant':row['tenant']}
        inputs=field('kind','反馈类型',kind='select_static',options=[('eta','预计时间'),('help','需要协助'),('submitted','提交材料'),('not_mine','非本人负责')],default=defaults.get('kind'))
        inputs+=field('eta','预计完成日期和时间（选择预计时间时必填）',kind='picker_datetime',required=defaults.get('kind')=='eta')
        inputs+=field('detail','说明 / 材料链接',default=defaults.get('detail',''))
        inputs.append(button('补齐并提交反馈',{'flow':'am_self','nonce':nonce,'action':'submit'},name='feedback_submit',submit=True))
        rendered=bind_rendered_card(card('反馈尚未提交',[paragraph('请选择预计完成的日期和时间。有效说明已保留；补齐后再提交。' if code=='FORM_TIME_REQUIRED' else '请补齐反馈类型和说明，再提交。有效说明已保留。'),{'tag':'form','name':'feedback_correction','elements':inputs}]))
        with self.s.tx() as c:
            out=self.notify(row,card=rendered,kind='feedback_correction',object_id=context['occurrence'],conn=c,event_key=nonce)
            c.execute('INSERT INTO am_cards VALUES(?,?,?,?,?,?,?,?,?,?,NULL)',(nonce,'feedback_correction',context['occurrence'],1,row['user'],'feedback-private:'+row['user'],canonical(snapshot),digest(snapshot),min(row['expires'],self.scheduler.occurrence(context['occurrence'])['scheduled_at']+86400),out))

    def handle_feedback_correction(self,row,c):
        body=json.loads(row['body']);snapshot=json.loads(c['snapshot'])
        if (c['actor']!=row['user'] or c['expires']<=self.s.clock() or not c['message_id'] or body['source_message']!=c['message_id']
            or digest(snapshot)!=c['hash'] or snapshot['app']!=row['app'] or snapshot['tenant']!=row['tenant']
            or body['value'].get('action')!='submit'):raise BoundaryError('FEEDBACK_BINDING_INVALID')
        if c['chat'].startswith('feedback-private:'):
            message=self.g.reminders.port.read_card(c['message_id'])
            if message['chat_id']!=row['chat'] or message['sender_id']!=self.g.config.bot_open_id or message.get('deleted'):raise BoundaryError('FEEDBACK_BINDING_INVALID')
            self.s.execute('UPDATE am_cards SET chat=? WHERE nonce=?',(row['chat'],c['nonce']))
        elif c['chat']!=row['chat']:raise BoundaryError('FEEDBACK_BINDING_INVALID')
        context=snapshot['context']
        origin=dict(row,chat=context['group'],chat_type='p2p',_feedback_submission='feedback-correction/'+c['nonce'])
        submitted={'value':{'feedback':context['occurrence'],'nonce':context['nonce'],'action':'feedback_form'},'source_message':context['source_message'],'form_value':body.get('form_value')}
        try:return self.handle_feedback(origin,submitted)
        finally:
            if origin.get('_feedback_context'):row['_feedback_context']=origin['_feedback_context']

    def briefing_payload(self,plan):
        sub=self.s.rows('SELECT id FROM am_subscriptions WHERE plan_id=?',(plan['id'],))
        deliveries=self.s.rows('SELECT event_id,state FROM am_briefing_deliveries WHERE subscription_id=?',(sub[0]['id'],)) if sub else []
        seen={r['event_id'] for r in deliveries if r['state']!='not_sent'}
        events=[c for c in self.ledger.catalog.changes() if c['id'] not in seen]
        parts=['本人项目变化简报（记录变化，不代表验收通过）']
        for c in events:parts.append(c['title']+'｜'+c['field']+'｜记录读取于'+datetime.fromtimestamp(c['read_at'],TZ).isoformat()+'\n原值：'+c['before_value']+'\n现值：'+c['after_value'])
        if not events:parts.append('本次没有可新投递的变化。')
        if any(r['state'] in ('pending','unknown') for r in deliveries):parts.append('另有变化仍待投递或对账；尚未视为已确认投递。')
        parts+=['来源：'+self.ledger.port.source_url if hasattr(self.ledger.port,'source_url') else '来源：固定 DEMO 项目台账','查询时间：'+datetime.fromtimestamp(self.s.clock(),TZ).isoformat()]
        return '\n\n'.join(parts),[c['id'] for c in events]

    def business_command(self,row):
        if row['body'].strip().startswith('/followup'):return True,self.followup.command(row)
        q=row['body'];parts=[x.strip() for x in q.replace('|','｜').split('｜')]
        first=parts[0]
        if first.startswith('/mapping'):
            projects=self.projects(row)
            if first.startswith('/mapping 关联 ') and len(parts)==2:
                old=first[len('/mapping 关联 '):]
                targets=[dict(r) for r in self.ledger.db.execute('SELECT * FROM project_refs WHERE namespace=?',(self.ledger.catalog.namespace,)) if old==r['business_id'] or old==r['title']]
                sources=[p for p in projects if parts[1] in (p.get('business_id'),p['title'])]
                if len(targets)!=1 or len(sources)!=1:raise BoundaryError('MAPPING_IDENTITIES_AMBIGUOUS')
                target,source=targets[0],sources[0]
                snapshot={'source_ref':source['key'],'target_ref':target['id'],'fingerprint':source['content_fingerprint']}
                self.card(row,object_id=source['key'],version=1,snapshot=snapshot,title='确认项目身份关联',body='将当前记录：'+source['title']+'\n关联到原项目：'+target['title']+'\n保留原身份和历史；不修改远端标题或编号。',actions=[('associate','确认所示关联')],flow='mapping')
                return True,None
            unresolved=[p for p in projects if p['identity_status']!='verified']
            return True,('\n'.join(p['title']+'：'+p['identity_status'] for p in unresolved) or '当前读取的项目均已具有可核对身份。')+'\n/mapping 关联 原编号或完整名称｜当前编号或完整名称'
        if first.startswith('/item'):
            if not self.flags['items']:raise BoundaryError('ITEMS_NOT_ENABLED')
            if first=='/item 清理预览':
                candidates=self.items.cleanup_preview(30)
                if not candidates:return True,'没有达到30天保留期且不存在未核对发送的终结事项。'
                self.card(row,object_id='retention',version=1,snapshot={'days':30,'candidates':candidates},title='终结记录清理预览',body='将清理 '+str(len(candidates))+' 个已关闭或取消超过30天的事项正文；活跃、暂停计划和未知发送不会清理。保留身份、状态、版本和防重墓碑。此操作不会删除飞书消息或台账内容。',actions=[('clean','确认清理所示终结记录正文')],flow='retention')
                return True,None
            if first in ('/item','/item 列表'):
                rows=self.s.rows('SELECT * FROM action_items WHERE owner=? ORDER BY updated DESC LIMIT 30',(self.owner,))
                return True,self.ux_work(row)
            if first=='/item 帮助':return True,ITEM_HELP
            if first.startswith('/item 新增 '):
                if len(parts)!=5:return True,ITEM_HELP
                project=first[len('/item 新增 '):];matches=select_projects(project,self.projects(row))
                if len(matches)!=1 or matches[0].get('identity_status')!='verified':raise BoundaryError('PROJECT_IDENTITY_UNVERIFIED')
                fields={'group_query':parts[2],'people_queries':[p.strip() for p in parts[3].split('、')],'body':parts[1],'delivery_mode':'immediate'}
                target,missing,options=self.g.reminders.resolve(fields,{})
                if not target:return True,'\n'.join(missing)+'\n请补充能唯一识别的准确群和人员。'
                snapshot={'project_ref':matches[0]['key'],'description':parts[1],'requirements':parts[4],'assignees':[p['id'] for p in target['people']],
                          'source':{'message_id':row['id'],'project_title':matches[0]['title'],'target':target,'original_time':row['created']}}
                self.card(row,object_id=row['id'],version=1,snapshot=snapshot,title='确认纳入 AM 跟进',
                          body=matches[0]['title']+'\n事项：'+parts[1]+'\n交付要求：'+parts[4]+'\n负责人：'+'、'.join(p['name'] for p in target['people'])+'\n不设未提供的截止时间；不自动发提醒。',actions=[('create','确认纳入跟进')],flow='item')
                return True,None
            m=re.fullmatch(r'/item (查看|处理中|暂停|恢复|取消|关闭|检查) ([a-f0-9]{32})(?: v(\d+))?',first)
            if not m:return True,ITEM_HELP
            item=self.items.get(m[2])
            if m[1]=='查看':return True,self.ux_item(row,item['id'])
            if not m[3]:raise BoundaryError('EXPECTED_VERSION_REQUIRED')
            action={'处理中':'processing','暂停':'pause','恢复':'resume','取消':'cancel','关闭':'close','检查':'check'}[m[1]]
            item=self.items.change(row['user'],row['id'],m[2],int(m[3]),action,evidence=parts[1] if len(parts)>1 and action=='close' else None,
                                   next_check=parse_time(parts[1],row['created']) if len(parts)>1 and action=='check' else None)
            return True,self.ux_item(row,item['id'])
        if first.startswith('/plan'):
            if not self.flags['scheduler']:raise BoundaryError('SCHEDULER_NOT_ENABLED')
            if first in ('/plan','/plan 帮助'):return True,PLAN_HELP
            edit=re.fullmatch(r'/plan (修改正文|改目标) ([a-f0-9]{32}) v(\d+)',first)
            if edit:
                if edit[1]=='修改正文' and len(parts)==2:
                    prior=self.scheduler.plan(edit[2]);body=parts[1]
                    if json.loads(prior['rule']).get('ux_literal_body'):body=body.replace('{','{{').replace('}','}}')
                    p=self.scheduler.revise(row['user'],row['id'],edit[2],int(edit[3]),template=body)
                elif edit[1]=='改目标' and len(parts)==3:
                    old=self.scheduler.plan(edit[2]);fields={'group_query':parts[1],'people_queries':parts[2].split('、'),'body':old['template'],'delivery_mode':'immediate'}
                    target,missing,options=self.g.reminders.resolve(fields,{})
                    if not target:return True,'\n'.join(missing)
                    payload=normalized_payload(old['item_id'],target['bot'],target['group'],target['people'],old['template'])
                    p=self.scheduler.revise(row['user'],row['id'],old['id'],int(edit[3]),target={'kind':'group','payload':payload})
                else:return True,PLAN_HELP
                return True,self.plan_card(row,p)
            m=re.fullmatch(r'/plan (一次|每天|工作日|特殊每天) ([a-f0-9]{32})',first)
            if m:
                item=self.items.get(m[2]);target=json.loads(item['source']).get('target')
                if not target:raise BoundaryError('ITEM_TARGET_NOT_VERIFIED')
                once=m[1]=='一次'
                if len(parts)!=(3 if once else 5):return True,PLAN_HELP
                body=parts[2] if once else parts[3]
                payload=normalized_payload(item['id'],target['bot'],target['group'],target['people'],body)
                rule={'at':parse_time(parts[1],row['created'])} if once else {'time':parts[1],'timezone':'Asia/Shanghai','calendar_version':self.scheduler.calendar.data.get('version'),'quiet_policy':'exact' if m[1]=='特殊每天' else 'business_hours'}
                valid_until=rule['at']+61 if once else parse_time(parts[2],row['created'])
                policy='fixed_rule' if once or parts[4]=='固定规则' else 'preview_each'
                p=self.scheduler.create(row['user'],row['id'],item_id=item['id'],kind='once' if once else 'workdays' if m[1]=='工作日' else 'daily',rule=rule,target={'kind':'group','payload':payload},template=body,policy=policy,valid_until=valid_until)
                return True,self.plan_card(row,p)
            m=re.fullmatch(r'/plan (查看|暂停|恢复|取消|对账) ([a-f0-9]{32})(?: v(\d+))?',first)
            if m:
                if m[1]=='对账':self.scheduler.reconcile(m[2],manual=True);return True,self.occurrence_text(self.scheduler.occurrence(m[2]))
                p=self.scheduler.plan(m[2])
                if m[1]=='查看':return True,canonical({k:p[k] for k in ('id','version','state','kind','rule','template','policy','error')})
                if not m[3]:raise BoundaryError('EXPECTED_VERSION_REQUIRED')
                p=self.scheduler.control(row['user'],row['id'],p['id'],int(m[3]),{'暂停':'pause','恢复':'resume','取消':'cancel'}[m[1]])
                return True,self.plan_card(row,p) if p['state']=='draft' else '计划状态：'+p['state']+'；已开始发送的结果仍需对账。'
            return True,PLAN_HELP
        m=re.fullmatch(r'(?:/briefing 订阅|订阅简报) (每天|工作日) (\d{2}:\d{2})',first)
        if m:
            if not self.flags['scheduler'] or len(parts)!=2:raise BoundaryError('SUBSCRIPTION_TIME_AND_VALIDITY_REQUIRED')
            self.projects(row)
            rule={'time':m[2],'timezone':'Asia/Shanghai','workdays':m[1]=='工作日','calendar_version':self.scheduler.calendar.data.get('version')}
            p=self.scheduler.create(row['user'],row['id'],item_id=None,kind='briefing',rule=rule,target={'kind':'owner','user':self.owner,'chat':row['chat']},template='',policy='owner_subscription',valid_until=parse_time(parts[1],row['created']))
            self.s.execute("INSERT OR IGNORE INTO am_subscriptions VALUES(?,?,?,'active')",(p['id'],self.owner,p['id']))
            return True,self.plan_card(row,p)
        return False,None
