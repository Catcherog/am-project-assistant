"""UX1 navigation and forms over the original AM services and durable queue.

Buttons carry only a nonce and a server-owned action key. Collection is bound at
receipt to the actual user's message, scope and non-renewable context revision.
"""
import json
import re
import uuid
import math
from datetime import datetime,timedelta
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical,digest
from fs_agent.scheduler import TZ,parse_time
from fs_agent.reminder_content import normalized_payload
from fs_agent.card_ui import card,paragraph,button,field
from group_ledger import render_project,select_projects

MENU_KEYS={'am_work':'home','am_projects':'projects','am_more':'more'}
PAGE_SIZE=5

def card_time(value):
    if not isinstance(value,str):raise BoundaryError('FORM_TIME_REQUIRED')
    try:
        dt=datetime.strptime(value,'%Y-%m-%d %H:%M %z')
        # Feishu appends the client's timezone as reference, not a timezone
        # selected by the user. Every UX1 time field explicitly says Beijing.
        return dt.replace(tzinfo=TZ).timestamp()
    except ValueError:
        # Some clients omit an offset. The form explicitly states Beijing time.
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}',value):raise BoundaryError('FORM_TIME_REQUIRED')
        return parse_time(value,0)

class UxMixin:
    def init_ux(self):
        with self.s.lock:self.s.conn.executescript('''
        CREATE TABLE IF NOT EXISTS am_ux_drafts(id TEXT PRIMARY KEY,scope TEXT NOT NULL,version INTEGER NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL,expires REAL NOT NULL,result TEXT);
        CREATE TABLE IF NOT EXISTS am_ux_collection(scope TEXT PRIMARY KEY,token TEXT NOT NULL,kind TEXT NOT NULL,target TEXT NOT NULL,expires REAL NOT NULL,state TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_ux_receipts(id TEXT PRIMARY KEY,context TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_followup_seen(item_id TEXT PRIMARY KEY,feedback_id TEXT NOT NULL);
        ''')

    def ux_cancel_collection(self,row,all_chat=False):
        scopes=[self.scope(row)]
        if all_chat:
            prefix=[row[k] for k in ('tenant','app','chat','user')]
            scopes=[r['scope'] for r in self.s.rows('SELECT scope FROM am_ux_collection') if json.loads(r['scope'])[:4]==prefix]
        for scope in scopes:self.s.execute("UPDATE am_ux_collection SET state='cancelled',target='{}' WHERE scope=?",(scope,))

    def ux_project(self,row,key,*,readonly=False):
        matches=[p for p in self.projects(row) if p['key']==key and (readonly or p.get('identity_status')=='verified')]
        if len(matches)!=1:raise BoundaryError('PROJECT_IDENTITY_UNVERIFIED')
        return matches[0]

    def ux_form_values(self,snapshot,body):
        specs=snapshot.get('fields')
        values=body.get('form_value') or {}
        if not isinstance(values,dict) or len(canonical(values))>16000:raise BoundaryError('FORM_INVALID')
        if specs is None:
            if values:raise BoundaryError('FORM_UNEXPECTED')
            return {}
        if set(values)-set(specs):raise BoundaryError('FORM_UNEXPECTED')
        out={}
        for name,spec in specs.items():
            value=values.get(name,[] if spec.get('kind')=='multi_select_static' else '')
            if spec.get('required',True) and not value:raise BoundaryError('FORM_REQUIRED')
            if spec.get('kind')=='multi_select_static':
                if not isinstance(value,list) or not all(isinstance(x,str) for x in value) or len(value)!=len(set(value)):raise BoundaryError('FORM_INVALID')
                if set(value)-{x[0] for x in spec['options']}:raise BoundaryError('FORM_OPTION_INVALID')
            else:
                if not isinstance(value,str) or len(value)>(1000 if spec.get('kind','input')=='input' else 4000):raise BoundaryError('FORM_INVALID')
                if spec.get('options') and value and value not in {x[0] for x in spec['options']}:raise BoundaryError('FORM_OPTION_INVALID')
                if spec.get('required',True) and not value.strip():raise BoundaryError('FORM_REQUIRED')
            out[name]=value
        return out

    def ux_draft(self,row,ident,version=None):
        ds=self.s.rows('SELECT * FROM am_ux_drafts WHERE id=? AND scope=? AND expires>?',(ident,self.scope(row),self.s.clock()))
        if not ds:raise BoundaryError('UX_DRAFT_EXPIRED')
        d=ds[0]
        if version is not None and version!=d['version']:raise BoundaryError('UX_DRAFT_CHANGED')
        if d['state']=='cancelled':raise BoundaryError('UX_DRAFT_EXPIRED')
        return d,json.loads(d['data'])

    def ux_remind(self,row,key,group=None,draft=None,item=None,cycle=False):
        if row['user']!=self.owner:raise BoundaryError('OWNER_ONLY')
        if not self.flags['scheduler'] or not self.flags['items']:raise BoundaryError('SCHEDULER_NOT_ENABLED')
        p=self.ux_project(row,key);port=self.g.reminders.port;existing=None
        if item:
            existing=self.items.get(item)
            if existing['owner']!=row['user'] or existing['project_ref']!=key or existing['paused'] or existing['state'] in ('closed','cancelled'):raise BoundaryError('ITEM_OPERATION_DENIED')
            target=json.loads(existing['source']).get('target')
            if not target:raise BoundaryError('ITEM_TARGET_NOT_VERIFIED')
            group=target['group']['id']
        groups=[port.group(g) for g in port.policy()['outbound_group_ids']]
        if not groups:raise BoundaryError('NO_APPROVED_GROUP')
        if group and group not in {g['id'] for g in groups}:raise BoundaryError('GROUP_NOT_APPROVED')
        group=group or groups[0]['id']
        group_info=next(g for g in groups if g['id']==group)
        members=list({m['id']:m for g in groups for m in port.members(g['id'])}.values())
        if not members:return self.ux_page(row,'暂无可选人员','当前群没有可核实的人员。',[('返回项目',{'page':'project','project':key})])
        if draft:
            d,old=self.ux_draft(row,draft)
            if d['state']=='done':
                result=json.loads(d['result'])
                return self.ux_plan_detail(row,result['plan']) if result.get('plan') else self.ux_item(row,result['item'])
            # Opening edit invalidates every prior preview before any new input.
            self.s.execute("UPDATE am_ux_drafts SET version=version+1,state='editing' WHERE id=?",(draft,))
            d,old=self.ux_draft(row,draft)
        else:
            ident=uuid.uuid4().hex;old={}
            if existing:old={'values':{'body':existing['description']},'people_ids':json.loads(existing['assignees']),'item':existing['id'],'item_version':existing['version']}
            self.s.execute("INSERT INTO am_ux_drafts VALUES(?,?,1,'editing','{}',?,NULL)",(ident,self.scope(row),row['expires']))
            d,_=self.ux_draft(row,ident)
        if old.get('item') and existing is None:
            existing=self.items.get(old['item'])
            if existing['owner']!=row['user'] or existing['project_ref']!=key:raise BoundaryError('ITEM_OPERATION_DENIED')
        people={str(i):m for i,m in enumerate(members)}
        defaults=old.get('values',{})
        selected=[i for i,m in people.items() if m['id'] in old.get('people_ids',[])]
        specs={
          'group':{'label':'目标群（仅已批准的群）','kind':'select_static','required':False,'options':[(g['id'],g['name']) for g in groups],'default':defaults.get('group',group)},
          'delivery_mode':{'label':'本次动作','kind':'select_static','required':False,'options':[('schedule','建立跟进并安排提醒'),('item_only','仅建立跟进，不发消息')],'default':defaults.get('delivery_mode','schedule')},
          'people':{'label':'负责人','kind':'multi_select_static','options':[(i,m['name']) for i,m in people.items()],'default':selected},
          'body':{'label':'事项及完整提醒正文','default':defaults.get('body',p.get('待办','') if len(p.get('待办',''))<=1000 and p.get('待办','').strip()!='—' else '')},
          'next_check':{'label':'需要反馈／下次跟进时间（可选，北京时间）','kind':'picker_datetime','required':False,'default':defaults.get('next_check','')},
          'when':{'label':'首次发送时间（仅安排提醒时必填，北京时间）','kind':'picker_datetime','required':False,'default':defaults.get('when',(datetime.fromtimestamp(self.s.clock(),TZ)+timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M'))},
          'frequency':{'label':'发送频率','kind':'select_static','options':[('once','仅一次'),('daily','每天（遵循已核实工作日历）'),('workdays','工作日（需已核实日历）'),('exact_daily','特殊每天（含休息日，按所示时刻）')],'default':defaults.get('frequency','once')},
          'until':{'label':'周期截止时间（仅周期提醒必填，北京时间）','kind':'picker_datetime','required':False,'default':defaults.get('until','')},
          'policy':{'label':'周期批准方式','kind':'select_static','required':False,'options':[('preview_each','每次发送前确认'),('fixed_rule','批准完整固定规则')],'default':defaults.get('policy','preview_each')}}
        if existing:specs.pop('next_check')
        return self.ux_page(row,'发起跟进',p['title']+'\n目标群：'+group_info['name']+'\n下一步查看完整预览。',
                [('取消',{'page':'cancel_draft','draft':d['id'],'version':d['version']})],fields=specs,submit=('查看完整预览',{'page':'preview_reminder'}),
                data={'draft':d['id'],'version':d['version'],'project':key,'group':group_info,'groups':{g['id']:g for g in groups},'people':people,'item':old.get('item'),'item_version':old.get('item_version'),'item_next_check':existing.get('next_check') if existing else None})

    def ux_preview_reminder(self,row,data,values):
        d,old=self.ux_draft(row,data['draft'],data['version'])
        if d['state']!='editing':raise BoundaryError('UX_DRAFT_CHANGED')
        p=self.ux_project(row,data['project']);people=[data['people'][i] for i in values['people']]
        port=self.g.reminders.port
        group=data.get('groups',{}).get(values.get('group') or data['group']['id'],data['group'])
        target={'bot':port.bot(),'group':group,'people':people}
        port.refresh(target)
        mode=values.get('delivery_mode') or 'schedule'
        frequency=values['frequency'];once=frequency=='once'
        next_check=data.get('item_next_check') if data.get('item') else card_time(values['next_check']) if values.get('next_check') else None
        saved={'project':p['key'],'title':p['title'],'values':values,'people_ids':[m['id'] for m in people],'target':target,'item':data.get('item'),'item_version':data.get('item_version'),'mode':mode,'next_check':next_check}
        if saved['item']:
            item=self.items.get(saved['item'])
            if item['version']!=saved['item_version'] or set(saved['people_ids'])!=set(json.loads(item['assignees'])):raise BoundaryError('ITEM_VERSION_CHANGED')
        # Preserve the user's form even if calendar validation fails.
        self.s.execute('UPDATE am_ux_drafts SET data=? WHERE id=?',(canonical(saved),d['id']))
        if next_check is not None and next_check<=self.s.clock():raise BoundaryError('FUTURE_CHECK_TIME_REQUIRED')
        if mode=='item_only':
            if saved['item']:raise BoundaryError('ITEM_ALREADY_EXISTS')
            self.s.execute("UPDATE am_ux_drafts SET data=?,version=version+1,state='preview' WHERE id=?",(canonical(saved),d['id']))
            d,_=self.ux_draft(row,d['id'])
            preview=p['title']+'\n负责人：'+'、'.join(x['name'] for x in people)+'\n归属群：'+target['group']['name']+'\n完整事项：\n'+values['body']+'\n下一次跟进：'+__import__('am_views').stamp(next_check)+'\n确认后仅建立 AM 跟进事项，不发送消息。'
            return self.ux_page(row,'完整跟进预览',preview,[('确认建立跟进',{'page':'confirm_reminder','draft':d['id'],'version':d['version']}),('返回修改',{'page':'remind','project':p['key'],'group':group['id'],'draft':d['id']}),('取消',{'page':'cancel_draft','draft':d['id'],'version':d['version']})])
        at=card_time(values['when'])
        if at<=self.s.clock():raise BoundaryError('FUTURE_SEND_TIME_REQUIRED')
        if not once and 'until' not in values:
            return self.ux_remind(row,p['key'],data['group']['id'],d['id'],cycle=True)
        kind='once' if once else 'workdays' if frequency=='workdays' else 'daily'
        rule={'at':at} if once else {'time':datetime.fromtimestamp(at,TZ).strftime('%H:%M'),'timezone':'Asia/Shanghai','calendar_version':self.scheduler.calendar.data.get('version'),'quiet_policy':'exact' if frequency=='exact_daily' else 'business_hours'}
        rule['ux_literal_body']=True
        if not once:rule['start_at']=at
        next_at=at if once else self.scheduler.next_time(kind,rule,at-1)
        until=at+61 if once else card_time(values['until'])
        if until<=next_at or until>self.s.clock()+366*86400:raise BoundaryError('PLAN_VALIDITY_REQUIRED')
        saved.update(kind=kind,rule=rule,next_at=next_at,until=until,policy='fixed_rule' if once else values.get('policy') or 'preview_each',window=60)
        self.s.execute("UPDATE am_ux_drafts SET data=?,version=version+1,state='preview' WHERE id=?",(canonical(saved),d['id']))
        d,_=self.ux_draft(row,d['id'])
        text=(p['title']+'\n目标群：'+target['group']['name']+'\n涉及人员：'+'、'.join(x['name'] for x in people)+
              '\n完整正文：\n'+values['body']+'\n首次实际发送：'+datetime.fromtimestamp(next_at,TZ).strftime('%Y-%m-%d %H:%M:%S')+
              '\n时区：Asia/Shanghai（北京时间）\n执行窗口：60 秒，超时不补发\n有效至：'+datetime.fromtimestamp(until,TZ).strftime('%Y-%m-%d %H:%M:%S')+
              '\n周期：'+{'once':'仅一次','daily':'每天，普通工作时间及已核实日历','workdays':'已核实工作日','exact_daily':'特殊每天，含休息日，按所示时刻'}[frequency]+
              '\n批准方式：'+('每次发送前仍需确认' if saved['policy']=='preview_each' else '批准此完整固定规则')+
              '\n下一次跟进：'+__import__('am_views').stamp(next_check)+'\n确认后纳入 AM 跟进并启用计划；不改变正式台账。')
        actions=[('确认所示提醒',{'page':'confirm_reminder','draft':d['id'],'version':d['version']}),('返回修改',{'page':'remind','project':p['key'],'group':target['group']['id'],'draft':d['id']}),('取消',{'page':'cancel_draft','draft':d['id'],'version':d['version']})]
        return self.ux_page(row,'完整提醒预览',text,actions)

    def ux_confirm_reminder(self,row,ident,version):
        d,v=self.ux_draft(row,ident,version)
        row['_immutable_output']=True;row['_business_event']='reminder-result/'+ident+'/'+str(version)
        if d['state']=='done':
            result=json.loads(d['result'])
            return self.ux_plan_detail(row,result['plan']) if result.get('plan') else self.ux_item(row,result['item'])
        if d['state']!='preview':raise BoundaryError('UX_DRAFT_CHANGED')
        p=self.ux_project(row,v['project'])
        if p['title']!=v['title']:raise BoundaryError('UX_DRAFT_CHANGED')
        if v.get('mode')!='item_only' and v['next_at']<self.s.clock():raise BoundaryError('FUTURE_SEND_TIME_REQUIRED')
        self.g.reminders.port.refresh(v['target'])
        op='ux/'+ident+'/'+str(version)
        if v.get('item'):
            item=self.items.get(v['item'])
            if item['version']!=v['item_version'] or item['paused'] or item['state'] in ('closed','cancelled'):raise BoundaryError('ITEM_VERSION_CHANGED')
        else:item=self.items.create(row['user'],op,project_ref=v['project'],description=v['values']['body'],requirements='',assignees=v['people_ids'],source={'project_title':v['title'],'target':v['target'],'preview':ident,'version':version},next_check=v.get('next_check'))
        if v.get('mode')=='item_only':
            self.s.execute("UPDATE am_ux_drafts SET state='done',result=? WHERE id=?",(canonical({'item':item['id'],'plan':None}),ident))
            return self.ux_item(row,item['id'])
        payload=normalized_payload(item['id'],v['target']['bot'],v['target']['group'],v['target']['people'],v['values']['body'])
        # The underlying scheduler supports templates. UX form text is literal,
        # so escape template braces exactly once before entering that service.
        literal_template=v['values']['body'].replace('{','{{').replace('}','}}')
        p=self.scheduler.create(row['user'],op,item_id=item['id'],kind=v['kind'],rule=v['rule'],target={'kind':'group','payload':payload},template=literal_template,policy=v['policy'],valid_until=v['until'],window_seconds=v['window'])
        self.scheduler.approve_plan(row['user'],p['id'],p['version'],p['hash'])
        self.s.execute("UPDATE am_ux_drafts SET state='done',result=? WHERE id=?",(canonical({'plan':p['id'],'item':item['id']}),ident))
        return self.ux_plan_detail(row,p['id'])

    def ux_occurrence_status(self,o):
        state=o['state']
        label='已发送并核对' if state=='sent' and o['readback_state']=='verified' else '结果待核对' if state in ('unknown','sending','sent') else '确定未发送' if state in ('confirmed_not_sent','cancelled') else '等待本次确认' if state=='awaiting_confirmation' else '等待发送'
        return datetime.fromtimestamp(o['scheduled_at'],TZ).strftime('%Y-%m-%d %H:%M')+'：'+label

    def ux_collect_start(self,row,key):
        if not self.flags['progress_collection']:raise BoundaryError('PROGRESS_COLLECTION_NOT_ENABLED')
        p=self.ux_project(row,key)
        self.ux_set_collection(row,'progress',{'project':p['key'],'title':p['title']})
        return self.ux_page(row,'更新进展','当前收集：'+p['title']+'\n请直接在当前聊天发送进展文字。将生成待核对提案；正式台账写回未开启。',[('更换项目',{'page':'projects','next':'collect'}),('取消收集',{'page':'cancel_collect'})])

    def ux_set_collection(self,row,kind,target):
        self.s.execute("INSERT INTO am_ux_collection VALUES(?,?,?,?,?,'active') ON CONFLICT(scope) DO UPDATE SET token=excluded.token,kind=excluded.kind,target=excluded.target,expires=excluded.expires,state='active'",(self.scope(row),uuid.uuid4().hex,kind,canonical(target),min(row['expires'],self.s.clock()+3600)))

    def ux_collection_at_receipt(self,row):
        rows=self.s.rows('SELECT * FROM am_ux_collection WHERE scope=?',(self.scope(row),))
        if not rows or rows[0]['state']=='cancelled':return None
        return rows[0]

    def ux_collection_in_chat(self,row):
        prefix=[row[k] for k in ('tenant','app','chat','user')]
        return any(json.loads(r['scope'])[:4]==prefix for r in self.s.rows("SELECT scope FROM am_ux_collection WHERE state IN ('active','expired')"))

    def ux_collect_message(self,row):
        receipts=self.s.rows('SELECT context FROM am_ux_receipts WHERE id=?',(row['id'],))
        if not receipts:raise BoundaryError('COLLECTION_CONTEXT_EXPIRED')
        receipt=json.loads(receipts[0]['context']);current=self.ux_collection_at_receipt(row)
        if not current or current['token']!=receipt['token'] or current['expires']<=self.s.clock():raise BoundaryError('COLLECTION_CONTEXT_EXPIRED')
        target=json.loads(current['target']);meta=self.source_meta(row)
        if current['kind']=='progress':
            if meta['message_type'] not in ('text','post'):raise BoundaryError('PROGRESS_TEXT_REQUIRED')
            p=self.ux_project(row,target['project'])
            from query_rules import identifiers
            mentioned=identifiers(row['body'])
            if mentioned and mentioned!=identifiers(p['title']):raise BoundaryError('COLLECTION_REFERENCE_CONFLICT')
            if meta.get('parent'):
                refs=self.s.rows('SELECT project_ref FROM am_query_refs WHERE message_id=? AND scope=? AND expires>?',(meta['parent'],self.scope(row),self.s.clock()))
                if not refs or refs[0]['project_ref']!=p['key']:raise BoundaryError('COLLECTION_REFERENCE_CONFLICT')
            r=self.updates.collect(row['user'],row['id'],p['key'],row['body'],self.collection_source(row),row['created'])
            return self.ux_proposal(row,self.updates.propose(r['id']))
        if current['kind']=='precheck':
            if not self.flags['precheck']:raise BoundaryError('PRECHECK_NOT_ENABLED')
            logic=self.g.logic;principal=dict(row,message_id=row['id'])
            d=logic.get(principal,target['draft'])
            if logic.current(principal)['id']!=d['id'] or json.loads(d['target'])!=target['target']:raise BoundaryError('COLLECTION_CONTEXT_CHANGED')
            if meta['message_type']=='file':
                if not self.flags['attachments']:raise BoundaryError('ATTACHMENTS_NOT_ENABLED')
                info=json.loads(row['body'])
                try:data=self.g.api.read_file(principal,info['file_key'],lambda:self.g.authorized(principal),limit=8*1024*1024)
                except BoundaryError as exc:
                    logic.add_source(principal,d,info['file_name'],b'',limitation='已提供文件，读取失败：'+str(exc))
                else:logic.add_source(principal,d,info['file_name'],data)
            else:logic.add_source(principal,d,'进件.txt',row['body'].encode())
            return self.ux_run_precheck(principal,target['draft'])
        raise BoundaryError('COLLECTION_CONTEXT_CHANGED')

    def ux_targets(self,row):
        if not self.flags['precheck']:raise BoundaryError('PRECHECK_NOT_ENABLED')
        settings=getattr(self.g.logic.business,'settings',{})
        targets=settings.get('targets',{})
        actions=[(str(v.get('label') or alias),{'page':'precheck_form','target':alias}) for alias,v in targets.items()]
        return self.ux_page(row,'选择预检目标','选择已登记的项目 / 节点 / 交付物。' if actions else '尚无已授权的预检目标，请先登记准确的项目、节点和交付物。',actions+[('返回首页',{'page':'home'})])

    def ux_precheck_form(self,row,alias):
        target=self.g.logic.business.resolve(row,alias)
        fields={'kind':{'label':'文种','kind':'select_static','options':[('test','测试报告'),('8d','异常分析'),('charter','项目章程')],'default':'test'},'requirements':{'label':'本次明确应交要求（每行一项；留空使用目标已登记要求）','required':False}}
        return self.ux_page(row,'资料预检',alias+'\n项目 / 节点 / 交付物已核实。材料会绑定此目标。',[('返回目标列表',{'page':'targets'})],fields=fields,submit=('开始收集材料',{'page':'start_precheck'}),data={'target':target})

    def ux_start_precheck(self,row,data,values):
        if self.g.logic.business.resolve(row,data['target']['id'])!=data['target']:raise BoundaryError('TARGET_REVISION_CHANGED')
        logic=self.g.logic;logic.create(row,values['kind']);d=logic.start_precheck(row,data['target']['id'],values['kind'])
        if values['requirements'].strip():d=logic.set_precheck_requirements(row,values['requirements'])
        if not self.g.authorized(row):raise BoundaryError('LOGIC_SCOPE_DENIED')
        self.ux_set_collection(row,'precheck',{'draft':d['id'],'target':data['target']})
        return self.ux_page(row,'等待资料','当前收集：'+data['target']['id']+'\n直接发送原文或 DOCX、文字层 PDF、文本附件。读取不完整会保留限制，不视为验收通过。',[('更换目标',{'page':'targets'}),('取消收集',{'page':'cancel_collect'})])

    def ux_handle(self,row,c,action):
        snapshot=json.loads(c['snapshot'])
        if row['user']!=self.owner:
            requested=snapshot.get('actions',{}).get(action,{})
            from am_query import READ_PAGES
            reading=requested.get('page') in READ_PAGES and (requested.get('page')!='long_content' or snapshot.get('data',{}).get('query_view'))
            document=requested.get('page')=='documents' and requested.get('draft') and self.g.authorized(row)
            if not reading and not document:raise BoundaryError('OWNER_ONLY')
            if reading and requested.get('next','project')!='project':raise BoundaryError('OWNER_ONLY')
        if snapshot.get('app')!=row['app'] or snapshot.get('tenant')!=row['tenant']:raise BoundaryError('CARD_IDENTITY_OR_VERSION_INVALID')
        if action not in snapshot['actions']:raise BoundaryError('CARD_ACTION_NOT_SUPPORTED')
        row['thread']=snapshot.get('thread')
        a=snapshot['actions'][action];body=json.loads(row['body'])
        page_flags={'preview_reminder':'scheduler','confirm_reminder':'scheduler','plan':'scheduler','plan_control':'scheduler','reconcile':'scheduler','subscribe':'scheduler','subscription_preview':'scheduler','subscription_confirm':'scheduler','start_precheck':'precheck','precheck_continue':'precheck','precheck_form':'precheck','item_change':'items','item_action_form':'items','item':'items','briefing':'snapshots'}
        if a['page'] in page_flags and not self.flags[page_flags[a['page']]]:raise BoundaryError('FEATURE_NOT_ENABLED')
        # Navigation buttons do not require the form's mandatory fields.
        submit=a['page'] in ('search','preview_reminder','start_precheck','subscription_preview','item_change','followup_edit_submit')
        saved=snapshot['data']
        if submit and saved.get('draft') and isinstance(body.get('form_value'),dict):
            d,old=self.ux_draft(row,saved['draft'],saved['version'])
            if d['state']!='editing':raise BoundaryError('UX_DRAFT_CHANGED')
            specs=snapshot.get('fields') or {};incoming=body['form_value']
            kept={k:v for k,v in incoming.items() if k in specs and isinstance(v,str) and len(v)<=4000}
            previous=dict(old,values=kept)
            if 'project' in saved:previous.update(project=saved['project'],item=saved.get('item'),item_version=saved.get('item_version'))
            if 'people' in saved and isinstance(incoming.get('people'),list):previous['people_ids']=[saved['people'][i]['id'] for i in incoming['people'] if isinstance(i,str) and i in saved['people']]
            self.s.execute('UPDATE am_ux_drafts SET data=? WHERE id=?',(canonical(previous),d['id']))
        values=self.ux_form_values(snapshot,body) if submit else {}
        data=dict(snapshot['data'],nonce=c['nonce']);page=a['page']
        current=self.s.rows('SELECT kind FROM am_interactions WHERE id=?',(snapshot.get('interaction_id'),))
        if page in ('home','projects','refresh','work') and current and current[0]['kind']=='recovery':
            row.pop('_interaction',None);row.pop('_view_version',None);row.pop('_snapshot_version',None)
            row['_view_kind']='navigation';row['_resume_navigation']=True
        if page=='reconcile_navigation':return self.navigation_recovery(row,a['interaction'],check=True)
        if page=='home':
            row.pop('_snapshot_version',None)
            return self.ux_home(row)
        if page=='recent':return self.ux_projects(row,query=a.get('query',''),page=a.get('offset',0),recent=True)
        if page=='refresh':
            row.pop('_snapshot_version',None)
            row['_force_refresh']=not row.get('_resumed_source')
            if a.get('project'):return self.ux_project_detail(row,a['project'],a.get('section'),origin=a.get('origin'))
            return self.ux_projects(row)
        if page=='projects':return self.ux_projects(row,next=a.get('next','project'),query=a.get('query',''),page=a.get('offset',0))
        if page=='search':return self.ux_projects(row,next=a['next'],query=values['query'])
        if page=='project':return self.ux_project_detail(row,a['project'],a.get('section'),origin=a.get('origin'))
        if page=='remind':return self.ux_remind(row,a['project'],a.get('group'),a.get('draft'),a.get('item'))
        if page=='preview_reminder':return self.ux_preview_reminder(row,data,values)
        if page=='confirm_reminder':return self.ux_confirm_reminder(row,a['draft'],a['version'])
        if page=='cancel_draft':
            d,_=self.ux_draft(row,a['draft'],a['version'])
            if d['state']=='done':raise BoundaryError('UX_DRAFT_CHANGED')
            self.s.execute("UPDATE am_ux_drafts SET state='cancelled',version=version+1 WHERE id=?",(d['id'],));return self.ux_home(row)
        if page=='collect':return self.ux_collect_start(row,a['project'])
        if page=='cancel_collect':self.ux_cancel_collection(row);return self.ux_home(row)
        if page=='retry_form':
            old=self.s.rows('SELECT * FROM am_cards WHERE nonce=?',(a['nonce'],))
            if not old or old[0]['actor']!=row['user'] or old[0]['chat']!=row['chat'] :raise BoundaryError('UX_DRAFT_EXPIRED')
            snap=json.loads(old[0]['snapshot']);saved=snap['data']
            if saved.get('draft'):
                d,v=self.ux_draft(row,saved['draft'])
                if saved.get('subscription'):return self.ux_more_action(row,{'page':'subscribe','draft':d['id']})
                return self.ux_remind(row,saved['project'],saved['group']['id'],d['id'])
            if saved.get('target'):return self.ux_precheck_form(row,saved['target']['id'])
            return self.ux_more_action(row,{'page':'subscribe'})
        if page=='targets':return self.ux_targets(row)
        if page=='precheck_form':return self.ux_precheck_form(row,a['target'])
        if page=='start_precheck':return self.ux_start_precheck(row,data,values)
        if page=='precheck_continue':
            d=self.g.logic.get(row,a['draft']);target=json.loads(d['target']);self.ux_set_collection(row,'precheck',{'draft':d['id'],'target':target})
            return '当前收集：'+target['id']+'。请直接发送补充材料。'
        if page=='plan':return self.ux_plan_detail(row,a['plan'])
        if page=='reconcile':
            self.scheduler.reconcile(a['occurrence'],manual=True);o=self.scheduler.occurrence(a['occurrence']);return self.ux_plan_detail(row,o['plan_id'])
        if page=='plan_control':
            p=self.scheduler.control(row['user'],'ux/'+c['nonce'],a['plan'],a['version'],a['control'])
            if p['state']=='draft':return self.plan_card(row,p)
            return self.ux_plan_detail(row,p['id'])
        return self.ux_more_action(row,a,values,data)

    def ux_more_action(self,row,a,values=None,data=None):
        if a['page']=='followup_dashboard':return self.followup.dashboard(row)
        if a['page']=='followup_source_review':
            self.followup.source_review_text(row,a['item'])
            return None
        if a['page']=='followup_import_candidate':
            self.followup.candidate_text(row,a['candidate'])
            return None
        if a['page']=='followup_edit_submit':
            self.followup.preview_text(row,data['item'],values['channel'],body=values['body'],recipient=values.get('recipient'))
            return None
        if a['page']=='followup_preview':
            self.followup.preview_text(row,a['item'],a['channel'])
            return None
        page=a['page'];values=values or {};data=data or {}
        if page=='more':return self.ux_page(row,'更多','选择要查看或设置的内容。',[('提醒计划',{'page':'plans'}),('变化简报',{'page':'briefing'}),('简报订阅',{'page':'subscribe'}),('资料预检',{'page':'targets'}),('文稿整理',{'page':'documents'}),('运行状态',{'page':'status'}),('帮助',{'page':'help'}),('返回首页',{'page':'home'})])
        if page in ('status','help','briefing'):
            if page=='status':return self.ux_status(row)
            elif page=='help':text='从首页选择项目、提醒、进展或资料预检。文字和附件直接在当前聊天发送。发送“/chat”可返回普通聊天。'
            else:return self.briefing(row)
            return self.ux_page(row,{'status':'运行状态','help':'使用帮助','briefing':'变化简报'}[page],text,[('返回首页',{'page':'home'}),('更多',{'page':'more'})])
        if page=='work':return self.ux_work(row,a.get('offset',0),a.get('bucket','action'))
        if page=='plans':
            if not self.flags['scheduler']:raise BoundaryError('SCHEDULER_NOT_ENABLED')
            rows=self.s.rows('SELECT * FROM reminder_plans WHERE owner=? ORDER BY created DESC',(self.owner,))
            return self.ux_object_list(row,'提醒计划','查看已确认或待处理的计划。',rows,'plan',a.get('offset',0))
        if page=='item':return self.ux_item(row,a['item'])
        if page=='item_action_form':
            fields={'detail':{'label':'核验依据 / 补充说明','required':a['control']=='close'}}
            if a['control']=='check':fields={'when':{'label':'下一次检查时间（北京时间）','kind':'picker_datetime'}}
            return self.ux_page(row,'确认跟进操作',self.ux_item_text(self.items.get(a['item'])),[('返回事项',{'page':'item','item':a['item']})],fields=fields,submit=('确认本次操作',{'page':'item_change'}),data=a)
        if page=='item_change':
            self.items.change(row['user'],row['id'],data['item'],data['version'],data['control'],evidence=values.get('detail') if data['control']=='close' else None,next_check=card_time(values['when']) if data['control']=='check' else None)
            return self.ux_more_action(row,{'page':'item','item':data['item']})
        if page=='subscribe':
            if not self.flags['scheduler'] or not self.flags['snapshots']:raise BoundaryError('SCHEDULER_NOT_ENABLED')
            if a.get('draft'):
                d,old=self.ux_draft(row,a['draft'])
                if d['state']=='done':return self.ux_plan_detail(row,json.loads(d['result'])['plan'])
                self.s.execute("UPDATE am_ux_drafts SET version=version+1,state='editing' WHERE id=?",(d['id'],))
                d,old=self.ux_draft(row,d['id'])
            else:
                ident=uuid.uuid4().hex
                self.s.execute("INSERT INTO am_ux_drafts VALUES(?,?,1,'editing','{}',?,NULL)",(ident,self.scope(row),row['expires']))
                d,old=self.ux_draft(row,ident)
            defaults=old.get('values',{})
            fields={'frequency':{'label':'频率','kind':'select_static','options':[('daily','每天'),('workdays','已核实工作日')],'default':defaults.get('frequency','daily')},'time':{'label':'北京时间（HH:MM）','default':defaults.get('time','09:00')},'until':{'label':'订阅截止时间（北京时间）','kind':'picker_datetime','default':defaults.get('until','')}}
            return self.ux_page(row,'本人简报订阅','范围：当前本人可访问的项目变化及待办。仅发本人私聊。',[('返回更多',{'page':'more'})],fields=fields,submit=('查看订阅预览',{'page':'subscription_preview'}),data={'draft':d['id'],'version':d['version'],'subscription':True})
        if page=='subscription_preview':
            d,old=self.ux_draft(row,data['draft'],data['version'])
            if d['state']!='editing':raise BoundaryError('UX_DRAFT_CHANGED')
            self.s.execute('UPDATE am_ux_drafts SET data=? WHERE id=?',(canonical({'values':values,'subscription':True}),d['id']))
            if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',values['time']):raise BoundaryError('SCHEDULE_TIME_REQUIRED')
            rule={'time':values['time'],'timezone':'Asia/Shanghai','workdays':values['frequency']=='workdays','calendar_version':self.scheduler.calendar.data.get('version'),'briefing_content':['changes','todos']}
            next_at=self.scheduler.next_time('briefing',rule,self.s.clock()-1);until=card_time(values['until'])
            if until<=next_at or until>self.s.clock()+366*86400:raise BoundaryError('PLAN_VALIDITY_REQUIRED')
            saved={'values':values,'subscription':True,'rule':rule,'until':until,'next_at':next_at}
            self.s.execute("UPDATE am_ux_drafts SET data=?,version=version+1,state='preview' WHERE id=?",(canonical(saved),d['id']))
            d,_=self.ux_draft(row,d['id'])
            return self.ux_page(row,'订阅完整预览','范围：本人当前可访问项目\n目标：本人私聊\n内容：项目变化、待办及来源时间\n北京时间：'+values['time']+'\n频率：'+('已核实工作日' if rule['workdays'] else '每天')+'\n下一次：'+datetime.fromtimestamp(next_at,TZ).isoformat()+'\n有效至：'+datetime.fromtimestamp(until,TZ).isoformat()+'\n执行窗口：60 秒，过期不补发。确认后按固定规则执行。',[('确认订阅',{'page':'subscription_confirm','draft':d['id'],'version':d['version']}),('返回修改',{'page':'subscribe','draft':d['id']})])
        if page=='subscription_confirm':
            d,v=self.ux_draft(row,a['draft'],a['version'])
            row['_immutable_output']=True;row['_business_event']='subscription-result/'+d['id']+'/'+str(d['version'])
            if d['state']=='done':return self.ux_plan_detail(row,json.loads(d['result'])['plan'])
            if d['state']!='preview':raise BoundaryError('UX_DRAFT_CHANGED')
            if v['next_at']<self.s.clock():raise BoundaryError('FUTURE_SEND_TIME_REQUIRED')
            self.projects(row)
            p=self.scheduler.create(row['user'],'ux-sub/'+d['id']+'/'+str(d['version']),item_id=None,kind='briefing',rule=v['rule'],target={'kind':'owner','user':self.owner,'chat':row['chat']},template='',policy='owner_subscription',valid_until=v['until'])
            self.s.execute("INSERT OR IGNORE INTO am_subscriptions VALUES(?,?,?,'active')",(p['id'],self.owner,p['id']))
            self.scheduler.approve_plan(row['user'],p['id'],p['version'],p['hash'])
            self.s.execute("UPDATE am_ux_drafts SET state='done',result=? WHERE id=?",(canonical({'plan':p['id']}),d['id']))
            return self.ux_plan_detail(row,p['id'])
        return self.ux_extra(row,a,values,data)

    def ux_object_list(self,row,title,text,rows,kind,page):
        selected=rows[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
        actions=[((r.get('description') or r.get('template') or '本人变化简报')[:80],{'page':kind,kind:r['id']}) for r in selected]
        parent='work' if kind=='item' else 'plans'
        if page:actions.append(('上一页',{'page':parent,'offset':page-1}))
        if (page+1)*PAGE_SIZE<len(rows):actions.append(('下一页',{'page':parent,'offset':page+1}))
        if not rows:text+='\n当前尚无 AM '+('跟进事项' if kind=='item' else '提醒计划')+'。'
        if kind=='plan':actions.append(('创建提醒计划',{'page':'projects','next':'remind'}))
        return self.ux_page(row,title,text,actions+[('返回首页',{'page':'home'}),('更多',{'page':'more'})])

    def ux_error(self,row,code):
        if code=='SOURCE_ACCESS_CHANGED' and row['user']==self.owner and row['chat_type']=='p2p':
            # Source-bearing output remains rejected. Only a newly constructed,
            # fixed recovery view may drop that stale read fence. It contains
            # no prior text, facts, source values, form fields or approval action.
            # Do not relabel the failed payload with a newer source epoch.
            recovery={k:row[k] for k in ('id','app','tenant','user','chat','chat_type','created','expires','kind','body')}
            recovery.update(_view_kind='recovery',_query_read=False,_query_denial=True,
                            _interaction=digest(['source-access-recovery',self.scope(row),row['id']]))
            return self.ux_page(recovery,'资料已变化',
                '这次读取期间资料范围发生变化，旧结果未展示。请重新打开跟进或项目查询；已有事项和发送记录保留，发送仍需有效预览确认。',
                [('重新打开我的跟进',{'page':'work'}),('项目查询',{'page':'projects'})])
        row['_view_kind']='recovery'
        if code in ('VIEW_DELIVERY_UNKNOWN','VIEW_DELIVERY_REJECTED'):
            ident=row.get('_blocked_interaction') or row.get('_interaction')
            return self.navigation_recovery(row,ident) if ident else None
        if code in ('VIEW_CHANGED','VIEW_EXPIRED','CARD_IDENTITY_OR_VERSION_INVALID','QUERY_SNAPSHOT_EXPIRED'):
            row['_interaction']=digest(['ux-recovery',self.scope(row)]);row.pop('_view_version',None);row.pop('_snapshot_version',None)
        messages={'VIEW_EXPIRED':'当前视图已过期，请重新打开首页；业务记录保留。','VIEW_CHANGED':'当前页面已更新，请打开最新导航。原业务记录和有效草稿保留。','FORM_REQUIRED':'请填写必填内容。','FORM_OPTION_INVALID':'选项已失效，请重新选择。','FORM_INVALID':'表单内容无法识别，请重新填写。','FORM_TIME_REQUIRED':'请选择完整日期和时间。','FUTURE_SEND_TIME_REQUIRED':'发送时刻已经过去，请重新设置未来时间。','PLAN_VALIDITY_REQUIRED':'截止时间需要晚于下一次发送，且在一年以内。','CALENDAR_COVERAGE_MISSING':'尚无覆盖该日期的已核实工作日历。可改为明确的一次提醒，或待日历核实后设置普通周期。','UX_DRAFT_CHANGED':'内容已变化，请重新预览再确认。','UX_DRAFT_EXPIRED':'草稿已过期或取消，请重新创建。','COLLECTION_CONTEXT_EXPIRED':'收集已过期或对象已更换，请重新选择收集目标后发送。','COLLECTION_REFERENCE_CONFLICT':'引用的项目与当前收集对象不一致，请重新选择目标。','TARGET_REVISION_CHANGED':'目标内容已变化，请重新选择并预检。','CARD_IDENTITY_OR_VERSION_INVALID':'此卡片已过期或不属于当前身份与聊天，请重新打开菜单。','PROJECT_IDENTITY_UNVERIFIED':'项目身份尚未核实，请重新选择。','PROGRESS_TEXT_REQUIRED':'进展采集需要文字；附件请通过资料预检入口提交。'}
        actions=[('返回首页',{'page':'home'}),('更多',{'page':'more'})]
        if row['kind']=='card':
            body=json.loads(row['body']);nonce=body.get('value',{}).get('nonce')
            old=self.s.rows('SELECT * FROM am_cards WHERE nonce=?',(nonce,))
            if old and old[0]['actor']==row['user'] and old[0]['chat']==row['chat'] and old[0]['flow']=='ux':
                snapshot=json.loads(old[0]['snapshot'])
                if snapshot.get('fields'):actions.insert(0,('返回修改',{'page':'retry_form','nonce':nonce}))
        return self.ux_page(row,'本次未完成',messages.get(code,'当前操作未完成，请返回后重试；已有记录保留。'),actions)

    def ux_cleanup(self):
        now=self.s.clock()
        self.s.execute("UPDATE am_interactions SET current_view='{}',subject_refs='[]' WHERE expires<=?",(now,))
        self.s.execute('DELETE FROM am_navigation WHERE interaction_id IN (SELECT id FROM am_interactions WHERE expires<=?)',(now,))
        self.s.execute("UPDATE am_ux_drafts SET data='{}',result=NULL,state='cancelled' WHERE expires<=?",(now,))
        self.s.execute("UPDATE am_ux_collection SET target='{}',state='expired' WHERE expires<=? AND state='active'",(now,))
        self.s.execute('DELETE FROM am_ux_receipts WHERE id IN (SELECT id FROM am_inbox WHERE expires<=?)',(now,))
