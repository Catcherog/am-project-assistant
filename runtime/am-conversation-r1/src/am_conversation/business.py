"""Natural-language tools backed by the existing AM business journals.

Only the host resolves people/groups, authorizes work and publishes messages.
Preparing a send never sends it. Stable conversation action IDs are also the
downstream idempotency keys; unknown outcomes are reconciled, never resent.
"""
from __future__ import annotations
from datetime import datetime
import hashlib,json,uuid
from .tools import Tool,obj,string,display_time
from .types import ConversationError,canonical

def fingerprint(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def absolute(value):
    try:
        result=datetime.fromisoformat(value.replace('Z','+00:00'))
        if result.tzinfo is None:raise ValueError()
        return result.timestamp()
    except (ValueError,TypeError,AttributeError):raise ConversationError('ABSOLUTE_TIME_WITH_ZONE_REQUIRED') from None

class BusinessTools:
    def __init__(self,bridge):
        self.b=bridge;self.am=bridge.service;self.s=bridge.s
        self.port=self.am.g.reminders.port
        with self.s.lock:self.s.conn.executescript('''
        CREATE TABLE IF NOT EXISTS am_conversation_sends(action_id TEXT PRIMARY KEY,actor TEXT NOT NULL,scope TEXT NOT NULL,args_hash TEXT NOT NULL,payload TEXT NOT NULL,state TEXT NOT NULL,started REAL,message_id TEXT,receipt TEXT,error TEXT);
        CREATE TABLE IF NOT EXISTS am_conversation_links(kind TEXT NOT NULL,object_id TEXT NOT NULL,scope TEXT NOT NULL,event_id TEXT NOT NULL,PRIMARY KEY(kind,object_id));
        ''')

    def row(self,ctx):return self.b._current(ctx)['row']
    def guard(self,ctx,operation='manage',args=None):
        self.b.authorize(ctx,operation,args or {})
        if operation=='report':self.am.ledger.port.authorize_user(ctx.actor_id)
        return True
    def operation(self,ctx,event,args):return 'conversation/'+fingerprint([ctx.scope,event,args])
    def project(self,ctx,ident):
        if ident is None:return {'id':'personal:'+ctx.actor_id,'title':'个人事项','identity_status':'verified'}
        p=self.b.projects.read_project(ctx,ident)
        if p.get('identity_status')!='verified':raise ConversationError('PROJECT_IDENTITY_UNVERIFIED')
        return p
    def item(self,ctx,ident):
        item=self.am.items.get(ident)
        if not ctx.can_access_project(item['project_ref']) and not (ctx.is_owner and item['project_ref']=='personal:'+ctx.actor_id):raise ConversationError('PROJECT_ACCESS_DENIED')
        if ctx.is_owner:
            if item['owner']!=ctx.actor_id:raise ConversationError('OWNER_REQUIRED')
        elif ctx.actor_id not in json.loads(item['assignees']):raise ConversationError('NOT_ASSIGNEE')
        return item
    def plan(self,ctx,ident):
        plan=self.am.scheduler.plan(ident)
        if not ctx.is_owner or plan['owner']!=ctx.actor_id:raise ConversationError('OWNER_REQUIRED')
        if plan['item_id']:self.item(ctx,plan['item_id'])
        return plan

    def targets(self,ctx,args,event):
        self.guard(ctx)
        groups=[]
        for ident in self.port.policy()['outbound_group_ids']:
            group=self.port.group(ident)
            if args.get('group_query') and args['group_query'].casefold() not in group['name'].casefold():continue
            people=self.port.members(ident)
            if args.get('person_query'):people=[p for p in people if args['person_query'].casefold() in p['name'].casefold()]
            groups.append({'id':ident,'name':group['name'],'members':people})
        return {'self':{'id':ctx.actor_id,'name':'本人'},'groups':groups,'scope':'仅原先已批准的出站群；名字不授予权限'}

    def target(self,ctx,args,event):
        target=args.get('target','self')
        body=args.get('body','')
        if not body or len(body)>4000 or '<at' in body.lower():raise ConversationError('PLAIN_MESSAGE_BODY_REQUIRED')
        if target=='self':return {'kind':'owner','user':ctx.actor_id,'chat':ctx.chat_id,'body':body}
        group_id=args.get('group_id');people_ids=args.get('recipient_ids',[])
        if group_id not in self.port.policy()['outbound_group_ids'] or not people_ids:raise ConversationError('EXACT_TARGET_REQUIRED')
        group=self.port.group(group_id);members={p['id']:p for p in self.port.members(group_id)}
        if len(set(people_ids))!=len(people_ids) or any(p not in members for p in people_ids):raise ConversationError('RECIPIENT_NOT_VERIFIED')
        if args.get('project_id'):
            p=self.project(ctx,args['project_id'])
            if not self.am.ledger.port.filter_projects(group_id,[dict(p,title=p['title'])]):raise ConversationError('GROUP_PROJECT_SCOPE')
        from fs_agent.reminder_content import normalized_payload
        marker=fingerprint([ctx.scope,event,args])[:32]
        payload=normalized_payload(marker,self.port.bot(),group,[members[p] for p in people_ids],body)
        return {'kind':'group','payload':payload,'body':body}

    def target_text(self,target):
        if target['kind']=='owner':return '目标：本人 AM 私聊\n完整正文：\n'+target['body']
        p=target['payload']
        if target.get('body') is not None:p=dict(p,body=target['body'])
        from fs_agent.reminder_content import display
        return '发送身份：'+p['bot']['name']+'\n目标群：'+p['group']['name']+'\n接收人：'+'、'.join(x['name'] for x in p['people'])+'\n完整群消息：\n'+display(p)

    def send_preview(self,ctx,args):
        self.guard(ctx,'manage',args)
        from fs_agent.reminder_content import scheduled_request
        text=self.row(ctx)['body']
        if scheduled_request(text) and not any(w in text for w in ('改为现在','改成现在','立即发送')):raise ConversationError('USE_SCHEDULE_FOR_FUTURE_SEND')
        target=self.target(ctx,args,self.b._current(ctx).get('engine_event_id',self.row(ctx)['id']))
        return self.target_text(target)+'\n发送时机：确认后立即发送一次。'

    def send(self,ctx,args,key):
        self.guard(ctx,'execute',args);action=self.b.store.get_action(ctx,key)
        target=self.target(ctx,args,action['presented_event_id'])
        preview=self.target_text(target)+'\n发送时机：确认后立即发送一次。'
        if preview!=action['preview']:return {'status':'failed','message':'发送对象或正文已变化，请重新预览。'}
        saved=self.s.rows('SELECT * FROM am_conversation_sends WHERE action_id=?',(key,))
        if saved:return self._send_result(saved[0])
        request_key=uuid.uuid5(uuid.NAMESPACE_URL,key).hex
        payload={'target':target,'request_key':request_key,'origin':{k:self.row(ctx)[k] for k in ('app','tenant','chat','user','chat_type','expires')}}
        with self.s.tx() as c:
            from am_harness.reservation import reserve_effect
            reserve_effect(c,key,'send',{'args':args,'payload':payload,'digest':action['digest']})
            c.execute("INSERT INTO am_conversation_sends VALUES(?,?,?,?,?,'sending',?,NULL,NULL,NULL)",(key,ctx.actor_id,ctx.session_id,fingerprint(args),canonical(payload),self.s.clock()))
        try:
            guard=lambda:self.guard(ctx,'execute',args)
            if target['kind']=='group':
                self.port.refresh(target['payload']);receipt=self.port.send(target['payload'],request_key,guard);mid=receipt['message_id']
            else:
                out=dict(payload['origin'],id=request_key,task_id=None,kind='text',body=target['body'],artifact=None,nonce=None)
                mid=self.am.g.api.send(out,guard)
            self.s.execute("UPDATE am_conversation_sends SET state='sent',message_id=? WHERE action_id=?",(mid,key))
            return self.reconcile_send(ctx,{'action_id':key},key)
        except Exception as exc:
            from fs_agent.feishu import SendRejected
            state='failed' if isinstance(exc,SendRejected) else 'unknown'
            self.s.execute('UPDATE am_conversation_sends SET state=?,error=? WHERE action_id=?',(state,'SEND_REJECTED' if state=='failed' else 'SEND_RESULT_UNKNOWN',key))
            return {'status':state}

    def _send_result(self,row):
        if row['state']=='verified':return {'status':'succeeded','verified':True,'receipt_id':row['message_id'],'message':'消息已发送，接收对象和正文已回读核对。'}
        return {'status':'failed' if row['state']=='failed' else 'unknown'}

    def reconcile_send(self,ctx,args,event):
        self.guard(ctx);key=args['action_id'];self.b.store.action_metadata(ctx,key)
        rows=self.s.rows('SELECT * FROM am_conversation_sends WHERE action_id=? AND actor=? AND scope=?',(key,ctx.actor_id,ctx.session_id))
        if not rows:raise ConversationError('SEND_NOT_FOUND')
        row=rows[0]
        if row['state'] in ('verified','failed'):return self._send_result(row)
        payload=json.loads(row['payload']);target=payload['target'];ok=False;receipt=None
        try:
            if target['kind']=='group':
                receipt=self.port.reconcile(target['payload'],payload['request_key'],row['started'],row['message_id'])
                from fs_agent.reminder_content import verify_message
                ok=bool(receipt and verify_message(target['payload'],receipt))
            elif row['message_id']:
                out=dict(payload['origin'],body=target['body'],kind='text',_query_readback_guard=lambda:self.guard(ctx))
                ok=self.am.g.api.verify_text(row['message_id'],out);receipt={'message_id':row['message_id']}
        except Exception:pass
        if not ok:
            self.s.execute("UPDATE am_conversation_sends SET state='unknown' WHERE action_id=?",(key,));return {'status':'unknown'}
        self.s.execute("UPDATE am_conversation_sends SET state='verified',message_id=?,receipt=?,error=NULL WHERE action_id=?",(receipt['message_id'],canonical({'message_id':receipt['message_id'],'verified':True,'observed_at':self.s.clock()}),key))
        result=self._send_result(dict(row,state='verified',message_id=receipt['message_id']))
        action=self.b.store.action_metadata(ctx,key)
        if action['status']=='unknown':self.b.store.finish_execution(ctx,key,status='succeeded',result=result)
        return result

    def create_followup(self,ctx,args,event):
        self.guard(ctx,'manage',args);p=self.project(ctx,args.get('project_id'))
        self.check_clock(ctx,args)
        people=args.get('assignee_ids') or [ctx.actor_id]
        source={'project_title':p['title'],'conversation_event':event,'record_kind':'am_followup','time_anchor':self.row(ctx)['created']}
        from fs_agent.reminder_content import dates
        deadline=dates(args['deadline'],source['time_anchor']) if args.get('deadline') else None
        if people!=[ctx.actor_id]:
            target=self.target(ctx,dict(args,target='group',recipient_ids=people,body=args['description']),event)
            source['target']=target['payload']
        item=self.am.items.create(ctx.actor_id,self.operation(ctx,event,args),project_ref=p['id'],description=args['description'],requirements=args.get('requirements',''),assignees=people,source=source,
            deadline=deadline,deadline_kind=args.get('deadline_kind','unspecified'),next_check=absolute(args['next_check']) if args.get('next_check') else None)
        labels={'estimate':'预计时间','commitment':'承诺时间','requirement':'要求时间','check':'检查时间','unspecified':'时间说明'}
        message='已登记这项跟进。'+(labels[item['deadline_kind']]+'：'+deadline+'。' if deadline else '')+'尚未向其他人发送消息。'
        return {'item':item,'formal_project_changed':False,'notification_sent':False,'message':message}

    def change_followup(self,ctx,args,event):
        self.guard(ctx);item=self.item(ctx,args['item_id'])
        self.check_clock(ctx,args)
        result=self.am.items.change(ctx.actor_id,self.operation(ctx,event,args),item['id'],args['version'],args['operation'],next_check=absolute(args['next_check']) if args.get('next_check') else None)
        labels={'processing':'已记为处理中','pause':'已暂停跟进','resume':'已恢复跟进','cancel':'已取消跟进','check':'已更新下次检查时间'}
        return {'item':result,'formal_project_changed':False,'message':labels[args['operation']]+'：'+item['description']}

    def check_clock(self,ctx,args):
        if args.get('next_check'):
            import re
            if not re.search(r'\d{1,2}[:：点时]|[一二三四五六七八九十两]+点|\d+\s*(?:分钟|小时)后',self.row(ctx)['body']):
                raise ConversationError('FOLLOWUP_CLOCK_UNSPECIFIED_USE_ESTIMATED_DEADLINE_TEXT')

    def close_preview(self,ctx,args):
        self.guard(ctx);item=self.item(ctx,args['item_id'])
        if item['version']!=args['version']:raise ConversationError('ITEM_VERSION_CHANGED')
        return '准备核验关闭跟进事项：'+item['description']+'\n核验依据：'+args['evidence']+'\n只关闭此 AM 跟进，不改变正式项目或验收节点。'

    def close_followup(self,ctx,args,key):
        if self.close_preview(ctx,args)!=self.b.store.get_action(ctx,key)['preview']:return {'status':'failed'}
        item=self.am.items.change(ctx.actor_id,key,args['item_id'],args['version'],'close',evidence=args['evidence'])
        actual=self.am.items.get(item['id'])
        return {'status':'succeeded','verified':actual['state']=='closed' and actual['close_evidence']==args['evidence'],'receipt_id':item['id'],'message':'这项跟进已按所示依据核验关闭，正式项目状态未修改。'}

    def feedback(self,ctx,args,event):
        self.guard(ctx,'report',args);item=self.item(ctx,args['item_id'])
        raw=self.row(ctx)['body']
        if args['content'] not in raw:raise ConversationError('FEEDBACK_MUST_QUOTE_CURRENT_INPUT')
        source={'message_id':event,'reporter':ctx.actor_id,'chat':ctx.chat_id,'evidence_state':'reported_not_accepted'}
        result=self.am.items.feedback(ctx.actor_id,self.operation(ctx,event,args),item['id'],args['version'],args['kind'],args['content'],source)
        return {'applied':result['applied'],'state':result['item']['state'],'professional_acceptance':False,'message':'反馈已记录；提交材料不等于核验完成。'}

    def schedule_data(self,ctx,args,event):
        self.guard(ctx,'manage',args)
        target=self.target(ctx,args,event);kind=args.get('frequency','once')
        if kind=='once':
            at=absolute(args['at'])
            if at<=self.s.clock():raise ConversationError('FUTURE_SEND_TIME_REQUIRED')
            until=absolute(args['until']) if args.get('until') else at+3600
            rule={'at':at};label=display_time(args['at'])
        else:
            import re
            if not re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d',args['at']):raise ConversationError('SCHEDULE_TIME_REQUIRED')
            if not args.get('until'):raise ConversationError('RECURRING_END_TIME_REQUIRED')
            until=absolute(args['until']);rule={'time':args['at'],'timezone':'Asia/Shanghai','quiet_policy':'exact'}
            if kind=='workdays':rule['calendar_version']=self.am.scheduler.calendar.data.get('version')
            label=('每天' if kind=='daily' else '工作日')+args['at']+'（北京时间）'
            at=self.am.scheduler.next_time(kind,rule,self.s.clock()-1)
        if until<=at or until>self.s.clock()+366*86400:raise ConversationError('PLAN_VALIDITY_REQUIRED')
        import re
        given='\n'.join(m['content'] for m in self.b.store.history(ctx,12) if m['role']=='user' and isinstance(m['content'],str))
        if not re.search(r'\d{1,2}[:：点时]|[一二三四五六七八九十两]+点|\d+\s*(?:分钟|小时)后',given):raise ConversationError('REMINDER_CLOCK_TIME_UNSPECIFIED')
        preview=self.target_text(target)+'\n提醒时间：'+label+'\n有效至：'+display_time(datetime.fromtimestamp(until,__import__('zoneinfo').ZoneInfo('Asia/Shanghai')).isoformat())+'\n按这份固定内容和规则执行；事项或计划暂停、取消、关闭时停止。'
        return target,kind,rule,until,preview

    def schedule_preview(self,ctx,args):
        if args.get('item_id'):self.item(ctx,args['item_id'])
        return self.schedule_data(ctx,args,self.b._current(ctx).get('engine_event_id',self.row(ctx)['id']))[-1]

    def schedule(self,ctx,args,key):
        action=self.b.store.get_action(ctx,key);target,kind,rule,until,preview=self.schedule_data(ctx,args,action['presented_event_id'])
        if preview!=action['preview']:return {'status':'failed','message':'提醒对象或规则已变化，需要重新预览。'}
        if args.get('item_id'):item=self.item(ctx,args['item_id'])
        else:
            p=self.project(ctx,args.get('project_id'));assignees=[ctx.actor_id] if target['kind']=='owner' else [p['id'] for p in target['payload']['people']]
            item=self.am.items.create(ctx.actor_id,key+'/item',project_ref=p['id'],description=args['body'],requirements='',assignees=assignees,source={'project_title':p['title'],'conversation_action':key,'target':target.get('payload',{})})
        plan_target={k:v for k,v in target.items() if k!='body'}
        if target['kind']=='group':plan_target['conversation_text']=True
        plan=self.am.scheduler.create(ctx.actor_id,key+'/plan',item_id=item['id'],kind=kind,rule=rule,target=plan_target,template=args['body'].replace('{','{{').replace('}','}}'),policy='fixed_rule',valid_until=until)
        plan=self.am.scheduler.approve_plan(ctx.actor_id,plan['id'],plan['version'],plan['hash'])
        actual=self.am.scheduler.plan(plan['id'])
        self.s.execute('INSERT OR IGNORE INTO am_conversation_links VALUES(?,?,?,?)',('plan',plan['id'],ctx.session_id,action['presented_event_id']))
        return {'status':'succeeded','verified':actual['state']=='active','receipt_id':plan['id'],'message':'提醒已按所示时间和规则安排；实际发送结果会另行记录。'}

    def list_plans(self,ctx,args,event):
        plans=self.s.rows('SELECT * FROM reminder_plans WHERE owner=? ORDER BY created DESC,rowid DESC LIMIT 30',(ctx.actor_id,))
        result=[]
        for p in plans:
            try:self.plan(ctx,p['id'])
            except Exception:continue
            item={k:p[k] for k in ('id','version','item_id','kind','state','rule','next_at','valid_until','template','created')}
            if item['next_at'] is not None and not __import__('math').isfinite(item['next_at']):item['next_at']=None
            item['schedule_exhausted']=item['next_at'] is None
            item['rule']=json.loads(item['rule'])
            target=json.loads(p['target']);item['target']='本人私聊' if target['kind']=='owner' else target['payload']['group']['name']+'；'+'、'.join(v['name'] for v in target['payload']['people'])
            item['recent_sends']=self.s.rows('SELECT state,readback_state,scheduled_at,error FROM reminder_occurrences WHERE plan_id=? ORDER BY scheduled_at DESC LIMIT 5',(p['id'],))
            result.append(item)
        return {'plans':result,'order':'created_desc'}

    def revised_data(self,ctx,args):
        self.guard(ctx);p=self.plan(ctx,args['plan_id'])
        if p['version']!=args['version']:raise ConversationError('PLAN_VERSION_CHANGED')
        if self.s.rows("SELECT 1 FROM reminder_occurrences WHERE plan_id=? AND (state IN ('sending','unknown') OR state='sent' AND readback_state!='verified')",(p['id'],)):raise ConversationError('PLAN_SEND_RESULT_UNRESOLVED')
        rule=json.loads(p['rule']);until=absolute(args['until']) if args.get('until') else p['valid_until']
        if args.get('at'):
            if p['kind']=='once':
                rule['at']=absolute(args['at']);until=absolute(args['until']) if args.get('until') else rule['at']+3600
            else:rule['time']=args['at']
        next_at=rule['at'] if p['kind']=='once' else self.am.scheduler.next_time(p['kind'],rule,self.s.clock()-1)
        if next_at<=self.s.clock() or not next_at<until<=self.s.clock()+366*86400:raise ConversationError('PLAN_VALIDITY_REQUIRED')
        target=json.loads(p['target']);body=args.get('body') or self.am.scheduler.render(p,self.item(ctx,p['item_id']))['body']
        target=dict(target,body=body)
        if target['kind']=='group':self.port.refresh(target['payload'])
        label=datetime.fromtimestamp(next_at,__import__('zoneinfo').ZoneInfo('Asia/Shanghai')).isoformat()
        preview='准备修订现有提醒（当前第'+str(p['version'])+'版）：\n'+self.target_text(target)+'\n下次时间：'+display_time(label)+'\n有效至：'+display_time(datetime.fromtimestamp(until,__import__('zoneinfo').ZoneInfo('Asia/Shanghai')).isoformat())+'\n其余规则及对象保持；确认后旧版本的未发实例失效。'
        return p,rule,until,preview

    def revise_preview(self,ctx,args):return self.revised_data(ctx,args)[-1]
    def revise_plan(self,ctx,args,key):
        p,rule,until,preview=self.revised_data(ctx,args)
        if preview!=self.b.store.get_action(ctx,key)['preview']:return {'status':'failed'}
        p=self.am.scheduler.revise(ctx.actor_id,key,p['id'],args['version'],template=args['body'].replace('{','{{').replace('}','}}') if args.get('body') else None,rule=rule,valid_until=until)
        p=self.am.scheduler.approve_plan(ctx.actor_id,p['id'],p['version'],p['hash'])
        return {'status':'succeeded','verified':self.am.scheduler.plan(p['id'])['state']=='active','receipt_id':p['id'],'message':'提醒已按所示新版本更新；旧版本未发送的实例已失效。'}

    def control_plan(self,ctx,args,event):
        self.guard(ctx);p=self.plan(ctx,args['plan_id'])
        changed=self.am.scheduler.control(ctx.actor_id,self.operation(ctx,event,args),p['id'],args['version'],args['operation'])
        return {'plan_id':p['id'],'version':changed['version'],'state':changed['state'],'message':'计划已暂停。' if changed['state']=='paused' else '计划已取消；已经发出的消息不因此撤回。'}

    def register(self):
        r=self.b.registry
        r.register(Tool('list_targets','核实原已批准的目标群和群成员；使用返回的稳定ID，重名不能猜。本人私聊可用target=self。',obj({'group_query':string(200),'person_query':string(100)}),self.targets,owner_only=True))
        r.register(Tool('create_followup','登记原AM跟进事项；不发送消息。不属于具体项目的本人事项可不填project_id；其他负责人须先核实group_id及assignee_ids。',obj({'project_id':string(256),'description':string(2000),'requirements':string(2000),'group_id':string(256),'assignee_ids':{'type':'array','maxItems':10,'items':string(256)},'deadline':string(100),'deadline_kind':{'type':'string','enum':['unspecified','estimate','commitment','requirement','check']},'next_check':string(64)},('description',)),self.create_followup,'local',True,final_reply=True))
        r.register(Tool('change_followup','更新已有跟进的处理、暂停、恢复、取消或下次检查时间；先读取准确事项与版本。',obj({'item_id':string(128),'version':{'type':'integer','minimum':1},'operation':{'type':'string','enum':['processing','pause','resume','cancel','check']},'next_check':string(64)},('item_id','version','operation')),self.change_followup,'local',True,final_reply=True))
        r.register(Tool('close_followup','根据明确核验依据准备关闭一个跟进事项；完整预览后由本人确认，不能把“好了”直接当验收。',obj({'item_id':string(128),'version':{'type':'integer','minimum':1},'evidence':string(2000)},('item_id','version','evidence')),self.close_followup,'external',True,self.close_preview))
        r.register(Tool('submit_followup_feedback','本人被指派事项的反馈。content逐字引用本条用户输入；提交材料仅记为待核验，不关闭事项。',obj({'project_id':string(256),'item_id':string(128),'version':{'type':'integer','minimum':1},'kind':{'type':'string','enum':['received','processing','eta','help','submitted','not_mine']},'content':string(3000)},('item_id','version','kind','content')),self.feedback,'local',final_reply=True))
        target_fields={'project_id':string(256),'target':{'type':'string','enum':['self','group']},'group_id':string(256),'recipient_ids':{'type':'array','maxItems':10,'items':string(256)},'body':string(4000)}
        r.register(Tool('send_message','准备立即发送的完整文字预览，不会直接发送。群目标及@成员必须先核实；未来时刻使用schedule_reminder。',obj(target_fields,('target','body')),self.send,'external',True,self.send_preview))
        r.register(Tool('schedule_reminder','准备定时提醒完整预览，确认后调用原调度器。一次提醒at为带时区绝对ISO日期；周期为HH:MM并需until截止；缺明确时刻先补问。',obj(dict(target_fields,item_id=string(128),frequency={'type':'string','enum':['once','daily','workdays']},at=string(64),until=string(64)),('target','body','frequency','at')),self.schedule,'external',True,self.schedule_preview))
        r.register(Tool('list_reminders','查询本人原AM提醒计划和状态；已安排、已发送与事项完成分别表达。',obj({}),self.list_plans,owner_only=True))
        r.register(Tool('control_reminder','暂停或取消已核实版本的提醒计划；不撤回已发送消息。',obj({'plan_id':string(128),'version':{'type':'integer','minimum':1},'operation':{'type':'string','enum':['pause','cancel']}},('plan_id','version','operation')),self.control_plan,'local',True,final_reply=True))
        r.register(Tool('revise_reminder','修改已有未来提醒的时间或正文并完整预览；确认后原版本失效。一次提醒at是带时区ISO日期，周期提醒是HH:MM。未提供的字段保持。',obj({'plan_id':string(128),'version':{'type':'integer','minimum':1},'at':string(64),'until':string(64),'body':string(4000)},('plan_id','version')),self.revise_plan,'external',True,self.revise_preview))
        r.register(Tool('reconcile_send','仅核对本会话已开始发送动作的真实结果；不重新发送。',obj({'action_id':string(128)},('action_id',)),self.reconcile_send,owner_only=True))
