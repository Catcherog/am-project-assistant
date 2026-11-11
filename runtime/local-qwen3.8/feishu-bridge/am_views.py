"""Small business projections and paged detail views over existing services."""
import json
import math
from datetime import datetime
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical,digest
from fs_agent.scheduler import TZ
from group_ledger import render_project,SOURCE,SHEET
from source_views import project_data,render_data

SIZE=5
KINDS={'received':'已收到','processing':'处理中','eta':'预计时间','help':'需要协助','submitted':'提交材料','not_mine':'非本人负责','reported_update':'自报进展，待核对'}
STATES={'waiting':'待反馈','in_progress':'处理中','blocked':'受阻','submitted':'已提交待核验','closed':'已关闭','cancelled':'已取消'}

def stamp(t):return datetime.fromtimestamp(t,TZ).strftime('%Y-%m-%d %H:%M') if t and math.isfinite(t) else '无下一次'
def short(value,limit=140,fallback='内容较长，请打开详情查看完整范围与条件。'):
    return value if len(value)<=limit else fallback

def paging(rows,page,size=SIZE):
    page=max(0,min(int(page),max(0,(len(rows)-1)//size)))
    return rows[page*size:(page+1)*size],page,f'第 {page+1} / {max(1,math.ceil(len(rows)/size))} 页 · 共 {len(rows)} 项'

def page_actions(page,count,base,size=SIZE):
    out=[]
    if page:out.append(('上一页',dict(base,offset=page-1)))
    if (page+1)*size<count:out.append(('下一页',dict(base,offset=page+1)))
    return out

class ViewsMixin:
    def ux_home(self,row):
        return self.ux_projects(row)

    def ux_projects(self,row,*,next='project',query='',page=0,recent=False):
        from group_ledger import select_projects
        if not self.flags['query']:raise BoundaryError('QUERY_NOT_ENABLED')
        if row['user']!=self.owner and next!='project':raise BoundaryError('OWNER_ONLY')
        rows=self.projects(row)
        if recent:
            visits=self.s.rows('SELECT project FROM am_query_recent WHERE scope=? AND expires>? ORDER BY opened DESC,project',(self.scope(row),self.s.clock()))
            by_key={p['key']:p for p in rows};rows=[by_key[v['project']] for v in visits if v['project'] in by_key]
        if query:rows=select_projects(query,rows)
        selected,page,label=paging(rows,page)
        origin={'page':'recent' if recent else 'projects','next':next,'query':query,'offset':page}
        actions=[(p['title'],{'page':next,'project':p['key'],'origin':origin}) for p in selected if next=='project' or p.get('identity_status')=='verified']
        entries=[]
        for p in selected:
            if next!='project' and p.get('identity_status')!='verified':continue
            people=project_data(p,['people'],budget=60)
            raw=p.get('人员与职责','').strip()
            auxiliary=raw if raw and len(raw)<=48 and all(q in raw for f in people['facts'] for q in f['qualifiers']) else '人员：打开查看' if raw else '人员：未记录'
            if p.get('identity_status')!='verified':auxiliary='项目编号待核对 · 可查看原文'
            entries.append({'title':p['title'],'auxiliary':auxiliary})
        actions+=page_actions(page,len(rows),origin)
        actions += [('全部项目' if recent else '最近查看',{'page':'projects' if recent else 'recent'}),('刷新资料',{'page':'refresh'})]
        if query:actions.append(('清空搜索',{'page':'projects'}))
        if row['user']==self.owner:actions += [('我的跟进',{'page':'work'}),('更多',{'page':'more'})]
        meta=row.get('_snapshot_meta',{})
        label+='\n资料读取于 '+stamp(row.get('_observed_at',self.s.clock()))
        if meta.get('stale'):label+='（较早的资料，可刷新）'
        if meta.get('refresh_error'):label+='\n本次刷新未成功，显示上次成功读取的资料。'
        if not rows:label+='\n暂无最近查看记录。' if recent else '\n未找到可查看的匹配项目。'
        return self.ux_page(row,'最近查看' if recent else '项目查询',label,actions,
            fields={'query':{'label':'搜索名称或编号（可不填）','required':False,'default':query}},submit=('搜索',{'page':'search','next':next}),
            data={'origin':origin,'query_view':True,'project_entries':entries})

    def ux_project_detail(self,row,key,section=None,origin=None):
        p=self.ux_project(row,key,readonly=True);self.save_session(row,key,None)
        origin=origin or row.get('_list_origin') or {'page':'projects'}
        row['_list_origin']=origin
        fields={'dates':['schedule'],'people':['people'],'todos':['todo'],'source':[],'full':['progress','schedule','people','todo']}.get(section,['progress'])
        projection=project_data(p,fields,detail=section=='full')
        observed=row.get('_observed_at',self.s.clock())
        text=render_project(p,fields,observed,detail=section=='full')
        if section=='source':text=p['title']+'\n来源：'+SOURCE+'?sheet='+SHEET+'\n资料读取于：'+stamp(observed)+'\n来源日期不代表完成或验收。'
        meta=row.get('_snapshot_meta',{})
        if meta.get('stale') or meta.get('refresh_error'):text='显示上次成功读取的资料；可刷新核对。\n'+text
        if p.get('identity_status')!='verified':text='项目编号待核对；此页仅展示来源记录。\n'+text
        back={'page':'project','project':key,'origin':origin}
        actions=[(label,dict(back,section=part)) for label,part in [('进展',None),('人员','people'),('计划','dates'),('待办','todos')]]
        actions+=[('完整内容',dict(back,section='full')),('返回项目列表',origin),('刷新资料',{'page':'refresh','project':key,'section':section,'origin':origin})]
        if row['user']==self.owner and p.get('identity_status')=='verified':
            if self.flags['scheduler']:actions.append(('发起跟进',{'page':'remind','project':key,'group':self.query_group(row)}))
            if self.flags['progress_collection']:actions.append(('更新进展',{'page':'collect','project':key}))
        return self.ux_page(row,p['title'],text,actions,data={'origin':origin,'query_view':True,'opened_project':key,'subject_refs':[key],
            'facts':projection['facts'],'mandatory_qualifiers':[q for f in projection['facts'] for q in f['qualifiers']] if section!='source' else [],
            'source_revision':projection['source_revision'],'detail':section=='full','section_tabs':True})

    def followup_state(self,item):
        if item['state'] in ('closed','cancelled'):return 'closed',STATES[item['state']]
        occurrences=self.s.rows('SELECT * FROM reminder_occurrences WHERE item_id=? ORDER BY scheduled_at DESC,rowid DESC',(item['id'],))
        if any(o['state'] in ('unknown','sending') for o in occurrences):return 'action','发送结果待核对'
        last=self.s.rows('SELECT id FROM item_feedback WHERE item_id=? AND applied=1 ORDER BY rowid DESC LIMIT 1',(item['id'],))
        seen=self.s.rows('SELECT feedback_id FROM am_followup_seen WHERE item_id=?',(item['id'],))
        if last and (not seen or seen[0]['feedback_id']!=last[0]['id']):return 'action','新反馈待查看'
        if item['state'] in ('blocked','submitted'):return 'action',STATES[item['state']]
        if item['paused']:return 'action','已暂停，待安排'
        if item['next_check'] is not None and item['next_check']<=self.s.clock():return 'action','已到跟进时间'
        plans=self.s.rows("SELECT * FROM reminder_plans WHERE item_id=? AND state='active' AND valid_until>?",(item['id'],self.s.clock()))
        if occurrences and occurrences[0]['state']=='sent':
            latest=json.loads(item['latest_feedback']) if item['latest_feedback'] else {}
            if latest.get('kind')=='reported_update':return 'waiting','已收到自报进展，业务仍待核验'
            return 'waiting','已发送，等待反馈'
        if plans:return 'waiting','已安排，尚待发送'
        if occurrences and occurrences[0]['state'] in ('pending','queued','awaiting_approval'):return 'action','等待发送或确认'
        return 'action','尚待安排下一步'

    def ux_work(self,row,page=0,bucket='action'):
        return self.followup.text_ui.listing(row,page,bucket)

    def ux_item_text(self,item):
        latest=json.loads(item['latest_feedback']) if item['latest_feedback'] else None
        lines=[json.loads(item['source']).get('project_title','项目归属待核对'),short(item['description']),
            '状态：'+STATES[item['state']]+('（暂停）' if item['paused'] else ''),'下次检查：'+stamp(item['next_check'])]
        if latest:lines+=['最新有效反馈：'+KINDS.get(latest['kind'],latest['kind'])+'；'+short(latest['body'],100)]
        lines.append('材料提交仍需核验。' if item['state']!='closed' else '已按核验依据关闭。')
        return '\n'.join(lines)

    def ux_item(self,row,ident):
        return self.followup.text_ui.item(row,ident)

    def ux_feedback_notice(self,row,item,summary,event):
        return self.followup.text_ui.notice(row,item,summary,event)

    def ux_proposal(self,row,p,section=None):
        r=self.updates.collection(p['collection_id']);basis=json.loads(p['basis']);patches=json.loads(p['patches'])
        if row['user']!=self.owner:raise BoundaryError('OWNER_ONLY')
        current=r['version']==p['collection_version'] and r['state']!='withdrawn'
        text=basis['title']+'\n'+('内容已核对' if r['reporter_confirmed'] else '已收集，待核对')+'；正式台账未写入。\n报告人自报，不代表专业验收。'
        actions=[]
        if section=='diff':
            text='\n\n'.join([basis['title']]+[f"字段：{k}\n修改前全文：\n{v['before']}\n修改后全文：\n{v['after']}" for k,v in patches.items()])
        elif section=='source':text='报告人原话与解释（自报待核验）：\n'+'\n'.join(f['original']+'｜'+f['status'] for f in json.loads(r['facts']))
        if current and not r['reporter_confirmed'] and r['reporter']==row['user']:
            actions.append(('确认内容',{'page':'review_content','proposal':p['id'],'version':p['version'],'collection_version':r['version']}))
        actions += [('查看精确差异',{'page':'proposal','proposal':p['id'],'section':'diff'}),('查看原话与解释',{'page':'proposal','proposal':p['id'],'section':'source'}),('继续补充',{'page':'collect','project':p['project_ref']}),('结束收集',{'page':'cancel_collect'})]
        return self.ux_page(row,'进展提案',text,actions,data={'subject_refs':[p['project_ref']],'full_preview':section=='diff'})

    def ux_status(self,row):
        counts=self.s.rows("SELECT state,count(*) n FROM reminder_occurrences WHERE state IN ('unknown','sending') GROUP BY state")
        n=sum(x['n'] for x in counts)
        return self.ux_page(row,'运行状态',f'正式台账写回：未启用。\n发送结果待核对：{n} 项。\n查询、资料收集和提案可分别使用。',[('提醒计划',{'page':'plans'}),('维护诊断',{'page':'diagnostics'}),('返回首页',{'page':'home'})])

    def ux_plan_detail(self,row,ident):
        p=self.scheduler.plan(ident)
        if p['owner']!=row['user']:raise BoundaryError('OWNER_ONLY')
        states={'draft':'待确认','active':'已启用','paused':'已暂停','cancelled':'已取消','expired':'已到期'}
        target=json.loads(p['target']);who='本人私聊' if target['kind']=='owner' else target['payload']['group']['name']
        occurrences=self.s.rows('SELECT * FROM reminder_occurrences WHERE plan_id=? ORDER BY scheduled_at DESC',(ident,))
        lines=['状态：'+states.get(p['state'],p['state']),'目标：'+who,'下一次：'+stamp(p['next_at'])]
        if occurrences:lines.append('最近结果：'+self.ux_occurrence_status(occurrences[0]))
        actions=[]
        if p['state']=='active':actions.append(('暂停计划',{'page':'plan_control','plan':ident,'version':p['version'],'control':'pause'}))
        if p['state']=='paused':actions.append(('恢复并重新预览',{'page':'plan_control','plan':ident,'version':p['version'],'control':'resume'}))
        if p['state']=='draft':actions.append(('查看完整预览',{'page':'plan_preview','plan':ident}))
        actions += [('规则与正文',{'page':'plan_rules','plan':ident}),('发送记录',{'page':'occurrences','plan':ident}),('返回计划',{'page':'plans'})]
        return self.ux_page(row,'提醒计划','\n'.join(lines),actions)

    def ux_run_precheck(self,row,ident):
        logic=self.g.logic;d=logic.get(row,ident);sources=json.loads(d['sources'])
        for source in logic.business.sources(row,json.loads(d['target'])):
            if source['id'] not in {s['id'] for s in sources}:sources.append(source)
        content,report=logic.evaluate_precheck(row,d,sources)
        complete=bool(sources) and all(s['read_status']=='complete' for s in sources)
        text=('本次材料读取完整；此结果为资料检查。' if complete else '材料未完整读取，暂不能判断是否齐备。')
        text+='\n待补 '+str(len(report['缺少']))+' 项；冲突 '+str(len(report['冲突']))+' 项；需专业判断 '+str(len(report['需专业判断']))+' 项。'
        for x in (report['缺少']+report['冲突'])[:2]:text+='\n'+short(x['finding'],95,'该项存在缺口，详见完整报告。')
        fresh=logic.get(row,ident)
        return self.ux_page(row,'资料预检结果',text,[('补充材料',{'page':'precheck_continue','draft':ident}),('完整报告',{'page':'precheck_report','draft':ident,'revision':fresh['revision']}),('结束收集',{'page':'cancel_collect'})],data={'report_snapshot':content,'report_revision':fresh['revision']})

    def ux_extra(self,row,a,values,data):
        page=a['page']
        if page=='feedback_seen':
            item=self.items.get(a['item'])
            if item['owner']!=row['user'] or item['version']!=a['version']:raise BoundaryError('ITEM_VERSION_CHANGED')
            latest=self.s.rows('SELECT id FROM item_feedback WHERE item_id=? AND applied=1 ORDER BY rowid DESC LIMIT 1',(item['id'],))
            if not latest or latest[0]['id']!=a['feedback']:raise BoundaryError('ITEM_VERSION_CHANGED')
            self.s.execute('INSERT OR REPLACE INTO am_followup_seen VALUES(?,?)',(item['id'],a['feedback']))
            return self.ux_item(row,item['id'])
        if page=='long_content':
            from fs_agent.document_view_binding import check_snapshot
            check_snapshot(self.g.logic,row,{'data':data})
            saved=dict(data,_long_index=a['offset']);content=saved['_long_view']
            return self.ux_page(row,content['title'],content['text'],content['actions'],data=saved)
        if page=='briefing_detail':return self.ux_briefing(row,data['briefing_snapshot'],a.get('section'),a.get('offset',0))
        if page=='documents':return self.ux_documents(row,a)
        if page=='project_actions':
            p=self.ux_project(row,a['project']);actions=[]
            origin=a.get('origin') or row.get('_list_origin') or {'page':'projects'}
            if self.flags['scheduler']:actions.append(('创建提醒',{'page':'remind','project':p['key']}))
            if self.flags['progress_collection']:actions.append(('更新进展',{'page':'collect','project':p['key']}))
            return self.ux_page(row,'项目操作',p['title'],actions+[('返回项目',{'page':'project','project':p['key'],'origin':origin})],data={'origin':origin})
        if page=='history':return self.followup.text_ui.history(row,a['item'],a.get('offset',0))
        if page=='item_full':return self.followup.text_ui.item(row,a['item'])
        if page in ('history','item_full','item_actions'):
            item=self.items.get(a['item'])
            if item['owner']!=row['user']:raise BoundaryError('OWNER_ONLY')
            back=[('返回事项',{'page':'item','item':item['id']}),('返回全部跟进',{'page':'work'})]
            if page=='history':
                expiry=self.items.feedback_expiry(item['id'])
                if expiry:row=dict(row,expires=min(row['expires'],expiry))
                rows=list(reversed(self.items.history(item['id'],include_raw=True)));selected,num,label=paging(rows,a.get('offset',0))
                people={p['id']:p['name'] for p in json.loads(item['source']).get('target',{}).get('people',[])}
                lines=[label]+[people.get(h['actor'],'授权反馈人')+'｜'+KINDS[h['kind']]+'｜'+('有效反馈' if h['applied'] else '迟到留证')+'｜'+stamp(h['created'])+'\n'+(h['body'] or '无补充') for h in selected]
                return self.ux_page(row,'反馈记录','\n\n'.join(lines),page_actions(num,len(rows),a)+back,data={'detail':True})
            if page=='item_full':return self.ux_page(row,'完整事项',item['description']+'\n交付要求：'+(item['requirements'] or '待确认')+'\n核验关闭依据：'+(item['close_evidence'] or '尚未核验关闭'),back,data={'detail':True})
            actions=[]
            if item['state'] not in ('closed','cancelled'):
                choices=[('设置检查时间','check'),('恢复跟进','resume') if item['paused'] else ('暂停跟进','pause'),('核验并关闭','close')]
                actions=[(label,{'page':'item_action_form','item':item['id'],'version':item['version'],'control':control}) for label,control in choices]
                if not item['paused']:actions.append(('创建提醒',{'page':'remind','project':item['project_ref'],'item':item['id']}))
            return self.ux_page(row,'跟进操作',STATES[item['state']],actions+back)
        if page=='proposal':return self.ux_proposal(row,self.updates.proposal(a['proposal']),a.get('section'))
        if page=='review_content':
            p=self.updates.proposal(a['proposal']);r=self.updates.collection(p['collection_id'])
            if p['version']!=a['version'] or p['collection_version']!=r['version'] or r['version']!=a['collection_version'] or p['expires']<=self.s.clock():raise BoundaryError('PROPOSAL_VERSION_CHANGED')
            self.updates.confirm_reporter(row['user'],r['id'],r['version'])
            return self.ux_proposal(row,p)
        if page=='precheck_report':
            d=self.g.logic.get(row,a['draft']);content=data.get('report_snapshot')
            if content is None:raise BoundaryError('REPORT_SNAPSHOT_UNAVAILABLE')
            label='完整报告'+('（旧版本）' if d['revision']!=a['revision'] else '')
            return self.ux_page(row,label,content,[('返回当前检查',{'page':'precheck_result','draft':d['id']})],data={'detail':True})
        if page=='precheck_result':return self.ux_run_precheck(row,a['draft'])
        if page=='diagnostics':return self.ux_page(row,'维护诊断',self.handle(dict(row,kind='text',body='/diagnostics')),[('返回状态',{'page':'status'})],data={'detail':True})
        if page=='plan_preview':return self.plan_card(row,self.scheduler.plan(a['plan']))
        if page=='plan_rules':
            p=self.scheduler.plan(a['plan']);text=self.plan_preview_text(p)
            actions=[('返回计划',{'page':'plan','plan':p['id']})]
            if p['state'] not in ('cancelled','expired'):actions.append(('取消计划',{'page':'plan_control','plan':p['id'],'version':p['version'],'control':'cancel'}))
            return self.ux_page(row,'完整规则与正文',text,actions,data={'detail':True})
        if page=='occurrences':
            p=self.scheduler.plan(a['plan'])
            if p['owner']!=row['user']:raise BoundaryError('OWNER_ONLY')
            rows=self.s.rows('SELECT * FROM reminder_occurrences WHERE plan_id=? ORDER BY scheduled_at DESC',(p['id'],));selected,num,label=paging(rows,a.get('offset',0))
            actions=[(self.ux_occurrence_status(o),{'page':'reconcile','occurrence':o['id']}) for o in selected if o['state'] in ('unknown','sending','sent') and o['readback_state']!='verified']
            return self.ux_page(row,'发送记录',label+'\n'+'\n'.join(self.ux_occurrence_status(o) for o in selected),actions+page_actions(num,len(rows),a)+[('返回计划',{'page':'plan','plan':p['id']})])
        raise BoundaryError('CARD_ACTION_NOT_SUPPORTED')
