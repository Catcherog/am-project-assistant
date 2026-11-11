"""Load relevant tool schemas without making unavailable powers implicit."""
import re
from .tools import Tool,obj,string

AREAS={
 'followups':{'list_followups','read_followup_status','read_followup_feedback','prepare_followup_message','create_followup','change_followup','close_followup','submit_followup_feedback'},
 'sending':{'list_targets','send_message','reconcile_send','list_pending_actions','revise_pending_action'},
 'reminders':{'list_targets','schedule_reminder','list_reminders','control_reminder','revise_reminder','list_pending_actions','revise_pending_action'},
 'documents':{'read_documents','read_document_delivery','edit_document','publish_document','list_pending_actions','revise_pending_action'},
 'updates':{'submit_project_update','list_project_updates','adopt_project_update'},
 'knowledge':{'search_knowledge','propose_knowledge','list_knowledge_candidates','revise_knowledge'},
 'briefings':{'read_briefing','subscribe_briefing','list_reminders','control_reminder'},
 'information':{'read_information','list_projects','read_project'},
}
PATTERNS={
 'followups':r'跟进|事项|负责人|反馈|回复|回信|回话|催|暂停|关闭|取消|处理中',
 'sending':r'发给|发送|发群|发.*(?:消息|这条|那条)|通知|催|核对.*发送',
 'reminders':r'提醒|闹钟|定时|每天|工作日|改.*时间|暂停|取消',
 'documents':r'文稿|稿件|文档|章程|生成|撰写|整理.*报告|写.*报告|新建.*报告|核对报告|采用.*(?:文字|稿)|预检|挂载|挂到|交付目标|云端|这版',
 'updates':r'更新.*进展|同步.*进展|提交.*进展|台账|更正|提案|项目反馈',
 'knowledge':r'知识|规则|以后|习惯|原则|教你|改.*写法|学习',
 'briefings':r'简报|订阅|哪些.*变化|项目变化|汇总',
 'information':r'最新|消息|审批|新反馈|信息来源|资料来源|同步信息',
}
BASE={'list_projects','read_project','read_information','search_knowledge','list_work_notes','save_work_note','update_work_note','record_report','list_followups','discover_capabilities'}

def install(bridge):
    def discover(ctx,args,event):
        current=bridge._current(ctx);area=args['area']
        current.setdefault('expanded_tools',set()).update(AREAS[area])
        return {'area':area,'available_tools':[{'name':t.name,'purpose':t.description} for t in bridge.registry._tools.values() if t.name in AREAS[area] and (ctx.is_owner or not t.owner_only)]}
    bridge.registry.register(Tool('discover_capabilities','按需取得现有业务工具。可用类别：followups跟进、sending发送、reminders提醒、documents文稿、updates进展提案、knowledge教学、briefings简报、information信息来源；未看到对应工具时先查询，不编造能力。',obj({'area':{'type':'string','enum':list(AREAS)}},('area',)),discover))
    def choose(ctx,tools):
        try:current=bridge._current(ctx)
        except Exception:return [t for t in tools if t.name in BASE]
        names=set(BASE)|current.get('expanded_tools',set());text=current['row']['body']
        native=getattr(bridge,'native_followup_tools',None)
        if native is not None and ctx.is_owner and bridge.s.rows(
                'SELECT 1 FROM am_followup_text_choices WHERE scope=? AND expires>?',
                (bridge.service.scope(current['row']),bridge.s.clock())):
            names.update(AREAS['followups'])
        for area,pattern in PATTERNS.items():
            if re.search(pattern,text):names.update(AREAS[area])
        # If a request spans several areas, retain a bounded catalog. Discovery
        # can load another area on the next loop without changing capabilities.
        return [t for t in tools if t.name in names][:32]
    bridge.registry.selector=choose

def information(bridge,ctx,args,event):
    """Bounded projection of the already authorized InformationService result.

    Status collections are never replaced by a guess from individual facts.
    Existing ``section``/``offset``/``limit`` arguments also page @conflicts,
    @incomplete_reasons, @pending_confirmation and @effective. Oversized items
    are explicitly omitted, never clipped into a different business assertion.
    """
    bridge.authorize(ctx,'read',args);host=getattr(bridge.service,'information',None)
    if host is None:return {'available':False,'message':'当前信息源模块尚未接入，可先查询台账记录。'}
    from copy import deepcopy
    import math
    import time
    from .types import canonical
    row=bridge._current(ctx)['row']
    raw=host.query(row,args['project_id'])
    offset=args.get('offset',0);limit=args.get('limit',6);section=args.get('section','summary')
    names=('conflicts','incomplete_reasons','pending_confirmation','effective')
    status_section=section[1:] if section.startswith('@') and section[1:] in names else None
    selected=[];size=0;projection_omissions=[]

    def window(items,start,count,budget):
        result=[];used=0;omitted=False
        for index,item in enumerate(items[start:start+count],start):
            cost=len(canonical(item))
            if used+cost>budget:
                if result:break
                # Consume one oversized item rather than returning a stuck
                # cursor. A dedicated status view has a larger budget.
                item={'details_omitted':True,'item_index':index,
                      'note':'本项超过本次展开范围，未截断正文；可单独读取此状态页或查看对应事实来源。'}
                omitted=True
                if isinstance(items[index],dict):
                    for key in ('field','kind','object_id'):
                        if key in items[index] and len(canonical(items[index][key]))<=200:
                            item[key]=deepcopy(items[index][key])
                cost=len(canonical(item))
            result.append(deepcopy(item));used+=cost
        end=start+len(result)
        return result,{'total':len(items),'returned':len(result),'offset':start,
                       'next_offset':end,'has_more':end<len(items),
                       'details_omitted':omitted,
                       'complete':start==0 and end>=len(items) and not omitted}

    status={};pages={}
    for name,budget in zip(names,(700,400,600,1000)):
        supplied=name in raw
        items=raw.get(name,[])
        if status_section is not None and name!=status_section:
            pages[name]={'available':supplied,'total':len(items),'not_requested':True,
                         'read_with':{'section':'@'+name,'offset':0,'limit':limit}}
            continue
        values,page=window(items,offset if status_section else 0,
                           limit if status_section else len(items),7500 if status_section else budget)
        status[name]=values
        page['available']=supplied
        if not page['complete']:
            page['read_with']={'section':'@'+name,
                'offset':page['offset'] if page['details_omitted'] else page['next_offset'],
                'limit':1 if page['details_omitted'] else limit}
            projection_omissions.append(name)
        pages[name]=page

    if status_section is not None:
        page=pages[status_section]
        return {'available':True,'version':raw['version'],'view':'@'+status_section,
                **status,'status_pages':pages,'has_more':page['has_more'],
                'next_offset':page['next_offset'],'complete_response':page['complete'],
                'projection_omissions':projection_omissions,
                'coverage':{'scope':'selected_status_projection',
                            'needs_refresh_or_review':page['details_omitted'],
                            'global_completeness_claim':False},
                'boundary':'本页仅展开上游当前可读的指定状态集合；未重选有效事实、确认业务状态或清除冲突。'}

    clock=getattr(getattr(host,'store',None),'clock',None)
    now_ms=clock() if callable(clock) else time.time()*1000
    # The conversation/goal stores retain a tool result using the immutable
    # context's lifetime. A shorter source quote must not acquire that longer
    # lifetime merely by appearing in a tool response. Derived values already
    # returned by the source service remain distinct from raw quotations.
    retention_end=getattr(ctx,'content_expires_at',None)
    if type(retention_end) not in (int,float) or not math.isfinite(retention_end):
        retention_end=now_ms/1000+86400
    facts=[f for f in raw['facts'] if not args.get('exclude_ledger') or f.get('field')!='ledger_record']
    if args.get('fact_id'):facts=[f for f in facts if f['fact_id']==args['fact_id']]
    for fact in facts[offset:offset+limit]:
        projected={k:fact[k] for k in ('fact_id','subject_id','field','assertion_type','business_state','status',
                    'authority','protected','stale') if k in fact}
        omitted=[]
        for key in ('polarity','time','time_scope','scope','condition','conditions','attribution','conflict_group'):
            if key not in fact:continue
            if len(canonical(fact[key]))>1000:omitted.append(key)
            else:projected[key]=deepcopy(fact[key])
        evidence=fact.get('evidence',{})
        projected['evidence']={k:deepcopy(v) for k,v in evidence.items() if k in ('source_id','object_id','version','url','occurred_at_ms','observed_at_ms','received_at_ms','locator','completeness','read_status','read_state','time_basis',
                              'raw_available','raw_expires_at_ms','verification') and len(canonical(v))<=1000}
        quote=evidence.get('quote')
        if quote is not None:
            expiry=evidence.get('raw_expires_at_ms')
            reason=None
            if evidence.get('raw_available') is not True:reason='raw_evidence_unavailable'
            elif type(expiry) not in (int,float) or not math.isfinite(expiry):reason='raw_expiry_unverified'
            elif expiry<=now_ms:reason='raw_evidence_expired'
            elif expiry<retention_end*1000:reason='source_retention_shorter_than_conversation'
            elif not isinstance(quote,str) or len(canonical(quote))>1000:reason='quote_not_expanded'
            if reason:projected['evidence']['quote_omitted_reason']=reason
            else:projected['evidence']['quote']=quote
        value=deepcopy(fact.get('value'))
        if section!='summary' and isinstance(value,dict):
            value={section:value[section]} if section in value else {'requested_section_unavailable':section,'available_sections':list(value)}
        elif fact.get('field')=='system_record' and isinstance(value,dict):
            reduced={k:value[k] for k in ('title','summary','status','instance_status','project_code','doc_type') if k in value and len(canonical(value[k]))<=1200}
            if isinstance(value.get('basic'),dict):reduced['basic']={k:v for k,v in value['basic'].items() if k in ('name','status','priority','work_item_type','work_item_name','auto_number')}
            reduced['available_sections']=list(value);reduced['selection_note']='仅为所列字段摘要，其余字段未在本次展开'
            value=reduced
        if omitted:
            # Never expose a bare positive value after dropping its condition,
            # reporter or other qualifier solely to fit the response budget.
            projected.update(qualifiers_omitted=omitted,value_omitted=True,
                note='本项限定信息尚未完整展开，未单独展示结论；请查看对应来源。')
            projected['evidence'].pop('quote',None)
        elif len(canonical(value))>2600:
            projected.update(value_omitted=True,available_sections=list(value) if isinstance(value,dict) else [],note='此条正文较长，未截断为完整事实；请指定字段或查看其来源原文。')
        else:projected['value']=value
        if size+len(canonical(projected))>4200:
            if selected:break
            projected={k:projected[k] for k in ('fact_id','subject_id','field','assertion_type','authority','business_state','status','protected','stale') if k in projected}
            projected.update(value_omitted=True,qualifiers_omitted=True,
                note='本项原文与限定合计过长，未拆开陈述；请缩小字段范围或查看来源。')
            projected['evidence']={k:evidence[k] for k in ('source_id','object_id','version','url','raw_available','raw_expires_at_ms') if k in evidence and len(canonical(evidence[k]))<=400}
        selected.append(projected);size+=len(canonical(projected))
    coverage={k:v for k,v in raw['coverage'].items() if k!='sources' and len(canonical(v))<=1000}
    sources=raw['coverage'].get('sources',[]);coverage['source_count']=len(sources);coverage['sources']=[]
    for source in sources:
        compact={k:v for k,v in source.items() if k in ('source_id','availability','last_checked_ms','stale','unprocessed_jobs','gap_kinds','completeness')}
        if len(canonical(coverage))+len(canonical(compact))>1400:break
        coverage['sources'].append(compact)
    coverage['source_details_omitted']=len(coverage['sources'])<len(sources)
    limited=any(f.get('value_omitted') or f.get('qualifiers_omitted')
                or f.get('evidence',{}).get('quote_omitted_reason')
                or isinstance(f.get('value'),dict) and f['value'].get('selection_note') for f in selected)
    if limited:projection_omissions.append('fact_details')
    if coverage['source_details_omitted']:projection_omissions.append('source_details')
    if projection_omissions:
        # Keep the source's value distinct from projection-related review work.
        coverage['source_needs_refresh_or_review']=coverage.get('needs_refresh_or_review')
        coverage['needs_refresh_or_review']=True
        coverage['projection_omissions']=list(projection_omissions)
    return {'available':True,'version':raw['version'],'coverage':coverage,'facts':selected,
            **status,'status_pages':pages,'has_more':offset+len(selected)<len(facts),
            'next_offset':offset+len(selected),'projection_omissions':projection_omissions,
            'complete_response':offset==0 and len(selected)==len(facts) and not projection_omissions,
            'boundary':'来源报告、预测、已核实事实及冲突分别保留；完整展开不等于业务已确认。状态集合过长时按status_pages给出的section继续读取；原文不可用时不从派生value重建quote。'}
