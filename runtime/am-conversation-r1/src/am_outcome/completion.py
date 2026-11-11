"""Host-owned obligations. Unknown language never proves complete coverage."""
import re
from .types import fingerprint

_CREATE = r'(?:登记|建立|创建|新建|建成|建|记录|记下|保存|记)'
_ACTIONS = ((r'(?:提醒我|提醒本人|安排提醒|建立提醒|创建提醒)', 'schedule_reminder'),
            (r'(?:发送|发给|通知)', 'send_message'),
            (_CREATE+r'.*?跟进', 'create_followup'),
            (_CREATE+r'.*?笔记', 'save_work_note'))
_UNRESOLVED = re.compile(r'如果|要是|除外|除了|不要|不用|无需|不需要|先别|暂不|取消|全部|所有|以及其他|等等')

def normalize(value):
    return re.sub(r'[\s，,。；;：:、！!？?]+', '', str(value))


def resolve_goals(text, catalog):
    reconciliation=re.fullmatch(r'(?:请)?(?:核对|对账)(?:发送|消息|通知)?(?:结果|状态)?\s*(action_[0-9a-f]{32})[。？?\s]*',text)
    if reconciliation:
        return [{'step_id':fingerprint([text,'reconcile'])[:24],'operation':'reconcile_send','action_id':reconciliation[1],
                 'project_id':None,'request_spans':[text],'matter':'','depends_on':[],'status':'pending'}],True
    if not re.search(_CREATE+r'.*(?:跟进|笔记)',text) and re.search(r'是否|有没有|了没有|了吗|到底|查询|查一下|列出|读取|[？?]',text):return [],False
    if not any(re.search(pattern,text) for pattern,_ in _ACTIONS):return [],False
    targets = []
    for project in catalog:
        ident = project.get('id', project.get('key'))
        aliases = [project.get('title'), project.get('business_id'), *(project.get('aliases') or [])]
        hits = [a for a in aliases if isinstance(a, str) and a and re.search(re.escape(a)+(r'(?!\d)' if a[-1].isdigit() else ''),text,re.I)]
        if hits and ident: targets.append((ident,hits))
    owners = {}
    for ident,hits in targets:
        for hit in hits: owners.setdefault(hit.casefold(),set()).add(ident)
    if not targets and re.search(r'提醒(?:我|本人|自己)',text):
        targets=[(None,['本人'])]
    if not targets or any(len(v)>1 for v in owners.values()): return [],False
    # A semicolon denotes a separate obligation, including on the same project.
    clauses = [x.strip() for x in re.split(r'[；;\n。]|[，,](?=(?:并|再|然后|明天|后天|请|同时|还要|帮我)*(?:提醒|发送|通知|保存|登记|建立|创建|记录))',text) if x.strip()]
    goals=[]; inherited=None; complete=not bool(_UNRESOLVED.search(text))
    if re.search(r'并且|然后|同时|另外|还要|还需|再帮|并给|再给',text):complete=False
    for index,clause in enumerate(clauses):
        found=[(m.start(),m.end(),op) for pattern,op in _ACTIONS if (m:=re.search(pattern,clause))]
        # More than one distinct operation in a clause is not silently collapsed.
        found.sort()
        if len(found)>1:
            # Creation text may contain a trailing reminder, split at that obligation.
            start=found[1][0]
            if start<=found[0][1]:
                # Overlapping noun phrases cannot make recursive progress. Keep
                # the ENTIRE unresolved obligation instead of losing its tail.
                goals.append({'step_id':fingerprint([index,clause,'unresolved'])[:24],
                    'operation':'unresolved','project_id':None,'request_spans':[clause],
                    'matter':clause,'depends_on':[],'status':'pending'})
                complete=False
                continue
            sub,ok=resolve_goals(clause[:start]+'；'+clause[start:],catalog)
            for item in sub:item['step_id']=fingerprint([index,item['step_id']])[:24]
            goals.extend(sub);complete &= ok;continue
        if found:
            _,end,op=found[0];inherited=op
            detail=clause[end:].lstrip(' ：:，,')
        elif inherited and len(clauses)>1 and index>0:
            op=inherited;detail=clause
            if re.search(r'提醒|发送|通知|采用|发布|挂载|批准|取消|暂停|恢复|笔记|跟进',clause):
                op='unresolved';complete=False
        else:
            op='unresolved';detail=clause;complete=False
        local=[(ident,hits) for ident,hits in targets if any(h.casefold() in clause.casefold() for h in hits)] or (targets if len(targets)==1 else [])
        if not local:complete=False;local=[(None,[clause])]
        if len(local)>1 and not re.search('分别|各',clause):complete=False
        for ident,hits in local:
            # Remove only a syntactic object suffix, never the individual matter.
            if detail and normalize(detail) in {normalize(h) for h in hits}:detail=''
            goals.append({'step_id':fingerprint([index,ident,op,clause])[:24], 'operation':op,
                'project_id':ident,'request_spans':[clause],'matter':detail,
                'depends_on':[], 'recipient_mode':'self' if op=='schedule_reminder' and re.search('提醒(?:我|本人|自己)',clause) else None, 'status':'pending'})
    for i,g in enumerate(goals):
        if g['operation']=='schedule_reminder':
            g['depends_on']=[p['step_id'] for p in goals[:i] if p['operation']=='create_followup' and p['project_id']==g['project_id']]
    return goals, bool(goals) and complete


def matches_goal(goal,unit, *, require_matter=False):
    if unit.get('goal_step_id') and unit['goal_step_id']!=goal['step_id']:return False
    if unit['operation']!=goal['operation']:return False
    target=unit['target']
    if goal.get('action_id') and target.get('action_id')!=goal['action_id']:return False
    if goal.get('recipient_mode')=='self' and target.get('target')!='self':return False
    if goal.get('project_id') and target.get('project_id')!=goal['project_id']:return False
    matter=normalize(goal.get('matter',''))
    if require_matter and matter and goal['operation'] in ('create_followup','save_work_note'):
        actual={normalize(target[k]) for k in ('title','description','body','details','content') if target.get(k)}
        if matter not in actual:return False
    return True


def creation_receipt_key(unit):
    """A native created object is one receipt even across argument/call aliases."""
    if unit['operation'] not in ('create_followup','save_work_note','schedule_reminder'):return None
    receipt=unit.get('receipt')
    ident=unit.get('object_id') or (receipt.get('object_id') if isinstance(receipt,dict) else receipt)
    return (unit['object_type'],str(ident)) if ident else None


def check_goals(goals, complete_set, units):
    results=[];consumed=set();receipts=set()
    for goal in goals:
        peers=sum(g['operation']==goal['operation'] and g.get('project_id')==goal.get('project_id') for g in goals)
        matches=[u for u in units if u['id'] not in consumed and matches_goal(goal,u,require_matter=bool(goal.get('matter')))]
        matched=next((u for u in matches if u['completion']=='fulfilled' and (creation_receipt_key(u) is None or creation_receipt_key(u) not in receipts)),None)
        observed=matched or next(iter(matches),None)
        if observed:consumed.add(observed['id'])
        if matched and creation_receipt_key(matched):receipts.add(creation_receipt_key(matched))
        state=('fulfilled' if matched else ('blocked' if observed and (observed['status']=='unknown' or observed['completion']=='fulfilled') else observed['completion']) if observed else 'pending')
        results.append(dict(goal,completion=state,receipt_refs=[matched['id']] if matched else [],
                            observed_refs=[observed['id']] if observed else []))
    done=sum(r['completion']=='fulfilled' for r in results)
    unexpected=any(u['id'] not in consumed and u.get('effect','read')!='read' and u['completion']=='fulfilled' for u in units)
    state=('fulfilled' if complete_set and results and done==len(results) and not unexpected else 'partial' if done or units else 'blocked')
    return results,state
