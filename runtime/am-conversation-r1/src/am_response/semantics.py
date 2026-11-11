"""Executable, conservative Chinese proposition checker. Unknown is not pass."""
import re

PROFILE = 'bounded-zh/3.1'
# These are lexical equivalences inside an ordered proposition, not bags of words.
_EQUIVALENTS = (('还没有','尚未'),('还没','尚未'),('暂时没有','尚无'),('预估','预计'),
               ('估计会','预计'),('已经','已'),('已完成了','已完成'),('尚未完成','未完成'))

def proposition(text):
    text=str(text).strip()
    text=re.sub(r'^(?:目前|当前|资料显示|记录显示|根据当前资料|根据记录)[，,：:\s]*','',text)
    for before,after in _EQUIVALENTS:text=text.replace(before,after)
    text=text.replace('，',',').replace('：',':')
    return re.sub(r'[\s。；;！？!?“”"]+','',text)


def atoms(text):
    # Split only sentence/semicolon boundaries: polarity/role clauses stay bound.
    return [proposition(x) for x in re.split(r'[。；;\n]',str(text)) if proposition(x)]


def constrained_value(claim):
    """Metadata constrains the proposition; a source prefix is not a qualifier."""
    value=claim['value']
    if not isinstance(value,(str,int,float)):return None,[]
    value=str(value);constraints=[]
    modality=claim.get('modality')
    modal={'forecast':'预计','prediction':'预计','recommendation':'建议','suggestion':'建议','inference':'可能'}.get(modality)
    if modal:
        constraints.append(modal)
        if modal not in proposition(value):value=modal+value
    quote=claim.get('source',{}).get('quote','') if isinstance(claim.get('source'),dict) else ''
    # An evidence quote may carry qualifiers absent from the typed value. We
    # cannot silently discard them or infer that metadata supersedes them.
    for token in ('预计','尚未','未完成','未确认','待核','可能','建议','仅','如果','未验收','未','不','没有','尚无','除非'):
        if token in proposition(quote) or token in proposition(value):constraints.append(token)
    for key in ('time','time_scope','scope'):
        entry=claim.get(key)
        if entry not in (None,''):
            if not isinstance(entry,str):return None,[]
            constraints.append(proposition(entry))
    polarity=claim.get('polarity')
    if polarity in ('negative','negated',False):
        if not re.search(r'不|未|无|没有',value):return None,[]
    elif polarity not in (None,'positive','affirmative',True):return None,[]
    states={'pending':'待确认','awaiting_approval':'待批准','submitted':'已提交','approved':'已批准',
            'accepted':'已验收','cancelled':'已取消','unknown':'待核','completed':'已完成'}
    if claim.get('business_state') in states:constraints.append(states[claim['business_state']])
    return value,list(dict.fromkeys(constraints))


def claim_forms(claim,value):
    """Ordered, whole propositions plus bounded subject-role-value relations."""
    predicate=str(claim.get('predicate',''));subject=claim.get('subject_label')
    forms=[atoms(value),atoms(predicate+'：'+value)]
    # Single explicit role only; never parse keyword co-occurrence in a paragraph.
    role=None;person=None
    if predicate in ('人员与职责','responsibilities'):
        match=re.fullmatch(r'([^：:\n。；;]+负责人)[：:]\s*(AR\d+[^\n。；;，,:：]+)[。]?',value.strip())
        if match:role,person=match.groups()
    elif predicate.endswith('负责人') and re.fullmatch(r'AR\d+[^\n。；;，,:：]+',value.strip()):
        role,person=predicate,value.strip()
    if role:
        for prefix in ('',str(subject)+'的',str(subject)+'项目的') if subject else ('',):
            forms.extend([atoms(prefix+role+'是'+person),atoms(prefix+role+'为'+person)])
    if subject and predicate in ('项目进展','进展','progress','forecast'):
        for prefix in (str(subject)+'的进展',str(subject)+'项目进展'):
            forms.extend([atoms(prefix+link+value) for link in ('为','是','：','为：','是：')])
        # Subject attachment preserves the entire typed predicate/value; no
        # token bag or inferred relation is used (e.g. 项目A的测试尚未完成).
        forms.extend([atoms(str(subject)+link+value) for link in ('','的')])
    return [f for f in forms if f]


def source_text(claim):
    """Canonical fallback keeps typed qualifiers even after candidate rejection."""
    from am_outcome.types import encode
    value,constraints=constrained_value(claim)
    if value is None:value=claim['value'] if isinstance(claim['value'],str) else encode(claim['value'])
    quote=claim.get('source',{}).get('quote') if isinstance(claim.get('source'),dict) else None
    parts=[value]
    if quote and any(c not in proposition(value) for c in constraints):parts.append('证据原文：'+str(quote))
    labels={'negative':'否定','negated':'否定','positive':'肯定','affirmative':'肯定',
            'reported':'来源记录','unconfirmed':'尚未核实','confirmed':'已确认','verified':'已核实',
            'pending':'待确认','awaiting_approval':'待批准','submitted':'已提交','approved':'已批准',
            'accepted':'已验收','cancelled':'已取消','unknown':'待核','completed':'已完成'}
    for key,label in (('polarity','极性'),('time','时间'),('time_scope','时间范围'),('scope','适用范围'),('business_state','记录状态')):
        if claim.get(key) not in (None,''):parts.append(label+'：'+labels.get(str(claim[key]),str(claim[key])))
    return '\n'.join(parts)


class SemanticAssessor:
    profile_version=PROFILE
    def __call__(self,plan,envelope):
        data=envelope.data;claims={c['id']:c for c in data['claims']};steps={s['id']:s for s in data['allowed_next_steps']}
        checks=[];verbatim=True;covered=set()
        for segment in plan['segments']:
            if segment['kind']!='text':continue
            text=segment['text'];refs=[claims[i] for i in segment['claim_ids']]
            intent=segment['intent']
            if intent in ('question','recommendation'):
                choices=[steps[i] for i in segment.get('next_step_ids',[]) if i in steps]
                matched=any(s['intent']==intent and (proposition(text) in [proposition(t) for t in s['permitted_utterances']] or intent=='question' and question_matches(text,s)) for s in choices)
                if matched and not any(proposition(text)==proposition(s['text']) for s in choices):verbatim=False
                checks.append({'decision':('reject' if refs else 'pass' if matched else 'indeterminate'),'rule':'host_allowed_next_step','covered_claim_ids':[]})
                continue
            if not refs:
                checks.append({'decision':'reject','rule':'unreferenced_statement'});continue
            values=[constrained_value(c) for c in refs]
            raw=lambda x:re.sub(r'[\s。；;，,:：]','',str(x))
            body=re.sub(r'^(?:目前|当前)?(?:资料显示|记录显示|根据当前资料|根据记录|资料记载|台账记载|台账显示)[，,：:\s]*','',text)
            segment_verbatim=any(raw(body)==raw(value) or raw(body)==raw(str(c.get('predicate',''))+str(value))
                                for c,(value,_) in zip(refs,values) if value is not None)
            verbatim=verbatim and segment_verbatim
            actual=atoms(text)
            forms=[[form for form in claim_forms(c,value) if all(token in '；'.join(form) for token in constraints)]
                   if value is not None else [] for c,(value,constraints) in zip(refs,values)]
            permitted={a for group in forms for form in group for a in form}
            missing=any(token not in proposition(text) for _,constraints in values for token in constraints)
            if missing:
                decision,rule='reject','semantic_constraint_missing'
            elif any(v is None for v,_ in values):
                decision,rule='indeterminate','unsupported_typed_constraint'
            elif actual and all(a in permitted for a in actual):
                decision,rule='pass','ordered_source_propositions'
                if len({c.get('subject') for c in refs})>1:
                    decision,rule='indeterminate','multiple_subject_binding_unproven'
                elif any(not any(set(form).issubset(set(actual)) for form in group) for group in forms):
                    decision,rule='reject','referenced_claim_partially_expressed'
                attribution=bool(re.match(r'^(?:目前|当前)?(?:资料显示|记录显示|根据当前资料|根据记录|资料记载|台账记载|台账显示)',text))
                unconfirmed=any(c.get('modality') in ('recorded','reported','unknown') or c.get('business_state') not in ('confirmed','verified') for c in refs)
                if decision=='pass' and not segment_verbatim and unconfirmed and not attribution:
                    decision,rule='indeterminate','source_attribution_required'
            else:
                # A clear change to an explicit temporal/modal/negative qualifier is rejected.
                source='；'.join(str(c['value']) for c in refs)
                altered=any(token in source and token not in text for token in ('预计','尚未','未确认','待核','可能','建议','仅','如果'))
                decision,rule=('reject','qualifier_or_scope_changed') if altered else ('indeterminate','unsupported_proposition')
            expressed=[c['id'] for c in refs] if decision=='pass' else []
            covered.update(expressed)
            checks.append({'decision':decision,'rule':rule,'render_as_source_quote':segment_verbatim,'covered_claim_ids':expressed})
        decision='reject' if any(c['decision']=='reject' for c in checks) else 'indeterminate' if any(c['decision']=='indeterminate' for c in checks) else 'pass'
        return {'profile_version':PROFILE,'decision':decision,'checks':checks,'covered_claim_ids':sorted(covered),'general_semantic_equivalence_proven':False,'expression_class':'source_projection' if verbatim else 'free_text'}


def next_steps(goals, units):
    out=[]
    for goal in goals:
        if goal.get('completion')=='fulfilled':continue
        op=goal.get('operation');span=' '.join(goal.get('request_spans',[]))
        slot=None;question=None
        if op in ('create_followup','save_work_note') and not goal.get('matter'):
            slot='content';question='请补充这条跟进的具体内容。' if op=='create_followup' else '请补充这条笔记的具体内容。'
        elif op=='schedule_reminder' and not re.search(r'\d{1,2}[:点时]|[一二三四五六七八九十两]+点',span):
            slot='clock';question='请补充具体的提醒时间。'
        elif op=='send_message' and not goal.get('matter'):
            slot='message';question='请补充要发送的具体内容。'
        if slot:
            out.append({'id':'ask:'+goal['step_id'],'intent':'question','goal_id':goal['step_id'],
                'missing':slot,'text':question,'date_context':'明天' if '明天' in span else None,
                'permitted_utterances':[question]})
    for u in units:
        if u['status']=='unknown':
            sentence='建议先核对这项操作的结果，核清前不重复办理。'
            out.append({'id':'reconcile:'+u['id'],'intent':'recommendation','operation':'reconcile',
                        'text':sentence,'permitted_utterances':[sentence]})
    return out


def question_matches(text,step):
    """Parse an ask(slot) speech act; no assertions or arbitrary named targets."""
    value=re.sub(r'[\s。？！?!]','',text)
    slots={'content':r'(?:具体内容|具体事项|事项内容|要跟进什么|要记录什么)',
           'clock':r'(?:具体时间|提醒时间|具体的提醒时间|几点|什么时间)',
           'message':r'(?:要发送的具体内容|要发送什么|消息内容)'}
    slot=slots.get(step.get('missing'))
    if slot is None:return False
    pattern=r'(?:请|麻烦|能否|可以)?(?:补充|提供|说明|告诉我|确认)?(?:一下)?(?:这条跟进的|这条笔记的|这项事项的)?'+slot+r'(?:吗|呢)?'
    if re.fullmatch(pattern,value):return True
    return step.get('missing')=='clock' and step.get('date_context')=='明天' and bool(re.fullmatch(r'明天(?:的提醒)?(?:安排在|设为)?(?:几点|什么时间)(?:合适|提醒你)?',value))
