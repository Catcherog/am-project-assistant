"""Evidence-first presentation. No generated wording replaces source claims.

A field is the smallest trustworthy block for the current free-form ledger.
We deliberately do not split sentences or specification headings: conditions
may occur later in that field. Oversize blocks remain available in detail.
"""
from dataclasses import dataclass, asdict
import re
from query_rules import FIELDS
from fs_agent.project_catalog import digest

ALIASES = {**{v:k for k,v in FIELDS.items()}, 'dates':'schedule', 'todos':'todo'}
CONDITION = re.compile(r'前提|条件|仍待|尚未|未验收|未核|不能|须|必须|仅|除外|待.*到货')
# Positive, deliberately small grammar. Arbitrary prose after a name is NOT
# evidence that a row stands alone. A full field remains the fallback unit.
ROLE = r'(?:职责[0-9]+|项目经理|项目负责人|负责人|硬件|软件|结构|测试|质量|采购|产品|研发)(?:工程师)?'
PERSON = r'AR[0-9]+[\u4e00-\u9fff]{2,12}'
LOCAL_DUTY = r'，负责本项目(?:第[0-9]+类)?(?:资料核对|硬件设计|软件开发|结构设计|测试验证|质量检查)(?:，不代表专业验收)?[。]?'
INDEPENDENT_ROLE = re.compile(ROLE+r'[：:]'+PERSON+r'(?:[、；]'+PERSON+r')*(?:'+LOCAL_DUTY+r')?')

@dataclass(frozen=True)
class SourceBlock:
    id: str
    text: str
    subject: str
    field: str
    span: tuple
    parent_heading: str
    scope: str
    source_revision: str | None
    read_status: str

@dataclass(frozen=True)
class FactUnit:
    subject: str
    source_block_ids: tuple
    claim_kind: str
    status: str | None
    date_type: str | None
    date_value: str | None
    owner: str | None
    qualifiers: tuple
    negation: tuple
    scope: str
    source_revision: str | None
    read_status: str

def normalize_fields(fields):
    if fields is None:return ['progress']
    result=[]
    for name in fields:
        key=ALIASES.get(name,name)
        if key not in FIELDS:raise ValueError('FIELD_NOT_SUPPORTED')
        if key not in result:result.append(key)
    return result

def source_blocks(project, fields):
    blocks=[]
    for key in normalize_fields(fields):
        value=project.get(FIELDS[key],'').strip()
        if not value:continue
        subject=project['key']
        spans=[(0,len(value),value)]
        lines=[m for m in re.finditer(r'[^\n]+',value) if m[0].strip()]
        # Every line must be wholly covered by the positive local-role grammar.
        # One unparsed/global sentence keeps ALL rows in the original block.
        if key=='people' and len(value)>200 and len(lines)>1 and all(
                INDEPENDENT_ROLE.fullmatch(m[0].strip()) for m in lines):
            spans=[(m.start(),m.end(),m[0]) for m in lines]
        for start,end,part in spans:
            blocks.append(SourceBlock(digest([subject,key,project.get('source_revision'),start,end,part]),part,subject,key,
                (start,end),project['title'],project['title'],project.get('source_revision'),project.get('read_status','complete')))
    return blocks

def fact(block):
    # Do not infer dates/status/owners from an arbitrary string. The extracted
    # claim carries its complete literal qualifiers and negation as one unit.
    return FactUnit(block.subject,(block.id,),'source_extract',None,None,None,None,
        (block.text,) if CONDITION.search(block.text) else (),
        (block.text,) if re.search(r'未|不|无|没|否',block.text) else (),
        block.scope,block.source_revision,block.read_status)

def select_blocks(project,fields,budget=200):
    keys=normalize_fields(fields)
    blocks=source_blocks(project,keys)
    # Date/todo statements can depend on a separate current-progress condition.
    # Include the dependency whole, or decline the short claim altogether.
    progress=project.get('项目进展','').strip()
    # Free-form progress can cancel/qualify another field without any known
    # keyword. Always carry it whole, or decline the short claim on budget.
    if set(keys)&{'schedule','todo'} and 'progress' not in keys and progress:
        if not all(progress in b.text for b in blocks):blocks+=source_blocks(project,['progress'])
    if any(b.read_status!='complete' for b in blocks):return [],'读取不完整，暂不能据此判断；请查看来源核对。'
    if len(blocks)>3 or sum(len(b.text)+len(FIELDS[b.field])+3 for b in blocks)>budget:
        if keys==['people'] and len(blocks)>1:
            selected=[];used=0
            for block in blocks:
                size=len(block.text)+len(FIELDS[block.field])+3
                if len(selected)==3 or used+size>budget:break
                selected.append(block);used+=size
            if selected:return selected,None
        return [],'已找到记录，内容包含多个范围或前提；请查看原文核对，或缩小问题。'
    return blocks,None

def project_data(project,fields=None,*,budget=200,detail=False):
    keys=normalize_fields(fields)
    if detail:blocks,limitation=source_blocks(project,keys),None
    else:blocks,limitation=select_blocks(project,keys,budget)
    return {'subject':project['key'],'title':project['title'],'fields':keys,
            'blocks':[asdict(b) for b in blocks],'facts':[asdict(fact(b)) for b in blocks],
            'omitted_blocks':max(0,len(source_blocks(project,keys))-len(blocks)) if blocks else 0,
            'empty_fields':[FIELDS[k] for k in keys if not project.get(FIELDS[k],'').strip()],
            'limitation':limitation,'source_revision':project.get('source_revision'),
            'read_status':project.get('read_status','complete'),'labels':dict(FIELDS,progress=project.get('progress_heading','项目进展'))}

def render_data(data):
    lines=[data['title']]
    if data['limitation']:
        lines.append('本次查询：'+'、'.join(FIELDS[k] for k in data['fields']))
        lines.append(data['limitation'])
    else:
        lines += [data.get('labels',FIELDS)[b['field']]+'（台账原文）：'+b['text'] for b in data['blocks']]
        lines += [field+'：台账未记录。' for field in data['empty_fields']]
        if data.get('omitted_blocks'):lines.append(f"另有 {data['omitted_blocks']} 条完整职责，可查看原文。")
    return '\n'.join(lines)

def validate_selection(ids,blocks,subject):
    available={b.id:b for b in blocks}
    if not ids or len(ids)!=len(set(ids)) or any(i not in available or available[i].subject!=subject for i in ids):
        raise ValueError('SOURCE_BLOCK_BINDING_INVALID')
    return [available[i] for i in ids]
