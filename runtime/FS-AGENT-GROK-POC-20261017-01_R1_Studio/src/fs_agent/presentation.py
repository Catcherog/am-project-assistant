"""Common response contract and measurable display projection, never an approval."""
from dataclasses import dataclass,field,asdict
import re
from .config import BoundaryError

@dataclass
class ResponseEnvelope:
    interaction_id: str
    view_version: int
    subject_refs: list
    task_state: str
    summary: str
    facts: list=field(default_factory=list)
    mandatory_qualifiers: list=field(default_factory=list)
    primary_action: object=None
    secondary_actions: list=field(default_factory=list)
    detail_ref: object=None
    source_revision: object=None
    delivery_state: str='pending'
    exception: str|None=None

    def validate(self):
        if not self.interaction_id or self.view_version<1:raise BoundaryError('VIEW_IDENTITY_REQUIRED')
        for fact in self.facts:
            if fact['subject'] not in self.subject_refs:raise BoundaryError('VIEW_SUBJECT_CHANGED')
        for qualifier in self.mandatory_qualifiers:
            if qualifier not in self.summary:raise BoundaryError('VIEW_QUALIFIER_MISSING')
        if not self.exception and re.search(r'\{"(?:state|id|kind|rule|policy)"\s*:',self.summary):raise BoundaryError('VIEW_INTERNAL_DATA')
        return asdict(self)


def visible_metrics(card):
    texts=[];buttons=[];inputs=[];body_lines=[]
    def walk(node):
        if isinstance(node,dict):
            if node.get('tag') in ('plain_text','lark_md','markdown') and isinstance(node.get('content'),str):texts.append(node['content'])
            if node.get('tag')=='div':body_lines.extend(x for x in node.get('text',{}).get('content','').splitlines() if x.strip())
            if node.get('tag')=='button':buttons.append(node)
            if node.get('tag') in ('input','select_static','multi_select_static','picker_datetime'):inputs.append(node)
            for k,v in node.items():
                if k not in ('value','behaviors','options'):walk(v)
        elif isinstance(node,list):
            for item in node:walk(item)
    walk(card)
    visible='\n'.join(texts)
    visible=re.sub(r'\[([^\]]+)\]\([^)]*\)',r'\1',visible)
    visible=re.sub(r'https?://\S+','来源链接',visible)
    return {'body_point_lines':len(body_lines),'visible_characters':len(visible),'buttons':len(buttons),'primary_buttons':sum(b.get('type')=='primary' for b in buttons),'secondary_buttons':sum(b.get('type')!='primary' for b in buttons),'inputs':len(inputs),'total_controls':len(buttons)+len(inputs)}
