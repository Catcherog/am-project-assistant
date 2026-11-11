"""Native template copies and minimal rich-text writes, with durable uncertainty journals."""
from __future__ import annotations

import copy
import json
import re
import uuid

from am_doc_engine.models import canonical, fingerprint
from .config import BoundaryError
from .document_sources import TEXT_KEYS, text_of

SCHEMA = '''
CREATE TABLE IF NOT EXISTS amdoc_native_writes(
 idem TEXT PRIMARY KEY,draft_id TEXT NOT NULL,revision INTEGER NOT NULL,content_hash TEXT NOT NULL,
 state TEXT NOT NULL,token TEXT,payload TEXT NOT NULL,receipt TEXT,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS amdoc_native_documents(
 draft_id TEXT PRIMARY KEY,token TEXT NOT NULL,snapshot TEXT NOT NULL,fields TEXT NOT NULL,
 slots TEXT NOT NULL,template_hash TEXT NOT NULL,expires REAL NOT NULL);
'''


def replacements(block,slot,value):
    """Preserve header labels and their inline styles; blank cells keep their style."""
    key=TEXT_KEYS.get(block['block_type'])
    if key is None: raise BoundaryError('DOCUMENT_SLOT_TYPE_CHANGED')
    original=block[key].get('elements',[]); elements=copy.deepcopy(original)
    if any(set(e)-{'text_run'} for e in elements): raise BoundaryError('DOCUMENT_RICH_SLOT_UNSUPPORTED')
    if slot['kind']=='placeholder':
        placeholder=slot['placeholder'];matches=[i for i,e in enumerate(elements) if placeholder in e['text_run']['content']]
        if len(matches)!=1: raise BoundaryError('DOCUMENT_PLACEHOLDER_SPLIT')
        i=matches[0];elements[i]['text_run']['content']=elements[i]['text_run']['content'].replace(placeholder,value,1)
    elif slot['kind']=='append':
        elements.append({'text_run':{'content':value,'text_element_style':{}}})
    else:
        style=original[0].get('text_run',{}).get('text_element_style',{}) if original else {}
        elements=[{'text_run':{'content':value,'text_element_style':style}}]
    updated=copy.deepcopy(block);updated[key]['elements']=elements
    return elements,updated


def normalize(native):
    """Graph identity independent, but retains all content, layout and inline properties."""
    paths=native['paths']
    def walk(v):
        if isinstance(v,dict):
            # Live readback materializes false inline-style defaults. Normalize
            # only these observed defaults; true flags, links and colors stay exact.
            result={}
            for k,x in v.items():
                if k in ('block_id','parent_id'):continue
                if k=='text_element_style' and isinstance(x,dict):
                    x={a:b for a,b in x.items() if not (a in {'bold','inline_code','italic','strikethrough','underline'} and b is False)}
                    if not x:continue
                result[k]=walk(x)
            return result
        if isinstance(v,list): return [walk(x) for x in v]
        if isinstance(v,str) and v in paths:return ['path',*paths[v]]
        return v
    return {tuple(paths[b['block_id']]):walk(b) for b in native['blocks']}


def verify_exact(expected,actual):
    return normalize(expected)==normalize(actual)


def native_values(document,slots):
    """Compact native cells; full gaps and evidence stay in the companion report."""
    labels={s['field_id']:s['label'] for s in slots};values={}
    for field in document['fields']:
        fid=field['field_id'];value=field['text'];status=field.get('status','filled')
        if status=='missing':value='〈待提供〉'
        elif status=='conflict':value='〈待核对：资料冲突〉'
        elif labels.get(fid) in ('项目名称','项目编号') and value.startswith('实际结果：'):
            value=value.removeprefix('实际结果：')
        elif labels.get(fid)=='项目类型' and re.fullmatch(r'待核实反馈（建议方案）：(?:IPD|IRAD|other)',value):
            value=value.rsplit('：',1)[-1]+'（本稿选用，待核对）'
        values[fid]=value
    return values


class NativePublisher:
    def __init__(self,store,docs,*,folder,host):
        self.s,self.docs,self.folder,self.host=store,docs,folder,host
        with store.lock:store.conn.executescript(SCHEMA)

    def saved(self,draft_id):
        rows=self.s.rows('SELECT * FROM amdoc_native_documents WHERE draft_id=?',(draft_id,))
        return rows[0] if rows else None

    def reconcile(self,idem):
        rows=self.s.rows('SELECT * FROM amdoc_native_writes WHERE idem=?',(idem,))
        if not rows:return None
        row=rows[0];payload=json.loads(row['payload'])
        if not row['token']:
            # Copy has no client token. Look up only the exact unique action title in
            # the configured folder; absence is not proof that creation did not occur.
            page=None;seen=set();matches=[]
            for _ in range(20):
                query={'folder_token':self.folder,'page_size':200}
                if page:query['page_token']=page
                data=self.docs.call('GET','/open-apis/drive/v1/files',query=query)
                matches.extend(f for f in data.get('files',[]) if f.get('name')==payload['name'] and f.get('type')=='docx')
                if not data.get('has_more'):break
                page=data.get('next_page_token') or data.get('page_token')
                if not page or page in seen:return None
                seen.add(page)
            else:return None
            if len(matches)!=1:return None
            self.s.execute('UPDATE amdoc_native_writes SET token=?,state=? WHERE idem=?',
                           (matches[0]['token'],'copied_unverified',idem))
            return None  # A reconciled copy is never silently filled on a retry.
        if not payload.get('expected'):return None
        current=self.docs.read(row['token'])
        if not verify_exact(payload['expected'],current):return None
        receipt={'document_id':row['token'],'url':'https://'+self.host+'/docx/'+row['token'],
                 'revision':current['revision'],'content_hash':row['content_hash'],'readback_hash':current['hash']}
        with self.s.tx() as conn:
            conn.execute('UPDATE amdoc_native_writes SET state=?,receipt=? WHERE idem=?',('verified',canonical(receipt),idem))
            latest=conn.execute("SELECT idem FROM amdoc_native_writes WHERE draft_id=? AND state='verified' ORDER BY rowid DESC LIMIT 1",(row['draft_id'],)).fetchone()
            if latest and latest[0]==idem:
                conn.execute('INSERT INTO amdoc_native_documents VALUES(?,?,?,?,?,?,?) ON CONFLICT(draft_id) DO UPDATE SET '
                    'token=excluded.token,snapshot=excluded.snapshot,fields=excluded.fields,slots=excluded.slots,template_hash=excluded.template_hash,expires=MIN(amdoc_native_documents.expires,excluded.expires)',
                    (row['draft_id'],row['token'],canonical(current),canonical(payload['fields']),canonical(payload['slots']),payload['template_hash'],row['expires']))
        return receipt

    @staticmethod
    def action_key(draft,document,slots,selected_fields=None):
        values=native_values(document,slots)
        selected=set(values) if selected_fields is None else set(selected_fields)
        if not selected or selected-set(values):raise BoundaryError('DOCUMENT_UPDATE_FIELDS_INVALID')
        return fingerprint([draft['id'],draft['revision'],draft['content_hash'],sorted(selected)])

    def save(self,draft,document,template,slots,*,selected_fields=None,revalidate=None):
        self.docs.check()
        values=native_values(document,slots)
        selected=set(values) if selected_fields is None else set(selected_fields)
        if not selected or selected-set(values):raise BoundaryError('DOCUMENT_UPDATE_FIELDS_INVALID')
        # Real project data never gets pasted into an unsupported/mismatched slot.
        allowed_gaps={'missing_required','missing_optional','field_missing','field_conflict','required_input_missing','fact_conflict','generation_failed','semantic_review_required'}
        if any(i['severity']=='error' and i['code'] not in allowed_gaps for i in document['issues']):
            raise BoundaryError('DOCUMENT_CHECK_ERRORS_BLOCK_NATIVE_SAVE')
        idem=self.action_key(draft,document,slots,selected_fields)
        old=self.s.rows('SELECT * FROM amdoc_native_writes WHERE idem=?',(idem,))
        if old:
            receipt=self.reconcile(idem)
            return {'state':'verified' if receipt else 'unknown','receipt':receipt,'idempotency_key':idem}
        prior=self.saved(draft['id']);name='AM文稿测试' if not draft.get('title') else draft['title']
        name=name[:55]+'（待审草稿）-'+idem[:12]
        payload={'name':name,'fields':values,'slots':slots,'template_hash':document['template_hash'],'document':document}
        if prior:
            if selected_fields is None:raise BoundaryError('DOCUMENT_EXPLICIT_UPDATE_SCOPE_REQUIRED')
            if prior['template_hash']!=document['template_hash']:raise BoundaryError('DOCUMENT_TEMPLATE_MIGRATION_REQUIRED')
            current=self.docs.read(prior['token']);before=json.loads(prior['snapshot'])
            old_by={b['block_id']:b for b in before['blocks']};now_by={b['block_id']:b for b in current['blocks']}
            slots=json.loads(prior['slots']);payload['slots']=slots
            for slot in slots:
                if slot['field_id'] in selected and now_by.get(slot['block_id'])!=old_by.get(slot['block_id']):
                    raise BoundaryError('DOCUMENT_HUMAN_EDIT_CONFLICT')
            histories=self.s.rows("SELECT payload FROM amdoc_native_writes WHERE draft_id=? AND state='verified' ORDER BY rowid DESC LIMIT 1",(draft['id'],))
            if not histories or revalidate is None:raise BoundaryError('DOCUMENT_REVISION_REVALIDATION_REQUIRED')
            base_document=json.loads(histories[0]['payload'])['document'];human_values={}
            slot_ids={s['block_id'] for s in slots}
            # Structural or fixed-template changes need explicit remapping; never
            # silently treat changed instructions/signature text as the old template.
            if set(old_by)!=set(now_by):raise BoundaryError('DOCUMENT_NATIVE_STRUCTURE_CHANGED')
            if any(now_by[k]!=old_by[k] for k in old_by if k not in slot_ids):
                raise BoundaryError('DOCUMENT_FIXED_CONTENT_CHANGED')
            for slot in slots:
                old=old_by[slot['block_id']];now=now_by[slot['block_id']]
                if now==old:continue
                text=text_of(now);previous=json.loads(prior['fields'])[slot['field_id']]
                old_text=text_of(old)
                if slot['kind'] in ('placeholder','append'):
                    if old_text.count(previous)!=1:raise BoundaryError('DOCUMENT_HUMAN_SLOT_REMAP_REQUIRED')
                    prefix,suffix=old_text.split(previous,1)
                    if not text.startswith(prefix) or (suffix and not text.endswith(suffix)):
                        raise BoundaryError('DOCUMENT_HUMAN_SLOT_REMAP_REQUIRED')
                    text=text[len(prefix):len(text)-len(suffix) if suffix else None]
                human_values[slot['field_id']]=text
            merged=revalidate(base_document,human_values,document,selected)
            if any(i['severity']=='error' and i['code'] not in allowed_gaps for i in merged['issues']):
                raise BoundaryError('DOCUMENT_MERGED_CONTENT_NEEDS_EVIDENCE')
            values=native_values(merged,slots);payload['document']=merged
            # Unselected human edits are preserved byte-for-byte in this snapshot.
            copy_source=current;token=None;payload['fields']=values
            payload['previous_document']=prior['token']
        else:
            token=None;current=None;copy_source=template
            if self.docs.read(template['token'])['hash']!=template['hash']:
                raise BoundaryError('DOCUMENT_TEMPLATE_CHANGED_BEFORE_COPY')
        with self.s.tx() as conn:
            pending=conn.execute("SELECT idem FROM amdoc_native_writes WHERE draft_id=? AND state!='verified'",(draft['id'],)).fetchone()
            if pending:raise BoundaryError('DOCUMENT_UNCERTAIN_ACTION_RECONCILE_FIRST')
            from am_harness.reservation import reserve_effect
            reserve_effect(conn,idem,'native_document',{'revision':draft['revision'],'accepted_hash':draft.get('accepted_hash'),'content_hash':draft['content_hash'],'selected':sorted(selected)})
            conn.execute('INSERT INTO amdoc_native_writes VALUES(?,?,?,?,?,?,?,?,?)',
                (idem,draft['id'],draft['revision'],draft['content_hash'],'copying',token,canonical(payload),None,draft['expires']))
        if not token:
            # The platform's revision parameter is not an atomic compare-and-swap
            # promise. Publish revisions to a copy, preserving the original and
            # any concurrent human edits. Never overwrite an existing user draft.
            data=self.docs.call('POST','/open-apis/drive/v1/files/'+copy_source['token']+'/copy',
                body={'name':name,'type':'docx','folder_token':self.folder})
            file=data.get('file',{});token=file.get('token')
            if not token:raise BoundaryError('DOCUMENT_COPY_OUTCOME_UNKNOWN')
            self.s.execute('UPDATE amdoc_native_writes SET token=?,state=? WHERE idem=?',(token,'copied',idem))
            self.docs.restrict_copy(token)
            current=self.docs.read(token)
            # Copies must preserve the original layout. The page title is the only
            # difference expected before filling. IDs are matched by graph paths.
            template_copy=copy.deepcopy(copy_source);root=template_copy['blocks'][0]
            root['page']=copy.deepcopy(current['blocks'][0]['page'])
            if not verify_exact(template_copy,current):raise BoundaryError('DOCUMENT_COPY_LAYOUT_MISMATCH')
            if self.docs.read(copy_source['token'])['hash']!=copy_source['hash']:
                raise BoundaryError('DOCUMENT_SOURCE_CHANGED_DURING_COPY')
            by_path={tuple(current['paths'][b['block_id']]):b for b in current['blocks']}
            slots=[dict(s,block_id=by_path[tuple(s['path'])]['block_id']) for s in slots]
            payload['slots']=slots
        expected=copy.deepcopy(current);expected_by={b['block_id']:b for b in expected['blocks']};requests=[]
        for slot in slots:
            ident=slot['field_id']
            if ident not in selected:continue
            block=expected_by.get(slot['block_id'])
            if block is None:raise BoundaryError('DOCUMENT_NATIVE_SLOT_REMOVED')
            # For revisions the filled paragraph is the entire previous value; the
            # original header prefix is reconstructed from the approved template slot.
            if prior and slot['kind'] in ('placeholder','append'):
                source=next(b for b in template['blocks'] if template['paths'][b['block_id']]==slot['path'])
                elements,changed=replacements(source,slot,values[ident])
                key=TEXT_KEYS[block['block_type']];changed=copy.deepcopy(block);changed[key]['elements']=elements
            else:elements,changed=replacements(block,slot,values[ident])
            block.clear();block.update(changed)
            requests.append({'block_id':slot['block_id'],'update_text_elements':{'elements':elements}})
        if len(requests)>200:raise BoundaryError('DOCUMENT_NATIVE_BATCH_LIMIT')
        payload['expected']=expected
        self.s.execute('UPDATE amdoc_native_writes SET payload=?,state=? WHERE idem=?',(canonical(payload),'writing',idem))
        fresh=self.docs.read(token)
        if fresh['revision']!=current['revision'] or fresh['hash']!=current['hash']:
            raise BoundaryError('DOCUMENT_NATIVE_CONCURRENT_CHANGE')
        self.docs.check()
        self.docs.call('PATCH','/open-apis/docx/v1/documents/'+token+'/blocks/batch_update',
            query={'document_revision_id':current['revision'],'client_token':str(uuid.UUID(idem[:32]))},body={'requests':requests})
        receipt=self.reconcile(idem)
        return {'state':'verified' if receipt else 'unknown','receipt':receipt,'idempotency_key':idem,'new_revision_copy':bool(prior)}
