"""Local document view fences shared by rendering, callbacks and delivery.

Only opaque fingerprints are persisted here; no new source retrieval, identity
renewal, model call or business action takes place inside a dispatch fence.
"""
import json
import math
import sqlite3
from .config import BoundaryError
from .project_catalog import digest
from .store import scope_of
from .document_evidence import BINDING_SCHEMA


SCOPED_SOURCE_FENCE = 'am-document-source-fence/2:'


def init(store):
    store.execute('CREATE TABLE IF NOT EXISTS am_document_view_outputs ('
        'outbox_id TEXT PRIMARY KEY,binding TEXT NOT NULL,expires REAL NOT NULL)')
    store.execute('CREATE TABLE IF NOT EXISTS am_document_source_outputs ('
        'outbox_id TEXT PRIMARY KEY,source_fence TEXT NOT NULL)')


def _capture(logic, row, draft):
    now=logic.s.clock()
    if draft['scope']!=scope_of(row) or draft['expires']<=now:
        raise BoundaryError('DOCUMENT_VIEW_EXPIRED')
    # Collection pages without a result may still describe their neutral state.
    has_result=any(draft.get(k) for k in ('content','analysis','review')) or draft['state']=='ready'
    if has_result:
        logic.require_current_document_evidence(row,draft)
    tables={r['name'] for r in logic.s.rows("SELECT name FROM sqlite_master WHERE type='table'")}
    context=logic.s.rows('SELECT * FROM amdoc_context WHERE draft_id=?',(draft['id'],)) if 'amdoc_context' in tables else []
    results=logic.s.rows('SELECT * FROM amdoc_results WHERE draft_id=? AND revision=?',
        (draft['id'],draft['revision'])) if 'amdoc_results' in tables else []
    rules=json.loads(draft.get('rules') or '{}')
    payload=json.loads(results[0]['payload']) if results else {}
    evidence=payload.get('document_binding') or rules.get('document_binding')
    if (isinstance(evidence,dict) and evidence.get('schema')==BINDING_SCHEMA
            and not logic.is_standard_draft(draft)):
        raise BoundaryError('DOCUMENT_VIEW_EVIDENCE_UNVERIFIED')
    expiries=[row['expires'],draft['expires']]
    expiries += [r['expires'] for r in context+results]
    if evidence:
        expiries.append(evidence['expires'])
        raw=evidence.get('ledger_evidence',{}).get('raw_expires_at_ms')
        if raw is not None:expiries.append(raw/1000)
    if any(type(t) not in (int,float) or not math.isfinite(t) or t<=now for t in expiries):
        raise BoundaryError('DOCUMENT_VIEW_EXPIRED')
    view={'schema':'am-document-view/1','draft_id':draft['id'],'revision':draft['revision'],
        'scope':draft['scope'],'content_hash':draft['content_hash'],
        'payload_hash':digest([draft.get(k) for k in (
            'content','analysis','review','rules','state','stage','accepted_hash','target')]),
        'evidence_hash':digest([context,results,evidence]),'expires':min(expiries)}
    # A neutral collection page has not passed the evidence verifier above and
    # cannot obtain a scoped delivery fence from a dormant rules field.
    return view,evidence if has_result else None


def capture(logic, row, draft):
    return _capture(logic,row,draft)[0]


def _require(logic, row, binding):
    if not isinstance(binding,dict) or binding.get('schema')!='am-document-view/1':
        raise BoundaryError('DOCUMENT_VIEW_REOPEN_REQUIRED')
    expiry=binding.get('expires')
    if type(expiry) not in (int,float) or not math.isfinite(expiry) or expiry<=logic.s.clock():
        raise BoundaryError('DOCUMENT_VIEW_EXPIRED')
    drafts=logic.s.rows('SELECT * FROM logic_drafts WHERE id=?',(binding.get('draft_id'),))
    if not drafts:raise BoundaryError('DOCUMENT_VIEW_CHANGED')
    current,evidence=_capture(logic,row,drafts[0])
    if set(current)!=set(binding) or any(current[k]!=binding[k] for k in current if k!='expires') or expiry>current['expires']:
        raise BoundaryError('DOCUMENT_VIEW_CHANGED')
    return evidence


def require(logic, row, binding):
    _require(logic,row,binding)
    return True


def source_fence(logic, row, binding):
    """Return a v2-only delivery marker after the full persisted-view check.

    The marker is a routing discriminator, not an authorization grant. Every
    use must repeat this verifier against the exact persisted output binding.
    Legacy/unbound/collection views never gain a scoped marker on upgrade.
    """
    evidence=_require(logic,row,binding)
    if isinstance(evidence,dict) and evidence.get('schema')==BINDING_SCHEMA:
        return SCOPED_SOURCE_FENCE+digest(binding)
    return None


def bind_source_output(row, verified_view_fence):
    """Bind the marker to this exact immutable outbox payload, not just a draft."""
    keys=('id','task_id','app','tenant','user','chat','chat_type','origin_chat_type',
          'kind','body','artifact','nonce','created','expires')
    return SCOPED_SOURCE_FENCE+digest([verified_view_fence,{k:row[k] for k in keys}])


def scoped_delivery_authorized(logic, row, marker):
    records=logic.s.rows('SELECT binding FROM am_document_view_outputs WHERE outbox_id=?',(row['id'],))
    if not records or not isinstance(marker,str) or not marker.startswith(SCOPED_SOURCE_FENCE):
        return False
    verified=source_fence(logic,row,json.loads(records[0]['binding']))
    return verified is not None and bind_source_output(row,verified)==marker


def references_document(snapshot):
    """Recognize older server-owned snapshots which predate the view binding."""
    data=snapshot.get('data',{})
    if 'document_revision' in data:return True
    detail=snapshot.get('envelope',{}).get('detail_ref') or {}
    if isinstance(detail,dict) and detail.get('draft'):return True
    actions=list(snapshot.get('actions',{}).values())
    actions += [a for _,a in data.get('_long_view',{}).get('actions',[])]
    return any(isinstance(a,dict) and a.get('page')=='documents' and a.get('draft') for a in actions)


def check_snapshot(logic,row,snapshot):
    binding=snapshot.get('data',{}).get('document_binding')
    if binding is not None:return require(logic,row,binding)
    data=snapshot.get('data',{})
    # A receipt is historical transport status, not reusable document content.
    if data.get('document_receipt') is True and not data.get('_long_view'):return True
    if references_document(snapshot):raise BoundaryError('DOCUMENT_VIEW_REOPEN_REQUIRED')
    return True


def valid_output(logic,row):
    records=logic.s.rows('SELECT binding FROM am_document_view_outputs WHERE outbox_id=?',(row['id'],))
    try:
        marker=logic.s.rows('SELECT source_fence FROM am_document_source_outputs WHERE outbox_id=?',(row['id'],))
        if marker and not records:return False
        checked_binding=None
        if records:
            checked_binding=json.loads(records[0]['binding'])
            verified=source_fence(logic,row,checked_binding)
            if verified is not None:
                if not marker or bind_source_output(row,verified)!=marker[0]['source_fence']:
                    return False
            elif marker:return False
        for card in logic.s.rows('SELECT snapshot FROM am_cards WHERE outbox_id=?',(row['id'],)):
            snapshot=json.loads(card['snapshot'])
            if snapshot.get('data',{}).get('document_binding') is not None and not records:return False
            # Reuse only this function's exact checked binding. A later guard
            # starts from current state again, including the final send guard.
            if checked_binding is None or snapshot.get('data',{}).get('document_binding')!=checked_binding:
                check_snapshot(logic,row,snapshot)
        return True
    except (BoundaryError,ValueError,KeyError,TypeError,sqlite3.Error):
        return False


def retire_unbound(logic):
    """Old caches cannot acquire a current binding retroactively on upgrade."""
    s=logic.s;key='document-view-binding-r9-fix1'
    if s.meta(key):return
    with s.tx() as c:
        ids=set()
        for row in c.execute('SELECT outbox_id,snapshot FROM am_cards').fetchall():
            snap=json.loads(row['snapshot'])
            if references_document(snap) and not snap.get('data',{}).get('document_binding'):
                ids.add(row['outbox_id'])
        for row in c.execute("SELECT outbox_id FROM am_notifications WHERE kind='document_result'").fetchall():
            if not c.execute('SELECT 1 FROM am_document_view_outputs WHERE outbox_id=?',(row[0],)).fetchone():ids.add(row[0])
        for out in ids:
            c.execute("INSERT OR IGNORE INTO am_document_view_outputs VALUES(?,'{}',?)",(out,s.clock()))
            c.execute('UPDATE outbox SET expires=MIN(expires,?) WHERE id=?',(s.clock(),out))
            c.execute("UPDATE am_cards SET expires=MIN(expires,?),snapshot='{}' WHERE outbox_id=?",(s.clock(),out))
        for view in c.execute('SELECT id,current_view FROM am_interactions').fetchall():
            snap=json.loads(view['current_view'])
            if references_document(snap) and not snap.get('data',{}).get('document_binding'):
                c.execute("UPDATE am_interactions SET current_view='{}',subject_refs='[]',expires=MIN(expires,?) WHERE id=?",(s.clock(),view['id']))
        c.execute('INSERT OR REPLACE INTO meta VALUES(?,?)',(key,'done'))
    cleanup(s)


def cleanup(store):
    # The journal retains only hashes/identities. Cache bodies are removed even
    # if an unrelated parent interaction or original input has a longer TTL.
    now=store.clock()
    with store.tx() as c:
        c.execute("UPDATE am_interactions SET current_view='{}',subject_refs='[]' WHERE expires<=?",(now,))
        c.execute("UPDATE am_cards SET snapshot='{}' WHERE expires<=? AND outbox_id IN (SELECT outbox_id FROM am_document_view_outputs)",(now,))
        c.execute("UPDATE outbox SET body=NULL,artifact=NULL,state=CASE WHEN state IN ('pending','callback_pending') THEN 'expired' ELSE state END WHERE id IN (SELECT outbox_id FROM am_document_view_outputs WHERE expires<=?)",(now,))
        c.execute('DELETE FROM am_document_source_outputs WHERE outbox_id NOT IN (SELECT id FROM outbox)')
