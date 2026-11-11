"""One immutable evidence contract consumed by query and document paths."""
from dataclasses import dataclass
import math
from .types import encode, fingerprint

@dataclass(frozen=True)
class EvidenceBundle:
    serialized: str
    @property
    def data(self):
        import json
        return json.loads(self.serialized)
    @classmethod
    def build(cls, project, *, scope, expires, information=None, document_binding=None):
        ident=project.get('id',project.get('key'));fields=project.get('fields') or {k:project[k] for k in ('关键日期','待办','项目进展','人员与职责') if k in project}
        source=project.get('snapshot') or {'version':project.get('source_revision'),'observed_at':project.get('_sync_observed_at') or project.get('observed_at')}
        claims=[];ledger_gaps=[];ledger_evidence=None
        if document_binding is not None:
            ledger_evidence=document_binding.get('ledger_evidence') if isinstance(document_binding,dict) else None
            bound_expiry=document_binding.get('expires') if isinstance(document_binding,dict) else None
            valid=(isinstance(ledger_evidence,dict)
                and document_binding.get('project_id')==ident
                and ledger_evidence.get('source_id')=='demo-ledger'
                and ledger_evidence.get('object_id')==ident
                and isinstance(ledger_evidence.get('version'),str) and bool(ledger_evidence['version'])
                and ledger_evidence.get('raw_available') is True
                and type(ledger_evidence.get('raw_expires_at_ms')) is int
                and type(bound_expiry) in (int,float) and math.isfinite(bound_expiry) and bound_expiry>0
                and ledger_evidence['raw_expires_at_ms']>=bound_expiry*1000)
            if not valid:
                # A retained project-cache row is not proof that its underlying
                # ledger quotation is still within the original retention period.
                fields={};ledger_evidence=None
                ledger_gaps.append({'kind':'document_ledger_evidence_unavailable','source_id':'demo-ledger'})
        for key,value in fields.items():
            origin=dict(source,source_id='demo-ledger',object_id=ident,locator=key)
            # A legacy read has no verified raw-source lease. Keep its bounded
            # query metadata; only a checked document binding adds quotations.
            if ledger_evidence:
                origin.update(version=ledger_evidence['version'],
                    quote=str(key)+'：'+(value if isinstance(value,str) else encode(value)),
                    raw_available=True,read_state=project.get('read_status','complete'),
                    raw_expires_at_ms=ledger_evidence['raw_expires_at_ms'])
            claims.append({'id':fingerprint([ident,key,source,value])[:24], 'subject':ident,'predicate':key,'value':value,
                'subject_label':project.get('title'),
                'modality':'recorded','business_state':'reported',
                **({'authority':'source_recorded'} if document_binding is not None else {}),
                'source':origin,
                'expression':'quote','observed_at':project.get('_sync_observed_at') or project.get('observed_at')})
        info=information if information is not None else project.get('information',{})
        for fact in info.get('facts',[]):
            if 'value' not in fact:continue
            claims.append({'id':fact['fact_id'],'subject':fact.get('subject_id',ident),'predicate':fact['field'],'value':fact['value'],
                'subject_label':fact.get('subject_label',project.get('title') if fact.get('subject_id',ident)==ident else None),
                'modality':fact.get('assertion_type','unknown'),'business_state':fact.get('business_state','unconfirmed'),
                'authority':fact.get('authority','unconfirmed'),
                'source':fact.get('evidence',{}),'expression':'quote',
                **{k:fact[k] for k in ('polarity','time','time_scope','scope','condition','attribution','conflict_group','stale') if k in fact}})
        content={'schema_version':'am-evidence/1','project_id':ident,'project_title':project.get('title'),
                 'claims':claims,'coverage':info.get('coverage',{}),'gaps':list(info.get('gaps',[]))+ledger_gaps,
                 **{k:info[k] for k in ('conflicts','incomplete_reasons') if k in info}}
        envelope=dict(content,bundle_id=fingerprint(content),scope=list(scope),expires=expires)
        if document_binding is not None:
            envelope['document_binding']=document_binding
        return cls(encode(envelope))

    def document_source(self, *, uri, legacy_project_id=None):
        d=self.data
        segments=[];gaps=list(d.get('gaps',[]))
        for c in d['claims']:
            source=dict(c.get('source') or {})
            # Retained derived values are not a new copy of expired source text.
            # Only the source quote admitted by the shared fact service is mapped.
            quote=source.get('quote')
            reason=None
            if c.get('subject')!=d['project_id']:reason='document_subject_mismatch'
            elif source.get('raw_available') is not True:reason='document_raw_evidence_unavailable'
            elif not isinstance(quote,str) or not quote.strip():reason='document_quote_unavailable'
            elif not all(source.get(k) for k in ('source_id','object_id','version')):reason='document_source_identity_incomplete'
            if reason:
                gaps.append({'kind':reason,'fact_id':c['id'],'source_id':source.get('source_id')})
                continue
            segments.append({'id':c['id'],'original':quote,'source':source,
                'subject_id':c['subject'],'predicate':c['predicate'],
                'modality':c['modality'],'business_state':c['business_state'],
                'authority':c.get('authority','unconfirmed'),
                **{k:c[k] for k in ('polarity','time','time_scope','scope','condition','attribution','conflict_group','stale','observed_at') if k in c}})
        gaps.extend({'kind':'document_fact_conflict','conflict':c} for c in d.get('conflicts',[]))
        gaps.extend({'kind':reason} for reason in d.get('incomplete_reasons',[]))
        partial=bool(gaps) or any(d['coverage'].get(k) for k in ('processing_pending','needs_refresh_or_review','stale'))
        return {'id':'shared-evidence','uri':uri,'version':d['bundle_id'],'read_status':'partial' if partial else 'complete',
                'provenance':'current_project','project_id':d['project_id'],
                'legacy_project_id':legacy_project_id,'evidence_bundle_id':d['bundle_id'],
                'evidence_contract':'am-document-evidence/1',
                'document_binding':d.get('document_binding'),
                'segments':segments,'coverage':d['coverage'],'gaps':gaps}
