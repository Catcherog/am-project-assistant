"""Exact dependencies over the existing information service, never a second fact DB."""
import json
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical, digest
from am_info_sync.models import Principal
from am_info_sync.policy import can_read


class FollowupSources:
    def __init__(self, host):
        self.host = host

    @property
    def info(self):
        info = self.host.am.information
        if info is None:raise BoundaryError('FOLLOWUP_INFORMATION_UNAVAILABLE')
        return info

    def facts(self, project, *, connection=None):
        h = self.host
        return self.info.service.facts.query(project, Principal(h.owner, h.tenant),connection=connection)

    def resolve(self, project, fact_ids, *, connection=None):
        facts = {f['fact_id']: f for f in self.facts(project,connection=connection)['facts']}
        if not fact_ids or len(set(fact_ids)) != len(fact_ids):raise BoundaryError('FOLLOWUP_EVIDENCE_REQUIRED')
        refs = []
        for key in fact_ids:
            f = facts.get(key)
            if not f or not f['evidence']['raw_available']:raise BoundaryError('FOLLOWUP_EVIDENCE_UNAVAILABLE')
            refs.append({'fact_id':key, **{k:f['evidence'][k] for k in ('source_id','object_id','version')},
                         'authority':f['authority'], 'field':f['field'], 'value_hash':digest(f['value'])})
        return refs

    def defaults(self, project):
        # Only the authoritative ledger row; no project-wide message association.
        facts = self.facts(project)['facts']
        selected = [f['fact_id'] for f in facts if f['field']=='ledger_record'
                    and f['evidence']['source_id']=='demo-ledger' and f['authority']=='source_confirmed']
        if len(selected)!=1:raise BoundaryError('FOLLOWUP_EXACT_EVIDENCE_REQUIRED')
        return self.resolve(project, selected)

    def refresh(self, refs):
        info = self.info
        for ref in refs:
            source=info.store.get_source(ref['source_id'])
            if not can_read(source,Principal(self.host.owner,self.host.tenant)) or info.store.source_status(source.source_id)['availability']=='denied':
                raise BoundaryError('FOLLOWUP_SOURCE_ACCESS_CHANGED')
        if any(r['source_id']=='demo-ledger' for r in refs):
            if info.ledger is None:raise BoundaryError('FOLLOWUP_SOURCE_READER_MISSING')
            info.ledger.read(force=True)
        seen=set()
        for ref in refs:
            pair=(ref['source_id'],ref['object_id'])
            if pair in seen:continue
            seen.add(pair)
            source=info.store.get_source(ref['source_id'])
            if source.source_id=='demo-ledger' or source.kind=='manual':continue
            adapter=info.service.sync_engine.adapters.get(source.kind)
            fetch=getattr(adapter,'fetch_message',None)
            if fetch is None:raise BoundaryError('FOLLOWUP_LIVE_SOURCE_UNSUPPORTED')
            before=source.to_dict()
            page=fetch(source,ref['object_id'])
            if page.next_cursor is not None or len(page.observations)!=1:raise BoundaryError('FOLLOWUP_LIVE_SOURCE_INCOMPLETE')
            obs=page.observations[0]
            if (obs.source_id,obs.object_id)!=pair:raise BoundaryError('FOLLOWUP_LIVE_SOURCE_IDENTITY')
            if info.store.get_source(source.source_id).to_dict()!=before:raise BoundaryError('FOLLOWUP_SOURCE_CHANGED_DURING_READ')
            info.store.record(obs,authoritative=True)

    def disclosure_memberships(self, target):
        """Resolve the only network-backed part of disclosure before any locks."""
        group = target.get('disclosure_group') if target['kind'] == 'recipient_p2p' else None
        return {group: frozenset(u for u, _ in self.host.full_members(group))} if group else {}

    def snapshot(self, project, refs, target, body, *, live=False):
        if live:
            self.refresh(refs)
        memberships = self.disclosure_memberships(target)
        # All paths that touch both stores take the AM mutex first. In particular,
        # never hold Information's transaction while waiting for an AM writer
        # that may itself need Information (for example an owner notification).
        with self.host.s.lock:
            with self.info.store.transaction() as db:
                return self.snapshot_locked(project, refs, target, body, db,
                                            memberships=memberships)

    def snapshot_locked(self, project, refs, target, body, db, *,
                        native_connection=None, memberships=None):
        """Pure local validation inside an already-held Information write fence.

        The caller must hold the AM mutex before that fence. A final send also
        supplies the coordinator's AM transaction, so source dependencies and
        exact disclosure rows cannot change before the sending transition.
        `memberships` comes from a completed live directory lookup outside both
        database transactions. This method must never call a network adapter.
        """
        info=self.info;h=self.host
        current={f['fact_id']:f for f in info.service.facts.query(
            project,Principal(h.owner,h.tenant),connection=db)['facts']}
        versions={};acls={};coverage={};policies={}
        memberships=memberships or {}
        for ref in refs:
            f=current.get(ref['fact_id'])
            if not f or digest(f['value'])!=ref['value_hash']:raise BoundaryError('FOLLOWUP_FACT_CHANGED')
            if any(f['evidence'][k]!=ref[k] for k in ('source_id','object_id','version')):raise BoundaryError('FOLLOWUP_SOURCE_VERSION_CHANGED')
            if not f['evidence']['raw_available']:raise BoundaryError('FOLLOWUP_RAW_EXPIRED')
            source=info.store.get_source(ref['source_id'])
            if not can_read(source,Principal(h.owner,h.tenant)):raise BoundaryError('FOLLOWUP_SOURCE_ACCESS_CHANGED')
            row=db.execute('SELECT * FROM objects WHERE source_id=? AND object_id=?',(ref['source_id'],ref['object_id'])).fetchone()
            obs=info.store.get_observation(ref['source_id'],ref['object_id'],ref['version'])
            if not row or row['state']!='active' or row['current_version']!=ref['version'] or obs is None or obs.deleted or obs.completeness!='complete':
                raise BoundaryError('FOLLOWUP_SOURCE_NOT_CURRENT')
            # Read the disclosure through the final AM transaction when present,
            # not through a second connection that could see a different snapshot.
            policy=h.disclosure(ref,target,body,connection=native_connection)
            if policy is None:raise BoundaryError('FOLLOWUP_DISCLOSURE_REQUIRED')
            if target['kind']=='group' and target['payload']['group']['id'] not in source.output_chats:
                raise BoundaryError('FOLLOWUP_GROUP_SOURCE_DISCLOSURE')
            if target['kind']=='recipient_p2p':
                user=target['user'];group=target.get('disclosure_group')
                explicit=source.settings.get('followup_policy',{})
                direct=(user==source.owner_user_id or 'user:'+user in source.readers)
                policy_allowed=(isinstance(explicit,dict) and bool(explicit.get('revision'))
                    and user in explicit.get('recipients',[]) and ref['field'] in explicit.get('fields',[])
                    and 'request_update' in explicit.get('purposes',[]))
                group_subset=bool(group and group in source.output_chats and user in memberships.get(group,()))
                if not (direct or policy_allowed or group_subset):raise BoundaryError('FOLLOWUP_PRIVATE_DISCLOSURE_NOT_GRANTED')
            if source.settings.get('sensitivity')=='private' or source.settings.get('followup_disclosure') is False:
                raise BoundaryError('FOLLOWUP_PRIVATE_SOURCE')
            key=digest([ref['source_id'],ref['object_id']])
            versions[key]=digest(obs.to_dict())
            settings={k:v for k,v in source.settings.items() if k not in ('subject_ids','subject_id')}
            acls[key]=digest([source.tenant_id,source.owner_user_id,source.readers,source.output_chats,source.enabled,source.start_ms,settings])
            gaps=[dict(r) for r in db.execute("SELECT kind,object_id FROM gaps WHERE source_id=? AND state='open' AND (object_id IS NULL OR object_id=?)",(source.source_id,ref['object_id']))]
            pending=[dict(r) for r in db.execute("SELECT kind,state,payload_json FROM jobs WHERE kind IN ('event','review_message') AND state NOT IN ('done','cancelled')")
                     if json.loads(r['payload_json']).get('source_id')==source.source_id and json.loads(r['payload_json']).get('object_id')==ref['object_id']]
            if gaps or pending:raise BoundaryError('FOLLOWUP_REQUIRED_COVERAGE_UNRESOLVED')
            coverage[key]=digest([source.source_id,ref['object_id'],gaps,pending])
            policies[key]=policy
        return {'versions':versions,'acl_versions':acls,'coverage':coverage,'policies':policies}
