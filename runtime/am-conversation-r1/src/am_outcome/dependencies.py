"""Precise dependency observation only. Never replaces the global source guard."""
from .types import fingerprint

_REQUIRED=('source_id','object_id','content_hash','access_revision','mapping_version','parser_version','expires')

def manifest(result, *, scope, expires, access_revision):
    bundle=result.get('evidence_bundle',{}) if isinstance(result,dict) else {}
    rows=[]
    for claim in bundle.get('claims',[]):
        source=claim.get('source',{})
        rows.append({'source_id':source.get('source_id'),'object_id':source.get('object_id'),
            'content_version':source.get('version'),'content_hash':fingerprint(claim['value']),
            'access_revision':source.get('access_revision'), # global epoch is not a per-object ACL proof
            'mapping_version':'am-evidence/1','parser_version':'am-evidence/1','expires':min(bundle.get('expires',expires),expires),
            'scope':list(scope),'predicate':claim.get('predicate'),'locator':source.get('locator'),'global_access_revision':access_revision})
    coverage=result.get('coverage',{}) if isinstance(result,dict) else {}
    return {'objects':rows,'query_set':{'predicate':result.get('query') if isinstance(result,dict) else None,
        'watermark':coverage.get('watermark'),'pagination_complete':result.get('has_more') is False if isinstance(result,dict) else False},
        'policy':'shadow_only_global_guard_retained'}


def compare(previous,current,*,now):
    def key(x):return (x.get('source_id'),x.get('object_id'),x.get('locator'))
    before={key(d):d for d in previous.get('objects',[])};after={key(d):d for d in current.get('objects',[])}
    if not before:return {'decision':'indeterminate','reason':'dependencies_missing'}
    for k,old in before.items():
        new=after.get(k)
        if new is None:return {'decision':'indeterminate','reason':'object_not_revalidated'}
        if old.get('expires',0)<=now:return {'decision':'expired','reason':'original_expiry'}
        if new.get('permission_revoked'):return {'decision':'permission_revoked','reason':'current_acl'}
        if new.get('deleted'):return {'decision':'content_changed','reason':'object_deleted'}
        if old.get('content_hash') is not None and new.get('content_hash') is not None and old['content_hash']!=new['content_hash']:
            return {'decision':'content_changed','reason':'current_object_content_changed'}
        if new.get('content_hash') is None and old.get('content_version') is not None and new.get('content_version') is not None and old['content_version']!=new['content_version']:
            return {'decision':'content_changed','reason':'current_object_version_changed'}
        if any(old.get(f) is None or new.get(f) is None for f in _REQUIRED):return {'decision':'indeterminate','reason':'dependency_or_acl_incomplete'}
        if any(old[f]!=new[f] for f in ('content_hash','access_revision','mapping_version','parser_version')):
            return {'decision':'content_changed','reason':'dependency_changed'}
    oldset=previous.get('query_set',{});newset=current.get('query_set',{})
    if oldset.get('predicate') is not None:
        if not oldset.get('pagination_complete') or not newset.get('pagination_complete') or oldset.get('watermark') is None:
            return {'decision':'indeterminate','reason':'query_coverage_incomplete'}
        if oldset!=newset or before.keys()!=after.keys():return {'decision':'content_changed','reason':'query_membership_changed'}
    return {'decision':'current','reason':'all_recorded_dependencies_match'}
