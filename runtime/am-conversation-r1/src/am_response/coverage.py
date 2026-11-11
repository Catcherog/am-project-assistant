"""Host-owned answer obligations. Model references never choose what is required."""
import re

def claim_requirements(request, claims):
    # Narrow only an explicit single-field query. Compound/unknown requests
    # conservatively require every retrieved claim, including conflicts/gaps.
    field=None
    if re.search(r'谁负责|负责人|人员与职责',request) and not re.search(r'进展|进度|日期|待办',request):
        field=re.compile(r'负责人|人员与职责|owner|responsib',re.I)
    elif re.search(r'进展|进度',request) and not re.search(r'谁负责|负责人|日期|待办',request):
        field=re.compile(r'进展|进度|progress|forecast',re.I)
    selected={c['id'] for c in claims if field and field.search(str(c.get('predicate','')))}
    required=[];optional=[]
    for c in claims:
        # Unknown raw shapes and explicitly conflicting/unverified evidence
        # cannot be silently classified as unrelated optional facts.
        unrelated=(selected and c['id'] not in selected and c.get('predicate') not in (None,'source_result')
                   and not c.get('conflict_group') and not c.get('required'))
        (optional if unrelated else required).append(c['id'])
    return required,optional
