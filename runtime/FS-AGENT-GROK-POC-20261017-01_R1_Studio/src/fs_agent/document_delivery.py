"""Read-only discovery of already authorized document mounting targets."""
from __future__ import annotations

from contextlib import nullcontext
import time
import re

from .config import BoundaryError
from .document_target import TargetValidationError


def discover_targets(business, row, *, project_code, doc_type, guard, deadline, limit=32):
    # Optional capability: existing fake/alternative ports retain their API.
    # A Meegle port shares this deadline/guard across aliases, metadata pages,
    # resolve and the independent identity reread rather than restarting it.
    reader_scope = getattr(business, 'read_scope', None)
    scope = reader_scope(row, guard=guard, deadline=deadline) if callable(reader_scope) else nullcontext()
    with scope:
        return _discover_targets(business, row, project_code=project_code, doc_type=doc_type,
                                 guard=guard, deadline=deadline, limit=limit)


def _discover_targets(business, row, *, project_code, doc_type, guard, deadline, limit):
    if doc_type not in ('charter', 'change', 'closure'):
        raise BoundaryError('DOCUMENT_DELIVERY_TYPE_INVALID')
    if guard() is False:
        raise BoundaryError('DOCUMENT_DELIVERY_SCOPE_CHANGED')
    settings = getattr(business, 'settings', None)
    configured = settings.get('targets') if isinstance(settings, dict) else None
    diagnostics=[]
    def result(state, targets=(), unresolved=(), labels=None, reviewable_candidates=()):
        return {'state': state, 'targets': list(targets), 'unresolved': list(unresolved),
                'labels': labels or {}, 'doc_type': doc_type, 'read_only': True,'diagnostics':diagnostics,
                'reviewable_candidates':list(reviewable_candidates)}
    if not isinstance(configured, dict):
        return result('not_configured')
    if len(configured) > limit:
        return result('unresolved', unresolved=['configured_target_limit'])
    resolver = getattr(business, 'resolve', None)
    identity_reader = getattr(business, 'document_identity', None)
    if not callable(resolver) or not callable(identity_reader):
        return result('unresolved', unresolved=['target_identity_unavailable'])
    matches = []; unresolved = []; labels = {}
    for alias in sorted(configured):
        if guard() is False:
            raise BoundaryError('DOCUMENT_DELIVERY_SCOPE_CHANGED')
        if time.monotonic() >= deadline:
            unresolved.append('deadline'); break
        stage='resolve'
        try:
            target = resolver(row, alias)
            if not isinstance(target, dict):
                raise BoundaryError('TARGET_BINDING_INCOMPLETE')
            stage='document_identity'
            identity = identity_reader(row, target)
            if not isinstance(identity, dict):
                raise BoundaryError('BUSINESS_DOCUMENT_IDENTITY_UNVERIFIED')
        except BoundaryError as exc:
            # A failure cannot prove that a formal target is absent.
            code=str(exc)
            diagnostic={'alias':alias,'stage':stage,'status':'unresolved',
                'error_code':code if re.fullmatch(r'[A-Z][A-Z0-9_]{0,100}',code) else 'BOUNDARY_ERROR'}
            if isinstance(exc, TargetValidationError) and exc.diagnostic:
                diagnostic['detail']=dict(exc.diagnostic)
            diagnostics.append(diagnostic)
            unresolved.append(alias); continue
        if guard() is False:
            raise BoundaryError('DOCUMENT_DELIVERY_SCOPE_CHANGED')
        if time.monotonic() >= deadline:
            unresolved.append('deadline'); break
        if identity.get('project_code') == project_code and identity.get('doc_type') == doc_type:
            if not all(target.get(k) for k in ('id', 'project_id', 'node_id', 'deliverable_id', 'revision')):
                diagnostics.append({'alias':alias,'stage':'binding','status':'unresolved','error_code':'TARGET_BINDING_INCOMPLETE'})
                unresolved.append(alias); continue
            diagnostics.append({'alias':alias,'stage':'binding','status':'matched','target_revision':target['revision'],
                'project_matches':True,'document_type_matches':True})
            matches.append(dict(target))
            config = configured[alias] if isinstance(configured[alias], dict) else {}
            names = [target.get(k) for k in ('title', 'name', 'label')] + [config.get(k) for k in ('display_name', 'title', 'name', 'label')]
            label = next((n.strip() for n in names if isinstance(n, str) and n.strip()), None)
            if label is None and re.search(r'[\u4e00-\u9fff]', alias):label = alias
            if label:labels[target['id']] = label
        else:
            diagnostics.append({'alias':alias,'stage':'binding','status':'mismatch','target_revision':target.get('revision'),
                'project_matches':identity.get('project_code')==project_code,
                'document_type_matches':identity.get('doc_type')==doc_type})
    state = ('unresolved' if unresolved else 'ambiguous' if len(matches) > 1
             else 'available' if matches else 'not_configured')
    if unresolved:
        # One readable candidate does not resolve an unreadable second binding.
        # Retain observations for review, but expose no actionable target list.
        return result(state, (), unresolved, labels, reviewable_candidates=matches)
    return result(state, matches, unresolved, labels)


def render_targets(result):
    state = result['state']
    if state == 'not_configured':
        return '当前授权配置中未找到本项目、本文种的正式挂载目标。草稿可以继续整理；请补齐对应项目、节点和交付物绑定。'
    if state == 'unresolved':
        codes={d.get('error_code') for d in result.get('diagnostics',[]) if d.get('status')=='unresolved'}
        reason=('项目编号或交付物文种尚未明确对应。' if 'BUSINESS_DOCUMENT_IDENTITY_UNVERIFIED' in codes
                else '目标版本已变化，需要重新核对。' if 'TARGET_REVISION_CHANGED' in codes
                else '本次读取期限已到，尚未完成的目标需要重新核验。' if 'BUSINESS_READ_DEADLINE' in codes
                else '配置仍缺少明确的文种身份字段、文档写入字段或预期项目身份，需要先按实际记录核对。' if codes & {'BUSINESS_TARGET_CONFIG_INCOMPLETE','BUSINESS_TARGET_CONFIG_INVALID','BUSINESS_TARGET_FIELD_CONFLICT'}
                else '平台字段元数据未能完整读取，字段类型与候选值尚未核实。' if codes & {'BUSINESS_METADATA_UNAVAILABLE','BUSINESS_METADATA_LIMIT','BUSINESS_FIELD_METADATA_MISSING','BUSINESS_FIELD_METADATA_AMBIGUOUS'}
                else '文种字段与文档写入字段的类型、候选值或实际文种未能对应；需要核对真实字段定义。' if codes & {'BUSINESS_FIELD_TYPE_UNSUPPORTED','BUSINESS_FIELD_CANDIDATES_UNVERIFIED','BUSINESS_FIELD_CANDIDATE_MISMATCH','BUSINESS_FIELD_VALUE_UNVERIFIED','BUSINESS_DOCUMENT_KIND_MISMATCH'}
                else '平台未返回可验证的项目关系字段，需要补齐真实关系读回。' if codes & {'BUSINESS_PROJECT_RELATION_MISSING','BUSINESS_PROJECT_RELATION_UNVERIFIED'}
                else '平台项目编号与预期项目尚未唯一对应。' if codes & {'BUSINESS_PROJECT_CODE_MISMATCH','BUSINESS_PROJECT_CODE_UNVERIFIED'}
                else '配置的交付物已无法在原节点的下级列表中找到，需要重新核对父子绑定。' if 'BUSINESS_PARENT_CHANGED' in codes
                else '配置的项目、节点或交付物与平台记录不一致。' if codes & {'BUSINESS_TARGET_CHANGED','BUSINESS_PROJECT_IDENTITY_CHANGED','BUSINESS_NODE_UNVERIFIED','BUSINESS_TARGET_BINDING_CHANGED','BUSINESS_TARGET_FIELD_MISSING'}
                else '目标读取尚未完成，具体失败阶段已记录。')
        return '正式挂载目标尚未核验完整。'+reason+'草稿已保留。'
    lines = ['已核验当前授权配置中的正式目标：' if state == 'available' else '找到多个匹配的正式目标，请明确选择：']
    kind = {'charter': '项目章程', 'change': '变更申请', 'closure': '结项报告'}.get(result.get('doc_type'), '文档')
    for number, target in enumerate(result['targets'], 1):
        label = result.get('labels', {}).get(target['id']) or '已配置的' + kind + '正式目标'
        lines.append((str(number) + '. ' if len(result['targets']) > 1 else '') + label)
    lines.append('选择目标、采用当前文字与正式挂载仍按原流程分别执行。')
    return '\n'.join(lines)
