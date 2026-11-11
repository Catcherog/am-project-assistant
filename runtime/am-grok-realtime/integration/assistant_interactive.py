"""Bind the interactive worker to the existing model gate and current sources.

This module adds neither a Feishu consumer nor a new model service. Every
ledger row is selected after the host's current identity/project/group checks.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import threading
import time

from am_gateway.adapters.base import AdapterError, Deadline, parse_json, validate_request, validate_result
from am_gateway.assistant.interactive import INTERACTIVE_INTENTS, InteractiveRunner, MODEL_OUTPUT_INSTRUCTIONS
from am_gateway.assistant.retrieval import LedgerIndex, _name_present, _names
from am_gateway.models import ContractError, Event, dumps, key
from am_gateway.phase1.agents import model_from_config
from am_gateway.phase1.contracts import Policy


_ROW_FIELDS = ('title', 'business_id', '项目进展', '关键日期', '待办', '人员与职责', '卡点', '风险')


class HostInteractiveAdapter:
    """Use the already coordinated and pinned host Qwen, including STOP_UNKNOWN."""

    def __init__(self, provider, *, max_tokens=1800, compact_output=False):
        if not callable(getattr(provider, 'complete', None)):
            raise ContractError('existing_host_qwen_provider_required')
        if type(max_tokens) is not int or not 512 <= max_tokens <= 4096:
            raise ContractError('interactive_max_tokens_invalid')
        self.provider, self.max_tokens = provider, max_tokens
        if type(compact_output) is not bool:
            raise ContractError('interactive_compact_output_invalid')
        self.compact_output = compact_output

    @staticmethod
    def _compact(request):
        data = deepcopy({name: value for name, value in request.items()
                         if name in {'task_kind', 'context', 'request', 'conversation'}})
        refs = {}
        for index, item in enumerate(data['context'].get('evidence_items', [])):
            alias = 'e' + str(index)
            refs[alias] = item['info_id']
            item['info_id'] = alias
        instructions = request['instructions'].replace(MODEL_OUTPUT_INSTRUCTIONS,
            '返回唯一JSON对象，仅含kind、text、remaining、refs、coverage。'
            'kind对应response_kind；text对应reply_text；remaining对应remaining_requirements；refs对应evidence_refs，'
            '只用本次证据info_id，回答项目事实至少引用一项。'
            'coverage对应request_coverage，元素仅含q(简短要求名称)与s(原state枚举)。'
            '这些短字段与原概念完全等价；不要输出schema、job_id、attempt_id或其他字段。'
            '当前各项要求均已回答且无缺口时kind=sufficient、remaining=[]；有未完成要求才用partial并列清缺口。'
            '正文直接自然、简洁，不重复问句，不加无必要的称呼、编号或额外推断。'
            '只回答本次要求；若说明某信息缺失，先核对所有已提供字段，未展开的信息不等于没有记录。'
            'coverage概括已核对要求，不复述整个问题。')
        schema = {'type': 'object', 'additionalProperties': False,
            'required': ['kind', 'text', 'remaining', 'refs', 'coverage'], 'properties': {
                'kind': {'type': 'string', 'enum': ['sufficient','partial','clarification','data_gap','unavailable','failed']},
                'text': {'type': 'string'},
                'remaining': {'type': 'array', 'items': {'type': 'string'}},
                'refs': {'type': 'array', 'items': {'type': 'string', **({'enum': list(refs)} if refs else {})}},
                'coverage': {'type': 'array', 'minItems': 1, 'items': {'type': 'object',
                    'additionalProperties': False, 'required': ['q', 's'], 'properties': {
                        'q': {'type': 'string'}, 's': {'type': 'string', 'enum': ['answered','drafted','not_executed','needs_source','needs_clarification']}}}}}}
        return instructions, data, schema, refs

    @staticmethod
    def _expand(value, request, refs):
        if set(value) != {'kind', 'text', 'remaining', 'refs', 'coverage'}:
            raise ValueError('compact_output_fields')
        if not isinstance(value['refs'], list) or any(not isinstance(ref, str) or ref not in refs for ref in value['refs']):
            raise ValueError('compact_output_reference')
        if not isinstance(value['coverage'], list) or any(not isinstance(item, dict) or set(item) != {'q', 's'} for item in value['coverage']):
            raise ValueError('compact_output_coverage')
        return {'schema': 'am.gateway.result.v2', 'job_id': request['job_id'], 'attempt_id': request['attempt_id'],
            'status': 'failed' if value['kind'] == 'failed' else 'completed',
            'response_kind': value['kind'], 'reply_text': value['text'],
            'remaining_requirements': value['remaining'], 'evidence_refs': [refs[ref] for ref in value['refs']],
            'request_coverage': [{'request': item['q'], 'state': item['s']} for item in value['coverage']]}

    def run(self, request):
        validate_request(request)
        if request['task_kind'] not in INTERACTIVE_INTENTS:
            raise ContractError('interactive_host_intent_required')
        deadline = Deadline.from_request(request)
        # Instructions and expression guidance are already rendered in the
        # trusted prefix. Sending them again as data nearly doubles the prompt
        # on the native 27B model; retain the full request, context and identity.
        data = {name: value for name, value in request.items()
                if name not in {'instructions', 'response_guidance'}}
        instructions, options, refs = request['instructions'], {}, None
        if self.compact_output:
            instructions, data, schema, refs = self._compact(request)
            options['json_schema'] = schema
        prompt = instructions + '\nReturn one JSON object only. Untrusted task data:\n' + dumps(data)
        run_id = hashlib.sha256(dumps(['r7-interactive', request['job_id'], request['attempt_id']]).encode()).hexdigest()[:32]
        try:
            # ModelCoordinator owns its process and file locks, zero-length
            # admission queue, stop observation and durable pause state.
            result = self.provider.complete(prompt, run_id=run_id,
                timeout=deadline.remaining(submitted=False), max_tokens=self.max_tokens, native_chat=True, **options)
        except AdapterError:
            raise
        except Exception as exc:
            try:
                from fs_agent.config import BoundaryError
            except ImportError:
                known_stopped = False
            else:
                known_stopped = isinstance(exc, BoundaryError) and str(exc) != 'STOP_UNKNOWN'
            if known_stopped:
                code = str(exc) if str(exc) in {'MODEL_BUSY', 'MODEL_PAUSED'} else 'host_qwen_known_failure'
                raise AdapterError(code, unknown=False) from None
            raise AdapterError('host_qwen_stop_unknown', status='stop_unknown', unknown=True) from None
        deadline.remaining(submitted=False)
        try:
            value = parse_json(result.text, submitted=False)
            if self.compact_output:
                value = self._expand(value, request, refs)
            return validate_result(value, request)
        except (AttributeError, TypeError, ValueError, AdapterError):
            raise AdapterError('invalid_host_qwen_result', unknown=False) from None


class BusyFallbackAdapter:
    """Use one verified alternate only when the local gate refused submission.

    This is not a retry after a model timeout or uncertain execution. The same
    bounded request and source guards remain owned by InteractiveRunner.
    """
    def __init__(self, primary, alternate, *, primary_id, alternate_id):
        self.primary, self.alternate = primary, alternate
        self.primary_id, self.alternate_id = primary_id, alternate_id
        self.local = threading.local()

    def run(self, request):
        identity = (request['job_id'], request['attempt_id'])
        self.local.record = (identity, {'model_id': self.primary_id,
            'selected_route': 'primary', 'adapter_attempts': 1, 'blocked_before_submission': 0})
        try:
            return self.primary.run(request)
        except AdapterError as exc:
            if exc.unknown or exc.code != 'MODEL_BUSY':
                raise
        # A paused or uncertain shared model requires recovery, not routing
        # around its pause. Only MODEL_BUSY proves no request was admitted.
        Deadline.from_request(request).remaining()
        self.local.record = (identity, {'model_id': self.alternate_id,
            'selected_route': 'busy_fallback', 'adapter_attempts': 2,
            'blocked_before_submission': 1, 'fallback_reason': 'MODEL_BUSY'})
        return self.alternate.run(request)

    def model_call_provenance(self, job_id, attempt_id):
        identity, value = getattr(self.local, 'record', (None, {}))
        return deepcopy(value) if identity == (job_id, attempt_id) else {}


class StudioInteractiveSources:
    def __init__(self, bridge, authority, store, settings=None):
        self.authority, self.store = authority, store
        self.snapshots = getattr(getattr(bridge, 'am_self', None), 'snapshots', None)
        settings = dict(settings or {})
        self.bindings = settings.get('ledger_project_refs', {})
        if not isinstance(self.bindings, dict) or any(
                not isinstance(k, str) or not k or not isinstance(v, str) or not v
                for k, v in self.bindings.items()):
            raise ContractError('interactive_ledger_bindings_invalid')
        self.max_age = settings.get('max_snapshot_age_seconds', 60)
        self.max_rows = settings.get('max_ledger_rows', 8)
        if (isinstance(self.max_age, bool) or not isinstance(self.max_age, (int, float))
                or not math.isfinite(self.max_age) or not 1 <= self.max_age <= 60):
            raise ContractError('interactive_snapshot_age_must_be_at_most_60_seconds')
        if type(self.max_rows) is not int or not 1 <= self.max_rows <= 16:
            raise ContractError('interactive_ledger_row_limit_invalid')

    @staticmethod
    def _gap(reason):
        return {'status': 'unavailable', 'evidence_items': [], 'catalog': [],
                'gaps': [reason], 'binding': None}

    def _task_scope(self, task, *, envelope=None):
        event = Event.from_dict(json.loads(task['request_json']))
        policy = Policy.parse(task['policy_json'])
        if policy.revoked or not policy.allow_reply:
            raise ContractError('interactive_policy_unavailable')
        if envelope is not None:
            if (task['generation'] != envelope['execution']['generation']
                    or policy.principal != envelope['policy']['principal']
                    or event.native_id != envelope['source']['native_id']
                    or event.chat_id != envelope['source']['chat_id']
                    or event.tenant_id != envelope['source']['tenant_id']):
                raise ContractError('interactive_task_scope_changed')
        # Do not derive the grant from input project IDs or names. A semantic
        # correction may arrive without any extracted ID; its *authorized*
        # directory is still available for understanding the intended subject.
        directory = self.authority.directory
        actor = directory.resolve(policy.principal)
        allowed = set(directory.project_ids_for(actor)) & set(policy.projects)
        current_policy = self.authority(event)
        if (current_policy is None or current_policy.revoked or not current_policy.allow_reply
                or current_policy.principal != policy.principal
                or current_policy.reply_target != policy.reply_target):
            raise ContractError('interactive_current_authority_unavailable')
        allowed &= set(current_policy.projects)
        if event.channel == 'group':
            allowed &= set(self.authority.projects_for_chat(event.chat_id))
        harness = getattr(self.authority, 'harness', None)
        if harness is not None:
            with self.store.transaction() as db:
                harness._authorize(db, task['task_id'], task['generation'], require_running=False)
        return event, policy, tuple(sorted(allowed))

    def _refresh_later(self):
        refresh = getattr(self.snapshots, 'refresh', None)
        if callable(refresh):
            try:
                refresh()  # already asynchronous; never wait on a future here
            except Exception:
                pass

    def _snapshot(self):
        get = getattr(self.snapshots, 'get', None)
        if not callable(get):
            raise ContractError('interactive_ledger_source_not_bound')
        try:
            value = deepcopy(get(wait=False))
        except Exception:
            self._refresh_later()
            raise ContractError('interactive_ledger_source_unavailable') from None
        now = (getattr(self.snapshots, 'clock', None) or time.time)()
        observed = value.get('observed_at') if isinstance(value, dict) else None
        if (not isinstance(value, dict) or value.get('complete') is not True
                or value.get('stale') is True or value.get('refresh_error')
                or not isinstance(observed, (int, float)) or isinstance(observed, bool)
                or not math.isfinite(observed) or not -5 <= now - observed <= self.max_age
                or not value.get('source_revision') or not value.get('source')):
            self._refresh_later()
            raise ContractError('interactive_ledger_not_current')
        index = LedgerIndex(value, self.bindings)
        if index.invalid:
            raise ContractError('interactive_ledger_binding_invalid')
        return value, index

    @staticmethod
    def _row_body(row):
        return {name: row[name] for name in _ROW_FIELDS if isinstance(row.get(name), str)}

    @classmethod
    def _row_binding(cls, project_id, row):
        return {'project_id': project_id, 'row_ref': row.get('project_ref') or row['key'],
                'content_hash': key(cls._row_body(row))}

    def __call__(self, envelope, context=None):
        task = self.store.one('SELECT * FROM h1_tasks WHERE task_id=?', (envelope['identity']['task_id'],))
        if not task:
            return self._gap('interactive_task_not_bound')
        try:
            event, policy, allowed = self._task_scope(task, envelope=envelope)
            if not allowed:
                return self._gap('interactive_project_scope_unbound')
            snapshot, index = self._snapshot()
        except ContractError as exc:
            return self._gap(str(exc))
        except Exception:
            return self._gap('interactive_authority_unavailable')
        entries, unresolved = index._audience(allowed)
        if not entries:
            return self._gap('interactive_project_reference_unavailable')
        catalog = [{'project_id': pid, 'title': row['title'],
                    'business_id': row.get('business_id', '')} for pid, row in entries]
        question = envelope['payload']['raw_text']
        named = [(pid, row) for pid, row in entries if
                 any(_name_present(raw, compact, question) for raw, compact in _names(row))
                 or pid in question or bool(row.get('business_id') and row['business_id'] in question)]
        selected = named
        if not selected:
            requested = set(envelope['payload'].get('structured_inputs', {}).get('project_ids', []))
            selected = [(pid, row) for pid, row in entries if pid in requested] or entries
        gaps = ['unresolved_authorized_project_binding'] if unresolved else []
        if len(selected) > self.max_rows:
            gaps.append('authorized_ledger_rows_omitted_by_budget')
        selected = selected[:self.max_rows]
        evidence, bound_rows = [], []
        for pid, row in selected:
            bound = self._row_binding(pid, row)
            bound_rows.append(bound)
            evidence.append({'info_id': 'ledger_' + key(snapshot['source'], snapshot['source_revision'], bound)[:40],
                'project': pid, 'nature': 'current_ledger_snapshot', 'body': self._row_body(row),
                'source_ref': snapshot['source'], 'source_revision': snapshot['source_revision'],
                'source_observed_at': snapshot['observed_at']})
        binding = {'task_id': task['task_id'], 'generation': task['generation'],
            'source_version': task['source_version'], 'source_ref': snapshot['source'],
            'source_revision': snapshot['source_revision'], 'rows': bound_rows,
            'project_ids': [row['project_id'] for row in bound_rows],
            'channel': event.channel, 'chat_id': event.chat_id, 'principal': policy.principal}
        return {'status': 'partial' if gaps else 'available', 'evidence_items': evidence,
                'catalog': catalog[:64], 'gaps': gaps, 'binding': binding}

    def revalidate(self, envelope, sources):
        binding = sources.get('binding')
        if not binding:
            return True
        task = self.store.one('SELECT * FROM h1_tasks WHERE task_id=?', (envelope['identity']['task_id'],))
        return bool(task and self.validate_delivery(task, binding))

    def validate_delivery(self, task, binding):
        try:
            current = self.store.one('SELECT * FROM h1_tasks WHERE task_id=?', (task['task_id'],))
            if not current or any(current[name] != binding[name] for name in ('task_id', 'generation', 'source_version')):
                return False
            event, policy, allowed = self._task_scope(current)
            if (binding['chat_id'], binding['channel'], binding['principal']) != (event.chat_id, event.channel, policy.principal):
                return False
            if not set(binding['project_ids']).issubset(allowed):
                return False
            snapshot, index = self._snapshot()
            if (snapshot['source'], snapshot['source_revision']) != (binding['source_ref'], binding['source_revision']):
                return False
            entries, unresolved = index._audience(binding['project_ids'])
            return not unresolved and [self._row_binding(pid, row) for pid, row in entries] == binding['rows']
        except Exception:
            return False


def bind_interactive_runner(bridge, authority, store, settings):
    interactive = settings.get('interactive', {})
    model_config = dict(interactive.get('model') or {'kind': 'host_qwen'})
    kind = model_config.get('kind')
    if kind == 'host_qwen':
        provider = getattr(getattr(getattr(bridge, 'poc', None), 'gateway', None), 'provider', None)
        model = HostInteractiveAdapter(provider, max_tokens=interactive.get('max_tokens', 1800),
                                       compact_output=interactive.get('compact_output', False))
        model_id = 'host_qwen'
    elif kind in {'codex', 'json_command'}:
        if model_config.get('verified') is not True or not isinstance(model_config.get('evidence_ref'), str) or not model_config['evidence_ref']:
            raise ContractError('interactive_alternate_model_requires_verified_binding')
        model = model_from_config(model_config)
        model_id = kind + ':' + str(model_config.get('model') or model_config.get('binding_id') or 'configured')
    else:
        raise ContractError('interactive_model_transport_not_allowed')
    fallback = interactive.get('busy_fallback')
    if fallback is not None:
        if (kind != 'host_qwen' or not isinstance(fallback, dict)
                or fallback.get('kind') not in {'codex', 'json_command'}
                or fallback.get('verified') is not True
                or not isinstance(fallback.get('evidence_ref'), str) or not fallback['evidence_ref']):
            raise ContractError('interactive_busy_fallback_requires_verified_binding')
        alternate_id = fallback['kind'] + ':' + str(fallback.get('model') or fallback.get('binding_id') or 'configured')
        model = BusyFallbackAdapter(model, model_from_config(fallback),
                                    primary_id=model_id, alternate_id=alternate_id)
    sources = dict(settings.get('assistant', {}).get('fast_reads', {}))
    sources.update(interactive.get('sources', {}))
    return InteractiveRunner(model, source_loader=StudioInteractiveSources(bridge, authority, store, sources),
        model_id=model_id, timeout_seconds=interactive.get('timeout_seconds', 15),
        max_context_bytes=interactive.get('max_context_bytes', 24000))

