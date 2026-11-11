"""Frozen-source replay of the real interactive worker, without delivery ports.

This is a component/model experiment. It cannot prove Feishu or phone latency,
human style acceptance, or live authority. Sources are reread by the verifier
from immutable, hash-pinned files; expected answers and model grades are unused.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace as NS
import uuid

from am_gateway.assistant.interactive import INTERACTIVE_INTENTS, InteractiveRunner
from am_gateway.assistant.quality import SCHEMA_VERSION, output_hash
from am_gateway.models import ContractError, Event, dumps
from am_gateway.phase1 import Harness, HarnessConfig, Policy
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.execution import ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.store import Store
from integration.assistant_interactive import HostInteractiveAdapter, StudioInteractiveSources


SOURCE_SCHEMA = 'am.rsi.frozen-source/1'
_SUPPORTED_SETUP = {'fixture', 'actor', 'department', 'timezone', 'now',
    'allowed_project_ids', 'audience', 'source_error', 'source_complete',
    'source_visible_rows', 'cached_age_s', 'cached_version'}
_MODULE_PATHS = (Path(__file__), Path(__import__(InteractiveRunner.__module__, fromlist=['']).__file__),
    Path(__import__(StudioInteractiveSources.__module__, fromlist=['']).__file__))


def _sha(data):
    return hashlib.sha256(data).hexdigest()


_LOADED_HASHES = {str(p.resolve()): _sha(p.read_bytes()) for p in _MODULE_PATHS}


def _implementation_binding(klass, name):
    """Pin the concrete Python implementation, including inherited behavior.

    The native presentation overlay can live outside the core package. The
    class identity alone would miss a later edit to that overlay's GUIDANCE.
    Keep the whole defining file for each class in the chain in the binding.
    """
    classes, files = [], {}
    for item in klass.__mro__:
        if item is object:
            continue
        try:
            source = inspect.getsourcefile(item)
        except TypeError:
            source = None
        if not source or not Path(source).is_file():
            raise ContractError('rsi_replay_' + name + '_source_unavailable')
        path = Path(source).resolve()
        classes.append(item.__module__ + ':' + item.__qualname__)
        files[str(path)] = _sha(path.read_bytes())
    return {'class': classes[0], 'class_chain': classes, 'source_file_sha256': files}


def _sources_unchanged(pins):
    try:
        return all(_sha(Path(path).read_bytes()) == digest for path, digest in pins.items())
    except OSError:
        return False


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ContractError('rsi_replay_' + name + '_required')
    return value


def _write_private(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w', encoding='utf-8') as stream:
        stream.write(dumps(value) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


class FrozenSources:
    """Pins are host configuration; neither a candidate nor a case can alter them."""
    def __init__(self, bindings, *, root=None):
        self._bindings = {}
        for ref, item in deepcopy(bindings).items():
            _text(ref, 'source_ref')
            if not isinstance(item, dict) or not re.fullmatch(r'[a-f0-9]{64}', str(item.get('sha256', ''))):
                raise ContractError('rsi_replay_source_pin_required')
            path = Path(_text(item.get('path'), 'source_path'))
            if not path.is_absolute():
                path = Path(root or '.') / path
            self._bindings[ref] = (path.resolve(), item['sha256'], _text(item.get('source_version'), 'source_version'))
        if not self._bindings:
            raise ContractError('rsi_replay_source_bindings_required')

    def resolve(self, case):
        ref = case.get('fixture_ref') or case.get('source_ref') or case.get('setup', {}).get('fixture')
        if ref is None and len(self._bindings) == 1:
            ref = next(iter(self._bindings))
        if ref not in self._bindings:
            raise ContractError('rsi_replay_unknown_frozen_source')
        path, expected, version = self._bindings[ref]
        raw = path.read_bytes()
        if _sha(raw) != expected:
            raise ContractError('rsi_replay_frozen_source_changed')
        value = json.loads(raw)
        if (not isinstance(value, dict) or value.get('schema_version') != SOURCE_SCHEMA
                or set(value) - {'schema_version', 'source_fixture', 'data_origin', 'independence_ref'}
                or not isinstance(value.get('source_fixture'), dict)):
            raise ContractError('rsi_replay_source_only_file_required')
        fixture = value['source_fixture']
        if fixture.get('version') != version or case.get('source_version') != version:
            raise ContractError('rsi_replay_source_version_mismatch')
        if not isinstance(fixture.get('projects'), list):
            raise ContractError('rsi_replay_project_source_required')
        seen = set()
        for project in fixture['projects']:
            if not isinstance(project, dict):
                raise ContractError('rsi_replay_invalid_project')
            pid = _text(project.get('project_id'), 'project_id')
            _text(project.get('name'), 'project_name')
            if pid in seen:
                raise ContractError('rsi_replay_duplicate_project')
            seen.add(pid)
        return deepcopy(fixture), {'ref': ref, 'sha256': expected, 'source_version': version,
            'data_origin': value.get('data_origin', 'unspecified'), 'path': str(path)}


class _FrozenSnapshot:
    def __init__(self, fixture, setup, source):
        self.now = datetime.fromisoformat(setup.get('now') or fixture['observed_at']).timestamp()
        self.clock = lambda: self.now
        rows = []
        for project in fixture['projects']:
            if 'source_visible_rows' in setup and project['project_id'] not in setup['source_visible_rows']:
                continue
            rows.append({'key': project['project_id'], 'project_ref': project['project_id'],
                'title': project['name'], 'aliases': project.get('aliases', []),
                'business_id': project.get('business_id', ''), 'identity_status': 'verified',
                '项目进展': project.get('progress', ''), '卡点': project.get('blocker', ''),
                '风险': project.get('risk', ''), '待办': project.get('next_step', ''),
                '关键日期': project.get('planned_finish', ''), '人员与职责': project.get('owner', '')})
        self.value = {'version': source['sha256'], 'source_revision': source['sha256'],
            'source': 'frozen-replay:' + source['ref'], 'observed_at': self.now - setup.get('cached_age_s', 0),
            'complete': setup.get('source_complete', fixture.get('complete', False)),
            'stale': False, 'refresh_error': setup.get('source_error'), 'rows': rows}

    def get(self, *, wait):
        if wait is not False:
            raise ContractError('rsi_replay_never_reads_live_source')
        return deepcopy(self.value)


class _FrozenAuthority:
    def __init__(self, event, allowed, policy):
        self.event, self.allowed, self.policy = event, tuple(allowed), policy
        self.harness = None
        self.directory = NS(resolve=lambda actor: self._actor(actor),
            project_ids_for=lambda actor: self.allowed if actor == event.sender_id else ())

    def _actor(self, actor):
        if actor != self.event.sender_id:
            raise ContractError('rsi_replay_actor_changed')
        return actor

    def __call__(self, event):
        if (event.sender_id, event.chat_id, event.tenant_id) != (
                self.event.sender_id, self.event.chat_id, self.event.tenant_id):
            return None
        return self.policy

    def projects_for_chat(self, chat):
        return self.allowed if chat == self.event.chat_id else ()


class _ObservedModel:
    def __init__(self, model):
        self.model, self.requests = model, []
        self.synthetic = getattr(model, 'synthetic', False) is True

    def run(self, request):
        self.requests.append(deepcopy(request))
        return self.model.run(request)

    def model_call_provenance(self, job_id, attempt_id):
        method = getattr(self.model, 'model_call_provenance', None)
        return method(job_id, attempt_id) if callable(method) else {}


def _closed_source_check(text, fixture, allowed, setup):
    """Exact field grammar proves only source projection, never general semantics.

    Every nonempty line must be a source identity or a labelled verbatim field.
    Merely containing a correct keyword, fact, or self-assessed score cannot pass.
    """
    projects = [p for p in fixture['projects'] if p['project_id'] in allowed
        and ('source_visible_rows' not in setup or p['project_id'] in setup['source_visible_rows'])]
    if setup.get('source_error') or setup.get('source_complete', fixture.get('complete')) is not True:
        return {'status': 'needs_review', 'reason': 'source_status_needs_semantic_review'}
    fields = {'进展': ('progress', 'progress'), '卡点': ('blocker', 'blocker'),
        '风险': ('risk', 'risk'), '下一步': ('next_step', 'next_step'), '待办': ('next_step', 'next_step'),
        '计划日期': ('planned_finish', 'plan'), '人员与职责': ('owner', 'owner')}
    selected, covered, selected_ids = None, set(), []
    seen_fields = set()
    for raw in text.splitlines():
        if not raw.strip():
            continue
        line = re.sub(r'^(?:\d+[.、]\s*|[•·*-]\s*)', '', raw.strip())
        matches = [p for p in projects if line in (p['name'], p['name'] + '（按台账记录）')]
        if len(matches) == 1:
            selected = matches[0]
            selected_ids.append(selected['project_id'])
            continue
        if selected is None or '：' not in line:
            return {'status': 'needs_review', 'reason': 'outside_closed_field_grammar'}
        label, value = line.split('：', 1)
        field, subtask = fields.get(label, (None, None))
        if field is None or not isinstance(selected.get(field), str) or not selected[field] or value != selected[field]:
            return {'status': 'needs_review', 'reason': 'field_not_exactly_bound_to_source'}
        identity = (selected['project_id'], field)
        if identity in seen_fields:
            return {'status': 'needs_review', 'reason': 'repeated_field'}
        seen_fields.add(identity)
        covered.add(subtask)
        if field == 'progress' and selected.get('completion_state') == 'marked_complete' and selected.get('acceptance_state') == 'pending':
            covered.add('state_qualification')
    if not seen_fields:
        return {'status': 'needs_review', 'reason': 'no_verified_source_fields'}
    facts = {'project_ids': list(dict.fromkeys(selected_ids))}
    if len(facts['project_ids']) == 1:
        facts['project_id'] = facts['project_ids'][0]
    return {'status': 'pass', 'reason': 'closed_verbatim_source_projection',
        'facts': facts, 'completed_subtasks': sorted(covered), 'proof_scope': 'closed_source_fields_only'}


class FrozenInteractiveReplay:
    """EvolutionEngine replayer(case=..., config=..., context=...) implementation."""
    def __init__(self, *, model, model_id, model_revision, model_binding_ref, sources,
                 evidence_dir, runtime_revision, actual_model=False, root=None,
                 runner_class=InteractiveRunner, runner_options=None):
        if not callable(getattr(model, 'run', None)):
            raise ContractError('rsi_replay_model_adapter_required')
        if not inspect.isclass(runner_class) or not issubclass(runner_class, InteractiveRunner):
            raise ContractError('rsi_replay_interactive_runner_class_required')
        options = {} if runner_options is None else deepcopy(runner_options)
        if not isinstance(options, dict) or set(options) - {'max_context_bytes', 'timeout_seconds'}:
            # Source loaders, active configuration, senders and tools from the
            # live runner must never cross into this isolated construction.
            raise ContractError('rsi_replay_unsafe_runner_options')
        if 'max_context_bytes' in options and (type(options['max_context_bytes']) is not int
                or not 4096 <= options['max_context_bytes'] <= 65536):
            raise ContractError('rsi_replay_invalid_context_budget')
        if 'timeout_seconds' in options and (type(options['timeout_seconds']) not in (int, float)
                or not math.isfinite(options['timeout_seconds']) or not 1 <= options['timeout_seconds'] <= 120):
            raise ContractError('rsi_replay_invalid_runner_timeout')
        if not _sources_unchanged(_LOADED_HASHES):
            raise ContractError('rsi_replay_source_code_changed_after_load')
        self.model = model
        self.runner_class, self.runner_options = runner_class, options
        self.runner_binding = _implementation_binding(runner_class, 'runner')
        self.model_adapter_binding = _implementation_binding(type(model), 'model_adapter')
        self._loaded_hashes = {**_LOADED_HASHES,
            **self.runner_binding['source_file_sha256'],
            **self.model_adapter_binding['source_file_sha256']}
        self.model_id = _text(model_id, 'model_id')
        self.model_revision = _text(model_revision, 'model_revision')
        self.model_binding_ref = _text(model_binding_ref, 'model_binding_ref')
        self.runtime_revision = _text(runtime_revision, 'runtime_revision')
        if type(actual_model) is not bool:
            raise ContractError('rsi_replay_actual_model_must_be_boolean')
        self.actual_model = actual_model and getattr(model, 'synthetic', False) is not True
        self.sources = FrozenSources(sources, root=root)
        self.evidence_dir = Path(evidence_dir).resolve()
        self.evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    def __call__(self, *, case, config, context):
        if context.get('mode') != 'evaluation' or context.get('effects_allowed') is not False:
            raise ContractError('rsi_replay_evaluation_without_effects_required')
        if context.get('layer') not in {'offline', 'real_model'}:
            raise ContractError('rsi_replay_cannot_claim_delivery_or_human_layer')
        if 'oracle' in case or 'expected' in case or 'expected_answer' in case:
            raise ContractError('rsi_replay_worker_must_not_receive_oracle')
        for name in ('case_id', 'channel', 'source_version'):
            if case.get(name) != context.get(name):
                raise ContractError('rsi_replay_case_context_mismatch')
        if case.get('channel') not in {'group', 'private'}:
            raise ContractError('rsi_replay_invalid_channel')
        if set(config) - {'reply.layout', 'reply.max_chars'}:
            raise ContractError('rsi_replay_unconsumed_configuration')
        if 'reply.layout' in config and config['reply.layout'] not in {'compact', 'paragraph'}:
            raise ContractError('rsi_replay_invalid_reply_layout')
        if 'reply.max_chars' in config and (type(config['reply.max_chars']) is not int
                or not 240 <= config['reply.max_chars'] <= 1200):
            raise ContractError('rsi_replay_invalid_reply_length')
        if not _sources_unchanged(self._loaded_hashes):
            raise ContractError('rsi_replay_source_code_changed_after_load')
        started = time.monotonic()
        trial = self.evidence_dir / uuid.uuid4().hex
        trial.mkdir(mode=0o700)
        evidence_ref = str(trial / 'evidence.json')
        record = {'schema_version': SCHEMA_VERSION, **{name: context[name] for name in (
            'case_id', 'channel', 'source_version', 'intent', 'attempt_id', 'layer')},
            'cold_or_warm': case.get('cold_or_warm', 'unknown'),
            'output': {'text': '', 'kind': 'answer', 'facts': {}, 'completed_subtasks': []},
            'outcome': {'status': 'not_run'},
            'execution_provenance': {'actual_assistant_intake': False, 'actual_assistant_run': False,
                'actual_model_call': False, 'actual_realtime_inbound': False, 'actual_feishu_delivery': False,
                'effects_allowed': False, 'sender_bound': False, 'business_writer_bound': False,
                'frozen_fixture_is_not_live_business_source': True,
                'model_id': self.model_id, 'model_revision': self.model_revision,
                'model_binding_ref': self.model_binding_ref, 'runtime_revision': self.runtime_revision,
                'loaded_module_sha256': deepcopy(self._loaded_hashes),
                'runner_binding': deepcopy(self.runner_binding),
                'model_adapter_binding': deepcopy(self.model_adapter_binding),
                'runner_options': deepcopy(self.runner_options), 'execution_ref': evidence_ref}}
        details = {'case': deepcopy(case), 'context': deepcopy(context), 'config': deepcopy(config)}
        model = _ObservedModel(self.model)
        result, store = None, None
        try:
            fixture, source = self.sources.resolve(case)
            record['source_sha256'] = source['sha256']
            record['input_sha256'] = _sha(dumps(case.get('input')).encode())
            details['source_binding'] = source
            setup, input_value = case.get('setup', {}), case.get('input', {})
            if set(setup) - _SUPPORTED_SETUP:
                record['reason'] = 'setup_capability_not_bound'
            elif context['layer'] == 'real_model' and not self.actual_model:
                record['reason'] = 'real_model_binding_unavailable'
            elif input_value.get('history') or input_value.get('attachments'):
                record['reason'] = 'history_or_attachment_replay_not_bound'
            elif (type(context.get('timeout_seconds')) not in (int, float)
                    or not math.isfinite(context['timeout_seconds']) or context['timeout_seconds'] < 1):
                record['reason'] = 'insufficient_replay_budget'
            else:
                timeout = min(120.0, context['timeout_seconds'])
                question = re.sub(r'^@AM\s*', '', _text(input_value.get('text'), 'question'))
                actor = _text(input_value.get('actor_id'), 'actor_id')
                if setup.get('actor', actor) != actor:
                    raise ContractError('rsi_replay_setup_actor_mismatch')
                allowed = set(setup.get('allowed_project_ids', []))
                if case['channel'] == 'group':
                    allowed &= set(setup.get('audience', {}).get('project_ids', []))
                event = Event('rsi-evaluation', _text(input_value.get('origin_event_id'), 'origin_event_id'),
                    _text(input_value.get('conversation_id'), 'conversation_id'), actor, question, time.time(),
                    channel='group' if case['channel'] == 'group' else 'bot_dm',
                    mentions=('rsi-bot',) if case['channel'] == 'group' else ())
                policy = Policy(actor, 'frozen-replay-authority', event.chat_id, (event.chat_id,),
                    tuple(sorted(allowed)), allow_reply=True)
                store = Store(trial / 'run.sqlite')
                receiver = DurableReceiver(store, namespace='rsi-frozen', intents=INTERACTIVE_INTENTS)
                adapter = LocalReceiverAdapter(receiver)
                capabilities = registry()
                for intent in INTERACTIVE_INTENTS:
                    capabilities[intent] = adapter.capability(intent)
                harness = Harness(store, HarnessConfig(event.tenant_id, 'rsi-bot', actor, event.chat_id,
                    dispatch_deadline_seconds=min(5.0, timeout), execution_deadline_seconds=timeout,
                    dispatch_reserve_seconds=min(.1, timeout / 2), capabilities=capabilities),
                    adapters={receiver.namespace: adapter})
                authority = _FrozenAuthority(event, allowed, policy)
                authority.harness = harness
                bridge = NS(am_self=NS(snapshots=_FrozenSnapshot(fixture, setup, source)))
                sources = StudioInteractiveSources(bridge, authority, store,
                    {'ledger_project_refs': {p['project_id']: p['project_id'] for p in fixture['projects']}})
                runner_options = deepcopy(self.runner_options)
                runner_options['timeout_seconds'] = min(timeout, runner_options.get('timeout_seconds', timeout))
                runner = self.runner_class(model, source_loader=sources, model_id=self.model_id,
                    reply_settings_provider=lambda: deepcopy(config), **runner_options)
                harness.ingest(event, policy)
                record['execution_provenance']['actual_assistant_intake'] = True
                task = harness.route_one()
                details['route'] = {k: task.get(k) for k in ('intent', 'route_state', 'task_id')}
                if task.get('route_state') != 'resolved' or task.get('intent') not in INTERACTIVE_INTENTS:
                    record['reason'] = 'interactive_route_not_supported'
                else:
                    harness.register_cutover(task['scope'], evidence_ref='rsi-frozen-no-delivery')
                    dispatched = harness.dispatch_one()
                    if not dispatched or dispatched.get('dispatch_state') != 'accepted':
                        record['reason'] = 'isolated_dispatch_not_accepted'
                    else:
                        pump = ReceiverPump(receiver, runner, harness=harness)
                        details['pump'] = pump.step()
                        record['execution_provenance']['actual_assistant_run'] = True
                        task = harness.task(task['task_id'])
                        details['execution_state'] = task['execution_state']
                        result = json.loads(task['result_json']) if task.get('result_json') else None
                        record['outcome'] = {'status': 'unknown' if task['execution_state'] == 'unknown' else 'error'}
                        if isinstance(result, dict):
                            kind = result.get('response_kind')
                            record['output'].update(text=result.get('reply_text', ''),
                                kind='clarification' if kind == 'clarification' else 'answer')
                            record['outcome'] = {'status': 'completed' if kind in {'sufficient', 'clarification', 'data_gap'}
                                else 'partial' if kind == 'partial' else 'error'}
                            details['actual_result'] = result
                        if task['execution_state'] == 'unknown':
                            record['physical_cancellation_proven'] = False
                        # There is intentionally no RuntimeService, sender, delivery
                        # worker, websocket, production store, or business executor.
                        details['effect_states'] = store.rows('SELECT state FROM h1_effects')
                        if details['effect_states']:
                            raise ContractError('rsi_replay_unexpected_local_effect_record')
                # Independent read after execution; never use a model-supplied
                # snapshot or a renderer-provided pass flag as verification input.
                verification_source, checked_binding = self.sources.resolve(case)
                if checked_binding['sha256'] != source['sha256']:
                    raise ContractError('rsi_replay_verification_source_changed')
                review = _closed_source_check(record['output']['text'], verification_source, allowed, setup)
                details['independent_verification'] = review
                record['semantic_review'] = {'status': review['status'], 'method': 'source_check',
                    'independent': True, 'evidence_ref': evidence_ref,
                    'output_sha256': output_hash(record['output']['text']),
                    'source_version': case['source_version'], 'reason': review['reason'],
                    'human_evaluation_performed': False}
                if review['status'] == 'pass':
                    record['output'].update(facts=review['facts'], completed_subtasks=review['completed_subtasks'])
                record['assertions'] = [{'id': name, 'status': 'pass', 'method': 'deterministic',
                    'evidence_ref': evidence_ref, 'output_sha256': output_hash(record['output']['text']),
                    'source_version': case['source_version']} for name in ('zero_external_effect', 'no_reply_sent')]
                if review['status'] == 'pass':
                    record['assertions'].append({'id': 'same_source_facts', 'status': 'pass', 'method': 'source_check',
                        'evidence_ref': evidence_ref, 'output_sha256': output_hash(record['output']['text']),
                        'source_version': case['source_version']})
        except Exception as exc:
            record['outcome'] = {'status': 'error'}
            record['reason'] = 'replay_error:' + type(exc).__name__
            details['failure_type'] = type(exc).__name__
        ended = time.monotonic()
        if not _sources_unchanged(self._loaded_hashes):
            record['outcome'] = {'status': 'error'}
            record['reason'] = 'replay_source_code_changed_during_execution'
        returned = isinstance(result, dict) and result.get('model_provenance', {}).get('inference_state') == 'returned'
        record['execution_provenance'].update(actual_model_call=self.actual_model and bool(model.requests) and returned,
            model_run_ref=evidence_ref if model.requests else '', model_adapter_attempts=len(model.requests))
        if isinstance(result, dict):
            record['execution_provenance']['result_model_provenance'] = result.get('model_provenance', {})
            selected_model = result.get('model_provenance', {}).get('model_id')
            if selected_model and selected_model != self.model_id:
                # A fallback may be a valid execution, but a revision bound to
                # the primary model is not evidence for the alternate model.
                record['execution_provenance']['selected_model_id'] = selected_model
                record['outcome'] = {'status': 'error'}
                record['reason'] = 'selected_model_revision_not_bound'
        record['timing'] = {'clock_domain': 'host.monotonic:' + str(os.getpid()),
            'events': {'received': started, 'completed': ended},
            'endpoint_scope': 'isolated_interactive_component_not_platform_or_phone'}
        if record['output']['text'] and record['outcome']['status'] == 'completed':
            record['output']['first_substantive_text'] = record['output']['text']
            record['timing']['events']['first_substantive_visible'] = ended
        details['model_requests'] = model.requests
        details['record'] = deepcopy(record)
        _write_private(evidence_ref, details)
        return record


def build_ports(*, bridge, coordinator, settings, root, interactive_runner=None):
    """Bind replay to the final runtime worker; never copy its live source port.

    A supplied runtime runner carries the exact adapter and presentation class
    used by the controlled scope. The provider fallback supports standalone
    experiments only; autonomous bindings must supply the actual runner.
    """
    from integration.assistant_evolution import runtime_source_revision
    config = settings.get('replay', {})
    if not isinstance(config, dict):
        raise ContractError('rsi_replay_settings_must_be_object')
    runner_class, runner_options = InteractiveRunner, {}
    model_id = config.get('model_id')
    if interactive_runner is not None:
        if not isinstance(interactive_runner, InteractiveRunner):
            raise ContractError('rsi_replay_runtime_interactive_runner_required')
        if model_id is not None and model_id != interactive_runner.model_id:
            raise ContractError('rsi_replay_runtime_model_identity_mismatch')
        model, model_id = interactive_runner.model, interactive_runner.model_id
        runner_class = type(interactive_runner)
        runner_options = {'max_context_bytes': interactive_runner.max_context_bytes,
            'timeout_seconds': interactive_runner.timeout_seconds}
    else:
        if settings.get('autonomy', {}).get('enabled') is True:
            raise ContractError('rsi_replay_autonomy_runtime_runner_required')
        provider = getattr(getattr(getattr(bridge, 'poc', None), 'gateway', None), 'provider', None)
        model = HostInteractiveAdapter(provider, max_tokens=config.get('max_tokens', 1800),
            compact_output=config.get('compact_output', True))
    folder = Path(config.get('evidence_dir', 'rsi-private/replay-evidence'))
    if not folder.is_absolute():
        folder = Path(root) / folder
    revision_options = {'interactive_runner': interactive_runner} if interactive_runner is not None else {}
    replay = FrozenInteractiveReplay(model=model, model_id=model_id,
        model_revision=config.get('model_revision'), model_binding_ref=config.get('model_binding_ref'),
        sources=config.get('sources', {}), evidence_dir=folder,
        runtime_revision=runtime_source_revision(**revision_options), actual_model=True, root=root,
        runner_class=runner_class, runner_options=runner_options)
    return {'replayer': replay}
