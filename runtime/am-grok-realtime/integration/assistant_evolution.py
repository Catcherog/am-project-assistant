"""Bind evidence-driven configuration experiments to the low-priority lane.

Native replay executes candidates and independently verifies their source and
result. An absent port is never replaced with a model's claimed score.
"""
from __future__ import annotations

import importlib
import hashlib
import inspect
import json
from pathlib import Path
import threading
import time
import uuid

from am_gateway.assistant.evolution import EvolutionEngine, ReplayBudget
from am_gateway.assistant.intelligence import AdaptiveReplyIntelligence, IntelligentProjectQueries
from am_gateway.assistant.quality import load_suite
from am_gateway.models import ContractError, dumps, key


def runtime_source_revision(root=None, *, interactive_runner=None):
    """Bind experiments to the Python and evaluation bytes loaded by this tree.

    This is a content identity, not a claim that a deployment has been tested.
    Runtime state, credentials and private workspaces are never scanned.
    """
    root = Path(root or Path(__file__).resolve().parents[1]).resolve()
    digest = hashlib.sha256()
    files = []
    for folder in ('src', 'integration', 'evaluation'):
        directory = root / folder
        if directory.is_dir():
            files.extend(p for p in directory.rglob('*')
                         if p.is_file() and p.suffix in ('.py', '.json', '.md'))
    if not files:
        raise ContractError('evolution_runtime_source_unavailable')
    for path in sorted(files):
        digest.update(path.relative_to(root).as_posix().encode() + b'\0')
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    # A native style subclass can live in the controlled overlay outside this
    # package. Bind the actual loaded class chain as part of the experiment,
    # rather than evaluating the core class and promoting into another prompt.
    if interactive_runner is not None:
        for klass in type(interactive_runner).__mro__:
            if klass is object:
                continue
            digest.update((klass.__module__ + ':' + klass.__qualname__).encode() + b'\0')
            try:
                source = inspect.getsourcefile(klass)
            except TypeError:
                source = None
            if source and Path(source).is_file():
                digest.update(hashlib.sha256(Path(source).read_bytes()).digest())
    return 'sha256:' + digest.hexdigest()


class HostEvolutionProposer:
    def __init__(self, provider):
        self.provider = provider

    def __call__(self, **request):
        examples, size = [], 0
        for case in request['development_cases']:
            encoded = dumps(case)
            if size + len(encoded.encode()) > 16000:
                break
            examples.append(case); size += len(encoded.encode())
        data = {name: request[name] for name in ('proposal', 'observations', 'feedback',
                'baseline_config', 'allowed_config_keys', 'known_model_ids')}
        data['development_cases'] = examples
        prompt = (
            '你只为AM提出一个局部配置改进候选，返回JSON {"patch":{...},"rationale":"..."}。'
            '只可使用allowed_config_keys和已知model_id。依据实际失败/体验原话定位改进。'
            '事实准确性优先，不可修改事实、权限、收件人、执行结果、评测答案或评分器。'
            '本次不执行命令、不发送消息、不训练模型。不要声称改进已经通过。数据：\n' + dumps(data))
        value = self.provider.complete(prompt, run_id=uuid.uuid4().hex,
            timeout=request['timeout_seconds'], native_chat=True, max_tokens=512)
        return json.loads(value.text)


class BackgroundEvolution:
    def __init__(self, engine, coordinator, *, scope, development_ref, holdout_ref=None,
                 interval_seconds=1800, budget=None):
        if type(interval_seconds) not in (int, float) or not 60 <= interval_seconds <= 86400:
            raise ContractError('evolution_interval_out_of_range')
        self.engine, self.coordinator, self.scope = engine, coordinator, scope
        self.development_ref, self.holdout_ref = development_ref, holdout_ref
        self.interval, self.budget = interval_seconds, ReplayBudget(**(budget or {})).validate()
        self._lock, self._next = threading.Lock(), 0.0
        self.last = {'state': 'not_started'}
        self.runtime_consumers = []

    def step(self):
        if time.monotonic() < self._next or not self._lock.acquire(blocking=False):
            return None
        try:
            pending = self.coordinator.store.one("""SELECT count(*) AS n FROM r5_intake
                WHERE state IN ('pending','processing','source_wait')""")
            if pending and pending['n']:
                self.last = {'state': 'deferred_for_interactive_work'}
                return self.last
            active = self.coordinator.store.one("""SELECT count(*) AS n FROM h1_tasks
                WHERE dispatch_state IN ('pending','dispatching','accepted','unknown')
                  AND execution_state NOT IN ('succeeded','failed','cancelled')""")
            if active and active['n']:
                self.last = {'state': 'deferred_for_interactive_work'}
                return self.last
            self._next = time.monotonic() + self.interval
            if not callable(self.engine.replayer):
                self.last = {'state': 'unbound', 'missing_bindings': ['replayer'],
                             'proposal_model_called': False}
                return self.last
            try:
                self.last = self.engine.one_cycle(self.coordinator.improvement, scope=self.scope,
                    development_ref=self.development_ref, holdout_ref=self.holdout_ref, budget=self.budget)
            except Exception as exc:
                # Background experiments must not stop the report/feedback lane
                # or persist credentials carried in exception messages.
                self.last = {'state': 'cycle_failed', 'exception_type': type(exc).__name__}
            return self.last
        finally:
            self._lock.release()

    def status(self):
        return {**self.engine.status(), 'last_cycle': self.last,
                'interval_seconds': self.interval, 'scope': self.scope,
                'runtime_revision': self.engine.runtime_revision,
                'runtime_consumers': self.runtime_consumers,
                'production_deployment_performed': False}


def bind_evolution(coordinator, bridge, settings, root, *, interactive_runner=None):
    config = settings.get('evolution', {})
    if not isinstance(config, dict):
        raise ContractError('assistant_evolution_settings_must_be_object')
    if config.get('enabled', False) is not True:
        return None
    autonomy = config.get('autonomy', {})
    if not isinstance(autonomy, dict):
        raise ContractError('assistant_autonomy_settings_must_be_object')
    autonomous = autonomy.get('enabled', False) is True
    consumers = []
    consumed_keys = set()
    if isinstance(coordinator.project_reader, IntelligentProjectQueries):
        consumers.append('source_bound_reply_planner')
        consumed_keys.update(('reply.layout', 'reply.model_budget_seconds', 'reply.cache_ttl_seconds'))
    if interactive_runner is not None:
        if not hasattr(interactive_runner, 'reply_settings_provider'):
            raise ContractError('interactive_configuration_consumer_unbound')
        consumers.append('interactive_semantic_runner')
        consumed_keys.update(('reply.layout', 'reply.max_chars'))
    if not consumers:
        raise ContractError('evolution_requires_runtime_configuration_consumer')
    if autonomous:
        if interactive_runner is None:
            raise ContractError('rsi_autonomy_requires_semantic_consumer')
        # The CLI may tune only settings the current semantic path consumes.
        # Model/provider replacement is a code experiment, not a hidden pointer.
        consumed_keys = {'reply.layout', 'reply.max_chars'}
        if not config.get('development_path'):
            raise ContractError('rsi_dedicated_development_suite_required')
    intelligence = settings.get('intelligence', {})
    model_id = intelligence.get('model_id', 'host_qwen')
    scope = config.get('scope', 'assistant')
    provider = getattr(getattr(getattr(bridge, 'poc', None), 'gateway', None), 'provider', None)
    ports = {}
    factory = config.get('ports_factory') or (
        'integration.assistant_rsi_replay:build_ports' if autonomous else None)
    if factory:
        if not isinstance(factory, str) or ':' not in factory:
            raise ContractError('evolution_ports_factory_must_be_host_callable')
        module, name = factory.split(':', 1)
        factory_fn = getattr(importlib.import_module(module), name)
        extra = {'interactive_runner': interactive_runner} if (
            'interactive_runner' in inspect.signature(factory_fn).parameters) else {}
        ports = factory_fn(bridge=bridge, coordinator=coordinator, settings=config, root=root, **extra)
        if not isinstance(ports, dict):
            raise ContractError('evolution_ports_factory_must_return_object')
    development_path = Path(config.get('development_path') or
        Path(__file__).resolve().parents[1] / 'evaluation' / 'r7_cases.json')
    resolved_development = development_path if development_path.is_absolute() else Path(root) / development_path
    development_cases = load_suite(resolved_development)['cases']
    if autonomous and any(case.get('required_layers') != ['real_model'] for case in development_cases):
        raise ContractError('rsi_development_suite_must_scope_actual_model_only')
    refs = {'development': config.get('development_ref') or 'r7:development:' + key(development_cases)}
    paths = {refs['development']: development_path}
    if config.get('holdout_path'):
        holdout_path = Path(config['holdout_path'])
        resolved_holdout = holdout_path if holdout_path.is_absolute() else Path(root) / holdout_path
        refs['holdout'] = config.get('holdout_ref') or 'r7:holdout:' + key(load_suite(resolved_holdout)['cases'])
        paths[refs['holdout']] = holdout_path

    def loader(ref):
        if ref not in paths:
            raise ContractError('unknown_frozen_suite_reference')
        path = paths[ref]
        path = path if path.is_absolute() else Path(root) / path
        return load_suite(path)['cases']

    proposer = ports.get('proposer') or (HostEvolutionProposer(provider)
        if callable(getattr(provider, 'complete', None)) else None)
    cli, work_root = None, None
    if autonomous:
        from am_gateway.assistant.cli_worker import ManagedCLI, CLIProposalWorker
        cli = ManagedCLI(**autonomy.get('cli', {}))
        source_tree = Path(__file__).resolve().parents[1]
        work_root = Path(autonomy.get('work_root') or source_tree.parent / (source_tree.name + '-rsi-private'))
        if not work_root.is_absolute():
            work_root = Path(root) / work_root
        work_root = work_root.resolve()
        proposer = CLIProposalWorker(cli, work_root=work_root / 'proposals',
            timeout_seconds=autonomy.get('proposal_timeout_seconds', 90))
        if not callable(ports.get('replayer')):
            raise ContractError('rsi_actual_replay_worker_required')
    policy = config.get('promotion_policy', 'relative_quality' if autonomous else 'absolute_release')
    controlled = tuple(autonomy.get('controlled_scopes', ())) if autonomous else ()
    if autonomous and scope not in controlled:
        raise ContractError('rsi_current_controlled_scope_required')
    engine = EvolutionEngine(coordinator.store, coordinator.owner_id,
        proposer=proposer,
        replayer=ports.get('replayer'), suite_loader=loader,
        known_model_ids=(model_id,),
        allowed_config_keys=consumed_keys,
        enabled_scopes=(scope,) if config.get('automatic_config_activation', False) is True else (),
        evaluation_layer=config.get('evaluation_layer', 'real_model'),
        host_source_freshness_seconds=settings.get('fast_reads', {}).get('max_snapshot_age_seconds', 60),
        runtime_revision=runtime_source_revision(interactive_runner=interactive_runner), promotion_policy=policy,
        relative_quality_scopes=controlled)
    if engine.active_config(scope) is None:
        baseline = {'reply.layout': intelligence.get('layout', 'paragraph'),
            'reply.max_chars': intelligence.get('max_chars', 600), 'reply.model_id': model_id,
            'reply.model_budget_seconds': intelligence.get('model_budget_seconds', 1.5),
            'reply.cache_ttl_seconds': intelligence.get('cache_ttl_seconds', 60)}
        engine.register_baseline(scope, {k: v for k, v in baseline.items() if k in consumed_keys})
    engine.freeze_suite(refs['development'], loader(refs['development']), split='development')
    if 'holdout' in refs:
        engine.freeze_suite(refs['holdout'], loader(refs['holdout']), split='holdout',
                            independence_ref=config.get('holdout_independence_ref', ''))
    reader = coordinator.project_reader
    if isinstance(reader, IntelligentProjectQueries):
        reader.intelligence = AdaptiveReplyIntelligence(reader.intelligence,
            lambda: engine.active_config(scope))
    if interactive_runner is not None:
        interactive_runner.reply_settings_provider = lambda: engine.active_config(scope)
    if autonomous:
        from am_gateway.assistant.autonomy import AutonomousRSI
        code_worker = None
        code_settings = autonomy.get('code', {})
        if code_settings.get('enabled', False) is True:
            from am_gateway.assistant.code_optimizer import CodeCandidateWorker
            code_worker = CodeCandidateWorker(cli, source_root=Path(__file__).resolve().parents[1],
                output_root=work_root / 'code', **{k: v for k, v in code_settings.items() if k != 'enabled'})
        result = AutonomousRSI(engine, coordinator, scope=scope, development_ref=refs['development'],
            holdout_ref=refs.get('holdout'), code_worker=code_worker,
            budget=autonomy.get('budget'), replay_budget=config.get('budget') or
                {'max_calls': 80, 'max_seconds': 1800, 'per_call_seconds': 60, 'repetitions': 1})
        proposer.cancelled = lambda: bool(engine.should_defer({})
            if callable(engine.should_defer) else result.paused())
    else:
        result = BackgroundEvolution(engine, coordinator, scope=scope,
            development_ref=refs['development'], holdout_ref=refs.get('holdout'),
            interval_seconds=config.get('interval_seconds', 1800), budget=config.get('budget'))
    result.runtime_consumers = consumers
    return result
