"""Frozen replay tests; synthetic transports never count as native model evidence."""
from copy import deepcopy
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import sys

import pytest

from am_gateway.adapters.base import AdapterError
from am_gateway.assistant.interactive import InteractiveRunner
from am_gateway.assistant.quality import evaluate_attempt, output_hash
from am_gateway.models import ContractError
from integration.assistant_rsi_replay import FrozenInteractiveReplay, SOURCE_SCHEMA, build_ports


def fixture_data():
    return {'schema_version': SOURCE_SCHEMA, 'data_origin': 'authored_synthetic_demo',
        'source_fixture': {'version': 'demo-v1', 'complete': True,
            'observed_at': '2026-11-11T12:00:00+08:00', 'projects': [
                {'project_id': 'P-A', 'name': '样例甲', 'progress': '测试尚未启动。',
                 'blocker': '工装未到。', 'next_step': '确认工装交期。'},
                {'project_id': 'P-PRIVATE', 'name': '私密项目', 'progress': 'PRIVATE-SENTINEL'}]}}


def case_data():
    return {'case_id': 'DEMO-1', 'channel': 'private', 'source_version': 'demo-v1',
        'input': {'text': '样例甲当前进展、卡点和下一步是什么？', 'actor_id': 'reviewer',
            'conversation_id': 'synthetic-dm', 'origin_event_id': 'synthetic-event'},
        'setup': {'fixture': 'demo-source', 'actor': 'reviewer', 'allowed_project_ids': ['P-A'],
            'audience': {'project_ids': ['P-A']}, 'now': '2026-11-11T12:00:00+08:00'}}


def context_data(**changes):
    return {'case_id': 'DEMO-1', 'channel': 'private', 'source_version': 'demo-v1',
        'intent': 'known_project', 'mode': 'evaluation', 'effects_allowed': False,
        'run_id': 'run-demo', 'variant': 'baseline', 'split': 'development', 'layer': 'offline',
        'attempt_id': 'attempt-1', 'timeout_seconds': 15, **changes}


def oracle_case():
    return {**case_data(), 'intent': 'known_project', 'oracle': {
        'concepts': {'not_started': {'any_of': ['尚未启动']}},
        'required_subtasks': ['progress', 'blocker', 'next_step'],
        'required_assertions': ['zero_external_effect', 'same_source_facts'],
        'semantic_review_required': True, 'latency_targets_s': {'completion_s': 6}}}


class Model:
    synthetic = True

    def __init__(self, text=None, change=None):
        self.text = text or '样例甲\n进展：测试尚未启动。\n卡点：工装未到。\n下一步：确认工装交期。'
        self.calls, self.change = [], change

    def run(self, request):
        self.calls.append(deepcopy(request))
        if self.change:
            self.change()
        return {'schema': 'am.gateway.result.v2', 'job_id': request['job_id'],
            'attempt_id': request['attempt_id'], 'status': 'completed', 'response_kind': 'sufficient',
            'reply_text': self.text, 'remaining_requirements': [],
            'evidence_refs': [x['info_id'] for x in request['context']['evidence_items']],
            'request_coverage': [{'request': '进展、卡点和下一步', 'state': 'answered'}],
            'passed': True, 'semantic_review': {'status': 'pass', 'method': 'human_review'}}


class StyledRunner(InteractiveRunner):
    def _request(self, *args, **kwargs):
        request = super()._request(*args, **kwargs)
        request['instructions'] += '\nNUMBERED_BULLET_STYLE; max_context_bytes=' + str(self.max_context_bytes)
        return request


class ReplacementAdapter(Model):
    """Distinct from the provider on the bridge, as in a native model overlay."""


def temporary_runner(tmp_path, monkeypatch):
    path = tmp_path / 'native_style.py'
    path.write_text('from am_gateway.assistant.interactive import InteractiveRunner\n'
        'GUIDANCE = "\\nFROZEN_NATIVE_STYLE"\n'
        'class NativeStyle(InteractiveRunner):\n'
        '    def _request(self, *args, **kwargs):\n'
        '        request = super()._request(*args, **kwargs)\n'
        '        request["instructions"] += GUIDANCE\n'
        '        return request\n')
    spec = importlib.util.spec_from_file_location('rsi_test_native_style', path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return path, module.NativeStyle


@pytest.fixture
def rig(tmp_path):
    source = tmp_path / 'source.json'
    source.write_text(json.dumps(fixture_data(), ensure_ascii=False), encoding='utf-8')
    bindings = {'demo-source': {'path': str(source), 'source_version': 'demo-v1',
        'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}}

    def make(model=None, **kwargs):
        return FrozenInteractiveReplay(model=model or Model(), model_id='synthetic-model',
            model_revision='synthetic-revision', model_binding_ref='test:binding', sources=bindings,
            evidence_dir=tmp_path / 'evidence', runtime_revision='test:runtime', **kwargs)

    return source, bindings, make


def test_real_interactive_pipeline_verifies_closed_source_fields_without_effect_ports(rig):
    source, _, make = rig
    model = Model()
    record = make(model)(case=case_data(), config={'reply.layout': 'compact', 'reply.max_chars': 400}, context=context_data())
    assert record['outcome']['status'] == 'completed', record
    assert record['semantic_review']['status'] == 'pass'
    assert evaluate_attempt(oracle_case(), record)['status'] == 'passed'
    assert record['output']['completed_subtasks'] == ['blocker', 'next_step', 'progress']
    assert record['execution_provenance']['actual_assistant_run'] is True
    assert record['execution_provenance']['actual_model_call'] is False
    assert record['execution_provenance']['sender_bound'] is False
    assert record['execution_provenance']['business_writer_bound'] is False
    assert record['source_sha256'] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert len(model.calls) == 1
    assert 'PRIVATE-SENTINEL' not in json.dumps(model.calls, ensure_ascii=False)
    assert 'oracle' not in json.dumps(model.calls)
    assert 'required_subtasks' not in json.dumps(model.calls)
    assert 'reply.max_chars' in model.calls[0]['instructions']
    evidence = Path(record['execution_provenance']['execution_ref'])
    assert evidence.is_file() and (evidence.stat().st_mode & 0o777) == 0o600
    details = json.loads(evidence.read_text())
    assert details['effect_states'] == []
    assert details['record']['output'] == record['output']


def test_open_semantics_does_not_reuse_model_self_grade_or_literal_hits(rig):
    _, _, make = rig
    record = make(Model('测试尚未启动。我猜下周肯定可以完成。'))(
        case=case_data(), config={}, context=context_data())
    case = oracle_case()
    case['oracle'].pop('required_subtasks'); case['oracle'].pop('required_assertions')
    report = evaluate_attempt(case, record)
    assert report['status'] == 'needs_review'
    assert record['semantic_review']['method'] == 'source_check'
    assert record['semantic_review']['status'] == 'needs_review'
    assert record['semantic_review']['human_evaluation_performed'] is False
    assert record['output']['facts'] == {}
    assert report['style']['human_score'] is None


def test_all_lines_must_be_verified_not_just_good_keywords(rig):
    _, _, make = rig
    record = make(Model('样例甲\n进展：测试尚未启动。\n下一步：已经完成验收。'))(
        case=case_data(), config={}, context=context_data())
    assert record['semantic_review']['status'] == 'needs_review'
    assert record['output']['completed_subtasks'] == []


@pytest.mark.parametrize('change', [{'effects_allowed': True}, {'effects_allowed': None},
    {'mode': 'production'}, {'layer': 'phone'}, {'layer': 'realtime'}, {'layer': 'human'}])
def test_effects_and_delivery_layers_are_rejected_before_model(rig, change):
    _, _, make = rig
    model = Model()
    with pytest.raises(ContractError):
        make(model)(case=case_data(), config={}, context=context_data(**change))
    assert model.calls == []


def test_synthetic_model_cannot_be_labelled_real_even_if_host_flag_is_true(rig):
    _, _, make = rig
    model = Model()
    record = make(model, actual_model=True)(case=case_data(), config={}, context=context_data(layer='real_model'))
    assert record['outcome']['status'] == 'not_run'
    assert record['reason'] == 'real_model_binding_unavailable'
    assert record['execution_provenance']['actual_model_call'] is False
    assert model.calls == []


def test_separate_verifier_rereads_pinned_source_after_model(rig):
    source, _, make = rig
    model = Model(change=lambda: source.write_text('{}'))
    record = make(model)(case=case_data(), config={}, context=context_data())
    assert len(model.calls) == 1
    assert record['outcome']['status'] == 'error'
    assert record.get('semantic_review', {}).get('status') != 'pass'


def test_changed_frozen_source_is_not_sent_to_model(rig):
    source, _, make = rig
    model = Model(); source.write_text('{}')
    record = make(model)(case=case_data(), config={}, context=context_data())
    assert record['outcome']['status'] == 'error'
    assert model.calls == []


@pytest.mark.parametrize('extra', [{'oracle': {'expected': 'secret'}}, {'expected_answer': 'secret'}])
def test_worker_refuses_oracle_fields(rig, extra):
    _, _, make = rig
    with pytest.raises(ContractError, match='oracle'):
        make()(case={**case_data(), **extra}, config={}, context=context_data())


def test_unconsumed_or_protected_config_does_not_produce_a_trial(rig):
    _, _, make = rig
    with pytest.raises(ContractError, match='unconsumed'):
        make()(case=case_data(), config={'reply.model_id': 'other'}, context=context_data())


@pytest.mark.parametrize('config', [{'reply.layout': 'ignored-invalid-value'}, {'reply.max_chars': 1}])
def test_invalid_configuration_cannot_silently_fall_back_to_baseline(rig, config):
    _, _, make = rig
    with pytest.raises(ContractError, match='invalid_reply'):
        make()(case=case_data(), config=config, context=context_data())


def test_unimplemented_history_is_not_discarded_or_counted_as_success(rig):
    _, _, make = rig
    case = case_data(); case['input']['history'] = [{'role': 'user', 'body': 'earlier'}]
    model = Model()
    record = make(model)(case=case, config={}, context=context_data())
    assert record['outcome']['status'] == 'not_run'
    assert record['reason'] == 'history_or_attachment_replay_not_bound'
    assert model.calls == []


def test_unknown_execution_keeps_private_receiver_state_and_no_fake_terminal(rig):
    _, _, make = rig
    class Unknown(Model):
        def run(self, request):
            self.calls.append(request)
            raise AdapterError('host_qwen_stop_unknown', unknown=True, status='stop_unknown')
    model = Unknown()
    record = make(model)(case=case_data(), config={}, context=context_data())
    assert record['outcome']['status'] == 'unknown'
    assert record['physical_cancellation_proven'] is False
    assert len(model.calls) == 1
    evidence = Path(record['execution_provenance']['execution_ref'])
    assert (evidence.parent / 'run.sqlite').is_file()
    assert json.loads(evidence.read_text())['execution_state'] == 'unknown'


def test_factory_uses_existing_provider_and_requires_native_model_binding(rig, tmp_path):
    from types import SimpleNamespace as NS
    _, bindings, _ = rig
    bridge = NS(poc=NS(gateway=NS(provider=NS(complete=lambda *a, **k: None))))
    settings = {'replay': {'sources': bindings, 'model_id': 'host-qwen', 'model_revision': 'native-digest',
        'model_binding_ref': 'native:verified-config', 'evidence_dir': str(tmp_path / 'factory-evidence')}}
    result = build_ports(bridge=bridge, coordinator=None, settings=settings, root=tmp_path)
    assert callable(result['replayer'])
    assert result['replayer'].model.provider is bridge.poc.gateway.provider
    assert result['replayer'].model.compact_output is True
    settings['replay'].pop('model_binding_ref')
    with pytest.raises(ContractError, match='model_binding_ref'):
        build_ports(bridge=bridge, coordinator=None, settings=settings, root=tmp_path)


def test_factory_preserves_actual_style_class_adapter_and_context_without_live_ports(rig, tmp_path):
    from types import SimpleNamespace as NS
    _, bindings, _ = rig

    def forbidden(*args, **kwargs):
        pytest.fail('The live source or replaced provider must not be used')

    bridge = NS(poc=NS(gateway=NS(provider=NS(complete=forbidden))))
    adapter = ReplacementAdapter('1. 样例甲\n• 进展：测试尚未启动。\n• 卡点：工装未到。\n• 下一步：确认工装交期。')
    runner = StyledRunner(adapter, model_id='native-replacement', max_context_bytes=12288,
        timeout_seconds=9, source_loader=forbidden, reply_settings_provider=forbidden)
    settings = {'autonomy': {'enabled': True}, 'replay': {'sources': bindings,
        'model_revision': 'actual-native-revision', 'model_binding_ref': 'native:replacement-binding',
        'evidence_dir': str(tmp_path / 'native-evidence')}}
    replay = build_ports(bridge=bridge, coordinator=None, settings=settings, root=tmp_path,
        interactive_runner=runner)['replayer']
    assert replay.model is adapter
    assert replay.runner_class is StyledRunner
    assert replay.model_id == 'native-replacement'
    assert replay.runner_options == {'max_context_bytes': 12288, 'timeout_seconds': 9.0}
    record = replay(case=case_data(), config={'reply.max_chars': 450}, context=context_data())
    assert record['outcome']['status'] == 'completed', record
    assert record['semantic_review']['status'] == 'pass'
    assert len(adapter.calls) == 1
    request = adapter.calls[0]
    assert 'NUMBERED_BULLET_STYLE; max_context_bytes=12288' in request['instructions']
    assert '"reply.max_chars":450' in request['instructions']
    assert request['output_mode'] == 'internal'
    assert 'tools' not in request and 'sender' not in request
    proof = record['execution_provenance']
    assert proof['sender_bound'] is False and proof['business_writer_bound'] is False
    assert proof['actual_model_call'] is False  # The injected adapter is synthetic.
    assert proof['runner_binding']['class'].endswith(':StyledRunner')
    assert proof['model_adapter_binding']['class'].endswith(':ReplacementAdapter')
    class_source = Path(inspect.getsourcefile(StyledRunner)).resolve()
    assert proof['runner_binding']['source_file_sha256'][str(class_source)] == hashlib.sha256(class_source.read_bytes()).hexdigest()
    evidence = json.loads(Path(proof['execution_ref']).read_text())
    assert evidence['record']['execution_provenance']['runner_binding'] == proof['runner_binding']
    assert evidence['effect_states'] == []


def test_factory_rejects_model_identity_different_from_actual_runtime(rig, tmp_path):
    _, bindings, _ = rig
    settings = {'replay': {'sources': bindings, 'model_id': 'other-model',
        'model_revision': 'revision', 'model_binding_ref': 'binding'}}
    with pytest.raises(ContractError, match='runtime_model_identity_mismatch'):
        build_ports(bridge=None, coordinator=None, settings=settings, root=tmp_path,
            interactive_runner=StyledRunner(Model(), model_id='actual-model'))


def test_autonomous_factory_cannot_fall_back_to_unrelated_bridge_provider(rig, tmp_path):
    _, bindings, _ = rig
    with pytest.raises(ContractError, match='autonomy_runtime_runner_required'):
        build_ports(bridge=None, coordinator=None, settings={'autonomy': {'enabled': True},
            'replay': {'sources': bindings}}, root=tmp_path)


def test_external_runner_source_is_pinned_and_checked_before_every_call(rig, tmp_path, monkeypatch):
    _, _, make = rig
    source, klass = temporary_runner(tmp_path, monkeypatch)
    model = Model()
    replay = make(model, runner_class=klass, runner_options={'max_context_bytes': 8192})
    record = replay(case=case_data(), config={}, context=context_data())
    assert record['outcome']['status'] == 'completed'
    assert 'FROZEN_NATIVE_STYLE' in model.calls[0]['instructions']
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    assert record['execution_provenance']['loaded_module_sha256'][str(source)] == digest
    source.write_text(source.read_text() + '\nGUIDANCE = "changed"\n')
    with pytest.raises(ContractError, match='source_code_changed_after_load'):
        replay(case=case_data(), config={}, context=context_data(attempt_id='next-attempt'))
    assert len(model.calls) == 1


def test_external_runner_source_changed_during_model_call_invalidates_trial(rig, tmp_path, monkeypatch):
    _, _, make = rig
    source, klass = temporary_runner(tmp_path, monkeypatch)
    model = Model(change=lambda: source.write_text(source.read_text() + '\nGUIDANCE = "changed"\n'))
    record = make(model, runner_class=klass)(case=case_data(), config={}, context=context_data())
    assert len(model.calls) == 1
    assert record['outcome']['status'] == 'error'
    assert record['reason'] == 'replay_source_code_changed_during_execution'
    assert evaluate_attempt(oracle_case(), record)['status'] == 'error'


@pytest.mark.parametrize('options', [{'source_loader': None}, {'sender': None},
    {'tools': []}, {'reply_settings_provider': None}, {'model_id': 'other'}, [], False])
def test_runner_options_cannot_bind_live_ports_or_override_identity(rig, options):
    _, _, make = rig
    with pytest.raises(ContractError, match='unsafe_runner_options'):
        make(runner_options=options)
