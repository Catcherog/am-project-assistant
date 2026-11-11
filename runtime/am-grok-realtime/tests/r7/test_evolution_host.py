from pathlib import Path
import json
import sys
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests' / 'r5'))
sys.path.insert(0, str(ROOT / 'tests' / 'r6'))
from test_assistant_coordinator import rig, message
from test_fast_path_integration import bind
from am_gateway.models import ContractError
from am_gateway.assistant.intelligence import AdaptiveReplyIntelligence
from integration.assistant_intelligence import bind_reply_intelligence
from integration.assistant_evolution import bind_evolution, runtime_source_revision


def enabled(rig, **extra):
    _, reader = bind(rig)
    settings = {'intelligence': {'enabled': True}, 'evolution': {'enabled': True, **extra}}
    rig.c.project_reader = bind_reply_intelligence(reader, NS(), rig.store, rig.cfg.owner_id, settings)
    return settings


def test_actual_source_content_changes_invalidate_experiment_revision(tmp_path):
    folder = tmp_path / 'src'; folder.mkdir()
    code = folder / 'main.py'; code.write_text('x = 1\n')
    before = runtime_source_revision(tmp_path)
    (tmp_path / 'private-state.json').write_text('not scanned')
    assert runtime_source_revision(tmp_path) == before
    code.write_text('x = 2\n')
    assert runtime_source_revision(tmp_path) != before


def test_missing_runtime_consumer_cannot_enable_self_improvement(rig, tmp_path):
    with pytest.raises(ContractError, match='runtime_configuration_consumer'):
        bind_evolution(rig.c, NS(), {'evolution': {'enabled': True}}, tmp_path)


def test_binding_freezes_development_and_consumes_config_without_fake_worker(rig, tmp_path):
    runner = NS(reply_settings_provider=None)
    evolution = bind_evolution(rig.c, NS(), enabled(rig), tmp_path, interactive_runner=runner)
    assert isinstance(rig.c.project_reader.intelligence, AdaptiveReplyIntelligence)
    assert runner.reply_settings_provider()['config']['reply.layout'] == 'paragraph'
    status = evolution.status()
    assert status['runtime_revision'].startswith('sha256:')
    assert status['bindings'] == {'proposer': False, 'replayer': False, 'suite_loader': True}
    assert status['production_deployment_performed'] is False
    assert evolution.step()['state'] == 'unbound'


def test_background_experiment_yields_to_interactive_and_redacts_failure(rig, tmp_path):
    evolution = bind_evolution(rig.c, NS(), enabled(rig), tmp_path)
    rig.c.capture(message(rig))
    assert evolution.step()['state'] == 'deferred_for_interactive_work'
    with rig.store.transaction() as db:
        db.execute("UPDATE r5_intake SET state='completed'")
    def broken(*args, **kwargs):
        raise RuntimeError('do not persist credentials here')
    evolution.engine.replayer = lambda **kwargs: None
    evolution.engine.one_cycle = broken
    assert evolution.step() == {'state': 'cycle_failed', 'exception_type': 'RuntimeError'}
    assert 'credentials' not in str(evolution.status())


def test_unconsumed_length_setting_is_not_offered_to_fact_only_planner(rig, tmp_path):
    evolution = bind_evolution(rig.c, NS(), enabled(rig), tmp_path)
    assert 'reply.max_chars' not in evolution.engine.allowed_config_keys
    with pytest.raises(ContractError, match='protected or unknown'):
        evolution.engine._validate_config({'reply.max_chars': 240}, patch=True)


def test_semantic_only_binding_offers_only_real_runtime_consumers(rig, tmp_path):
    runner = NS(reply_settings_provider=None)
    settings = {'project_answer_mode': 'semantic', 'evolution': {'enabled': True}}
    evolution = bind_evolution(rig.c, NS(), settings, tmp_path, interactive_runner=runner)
    assert evolution.engine.allowed_config_keys == {'reply.layout', 'reply.max_chars'}
    assert set(runner.reply_settings_provider()['config']) == {'reply.layout', 'reply.max_chars'}


def test_updated_development_content_creates_new_ref_preserving_frozen_suite(rig, tmp_path):
    path = tmp_path / 'development.json'
    suite = json.loads((ROOT / 'evaluation' / 'r7_cases.json').read_text())
    path.write_text(json.dumps(suite))
    settings = enabled(rig, development_path=str(path))
    first = bind_evolution(rig.c, NS(), settings, tmp_path)
    previous = first.development_ref
    suite['cases'][0]['input']['text'] += ' 请按当前来源答复。'
    path.write_text(json.dumps(suite))
    # Recreate the root wrapper as a real process restart does.
    settings = enabled(rig, development_path=str(path))
    second = bind_evolution(rig.c, NS(), settings, tmp_path)
    assert second.development_ref != previous
    assert rig.store.one('SELECT 1 FROM r7_evo_suites WHERE suite_ref=?', (previous,))
