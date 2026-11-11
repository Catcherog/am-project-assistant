import json
from pathlib import Path
import sys
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'r5'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'r7'))
from test_assistant_coordinator import rig
from test_evolution import cases
from am_gateway.models import ContractError
from integration.assistant_evolution import bind_evolution, runtime_source_revision


def settings(tmp_path):
    suite={'schema_version':'am.assistant.quality.v2','cases':cases('dev',2,'development')}
    for case in suite['cases']:
        case['required_layers']=['real_model']
    # load_suite accepts cases under the current schema_version identifier.
    from am_gateway.assistant.quality import SCHEMA_VERSION
    suite['schema_version']=SCHEMA_VERSION
    path=tmp_path/'dedicated-development.json';path.write_text(json.dumps(suite))
    return {'project_answer_mode':'semantic','evolution':{'enabled':True,'scope':'controlled',
        'development_path':str(path),'evaluation_layer':'real_model',
        'ports_factory':'r8_test_ports:build','automatic_config_activation':True,
        'autonomy':{'enabled':True,'controlled_scopes':['controlled'],
            'work_root':str(tmp_path/'private-jobs'),
            'cli':{'executable':sys.executable,'kind':'json_stdin','argv':['-c','print("{}")']}}}}


def install_ports(monkeypatch, seen):
    def factory(*,bridge,coordinator,settings,root,interactive_runner):
        seen.append(interactive_runner)
        return {'replayer':lambda **kwargs: None}
    monkeypatch.setitem(sys.modules,'r8_test_ports',NS(build=factory))


def test_binds_autonomy_to_actual_runtime_runner_and_existing_lane(rig,tmp_path,monkeypatch):
    seen=[];install_ports(monkeypatch,seen)
    runner=NS(reply_settings_provider=None)
    service=bind_evolution(rig.c,NS(),settings(tmp_path),tmp_path,interactive_runner=runner)
    assert seen==[runner]
    assert service.status()['autonomous'] is True
    assert service.engine.allowed_config_keys=={'reply.layout','reply.max_chars'}
    assert service.engine.promotion_policy=='relative_quality'
    assert runner.reply_settings_provider()['config']=={'reply.layout':'paragraph','reply.max_chars':600}
    assert service.engine.replayer is not None
    assert service.engine.proposer.cancelled() is False
    service.set_paused(True)
    assert service.engine.proposer.cancelled() is True


def test_existing_global_suite_is_not_silently_relabelled_for_autonomy(rig,tmp_path,monkeypatch):
    install_ports(monkeypatch,[])
    config=settings(tmp_path)
    path=Path(config['evolution']['development_path'])
    suite=json.loads(path.read_text());suite['cases'][0]['required_layers']=['real_model','phone']
    path.write_text(json.dumps(suite))
    with pytest.raises(ContractError,match='scope_actual_model'):
        bind_evolution(rig.c,NS(),config,tmp_path,interactive_runner=NS(reply_settings_provider=None))


def test_autonomy_requires_explicit_controlled_scope(rig,tmp_path,monkeypatch):
    install_ports(monkeypatch,[])
    config=settings(tmp_path);config['evolution']['autonomy']['controlled_scopes']=[]
    with pytest.raises(ContractError,match='controlled_scope'):
        bind_evolution(rig.c,NS(),config,tmp_path,interactive_runner=NS(reply_settings_provider=None))


def test_runtime_overlay_style_file_is_part_of_revision(tmp_path,monkeypatch):
    source=tmp_path/'overlay_style.py'
    source.write_text('class StyleRunner:\n    def extra(self):\n        return "numbered"\n')
    import importlib.util
    spec=importlib.util.spec_from_file_location('r8_test_style',source)
    module=importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules,'r8_test_style',module);spec.loader.exec_module(module)
    runner=module.StyleRunner()
    baseline=runtime_source_revision(interactive_runner=runner)
    source.write_text('class StyleRunner:\n    def extra(self):\n        return "bullets"\n')
    assert runtime_source_revision(interactive_runner=runner)!=baseline
