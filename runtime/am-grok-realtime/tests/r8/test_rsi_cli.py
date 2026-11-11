from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest


def tool():
    path = Path(__file__).resolve().parents[2] / 'tools/am_rsi.py'
    spec = importlib.util.spec_from_file_location('am_rsi_cli_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Clock:
    def __init__(self):
        self.now = 0
        self.waits = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= .25
        self.waits.append(seconds)
        self.now += seconds


def test_one_shot_process_observes_idle_window_before_starting_job():
    clock = Clock()
    class Service:
        calls = 0
        def step(self, *, force):
            assert force is True
            self.calls += 1
            return {'state': 'waiting_for_idle_window'} if clock() < 30 else {'state': 'completed', 'job_id': 'job-1'}
    service = Service()
    value = tool().run_once_with_idle_wait(service, monotonic=clock, sleep=clock.sleep)
    assert value == {'state': 'completed', 'job_id': 'job-1'}
    assert clock() == 30
    assert service.calls == 121


@pytest.mark.parametrize('state', ['busy', 'paused', 'deferred_for_interactive_work',
    'needs_reconciliation', 'daily_job_budget_reached', 'daily_time_budget_reached', 'idle'])
def test_other_stop_decisions_are_returned_without_polling(state):
    clock = Clock()
    class Service:
        calls = 0
        def step(self, *, force):
            self.calls += 1
            return {'state': state}
    service = Service()
    assert tool().run_once_with_idle_wait(service, monotonic=clock, sleep=clock.sleep) == {'state': state}
    assert service.calls == 1 and clock.waits == []


def test_foreground_work_arriving_during_idle_wait_returns_immediately():
    clock = Clock()
    class Service:
        def step(self, *, force):
            return {'state': 'waiting_for_idle_window' if clock() < 1 else 'deferred_for_interactive_work'}
    assert tool().run_once_with_idle_wait(Service(), monotonic=clock, sleep=clock.sleep)['state'] == 'deferred_for_interactive_work'
    assert clock() == 1


def test_longer_idle_requirement_does_not_wait_forever_or_force_admission():
    clock = Clock()
    class Service:
        def step(self, *, force):
            return {'state': 'waiting_for_idle_window'}
    value = tool().run_once_with_idle_wait(Service(), monotonic=clock, sleep=clock.sleep)
    assert value['state'] == 'waiting_for_idle_window'
    assert value['idle_wait_expired'] is True
    assert value['job_started_by_cli'] is False
    assert clock() == 45


@pytest.mark.parametrize('seconds', [-1, 61, float('nan'), True])
def test_invalid_cli_idle_wait_does_not_start_service(seconds):
    class Service:
        def step(self, *, force):
            raise AssertionError('should not call')
    with pytest.raises(ValueError):
        tool().run_once_with_idle_wait(Service(), wait_idle_seconds=seconds)


def _database(path):
    db = sqlite3.connect(path)
    db.executescript('''CREATE TABLE r8_rsi_control(owner_id,scope,paused,updated);
      INSERT INTO r8_rsi_control VALUES('owner','controlled',0,0);
      CREATE TABLE r8_rsi_jobs(owner_id,scope,kind,state,result_json,result_hash,updated);''')
    db.commit(); db.close()


def test_direct_reports_script_imports_project_source_without_install_or_pythonpath(tmp_path):
    path = tmp_path / 'state.sqlite'; _database(path)
    root = Path(__file__).resolve().parents[2]
    environment = {k: v for k, v in os.environ.items() if k not in {'PYTHONPATH', 'PYTHONHOME'}}
    result = subprocess.run([sys.executable, str(root / 'tools/am_rsi.py'),
        '--database', str(path), '--owner', 'owner', '--scope', 'controlled', 'reports'],
        cwd=tmp_path, env=environment, text=True, capture_output=True, timeout=5)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == []


def test_direct_run_once_imports_integration_factory_and_keeps_identity_binding(tmp_path):
    root = tmp_path / 'project'; (root / 'tools').mkdir(parents=True)
    (root / 'integration').mkdir()
    (root / 'integration/__init__.py').write_text('')
    original = Path(__file__).resolve().parents[2] / 'tools/am_rsi.py'
    (root / 'tools/am_rsi.py').write_text(original.read_text())
    db = tmp_path / 'state.sqlite'; _database(db)
    (root / 'integration/local_factory.py').write_text(
        'from types import SimpleNamespace as NS\n'
        'def build():\n'
        '    return NS(store=NS(path=' + repr(str(db)) + '),owner_id="owner",scope="controlled",'
        'step=lambda **kwargs: {"state":"idle"})\n')
    environment = {k: v for k, v in os.environ.items() if k not in {'PYTHONPATH', 'PYTHONHOME'}}
    command = [sys.executable, str(root / 'tools/am_rsi.py'), '--database', str(db),
        '--owner', 'owner', '--scope', 'controlled', 'run-once',
        '--host-factory', 'integration.local_factory:build']
    result = subprocess.run(command, cwd=tmp_path, env=environment, text=True, capture_output=True, timeout=5)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {'state': 'idle'}
    command[command.index('controlled')] = 'wrong-scope'
    result = subprocess.run(command, cwd=tmp_path, env=environment, text=True, capture_output=True, timeout=5)
    assert result.returncode == 2
    assert 'identity' not in result.stdout
