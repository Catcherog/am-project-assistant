from pathlib import Path
import json
import os
import sys
import time

import pytest

from am_gateway.assistant.cli_worker import ManagedCLI, CLIProposalWorker, run_process
from am_gateway.models import ContractError


def test_subprocess_receives_literal_stdin_and_no_shell(tmp_path):
    value = '$(touch SHOULD_NOT_EXIST); `echo secret`\n中文'
    result = run_process([sys.executable, '-c', 'import sys;print(sys.stdin.read())'],
        workspace=tmp_path, timeout_seconds=5, stdin_text=value)
    assert result['state'] == 'completed'
    assert result['stdout'].strip() == value
    assert not (tmp_path / 'SHOULD_NOT_EXIST').exists()


def test_git_redirect_environment_is_not_inherited(tmp_path, monkeypatch):
    monkeypatch.setenv('GIT_DIR', '/other/project/.git')
    monkeypatch.setenv('GIT_CONFIG_COUNT', '1')
    result = run_process([sys.executable, '-c', 'import os;print(os.getenv("GIT_DIR"));print(os.getenv("GIT_CONFIG_COUNT"))'],
        workspace=tmp_path, timeout_seconds=5)
    assert result['stdout'] == 'None\nNone\n'


def test_timeout_kills_local_process_and_does_not_claim_remote_stop(tmp_path):
    result = run_process([sys.executable, '-c', 'import time;time.sleep(60)'],
        workspace=tmp_path, timeout_seconds=.12)
    assert result['state'] == 'timeout'
    assert result['elapsed_seconds'] < 4
    assert result['local_process_stopped'] is True
    assert result['provider_cancellation_proven'] is False


def test_cancellation_before_start_does_not_spawn(tmp_path):
    result = run_process([sys.executable, '-c', 'raise SystemExit(1)'],
        workspace=tmp_path, timeout_seconds=5, cancelled=lambda: True)
    assert result['state'] == 'cancelled' and not result['process_started']


def test_output_limit_rejects_process(tmp_path):
    result = run_process([sys.executable, '-c', 'print("x"*100000)'],
        workspace=tmp_path, timeout_seconds=5, max_output_bytes=1024)
    assert result['state'] == 'output_limit'
    assert len(result['stdout']) <= 1024


def test_signal_permission_failure_preserves_unknown(monkeypatch):
    from am_gateway.assistant.cli_worker import _stop_group
    from types import SimpleNamespace
    process = SimpleNamespace(pid=12345, returncode=0,
        poll=lambda: 0, wait=lambda timeout: 0)
    def denied(*args):
        raise PermissionError('signal denied')
    monkeypatch.setattr(os, 'killpg', denied)
    assert _stop_group(process) is False


def test_exited_group_is_verified_after_signal_failure(monkeypatch):
    from am_gateway.assistant.cli_worker import _stop_group
    from types import SimpleNamespace
    process = SimpleNamespace(pid=12345, returncode=0,
        poll=lambda: 0, wait=lambda timeout: 0)
    def gone(pid, sig):
        if sig == 0:
            raise ProcessLookupError()
        raise PermissionError('leader exited during signal')
    monkeypatch.setattr(os, 'killpg', gone)
    assert _stop_group(process) is True


def test_completed_cli_with_unresolved_group_is_not_success(tmp_path, monkeypatch):
    import am_gateway.assistant.cli_worker as worker
    monkeypatch.setattr(worker, '_stop_group', lambda process: (process.wait(), False)[1])
    result = run_process([sys.executable, '-c', 'print("finished")'],
        workspace=tmp_path, timeout_seconds=5)
    assert result['state'] == 'stop_unknown'
    assert result['local_process_stopped'] is False


def test_json_wrapper_uses_real_process(tmp_path):
    cli = ManagedCLI(executable=sys.executable, kind='json_stdin',
        argv=['-c', 'import json,sys;print(json.dumps({"text":sys.stdin.read()}))'])
    result = cli.run('literal prompt', tmp_path, 5, schema={'type': 'object'})
    assert result['state'] == 'completed'
    assert result['json']['text'] == 'literal prompt'


def test_malformed_cli_output_rejected(tmp_path):
    cli = ManagedCLI(executable=sys.executable, kind='json_stdin', argv=['-c', 'print("bad")'])
    result = cli.run('x', tmp_path, 5, schema={'type': 'object'})
    assert result['state'] == 'failed' and result['error_type'] == 'InvalidJSON'


def proposal_request():
    return {'proposal': {'issues': [{'code': 'latency_review'}]}, 'observations': [], 'feedback': [],
        'baseline_config': {'reply.layout': 'paragraph', 'reply.max_chars': 600},
        'development_cases': [{'input': {'text': '合成开发问题'}, 'oracle': {'state': 'pending'}}],
        'allowed_config_keys': ['reply.layout', 'reply.max_chars'], 'known_model_ids': ['host_qwen'],
        'timeout_seconds': 5}


class ProposalCLI:
    def __init__(self, value):
        self.value = value
        self.seen = None

    def run(self, prompt, workspace, timeout_seconds, **kwargs):
        self.seen = (prompt, kwargs)
        return {'state': 'completed', 'output': json.dumps(self.value), 'json': self.value,
                'local_process_stopped': True, 'provider_cancellation_proven': False, 'elapsed_seconds': .01}


def test_proposer_is_data_only_and_writes_receipt(tmp_path):
    cli = ProposalCLI({'changes': [{'key': 'reply.layout', 'value_json': '"compact"'}], 'rationale': '尝试简洁表达'})
    worker = CLIProposalWorker(cli, work_root=tmp_path)
    result = worker(**proposal_request())
    assert result['patch'] == {'reply.layout': 'compact'}
    assert cli.seen[1]['write'] is False
    assert 'holdout' not in cli.seen[0]
    assert len(list(tmp_path.glob('proposal-*/receipt.json'))) == 1


@pytest.mark.parametrize('changes', [
    [{'key': 'business.allow_send', 'value_json': 'true'}],
    [{'key': 'reply.layout', 'value_json': '"compact"'}, {'key': 'reply.layout', 'value_json': '"paragraph"'}],
    [{'key': 'reply.layout', 'value_json': 'not json'}],
])
def test_proposer_cannot_add_permission_keys_or_ambiguous_values(tmp_path, changes):
    worker = CLIProposalWorker(ProposalCLI({'changes': changes, 'rationale': 'x'}), work_root=tmp_path)
    with pytest.raises(ContractError):
        worker(**proposal_request())


def test_proposer_accepts_no_improvement(tmp_path):
    worker = CLIProposalWorker(ProposalCLI({'changes': [], 'rationale': '证据不足'}), work_root=tmp_path)
    assert worker(**proposal_request())['patch'] == {}


def test_unknown_cli_executable_fails_explicitly(tmp_path):
    with pytest.raises(ContractError, match='unavailable'):
        ManagedCLI(executable=str(tmp_path / 'missing'))
