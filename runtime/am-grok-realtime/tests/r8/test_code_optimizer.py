"""Real local-process checks for isolated code-candidate lifecycle boundaries."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

# Also permits running this review-only copy against the main runtime package.
import am_gateway.assistant as assistant_package
assistant_package.__path__.insert(0,str(Path(__file__).resolve().parents[2]/'src/am_gateway/assistant'))
from am_gateway.assistant.code_optimizer import CodeCandidateWorker
from am_gateway.assistant.cli_worker import ManagedCLI


class CLI:
    def __init__(self, change=lambda workspace: None, state='completed', stopped=True):
        self.change,self.state,self.stopped=change,state,stopped
        self.calls=[]
    def run(self,prompt,*,workspace,timeout_seconds,write,cancelled):
        self.calls.append({'workspace':str(workspace),'write':write,'prompt':prompt})
        self.change(Path(workspace))
        return {'state':self.state,'output':'Tests passed; deploy this now.',
            'local_process_stopped':self.stopped,'provider_cancellation_proven':False,
            'elapsed_seconds':.001}


@pytest.fixture
def project(tmp_path):
    root=tmp_path/'source';(root/'src').mkdir(parents=True);(root/'tests').mkdir()
    (root/'src/work.py').write_text('def calculate():\n    return sum(range(4))\n')
    (root/'src/policy.py').write_text('ALLOW_EXTERNAL_SEND = False\n')
    (root/'tests/check.py').write_text(
        'import runpy\nassert runpy.run_path("src/work.py")["calculate"]() == 6\nprint("actual-check-passed")\n')
    return root


def worker(project,tmp_path,cli,**settings):
    return CodeCandidateWorker(cli,source_root=project,output_root=tmp_path/'attempts',
        include_patterns=('src/**','tests/**','pyproject.toml'),
        test_argv=(sys.executable,'tests/check.py'),**settings)


def optimized(workspace):
    (workspace/'src/work.py').write_text('def calculate():\n    return 6\n')


def report_file(result,name):
    return Path(result['artifact_directory'])/name


def test_real_baseline_and_candidate_checks_create_reviewable_diff_without_touching_source(project,tmp_path):
    original=(project/'src/work.py').read_bytes();cli=CLI(optimized)
    result=worker(project,tmp_path,cli).run('Reduce unnecessary work in calculate().')
    assert result['state']=='ready_for_native_integration'
    assert result['production_changed'] is False and result['automatic_promotion'] is False
    assert result['native_integration_verified'] is False
    assert (project/'src/work.py').read_bytes()==original
    assert cli.calls[0]['write'] is True
    assert (Path(result['workspace'])/'.git').is_dir()
    for filename in ('baseline-tests.json','candidate-tests.json'):
        evidence=json.loads(report_file(result,filename).read_text())
        assert evidence['returncode']==0 and 'actual-check-passed' in evidence['stdout']
    assert '+    return 6' in report_file(result,'candidate.diff').read_text()
    assert result['changed_files']==['src/work.py']
    assert report_file(result,'report.json').stat().st_mode & 0o077 == 0


def test_model_claim_of_pass_does_not_override_failed_candidate_tests(project,tmp_path):
    cli=CLI(lambda p:(p/'src/work.py').write_text('def calculate():\n    return 99\n'))
    result=worker(project,tmp_path,cli).run('Optimize.')
    assert result['state']=='candidate_failed' and result['candidate_usable'] is False
    evidence=json.loads(report_file(result,'candidate-tests.json').read_text())
    assert evidence['returncode']!=0 and 'AssertionError' in evidence['stderr']


def test_structured_journal_proposal_runs_with_real_managed_cli(project,tmp_path):
    script = ('import sys; from pathlib import Path; sys.stdin.read(); '
              'Path("src/work.py").write_text("def calculate():\\n    return 6\\n"); '
              'print("candidate written")')
    cli=ManagedCLI(executable=sys.executable,kind='json_stdin',argv=['-c',script])
    result=worker(project,tmp_path,cli).run({'proposal_id':'p1','issues':[{'code':'latency_review'}]})
    assert result['state']=='ready_for_native_integration'
    manifest=json.loads(report_file(result,'candidate-manifest.json').read_text())
    assert all('.rsi-invocation-' not in name for name in manifest)
    assert result['changed_files']==['src/work.py']


def test_unexpected_cli_exception_cannot_reuse_baseline_stop_evidence(project,tmp_path):
    class Raises(CLI):
        def run(self,*args,**kwargs):raise RuntimeError('execution status unavailable')
    result=worker(project,tmp_path,Raises()).run('Optimize.')
    assert result['state']=='stop_unknown' and result['local_process_stopped'] is False
    assert result['candidate_usable'] is False


def test_failed_baseline_never_invokes_code_model(project,tmp_path):
    (project/'src/work.py').write_text('def calculate():\n    return 99\n')
    cli=CLI(optimized);result=worker(project,tmp_path,cli).run('Optimize.')
    assert result['state']=='baseline_failed' and not cli.calls


@pytest.mark.parametrize('path',['tests/check.py','src/policy.py','src/quality.py','src/authorization.py',
                               'src/dispatch.py','src/evolution.py','src/conftest.py','README.md',
                               'src/autonomy.py','src/assistant_rsi_replay.py'])
def test_protected_or_out_of_scope_changes_are_rejected_before_candidate_tests(project,tmp_path,path):
    cli=CLI(lambda p:(p/path).write_text('print("do not run")\n'))
    result=worker(project,tmp_path,cli).run('Optimize.')
    assert result['state']=='scope_rejected' and path in result['rejected_files']
    assert not report_file(result,'candidate-tests.json').exists()
    assert result['candidate_usable'] is False


def test_sensitive_files_and_holdouts_are_not_copied_even_under_broad_include(project,tmp_path):
    (project/'.env').write_text('SECRET=private')
    (project/'private').mkdir();(project/'private/key.json').write_text('private')
    (project/'tests/independent_holdout.json').write_text('independent')
    (project/'config.json').write_text('private')
    (project/'src/credentials.json').write_text('private')
    cli=CLI(optimized)
    w=CodeCandidateWorker(cli,source_root=project,output_root=tmp_path/'attempts',
        include_patterns=('**','*'),test_argv=(sys.executable,'tests/check.py'))
    result=w.run('Optimize.')
    assert result['state']=='ready_for_native_integration'
    manifest=json.loads(report_file(result,'baseline-manifest.json').read_text())
    assert set(manifest)=={'src/work.py','src/policy.py','tests/check.py'}


def test_omitted_environment_symlink_does_not_block_an_unrelated_source_copy(project,tmp_path):
    (project/'.venv').symlink_to(tmp_path,target_is_directory=True)
    result=worker(project,tmp_path,CLI(optimized)).run('Optimize.')
    assert result['state']=='ready_for_native_integration'
    assert not (Path(result['workspace'])/'.venv').exists()


@pytest.mark.parametrize('link_kind',['file','directory'])
def test_source_symlinks_are_not_followed(project,tmp_path,link_kind):
    outside=tmp_path/'outside';outside.mkdir();(outside/'payload.py').write_text('outside')
    (project/'src/link.py').symlink_to(outside/'payload.py' if link_kind=='file' else outside,
                                     target_is_directory=link_kind=='directory')
    cli=CLI(optimized);result=worker(project,tmp_path,cli).run('Optimize.')
    assert result['state']=='failed' and not cli.calls
    assert 'source_' in result['error']


def test_candidate_symlink_is_rejected_before_test_execution(project,tmp_path):
    def change(p):
        (p/'src/work.py').unlink();(p/'src/work.py').symlink_to(project/'src/work.py')
    result=worker(project,tmp_path,CLI(change)).run('Optimize.')
    assert result['state']=='failed' and not report_file(result,'candidate-tests.json').exists()


def test_cancelled_local_cli_is_deferred_with_a_new_workspace_on_next_attempt(project,tmp_path):
    cli=CLI(optimized,state='cancelled',stopped=True);w=worker(project,tmp_path,cli)
    first=w.run('Optimize.')
    assert first['state']=='deferred' and first['retry_uses_new_workspace'] is True
    assert first['candidate_usable'] is False and first['provider_cancellation_proven'] is False
    assert report_file(first,'candidate.diff').exists()
    cli.state='completed';second=w.run('Optimize.')
    assert second['state']=='ready_for_native_integration'
    assert first['workspace']!=second['workspace']


def test_uncertain_local_stop_is_not_a_retryable_candidate(project,tmp_path):
    result=worker(project,tmp_path,CLI(optimized,state='cancelled',stopped=False)).run('Optimize.')
    assert result['state']=='stop_unknown' and result['candidate_usable'] is False
    assert result['provider_cancellation_proven'] is False


@pytest.mark.parametrize('state',['timeout','failed','output_limit','completed'])
def test_any_unconfirmed_cli_stop_blocks_workspace_inspection_and_candidate_tests(project,tmp_path,state):
    result=worker(project,tmp_path,CLI(optimized,state=state,stopped=False)).run('Optimize.')
    assert result['state']=='stop_unknown' and result['candidate_usable'] is False
    assert result['local_process_stopped'] is False
    assert not report_file(result,'candidate-manifest.json').exists()
    assert not report_file(result,'candidate-tests.json').exists()


def test_preempted_before_start_uses_no_model_or_test(project,tmp_path):
    cli=CLI(optimized);result=worker(project,tmp_path,cli).run('Optimize.',cancelled=lambda:True)
    assert result['state']=='deferred' and not cli.calls
    assert not report_file(result,'baseline-tests.json').exists()


def test_no_change_is_not_marked_as_an_improvement(project,tmp_path):
    result=worker(project,tmp_path,CLI()).run('Optimize.')
    assert result['state']=='no_change' and result['candidate_usable'] is False
    assert not report_file(result,'candidate-tests.json').exists()


def test_no_output_directory_or_relative_test_command_can_escape_configuration(project,tmp_path):
    with pytest.raises(ValueError,match='outside_source'):
        CodeCandidateWorker(CLI(),source_root=project,output_root=project/'attempts',
            include_patterns=('src/**',),test_argv=(sys.executable,'tests/check.py'))
    with pytest.raises(ValueError,match='absolute_test'):
        CodeCandidateWorker(CLI(),source_root=project,output_root=tmp_path/'attempts',
            include_patterns=('src/**',),test_argv=('python','tests/check.py'))


def test_candidate_tests_cannot_silently_rewrite_protected_tests(project,tmp_path):
    def mutate(p):
        (p/'src/work.py').write_text(
            'from pathlib import Path\nPath("tests/check.py").write_text("pass\\n")\n'
            'def calculate():\n    return 6\n')
    result=worker(project,tmp_path,CLI(mutate)).run('Optimize.')
    assert result['state']=='candidate_tests_mutated_source' and result['candidate_usable'] is False


def test_protected_file_deletion_is_rejected_before_candidate_test(project,tmp_path):
    result=worker(project,tmp_path,CLI(lambda p:(p/'tests/check.py').unlink())).run('Optimize.')
    assert result['state']=='scope_rejected' and result['rejected_files']==['tests/check.py']
    assert not report_file(result,'candidate-tests.json').exists()


def test_diff_preserves_missing_final_newline_for_native_patch_review(project,tmp_path):
    cli=CLI(lambda p:(p/'src/work.py').write_text('def calculate():\n    return 6'))
    result=worker(project,tmp_path,cli).run('Optimize.')
    assert result['state']=='ready_for_native_integration'
    patch=report_file(result,'candidate.diff')
    assert '\\ No newline at end of file' in patch.read_text()
    checked=subprocess.run(['git','apply','--reverse','--check',str(patch)],
        cwd=result['workspace'],capture_output=True,text=True)
    assert checked.returncode==0,checked.stderr
