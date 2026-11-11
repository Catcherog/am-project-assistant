"""Autonomous code candidates in disposable copies, never runtime deployment.

The host owns copy/edit scopes and a fixed test command. Model statements about
tests or improvements are not evidence. A successful candidate remains ready
for native integration; this module has no production switch or message port.
"""
from __future__ import annotations

import difflib
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
from typing import Sequence
import uuid

from .cli_worker import run_process


DEFAULT_MUTABLE = ('src/**', 'integration/**')
# These are protected even when a host supplies a broad mutable source scope.
PROTECTED = (
    'tests/**', '**/tests/**', '**/test_*.py', '**/conftest.py',
    'pyproject.toml', 'pytest.ini', 'setup.cfg', 'setup.py', 'tox.ini',
    '**/*evolution*', '**/*quality*', '**/*authoriz*', '**/*identity*',
    '**/*policy*', '**/*dispatch*', '**/*boundary*', '**/*receiver*',
    '**/*evaluat*', '**/*cli_worker*', '**/*code_optimizer*', '**/*autonomy*', '**/*rsi_replay*',
    '**/contracts.py', '**/control_api.py', '**/actions.py', '**/models.py',
    '**/store.py', '**/storage.py', '**/runtime.py', '**/service.py',
    '**/execution.py', '**/assistant_host.py', '**/studio_host.py',
    '**/studio_phase1_factory.py',
)
OMITTED_PARTS = {'.git', '.venv', 'venv', 'node_modules', '__pycache__',
                 '.pytest_cache', 'private', 'secrets', 'credentials', 'state', 'logs'}
SECRET_SUFFIXES = {'.pem', '.key', '.p12', '.pfx', '.sqlite', '.sqlite3', '.db'}
GENERATED = {'__pycache__', '.pytest_cache'}


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _matches(name, patterns):
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def _patterns(values, *, required=False):
    if isinstance(values, (str, bytes)) or required and not values:
        raise ValueError('explicit_path_patterns_required')
    values = tuple(values)
    if any(not isinstance(v, str) or not v or v.startswith('/') or
           '..' in v.split('/') or '\\' in v for v in values):
        raise ValueError('relative_path_patterns_required')
    return values


def _omitted(path):
    return (any(p.lower() in OMITTED_PARTS or 'holdout' in p.lower() for p in path.parts)
            or path.name.lower().startswith('.env')
            or path.suffix.lower() in SECRET_SUFFIXES
            or path.name.lower() in {'config.json', 'credentials.json', 'auth.json'}
            or path.name.lower().endswith(('.log', '.jsonl')))


def _write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    path.chmod(0o600)


def _inventory(root):
    records, total = {}, 0
    for current, dirs, files in os.walk(root, followlinks=False):
        here = Path(current)
        for name in list(dirs):
            child = here / name
            if child.is_symlink():
                raise ValueError('candidate_symlink_not_allowed:' + str(child.relative_to(root)))
        dirs[:] = [d for d in dirs if d != '.git' and d not in GENERATED]
        for name in files:
            path = here / name
            if path.is_symlink() or not path.is_file():
                raise ValueError('candidate_nonregular_file:' + str(path.relative_to(root)))
            rel = path.relative_to(root).as_posix()
            stat = path.stat()
            total += stat.st_size
            if total > 64 * 1024 * 1024 or len(records) >= 4096:
                raise ValueError('candidate_snapshot_budget_exceeded')
            records[rel] = {'sha256': _sha(path), 'bytes': stat.st_size,
                            'executable': bool(stat.st_mode & 0o111)}
    return records


def _clean_generated(root):
    for current, dirs, _ in os.walk(root, topdown=True, followlinks=False):
        dirs[:] = [d for d in dirs if d != '.git']
        for name in list(dirs):
            path = Path(current) / name
            if path.is_symlink():
                raise ValueError('candidate_symlink_not_allowed:' + str(path.relative_to(root)))
            if name in GENERATED:
                shutil.rmtree(path)
                dirs.remove(name)


class ProcessAttemptError(RuntimeError):
    def __init__(self, outcome):
        super().__init__('isolated_git_initialization_failed:' + outcome['state'])
        self.outcome = outcome


class CodeCandidateWorker:
    """Generate, independently test, and retain one isolated code proposal.

    ``include_patterns`` is a required host decision. Include source needed by
    the fixed tests, the tests themselves and their nonsecret build metadata.
    Only ``mutable_patterns`` minus PROTECTED can change. No source symlinks,
    runtime state, credentials or independent holdout files are copied.
    """
    def __init__(self, cli, *, source_root, output_root,
                 include_patterns: Sequence[str], test_argv: Sequence[str],
                 mutable_patterns=DEFAULT_MUTABLE, protected_patterns=(),
                 test_timeout_seconds=300, cli_timeout_seconds=600,
                 max_copy_bytes=16 * 1024 * 1024, max_changed_files=12):
        original = Path(source_root).expanduser()
        if original.is_symlink() or not original.is_dir():
            raise ValueError('regular_source_directory_required')
        self.source = original.resolve()
        self.output = Path(output_root).expanduser().resolve()
        if self.output == self.source or self.output.is_relative_to(self.source):
            raise ValueError('output_must_be_outside_source_tree')
        self.include = _patterns(include_patterns, required=True)
        self.mutable = _patterns(mutable_patterns, required=True)
        self.protected = (*PROTECTED, *_patterns(protected_patterns))
        if (isinstance(test_argv, (str, bytes)) or not test_argv or
                any(not isinstance(a, str) or not a or '\x00' in a for a in test_argv)
                or not Path(test_argv[0]).is_absolute()):
            raise ValueError('fixed_absolute_test_argv_required')
        self.tests = tuple(test_argv)
        self.cli = cli
        self.test_timeout = float(test_timeout_seconds)
        self.cli_timeout = float(cli_timeout_seconds)
        if not 1 <= self.test_timeout <= 1800 or not 1 <= self.cli_timeout <= 1800:
            raise ValueError('bounded_worker_timeouts_required')
        if type(max_copy_bytes) is not int or not 1 <= max_copy_bytes <= 64 * 1024 * 1024:
            raise ValueError('bounded_copy_size_required')
        if type(max_changed_files) is not int or not 1 <= max_changed_files <= 50:
            raise ValueError('bounded_change_count_required')
        self.max_bytes, self.max_changes = max_copy_bytes, max_changed_files

    def _copy(self, workspace):
        total = 0
        for current, dirs, files in os.walk(self.source, followlinks=False):
            here = Path(current)
            for name in list(dirs):
                path = here / name
                if _omitted(path.relative_to(self.source)):
                    dirs.remove(name)
                    continue
                if path.is_symlink():
                    raise ValueError('source_symlink_not_allowed:' + str(path.relative_to(self.source)))
            for name in files:
                path = here / name
                relative = path.relative_to(self.source)
                if _omitted(relative) or not _matches(relative.as_posix(), self.include):
                    continue
                if path.is_symlink() or not path.is_file():
                    raise ValueError('source_nonregular_file:' + relative.as_posix())
                total += path.stat().st_size
                if total > self.max_bytes:
                    raise ValueError('source_copy_budget_exceeded')
                target = workspace / relative
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                target.write_bytes(path.read_bytes())
                target.chmod(0o700 if path.stat().st_mode & 0o111 else 0o600)
        manifest = _inventory(workspace)
        if not manifest:
            raise ValueError('empty_source_copy')
        return manifest

    def _git(self, workspace, cancelled):
        git = shutil.which('git')
        if not git:
            raise ValueError('git_not_available')
        prefix = [git, '--no-pager', '--git-dir=' + str(workspace / '.git'),
                  '--work-tree=' + str(workspace), '-c', 'core.hooksPath=/dev/null',
                  '-c', 'core.fsmonitor=false', '-c', 'commit.gpgsign=false',
                  '-c', 'user.name=AM RSI', '-c', 'user.email=am-rsi@localhost']
        for args in (['init', '--quiet', '--template=', str(workspace)],
                     ['add', '--all'], ['commit', '--quiet', '--no-verify', '--no-gpg-sign', '-m', 'isolated baseline']):
            outcome = run_process(prefix + args, workspace=workspace,
                timeout_seconds=30, cancelled=cancelled)
            if outcome['state'] != 'completed' or outcome.get('returncode') != 0:
                raise ProcessAttemptError(outcome)
        return outcome

    def _allowed(self, name):
        return (_matches(name, self.mutable) and not _matches(name, self.protected)
                and not _omitted(Path(name)) and not name.startswith('.'))

    def run(self, reason, *, cancelled=lambda: False):
        if isinstance(reason, dict):
            reason = json.dumps(reason, ensure_ascii=False, sort_keys=True)
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 24000:
            raise ValueError('bounded_optimization_reason_required')
        self.output.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.output.chmod(0o700)
        attempt = self.output / ('code-' + uuid.uuid4().hex)
        attempt.mkdir(mode=0o700)
        workspace = attempt / 'workspace'
        workspace.mkdir(mode=0o700)
        report = {'schema': 'am.rsi.code_candidate.v1', 'attempt_id': attempt.name,
            'state': 'preparing', 'started_at': time.time(), 'source_root': str(self.source),
            'workspace': str(workspace), 'artifact_directory': str(attempt),
            'include_patterns': list(self.include), 'mutable_patterns': list(self.mutable),
            'protected_patterns': list(self.protected), 'test_argv': list(self.tests),
            'production_changed': False, 'native_integration_verified': False,
            'real_model_quality_verified': False, 'external_message_sent': False,
            'candidate_usable': False}
        last_process = {'local_process_stopped': True}
        def finish(state, **extra):
            report.update(state=state, finished_at=time.time(), **extra)
            _write_json(attempt / 'report.json', report)
            return report
        def interrupted(outcome):
            stopped = outcome.get('local_process_stopped') is True
            return finish('deferred' if stopped else 'stop_unknown',
                local_process_stopped=stopped, provider_cancellation_proven=False,
                candidate_usable=False, retry_uses_new_workspace=True)
        try:
            if cancelled():
                return interrupted(last_process)
            before = self._copy(workspace)
            _write_json(attempt / 'baseline-manifest.json', before)
            originals = {name: (workspace / name).read_bytes() for name in before}
            last_process = {'local_process_stopped': False}
            last_process = self._git(workspace, cancelled)
            last_process = {'local_process_stopped': False}
            baseline = run_process(self.tests, workspace=workspace,
                timeout_seconds=self.test_timeout, cancelled=cancelled)
            last_process = baseline
            _write_json(attempt / 'baseline-tests.json', baseline)
            if cancelled() or baseline['state'] == 'cancelled':
                return interrupted(baseline)
            if baseline.get('local_process_stopped') is not True:
                return finish('stop_unknown', local_process_stopped=False, provider_cancellation_proven=False)
            if baseline['state'] != 'completed' or baseline.get('returncode') != 0:
                return finish('baseline_failed', baseline_state=baseline['state'])
            _clean_generated(workspace)
            if _inventory(workspace) != before:
                return finish('baseline_mutated_source')
            if cancelled():
                return interrupted(last_process)
            prompt = (
                'Improve this isolated code candidate for the stated observation. '
                'Do not commit, change tests/scorers/authorization/protocol gates, '
                'read credentials or independent holdouts, deploy, or send messages. '
                'Only edit paths matching the mutable patterns minus protected patterns. '
                'Preserve current business semantics. Host-side tests and diff decide acceptance.\n'
                + json.dumps({'observation': reason, 'mutable_patterns': self.mutable,
                              'protected_patterns': self.protected}, ensure_ascii=False))
            (attempt / 'request.txt').write_text(prompt)
            (attempt / 'request.txt').chmod(0o600)
            last_process = {'local_process_stopped': False}
            generation = self.cli.run(prompt, workspace=workspace,
                timeout_seconds=self.cli_timeout, write=True, cancelled=cancelled)
            last_process = generation
            _write_json(attempt / 'cli-result.json', generation)
            report['cli_state'] = generation.get('state')
            # A failed/timeout return is not proof that the local writer stopped.
            # Do not inventory or test a workspace that may still be changing.
            if generation.get('local_process_stopped') is not True:
                return finish('stop_unknown', local_process_stopped=False,
                    provider_cancellation_proven=False, candidate_usable=False)
            after = _inventory(workspace)
            _write_json(attempt / 'candidate-manifest.json', after)
            changed = sorted(name for name in set(before) | set(after) if before.get(name) != after.get(name))
            report['changed_files'] = changed
            diff = []
            for name in changed:
                old = originals.get(name, b'').decode('utf-8')
                new = (workspace / name).read_bytes().decode('utf-8') if name in after else ''
                diff.append('diff --git a/' + name + ' b/' + name + '\n')
                old_mode = '100755' if before.get(name, {}).get('executable') else '100644'
                new_mode = '100755' if after.get(name, {}).get('executable') else '100644'
                if name not in before:
                    diff.append('new file mode ' + new_mode + '\n')
                elif name not in after:
                    diff.append('deleted file mode ' + old_mode + '\n')
                elif old_mode != new_mode:
                    diff.extend(['old mode ' + old_mode + '\n', 'new mode ' + new_mode + '\n'])
                lines = difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                    fromfile='a/' + name if name in before else '/dev/null',
                    tofile='b/' + name if name in after else '/dev/null')
                for line in lines:
                    diff.append(line if line.endswith('\n') else line + '\n\\ No newline at end of file\n')
            (attempt / 'candidate.diff').write_text(''.join(diff))
            (attempt / 'candidate.diff').chmod(0o600)
            if cancelled() or generation.get('state') == 'cancelled':
                return interrupted(generation)
            if generation.get('state') != 'completed':
                return finish('cli_incomplete',
                    local_process_stopped=True,
                    provider_cancellation_proven=generation.get('provider_cancellation_proven', False))
            denied = [name for name in changed if not self._allowed(name)]
            if denied or len(changed) > self.max_changes:
                return finish('scope_rejected', rejected_files=denied)
            if not changed:
                return finish('no_change')
            _clean_generated(workspace)
            last_process = {'local_process_stopped': False}
            candidate = run_process(self.tests, workspace=workspace,
                timeout_seconds=self.test_timeout, cancelled=cancelled)
            last_process = candidate
            _write_json(attempt / 'candidate-tests.json', candidate)
            if cancelled() or candidate['state'] == 'cancelled':
                return interrupted(candidate)
            if candidate.get('local_process_stopped') is not True:
                return finish('stop_unknown', local_process_stopped=False, provider_cancellation_proven=False)
            _clean_generated(workspace)
            if _inventory(workspace) != after:
                return finish('candidate_tests_mutated_source')
            if candidate['state'] != 'completed' or candidate.get('returncode') != 0:
                return finish('candidate_failed', candidate_state=candidate['state'])
            if cancelled():
                return interrupted(last_process)
            return finish('ready_for_native_integration',
                candidate_usable=True, evidence_kind='fixed_host_tests_only', automatic_promotion=False)
        except Exception as exc:
            if isinstance(exc, ProcessAttemptError):
                last_process = exc.outcome
                _write_json(attempt / 'git-failure.json', last_process)
            if cancelled() or last_process.get('state') == 'cancelled':
                return interrupted(last_process)
            if last_process.get('local_process_stopped') is not True:
                return finish('stop_unknown', local_process_stopped=False,
                    provider_cancellation_proven=False, error_type=type(exc).__name__, error=str(exc))
            return finish('failed', error_type=type(exc).__name__, error=str(exc))
