"""Execute a reviewed business operation through an explicitly bound host CLI.

The configured command consumes JSON on stdin. Nothing from a proposal becomes
a shell command, command-line option or executable. A separate read command must
confirm the exact operation before it is reported as applied.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess

from am_gateway.models import ContractError, dumps


class ApprovedCommandExecutor:
    def __init__(self, *, argv, readback_argv, targets, actions, timeout_seconds=60, cwd=None):
        self.argv, self.readback_argv = self._argv(argv), self._argv(readback_argv)
        self.targets, self.actions = dict(targets), frozenset(actions)
        if not self.targets or not self.actions or not 1 <= timeout_seconds <= 600:
            raise ContractError('explicit target/action scope and bounded timeout required')
        self.timeout, self.cwd = timeout_seconds, str(Path(cwd).resolve()) if cwd else None

    @staticmethod
    def _argv(argv):
        if not isinstance(argv, list) or not argv or any(not isinstance(x, str) or not x or '\0' in x for x in argv):
            raise ContractError('configured argv array required')
        binary = Path(argv[0])
        if not binary.is_absolute() or not os.access(binary, os.X_OK):
            raise ContractError('absolute installed executable required')
        return tuple(argv)

    def _run(self, argv, request):
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, cwd=self.cwd, start_new_session=True)
        try:
            stdout, _ = proc.communicate(dumps(request), timeout=self.timeout)
            if proc.returncode != 0 or len(stdout.encode()) > 1048576:
                raise ContractError('host command did not return a bounded successful response')
            result = json.loads(stdout)
            if not isinstance(result, dict):
                raise ContractError('host response must be a JSON object')
            return result
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()

    def __call__(self, proposal, *, idempotency_key):
        payload = proposal['payload']
        project, target, action = payload['project_id'], payload['target'], payload['action']
        approved_targets = self.targets.get(project, {}).get(target['type'], ())
        if action not in self.actions or target['id'] not in approved_targets:
            raise ContractError('business target/action is not configured for this executor')
        if not isinstance(payload.get('changes'), dict) or not payload['changes']:
            raise ContractError('approved changes required')
        request = {'schema': 'am.approved.operation.v1', 'idempotency_key': idempotency_key,
                   'proposal_id': proposal['proposal_id'], 'version': proposal['version'],
                   'digest': proposal['digest'], 'payload': payload}
        receipt = self._run(self.argv, {'operation': 'apply', **request})
        if receipt.get('idempotency_key') != idempotency_key:
            return {'status': 'unknown', 'reason': 'host_receipt_identity_mismatch'}
        if receipt.get('status') == 'not_applied':
            return {'status': 'not_applied', 'receipt': receipt}
        # Even an applied receipt is only a write response until this separate
        # authenticated host read confirms target, changes and operation identity.
        observed = self._run(self.readback_argv, {'operation': 'readback', **request, 'receipt': receipt})
        matched = (observed.get('idempotency_key') == idempotency_key
                   and observed.get('target') == target
                   and observed.get('project_id') == project
                   and observed.get('changes') == payload['changes']
                   and observed.get('status') == 'applied'
                   and isinstance(observed.get('evidence_ref'), str) and observed['evidence_ref'])
        return ({'status': 'applied', 'evidence_ref': observed['evidence_ref'],
                 'receipt': receipt, 'readback': observed} if matched else
                {'status': 'unknown', 'reason': 'independent_readback_not_matched', 'receipt': receipt})


def build_executor(bridge, host_config, client, root):
    settings = host_config.get('phase1_gateway', host_config.get('multi_ai_gateway', {}))
    return ApprovedCommandExecutor(**settings['business_executor'])
