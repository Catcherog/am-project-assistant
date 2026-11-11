#!/usr/bin/env python3
"""Create a synthetic development demo; never create a claimed independent holdout."""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'src'))

from am_gateway.assistant.quality import SCHEMA_VERSION, evaluate_quality
from am_gateway.assistant.evolution import EvolutionEngine
from integration.assistant_rsi_replay import FrozenInteractiveReplay, SOURCE_SCHEMA


class _DemoSourceProjection:
    """A deterministic source-only stand-in, explicitly not a language model."""
    synthetic = True

    def run(self, request):
        item = request['context']['evidence_items'][0]
        row = item['body']
        body = '\n'.join((row['title'], '进展：' + row['项目进展'],
            '卡点：' + row['卡点'], '下一步：' + row['待办']))
        return {'schema': 'am.gateway.result.v2', 'job_id': request['job_id'],
            'attempt_id': request['attempt_id'], 'status': 'completed', 'response_kind': 'sufficient',
            'reply_text': body, 'remaining_requirements': [], 'evidence_refs': [item['info_id']],
            'request_coverage': [{'request': '进展、卡点、下一步', 'state': 'answered'}]}


def _write(path, data):
    with path.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + '\n')


def create_demo(folder, *, run_offline=False):
    folder = Path(folder).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    source = {'schema_version': SOURCE_SCHEMA, 'data_origin': 'authored_synthetic_demo',
        'source_fixture': {'version': 'r8-demo-source-v1', 'complete': True,
            'observed_at': '2026-11-11T12:00:00+08:00', 'projects': [{
                'project_id': 'DEMO-P-A', 'name': '示例项目甲', 'progress': '样机已到，测试尚未启动。',
                'blocker': '工装未到。', 'next_step': '确认工装交期。'}]}}
    source_path = folder / 'source.json'
    _write(source_path, source)
    digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    bindings = {'r8-demo-source': {'path': str(source_path), 'sha256': digest,
        'source_version': 'r8-demo-source-v1'}}
    cases = []
    for channel in ('group', 'private'):
        cases.append({'case_id': 'R8-DEMO-' + channel, 'channel': channel, 'intent': 'known_project',
            'family': 'known_project', 'split': 'development', 'split_group': 'same-demo-source',
            'data_origin': 'authored_synthetic_demo', 'source_version': 'r8-demo-source-v1',
            'fixture_ref': 'r8-demo-source', 'source_sha256': digest,
            'required_layers': ['offline', 'real_model'],
            'input': {'text': '示例项目甲当前进展、卡点和下一步是什么？', 'actor_id': 'DEMO-OWNER',
                'conversation_id': 'DEMO-' + channel, 'origin_event_id': 'EVENT-DEMO-' + channel},
            'setup': {'actor': 'DEMO-OWNER', 'now': '2026-11-11T12:00:00+08:00',
                'allowed_project_ids': ['DEMO-P-A'], 'audience': {'project_ids': ['DEMO-P-A']}},
            'oracle': {'concepts': {'qualification': {'any_of': ['尚未启动']}},
                'required_subtasks': ['progress', 'blocker', 'next_step'],
                'required_assertions': ['zero_external_effect', 'same_source_facts'],
                'semantic_review_required': True, 'latency_targets_s': {'completion_s': 6}}})
    suite = {'schema_version': SCHEMA_VERSION, 'suite_id': 'R8-REPLAY-DEMO',
        'demo_only': True, 'holdout_status': 'not_supplied',
        'authored_cases_are_independent_holdout': False, 'cases': cases}
    _write(folder / 'development.json', suite)
    _write(folder / 'host_config.example.json', {
        'ports_factory': 'integration.assistant_rsi_replay:build_ports',
        'development_path': str(folder / 'development.json'),
        'evaluation_layer': 'real_model', 'automatic_config_activation': False,
        'replay': {'sources': bindings, 'evidence_dir': str(folder / 'private-evidence'),
            'model_id': '<verified-native-model-id>', 'model_revision': '<native-model-revision>',
            'model_binding_ref': '<native-readback-evidence>', 'compact_output': True}})
    records = []
    if run_offline:
        replay = FrozenInteractiveReplay(model=_DemoSourceProjection(), model_id='deterministic-demo',
            model_revision='demo-v1', model_binding_ref='demo:no-real-model', sources=bindings,
            evidence_dir=folder / 'offline-evidence', runtime_revision='demo:isolated-replay', actual_model=False)
        for case in cases:
            context = {name: case[name] for name in ('case_id', 'channel', 'intent', 'source_version')}
            context.update(mode='evaluation', effects_allowed=False, run_id='demo-run', variant='baseline',
                split='development', layer='offline', attempt_id='demo-1', timeout_seconds=15)
            records.append(replay(case=EvolutionEngine._worker_case(case),
                config={'reply.layout': 'paragraph', 'reply.max_chars': 600}, context=context))
        _write(folder / 'offline-report.json', evaluate_quality(suite, records, layers=('offline',)))
        (folder / 'offline-records.jsonl').write_text(''.join(
            json.dumps(r, ensure_ascii=False, allow_nan=False) + '\n' for r in records), encoding='utf-8')
    return {'source': str(source_path), 'development': str(folder / 'development.json'),
        'host_configuration': str(folder / 'host_config.example.json'),
        'offline_records': len(records), 'independent_holdout_created': False,
        'automatic_activation_enabled': False, 'real_model_called': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--run-offline', action='store_true')
    args = parser.parse_args(argv)
    print(json.dumps(create_demo(args.output_dir, run_offline=args.run_offline), ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
