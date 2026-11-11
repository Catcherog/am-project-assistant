#!/usr/bin/env python3
"""Local RSI controls; no chat, Feishu sender, credential copy or service install."""
from __future__ import annotations

import argparse
import importlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / 'src'))


def run_once_with_idle_wait(service, *, wait_idle_seconds=45,
                            monotonic=time.monotonic, sleep=time.sleep):
    """A one-shot process must live long enough to observe its idle window.

    Only the idle-window state is polled. Pause, foreground work, contention,
    unknown operations and budgets keep their original immediate decisions.
    """
    if (type(wait_idle_seconds) not in (int, float) or not math.isfinite(wait_idle_seconds)
            or not 0 <= wait_idle_seconds <= 60):
        raise ValueError('idle_wait_must_be_between_zero_and_sixty_seconds')
    began = monotonic()
    while True:
        value = service.step(force=True)
        if value.get('state') != 'waiting_for_idle_window':
            return value
        remaining = wait_idle_seconds - (monotonic() - began)
        if remaining <= 0:
            return {**value, 'idle_wait_expired': True, 'idle_wait_seconds': wait_idle_seconds,
                    'job_started_by_cli': False}
        sleep(min(.25, remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--owner', required=True)
    parser.add_argument('--scope', required=True)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('status', 'reports', 'pause', 'resume'):
        commands.add_parser(name)
    one = commands.add_parser('run-once')
    one.add_argument('--host-factory', required=True,
        help='Trusted local module:function returning the already configured AutonomousRSI')
    one.add_argument('--wait-idle-seconds', type=float, default=45,
        help='Wait up to this many seconds only for the idle window (default 45; max 60)')
    args = parser.parse_args(argv)
    path = Path(args.database).resolve(strict=True)
    if args.command == 'run-once':
        if ':' not in args.host_factory:
            parser.error('host-factory must be module:function')
        module, name = args.host_factory.split(':', 1)
        service = getattr(importlib.import_module(module), name)()
        if (Path(service.store.path).resolve() != path or service.owner_id != args.owner
                or service.scope != args.scope):
            raise RuntimeError('RSI factory identity mismatch')
        value = run_once_with_idle_wait(service, wait_idle_seconds=args.wait_idle_seconds)
    else:
        # Existing local database only. A mistyped path must not create a blank
        # gateway and misleadingly report that an installed service is paused.
        db = sqlite3.connect(str(path)); db.row_factory = sqlite3.Row
        try:
            control = db.execute('SELECT * FROM r8_rsi_control WHERE owner_id=? AND scope=?',
                                 (args.owner, args.scope)).fetchone()
            if control is None:
                raise RuntimeError('RSI scope is not installed in this database')
            if args.command in {'pause', 'resume'}:
                db.execute('UPDATE r8_rsi_control SET paused=? WHERE owner_id=? AND scope=?',
                           (int(args.command == 'pause'), args.owner, args.scope)); db.commit()
                value = {'state': 'paused' if args.command == 'pause' else 'resumed',
                         'running_call_stop_proven': False}
            elif args.command == 'reports':
                rows = db.execute("SELECT result_json,result_hash FROM r8_rsi_jobs WHERE owner_id=? AND scope=? AND result_json<>'' ORDER BY updated DESC LIMIT 20",
                                  (args.owner, args.scope))
                from am_gateway.models import key
                value = []
                for row in rows:
                    report = json.loads(row['result_json'])
                    if key(report) != row['result_hash']:
                        raise RuntimeError('RSI report integrity mismatch')
                    value.append(report)
            else:
                rows = db.execute('SELECT kind,state,count(*) AS count FROM r8_rsi_jobs WHERE owner_id=? AND scope=? GROUP BY kind,state',
                                  (args.owner, args.scope))
                value = {'paused': bool(control['paused']), 'jobs': [dict(r) for r in rows],
                         'production_deployment_performed': False}
        finally:
            db.close()
    print(json.dumps(value, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, sqlite3.DatabaseError, ImportError, ValueError) as exc:
        print(json.dumps({'state': 'error', 'error_type': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(2)

