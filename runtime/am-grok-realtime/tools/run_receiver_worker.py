#!/usr/bin/env python3
"""Run a native worker against the durable receiver using a trusted runner factory."""
import argparse
import importlib
import os
from pathlib import Path
import signal
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from am_gateway.phase1.execution import HTTPWorkerClient, ReceiverPump


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default=os.environ.get('AM_RECEIVER_URL', 'http://127.0.0.1:8766'))
    parser.add_argument('--factory', required=True, help='Trusted installed module:zero_argument_runner_factory')
    parser.add_argument('--worker-id', default='configured-native-worker')
    parser.add_argument('--namespace', default='grok_bot')
    args = parser.parse_args()
    token = os.environ.get('AM_RECEIVER_WORKER_TOKEN')
    if not token:
        parser.error('AM_RECEIVER_WORKER_TOKEN must be set by the host')
    module, separator, factory_name = args.factory.partition(':')
    if not separator or not module or not factory_name or '.' in factory_name:
        parser.error('--factory must be module:function')
    runner = getattr(importlib.import_module(module), factory_name)()
    client = HTTPWorkerClient(args.url, token, namespace=args.namespace)
    pump = ReceiverPump(client, runner, worker_id=args.worker_id)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    pump.start()
    try:
        stop.wait()
    finally:
        outcome = pump.stop(timeout=10)
    if not outcome['stopped']:
        print('Worker is still running; receiver keeps its original operation unresolved.', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
