#!/usr/bin/env python3
"""A small native-tool entrypoint for an already configured Grok Bot.

Expose only this tool to the Bot. Configure the product's own memory/tools
boundary separately and verify it before enabling the external capability.
No vendor endpoint, login or model is assumed by this script.
"""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from am_gateway.phase1.execution import HTTPWorkerClient


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['claim', 'heartbeat', 'read_information', 'result'])
    parser.add_argument('--url', default=os.environ.get('AM_RECEIVER_URL', 'http://127.0.0.1:8766'))
    parser.add_argument('--worker-id', default='grok-native')
    args = parser.parse_args()
    token = os.environ.get('AM_RECEIVER_WORKER_TOKEN')
    if not token:
        parser.error('AM_RECEIVER_WORKER_TOKEN must be set by the host')
    client = HTTPWorkerClient(args.url, token)
    try:
        if args.action == 'claim':
            result = {'claim': client.claim(worker_id=args.worker_id)}
        else:
            body = json.loads(sys.stdin.read(1048577))
            if not isinstance(body, dict):
                raise ValueError('object required')
            method = client.complete if args.action == 'result' else getattr(client, args.action)
            result = method(**body)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except Exception as exc:
        # Never echo credentials, raw request bodies or third-party errors.
        print(json.dumps({'error': type(exc).__name__}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
