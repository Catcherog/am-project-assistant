#!/usr/bin/env python3
"""Use the running gateway's shared claim ledger from an existing old poller.

Read one JSON object from stdin; print one JSON result. The helper neither runs
an Agent nor sends a Feishu message. Only claimed=true authorizes this poller to
continue its already-authorized handling of that exact source event.
"""
import argparse
import json
import os
import sys
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler


ROUTES = {'claim': '/v1/legacy/claim', 'status': '/v1/legacy/status',
          'finish': '/v1/legacy/finish', 'rollback-prepare': '/v1/cutover/prepare',
          'rollback-import': '/v1/cutover/import'}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError('gateway redirects are not accepted')


def request(operation, payload, *, url, token, timeout=10):
    parsed = urlsplit(url)
    if (parsed.scheme != 'https' and not
            (parsed.scheme == 'http' and parsed.hostname in {'127.0.0.1', 'localhost', '::1'})):
        raise ValueError('gateway URL must be loopback HTTP or HTTPS')
    if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in {'', '/'}:
        raise ValueError('gateway URL must be an origin without credentials')
    if not token or not isinstance(payload, dict):
        raise ValueError('operator token and JSON object required')
    raw = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode()
    if len(raw) > 1024 * 1024:
        raise ValueError('request too large')
    req = Request(url.rstrip('/') + ROUTES[operation], data=raw,
                  headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}, method='POST')
    with build_opener(NoRedirect()).open(req, timeout=timeout) as response:
        body = response.read(2 * 1024 * 1024 + 1)
    if len(body) > 2 * 1024 * 1024:
        raise ValueError('response too large')
    return json.loads(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=ROUTES)
    parser.add_argument('--url', default='http://127.0.0.1:8767')
    parser.add_argument('--token-env', default='AM_GATEWAY_OPERATOR_TOKEN')
    parser.add_argument('--timeout', type=float, default=10)
    args = parser.parse_args()
    try:
        raw = sys.stdin.buffer.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024 or not 0 < args.timeout <= 60:
            raise ValueError('request size or timeout invalid')
        result = request(args.operation, json.loads(raw), url=args.url,
                         token=os.environ.get(args.token_env, ''), timeout=args.timeout)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except Exception as exc:
        # Do not copy private source text or authorization headers into stderr.
        print(json.dumps({'ok': False, 'error': type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
