"""Runnable receiver workers and an administrator-owned HTTP integration API.

These endpoints implement this repository's protocol. They are not vendor
Grok endpoints. A third-party Bot must be configured to claim and complete a
task here; merely accepting a webhook does not mark that Bot connected.
"""
from __future__ import annotations

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from urllib.parse import quote, unquote, urlsplit

from ..adapters.base import AdapterError, Deadline
from ..adapters.http import _validate_url, request_json
from ..models import ContractError, dumps
from .contracts import Capability
from .receiver import reconcile_worker_lease


def _worker_identity(claim):
    return {name: claim[name] for name in ('operation_id', 'run_id', 'generation', 'lease_token')}


def _authorize_claim(harness, claim):
    if harness is not None:
        with harness.store.transaction() as db:
            harness._authorize(db, claim['envelope']['identity']['task_id'], claim['generation'])


def _authorized_history(harness, claim):
    """Discard old turns if their source or accepted context is no longer valid."""
    result = []
    for turn in claim.get('history', []):
        if harness is not None:
            task = harness.store.one('SELECT task_id,generation FROM h1_tasks WHERE business_id=?',
                                     (turn.get('operation_id', ''),))
            if task is None:
                continue
            try:
                with harness.store.transaction() as db:
                    harness._authorize(db, task['task_id'], task['generation'], require_running=False)
            except ContractError:
                continue
        result.append({'role': turn['role'], 'body': turn['body']})
    return result


class ReceiverPump:
    """Execute one accepted operation with a configured, bounded runner.

The runner receives only the envelope and task-scoped context. This object
does not give it a sender, credentials or unrestricted filesystem tool.
Runners that execute external commands must separately enforce their sandbox.
"""
    def __init__(self, receiver, runner, *, harness=None, worker_id=None, poll_interval=.25):
        if not callable(runner) or not .01 <= poll_interval <= 300:
            raise ContractError('callable runner and valid polling interval required')
        self.receiver, self.runner, self.harness = receiver, runner, harness
        self.worker_id = worker_id or receiver.namespace + ':local'
        self.poll_interval = poll_interval
        self.stopped = threading.Event()
        self.wakeup = threading.Event()
        self.thread = None
        self._control = threading.Lock()
        self._step = threading.Lock()

    def notify(self):
        self.wakeup.set()

    def step(self):
        # Calling step from a service tick while its worker is active never starts
        # a second model execution on the same pump.
        if not self._step.acquire(blocking=False):
            return None
        heartbeat_stop = threading.Event()
        heartbeat_thread = None
        try:
            if self.harness is not None:
                self.receiver.publish_delivered_memory(self.harness)
            claim = self.receiver.claim(worker_id=self.worker_id)
            if claim is None:
                if self.harness is not None:
                    self.receiver.reconcile(self.harness)
                return None
            credentials = _worker_identity(claim)
            if self.harness is not None:
                self.receiver.reconcile(self.harness)
            cancelled = threading.Event()

            def heartbeat():
                while not heartbeat_stop.wait(max(.05, self.receiver.worker_lease_seconds / 3)):
                    try:
                        response = self.receiver.heartbeat(**credentials)
                        if response.get('cancel_requested'):
                            cancelled.set()
                    except Exception:
                        cancelled.set()
                        break

            def is_cancelled():
                response = self.receiver.heartbeat(**credentials)
                return cancelled.is_set() or response.get('cancel_requested', False)

            def read_information(info_id):
                if self.harness is None:
                    remote_read = getattr(self.receiver, 'read_information', None)
                    if callable(remote_read):
                        return remote_read(**credentials, info_id=info_id)
                    raise ContractError('task information tool is not bound')
                return self.harness.read_information(claim['envelope']['identity']['task_id'],
                                                     claim['generation'], info_id)

            context = {'run_id': claim['run_id'], 'session_key': claim['session_key'],
                       'history': _authorized_history(self.harness, claim), 'cancelled': is_cancelled,
                       'read_information': read_information}
            heartbeat_thread = threading.Thread(target=heartbeat, daemon=True,
                                                 name='receiver-heartbeat-' + self.receiver.namespace)
            heartbeat_thread.start()
            try:
                _authorize_claim(self.harness, claim)
                result = self.runner(claim['envelope'], context)
                status = 'failed' if isinstance(result, dict) and result.get('response_kind') == 'failed' else 'succeeded'
                outcome = self.receiver.complete(**credentials, result=result, status=status,
                                                 stopped_evidence='local_runner_returned')
            except Exception as exc:
                # Exception details may contain credentials or source text; expose
                # only a safe code or the exception class. An unknown protected
                # execution must never be reported through the completion API.
                if isinstance(exc, AdapterError) and exc.unknown:
                    heartbeat_stop.set()
                    mark_unknown = getattr(self.receiver, 'mark_unknown', None)
                    if not callable(mark_unknown):
                        # Older transports keep the original lease until it
                        # expires; they must not receive a fabricated end.
                        raise AdapterError('receiver_unknown_reporting_unavailable',
                                           status='unknown', unknown=True) from None
                    reason = ('host_qwen_stop_unknown' if exc.code == 'host_qwen_stop_unknown'
                              else 'adapter_execution_unknown')
                    try:
                        outcome = mark_unknown(**credentials, reason=reason)
                    except Exception:
                        # A lost/unsupported uncertainty report leaves the same
                        # claim unresolved. Never fall back to sending complete.
                        raise AdapterError('receiver_unknown_report_unconfirmed',
                                           status='unknown', unknown=True) from None
                else:
                    result = {'response_kind': 'failed',
                              'reply_text': '这次任务未完成：执行端运行出错。已保留原任务记录。', 'remaining_requirements':
                              ['runner_error:' + type(exc).__name__], 'artifacts': []}
                    outcome = self.receiver.complete(**credentials, result=result, status='failed',
                                                     stopped_evidence='local_runner_exception_returned')
            if self.harness is not None:
                self.receiver.reconcile(self.harness)
            return outcome
        finally:
            heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(1)
            self._step.release()

    def _run(self):
        while not self.stopped.is_set():
            try:
                result = self.step()
            except Exception as exc:
                if hasattr(self.receiver, 'store'):
                    with self.receiver.store.transaction() as db:
                        self.receiver.store.audit(db, 'receiver_worker_error', self.receiver.namespace,
                                                  type(exc).__name__)
                result = None
            if result is None:
                self.wakeup.wait(self.poll_interval)
                self.wakeup.clear()

    def start(self):
        with self._control:
            if self.thread is not None and self.thread.is_alive():
                return
            self.stopped.clear()
            self.thread = threading.Thread(target=self._run, daemon=True,
                                           name='receiver-worker-' + self.receiver.namespace)
            self.thread.start()

    def stop(self, timeout=2):
        self.stopped.set()
        self.wakeup.set()
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join(timeout)
        return {'stopped': self.thread is None or not self.thread.is_alive()}


class ConfiguredWebhookWake:
    """Coalesced advisory wakes. Durable task claims use the receiver API."""
    def __init__(self, url, *, token=None, namespace='grok_bot', timeout=2):
        _validate_url(url)
        if timeout <= 0 or timeout > 30:
            raise ContractError('invalid wake timeout')
        self.url, self.token, self.namespace, self.timeout = url, token, namespace, timeout
        self.wakeup, self.stopped = threading.Event(), threading.Event()
        self.thread = None
        self.lock = threading.Lock()
        self.last_error = ''
        self.last_notified = None

    def notify(self):
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.stopped.clear()
                self.thread = threading.Thread(target=self._run, daemon=True, name='grok-advisory-wake')
                self.thread.start()
            self.wakeup.set()

    def _run(self):
        while not self.stopped.is_set():
            self.wakeup.wait(.5)
            if not self.wakeup.is_set():
                continue
            self.wakeup.clear()
            if self.stopped.is_set():
                break
            try:
                request_json(self.url, method='POST', payload={
                    'schema': 'am.receiver.wake.v1', 'namespace': self.namespace, 'pending': True},
                    deadline=Deadline(time.monotonic() + self.timeout), bearer_token=self.token)
                self.last_notified, self.last_error = time.time(), ''
            except Exception as exc:
                self.last_error = type(exc).__name__

    def stop(self):
        self.stopped.set()
        self.wakeup.set()
        if self.thread is not None:
            self.thread.join(self.timeout + .5)
        return {'stopped': self.thread is None or not self.thread.is_alive()}


class HTTPReceiverAdapter:
    def __init__(self, base_url, token, *, namespace='grok_bot', timeout=3,
                 evidence_ref='', verified_boundary=False):
        _validate_url(base_url)
        if not isinstance(token, str) or not token or '\r' in token or '\n' in token:
            raise ContractError('receiver credential required')
        if urlsplit(base_url).query or not 0 < timeout <= 30:
            raise ContractError('receiver base URL or timeout invalid')
        self.base_url, self.token, self.namespace = base_url.rstrip('/'), token, namespace
        self.timeout, self.evidence_ref, self.verified_boundary = timeout, evidence_ref, verified_boundary

    def _request(self, path, method='GET', payload=None):
        return request_json(self.base_url + path, method=method, payload=payload,
                            deadline=Deadline(time.monotonic() + self.timeout), bearer_token=self.token)

    def accept(self, envelope):
        from .contracts import validate_envelope
        validate_envelope(envelope)
        response = self._request('/v1/tasks', 'POST', {'envelope': envelope})
        if response.get('schema') != 'am.receiver.v1' or (
                response.get('operation_id'), response.get('generation')) != (
                envelope['identity']['business_operation_id'], envelope['execution']['generation']):
            raise ContractError('remote acceptance identity mismatch')
        receipt = response.get('receipt', {})
        if receipt.get('kind') != 'accepted' or not receipt.get('payload', {}).get('durable_record'):
            raise ContractError('remote receiver did not durably acknowledge')
        return receipt

    def query(self, operation_id, run_id='', generation=None):
        result = self._request('/v1/operations/' + quote(operation_id, safe=''))
        if result.get('operation_id') != operation_id or (run_id and result.get('run_id') != run_id) or (
                generation is not None and result.get('generation') != generation):
            raise ContractError('remote status identity mismatch')
        return result

    def cancel(self, operation_id, run_id='', generation=None):
        response = self._request('/v1/operations/' + quote(operation_id, safe='') + '/cancel',
                                 'POST', {'run_id': run_id, 'generation': generation})
        if response.get('operation_id') != operation_id or (run_id and response.get('run_id') != run_id) or (
                generation is not None and response.get('generation') != generation):
            raise ContractError('remote cancellation identity mismatch')
        return response

    def capability(self, intent, *, scope=None):
        connected = bool(self.verified_boundary and self.evidence_ref)
        return Capability(intent, self.namespace,
            scope or self.namespace + ':{tenant_id}:{chat_id}:{principal}',
            available=connected, durable_acceptance=True, status_query=True, cancellation=True,
            isolation=self.verified_boundary, enforced_tools=self.verified_boundary,
            evidence_ref=self.evidence_ref)

    def reconcile(self, harness, *, limit=100):
        tasks = harness.store.rows('''SELECT * FROM h1_tasks WHERE business_id!=''
            AND (execution_state NOT IN ('succeeded','failed','cancelled') OR result_json IS NULL)
            ORDER BY created LIMIT ?''', (limit,))
        count = 0
        for task in tasks:
            if harness.config.capabilities[task['intent']].adapter != self.namespace:
                continue
            response = self.query(task['business_id'], task['run_id'], task['generation'])
            for receipt in response.get('events', []):
                if (receipt.get('task_id'), receipt.get('business_id'), receipt.get('generation')) != (
                        task['task_id'], task['business_id'], task['generation']):
                    raise ContractError('remote receipt identity mismatch')
                harness.record_receipt(**receipt)
                count += 1
            reconcile_worker_lease(harness, task_id=task['task_id'], operation_id=task['business_id'],
                run_id=response.get('run_id'), generation=response.get('generation'),
                status=response.get('status'), lease_until=response.get('lease_until'))
        return count

    def health(self):
        result = self._request('/v1/health')
        return {**result, 'boundary_verified': self.verified_boundary,
                'evidence_ref': self.evidence_ref}


class HTTPWorkerClient:
    """Worker credential client; it cannot submit jobs, list status or send messages.

Use with ``ReceiverPump(client, runner)`` for an independent native worker
process. The same class can be wrapped by a Bot's own native tool adapter.
"""
    def __init__(self, base_url, worker_token, *, namespace='grok_bot', timeout=3):
        _validate_url(base_url)
        if not worker_token or '\r' in worker_token or '\n' in worker_token or not 0 < timeout <= 30:
            raise ContractError('worker credential and timeout required')
        self.base_url, self.token = base_url.rstrip('/'), worker_token
        self.namespace, self.timeout = namespace, timeout
        self.worker_lease_seconds = 60

    def _request(self, action, body):
        return request_json(self.base_url + '/v1/worker/' + action, method='POST', payload=body,
                            deadline=Deadline(time.monotonic() + self.timeout), bearer_token=self.token)

    def claim(self, *, worker_id):
        claim = self._request('claim', {'worker_id': worker_id}).get('claim')
        if claim is not None:
            self.worker_lease_seconds = claim['lease_seconds']
        return claim

    def heartbeat(self, **identity):
        return self._request('heartbeat', identity)

    def complete(self, **body):
        return self._request('result', body)

    def mark_unknown(self, **body):
        # Reuse the existing result tool; no fifth worker capability is needed.
        return self._request('result', {**body, 'status': 'unknown'})

    def read_information(self, **body):
        return self._request('read_information', body)


class ReceiverHTTPServer:
    """Token-separated admin and task-worker HTTP interface, loopback by default.

Workers may claim, heartbeat, return a result, and read one authorized info item.
There is no arbitrary send, filesystem, credential or shell endpoint.
"""
    def __init__(self, receiver, *, admin_token, worker_token, host='127.0.0.1', port=0,
                 harness=None, max_request_bytes=1048576):
        if not admin_token or not worker_token or admin_token == worker_token:
            raise ContractError('distinct administrator and worker credentials required')
        if any('\n' in x or '\r' in x for x in (admin_token, worker_token)):
            raise ContractError('invalid receiver credential')
        self.receiver, self.harness = receiver, harness
        self.admin_token, self.worker_token = admin_token, worker_token
        self.max_request_bytes = max_request_bytes
        outer = self

        class Handler(BaseHTTPRequestHandler):
            server_version = 'AMReceiver/1'

            def log_message(self, format, *args):
                pass

            def _respond(self, code, body):
                encoded = dumps(body).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(encoded)))
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.wfile.write(encoded)

            def _authorized(self, worker=False):
                expected = outer.worker_token if worker else outer.admin_token
                actual = self.headers.get('Authorization', '')
                return hmac.compare_digest(actual, 'Bearer ' + expected)

            def _body(self):
                length = int(self.headers.get('Content-Length', '0'))
                if length < 0 or length > outer.max_request_bytes:
                    raise ContractError('request too large')
                self.connection.settimeout(5)
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise ContractError('incomplete request')
                result = json.loads(raw or b'{}')
                if not isinstance(result, dict):
                    raise ContractError('JSON object required')
                return result

            def _route(self, method):
                path = urlsplit(self.path).path
                worker = path.startswith('/v1/worker/')
                if not self._authorized(worker=worker):
                    return self._respond(401, {'error': 'unauthorized'})
                try:
                    body = self._body() if method == 'POST' else {}
                    if method == 'GET' and path == '/v1/health':
                        return self._respond(200, outer.receiver.health())
                    if method == 'POST' and path == '/v1/tasks':
                        envelope = body['envelope']
                        receipt = outer.receiver.accept(envelope)
                        return self._respond(202, {'schema': 'am.receiver.v1',
                            'operation_id': envelope['identity']['business_operation_id'],
                            'generation': envelope['execution']['generation'], 'receipt': receipt})
                    if path.startswith('/v1/operations/'):
                        remainder = path[len('/v1/operations/'):]
                        if method == 'POST' and remainder.endswith('/cancel'):
                            operation_id = unquote(remainder[:-len('/cancel')])
                            return self._respond(200, outer.receiver.cancel(operation_id,
                                body.get('run_id', ''), body.get('generation')))
                        if method == 'GET' and '/' not in remainder:
                            return self._respond(200, outer.receiver.query(unquote(remainder)))
                    if method == 'POST' and path == '/v1/worker/claim':
                        if outer.harness is not None:
                            outer.receiver.publish_delivered_memory(outer.harness)
                        claim = outer.receiver.claim(worker_id=body['worker_id'])
                        if claim is not None:
                            try:
                                _authorize_claim(outer.harness, claim)
                                claim['history'] = _authorized_history(outer.harness, claim)
                            except ContractError:
                                outer.receiver.complete(**_worker_identity(claim), status='failed',
                                    result={'response_kind': 'failed', 'reply_text': '',
                                            'remaining_requirements': ['source_or_authorization_changed'], 'artifacts': []},
                                    stopped_evidence='claim_not_disclosed')
                                claim = None
                        return self._respond(200, {'claim': claim})
                    if method == 'POST' and path == '/v1/worker/heartbeat':
                        return self._respond(200, outer.receiver.heartbeat(**body))
                    if method == 'POST' and path == '/v1/worker/result':
                        if body.get('status') == 'unknown':
                            body.pop('status')
                            return self._respond(200, outer.receiver.mark_unknown(**body))
                        return self._respond(200, outer.receiver.complete(**body))
                    if method == 'POST' and path == '/v1/worker/read_information':
                        info_id = body.pop('info_id')
                        envelope = outer.receiver.worker_context(**body)
                        if outer.harness is None:
                            raise ContractError('task information tool not bound')
                        result = outer.harness.read_information(envelope['identity']['task_id'],
                                                               body['generation'], info_id)
                        return self._respond(200, result)
                    return self._respond(404, {'error': 'unknown_endpoint'})
                except ContractError as exc:
                    return self._respond(409, {'error': str(exc)})
                except (KeyError, ValueError, TypeError):
                    return self._respond(400, {'error': 'invalid_request'})
                except Exception:
                    return self._respond(500, {'error': 'receiver_error'})

            def do_GET(self):
                self._route('GET')

            def do_POST(self):
                self._route('POST')

        self._handler = Handler
        self.server = ThreadingHTTPServer((host, port), Handler)
        self._bind_address = self.server.server_address
        self._closed = False
        self.thread = None

    @property
    def address(self):
        return self.server.server_address

    @property
    def base_url(self):
        host, port = self.address[:2]
        return 'http://' + str(host) + ':' + str(port)

    def start(self):
        if self.thread is not None and self.thread.is_alive():
            return
        if self._closed:
            self.server = ThreadingHTTPServer(self._bind_address, self._handler)
            self._closed = False
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True,
                                       name='am-receiver-http')
        self.thread.start()

    def stop(self):
        if self.thread is not None and self.thread.is_alive():
            self.server.shutdown()
            self.thread.join(2)
        self.server.server_close()
        self._closed = True
        return {'stopped': self.thread is None or not self.thread.is_alive()}
