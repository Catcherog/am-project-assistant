"""Task-bound dispatch reservations; original business journals remain authoritative.

The admission commit linearizes against cancellation in the original AM database.
A committed reservation is never undone by cancellation or automatically replayed.
Downstream journals also call reserve_effect in their actual dispatch transaction.
"""
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
from .store import canonical

_current = ContextVar('am_harness_action_reservation', default=None)


@contextmanager
def action_context(host, ctx, action):
    host.bridge.authorize(ctx, 'execute', action['arguments'])
    fence = host.hooks.fence
    if fence is None:
        raise RuntimeError('HARNESS_UNBOUND_RUN')
    expected = (action['version'], action['digest'], action['execution_id'])
    fresh = host.bridge.store.get_action(ctx, action['id'])
    if fresh['status'] != 'executing' or expected != (fresh['version'], fresh['digest'], fresh['execution_id']):
        raise RuntimeError('HARNESS_ACTION_VERSION_CHANGED')
    token = _current.set((host, fence, action))
    try:
        with host.s.tx() as conn:
            reserve_effect(conn, action['id'], 'action',
                           {'tool': action['tool_name'], 'version': action['version'],
                            'digest': action['digest'], 'execution_id': action['execution_id']})
        yield
    finally:
        _current.reset(token)


def reserve_effect(conn, operation_id, kind, payload):
    """Optional only for original non-harness calls; never a permissive authorizer."""
    bound = _current.get()
    if bound is None:
        if _local.get() is not None:
            raise RuntimeError('HARNESS_LOCAL_BINDING_REQUIRED')
        return
    host, fence, action = bound
    if kind in ('native_document','document_mount') and (not payload.get('accepted_hash') or payload['accepted_hash'] != payload.get('content_hash')):
        raise RuntimeError('HARNESS_CONTENT_ADOPTION_REQUIRED')
    host.effects._transaction(conn)
    host.tasks.check_in_transaction(conn, fence, allowed_states=('running',))
    key = hashlib.sha256(canonical([action['id'], kind, operation_id]).encode()).hexdigest()
    value_hash = hashlib.sha256(canonical(payload).encode()).hexdigest()
    old = conn.execute('SELECT * FROM am_harness_reservations WHERE id=?', (key,)).fetchone()
    if old:
        if (old['run_id'], old['revision'], old['cancel_epoch'], old['payload_hash']) != (fence.run_id, fence.revision, fence.cancel_epoch, value_hash):
            raise RuntimeError('HARNESS_RESERVATION_CONFLICT')
        raise RuntimeError('HARNESS_RESERVATION_RECONCILE_REQUIRED')
    conn.execute('INSERT INTO am_harness_reservations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                 (key, fence.run_id, fence.scope_key, fence.revision, fence.cancel_epoch,
                  fence.generation, action['id'], kind, value_hash,
                  'succeeded' if _local.get() is not None and kind != 'local_database' else 'reserved',
                  None, host.s.clock()))


def record_result(host, action_id, result):
    # This receipt is metadata; the original action/accepted_hash/receipt is the authority.
    state = result.get('status', 'unknown') if isinstance(result, dict) else 'unknown'
    if state not in ('succeeded', 'failed', 'unknown'):
        state = 'unknown'
    receipt_hash = hashlib.sha256(str(result.get('receipt_id', '')).encode()).hexdigest() if isinstance(result, dict) and result.get('receipt_id') else None
    host.s.execute('UPDATE am_harness_reservations SET state=?,receipt_hash=? WHERE action_id=?',
                   (state, receipt_hash, action_id))


_local = ContextVar('am_harness_local_mutation', default=None)


@contextmanager
def local_context(host, ctx, call, event_id):
    """Ordinary local tools need a task fence, never a new user approval."""
    fence = host.hooks.fence
    if fence is None or fence.scope_key != ctx.session_id:
        raise RuntimeError('HARNESS_LOCAL_BINDING_REQUIRED')
    host.tasks.guard(fence)
    action = {'id': 'local:' + hashlib.sha256(canonical([ctx.session_id, event_id, call.name, call.arguments]).encode()).hexdigest()}
    token = _current.set((host, fence, action))
    local_token = _local.set({'host': host, 'sequence': 0, 'tool': call.name})
    try:
        yield
    finally:
        _local.reset(local_token)
        _current.reset(token)


def _next_local(host):
    local = _local.get()
    if local is None:
        return None
    bound = _current.get()
    if local['host'] is not host or bound is None or bound[0] is not host:
        raise RuntimeError('HARNESS_LOCAL_BINDING_REQUIRED')
    local['sequence'] += 1
    return bound[1], bound[2], str(local['sequence']), local['tool']


def guard_local_transaction(host, conn):
    """Same-db reservation and mutation commit under the cancellation lock."""
    current = _next_local(host)
    if current is None:
        return
    fence, action, sequence, tool = current
    reserve_effect(conn, sequence, 'local_transaction', {'tool': tool, 'sequence': sequence})
    conn.execute("UPDATE am_harness_reservations SET state='succeeded' WHERE action_id=? AND kind='local_transaction'", (action['id'],))


@contextmanager
def guard_local_database(host, conn, *, require_binding=False):
    """Cross-db writes: durable intent, cancellation lock, local commit receipt.

    The caller commits its local SQLite transaction inside this context. This
    is NOT a distributed atomic commit. A crash between commits leaves reserved
    intent plus a local receipt for explicit reconciliation; it is never replayed.
    The gateway lock orders cancellation against the entire short local commit.
    """
    if require_binding and host.hooks.fence is not None and _current.get() is None:
        raise RuntimeError('HARNESS_LOCAL_BINDING_REQUIRED')
    current = _next_local(host)
    if current is None:
        yield
        return
    fence, action, sequence, tool = current
    database = conn.execute('PRAGMA database_list').fetchone()[2]
    database_hash = hashlib.sha256(database.encode()).hexdigest()
    operation = database_hash + ':' + sequence
    key = hashlib.sha256(canonical([action['id'], 'local_database', operation]).encode()).hexdigest()
    token = _local.set(None)  # Reservation bookkeeping is not a nested business write.
    try:
        with host.s.tx() as gateway:
            reserve_effect(gateway, operation, 'local_database', {'tool': tool, 'database': database_hash, 'sequence': sequence})
    finally:
        _local.reset(token)
    committed = False
    try:
        with host.tasks.transaction_guard(fence) as gateway:
            conn.execute('CREATE TABLE IF NOT EXISTS am_harness_local_commits(id TEXT PRIMARY KEY, run_id TEXT NOT NULL, revision INTEGER NOT NULL, created REAL NOT NULL)')
            conn.execute('INSERT INTO am_harness_local_commits VALUES(?,?,?,?)', (key, fence.run_id, fence.revision, host.s.clock()))
            yield
            committed = not conn.in_transaction
            if not committed:
                raise RuntimeError('HARNESS_LOCAL_COMMIT_UNKNOWN')
            gateway.execute("UPDATE am_harness_reservations SET state='succeeded',receipt_hash=? WHERE id=?", (key, key))
    except BaseException as exc:
        if committed or not conn.in_transaction:
            raise RuntimeError('HARNESS_LOCAL_COMMIT_UNKNOWN') from exc
        # The local transaction is still open; the store rolls it back. Keep
        # durable intent for reconciliation rather than inventing a receipt.
        raise


@contextmanager
def ingress_context():
    """A new incoming event never inherits another event's business binding."""
    token = _current.set(None)
    local_token = _local.set(None)
    try:
        yield
    finally:
        _local.reset(local_token)
        _current.reset(token)


def check_local_commit(host, conn):
    local = _local.get()
    if local is None:
        return
    bound = _current.get()
    if bound is None or bound[0] is not host:
        raise RuntimeError('HARNESS_LOCAL_BINDING_REQUIRED')
    host.tasks.check_in_transaction(conn, bound[1], allowed_states=('running',))


def reconcile_local_commit(host, reservation_id):
    """Explicit receipt-only reconciliation; never replay or clear the lane.

    Only the two configured original stores can prove a cross-db commit.
    A missing receipt remains unknown; callers must not infer rollback from it.
    """
    import sqlite3
    rows = host.s.rows("SELECT * FROM am_harness_reservations WHERE id=? AND kind='local_database'", (reservation_id,))
    if len(rows) != 1:
        raise RuntimeError('HARNESS_RESERVATION_NOT_FOUND')
    row = rows[0]
    original = host.bridge.store.original
    paths = [original._path, host.bridge.knowledge._db.execute('PRAGMA database_list').fetchone()[2]]
    receipts = []
    for path in paths:
        with sqlite3.connect('file:'+path+'?mode=ro',uri=True) as conn:
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='am_harness_local_commits'").fetchone():
                receipts.extend(conn.execute('SELECT run_id,revision FROM am_harness_local_commits WHERE id=?',(reservation_id,)).fetchall())
    if receipts != [(row['run_id'],row['revision'])]:
        return 'unknown'
    token = _local.set(None)
    try:
        with host.s.tx() as conn:
            conn.execute("UPDATE am_harness_reservations SET state='succeeded',receipt_hash=? WHERE id=? AND state IN ('reserved','succeeded')", (reservation_id,reservation_id))
    finally:
        _local.reset(token)
    return 'succeeded'
