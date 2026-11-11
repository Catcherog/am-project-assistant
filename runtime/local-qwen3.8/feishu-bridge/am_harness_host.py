"""Harness integration into the original AM consumer, grants and outbox.

No transport/provider is created. Admission threads do bounded original reads;
one durable worker invokes the existing ConversationAgent/ModelCoordinator.
"""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
import json
import time

from am_conversation.legacy import _hash, _reply
from am_conversation.types import AccessContext, ConversationError, canonical
from am_harness.am_adapter import LifecycleBinding, OneShotReadRunner, engine_event_id
from am_harness.config import HarnessConfig
from am_harness.controls import IngressController, parse_control
from am_harness.effects import FencedOutbox
from am_harness.history_adapter import FencedHistoryStore
from am_harness.install import install_fastpath_only
from am_harness.legacy_binding import deterministic_publication_key
from am_harness.runtime import TaskRuntime
from am_harness.store import DurableStore, Scope, StoreError
from am_harness.telemetry import MetadataTrace
from am_harness.checkpoints import CheckpointStore
from am_harness.reservation import action_context, record_result, local_context, guard_local_transaction, guard_local_database, check_local_commit


class _Runner:
    def __init__(self, host):
        self.host = host

    def run(self, snapshot):
        h = self.host
        with h.context(snapshot) as ctx, h.hooks.bind(snapshot.fence):
            h.hooks(ctx, 'before_handle', {})
            row = h.bridge._current(ctx)['row']
            # Translate only the genuine current requirement, never the control
            # command wrapper. The original source body stays immutable in SQLite.
            from am_conversation.text_ui import translate
            text = translate(h.bridge, ctx, dict(row, body=snapshot.input_text))
            h.bridge._current(ctx)['input_text'] = snapshot.input_text
            h.bridge._current(ctx)['engine_event_id'] = engine_event_id(snapshot)
            remaining = min(snapshot.deadline, snapshot.expires_at, row['expires']) - h.s.clock()
            if remaining <= 0:
                raise RuntimeError('HARNESS_DEADLINE_EXPIRED')
            if len(ctx.reference_project_ids) == 1:
                h.bridge.store.set_focus(ctx, ctx.reference_project_ids[0])
            h.trace.emit(snapshot.id, 'lease_acquired', queue_ms=max(0, (h.s.clock()-snapshot.queued_at)*1000))
            reply = h.bridge.agent.handle(ctx, engine_event_id(snapshot), text, timeout=remaining)
            h.hooks(ctx, 'after_handle', {})
            prepared = h.bridge.prepare_reply(ctx, reply)
            return {'prepared': prepared}


class HarnessHost:
    def __init__(self, service, settings):
        self.service, self.s, self.bridge = service, service.s, service.conversation
        self.config = HarnessConfig.from_mapping(settings)
        if not self.config.enabled or self.bridge is None:
            raise ValueError('HARNESS_CONVERSATION_REQUIRED')
        self.durable = self.config.durable_queue_enabled
        self.installation = install_fastpath_only(self.bridge) if self.config.fastpath_enabled else None
        self.pool = self.control_pool = self.worker_pool = None
        self.owns_pool = False
        self.futures = {}; self.worker = None; self.closed = False
        if not self.durable:
            if self.config.control_enabled:
                raise ValueError('HARNESS_CONTROL_REQUIRES_QUEUE')
            return
        path = self.s.conn.execute('PRAGMA database_list').fetchone()[2]
        if not path:
            raise ValueError('HARNESS_PERSISTENT_ORIGINAL_DATABASE_REQUIRED')
        self.tasks = DurableStore(path, clock=self.s.clock, max_queued=self.config.max_queued,
                                  aging_seconds=self.config.aging_seconds)
        self.effects = FencedOutbox(self.tasks)
        self.checkpoints = CheckpointStore(path, clock=self.s.clock)
        self.trace = MetadataTrace(Path(service.g.config.root)/'state/am-harness-r1/trace.jsonl', wall_clock=self.s.clock)
        self.hooks = LifecycleBinding(self.tasks.guard, clock=self.s.clock, observer=self.observe)
        self.bridge.agent.lifecycle = self.hooks
        self.reads = OneShotReadRunner(self.bridge.agent, self.hooks, clock=self.s.clock)
        history = FencedHistoryStore(self.bridge.store, self.tasks)
        self.bridge.store = self.bridge.agent.store = history
        self.runtime = TaskRuntime(self.tasks, _Runner(self), authorize=self.authorize,
                                   publish=self.publish, config=self.config)
        self.controls = IngressController(self.tasks, authorize=self.authorize,
            on_change=self.effects.suppress_pending, execution_seconds=self.config.execution_seconds)
        self.bridge.agent.external_executor = self.execute_action
        self.bridge.agent.local_executor = self.execute_local
        self.bridge.agent.goal_reconciler = self.reconcile_goal_submission
        self.bridge.agent.local_control_executor = self.execute_local_control
        self.bridge.project_read_observer = self.record_project_read
        self.s.local_write_guard = lambda conn: guard_local_transaction(self, conn)
        self.s.local_commit_guard = lambda conn: check_local_commit(self, conn)
        history.original.local_write_guard = lambda conn: guard_local_database(self, conn)
        self.bridge.knowledge.local_write_guard = lambda conn: guard_local_database(self, conn, require_binding=True)
        from am_harness.host_focus import HostFocus
        self.focus = HostFocus(self)
        history.original.focus_setter = self.focus.remember
        history.original.focus_getter = self.focus.project
        if self.installation:
            self.installation.router.focus = self.focus
        self.bridge.agent.context_builder = self.build_context
        self.bridge.agent.metrics = self.measure
        self.bridge.agent.read_executor = self.read_tool
        self.bridge.agent.model.model.usage_observer = self.observe_usage
        with self.s.tx() as c:
            c.execute('''CREATE TABLE IF NOT EXISTS am_harness_context_sources(event_id TEXT PRIMARY KEY,scope_key TEXT NOT NULL,permission TEXT NOT NULL,dependencies TEXT NOT NULL,expiries TEXT NOT NULL,expires REAL NOT NULL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS am_harness_reservations(
                id TEXT PRIMARY KEY,run_id TEXT NOT NULL,scope_key TEXT NOT NULL,revision INTEGER NOT NULL,
                cancel_epoch INTEGER NOT NULL,generation INTEGER NOT NULL,action_id TEXT NOT NULL,
                kind TEXT NOT NULL,payload_hash TEXT NOT NULL,state TEXT NOT NULL,receipt_hash TEXT,created REAL NOT NULL)''')
            c.execute("UPDATE am_inbox SET state='received' WHERE state='harness_admitting' AND expires>?", (self.s.clock(),))
        self.phase_starts = __import__('contextvars').ContextVar('am_harness_phase_starts', default=None)
        with self.s.tx() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS am_harness_terminal_sync(
                run_id TEXT NOT NULL,revision INTEGER NOT NULL,state TEXT NOT NULL,
                status TEXT NOT NULL,next_attempt REAL NOT NULL,
                PRIMARY KEY(run_id,revision,state))""")
        self.last_maintenance = 0

    def _row(self, event_id):
        rows = self.s.rows('SELECT * FROM am_inbox WHERE id=?', (event_id,))
        if not rows:
            raise ConversationError('HARNESS_SOURCE_MISSING')
        row = rows[0]
        self.bridge._identity(row)
        return row

    def source(self, run):
        event = run.event_id
        if run.revision > 1:
            events = self.s.rows("SELECT event_id FROM am_harness_events WHERE run_id=? AND scope_key=? AND operation='revise' AND applied_revision=?",
                                 (run.id, run.scope.key, run.revision))
            if len(events) != 1:
                raise ConversationError('HARNESS_REVISION_SOURCE_MISSING')
            event = events[0]['event_id']
        row = self._row(event)
        original = self._row(run.event_id)
        if Scope(*self.bridge._identity(row)) != run.scope or Scope(*self.bridge._identity(original)) != run.scope:
            raise ConversationError('HARNESS_SCOPE_MISMATCH')
        expected = row['body'] if run.revision == 1 else getattr(parse_control(row['body']), 'replacement', None)
        if expected != run.input_text:
            raise ConversationError('HARNESS_SOURCE_INPUT_CHANGED')
        row['expires'] = min(row['expires'], original['expires'], run.expires_at)
        return row

    def authorize(self, scope, operation):
        if operation not in ('submit', 'execute', 'publish', 'control'):
            raise ConversationError('HARNESS_OPERATION_INVALID')
        if not self.config.enabled or not self.bridge.enabled or self.service.flags.get('query') is not True:
            raise ConversationError('QUERY_NOT_ENABLED')
        if operation == 'control' and not self.config.control_enabled:
            raise ConversationError('HARNESS_CONTROL_DISABLED')
        rows = self.s.rows('SELECT * FROM am_inbox WHERE tenant=? AND app=? AND user=? AND chat=? AND expires>? ORDER BY created DESC LIMIT 64',
                          (*scope.values[:4], self.s.clock()))
        row = next((r for r in rows if Scope(*self.bridge._identity(r)) == scope), None)
        if row is None:
            raise ConversationError('HARNESS_SOURCE_MISSING')
        self.bridge._grant(row)  # Original current policy and 600-second identity mechanism.

    @contextmanager
    def context(self, run):
        row = self.source(run)
        with self.bridge.request_context(row) as ctx:
            if ctx.session_id != run.scope.key:
                raise ConversationError('HARNESS_SCOPE_MISMATCH')
            yield ctx

    def publish(self, conn, run, outcome):
        prepared = outcome['prepared']; row = prepared['row']
        ctx = AccessContext(**prepared['context'])
        if ctx.session_id != run.scope.key or row['expires'] > run.expires_at:
            raise ConversationError('HARNESS_PUBLICATION_SCOPE_CHANGED')
        from am_harness.host_context import validate_publication_sources
        validate_publication_sources(self, conn, run, prepared)
        # All network reads finished before this short transaction. Original
        # dispatcher repeats fresh authorization; this check sees local policy.
        if self.bridge._grant(dict(row), verify=False) != prepared['grant']:
            raise ConversationError('CONVERSATION_SCOPE_CHANGED')
        self.effects.enqueue(conn, run, lambda c, r: self.bridge.enqueue_reply(
            c, prepared, event_key=deterministic_publication_key(r)))

    def admit(self, source):
        from am_harness.reservation import ingress_context
        with ingress_context():
            return self._admit_source(source)

    def _admit_source(self, source):
        row = dict(source)
        if self.closed:
            raise RuntimeError('HARNESS_CLOSED')
        scope = Scope(*self.bridge._identity(row))
        self.authorize(scope, 'submit')
        command = parse_control(row['body'])
        if command is not None and self.config.control_enabled:
            reply = self.controls.handle(scope, row['id'], row['body'])
            row['_query_read'] = True; row['_query_grant'] = self.bridge._grant(row)
            if row['chat_type'] == 'group':
                row['_query_group'] = row['chat']; row['_public_query'] = True
            text = reply.text
            if reply.run_id:
                text += '\n任务编号：' + reply.run_id
                reservations = self.s.rows("SELECT state,count(*) AS n FROM am_harness_reservations WHERE run_id=? GROUP BY state", (reply.run_id,))
                if reservations:
                    text += '\n已有动作占用或提交记录；停止任务不会自动撤回已提交动作，请按原记录核对。'
                self.trace.emit(reply.run_id, 'control', revision=reply.revision, cancel_epoch=reply.cancel_epoch)
            out = self.service.notify(row, text=text, kind='harness_control', event_key='harness-control:'+row['id'])
            self.s.execute("UPDATE am_inbox SET state='done' WHERE id=?", (row['id'],))
            return out
        # A successful short read uses the original dedupe/render/outbox chain.
        # On a miss no original event is claimed; only the durable worker runs it.
        with self.bridge.request_context(row) as ctx:
            reply = self.reads.try_handle(ctx, row['id'], row['body']) if self.config.fastpath_enabled else None
            if reply is not None:
                if reply.status == 'in_progress':
                    self.s.execute("UPDATE am_inbox SET state='harness_unknown',error='ORIGINAL_EVENT_IN_PROGRESS' WHERE id=?", (row['id'],))
                    return None
                prepared = self.bridge.prepare_reply(ctx, reply)
                with self.s.tx() as c:
                    out = self.bridge.enqueue_reply(c, prepared, event_key='conversation:'+row['id'])
                    self.record_read_sources(c, ctx, prepared)
                    c.execute("UPDATE am_inbox SET state='done' WHERE id=?", (row['id'],))
                return out
            # A newer unresolved request invalidates stale pronoun context. The
            # worker can establish its project only while its version is current.
            self.focus.invalidate(ctx)
        run = self.runtime.submit(scope, row['id'], row['body'], source_expires_at=row['expires'])
        self.trace.start_run(run.id)
        self.trace.emit(run.id, 'queued', revision=run.revision)
        self.s.execute("UPDATE am_inbox SET state='harness_queued' WHERE id=?", (row['id'],))
        return run.id

    def record_read_sources(self, conn, ctx, prepared):
        current = self.bridge._current(ctx)
        row = current['row']; snapshot = row.get('_snapshot_meta', {})
        version = snapshot.get('version')
        if not version:raise ConversationError('HARNESS_READ_SOURCE_VERSION_REQUIRED')
        ids = prepared['reply'].get('source_ids', [])
        recorded_versions = [v.removeprefix('ledger-snapshot:') for v in ids if v.startswith('ledger-snapshot:')]
        recorded_projects = {v.removeprefix('ledger-project:') for v in ids if v.startswith('ledger-project:')}
        if len(recorded_versions) != 1 or recorded_projects != set(prepared['project_refs']):
            raise ConversationError('HARNESS_READ_PROVENANCE_MISMATCH')
        dependencies = {'ledger:'+p: recorded_versions[0] for p in recorded_projects}
        old = conn.execute('SELECT dependencies FROM am_harness_context_sources WHERE event_id=?', (row['id'],)).fetchone()
        if old:
            if json.loads(old['dependencies']) != dependencies:
                raise ConversationError('HARNESS_READ_PROVENANCE_MISMATCH')
            return  # A replay never renews the original source version or TTL.
        if recorded_versions[0] != str(version):
            raise ConversationError('HARNESS_READ_SOURCE_CHANGED_REPLAY')
        expires = min(ctx.content_expires_at, row.get('_observed_at', self.s.clock())+3600)
        permission = _hash([ctx.scope, sorted(ctx.allowed_project_ids), current['grant'],
                            ctx.is_owner, ctx.source_access_version])
        conn.execute('INSERT INTO am_harness_context_sources VALUES(?,?,?,?,?,?)',
            (row['id'],ctx.session_id,permission,canonical(dependencies),
             canonical({s:expires for s in dependencies}),expires))

    def _admit(self, row):
        from fs_agent import request_timing as timing
        token = timing.set_context(self.s, row['id'])
        try:
            timing.anchor(self.s, row['id'], 'task_start')
            return self.admit(row)
        except Exception as exc:
            code = str(exc) if __import__('re').fullmatch('[A-Z_0-9]{1,80}', str(exc)) else 'HARNESS_ADMISSION_FAILED'
            self.s.execute("UPDATE am_inbox SET state='failed',error=? WHERE id=?", (code, row['id']))
            # Fixed status only, with the original identity and query grant.
            try:
                status_row=dict(row,_query_read=True,_query_grant=self.bridge._grant(row))
                if row['chat_type']=='group':status_row.update(_query_group=row['chat'],_public_query=True)
                self.service.notify(status_row,text='这条消息尚未处理完成，请稍后重试或核对任务状态。',kind='harness_error',event_key='harness-error:'+row['id'])
            except Exception:
                pass  # A denied/unavailable identity never receives project data.
        finally:
            timing.reset_context(token)
            self.s.outbox_wakeup()

    def run_once(self):
        outcome = self.runtime.run_once('original-am-worker')
        self.sync_terminal()
        self.s.outbox_wakeup()
        return outcome

    def sync_terminal(self):
        """Durable state-derived work; restarts cannot lose maintenance-only exits."""
        now = self.s.clock()
        with self.s.tx() as conn:
            conn.execute("""INSERT OR IGNORE INTO am_harness_terminal_sync
                SELECT id,revision,state,'pending',0 FROM am_harness_runs
                WHERE state IN ('succeeded','failed','cancelled','expired','unknown')""")
            conn.execute("""UPDATE am_inbox SET
                state=(SELECT CASE WHEN r.state='succeeded' THEN 'done' ELSE 'harness_'||r.state END
                       FROM am_harness_runs r WHERE r.event_id=am_inbox.id),
                error=(SELECT r.error FROM am_harness_runs r WHERE r.event_id=am_inbox.id)
                WHERE id IN (SELECT event_id FROM am_harness_runs
                             WHERE state IN ('succeeded','failed','cancelled','expired','unknown'))""")
        rows = self.s.rows("""SELECT r.* FROM am_harness_terminal_sync t JOIN am_harness_runs r
            ON r.id=t.run_id AND r.revision=t.revision AND r.state=t.state
            WHERE t.status='pending' AND t.next_attempt<=? LIMIT 32""", (now,))
        for saved in rows:
            notice = saved['state'] in ('failed','expired','unknown')
            status = 'done'
            if notice:
                if saved['expires_at'] <= now or saved.get('purged'):
                    status = 'suppressed'
                elif not self.failure_notice(saved):
                    status = 'pending'
            self.s.execute('UPDATE am_harness_terminal_sync SET status=?,next_attempt=? WHERE run_id=? AND revision=? AND state=?',
                (status, now+30, saved['id'], saved['revision'], saved['state']))
            self.trace.emit(saved['id'], 'run_state', status=saved['state'])

    def failure_notice(self, saved):
        """Data-free status after failure, using the original permission/sender.

        Business results still require a succeeded run and the strict outbox
        binding. This notice cannot contain source data or assert any effect.
        """
        try:
            run = self.tasks._snapshot(saved)
            row = self.source(run)
            row['_query_read'] = True
            row['_query_grant'] = self.bridge._grant(row)
            if row['chat_type'] == 'group':
                row.update(_query_group=row['chat'], _public_query=True)
            text = ('这项任务的执行结果仍待核对，没有自动重试。' if run.state == 'unknown'
                    else ('这项任务排队超时，未自动重试。请重新提问。' if run.lease_generation == 0
                          else '这项任务已超时，执行结果需要核对，系统没有自动重试。')
                    if run.state == 'expired' and run.error == 'DEADLINE_EXCEEDED'
                    else '这项任务未完成。请稍后重新提问或核对任务状态。')
            if run.error == 'MODEL_PAUSED':
                text = '模型暂时不可用，这次请求尚未完成。已记录的结果保留；恢复后可重新查询。'
            text += '\n任务编号：' + run.id
            self.service.notify(row, text=text, kind='harness_error',
                event_key='harness-status:'+run.id+':v'+str(run.revision)+':'+run.state)
        except Exception:
            # Permission/TTL failure cannot turn an error notice into an
            # alternate route to the original source content.
            return False
        return True

    def pump(self):
        if not self.durable or self.closed:
            return False
        if self.pool is None:
            self.pool = getattr(self.service,'io_pool',None)
            self.owns_pool = self.pool is None
            if self.owns_pool:self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='am-harness-admit')
            self.control_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='am-harness-control')
            self.worker_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='am-harness-worker')
        for key, future in list(self.futures.items()):
            if future.done():
                future.result(); del self.futures[key]
        if self.worker is not None and self.worker.done():
            self.worker.result(); self.worker = None
        rows = self.s.rows("SELECT * FROM am_inbox WHERE kind='conversation' AND state='received' AND expires>? ORDER BY created,rowid LIMIT 32", (self.s.clock(),))
        for row in rows:
            # A running durable model task does not own an admission slot.
            control = self.config.control_enabled and parse_control(row['body']) is not None
            key = (row['tenant'], row['app'], row['user'], 'control' if control else 'read')
            if key in self.futures or sum(k[-1]==key[-1] for k in self.futures) >= (1 if control else 2):
                continue
            if self.s.execute("UPDATE am_inbox SET state='harness_admitting' WHERE id=? AND state='received'", (row['id'],)):
                self.futures[key] = (self.control_pool if control else self.pool).submit(self._admit, row)
        # Metadata admission, source reads and authenticated controls above keep
        # running. Only new model work waits; DurableStore.claim rechecks owner
        # atomically, so the observation here is an optimization, not the gate.
        if self.worker is None and not self.s.meta('resource_window_owner') and self.s.rows("SELECT 1 FROM am_harness_runs WHERE state='queued' AND expires_at>? LIMIT 1", (self.s.clock(),)):
            if self.tasks.lane_status()['state'] == 'idle':
                self.worker = self.worker_pool.submit(self.run_once)
        if self.s.clock()-self.last_maintenance > 60:
            self.last_maintenance = self.s.clock()
            self.tasks.purge_expired(); self.checkpoints.purge_expired()
            self.s.execute('DELETE FROM am_harness_context_sources WHERE expires<=?',(self.s.clock(),))
            from fs_agent.provider import atomic_json
            lane = self.tasks.lane_status()
            atomic_json(Path(self.service.g.config.root)/'state/am-harness-r1/status.json', {
                'enabled': True, 'strict_bound': self.hooks.strict_bound, 'lane': lane['state'],
                'queue': len(self.s.rows("SELECT 1 FROM am_harness_runs WHERE state='queued'")),
                'admissions': len(self.futures), 'worker': self.worker is not None,
                'ledger_write': self.service.flags['ledger_write']})
        self.sync_terminal()
        return bool(rows or self.futures or self.worker)

    def execute_local_control(self, ctx, event_id, name, arguments, apply):
        from am_conversation.types import ToolCall
        from am_harness.runtime import mark_dispatch
        mark_dispatch('local_control')
        with local_context(self, ctx, ToolCall('control', name, arguments), event_id):
            return apply()

    def execute_local(self, ctx, call, event_id, tool):
        from am_harness.runtime import mark_dispatch
        mark_dispatch("local_tool")
        with local_context(self, ctx, call, event_id):
            return tool.handler(ctx, call.arguments, event_id)

    def reconcile_goal_submission(self, ctx, submission):
        """Read the original native operation journal; never call create again."""
        if submission.get('operation')!='create_followup' or not submission.get('engine_event_id'):
            return None
        args=submission['arguments'];business=self.bridge.business
        business.guard(ctx,'manage',args)
        operation=business.operation(ctx,submission['engine_event_id'],args)
        rows=self.s.rows('SELECT * FROM item_operations WHERE id=? AND actor=?',(operation,ctx.actor_id))
        if len(rows)!=1:return None
        row=rows[0];item=json.loads(row['result'])
        # Check the durable original identity and its live authorized object.
        current=business.item(ctx,row['item_id'])
        if (item.get('id')!=current['id'] or item.get('owner')!=ctx.actor_id
                or item.get('description')!=args.get('description')
                or item.get('project_ref')!=(args.get('project_id') or 'personal:'+ctx.actor_id)
                or json.loads(item.get('source','{}')).get('conversation_event')!=submission['engine_event_id']):
            return None
        return {'item':item,'formal_project_changed':False,'notification_sent':False,
                'verified':True,'reconciled_operation_id':operation,
                'message':'已核对原登记回执，原跟进事项保留。'}

    def execute_action(self, ctx, action, tool):
        from am_harness.runtime import mark_dispatch
        mark_dispatch("external_action")
        try:
            with action_context(self, ctx, action):
                result = tool.handler(ctx, action['arguments'], action['id'])
        except StoreError as exc:
            reserved = self.s.rows("SELECT 1 FROM am_harness_reservations WHERE action_id=? AND kind!='action'", (action['id'],))
            if not reserved and exc.code in ('FENCE_STALE','RUN_CANCELLED','RUN_NOT_ACTIVE','CONTENT_EXPIRED','DEADLINE_EXCEEDED'):
                result = {'status':'failed','message':'操作在正式提交前已停止，准备记录保留。'}
                record_result(self, action['id'], result)
                return result
            record_result(self, action['id'], {'status': 'unknown'})
            raise
        except BaseException:
            record_result(self, action['id'], {'status': 'unknown'})
            raise
        record_result(self, action['id'], result)
        return result

    def record_project_read(self, ctx, project):
        from am_harness.host_context import record_read_source
        record_read_source(self, ctx, project=project)

    def read_tool(self, ctx, call, event_id):
        from am_harness.registry_adapter import AmRegistryAdapter
        from am_harness.tool_policy import ToolInvocation
        # The current project/info ports mutate request-local snapshot/focus;
        # none are certified independent. Preserve serial execution explicitly.
        adapter = AmRegistryAdapter(self.bridge.registry, ctx, event_id,
            authorize=self.bridge.authorize, independent_read_tools=frozenset())
        invocation = ToolInvocation(call.id, call.name, call.arguments)
        spec = adapter.resolve(call.name, call.arguments)
        if spec.effect != 'read' or adapter.authorize(spec, invocation) is not True:
            raise ConversationError('PROJECT_ACCESS_DENIED')
        self.hooks(ctx, 'before_tool', call.arguments)
        result = spec.handler(invocation)
        self.hooks(ctx, 'after_tool', call.arguments)
        if adapter.authorize(spec, invocation) is not True:
            raise ConversationError('PROJECT_ACCESS_DENIED')
        if call.name == 'list_projects':
            from am_harness.host_context import record_read_source
            record_read_source(self, ctx, catalog=True)
        return result

    def reconcile_receipt(self, outbox_id):
        rows = self.s.rows("SELECT o.* FROM outbox o JOIN am_harness_outbox h ON h.outbox_id=o.id WHERE o.id=? AND o.state='sent' AND h.verified=0 AND o.expires>?", (outbox_id,self.s.clock()))
        if not rows:return False
        row=rows[0]
        self.s.execute('UPDATE am_conversation_outputs SET verification_attempts=verification_attempts+1,next_verification=? WHERE outbox_id=?',(self.s.clock()+30,outbox_id))
        try:verified=self.service.g.api.verify_text(row['message_id'],self.service.query_readback_row(row))
        except Exception:verified=False
        if verified:
            with self.s.tx() as c:self.verified_receipt(c,row,row['message_id'])
        return verified

    def observe_usage(self, usage):
        fence = self.hooks.fence
        if fence is None:raise RuntimeError('HARNESS_UNBOUND_RUN')
        self.trace.emit(fence.run_id, 'run_state', status='running', phase='model',
                        exact_token_count=True, **usage)

    def measure(self, metric):
        current = self.bridge._active.get()
        if current:self.bridge._timing(dict(metric,event_id=current['row']['id']))

    def observe(self, event):
        starts = self.phase_starts.get()
        if starts is None:
            starts = {}; self.phase_starts.set(starts)
        phase = event['phase']
        if phase == 'before_model':
            from am_harness.runtime import mark_dispatch
            mark_dispatch('model')
        for name in ('model', 'tool'):
            if phase == 'before_'+name:
                starts[name] = time.monotonic()
            elif phase == 'after_'+name and name in starts:
                self.trace.emit(event['run_id'], 'span_end', phase=name, status='ok',
                                elapsed_ms=max(0, (time.monotonic()-starts.pop(name))*1000))

    def verified_receipt(self, conn, row, message_id):
        projection = getattr(self.s, 'response_projection', None)
        if projection is not None:
            projection.receipt(conn, row, message_id, verified=True)
        binding = conn.execute('SELECT run_id FROM am_harness_outbox WHERE outbox_id=?', (row['id'],)).fetchone()
        if binding:
            self.effects.observe_receipt(conn, row['id'], message_id, verified=True)
            self.trace.mark_first_useful(binding['run_id'], verified=True)
            self.trace.mark_final(binding['run_id'], status='succeeded', verified=True)

    def close(self):
        self.closed = True
        if self.pool and self.owns_pool:
            self.pool.shutdown(wait=True, cancel_futures=False)
        if self.control_pool:
            self.control_pool.shutdown(wait=True, cancel_futures=False)
        for future in self.futures.values():future.result()
        if self.worker_pool:
            self.worker_pool.shutdown(wait=True, cancel_futures=False)

    def build_context(self, ctx, **data):
        from am_harness.host_context import build_context
        return build_context(self, ctx, **data)
