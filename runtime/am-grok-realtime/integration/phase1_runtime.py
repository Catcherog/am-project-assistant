"""Candidate ports into the existing consumer and review area; no new connection.

This is intentionally not a Grok Bot endpoint implementation. External activation
fails while any actual receiver/result/memory/polling binding is missing.
"""
from dataclasses import dataclass, replace
import json
from pathlib import Path
import threading
import time

from am_gateway.models import ContractError
from am_gateway.feishu_snapshot import normalize_feishu_snapshot, snapshot_host_payload
from am_gateway.phase1 import Harness, HarnessConfig
from am_gateway.store import Store
from integration.am_multi_ai_host import HostAdmission, normalize_message


@dataclass
class Phase1Ports:
    config: HarnessConfig
    database_path: str
    verified_scope: object
    authorization_for: object
    projects_for: object
    legacy_control: object
    classifier: object = None
    adapters: object = None
    authenticated_actor: object = None
    normalize_recall: object = None
    # Only actual host adapter evidence may populate these, not a settings switch.
    receiver_evidence: str = ''
    result_channel_evidence: str = ''
    memory_boundary_evidence: str = ''
    polling_exclusion_evidence: str = ''
    # A concrete FeishuMessageSnapshotReader using the existing authenticated
    # read transport. It is never invoked by the websocket hot path or install().
    snapshot_reader: object = None
    verified_snapshot_scope: object = None
    # R4 supplies an actual assembly function, rather than evidence strings.
    runtime_factory: object = None


class ReviewArea:
    """Reuse the host's pending-review projection, without an external send here."""
    def __init__(self, harness):
        self.harness = harness

    def items(self):
        return self.harness.pending()

    def apply(self, *, authenticated_actor, task_id, action, intent=None, supplement=''):
        if action == 'technical_retry':
            if authenticated_actor != self.harness.config.owner_id:
                raise ContractError('operator required')
            return self.harness.retry_classification(task_id)
        if action not in {'resume','ignore'}:
            raise ContractError('unsupported review action')
        return self.harness.resume(task_id,authenticated_actor,supplement=supplement,
                                   intent=intent,ignore=action=='ignore')


class Phase1Ingress:
    def __init__(self, harness, ports, scheduler=None):
        self.gateway, self.ports, self.scheduler = harness, ports, scheduler
        self.assistant = None
        self.review = ReviewArea(harness)
        with self.gateway.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS h1_snapshot_reads(
                source_id TEXT NOT NULL, message_id TEXT NOT NULL,
                chat_id TEXT NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL,
                accepted_revision TEXT NOT NULL DEFAULT '', updated REAL NOT NULL,
                PRIMARY KEY(source_id,message_id))''')

    def ingest(self, payload):
        if not self.ports.verified_scope(payload):
            return HostAdmission(False,False,False,'',False)
        event = normalize_message(payload,self.gateway.config,project_ids=self.ports.projects_for(payload))
        # Existing document commands, controls, and already-owned operations remain owned.
        legacy = self.ports.legacy_control(payload)
        if self.assistant is not None:
            captured = self.assistant.capture(event, legacy_owned=legacy)
            if captured:
                if self.scheduler:
                    self.scheduler.notify()
                return HostAdmission(True, True, True, event.item_id, captured['duplicate'])
        policy = self.ports.authorization_for(event)
        admission = self.gateway.ingest(event, policy, collection_only=policy is None,
                                        legacy_owned=legacy)
        if self.scheduler:
            self.scheduler.notify()
        return HostAdmission(True,admission.handled,admission.handled,event.item_id,not admission.inserted)

    def ingest_feishu_snapshot(self, message, *, source, expected_message_id=None):
        """Admit a complete raw get/list item through Harness, never R2 Store.

        The caller supplies a host-approved source binding. A JSON flag inside a
        normal receive/poll payload cannot select this authoritative contract.
        """
        event = normalize_feishu_snapshot(message, self.gateway.config, source,
                                          expected_message_id=expected_message_id)
        if source.channel == 'personal_dm' and event.sender_id == self.gateway.config.owner_id:
            event = replace(event, is_self=True, direction='outbound')
        if self.ports.verified_snapshot_scope is None:
            raise ContractError('raw snapshot scope verifier is not bound')
        if not self.ports.verified_snapshot_scope(source, message):
            return HostAdmission(False, False, False, '', False)
        payload = snapshot_host_payload(message, self.gateway.config, source)
        legacy = self.ports.legacy_control(payload)
        if self.assistant is not None:
            captured = self.assistant.capture(event, legacy_owned=legacy)
            if captured:
                if self.scheduler:
                    self.scheduler.notify()
                return HostAdmission(True, True, True, event.item_id, captured['duplicate'])
        policy = self.ports.authorization_for(event)
        admission = self.gateway.ingest(event, policy, collection_only=policy is None,
                                        legacy_owned=legacy)
        if self.scheduler:
            self.scheduler.notify()
        return HostAdmission(True, admission.handled, admission.handled, event.item_id,
                             not admission.inserted and not admission.changed)

    def refresh_message(self, message_id, *, source):
        """Explicit, read-only get of an already known message outside ingress.

        Run from the host's existing background/validation worker. The normal
        ingest callback remains local and returns without waiting for an API.
        A failed read/parse is recorded as unavailable, never as attachment removal.
        """
        self._validate_snapshot_status_key(message_id, source)
        try:
            if self.ports.snapshot_reader is None:
                raise ContractError('Feishu snapshot reader is not bound')
            message = self.ports.snapshot_reader.read_message(source, message_id)
            admission = self.ingest_feishu_snapshot(message, source=source,
                                                    expected_message_id=message_id)
            if admission.captured:
                row, detail = None, ''
                if self.assistant is not None:
                    row = self.gateway.store.one('SELECT event_json,state FROM r5_intake WHERE item_id=?',
                                                 (admission.item_id,))
                    if row:
                        detail = 'assistant_intake:' + row['state']
                if row is None:
                    row = self.gateway.store.one('SELECT event_json FROM gw_items WHERE item_id=?',
                                                 (admission.item_id,))
                if row is None:
                    raise ContractError('accepted snapshot source is unavailable')
                revision = json.loads(row['event_json']).get('revision', '')
                self._record_snapshot_read(message_id, source, 'saved', detail, revision)
            else:
                self._record_snapshot_read(message_id, source, 'scope_rejected',
                                           'raw_snapshot_scope_rejected')
        except Exception as exc:
            self._record_snapshot_read(message_id, source, 'failed', type(exc).__name__)
            raise
        return admission

    def _validate_snapshot_status_key(self, message_id, source):
        source.validate()
        if source.tenant_id != self.gateway.config.tenant_id:
            raise ContractError('snapshot source scope mismatch')
        if not isinstance(message_id, str) or not message_id:
            raise ContractError('snapshot message identity required')

    def _record_snapshot_read(self, message_id, source, state, detail, accepted_revision=None):
        # A one-message read never advances or replaces chat-wide synchronization
        # coverage. Failures preserve the last actually accepted source revision.
        with self.gateway.store.transaction() as db:
            db.execute('''INSERT INTO h1_snapshot_reads VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(source_id,message_id) DO UPDATE SET
                chat_id=excluded.chat_id,state=excluded.state,detail=excluded.detail,
                accepted_revision=CASE WHEN excluded.state='saved'
                    THEN excluded.accepted_revision ELSE h1_snapshot_reads.accepted_revision END,
                updated=excluded.updated''',
                       (source.source_id, message_id, source.effective_chat_id,
                        state, detail[:120], accepted_revision or '', self.gateway.store.clock()))

    def snapshot_refresh_status(self, message_id, *, source):
        """Return one refresh diagnostic without reading a service or changing state."""
        self._validate_snapshot_status_key(message_id, source)
        return self.gateway.store.one('''SELECT * FROM h1_snapshot_reads
            WHERE source_id=? AND message_id=? AND chat_id=?''',
                                      (source.source_id, message_id, source.effective_chat_id))

    def card_callback(self, payload):
        # Reuse an actual authenticated host callback; never trust an actor in action.value.
        event = payload.get('event',payload)
        value = event.get('action',{}).get('value',{})
        if isinstance(value, dict) and value.get('kind') == 'assistant_scope' and self.assistant is not None:
            if self.ports.authenticated_actor is None:
                raise ContractError('host callback identity binding unavailable')
            actor = self.ports.authenticated_actor(payload)
            self.assistant.handle('POST', '/v1/assistant/tasks/' + value['task_id'] + '/decision',
                {'scope_digest': value['scope_digest'], 'accept': value['accept'],
                 'source_ref': 'feishu:scope-decision:' + value['task_id'] + ':' + value['scope_digest']}, actor=actor)
            if self.scheduler:
                self.scheduler.notify()
            return {'toast': {'type': 'info', 'content': '已记录是否承接，后续进展会汇报。'}}
        if isinstance(value, dict) and value.get('kind') == 'assistant_action' and self.assistant is not None:
            if self.ports.authenticated_actor is None:
                raise ContractError('host callback identity binding unavailable')
            actor = self.ports.authenticated_actor(payload)
            self.assistant.handle('POST', '/v1/assistant/actions/' + value['action_id'] + '/decision',
                {'version': value['version'], 'digest': value['digest'], 'approve': value['approve']}, actor=actor)
            if self.scheduler:
                self.scheduler.notify()
            return {'toast': {'type': 'info', 'content': '已记录本次决定，后续执行结果会汇报。'}}
        if not isinstance(value,dict) or value.get('kind')!='harness_review':
            return None
        if self.ports.authenticated_actor is None:
            raise ContractError('host callback identity binding unavailable')
        actor = self.ports.authenticated_actor(payload)
        selected = event.get('action', {}).get('option')
        self.review.apply(authenticated_actor=actor,task_id=value['task_id'],action=value['action'],
                          intent=selected or value.get('intent'),supplement=value.get('supplement',''))
        if self.scheduler:
            self.scheduler.notify()
        return {'toast':{'type':'info','content':'任务记录已更新，实际执行状态以核验结果为准。'}}

    def recall(self, payload):
        # Normalize recall through the same verified source adapter before ingestion.
        if self.ports.normalize_recall is None:
            self.gateway.information.sync_failure('recalls','recall_adapter_unavailable')
            return {'status':'unsupported','source_changed':False}
        event=self.ports.normalize_recall(payload)
        if not event.is_deleted:
            raise ContractError('recall adapter must return a verified deletion')
        if self.assistant is not None:
            self.assistant.revoke(event)
        self.gateway.ingest(event)
        if self.scheduler:
            self.scheduler.notify()
        return {'status':'recorded','source_changed':True}


class Phase1Scheduler:
    """One fast routing worker plus one bounded slow-classification worker.

    Both consume durable inbox states. No model call runs on the fast worker;
    the classifier backlog is persisted rather than submitted to an unbounded
    in-memory executor. This scheduler still does not activate an external Bot.
    """
    def __init__(self, harness):
        self.harness = harness
        self.wakeup = threading.Event()
        self.classification_wakeup = threading.Event()
        self.stopped = threading.Event()
        self.thread = None
        self.classifier_thread = None
        self._control_lock = threading.Lock()

    def notify(self):
        self.wakeup.set()

    def start(self):
        with self._control_lock:
            workers = (self.thread, self.classifier_thread)
            if any(worker is not None and worker.is_alive() for worker in workers):
                return
            self.stopped.clear()
            self.thread = threading.Thread(target=self._run, daemon=True,
                                           name="existing-gateway-phase1")
            self.classifier_thread = threading.Thread(target=self._run_classifier, daemon=True,
                                                      name="existing-gateway-phase1-classifier")
            self.thread.start()
            self.classifier_thread.start()
            # Recover persisted work immediately, including after an explicit stop/start.
            self.wakeup.set()
            self.classification_wakeup.set()

    def _record_error(self, kind, exc):
        with self.harness.store.transaction() as db:
            self.harness.store.audit(db, kind, "scheduler", type(exc).__name__)

    def _run(self):
        while not self.stopped.is_set():
            self.wakeup.wait(.5)
            self.wakeup.clear()
            try:
                processed = 0
                for _ in range(50):
                    if self.stopped.is_set():
                        break
                    outcome = self.harness.route_one(defer_classification=True)
                    if outcome is None:
                        break
                    processed += 1
                    if outcome.get("disposition") == "classification_pending":
                        self.classification_wakeup.set()
                if self.stopped.is_set():
                    break
                self.harness.expire()
                self.harness.information.project_sources()
                if processed == 50:
                    self.wakeup.set()  # no artificial half-second gap between full batches
            except Exception as exc:
                self._record_error("phase1_scheduler_error", exc)

    def _run_classifier(self):
        while not self.stopped.is_set():
            self.classification_wakeup.wait(.5)
            self.classification_wakeup.clear()
            try:
                while not self.stopped.is_set():
                    if self.harness.classify_one() is None:
                        break
                    self.wakeup.set()
            except Exception as exc:
                self._record_error("phase1_classifier_worker_error", exc)

    def stop(self):
        # A running pure classification may finish and commit its result; no new
        # work starts after stopping. Do not claim success if its transport hangs.
        with self._control_lock:
            self.stopped.set()
            self.wakeup.set()
            self.classification_wakeup.set()
            deadline = time.monotonic() + self.harness.config.classification_budget_seconds + 2
            for worker in (self.thread, self.classifier_thread):
                if worker is not None and worker is not threading.current_thread():
                    worker.join(max(0, deadline - time.monotonic()))
            return {"stopped": not any(worker is not None and worker.is_alive()
                                       for worker in (self.thread, self.classifier_thread))}


def install(ports, *, activate=False):
    if not isinstance(ports,Phase1Ports):
        raise ContractError('trusted phase-one host ports required')
    if activate:
        if callable(ports.runtime_factory):
            ingress, runtime = ports.runtime_factory(ports)
            runtime.start()
            return ingress, runtime
        required = ('receiver_evidence','result_channel_evidence','memory_boundary_evidence','polling_exclusion_evidence')
        if any(not getattr(ports,k) for k in required):
            raise ContractError('Grok Bot activation blocked: actual receiver/result/memory/polling bindings missing')
        raise ContractError('real Grok Bot adapter not implemented; bind the R4 runtime factory')
    harness=Harness(Store(ports.database_path),ports.config,classifier=ports.classifier,adapters=ports.adapters)
    scheduler=Phase1Scheduler(harness)
    return Phase1Ingress(harness,ports,scheduler),scheduler
