"""Recoverable event consumption and unique dispatch. No implicit network calls."""
from dataclasses import asdict, replace
from contextlib import nullcontext
import json
import uuid

from ..models import Admission, ContractError, Event, dumps, key
from .contracts import Policy, business_fingerprint
from .information import Information
from . import routing, storage
from .lifecycle import Lifecycle
from .boundary import ToolBoundary
from .cutover import Cutover
from ..assistant.continuity import add_work_context, mark_owner_results_available
from ..assistant.response import response_guidance


class Harness(Lifecycle, ToolBoundary, Cutover):
    def __init__(self, store, config, *, classifier=None, adapters=None):
        config.validate()
        self.store, self.config = store, config
        self.classifier = classifier
        self.adapters = dict(adapters or {})
        storage.initialize(store)
        self.information = Information(store, config)
        with store.transaction() as db:
            tenant = db.execute("SELECT value FROM gw_meta WHERE key='tenant_id'").fetchone()
            if tenant and tenant[0] != config.tenant_id:
                raise ContractError('database belongs to another tenant')
            db.execute("INSERT OR IGNORE INTO gw_meta VALUES('tenant_id',?)", (config.tenant_id,))

    def task(self, task_id, db=None):
        sql = 'SELECT * FROM h1_tasks WHERE task_id=?'
        row = db.execute(sql, (task_id,)).fetchone() if db else self.store.one(sql, (task_id,))
        if not row:
            raise ContractError('unknown task')
        return dict(row)

    def owner_results_available(self, db=None):
        if db is None:
            with self.store.transaction() as connection:
                mark_owner_results_available(self, connection)
        else:
            mark_owner_results_available(self, db)

    def _validate_request_policy(self, event, policy, *, existing=False):
        if policy is None:
            return
        policy.validate()
        if not existing and not policy.revoked and event.chat_id not in policy.source_chats:
            raise ContractError("request source outside authorization")
        expected = self.config.owner_chat_id if event.channel == "personal_dm" else event.chat_id
        if policy.reply_target != expected:
            raise ContractError("reply target requires a separately authorized operation")
        if event.channel == "personal_dm" and policy.principal != self.config.owner_id:
            raise ContractError("passive assistance must act for the owner")
        if event.channel != "personal_dm" and policy.principal != event.sender_id:
            raise ContractError("requester principal mismatch")

    @staticmethod
    def _narrow_policy(old, new):
        """A source edit or replay never grants authority to an existing request."""
        if old is None:
            return new
        if new is None or new.json() == old.json() or new.version < old.version:
            return old
        if new.version == old.version:
            raise ContractError('same policy version has conflicting authority')
        if (new.principal, new.reply_target) != (old.principal, old.reply_target):
            raise ContractError('policy revision cannot change business identity')
        if not set(new.source_chats) <= set(old.source_chats) or not set(new.projects) <= set(old.projects):
            raise ContractError('scope expansion requires a new authorization review')
        if ((new.allow_reply and not old.allow_reply)
                or (new.allow_supplement and not old.allow_supplement)
                or (old.revoked and not new.revoked)):
            raise ContractError('policy refresh can only narrow authority')
        return new

    @staticmethod
    def _preserve_authoritative_source(event, prior):
        """Separate source revisions from later, weaker observations.

        Event.revision is supplied by a trusted source adapter. Once known, its
        body, attachment set and source timestamp cannot be changed by an
        unversioned observation or a duplicate of that revision. Host metadata
        (project bindings, self/direction markers and channel) can still enrich
        the record, and a verified deletion remains an independent source fact.
        """
        if prior is None:
            return event
        previous = Event.from_dict(json.loads(prior['event_json']))
        if not previous.revision:
            return event
        native_prefix = 'feishu-message-v1:'
        if (isinstance(previous.revision, str) and isinstance(event.revision, str)
                and previous.revision.startswith(native_prefix)
                and event.revision.startswith(native_prefix)
                and event.revision != previous.revision
                and event.version_time == prior['version_time']):
            # Arrival order does not order two different snapshots at the same
            # native timestamp. Roll back this observation/cursor and read the
            # message again; generic existing revision formats keep their rules.
            raise ContractError('conflicting native snapshot at the same source timestamp')
        if event.revision and event.revision != previous.revision:
            return event
        if event.version_time < prior['version_time']:
            return event  # Preserve Store's stale-observation handling.
        # is_deleted is already verified by the normalized Event producer. This
        # exception permits the existing recall port without inventing a revision
        # from collection time; an ordinary observation cannot undo a deletion.
        newly_deleted = event.is_deleted and not previous.is_deleted
        return replace(event, body=previous.body, attachments=previous.attachments,
                       mentions=previous.mentions, original_time=previous.original_time,
                       thread_id=previous.thread_id, parent_id=previous.parent_id,
                       root_id=previous.root_id, content_status=previous.content_status,
                       is_deleted=previous.is_deleted or newly_deleted,
                       revision=previous.revision,
                       version_time=event.version_time if newly_deleted else prior['version_time'])

    def ingest(self, event, policy=None, *, cursor=None, coverage=None,
               collection_only=False, legacy_owned=False, _db=None):
        """Commit the effective source revision, inbox responsibility and cursor together.

        R2 Store merges metadata without a body revision. Phase one gives meaningful
        enrichment a local evidence revision so completed inbox/projection rows can
        be reconsidered, while stable task/operation identity prevents replay.
        None means missing authorization metadata; collection_only is an explicit
        no-admission decision. Legacy ownership is sticky across all later polls.
        """
        event.validate()
        if event.tenant_id != self.config.tenant_id:
            raise ContractError("tenant mismatch")
        if policy is not None:
            policy.validate()
        with (self.store.transaction() if _db is None else nullcontext(_db)) as db:
            prior = db.execute(
                "SELECT * FROM gw_items WHERE item_id=?",
                (event.item_id,),
            ).fetchone()
            event = self._preserve_authoritative_source(event, prior)
            inbox = db.execute("SELECT * FROM h1_inbox WHERE item_id=?", (event.item_id,)).fetchone()
            task = db.execute("SELECT * FROM h1_tasks WHERE item_id=?", (event.item_id,)).fetchone()
            inbox_policy = Policy.parse(inbox['policy_json']) if inbox and inbox['policy_json'] != '{}' else None
            task_policy = Policy.parse(task['policy_json']) if task else None
            known_policy = self._narrow_policy(inbox_policy, task_policy)
            if collection_only:
                active_policy = None
            elif policy is not None:
                active_policy = self._narrow_policy(known_policy, policy)
            elif inbox_policy is not None:
                active_policy = self._narrow_policy(known_policy, inbox_policy)
            else:
                active_policy = None  # Explicit collection-only stays so on a weak poll.
            admission = self.store.ingest_in_tx(db, event)
            current = db.execute("SELECT version,event_json FROM gw_items WHERE item_id=?",
                                 (event.item_id,)).fetchone()
            effective = json.loads(current["event_json"])
            policy_json = active_policy.json() if active_policy else "{}"
            policy_changed = bool(inbox and policy_json != inbox["policy_json"])
            local_revision = False
            if prior and not admission.changed:
                previous = json.loads(prior["event_json"])
                native_revision_changed = bool(
                    event.revision and event.version_time >= prior["version_time"]
                    and event.revision != previous.get("revision")
                )
                if native_revision_changed:
                    # A verified new native revision replaces mutable source data;
                    # only weak observations use Store's enrichment union.
                    effective = event.to_dict()
                    effective['is_self'] = bool(previous['is_self'] or event.is_self)
                    if previous['channel'] == 'bot_dm':
                        effective['channel'] = 'bot_dm'
                    if previous['direction'] == 'outbound':
                        effective['direction'] = 'outbound'
                    if previous.get('content_status') == 'draft_edit':
                        effective['content_status'] = 'draft_edit'
                else:
                    effective["revision"] = previous.get("revision", "")
                fields = ("mentions", "project_ids", "attachments", "channel", "direction",
                          "is_self", "parent_id", "root_id", "thread_id")
                enriched = any(effective.get(name) != previous.get(name) for name in fields)
                local_revision = native_revision_changed or enriched or policy_changed
                if local_revision:
                    version = prior["version"] + 1
                    # Store has already merged the old version in place; restore its
                    # exact evidence before adding the new effective version.
                    db.execute("UPDATE gw_item_versions SET event_json=? WHERE item_id=? AND version=?",
                               (prior["event_json"], event.item_id, prior["version"]))
                    effective_source = Event.from_dict(effective)
                    db.execute("""UPDATE gw_items SET version=?,event_json=?,content_hash=?,
                        is_self=?,is_deleted=?,channel=?,thread_id=? WHERE item_id=?""",
                               (version, dumps(effective), effective_source.content_hash,
                                int(effective_source.is_self), int(effective_source.is_deleted),
                                effective_source.channel, effective_source.thread_id, event.item_id))
                    db.execute("INSERT INTO gw_item_versions VALUES(?,?,?,?)",
                               (event.item_id, version, dumps(effective), effective_source.content_hash))
                elif dumps(effective) != current["event_json"]:
                    # Preserve a known native revision when a weaker poll omits it.
                    db.execute("UPDATE gw_items SET event_json=? WHERE item_id=?",
                               (dumps(effective), event.item_id))
                    db.execute("UPDATE gw_item_versions SET event_json=? WHERE item_id=? AND version=?",
                               (dumps(effective), event.item_id, current["version"]))
            effective_event = Event.from_dict(effective)
            self._validate_request_policy(effective_event, active_policy, existing=known_policy is not None)
            owned_by_legacy = legacy_owned or bool(prior and prior['legacy_owned'])
            if owned_by_legacy:
                db.execute('UPDATE gw_items SET legacy_owned=1 WHERE item_id=?', (event.item_id,))
            accepted = bool(
                active_policy and not active_policy.revoked and not effective_event.is_self
                and not owned_by_legacy and not collection_only
                and effective_event.chat_id in active_policy.source_chats
                and not effective_event.is_deleted and effective_event.direction == "inbound"
                and effective_event.kind == "message"
                and (effective_event.channel in {"bot_dm", "personal_dm"}
                     or self.config.bot_id in effective_event.mentions)
            )
            changed = admission.changed or local_revision
            version = db.execute("SELECT version FROM gw_items WHERE item_id=?",
                                 (event.item_id,)).fetchone()[0]
            if changed:
                self.information.invalidate(event.item_id, db)
            if changed or not inbox or bool(inbox["accepted"]) != accepted:
                db.execute("""INSERT INTO h1_inbox(item_id,version,policy_json,state,accepted,created)
                    VALUES(?,?,?,'pending',?,?) ON CONFLICT(item_id) DO UPDATE SET
                    version=excluded.version,policy_json=excluded.policy_json,state='pending',
                    token='',lease_until=NULL,accepted=excluded.accepted""",
                           (event.item_id, version, policy_json, int(accepted), self.store.clock()))
            # Tools and claim() read task authority. Update it in this transaction,
            # including already-running tasks, rather than waiting for a router.
            if task and active_policy is not None:
                db.execute('UPDATE h1_tasks SET policy_json=? WHERE task_id=?',
                           (active_policy.json(), task['task_id']))
            if task and not accepted:
                if owned_by_legacy and not task['business_id']:
                    db.execute("UPDATE h1_tasks SET dispatch_state='legacy_owned' WHERE task_id=?", (task['task_id'],))
                    db.execute("UPDATE h1_dispatch SET state='legacy_owned' WHERE task_id=?", (task['task_id'],))
                else:
                    self._stop_for_source(db, dict(task))
                if owned_by_legacy and task['business_id'] and (not prior or not prior['legacy_owned']):
                    self.store.audit(db, 'legacy_ownership_conflict', task['task_id'],
                                     'existing operation remains responsible for effects; no replay')
            if cursor is not None:
                db.execute("""INSERT INTO h1_sync VALUES(?,?,'saved','',?,?)
                    ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor,state='saved',
                    detail='',coverage=excluded.coverage,updated=excluded.updated""",
                           (event.chat_id, dumps(cursor),
                            dumps(coverage or {"history": "not_verified"}), self.store.clock()))
            return Admission(admission.item_id, admission.inserted, changed, accepted)

    def _claim_route_input(self, *, classification=False):
        now = self.store.clock()
        waiting, working = (("classification_pending", "classifying") if classification
                            else ("pending", "routing"))
        with self.store.transaction() as db:
            db.execute("UPDATE h1_inbox SET state=?,token='' WHERE state=? AND lease_until<?",
                       (waiting, working, now))
            row = db.execute("SELECT * FROM h1_inbox WHERE state=? ORDER BY created,item_id LIMIT 1",
                             (waiting,)).fetchone()
            if row is None:
                return None
            pending = dict(row)
            token = uuid.uuid4().hex
            db.execute("UPDATE h1_inbox SET state=?,token=?,lease_until=? WHERE item_id=?",
                       (working, token, now + self.config.classification_budget_seconds + 5,
                        row["item_id"]))
            event = self.store.get_event({"item_id": row["item_id"], "version": row["version"]}, db)
            return pending, event, token

    def _classify_route(self, event, pending):
        try:
            if self.classifier is None:
                raise ContractError("classifier_unconfigured")
            budget = min(self.config.classification_budget_seconds,
                         pending["created"] + self.config.dispatch_deadline_seconds
                         - self.store.clock() - self.config.dispatch_reserve_seconds)
            if budget <= 0:
                raise TimeoutError("request_deadline")
            return routing.validate(self.classifier.classify(event, budget)), ""
        except Exception as exc:
            # Keep remote body/token out of audit text; failure affects this item only.
            return routing.result("unknown", "unknown", reason="classification_error"), type(exc).__name__

    def route_one(self, *, defer_classification=False):
        """Process one lightweight item; the scheduler always defers slow classification.

        The synchronous default is retained for direct callers and existing tests.
        Deferred work is durable and has its own bounded worker, not a growing
        in-memory futures queue.
        """
        claimed = self._claim_route_input()
        if claimed is None:
            return None
        pending, event, token = claimed
        route, error = None, ""
        if pending["accepted"] and not event.is_deleted:
            route = routing.rules(event.body, passive=event.channel == "personal_dm")
            if route is None and self.config.capabilities['project.query'].adapter == 'interactive':
                # Only the trusted assistant ledger lookup writes this table.
                # A message body cannot declare a route or expand a policy.
                with self.store.transaction() as db:
                    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='r7_query_route_hints'").fetchone():
                        hint = db.execute('SELECT * FROM r7_query_route_hints WHERE item_id=?',
                                          (event.item_id,)).fetchone()
                        if (hint and hint['source_version'] == pending['version']
                                and hint['content_hash'] == event.content_hash
                                and hint['policy_hash'] == key(pending['policy_json'])):
                            projects = json.loads(hint['project_ids'])
                            policy = Policy.parse(pending['policy_json'])
                            if projects and set(projects) <= set(policy.projects) and not policy.revoked:
                                route = routing.result('project.query', reason='trusted_ledger_subject')
            if route is None:
                if defer_classification:
                    with self.store.transaction() as db:
                        updated = db.execute("""UPDATE h1_inbox SET state='classification_pending',
                            token='',lease_until=NULL WHERE item_id=? AND token=? AND version=?""",
                            (pending["item_id"], token, pending["version"])).rowcount
                    return ({"item_id": event.item_id, "disposition": "classification_pending"}
                            if updated else None)
                route, error = self._classify_route(event, pending)
        return self._finish_route(pending, event, token, route, error)

    def classify_one(self):
        """Claim one persisted classification item without holding a DB lock during inference."""
        claimed = self._claim_route_input(classification=True)
        if claimed is None:
            return None
        pending, event, token = claimed
        route, error = self._classify_route(event, pending)
        return self._finish_route(pending, event, token, route, error)

    def _finish_route(self, pending, event, token, route, error):
        with self.store.transaction() as db:
            current = db.execute("SELECT * FROM h1_inbox WHERE item_id=?",
                                 (pending["item_id"],)).fetchone()
            if current is None or current["token"] != token or current["version"] != pending["version"]:
                return None  # Source or authority changed while pure classification was running.
            tid = "task_" + key(event.item_id)[:32]
            old = db.execute("SELECT * FROM h1_tasks WHERE task_id=?", (tid,)).fetchone()
            source = db.execute('SELECT legacy_owned FROM gw_items WHERE item_id=?', (event.item_id,)).fetchone()
            if not current['accepted'] or source['legacy_owned']:
                route = None
            if route is None:
                if old:
                    self._stop_for_source(db, dict(old))
                db.execute("UPDATE h1_inbox SET state='done',lease_until=NULL WHERE item_id=?",
                           (event.item_id,))
                return {"item_id": event.item_id, "disposition": "collection_only"}
            if old and old["business_id"]:
                self._stop_for_source(db, dict(old))
                db.execute("INSERT OR IGNORE INTO h1_revisions VALUES(?,?,?,?,?)",
                           (tid, pending["version"], dumps(event.to_dict()),
                            "source_changed_after_dispatch", self.store.clock()))
            else:
                self._save_routed(db, tid, event, Policy.parse(current["policy_json"]), route, error,
                                  pending["version"], pending["created"], old)
            db.execute("UPDATE h1_inbox SET state='done',lease_until=NULL WHERE item_id=?",
                       (event.item_id,))
            return self.task(tid, db)

    def _stop_for_source(self, db, task):
        db.execute("UPDATE h1_tasks SET last_error='source_changed',verification_state='insufficient' WHERE task_id=?", (task['task_id'],))
        db.execute("UPDATE h1_dispatch SET state='source_changed' WHERE task_id=? AND state='pending'", (task['task_id'],))
        # Active executions retain their scope until an actual stopped receipt.

    def _save_routed(self, db, tid, event, policy, route, error, revision, created, old=None):
        if old:
            policy = self._narrow_policy(Policy.parse(old['policy_json']), policy)
        intent = route['intent']
        state = 'classification_error' if error else route['decision']
        if route['reason_code'] == 'passive_nonwork':
            state = 'out_of_scope'
        if state == 'matched':
            state = 'resolved'
        capability = self.config.capabilities.get(intent)
        scope = (capability.execution_scope.format(tenant_id=event.tenant_id,
                 chat_id=event.chat_id, principal=policy.principal)
                 if capability else '')
        read_only_partial = bool(capability and capability.adapter == 'interactive'
            and intent in {'project.query', 'conversation.reply', 'work_message.assist'}
            and set(route['secondary_intents']) <= {'delivery.extra'})
        if route['secondary_intents'] and not read_only_partial:
            state = 'capability_unavailable'  # no registered executor handles the whole compound request
        elif state == 'resolved' and capability and not capability.available:
            state = 'capability_unavailable'
        if state == 'resolved' and intent == 'context.supplement':
            if event.sender_id != self.config.owner_id or not policy.allow_supplement:
                state = 'authorization_required'
            elif len(event.project_ids) != 1 or event.project_ids[0] not in policy.projects:
                state = 'needs_clarification'
                route['missing_fields'] = ['project_id']
                route['clarification_question'] = '请指定补充信息所属项目。'
        deadline = old['deadline'] if old else created + self.config.dispatch_deadline_seconds
        dispatch = 'pending' if state == 'resolved' and intent != 'context.supplement' else 'none'
        if dispatch == 'pending':
            self.owner_results_available(db)
            # Count unresolved work, not the lifetime number of accepted requests.
            # Keep uncertain effects and held executions in the capacity accounting.
            count = db.execute("""SELECT count(*) FROM h1_tasks t WHERE t.scope=? AND t.task_id<>?
                AND t.dispatch_state IN ('pending','dispatching','accepted','unknown')
                AND NOT (t.execution_state IN ('succeeded','failed','cancelled')
                    AND ((t.delivery_state='delivered' AND t.verification_state='sufficient')
                         OR (t.delivery_state='available_to_owner' AND t.verification_state!='conflict'
                             AND t.last_error!='source_changed')
                         OR t.execution_state='cancelled')
                    AND NOT EXISTS(SELECT 1 FROM h1_effects e WHERE e.task_id=t.task_id
                                   AND e.state IN ('sending','unknown'))
                    AND NOT EXISTS(SELECT 1 FROM h1_sessions s WHERE s.task_id=t.task_id
                                   AND s.state<>'free'))""", (scope, tid)).fetchone()[0]
            if count >= self.config.queue_limit:
                state, dispatch = 'queue_full', 'none'
        stamps = json.loads(old['stamps']) if old else {'t_m': event.original_time, 't_i': created}
        stamps['t_r'] = self.store.clock()
        request = event.to_dict()
        values = (tid, event.item_id, revision, dumps(request), policy.json(), dumps(route), intent, state,
                  dispatch, 'queued', 'not_delivered', 'pending', scope, deadline,
                  created + self.config.execution_deadline_seconds, dumps(stamps), error, created)
        db.execute("""INSERT INTO h1_tasks(task_id,item_id,revision,request_json,policy_json,route_json,intent,
          route_state,dispatch_state,execution_state,delivery_state,verification_state,scope,deadline,
          execution_deadline,stamps,last_error,created) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(task_id) DO UPDATE SET revision=excluded.revision,request_json=excluded.request_json,
          policy_json=excluded.policy_json,route_json=excluded.route_json,intent=excluded.intent,
          route_state=excluded.route_state,dispatch_state=excluded.dispatch_state,scope=excluded.scope,
          stamps=excluded.stamps,last_error=excluded.last_error""", values)
        db.execute('INSERT OR REPLACE INTO h1_revisions VALUES(?,?,?,?,?)',
                   (tid, revision, dumps(request), 'routed', self.store.clock()))
        db.execute('UPDATE h1_tasks SET source_version=(SELECT version FROM gw_items WHERE item_id=?) WHERE task_id=?',
                   (event.item_id,tid))
        if dispatch == 'pending':
            db.execute("INSERT OR REPLACE INTO h1_dispatch VALUES(?,'pending','')", (tid,))
        if state == 'resolved' and intent == 'context.supplement':
            info = self.information.append(project=event.project_ids[0], nature='pending_requirement',
                body=event.body, sources=[{'item_id': event.item_id, 'version': db.execute('SELECT version FROM gw_items WHERE item_id=?',(event.item_id,)).fetchone()[0]}],
                readers=[policy.principal], audiences=[policy.reply_target], operation_id=tid, db=db)
            db.execute("UPDATE h1_tasks SET execution_state='succeeded',result_json=?,verification_state='sufficient',business_id=? WHERE task_id=?",
                       (dumps({'recorded': info['info_id'], 'minimum_visible_version': info['seq'],
                               'formal_business_changed': False}), 'op_' + key(tid,'supplement')[:32], tid))

    def update_policy(self, task_id, policy):
        policy.validate()
        with self.store.transaction() as db:
            row = self.task(task_id, db)
            old = Policy.parse(row['policy_json'])
            if policy.version <= old.version:
                raise ContractError('policy update needs a newer version')
            policy = self._narrow_policy(old, policy)
            db.execute('UPDATE h1_tasks SET policy_json=? WHERE task_id=?', (policy.json(), task_id))
            source = db.execute('SELECT chat_id,legacy_owned FROM gw_items WHERE item_id=?', (row['item_id'],)).fetchone()
            denied = policy.revoked or source['chat_id'] not in policy.source_chats or source['legacy_owned']
            # A policy change invalidates an in-flight pure classifier's token.
            # Do not promote an explicitly collection-only row back to accepted.
            db.execute("""UPDATE h1_inbox SET policy_json=CASE WHEN policy_json='{}' THEN '{}' ELSE ? END,
                accepted=CASE WHEN ? THEN 0 ELSE accepted END,
                state=CASE WHEN state IN ('routing','classifying','classification_pending') THEN 'pending' ELSE state END,
                token='',lease_until=NULL WHERE item_id=?""", (policy.json(), int(denied), row['item_id']))
            if denied and not row['business_id']:
                state = 'revoked' if policy.revoked else 'source_changed'
                db.execute('UPDATE h1_tasks SET dispatch_state=? WHERE task_id=?', (state, task_id))
                db.execute('UPDATE h1_dispatch SET state=? WHERE task_id=?', (state, task_id))

    def claim(self, *, require_cutover=False):
        now = self.store.clock()
        with self.store.transaction() as db:
            for row in db.execute("SELECT task_id FROM h1_tasks WHERE dispatch_state='pending' AND deadline<=?", (now,)).fetchall():
                db.execute("UPDATE h1_tasks SET dispatch_state='expired',last_error='not_executed_deadline' WHERE task_id=?", (row[0],))
                db.execute("UPDATE h1_dispatch SET state='expired' WHERE task_id=?", (row[0],))
            row = db.execute("""SELECT t.* FROM h1_tasks t JOIN h1_dispatch d USING(task_id)
                WHERE d.state='pending' AND t.dispatch_state='pending' AND t.deadline>?
                AND EXISTS(SELECT 1 FROM h1_inbox b WHERE b.item_id=t.item_id AND b.accepted=1)
                AND EXISTS(SELECT 1 FROM gw_items i WHERE i.item_id=t.item_id AND i.legacy_owned=0)
                AND NOT EXISTS(SELECT 1 FROM h1_sessions s WHERE s.scope=t.scope AND s.state!='free')
                AND NOT EXISTS(SELECT 1 FROM h1_cutover c WHERE c.scope=t.scope AND c.state!='harness')
                AND (?=0 OR EXISTS(SELECT 1 FROM h1_cutover c WHERE c.scope=t.scope AND c.state='harness'))
                ORDER BY t.created,t.task_id LIMIT 1""", (now,int(require_cutover))).fetchone()
            if not row:
                return None
            task = dict(row)
            policy = Policy.parse(task['policy_json'])
            if policy.revoked:
                db.execute("UPDATE h1_tasks SET dispatch_state='revoked' WHERE task_id=?", (task['task_id'],))
                db.execute("UPDATE h1_dispatch SET state='revoked' WHERE task_id=?", (task['task_id'],))
                return None
            event = Event.from_dict(json.loads(task['request_json']))
            source = db.execute('SELECT version,is_deleted FROM gw_items WHERE item_id=?',(task['item_id'],)).fetchone()
            if source['is_deleted'] or source['version'] != task['source_version'] or event.chat_id not in policy.source_chats:
                self._stop_for_source(db,task)
                db.execute("UPDATE h1_tasks SET dispatch_state='source_changed' WHERE task_id=?",(task['task_id'],))
                return None
            context = add_work_context(self, db, task, policy, self.information.snapshot(policy, db=db))
            fingerprint = business_fingerprint(text=event.body, intent=task['intent'], principal=policy.principal,
                reply_target=policy.reply_target, attachments=event.attachments, inputs={'project_ids': event.project_ids})
            if task['fingerprint'] and task['fingerprint'] != fingerprint:
                raise ContractError('same operation different business inputs')
            session = db.execute('SELECT generation FROM h1_sessions WHERE scope=?', (task['scope'],)).fetchone()
            generation = session[0] + 1 if session else 1
            attempt = uuid.uuid4().hex
            operation = task['business_id'] or 'op_' + key(task['task_id'], 'primary')[:32]
            db.execute("INSERT OR REPLACE INTO h1_sessions VALUES(?,?,?,'held')", (task['scope'], generation, task['task_id']))
            db.execute("""UPDATE h1_tasks SET dispatch_state='dispatching',business_id=?,fingerprint=?,
                generation=?,attempt=?,context_json=?,lease_until=? WHERE task_id=?""",
                       (operation, fingerprint, generation, attempt, dumps(context), now+self.config.lease_seconds, task['task_id']))
            db.execute("UPDATE h1_dispatch SET state='claimed',attempt=? WHERE task_id=?", (attempt, task['task_id']))
            return self.envelope(self.task(task['task_id'], db))

    def envelope(self, task):
        request, policy = json.loads(task['request_json']), Policy.parse(task['policy_json'])
        return {'schema_version': '1.0', 'identity': {'task_id': task['task_id'], 'request_revision': task['revision'],
                'business_operation_id': task['business_id'], 'idempotency_key': task['business_id'],
                'dispatch_attempt_id': task['attempt'], 'business_fingerprint': task['fingerprint']},
                'routing': {'intent': task['intent'], 'route_version': self.config.route_version,
                            'adapter': self.config.capabilities[task['intent']].adapter},
                'source': {k: request[k] for k in ('tenant_id','native_id','chat_id','thread_id','sender_id')},
                'payload': {'raw_text': request['body'], 'attachment_refs': request['attachments'],
                            'structured_inputs': {'project_ids': request['project_ids']}},
                'request_context': {'requester_id':policy.principal,'acting_principal':policy.principal,
                                    'authorization_ref':policy.authorization_ref},
                'policy': json.loads(policy.json()), 'context': json.loads(task['context_json'] or '{}'),
                'execution': {'scope': task['scope'], 'generation': task['generation'],
                              'accept_deadline': task['deadline'], 'completion_deadline': task['execution_deadline']},
                'output': {'reply_target': policy.reply_target,
                           'response_guidance': response_guidance(channel=request['channel'],
                               owner_request=policy.principal == self.config.owner_id),
                           'reply_binding': request['native_id'] if request['chat_id'] == policy.reply_target else None}}

    def expire(self):
        with self.store.transaction() as db:
            now = self.store.clock()
            rows = db.execute("""SELECT task_id FROM h1_tasks WHERE dispatch_state IN ('dispatching','accepted','unknown')
                AND execution_state NOT IN ('succeeded','failed','cancelled')
                AND (lease_until<? OR execution_deadline<?)""", (now, now)).fetchall()
            for row in rows:
                db.execute("UPDATE h1_tasks SET execution_state='unknown',last_error='execution_status_required' WHERE task_id=?", (row[0],))
            db.execute("UPDATE h1_effects SET state='unknown' WHERE state='sending' AND created<?", (now-self.config.lease_seconds,))
            return len(rows)  # session held; elapsed time is not proof of stopped execution

    def pending(self):
        return self.store.rows("""SELECT task_id,request_json,route_state,route_json,dispatch_state,
            execution_state,delivery_state,last_error FROM h1_tasks
            WHERE route_state!='resolved' OR execution_state='unknown' ORDER BY created""")

    def resume(self, task_id, actor, *, supplement='', intent=None, ignore=False):
        with self.store.transaction() as db:
            task = self.task(task_id, db)
            event = Event.from_dict(json.loads(task['request_json']))
            if actor not in {event.sender_id, self.config.owner_id}:
                raise ContractError('supplement actor mismatch')
            if task['business_id'] or task['execution_state'] == 'succeeded':
                raise ContractError('existing effect requires reconciliation, not restart')
            if ignore:
                db.execute("UPDATE h1_tasks SET route_state='ignored',dispatch_state='none' WHERE task_id=?", (task_id,))
                db.execute("DELETE FROM h1_dispatch WHERE task_id=?", (task_id,))
                return self.task(task_id, db)
            admission = db.execute("""SELECT b.accepted,i.legacy_owned FROM h1_inbox b
                JOIN gw_items i USING(item_id) WHERE item_id=?""", (task['item_id'],)).fetchone()
            if not admission or not admission['accepted'] or admission['legacy_owned']:
                raise ContractError('request is not owned and authorized by the Harness')
            if task['route_state'] == 'classification_error' and intent is not None:
                raise ContractError('classification outage needs technical retry')
            raw = event.to_dict()
            raw['body'] += ('\n补充：' + supplement) if supplement else ''
            event = Event.from_dict(raw)
            route = routing.result(intent, reason='manual_selection') if intent else routing.rules(event.body)
            if route is None:
                raise ContractError('still needs routing clarification')
            routing.validate(route)
            # User choice never grants read, write or delivery permissions.
            self._save_routed(db, task_id, event, Policy.parse(task['policy_json']), route, '',
                              task['revision']+1, task['created'], task)
            return self.task(task_id, db)

    def retry_classification(self, task_id):
        with self.store.transaction() as db:
            task = self.task(task_id, db)
            if task['route_state'] != 'classification_error' or task['business_id']:
                raise ContractError('not a classification recovery')
            db.execute("UPDATE h1_inbox SET state='pending',token='' WHERE item_id=?", (task['item_id'],))
