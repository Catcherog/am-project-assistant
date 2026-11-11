"""Connect the assistant's permissions and work records to the existing runtime.

The event callback only writes a durable intake. Identity lookups, model work,
and sending happen in existing background lanes. A generated reply is not a
sent reply, and a delivered answer is not a verified business completion.
"""
from __future__ import annotations

from dataclasses import replace
import json
import re
import threading
import time
from urllib.parse import unquote, urlsplit
import uuid

from ..models import ContractError, Event, dumps, key
from ..phase1.contracts import Policy
from .actions import AssistantActions
from .policy import ActionContext, SourceEvidence
from .reports import AssistantReports
from .workbook import AssistantWorkbook, ORIGINS
from .telemetry import AssistantTrace
from .improvement import ImprovementJournal, _FEEDBACK_RULES


VERSION = "2026-11-10.R7"
_CLOSE = re.compile(r"^(?:谢谢[，,。！! ]*)?(?:先这样|就这样|没有其他了|没别的了|结束这次对话)[。！! ]*$")
_GREETING = re.compile(r"^(?:你好|早上好|下午好|谢谢|辛苦了)[。！! ]*$")
_TODO = re.compile(r"^(?:查一下|看看|查询)?\s*(?:我的待办|我的任务|待办清单)[？?。 ]*$")
_STATE_LABELS = {"pending": "待处理", "in_progress": "处理中", "waiting_feedback": "等待反馈",
    "awaiting_owner": "待示例负责人判断", "reported_complete": "对方反馈完成，待核验", "completed": "已核实完成",
    "blocked": "受阻", "cancelled": "已取消"}


def _completion_feedback(text):
    # Keep uncertain language as a quoted report. This small fast path never
    # upgrades a colleague's assertion into a verified project acceptance.
    if re.search(r"预计|计划|希望|如果|假如|除非|是否|吗|[？?]|未|没|尚|等待|待核|请|帮我", text):
        return False
    return bool(re.search(r"(?:^|[，,。；;\s])(?:测试|报告|任务|工作|样件)?(?:已经|已|都)?(?:完成|做完|测完)了?(?:[，,。；;！!]|$)", text))


class AssistantCoordinator:
    def __init__(self, harness, *, directory, policy, authority, on_dm_bound=None,
                 report_sender=None, action_executor=None, settings=None, project_reader=None):
        self.harness, self.store, self.cfg = harness, harness.store, harness.config
        self.directory, self.policy, self.authority = directory, policy, authority
        self.on_dm_bound = on_dm_bound
        self.report_sender, self.action_executor = report_sender, action_executor
        self.project_reader = project_reader
        self.settings = dict(settings or {})
        self.project_answer_mode = self.settings.get('project_answer_mode', 'direct')
        if self.project_answer_mode not in {'direct', 'semantic'}:
            raise ContractError('project_answer_mode must be direct or semantic')
        if (self.project_answer_mode == 'semantic'
                and self.cfg.capabilities['project.query'].adapter != 'interactive'):
            raise ContractError('semantic project answers require the interactive capability')
        self.owner_id = self.cfg.owner_id
        self.book = AssistantWorkbook(self.store.path, self.owner_id, clock=self.store.clock)
        self.reports = AssistantReports(self.store.path, self.owner_id, self.cfg.owner_chat_id,
            idle_seconds=self.settings.get("conversation_idle_seconds", 600))
        self.actions = AssistantActions(self.store, self.owner_id, policy)
        self.intake_workers = 3
        self._tick_lock = threading.BoundedSemaphore(self.intake_workers)
        self.delivery_workers = self.settings.get('delivery_workers', 4)
        if type(self.delivery_workers) is not int or not 1 <= self.delivery_workers <= 8:
            raise ContractError('delivery_workers must be an integer between 1 and 8')
        self.delivery_lease_seconds = 120.0
        self._background_lock = threading.Lock()
        self.trace = AssistantTrace(self.store, self.owner_id)
        self.improvement = ImprovementJournal(self.store, self.owner_id)
        with self.store.transaction() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS r7_query_route_hints(
                item_id TEXT PRIMARY KEY,source_version INTEGER NOT NULL,
                content_hash TEXT NOT NULL,policy_hash TEXT NOT NULL,project_ids TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r5_intake(
                item_id TEXT PRIMARY KEY,event_json TEXT NOT NULL,origin TEXT NOT NULL,
                work_task_id TEXT NOT NULL DEFAULT '',state TEXT NOT NULL,token TEXT NOT NULL DEFAULT '',
                lease_until REAL,source_hash TEXT NOT NULL,created REAL NOT NULL,last_error TEXT NOT NULL DEFAULT '')''')
            db.execute('''CREATE TABLE IF NOT EXISTS r5_links(
                item_id TEXT PRIMARY KEY,work_task_id TEXT NOT NULL,origin TEXT NOT NULL,
                conversation_key TEXT NOT NULL,result_hash TEXT NOT NULL DEFAULT '')''')
            if 'intake_hash' not in {r[1] for r in db.execute('PRAGMA table_info(r5_links)')}:
                db.execute("ALTER TABLE r5_links ADD COLUMN intake_hash TEXT NOT NULL DEFAULT ''")
            db.execute('''CREATE TABLE IF NOT EXISTS r5_followup_targets(
                followup_id TEXT PRIMARY KEY,target_json TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r5_local_replies(
                action_id TEXT PRIMARY KEY,item_id TEXT NOT NULL,body TEXT NOT NULL,observed INTEGER NOT NULL DEFAULT 0)''')
            if 'event_json' not in {r[1] for r in db.execute('PRAGMA table_info(r5_local_replies)')}:
                db.execute("ALTER TABLE r5_local_replies ADD COLUMN event_json TEXT NOT NULL DEFAULT ''")
            db.execute('''CREATE TABLE IF NOT EXISTS r5_observations(
                effect_id TEXT PRIMARY KEY,observed INTEGER NOT NULL DEFAULT 0)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r5_risks(
                risk_id TEXT PRIMARY KEY,payload_json TEXT NOT NULL,queued INTEGER NOT NULL DEFAULT 0)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r6_reads(
                item_id TEXT PRIMARY KEY,source_hash TEXT NOT NULL,started REAL NOT NULL,
                retry_at REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r6_reply_sources(
                action_id TEXT PRIMARY KEY,result_json TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r6_reply_schedule(
                action_id TEXT PRIMARY KEY,checks INTEGER NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r6_intake_scopes(
                item_id TEXT PRIMARY KEY,scope TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r7_delivery_scopes(
                owner_id TEXT NOT NULL,action_id TEXT PRIMARY KEY,scope TEXT NOT NULL)''')
            db.execute('''CREATE INDEX IF NOT EXISTS r7_delivery_scope
                ON r7_delivery_scopes(owner_id,scope,action_id)''')
            db.execute('''CREATE TABLE IF NOT EXISTS r7_delivery_claims(
                owner_id TEXT NOT NULL,scope TEXT NOT NULL,action_id TEXT NOT NULL,
                token TEXT NOT NULL,lease_until REAL NOT NULL,PRIMARY KEY(owner_id,scope))''')
            for pending in db.execute('''SELECT item_id,event_json FROM r5_intake
                WHERE item_id NOT IN (SELECT item_id FROM r6_intake_scopes)''').fetchall():
                prior = Event.from_dict(json.loads(pending['event_json']))
                db.execute('INSERT INTO r6_intake_scopes VALUES(?,?)',
                           (prior.item_id, key(prior.tenant_id, prior.chat_id, prior.thread_id, prior.sender_id)))
            for pending in db.execute('''SELECT l.action_id,COALESCE(NULLIF(l.event_json,''),i.event_json) AS event_json
                FROM r5_local_replies l JOIN r5_intake i USING(item_id)
                WHERE l.action_id NOT IN (SELECT action_id FROM r7_delivery_scopes)''').fetchall():
                prior = Event.from_dict(json.loads(pending['event_json']))
                db.execute('INSERT INTO r7_delivery_scopes VALUES(?,?,?)',
                    (self.owner_id, pending['action_id'], self._delivery_scope(prior)))
        for origin in ORIGINS:
            self.book.bind_identity(origin, self.owner_id, self.owner_id,
                "host-owner-binding:" + self.owner_id, authorized_by=self.owner_id)

    def _owner(self):
        return self.directory.resolve(self.owner_id)

    def _require_owner(self, actor):
        if actor != self.owner_id:
            raise ContractError("authenticated owner required")

    @staticmethod
    def source_ref(event):
        return ("entry:" + event.observed_via + ":" + event.native_id
                if event.observed_via in {"gpt_dot", "grok_bot"}
                else "feishu:message:" + event.native_id)

    def capture(self, event, *, origin="feishu", work_task_id="", legacy_owned=False):
        """Called only after the existing host verified the event envelope."""
        event.validate()
        if origin not in ORIGINS or event.tenant_id != self.cfg.tenant_id:
            raise ContractError("assistant ingress identity mismatch")
        if event.is_deleted:
            return self.revoke(event)
        if (legacy_owned or event.is_self or event.direction != "inbound"
                or event.kind != "message" or event.channel == "personal_dm"
                or not (event.channel == "bot_dm" or self.cfg.bot_id in event.mentions)):
            return None
        if event.original_time < self.authority.collection_config.live_cutover_at:
            return None
        encoded, fingerprint = dumps(event.to_dict()), key(event.to_dict(), origin, work_task_id)
        with self.store.transaction() as db:
            legacy = db.execute("SELECT legacy_owned FROM gw_items WHERE item_id=?", (event.item_id,)).fetchone()
            if legacy and legacy["legacy_owned"]:
                return None
            old = db.execute("SELECT * FROM r5_intake WHERE item_id=?", (event.item_id,)).fetchone()
            if old and old["source_hash"] == fingerprint:
                return {"captured": True, "duplicate": True, "item_id": event.item_id}
            if old:
                previous = Event.from_dict(json.loads(old["event_json"]))
                if previous.is_deleted:
                    return {"captured": True, "duplicate": True, "item_id": event.item_id}
                if event.version_time <= previous.version_time:
                    return {"captured": True, "duplicate": True, "item_id": event.item_id}
                db.execute("UPDATE r5_intake SET event_json=?,source_hash=?,state='pending',token='',lease_until=NULL WHERE item_id=?",
                           (encoded, fingerprint, event.item_id))
            else:
                db.execute("INSERT INTO r5_intake VALUES(?,?,?,?,'pending','',NULL,?,?,'')",
                    (event.item_id, encoded, origin, work_task_id, fingerprint, self.store.clock()))
            db.execute('INSERT OR REPLACE INTO r6_intake_scopes VALUES(?,?)',
                       (event.item_id, key(event.tenant_id, event.chat_id, event.thread_id, event.sender_id)))
        self.trace.mark(event.item_id, 'ingress_received', details={'channel': event.channel,
            'request_version': key(event.version_time, event.content_hash)})
        self.trace.mark(event.item_id, 'platform_input_created', at=event.original_time,
                        clock_domain='feishu_platform' if origin == 'feishu' else 'entry_source')
        return {"captured": True, "duplicate": False, "item_id": event.item_id}

    def revoke(self, event):
        """Retire queued work even when recall arrives before Harness admission."""
        if not event.is_deleted or event.tenant_id != self.cfg.tenant_id:
            raise ContractError("verified source recall required")
        with self.store.transaction() as db:
            row = db.execute("SELECT * FROM r5_intake WHERE item_id=?", (event.item_id,)).fetchone()
            if not row:
                return None
            previous = Event.from_dict(json.loads(row["event_json"]))
            if (previous.chat_id, previous.sender_id) != (event.chat_id, event.sender_id):
                raise ContractError("recall source identity mismatch")
            duplicate = previous.is_deleted
            db.execute("UPDATE r5_intake SET event_json=?,source_hash=?,state='recalled',token='',lease_until=NULL WHERE item_id=?",
                (dumps(event.to_dict()), key(event.to_dict(), row["origin"], row["work_task_id"]), event.item_id))
        linked = self.store.one("SELECT work_task_id FROM r5_links WHERE item_id=?", (event.item_id,))
        if linked:
            self.book.append_event(linked["work_task_id"], "recalled:" + event.item_id,
                principal_id=self.owner_id, kind="source_recalled", text="原请求已撤回，依赖该来源的未执行动作已停止。",
                source_ref=self.source_ref(event))
            self.reports.enqueue_notice("recalled:" + event.item_id, title="一项工作请求已撤回",
                body="相关未执行动作已停止；原工作记录和已发生的事实保留，后续安排由当前有效指令决定。",
                source_refs=[self.source_ref(event)], task_ids=[linked["work_task_id"]], now=self.store.clock())
        return {"captured": True, "duplicate": duplicate, "item_id": event.item_id}

    def _intake_current(self, item):
        row = self.store.one("SELECT state,source_hash,token FROM r5_intake WHERE item_id=?", (item["item_id"],))
        return bool(row and row["state"] == "processing" and row["source_hash"] == item["source_hash"]
                    and row["token"] == item["token"])

    def _claim_intake(self):
        now, token = self.store.clock(), uuid.uuid4().hex
        with self.store.transaction() as db:
            db.execute("UPDATE r5_intake SET state='pending',token='' WHERE state='processing' AND lease_until<?", (now,))
            row = db.execute("""SELECT i.* FROM r5_intake i LEFT JOIN r6_reads r USING(item_id)
                JOIN r6_intake_scopes s USING(item_id)
                WHERE (i.state='pending' OR (i.state='source_wait' AND r.retry_at<=?))
                  AND NOT EXISTS(SELECT 1 FROM r5_intake active JOIN r6_intake_scopes other USING(item_id)
                    WHERE active.state='processing' AND other.scope=s.scope AND active.item_id!=i.item_id)
                ORDER BY i.created,i.item_id LIMIT 1""", (now,)).fetchone()
            if not row:
                return None
            db.execute("UPDATE r5_intake SET state='processing',token=?,lease_until=? WHERE item_id=?",
                       (token, now + 120, row["item_id"]))
            return {**dict(row), "token": token}

    def _finish_intake(self, item, state, error=""):
        with self.store.transaction() as db:
            db.execute("UPDATE r5_intake SET state=?,last_error=?,token='',lease_until=NULL WHERE item_id=? AND token=? AND source_hash=?",
                       (state, error, item["item_id"], item["token"], item["source_hash"]))

    def _bind_actor(self, actor, event):
        if not actor.verified:
            raise ContractError("unverified assistant actor")
        if actor.user_id != self.owner_id and not set(actor.department_ids) & self.policy.config.demo_department_ids:
            raise ContractError("assistant service is limited to DEMO")
        self.book.bind_identity("feishu", actor.user_id, actor.user_id,
            "verified-directory:" + actor.user_id, authorized_by=self.owner_id)
        projects = self._current_projects(actor)
        if event.channel == "bot_dm" and event.observed_via not in {"gpt_dot", "grok_bot"}:
            self.directory.register_dm(event)
            if self.on_dm_bound:
                self.on_dm_bound(event)
        return projects

    def _current_projects(self, actor):
        projects = tuple(self.directory.project_ids_for(actor))
        self.book.sync_project_grants(actor.user_id, projects,
                                     authorized_by=self.owner_id, can_write=True)
        return projects

    def _group_projects(self, chat_id):
        """Use host chat bindings, never project names supplied in the message."""
        if chat_id not in self.authority.group_chats:
            return ()
        resolver = getattr(self.authority, "projects_for_chat", None)
        if callable(resolver):
            return tuple(resolver(chat_id))
        configured = self.settings.get("project_chats", {})
        return tuple(project for project, chats in configured.items() if chat_id in chats)

    def _record_human(self, event, actor, task_id="", *, related=True):
        if event.observed_via in {"gpt_dot", "grok_bot"}:
            return
        event_id = "human:" + event.item_id + ":" + str(event.version_time)
        if self.store.one("SELECT 1 FROM ar5_turns WHERE owner_id=? AND event_id=?", (self.owner_id, event_id)):
            return  # Owner decisions add new events; they never rewrite this turn.
        self.reports.record_turn(event_id, event.chat_id,
            timestamp=event.original_time, actor_id=actor.user_id, actor_name=actor.display_name,
            text=event.body, thread_id=event.thread_id, participant_ids=[actor.user_id],
            task_id=task_id, work_scope="DEMO", source_ref=self.source_ref(event),
            details={"pending": ["工作请求已记录，处理结果待核对"] if task_id else [],
                     "needs_owner": [] if related else ["范围待本人判断"]})

    def _local_answer(self, event, actor, *, current_projects=None):
        if _GREETING.fullmatch(event.body.strip()):
            return "你好，我是示例负责人的项目助理 AM。你可以告诉我需要查询或推进的 DEMO 工作。" if "好" in event.body else "收到，有新的进展可以继续告诉我。"
        if _CLOSE.fullmatch(event.body.strip()):
            return "好的，这段沟通我会向示例负责人汇报；未完成的事项会继续保留跟进。"
        if _TODO.fullmatch(event.body.strip()):
            projects = self._current_projects(actor) if current_projects is None else current_projects
            audience = (tuple(p for p in self._group_projects(event.chat_id) if p in projects)
                        if event.channel == "group" else None)
            tasks = self.book.tasks_for_principal(actor.user_id, project_ids=audience,
                project_only=event.channel == "group")
            if not tasks:
                return ("本项目群当前没有可展示的未完成待办。" if event.channel == "group"
                        else "当前工作记录中没有你的未完成待办。")
            heading = "本项目群可查看的待办：\n" if event.channel == "group" else "当前待办：\n"
            return heading + "\n".join(
                "• " + t["title"] + "（" + _STATE_LABELS.get(t["state"], "待核对") + "）\n  任务编号：" + t["task_id"] for t in tasks[:20])
        return None

    def _feedback(self, event):
        if not (any(pattern.search(event.body) for _, pattern in _FEEDBACK_RULES)
                or re.search(r'回答.{0,5}(?:错|不对|不准确)|这次(?:不错|可以)', event.body)):
            return
        try:
            self.improvement.record_feedback('message:' + event.item_id + ':' + str(event.version_time),
                actor_id=event.sender_id, text=event.body, source_ref=self.source_ref(event))
        except Exception as exc:
            with self.store.transaction() as db:
                self.store.audit(db, 'r6_feedback_journal_error', event.item_id, type(exc).__name__)

    def _observe(self, event, body, *, stage, source_version='', status='', evidence_ref='', latency_seconds=None):
        # Diagnostics must never turn a successful business response into a
        # resend or erase a previously recorded failure.
        try:
            self.improvement.record_observation(
                event.item_id + ':' + str(event.version_time) + ':' + stage + ':' + key(event.content_hash, body, source_version),
                request=event.body, response=body, channel=event.channel, actor_id=event.sender_id,
                source_version=source_version, status=status or stage,
                evidence_ref=evidence_ref, latency_seconds=latency_seconds)
        except Exception as exc:
            with self.store.transaction() as db:
                self.store.audit(db, 'r6_observation_journal_error', event.item_id, type(exc).__name__)

    def _read_project(self, event, actor, projects, item):
        if self.project_reader is None:
            return None
        from integration.assistant_queries import FastReadPending
        try:
            with self.trace.span(event.item_id, 'ledger_retrieval'):
                return self.project_reader(event, actor, projects, focus_project_id=self._reply_focus(event))
        except FastReadPending:
            now = self.store.clock()
            wait_budget = max(1, min(45, float(self.settings.get('fast_read_wait_seconds', 12))))
            with self.store.transaction() as db:
                previous = db.execute('SELECT * FROM r6_reads WHERE item_id=?', (event.item_id,)).fetchone()
                started = previous['started'] if previous and previous['source_hash'] == item['source_hash'] else now
                db.execute('''INSERT INTO r6_reads VALUES(?,?,?,?,1)
                    ON CONFLICT(item_id) DO UPDATE SET source_hash=excluded.source_hash,
                    started=excluded.started,retry_at=excluded.retry_at,attempts=r6_reads.attempts+1''',
                    (event.item_id, item['source_hash'], started, now + .25))
            if now - started >= wait_budget:
                return {'status': 'unavailable', 'project_ids': [], 'source_version': '',
                    'source_ref': '', 'requires_refresh': False,
                    'body': '当前台账同步还未完成，暂时无法可靠核对这项进展。原请求已保留，稍后可继续查询。'}
            self._finish_intake(item, 'source_wait', 'ledger_refresh_in_progress')
            return {'status': 'deferred'}

    def _store_project_reply(self, event, actor, result):
        # One stable action per input version, even after a crash between
        # preparing the action and marking the intake ready.
        action_id = 'assistant_query_' + key(event.item_id, event.version_time, event.content_hash)[:32]
        action = self._prepare_local_reply(event, actor, result['body'],
            extra_stamp=self._project_stamp(result), action_id=action_id, source_result=result)
        self._observe(event, result['body'], stage='generated', source_version=result.get('source_version', ''),
                      status=result.get('status', 'generated'))
        return action

    @staticmethod
    def _project_stamp(result):
        # Refreshing identical facts does not create a new approval. The latest
        # source version is retained separately in r6_reply_sources.
        return {'ledger_source': result.get('source_ref', ''),
                'ledger_result': key(result.get('status'), result.get('body'), result.get('project_ids', []))}

    def _reply_focus(self, event):
        query = '''SELECT l.event_json,s.result_json FROM r5_local_replies l
            JOIN r6_reply_sources s USING(action_id)
            JOIN a5_actions a USING(action_id)
            WHERE a.owner_id=? AND a.state='applied' '''
        if event.parent_id:
            rows = self.store.rows(query + 'AND a.evidence_ref=?',
                                  (self.owner_id, 'feishu:message:' + event.parent_id))
        else:
            # Natural short follow-ups use only an actually delivered answer in
            # this user's current conversation. A private answer never supplies
            # another group's focus, and current grants are checked again later.
            intake = self.store.one('SELECT rowid AS order_id,created FROM r5_intake WHERE item_id=?', (event.item_id,))
            received = intake['created'] if intake else self.store.clock()
            if not intake:
                return None
            adjacent = self.store.one('''SELECT i.item_id,i.event_json FROM r5_intake i
                JOIN r6_intake_scopes s USING(item_id)
                WHERE s.scope=? AND i.origin='feishu'
                  AND (i.created<? OR (i.created=? AND i.rowid<?))
                ORDER BY i.created DESC,i.rowid DESC LIMIT 1''',
                (key(event.tenant_id, event.chat_id, event.thread_id, event.sender_id),
                 received, received, intake['order_id']))
            if not adjacent:
                return None
            prior_input = Event.from_dict(json.loads(adjacent['event_json']))
            if prior_input.is_deleted:
                return None
            # Do not jump over a later report request or another topic to reuse
            # an older project. Explicit reply-to remains available separately.
            rows = self.store.rows(query + '''AND l.observed=1 AND a.updated_at>=? AND a.updated_at<=?
                AND l.item_id=? ORDER BY a.updated_at DESC,l.rowid DESC''',
                (self.owner_id, received - 86400, received, adjacent['item_id']))
        for row in rows:
            previous = Event.from_dict(json.loads(row['event_json']))
            result = json.loads(row['result_json'])
            if not event.parent_id and (previous.content_hash, previous.version_time) != (
                    prior_input.content_hash, prior_input.version_time):
                continue
            if ((previous.chat_id, previous.thread_id, previous.sender_id, previous.channel)
                    == (event.chat_id, event.thread_id, event.sender_id, event.channel)):
                return (result['project_ids'][0] if result.get('status') == 'found'
                        and len(result.get('project_ids', [])) == 1 else None)
        return None

    def intake_once(self):
        item = self._claim_intake()
        if item is None:
            return None
        event = Event.from_dict(json.loads(item["event_json"]))
        try:
            self.trace.mark(event.item_id, 'intake_claimed')
            with self.trace.span(event.item_id, 'identity_and_scope'):
                actor = self.directory.resolve(event.sender_id)
                projects = self._bind_actor(actor, event)
            self._feedback(event)
            project_ids = tuple(p for p in event.project_ids if p in projects)
            if not project_ids:
                names = self.settings.get("project_names", {})
                project_ids = tuple(p for p in projects if p in event.body or
                    any(name in event.body for name in names.get(p, ()) if name))
            if event.channel == "group":
                group_projects = self._group_projects(event.chat_id)
                project_ids = tuple(p for p in project_ids if p in group_projects)
            event = replace(event, project_ids=project_ids)
            semantic_project = False
            local = self._local_answer(event, actor, current_projects=projects)
            if local is not None and item["origin"] == "feishu":
                if not self._intake_current(item):
                    return {'state': 'superseded'}
                self._record_human(event, actor)
                self._prepare_local_reply(event, actor, local)
                self._finish_intake(item, "local_ready")
                return {"state": "local_ready", "item_id": event.item_id}
            if item['origin'] == 'feishu':
                project_answer = self._read_project(event, actor, projects, item)
                if project_answer is not None:
                    if project_answer['status'] == 'deferred':
                        return {'state': 'source_wait', 'item_id': event.item_id}
                    if not self._intake_current(item):
                        return {'state': 'superseded'}
                    event = replace(event, project_ids=tuple(project_answer.get('project_ids', ())))
                    if self.project_answer_mode == 'semantic' and project_answer['status'] == 'found':
                        # A found lookup establishes the trusted subject only.
                        # The complete original request goes to one semantic
                        # model round; no extractive/lay-out reply is published.
                        project_ids = event.project_ids
                        semantic_project = True
                        self.trace.mark(event.item_id, 'ledger_subject_bound_for_semantic')
                    else:
                        self._record_human(event, actor)
                        self._store_project_reply(event, actor, project_answer)
                        self._finish_intake(item, 'local_ready')
                        return {'state': 'local_ready', 'item_id': event.item_id,
                                'route': 'ledger_direct', 'retrieval_status': project_answer['status']}
            related = actor.user_id == self.owner_id or bool(project_ids)
            if not related and self.project_answer_mode == 'semantic' and projects:
                from ..phase1.routing import rules
                query_route = rules(event.body, passive=False)
                audience = (set(projects) & set(self._group_projects(event.chat_id))
                            if event.channel == 'group' else set(projects))
                # A read-only project question can need semantic subject
                # understanding before any project ID is extracted. It remains
                # bounded by the current directory/group policy, not its text.
                related = bool(audience and query_route and query_route['intent'] == 'project.query')
            source = self.source_ref(event)
            prior_link = self.store.one("SELECT work_task_id,intake_hash FROM r5_links WHERE item_id=?", (event.item_id,))
            continuation = item["work_task_id"] or (prior_link["work_task_id"] if prior_link else None)
            if not continuation and actor.user_id == self.owner_id:
                reference = re.search(r"(?:继续(?:任务)?|查询任务|查看任务|跟进任务|任务)[：:\s]*(wt_[A-Za-z0-9_-]+)", event.body)
                if reference:
                    # Explicit task references can cross all three entry points;
                    # matching titles or an ambiguous 'the previous one' cannot.
                    self.book.get_task(reference.group(1), principal_id=self.owner_id)
                    continuation = reference.group(1)
            # A colleague can answer an exact delivered follow-up without being
            # granted access to the owner's private task or its model context.
            feedback = None
            if event.parent_id and actor.user_id != self.owner_id:
                feedback = self.store.one("SELECT f.task_id,f.followup_id FROM a5_wb_followup_proposals p JOIN a5_wb_followups f USING(followup_id) WHERE p.delivery_ref=? AND f.recipient_id=?",
                    ("feishu:message:" + event.parent_id, actor.user_id))
            if feedback:
                if not self._intake_current(item):
                    return {"state": "superseded"}
                self.book.record_reply(feedback["task_id"], "feedback:" + event.item_id + ":" + str(event.version_time),
                    "feishu", actor.user_id, event.body, source_ref=source,
                    claims_complete=_completion_feedback(event.body), followup_id=feedback["followup_id"],
                    at=event.original_time)
                with self.store.transaction() as db:
                    db.execute("INSERT OR IGNORE INTO r5_links(item_id,work_task_id,origin,conversation_key,intake_hash) VALUES(?,?,?,?,?)",
                        (event.item_id, feedback["task_id"], item["origin"], event.chat_id, item["source_hash"]))
                self._record_human(event, actor, feedback["task_id"])
                self._message_risk(event, actor, feedback["task_id"])
                self._prepare_local_reply(event, actor, "收到，这条反馈已记入对应事项，我会向示例负责人汇报。")
                self._finish_intake(item, "feedback_recorded")
                return {"state": "feedback_recorded", "task_id": feedback["task_id"]}
            if not continuation and event.parent_id:
                prior_reply = self.store.one("SELECT l.work_task_id FROM h1_effects e JOIN h1_tasks t USING(task_id) JOIN r5_links l USING(item_id) WHERE e.platform_id=?", (event.parent_id,))
                if prior_reply:
                    continuation = prior_reply["work_task_id"]
                else:
                    prior_followup = self.store.one("SELECT f.task_id FROM a5_wb_followup_proposals p JOIN a5_wb_followups f USING(followup_id) WHERE p.delivery_ref=? AND f.recipient_id=?", ("feishu:message:" + event.parent_id, actor.user_id))
                    if prior_followup:
                        continuation = prior_followup["task_id"]
            continuation_exists = bool(continuation and self.store.one("SELECT 1 FROM a5_wb_tasks WHERE task_id=?", (continuation,)))
            if continuation_exists:
                existing = self.book.get_task(continuation, principal_id=actor.user_id)
                if (event.channel == "group" and
                        (existing["visibility"] != "project" or not existing["project_id"]
                         or existing["project_id"] not in self._group_projects(event.chat_id)
                         or existing["project_id"] not in projects)):
                    raise ContractError("task is outside the current group audience")
                related = bool(existing["related"])
                if existing["project_id"] and existing["project_id"] in projects:
                    project_ids = (existing["project_id"],)
                    event = replace(event, project_ids=project_ids)
            if prior_link and prior_link["intake_hash"] == item["source_hash"]:
                work = self.book.get_task(prior_link["work_task_id"], principal_id=actor.user_id)
            else:
                work = self.book.accept_task(
                    event.item_id + ":" + key(event.body, event.revision, event.version_time)[:20],
                    item["origin"], actor.user_id, event.body or "待补充的工作请求",
                    project_ids[0] if len(project_ids) == 1 else None, related=related,
                    task_id=continuation, source_ref=source,
                    visibility="private" if event.channel == "bot_dm" else "project")
            related = bool(work["related"])
            if continuation_exists and not prior_link and actor.user_id != self.owner_id:
                self.book.record_reply(work["task_id"], "feedback:" + event.item_id,
                    "feishu", actor.user_id, event.body, source_ref=source,
                    claims_complete=_completion_feedback(event.body),
                    at=event.original_time)
            with self.store.transaction() as db:
                db.execute("INSERT INTO r5_links(item_id,work_task_id,origin,conversation_key,intake_hash) VALUES(?,?,?,?,?) ON CONFLICT(item_id) DO UPDATE SET work_task_id=excluded.work_task_id,intake_hash=excluded.intake_hash",
                    (event.item_id, work["task_id"], item["origin"], event.chat_id, item["source_hash"]))
            self._record_human(event, actor, work["task_id"], related=related)
            self._message_risk(event, actor, work["task_id"])
            if not related:
                self.reports.enqueue_notice("scope:" + event.item_id + ":" + item["source_hash"],
                    title="有一项交办需要你判断", body=actor.display_name + "：" + event.body +
                    "\n尚未执行，需确认是否属于要承接的工作。", source_refs=[source],
                    task_ids=[work["task_id"]], now=item["created"], metadata={"kind": "referral",
                        "task_id": work["task_id"], "scope_digest": self._scope_digest(work["task_id"])})
                self._finish_intake(item, "referred")
                return {"state": "referred", "task_id": work["task_id"]}
            policy = self.authority(event)
            if policy is None:
                raise ContractError("current source authority unavailable")
            if not self._intake_current(item):
                return {"state": "superseded"}
            with self.store.transaction() as db:
                self.harness.ingest(event, policy, _db=db)
                if semantic_project:
                    # Host-only hint: bound to the exact accepted source and
                    # policy. Incoming JSON/text has no route-hint field.
                    source_row = db.execute('SELECT version FROM gw_items WHERE item_id=?',
                                            (event.item_id,)).fetchone()
                    accepted = db.execute('SELECT policy_json FROM h1_inbox WHERE item_id=?',
                                          (event.item_id,)).fetchone()
                    db.execute('INSERT OR REPLACE INTO r7_query_route_hints VALUES(?,?,?,?,?)',
                        (event.item_id, source_row['version'], event.content_hash,
                         key(accepted['policy_json']), dumps(list(event.project_ids))))
            if work["state"] not in {"completed", "cancelled", "reported_complete"} and not continuation_exists:
                self.book.transition(work["task_id"], "in_progress", principal_id=self.owner_id, source_ref=source)
            self._finish_intake(item, "admitted")
            return {"state": "admitted", "task_id": work["task_id"],
                    **({'route': 'ledger_semantic', 'retrieval_status': 'found'} if semantic_project else {})}
        except Exception as exc:
            if not self._intake_current(item):
                return {'state': 'superseded'}
            self._finish_intake(item, "needs_owner", type(exc).__name__)
            self.reports.enqueue_notice("intake:" + event.item_id + ":" + item["source_hash"],
                title="一项请求暂未承接", body="身份、项目范围或接入信息需要核对；该请求尚未执行。\n来源：" + self.source_ref(event),
                source_refs=[self.source_ref(event)], now=self.store.clock(), metadata={"kind": "referral"})
            return {"state": "needs_owner", "reason": type(exc).__name__}

    def _context(self, event, actor, body, action_id, *, kind="reply", task_id=None):
        sensitivity = set()
        outward = event.channel == "group" or actor.user_id != self.owner_id
        if outward:
            text = event.body + "\n" + body
            checks = {
                "approval": r"审批|正式批准|同意延期|批准延期|批准变更",
                "formal_commitment": r"承诺交付|保证交付|保证完成|我代表.*(?:同意|承诺)",
                "milestone_change": r"(?:修改|调整|推迟|提前|变更).{0,12}(?:里程碑|正式计划|交付日期|验收日期)",
                "responsibility_change": r"(?:更换|变更|改为).{0,12}(?:负责人|责任人)|(?:负责人|责任人).{0,12}(?:改为|换成)",
                "acceptance": r"正式验收|验收通过|同意结项|批准结项",
            }
            sensitivity.update(tag for tag, pattern in checks.items() if re.search(pattern, text))
        # Native @ markup is an additional recipient, even when it is absent
        # from the original message's trusted mentions. Never let model text
        # silently add a notification target to an ordinary passive reply.
        mention_ids = set(person for person in event.mentions if person != self.cfg.bot_id)
        rendered_mentions = set(re.findall(r'<at\s+[^>]*(?:id|user_id)=["\']([^"\']+)', body))
        if rendered_mentions:
            sensitivity.add("uncertain_risk")
            mention_ids.update(rendered_mentions)
        return ActionContext(action_id, kind, actor, recipients=(actor,),
            project_scope="demo" if event.project_ids or actor.user_id != self.owner_id else "unrelated",
            source_evidence=(SourceEvidence(self.source_ref(event), verified=True,
                project_id=event.project_ids[0] if len(event.project_ids) == 1 else None),),
            content_text=body, passive_reply=kind == "reply",
            mentioned_names=(event.body,) if event.body.strip() else (),
            mentioned_person_ids=tuple(sorted(mention_ids)), sensitivity=frozenset(sensitivity),
            external_visibility=outward,
            subject_id=task_id, project_id=event.project_ids[0] if len(event.project_ids) == 1 else None)

    @staticmethod
    def _target(event):
        return {"chat_id": event.chat_id, "actor_id": event.sender_id,
                "channel": event.channel, "thread_id": event.thread_id,
                "reply_to_message_id": event.native_id}

    def _request_approval(self, action, *, task_id=""):
        if action["state"] not in {"awaiting_approval", "referred"}:
            return
        frozen = self.actions.snapshot(action["action_id"])
        payload = frozen["payload"]
        names = "、".join(r.get("display_name") or r.get("user_id", "待核对对象") for r in frozen["context"].get("recipients", ()))
        target = payload.get("target", {})
        target_label = ("机器人私聊" if target.get("channel") == "bot_dm" else
            self.settings.get("group_chat_names", {}).get(target.get("chat_id"), "已绑定项目群：" + target.get("chat_id", "")))
        labels = {"reply": "回复对方", "proactive_followup": "主动跟进", "send_message": "发送消息",
                  "update_progress": "更新项目进展", "update_todo": "整理项目待办"}
        artifacts = payload.get("artifacts", [])
        artifact_label = "、".join(str(a.get("name") or a.get("filename") or a.get("artifact_id", "待核对附件"))
            if isinstance(a, dict) else str(a) for a in artifacts) or "无"
        body = ("拟执行：" + labels.get(frozen["context"]["action_kind"], "处理待确认事项") +
                "\n对象：" + (names or "已绑定业务目标") + "\n位置：" + target_label +
                "\n内容：\n" + payload.get("body", dumps(payload)) +
                "\n附件：" + artifact_label +
                "\n需要你确认后才会执行；本次确认只适用于上面的对象与内容。")
        self.reports.enqueue_notice("approval:" + action["action_id"] + ":" + action["digest"],
            title="这件事需要你确认", body=body, now=self.store.clock(),
            task_ids=[task_id] if task_id else [],
            metadata={"kind": "approval", "action_id": action["action_id"],
                      "version": action["version"], "digest": action["digest"]})

    def _prepare_local_reply(self, event, actor, body, *, extra_stamp=None, action_id=None, source_result=None):
        action_id = action_id or "assistant_reply_" + key(event.item_id, event.version_time, event.revision, body)[:32]
        context = self._context(event, actor, body, action_id)
        payload = {"body": body, "target": self._target(event), "artifacts": []}
        source = self.store.one("SELECT source_hash,event_json FROM r5_intake WHERE item_id=?", (event.item_id,))
        current = Event.from_dict(json.loads(source['event_json']))
        if ((current.content_hash, current.version_time, current.sender_id, current.chat_id)
                != (event.content_hash, event.version_time, event.sender_id, event.chat_id)):
            raise ContractError('local reply input changed before preparation')
        stamp = {"item_id": event.item_id, "source_hash": source["source_hash"], **(extra_stamp or {})}
        action = self.actions.prepare(context, payload, stamp)
        with self.store.transaction() as db:
            latest = db.execute('SELECT source_hash FROM r5_intake WHERE item_id=?', (event.item_id,)).fetchone()
            if not latest or latest['source_hash'] != source['source_hash']:
                raise ContractError('local reply input changed during preparation')
            db.execute("""INSERT INTO r5_local_replies(action_id,item_id,body,observed,event_json) VALUES(?,?,?,0,?)
                       ON CONFLICT(action_id) DO UPDATE SET body=excluded.body,event_json=excluded.event_json""",
                       (action_id, event.item_id, body, dumps(event.to_dict())))
            if source_result is not None:
                db.execute('INSERT OR REPLACE INTO r6_reply_sources VALUES(?,?)', (action_id, dumps(source_result)))
            db.execute('''INSERT INTO r7_delivery_scopes VALUES(?,?,?)
                ON CONFLICT(action_id) DO UPDATE SET owner_id=excluded.owner_id,scope=excluded.scope''',
                (self.owner_id, action_id, self._delivery_scope(event)))
        self._request_approval(action)
        return action

    def prepare_reply(self, task, result):
        """Gate the existing text AND attachment delivery using one frozen action."""
        if result.get('interactive_source_binding') and not self.store.one(
                'SELECT 1 FROM h1_effects WHERE task_id=?', (task['task_id'],)):
            runner = getattr(self, 'interactive_runner', None)
            if runner is None or not runner.validate_delivery(task, result):
                return False
        event = Event.from_dict(json.loads(task["request_json"]))
        intake = self.store.one("SELECT event_json,state FROM r5_intake WHERE item_id=?", (event.item_id,))
        if intake:
            current = Event.from_dict(json.loads(intake["event_json"]))
            if (current.is_deleted or current.content_hash != event.content_hash or
                    current.version_time > event.version_time or intake["state"] != "admitted"):
                return False
        if event.observed_via in {"gpt_dot", "grok_bot"}:
            self._record_available_result(task, result)
            return False  # owner CLI reads the result from this same work record
        actor = self.directory.resolve(event.sender_id)
        projects = self._current_projects(actor)
        if (any(p not in projects for p in event.project_ids)
                or event.channel == "group" and
                any(p not in self._group_projects(event.chat_id) for p in event.project_ids)):
            return False
        body = result.get("reply_text", "")
        action_id = "assistant_delivery_" + key(task["task_id"], task["generation"], task["source_version"])[:32]
        context = self._context(event, actor, body, action_id, task_id=task["task_id"])
        stamp = {k: task[k] for k in ("task_id", "generation", "source_version", "revision", "policy_json")}
        payload = {"body": body, "target": self._target(event), "artifacts": result.get("artifacts", [])}
        action = self.actions.prepare(context, payload, stamp)
        self._request_approval(action, task_id=task["task_id"])
        verdict = self.actions.can_execute(action_id, action["digest"], stamp, context)
        return bool(verdict["allowed"])

    def _record_available_result(self, task, result):
        link = self.store.one("SELECT * FROM r5_links WHERE item_id=?", (task["item_id"],))
        if not link:
            return
        digest = key(result)
        if link["result_hash"] != digest:
            self.book.append_event(link["work_task_id"], "result:" + task["task_id"] + ":" + digest,
                principal_id=self.owner_id, kind="result_available", text=result.get("reply_text", ""),
                source_ref="harness:result:" + task["task_id"] + ":" + digest)
        work = self.book.get_task(link["work_task_id"], principal_id=self.owner_id)
        current = self.harness.task(task["task_id"])
        available_to_caller = (link["origin"] in {"gpt_dot", "grok_bot"} or
            current["delivery_state"] == "delivered" and current["verification_state"] == "sufficient")
        if available_to_caller and work["state"] not in {"reported_complete", "completed", "cancelled", "awaiting_owner"}:
            self.book.transition(link["work_task_id"], "waiting_feedback", principal_id=self.owner_id,
                                 source_ref="harness:result:" + task["task_id"])
        with self.store.transaction() as db:
            db.execute("UPDATE r5_links SET result_hash=? WHERE item_id=?", (digest, task["item_id"]))

    def record_delivery(self, task, result, evidence):
        self._record_available_result(task, result)
        action_id = "assistant_delivery_" + key(task["task_id"], task["generation"], task["source_version"])[:32]
        action = self.actions.get(action_id)
        self.actions.record_effect(action_id, action["version"], action["digest"],
            evidence_ref="feishu:message:" + evidence["platform_id"],
            readback_evidence_ref="feishu:readback:" + evidence["platform_id"])
        event = Event.from_dict(json.loads(task["request_json"]))
        self.trace.mark(event.item_id, 'delivery_readback_observed',
                        details={'request_version': key(event.version_time, event.content_hash)})
        self.trace.mark(event.item_id, 'platform_reply_created', at=evidence['platform_time'],
                        clock_domain='feishu_platform')
        self._observe(event, result.get('reply_text', ''), stage='model_delivered',
            source_version=str(task.get('source_version', '')), status='delivered',
            evidence_ref='feishu:message:' + evidence['platform_id'],
            latency_seconds=self.trace.local_elapsed(event.item_id))
        actor = self.directory.resolve(event.sender_id)
        link = self.store.one("SELECT work_task_id FROM r5_links WHERE item_id=?", (event.item_id,))
        self._record_human(event, actor, link["work_task_id"] if link else "")
        self.reports.record_turn("sent:" + evidence["platform_id"], event.chat_id,
            timestamp=evidence["platform_time"], actor_id=self.cfg.bot_id, actor_name="AM",
            speaker_role="assistant", text=result["reply_text"], thread_id=event.thread_id,
            task_id=link["work_task_id"] if link else "", work_scope="DEMO",
            reply_to_event_id="human:" + event.item_id + ":" + str(event.version_time),
            delivery_status="sent", delivery_ref="feishu:message:" + evidence["platform_id"],
            source_ref="feishu:message:" + evidence["platform_id"],
            details={"pending": result.get("remaining_requirements", [])})
        with self.store.transaction() as db:
            db.execute("INSERT OR REPLACE INTO r5_observations VALUES(?,1)", ("delivery:" + task["task_id"],))

    def observe_results_once(self):
        """Recover reporting after a crash without sending the answer again."""
        tasks = self.store.rows("""SELECT t.* FROM h1_tasks t JOIN r5_links l USING(item_id)
            WHERE t.result_json IS NOT NULL AND t.execution_state IN ('succeeded','failed')
              AND t.verification_state!='conflict' AND t.last_error!='source_changed' LIMIT 200""")
        for task in tasks:
            result = json.loads(task["result_json"])
            self._record_available_result(task, result)
            if task["delivery_state"] != "delivered" or task["verification_state"] != "sufficient":
                continue
            if self.store.one("SELECT 1 FROM r5_observations WHERE effect_id=? AND observed=1",
                              ("delivery:" + task["task_id"],)):
                continue
            effect = self.store.one("SELECT * FROM h1_effects WHERE task_id=?", (task["task_id"],))
            stamps = json.loads(task["stamps"])
            if effect and effect["platform_id"] and stamps.get("t_p"):
                self.record_delivery(task, result, {"platform_id": effect["platform_id"],
                    "platform_time": stamps["t_p"], "body": effect["body"],
                    "target": effect["target"], "sender_id": self.cfg.bot_id})

    @staticmethod
    def _delivery_scope(event):
        # Group members share the same delivery sequence within a thread.
        # This is distinct from model-context scope, which also includes actor.
        return key(event.tenant_id, event.chat_id, event.thread_id, event.channel)

    def _claim_local_delivery(self, attempted=()):
        """Atomically claim one conversation; never reclaim an external effect.

        The lease only covers queue preparation and observation. AssistantActions
        still owns the durable execution token. An unresolved/executing action
        cannot be retried when this queue lease expires. Approval waits and
        terminal-unknown results do not starve later independent replies.
        """
        if self.action_executor is None:
            return None
        now, token = self.store.clock(), uuid.uuid4().hex
        exclude = ''
        args = [self.owner_id, self.owner_id, now]
        if attempted:
            exclude = ' AND l.action_id NOT IN (' + ','.join('?' for _ in attempted) + ')'
            args.extend(attempted)
        with self.store.transaction() as db:
            row = db.execute("""SELECT l.*,d.scope AS delivery_scope FROM r5_local_replies l
                JOIN r5_intake i USING(item_id)
                JOIN r7_delivery_scopes d USING(action_id)
                JOIN a5_actions a ON a.owner_id=d.owner_id AND a.action_id=l.action_id
                LEFT JOIN r6_reply_schedule s ON s.action_id=l.action_id
                WHERE d.owner_id=? AND l.observed=0
                  AND i.state IN ('local_ready','feedback_recorded')
                  AND a.state NOT IN ('executing','unknown','applied_unverified')
                  AND NOT EXISTS(SELECT 1 FROM r7_delivery_claims c
                    WHERE c.owner_id=? AND c.scope=d.scope AND c.lease_until>?)
                  AND NOT EXISTS(SELECT 1 FROM r7_delivery_scopes d2
                    JOIN r5_local_replies l2 ON l2.action_id=d2.action_id
                    JOIN r5_intake i2 ON i2.item_id=l2.item_id
                    JOIN a5_actions a2 ON a2.owner_id=d2.owner_id AND a2.action_id=l2.action_id
                    WHERE d2.owner_id=d.owner_id AND d2.scope=d.scope AND l2.observed=0
                      AND i2.state IN ('local_ready','feedback_recorded')
                      AND (a2.state='executing' OR (l2.rowid<l.rowid
                        AND a2.state IN ('ready','approved','not_applied','applied'))))
                """ + exclude + " ORDER BY COALESCE(s.checks,0),l.rowid LIMIT 1", args).fetchone()
            if row is None:
                return None
            db.execute("""INSERT INTO r7_delivery_claims VALUES(?,?,?,?,?)
                ON CONFLICT(owner_id,scope) DO UPDATE SET action_id=excluded.action_id,
                token=excluded.token,lease_until=excluded.lease_until""",
                (self.owner_id, row['delivery_scope'], row['action_id'], token,
                 now + self.delivery_lease_seconds))
            result = dict(row)
            result['claim_token'] = token
            return result

    def _renew_delivery_claim(self, claim):
        with self.store.transaction() as db:
            # The token, rather than the old expiry alone, fences a slow worker.
            # It may finish after expiry if nobody has taken its queue lease.
            return db.execute('''UPDATE r7_delivery_claims SET lease_until=?
                WHERE owner_id=? AND scope=? AND action_id=? AND token=?''',
                (self.store.clock() + self.delivery_lease_seconds, self.owner_id,
                 claim['delivery_scope'], claim['action_id'], claim['claim_token'])).rowcount == 1

    def _release_delivery_claim(self, claim):
        with self.store.transaction() as db:
            db.execute('''DELETE FROM r7_delivery_claims
                WHERE owner_id=? AND scope=? AND action_id=? AND token=?''',
                (self.owner_id, claim['delivery_scope'], claim['action_id'], claim['claim_token']))

    def local_delivery_once(self):
        return self._local_delivery_once()

    def _local_delivery_once(self):
        count, attempted = 0, []
        for _ in range(20):
            claim = self._claim_local_delivery(attempted)
            if claim is None:
                break
            attempted.append(claim['action_id'])
            try:
                self.trace.mark(claim['item_id'], 'delivery_queue_claimed',
                    details={'action_id': claim['action_id'], 'phone_visibility_measured': False})
                count += self._deliver_local_row(claim, claim)
            finally:
                self._release_delivery_claim(claim)
        return count

    def _deliver_local_row(self, row, claim):
        with self.store.transaction() as db:
            db.execute('''INSERT INTO r6_reply_schedule VALUES(?,1)
                ON CONFLICT(action_id) DO UPDATE SET checks=r6_reply_schedule.checks+1''', (row['action_id'],))
        item = self.store.one("SELECT event_json,source_hash,state FROM r5_intake WHERE item_id=?", (row["item_id"],))
        current = Event.from_dict(json.loads(item["event_json"]))
        if current.is_deleted or item["state"] not in {"local_ready", "feedback_recorded"}:
            return 0
        event = Event.from_dict(json.loads(row["event_json"] or item["event_json"]))
        if ((current.content_hash, current.version_time, current.sender_id, current.chat_id)
                != (event.content_hash, event.version_time, event.sender_id, event.chat_id)):
            return 0
        actor = self.directory.resolve(event.sender_id)
        snap = self.actions.snapshot(row["action_id"])
        stamp = {"item_id": event.item_id, "source_hash": item["source_hash"]}
        if any(snap['source_stamp'].get(k) != v for k, v in stamp.items()):
            return 0  # Audience refresh cannot adopt an edited/withdrawn source.
        stored_read = self.store.one('SELECT result_json FROM r6_reply_sources WHERE action_id=?',
                                    (row['action_id'],))
        if row['action_id'].startswith('assistant_query_') and stored_read is None:
            return 0  # A missing source binding can never turn into a generic reply.
        project_result = json.loads(stored_read['result_json']) if stored_read else None
        if project_result is not None:
            stamp.update(self._project_stamp(project_result))
        is_todo = bool(_TODO.fullmatch(event.body.strip()))
        if snap["state"] != "applied":
            projects = self._current_projects(actor)
            if project_result is not None:
                # Timeout notices disclose no project facts. Other answers
                # are re-derived from the current source and audience before
                # preparing the same durable action for transmission.
                try:
                    fresh_result = (project_result if project_result['status'] == 'unavailable'
                        else self.project_reader(event, actor, projects,
                            focus_project_id=self._reply_focus(event)))
                except Exception:
                    return 0
                if fresh_result is None or fresh_result.get('requires_refresh'):
                    return 0
                event = replace(event, project_ids=tuple(fresh_result.get('project_ids', ())))
                fresh_stamp = {**{k: stamp[k] for k in ('item_id', 'source_hash')},
                               **self._project_stamp(fresh_result)}
                if (fresh_stamp != snap['source_stamp'] or fresh_result['body'] != row['body']
                        or tuple(json.loads(row['event_json']).get('project_ids', ())) != event.project_ids):
                    if snap['state'] in {'executing', 'unknown', 'applied_unverified'}:
                        return 0
                    fresh_context = self._context(event, actor, fresh_result['body'], row['action_id'])
                    revised = self.actions.prepare(fresh_context,
                        {**snap['payload'], 'body': fresh_result['body']}, fresh_stamp)
                    with self.store.transaction() as db:
                        db.execute('UPDATE r5_local_replies SET body=?,event_json=? WHERE action_id=? AND observed=0',
                                   (fresh_result['body'], dumps(event.to_dict()), row['action_id']))
                        db.execute('UPDATE r6_reply_sources SET result_json=? WHERE action_id=?',
                                   (dumps(fresh_result), row['action_id']))
                    row['body'], row['event_json'] = fresh_result['body'], dumps(event.to_dict())
                    snap = self.actions.snapshot(row['action_id'])
                    self._request_approval(revised)
                stamp, project_result = fresh_stamp, fresh_result
                with self.store.transaction() as db:
                    db.execute('UPDATE r6_reply_sources SET result_json=? WHERE action_id=?',
                               (dumps(fresh_result), row['action_id']))
            elif is_todo:
                # Lists are generated from work records, not from the incoming
                # event's often-empty project_ids. Refresh the actual audience
                # before sending, including membership and visibility changes.
                fresh_body = self._local_answer(event, actor, current_projects=projects)
                if fresh_body != row["body"]:
                    if snap["state"] in {"executing", "unknown", "applied_unverified"}:
                        return 0  # Reconcile an unresolved send before revising it.
                    context = self._context(event, actor, fresh_body, row["action_id"])
                    revised = self.actions.prepare(context,
                        {**snap["payload"], "body": fresh_body}, stamp)
                    with self.store.transaction() as db:
                        db.execute("UPDATE r5_local_replies SET body=? WHERE action_id=? AND observed=0",
                                   (fresh_body, row["action_id"]))
                    row["body"] = fresh_body
                    snap = self.actions.snapshot(row["action_id"])
                    self._request_approval(revised)
            elif (any(p not in projects for p in event.project_ids)
                    or event.channel == "group" and
                    any(p not in self._group_projects(event.chat_id) for p in event.project_ids)):
                return 0
        context = self._context(event, actor, row["body"], row["action_id"])
        verdict = self.actions.can_execute(row["action_id"], snap["digest"], stamp, context)
        if verdict["allowed"]:
            def execute_current(frozen):
                fresh = self.store.one("SELECT source_hash,state FROM r5_intake WHERE item_id=?", (event.item_id,))
                if not fresh or fresh["source_hash"] != stamp["source_hash"] or fresh["state"] not in {"local_ready", "feedback_recorded"}:
                    return {"status": "not_applied", "evidence_ref": "assistant:source_changed:" + event.item_id}
                if is_todo:
                    fresh_actor = self.directory.resolve(event.sender_id)
                    if self._local_answer(event, fresh_actor) != frozen["payload"]["body"]:
                        return {"status": "not_applied",
                                "evidence_ref": "assistant:todo_scope_changed:" + event.item_id}
                if project_result is not None and project_result['status'] != 'unavailable':
                    try:
                        fresh_actor = self.directory.resolve(event.sender_id)
                        fresh_projects = self._current_projects(fresh_actor)
                        fresh_result = self.project_reader(event, fresh_actor, fresh_projects,
                            focus_project_id=self._reply_focus(event))
                        if (fresh_result is None or fresh_result.get('requires_refresh')
                                or self._project_stamp(fresh_result) != self._project_stamp(project_result)):
                            return {'status': 'not_applied',
                                    'evidence_ref': 'assistant:ledger_changed:' + event.item_id}
                    except Exception:
                        return {'status': 'not_applied',
                                'evidence_ref': 'assistant:ledger_unavailable:' + event.item_id}
                if not self._renew_delivery_claim(claim):
                    return {'status': 'not_applied',
                            'evidence_ref': 'assistant:delivery_claim_changed:' + event.item_id}
                self.trace.mark(event.item_id, 'action_executor_started',
                    details={'action_id': row['action_id'], 'phone_visibility_measured': False})
                with self.trace.span(event.item_id, 'send_and_readback'):
                    result = self.action_executor(frozen)
                self.trace.mark(event.item_id, 'action_executor_returned',
                    details={'action_id': row['action_id'], 'phone_visibility_measured': False})
                return result
            snap = self.actions.execute(row["action_id"], snap["digest"], stamp, context, execute_current)
        if not self._renew_delivery_claim(claim):
            return 0  # A current claimant will observe the same durable action.
        if snap["state"] != "applied":
            self._observe(event, row['body'], stage='local_attempt_' + str(snap.get('attempt_count', 0)),
                source_version=project_result.get('source_version', '') if project_result else '',
                status=snap['state'], evidence_ref=snap.get('evidence_ref', ''))
            return 0
        reference = snap.get("evidence_ref", "")
        if not reference:
            return 0
        self.trace.mark(event.item_id, 'delivery_readback_observed',
            details={'evidence_ref': reference, 'platform_visible_time_measured': False,
                     'request_version': key(event.version_time, event.content_hash)})
        self._observe(event, row['body'], stage='local_delivered',
            source_version=project_result.get('source_version', '') if project_result else '',
            status='delivered', evidence_ref=reference,
            latency_seconds=self.trace.local_elapsed(event.item_id))
        recorded = self.store.one("SELECT 1 FROM ar5_turns WHERE owner_id=? AND event_id=?",
                                  (self.owner_id, "local:" + row["action_id"]))
        if not recorded:
            observed = self.store.one("SELECT updated_at FROM a5_actions WHERE owner_id=? AND action_id=?",
                                      (self.owner_id, row["action_id"]))
            self.reports.record_turn("local:" + row["action_id"], event.chat_id,
                timestamp=observed["updated_at"], actor_id=self.cfg.bot_id,
                actor_name="AM", speaker_role="assistant", text=row["body"],
                reply_to_event_id="human:" + event.item_id + ":" + str(event.version_time),
                delivery_status="sent", delivery_ref=reference, source_ref=reference,
                thread_id=event.thread_id, work_scope="DEMO")
        if _CLOSE.fullmatch(event.body.strip()):
            self.reports.close_conversation(event.chat_id, closed_at=self.store.clock(),
                thread_id=event.thread_id, actor_id=event.sender_id)
        with self.store.transaction() as db:
            db.execute("UPDATE r5_local_replies SET observed=1 WHERE action_id=?", (row["action_id"],))
        return 1

    def reports_once(self):
        if self.report_sender is None:
            return 0
        report = self.reports.claim("assistant-runtime", now=self.store.clock())
        if report is None:
            return 0
        try:
            outcome = self.report_sender(report)
        except Exception as exc:
            self.reports.mark_unknown(report["report_id"], report["lease_token"], reason=type(exc).__name__)
            return 0
        if outcome.get("status") == "sent" and outcome.get("evidence_ref"):
            self.reports.ack(report["report_id"], report["lease_token"], delivery_ref=outcome["evidence_ref"])
            return 1
        self.reports.mark_unknown(report["report_id"], report["lease_token"],
                                  reason=outcome.get("reason", "delivery_unconfirmed"))
        return 0

    def followups_once(self):
        for proposal in self.book.due_followups(self.store.clock(), principal_id=self.owner_id):
            existing = self.store.one("SELECT state FROM a5_actions WHERE owner_id=? AND action_id=?",
                                      (self.owner_id, proposal["proposal_id"]))
            if existing and existing["state"] in {"unknown", "executing", "applied_unverified"}:
                continue  # only evidence reconciliation can resolve an uncertain effect
            if existing and existing["state"] == "applied":
                sent = self.actions.snapshot(proposal["proposal_id"])
                self.book.record_followup_delivery(proposal["proposal_id"], principal_id=self.owner_id,
                    approval_ref="approval:" + sent["digest"], delivery_ref=sent["evidence_ref"])
                continue
            source_digest = self._followup_source_digest(proposal["task_id"])
            if source_digest is None:
                continue
            target = self.store.one("SELECT target_json FROM r5_followup_targets WHERE followup_id=?",
                                    (proposal["followup_id"],))
            if not target:
                continue
            target = json.loads(target["target_json"])
            recipient = self.directory.resolve(proposal["recipient_id"])
            context = ActionContext(proposal["proposal_id"], "proactive_followup", self._owner(),
                recipients=(recipient,), project_scope="demo", passive_reply=False,
                content_text=proposal["prompt"], external_visibility=True,
                source_evidence=(SourceEvidence(proposal["source_ref"], verified=True),))
            stamp = {"followup_id": proposal["followup_id"], "task_id": proposal["task_id"],
                     "source_ref": proposal["source_ref"], "due_at": proposal["due_at"],
                     "work_source_digest": source_digest}
            action = self.actions.prepare(context, {"body": proposal["prompt"], "target": target, "artifacts": []}, stamp)
            self._request_approval(action, task_id=proposal["task_id"])
            done = action
            if self.action_executor and self.actions.can_execute(action["action_id"], action["digest"], stamp, context)["allowed"]:
                def execute_current(frozen):
                    active = self.store.one("""SELECT p.state AS proposal_state,f.state AS followup_state,t.state AS task_state
                        FROM a5_wb_followup_proposals p JOIN a5_wb_followups f USING(followup_id)
                        JOIN a5_wb_tasks t USING(task_id) WHERE p.proposal_id=?""", (proposal["proposal_id"],))
                    if (not active or active["proposal_state"] != "awaiting_owner" or active["followup_state"] != "proposed"
                            or active["task_state"] in {"completed", "cancelled", "awaiting_owner", "reported_complete"}
                            or self._followup_source_digest(proposal["task_id"]) != source_digest):
                        return {"status": "not_applied", "evidence_ref": "assistant:followup_inactive:" + proposal["proposal_id"]}
                    return self.action_executor(frozen)
                done = self.actions.execute(action["action_id"], action["digest"], stamp, context, execute_current)
            if done["state"] == "applied":
                active = self.store.one("SELECT state FROM a5_wb_followup_proposals WHERE proposal_id=?", (proposal["proposal_id"],))
                if active and active["state"] in {"awaiting_owner", "sent"}:
                    self.book.record_followup_delivery(proposal["proposal_id"], principal_id=self.owner_id,
                        approval_ref="approval:" + action["digest"], delivery_ref=done["evidence_ref"])

    def _scope_digest(self, task_id):
        rows = self.store.rows("SELECT i.item_id,i.source_hash,i.state FROM r5_intake i JOIN r5_links l USING(item_id) WHERE l.work_task_id=? ORDER BY i.item_id", (task_id,))
        # The digest freezes current sources, not the temporary processing lease.
        return key(task_id, [(r["item_id"], r["source_hash"], r["state"] == "recalled") for r in rows])

    def _followup_source_digest(self, task_id):
        task = self.book.get_task(task_id, principal_id=self.owner_id)
        if not task["related"] or task["state"] in {"completed", "cancelled", "awaiting_owner", "reported_complete"}:
            return None
        sources = self.store.rows("SELECT i.state,i.event_json FROM r5_intake i JOIN r5_links l USING(item_id) WHERE l.work_task_id=?", (task_id,))
        if any(row["state"] not in {"admitted", "feedback_recorded"} or json.loads(row["event_json"]).get("is_deleted")
               for row in sources):
            return None
        return self._scope_digest(task_id)

    def _risk(self, risk_id, *, text, source_ref, task_id="", priority="P1", needs_owner="请判断下一步处理。"):
        if not isinstance(text, str) or not text.strip() or not isinstance(source_ref, str) or not source_ref:
            raise ContractError("risk text and source reference required")
        if priority not in {"P0", "P1", "P2", "P3"}:
            raise ContractError("invalid risk priority")
        payload = {"timestamp": self.store.clock(), "text": text, "source_ref": source_ref,
                   "task_id": task_id, "priority": priority, "needs_owner": needs_owner}
        with self.store.transaction() as db:
            old = db.execute("SELECT payload_json FROM r5_risks WHERE risk_id=?", (risk_id,)).fetchone()
            if old:
                previous = json.loads(old["payload_json"])
                if any(previous[k] != v for k, v in payload.items() if k != "timestamp"):
                    raise ContractError("risk event_id reused with different content")
            db.execute("INSERT OR IGNORE INTO r5_risks VALUES(?,?,0)", (risk_id, dumps(payload)))

    def _message_risk(self, event, actor, task_id):
        text = event.body
        if (actor.user_id != self.owner_id and
                re.search(r"卡住|阻塞|无法按期|存在风险|有风险|延期|停线|事故|返工|预算超", text)
                and not re.search(r"无风险|没有风险|未发生|不需要延期|无需延期", text)):
            self._risk("reported:" + event.item_id + ":" + str(event.version_time),
                text=actor.display_name + "报告了需要关注的情况（尚未独立核验）：\n" + text,
                source_ref=self.source_ref(event), task_id=task_id,
                priority="P0" if re.search(r"紧急|停线|事故", text) else "P1")

    def risks_once(self):
        now = self.store.clock()
        for task in self.store.rows("""SELECT * FROM a5_wb_tasks WHERE related=1
            AND state NOT IN ('completed','cancelled','reported_complete','awaiting_owner')
            AND (state='blocked' OR (due_at IS NOT NULL AND due_at<=?)) LIMIT 100""", (now,)):
            reason = "当前记录为受阻" if task["state"] == "blocked" else "已到记录中的截止时间，尚无核验完成记录"
            self._risk("task:" + task["task_id"] + ":" + key(task["state"], task["due_at"]),
                text=task["title"] + "：" + reason + "。", source_ref=task["source_ref"],
                task_id=task["task_id"], priority="P0" if task["priority"] == 0 else "P1")
        for row in self.store.rows("SELECT * FROM r5_risks WHERE queued=0 LIMIT 100"):
            self.reports.enqueue_risk(row["risk_id"], **json.loads(row["payload_json"]))
            with self.store.transaction() as db:
                db.execute("UPDATE r5_risks SET queued=1 WHERE risk_id=?", (row["risk_id"],))

    def foreground_tick(self):
        """Prepare new requests without waiting for sending or maintenance."""
        if not self._tick_lock.acquire(blocking=False):
            return
        try:
            started = time.monotonic()
            for _ in range(8):
                if self.intake_once() is None:
                    break
                if time.monotonic() - started >= .1:
                    break
        finally:
            self._tick_lock.release()

    def background_tick(self):
        """Reports and routine maintenance cannot occupy the intake lane."""
        if not self._background_lock.acquire(blocking=False):
            return
        try:
            self.followups_once()
            self.observe_results_once()
            self.risks_once()
            if getattr(self, "maintenance", None) is not None:
                self.maintenance.step()
            now = self.store.clock()
            self.reports.sweep_idle(now=now)
            self.reports.enqueue_due_digests(now=now)
            self.reports_once()
        finally:
            self._background_lock.release()

    def improvement_tick(self):
        # Optional experiments run in the existing independent background lane.
        result = self.improvement.analyze_once()
        evolution = getattr(self, 'evolution', None)
        if evolution is not None:
            evolution.step()
        return result

    def tick(self):
        """Embedded/test convenience. The live service runs separate lanes."""
        self.foreground_tick()
        self.local_delivery_once()
        self.background_tick()
        self.improvement_tick()

    def status(self):
        return {"version": VERSION, "proactive_mode": "observe_then_confirm",
                "protected_people": ["示例主管甲", "示例主管乙"], "service_scope": "DEMO",
                "conversation_report": "each_segment", "digest_hours": [9, 14],
                "timezone": "Asia/Shanghai", "live_report_sender_bound": self.report_sender is not None,
                'project_reader': self.project_reader.health() if hasattr(self.project_reader, 'health') else
                                  {'bound': self.project_reader is not None},
                'runtime_lanes': ['intake', 'local_delivery', 'background', 'improvement'],
                "intake": self.store.rows("SELECT state,count(*) AS count FROM r5_intake GROUP BY state")}

    def handle(self, method, raw_path, data, *, actor):
        self._require_owner(actor)
        path = urlsplit(raw_path).path.rstrip("/")
        root = "/v1/assistant"
        if not path.startswith(root):
            raise ContractError("unknown assistant API")
        route = path[len(root):]
        if method == "GET":
            if route == '/improvements':
                evolution = getattr(self, 'evolution', None)
                return {'proposals': self.improvement.list_proposals(),
                        'evolution': evolution.status() if evolution is not None else {'state': 'unbound'},
                        'autonomous_reports': evolution.reports() if callable(getattr(evolution, 'reports', None)) else [],
                        'production_deployment_performed': False}
            if route.startswith('/trace/'):
                return {'events': self.trace.read(unquote(route[len('/trace/'):])),
                        'timing_note': 'Clock domains are separate; host readback is not a phone observation.'}
            if route == "/status":
                return self.status()
            if route == "/tasks":
                return self.book.owner_brief(principal_id=actor)
            if route.startswith("/tasks/"):
                tid = unquote(route[len("/tasks/"):])
                task = self.book.get_task(tid, principal_id=actor, include_events=True)
                runs = self.store.rows("SELECT t.task_id,t.execution_state,t.result_json FROM h1_tasks t JOIN r5_links l USING(item_id) WHERE l.work_task_id=?", (tid,))
                return {"task": task, "runs": [{"task_id": r["task_id"], "execution_state": r["execution_state"],
                    "result": json.loads(r["result_json"] or "null")} for r in runs]}
            if route.startswith("/projects/"):
                return self.book.project_brief(unquote(route[len("/projects/"):]), principal_id=actor)
            if route == "/reports":
                return {"reports": self.reports.list_reports(actor_id=actor)}
            if route == "/actions":
                return {"actions": self.actions.list()}
        if method == "POST":
            if route == '/feedback':
                return {'feedback_id': self.improvement.record_feedback(data['event_id'],
                    actor_id=actor, text=data['text'], observation_id=data.get('observation_id', ''),
                    source_ref=data.get('source_ref', '')), 'state': 'recorded_for_background_review'}
            if any(k in data for k in ("actor", "actor_id", "principal_id", "department_ids", "is_owner")):
                raise ContractError("identity is supplied by the authenticated host")
            if route == "/tasks":
                origin = data["origin"]
                if origin not in {"gpt_dot", "grok_bot"}:
                    raise ContractError("owner CLI origin required")
                if not isinstance(data.get("event_id"), str) or not data["event_id"]:
                    raise ContractError("stable entry event_id required")
                project = data.get("project_id")
                if project and project not in self.directory.project_ids_for(self._owner()):
                    raise ContractError("project outside current owner bindings")
                stable_id = "entry:" + origin + ":" + data["event_id"]
                old = self.store.one("SELECT event_json,work_task_id FROM r5_intake WHERE item_id=?", (key(self.cfg.tenant_id, "message", stable_id),))
                now = json.loads(old["event_json"])["original_time"] if old else self.store.clock()
                event = Event(self.cfg.tenant_id, stable_id, self.cfg.owner_chat_id, actor,
                    data["text"], now, channel="bot_dm", observed_via=origin,
                    project_ids=(project,) if project else (), version_time=now)
                work_id = data.get("task_id") or "wt_" + key(origin, data["event_id"])[:32]
                if old and (json.loads(old["event_json"])["body"] != event.body or
                            tuple(json.loads(old["event_json"])["project_ids"]) != event.project_ids or old["work_task_id"] != work_id):
                    raise ContractError("entry event_id reused with different content or scope")
                outcome = self.capture(event, origin=origin, work_task_id=work_id)
                return {**outcome, "state": "accepted", "task_id": work_id,
                        "note": "请求已持久化，结果可从同一任务查询。"}
            if route.startswith("/actions/") and route.endswith("/decision"):
                aid = unquote(route[len("/actions/"):-len("/decision")])
                frozen = self.actions.snapshot(aid)
                stamp = frozen["source_stamp"]
                if "item_id" in stamp and "source_hash" in stamp:
                    current = self.store.one("SELECT source_hash,state FROM r5_intake WHERE item_id=?", (stamp["item_id"],))
                    if not current or current["source_hash"] != stamp["source_hash"] or current["state"] == "recalled":
                        raise ContractError("action source changed; review the current request")
                if "task_id" in stamp and "generation" in stamp:
                    current = self.harness.task(stamp["task_id"])
                    if any(current[k] != stamp[k] for k in ("generation", "source_version", "revision", "policy_json") if k in stamp):
                        raise ContractError("task authority changed; review the current action")
                    source = self.store.one("SELECT state,event_json FROM r5_intake WHERE item_id=?", (current["item_id"],))
                    if source and (source["state"] != "admitted" or
                            Event.from_dict(json.loads(source["event_json"])).content_hash != Event.from_dict(json.loads(current["request_json"])).content_hash):
                        raise ContractError("task source changed; review the current action")
                if "followup_id" in stamp:
                    current = self.store.one("SELECT state FROM a5_wb_followups WHERE followup_id=?", (stamp["followup_id"],))
                    if not current or current["state"] != "proposed":
                        raise ContractError("follow-up is no longer awaiting a decision")
                    if self._followup_source_digest(stamp["task_id"]) != stamp.get("work_source_digest"):
                        raise ContractError("follow-up source changed; review the current action")
                return self.actions.decide(aid, data["version"], data["digest"], self._owner(), approve=data["approve"])
            if route.startswith("/actions/") and route.endswith("/reconcile"):
                aid = unquote(route[len("/actions/"):-len("/reconcile")])
                return self.actions.reconcile(aid, data["version"], data["digest"], self._owner(),
                    data["status"], data["evidence_ref"], data.get("readback_evidence_ref"))
            if route.startswith("/reports/") and route.endswith("/reconcile"):
                rid = unquote(route[len("/reports/"):-len("/reconcile")])
                return self.reports.resolve_unknown(rid, outcome=data["outcome"],
                    evidence_ref=data["evidence_ref"], attempt_no=data["attempt_no"],
                    delivery_ref=data.get("delivery_ref", ""), actor_id=actor, now=self.store.clock())
            if route == "/facts":
                project = data["project_id"]
                if project not in self.directory.project_ids_for(self._owner()):
                    raise ContractError("project outside current owner bindings")
                return self.book.record_fact(project, data["field"], data["value"], principal_id=actor,
                    source_ref=data["source_ref"], event_key=data["event_id"],
                    **{k: data[k] for k in ("status", "category", "verification_ref", "observed_at", "supersedes_fact_id") if k in data})
            if route == "/risks":
                if data.get("task_id"):
                    self.book.get_task(data["task_id"], principal_id=actor)
                if not isinstance(data.get("event_id"), str) or not data["event_id"]:
                    raise ContractError("stable risk event_id required")
                self._risk("owner:" + data["event_id"], text=data["text"],
                    source_ref=data["source_ref"], task_id=data.get("task_id", ""),
                    priority=data.get("priority", "P1"), needs_owner=data.get("needs_owner", ""))
                self.risks_once()
                return {"state": "recorded", "risk_id": "owner:" + data["event_id"]}
            if route.startswith("/tasks/") and route.endswith("/state"):
                tid = unquote(route[len("/tasks/"):-len("/state")])
                return self.book.transition(tid, data["state"], principal_id=actor,
                    source_ref=data["source_ref"], verification_ref=data.get("verification_ref", ""))
            if route.startswith("/tasks/") and route.endswith("/steps"):
                tid = unquote(route[len("/tasks/"):-len("/steps")])
                return self.book.add_step(tid, data["event_id"], data["title"], principal_id=actor,
                    assignee_id=data.get("assignee_id", ""), source_ref=data["source_ref"])
            if route == "/followups":
                recipient = self.directory.resolve(data["recipient_id"])
                chat = data["target_chat"]
                channel = "bot_dm" if self.directory.dm_actor(chat) == recipient.user_id else "group"
                if channel == "group" and chat not in self.authority.group_chats:
                    raise ContractError("follow-up target is not bound")
                if not recipient.verified:
                    raise ContractError("unverified follow-up recipient")
                planned = self.book.plan_followup(data["task_id"], data["event_id"], recipient.user_id,
                    data["due_at"], data["text"], principal_id=actor, source_ref=data["source_ref"])
                target = {"chat_id": chat, "actor_id": recipient.user_id, "channel": channel}
                with self.store.transaction() as db:
                    old = db.execute("SELECT target_json FROM r5_followup_targets WHERE followup_id=?", (planned["followup_id"],)).fetchone()
                    if old and old["target_json"] != dumps(target):
                        raise ContractError("follow-up target changed")
                    db.execute("INSERT OR IGNORE INTO r5_followup_targets VALUES(?,?)", (planned["followup_id"], dumps(target)))
                return planned
            if route.startswith("/tasks/") and route.endswith("/decision"):
                tid = unquote(route[len("/tasks/"):-len("/decision")])
                if data.get("scope_digest") and data["scope_digest"] != self._scope_digest(tid):
                    raise ContractError("scope decision source changed; review the current request")
                if self.store.one("SELECT 1 FROM r5_intake i JOIN r5_links l USING(item_id) WHERE l.work_task_id=? AND i.state='recalled'", (tid,)):
                    raise ContractError("recalled request cannot be accepted")
                result = self.book.decide_unrelated(tid, principal_id=actor, accept=data["accept"], source_ref=data["source_ref"])
                if data["accept"]:
                    with self.store.transaction() as db:
                        db.execute("UPDATE r5_intake SET state='pending' WHERE item_id IN (SELECT item_id FROM r5_links WHERE work_task_id=?) AND state='referred'", (tid,))
                return result
        raise ContractError("unknown assistant operation")
