"""Opt-in adapter for the original AM consumer, authorization and outbox.

No transport, consumer, model provider, scheduler or external-action executor is
created here.  The caller supplies an already constructed AmSelfService.  Its
trusted inbox rows and current query grants are the only source of identity and
project capabilities.
"""
from __future__ import annotations

from contextvars import ContextVar
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re
import time
from typing import Any
from zoneinfo import ZoneInfo

from .engine import ConversationAgent, RuntimeConfig
from .knowledge import KnowledgeStore
from .model import CoordinatorModel
from .store import ConversationStore
from .tools import Tool, build_registry, obj, string
from .types import AccessContext, AgentReply, ConversationError, canonical


FAILURE_TEXT = "当前未能处理这条消息。请稍后重试，或联系管理员核对访问范围。"


def _hash(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _encode_context(ctx: AccessContext) -> dict:
    value = asdict(ctx)
    value["allowed_project_ids"] = sorted(ctx.allowed_project_ids)
    return value


def _reply(value: dict) -> AgentReply:
    return AgentReply(value["event_id"], value["text"], value.get("status", "answered"),
                      value.get("replayed", False), tuple(value.get("pending_actions", [])),
                      tuple(value.get("source_ids", [])), value.get('terminal_json'))


def _plain_text(text: str) -> str:
    """Render possible Feishu mention tags inert, without changing tool data."""
    return re.sub(r"<(?=/?at\b)", "＜", text, flags=re.IGNORECASE)


class _PlainTextModel:
    """Sanitize before the engine commits history, reply and delivery content."""
    def __init__(self, model):
        self.model = model

    def complete(self, messages, tools, *, request_id=""):
        turn = self.model.complete(messages, tools, request_id=request_id)
        return replace(turn, text=_plain_text(turn.text))

    def context_budget(self, tools):
        return self.model.context_budget(tools)
    def set_deadline(self,deadline):return self.model.set_deadline(deadline)
    def reset_deadline(self,token):self.model.reset_deadline(token)
    def set_response_protocol(self, enabled): return self.model.set_response_protocol(enabled)
    def reset_response_protocol(self, token): self.model.reset_response_protocol(token)


class _LegacyProjects:
    def __init__(self, bridge: "LegacyConversationBridge"):
        self.bridge = bridge

    def list_projects(self, ctx: AccessContext) -> list[dict]:
        self.bridge.authorize(ctx, "read", {})
        current = self.bridge._current(ctx)
        rows = self.bridge._read_projects(current["row"])
        return [self.bridge._project(row, current["row"]) for row in rows
                if row.get("key") in ctx.allowed_project_ids]

    def read_project(self, ctx: AccessContext, project_id: str) -> dict:
        self.bridge.authorize(ctx, "read", {"project_id": project_id})
        matches = [row for row in self.list_projects(ctx) if row["id"] == project_id]
        if len(matches) != 1:
            raise ConversationError("PROJECT_NOT_AVAILABLE")
        result = matches[0]
        self.bridge._current(ctx).setdefault('read_projects',set()).add(project_id)
        information = getattr(self.bridge.service, 'information', None)
        if information is not None:
            current = self.bridge._current(ctx)
            from .capabilities import information as project_information
            result['information'] = project_information(self.bridge,ctx,{'project_id':project_id,'limit':3,'exclude_ledger':True},current['row']['id'])
        pipeline=self.bridge.agent.response_pipeline
        if pipeline and pipeline.state() is not None and pipeline.mode != 'shadow':
            from am_outcome.evidence import EvidenceBundle
            if information is not None:
                _,bundle=information.evidence_bundle(self.bridge._current(ctx)['row'],project_id)
            else:bundle=EvidenceBundle.build(result,scope=ctx.scope,expires=ctx.content_expires_at)
            result['evidence_bundle']=bundle.data
        observer = getattr(self.bridge, 'project_read_observer', None)
        if observer is not None:
            observer(ctx, result)
        return result


class LegacyConversationBridge:
    """Natural p2p text entry point, disabled unless settings.enabled is True.

    Settings may contain ``enabled``, ``model`` (CoordinatorModel limits), and
    ``runtime`` (RuntimeConfig limits).  External action execution is deliberately
    unavailable in this adapter.  State stays below the original service root.
    """

    def __init__(self, service, settings=None):
        settings = {} if settings is None else settings
        if (type(settings) is not dict or set(settings) - {"enabled", "model", "runtime","business_actions","text_previews","verify_previews","group_conversations","output_rewrite","response","terminal_expression"}
                or type(settings.get("enabled", False)) is not bool):
            raise ConversationError("AM_CONVERSATION_SETTINGS_INVALID")
        self.service, self.s = service, service.s
        self.enabled = settings.get("enabled", False)
        self.business_actions=settings.get('business_actions',False)
        self.text_previews=settings.get('text_previews',False)
        self.verify_previews=settings.get('verify_previews',False)
        self.group_conversations=settings.get('group_conversations',False)
        if any(type(v) is not bool for v in (self.business_actions,self.text_previews,self.verify_previews,self.group_conversations)):raise ConversationError('AM_CONVERSATION_SETTINGS_INVALID')
        if self.business_actions and not self.verify_previews:raise ConversationError('BUSINESS_PREVIEW_VERIFICATION_REQUIRED')
        self.store = self.knowledge = self.agent = self.registry = None
        self._active = ContextVar("am_conversation_legacy_request", default=None)
        if not self.enabled:
            return
        if service.flags.get("query") is not True:
            raise ConversationError("QUERY_NOT_ENABLED")
        from am_response.pipeline import ResponsePipeline
        response = ResponsePipeline(settings.get('response'), binding=self.response_binding, dependency_lookup=self.response_dependencies,
                                    targets=lambda ctx: self._current(ctx).get('project_catalog', []), clock=self.s.clock)
        from .terminal import TerminalExpression, coordinator_provider
        terminal = TerminalExpression(settings.get('terminal_expression'),binding=self.response_binding,
            dependency_lookup=self.response_dependencies, targets=lambda ctx:self._current(ctx).get('project_catalog',[]),
            provider=coordinator_provider(service.g.provider),clock=self.s.clock)
        if terminal.mode!='off' and (response.mode!='legacy' or settings.get('output_rewrite',{}).get('mode','off')!='off'):
            raise ConversationError('CONFLICTING_PRESENTATION_MODES')
        model_settings = settings.get("model", {})
        runtime_settings = dict(settings.get("runtime", {})) if type(settings.get("runtime", {})) is dict else None
        if type(model_settings) is not dict or runtime_settings is None:
            raise ConversationError("AM_CONVERSATION_SETTINGS_INVALID")
        model_settings = dict(model_settings, response_protocol=response.mode in ('canonical', 'enforce'))
        # A late HTTP dispatch cannot safely be killed merely because its client budget expires.
        model_settings.setdefault('min_dispatch_seconds',60)
        if runtime_settings.get("allow_external_actions", False) is not False and not self.business_actions:
            raise ConversationError("CONVERSATION_EXTERNAL_ACTIONS_UNBOUND")
        runtime_settings["allow_external_actions"] = bool(self.business_actions and runtime_settings.get('allow_external_actions',False))
        try:
            model = _PlainTextModel(CoordinatorModel(service.g.provider, **model_settings))
            config = RuntimeConfig(**runtime_settings)
        except TypeError:
            raise ConversationError("AM_CONVERSATION_SETTINGS_INVALID") from None
        directory = Path(service.g.config.root) / "state" / "am-conversation-r1"
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.store = ConversationStore(directory / "conversation.sqlite3", clock=self.s.clock)
        self.knowledge = KnowledgeStore(directory / "knowledge.sqlite3", clock=self.s.clock,
            owner_ids={(service.g.config.tenant_key, service.g.config.app_id): {service.owner}})
        for path in (directory / "conversation.sqlite3", directory / "knowledge.sqlite3"):
            path.chmod(0o600)
        self.projects = _LegacyProjects(self)
        self.registry = build_registry(self.store, self.knowledge, self.projects)
        from .capabilities import information
        self.registry.register(Tool('read_information','读取已接入信息层的本项目来源、反馈与覆盖情况。默认摘要；可用fact_id和section按字段读取。只返回当前身份可见来源，不代替正式验收。',obj({'project_id':string(256),'fact_id':string(128),'section':string(100),'offset':{'type':'integer','minimum':0,'maximum':10000},'limit':{'type':'integer','minimum':1,'maximum':12}},('project_id',)),lambda ctx,args,event:information(self,ctx,args,event)))
        if service.flags.get("items", False):
            self.registry.register(Tool("list_followups",
                "读取原AM已经登记的跟进事项。管理人仅看获准项目，同事仅看本人被指派的事项；"
                "与本对话的私有work_notes分开，也不等同于正式台账计划。",
                obj({"project_id": string(256), "include_closed": {"type": "boolean"},
                     "limit": {"type": "integer", "minimum": 1, "maximum": 30}}),
                self._followups))
        if self.business_actions:
            from .business import BusinessTools
            self.business=BusinessTools(self);self.business.register()
            from .documents import DocumentTools
            self.documents=DocumentTools(self);self.documents.register()
            from .reports import ReportTools,install_knowledge_review
            self.reports=ReportTools(self);self.reports.register();install_knowledge_review(self)
            from .briefings import BriefingTools
            self.briefings=BriefingTools(self);self.briefings.register()
            from .capabilities import install
            install(self)
        self.native_followup_tools=None
        if service.flags.get('items') and getattr(service,'followup',None) is not None:
            from .followup_tools import NativeFollowupTools
            self.native_followup_tools=NativeFollowupTools(self)
            self.native_followup_tools.register()
        output_finalizer = None
        if "output_rewrite" in settings:
            from .rewrite_host import OwnerPrivateRewrite
            output_finalizer = OwnerPrivateRewrite(self, settings["output_rewrite"])
        # Lifecycle maintenance also runs when expression is disabled.
        from am_response.gateway import GatewayProjection
        self.s.response_projection = GatewayProjection(self.s)
        self.agent = ConversationAgent(model, self.store, self.registry, knowledge=self.knowledge,
            config=config, authorize=self.authorize, source_access_version=self.source_access_version,
            output_finalizer=output_finalizer,
            metrics=self._timing, clock=self.s.clock, response_pipeline=response,terminal_expression=terminal)
        # This table joins an exact reply to the original outbox and inbox.  It
        # does not itself send, approve, schedule or retry anything.
        with self.s.lock:
            self.s.conn.execute("""CREATE TABLE IF NOT EXISTS am_conversation_outputs(
                outbox_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, tenant TEXT NOT NULL,
                app TEXT NOT NULL, scope TEXT NOT NULL, input_hash TEXT NOT NULL,
                context TEXT NOT NULL, grant_json TEXT NOT NULL, reply TEXT NOT NULL,
                expires REAL NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                message_id TEXT, error TEXT, UNIQUE(tenant,app,event_id))""")
            self.s.conn.execute('CREATE TABLE IF NOT EXISTS am_conversation_controls(event_id TEXT PRIMARY KEY,scope TEXT NOT NULL,translated TEXT NOT NULL,expires REAL NOT NULL)')
            columns={r[1] for r in self.s.conn.execute('PRAGMA table_info(am_conversation_outputs)')}
            for name,kind in {'rendered_text':'TEXT','verified_at':'REAL','verification_attempts':'INTEGER NOT NULL DEFAULT 0','next_verification':'REAL NOT NULL DEFAULT 0','project_refs':"TEXT NOT NULL DEFAULT '[]'"}.items():
                if name not in columns:self.s.conn.execute('ALTER TABLE am_conversation_outputs ADD COLUMN '+name+' '+kind)

    def wants(self, text: str) -> bool:
        return (self.enabled and self.service.flags.get("query") is True and type(text) is str
                and bool(text.strip()) and not text.lstrip().startswith("/"))

    def _timing(self, metric: dict) -> None:
        try:
            from fs_agent import request_timing
            request_timing.add(self.s, metric["event_id"], "conversation_" + metric["span"], metric["seconds"])
            if metric['span']=='context_budget':
                with self.s.tx() as c:
                    row=c.execute('SELECT stages FROM am_request_timings WHERE event_id=?',(metric['event_id'],)).fetchone()
                    if row:
                        value=json.loads(row['stages']);value['conversation_context_budget'].update({k:metric[k] for k in ('characters','budget','tool_count')})
                        c.execute('UPDATE am_request_timings SET stages=? WHERE event_id=?',(canonical(value),metric['event_id']))
            if metric['span'] in ('output_rewrite','output_rewrite_decision'):
                with self.s.tx() as c:
                    row=c.execute('SELECT stages FROM am_request_timings WHERE event_id=?',(metric['event_id'],)).fetchone()
                    if row:
                        value=json.loads(row['stages'])
                        safe_keys=('mode','disposition','reasons','attempted','contract_fingerprint','prompt_version','output_chars','host_accepted','candidate_adopted')
                        value.setdefault('conversation_output_rewrite',{}).update({k:metric[k] for k in safe_keys if k in metric})
                        c.execute('UPDATE am_request_timings SET stages=? WHERE event_id=?',(canonical(value),metric['event_id']))
        except Exception:
            return  # Optional telemetry must never change the business outcome.

    def _identity(self, row: dict) -> tuple[str, str, str, str, str]:
        config = self.service.g.config
        expires = row.get("expires")
        if (row.get("app") != config.app_id or row.get("tenant") != config.tenant_key
                or row.get("chat_type") not in (('p2p','group') if self.group_conversations else ('p2p',)) or type(expires) not in (int, float)
                or not math.isfinite(expires) or expires <= self.s.clock()):
            raise ConversationError("CONVERSATION_IDENTITY_INVALID")
        meta = self.service.source_meta(row)
        thread = row.get("thread") or meta.get("thread") or ""
        result = (row["tenant"], row["app"], row.get("user"), row.get("chat"), thread)
        if any(type(value) is not str or len(value) > 256 for value in result) or not all(result[:4]):
            raise ConversationError("CONVERSATION_IDENTITY_INVALID")
        return result

    def _grant(self, row: dict, *, verify: bool = True) -> list[str]:
        self._identity(row)
        if row['chat_type']=='group':row['_query_group']=row['chat']
        grant = self.service.query_grant(row, verify=verify)
        if type(grant) is not list or not grant or any(type(v) is not str or not v for v in grant):
            raise ConversationError("CONVERSATION_GRANT_INVALID")
        return sorted(set(grant))

    def _read_projects(self, row: dict) -> list[dict]:
        try:
            return self.service.query_projects(row)
        except Exception as exc:
            if type(exc).__name__ != "SourceLoading" or str(exc) != "SOURCE_LOADING":
                raise
        # Remain in the existing background worker.  This prevents the old
        # SourceLoading handler from creating a loading card in text mode.
        started = time.monotonic()
        try:
            snapshot = self.service.snapshots.get(row.get("_snapshot_version"), wait=True)
            row["_snapshot_version"] = snapshot["version"]
            return self.service.query_projects(row)
        except Exception as exc:
            if type(exc).__name__ == "SourceLoading":
                raise ConversationError("SOURCE_LOADING") from None
            raise
        finally:
            row["_conversation_source_wait_seconds"] = time.monotonic() - started
            self._timing({"event_id": row["id"], "span": "source_wait",
                          "seconds": row["_conversation_source_wait_seconds"]})

    def _current(self, ctx: AccessContext) -> dict:
        current = self._active.get()
        if not current or ctx != current["ctx"]:
            raise ConversationError("CONVERSATION_CONTEXT_NOT_ACTIVE")
        if self._identity(current["row"]) != ctx.scope:
            raise ConversationError("CONVERSATION_SCOPE_CHANGED")
        return current

    def source_access_version(self):
        host=getattr(self.service,'information',None)
        if host is None:return None
        return host.store.access_version()

    def delivery_source_authorized(self,row):
        native=getattr(self,'native_followup_tools',None)
        if native is not None and not native.output_authorized(row):return False
        records=self.s.rows('SELECT context FROM am_conversation_outputs WHERE outbox_id=?',(row['id'],))
        if not records:return True
        try:return json.loads(records[0]['context']).get('source_access_version')==self.source_access_version()
        except Exception:return False

    def authorize(self, ctx: AccessContext, operation: str, args: dict | None = None) -> None:
        current = self._current(ctx)
        if ctx.source_access_version!=self.source_access_version():
            raise ConversationError('SOURCE_ACCESS_CHANGED')
        row = current["row"]
        if self.service.flags.get("query") is not True:
            raise ConversationError("QUERY_NOT_ENABLED")
        if self._grant(row) != current["grant"]:
            raise ConversationError("CONVERSATION_SCOPE_CHANGED")
        if ctx.is_owner != (ctx.actor_id == self.service.owner and row['chat_type']=='p2p'):
            raise ConversationError("CONVERSATION_SCOPE_CHANGED")
        if operation not in ("read", "report", "manage", "execute"):
            raise ConversationError("CONVERSATION_OPERATION_INVALID")
        if operation in ("manage", "execute"):
            if not ctx.is_owner:
                raise ConversationError("OWNER_REQUIRED")
            # Writes/management never reuse the read-only ten-minute identity cache.
            self.service.ledger.port.authorize_user(ctx.actor_id)
        project = (args or {}).get("project_id")
        if project is not None and not ctx.can_access_project(project):
            raise ConversationError("PROJECT_ACCESS_DENIED")
        row["_query_grant"] = list(current["grant"])

    @staticmethod
    def _project(project: dict, row: dict) -> dict:
        observed = row.get("_observed_at")
        observed_at = (datetime.fromtimestamp(observed, ZoneInfo("Asia/Shanghai")).isoformat()
                       if type(observed) in (int, float) else None)
        aliases = [str(value) for value in project.get("aliases", []) if isinstance(value, str)]
        if project.get("business_id"):
            aliases.append(str(project["business_id"]))
        fields = {key: deepcopy(project[key]) for key in ("关键日期", "待办", "项目进展", "人员与职责") if key in project}
        return {"id": project["key"], "title": project.get("title", ""),
                "aliases": list(dict.fromkeys(aliases)), "observed_at": observed_at,
                "identity_status": project.get("identity_status", "unknown"), "fields": fields,
                "snapshot": deepcopy(row.get("_snapshot_meta", {})),
                "evidence_state": "recorded_not_independently_verified"}

    def _followups(self, ctx: AccessContext, args: dict, event_id: str) -> dict:
        self.authorize(ctx, "read", args)
        keys = sorted(ctx.allowed_project_ids)
        if ctx.is_owner and not args.get('project_id'):keys.append('personal:'+ctx.actor_id)
        if ctx.is_owner and not args.get('project_id') and getattr(self,'native_followup_tools',None) is not None:
            tests=self.s.rows("SELECT project_ref,source FROM action_items WHERE owner=? AND project_ref LIKE 'validation:AM-FOLLOWUP-20261102-01:%'",(ctx.actor_id,))
            keys.extend(r['project_ref'] for r in tests if json.loads(r['source']).get('kind')=='synthetic_validation')
            keys=sorted(set(keys))
        if args.get("project_id"):
            keys = [args["project_id"]]
        result = {"record_kind": "am_followup", "source": "existing_am_action_items",
                  "private_work_notes": False, "formal_ledger_plan": False,
                  "items": [], "has_more": False}
        if not keys:
            return result
        # SQL narrows projects; JSON assignees are checked as exact IDs in Python.
        # Chunking avoids host SQLite variable-count differences.
        rows = []
        for start in range(0, len(keys), 200):
            part = keys[start:start + 200]
            where = "project_ref IN (" + ",".join("?" for _ in part) + ")"
            params: list[Any] = list(part)
            if not args.get("include_closed", False):
                where += " AND state NOT IN ('closed','cancelled')"
            if ctx.is_owner:
                where += " AND owner=?"
                params.append(ctx.actor_id)
            rows.extend(self.s.rows("SELECT * FROM action_items WHERE " + where + " ORDER BY updated DESC", tuple(params)))
        visible = []
        for row in rows:
            try:
                assignees = json.loads(row["assignees"])
            except (TypeError, ValueError):
                continue
            if type(assignees) is not list or any(type(actor) is not str for actor in assignees):
                continue
            if not ctx.is_owner and ctx.actor_id not in assignees:
                continue
            item = {key: row.get(key) for key in ("id", "version", "description", "requirements", "deadline",
                    "deadline_kind", "timezone", "state", "paused", "updated", "latest_feedback")}
            item.update(project_id=row["project_ref"], assignees=assignees)
            if getattr(self,'native_followup_tools',None) is not None:
                item['system_test_only']=row['project_ref'].startswith('validation:AM-FOLLOWUP-20261102-01:')
                item['followup_state']=self.service.followup_state(row)[1]
                self.native_followup_tools.remember(ctx,row['id'])
            visible.append(item)
        visible.sort(key=lambda value: value.get("updated") or 0, reverse=True)
        limit = args.get("limit", 15)
        result["items"], result["has_more"] = visible[:limit], len(visible) > limit
        native=getattr(self,'native_followup_tools',None)
        if native is not None and ctx.is_owner:
            native.h.text_ui._remember(self._current(ctx)['row'],[item['id'] for item in result['items']])
        # Re-check after the database read, before any result reaches the model.
        self.authorize(ctx, "read", args)
        return result

    @contextmanager
    def request_context(self, row: dict):
        """Fresh original identity, grant and source scope, isolated per execution."""
        if row.get('chat_type')=='group':row['_query_group']=row['chat'];row['_public_query']=True
        if not self.enabled or self.agent is None:
            raise ConversationError("CONVERSATION_DISABLED")
        if self.service.flags.get("query") is not True:
            raise ConversationError("QUERY_NOT_ENABLED")
        if row.get("kind") != "conversation" or not self.wants(row.get("body")):
            raise ConversationError("CONVERSATION_ROUTE_INVALID")
        identity = self._identity(row)
        grant = self._grant(row)
        row["_query_read"], row["_query_grant"] = True, grant
        # Explicit latest requests may refresh; subsequent reads stay on that
        # observed snapshot for a coherent response.
        if "最新" in row["body"] and "_snapshot_version" not in row:
            row["_force_refresh"] = True
        projects = self._read_projects(row)
        keys = [project.get("key") for project in projects]
        if (any(type(key) is not str or not key or len(key) > 256 for key in keys)
                or len(set(keys)) != len(keys)):
            raise ConversationError("PROJECT_IDENTITIES_INVALID")
        references=[];parent=self.service.source_meta(row).get('parent')
        if row['chat_type']=='group' and parent:
            refs=self.s.rows('SELECT * FROM am_group_refs WHERE message_id=? AND app=? AND tenant=? AND chat=? AND expires>?',(parent,row['app'],row['tenant'],row['chat'],self.s.clock()))
            if not refs:raise ConversationError('QUOTED_PROJECT_UNAVAILABLE')
            references=[p for p in json.loads(refs[0]['projects']) if p in keys]
            row['expires']=min(row['expires'],refs[0]['expires'])
        ctx = AccessContext(*identity, allowed_project_ids=frozenset(keys), all_projects=False,
                            is_owner=identity[2] == self.service.owner and row['chat_type']=='p2p',content_expires_at=row['expires'],reference_project_ids=tuple(references),
                            source_access_version=self.source_access_version())
        token = self._active.set({"row": row, "ctx": ctx, "grant": list(grant), 'project_catalog': projects})
        try:
            self.authorize(ctx, "read")
            yield ctx
        finally:
            self._active.reset(token)

    def response_dependencies(self, ctx, previous):
        service=getattr(self.service,'information',None)
        if service is None or not hasattr(service,'dependency_current'):return {'objects':[]}
        return service.dependency_current(ctx,previous)

    def response_binding(self, ctx, event_id):
        current = self._current(ctx); row = current['row']
        binding = {'kind': 'event', 'scope': list(ctx.scope), 'origin_event_id': row['id'],
                   'engine_event_id': event_id, 'source_access_version': ctx.source_access_version,
                   'dependencies': {}, 'dependency_complete': False}
        harness = getattr(self.service, 'harness', None)
        if harness is not None and harness.durable:
            fence = harness.hooks.fence
            if fence is not None:
                binding.update(kind='run', run_id=fence.run_id, revision=fence.revision,
                               cancel_epoch=fence.cancel_epoch, generation=fence.generation)
            sources = self.s.rows('SELECT dependencies,expiries FROM am_harness_context_sources WHERE event_id=? AND scope_key=?', (event_id, ctx.session_id))
            if sources:
                binding.update(dependencies=json.loads(sources[0]['dependencies']),
                               dependency_expiries=json.loads(sources[0]['expiries']))
        return binding

    def prepare_reply(self, ctx, reply):
        if not isinstance(reply, AgentReply) or not isinstance(reply.text, str) or not reply.text.strip():
            raise ConversationError("CONVERSATION_REPLY_INVALID")
        if _plain_text(reply.text) != reply.text:
            raise ConversationError("CONVERSATION_REPLY_MARKUP_INVALID")
        from .text_ui import display
        rendered = display(self, ctx, reply)
        self.authorize(ctx, "read")
        current = self._current(ctx)
        native=getattr(self,'native_followup_tools',None)
        if native is not None and not native.current(current['row']['id']):
            raise ConversationError('FOLLOWUP_FACTS_CHANGED')
        prepared_row = dict(current['row'], _source_meta=self.service.source_meta(current['row']), _source_access_version=ctx.source_access_version)
        terminal=self.agent.terminal_expression
        if terminal:
            terminal_payload=terminal.before_enqueue(self.agent,ctx,reply)
            if terminal_payload:
                prepared_row['_response_v3']=terminal_payload
                prepared_row['expires']=min(prepared_row['expires'],terminal_payload['expires_at'])
        presentation = self.store.presentation(ctx, reply.event_id)
        if presentation is not None:
            if presentation['semantic_hash'] != _hash(reply.text):
                raise ConversationError('PRESENTATION_HISTORY_MISMATCH')
            for block in presentation['outcome']['protected_blocks']:
                if presentation.get('mode') != 'shadow' and block['role'] != 'terminal' and block['text'] not in rendered:
                    rendered += '\n\n' + block['text']
            prepared_row['_response_v2'] = presentation
            prepared_row['expires'] = min(prepared_row['expires'], presentation['expires_at'])
        return {"row": prepared_row, "context": _encode_context(ctx),
                "grant": list(current["grant"]), "reply": reply.to_dict(),
                "rendered": rendered, "project_refs": sorted(current.get('read_projects', set()))}

    def enqueue_reply(self, conn, prepared, *, event_key):
        """Local SQL only, using the caller's original outbox transaction."""
        row = prepared['row']; ctx = AccessContext(**prepared['context']); grant = prepared['grant']
        native=getattr(self,'native_followup_tools',None)
        if native is not None and not native.current(row['id']):raise ConversationError('FOLLOWUP_FACTS_CHANGED')
        # Recheck again inside the existing atomic outbox transaction. Preparation
        # alone is never a send receipt and cannot outlive a cancellation fence.
        if self.agent.terminal_expression:
            self.agent.terminal_expression.before_enqueue(self.agent,ctx,_reply(prepared['reply']))
        old = conn.execute("SELECT * FROM am_conversation_outputs WHERE tenant=? AND app=? AND event_id=?",
                           (ctx.tenant_id, ctx.app_id, row['id'])).fetchone()
        if old:
            if old['scope'] != ctx.session_id or old['input_hash'] != _hash(row['body']):
                raise ConversationError('CONVERSATION_EVENT_CONFLICT')
            if old['grant_json'] != canonical(grant) or old['context'] != canonical(prepared['context']):
                raise ConversationError('CONVERSATION_SCOPE_CHANGED')
            return old['outbox_id']
        outbox_id = self.service.notify(row, text=prepared['rendered'], kind='conversation',
                                       conn=conn, event_key=event_key)
        conn.execute("""INSERT INTO am_conversation_outputs
            (outbox_id,event_id,tenant,app,scope,input_hash,context,grant_json,reply,expires,rendered_text,project_refs)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (outbox_id,row['id'],ctx.tenant_id,ctx.app_id,ctx.session_id,_hash(row['body']),
             canonical(prepared['context']),canonical(grant),canonical(prepared['reply']),row['expires'],
             prepared['rendered'],canonical(prepared['project_refs'])))
        return outbox_id

    def process(self, row: dict) -> str:
        """Legacy/Phase 1 path. Full harness routes through its persisted worker."""
        harness = getattr(self.service, 'harness', None)
        if harness is not None and harness.durable:
            return harness.admit(row)
        if not self.enabled or self.agent is None:
            raise ConversationError('CONVERSATION_DISABLED')
        started = time.monotonic()
        self.cleanup()
        with self.request_context(row) as ctx:
            old = self.s.rows('SELECT * FROM am_conversation_outputs WHERE tenant=? AND app=? AND event_id=?',
                              (ctx.tenant_id, ctx.app_id, row['id']))
            if old:
                if old[0]['scope'] != ctx.session_id or old[0]['input_hash'] != _hash(row['body']):
                    raise ConversationError('CONVERSATION_EVENT_CONFLICT')
                if old[0]['grant_json'] != canonical(self._current(ctx)['grant']) or old[0]['context'] != canonical(_encode_context(ctx)):
                    raise ConversationError('CONVERSATION_SCOPE_CHANGED')
                return old[0]['outbox_id']
            if len(ctx.reference_project_ids) == 1:
                self.store.set_focus(ctx, ctx.reference_project_ids[0])
            from .text_ui import translate
            reply = self.agent.handle(ctx,row['id'],translate(self,ctx,row),timeout=max(.1,120-(time.monotonic()-started)))
            if reply.event_id != row['id']:
                raise ConversationError('CONVERSATION_REPLY_INVALID')
            prepared = self.prepare_reply(ctx, reply)
            with self.s.tx() as conn:
                return self.enqueue_reply(conn, prepared, event_key='conversation:'+row['id'])

    def delivered(self, row: dict, message_id: str, *, conn=None) -> bool:
        """Acknowledge only a matching, verified original-outbox delivery.

        Called by AmSelfService.delivered inside the gateway's delivery commit.
        A bookkeeping failure must not turn a known successful send into an
        unknown/retried send; failed acknowledgements leave approval unavailable.
        """
        if not self.enabled or self.agent is None or self.service.flags.get("query") is not True:
            return False
        if conn is None:
            with self.s.tx() as connection:
                return self.delivered(row, message_id, conn=connection)
        saved = conn.execute("SELECT * FROM am_conversation_outputs WHERE outbox_id=?", (row.get("id"),)).fetchone()
        if saved is None:
            return False
        try:
            sent = conn.execute("SELECT * FROM outbox WHERE id=?", (row.get("id"),)).fetchone()
            if not sent:
                return False
            sent = dict(sent)
            reply = _reply(json.loads(saved["reply"]))
            ctx = AccessContext(**json.loads(saved["context"]))
            if ctx.source_access_version!=self.source_access_version():return False
            expected=saved['rendered_text'] if 'rendered_text' in saved.keys() and saved['rendered_text'] is not None else reply.text
            if (type(message_id) is not str or not message_id or sent.get("state") != "sent"
                    or sent.get("message_id") != message_id or sent.get("kind") != "text"
                    or sent.get("body") != expected or row.get("body") != expected
                    or sent.get("expires", 0) <= self.s.clock() or saved["expires"] <= self.s.clock()
                    or any(sent.get(key) != row.get(key) for key in ("app", "tenant", "user", "chat"))
                    or (sent["tenant"], sent["app"], sent["user"], sent["chat"]) != ctx.scope[:4]
                    or ctx.session_id != saved["scope"] or ctx.is_owner != (ctx.actor_id == self.service.owner and sent['chat_type']=='p2p')):
                return False
            if saved["state"] == "acknowledged":
                return saved["message_id"] == message_id
            origin = conn.execute("SELECT * FROM am_inbox WHERE id=?", (saved["event_id"],)).fetchone()
            if not origin:
                return False
            origin = dict(origin)
            saved_grant = json.loads(saved["grant_json"])
            # Gateway already checked query_delivery_authorized before sending
            # and via the original readback guard. Its outbox transaction is
            # active here: query_grant(verify=True) would run request_timing's
            # write transaction inside that transaction. Only inspect current
            # local policy and the original persisted delivery bindings here.
            if (self._identity(origin) != ctx.scope or _hash(origin["body"]) != saved["input_hash"]
                    or ctx.actor_id in getattr(self.service, "query_settings", {}).get("denied_users", [])
                    or self._grant(origin, verify=False) != saved_grant):
                return False
            notice = conn.execute("SELECT * FROM am_query_deliveries WHERE outbox_id=?", (row["id"],)).fetchone()
            source = conn.execute("SELECT event_id FROM am_outbox_events WHERE outbox_id=?", (row["id"],)).fetchone()
            if (not notice or notice["denial"] or notice["scope_hash"] != _hash(saved_grant)
                    or not source or source["event_id"] != saved["event_id"]):
                return False
            if self.verify_previews and (reply.pending_actions or reply.status=='knowledge_candidate') and not saved['verified_at']:
                conn.execute("UPDATE am_conversation_outputs SET state='awaiting_verification',message_id=? WHERE outbox_id=?",(message_id,row['id']))
                return False
            self.agent.acknowledge_presentation(ctx, reply)
            conn.execute("UPDATE am_conversation_outputs SET state='acknowledged',message_id=?,error=NULL WHERE outbox_id=?",
                         (message_id, row["id"]))
            if sent['chat_type']=='group':conn.execute('INSERT OR IGNORE INTO am_group_refs VALUES(?,?,?,?,?,?)',(message_id,sent['app'],sent['tenant'],sent['chat'],saved['project_refs'],saved['expires']))
            return True
        except Exception:
            conn.execute("UPDATE am_conversation_outputs SET state='ack_failed',error='PRESENTATION_NOT_ACKNOWLEDGED' WHERE outbox_id=?",
                         (row.get("id"),))
            return False

    def verify_presentation(self,outbox_id):
        saved=self.s.rows("SELECT * FROM am_conversation_outputs WHERE outbox_id=? AND state='awaiting_verification'",(outbox_id,))
        if not saved:return
        saved=saved[0]
        if saved['expires']<=self.s.clock():return
        out=self.s.rows('SELECT * FROM outbox WHERE id=?',(outbox_id,))[0]
        self.s.execute('UPDATE am_conversation_outputs SET verification_attempts=verification_attempts+1,next_verification=? WHERE outbox_id=?',(self.s.clock()+30,outbox_id))
        try:ok=self.service.g.api.verify_text(saved['message_id'],self.service.query_readback_row(out))
        except Exception:ok=False
        if ok:
            with self.s.tx() as c:
                c.execute('UPDATE am_conversation_outputs SET verified_at=? WHERE outbox_id=?',(self.s.clock(),outbox_id))
                self.delivered(out,saved['message_id'],conn=c)
                projection = getattr(self.s, 'response_projection', None)
                if projection is not None:
                    projection.receipt(c, out, saved['message_id'], verified=True)

    def close(self) -> None:
        if self.store is not None:
            self.store.close()
        if self.knowledge is not None:
            self.knowledge.close()

    def cleanup(self):
        now=self.s.clock()
        if now-getattr(self,'_cleaned_at',0)<60:return
        self._cleaned_at=now
        self.store.cleanup_messages()
        self.s.execute("UPDATE am_conversation_outputs SET reply='{}',context='{}',grant_json='[]',rendered_text=NULL,project_refs='[]',state='expired' WHERE expires<=? AND (state!='expired' OR rendered_text IS NOT NULL OR project_refs!='[]')",(now,))
        self.s.execute('DELETE FROM am_conversation_controls WHERE expires<=?',(now,))
