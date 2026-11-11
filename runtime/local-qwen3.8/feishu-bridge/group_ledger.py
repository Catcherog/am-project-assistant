"""Read-only DEMO answers for verified @ messages in groups containing this bot.

The platform client owns identity, source and reply target. Optional local Qwen
classifies and phrases replies; it has no tools, credentials or write API.
"""
from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context

from fs_agent.message_text import normalize_message
from query_rules import FIELDS, fields_for, deterministic_intent, identifiers, choice_number, resolve_identifiers

TTL = 86400
SOURCE = "https://example.invalid/sheets/REPLACE_WITH_SPREADSHEET_TOKEN"
TOKEN = "REPLACE_WITH_SPREADSHEET_TOKEN"
SHEET = "REPLACE_WITH_SHEET_ID"
HEADERS = ("项目／需求", "关键日期", "待办", "项目进展", "人员与职责")
CODE = re.compile(r"(?<![A-Za-z0-9])(?:IRAD|CERT|RA|QI|PT)-\d{4}-\d{3}(?!\d)", re.I)
REQUEST = re.compile(r"需求\s*([0-9]+)(?!\d)")
STOP_WORDS = ("项目进展", "项目进度", "项目", "需求", "目前", "现在", "最新", "进展", "进度",
              "如何", "怎么样", "什么情况", "什么", "情况", "请问", "请", "帮我查一下", "查一下",
              "查询", "一下", "告诉我", "还有", "是否", "已经", "了吗", "问题", "下一步", "计划",
              "什么时候", "完成", "时间", "日期", "谁负责", "负责人", "人员", "待办", "要求", "的")


class LedgerError(RuntimeError):
    """Content-free operational error, suitable for status files."""


def compact(value):
    return re.sub(r"[^a-z0-9\u3400-\u9fff]", "", unicodedata.normalize("NFKC", value).lower())


def title(value):
    lines = [s.strip() for s in value.splitlines() if s.strip()]
    if not lines:
        return ""
    if (CODE.fullmatch(lines[0]) or re.fullmatch(r"需求\s*\d+", lines[0])) and len(lines) > 1:
        return "｜".join(lines[:2])
    return lines[0]


def cell_text(value):
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    # ToString normally returns strings. Reject unknown rich representations
    # instead of silently losing qualifications or treating a URL as content.
    raise LedgerError("LEDGER_CELL_FORMAT")


def parse_rows(values):
    if not values or not isinstance(values[0], list):
        raise LedgerError("LEDGER_EMPTY")
    headers = [cell_text(x) for x in values[0]]
    normalized = [compact(x) for x in headers]
    indices = {}
    for name in HEADERS:
        # Bind only independently observed headings. The progress alias was
        # verified on 2026.10.28 and the owner-action alias on 2026.11.4.
        # Duplicate aliases or unknown headings still fail closed.
        aliases = {compact(name)}
        if name == "项目进展":
            aliases.add(compact("项目信息"))
        if name == "待办":
            aliases.add(compact("本人待办与跟进"))
        matches = [i for i, h in enumerate(normalized) if h in aliases]
        if len(matches) != 1:
            raise LedgerError("LEDGER_HEADER_CHANGED")
        indices[name] = matches[0]
    projects = []
    for number, row in enumerate(values[1:], 2):
        if not isinstance(row, list):
            raise LedgerError("LEDGER_ROW_FORMAT")
        item = {key: cell_text(row[i]) if i < len(row) else "" for key, i in indices.items()}
        item["title"] = title(item["项目／需求"])
        if not item["title"]:
            continue
        item["key"] = compact(item["title"])
        item["progress_heading"] = headers[indices["项目进展"]]
        item["row"] = number
        projects.append(item)
    if not projects or len({p["key"] for p in projects}) != len(projects):
        raise LedgerError("LEDGER_PROJECT_IDENTITIES")
    return projects


def select_projects(question, projects, previous=None):
    q = unicodedata.normalize("NFKC", question)
    codes = {x.upper() for x in CODE.findall(q)}
    requests = set(REQUEST.findall(q))
    if codes or requests:
        result = [p for p in projects if codes.intersection(x.upper() for x in CODE.findall(p["title"]))
                  or requests.intersection(REQUEST.findall(p["title"]))]
        # An unknown explicit identifier never falls through to a different project.
        return result
    # A complete recorded name can appear anywhere in a natural question.
    # This is source-name matching, never a grant of project visibility; callers
    # must still supply only the audience's currently authorised rows.
    literal = []
    normalized_question = compact(q)
    for p in projects:
        for alias in [p['title'], *p.get('aliases', ())]:
            if not isinstance(alias, str):
                continue
            bare = REQUEST.sub('', CODE.sub('', alias)).strip(' ｜:：\n')
            name = compact(bare)
            if not name:
                continue
            if re.search(r'[\u3400-\u9fff]', name):
                matched = len(name) >= 3 and name in normalized_question
            else:
                matched = bool(re.search(r'(?<![a-z0-9])' + re.escape(name) + r'(?![a-z0-9])',
                                         unicodedata.normalize('NFKC', q).lower()))
            if matched:
                literal.append(p)
                break
    if literal:
        return literal
    cleaned = q.lower()
    for word in STOP_WORDS:
        cleaned = cleaned.replace(word, "")
    cleaned = re.sub(r"/(?:progress|help|status|menu)\b", "", cleaned)
    query = compact(cleaned)
    scores = []
    for p in projects:
        name = compact(p["title"])
        names=[name]+[compact(a) for a in p.get('aliases',[]) if isinstance(a,str)]
        score = 0
        if len(query) >= 2 and any(query in n for n in names):
            score = len(query)
        # Model names are matched as complete tokens, with an optional family
        # prefix such as DemoDrive70 in the recorded DemoDrive70FV4 project title.
        tokens = re.findall(r"demodrive\d+[a-z0-9]*|demodrive\d+|hd\d+|demotuner|ota|sdk|vts|8d", q.lower())
        for token in tokens:
            if re.search(re.escape(token) + (r"(?!\d)" if token[-1].isdigit() else r"(?![a-z0-9])"), name):
                score = max(score, len(token))
        words = re.findall(r"[\u3400-\u9fff]{2,}", cleaned)
        for word in words:
            if word in name:
                score = max(score, len(word))
        if score:
            scores.append((score, p))
    if scores:
        best = max(s for s, _ in scores)
        return [p for s, p in scores if s == best]
    if previous and re.search(r"^(那|它|这个|该项目)|什么时候|谁负责|下一步|还有什么问题", q.strip()):
        return [p for p in projects if p["key"] == previous]
    return []


def render_answer(question, projects, previous=None, now=None):
    matches = select_projects(question, projects, previous)
    if len(matches) != 1:
        choices = matches or projects
        intro = "匹配到多个项目，请补充项目编号或完整名称：" if matches else "请提供台账中的项目编号或名称；目前可查询："
        listing = "\n".join("• " + p["title"] for p in choices[:5])
        return f"{intro}\n{listing}\n\n例如：@机器人 需求75目前进展如何？\n来源：{SOURCE}", None
    return render_project(matches[0], fields_for(question), now), matches[0]["key"]


def render_project(p, requested_fields=None, now=None, *, detail=False, budget=200):
    from source_views import project_data,render_data
    data=project_data(p,requested_fields,budget=budget,detail=detail)
    body=render_data(data)
    read_at=datetime.fromtimestamp(now if now is not None else time.time(),ZoneInfo("Asia/Shanghai"))
    return body+f"\n读取：{read_at:%Y-%m-%d %H:%M}｜[查看来源原文]({SOURCE}?sheet={SHEET})"


def render_projects(projects, fields, now=None, *, unresolved=(),conflicts=()):
    from source_views import project_data,render_data
    selected=projects[:5]
    budget=max(60,220//max(1,len(selected)))
    parts=[render_data(project_data(p,fields,budget=budget)) for p in selected]
    if unresolved:parts.append('未找到编号：'+'、'.join(unresolved[:5]))
    if conflicts:parts.append('编号重复，需核对：'+'、'.join(conflicts[:5]))
    if len(projects)>5 or len(unresolved)>5 or len(conflicts)>5:parts.append('本页显示前 5 项，请缩小编号范围或在项目列表翻页。')
    read_at=datetime.fromtimestamp(now if now is not None else time.time(),ZoneInfo('Asia/Shanghai'))
    return '\n\n'.join(parts)+f"\n读取：{read_at:%Y-%m-%d %H:%M}｜[查看来源原文]({SOURCE}?sheet={SHEET})"


class FeishuLedgerPort:
    def __init__(self, client, *, app_id, tenant_key, bot_open_id, department_root_id, disclosure=None):
        if not isinstance(department_root_id, str) or not re.fullmatch(r"od-[A-Za-z0-9]+", department_root_id):
            raise LedgerError("DEMO_DEPARTMENT_NOT_CONFIGURED")
        self.client, self.app_id = client, app_id
        self.tenant_key, self.bot_open_id = tenant_key, bot_open_id
        self.department_root_id = department_root_id
        self.disclosure = disclosure or {}
        self.identity_pool = ThreadPoolExecutor(max_workers=3,thread_name_prefix='demo-identity')
        self.directory_lock = threading.RLock()
        self.directory_future = None
        self.directory = None
        self.io_counts = {'directory':0,'profile':0,'group':0,'source':0}

    def _read_directory(self):
        started=time.monotonic();ids=set();token=None;seen=set()
        for _ in range(20):
            query={'department_id_type':'open_department_id','fetch_child':'true','page_size':'50'}
            if token:query['page_token']=token
            data=self.get(f'/open-apis/contact/v3/departments/{self.department_root_id}/children',query)
            items=data.get('items')
            if not isinstance(items,list):raise LedgerError('DIRECTORY_READ_UNAVAILABLE')
            ids.update(d['open_department_id'] for d in items if isinstance(d,dict) and d.get('open_department_id') and not (d.get('status') or {}).get('is_deleted'))
            if not data.get('has_more'):
                result={'ids':ids,'valid_until':started+600}
                with self.directory_lock:self.directory=result
                return result
            token=data.get('page_token')
            if not isinstance(token,str) or not token or token in seen:break
            seen.add(token)
        raise LedgerError('DIRECTORY_READ_UNAVAILABLE')

    def prewarm_directory(self):
        with self.directory_lock:
            if self.directory_future and not self.directory_future.done():return self.directory_future
            self.directory_future=self.identity_pool.submit(self._read_directory)
            return self.directory_future

    def get(self, uri, query=None):
        import lark_oapi as lark
        category='source' if '/sheets/' in uri else 'directory' if '/departments/' in uri else 'profile' if '/users/' in uri else 'group'
        with self.directory_lock:self.io_counts[category]+=1
        builder = (lark.BaseRequest.builder().http_method(lark.HttpMethod.GET).uri(uri)
                   .token_types({lark.AccessTokenType.TENANT}))
        builder.queries(list((query or {}).items()))
        try:
            from fs_agent import request_timing
            with request_timing.remote('remote_source' if '/sheets/' in uri else 'remote_identity'):
                response = self.client.request(builder.build())
            if len(response.raw.content) > 2_000_000:
                raise LedgerError("LEDGER_RESPONSE_TOO_LARGE")
            value = json.loads(response.raw.content)
        except LedgerError:
            raise
        except Exception:
            raise LedgerError("PLATFORM_READ_UNAVAILABLE") from None
        if value.get("code") != 0:
            raise LedgerError("PLATFORM_READ_" + str(value.get("code", "UNKNOWN")))
        return value.get("data", {})

    def authorize_private_user(self,user):
        profile=self.get(f'/open-apis/contact/v3/users/{user}',{'user_id_type':'open_id','department_id_type':'open_department_id'}).get('user',{})
        status=profile.get('status') or {}
        if (profile.get('open_id')!=user or status.get('is_activated') is not True
                or any(status.get(k) is True for k in ('is_resigned','is_frozen','is_exited'))):
            raise LedgerError('QUESTIONER_NOT_ACTIVE')
        departments=profile.get('department_ids')
        if not isinstance(departments,list) or not departments or len(departments)>20:raise LedgerError('DEMO_MEMBERSHIP_UNVERIFIED')
        if self.department_root_id in departments:return
        with self.directory_lock:directory=self.directory
        if not directory or directory['valid_until']<=time.monotonic():
            directory=self.prewarm_directory().result(timeout=45)
        elif directory['valid_until']-time.monotonic()<300:
            self.prewarm_directory()
        if not set(departments).intersection(directory['ids']):raise LedgerError('DEMO_MEMBERSHIP_UNVERIFIED')
        return {'valid_until':directory['valid_until']}

    def authorize_user(self, user, *, parallel=False):
        """Refresh employment and the DEMO subtree; names never grant access.

        The root open_department_id must come from the verified DEMO selection.
        A user who belongs to several departments needs one verified DEMO branch.
        No allow decision is cached across queries or between read and delivery.
        """
        first=None
        if parallel:
            first=self.identity_pool.submit(copy_context().run,self.get,
                f"/open-apis/contact/v3/departments/{self.department_root_id}/children",
                {"department_id_type":"open_department_id","fetch_child":"true","page_size":"50"})
        profile = self.get(f"/open-apis/contact/v3/users/{user}",
                           {"user_id_type": "open_id", "department_id_type": "open_department_id"}).get("user", {})
        status = profile.get("status") or {}
        if (profile.get("open_id") != user or status.get("is_activated") is not True
                or any(status.get(key) is True for key in ("is_resigned", "is_frozen", "is_exited"))):
            raise LedgerError("QUESTIONER_NOT_ACTIVE")
        departments = profile.get("department_ids")
        if not isinstance(departments, list) or not departments or len(departments) > 20:
            raise LedgerError("DEMO_MEMBERSHIP_UNVERIFIED")
        if self.department_root_id in departments:
            return
        # The approved basic-contact scope exposes department IDs but redacts
        # names and parent_department_id. Ask the platform for the descendants
        # of the pinned DEMO root instead of inferring a hierarchy from names or
        # requiring additional contact fields. Paginate, and never cache allow.
        target_ids = {d for d in departments if isinstance(d, str) and re.fullmatch(r"od-[A-Za-z0-9]+", d)}
        token, seen = None, set()
        for _ in range(20):
            query = {"department_id_type": "open_department_id", "fetch_child": "true", "page_size": "50"}
            if token:
                query["page_token"] = token
            data = first.result(timeout=22) if first is not None and token is None else self.get(f"/open-apis/contact/v3/departments/{self.department_root_id}/children", query)
            items = data.get("items")
            if not isinstance(items, list):
                raise LedgerError("DEMO_MEMBERSHIP_UNVERIFIED")
            if any(d.get("open_department_id") in target_ids and not (d.get("status") or {}).get("is_deleted")
                   for d in items if isinstance(d, dict)):
                return
            if not data.get("has_more"):
                break
            token = data.get("page_token")
            if not isinstance(token, str) or not token or token in seen:
                break
            seen.add(token)
        raise LedgerError("DEMO_MEMBERSHIP_UNVERIFIED")

    def guard(self, chat, user):
        if chat not in self.disclosure:
            raise LedgerError("GROUP_DISCLOSURE_NOT_APPROVED")
        group = self.get(f"/open-apis/im/v1/chats/{chat}")
        member = self.get(f"/open-apis/im/v1/chats/{chat}/members/is_in_chat")
        if (group.get("tenant_key") != self.tenant_key or group.get("chat_status") != "normal"
                or group.get("chat_mode") not in ("group", "topic") or member.get("is_in_chat") is not True):
            raise LedgerError("GROUP_NOT_AVAILABLE")
        token = None
        seen = set()
        for _ in range(30):
            query = {"member_id_type": "open_id", "page_size": "100"}
            if token:
                query["page_token"] = token
            members = self.get(f"/open-apis/im/v1/chats/{chat}/members", query)
            if any(m.get("member_id") == user and m.get("member_id_type") == "open_id" for m in members.get("items", [])):
                self.authorize_user(user)
                return
            if not members.get("has_more"):
                break
            token = members.get("page_token")
            if not token or token in seen:
                break
            seen.add(token)
        raise LedgerError("QUESTIONER_NOT_VERIFIED_IN_GROUP")

    def filter_projects(self, chat, projects):
        scope = self.disclosure.get(chat, [])
        return [p for p in projects if "*" in scope or set(identifiers(p["title"])).intersection(scope)]

    def guard_disclosure(self, chat, projects):
        if len(self.filter_projects(chat, projects)) != len(projects):
            raise LedgerError("PROJECT_DISCLOSURE_CHANGED")

    def read(self):
        data = self.get(f"/open-apis/sheets/v3/spreadsheets/{TOKEN}/sheets/query")
        sheets = [s for s in data.get("sheets", []) if s.get("sheet_id") == SHEET]
        if len(sheets) != 1:
            raise LedgerError("LEDGER_SHEET_CHANGED")
        grid = sheets[0].get("grid_properties", {})
        count = grid.get("row_count", 0)
        if not isinstance(count, int) or not 2 <= count <= 1000:
            raise LedgerError("LEDGER_SIZE_CHANGED")
        data = self.get(f"/open-apis/sheets/v2/spreadsheets/{TOKEN}/values/{SHEET}!A1:G{count}",
                        {"valueRenderOption": "ToString", "dateTimeRenderOption": "FormattedString"})
        result = data.get("valueRange", {})
        if result.get("range") != f"{SHEET}!A1:G{count}":
            raise LedgerError("LEDGER_RANGE_CHANGED")
        values = result.get("values")
        if not isinstance(values, list) or len(values) > count:
            raise LedgerError("LEDGER_VALUES_INVALID")
        projects = parse_rows(values)
        for project in projects:
            project["source_revision"] = data.get("revision")
        return projects


class GroupLedger:
    def __init__(self, root, port, reply, *, clock=time.time, stopped=lambda: False, assistant=None):
        self.port, self.reply, self.clock, self.stopped = port, reply, clock, stopped
        self.assistant = assistant
        self.root = Path(root)
        self.mutex = threading.RLock()
        self.db = sqlite3.connect(self.root / "group-ledger.sqlite3", timeout=1, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA secure_delete=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS ledger_inbox(
              mid TEXT PRIMARY KEY, chat TEXT NOT NULL, user TEXT NOT NULL,
              question TEXT NOT NULL, created REAL NOT NULL, received REAL NOT NULL,
              state TEXT NOT NULL, error TEXT);
            CREATE TABLE IF NOT EXISTS ledger_focus(
              chat TEXT NOT NULL, user TEXT NOT NULL, project TEXT NOT NULL,
              expires REAL NOT NULL, PRIMARY KEY(chat,user));
            CREATE TABLE IF NOT EXISTS group_turns(
              mid TEXT PRIMARY KEY, chat TEXT NOT NULL, user TEXT NOT NULL,
              question TEXT NOT NULL, answer TEXT NOT NULL, mode TEXT NOT NULL,
              created REAL NOT NULL, expires REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS ledger_ai_events(
              mid TEXT PRIMARY KEY, mode TEXT NOT NULL, model_calls INTEGER NOT NULL,
              grounding TEXT);
        """)
        with self.db:
            self.db.execute("UPDATE ledger_inbox SET state='interrupted',question='' WHERE state='processing'")
            self.db.execute("UPDATE ledger_inbox SET state='delivery_unknown',question='' WHERE state='sending'")
        with self.mutex, self.db:
            columns={r[1] for r in self.db.execute("PRAGMA table_info(ledger_inbox)")}
            for name,kind in {"thread":"TEXT NOT NULL DEFAULT ''", "parent":"TEXT", "root":"TEXT", "project":"TEXT", "answer_mid":"TEXT", "chat_type":"TEXT NOT NULL DEFAULT 'group'"}.items():
                if name not in columns:self.db.execute("ALTER TABLE ledger_inbox ADD COLUMN "+name+" "+kind)
            if "thread" not in {r[1] for r in self.db.execute("PRAGMA table_info(group_turns)")}:
                self.db.execute("ALTER TABLE group_turns ADD COLUMN thread TEXT NOT NULL DEFAULT ''")
            self.db.execute("CREATE TABLE IF NOT EXISTS query_conversations(scope TEXT PRIMARY KEY,focus TEXT,pending TEXT,version INTEGER NOT NULL,expires REAL NOT NULL)")
            self.db.execute("CREATE TABLE IF NOT EXISTS query_migrations(version TEXT PRIMARY KEY)")
            self.db.execute("INSERT OR IGNORE INTO query_migrations VALUES('A-conversations-1')")
        from fs_agent.project_catalog import ProjectCatalog
        from am_response.channels import ProtectedChannelJournal
        self.response_journal = ProtectedChannelJournal(self.db)
        self.catalog=ProjectCatalog(self.db, self.port.tenant_key+"/"+self.port.app_id+"/"+TOKEN+"/"+SHEET,self.clock)
        self.cleanup()

    def cleanup(self):
        now = self.clock()
        with self.mutex, self.db:
            self.response_journal.cleanup(now)
            self.db.execute("DELETE FROM ledger_focus WHERE expires<=?", (now,))
            self.db.execute("DELETE FROM query_conversations WHERE expires<=?", (now,))
            self.db.execute("DELETE FROM group_turns WHERE expires<=?", (now,))
            self.db.execute("UPDATE ledger_inbox SET question='',state='expired' WHERE created<=? AND state='queued'", (now-TTL,))
            self.db.execute("UPDATE ledger_inbox SET question='' WHERE created<=?", (now-TTL,))
            self.db.execute("DELETE FROM ledger_inbox WHERE created<=?", (now-7*TTL,))
            self.db.execute("DELETE FROM ledger_ai_events WHERE mid NOT IN (SELECT mid FROM ledger_inbox)")

    def history(self, chat, user, thread=""):
        rows = self.db.execute("SELECT question,answer,mode,expires FROM group_turns WHERE chat=? AND user=? AND thread=? AND expires>? ORDER BY created DESC LIMIT 4",
                               (chat, user, thread, self.clock())).fetchall()
        history, size = [], 0
        for row in rows:
            size += len(row["question"]) + len(row["answer"])
            if size > 6000:
                break
            history.append(dict(row))
        return list(reversed(history))

    def ingest(self, payload):
        try:
            h, e = payload["header"], payload["event"]
            m, s = e["message"], e["sender"]
            if (h["app_id"] != self.port.app_id or h["tenant_key"] != self.port.tenant_key
                    or h["event_type"] != "im.message.receive_v1" or s["sender_type"] != "user"
                    or m["chat_type"] != "group" or m["message_type"] not in ("text", "post")):
                return False
            user, chat, mid = s["sender_id"]["open_id"], m["chat_id"], m["message_id"]
            if not all(re.fullmatch(prefix + r"[A-Za-z0-9_]{1,160}", value)
                       for prefix, value in (("ou_", user), ("oc_", chat), ("om_", mid))):
                return False
            normalized = normalize_message(m, bot_open_id=self.port.bot_open_id, max_chars=2000)
            created, now = int(m["create_time"]) / 1000, self.clock()
            if not normalized.bot_mentioned or not now-TTL < created <= now+60:
                return False
        except (KeyError, ValueError, TypeError):
            return False
        with self.mutex, self.db:
            if self.db.execute("SELECT 1 FROM ledger_inbox WHERE mid=?", (mid,)).fetchone():
                return False
            count = self.db.execute("SELECT COUNT(*) FROM ledger_inbox WHERE state IN ('queued','processing','sending')").fetchone()[0]
            personal = self.db.execute("SELECT COUNT(*) FROM ledger_inbox WHERE user=? AND state IN ('queued','processing','sending')", (user,)).fetchone()[0]
            if count >= 32 or personal >= 4:
                raise LedgerError("LEDGER_QUEUE_FULL")
            self.db.execute("INSERT INTO ledger_inbox(mid,chat,user,question,created,received,state,thread,parent,root) VALUES(?,?,?,?,?,?,'queued',?,?,?)",
                            (mid, chat, user, normalized.text.strip(), created, now,str(m.get("thread_id") or ""),m.get("parent_id"),m.get("root_id")))
        return True

    def finish(self, mid, state, error=None):
        with self.mutex, self.db:
            self.db.execute("UPDATE ledger_inbox SET state=?,error=?,question='' WHERE mid=?", (state, error, mid))
        self.write_status()

    def scope(self,row):
        return json.dumps([self.port.tenant_key,self.port.app_id,row["chat"],row["user"],row["thread"]],separators=(",",":"))

    def can_process_without_model(self,row):
        route=deterministic_intent(row["question"])
        return route in ("help","cancel","action","explain") or route=="project" or choice_number(row["question"]) is not None

    def step(self, *, model_available=True):
        self.cleanup()
        with self.mutex,self.db:
            rows=self.db.execute("SELECT * FROM ledger_inbox WHERE state='queued' ORDER BY received LIMIT 32").fetchall()
            blocked=not model_available or self.assistant is not None and self.assistant.paused()
            row=next((dict(r) for r in rows if not blocked or self.can_process_without_model(r)),None)
            if row is None:return False
            self.db.execute("UPDATE ledger_inbox SET state='processing' WHERE mid=? AND state='queued'",(row["mid"],))
            scope=self.scope(row)
            conversation=self.db.execute("SELECT * FROM query_conversations WHERE scope=? AND expires>?",(scope,self.clock())).fetchone()
            previous=conversation["focus"] if conversation else None
            pending=json.loads(conversation["pending"] or "null") if conversation else None
            history=self.history(row["chat"],row["user"],row["thread"])
            expires=min([row["created"]+TTL]+[h["expires"] for h in history]+([conversation["expires"]] if conversation else []))
            if row["parent"]:
                reference=self.db.execute("SELECT project,created FROM ledger_inbox WHERE (mid=? OR answer_mid=?) AND chat=? AND user=? AND created>?",(row["parent"],row["parent"],row["chat"],row["user"],self.clock()-TTL)).fetchone()
                # An explicit unresolved/cross-user quote never borrows recent focus.
                previous=reference["project"] if reference else None
                if reference:expires=min(expires,reference["created"]+TTL)
                pending=None
        try:self.port.guard(row["chat"],row["user"])
        except Exception:
            self.finish(row["mid"],"rejected","GROUP_OR_MEMBER_UNVERIFIED");return True
        selected=None;error=None;mode="project";used=[];next_pending=None;result=None
        calls_before=self.assistant.total_calls if self.assistant is not None else 0
        def load_projects():
            nonlocal used
            fetched=time.monotonic()
            projects=self.port.read()
            if hasattr(self,'health'):self.health.mark('fetch','ok',request_id=row['mid'],elapsed=time.monotonic()-fetched)
            with self.mutex:
                projects=self.catalog.sync(projects,complete=all(p.get("read_status","complete")=="complete" for p in projects),revision=projects[0].get("source_revision") if projects else None)
            used=self.port.filter_projects(row["chat"],projects) if hasattr(self.port,"filter_projects") else projects
            return used
        try:
            from group_ai import Reply
            choice=choice_number(row["question"])
            if self.clock()-row["received"]>600:
                result=Reply("这条查询已排队超过10分钟，请重新提供项目名称。",mode="error")
            elif choice:
                if not pending or choice>len(pending["candidates"]):
                    result=Reply("没有对应的有效候选，请重新查询项目。",mode="clarify")
                else:
                    projects=load_projects();key=pending["candidates"][choice-1]
                    matches=[p for p in projects if p["key"]==key]
                    if len(matches)!=1:result=Reply("候选身份已变化，请重新查询。",mode="clarify")
                    else:result=Reply(render_project(matches[0],pending["fields"],self.clock()),key,"project")
            elif self.assistant is not None and not blocked:
                result=self.assistant.reply(row["question"],history,previous,load_projects,timeout=min(120,expires-self.clock()),chat_id=row["chat"],message_id=row["mid"])
            else:
                route=deterministic_intent(row["question"])
                if route in ("action","explain","help","cancel"):
                    result=Reply("可按项目编号查询进展、待办和负责人；操作与编号规则解释请使用相应入口。",mode="general")
                else:
                    projects=load_projects();requested,resolved,unresolved,conflicts=resolve_identifiers(row["question"],projects)
                    if requested:
                        parts=[render_projects(resolved,fields_for(row["question"]),self.clock(),unresolved=unresolved,conflicts=conflicts)]
                        result=Reply("\n\n".join(parts),resolved[0]["key"] if len(requested)==len(resolved)==1 else None,"project")
                    else:
                        matches=select_projects(row["question"],projects,previous)
                        if len(matches)==1:result=Reply(render_project(matches[0],fields_for(row["question"]),self.clock()),matches[0]["key"],"project")
                        else:
                            candidates=matches[:5]
                            result=Reply("请提供项目编号或名称。"+("\n"+"\n".join(f"{i}. "+p["title"] for i,p in enumerate(candidates,1)) if candidates else ""),mode="clarify",candidates=[p["key"] for p in candidates],fields=fields_for(row["question"]),original_question=row["question"])
            answer,selected,mode=result.text,result.project,result.mode
            if result.candidates:next_pending={"question":result.original_question,"fields":result.fields,"candidates":result.candidates}
        except Exception as exc:
            error=str(exc) if re.fullmatch(r"[A-Z_0-9]{1,80}",str(exc)) else "GROUP_ANSWER_FAILED"
            if hasattr(self,'health'):self.health.mark('fetch_or_model','failed',request_id=row['mid'],error=error)
            mode="error";answer="本次未能读取最新资料或完成回答，未据此确认项目状态。请稍后重试。"
        if self.stopped() or self.clock()>=expires:self.finish(row["mid"],"expired");return True
        try:
            self.port.guard(row["chat"],row["user"])
            if hasattr(self.port,"guard_disclosure"):self.port.guard_disclosure(row["chat"],used)
        except Exception:
            self.finish(row["mid"],"rejected","GROUP_OR_MEMBER_CHANGED");return True
        answer=re.sub(r"(?i)<(?=/?at\b)","＜",answer)
        if result is not None and getattr(result, "dry_run", False):
            self.finish(row["mid"], "dry_run")
            return True
        with self.mutex,self.db:
            self.response_journal.prepare(row['mid'],
                (self.port.tenant_key, self.port.app_id, row['user'], row['chat'], row['thread']),
                'legacy_group_query', answer, expires,
                source_metadata={'selected_project': selected,
                    'used_projects': sorted(p['key'] for p in used), 'route': mode})
            self.db.execute("UPDATE ledger_inbox SET state='sending',project=? WHERE mid=?",(selected,row["mid"]))
        try:
            answer_mid=self.reply(row["mid"],answer)
            if hasattr(self,"health"):self.health.mark("delivery","ok",request_id=row["mid"])
        except Exception:
            with self.mutex,self.db:
                self.response_journal.receipt(row['mid'], unknown=True)
            if hasattr(self,"health"):self.health.mark("delivery","unknown",request_id=row["mid"],error="REPLY_OUTCOME_UNKNOWN")
            self.finish(row["mid"],"delivery_unknown","REPLY_OUTCOME_UNKNOWN");return True
        with self.mutex,self.db:
            self.response_journal.receipt(row['mid'], message_id=answer_mid)
            self.db.execute("UPDATE ledger_inbox SET answer_mid=? WHERE mid=?",(answer_mid if isinstance(answer_mid,str) else None,row["mid"]))
            self.db.execute("INSERT INTO query_conversations VALUES(?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET focus=excluded.focus,pending=excluded.pending,version=query_conversations.version+1,expires=excluded.expires",(scope,selected,json.dumps(next_pending,ensure_ascii=False),1,expires))
            if self.assistant is not None:
                self.db.execute("INSERT OR REPLACE INTO ledger_ai_events VALUES(?,?,?,?)",(row["mid"],mode,self.assistant.total_calls-calls_before,self.assistant.last_grounding))
            if mode in ("general","project","clarify") and error is None:
                self.db.execute("INSERT OR REPLACE INTO group_turns(mid,chat,user,question,answer,mode,created,expires,thread) VALUES(?,?,?,?,?,?,?,?,?)",(row["mid"],row["chat"],row["user"],row["question"],answer,mode,row["created"],expires,row["thread"]))
            if not row["thread"]:
                if selected:self.db.execute("INSERT OR REPLACE INTO ledger_focus VALUES(?,?,?,?)",(row["chat"],row["user"],selected,expires))
                else:self.db.execute("DELETE FROM ledger_focus WHERE chat=? AND user=?",(row["chat"],row["user"]))
        self.finish(row["mid"],"done",error);return True

    def write_status(self):
        from bridge import atomic_json
        with self.mutex:
            counts = dict(self.db.execute("SELECT state,COUNT(*) FROM ledger_inbox GROUP BY state").fetchall())
        atomic_json(self.root / "group-ledger-status.json", {
            "time": self.clock(), "enabled": True, "mode": "demo_members_at_only",
            "source": SOURCE, "counts": counts, "qwen_enabled": self.assistant is not None,
        })

    def close(self):
        if hasattr(self.port,'identity_pool'):self.port.identity_pool.shutdown(wait=True,cancel_futures=True)
        self.db.close()
