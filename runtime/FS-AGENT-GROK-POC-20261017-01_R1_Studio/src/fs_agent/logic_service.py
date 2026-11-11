"""Document state in the existing SQLite store. Business permissions stay server-owned."""
from __future__ import annotations
import json
import re
import time
import uuid
from pathlib import Path
from .config import BoundaryError
from .doc_logic import (read_source, digest, build_prompt, parse_json, validate_analysis,
                        semantic_prompt, validate_semantics, render, MAX_BYTES, DOCUMENT_TYPES, apply_program_findings, program_questions, mechanical_issues, normalize_limits)
from .question_state import (load_ledger, register, reconciliation_prompt, reconcile, reconcile_questions, no_answer)
from .store import scope_of
from .provider import Result

WORKFLOW_SHA256 = digest(Path(__file__).read_bytes())

SCHEMA = """
CREATE TABLE IF NOT EXISTS logic_drafts (
 id TEXT PRIMARY KEY, scope TEXT NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL,
 document_type TEXT NOT NULL, sources TEXT NOT NULL, answers TEXT NOT NULL,
 analysis TEXT, review TEXT, rules TEXT, content TEXT, content_hash TEXT, accepted_hash TEXT,
 target TEXT, expires REAL NOT NULL, stage INTEGER NOT NULL DEFAULT 0, error TEXT
);
CREATE INDEX IF NOT EXISTS logic_scope ON logic_drafts(scope,expires);
CREATE TABLE IF NOT EXISTS logic_jobs (
 task_id TEXT PRIMARY KEY, draft_id TEXT NOT NULL, revision INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS logic_receipts (
 inbox_seq INTEGER PRIMARY KEY, outbox_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS logic_writes (
 idem TEXT PRIMARY KEY, draft_id TEXT NOT NULL, revision INTEGER NOT NULL,
 target TEXT NOT NULL, content_hash TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT
);
"""
HELP = ("逻辑整理：/doc logic 8d（异常分析）、/doc logic test（测试报告）、/doc logic charter（项目章程）。"
        "命令后可接文字；长文请先选文种再发 TXT、Markdown、HTML 文件。云文档链接需先导出为上述文件。"
        "直接回复可补充同一稿；/doc 阶段稿、/doc 暂停、/doc 继续、/doc 采用、/doc 放弃。"
        "生成附件仅发本人。挂载需先选择可用目标，以实际回执为准。")

def link_only(argument):
    parts = argument.split(maxsplit=2)
    text = parts[2] if len(parts) == 3 and parts[0] == "logic" else argument
    return bool(re.fullmatch(r"https?://\S+", text.strip()))

class MissingBusiness:
    """Fail explicitly: no invented project, platform receipt, approval or remote URL."""
    def resolve(self, principal, target_id):
        raise BoundaryError("BUSINESS_CONNECTION_UNAVAILABLE")
    def sources(self, principal, target):
        raise BoundaryError("BUSINESS_CONNECTION_UNAVAILABLE")
    def commit(self, principal, target, content, content_hash, idem, guard):
        raise BoundaryError("BUSINESS_CONNECTION_UNAVAILABLE")
    def reconcile(self, principal, idem):
        raise BoundaryError("BUSINESS_CONNECTION_UNAVAILABLE")

from .precheck import PrecheckMixin

class LogicService(PrecheckMixin):
    def __init__(self, gateway, business=None):
        self.g, self.s = gateway, gateway.store
        from .meegle_port import load_meegle_port
        self.business = business or MissingBusiness()
        if business is None:
            try:
                self.business = load_meegle_port(gateway) or MissingBusiness()
                self.s.set_meta("business_error", "")
            except (BoundaryError, KeyError, TypeError, OSError, ValueError):
                self.s.set_meta("business_error", "BUSINESS_CONNECTION_UNAVAILABLE")
        with self.s.lock:
            self.s.conn.executescript(SCHEMA)
        with self.s.tx() as c:
            columns={r[1] for r in c.execute("PRAGMA table_info(logic_drafts)")}
            for name,kind in {"mode":"TEXT NOT NULL DEFAULT 'document'","analysis_basis":"TEXT","target_requirements":"TEXT"}.items():
                if name not in columns:c.execute("ALTER TABLE logic_drafts ADD COLUMN "+name+" "+kind)
        self.migrate_precheck_hashes()
        self.standards = None
        from .document_standards import DocumentStandards, load_settings
        try:
            settings = load_settings(gateway.config.root)
            if settings: self.standards = DocumentStandards(self, settings)
            self.s.set_meta('document_standards_error', '')
            self.s.set_meta('document_standards_enabled', 'true' if self.standards else 'false')
        except (BoundaryError, OSError, ValueError, KeyError):
            self.s.set_meta('document_standards_error', 'DOCUMENT_STANDARDS_UNAVAILABLE')
            self.s.set_meta('document_standards_enabled', 'false')

    def migrate_precheck_hashes(self):
        from .project_catalog import digest as basis_digest
        with self.s.tx() as c:
            if c.execute("SELECT 1 FROM meta WHERE key='R1-FIX1-precheck-hash-1'").fetchone():return
            for d in c.execute("SELECT * FROM logic_drafts WHERE mode='precheck' AND content IS NOT NULL AND content_hash IS NOT NULL").fetchall():
                if d['content_hash']==digest(d['content']):continue
                if d['content_hash']==basis_digest(d['content']):
                    # R1's byte check precedes upload. The precise legacy hash
                    # mismatch plus absence of any external receipt proves this
                    # specific attempt never reached upload; others stay unknown.
                    c.execute("UPDATE logic_writes SET state='confirmed_not_sent',receipt=? WHERE draft_id=? AND content_hash=? AND state='unknown' AND receipt IS NULL",
                              (json.dumps({'reason':'LEGACY_PREUPLOAD_HASH_MISMATCH'}),d['id'],d['content_hash']))
                c.execute("UPDATE logic_drafts SET accepted_hash=NULL,analysis_basis=NULL,revision=revision+1,state=CASE WHEN state IN ('cancelled','expired','paused') THEN state ELSE 'collecting' END,error='LEGACY_PRECHECK_HASH_INVALID' WHERE id=?",(d['id'],))
            c.execute("INSERT INTO meta(key,value) VALUES('R1-FIX1-precheck-hash-1','done')")

    def current(self, row):
        items = self.s.rows("SELECT * FROM logic_drafts WHERE scope=? AND expires>? ORDER BY rowid DESC LIMIT 1",
                            (scope_of(row), self.s.clock()))
        return items[0] if items else None

    def get(self, row, draft_id):
        items = self.s.rows("SELECT * FROM logic_drafts WHERE id=? AND scope=? AND expires>?",
                            (draft_id, scope_of(row), self.s.clock()))
        if not items or not self.g.authorized(row):
            raise BoundaryError("LOGIC_SCOPE_DENIED")
        return items[0]

    def is_standard_draft(self, draft):
        rules = json.loads(draft.get('rules') or '{}')
        if rules.get('engine', '').startswith('am-doc-standards') or rules.get('document_binding'):
            return True
        if self.s.rows("SELECT 1 FROM sqlite_master WHERE type='table' AND name='amdoc_context'"):
            # An expired context is still evidence of this draft's type. It is
            # not permission to silently switch the draft to the legacy path.
            return bool(self.s.rows('SELECT 1 FROM amdoc_context WHERE draft_id=?', (draft['id'],)))
        return False

    def require_current_document_evidence(self, row, draft):
        if self.standards is not None:
            if self.is_standard_draft(draft) and not self.standards.context(draft):
                raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_EXPIRED')
            return self.standards.require_current_evidence(row, draft)
        if self.is_standard_draft(draft):
            raise BoundaryError('DOCUMENT_STANDARDS_UNAVAILABLE')
        return True

    def observe_ingress(self, conn, row, expires):
        """Called in the durable receipt transaction. Supersede before slow work returns."""
        if not self.g.config.document_logic:
            return
        body = row.get("body") or ""
        sel = conn.execute("SELECT route FROM selection WHERE scope=? AND expires>?", (scope_of(row), self.s.clock())).fetchone()
        current = conn.execute("SELECT id FROM logic_drafts WHERE scope=? AND expires>? ORDER BY rowid DESC LIMIT 1", (scope_of(row), self.s.clock())).fetchone()
        relevant = body.startswith(("/doc logic", "/doc template", "/doc 模板")) or row.get("kind") == "file" or (
            current and sel and sel[0] == "doc" and (body.startswith("/doc ") or not body.startswith("/")))
        if body in ("sample", "/doc sample") or not relevant:
            return
        # Receipt is in the same transaction as inbox insertion. Delivery still performs
        # the original live identity/department checks, before any document work.
        seq = conn.execute("SELECT seq FROM inbox WHERE app=? AND tenant=? AND event_id=?", (row["app"], row["tenant"], row["event_id"])).fetchone()[0]
        outbox = self.s.add_outbox(dict(row, expires=expires), text="已收到，正在核验权限与资料。", private=True, conn=conn)
        conn.execute("INSERT INTO logic_receipts VALUES (?,?)", (seq, outbox))
        if link_only(body.removeprefix("/doc ")):
            return
        if body in ("/doc 采用", "/doc adopt", "/doc 挂载", "/doc mount", "/doc 对账", "/doc 保存云文档", "/doc 云文档对账", "/doc 模板帮助") or body.startswith(("/doc target ", "/doc 云端更新 ")):
            return
        state = "cancelled" if body in ("/doc 放弃", "/doc discard") else "paused" if body in ("/doc 暂停", "/doc pause") else "dirty"
        draft = conn.execute("SELECT id,state FROM logic_drafts WHERE scope=? AND expires>? AND state!='cancelled' ORDER BY rowid DESC LIMIT 1",
                             (scope_of(row), self.s.clock())).fetchone()
        if draft:
            if draft[1] == "paused" and body not in ("/doc 继续", "/doc resume", "/doc 阶段稿", "/doc stage", "/doc 放弃", "/doc discard") and not body.startswith("/doc logic"):
                return
            conn.execute("UPDATE logic_drafts SET revision=revision+1,state=?,accepted_hash=CASE WHEN ? IN ('paused','cancelled') THEN NULL ELSE accepted_hash END WHERE id=?", (state, state, draft[0]))
            conn.execute("UPDATE outbox SET state='rejected',error='DRAFT_SUPERSEDED' WHERE task_id IN "
                         "(SELECT task_id FROM logic_jobs WHERE draft_id=?) AND state='pending'", (draft[0],))

    def notify(self, row, text):
        self.s.add_outbox(row, text=text, private=True)

    def create(self, row, kind):
        draft = uuid.uuid4().hex
        with self.s.tx() as c:
            c.execute("UPDATE logic_drafts SET state='cancelled',revision=revision+1,accepted_hash=NULL WHERE scope=? AND state!='cancelled'",
                      (scope_of(row),))
            c.execute("INSERT INTO logic_drafts(id,scope,revision,state,document_type,sources,answers,expires) VALUES(?,?,1,'collecting',?,'[]','{}',?)",
                      (draft, scope_of(row), kind, row["expires"]))
        return self.get(row, draft)

    def add_source(self, row, draft, name, data, *, limitation=None):
        sources = json.loads(draft["sources"])
        if len(sources) >= 12:
            raise BoundaryError("LOGIC_SOURCE_LIMIT")
        if Path(name).suffix.lower() in (".pdf",".docx") and getattr(self.g,"am_self",None) is not None and not self.g.am_self.flags["attachments"]:
            limitation="附件解析尚未启用；资料已提供，但未读取正文"
        source = read_source(name, b"" if limitation else data, source_id=f"S{len(sources)+1:02d}",
                             origin=row.get("message_id"), formed_at=None)
        if limitation:
            source.update(read_status="failed", limitation=limitation, segments=[], version=None, version_reason="文件读取失败，未获得内容指纹")
        if source["version"] is not None and any(s["version"] == source["version"] and s["name"] == source["name"] for s in sources):
            return False
        if sum(len(p["original"]) for s in sources + [source] for p in s["segments"]) > 12000:
            remaining=max(0,12000-sum(len(p["original"]) for s in sources for p in s["segments"]))
            kept=[]
            for segment in source["segments"]:
                if remaining<=0:break
                text=segment["original"][:remaining];kept.append(dict(segment,original=text));remaining-=len(text)
            source.update(segments=kept,read_status="partial",limitation="材料已提供；累计正文超过12000字符，仅检查列出的来源范围")
        sources.append(source)
        # Persist the new source first. Merely receiving a reply changes no answer status.
        answers = json.loads(draft["answers"])
        if draft["analysis"]:
            source["reply_to"] = [q["key"] for q in json.loads(draft["analysis"])["questions"]]
        self.s.execute("UPDATE logic_writes SET target='{}',receipt=NULL WHERE draft_id IN (SELECT id FROM logic_drafts WHERE expires<=?)", (self.s.clock(),))
        self.s.execute("UPDATE logic_drafts SET sources=?,answers=?,revision=revision+1,state='collecting',"
                       "accepted_hash=NULL,expires=MIN(expires,?),error=NULL WHERE id=?",
                       (json.dumps(sources, ensure_ascii=False), json.dumps(answers, ensure_ascii=False), row["expires"], draft["id"]))
        if source["read_status"] != "complete":
            self.notify(row, "资料已记录：" + (source["limitation"] or "部分读取"))
        return True

    def queue(self, row, draft):
        receipts = self.s.rows("SELECT outbox_id FROM logic_receipts WHERE inbox_seq=?", (row["seq"],))
        task_id = self.s.enqueue(row, "doc", acknowledgement=not bool(receipts))
        self.s.execute("INSERT OR IGNORE INTO logic_jobs VALUES(?,?,?)", (task_id, draft["id"], draft["revision"]))
        if receipts:
            self.s.execute("UPDATE outbox SET task_id=? WHERE id=?", (task_id, receipts[0]["outbox_id"]))
        else:
            self.s.execute("UPDATE outbox SET body=? WHERE task_id=? AND kind='text' AND state='pending'",
                           ("已收到，正在整理；完成后交付候选稿、来源和待确认项。", task_id))
        return task_id

    def handle(self, row, argument, *, file=False):
        draft = self.current(row)
        parts = argument.split(maxsplit=2)
        if not file and argument=='模板帮助':
            if not self.standards:raise BoundaryError('DOCUMENT_STANDARDS_UNAVAILABLE')
            self.standards.authorize(row)
            self.notify(row,'发送 /doc 模板 章程 项目编号 IRAD L1 开始。文种还可选“变更申请”或“结项报告”；请按项目实际类型与级别填写。生成后可补资料、采用文字、保存云文档；挂载需另选匹配的交付物。');return
        if not file and argument.startswith(('template ','模板 ')):
            if not self.standards: raise BoundaryError('DOCUMENT_STANDARDS_UNAVAILABLE')
            self.standards.begin(row, argument); return
        if not file and argument == '正式交付目标':
            if not self.standards or not draft: raise BoundaryError('DOCUMENT_STANDARD_RESULT_REQUIRED')
            result = self.standards.delivery_status(row, draft)
            self.notify(row, result['message']); return
        if not file and (argument in ('保存云文档', '云文档对账') or argument.startswith('云端更新 ')):
            if not self.standards or not draft: raise BoundaryError('DOCUMENT_STANDARD_RESULT_REQUIRED')
            self.notify(row, self.standards.native_action(row, draft, argument)); return
        if not file and link_only(argument):
            self.notify(row, "收到链接，但当前机器人不能直接读取网页正文。请发送文档的 TXT、Markdown、HTML、DOCX 或文字层 PDF 导出文件，或粘贴正文；尚未对链接内容作分析。")
            return
        if parts and parts[0] == "logic":
            kind = parts[1] if len(parts) > 1 else "8d"
            if kind not in DOCUMENT_TYPES:
                self.notify(row, HELP); return
            draft = self.create(row, kind)
            if len(parts) < 3:
                self.notify(row, "已开始" + DOCUMENT_TYPES[kind] + "整理，请发送文字或 TXT、Markdown、HTML、DOCX、文字层 PDF 文件；云文档链接需先导出。")
                return
            argument = parts[2]
        elif draft is None:
            self.notify(row, HELP); return
        elif argument in ("放弃", "discard"):
            self.cancel(row, draft["id"]); return
        elif argument in ("暂停", "pause"):
            self.s.execute("UPDATE logic_drafts SET state='paused',revision=revision+1,accepted_hash=NULL WHERE id=?", (draft["id"],))
            self.notify(row, "已暂停本稿，已有资料保留到原有效期。"); return
        elif argument in ("采用", "adopt"):
            self.adopt(row, draft["id"]); self.notify(row, "已记录采用当前文字；尚未挂载，不代表专业审核或节点完成。"); return
        elif argument.startswith("target "):
            self.select_target(row, draft["id"], argument.split(maxsplit=1)[1])
            current = self.get(row, draft["id"])
            if current["state"] == "collecting" and current["sources"] != "[]":
                self.queue(row, current)
            self.notify(row, "已选择目标，保存时仍会核对修订。"); return
        elif argument == "对账":
            self.notify(row, self.reconcile_draft(row, draft["id"])); return
        elif argument in ("挂载", "mount"):
            result = self.mount(row, draft["id"])
            message = result["message"]
            if result.get("receipt"):
                message += "\n查看已挂载的交付物：" + result["receipt"]["url"]
            self.notify(row, message); return
        elif argument in ("继续", "resume", "阶段稿", "stage"):
            if draft["state"] == "cancelled":
                raise BoundaryError("LOGIC_DRAFT_CANCELLED")
            stage = argument in ("阶段稿", "stage")
            if stage and draft["stage"] and draft["content"] and not self.is_standard_draft(draft):
                self.s.execute("UPDATE logic_drafts SET state='ready' WHERE id=?", (draft["id"],))
                self.notify(row, "已有阶段稿仍有效；没有新资料，不重复生成。")
                return
            self.s.execute("UPDATE logic_drafts SET state='collecting',stage=?,revision=revision+1,accepted_hash=NULL WHERE id=?",
                           (int(stage), draft["id"]))
            self.queue(row, self.get(row, draft["id"])); return
        elif draft["state"] == "cancelled":
            self.notify(row, "本稿已放弃；" + HELP); return
        elif draft["state"] == "paused":
            self.notify(row, "本稿已暂停；用 /doc 继续 恢复。"); return
        if file:
            info = json.loads(row["body"])
            try:
                data = self.g.api.read_file(row, info["file_key"], lambda: self.g.authorized(row), limit=8*1024*1024)
                changed = self.add_source(row, draft, info["file_name"], data)
            except BoundaryError as exc:
                # Persist the specific unread source, so failure never becomes "not provided".
                self.add_source(row, draft, info["file_name"], b"", limitation="已提供文件，读取暂失败：" + str(exc))
                self.notify(row, "文件暂不可读，已保留本稿；可重发这一份文件或先形成阶段稿。")
                return
        else:
            if not argument.strip():
                self.notify(row, HELP); return
            changed = self.add_source(row, draft, "补充.txt" if draft["sources"] != "[]" else "输入.txt", argument.encode())
        if not changed:
            if draft["content"]:
                self.s.execute("UPDATE logic_drafts SET state='ready' WHERE id=?", (draft["id"],))
                self.notify(row, "这份内容已记录，继续使用已有候选稿；不重复生成或追问。")
            else:
                self.queue(row, self.get(row, draft["id"]))
            return
        self.queue(row, self.get(row, draft["id"]))

    def generate(self, task, started):
        entry_monotonic = time.monotonic()
        jobs = self.s.rows("SELECT * FROM logic_jobs WHERE task_id=?", (task["id"],))
        if not jobs:
            return None
        job = jobs[0]; d = self.get(task, job["draft_id"])
        if d["revision"] != job["revision"] or d["state"] in ("cancelled", "paused"):
            raise BoundaryError("LATE_RESULT_REJECTED")
        sources, answers = json.loads(d["sources"]), json.loads(d["answers"])
        if d["target"]:
            target = json.loads(d["target"])
            # Adapter contract: authenticate and filter the exact project BEFORE fetching.
            current = self.business.resolve(task, target["id"])
            if current != target:
                raise BoundaryError("TARGET_REVISION_CHANGED")
            if self.standards: self.standards.validate_target(task, d, current)
            for source in self.business.sources(task, target):
                if source["provenance"] == "current_project" and source["id"] not in {s["id"] for s in sources}:
                    sources.append(source)
        if d["mode"]=="precheck":return self.run_precheck(task,d,sources,job)
        if self.standards and self.standards.context(d):
            deadline = entry_monotonic + min(120, task['expires']-self.s.clock(), d['expires']-self.s.clock())
            return self.standards.prepare(task, d, job, deadline, started, sources)
        if self.is_standard_draft(d):
            raise BoundaryError('DOCUMENT_SHARED_EVIDENCE_EXPIRED' if self.standards
                                else 'DOCUMENT_STANDARDS_UNAVAILABLE')
        if not any(s["segments"] for s in sources):
            raise BoundaryError("LOGIC_NO_READABLE_SOURCE")
        expires = min(task["expires"], d["expires"])
        deadline = time.monotonic() + min(120, expires - self.s.clock())
        model_elapsed = []
        def call(prompt):
            fresh = self.get(task, d["id"])
            if fresh["revision"] != job["revision"] or fresh["state"] in ("cancelled", "paused"):
                raise BoundaryError("LATE_RESULT_REJECTED")
            remaining = deadline - time.monotonic()
            if remaining <= 0: raise BoundaryError("EXECUTION_TIMEOUT")
            response = self.g.provider.complete(prompt, run_id=task["run_id"], timeout=remaining, on_start=started)
            model_elapsed.append(response.elapsed)
            return response
        ledger = load_ledger(answers)
        for question in program_questions(sources):
            register(ledger, question, sources)
        previous_meta = json.loads(d["rules"]) if d["rules"] else {}
        seen = set(previous_meta.get("processed_source_ids", []))
        new_ids = {s["id"] for s in sources} - seen
        updates = {"updates": []}
        new_text = "\n".join(p["original"] for s in sources if s["id"] in new_ids for p in s["segments"])
        if ledger and new_ids and (d["analysis"] or len(sources) > 1) and not no_answer(new_text):
            response = call(reconciliation_prompt(sources, ledger))
            updates = parse_json(response.text)
            ledger = reconcile(updates, ledger, sources, new_ids)
        prompt, meta = build_prompt(sources, d["document_type"], bool(d["stage"]), ledger)
        meta["workflow_sha256"] = WORKFLOW_SHA256
        meta["model"] = {"name": self.g.config.api_model, "digest": self.g.config.api_model_digest}
        meta["processed_source_ids"] = sorted(s["id"] for s in sources)
        meta["answer_updates"] = updates
        # A bounded repair is allowed within the same original 120-second deadline.
        # Neither program checks nor model review silently rewrite engineering facts.
        generation_prompt = prompt
        for attempt in range(2):
            result = call(generation_prompt)
            try:
                analysis = validate_analysis(parse_json(result.text), sources, stage=bool(d["stage"]),
                                             document_type=d["document_type"])
            except BoundaryError as exc:
                if attempt or str(exc) not in {"LOGIC_RELATION_INVALID", "LOGIC_SCHEMA_INVALID", "LOGIC_JSON_INVALID", "LOGIC_REFERENCE_INVALID", "LOGIC_QUESTION_SCOPE"}:
                    raise
                generation_prompt = prompt + "\n修正上次结构或引用错误：" + str(exc) + "。原句逐字引用对应ref。\n" + result.text
                continue
            candidate_ledger = load_ledger(ledger)
            analysis = reconcile_questions(analysis, candidate_ledger, sources, stage=bool(d["stage"]))
            analysis = apply_program_findings(analysis, sources, candidate_ledger)
            analysis, program_repairs = normalize_limits(analysis, sources)
            meta["program_repairs"] = program_repairs
            issues = mechanical_issues(analysis, sources)
            review = {"ok": False, "issues": issues}
            semantic = validate_semantics(call(semantic_prompt(sources, analysis, candidate_ledger,
                                                           document_type=d["document_type"])).text)
            review = {"ok": not issues and semantic["ok"], "issues": issues + semantic["issues"]}
            if review["ok"]:
                ledger = candidate_ledger
                break
            if attempt:
                self.s.execute("UPDATE logic_drafts SET review=?,error='LOGIC_SEMANTIC_REVIEW' WHERE id=? AND revision=?",
                               (json.dumps(review, ensure_ascii=False), d["id"], job["revision"]))
                raise BoundaryError("LOGIC_SEMANTIC_REVIEW")
            generation_prompt = prompt + "\n下列校核指出本稿存在具体错误。依据来源修正，保留有效事实，不添加新事实。\n" + json.dumps(
                {"review": review, "candidate": analysis}, ensure_ascii=False)
        meta["model_calls"] = len(model_elapsed)
        content = render(sources, analysis, review, meta, document_type=d["document_type"],
                         stage=bool(d["stage"]), business="候选稿已生成；尚未采用／挂载；专业批准状态未从业务系统确认。")
        with self.s.tx() as c:
            changed = c.execute("UPDATE logic_drafts SET state='ready',analysis=?,review=?,rules=?,content=?,content_hash=?,answers=?,error=NULL "
                                "WHERE id=? AND revision=? AND state NOT IN ('cancelled','paused') AND expires>?",
                                (json.dumps(analysis, ensure_ascii=False), json.dumps(review, ensure_ascii=False),
                                 json.dumps(meta), content, digest(content), json.dumps(ledger, ensure_ascii=False), d["id"], job["revision"], self.s.clock())).rowcount
            if not changed: raise BoundaryError("LATE_RESULT_REJECTED")
        text = ("阶段稿已形成；未知项与限制保留。" if d["stage"] else "候选稿已形成，尚未采用或挂载。") + "\n\n" + "\n".join(q["ask"] for q in analysis["questions"][:2])
        text += "\n\n可直接回复补充，或 /doc 阶段稿。附件含原句、证据和关键修改。业务状态：尚未采用／挂载。"
        return content, Result(text, result.model, sum(model_elapsed)), expires

    def valid_job(self, task):
        rows = self.s.rows("SELECT d.*,j.revision AS expected FROM logic_jobs j "
                           "JOIN logic_drafts d ON d.id=j.draft_id WHERE j.task_id=?", (task["id"],))
        if not rows:
            return True
        draft = rows[0]
        if (draft['revision'] != draft['expected'] or draft['state'] != 'ready'
                or draft['expires'] <= self.s.clock()):
            return False
        if self.is_standard_draft(draft):
            # Gateway delivery may supply only task_id. Bind to the real task
            # actor and TTL, never to metadata supplied by the caller/model.
            tasks = self.s.rows('SELECT * FROM tasks WHERE id=?', (task['id'],))
            if not tasks:
                return False
            try:
                self.require_current_document_evidence(tasks[0], draft)
            except BoundaryError:
                return False
        return True

    def adopt(self, row, draft_id):
        d = self.get(row, draft_id)
        self.validate_precheck_basis(row,d)
        if d["state"] != "ready" or not d["content_hash"]: raise BoundaryError("LOGIC_NO_CURRENT_DRAFT")
        if self.standards and self.is_standard_draft(d): self.standards.authorize(row)
        self.require_current_document_evidence(row, d)
        self.s.execute("UPDATE logic_drafts SET accepted_hash=content_hash WHERE id=? AND revision=?", (draft_id, d["revision"]))

    def select_target(self, row, draft_id, target_id):
        d = self.get(row, draft_id)
        if d["state"] == "cancelled":
            raise BoundaryError("LOGIC_DRAFT_CANCELLED")
        target = self.business.resolve(row, target_id)
        if self.standards:
            if self.is_standard_draft(d): self.standards.authorize(row)
            self.standards.validate_target(row, d, target)
        if not all(target.get(k) for k in ("id", "project_id", "node_id", "deliverable_id", "revision")):
            raise BoundaryError("TARGET_INCOMPLETE")
        encoded = json.dumps(target, sort_keys=True)
        if d["target"] != encoded:
            state = "collecting" if (d["target"] or d["mode"]=="precheck") and d["state"] != "paused" else d["state"]
            self.s.execute("UPDATE logic_drafts SET target=?,accepted_hash=NULL,revision=revision+1,state=? WHERE id=?",
                           (encoded, state, d["id"]))

    def mount(self, row, draft_id):
        d = self.get(row, draft_id)
        if not d["target"]: raise BoundaryError("BUSINESS_TARGET_MISSING")
        if d["state"] != "ready" or d["accepted_hash"] != d["content_hash"]:
            raise BoundaryError("CONTENT_ADOPTION_REQUIRED")
        target = json.loads(d["target"])
        if self.standards:
            if self.is_standard_draft(d): self.standards.authorize(row)
            self.standards.validate_target(row, d, target)
        idem = digest(json.dumps([d["id"], d["revision"], target, d["content_hash"]], sort_keys=True))
        old = self.s.rows("SELECT * FROM logic_writes WHERE idem=?", (idem,))
        if old:
            receipt = json.loads(old[0]["receipt"]) if old[0]["receipt"] else None
            if old[0]['state']=='confirmed_not_sent':
                return {'message':'本次挂载在上传前确定失败，未上传或挂载；请修正条件后重新生成并采用。','receipt':None}
            if old[0]["state"] == "committed":
                current_receipt = self.business.reconcile(row, idem)
                if current_receipt != receipt:
                    return {"message": "原挂载回执已存在，但当前读回尚未确认；未重复写入。", "receipt": None}
                return {"message": "已挂载并重新读回，复用原回执；未改变专业审核状态。", "receipt": receipt}
            # Never submit again after an ambiguous dispatch, even if reconcile is unavailable.
            receipt = self.business.reconcile(row, idem)
            if receipt:
                self._receipt(idem, receipt, target, d["content_hash"])
                return {"message": "对账确认已挂载；未重复写入。", "receipt": receipt}
            return {"message": "挂载结果待确认；保留候选稿，未再次写入。", "receipt": None}
        self.validate_precheck_basis(row,d)
        self.require_current_document_evidence(row, d)
        if self.business.resolve(row, target["id"]) != target:
            raise BoundaryError("TARGET_REVISION_CHANGED")
        fresh = self.get(row, draft_id)
        if fresh["revision"] != d["revision"] or fresh["state"] != "ready":
            raise BoundaryError("LATE_RESULT_REJECTED")
        # Journal BEFORE dispatch; the port must enforce revision and idempotency atomically.
        with self.s.tx() as conn:
            from am_harness.reservation import reserve_effect
            reserve_effect(conn,idem,'document_mount',{'revision':d['revision'],'target':target,
                           'accepted_hash':d['accepted_hash'],'content_hash':d['content_hash']})
            conn.execute("INSERT INTO logic_writes VALUES(?,?,?,?,?,'unknown',NULL)",
                         (idem,d['id'],d['revision'],json.dumps(target),d['content_hash']))
        from .meegle_port import BusinessNotStarted
        try:
            def guard():
                fresh = self.get(row, draft_id)
                self.require_current_document_evidence(row, fresh)
                return fresh["revision"] == d["revision"] and fresh["state"] == "ready" and fresh["accepted_hash"] == d["content_hash"]
            receipt = self.business.commit(row, target, d["content"], d["content_hash"], idem, guard)
        except BusinessNotStarted as exc:
            self.s.execute("UPDATE logic_writes SET state='confirmed_not_sent',receipt=? WHERE idem=? AND state='unknown'",
                           (json.dumps({'reason':str(exc)}),idem))
            return {'message':'本次挂载在上传前确定失败，未上传或挂载：'+str(exc),'receipt':None}
        except Exception:
            return {"message": "挂载结果待确认；先对账，未盲目重试。", "receipt": None}
        verified = self.business.reconcile(row, idem)
        if verified != receipt:
            return {"message": "已收到提交回执，但平台读回尚未一致，结果待确认。", "receipt": None}
        self._receipt(idem, receipt, target, d["content_hash"])
        cancelled = self.get(row, draft_id)["state"] == "cancelled"
        return {"message": ("已提交后收到撤销，保留真实挂载结果并停止后续动作；未删除历史。"
                            if cancelled else "已挂载并取得回执；不代表专业审核通过。"), "receipt": receipt}

    def reconcile_draft(self, row, draft_id):
        self.get(row, draft_id)
        writes = self.s.rows("SELECT * FROM logic_writes WHERE draft_id=?", (draft_id,))
        if not writes:
            return "本稿没有已发起的业务挂载。"
        unknown = False
        not_sent = False
        for write in writes:
            if write['state']=='confirmed_not_sent':
                not_sent=True;continue
            if write["state"] == "committed":
                continue
            receipt = self.business.reconcile(row, write["idem"])
            if receipt:
                self._receipt(write["idem"], receipt, json.loads(write["target"]), write["content_hash"])
            else:
                unknown = True
        return "仍有挂载结果待确认；未再次写入。" if unknown else "存在上传前确定失败的尝试，未上传或挂载；其余已确认结果保留。" if not_sent else "已对账确认原挂载结果；未删除历史或改变审核状态。"

    def _receipt(self, idem, receipt, target, content_hash):
        if (not isinstance(receipt, dict) or receipt.get("target_id") != target["id"]
                or receipt.get("content_hash") != content_hash or not receipt.get("object_id")
                or not receipt.get("url")):
            raise BoundaryError("MOUNT_RECEIPT_UNVERIFIED")
        self.s.execute("UPDATE logic_writes SET state='committed',receipt=? WHERE idem=?",
                       (json.dumps(receipt), idem))

    def cancel(self, row, draft_id):
        self.get(row, draft_id)
        self.s.execute("UPDATE logic_drafts SET state='cancelled',revision=revision+1,accepted_hash=NULL WHERE id=?", (draft_id,))
        states = self.s.rows("SELECT state FROM logic_writes WHERE draft_id=?", (draft_id,))
        self.notify(row, "已放弃本稿的后续动作。"
                    + ("曾提交挂载；实际结果保留，未知结果仍需对账，不删除历史。" if states else "尚未发起业务挂载。"))

    def cleanup(self):
        from .document_standards import DocumentStandards
        DocumentStandards.cleanup_store(self.s)
        self.s.execute("DELETE FROM logic_receipts WHERE inbox_seq IN (SELECT seq FROM inbox WHERE expires<=?)", (self.s.clock(),))
        self.s.execute("UPDATE logic_drafts SET sources='[]',answers='{}',analysis=NULL,review=NULL,rules=NULL,content=NULL,"
                       "content_hash=NULL,accepted_hash=NULL,target=NULL,analysis_basis=NULL,target_requirements=NULL,state='expired' WHERE expires<=?", (self.s.clock(),))
