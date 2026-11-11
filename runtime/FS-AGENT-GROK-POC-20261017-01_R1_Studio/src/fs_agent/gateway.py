from __future__ import annotations

import json
import os
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .config import BoundaryError, Config
from .feishu import SendRejected, SendUnknown
from .message_text import normalize_message
from .provider import group_alive, process_identity, stop_group
from .qwen_provider import (api_request_unknown, build_doc_prompt, discard_payload,
                            synthetic_progress_result)
from .store import CODE_PROGRESS_MODEL, Store
from .logic_service import LogicService
from .reminder import ReminderService

FIXTURES = {
    "doc": "合成资料：项目 Aurora 测试版；目标为验证离线文档流程；阶段一已完成接口草稿，阶段二正在补齐测试；无真实人员与业务信息。请整理为简洁中文摘要。",
    "progress": "合成项目 Aurora：总计 5 项，已完成 3 项，进行中 2 项，阻塞 0 项。请用中文说明当前进度，不编造信息。",
}


def menu_card(nonce):
    return {"config": {"wide_screen_mode": True},
            "header": {"title": {"tag": "plain_text", "content": "合成测试入口"}},
            "elements": [{"tag": "div", "text": {"tag": "plain_text", "content": "文档：/doc logic 8d（异常分析）、test（测试报告）或 charter（项目章程）；长文选择后发文件。/chat 返回普通聊天。"}},
                         {"tag": "action", "actions": [
                             {"tag": "button", "text": {"tag": "plain_text", "content": label},
                              "type": "primary", "value": {"nonce": nonce, "route": route}}
                             for route, label in (("doc", "文档整理"), ("progress", "合成进度"))]}]}


class Gateway:
    def __init__(self, config: Config, store: Store, api, provider):
        self.config, self.store, self.api, self.provider = config, store, api, provider
        self.pool = ThreadPoolExecutor(max_workers=config.concurrency, thread_name_prefix="model")
        self.stopping = threading.Event()
        self.threads = []
        self.logic = LogicService(self)
        self.reminders = ReminderService(self)
        self.delivery_pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix='am-delivery')
        self.verification_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='am-verify')
        self.delivery_futures = {}
        self.verification_futures = {}
        with self.store.lock:
            self.store.conn.executescript('''
            CREATE TABLE IF NOT EXISTS am_view_verifications(outbox_id TEXT PRIMARY KEY,interaction_id TEXT NOT NULL,version INTEGER NOT NULL,message_id TEXT NOT NULL,state TEXT NOT NULL,finished REAL);
            UPDATE am_view_verifications SET state='pending' WHERE state='verifying';
            ''')

    def ingest(self, payload: dict, *, on_insert=None) -> bool:
        """Only normalize, hard-allowlist and commit. No network/model work in the callback."""
        try:
            header, event = payload["header"], payload["event"]
            if header["app_id"] != self.config.app_id or header["tenant_key"] != self.config.tenant_key:
                return False
            row = {"app": header["app_id"], "tenant": header["tenant_key"], "event_id": header["event_id"]}
            if header["event_type"] == "im.message.receive_v1":
                sender, message = event["sender"], event["message"]
                if sender["sender_type"] != "user" or sender.get("tenant_key") != self.config.tenant_key:
                    return False
                user = sender["sender_id"]["open_id"]
                if user not in self.config.users or message["message_type"] not in ("text", "post", "file"):
                    return False
                chat, chat_type = message["chat_id"], message["chat_type"]
                if chat_type not in ("p2p", "group") or (chat_type == "group" and chat not in self.config.chats):
                    return False
                row.update(user=user, chat=chat, chat_type=chat_type,
                           message_id=message["message_id"], source_time=int(message["create_time"]) / 1000)
                if message["message_type"] == "file":
                    if not self.config.document_logic or chat_type != "p2p" or self.store.selection(row)["route"] != "doc" or not self.logic.current(row):
                        return False
                    value = json.loads(message["content"])
                    if (set(value) - {"file_key", "file_name", "file_size"}
                            or not isinstance(value.get("file_key"), str) or len(value["file_key"]) > 200
                            or not re.fullmatch(r"[A-Za-z0-9_-]+", value["file_key"])
                            or not isinstance(value.get("file_name"), str) or len(value["file_name"]) > 160):
                        return False
                    row.update(kind="file", body=json.dumps(value, ensure_ascii=False))
                else:
                    normalized = normalize_message(message, bot_open_id=self.config.bot_open_id, max_chars=2000)
                    if chat_type == "group" and not normalized.bot_mentioned:
                        return False
                    row.update(kind="message", body=normalized.text.strip())
            elif header["event_type"] == "card.action.trigger":
                user = event["operator"]["open_id"]
                value, context = event["action"]["value"], event["context"]
                if user not in self.config.users or value.get("route") not in FIXTURES:
                    return False
                menus = self.store.rows("SELECT o.chat_type FROM outbox o WHERE o.nonce=? AND o.user=? AND o.chat=? AND o.state='sent'",
                                        (value["nonce"], user, context["open_chat_id"]))
                if not menus:
                    return False
                row.update(user=user, chat=context["open_chat_id"], chat_type=menus[0]["chat_type"],
                           kind="card", body=value["route"], nonce=value["nonce"],
                           source_message=context["open_message_id"], source_time=self.store.clock())
            else:
                return False
        except (KeyError, ValueError, TypeError):
            return False
        def inserted(conn, event, expires):
            self.logic.observe_ingress(conn, event, expires)
            if on_insert is not None:
                on_insert(conn, event, expires)
        return self.store.put(row, on_insert=inserted)

    def authorized(self, row) -> bool:
        # Local invariants still hold with an injected/test transport.
        c = self.config
        origin_type = row.get("origin_chat_type", row["chat_type"])
        return (row["expires"] > self.store.clock() and row["app"] == c.app_id
                and row["tenant"] == c.tenant_key and row["user"] in c.users
                and (origin_type == "p2p" or origin_type == "group" and row["chat"] in c.chats)
                and self.api.authorized(row))

    def receive_one(self):
        rows = self.store.pending_inbox()
        if not rows:
            return False
        row = rows[0]
        if not self.authorized(row):
            self.store.execute("UPDATE inbox SET state='denied',body=NULL WHERE seq=?", (row["seq"],))
            return True
        if self.store.clock() >= row["received"] + 600:
            self.store.execute("UPDATE inbox SET state='queue_timeout',body=NULL WHERE seq=?", (row["seq"],))
            return True
        text = row["body"] or ""
        if row["kind"] in ("reminder", "reminder_action"):
            try:
                self.reminders.handle(row)
            except BoundaryError as exc:
                self.reminders.notify(row, "提醒操作未完成："+str(exc)+"；没有因此向群发送。")
        elif row["kind"] == "file":
            try:
                self.logic.handle(row, "", file=True)
            except BoundaryError as exc:
                self.logic.notify(row, "文件整理未完成：" + str(exc) + "；已完成资料保留。")
        elif text == "/chat" and row["kind"] == "message":
            self.store.select(row, None)
            self.store.add_outbox(row, text="已返回普通聊天。发送 /doc sample 或 /progress sample 可重新进入合成测试。")
        elif text in ("/help", "/status") and row["kind"] == "message":
            self.store.add_outbox(row, text="文档整理：/doc logic 8d、/doc logic test 或 /doc logic charter，随后发文字或文件；/doc 阶段稿 可保留未知。原合成测试：/doc sample、/progress sample。/menu 切换；/chat 返回普通聊天。")
        elif row["kind"] == "card":
            if self.store.valid_card(row):
                self.store.select(row, text)
                self.store.add_outbox(row, text=f"已选择 /{text}，发送 sample 运行合成样例。")
        elif text.startswith("/retry ") or text.startswith("/resend "):
            operation, task_id = text.split(maxsplit=1)
            ok = (self.store.retry if operation == "/retry" else self.store.resend)(row, task_id)
            self.store.add_outbox(row, text="已记录操作。" if ok else "当前不能执行该操作；任务可能已过期、超出排队期限，或发送回执仍未知。")
        else:
            parts = text.split(maxsplit=1)
            route = self.store.selection(row)["route"]
            argument = text
            if parts and parts[0] in ("/doc", "/progress"):
                route = parts[0][1:]
                self.store.select(row, route)
                argument = parts[1] if len(parts) == 2 else ""
            if text == "/menu" or not text or not route:
                nonce = self.store.menu(row)
                self.store.add_outbox(row, card=menu_card(nonce), nonce=nonce)
            elif route == "doc" and argument != "sample" and (argument.startswith(("logic", "template ", "模板")) or self.logic.current(row)):
                try:
                    if not self.config.document_logic:
                        raise BoundaryError("DOCUMENT_LOGIC_NOT_ENABLED")
                    self.logic.handle(row, argument)
                except BoundaryError as exc:
                    self.logic.notify(row, "本次操作未完成：" + str(exc) + "；已有材料保留。")
            elif argument == "sample":
                self.store.enqueue(row, route)
            else:
                self.store.add_outbox(row, text=f"当前为 /{route} 合成测试。发送 sample，或用 /menu 切换。")
        self.store.execute("UPDATE inbox SET state='handled',body=NULL WHERE seq=?", (row["seq"],))
        return True

    def run_task(self, task):
        try:
            if not self.authorized(task):
                raise BoundaryError("EXECUTION_AUTH_DENIED")
            if self.store.clock() >= task["queue_deadline"]:
                raise BoundaryError("QUEUE_TIMEOUT")
            if self.reminders.generate(task, started=lambda pgid,identity: self.store.execute(
                    "UPDATE tasks SET pgid=?,process_identity=? WHERE id=? AND run_id=? AND state='running'",
                    (pgid,identity,task["id"],task["run_id"]))):
                return
            is_logic = bool(self.store.rows("SELECT 1 FROM logic_jobs WHERE task_id=?", (task["id"],)))
            history = [] if is_logic else self.store.history(task)
            # Bound context without copying old items into a new timestamp.
            history = history[-8:]
            while history and sum(len(r["body"]) for r in history) > 20000:
                history.pop(0)
            expires = min([task["expires"]] + [r["expires"] for r in history])
            prompt = FIXTURES[task["route"]]
            def started(pgid, identity):
                self.store.execute("UPDATE tasks SET pgid=?,process_identity=? WHERE id=? AND run_id=? AND state='running'",
                                   (pgid, identity, task["id"], task["run_id"]))
            logic_result = self.logic.generate(task, started)
            if logic_result:
                content, result, expires = logic_result
            elif task["route"] == "progress":
                # These fixed counts require no language inference. Keep the
                # same task, permission, expiry and delivery contracts.
                result = synthetic_progress_result()
            else:
                full_prompt = build_doc_prompt(prompt, history)
                result = self.provider.complete(full_prompt, run_id=task["run_id"],
                                                timeout=min(120, expires - self.store.clock()), on_start=started)
            if expires <= self.store.clock():
                raise BoundaryError("CONTENT_EXPIRED")
            artifact = None
            if task["route"] == "doc":
                path = self.config.root / "artifacts" / f"{task['id']}.md"
                if path.is_symlink():
                    raise BoundaryError("ARTIFACT_PATH_INVALID")
                # Model text is only content inside a fixed file, never a path or template instruction.
                path.write_text(content if logic_result else f"# 合成文档\n\n任务编号：{task['id']}\n\n用途：PoC 合成测试\n\n## 整理结果\n\n{result.text}\n")
                path.chmod(0o600)
                artifact = str(path)
            if not self.store.complete(task, prompt, result, expires=expires, artifact=artifact, guard=lambda: self.logic.valid_job(task), label="文档整理" if logic_result else "合成任务", record_context=not is_logic, presenter=getattr(getattr(self,"am_self",None),"document_completion",None) if logic_result else None):
                if artifact:
                    Path(artifact).unlink(missing_ok=True)
                raise BoundaryError("LATE_RESULT_REJECTED")
        except BoundaryError as exc:
            error = str(exc)
            state = "stop_unknown" if error == "STOP_UNKNOWN" else "timed_out" if error == "EXECUTION_TIMEOUT" else "failed"
            reminder_failure = self.reminders.failed_job(task,error)
            self.store.finish(task, state, error)
            if error in ("AUTH_FAILED", "QUOTA_EXHAUSTED", "RATE_LIMITED", "LOCAL_API_UNAVAILABLE",
                         "LOCAL_MODEL_CHANGED", "LOCAL_PROVIDER_PIN_CHANGED", "STOP_UNKNOWN"):
                self.store.set_meta("provider_pause", error)
                owner = dict(task, user=self.config.owner, chat_type="p2p")
                self.store.add_outbox(owner, text=f"测试入口的新推理已暂停：{error}。已有结果仍可交付。")
            if reminder_failure:
                return
            if self.store.rows("SELECT 1 FROM logic_jobs WHERE task_id=?", (task["id"],)):
                reasons = {"LOGIC_SEMANTIC_REVIEW":"候选意见仍有事实或证据问题，需要修正",
                           "LOGIC_NEW_NUMBER":"候选意见出现了原资料之外的数值",
                           "LOGIC_REFERENCE_INVALID":"候选引用未能与原句准确对应",
                           "LATE_RESULT_REJECTED":"新输入或撤销已生效，旧结果已停止交付",
                           "LOGIC_JSON_INVALID":"模型返回内容不完整",
                           "MODEL_OUTPUT_LIMIT":"本次整理内容超出模型单次输出范围"}
                detail = reasons.get(error, "本次读取或整理暂未完成")
                self.store.add_outbox(task, text=detail + "。已有资料保留，可补充后继续或先形成阶段稿。", private=True)
            else:
                self.store.add_outbox(task, text=f"任务 {task['id']} 未完成：{error}。")
        except Exception:
            self.reminders.failed_job(task,"INTERNAL_ERROR")
            self.store.finish(task, "failed", "INTERNAL_ERROR")

    def schedule_one(self):
        if self.stopping.is_set() or self.store.meta("provider_pause"):
            return False
        task = self.store.claim(self.config.concurrency)
        if not task:
            return False
        self.pool.submit(self.run_task, task)
        return True

    def deliver_one(self,row=None):
        from . import request_timing as timing
        rows = [row] if row else self.store.rows("SELECT * FROM outbox WHERE state='pending' AND expires>? ORDER BY created,rowid LIMIT 1", (self.store.clock(),))
        if not rows:
            return False
        row = rows[0]
        from . import request_timing as timing
        event=timing.origin(self.store,row['id']) if getattr(self,'am_self',None) else None
        token=timing.set_context(self.store,event)
        try:return self._deliver_row(row)
        finally:timing.reset_context(token)

    def _deliver_row(self,row):
        from . import request_timing as timing
        am = getattr(self,"am_self",None)
        event=timing.origin(self.store,row['id']) if am is not None else None
        if am is not None:timing.add(self.store,event,'outbox_wait',self.store.clock()-row['created'])
        if am is not None:timing.anchor(self.store,event,'delivery_start')
        def delivery_authorized():
            if am is None:return self.authorized(row)
            with timing.measure(self.store,event,'authorization_delivery'):
                special=am.authorized_notification(row)
                return self.authorized(row) if special is None else special
        if am is not None and am.valid_notification(row) is False:
            self.store.execute("UPDATE outbox SET state='rejected',error='AM_CARD_EXPIRED' WHERE id=?",(row["id"],));return True
        reminder_delivery = self.reminders.valid_notification(row)
        if reminder_delivery is False:
            self.store.execute("UPDATE outbox SET state='rejected',error='REMINDER_SUPERSEDED' WHERE id=?", (row["id"],))
            return True
        if row["nonce"]:
            menus = self.store.rows("SELECT m.version,s.version AS current FROM menus m JOIN selection s ON s.scope=m.scope WHERE m.nonce=?", (row["nonce"],))
            if not menus or menus[0]["version"] != menus[0]["current"]:
                self.store.execute("UPDATE outbox SET state='rejected',error='STALE_CARD' WHERE id=?", (row["id"],))
                return True
        if not delivery_authorized():
            self.store.execute("UPDATE outbox SET state='rejected',error='DELIVERY_AUTH_OR_TTL' WHERE id=?", (row["id"],))
            return True
        harness=getattr(am,'harness',None) if am is not None else None
        with self.store.tx() as c:
            if harness is not None and harness.durable and not harness.effects.before_dispatch(c,row['id']):
                return True
            if not c.execute("UPDATE outbox SET state='sending' WHERE id=? AND state='pending'", (row["id"],)).rowcount:
                return True
        try:
            checked_at = time.monotonic()
            guard_calls = 0
            is_receipt = bool(self.store.rows("SELECT 1 FROM logic_receipts WHERE outbox_id=?", (row["id"],)))
            def delivery_guard():
                nonlocal checked_at, guard_calls
                guard_calls += 1
                if (row["expires"] <= self.store.clock() or self.reminders.valid_notification(row) is False
                        or (am is not None and am.valid_notification(row) is False)
                        or (reminder_delivery is None and row["task_id"] and not is_receipt and not self.logic.valid_job({"id":row["task_id"]}))):
                    return False
                if harness is not None and harness.durable and not harness.bridge.delivery_source_authorized(row):
                    return False
                # Reuse this delivery's just-completed identity check during local formatting.
                # After a file upload, always refresh again as in the original transport contract.
                if (row["kind"] == "file" and guard_calls > 1) or time.monotonic()-checked_at > 2:
                    allowed = delivery_authorized(); checked_at = time.monotonic(); return allowed
                return True
            view_delivery=am.view_delivery(row) if am is not None else None
            if am is not None:row=am.query_readback_row(row)
            if am is not None:row=am.group_transport_row(row)
            with timing.measure(self.store,event,'publish') if am is not None else __import__('contextlib').nullcontext():
                if view_delivery and view_delivery['target_message']:
                    row=dict(row,update_message_id=view_delivery['target_message'])
                    message_id=self.api.update_card(row,delivery_guard)
                else:message_id = self.api.send(row, delivery_guard)
            if event and (not view_delivery or json.loads(view_delivery['metrics']).get('task_state')!='loading'):
                self.store.execute('UPDATE am_request_timings SET actionable=COALESCE(actionable,?) WHERE event_id=?',(self.store.clock(),event))
                timing.anchor(self.store,event,'publish')
            with self.store.tx() as c:
                c.execute("UPDATE outbox SET state='sent',message_id=? WHERE id=?", (message_id, row["id"]))
                self.reminders.delivered(c,row,message_id)
                if am is not None:am.delivered(c,row,message_id)
                if row["nonce"]:
                    c.execute("UPDATE menus SET message_id=? WHERE nonce=?", (message_id, row["nonce"]))
            if harness is not None and harness.durable:
                bound=self.store.rows('SELECT 1 FROM am_harness_outbox WHERE outbox_id=?',(row['id'],))
                if bound:
                    try:verified=self.api.verify_text(message_id,am.query_readback_row(row))
                    except Exception:verified=False
                    if verified:
                        with self.store.tx() as c:harness.verified_receipt(c,row,message_id)
                    else:
                        self.store.execute("UPDATE am_harness_outbox SET state='needs_reconciliation',receipt_id=? WHERE outbox_id=?",(message_id,row['id']))
            query_read=am is not None and bool(self.store.rows('SELECT 1 FROM am_query_deliveries WHERE outbox_id=?',(row['id'],)))
            sample_every=max(1,int(am.query_settings.get('readback_sample_every',1))) if query_read else 1
            sampled=not query_read or int(row['id'][:8],16)%sample_every==0
            if view_delivery and event and sampled:
                self.store.execute("INSERT OR IGNORE INTO am_view_verifications VALUES(?,?,?,?,'pending',NULL)",(row['id'],view_delivery['interaction_id'],view_delivery['version'],message_id))
                if not getattr(am,'background_enabled',False):self.verify_view(row['id'])
        except SendRejected as exc:
            self.store.execute("UPDATE outbox SET state='rejected',error=? WHERE id=?", (str(exc), row["id"]))
        except (SendUnknown, Exception):
            self.store.execute("UPDATE outbox SET state='unknown',error='SEND_RECEIPT_UNKNOWN' WHERE id=?", (row["id"],))
        return True

    def verify_view(self,outbox_id):
        from . import request_timing as timing
        token=timing.set_context(self.store,timing.origin(self.store,outbox_id))
        try:return self._verify_view(outbox_id)
        finally:timing.reset_context(token)

    def _verify_view(self,outbox_id):
        from . import request_timing as timing
        records=self.store.rows('SELECT * FROM am_view_verifications WHERE outbox_id=?',(outbox_id,))
        if not records:return
        v=records[0]
        out=self.store.rows('SELECT * FROM outbox WHERE id=?',(outbox_id,))[0]
        am=getattr(self,'am_self',None)
        if am is not None:out=am.query_readback_row(out)
        event=timing.origin(self.store,outbox_id)
        if out['expires']<=self.store.clock():
            state='expired';verified=False
        else:
            with timing.measure(self.store,event,'readback'):
                try:verified=self.api.verify_card(v['message_id'],out)
                except Exception:verified=False
            current=self.store.rows('SELECT view_version FROM am_interactions WHERE id=?',(v['interaction_id'],))
            state='verified' if verified else 'superseded' if current and current[0]['view_version']>v['version'] else 'failed'
        self.store.execute('UPDATE am_view_verifications SET state=?,finished=? WHERE outbox_id=?',(state,self.store.clock(),outbox_id))
        output=self.store.rows('SELECT metrics FROM am_view_outputs WHERE outbox_id=?',(outbox_id,))
        if output and json.loads(output[0]['metrics']).get('task_state')!='loading':
            self.store.execute('UPDATE am_request_timings SET finished=?,readback_verified=? WHERE event_id=?',(self.store.clock(),int(verified),event))
            timing.anchor(self.store,event,'verify',state=state)

    def pump_delivery(self):
        """Bounded I/O in the original consumer; one publication per recipient."""
        if self.stopping.is_set():return False
        for key,future in list(self.delivery_futures.items()):
            if future.done():
                try:future.result()
                except Exception:self.store.set_meta('delivery_worker_error','DELIVERY_WORKER_FAILED')
                del self.delivery_futures[key]
        for key,future in list(self.verification_futures.items()):
            if future.done():
                try:future.result()
                except Exception:self.store.execute("UPDATE am_view_verifications SET state='failed' WHERE outbox_id=?",(key,))
                del self.verification_futures[key]
        for row in self.store.rows("SELECT * FROM am_view_verifications WHERE state='pending' ORDER BY rowid LIMIT 32"):
            if len(self.verification_futures)>=2:break
            if self.store.execute("UPDATE am_view_verifications SET state='verifying' WHERE outbox_id=? AND state='pending'",(row['outbox_id'],)):
                self.verification_futures[row['outbox_id']]=self.verification_pool.submit(self.verify_view,row['outbox_id'])
        conversation=getattr(getattr(self,'am_self',None),'conversation',None)
        if conversation is not None:
            for row in self.store.rows("SELECT outbox_id FROM am_conversation_outputs WHERE state='awaiting_verification' AND verification_attempts<3 AND next_verification<=? AND expires>? ORDER BY rowid LIMIT 8",(self.store.clock(),self.store.clock())):
                if len(self.verification_futures)>=2:break
                if row['outbox_id'] not in self.verification_futures:
                    self.verification_futures[row['outbox_id']]=self.verification_pool.submit(conversation.verify_presentation,row['outbox_id'])
        worked=False
        am=getattr(self,'am_self',None)
        harness=getattr(am,'harness',None) if am is not None else None
        if harness is not None and harness.durable:
            for saved in self.store.rows("""SELECT h.outbox_id FROM am_harness_outbox h
                JOIN outbox o ON o.id=h.outbox_id JOIN am_conversation_outputs c ON c.outbox_id=o.id
                WHERE h.verified=0 AND o.state='sent' AND c.verification_attempts<3 AND c.next_verification<=? AND o.expires>? LIMIT 4""",(self.store.clock(),self.store.clock())):
                ident=saved['outbox_id']
                if len(self.verification_futures)<2 and ident not in self.verification_futures:
                    self.verification_futures[ident]=self.verification_pool.submit(harness.reconcile_receipt,ident)
        for row in self.store.rows("SELECT * FROM outbox WHERE state='pending' AND expires>? ORDER BY created,rowid LIMIT 32",(self.store.clock(),)):
            if len(self.delivery_futures)>=3:break
            key=(row['app'],row['tenant'],row['user'])
            if key in self.delivery_futures:continue
            self.delivery_futures[key]=self.delivery_pool.submit(self.deliver_one,row)
            self.delivery_futures[key].add_done_callback(lambda _: self.store.outbox_wakeup())
            worked=True
        return worked

    def recover(self):
        self.reminders.recover()
        # Recovery never reruns inference. Kill only a journalled, still-identical owned leader.
        unknown_runs = set()
        known_api_runs = set()
        def clean_or_hold(directory):
            if self.config.provider_kind == "local_qwen_api":
                if api_request_unknown(directory):
                    discard_payload(directory)
                    unknown_runs.add(directory.name)
                    return
                known_api_runs.add(directory.name)
            shutil.rmtree(directory)
        for directory in (self.config.root / "tasks").iterdir():
            if directory.is_symlink() or not re.fullmatch(r"[a-f0-9]{32}", directory.name):
                self.store.set_meta("provider_pause", "UNRECOGNIZED_TASK_DIRECTORY")
                continue
            try:
                data = json.loads((directory / "process.json").read_text())
                pgid = data.get("pgid")
                if data["state"] == "stopped" or (pgid and not group_alive(pgid)):
                    clean_or_hold(directory)
                    continue
                identity = process_identity(pgid) if pgid else ""
                owned = identity.split()[:2] == [str(os.getuid()), str(pgid)]
                if pgid and owned and identity == data.get("identity") and stop_group(pgid):
                    clean_or_hold(directory)
                else:
                    unknown_runs.add(directory.name)
            except Exception:
                unknown_runs.add(directory.name)
        # A missing temporary directory is not evidence that a journalled process stopped.
        for task in self.store.rows("SELECT * FROM tasks WHERE state IN ('running','stop_unknown')"):
            pgid = task["pgid"]
            if pgid and group_alive(pgid):
                identity = process_identity(pgid)
                owned = identity.split()[:2] == [str(os.getuid()), str(pgid)]
                if not (owned and identity == task["process_identity"] and stop_group(pgid)):
                    unknown_runs.add(task["run_id"])
            code_progress = (((task["route"] == "progress" and task["model"] == CODE_PROGRESS_MODEL)
                              or (task["route"] == "remind" and task["model"] == "python:reminder-resolution")
                              or task["model"] == "python:target-precheck-v1")
                             and not pgid and not task["process_identity"])
            if (self.config.provider_kind == "local_qwen_api" and not code_progress
                    and task["run_id"] not in known_api_runs):
                unknown_runs.add(task["run_id"])
        with self.store.tx() as c:
            for run_id in unknown_runs:
                c.execute("UPDATE tasks SET state='stop_unknown',error='STOP_UNKNOWN' WHERE run_id=?", (run_id,))
                if c.execute("SELECT changes()").fetchone()[0] == 0:
                    c.execute("INSERT INTO meta VALUES ('provider_pause','ORPHAN_STOP_UNKNOWN') ON CONFLICT(key) DO UPDATE SET value=excluded.value")
            for task in c.execute("SELECT id,run_id FROM tasks WHERE state IN ('running','stop_unknown')").fetchall():
                if task["run_id"] not in unknown_runs:
                    c.execute("UPDATE tasks SET state='interrupted',error='RESTART_INTERRUPTED',pgid=NULL,process_identity=NULL WHERE id=?", (task["id"],))
            c.execute("UPDATE outbox SET state='unknown',error='RESTART_SEND_UNKNOWN' WHERE state='sending'")
            c.execute("UPDATE outbox SET state='unknown',error='CALLBACK_APPLICATION_UNKNOWN' WHERE state='callback_pending'")
        self.cleanup()

    def cleanup(self):
        self.reminders.cleanup()
        self.logic.cleanup()
        for value in self.store.cleanup():
            path = Path(value)
            if path.parent == self.config.root / "artifacts" and re.fullmatch(r"[a-f0-9]{32}\.md", path.name):
                path.unlink(missing_ok=True)
        # Files created immediately before a crash but never committed are also reclaimed.
        live = {r["artifact"] for r in self.store.rows("SELECT artifact FROM outbox WHERE artifact IS NOT NULL AND expires>?", (self.store.clock(),))}
        active = {r["id"] + ".md" for r in self.store.rows("SELECT id FROM tasks WHERE state IN ('running','stop_unknown')")}
        for path in (self.config.root / "artifacts").glob("*.md"):
            if str(path) not in live and path.name not in active and not path.is_symlink():
                path.unlink()

    def start(self):
        self.recover()
        def loop(action, interval):
            while not self.stopping.is_set():
                try:
                    action()
                except Exception:
                    self.store.set_meta("worker_error", action.__name__)
                self.stopping.wait(interval)
        for action, interval in ((self.receive_one, .1), (self.schedule_one, .1), (self.deliver_one, .1), (self.cleanup, 1)):
            thread = threading.Thread(target=loop, args=(action, interval), daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        self.stopping.set()
        self.delivery_pool.shutdown(wait=True, cancel_futures=True)
        self.verification_pool.shutdown(wait=True, cancel_futures=True)
        # Running calls remain bounded by their original 120-second deadlines.
        self.pool.shutdown(wait=True)
        for thread in self.threads:
            thread.join(timeout=12)
