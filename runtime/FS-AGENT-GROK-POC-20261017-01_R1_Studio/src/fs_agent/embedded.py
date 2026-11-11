"""PoC dispatch inside the existing AM bridge; never creates a socket or worker."""
from __future__ import annotations

import fcntl
import json
import time
from pathlib import Path

from .config import BoundaryError, Config
from .feishu import Feishu
from .gateway import Gateway
from .message_text import MessageTextError, normalize_message
from .provider import atomic_json
from .qwen_provider import LocalQwenProvider
from .store import Store, StoreBusyBeforeTransaction, scope_of

COMMANDS = {"/doc", "/progress", "/menu", "/retry", "/resend", "/chat", "/remind"}


class EmbeddedPoC:
    def __init__(self, gateway: Gateway):
        self.gateway, self.store, self.config = gateway, gateway.store, gateway.config
        self.lock = None
        self.suspended = False

    @classmethod
    def open(cls, path: Path, client, *, app_id: str):
        config = Config.load(path)
        config.check_service()
        config.check_feishu()
        if (config.provider_kind != "local_qwen_api"
                or config.app_id != app_id or config.event_transport != "existing_bridge"
                or config.runtime_mode != "existing_login" or config.concurrency != 1):
            raise BoundaryError("EXISTING_BRIDGE_CONFIG_INVALID")
        lock = (config.root / "state" / "instance.lock").open("a+")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            store = Store(config.db)
            from .model_coordinator import ModelCoordinator
            provider = ModelCoordinator(LocalQwenProvider(config), store, config.root / "state")
            gateway = Gateway(config, store, Feishu(config, client=client), provider)
            obj = cls(gateway)
            obj.lock = lock
            # Recover before accepting any new PoC input; no inference is rerun.
            gateway.recover()
            try:
                provider.pin()
            except BoundaryError as exc:
                store.set_meta("provider_pause", str(exc))
            obj.write_status()
            return obj
        except BaseException:
            if "gateway" in locals():
                gateway.stop()
            if "store" in locals():
                store.close()
            lock.close()
            raise

    def _mode(self, row):
        rows = self.store.rows("SELECT mode FROM bridge_mode WHERE scope=? AND expires>?",
                               (scope_of(row), self.store.clock()))
        return rows[0]["mode"] if rows else "chat"

    def ingest(self, payload) -> tuple[bool, bool]:
        """Return (belongs to PoC, newly persisted). Callback has no network I/O."""
        try:
            h, e = payload["header"], payload["event"]
            if h["app_id"] != self.config.app_id or h["tenant_key"] != self.config.tenant_key:
                return False, False
            if self.suspended:
                user = (e.get("operator", {}).get("open_id") if h["event_type"] == "card.action.trigger"
                        else e.get("sender", {}).get("sender_id", {}).get("open_id"))
                if user in self.config.users:
                    raise BoundaryError("POC_SUSPENDED")
            if h["event_type"] == "card.action.trigger" and e.get("action",{}).get("value",{}).get("flow")=="reminder":
                return True,self.gateway.reminders.ingest(payload)
            if h["event_type"] == "card.action.trigger":
                row = {"app": h["app_id"], "tenant": h["tenant_key"],
                       "user": e["operator"]["open_id"], "chat": e["context"]["open_chat_id"]}
                if row["user"] not in self.config.users or self._mode(row) != "poc":
                    return True, False
                return True, self.gateway.ingest(payload)
            if h["event_type"] != "im.message.receive_v1":
                return False, False
            message, sender = e["message"], e["sender"]
            row = {"app": h["app_id"], "tenant": h["tenant_key"],
                   "user": sender["sender_id"]["open_id"], "chat": message["chat_id"]}
            if self.store.rows("SELECT 1 FROM inbox WHERE app=? AND tenant=? AND message_id=?",
                               (row["app"], row["tenant"], message["message_id"])):
                # Receipt identity wins even if a replay changes body/type or
                # arrives after /chat. Never reclassify it into ordinary chat.
                return True, False
            if message["message_type"] == "file":
                if row["user"] in self.config.users and self._mode(row) == "poc":
                    return True, self.gateway.ingest(payload)
                return False, False
            if message["message_type"] not in ("text", "post"):
                return False, False
            try:
                # Classification is local and read-only. The original event goes
                # to Gateway so identity, time and mention evidence stay intact.
                text = normalize_message(message, bot_open_id=self.config.bot_open_id,
                                         max_chars=6000).text
            except MessageTextError as exc:
                # Let the bridge retain its existing fixed too-long notice. It
                # never submits this body to a model. Unsafe structures are
                # consumed here, preventing ordinary-chat fallback.
                return (False, False) if exc.code == "INPUT_TOO_LONG" else (True, False)
            if self.config.reminders_enabled and message["chat_type"]=="p2p":
                reminder_row=dict(row,chat_type="p2p")
                if text.startswith(("/doc","/progress","/chat","/menu","/new")):
                    self.gateway.reminders.suspend_focus(reminder_row)
                elif self.gateway.reminders.wants(reminder_row,text):
                    return True,self.gateway.reminders.ingest(payload)
            parts = text.strip().split(maxsplit=1)
            command = parts[0] if parts else ""
            if row["user"] not in self.config.users:
                return command in COMMANDS, False
            if command not in COMMANDS and self._mode(row) != "poc":
                return False, False
            mode = "chat" if text.strip() == "/chat" else "poc"

            def save_mode(conn, event, expires):
                # Same transaction as durable receipt: a rapid next message cannot
                # escape into ordinary chat while /doc is waiting for its worker.
                if command in COMMANDS:
                    conn.execute("INSERT INTO bridge_mode VALUES (?,?,?) ON CONFLICT(scope) DO UPDATE SET "
                                 "mode=excluded.mode,expires=excluded.expires", (scope_of(event), mode, expires))

            return True, self.gateway.ingest(payload, on_insert=save_mode)
        except (KeyError, TypeError, ValueError):
            return False, False

    def step(self) -> bool:
        """Called synchronously by the one existing bridge worker, between chat jobs."""
        if self.gateway.stopping.is_set() or self.suspended:
            return False
        try:
            return self._step()
        except StoreBusyBeforeTransaction:
            # No transaction began and no write was attempted. Leave durable
            # work in place and let the original loop try its next tick.
            atomic_json(self.config.root / "state" / "embedded-status.json", {
                "time":time.time(),"suspended":False,"state":"waiting_for_store",
                "error":"STORE_BUSY_BEFORE_TRANSACTION"})
            return False
        except Exception as exc:
            # A PoC storage/worker fault cannot terminate the existing chat worker.
            # Reject affected users' input until recovery instead of rerouting it.
            self.suspended = True
            import traceback
            atomic_json(self.config.root / "state" / "embedded-status.json", {
                "time": time.time(), "suspended": True, "error": "POC_WORKER_FAILED",
                "error_type":type(exc).__name__,
                "sqlite_errorcode":getattr(exc,'sqlite_errorcode',None),
                "sqlite_errorname":getattr(exc,'sqlite_errorname',None),
                "transaction_open":self.store.conn.in_transaction,
                "frames":[{'module':Path(f.filename).name,'function':f.name,'line':f.lineno}
                          for f in traceback.extract_tb(exc.__traceback__)[-8:]]})
            return False

    def _step(self):
        self.gateway.cleanup()
        deliver=self.gateway.pump_delivery if getattr(getattr(self.gateway,'am_self',None),'background_enabled',False) else self.gateway.deliver_one
        worked = deliver()
        worked = self.gateway.receive_one() or worked
        worked = deliver() or worked
        try:
            worked = self.gateway.reminders.dispatch_one() or worked
        except Exception:
            # A reminder-only egress fault must not suspend ordinary chat or docs.
            self.store.execute("UPDATE reminder_drafts SET state='unknown',error='REMINDER_DISPATCH_INTERRUPTED' WHERE state='sending'")
            self.store.set_meta("reminder_pause","REMINDER_DISPATCH_INTERRUPTED")
        group_future=getattr(self.gateway,"group_query_future",None)
        if not self.store.meta("provider_pause") and not (group_future is not None and not group_future.done()) and not getattr(self.gateway.provider,"lock",__import__("threading").Lock()).locked():
            task = self.store.claim(1)
            if task:
                if self.config.document_logic or self.config.reminders_enabled:
                    self.gateway.pool.submit(self.gateway.run_task, task)
                else:
                    self.gateway.run_task(task)
                worked = True
        if worked:
            self.write_status()
        return worked

    def model_busy(self):
        return bool(self.store.rows("SELECT 1 FROM tasks WHERE state='running' LIMIT 1"))

    def write_status(self):
        atomic_json(self.config.root / "state" / "embedded-status.json", {
            "time": self.store.clock(), "transport": "existing_bridge", "concurrency": 1,
            "tasks": self.store.rows("SELECT state,COUNT(*) AS count FROM tasks GROUP BY state"),
            "deliveries": self.store.rows("SELECT state,COUNT(*) AS count FROM outbox GROUP BY state"),
            "provider_pause": self.store.meta("provider_pause"),
            "reminder": self.gateway.reminders.operational_status(),
        })

    def close(self):
        self.gateway.stop()
        self.store.close()
        if self.lock:
            self.lock.close()
