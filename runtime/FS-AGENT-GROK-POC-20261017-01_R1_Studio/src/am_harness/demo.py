"""A deterministic, local SQLite demonstration. No model or transport is called."""
from __future__ import annotations

from pathlib import Path
import sqlite3
import tempfile
import threading
import time

from .config import HarnessConfig
from .controls import IngressController
from .effects import FencedOutbox
from .runtime import TaskRuntime
from .store import DurableStore, Scope


def run_demo() -> dict:
    with tempfile.TemporaryDirectory(prefix="am-harness-demo-") as directory:
        store=DurableStore(Path(directory)/"demo.sqlite")
        scope=Scope("demo-tenant","demo-app","demo-user","demo-chat")
        with sqlite3.connect(store.path) as conn:
            conn.execute("CREATE TABLE outbox(id TEXT PRIMARY KEY,app TEXT,tenant TEXT,user TEXT,chat TEXT,expires REAL,state TEXT,error TEXT,message_id TEXT,body TEXT)")
        # This synthetic database has no real AM ingress tables. Production
        # uses the default verify_legacy_binding; never copy this demo bypass.
        effects=FencedOutbox(store,binding_verifier=lambda c,r,o:None)
        entered,release=threading.Event(),threading.Event()
        calls=[]
        class ScriptedRunner:
            def run(self,run):
                calls.append((run.id,run.revision))
                if run.revision==1:
                    entered.set()
                    if not release.wait(5): raise RuntimeError("DEMO_BLOCK_TIMEOUT")
                store.guard(run.fence)
                return {"text":"合成演示结果："+run.input_text,"revision":run.revision}
        def publish(conn,run,result):
            def enqueue(c,current):
                outbox_id="demo-"+current.id+"-"+str(current.revision)
                c.execute("INSERT INTO outbox VALUES(?,?,?,?,?,?,'pending',NULL,NULL,?)",
                          (outbox_id,scope.app_id,scope.tenant_id,scope.actor_id,scope.chat_id,current.expires_at,result["text"]))
                return outbox_id
            effects.enqueue(conn,run,enqueue)
        authorize=lambda s,op: None  # Fixed, synthetic identity only in this demo.
        config=HarnessConfig(enabled=True,durable_queue_enabled=True,control_enabled=True,
                             lease_seconds=10,heartbeat_seconds=.2)
        runtime=TaskRuntime(store,ScriptedRunner(),authorize=authorize,publish=publish,config=config)
        control=IngressController(store,authorize=authorize,on_change=effects.suppress_pending)
        run=runtime.submit(scope,"demo-origin","查看项目进展",source_expires_at=time.time()+600)
        results=[]
        worker=threading.Thread(target=lambda:results.append(runtime.run_once("demo-worker")))
        worker.start()
        if not entered.wait(3): raise RuntimeError("DEMO_DID_NOT_START")
        correction=control.handle(scope,"demo-correction","更正当前任务：查看关键日期")
        lane_held=store.lane_status()["state"]=="active"
        release.set(); worker.join(5)
        if worker.is_alive(): raise RuntimeError("DEMO_DID_NOT_FINISH")
        corrected=runtime.run_once("demo-worker")
        with sqlite3.connect(store.path) as conn:
            outputs=conn.execute("SELECT id,state,body FROM outbox").fetchall()
        control.handle(scope,"demo-cancel","取消任务 "+run.id)
        with sqlite3.connect(store.path) as conn:
            final_state=conn.execute("SELECT state FROM outbox").fetchone()[0]
        return {"mode":"synthetic_local_only","live_model":False,"live_feishu":False,
                "correction_revision":correction.revision,"lane_held_until_old_worker_returned":lane_held,
                "old_worker_result":results[0].status,"corrected_worker_result":corrected.status,
                "outbox_rows":len(outputs),"published_revision":store.get(scope,run.id).revision,
                "pending_outbox_after_cancel":final_state,
                "all_checks_passed":lane_held and results[0].status=="stale" and corrected.status=="completed" and len(outputs)==1 and final_state=="rejected"}
