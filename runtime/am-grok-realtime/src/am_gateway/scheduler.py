"""Independent resource pools; background model waits cannot occupy Bot slots."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time


class Scheduler:
    def __init__(self, gateway, sender, *, collectors=None):
        self.gateway,self.sender,self.collectors=gateway,sender,collectors
        self.stopping=threading.Event()
        self.wakeup=threading.Event()
        self.threads=[]
        self.errors=[]

    def start(self):
        if self.threads:
            return self
        c=self.gateway.config
        workers=set(c.worker_map.values())-{"grok_bot"}
        groups=[
            ("routing",self._route),
            ("dialogue",lambda:self._pool({"grok_bot"},c.dialogue_workers)),
            ("business",lambda:self._pool(workers,c.business_workers)),
            ("sending",self._send),
        ]
        if self.collectors is not None:
            groups.append(("collecting",self._collect))
        for name,target in groups:
            t=threading.Thread(name="am-r2-"+name,target=target,daemon=True)
            self.threads.append(t)
            t.start()
        return self

    def _route(self):
        last_recover=0
        while not self.stopping.is_set():
            try:
                self.gateway.route()
                if time.monotonic()-last_recover>5:
                    self.gateway.maintenance()
                    last_recover=time.monotonic()
            except Exception as exc:
                self.errors.append(("routing",type(exc).__name__))
            self.wakeup.wait(.05)
            self.wakeup.clear()

    def _pool(self, workers, capacity):
        pool=ThreadPoolExecutor(max_workers=capacity,thread_name_prefix="am-"+",".join(sorted(workers)))
        futures=set()
        try:
            while not self.stopping.is_set():
                done={f for f in futures if f.done()}
                for f in done:
                    try: f.result()
                    except Exception as exc: self.errors.append(("worker",type(exc).__name__))
                futures-=done
                while len(futures)<capacity and not self.stopping.is_set():
                    try:
                        job=self.gateway.claim(workers)
                    except Exception as exc:
                        self.errors.append(("claim",type(exc).__name__))
                        break
                    if not job:
                        break
                    futures.add(pool.submit(self.gateway.run_claimed,job))
                self.stopping.wait(.03)
        finally:
            # Configured adapters enforce deadlines. Native host callables must
            # honor the same contract; stop reports live operations honestly.
            pool.shutdown(wait=True,cancel_futures=True)

    def _send(self):
        last_reconcile=0
        while not self.stopping.is_set():
            try:
                result=self.gateway.outbox.send_one(self.sender)
                if time.monotonic()-last_reconcile>5:
                    self.gateway.outbox.reconcile_unknown(self.sender)
                    last_reconcile=time.monotonic()
                if result:
                    continue
            except Exception as exc:
                self.errors.append(("sending",type(exc).__name__))
            self.stopping.wait(.05)

    def _collect(self):
        while not self.stopping.is_set():
            try:
                self.collectors.run_due()
            except Exception as exc:
                self.errors.append(("collecting",type(exc).__name__))
            self.stopping.wait(.1)

    def stop(self, timeout=3):
        self.stopping.set()
        self.wakeup.set()
        deadline=time.monotonic()+timeout
        for t in self.threads:
            t.join(max(0,deadline-time.monotonic()))
        active=[t.name for t in self.threads if t.is_alive()]
        return {"stopped":not active,"active_threads":active,
                "errors":self.errors[-20:]}
