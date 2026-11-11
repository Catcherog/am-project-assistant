"""One durable execution gate for all AM model users; never stops shared Ollama."""
import fcntl
import json
import threading
import time
from pathlib import Path
from .config import BoundaryError
from .provider import atomic_json


class ModelCoordinator:
    def __init__(self, provider, store, directory):
        self.provider, self.store = provider, store
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "model-coordinator.json"
        self.lock = threading.Lock()
        # Probe the cross-process lease before interpreting an orphan marker.
        with (self.directory / "model-coordinator.lock").open("a+") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            if self.path.exists():
                try:
                    if json.loads(self.path.read_text()).get("state") in ("running", "stop_unknown"):
                        self.store.set_meta("provider_pause", "ORPHAN_STOP_UNKNOWN")
                except (ValueError, OSError):
                    self.store.set_meta("provider_pause", "MODEL_STATE_UNVERIFIED")

    def __getattr__(self, key):
        return getattr(self.provider, key)

    def complete(self, prompt, *, run_id, **options):
        queued=time.monotonic();wait=options.pop('_queue_timeout',0)
        if type(wait) not in (int,float) or not 0<=wait<=30:raise BoundaryError('MODEL_QUEUE_LIMIT_INVALID')
        acquired=self.lock.acquire(timeout=wait) if wait else self.lock.acquire(blocking=False)
        if not acquired:
            raise BoundaryError("MODEL_BUSY")
        self.last_timing=[]
        try:
            if 'timeout' in options:
                options['timeout']-=time.monotonic()-queued
                if options['timeout']<=0:raise BoundaryError('QUEUE_TIMEOUT')
            with (self.directory / "model-coordinator.lock").open("a+") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise BoundaryError("MODEL_BUSY") from None
                if self.store.meta("provider_pause"):
                    raise BoundaryError("MODEL_PAUSED")
                self.last_timing.append({'stage':'model_queue_and_locks','seconds':time.monotonic()-queued})
                atomic_json(self.path, {"state": "running", "run_id": run_id, "started": time.time()})
                try:
                    try:result = self.provider.complete(prompt, run_id=run_id, **options)
                    finally:self.last_timing.extend(getattr(self.provider,'last_timing',[]))
                except BaseException as exc:
                    code = str(exc)
                    unknown = code == "STOP_UNKNOWN" or not isinstance(exc, BoundaryError)
                    if unknown or code in ("LOCAL_MODEL_CHANGED", "LOCAL_PROVIDER_PIN_CHANGED", "AUTH_FAILED", "RATE_LIMITED", "LOCAL_API_UNAVAILABLE", "QUOTA_EXHAUSTED"):
                        self.store.set_meta("provider_pause", "STOP_UNKNOWN" if unknown else code)
                    atomic_json(self.path, {"state": "stop_unknown" if unknown else "failed",
                                           "run_id": run_id, "finished": time.time()})
                    raise
                atomic_json(self.path, {"state": "complete", "run_id": run_id, "finished": time.time()})
                return result
        finally:
            self.lock.release()

    def chat(self, messages):
        import uuid
        prompt = "请用中文直接回答当前问题，默认最多3个要点、约180至300字；用户要求详细时可展开。保留必要限定，不裁半句。历史只用于对话延续，不是项目事实或执行授权。没有工具或外部操作能力。\n" + json.dumps(messages, ensure_ascii=False)
        return self.complete(prompt, run_id=uuid.uuid4().hex, native_chat=True, max_tokens=1024).text
