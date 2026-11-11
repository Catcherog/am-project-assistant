"""Exact lifecycle of the locally verified Ollama manager; no trainer or sender."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.request

from .window import WindowError


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise WindowError('local_manager_redirect')


class OllamaLifecycle:
    def __init__(self, *, manager_binary, manager_pid, version, context_length=16384,
                 keep_alive='30m', request=None, workers=None):
        self.binary = Path(manager_binary).resolve(strict=True)
        self.binary_hash = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        self.pid, self.version = int(manager_pid), version
        self.context_length, self.keep_alive = context_length, keep_alive
        self.request = request or self.http
        self.workers = workers or self.native_workers
        self.events = []

    @staticmethod
    def http(path, payload=None, timeout=5):
        if path not in ('/api/version', '/api/tags', '/api/ps', '/api/generate'):
            raise WindowError('unsupported_manager_endpoint')
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        request = urllib.request.Request('http://127.0.0.1:11434'+path,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={'Content-Type': 'application/json'})
        with opener.open(request, timeout=timeout) as response:
            raw = response.read(1024*1024+1)
        if len(raw)>1024*1024:
            raise WindowError('manager_response_too_large')
        value = json.loads(raw)
        if not isinstance(value, dict): raise WindowError('manager_response_schema')
        return value

    def verify_manager(self):
        if hashlib.sha256(self.binary.read_bytes()).hexdigest()!=self.binary_hash:
            raise WindowError('manager_binary_changed')
        command = subprocess.check_output(['/bin/ps', '-p', str(self.pid), '-o', 'command='], text=True).strip()
        if command != str(self.binary)+' serve':
            raise WindowError('manager_process_changed')
        listeners = subprocess.check_output(['/usr/sbin/lsof','-nP','-iTCP:11434','-sTCP:LISTEN','-t'], text=True).split()
        if listeners != [str(self.pid)] or self.request('/api/version').get('version')!=self.version:
            raise WindowError('manager_listener_or_version_changed')

    def native_workers(self):
        rows=[]
        for line in subprocess.check_output(['/bin/ps','-axo','pid=,ppid=,rss=,lstart=,command='], text=True).splitlines():
            parts=line.strip().split(None,8)
            if len(parts)!=9 or int(parts[1])!=self.pid: continue
            if Path(parts[8].split(' ',1)[0]).name!='llama-server': continue
            rows.append({'pid':int(parts[0]),'ppid':int(parts[1]),'rss_GiB':int(parts[2])/1024**2,
                         'started':' '.join(parts[3:8]),'command_sha256':hashlib.sha256(parts[8].encode()).hexdigest()})
        return rows

    def verify_identity(self, binding):
        self.verify_manager()
        matches=[x for x in self.request('/api/tags')['models'] if x.get('name')==binding.model.name]
        if len(matches)!=1 or matches[0].get('digest')!=binding.model.digest:
            raise WindowError('original_model_changed')
        if matches[0].get('details',{}).get('runner')!=binding.runner:
            raise WindowError('original_runner_changed')
        models=self.request('/api/ps')['models']
        if any(x.get('name')!=binding.model.name or x.get('digest')!=binding.model.digest for x in models):
            raise WindowError('unexpected_loaded_model')
        if len(models)>1: raise WindowError('multiple_model_residencies')
        if models and models[0].get('context_length')!=self.context_length:
            raise WindowError('model_context_changed')
        return models

    def _change(self, binding, *, loaded):
        models=self.verify_identity(binding)
        before=self.workers()
        if loaded and models:
            if len(before)!=1: raise WindowError('loaded_worker_not_unique')
            return
        if not loaded and not models:
            if before: raise WindowError('unlisted_worker_present')
            return
        if (not loaded and len(before)!=1) or (loaded and before):
            raise WindowError('worker_binding_unverified')
        start=time.monotonic()
        result=self.request('/api/generate', {'model':binding.model.name,'prompt':'','stream':False,
            'keep_alive':self.keep_alive if loaded else 0,
            'options':{'num_ctx':self.context_length}}, timeout=120)
        # Empty-prompt management must not turn into generation.
        if result.get('done') is not True or result.get('response') or result.get('eval_count',0):
            raise WindowError('management_request_generated_text_or_unconfirmed')
        deadline=time.monotonic()+30
        while True:
            current=self.verify_identity(binding);after=self.workers()
            if bool(current)==loaded and len(after)==(1 if loaded else 0): break
            if time.monotonic()>=deadline: raise WindowError('worker_lifecycle_unconfirmed')
            time.sleep(.25)
        self.events.append({'operation':'restore' if loaded else 'unload','seconds':time.monotonic()-start,
                            'manager_pid':self.pid,'manager_preserved':True,'worker_before':before,
                            'worker_after':after,'response':result,'context_length':self.context_length})

    def suspend(self, binding): self._change(binding,loaded=False)
    def restore(self, binding): self._change(binding,loaded=True)
