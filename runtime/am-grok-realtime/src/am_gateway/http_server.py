"""Optional worker bridge. Bind to loopback behind the host's existing HTTPS/auth.

Credentials are scoped to a worker. There is deliberately no HTTP ingest or
approval endpoint accepting caller-supplied user identities.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import json
import threading
from urllib.parse import urlparse,parse_qs

from .models import ContractError, dumps


class WorkerServer:
    def __init__(self,gateway,worker_tokens,*,host="127.0.0.1",port=0):
        if host not in {"127.0.0.1","::1","localhost"}:
            raise ValueError("use the existing authenticated HTTPS reverse proxy for remote access")
        if not worker_tokens or any(not isinstance(t,str) or len(t)<24 for t in worker_tokens.values()):
            raise ValueError("bind nonempty host-managed worker tokens")
        self.gateway,self.tokens=gateway,dict(worker_tokens)
        outer=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def _reply(self,status,data):
                blob=dumps(data).encode()
                self.send_response(status)
                self.send_header("Content-Type","application/json; charset=utf-8")
                self.send_header("Content-Length",str(len(blob)))
                self.end_headers()
                self.wfile.write(blob)
            def _worker(self):
                auth=self.headers.get("Authorization","")
                if not auth.startswith("Bearer "): return None
                token=auth[7:]
                for worker, expected in outer.tokens.items():
                    if hmac.compare_digest(token,expected): return worker
                return None
            def _serve(self):
                worker=self._worker()
                if not worker:
                    return self._reply(401,{"error":"unauthorized"})
                path=urlparse(self.path).path
                try:
                    if self.command=="POST":
                        length=int(self.headers.get("Content-Length","0"))
                        if not 0<length<=262144:
                            return self._reply(413,{"error":"body_size"})
                        body=json.loads(self.rfile.read(length))
                        if not isinstance(body,dict):
                            raise ContractError("object required")
                    else: body={}
                    if path=="/v1/jobs/claim" and self.command=="POST":
                        job=outer.gateway.claim({worker})
                        return self._reply(200,{"job":outer.gateway.request(job) if job else None})
                    parts=path.strip("/").split("/")
                    if len(parts) not in {3,4,5} or parts[:2]!=["v1","jobs"]:
                        return self._reply(404,{"error":"route_not_found"})
                    job=outer.gateway.store.one("SELECT * FROM gw_jobs WHERE job_id=?",(parts[2],))
                    if not job or job["worker"]!=worker:
                        return self._reply(404,{"error":"job_not_found"})
                    if self.command=="GET" and len(parts)==3:
                        data={k:job[k] for k in ("job_id","state","attempt_id","provider_run_id","last_error")}
                        data["context"]=json.loads(job["context_json"]) if job["context_json"] else None
                        return self._reply(200,data)
                    if self.command=="GET" and len(parts)==5 and parts[3]=="items":
                        params=parse_qs(urlparse(self.path).query)
                        data=outer.gateway.read_input(job["job_id"],worker,parts[4],
                            offset=int(params.get("offset",["0"])[0]),limit=int(params.get("limit",["4000"])[0]))
                        return self._reply(200,data)
                    if self.command=="POST" and len(parts)==4 and parts[3]=="result":
                        result=outer.gateway.complete(job["job_id"],body.get("attempt_id"),body)
                        return self._reply(200,{"job_id":job["job_id"],"state":result["state"]})
                    return self._reply(404,{"error":"route_not_found"})
                except (ValueError,KeyError,ContractError) as exc:
                    return self._reply(409,{"error":type(exc).__name__,"detail":str(exc)[:160]})
                except Exception as exc:
                    return self._reply(500,{"error":type(exc).__name__})
            do_GET=_serve
            do_POST=_serve
        self.server=ThreadingHTTPServer((host,port),Handler)
        self.server.daemon_threads=True
        self.thread=None

    @property
    def url(self):
        host,port=self.server.server_address[:2]
        return f"http://{host}:{port}"

    def start(self):
        if not self.thread:
            self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
            self.thread.start()
        return self

    def close(self):
        if self.thread:
            self.server.shutdown()
            self.thread.join(2)
        self.server.server_close()
