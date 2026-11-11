"""Durable, exact-command process supervision around unchanged R4 workers.

The selected training Python supplies its existing psutil dependency. The live
bridge needs only the standard library. No process-name kill, shell, or trainer
patch is used. Restart recovery reads the same launch intent and identities.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

from .window import WindowError


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def private_json(path, value, *, exclusive=False):
    path = Path(path)
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        if exclusive:
            os.link(temp, path)
            temp.unlink()
        else:
            if path.is_symlink():
                raise WindowError("private_evidence_symlink")
            os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temp.exists():
            temp.unlink()


# Read argv and process creation time from the OS, not a trainer success flag.
# Return only this run's exact commands and members of its recorded groups.
_SCAN = r'''
import json,os,sys,psutil
q=json.load(sys.stdin); table={}; errors=[]
for p in psutil.process_iter():
    try:
        with p.oneshot():
            if p.uids().real!=os.getuid():continue
            a=p.cmdline(); pg=os.getpgid(p.pid)
            table[p.pid]={'pid':p.pid,'ppid':p.ppid(),'pgid':pg,'uid':p.uids().real,
                'start_time':p.create_time(),'argv':a,'status':p.status()}
    except psutil.NoSuchProcess:pass
    except (psutil.AccessDenied,PermissionError):errors.append(p.pid)
    except ProcessLookupError:pass
def same(a,b):
    return bool(a and b and os.path.realpath(a[0])==os.path.realpath(b[0]) and a[1:]==b[1:])
matches=[]; groups=set(q['groups']); old=set(q['pids'])
for p in table.values():
    roles=[k for k,a in q['commands'].items() if same(p['argv'],a)]
    if roles:
        p['role']=roles[0]; matches.append(p); groups.add(p['pgid'])
selected=[p for p in table.values() if p['pgid'] in groups or p['pid'] in old]
print(json.dumps({'matches':matches,'members':selected,'unreadable_recorded_pids':list(old&set(errors))}))
'''


class BoundProcessGuard:
    def __init__(self, path, data):
        self.path, self.data = Path(path), data

    @classmethod
    def create(cls, path, *, python, commands, request_id, plan_path, run_directory, sources):
        data = {"schema": "am-r7-process-ledger/1", "request_id": request_id,
                "plan_path": str(plan_path), "plan_sha256": digest(plan_path),
                "run_directory": str(run_directory), "python": str(python),
                "python_sha256": digest(python), "commands": commands,
                "launch_intent_at": time.time(), "sources": sources,
                "processes": [], "reported_groups": [], "events": [], "stop_requested": False}
        private_json(path, data, exclusive=True)
        return cls(path, data)

    @classmethod
    def load(cls, path):
        path = Path(path)
        st = path.lstat()
        if path.is_symlink() or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise WindowError("process_ledger_not_private")
        data = json.loads(path.read_text())
        if data.get("schema") != "am-r7-process-ledger/1":
            raise WindowError("process_ledger_schema")
        uuid.UUID(data["request_id"])
        if digest(data["python"]) != data["python_sha256"]:
            raise WindowError("process_probe_interpreter_changed")
        return cls(path, data)

    def write(self):
        private_json(self.path, self.data)

    def refresh(self):
        query = {"commands": self.data["commands"],
                 "groups": list({p["pgid"] for p in self.data["processes"]} | set(self.data["reported_groups"])),
                 "pids": [p["pid"] for p in self.data["processes"]]}
        done = subprocess.run([self.data["python"], "-c", _SCAN], input=json.dumps(query),
                              text=True, capture_output=True, timeout=5, check=True,
                              env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
        view = json.loads(done.stdout)
        if view["unreadable_recorded_pids"]:
            raise WindowError("recorded_process_identity_unreadable")
        known = {p["pid"]: p for p in self.data["processes"]}
        for p in view["matches"]:
            if p["start_time"] < self.data["launch_intent_at"] - .02:
                raise WindowError("matching_process_predates_this_launch")
            if p["role"] in {"cli", "fit", "reload", "mlx_worker"} and p["pgid"] != p["pid"]:
                raise WindowError("expected_independent_session_missing")
            if p["pid"] in known and not self.same_identity(known[p["pid"]], p):
                raise WindowError("recorded_pid_reused")
            if p["pid"] not in known:
                self.data["processes"].append(p.copy()); known[p["pid"]] = p
        # Children such as sysctl share their verified parent's process group.
        # Keep their OS identity too; a later unrelated process is not adopted.
        changed = True
        while changed:
            changed = False
            for p in view["members"]:
                parent = known.get(p["ppid"])
                if p["pid"] not in known and parent and p["start_time"] >= parent["start_time"] and p["pgid"] == parent["pgid"]:
                    child = dict(p, role="descendant")
                    self.data["processes"].append(child); known[p["pid"]] = child; changed = True
        for p in view["members"]:
            if p["pid"] in known and not self.same_identity(known[p["pid"]], p):
                raise WindowError("recorded_pid_reused")
        self.data["last_scan_at"] = time.time()
        self.data["live_identities"] = view["members"]
        self.write()
        return view

    @staticmethod
    def same_identity(old, new):
        return all(old.get(k) == new.get(k) for k in ("pid", "pgid", "uid", "start_time", "argv"))

    def report_result_groups(self, phases):
        for phase in phases:
            pid = phase.get("pid")
            if phase.get("attempted") is True and type(pid) is int and pid > 1:
                self.data["reported_groups"] = sorted(set(self.data["reported_groups"]) | {pid})
        self.write()

    def stopped(self):
        view = self.refresh()
        if view["matches"] or view["members"]:
            return False
        groups = {p["pgid"] for p in self.data["processes"]} | set(self.data["reported_groups"])
        for pgid in groups:
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                continue
            except PermissionError:
                return False
            return False
        # A crash before a worker identity was captured is not a stop proof.
        if Path(self.data["run_directory"]).exists() and not self.data["reported_groups"] and not any(
                p["role"] in {"fit", "reload", "mlx_worker"} for p in self.data["processes"]):
            return False
        return True

    def signal_group(self, pgid, sig):
        view = self.refresh()
        members = [p for p in view["members"] if p["pgid"] == pgid]
        known = {p["pid"]: p for p in self.data["processes"]}
        if not members:
            return
        if any(p["pid"] not in known or not self.same_identity(known[p["pid"]], p) for p in members):
            raise WindowError("unverified_process_group_member_no_signal")
        if pgid <= 1 or pgid == os.getpgrp():
            raise WindowError("refuse_controller_or_system_process_group")
        self.data["events"].append({"at": time.time(), "pgid": pgid, "signal": int(sig),
                                    "verified_pids": [p["pid"] for p in members]})
        self.write()  # Intent is durable before the signal.
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass

    def stop(self, reason, *, reap=lambda: None):
        self.data.update(stop_requested=True, stop_reason=reason)
        self.write()
        self.refresh()
        parents = {p["pgid"] for p in self.data["processes"] if p["role"] in {"cli", "r4_parent"}}
        # Freeze the launchers first so cancellation cannot race a fresh reload.
        for pgid in parents:
            self.signal_group(pgid, signal.SIGSTOP)
        self.refresh()
        workers = {p["pgid"] for p in self.data["processes"] if p["role"] in {"fit", "reload", "mlx_worker"}}
        for pgid in workers:
            self.signal_group(pgid, signal.SIGTERM)
        time.sleep(.25)
        for pgid in workers | parents:
            self.signal_group(pgid, signal.SIGKILL)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            reap()
            if self.stopped():
                self.data["stop_confirmed_at"] = time.time(); self.write()
                return True
            time.sleep(.1)
        return False
