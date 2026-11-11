"""Scoped Meegle CLI adapter. Exact targets are bound by the service, never by model text."""
from __future__ import annotations
from contextlib import contextmanager
from contextvars import ContextVar
import fcntl
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from .config import BoundaryError
from .doc_logic import digest, read_source, MAX_BYTES
from .document_target import (TargetValidationError, binding_config, fingerprint,
                              immutable_binding, project_identity, target_identity,
                              validate_metadata)

class BusinessNotStarted(BoundaryError):
    """The adapter has proven that no external mutation was attempted."""

class MeeglePort:
    def __init__(self, gateway, settings):
        self.g, self.s, self.c = gateway, gateway.store, gateway.config
        self.settings = settings
        self._read_context = ContextVar("meegle_document_read_scope", default=None)
        self.binary = Path(settings["executable"])
        if (self.binary.is_symlink() or not self.binary.resolve().is_relative_to(self.c.root / "bin")
                or digest(self.binary.read_bytes()) != settings["sha256"]):
            raise BoundaryError("BUSINESS_EXECUTABLE_UNVERIFIED")
        if settings["profile"] != "fs-doc-logic-r1" or settings["allowed_user"] != self.c.owner:
            raise BoundaryError("BUSINESS_IDENTITY_BINDING_INVALID")
        self.tmp = self.c.root / "state" / "business-tmp"
        self.tmp.mkdir(mode=0o700, exist_ok=True)
        if self.tmp.is_symlink():
            raise BoundaryError("BUSINESS_DIRECTORY_INVALID")

    @contextmanager
    def read_scope(self, principal=None, *, guard=None, deadline=None):
        """Share one bounded, context-local read budget across discovery/pages.

        Nested state reads may narrow a budget, never extend an outer deadline.
        The expiry uses the store's clock; elapsed I/O uses monotonic time.
        """
        now = time.monotonic()
        current = self._read_context.get()
        deadlines = [now + 120.0]
        guards = current[1] if current is not None else ()
        if current is not None:
            deadlines.append(current[0])
        if deadline is not None:
            deadlines.append(deadline)
        if principal is not None and principal.get("expires") is not None:
            deadlines.append(now + max(0.0, principal["expires"] - self.s.clock()))
        if guard is not None and guard not in guards:
            guards += (guard,)
        token = self._read_context.set((min(deadlines), guards))
        try:
            yield
        finally:
            self._read_context.reset(token)

    def _read_timeout(self):
        scope = self._read_context.get()
        if scope is None:
            return 40.0
        if time.monotonic() >= scope[0]:
            raise BoundaryError("BUSINESS_READ_DEADLINE")
        for guard in scope[1]:
            if guard() is False:
                raise BoundaryError("DOCUMENT_DELIVERY_SCOPE_CHANGED")
        remaining = scope[0] - time.monotonic()
        if remaining <= 0:
            raise BoundaryError("BUSINESS_READ_DEADLINE")
        return min(40.0, remaining)

    def _run(self, args, *, params=None, json_result=True):
        env = {k:v for k,v in os.environ.items() if not k.startswith("MEEGLE_")}
        env.update(MEEGLE_HOST="project.feishu.cn", MEEGLE_TELEMETRY="disabled",
                   MEEGLE_AI_HANDOFF="disabled", MEEGLE_NO_UPDATE_CHECK="1")
        command = [str(self.binary), *args, "--profile", self.settings["profile"], "--format", "json"]
        with tempfile.TemporaryDirectory(dir=self.tmp) as tmp:
            if params is not None:
                path = Path(tmp) / "params.json"
                path.write_text(json.dumps(params, ensure_ascii=False)); path.chmod(0o600)
                command += ["--params", "@" + str(path)]
            try:
                result = subprocess.run(command, env=env, capture_output=True, timeout=self._read_timeout())
            except (OSError, subprocess.TimeoutExpired):
                self._read_timeout()
                raise BoundaryError("BUSINESS_CALL_UNCERTAIN") from None
            self._read_timeout()
        if result.returncode:
            raise BoundaryError("BUSINESS_CALL_FAILED")
        if not json_result:
            return None
        if len(result.stdout) > 4_000_000:
            raise BoundaryError("BUSINESS_RESPONSE_LIMIT")
        try:
            value = json.loads(result.stdout)
        except ValueError:
            raise BoundaryError("BUSINESS_RESPONSE_INVALID") from None
        if not isinstance(value, dict) or value.get("error"):
            raise BoundaryError("BUSINESS_RESPONSE_INVALID")
        return value

    def _authorize(self, principal, alias):
        # Local actor and target filter always precede a request to the project platform.
        if (principal["user"] != self.settings["allowed_user"]
                or alias not in self.settings["targets"] or not self.g.authorized(principal)):
            raise BoundaryError("BUSINESS_SCOPE_DENIED")
        identity = self._run(["user", "me"])
        if identity.get("user_key") != self.settings["user_key"]:
            raise BoundaryError("BUSINESS_IDENTITY_CHANGED")
        return self.settings["targets"][alias]

    @staticmethod
    def _fields(data):
        fields = data.get("work_item_fields")
        if not isinstance(fields, list):
            raise BoundaryError("BUSINESS_FIELDS_INVALID")
        result = {}
        for field in fields:
            if (not isinstance(field, dict) or not isinstance(field.get("key"), str)
                    or field["key"] in result):
                raise BoundaryError("BUSINESS_FIELDS_INVALID")
            result[field["key"]] = field.get("value")
        return result

    @staticmethod
    def _files(raw):
        if isinstance(raw, str):
            try: raw = json.loads(raw)
            except ValueError: raise BoundaryError("BUSINESS_FILES_INVALID") from None
        if raw is None:
            return None
        if not isinstance(raw, list) or len(raw) > 50 or any(not isinstance(f,dict) for f in raw):
            raise BoundaryError("BUSINESS_FILES_INVALID")
        return raw

    def _metadata(self, binding):
        # Use the same paginated CLI reader as the information source adapter.
        # A missing page/completeness marker never authorizes a write.
        rows = []
        for page in range(1, 21):
            try:
                data = self._run(["workitem", "meta-fields", "--project-key", binding["space"],
                                  "--work-item-type", binding["type"], "--page-num", str(page)])
            except BoundaryError as exc:
                code = str(exc)
                if code not in {"BUSINESS_CALL_UNCERTAIN", "BUSINESS_CALL_FAILED",
                                "BUSINESS_RESPONSE_LIMIT", "BUSINESS_RESPONSE_INVALID"}:
                    raise
                raise TargetValidationError("BUSINESS_METADATA_UNAVAILABLE",
                    cause=code if re.fullmatch(r"[A-Z][A-Z0-9_]{0,100}", code) else "BOUNDARY_ERROR") from None
            pagination = data.get("pagination") if isinstance(data, dict) else None
            if (not isinstance(data, dict) or not isinstance(data.get("list"), list)
                    or not isinstance(pagination, dict) or type(pagination.get("has_more")) is not bool):
                raise TargetValidationError("BUSINESS_METADATA_UNAVAILABLE", check="complete_metadata_page")
            rows.extend(data["list"])
            if len(rows) > 4096:
                raise TargetValidationError("BUSINESS_METADATA_LIMIT")
            if not pagination["has_more"]:
                return validate_metadata(binding, rows)
        raise TargetValidationError("BUSINESS_METADATA_LIMIT")

    def _state(self, principal, alias):
        with self.read_scope(principal):
            self._read_timeout()
            result = self._read_state(principal, alias)
            self._read_timeout()
            return result

    def _read_state(self, principal, alias):
        b = binding_config(self._authorize(principal, alias))
        metadata = self._metadata(b)
        project = self._run(["workitem", "get", "--project-key", b["space"],
                             "--work-item-id", b["project"]])
        project_attr = project_identity(b, project)
        node = self._run(["workflow","get-node","--project-key",b["space"],
                          "--work-item-id",b["project"],"--node-id-list",b["node"],"--need-sub-task"])
        nodes = node.get("list", [])
        pagination = node.get("pagination")
        if (not isinstance(nodes, list) or len(nodes) != 1 or not isinstance(nodes[0], dict)
                or not isinstance(nodes[0].get("basic"), dict)
                or nodes[0]["basic"].get("node_key") != b["node"]
                or not isinstance(pagination, dict) or type(pagination.get("has_more")) is not bool
                or pagination["has_more"]):
            raise BoundaryError("BUSINESS_NODE_UNVERIFIED")
        groups = nodes[0].get("sub_workitems")
        if (not isinstance(groups, list) or any(not isinstance(group, dict)
                or not isinstance(group.get("workitems"), list) for group in groups)):
            raise BoundaryError("BUSINESS_NODE_UNVERIFIED")
        members = [str(i) for group in groups for i in group["workitems"]
                   if not isinstance(i, bool) and isinstance(i, (str, int))]
        if b["deliverable"] not in members:
            raise BoundaryError("BUSINESS_PARENT_CHANGED")
        keys = [b["document_link_field"],b["approval_field"],b["node_field"],b["project_field"],
                b["document_kind_field"],b["delivery_mode_field"],"updated_at"]
        item = self._run(["workitem","get","--project-key",b["space"],"--work-item-id",b["deliverable"],
                          "--fields",",".join(keys)])
        attr, fields = item.get("work_item_attribute",{}), self._fields(item)
        if (not isinstance(attr, dict) or str(attr.get("work_item_id")) != b["deliverable"]
                or not isinstance(attr.get("owned_project"), dict)
                or attr["owned_project"].get("key") != b["space"]
                or not isinstance(attr.get("work_item_type"), dict)
                or attr["work_item_type"].get("key") != b["type"]):
            raise BoundaryError("BUSINESS_TARGET_CHANGED")
        identity, kind = target_identity(b, fields, metadata)
        binding_hash = immutable_binding(b, metadata, identity, kind)
        snapshot = {"id":attr["work_item_id"], "type":attr["work_item_type"]["key"],
                    "name":attr.get("work_item_name"), "update_time":attr.get("update_time"),
                    "status":attr.get("work_item_status"), "fields":fields,
                    "node_status":nodes[0]["basic"].get("status"),
                    "project_name":project_attr.get("work_item_name"),
                    "project_update_time":project_attr.get("update_time"),
                    "binding_fingerprint":binding_hash}
        revision = digest(json.dumps(snapshot, ensure_ascii=False, sort_keys=True))
        target = {"id":alias,"project_id":b["project"],"node_id":b["node"],"deliverable_id":b["deliverable"],
                  "revision":revision,"business_status":attr.get("work_item_status"),
                  "professional_review":fields.get(b["approval_field"]),
                  "url":b["url"], "document_identity":identity,
                  "document_kind_field":b["document_kind_field"],
                  "document_link_field":b["document_link_field"],
                  "binding_fingerprint":binding_hash, "field_metadata_fingerprint":fingerprint(metadata)}
        return b, target, fields, self._files(fields.get(b["document_link_field"]))

    def resolve(self, principal, target_id):
        return self._state(principal, target_id)[1]

    def document_identity(self, principal, target):
        _,current,_,_=self._state(principal,target['id'])
        if current!=target:raise BoundaryError('TARGET_REVISION_CHANGED')
        return dict(current['document_identity'])

    def requirements(self,principal,target):
        b,current,_,_=self._state(principal,target["id"])
        if current!=target:raise BoundaryError("TARGET_REVISION_CHANGED")
        value=b.get("precheck_requirements")
        return value if isinstance(value,dict) else None

    def _download(self, b, file, destination):
        url = file.get("url") or file.get("file_url")
        if not isinstance(url,str) or not url:
            raise BoundaryError("BUSINESS_FILE_URL_MISSING")
        self._run(["attachment","+download",url,"--project-key",b["space"],
                   "--work-item-id",b["deliverable"],"--output",str(destination)], json_result=False)
        if not destination.is_file() or destination.stat().st_size > MAX_BYTES:
            raise BoundaryError("BUSINESS_FILE_LIMIT")
        return destination.read_bytes()

    def sources(self, principal, target):
        b, current, _, files = self._state(principal, target["id"])
        if current != target:
            raise BoundaryError("TARGET_REVISION_CHANGED")
        results=[]
        for n,file in enumerate(files or [],1):
            if n > 6:
                raise BoundaryError("BUSINESS_SOURCE_LIMIT")
            with tempfile.TemporaryDirectory(dir=self.tmp) as tmp:
                try:
                    size=int(file.get("size",0))
                    if size <= 0 or size > MAX_BYTES: raise BoundaryError("BUSINESS_FILE_LIMIT")
                    data=self._download(b,file,Path(tmp)/"attachment")
                    src=read_source(file.get("name","unknown"),data,source_id=f"P{n:02d}",
                                    origin=target["deliverable_id"],provenance="current_project")
                except (BoundaryError,ValueError):
                    src={"id":f"P{n:02d}","name":file.get("name","unknown"),"version":None,
                         "formed_at":None,"origin":target["deliverable_id"],"provenance":"current_project",
                         "segments":[],"read_status":"failed","limitation":"本项目已提供的关联附件暂不可读，未当作没有资料"}
                results.append(src)
        return results

    def commit(self, principal, target, content, content_hash, idem, guard):
        started=[False]
        try:return self._commit(principal,target,content,content_hash,idem,guard,started)
        except Exception as exc:
            if not started[0]:raise BusinessNotStarted(str(exc)) from None
            raise

    def _commit(self, principal, target, content, content_hash, idem, guard, started):
        alias=target["id"]
        b=self._authorize(principal,alias)
        # Serialize this service's target writes. The platform API has no exposed CAS field;
        # compare again immediately before update and verify exact bytes after it.
        lockpath=self.c.root/"state"/("business-"+digest(alias)[:16]+".lock")
        with lockpath.open("a") as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            b,current,fields,files=self._state(principal,alias)
            if current != target or not guard():
                raise BoundaryError("TARGET_REVISION_CHANGED")
            approval=fields.get(b["approval_field"])
            if not isinstance(approval,dict) or approval.get("value") != b["unreviewed_value"]:
                raise BoundaryError("BUSINESS_REVIEWED_CONTENT_PROTECTED")
            if files is None:
                # Missing is not silently interpreted as empty. Only the explicitly UI-verified
                # initial empty slot at this exact fingerprint can be initialized.
                if target["revision"] != b.get("verified_empty_revision"):
                    raise BoundaryError("BUSINESS_ATTACHMENT_STATE_UNKNOWN")
                files=[]
            if len(files)>=50:
                raise BoundaryError("BUSINESS_ATTACHMENT_LIMIT")
            # A known-unpreservable existing file must fail before uploading a
            # new object; do not create an uncertain external side effect first.
            previous=[]
            for f in files:
                token=f.get("fileToken") or f.get("file_token")
                if not token or not all(k in f for k in ("name","type","size")):
                    raise BoundaryError("BUSINESS_EXISTING_ATTACHMENT_UNVERIFIED")
                previous.append({"name":f["name"],"type":f["type"],"size":str(f["size"]),"fileToken":token})
            with tempfile.TemporaryDirectory(dir=self.tmp) as tmp:
                path=Path(tmp)/f"logic-{idem[:16]}-{content_hash[:12]}.md"
                path.write_text(content,encoding="utf-8");path.chmod(0o600)
                if digest(path.read_bytes()) != content_hash:
                    raise BoundaryError("BUSINESS_CONTENT_CHANGED")
                # From this boundary onward a missing receipt is ambiguous.
                self.s.execute("UPDATE logic_writes SET receipt=? WHERE idem=? AND state='unknown'",
                               (json.dumps({'phase':'upload_started'}),idem))
                started[0]=True
                upload=self._run(["attachment","+upload",str(path),"--resource-type","15",
                                  "--project-key",b["space"],"--work-item-id",b["deliverable"],
                                  "--field-key",b["document_link_field"],"--content-type","text/markdown"])
                if not all(upload.get(k) for k in ("file_token","name","size","mime_type")):
                    raise BoundaryError("BUSINESS_UPLOAD_UNCERTAIN")
                self.s.execute("UPDATE logic_writes SET receipt=? WHERE idem=? AND state='unknown'",
                               (json.dumps({"phase":"uploaded","upload":upload,"previous_tokens":[f.get("fileToken") or f.get("file_token") for f in files]}),idem))
                # Recheck after upload and immediately before changing the attachment field.
                _,fresh,fresh_fields,fresh_files=self._state(principal,alias)
                if fresh != target or not guard():
                    raise BoundaryError("TARGET_REVISION_CHANGED")
                new={"name":upload["name"],"type":upload["mime_type"],"size":str(upload["size"]),"fileToken":upload["file_token"]}
                self._run(["workitem","update","--project-key",b["space"],"--work-item-id",b["deliverable"]],
                          params={"fields":[{"field_key":b["document_link_field"],
                                             "field_value":json.dumps(previous+[new],ensure_ascii=False)}]},json_result=False)
                result=self.reconcile(principal,idem)
                if result is None:
                    raise BoundaryError("BUSINESS_WRITE_UNCERTAIN")
                return result

    def reconcile(self, principal, idem):
        rows=self.s.rows("SELECT * FROM logic_writes WHERE idem=?",(idem,))
        if len(rows)!=1: return None
        write=rows[0];target=json.loads(write["target"]);b,current,_,files=self._state(principal,target["id"])
        # The successful append changes the item revision and file list. Re-read
        # the exact target contract, not its pre-write mutable revision.
        if (not target.get("binding_fingerprint")
                or current.get("binding_fingerprint") != target["binding_fingerprint"]):
            raise BoundaryError("BUSINESS_TARGET_BINDING_CHANGED")
        name=f"logic-{idem[:16]}-{write['content_hash'][:12]}.md"
        matches=[f for f in (files or []) if f.get("name")==name]
        if len(matches)!=1:
            return None
        file=matches[0]
        saved=json.loads(write["receipt"]) if write["receipt"] else {}
        upload=saved.get("upload",{})
        previous=set(saved.get("previous_tokens",[]))
        actual={f.get("fileToken") or f.get("file_token") for f in files or []}
        if previous-actual:raise BoundaryError("BUSINESS_EXISTING_ATTACHMENT_LOST")
        token=file.get("fileToken") or file.get("file_token")
        if not token: return None
        if not file.get("url") and not file.get("file_url") and token==upload.get("file_token"):
            file={**file,"file_url":upload.get("file_url")}
        with tempfile.TemporaryDirectory(dir=self.tmp) as tmp:
            if digest(self._download(b,file,Path(tmp)/"verify.md")) != write["content_hash"]:
                raise BoundaryError("BUSINESS_READBACK_MISMATCH")
        return {"target_id":target["id"],"content_hash":write["content_hash"],"object_id":token,"url":b["url"],"previous_tokens":sorted(previous)}

def load_meegle_port(gateway):
    path=gateway.config.root/"config"/"document-business.json"
    if not path.exists():
        return None
    stat=path.stat()
    if path.is_symlink() or stat.st_uid!=os.getuid() or stat.st_mode & 0o077:
        raise BoundaryError("BUSINESS_CONFIG_NOT_PRIVATE")
    try:settings=json.loads(path.read_text())
    except (OSError,ValueError):raise BoundaryError("BUSINESS_CONFIG_INVALID") from None
    return MeeglePort(gateway,settings)
