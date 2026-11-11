from __future__ import annotations

import json
import os
import pwd
import stat
from dataclasses import dataclass
from pathlib import Path


class BoundaryError(RuntimeError):
    """A safe, body-free reason for refusing an operation."""


@dataclass(frozen=True)
class Config:
    service_user: str
    root: Path
    executable: Path
    credential_file: Path
    app_secret_file: Path
    app_id: str
    tenant_key: str
    bot_open_id: str
    users: tuple[str, ...]
    departments: tuple[str, ...]
    chats: tuple[str, ...]
    owner: str
    exclusive_consumer: bool
    department_match: str = "any_exact"
    concurrency: int = 2
    queue_seconds: int = 600
    execution_seconds: int = 120
    ttl: int = 86400
    auth_method: str = "dedicated_xai_api_key"
    provider_kind: str = "grok_cli"
    api_base_url: str = "http://127.0.0.1:11434/v1"
    api_model: str = "qwen3.8:27b-bartowski-q8_0"
    api_model_digest: str = ""
    api_max_tokens: int = 1024
    event_transport: str = "standalone"
    runtime_mode: str = "dedicated"
    document_logic: bool = False
    reminders_enabled: bool = False

    @property
    def db(self) -> Path:
        return self.root / "state" / "gateway.sqlite3"

    @classmethod
    def load(cls, path: Path, *, provider_setup: bool = False) -> Config:
        data = json.loads(path.read_text())
        if data.get("template_only", True) and not provider_setup:
            raise BoundaryError("CONFIG_IS_TEMPLATE")
        try:
            s, g, f = data["service"], data.get("grok", {}), data["feishu"]
            p = data.get("provider", {"kind": "grok_cli"})
            root = Path(s["root_directory"])
            obj = cls(
                service_user=s["user"], root=root,
                executable=Path(g.get("executable", root / "bin" / "grok-disabled")),
                credential_file=Path(g.get("dedicated_credential_reference", root / "secrets" / "unused")),
                app_secret_file=Path(f["app_secret_reference"]),
                app_id=f["app_id"], tenant_key=f["tenant_key"], bot_open_id=f["bot_open_id"],
                users=tuple(f["allowed_open_ids"]), departments=tuple(f["allowed_department_ids"]),
                chats=tuple(f["allowed_test_chat_ids"]), owner=f["owner_notification_open_id"],
                exclusive_consumer=f["exclusive_test_consumer_confirmed"],
                department_match=f.get("department_match") or "",
                concurrency=data["limits"]["global_concurrency"],
                auth_method=g.get("auth_method", "dedicated_xai_api_key"),
                provider_kind=p["kind"], api_base_url=p.get("base_url", "http://127.0.0.1:11434/v1"),
                api_model=p.get("model", "qwen3.8:27b-bartowski-q8_0"),
                api_model_digest=p.get("model_digest", ""), api_max_tokens=p.get("max_tokens", 1024),
                event_transport=f.get("event_transport", "standalone"),
                runtime_mode=s.get("mode", "dedicated"),
                document_logic=data.get("document_logic", {}).get("enabled", False),
                reminders_enabled=data.get("group_reminder", {}).get("enabled", False),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BoundaryError("CONFIG_INCOMPLETE") from exc
        if type(obj.document_logic) is not bool or set(data.get("document_logic", {})) - {"enabled"}:
            raise BoundaryError("DOCUMENT_LOGIC_CONFIG_INVALID")
        if type(obj.reminders_enabled) is not bool or set(data.get("group_reminder", {})) - {"enabled"}:
            raise BoundaryError("REMINDER_CONFIG_INVALID")
        if obj.reminders_enabled and obj.provider_kind != "local_qwen_api":
            raise BoundaryError("REMINDER_QWEN_REQUIRED")
        if obj.document_logic and obj.provider_kind != "local_qwen_api":
            raise BoundaryError("DOCUMENT_LOGIC_QWEN_REQUIRED")
        if obj.provider_kind not in ("grok_cli", "local_qwen_api"):
            raise BoundaryError("UNSUPPORTED_PROVIDER")
        if obj.runtime_mode == "existing_login":
            if obj.provider_kind != "local_qwen_api" or obj.event_transport != "existing_bridge":
                raise BoundaryError("EXISTING_BRIDGE_REQUIRED")
            try:
                account_home = Path(pwd.getpwnam(obj.service_user).pw_dir)
            except KeyError as exc:
                raise BoundaryError("SERVICE_USER_MISSING") from exc
            expected_root = account_home / ".local/share/local-qwen-feishu/poc"
        elif obj.runtime_mode == "dedicated":
            expected_root = Path("/Library/FSAgentPoC" if obj.provider_kind == "local_qwen_api" else "/Library/FSAgentGrok")
        else:
            raise BoundaryError("UNSUPPORTED_RUNTIME_MODE")
        if not obj.root.is_absolute() or obj.root.resolve() != expected_root:
            raise BoundaryError("DEDICATED_ROOT_REQUIRED")
        if obj.concurrency not in (1, 2):
            raise BoundaryError("UNSUPPORTED_POLICY")
        if obj.auth_method not in ("dedicated_xai_api_key", "dedicated_device_oauth"):
            raise BoundaryError("UNSUPPORTED_AUTH_METHOD")
        if obj.provider_kind == "local_qwen_api":
            from .local_api import validate_endpoint
            validate_endpoint(obj.api_base_url)
            if obj.concurrency != 1 or obj.api_model != "qwen3.8:27b-bartowski-q8_0" or obj.api_max_tokens != 1024:
                raise BoundaryError("QWEN_POLICY_CHANGED")
            if obj.event_transport != "existing_bridge":
                raise BoundaryError("EXISTING_BRIDGE_REQUIRED")
        elif obj.auth_method == "dedicated_device_oauth" and obj.concurrency != 1:
            # Start with one real lane: rotating OAuth refresh tokens must not race.
            raise BoundaryError("OAUTH_SINGLE_LANE_REQUIRED")
        expected = {"per_user_concurrency": 1, "queue_timeout_seconds": 600,
                    "execution_timeout_seconds": 120, "message_ttl_seconds": 86400}
        if any(data["limits"].get(k) != v for k, v in expected.items()):
            raise BoundaryError("LIMITS_CHANGED")
        if data.get("features") != {
            "routes": ["doc", "progress"] + (["remind"] if obj.reminders_enabled else []), "synthetic_data_only": True,
            "memory_enabled": False, "subagents_enabled": False,
            "search_enabled": False, "personal_connectors_enabled": False,
            "provider_fallback_enabled": False,
        }:
            raise BoundaryError("FEATURE_BOUNDARY_CHANGED")
        return obj

    def check_service(self) -> None:
        try:
            account = pwd.getpwnam(self.service_user)
        except KeyError as exc:
            raise BoundaryError("SERVICE_USER_MISSING") from exc
        if os.getuid() == 0 or os.getuid() != account.pw_uid:
            raise BoundaryError("WRONG_SERVICE_USER")
        if self.runtime_mode == "dedicated" and Path(account.pw_dir) != self.root / "home":
            raise BoundaryError("SERVICE_HOME_NOT_DEDICATED")
        if self.runtime_mode == "existing_login" and self.root != Path(account.pw_dir) / ".local/share/local-qwen-feishu/poc":
            raise BoundaryError("SERVICE_DIRECTORY_INVALID")
        for p in (self.root, self.root / "home", self.root / "state", self.root / "tasks",
                  self.root / "artifacts", self.root / "secrets"):
            if p.is_symlink() or not p.is_dir() or p.stat().st_uid != os.getuid():
                raise BoundaryError("SERVICE_DIRECTORY_INVALID")
            if p.stat().st_mode & 0o077:
                raise BoundaryError("SERVICE_DIRECTORY_NOT_PRIVATE")
        if self.provider_kind == "local_qwen_api":
            return
        if not self.executable.is_absolute() or self.executable.is_symlink():
            raise BoundaryError("CLI_PATH_INVALID")
        if not self.executable.resolve().is_relative_to(self.root / "bin"):
            raise BoundaryError("CLI_NOT_DEDICATED")
        if not self.executable.is_file() or not os.access(self.executable, os.X_OK):
            raise BoundaryError("CLI_MISSING")

    def check_feishu(self) -> None:
        if self.department_match not in ("any_exact", "all_exact"):
            raise BoundaryError("DEPARTMENT_MATCH_UNCONFIRMED")
        if len(set(self.users)) != 2 or len(self.users) != 2 or not all(x.startswith("ou_") for x in self.users):
            raise BoundaryError("EXACT_TWO_USERS_REQUIRED")
        if not self.departments or self.owner not in self.users:
            raise BoundaryError("DEPARTMENT_OR_OWNER_MISSING")
        if not all(isinstance(x, str) and x for x in (self.app_id, self.tenant_key, self.bot_open_id)):
            raise BoundaryError("FEISHU_IDENTITIES_MISSING")
        if not self.exclusive_consumer:
            raise BoundaryError("EXCLUSIVE_TEST_CONSUMER_UNCONFIRMED")

    def read_secret(self, path: Path) -> str:
        existing_app_secret = (self.runtime_mode == "existing_login" and
                               path == self.root.parent / "secrets" / "app_secret" and
                               path.resolve() == path)
        if path.is_symlink() or not (existing_app_secret or path.resolve().is_relative_to(self.root / "secrets")):
            raise BoundaryError("SECRET_NOT_DEDICATED")
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError as exc:
            raise BoundaryError("SECRET_MISSING") from exc
        with os.fdopen(fd) as stream:
            st = os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
                raise BoundaryError("SECRET_PERMISSIONS_INVALID")
            value = stream.read(8193).strip()
        if not value or len(value) > 8192 or "\n" in value or value.startswith("REPLACE"):
            raise BoundaryError("SECRET_INVALID")
        return value

    def read_auth_json(self, path: Path) -> dict:
        if path.is_symlink() or not any(path.resolve().is_relative_to(self.root / p) for p in ("secrets", "tasks")):
            raise BoundaryError("AUTH_NOT_DEDICATED")
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError as exc:
            raise BoundaryError("AUTH_MISSING") from exc
        with os.fdopen(fd) as stream:
            st = os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
                raise BoundaryError("AUTH_PERMISSIONS_INVALID")
            raw = stream.read(65537)
        try:
            value = json.loads(raw)
        except ValueError as exc:
            raise BoundaryError("AUTH_INVALID") from exc
        if not isinstance(value, dict) or not value or len(raw) > 65536:
            raise BoundaryError("AUTH_INVALID")
        return value
