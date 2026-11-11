from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import platform
import pwd
import signal
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

from .config import BoundaryError, Config
from .feishu import Feishu
from .gateway import Gateway
from .provider import atomic_json
from .qwen_provider import LocalQwenProvider
from .store import Store


@contextmanager
def instance_lock(root: Path):
    with (root / "state" / "instance.lock").open("a+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BoundaryError("INSTANCE_ALREADY_RUNNING") from exc
        yield


def doctor(path: Path):
    data = json.loads(path.read_text())
    missing = []
    if data.get("template_only"):
        missing.append("CONFIG_IS_TEMPLATE")
    name = data.get("service", {}).get("user")
    try:
        pwd.getpwnam(name or "_fsagent")
    except KeyError:
        missing.append("DEDICATED_SERVICE_USER")
    is_qwen = data.get("provider", {}).get("kind") == "local_qwen_api"
    if not is_qwen:
        missing.append("QWEN_RUNTIME_REQUIRED")
    for section, keys in {
        "feishu": ["app_id", "tenant_key", "bot_open_id", "allowed_open_ids", "allowed_department_ids",
                    "department_match", "owner_notification_open_id", "exclusive_test_consumer_confirmed"],
    }.items():
        for key in keys:
            if not data.get(section, {}).get(key):
                missing.append(f"{section}.{key}")
    for section, key in (("feishu", "app_secret_reference"),):
        value = data.get(section, {}).get(key)
        if not value or not Path(value).is_file():
            missing.append(f"{section}.{key}")
    root = data.get("service", {}).get("root_directory")
    if not root or not (Path(root) / "state" / "provider-pin.json").exists():
        missing.append("REAL_P1_AND_MODEL_PIN")
    result = {"platform": platform.platform(), "python": platform.python_version(),
              "sqlite": sqlite3.sqlite_version, "missing": missing,
              "note": "Read-only presence check; does not read credentials or invoke Grok/Feishu."}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 2 if missing else 0


def main():
    os.umask(0o077)
    logging.disable(logging.CRITICAL)  # SDK exception logs can contain full remote payloads.
    parser = argparse.ArgumentParser(description="Feishu synthetic PoC — explicitly configured local provider")
    parser.add_argument("command", choices=["doctor", "probe", "login", "p1", "serve", "status", "resume", "configure"])
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "doctor":
        return doctor(args.config)
    try:
        config = Config.load(args.config, provider_setup=args.command in ("probe", "login", "p1", "configure"))
        # Historical Grok files are retained for reference only. Reject their
        # configuration before credentials, processes or clients are touched.
        if config.provider_kind != "local_qwen_api":
            raise BoundaryError("QWEN_RUNTIME_REQUIRED")
        if args.command == "serve" and config.event_transport == "existing_bridge":
            raise BoundaryError("EXISTING_BRIDGE_REQUIRED")
        config.check_service()
        provider = LocalQwenProvider(config)
        with instance_lock(config.root):
            store = Store(config.db)
            try:
                if args.command == "configure":
                    from .setup import configure
                    configure(config, args.config)
                elif args.command == "login":
                    raise BoundaryError("LOCAL_QWEN_NO_LOGIN_REQUIRED")
                elif args.command == "probe":
                    receipt = provider.fingerprint()
                    atomic_json(config.root / "state" / "provider-probe.json", receipt)
                    print(json.dumps(receipt, indent=2))
                elif args.command == "status":
                    print(json.dumps({"tasks": store.rows("SELECT state,COUNT(*) AS count FROM tasks GROUP BY state"),
                                      "deliveries": store.rows("SELECT state,COUNT(*) AS count FROM outbox GROUP BY state"),
                                      "provider_pause": store.meta("provider_pause")}, indent=2))
                elif args.command == "resume":
                    # Explicit local operator action after credentials/quota/network recovery.
                    if store.rows("SELECT id FROM tasks WHERE state='stop_unknown'"):
                        raise BoundaryError("STOP_UNKNOWN_REQUIRES_INVESTIGATION")
                    provider.pin()
                    store.execute("DELETE FROM meta WHERE key='provider_pause'")
                    print('{"resumed":true}')
                elif args.command == "p1":
                    if any((config.root / "tasks").iterdir()):
                        raise BoundaryError("TASK_DIRECTORY_RECOVERY_REQUIRED")
                    print(json.dumps(provider.p1(), ensure_ascii=False, indent=2))
                else:
                    config.check_feishu()
                    provider.pin()  # A real P1 receipt is mandatory before an event consumer starts.
                    api = Feishu(config)
                    gateway = Gateway(config, store, api, provider)
                    keep_awake = None
                    def stop(*_):
                        raise KeyboardInterrupt
                    signal.signal(signal.SIGTERM, stop)
                    signal.signal(signal.SIGINT, stop)
                    try:
                        gateway.start()
                        keep_awake = subprocess.Popen(["/usr/bin/caffeinate", "-i", "-w", str(os.getpid())],
                                                      stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        api.socket(gateway).start()
                    except KeyboardInterrupt:
                        pass
                    finally:
                        gateway.stop()
                        if keep_awake:
                            keep_awake.terminate()
                            keep_awake.wait(timeout=5)
            finally:
                store.close()
    except BoundaryError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    except Exception as exc:
        # Never print exception text from third-party libraries or configuration values.
        print(json.dumps({"error": "UNEXPECTED_ERROR", "type": type(exc).__name__}), file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
