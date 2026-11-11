"""A bounded, disposable HTTP client process; never an owner of the shared model server."""
import json
import os
import sys
from pathlib import Path

from .config import BoundaryError
from .local_api import LocalQwenClient
from .provider import atomic_json


def main():
    os.umask(0o077)
    path = Path(sys.argv[1])
    try:
        data = json.loads(path.read_text())
        client = LocalQwenClient(data["base_url"], data["model"], data["model_digest"])
        def phase(state):
            atomic_json(path.parent / "api-state.json", {"state": state})
        options = {"json_schema": data["json_schema"]} if "json_schema" in data else {}
        if data.get("native_chat") is True:
            options["native_chat"] = True
        result = client.complete(data["prompt"], timeout=data["timeout"], max_tokens=data["max_tokens"], phase=phase, **options)
        print(json.dumps({"text": result, "model": data["model"], "usage":client.last_usage}, ensure_ascii=False))
        return 0
    except BoundaryError as exc:
        print(json.dumps({"error": str(exc)}))
        return 2
    except Exception:
        print('{"error":"LOCAL_API_WORKER_FAILED"}')
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
