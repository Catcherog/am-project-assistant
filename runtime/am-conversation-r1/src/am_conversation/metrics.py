"""Content-free timing records. No messages, knowledge, arguments or credentials."""
from __future__ import annotations

from collections import defaultdict
import json
import math
from pathlib import Path
import threading


class JsonlMetrics:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def __call__(self, value: dict):
        record = {k: value[k] for k in ("event_id", "span", "seconds")}
        if not isinstance(record["span"], str) or not isinstance(record["event_id"], str):
            raise ValueError("INVALID_METRIC")
        if not isinstance(record["seconds"], (int, float)) or not math.isfinite(record["seconds"]) or record["seconds"] < 0:
            raise ValueError("INVALID_METRIC")
        with self._lock, self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")


def summarize(records: list[dict]) -> dict:
    spans = defaultdict(list)
    for row in records:
        spans[row["span"]].append(float(row["seconds"]))
    output = {}
    for name, values in spans.items():
        ordered = sorted(values)
        output[name] = {"count": len(ordered), "total_seconds": sum(ordered),
                        "p50_seconds": ordered[max(0, math.ceil(len(ordered) * .5) - 1)],
                        "p95_seconds": ordered[max(0, math.ceil(len(ordered) * .95) - 1)]}
    return {"spans": output, "scope": "server spans only; client delivery and live Qwen latency require actual measurements"}
