"""Deterministic evidence helpers; content hashes are not authentication."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re


class R5Error(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def text_hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def object_hash(value):
    return text_hash(canonical(value))


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require_sha(value, label="sha256"):
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise R5Error("invalid " + label)
    return value


def seal(value, key="sha256"):
    out = deepcopy(value)
    out.pop(key, None)
    out[key] = object_hash(out)
    return out


def check_seal(value, key="sha256"):
    if not isinstance(value, dict) or value.get(key) != object_hash({k: v for k, v in value.items() if k != key}):
        raise R5Error("changed or missing content seal: " + key)
    return value


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise R5Error("duplicate JSON key: " + key)
        result[key] = value
    return result


def loads(text):
    def bad(value):
        raise R5Error("non-finite JSON: " + value)
    return json.loads(text, object_pairs_hook=_unique, parse_constant=bad)


def read_json(path):
    return loads(Path(path).read_text(encoding="utf-8"))


def read_jsonl(path):
    return [loads(line) for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        output.write(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    return file_hash(path)


def write_jsonl(path, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        output.write("".join(canonical(row) + "\n" for row in rows))
    return file_hash(path)
