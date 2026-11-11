"""Grok routing for bot private chat and group @ answers.

The route reads the knowledge directory and decides the reply text. It never
opens a Feishu connection, never reads app secrets, and never sends as the user.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import threading
import time
import unicodedata

from grok_backend import GrokBackend, GrokError

NO_BASIS = "这个需要和示例负责人确认"
USER_PREFIX = "【来自 Grok 助手】"
FAILURE_TEXT = "这次没能生成回答，请稍后再试。"
KNOWLEDGE_DEFAULT = "/path/to/am-knowledge"
SIDECAR_DEFAULT = Path.home() / ".local/share/local-qwen-feishu/grok_routing.json"
SENSITIVE_KEY = re.compile(r"secret|token|password|credential|private_key", re.I)
SENSITIVE_LINE = re.compile(r"(?i)(app_secret|access_token|refresh_token|private_key|authorization:\s*bearer)\b")
KNOWLEDGE_DIRS = {"最新台账导出", "项目近况", "口径规则"}
STOP = {
    "什么", "怎么", "如何", "是否", "可以", "我们", "这个", "那个", "项目", "进展",
    "进度", "现在", "最近", "一下", "请问", "你好", "还是", "以及", "如果", "没有",
    "需要", "需求", "什么时候", "测试", "日期", "负责", "负责人",
}
INTERNAL = re.compile(
    r"需求\s*\d+|台账|口径|到料|排产|样机|减速|DVT|OTA|项目|进度|进展|负责人|谁负责|交期|排期|采购|示例负责人|DemoTuner|安规",
    re.I,
)
ACTION_CLAIM = re.compile(r"已(?:修改|更新|发送|提交|删除|批准|完成)")


def drop_secrets(value):
    if isinstance(value, dict):
        return {k: drop_secrets(v) for k, v in value.items() if not SENSITIVE_KEY.search(str(k))}
    if isinstance(value, list):
        return [drop_secrets(item) for item in value]
    return value


def merge_grok_config(config, *, sidecar_path=None, use_sidecar=False):
    """Overlay grok keys. Sidecar wins. Secret-like keys from the sidecar are dropped."""
    if not isinstance(config, dict):
        return {}
    merged = dict(config)
    if not use_sidecar:
        return merged
    path = Path(sidecar_path) if sidecar_path else SIDECAR_DEFAULT
    try:
        if not path.is_file() or path.is_symlink():
            return merged
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeError):
        return merged
    if not isinstance(raw, dict):
        return merged
    raw = drop_secrets(raw)
    routing = dict(merged.get("grok_routing") or {})
    extra = raw.get("grok_routing")
    if isinstance(extra, dict):
        routing.update(drop_secrets(extra))
    merged["grok_routing"] = routing
    for key in ("knowledge_dir", "grok_bin", "send_live"):
        if key in raw and not SENSITIVE_KEY.search(key):
            merged[key] = raw[key]
    return merged


@dataclass
class GrokAnswer:
    status: str
    reply_text: str
    kind: str
    chat_id: str = ""
    message_id: str = ""
    actions: list = field(default_factory=list)
    send_live: bool = False
    source: str = "grok"

    @property
    def dry_run(self) -> bool:
        if self.status == "need_confirm":
            return True
        return self.send_live is not True

    @property
    def via_grok(self) -> bool:
        return True

    def as_record(self) -> dict:
        return {
            "schema": "gw.result.v1",
            "time": time.time(),
            "kind": self.kind,
            "status": self.status,
            "reply_text": self.reply_text,
            "actions": self.actions,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "deliver": "dry_run" if self.dry_run else "reply",
            "sent": False,
            "source": self.source,
        }


@dataclass
class GrokSettings:
    enabled: bool = False
    bot_p2p: bool = True
    group_mention: bool = True
    answer_backend: str = "qwen"
    on_failure: str = "fixed"
    failure_text: str = FAILURE_TEXT
    test_group_chat_id: str = "oc_SYNTHETIC_01"
    knowledge_dir: str = KNOWLEDGE_DEFAULT
    grok_bin: str = ""
    send_live: bool = False
    timeout_seconds: int = 90
    disclosure_chats: frozenset = field(default_factory=frozenset)
    outbox_path: str = ""
    user_project_enabled: bool = False
    user_project_require_preview: bool = True

    @property
    def uses_group(self) -> bool:
        return bool(self.enabled and self.group_mention and self.answer_backend == "grok")

    def allow_ledger(self, kind: str, chat_id: str) -> bool:
        if kind == "bot_p2p":
            return True
        if kind != "group_mention" or not chat_id:
            return False
        return chat_id == self.test_group_chat_id or chat_id in self.disclosure_chats


def load_grok_settings(config) -> GrokSettings:
    config = config if isinstance(config, dict) else {}
    routing = config.get("grok_routing") if isinstance(config.get("grok_routing"), dict) else {}
    routing = drop_secrets(routing)
    user_project = routing.get("user_project") if isinstance(routing.get("user_project"), dict) else {}
    disclosure = config.get("group_ledger") if isinstance(config.get("group_ledger"), dict) else {}
    chats = disclosure.get("disclosure") if isinstance(disclosure.get("disclosure"), dict) else {}
    state = config.get("state_directory")
    outbox = str(Path(state) / "grok-outbox.jsonl") if isinstance(state, str) and state else ""
    backend = routing.get("answer_backend")
    if backend not in ("grok", "qwen"):
        backend = "grok" if routing.get("enabled") is True else "qwen"
    on_failure = routing.get("on_failure") if routing.get("on_failure") in ("fixed", "qwen") else "fixed"
    failure = routing.get("failure_text")
    if not isinstance(failure, str) or not failure.strip():
        failure = FAILURE_TEXT
    timeout = routing.get("timeout_seconds", 90)
    if type(timeout) is not int or not 5 <= timeout <= 300:
        timeout = 90
    knowledge = routing.get("knowledge_dir", config.get("knowledge_dir", KNOWLEDGE_DEFAULT))
    if not isinstance(knowledge, str) or not knowledge.strip():
        knowledge = KNOWLEDGE_DEFAULT
    binary = routing.get("grok_bin", config.get("grok_bin", ""))
    if not isinstance(binary, str):
        binary = ""
    test_group = routing.get("test_group_chat_id") or "oc_SYNTHETIC_01"
    if not isinstance(test_group, str):
        test_group = "oc_SYNTHETIC_01"
    send_live = routing.get("send_live", config.get("send_live", False)) is True
    return GrokSettings(
        enabled=routing.get("enabled") is True,
        bot_p2p=routing.get("bot_p2p", True) is not False,
        group_mention=routing.get("group_mention", True) is not False,
        answer_backend=backend,
        on_failure=on_failure,
        failure_text=failure.strip()[:200],
        test_group_chat_id=test_group,
        knowledge_dir=knowledge,
        grok_bin=binary,
        send_live=send_live,
        timeout_seconds=timeout,
        disclosure_chats=frozenset(str(key) for key in chats),
        outbox_path=outbox,
        user_project_enabled=user_project.get("enabled") is True,
        user_project_require_preview=user_project.get("require_preview", True) is not False,
    )


def _salient(text: str) -> set[str]:
    text = unicodedata.normalize("NFKC", text or "")
    compact = re.sub(r"\s+", "", text)
    tokens = set(re.findall(r"需求\d+", compact))
    tokens.update(re.findall(r"[\u4e00-\u9fff]{2,12}", text))
    tokens.update(word.lower() for word in re.findall(r"[A-Za-z][A-Za-z0-9_-]{1,}", text))
    return {token for token in tokens if token not in STOP}


def _needs_internal(question: str) -> bool:
    return bool(INTERNAL.search(unicodedata.normalize("NFKC", question or "")))


def _read_knowledge(root: Path, *, allow_ledger: bool) -> list[tuple[str, str]]:
    if not allow_ledger or not root.is_dir():
        return []
    docs = []
    for path in sorted(root.rglob("*")):
        if len(docs) >= 40:
            break
        if not path.is_file() or path.is_symlink():
            continue
        if path.suffix.lower() not in {".md", ".txt", ".csv"}:
            continue
        try:
            rel = path.relative_to(root)
        except ValueError:
            continue
        if not rel.parts or rel.parts[0] not in KNOWLEDGE_DIRS:
            continue
        if any(part.startswith(".") or SENSITIVE_KEY.search(part) for part in rel.parts):
            continue
        try:
            raw = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        lines = []
        for line in raw.splitlines():
            if SENSITIVE_LINE.search(line):
                continue
            lines.append(line)
        text = "\n".join(lines).strip()
        if text:
            docs.append((rel.as_posix(), text[:4000]))
    return docs


def _excerpts(question: str, docs: list[tuple[str, str]], limit: int = 3500) -> list[tuple[str, str]]:
    question_tokens = _salient(question)
    ranked = []
    for name, text in docs:
        doc_tokens = _salient(text)
        hits = [token for token in question_tokens if token in text]
        hits += [token for token in doc_tokens if len(token) >= 3 and token in question]
        strong = any(token.startswith("需求") and token[2:].isdigit() for token in hits) or any(len(token) >= 3 for token in hits)
        if strong:
            ranked.append((len("".join(hits)), name, text))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    selected, size = [], 0
    for _, name, text in ranked[:3]:
        chunk = text[:1200]
        if size + len(chunk) > limit:
            chunk = chunk[: max(0, limit - size)]
        if not chunk:
            break
        selected.append((name, chunk))
        size += len(chunk)
    return selected


def _parse_output(raw: str) -> dict:
    text = (raw or "").strip()
    if text == NO_BASIS:
        return {"status": "need_owner", "reply_text": NO_BASIS, "actions": []}
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S)
    if fenced:
        text = fenced.group(1)
    else:
        start, end = text.find("{"), text.rfind("}")
        if 0 <= start < end:
            text = text[start:end + 1]
    try:
        value = json.loads(text)
    except (ValueError, TypeError):
        raise GrokError("GROK_BAD_OUTPUT") from None
    if not isinstance(value, dict):
        raise GrokError("GROK_BAD_OUTPUT")
    status = value.get("status")
    reply = value.get("reply_text")
    actions = value.get("actions") or []
    if status not in ("ok", "need_confirm", "need_owner", "error") or not isinstance(reply, str) or not reply.strip():
        raise GrokError("GROK_BAD_OUTPUT")
    if not isinstance(actions, list):
        raise GrokError("GROK_BAD_OUTPUT")
    if status == "need_owner":
        reply = NO_BASIS
    return {"status": status, "reply_text": reply.strip(), "actions": actions}


def _bot_text(text: str) -> str:
    text = text.replace("\x00", "").strip()
    if text.startswith(USER_PREFIX):
        text = text[len(USER_PREFIX):].lstrip()
    if len(text) > 4000:
        text = text[:4000] + "…"
    return text


class GrokRoute:
    def __init__(self, settings: GrokSettings, backend=None):
        self.settings = settings
        self.backend = backend or GrokBackend(settings.grok_bin or None, timeout=settings.timeout_seconds)
        self.records = []
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.settings.enabled

    @property
    def bot_p2p(self) -> bool:
        return self.settings.bot_p2p

    @property
    def group_mention(self) -> bool:
        return self.settings.group_mention

    @property
    def on_failure(self) -> str:
        return self.settings.on_failure

    @property
    def failure_text(self) -> str:
        return self.settings.failure_text

    @property
    def send_live(self) -> bool:
        return self.settings.send_live

    @property
    def answer_backend(self) -> str:
        return self.settings.answer_backend

    @property
    def uses_group(self) -> bool:
        return self.settings.uses_group

    def answer(self, *, kind, question, history=None, chat_id="", message_id="", route_hint=None):
        question = "" if question is None else str(question)
        if not question.strip():
            return self._pack(self.failure_text, status="error", kind=kind, chat_id=chat_id,
                              message_id=message_id, source="fixed")
        allow = self.settings.allow_ledger(kind, str(chat_id or ""))
        docs = _read_knowledge(Path(self.settings.knowledge_dir), allow_ledger=allow)
        excerpts = _excerpts(question, docs) if docs else []
        if _needs_internal(question) and not excerpts:
            return self._pack(NO_BASIS, status="need_owner", kind=kind, chat_id=chat_id,
                              message_id=message_id, source="no_basis")
        prompt = self._prompt(question, history or [], excerpts, route_hint)
        try:
            raw = self.backend.complete(prompt)
        except GrokError:
            raise
        except Exception:
            raise GrokError("GROK_UNAVAILABLE") from None
        parsed = _parse_output(raw)
        text = NO_BASIS if parsed["status"] == "need_owner" else _bot_text(parsed["reply_text"])
        if not text:
            raise GrokError("GROK_BAD_OUTPUT")
        if route_hint == "action" and ACTION_CLAIM.search(text):
            text = "我目前可以回答问题和查询项目台账，不能在群里替你执行这项操作。"
        actions = [self._user_action(item, chat_id=str(chat_id or ""), message_id=str(message_id or ""))
                   for item in parsed["actions"] if isinstance(item, dict) and item.get("as") == "user"]
        status = parsed["status"]
        if actions and status == "ok":
            # A user-identity send is never part of an automatic ok reply.
            status = "need_confirm"
        return self._pack(text, status=status, kind=kind, chat_id=str(chat_id or ""),
                          message_id=str(message_id or ""), actions=actions, source="grok")

    def answer_p2p(self, question, history, chat_id, message_id, qwen=None, qwen_paused=False):
        try:
            return self.answer(kind="bot_p2p", question=question, history=history,
                               chat_id=chat_id, message_id=message_id)
        except GrokError:
            if self.on_failure == "qwen" and qwen is not None and not qwen_paused:
                try:
                    text = qwen()
                except Exception:
                    text = None
                if isinstance(text, str) and text.strip():
                    return self._pack(text.strip()[:4000], status="ok", kind="bot_p2p", chat_id=str(chat_id or ""),
                                      message_id=str(message_id or ""), source="qwen_fallback")
            return self._pack(self.failure_text, status="error", kind="bot_p2p", chat_id=str(chat_id or ""),
                              message_id=str(message_id or ""), source="fixed")

    def note_fallback(self, text, *, kind, chat_id="", message_id=""):
        return self._pack(str(text or self.failure_text)[:4000], status="ok", kind=kind,
                          chat_id=str(chat_id or ""), message_id=str(message_id or ""), source="qwen_fallback")

    def plan_user_send(self, action, *, chat_id="", message_id="", kind="user_project"):
        """Skeleton only. User-identity sends stay need_confirm and are never delivered."""
        proposed = self._user_action(action or {}, chat_id=str(chat_id or ""), message_id=str(message_id or ""))
        answer = GrokAnswer(
            status="need_confirm", reply_text=proposed["text"], kind=kind,
            chat_id=str(chat_id or ""), message_id=str(message_id or ""),
            actions=[proposed], send_live=False, source="user_plan",
        )
        self._commit(answer)
        return answer

    def read_outbox(self) -> list:
        path = self.settings.outbox_path
        if path and Path(path).is_file():
            rows = []
            for line in Path(path).read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    continue
            return rows
        with self._lock:
            return list(self.records)

    def _prompt(self, question, history, excerpts, route_hint) -> str:
        pairs = []
        for item in list(history or [])[-4:]:
            if not isinstance(item, dict):
                continue
            asked = str(item.get("question") or item.get("usertext") or "")[:500]
            answered = str(item.get("answer") or "")[:500]
            if asked or answered:
                pairs.append({"question": asked, "answer": answered})
        excerpt_text = "\n\n".join(f"[{name}]\n{text}" for name, text in excerpts) or "（无）"
        extra = ""
        if route_hint == "help":
            extra = "对方在问机器人能做什么。只说明可以回答问题和按编号查询项目，不要声称执行过操作。\n"
        elif route_hint == "action":
            extra = "对方在要求修改、审批或发通知。说明目前不能执行该操作，不要声称已经完成。\n"
        return (
            "你是飞书机器人的回复草稿，只输出一个 JSON 对象，不要输出其它文字。\n"
            'JSON：{"status":"ok|need_confirm|need_owner|error","reply_text":"...","actions":[]}\n'
            "普通知识可以简短回答。涉及示例负责人的项目、需求、日期、人员或口径时，只能使用知识库摘录。\n"
            f"摘录没有依据时，status 必须是 need_owner，reply_text 必须正好是：{NO_BASIS}\n"
            "不要编造摘录里没有的日期、承诺、负责人或进度。机器人回复不要加前缀。\n"
            "需要以示例负责人本人账号代发时不要直接发，把动作放进 actions 且 as 为 user，status 为 need_confirm。\n"
            "不要声称已经修改台账、审批或发出通知。\n"
            + extra
            + "知识库摘录：\n" + excerpt_text
            + "\n最近对话：\n" + json.dumps(pairs, ensure_ascii=False)
            + "\n问题：\n" + question[:2000]
        )

    def _user_action(self, action, *, chat_id, message_id) -> dict:
        text = str((action or {}).get("text") or "").replace("\x00", "").strip()
        if not text:
            text = NO_BASIS
        if not text.startswith(USER_PREFIX):
            text = USER_PREFIX + text
        if len(text) > 4000:
            text = text[:4000]
        return {
            "type": "reply_message",
            "as": "user",
            "chat_id": chat_id,
            "root_message_id": message_id,
            "text": text,
            "prefix_user_p2p": True,
            "deliver": "need_confirm",
            "sent": False,
        }

    def _pack(self, text, *, status, kind, chat_id, message_id, actions=None, source):
        answer = GrokAnswer(
            status=status, reply_text=text, kind=kind, chat_id=str(chat_id or ""),
            message_id=str(message_id or ""), actions=list(actions or []),
            send_live=self.settings.send_live, source=source,
        )
        self._commit(answer)
        return answer

    def _commit(self, answer: GrokAnswer) -> None:
        record = answer.as_record()
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with self._lock:
            self.records.append(record)
            if len(self.records) > 200:
                del self.records[:-200]
            path = self.settings.outbox_path
            if not path:
                return
            target = Path(path)
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                fd = os.open(target, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
                try:
                    os.write(fd, line.encode("utf-8"))
                finally:
                    os.close(fd)
                os.chmod(target, 0o600)
            except OSError:
                return


def route_from_config(config) -> GrokRoute:
    return GrokRoute(load_grok_settings(config))
