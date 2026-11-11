"""Read a complete, revision-checked Docx block snapshot.

All blocks are retained. Text projections are explicitly partial when embedded
resources or unsupported element/block types require a further content reader.
"""
from __future__ import annotations

from typing import Any

from ..models import (Discovery, InvalidCursor, Observation, Page, ScopeViolation,
                      SnapshotChanged, Source, digest)
from .http import FeishuClient, InvalidResponse, page_items, remote_id
from .messages import document_discoveries


_TEXT_KEYS = {"page", "text", "bullet", "ordered", "code", "quote", "todo",
              "equation", *(f"heading{i}" for i in range(1, 10))}
_CONTAINER_KEYS = {"table", "table_cell", "grid", "grid_column", "callout",
                   "quote_container", "view", "divider"}
_EMBED_KEYS = {"image", "file", "sheet", "bitable", "mindnote", "iframe",
               "board", "diagram", "isv", "chat_card", "task", "wiki_catalog",
               "add_ons", "jira_issue", "okr", "okr_objective", "okr_key_result",
               "okr_progress"}
_META_KEYS = {"block_id", "block_type", "parent_id", "children", "comment_ids"}


def _revision(document: dict[str, Any], expected_id: str) -> int:
    if document.get("document_id") != expected_id:
        raise ScopeViolation("Document metadata belongs to another object")
    revision = document.get("revision_id")
    if type(revision) is not int or revision < 0:
        raise InvalidResponse("Document revision is missing or invalid")
    return revision


def _element_text(elements: Any, issues: list[str], links: list[str],
                  discoveries: list[Discovery]) -> str:
    if not isinstance(elements, list):
        raise InvalidResponse("Document text elements are invalid")
    result: list[str] = []
    for element in elements:
        if not isinstance(element, dict):
            raise InvalidResponse("Document text element is invalid")
        if isinstance(element.get("text_run"), dict):
            run = element["text_run"]
            text = run.get("content")
            if not isinstance(text, str):
                raise InvalidResponse("Document text run has no content")
            # Retain exact prose; styles remain in raw blocks for fact extraction.
            result.append(text)
            style = run.get("text_element_style", {})
            if isinstance(style, dict) and style.get("strikethrough"):
                issues.append("strikethrough_requires_structured_reader")
            if isinstance(style, dict) and isinstance(style.get("link"), dict):
                url = style["link"].get("url")
                if isinstance(url, str):
                    from urllib.parse import unquote
                    links.append(unquote(url))
        elif isinstance(element.get("mention_user"), dict):
            mention = element["mention_user"]
            result.append("@" + str(mention.get("name") or mention.get("user_id") or "unknown"))
        elif isinstance(element.get("mention_doc"), dict):
            mention = element["mention_doc"]
            result.append(str(mention.get("title") or "[document reference]"))
            if isinstance(mention.get("url"), str):
                links.append(mention["url"])
            # Only a recognized direct docx URL establishes a docx discovery;
            # a bare token/type from an unknown resource is not guessed.
        elif isinstance(element.get("equation"), dict):
            value = element["equation"].get("content")
            if isinstance(value, str):
                result.append(value)
            else:
                issues.append("equation_content_pending")
        else:
            issues.append("unsupported_text_element")
    return "".join(result)


def extract_blocks(blocks: list[dict[str, Any]], document_id: str) -> tuple[str, list[str], list[dict[str, Any]], tuple[Discovery, ...]]:
    by_id: dict[str, dict[str, Any]] = {}
    for block in blocks:
        identity = block.get("block_id")
        if not isinstance(identity, str) or not identity:
            raise InvalidResponse("Document block has no ID")
        if identity in by_id:
            raise InvalidResponse("Document returned duplicate block IDs")
        by_id[identity] = block
    if document_id not in by_id:
        raise InvalidResponse("Document snapshot has no root block")
    # Use document child order rather than assuming API page order is reading order.
    ordered: list[dict[str, Any]] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(identity: str) -> None:
        if identity in visiting:
            raise InvalidResponse("Document block tree has a cycle")
        if identity in visited:
            return
        block = by_id.get(identity)
        if block is None:
            raise InvalidResponse("Document snapshot is missing a referenced child block")
        visiting.add(identity)
        ordered.append(block)
        children = block.get("children", [])
        if not isinstance(children, list) or any(not isinstance(x, str) for x in children):
            raise InvalidResponse("Document children are invalid")
        for child in children:
            visit(child)
        visiting.remove(identity)
        visited.add(identity)

    visit(document_id)
    issues: list[str] = []
    if len(visited) != len(by_id):
        # Keep otherwise accessible content, but do not call an unlinked tree complete.
        issues.append("unlinked_blocks")
        for identity in by_id:
            visit(identity)
    lines: list[str] = []
    links: list[str] = []
    pending: list[dict[str, Any]] = []
    discovered: list[Discovery] = []
    for block in ordered:
        recognized = False
        for key in _TEXT_KEYS:
            value = block.get(key)
            if isinstance(value, dict):
                recognized = True
                if "elements" in value:
                    lines.append(_element_text(value["elements"], issues, links, discovered))
                else:
                    issues.append("text_elements_missing:" + key)
        if any(key in block for key in _CONTAINER_KEYS):
            recognized = True
        if "table" in block:
            issues.append("table_relationships_require_structured_reader")
        for key in _EMBED_KEYS:
            if key in block:
                recognized = True
                pending.append({"block_id": block["block_id"], "kind": key,
                                "status": "pending_content", "reference": block[key]})
                issues.append("embedded_content_pending:" + key)
        if not recognized:
            issues.append("unsupported_block_type:" + str(block.get("block_type")))
        extra = set(block) - _META_KEYS - _TEXT_KEYS - _CONTAINER_KEYS - _EMBED_KEYS
        if extra:
            issues.append("unrecognized_block_fields")
    text = "\n".join(line for line in lines if line)
    discovered.extend(document_discoveries([text, *links]))
    unique = {(d.kind, d.remote_id): d for d in discovered if d.remote_id != document_id}
    return text, sorted(set(issues)), pending, tuple(unique.values())


class DocxAdapter:
    def __init__(self, client: FeishuClient, *, page_size: int = 500,
                 max_pages: int = 1000):
        if not 1 <= page_size <= 500 or max_pages < 1:
            raise ValueError("Invalid Docx pagination limits")
        self.client = client
        self.page_size = page_size
        self.max_pages = max_pages

    def fetch_page(self, source: Source, start_ms: int, end_ms: int,
                   cursor: str | None = None) -> Page:
        if source.kind not in {"docx", "feishu_docx"}:
            raise ScopeViolation("Unsupported Docx source kind")
        if start_ms < source.start_ms or end_ms < start_ms:
            raise ScopeViolation("Snapshot request is outside registered time bounds")
        if cursor is not None:
            raise InvalidCursor("Docx snapshots are atomic; restart the whole snapshot")
        identity = remote_id(source.remote_id)
        path = f"/open-apis/docx/v1/documents/{identity}"
        before_data = self.client.get(source, path)
        before = before_data.get("document")
        if not isinstance(before, dict):
            raise InvalidResponse("Document metadata is missing")
        revision = _revision(before, identity)
        token = None
        seen: set[str] = set()
        blocks: list[dict[str, Any]] = []
        for _ in range(self.max_pages):
            params: dict[str, int | str] = {"page_size": self.page_size,
                                           "document_revision_id": revision}
            if token:
                params["page_token"] = token
            data = self.client.get(source, path + "/blocks", params)
            items, next_token = page_items(data, token)
            blocks.extend(items)
            if next_token is None:
                break
            if next_token in seen:
                raise InvalidCursor("Docx pagination cursor loop")
            seen.add(next_token)
            token = next_token
        else:
            raise InvalidResponse("Docx snapshot exceeds pagination limit; no partial snapshot committed")
        after_data = self.client.get(source, path)
        after = after_data.get("document")
        if not isinstance(after, dict):
            raise InvalidResponse("Document verification metadata is missing")
        if _revision(after, identity) != revision or before.get("title") != after.get("title"):
            raise SnapshotChanged("Document changed during snapshot; retry from metadata")
        text, issues, pending, discoveries = extract_blocks(blocks, identity)
        # Pagination order must not create a new version of an unchanged document.
        stable_blocks = sorted(blocks, key=lambda block: block["block_id"])
        version = f"docx:{revision:020d}:{digest({'document': before, 'blocks': stable_blocks})}"
        payload = {"document": before, "blocks": stable_blocks, "revision_id": revision,
                   "content_issues": issues, "pending_resources": pending,
                   "block_enumeration_complete": True, "revision_verified": True,
                   "time_basis": "observed_at", "trusted_instruction": False}
        observation = Observation(source.source_id, identity, version, end_ms,
                                  text=text, payload=payload,
                                  completeness="complete" if not issues else ("partial" if text else "unreadable"),
                                  url=source.settings.get('url'))
        return Page((observation,), discoveries=discoveries, time_coverage=False)
