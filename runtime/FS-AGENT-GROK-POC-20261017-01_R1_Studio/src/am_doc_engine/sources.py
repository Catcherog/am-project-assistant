"""Offline text ingestion and outline suggestions, with explicit completeness limits.

Inputs must already be obtained by an authorized host adapter. This module does not
follow URLs, search drives, perform OCR, activate templates or infer project facts.
"""
from __future__ import annotations

import hashlib
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence
from xml.etree import ElementTree as ET

from .models import ContractError, SourceAsset, canonical, fingerprint, strict_json


@dataclass(frozen=True)
class SourceSegment:
    locator: str
    kind: str
    text: str
    heading_level: int = 0


@dataclass(frozen=True)
class ParsedSource:
    source_id: str
    title: str
    media_type: str
    source_revision: str
    read_state: str
    content: str
    segments: tuple[SourceSegment, ...]
    unsupported: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()

    def as_asset(self, *, role: str, uri: str) -> SourceAsset:
        return SourceAsset(self.source_id, role, uri, self.source_revision,
                           self.content, self.read_state, self.title)


def read_source(path: str | Path, *, source_id: str, title: str | None = None,
                max_bytes: int = 8 * 1024 * 1024) -> ParsedSource:
    """Read an explicit local file only. Filename extension determines supported format."""
    file = Path(path)
    if not file.is_file():
        raise ContractError("资料路径不是可读取文件")
    if file.stat().st_size > max_bytes:
        raise ContractError("资料超过本次本地解析大小限制")
    raw = file.read_bytes()
    if len(raw) > max_bytes:
        raise ContractError("资料在读取过程中变大，超出限制")
    revision = hashlib.sha256(raw).hexdigest()
    label = title or file.name
    if file.suffix.lower() == ".docx":
        return _read_docx(file, raw, source_id, label, revision)
    if file.suffix.lower() not in {".md", ".markdown", ".txt", ".json"}:
        return ParsedSource(source_id, label, "unsupported", revision, "unavailable", "", (),
                            (f"unsupported_extension:{file.suffix.lower()}",), ("需要宿主提供相应格式解析器",))
    try:
        content = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ContractError("文本资料需要 UTF-8；未自动猜测编码") from exc
    if file.suffix.lower() == ".json":
        value = strict_json(content)
        entries = value.items() if isinstance(value, dict) else enumerate(value) if isinstance(value, list) else [("value", value)]
        segments = tuple(SourceSegment("/" + str(k).replace("~", "~0").replace("/", "~1"), "json", canonical(v)) for k, v in entries)
        return ParsedSource(source_id, label, "application/json", revision,
                            "complete" if content.strip() else "unavailable", content, segments,
                            limitations=("JSON 结构作为资料读取；未自动授予来源身份或批准状态",))
    return _read_markdown(content, source_id, label, revision)


def _read_markdown(content: str, source_id: str, title: str, revision: str) -> ParsedSource:
    segments: list[SourceSegment] = []
    unsupported: list[str] = []
    fence: str | None = None
    for number, line in enumerate(content.splitlines(), 1):
        if not line.strip():
            continue
        fence_match = re.match(r"^\s*(`{3,}|~{3,})", line)
        if fence_match:
            token = fence_match[1][0]
            fence = None if fence == token else token if fence is None else fence
        heading = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", line) if fence is None else None
        kind = "heading" if heading else "table_row" if "|" in line and fence is None else "text"
        text = heading[2] if heading else line
        segments.append(SourceSegment(f"line:{number}", kind, text, len(heading[1]) if heading else 0))
        if re.search(r"!\[[^\]]*\]\([^)]*\)", line):
            unsupported.append(f"image_not_read:line:{number}")
    return ParsedSource(source_id, title, "text/markdown", revision,
                        "partial" if unsupported else "complete" if content.strip() else "unavailable",
                        content, tuple(segments), tuple(unsupported),
                        ("文本链接仅保留地址，未读取被链接资料；需目录将其作为独立资料接入",))


_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


def _read_docx(path: Path, raw: bytes, source_id: str, title: str, revision: str) -> ParsedSource:
    # Zip contents are read by exact known names; nothing is extracted to the filesystem.
    try:
        from io import BytesIO
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            infos = archive.infolist()
            if len(infos) > 2000 or sum(x.file_size for x in infos) > 32 * 1024 * 1024:
                raise ContractError("Word 压缩内容超过安全解析预算")
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise ContractError("Word 包含重复 ZIP 条目")
            if "word/document.xml" not in names:
                raise ContractError("Word 缺少正文 XML")
            segments: list[SourceSegment] = []
            unsupported: list[str] = []
            parts = ["word/document.xml"] + sorted(n for n in names if re.fullmatch(r"word/(header\d+|footer\d+|footnotes|endnotes|comments)\.xml", n))
            for name in parts:
                data = archive.read(name)
                if b"<!DOCTYPE" in data or b"<!ENTITY" in data:
                    raise ContractError("Word XML 含不支持的 DTD/实体定义")
                root = ET.fromstring(data)
                tags = {e.tag.rsplit("}", 1)[-1] for e in root.iter()}
                for tag in sorted(tags & {"drawing", "pict", "object", "altChunk", "oMath", "oMathPara", "del", "ins", "instrText", "fldSimple", "gridSpan", "vMerge"}):
                    unsupported.append(f"{name}:{tag}")
                if name == "word/comments.xml":
                    unsupported.append("comments_require_business_interpretation")
                # Every paragraph is visited in XML document order, including paragraphs in
                # table cells. The locator is stable within this pinned binary revision.
                table_paragraphs = {id(p) for table in root.iter(_W + "tbl") for p in table.iter(_W + "p")}
                table_locations: dict[int, str] = {}
                for table_n, table in enumerate(root.iter(_W + "tbl"), 1):
                    for row_n, row in enumerate(table.findall(_W + "tr"), 1):
                        for cell_n, cell in enumerate(row.findall(_W + "tc"), 1):
                            for paragraph_n, paragraph in enumerate(cell.findall(_W + "p"), 1):
                                table_locations[id(paragraph)] = f"{name}:table:{table_n}:row:{row_n}:cell:{cell_n}:p:{paragraph_n}"
                for index, paragraph in enumerate(root.iter(_W + "p"), 1):
                    pieces: list[str] = []
                    for item in paragraph.iter():
                        if item.tag == _W + "t":
                            pieces.append(item.text or "")
                        elif item.tag == _W + "tab":
                            pieces.append("\t")
                        elif item.tag in {_W + "br", _W + "cr"}:
                            pieces.append("\n")
                    text = "".join(pieces)
                    if not text.strip():
                        continue
                    style = paragraph.find(f"{_W}pPr/{_W}pStyle")
                    style_id = style.attrib.get(_W + "val", "") if style is not None else ""
                    match = re.fullmatch(r"(?:Heading|heading|标题)([1-6])", style_id)
                    kind = "heading" if match else "table_cell_text" if id(paragraph) in table_paragraphs else "text"
                    segments.append(SourceSegment(table_locations.get(id(paragraph), f"{name}:p:{index}"), kind, text, int(match[1]) if match else 0))
            if any(n.startswith("word/media/") for n in names):
                unsupported.append("embedded_media_not_read")
            if any(n.endswith("vbaProject.bin") for n in names):
                unsupported.append("macro_content_not_read")
            content = "\n".join(s.text for s in segments)
            return ParsedSource(source_id, title, "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                                revision, "partial" if unsupported else "complete" if content else "unavailable",
                                content, tuple(segments), tuple(dict.fromkeys(unsupported)),
                                ("保留正文、表格单元格文字及页眉页脚等文本，不保留 Word 版式、合并单元格关系或样式",
                                 "未执行 OCR；批注与修订需要宿主根据业务状态单独解释"))
    except (zipfile.BadZipFile, ET.ParseError, RuntimeError) as exc:
        raise ContractError("Word 文件无法安全解析") from exc


def parse_feishu_blocks(blocks: Sequence[Mapping[str, Any]], *, source_id: str,
                         title: str, revision: str, complete: bool = False,
                         has_more: bool = False) -> ParsedSource:
    """Read HOST-NORMALIZED blocks, not raw Feishu API responses.

    Supported kinds: text, heading, list, table (rows of strings), table_cell, container.
    ``children`` holds nested normalized records or block IDs in this sequence. The host
    asserts ``complete=True`` only after all pages and child content are read at one revision.
    """
    if not revision:
        raise ContractError("云文档资料必须绑定真实版本")
    index: dict[str, Mapping[str, Any]] = {}
    unsupported: list[str] = []
    referenced: set[str] = set()

    def register(value: Mapping[str, Any]):
        if not isinstance(value, Mapping) or not isinstance(value.get("block_id"), str) or not value["block_id"]:
            raise ContractError("归一化块缺少稳定 block_id")
        bid = value["block_id"]
        if bid in index:
            if canonical(index[bid]) != canonical(value):
                raise ContractError("同一 block_id 的内容冲突")
            return
        index[bid] = value
        children = value.get("children", [])
        if not isinstance(children, (list, tuple)):
            raise ContractError("块 children 必须为数组")
        for child in children:
            if isinstance(child, Mapping):
                register(child)
                referenced.add(child["block_id"])
            elif isinstance(child, str):
                referenced.add(child)
            else:
                raise ContractError("块 children 包含无效值")
    for block in blocks:
        register(block)
    segments: list[SourceSegment] = []
    visited: set[str] = set()

    def visit(bid: str, stack: set[str]):
        if bid in stack:
            raise ContractError("云文档块树存在循环")
        if bid in visited:
            return
        if bid not in index:
            unsupported.append(f"missing_child:{bid}")
            return
        visited.add(bid)
        value = index[bid]
        kind = value.get("kind", "unknown")
        if kind in {"text", "heading", "list", "table_cell"}:
            text = value.get("text", "")
            if not isinstance(text, str):
                raise ContractError("归一化块 text 必须为字符串")
            level = value.get("level", 1) if kind == "heading" else 0
            if type(level) is not int or not 0 <= level <= 6:
                raise ContractError("标题层级无效")
            if text.strip():
                segments.append(SourceSegment(f"block:{bid}", kind, text, level))
        elif kind == "table":
            rows = value.get("rows", [])
            if not isinstance(rows, list) or any(not isinstance(row, list) or any(not isinstance(c, str) for c in row) for row in rows):
                raise ContractError("表格 rows 必须为字符串二维数组")
            for n, row in enumerate(rows, 1):
                segments.append(SourceSegment(f"block:{bid}:row:{n}", "table_row", " | ".join(row)))
        elif kind != "container":
            unsupported.append(f"unsupported_block:{bid}:{kind}")
        for child in value.get("children", []):
            visit(child["block_id"] if isinstance(child, Mapping) else child, stack | {bid})
    roots = [block["block_id"] for block in blocks if block["block_id"] not in referenced]
    for bid in roots:
        visit(bid, set())
    # No-root cycles and disconnected child components must not vanish silently.
    for bid in index:
        if bid not in visited:
            visit(bid, set())
    if has_more or not complete:
        unsupported.append("pagination_or_completeness_unverified")
    content = "\n".join(s.text for s in segments)
    return ParsedSource(source_id, title, "application/x-am-normalized-feishu-blocks", revision,
                        "partial" if unsupported else "complete" if content else "unavailable",
                        content, tuple(segments), tuple(unsupported),
                        ("未联网；资料范围、权限、分页与版本一致性由宿主读取适配器保证",
                         "保留文本和块位置；原生样式、图片、引用内容需对应适配器另行处理"))


def candidate_outline(parsed: ParsedSource) -> dict[str, Any]:
    """Suggest structure only. No inferred clause becomes an effective required rule."""
    sections = [{"section_id": f"section-{n:03d}", "title": s.text,
                 "source_locator": s.locator, "heading_level": s.heading_level}
                for n, s in enumerate((v for v in parsed.segments if v.kind == "heading"), 1)]
    placeholders = list(dict.fromkeys(re.findall(r"\{\{\s*([A-Za-z][A-Za-z0-9_.-]{0,100})\s*\}\}", parsed.content)))
    return {"status": "candidate", "source_id": parsed.source_id, "source_revision": parsed.source_revision,
            "source_content_hash": fingerprint(parsed.content), "read_state": parsed.read_state,
            "sections": sections,
            "unmapped_fields": [{"field_id": fid, "status": "needs_fact_and_rule_mapping"} for fid in placeholders],
            "unsupported": list(parsed.unsupported),
            "activation_allowed": False,
            "next_step": "将字段与事实键、适用条件和明确标准条款绑定；由台账的有效状态决定采用范围"}
