"""Read-only MCP helpers for commenting on open rules and reading Federal Register text.

Container runtime boundary: this module imports only Python stdlib, ``duckdb``,
``httpx``, and ``spicy_regs.runtime_bounds``. Never import ETL or source-reader
packages (``spicy_docs``, ``polars``, ``boto3``, ``pyarrow``) here.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse
from uuid import UUID

import duckdb
import httpx

from spicy_regs.runtime_bounds import checkpoint, remaining_timeout

REGULATIONS_GOV = "https://www.regulations.gov"
USER_AGENT = "spicy-regs-mcp (+https://github.com/mikewolfd/spicy-regs)"
COMMENT_MAX_CHARS = 5000
DOCKET_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{2,63}")

GOVINFO_HOST = "www.govinfo.gov"
FEDERAL_REGISTER_HOST = "www.federalregister.gov"
GOVINFO_FR_URL = "https://www.govinfo.gov/content/pkg/FR-{date}/html/{number}.htm"
FR_DOC_NUMBER_PATTERN = re.compile(r"(?:FR\s*Doc\.?\s*)?(\d{4}-\d{4,6})", re.IGNORECASE)
_CATCHALL_DOCKET_PATTERN = re.compile(r"_FRDOC_[^-]+$", re.IGNORECASE)

RULE_TEXT_MAX_BYTES = 8 * 1024 * 1024
RULE_TEXT_MIN_CHARS = 500
RULE_TEXT_DEFAULT_CHARS = 8000
RULE_TEXT_MAX_CHARS = 20000
RULE_TEXT_TIMEOUT_SECONDS = 20.0
RULE_TEXT_RETRY_DELAYS = (0.5, 1.0)
RULE_TEXT_MAX_REDIRECTS = 5
RULE_TYPE_PRIORITY = ("Proposed Rule", "Rule", "Interim Final Rule", "Direct Final Rule")

_SECTION_HEADING = re.compile(r"^(?:Sec\.|§+)\s+(\d+[A-Za-z]?\.\d+[A-Za-z0-9-]*)\s+(\S.*)$")
_SECTION_MARKER_ONLY = re.compile(r"^(?:Sec\.|§+)\s+(\d+[A-Za-z]?\.\d+[A-Za-z0-9-]*)$")
_PART_HEADING = re.compile(r"^PART\s+(\d+[A-Za-z]?)\s*(?:--|\u2014|\u2013)\s*(.*)$")
_PAGE_MARKER = re.compile(r"^\s*\[\[Page \d+\]\]\s*$")
_SECTION_ARG = re.compile(r"^(?:§+|sec(?:tion)?\.?)?\s*(\d+[A-Za-z]?\.\d+[A-Za-z0-9-]*)$", re.IGNORECASE)
_PART_ARG = re.compile(r"^part\s+(\d+[A-Za-z]?)$", re.IGNORECASE)
_AMENDATORY_INSTRUCTION = re.compile(
    r"^\d+\.\s+(Amend|Revise|Add|Remove|Redesignate|Republish|Suspend|Reserve|Effective|"
    r"The authority citation|In (?:Sec\.|§)|Section|Part|Subpart|Appendix)\b"
)
_ISO_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CAPTCHA_TITLE = re.compile(r"<title>\s*Federal Register\s*::\s*Request Access\s*</title>", re.IGNORECASE)

_BLOCK_TAGS = frozenset({
    "address", "article", "aside", "blockquote", "dd", "div", "dl", "dt",
    "figcaption", "figure", "footer", "h1", "h2", "h3", "h4", "h5", "h6",
    "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section",
    "table", "tbody", "tfoot", "thead", "tr", "ul",
})
_VOID_TAGS = frozenset({
    "area", "base", "br", "col", "embed", "hr", "img", "input",
    "link", "meta", "param", "source", "track", "wbr",
})
_SKIP_TAGS = frozenset({"script", "style", "noscript"})
_SKIP_CLASS_TOKENS = ("printed-page", "unprinted-element", "page-number")


class RuleTextError(RuntimeError):
    """Raised when official rule text cannot be fetched safely or parsed."""


def _utc_now() -> datetime:
    """Current UTC instant; isolated as a seam so tests never depend on wall-clock time."""
    return datetime.now(timezone.utc)


def _sleep(seconds: float) -> None:
    """Sleep helper isolated for fast retry unit tests."""
    time.sleep(seconds)


def _jsonify(value: Any) -> Any:
    """Convert DuckDB result values to JSON-safe Python primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return str(value)


def _parse_utc(value: Any, *, end_of_day: bool = False) -> datetime | None:
    """Parse an ISO timestamp (with Z/offset or naive) or bare YYYY-MM-DD date to UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        if end_of_day:
            return datetime(value.year, value.month, value.day, 23, 59, 59, tzinfo=timezone.utc)
        return datetime(value.year, value.month, value.day, 0, 0, 0, tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if _ISO_DATE_ONLY.fullmatch(text):
        try:
            day = date.fromisoformat(text)
        except ValueError:
            return None
        return _parse_utc(day, end_of_day=end_of_day)
    normalized = f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _resolve_now(today: datetime | date | str | None) -> datetime:
    if today is None:
        return _utc_now()
    parsed = _parse_utc(today, end_of_day=False)
    if parsed is None:
        raise ValueError(f"Invalid today value: {today!r}")
    return parsed


def _is_withdrawn(value: Any) -> bool:
    """Handle both native BOOLEAN withdrawn values and legacy string flags."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


def _has_relation(cursor: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Return whether ``name`` exists as a view or table in DuckDB."""
    view_row = cursor.execute(
        "SELECT count(*) FROM duckdb_views() WHERE lower(view_name) = lower(?)",
        [name],
    ).fetchone()
    if view_row and view_row[0] > 0:
        return True
    table_row = cursor.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE lower(table_name) = lower(?)",
        [name],
    ).fetchone()
    return bool(table_row and table_row[0] > 0)


def _relation_columns(cursor: duckdb.DuckDBPyConnection, name: str) -> dict[str, str]:
    """Map lowercased column names to exact declared names via ``DESCRIBE``."""
    quoted = name.replace('"', '""')
    return {str(row[0]).lower(): str(row[0]) for row in cursor.execute(f'DESCRIBE "{quoted}"').fetchall()}


def _has_document_subtype(cursor: duckdb.DuckDBPyConnection) -> bool:
    """Return True when ``document_attributes`` is loaded and carries ``document_id`` and ``subtype``."""
    if not _has_relation(cursor, "document_attributes"):
        return False
    cols = _relation_columns(cursor, "document_attributes")
    return "document_id" in cols and "subtype" in cols


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _check_draft(draft: str) -> dict[str, Any]:
    chars = len(draft)
    utf8_bytes = len(draft.encode("utf-8"))
    over = utf8_bytes > COMMENT_MAX_CHARS
    check: dict[str, Any] = {
        "characters": chars,
        "utf8_bytes": utf8_bytes,
        "limit": COMMENT_MAX_CHARS,
        "fits_comment_box": not over,
    }
    if over:
        check["advice"] = (
            f"The regulations.gov comment box takes at most {COMMENT_MAX_CHARS} characters/UTF-8 bytes. "
            "Shorten it, or put the full text in an attached file and summarize it in the box."
        )
    return check


def prepare_comment(
    cursor: duckdb.DuckDBPyConnection,
    docket_id: str,
    draft: str = "",
    *,
    today: datetime | date | str | None = None,
) -> dict[str, Any]:
    """Inspect open comment windows for ``docket_id`` and optionally check ``draft`` length.

    Strictly read-only; never submits comments.
    """
    checkpoint()
    now = _resolve_now(today)
    cleaned_id = docket_id.strip()
    if not DOCKET_ID_PATTERN.fullmatch(cleaned_id):
        return {"error": f"Not a docket ID: {cleaned_id!r}. Docket IDs look like 'USBC-2026-0628'."}

    if not _has_relation(cursor, "dockets"):
        raise ValueError("dockets is not available in this connection")
    if not _has_relation(cursor, "documents"):
        raise ValueError("documents is not available in this connection")

    checkpoint()
    docket = cursor.execute(
        "SELECT docket_id, agency_code, title FROM dockets WHERE docket_id = ? LIMIT 1",
        [cleaned_id],
    ).fetchone()
    if docket is None:
        return {"error": f"Docket {cleaned_id} is not in the Spicy Regs mirror."}

    checkpoint()
    doc_cols = _relation_columns(cursor, "documents")
    has_subtype = _has_document_subtype(cursor)

    withdrawn_expr = f"d.{_qident(doc_cols['withdrawn'])}" if "withdrawn" in doc_cols else "NULL"
    subtype_expr = "da.subtype" if has_subtype else "NULL"
    join_clause = "LEFT JOIN document_attributes da ON da.document_id = d.document_id" if has_subtype else ""
    removed_clause = (
        f" AND LOWER(COALESCE(CAST(d.{_qident(doc_cols['publisher_status'])} AS VARCHAR), '')) <> 'removed'"
        if "publisher_status" in doc_cols
        else ""
    )

    checkpoint()
    rows = cursor.execute(
        f"SELECT d.document_id, d.document_type, d.title, d.comment_start_date, d.comment_end_date, "
        f"{withdrawn_expr}, {subtype_expr} "
        f"FROM documents d {join_clause} "
        f"WHERE d.docket_id = ? AND d.comment_end_date IS NOT NULL{removed_clause}",
        [cleaned_id],
    ).fetchall()

    open_entries: list[tuple[bool, datetime, str, dict[str, Any]]] = []
    latest_close: datetime | None = None
    for document_id, document_type, title, start_raw, end_raw, withdrawn, subtype in rows:
        if _is_withdrawn(withdrawn):
            continue
        closes = _parse_utc(end_raw, end_of_day=True)
        if closes is None:
            continue
        if latest_close is None or closes > latest_close:
            latest_close = closes
        not_before = _parse_utc(start_raw, end_of_day=False)
        if closes < now or (not_before is not None and not_before > now):
            continue
        entry: dict[str, Any] = {
            "document_id": document_id,
            "document_type": document_type,
        }
        if has_subtype:
            entry["subtype"] = subtype
        entry.update(
            {
                "title": title,
                "comment_start_date": _jsonify(start_raw),
                "comment_end_date": _jsonify(end_raw),
                "days_left": max(0, (closes - now).days),
                "comment_url": f"{REGULATIONS_GOV}/commenton/{document_id}",
                "document_url": f"{REGULATIONS_GOV}/document/{document_id}",
            }
        )
        open_entries.append((document_type != "Proposed Rule", closes, str(document_id), entry))

    open_entries.sort(key=lambda item: (item[0], item[1], item[2]))
    open_documents = [item[3] for item in open_entries]

    result: dict[str, Any] = {
        "docket_id": docket[0],
        "agency_code": docket[1],
        "docket_title": docket[2],
        "docket_url": f"{REGULATIONS_GOV}/docket/{docket[0]}",
        "checked_at": now.isoformat(timespec="seconds"),
        "status": "open" if open_documents else "closed",
        "open_documents": open_documents,
        "how_to_submit": (
            "Spicy Regs does not submit comments. Open comment_url, paste the comment, "
            "and submit it on regulations.gov. Deadlines change when agencies extend them, "
            "and this mirror refreshes nightly, so confirm the date on that page."
        ),
    }
    if not open_documents:
        result["last_comment_deadline"] = latest_close.isoformat(timespec="seconds") if latest_close else None
        result["how_to_submit"] = (
            "Nothing in this docket is open for comment in the mirror. Check docket_url in case "
            "the agency reopened or extended the comment period since the last nightly refresh."
        )
    if draft.strip():
        result["draft_check"] = _check_draft(draft)
    return result


# --- get_rule_text -----------------------------------------------------------


def _validate_https_host(url: str, allowed_host: str) -> str | None:
    """Return ``url`` if it uses HTTPS on ``allowed_host`` with no userinfo, else None."""
    cleaned = url.strip()
    if not cleaned:
        return None
    parsed = urlparse(cleaned)
    if parsed.scheme.lower() != "https":
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if (parsed.hostname or "").lower() != allowed_host.lower():
        return None
    return cleaned


def _format_size_limit(max_bytes: int) -> str:
    if max_bytes >= 1024 * 1024 and max_bytes % (1024 * 1024) == 0:
        return f"{max_bytes // (1024 * 1024)} MB"
    return f"{max_bytes} bytes"


def _fetch_url_text_once(url: str, *, allowed_host: str, source_label: str) -> str:
    """Stream one HTTPS GET while enforcing host-locked redirects and ``RULE_TEXT_MAX_BYTES``."""
    if _validate_https_host(url, allowed_host) is None:
        raise RuleTextError(f"refused non-{allowed_host} URL: {url}")

    timeout = remaining_timeout(RULE_TEXT_TIMEOUT_SECONDS)
    current = url
    with httpx.Client(timeout=timeout, follow_redirects=False, headers={"User-Agent": USER_AGENT}) as client:
        for _ in range(RULE_TEXT_MAX_REDIRECTS + 1):
            checkpoint()
            with client.stream("GET", current) as response:
                if (urlparse(str(response.url)).hostname or "").lower() != allowed_host.lower():
                    raise RuleTextError(f"{source_label} redirected off its own host to {response.url}")
                if response.status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get("location")
                    if not location:
                        raise RuleTextError(f"{source_label} returned redirect {response.status_code} without Location")
                    target = urljoin(current, location)
                    if _validate_https_host(target, allowed_host) is None:
                        raise RuleTextError(f"{source_label} redirected off its own host to {target}")
                    current = target
                    continue
                if response.status_code == 404:
                    raise RuleTextError(f"{source_label} has no text for this document yet")
                response.raise_for_status()
                chunks: list[bytes] = []
                size = 0
                for chunk in response.iter_bytes():
                    checkpoint()
                    size += len(chunk)
                    if size > RULE_TEXT_MAX_BYTES:
                        raise RuleTextError(f"the document is larger than {_format_size_limit(RULE_TEXT_MAX_BYTES)}")
                    chunks.append(chunk)
                encoding = response.encoding or "utf-8"
                return b"".join(chunks).decode(encoding, errors="replace")
    raise RuleTextError(f"{source_label} exceeded {RULE_TEXT_MAX_REDIRECTS} redirects")


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def _fetch_url_text(
    url: str,
    *,
    allowed_host: str = GOVINFO_HOST,
    source_label: str = "govinfo",
) -> str:
    """Fetch ``url`` with strict host validation, 8 MiB bound, and bounded transient retries."""
    delays: tuple[float | None, ...] = (*RULE_TEXT_RETRY_DELAYS, None)
    last_exc: Exception | None = None
    for delay in delays:
        checkpoint()
        try:
            return _fetch_url_text_once(url, allowed_host=allowed_host, source_label=source_label)
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if delay is None or not _is_retryable_http_status(exc.response.status_code):
                raise
        except httpx.TransportError as exc:
            last_exc = exc
            if delay is None:
                raise
        checkpoint()
        _sleep(delay)
        checkpoint()
    assert last_exc is not None
    raise last_exc


def _govinfo_text(page: str) -> str:
    match = re.search(r"<pre>(.*)</pre>", page, re.DOTALL | re.IGNORECASE)
    body = unescape(re.sub(r"<[^>]+>", "", match.group(1) if match else page))
    lines = [line.rstrip() for line in body.splitlines() if not _PAGE_MARKER.match(line)]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


class _FRBodyHTMLParser(HTMLParser):
    """Convert Federal Register body HTML fragments to readable plain text with structure intact."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._skip_stack: list[str] = []
        self._in_pre = 0
        self._in_row = False
        self._row_has_cell = False

    def _emit_break(self, count: int = 2) -> None:
        if not self._chunks:
            return
        tail = "".join(self._chunks[-3:])
        existing = len(tail) - len(tail.rstrip("\n"))
        needed = max(0, count - existing)
        if needed:
            self._chunks.append("\n" * needed)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        name = tag.lower()
        if self._skip_stack:
            if name not in _VOID_TAGS:
                self._skip_stack.append(name)
            return
        attr_map = {k.lower(): (v or "") for k, v in attrs}
        classes = attr_map.get("class", "").lower()
        if name in _SKIP_TAGS or any(token in classes for token in _SKIP_CLASS_TOKENS):
            if name not in _VOID_TAGS:
                self._skip_stack.append(name)
            return
        if name == "br":
            self._emit_break(1)
            return
        if name == "pre":
            self._emit_break(2)
            self._in_pre += 1
            return
        if name == "tr":
            self._emit_break(1)
            self._in_row = True
            self._row_has_cell = False
            return
        if name in ("td", "th"):
            if self._in_row and self._row_has_cell:
                self._chunks.append(" | ")
            self._row_has_cell = True
            return
        if name in _BLOCK_TAGS:
            self._emit_break(2)

    def handle_endtag(self, tag: str) -> None:
        name = tag.lower()
        if self._skip_stack:
            if name in self._skip_stack:
                while self._skip_stack and self._skip_stack.pop() != name:
                    pass
            return
        if name == "pre":
            self._in_pre = max(0, self._in_pre - 1)
            self._emit_break(2)
            return
        if name == "tr":
            self._in_row = False
            self._row_has_cell = False
            self._emit_break(1)
            return
        if name in _BLOCK_TAGS:
            self._emit_break(2)

    def handle_data(self, data: str) -> None:
        if self._skip_stack or not data:
            return
        if self._in_pre:
            self._chunks.append(data)
            return
        collapsed = re.sub(r"[ \t\r\n\f\v\xa0]+", " ", data)
        if not collapsed.strip():
            if self._chunks and not self._chunks[-1].endswith(("\n", " ")):
                self._chunks.append(" ")
            return
        if self._chunks and self._chunks[-1].endswith("\n"):
            collapsed = collapsed.lstrip(" ")
        self._chunks.append(collapsed)

    def text(self) -> str:
        raw = "".join(self._chunks)
        lines = [line.strip() if not self._in_pre else line.rstrip() for line in raw.splitlines()]
        lines = [line for line in lines if not _PAGE_MARKER.match(line)]
        return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _fr_body_html_text(page: str) -> str:
    """Extract structured plain text from a Federal Register ``body_html_url`` response."""
    if _CAPTCHA_TITLE.search(page) or "g-recaptcha" in page.lower():
        raise RuleTextError("federalregister.gov blocked automated access (Request Access / CAPTCHA)")
    parser = _FRBodyHTMLParser()
    parser.feed(page)
    parser.close()
    text = parser.text()
    if not text:
        raise RuleTextError("federalregister.gov body HTML contained no readable text")
    return text


@lru_cache(maxsize=16)
def _fetch_rule_text_with_source(
    document_number: str,
    publication_date: str,
    body_html_url: str | None = None,
) -> tuple[str, str, str | None]:
    """Fetch official rule text from GovInfo, falling back to Federal Register body HTML."""
    govinfo_url = GOVINFO_FR_URL.format(date=publication_date, number=document_number)
    try:
        raw = _fetch_url_text(govinfo_url, allowed_host=GOVINFO_HOST, source_label="govinfo")
        return _govinfo_text(raw), govinfo_url, None
    except (RuleTextError, httpx.HTTPError) as govinfo_exc:
        fallback_url = (
            _validate_https_host(body_html_url, FEDERAL_REGISTER_HOST)
            if isinstance(body_html_url, str) and body_html_url.strip()
            else None
        )
        if fallback_url is None:
            raise
        try:
            raw_html = _fetch_url_text(
                fallback_url,
                allowed_host=FEDERAL_REGISTER_HOST,
                source_label="federalregister.gov",
            )
            return _fr_body_html_text(raw_html), fallback_url, "federal_register_body_html"
        except (RuleTextError, httpx.HTTPError) as fallback_exc:
            raise RuleTextError(
                f"govinfo failed ({govinfo_exc}); federalregister.gov body_html fallback failed ({fallback_exc})"
            ) from fallback_exc


def _rule_structure(text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    lines = text.splitlines()
    has_blank_paragraphs = "\n\n" in text
    parts: list[dict[str, Any]] = []
    sections: list[dict[str, Any]] = []
    current_part: str | None = None
    for i, line in enumerate(lines):
        if has_blank_paragraphs and i > 0 and lines[i - 1].strip():
            continue
        following = lines[i + 1].strip() if i + 1 < len(lines) else ""
        part = _PART_HEADING.match(line)
        if part:
            current_part = part.group(1)
            title = part.group(2).strip()
            if has_blank_paragraphs and following and following.isupper():
                title = f"{title} {following}"
            parts.append({"part": current_part, "title": " ".join(title.split()), "line": i})
            continue
        section = _SECTION_HEADING.match(line)
        if section:
            title = section.group(2).strip()
            if has_blank_paragraphs and following and not title.endswith("."):
                title = f"{title} {following}"
            sections.append(
                {"section": section.group(1), "title": " ".join(title.split()), "part": current_part, "line": i}
            )
            continue
        if not has_blank_paragraphs:
            marker = _SECTION_MARKER_ONLY.match(line.strip())
            if marker and following:
                sections.append(
                    {"section": marker.group(1), "title": " ".join(following.split()), "part": current_part, "line": i}
                )
    return parts, sections


def _slice_lines(text: str, start: int, stops: list[int], stop_at_instructions: bool) -> str:
    lines = text.splitlines()
    end = min([s for s in stops if s > start], default=len(lines))
    for j in range(start + 1, end):
        if lines[j].startswith("[FR Doc") or (stop_at_instructions and _AMENDATORY_INSTRUCTION.match(lines[j])):
            end = j
            break
    while end > start + 1 and lines[end - 1].strip() in ("", "0"):
        end -= 1
    return "\n".join(lines[start:end]).strip()


def _labeled_paragraph(text: str, label: str) -> str | None:
    match = re.search(
        rf"^{re.escape(label)}:[ \t]*(?:\n+[ \t]*)?(.*?)(?:\n\s*\n|\n[A-Z][A-Z ]+:|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    if not match:
        return None
    value = " ".join(match.group(1).split())
    if not value or re.match(r"^[A-Z][A-Z ]+:$", value):
        return None
    return value[:1500]


def _lookup_offline_body(
    cursor: duckdb.DuckDBPyConnection,
    document_number: str,
    publication_date: str,
    document_type: str | None,
) -> tuple[str, str | None, str | None, str | None] | None:
    """Return ``(body_text, body_url, body_sha256, text_sha256)`` from local/R2 offline body tables when acquired."""
    candidates = (
        ("executive_order_observations", "federal_register_document_bodies")
        if document_type == "Presidential Document"
        else ("federal_register_document_bodies", "executive_order_observations")
    )
    for relation in candidates:
        checkpoint()
        if not _has_relation(cursor, relation):
            continue
        cols = _relation_columns(cursor, relation)
        if not {"document_number", "publication_date", "body_status", "body_text"} <= set(cols):
            continue
        doc_col = _qident(cols["document_number"])
        date_col = _qident(cols["publication_date"])
        status_col = _qident(cols["body_status"])
        text_col = _qident(cols["body_text"])
        url_col = _qident(cols["body_url"]) if "body_url" in cols else "NULL"
        body_sha_col = _qident(cols["body_sha256"]) if "body_sha256" in cols else "NULL"
        text_sha_col = _qident(cols["text_sha256"]) if "text_sha256" in cols else "NULL"
        row = cursor.execute(
            f"SELECT {text_col}, {url_col}, {body_sha_col}, {text_sha_col} "
            f"FROM {_qident(relation)} "
            f"WHERE {doc_col} = ? "
            f"  AND TRY_CAST({date_col} AS DATE) = CAST(? AS DATE) "
            f"  AND {status_col} = 'acquired' "
            f"  AND {text_col} IS NOT NULL "
            f"  AND trim({text_col}) <> '' "
            "LIMIT 1",
            [document_number, publication_date],
        ).fetchone()
        if row is not None and isinstance(row[0], str) and row[0].strip():
            return (
                row[0],
                str(row[1]) if isinstance(row[1], str) and row[1].strip() else None,
                str(row[2]) if isinstance(row[2], str) and row[2].strip() else None,
                str(row[3]) if isinstance(row[3], str) and row[3].strip() else None,
            )
    return None


def _page_of(text: str, offset: int, max_chars: int) -> dict[str, Any]:
    chunk = text[offset : offset + max_chars]
    end = offset + len(chunk)
    return {
        "text": chunk,
        "offset": offset,
        "total_chars": len(text),
        "next_offset": end if end < len(text) else None,
    }


def _normalize_list(raw: Any) -> list[Any]:
    """Normalize either a native Arrow list/struct array or a legacy JSON string into a JSON-safe list."""
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError):
            return []
        return _jsonify(parsed) if isinstance(parsed, list) else []
    if isinstance(raw, (list, tuple)):
        return [_jsonify(item) for item in raw]
    return []


def _resolve_fr_number(
    cursor: duckdb.DuckDBPyConnection,
    ref: str,
) -> tuple[str | None, list[dict[str, Any]], str | None]:
    fr_match = FR_DOC_NUMBER_PATTERN.fullmatch(ref)
    if fr_match:
        return fr_match.group(1), [], None
    if not DOCKET_ID_PATTERN.fullmatch(ref):
        return (
            None,
            [],
            (
                f"Not a docket, document, or Federal Register number: {ref!r}. "
                "Try 'USBC-2026-0628', 'USBC-2026-0628-0001', or '2026-18481'."
            ),
        )
    if _CATCHALL_DOCKET_PATTERN.search(ref):
        return (
            None,
            [],
            (
                f"{ref} is an agency catch-all docket holding many unrelated notices. "
                "Pass the regulations.gov document ID or the Federal Register document number instead."
            ),
        )

    if not _has_relation(cursor, "documents"):
        raise ValueError("documents is not available in this connection")

    checkpoint()
    doc_cols = _relation_columns(cursor, "documents")
    has_subtype = _has_document_subtype(cursor)
    subtype_expr = "da.subtype" if has_subtype else "NULL"
    join_clause = "LEFT JOIN document_attributes da ON da.document_id = d.document_id" if has_subtype else ""
    removed_clause = (
        f" AND (d.document_id = ? OR LOWER(COALESCE(CAST(d.{_qident(doc_cols['publisher_status'])} AS VARCHAR), '')) <> 'removed')"
        if "publisher_status" in doc_cols
        else ""
    )
    params = [ref, ref, ref] if "publisher_status" in doc_cols else [ref, ref]

    checkpoint()
    rows = cursor.execute(
        f"SELECT d.document_id, d.document_type, d.title, d.posted_date, d.fr_doc_num, {subtype_expr} "
        f"FROM documents d {join_clause} "
        f"WHERE (d.document_id = ? OR d.docket_id = ?) AND d.fr_doc_num IS NOT NULL AND d.fr_doc_num <> ''{removed_clause}",
        params,
    ).fetchall()

    candidates: list[dict[str, Any]] = []
    for document_id, document_type, title, posted, fr_doc_num, subtype in rows:
        number = FR_DOC_NUMBER_PATTERN.search(str(fr_doc_num))
        if number:
            candidate: dict[str, Any] = {
                "document_id": document_id,
                "document_type": document_type,
            }
            if has_subtype:
                candidate["subtype"] = subtype
            candidate.update(
                {
                    "title": title,
                    "posted_date": _jsonify(posted),
                    "fr_doc_num": number.group(1),
                }
            )
            candidates.append(candidate)

    if not candidates:
        return (
            None,
            [],
            (
                f"No Federal Register notice is linked to {ref} in the mirror. "
                "Pass its Federal Register document number (e.g. '2026-18481') instead."
            ),
        )
    exact = [c for c in candidates if c["document_id"] == ref]
    if exact:
        return exact[0]["fr_doc_num"], [], None
    if "_FRDOC_" in ref.upper():
        return (
            None,
            [],
            (
                f"{ref} is an agency catch-all docket holding many unrelated notices. "
                "Pass the regulations.gov document ID or the Federal Register document number instead."
            ),
        )

    def priority(c: dict[str, Any]) -> int:
        kind = c["document_type"] or ""
        return RULE_TYPE_PRIORITY.index(kind) if kind in RULE_TYPE_PRIORITY else len(RULE_TYPE_PRIORITY)

    candidates.sort(key=lambda c: c["posted_date"] or "", reverse=True)
    candidates.sort(key=priority)
    return candidates[0]["fr_doc_num"], candidates[1:10], None


def get_rule_text(
    cursor: duckdb.DuckDBPyConnection,
    document: str,
    section: str = "",
    offset: int = 0,
    max_chars: int = RULE_TEXT_DEFAULT_CHARS,
    *,
    fetch: Callable[[str, str], str] | None = None,
) -> dict[str, Any]:
    """Read an outline, CFR section/part, or paginated full text of a Federal Register rule."""
    checkpoint()
    if max_chars < RULE_TEXT_MIN_CHARS or max_chars > RULE_TEXT_MAX_CHARS:
        return {"error": f"max_chars must be between {RULE_TEXT_MIN_CHARS} and {RULE_TEXT_MAX_CHARS}"}
    if offset < 0:
        return {"error": "offset must be 0 or more"}

    number, others, error = _resolve_fr_number(cursor, document.strip())
    if error:
        return {"error": error}
    assert number is not None

    if not _has_relation(cursor, "federal_register"):
        raise ValueError("federal_register is not available in this connection")

    checkpoint()
    fr_cols = _relation_columns(cursor, "federal_register")
    cfr_col = fr_cols.get("cfr_references") or fr_cols.get("cfr_references_json")
    rin_col = fr_cols.get("regulation_id_numbers") or fr_cols.get("regulation_id_numbers_json")
    body_html_col = fr_cols.get("body_html_url")

    def col_expr(name: str | None) -> str:
        return _qident(name) if name is not None else "NULL"

    checkpoint()
    row = cursor.execute(
        f"SELECT {col_expr(fr_cols.get('document_number'))}, "
        f"{col_expr(fr_cols.get('title'))}, "
        f"{col_expr(fr_cols.get('document_type'))}, "
        f"{col_expr(fr_cols.get('publication_date'))}, "
        f"{col_expr(fr_cols.get('comments_close_on'))}, "
        f"{col_expr(fr_cols.get('abstract'))}, "
        f"{col_expr(cfr_col)}, "
        f"{col_expr(rin_col)}, "
        f"{col_expr(fr_cols.get('html_url'))}, "
        f"{col_expr(body_html_col)} "
        "FROM federal_register WHERE document_number = ? ORDER BY publication_date DESC LIMIT 1",
        [number],
    ).fetchone()
    if row is None:
        return {"error": f"Federal Register document {number} is not in the mirror's federal_register table."}

    (
        fr_number,
        title,
        doc_type,
        published_raw,
        closes,
        abstract,
        cfr_raw,
        rin_raw,
        html_url,
        body_html_url,
    ) = row
    published = str(_jsonify(published_raw) or "")[:10]
    if not _ISO_DATE_ONLY.fullmatch(published):
        return {"error": f"Federal Register document {fr_number} has no usable publication date."}

    govinfo_url = GOVINFO_FR_URL.format(date=published, number=fr_number)
    result: dict[str, Any] = {
        "fr_document_number": fr_number,
        "title": title,
        "document_type": doc_type,
        "publication_date": published,
        "comments_close_on_in_notice": _jsonify(closes),
        "cfr_references": _normalize_list(cfr_raw),
        "rins": _normalize_list(rin_raw),
        "federal_register_url": html_url,
        "source": govinfo_url,
    }
    if others:
        result["other_notices_in_docket"] = others

    checkpoint()
    offline_body_sha256: str | None = None
    offline_text_sha256: str | None = None
    source_mode: str | None = None
    try:
        if fetch is not None:
            text = fetch(str(fr_number), published)
            source_url = govinfo_url
            source_fallback: str | None = None
        else:
            offline = _lookup_offline_body(
                cursor,
                str(fr_number),
                published,
                str(doc_type) if doc_type is not None else None,
            )
            if offline is not None:
                text, offline_url, offline_body_sha256, offline_text_sha256 = offline
                source_url = offline_url or govinfo_url
                source_fallback = None
                source_mode = "offline_table"
            else:
                text, source_url, source_fallback = _fetch_rule_text_with_source(
                    str(fr_number),
                    published,
                    body_html_url if isinstance(body_html_url, str) else None,
                )
    except (RuleTextError, httpx.HTTPError) as exc:
        result["error"] = f"Could not fetch the text from govinfo: {exc}. Read it at federal_register_url."
        return result

    result["source"] = source_url
    if source_mode is not None:
        result["source_mode"] = source_mode
    if offline_body_sha256 is not None:
        result["body_sha256"] = offline_body_sha256
    if offline_text_sha256 is not None:
        result["text_sha256"] = offline_text_sha256
    if source_fallback is not None:
        result["source_fallback"] = source_fallback

    parts, sections = _rule_structure(text)
    wanted = section.strip()
    if not wanted:
        result.update(
            {
                "mode": "outline",
                "summary": abstract or _labeled_paragraph(text, "SUMMARY"),
                "dates": _labeled_paragraph(text, "DATES"),
                "parts": [{"part": p["part"], "title": p["title"]} for p in parts],
                "sections": [{"section": s["section"], "title": s["title"], "part": s["part"]} for s in sections],
                "total_chars": len(text),
                "next_step": (
                    "Call again with section='<number>' (e.g. '"
                    + (sections[0]["section"] if sections else "60.1")
                    + "') for one section, 'part <n>' for a whole part, or 'full' for the whole document in pages."
                    if sections
                    else "This document has no regulatory text sections; call again with section='full' to read it."
                ),
            }
        )
        return result

    stops = [p["line"] for p in parts] + [s["line"] for s in sections]
    part_arg = _PART_ARG.match(wanted)
    section_arg = _SECTION_ARG.match(wanted)
    if wanted.lower() == "full":
        body = text
    elif part_arg:
        part_match = next((p for p in parts if p["part"] == part_arg.group(1)), None)
        if part_match is None:
            result["error"] = f"No part {part_arg.group(1)} in this document. Parts: {[p['part'] for p in parts]}"
            return result
        body = _slice_lines(text, part_match["line"], [p["line"] for p in parts], stop_at_instructions=False)
    elif section_arg:
        sec_match = next((s for s in sections if s["section"] == section_arg.group(1)), None)
        if sec_match is None:
            result["error"] = (
                f"No section {section_arg.group(1)} in this document. Sections: {[s['section'] for s in sections][:60]}"
            )
            return result
        body = _slice_lines(text, sec_match["line"], stops, stop_at_instructions=True)
    else:
        result["error"] = "section must be empty (outline), a section number like '60.4', 'part 60', or 'full'."
        return result

    result["section"] = wanted
    result.update(_page_of(body, offset, max_chars))
    return result
