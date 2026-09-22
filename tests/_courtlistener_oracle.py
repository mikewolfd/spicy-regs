"""Test-only SR04 oracle: maps bulk CourtListener CSV rows to the columns SpicyRegs 8b383714 published.

Never imported by production.
"""

import csv
import io
import sys
from datetime import date


CL_BASE_URL = "https://www.courtlistener.com"


_TEXT_FIELDS = (
    "plain_text",
    "html",
    "html_lawbox",
    "html_columbia",
    "html_anon_2020",
    "html_with_citations",
    "xml_harvard",
    "xml_scan",
)


def _s(value: object) -> str | None:
    """Coerce a CSV cell to a non-empty string, or ``None`` when blank."""
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def old_body(row: dict, *, dump_date: date | None) -> dict:
    """Map one bulk ``opinions`` CSV row onto the published columns.

    ``text_char_count`` is the longest present text field's length and
    ``available_text_fields`` lists the present fields in canonical order.
    """
    present = [name for name in _TEXT_FIELDS if row.get(name)]
    plain = _s(row.get("plain_text"))
    with_citations = _s(row.get("html_with_citations"))
    longest = max((len(str(row[name])) for name in present), default=0)
    return {
        "opinion_id": _s(row.get("id")),
        "cluster_id": _s(row.get("cluster_id")),
        "opinion_type": _s(row.get("type")),
        "author_str": _s(row.get("author_str")),
        "author_id": _s(row.get("author_id")),
        "joined_by_str": _s(row.get("joined_by_str")),
        "per_curiam": _s(row.get("per_curiam")),
        "sha1": _s(row.get("sha1")),
        "page_count": _s(row.get("page_count")),
        "download_url": _s(row.get("download_url")),
        "local_path": _s(row.get("local_path")),
        "extracted_by_ocr": _s(row.get("extracted_by_ocr")),
        "plain_text": plain,
        "html_with_citations": with_citations,
        "available_text_fields": ",".join(present) if present else None,
        "text_char_count": str(longest),
        "date_created": _s(row.get("date_created")),
        "date_modified": _s(row.get("date_modified")),
        "dump_date": dump_date.isoformat() if dump_date else None,
    }


def _cluster_url(cluster_id: str | None, slug: str | None) -> str | None:
    """Build the canonical courtlistener.com opinion URL for a cluster."""
    if not cluster_id:
        return None
    return f"{CL_BASE_URL}/opinion/{cluster_id}/{slug or ''}".rstrip("/") + "/"


def old_cluster(row: dict, *, scope=None) -> dict:
    """Map one bulk ``opinion-clusters`` CSV row onto the published columns.

    Court fields come from ``scope.for_docket`` when a scope is given.
    """
    cluster_id = _s(row.get("id"))
    slug = _s(row.get("slug"))
    cl_docket_id = _s(row.get("docket_id"))
    court_id, jurisdiction, federal = scope.for_docket(cl_docket_id) if scope else (None, None, None)
    return {
        "cluster_id": cluster_id,
        "cl_docket_id": cl_docket_id,
        "court_id": court_id,
        "court_jurisdiction": jurisdiction,
        "court_is_federal": federal,
        "case_name": _s(row.get("case_name")),
        "case_name_short": _s(row.get("case_name_short")),
        "case_name_full": _s(row.get("case_name_full")),
        "date_filed": _s(row.get("date_filed")),
        "date_filed_is_approximate": _s(row.get("date_filed_is_approximate")),
        "judges": _s(row.get("judges")),
        "nature_of_suit": _s(row.get("nature_of_suit")),
        "precedential_status": _s(row.get("precedential_status")),
        "citation_count": _s(row.get("citation_count")),
        "scdb_id": _s(row.get("scdb_id")),
        "scdb_decision_direction": _s(row.get("scdb_decision_direction")),
        "scdb_votes_majority": _s(row.get("scdb_votes_majority")),
        "scdb_votes_minority": _s(row.get("scdb_votes_minority")),
        "source": _s(row.get("source")),
        "procedural_history": _s(row.get("procedural_history")),
        "attorneys": _s(row.get("attorneys")),
        "posture": _s(row.get("posture")),
        "syllabus": _s(row.get("syllabus")),
        "headnotes": _s(row.get("headnotes")),
        "summary": _s(row.get("summary")),
        "disposition": _s(row.get("disposition")),
        "history": _s(row.get("history")),
        "other_dates": _s(row.get("other_dates")),
        "cross_reference": _s(row.get("cross_reference")),
        "correction": _s(row.get("correction")),
        "arguments": _s(row.get("arguments")),
        "headmatter": _s(row.get("headmatter")),
        "blocked": _s(row.get("blocked")),
        "date_blocked": _s(row.get("date_blocked")),
        "slug": slug,
        "absolute_url": _cluster_url(cluster_id, slug),
        "date_created": _s(row.get("date_created")),
        "date_modified": _s(row.get("date_modified")),
        "ingest_source": "bulk",
    }


def old_rows(body: bytes) -> list[dict]:
    """Parse bulk CSV bytes with the frozen old dialect: backslash-escaped quotes and empty cells as ``None``."""

    previous_limit = csv.field_size_limit(sys.maxsize)
    try:
        with io.TextIOWrapper(io.BytesIO(body), encoding="utf-8", errors="replace", newline="") as text:
            return [
                {k: (v if v != "" else None) for k, v in row.items() if k is not None}
                for row in csv.DictReader(text, escapechar="\\", doublequote=True)
            ]
    finally:
        csv.field_size_limit(previous_limit)
