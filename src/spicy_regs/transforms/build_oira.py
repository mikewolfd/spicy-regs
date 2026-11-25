"""Transforms: build ``oira_reviews.parquet`` and ``oira_meetings.parquet`` from reginfo.gov.

Both tables cover OIRA's Executive Order 12866 review of significant rules, the
lifecycle stage between an agency finishing a draft and the rule appearing in
the Federal Register. Both are keyed by RIN, so they join to ``unified_agenda``
and ``federal_register``. Source acquisition and XML parsing live in
``spicy_docs.sources.oira``.

Both builds are **full refreshes**, not incremental merges: OIRA review records
carry no unique review ID (and pre-2000 files hold distinct reviews sharing RIN,
stage, and received date), while the full history is ~20 MB across ~50 XML files.
Each downloaded XML original is retained once in the shared raw archive via
``CaptureEvidence`` and every emitted row carries ``source_sha256`` pointing to
the exact XML file that produced it (ss-vse3; no per-row ETL receipts).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

if TYPE_CHECKING:
    from spicy_docs.sources.oira import AgencyEntry, OiraCapturedFile, OiraMeetingsReader, OiraReviewsReader
    from spicy_regs.source_evidence import CaptureEvidence

REVIEWS_OUTPUT = "oira_reviews.parquet"
MEETINGS_OUTPUT = "oira_meetings.parquet"

REVIEW_COLUMNS = (
    "rin",
    "agency_code",
    "agency_name",
    "title",
    "stage",
    "review_status",
    "received_date",
    "concluded_date",
    "review_days",
    "review_outcome",
    "economically_significant",
    "major",
    "legal_deadline",
    "review_extended",
    "published_date",
    "flags_json",
    "source_file",
    "source_sha256",
)
REVIEW_SCHEMA = pa.schema([(c, pa.int64() if c == "review_days" else pa.string()) for c in REVIEW_COLUMNS])
_REVIEW_SCHEMA = REVIEW_SCHEMA

MEETING_COLUMNS = (
    "meeting_id",
    "rin",
    "agency_code",
    "agency_name",
    "title",
    "rule_stage",
    "meeting_date",
    "meeting_status",
    "requestor_organization",
    "requestor_name",
    "summary",
    "source_file",
    "source_sha256",
    "url",
)
MEETING_SCHEMA = pa.schema([(c, pa.string()) for c in MEETING_COLUMNS])
_MEETING_SCHEMA = MEETING_SCHEMA

UNDER_REVIEW = "Under Review"
CONCLUDED = "Concluded"

_MEETING_URL_TEMPLATE = "https://www.reginfo.gov/public/do/viewEO12866Meeting?viewRule=true&rin={rin}&meetingId={meeting_id}"

AgencyLookup = Mapping[str, Any]


def _days_between(start: str | None, end: str | None) -> int | None:
    """Calendar days from ``start`` to ``end`` (ISO dates), or ``None`` if either is missing."""
    if not start or not end:
        return None
    return (date.fromisoformat(end) - date.fromisoformat(start)).days


def _lookup_agency(agencies: AgencyLookup, number: str | None) -> tuple[str | None, str | None]:
    if not number or number not in agencies:
        return (None, None)
    entry: AgencyEntry | tuple[str | None, str | None] = agencies[number]
    if isinstance(entry, tuple):
        return entry
    return (entry.acronym, entry.name)


def _agency(
    agencies: AgencyLookup,
    number: str | None,
    fallback_acronym: str | None = None,
) -> tuple[str | None, str | None]:
    """Resolve ``(agency_code, agency_name)`` for a 4-digit RegInfo agency code.

    ``agency_code`` prefers the agency acronym (matching ``unified_agenda.agency_code``).
    When an agency code is absent from ``AGY_AGENCY_LIST.xml`` (for example, defunct
    1981 codes ``2527`` and ``3039``), it falls back to ``fallback_acronym``'s last
    path segment (``USDA/AMS`` -> ``AMS``) and then to the raw numeric code.
    """
    acronym, name = _lookup_agency(agencies, number)
    if acronym:
        return acronym, name
    if fallback_acronym:
        segment = fallback_acronym.rsplit("/", 1)[-1].strip()
        if segment:
            return segment, name
    return number or None, name


def _meeting_url(rin: str | None, meeting_id: str | None) -> str | None:
    if not rin or not meeting_id:
        return None
    return _MEETING_URL_TEMPLATE.format(rin=rin, meeting_id=meeting_id)


def _retain_capture(evidence: CaptureEvidence | None, *, stage: str):
    """Return an ``on_capture`` callback retaining exact raw XML bytes in ``CentralRawArchiveStore``."""
    if evidence is None:
        return None

    def _on_capture(captured: OiraCapturedFile) -> None:
        evidence.retain_bytes(
            captured.raw_bytes,
            stage=stage,
            raw_source=True,
            file_name=captured.file_name,
            source_url=captured.url,
            fetched_at=captured.fetched_at,
        )

    return _on_capture


def _shape_review(doc: Mapping[str, Any], agencies: AgencyLookup) -> dict[str, Any]:
    """Map one ``spicy_docs.sources.oira`` review dict onto published ``oira_reviews`` columns."""
    agency_code, agency_name = _agency(agencies, doc.get("agency_number"))
    received = doc.get("received_date")
    concluded = doc.get("concluded_date")
    is_pending = bool(doc.get("under_review"))
    flags = dict(doc.get("flags") or {})
    # Keep top-level review_extended separate from flags_json to match the declared schema.
    flags.pop("review_extended", None)
    return {
        "rin": doc.get("rin"),
        "agency_code": agency_code,
        "agency_name": agency_name,
        "title": doc.get("title"),
        "stage": doc.get("stage"),
        "review_status": UNDER_REVIEW if is_pending else CONCLUDED,
        "received_date": received,
        "concluded_date": None if is_pending else concluded,
        "review_days": None if is_pending else _days_between(received, concluded),
        "review_outcome": None if is_pending else doc.get("decision"),
        "economically_significant": doc.get("economically_significant"),
        "major": doc.get("major"),
        "legal_deadline": doc.get("legal_deadline"),
        "review_extended": doc.get("review_extended"),
        "published_date": None if is_pending else doc.get("published_date"),
        "flags_json": json.dumps(flags, sort_keys=True) if flags else None,
        "source_file": doc.get("source_file"),
        "source_sha256": doc.get("source_sha256"),
    }


def _shape_meeting(doc: Mapping[str, Any], agencies: AgencyLookup) -> dict[str, Any]:
    """Map one ``spicy_docs.sources.oira`` meeting dict onto published ``oira_meetings`` columns."""
    agency_code, agency_name = _agency(agencies, doc.get("agency_number"), doc.get("agency_acronym"))
    rin, meeting_id = doc.get("rin"), doc.get("meeting_id")
    return {
        "meeting_id": meeting_id,
        "rin": rin,
        "agency_code": agency_code,
        "agency_name": agency_name,
        "title": doc.get("title"),
        "rule_stage": doc.get("rule_stage"),
        "meeting_date": doc.get("meeting_date"),
        "meeting_status": doc.get("meeting_status"),
        "requestor_organization": doc.get("requestor_organization"),
        "requestor_name": doc.get("requestor_name"),
        "summary": doc.get("summary"),
        "source_file": doc.get("source_file"),
        "source_sha256": doc.get("source_sha256"),
        "url": _meeting_url(rin, meeting_id),
    }


def build_oira_reviews(
    output_dir: Path,
    *,
    reader: OiraReviewsReader | None = None,
    evidence: CaptureEvidence | None = None,
) -> Path:
    """Build ``oira_reviews.parquet``: every EO 12866 review, concluded and pending."""
    from spicy_docs.sources.oira import OiraReviewsReader as DefaultReviewsReader, OiraUnavailableError

    active_reader = reader or DefaultReviewsReader(
        on_capture=_retain_capture(evidence, stage="oira-reviews-xml"),
    )
    docs = [doc for doc in active_reader.iter_records() if doc.get("rin")]
    if not docs:
        raise OiraUnavailableError("no EO 12866 review records fetched")
    rows = [_shape_review(doc, active_reader.agencies) for doc in docs]
    rows.sort(
        key=lambda r: (r["received_date"] or "", r["rin"] or "", r["source_file"] or ""),
        reverse=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / REVIEWS_OUTPUT
    pq.write_table(pa.Table.from_pylist(rows, schema=REVIEW_SCHEMA), out_file, compression="zstd")
    pending = sum(1 for r in rows if r["review_status"] == UNDER_REVIEW)
    if evidence is not None:
        evidence.event(
            "oira-reviews-built",
            rows=len(rows),
            under_review=pending,
            files=dict(active_reader.file_counts),
            file_sha256=dict(active_reader.file_sha256),
        )
    logger.info(
        "OIRA reviews: {:,} rows ({:,} under review) from {} files",
        len(rows),
        pending,
        len(active_reader.file_counts),
    )
    return out_file


def build_oira_meetings(
    output_dir: Path,
    *,
    reader: OiraMeetingsReader | None = None,
    evidence: CaptureEvidence | None = None,
) -> Path:
    """Build ``oira_meetings.parquet``: OIRA's EO 12866 meeting log, one row per log entry."""
    from spicy_docs.sources.oira import OiraMeetingsReader as DefaultMeetingsReader, OiraUnavailableError

    active_reader = reader or DefaultMeetingsReader(
        on_capture=_retain_capture(evidence, stage="oira-meetings-xml"),
    )
    docs = [doc for doc in active_reader.iter_records() if doc.get("rin")]
    if not docs:
        raise OiraUnavailableError("no EO 12866 meeting records fetched")
    rows = [_shape_meeting(doc, active_reader.agencies) for doc in docs]
    rows.sort(
        key=lambda r: (
            r["meeting_date"] or "",
            r["rin"] or "",
            r["requestor_organization"] or "",
            r["meeting_id"] or "",
        ),
        reverse=True,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / MEETINGS_OUTPUT
    pq.write_table(pa.Table.from_pylist(rows, schema=MEETING_SCHEMA), out_file, compression="zstd")
    if evidence is not None:
        evidence.event(
            "oira-meetings-built",
            rows=len(rows),
            files=dict(active_reader.file_counts),
            file_sha256=dict(active_reader.file_sha256),
        )
    logger.info("OIRA meetings: {:,} rows from {} files", len(rows), len(active_reader.file_counts))
    return out_file
