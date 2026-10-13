"""Build-time interpretation of held FEC context; no source acquisition or SQL."""

from datetime import datetime, timezone
import re

import pyarrow as pa

QUERY_SHAPE_VERSION = "fec-query-shape-v1"
API_CONTROL_FIELDS = (
    [
        (n, pa.string())
        for n in ("response_field", "response_field_role", "value_status", "api_version", "parsing_status")
    ]
    + [(n, pa.int64()) for n in ("page", "per_page", "reported_count", "reported_pages", "observed_count")]
    + [("is_count_exact", pa.bool_())]
)


def nonnegative_integer(value):
    """Preserve exact integer meaning and reject Python's bool-is-int coercion."""
    return value if type(value) is int and 0 <= value <= 9223372036854775807 else None


def api_control_values(native):
    field, value = native["field"], native.get("value")
    row = dict.fromkeys(n for n, _ in API_CONTROL_FIELDS)
    row.update(
        response_field=field,
        response_field_role="result-container" if field == "results" else "response-control",
        value_status="source_null" if value is None else "source_empty" if value in ("", []) else "reported",
        parsing_status="unsupported_field",
    )
    if field == "api_version":
        row["api_version"] = value if isinstance(value, str) else None
        row["parsing_status"] = "parsed" if isinstance(value, str) else "unsupported_value"
    elif field == "results":
        row["observed_count"] = len(value) if isinstance(value, list) else None
        row["parsing_status"] = "parsed" if isinstance(value, list) else "unsupported_value"
    elif field == "pagination":
        if isinstance(value, dict):
            for name, source in (
                ("page", "page"),
                ("per_page", "per_page"),
                ("reported_count", "count"),
                ("reported_pages", "pages"),
            ):
                row[name] = nonnegative_integer(value.get(source))
            row["is_count_exact"] = value.get("is_count_exact") if type(value.get("is_count_exact")) is bool else None
        row["parsing_status"] = (
            "parsed"
            if all(
                row[n] is not None for n in ("page", "per_page", "reported_count", "reported_pages", "is_count_exact")
            )
            else "invalid_or_missing_pagination"
        )
    return row


def exact_date(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", value):
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None


def feed_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT", value
    ):
        return None
    try:
        return datetime.strptime(value, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def iso_timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
    except ValueError:
        return None


def filing_announcement(description, published):
    fields = {
        "reported_filer_id": "CommitteeId",
        "filing_id": "FilingId",
        "form_type": "FormType",
        "coverage_start_raw": "CoverageFrom",
        "coverage_end_raw": "CoverageThrough",
        "report_type": "ReportType",
    }
    row, repeated = {}, False
    for name, label in fields.items():
        values = re.findall(r"(?:^|[|*])\s*" + label + r":\s*([^|*]*)", description or "")
        repeated |= len(values) > 1
        row[name] = values[0].strip() or None if len(values) == 1 else None
    filer = row["reported_filer_id"] or ""
    row["committee_id"] = filer if re.fullmatch(r"C[0-9]{8}", filer) else None
    row["candidate_id"] = filer if re.fullmatch(r"(?:[HS][0-9][A-Z]{2}[0-9]{5}|P[0-9]{8})", filer) else None
    row["source_label_status"] = (
        "committee_id"
        if row["committee_id"]
        else "candidate_id_under_committee_label"
        if row["candidate_id"]
        else "missing"
        if not filer
        else "unsupported_identifier"
    )
    row["coverage_start_date"] = exact_date(row["coverage_start_raw"])
    row["coverage_end_date"] = exact_date(row["coverage_end_raw"])
    row["published_at"] = feed_timestamp(published)
    row["filing_link_status"] = "source_assertion_not_qualified_filing"
    row["parsing_status"] = (
        "repeated_labels"
        if repeated
        else "missing_identifiers"
        if not filer or not row["filing_id"]
        else "invalid_identifiers"
        if not (row["committee_id"] or row["candidate_id"]) or not re.fullmatch(r"[0-9]+", row["filing_id"])
        else "invalid_date"
        if any(
            row[raw] is not None and row[typed] is None
            for raw, typed in (("coverage_start_raw", "coverage_start_date"), ("coverage_end_raw", "coverage_end_date"))
        )
        or published is not None
        and row["published_at"] is None
        else "parsed"
    )
    return row


def source_page_body(title, text, url):
    """Known held-text boundaries only; unsupported captures remain diagnostics."""
    if (title or "").strip().lower() in {"server error", "403 forbidden", "404 not found", "access denied"}:
        return None, "failed_page"
    if not text or not text.strip():
        return None, "empty_page"
    parts = text.split("Federal Election Commission | United States of America")
    footer = "About\nCareers\nPress\nContact\nPrivacy and security policy"
    if len(parts) == 3 and footer in parts[2]:
        return parts[2].split(footer, 1)[0].strip() or None, "body_extracted"
    if (url or "").startswith("https://docquery.fec.gov/") and title in {"FEC FORM 3", "FEC FORM 99"}:
        marker = "\n" + title + "\n"
        if marker in text and "\nGenerated " in text and "Federal Election Commission (800) 424-9530" in text:
            return title + "\n" + text.split(marker, 1)[1].split("\nGenerated ", 1)[0], "body_extracted"
    return None, "unsupported_body_boundaries"


CSV_FIELDS = (
    "amendment_indicator",
    "amendment_indicator_desc",
    "memo_code",
    "memo_text",
    "file_number",
    "filing_form",
    "report_type",
    "line_number",
    "schedule_type",
    "image_number",
    "back_reference_transaction_id",
    "back_reference_schedule_name",
)


def document_context_fields(native, *, source_created_at=None, source_updated_at=None):
    """Interpret held document metadata; report unavailable fields separately."""
    if native is not None and not isinstance(native, dict):
        raise ValueError('Document metadata must be a named source object or null')
    available = native is not None
    metadata = native or {}
    language = metadata.get('language')
    if language is not None and not isinstance(language, str):
        raise ValueError('Document language must be native text or null')
    count = nonnegative_integer(metadata.get('page_count'))
    count_status = ('unavailable_retained_metadata' if not available else 'parsed' if count is not None
                    else 'missing' if metadata.get('page_count') is None else 'invalid_count')
    created = metadata.get('created_at') if available else source_created_at
    updated = metadata.get('updated_at') if available else source_updated_at
    values = dict(language=language, page_count=count, page_count_status=count_status,
                  created_at=iso_timestamp(created), updated_at=iso_timestamp(updated), evidence_use='discovery_only')
    statuses = {'language': 'unavailable_retained_metadata' if not available else 'source_null' if language is None else 'reported',
                'page_count': count_status}
    for name, raw in (('created_at', created), ('updated_at', updated)):
        statuses[name] = ('parsed' if values[name] is not None else ('source_null' if name in metadata else 'source_missing') if raw is None and available
                          else 'unavailable_retained_timestamp' if raw is None else 'source_empty' if raw == ''
                          else 'invalid_or_unzoned_timestamp')
    return values, statuses


def csv_context_fields(native):
    """Reuse native CSV control/year spellings without widening partial coverage."""
    if native is not None and not isinstance(native, dict):
        raise ValueError('CSV fields must be a named source object or null')
    fields = native or {}
    for name in (*CSV_FIELDS, 'report_year'):
        if fields.get(name) is not None and not isinstance(fields[name], str):
            raise ValueError(f'CSV {name} must be native text or null')
    raw_year = fields.get('report_year')
    parsed = bool(re.fullmatch(r'[1-9][0-9]{3}', raw_year or ''))
    status = ('unavailable_retained_fields' if native is None else 'parsed' if parsed
              else 'missing' if not raw_year else 'unsupported_year')
    values = {name: fields.get(name) or None for name in CSV_FIELDS}
    values.update(report_year=int(raw_year) if parsed and raw_year is not None else None, report_year_status=status)
    statuses = {name: 'unavailable_retained_fields' if native is None else 'missing' if not fields.get(name) else 'reported'
                for name in CSV_FIELDS}
    statuses['report_year'] = status
    return values, statuses


def response_capture_disposition(profile_refusal, payload_shape):
    """Classify reported refusal/shape; empty payloads never prove coverage."""
    if profile_refusal is not None and not isinstance(profile_refusal, str):
        raise ValueError('Response profile refusal must be native text or null')
    if payload_shape is not None and not isinstance(payload_shape, str):
        raise ValueError('Response payload shape must be native text or null')
    if profile_refusal:
        return 'refused'
    if payload_shape is None:
        return 'unavailable_retained_payload_shape'
    return ('empty_payload_not_verified_success' if payload_shape == 'results_empty'
            else 'captured_payload_not_verified_success')

COL_DESCRIPTIONS = {
    "reported_filer_id": "Literal identifier labelled CommitteeId in the source feed; candidate filings can carry candidate identifiers under this label.",
    "source_label_status": "Whether the feed's CommitteeId label contains a committee identifier, a candidate identifier, a missing value or an unsupported spelling.",
    "filing_link_status": "Source feed assertion only; the value does not qualify a filing match or establish the target exists.",
    "coverage_start_raw": "Literal CoverageFrom value from the feed's explicitly labelled metadata.",
    "coverage_end_raw": "Literal CoverageThrough value from the feed's explicitly labelled metadata.",
    "coverage_start_date": "Build-parsed CoverageFrom date; requires exact MM/DD/YYYY spelling and a valid calendar date.",
    "coverage_end_date": "Build-parsed CoverageThrough date; requires exact MM/DD/YYYY spelling and a valid calendar date.",
    "published_at": "Build-parsed feed publication instant from an exact four-digit-year GMT timestamp; invalid input remains null with parsing status.",
    "content_status": "Page body disposition: body_extracted, failed_page, empty_page or unsupported_body_boundaries. Filter body_extracted for subject search; every capture retains its source evidence pointer.",
    "capture_disposition": "Distinguishes a provider refusal from an empty or present held payload. No value asserts successful source coverage.",
    "evidence_use": "Discovery-only research lead; does not establish a verified FEC matter relationship or fetched document body.",
    "page_count_status": "Whether the document provider supplied an exact nonnegative integer page count, no value, or an invalid count.",
    "report_year_status": "Whether the CSV source report year has an exact four-digit spelling, is missing, or is unsupported.",
    "source_fact_count": "Number of held native facts or events processed in this collection context, stored as an integer.",
    "observed_count": "Number of results in the held API response array; the array remains source evidence and is not copied into this row.",
    "query_completeness_status": "Whether the aggregated controls agree on their declared completeness; agreement does not verify completeness.",
}
