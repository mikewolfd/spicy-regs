"""Build declared executive disclosure lookup, asset/income, transaction, and page tables."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import ExitStack
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

TABLES = (
    "executive_disclosure_filings",
    "executive_disclosure_facts",
    "executive_disclosure_transactions",
    "executive_disclosure_pages",
)
ACCESS_STATES = frozenset({"not_requested", "requested", "available", "unavailable", "expired", "access_refused"})
ACCESS_BASES = frozenset({"public_posting", "form201_required", "agency_request_required", "human_attestation_required"})
AMENDMENT_STATES = frozenset({"none_observed_in_retained_document", "included_unreconciled", "unknown"})
FORM_TYPES = frozenset({"278e", "278-T"})
ALLOWED_REPORT_TYPES = frozenset(
    {"Annual", "New Entrant", "Nominee", "Termination", "Candidate", "Periodic Transaction", "Amendment"}
)
USD_AMOUNT_STATUSES = frozenset({"range", "below_threshold", "over_threshold", "exact_amount"})
MANIFEST_V2_FORMAT = "executive-disclosures-cohort"
MANIFEST_V2_VERSION = 2
DEFAULT_BATCH_SIZE = 2_000

_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")
_OWNER_RE = re.compile(r"^\((Self|Spouse|Joint)\)")
_BELOW_THRESHOLD_RE = re.compile(
    r"None\s*\(\s*or\s+less\s+than\s+\$([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)\s*\)",
    re.IGNORECASE,
)
_RANGE_RE = re.compile(
    r"\$([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)\s*[-\u2013]\s*\$([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)"
)
_OVER_THRESHOLD_RE = re.compile(r"Over\s+\$([0-9]{1,3}(?:,[0-9]{3})*|[0-9]+)", re.IGNORECASE)
_EXACT_AMOUNT_RE = re.compile(r"\$(0|[1-9][0-9]{0,2}(?:,[0-9]{3})*|[1-9][0-9]*)(?:\.([0-9]{2}))?")
_MDY_DATE_RE = re.compile(r"[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}")
_ISO_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def _schema(text: str, extra: Sequence[tuple[str, pa.DataType]] = (), **typed: pa.DataType) -> pa.Schema:
    return pa.schema([(name, pa.string()) for name in text.split()] + list(typed.items()) + list(extra))


SCHEMAS = {
    TABLES[0]: _schema(
        """cohort_id coverage_note filing_id official_id official_name position_raw agency_raw
        report_type report_period_raw form_type filing_date filer_signed_date agency_certification_date
        oge_certification_date metadata_review_ref source_url source_sha256 acquired_at access_status access_basis
        access_reason access_evidence_ref amendment_status amendment_details_json facts_status facts_reason
        metadata_json""",
        extra=(("qualified_sections_json", pa.string()),),
        report_year=pa.int64(),
        byte_size=pa.int64(),
        cover_page=pa.int64(),
    ),
    TABLES[1]: _schema(
        """cohort_id filing_id fact_id section row_number_raw description_raw eif_raw value_raw
        income_type_raw income_amount_raw ownership_label value_status income_status currency
        source_url source_sha256 source_locator parser_revision backend_version raw_cells_json cell_boxes_json""",
        extra=(
            ("section_code", pa.string()),
            ("value_exact_usd", pa.string()),
            ("income_exact_usd", pa.string()),
        ),
        page=pa.int64(),
        value_lower=pa.int64(),
        value_upper=pa.int64(),
        income_lower=pa.int64(),
        income_upper=pa.int64(),
    ),
    TABLES[2]: _schema(
        """cohort_id filing_id transaction_id form_type section section_code row_number_raw
        description_raw ownership_label transaction_type_raw transaction_date_raw transaction_date_status
        notification_over_30_days_raw amount_raw amount_status currency source_url source_sha256
        source_locator parser_revision backend_version raw_cells_json cell_boxes_json""",
        page=pa.int64(),
        amount_lower=pa.int64(),
        amount_upper=pa.int64(),
        transaction_date=pa.date32(),
    ),
    TABLES[3]: _schema(
        """cohort_id filing_id source_url source_sha256 source_locator page_text filer_identity_json table_bbox_json
        parser_revision backend_version""",
        extra=(
            ("section", pa.string()),
            ("layout_profile", pa.string()),
            ("endnotes_json", pa.string()),
        ),
        page=pa.int64(),
        source_slot_count=pa.int64(),
        blank_slot_count=pa.int64(),
    ),
}

FILING_FIELDS = set(SCHEMAS[TABLES[0]].names) - {
    "cohort_id",
    "coverage_note",
    "facts_status",
    "facts_reason",
    "metadata_json",
    "qualified_sections_json",
}

V2_REQUIRED_FILING_FIELDS = frozenset(
    {
        "filing_id",
        "official_id",
        "official_name",
        "source_url",
        "source_sha256",
        "byte_size",
        "acquired_at",
        "access_status",
        "access_basis",
        "access_reason",
        "access_evidence_ref",
    }
)
V2_OPTIONAL_FILING_FIELDS = frozenset(
    {
        "amendment_status",
        "amendment_details_json",
        "cover_page",
        "metadata_review_ref",
        "cover_override",
    }
)
V2_ALLOWED_FILING_FIELDS = V2_REQUIRED_FILING_FIELDS | V2_OPTIONAL_FILING_FIELDS
COVER_OVERRIDE_REQUIRED_FIELDS = frozenset(
    {"position_raw", "agency_raw", "report_type", "report_period_raw", "report_year", "form_type"}
)
COVER_OVERRIDE_OPTIONAL_FIELDS = frozenset(
    {"filing_date", "filer_signed_date", "agency_certification_date", "oge_certification_date", "cover_page"}
)
COVER_OVERRIDE_ALLOWED_FIELDS = COVER_OVERRIDE_REQUIRED_FIELDS | COVER_OVERRIDE_OPTIONAL_FIELDS


def _collapse_ws(value: str) -> str:
    return " ".join(value.split())


def is_v2_manifest(selection: Mapping[str, Any]) -> bool:
    return selection.get("format") == MANIFEST_V2_FORMAT or selection.get("version") == MANIFEST_V2_VERSION


def amount_range(raw: str) -> tuple[str, int | None, int | None, str | None]:
    """Classify OGE statutory dollar brackets and exact USD amounts without guessing or truncating cents.

    Returns ``(status, lower, upper, exact_usd)`` where ``status`` is one of:
    ``blank``, ``below_threshold``, ``range``, ``over_threshold``, ``exact_amount``, or ``unrecognized``.
    """
    if not isinstance(raw, str):
        return "unrecognized", None, None, None
    value = _collapse_ws(raw)
    if value == "":
        return "blank", None, None, None

    below = _BELOW_THRESHOLD_RE.fullmatch(value)
    if below:
        threshold = int(below[1].replace(",", ""))
        if threshold in (201, 1001):
            return "below_threshold", 0, threshold - 1, None
        return "unrecognized", None, None, None

    rng = _RANGE_RE.fullmatch(value)
    if rng:
        lower, upper = (int(part.replace(",", "")) for part in rng.groups())
        if lower > upper:
            return "unrecognized", None, None, None
        return "range", lower, upper, None

    over = _OVER_THRESHOLD_RE.fullmatch(value)
    if over:
        lower = int(over[1].replace(",", ""))
        if lower <= 0:
            return "unrecognized", None, None, None
        return "over_threshold", lower, None, None

    exact = _EXACT_AMOUNT_RE.fullmatch(value)
    if exact:
        whole = exact[1].replace(",", "")
        cents = exact[2]
        exact_usd = f"{whole}.{cents}" if cents is not None else whole
        return "exact_amount", None, None, exact_usd

    return "unrecognized", None, None, None


def resolve_income_amount(
    income_amount_raw: str | None, income_exact_raw: str | None = ""
) -> tuple[str, str, int | None, int | None, str | None]:
    """Resolve income amount across Updated July 2020 and Updated Nov. 2021 layouts, failing closed on conflict.

    On Updated Nov. 2021 forms, ``read_excel_278e`` sets ``income_exact_raw`` to the split col-6 cell
    and coalesces it into ``income_amount_raw`` (so both carry the same value). On Updated July 2020
    forms, exact amounts live in ``income_amount_raw`` with ``income_exact_raw == ""``. If both are
    non-empty and differ, raise ``ValueError`` rather than guessing which cell is authoritative.
    """
    amount_str = "" if income_amount_raw is None else income_amount_raw
    exact_str = "" if income_exact_raw is None else income_exact_raw
    if not isinstance(amount_str, str) or not isinstance(exact_str, str):
        return "", "unrecognized", None, None, None
    amount_norm = _collapse_ws(amount_str)
    exact_norm = _collapse_ws(exact_str)
    if amount_norm and exact_norm and amount_norm != exact_norm:
        raise ValueError(
            f"conflicting income_amount_raw ({income_amount_raw!r}) and income_exact_raw ({income_exact_raw!r})"
        )
    canonical = exact_str if exact_norm and not amount_norm else amount_str
    status, lower, upper, exact_usd = amount_range(canonical)
    return canonical, status, lower, upper, exact_usd


def parse_transaction_date(raw: str) -> tuple[date | None, str]:
    """Parse explicit printed OGE transaction dates without guessing partial or invalid dates."""
    if not isinstance(raw, str):
        return None, "unparsed"
    value = _collapse_ws(raw)
    if value == "":
        return None, "blank"
    if _MDY_DATE_RE.fullmatch(value):
        try:
            parsed = datetime.strptime(value, "%m/%d/%Y").date()
        except ValueError:
            return None, "unparsed"
        return (parsed, "parsed") if 1900 <= parsed.year <= 9999 else (None, "unparsed")
    if _ISO_DATE_RE.fullmatch(value):
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None, "unparsed"
        return (parsed, "parsed") if 1900 <= parsed.year <= 9999 else (None, "unparsed")
    return None, "unparsed"


def _extract_ownership_label(description_raw: str) -> str | None:
    match = _OWNER_RE.match(description_raw.strip())
    return match[1] if match else None


def _validate_report_identity(
    *,
    position_raw: Any,
    agency_raw: Any,
    report_type: Any,
    report_period_raw: Any,
    report_year: Any,
    form_type: Any,
    cover_page: Any,
) -> None:
    if not isinstance(position_raw, str) or not position_raw.strip():
        raise ValueError("each filing needs a nonempty position_raw")
    if not isinstance(agency_raw, str):
        raise ValueError("agency_raw must be a string (empty string for blank cover cell)")
    if not isinstance(form_type, str) or form_type not in FORM_TYPES:
        raise ValueError(f"unsupported form_type {form_type!r}; expected one of {sorted(FORM_TYPES)}")
    if not isinstance(report_type, str) or report_type not in ALLOWED_REPORT_TYPES:
        raise ValueError(f"unsupported report_type {report_type!r}")
    if not isinstance(report_period_raw, str):
        raise ValueError("report_period_raw must be a string (empty string for blank non-annual cover cell)")
    if report_type == "Annual":
        if not report_period_raw.strip():
            raise ValueError("annual filing needs a nonempty report_period_raw")
        if type(report_year) is not int or not 1900 <= report_year <= 9999:
            raise ValueError("native report year must be an explicit calendar year")
        if report_period_raw != str(report_year):
            raise ValueError("annual native reporting period must match its reviewed cover year")
    elif report_year is not None and (type(report_year) is not int or not 1900 <= report_year <= 9999):
        raise ValueError("native report year must be an explicit calendar year or null")
    if type(cover_page) is not int or cover_page < 1:
        raise ValueError("cover page must be a one-based original locator")


def _validate_partial_cover_override(override: Mapping[str, Any], *, official_id: str, default_cover_page: Any) -> None:
    """Validate field types and constraints on partial or complete cover_override mappings up front."""
    if "cover_page" in override and (type(override["cover_page"]) is not int or override["cover_page"] < 1):
        raise ValueError("cover_override.cover_page must be a positive integer")
    if "position_raw" in override:
        if not isinstance(override["position_raw"], str) or not override["position_raw"].strip():
            raise ValueError("cover_override.position_raw must be a nonempty string")
        if override["position_raw"] == official_id:
            raise ValueError("person identity cannot be an office")
    if "agency_raw" in override and not isinstance(override["agency_raw"], str):
        raise ValueError("cover_override.agency_raw must be a string")
    if "form_type" in override and (not isinstance(override["form_type"], str) or override["form_type"] not in FORM_TYPES):
        raise ValueError(f"unsupported cover_override.form_type {override['form_type']!r}")
    if "report_type" in override and (
        not isinstance(override["report_type"], str) or override["report_type"] not in ALLOWED_REPORT_TYPES
    ):
        raise ValueError(f"unsupported cover_override.report_type {override['report_type']!r}")
    if "report_period_raw" in override and not isinstance(override["report_period_raw"], str):
        raise ValueError("cover_override.report_period_raw must be a string")
    if "report_year" in override and override["report_year"] is not None:
        if type(override["report_year"]) is not int or not 1900 <= override["report_year"] <= 9999:
            raise ValueError("cover_override.report_year must be an explicit calendar year or null")
    if COVER_OVERRIDE_REQUIRED_FIELDS <= set(override):
        _validate_report_identity(
            position_raw=override["position_raw"],
            agency_raw=override["agency_raw"],
            report_type=override["report_type"],
            report_period_raw=override["report_period_raw"],
            report_year=override["report_year"],
            form_type=override["form_type"],
            cover_page=override.get("cover_page", default_cover_page),
        )
    elif override.get("report_type") == "Annual":
        if "report_year" in override and (
            type(override["report_year"]) is not int or not 1900 <= override["report_year"] <= 9999
        ):
            raise ValueError("annual cover_override.report_year must be an explicit calendar year")
        if "report_period_raw" in override and not override["report_period_raw"].strip():
            raise ValueError("annual cover_override.report_period_raw must be nonempty")
        if (
            "report_year" in override
            and "report_period_raw" in override
            and override["report_period_raw"] != str(override["report_year"])
        ):
            raise ValueError("annual cover_override.report_period_raw must match report_year")


def _validate_optional_dates(container: Mapping[str, Any]) -> None:
    for key in ("filing_date", "filer_signed_date", "agency_certification_date", "oge_certification_date"):
        val = container.get(key)
        if val is not None and (not isinstance(val, str) or not val.strip()):
            raise ValueError(f"{key} must be a nonempty string or null")


def _validate_common_access(item: Mapping[str, Any]) -> None:
    if not isinstance(item.get("access_reason"), str) or not item["access_reason"].strip():
        raise ValueError("each access outcome needs its explicit reason")
    if item.get("access_status") not in ACCESS_STATES or item.get("access_basis") not in ACCESS_BASES:
        raise ValueError("unknown access outcome or access basis")
    amendment_status = item.get("amendment_status", "none_observed_in_retained_document")
    if amendment_status not in AMENDMENT_STATES:
        raise ValueError("unknown amendment state")
    amendment_details_json = item.get("amendment_details_json", "[]")
    if not isinstance(amendment_details_json, str) or not isinstance(json.loads(amendment_details_json), list):
        raise ValueError("amendment details must be an ordered JSON list")
    if item["access_status"] == "available":
        from spicy_docs.sources.executive_disclosures import classify_filing_url

        classify_filing_url(item["source_url"])
        if item["access_basis"] != "public_posting":
            raise ValueError("this route only replays public postings; human access is separate")
        if not isinstance(item.get("source_sha256"), str) or not _SHA256_RE.fullmatch(item["source_sha256"]):
            raise ValueError("available filing requires an exact original digest")
        if type(item.get("byte_size")) is not int or item["byte_size"] <= 0:
            raise ValueError("available filing requires its original byte size")
        if not isinstance(item.get("acquired_at"), str) or not item["acquired_at"].strip():
            raise ValueError("available filing requires its acquisition timestamp")
    elif item.get("source_sha256") is not None or item.get("byte_size") is not None:
        raise ValueError("an unavailable/requested record cannot claim available original bytes")


def _validate_v1_filing(item: Any, seen: set[str]) -> None:
    if not isinstance(item, dict) or set(item) != FILING_FIELDS:
        raise ValueError("filing selection fields differ from the declared schema")
    if not all(
        isinstance(item[key], str) and item[key].strip()
        for key in (
            "filing_id",
            "official_id",
            "official_name",
            "position_raw",
            "report_type",
            "form_type",
            "metadata_review_ref",
            "access_evidence_ref",
        )
    ):
        raise ValueError("each filing needs reviewed person/report identity and evidence")
    if item["official_id"] == item["position_raw"] or item["filing_id"] in seen:
        raise ValueError("person identity cannot be an office and filing identities must be unique")
    seen.add(item["filing_id"])
    _validate_report_identity(
        position_raw=item["position_raw"],
        agency_raw=item["agency_raw"],
        report_type=item["report_type"],
        report_period_raw=item["report_period_raw"],
        report_year=item["report_year"],
        form_type=item["form_type"],
        cover_page=item["cover_page"],
    )
    _validate_optional_dates(item)
    _validate_common_access(item)


def _validate_v2_filing(item: Any, seen: set[str]) -> None:
    if not isinstance(item, dict):
        raise ValueError("each v2 filing entry must be a JSON object")
    keys = set(item)
    missing = V2_REQUIRED_FILING_FIELDS - keys
    extra = keys - V2_ALLOWED_FILING_FIELDS
    if missing or extra:
        raise ValueError(f"v2 filing fields differ from manifest schema (missing={sorted(missing)}, extra={sorted(extra)})")
    if not all(
        isinstance(item[key], str) and item[key].strip()
        for key in ("filing_id", "official_id", "official_name", "access_evidence_ref")
    ):
        raise ValueError("each v2 filing needs filing_id, official_id, official_name, and access_evidence_ref")
    if item["filing_id"] in seen:
        raise ValueError("filing identities must be unique")
    seen.add(item["filing_id"])
    if "cover_page" in item and (type(item["cover_page"]) is not int or item["cover_page"] < 1):
        raise ValueError("cover_page must be a one-based positive integer")
    if "metadata_review_ref" in item and item["metadata_review_ref"] is not None:
        if not isinstance(item["metadata_review_ref"], str) or not item["metadata_review_ref"].strip():
            raise ValueError("metadata_review_ref must be a nonempty string when provided")

    override = item.get("cover_override")
    if override is not None:
        if not isinstance(override, dict):
            raise ValueError("cover_override must be a JSON object")
        extra_ov = set(override) - COVER_OVERRIDE_ALLOWED_FIELDS
        if extra_ov:
            raise ValueError(f"unknown cover_override fields: {sorted(extra_ov)}")
        _validate_optional_dates(override)
        _validate_partial_cover_override(
            override,
            official_id=item["official_id"],
            default_cover_page=item.get("cover_page", 1),
        )

    if item["access_status"] != "available":
        if not isinstance(override, dict) or not (COVER_OVERRIDE_REQUIRED_FIELDS <= set(override)):
            raise ValueError("unavailable v2 filings require a complete cover_override")
        if not isinstance(item.get("metadata_review_ref"), str) or not item["metadata_review_ref"].strip():
            raise ValueError("unavailable v2 filings require a nonempty metadata_review_ref")

    _validate_common_access(item)


def validate_selection(selection: dict) -> None:
    """Validate either a legacy v1 selection or a compact v2 cohort manifest."""
    if not isinstance(selection, dict) or not selection.get("cohort_id") or not selection.get("coverage_note"):
        raise ValueError("selection needs a cohort identity and coverage limit")
    v2 = is_v2_manifest(selection)
    if v2 and (selection.get("format") != MANIFEST_V2_FORMAT or selection.get("version") != MANIFEST_V2_VERSION):
        raise ValueError(f"v2 cohort manifest requires format={MANIFEST_V2_FORMAT!r} and version={MANIFEST_V2_VERSION}")
    filings = selection.get("filings")
    if not isinstance(filings, list) or not filings:
        raise ValueError("declare a nonempty report cohort")
    seen: set[str] = set()
    for item in filings:
        if v2:
            _validate_v2_filing(item, seen)
        else:
            _validate_v1_filing(item, seen)


def _resolve_v2_filing_metadata(item: Mapping[str, Any], body: bytes | None) -> tuple[dict[str, Any], Any]:
    """Derive cover metadata deterministically from readable native covers or reviewed cover_override.

    Returns ``(resolved_metadata, native_cover)`` where ``native_cover`` is the verified
    ``CoverMetadata`` object only when the PDF's cover page was natively ``readable`` (never a
    human-supplied dict override for scanned covers).
    """
    override = item.get("cover_override") or {}
    cover_page = override.get("cover_page", item.get("cover_page", 1))
    amendment_status = item.get("amendment_status", "none_observed_in_retained_document")
    amendment_details_json = item.get("amendment_details_json", "[]")

    if body is None:
        resolved = {
            "filing_id": item["filing_id"],
            "official_id": item["official_id"],
            "official_name": item["official_name"],
            "position_raw": override["position_raw"],
            "agency_raw": override["agency_raw"],
            "report_type": override["report_type"],
            "report_period_raw": override["report_period_raw"],
            "report_year": override["report_year"],
            "form_type": override["form_type"],
            "filing_date": override.get("filing_date"),
            "filer_signed_date": override.get("filer_signed_date"),
            "agency_certification_date": override.get("agency_certification_date"),
            "oge_certification_date": override.get("oge_certification_date"),
            "metadata_review_ref": item["metadata_review_ref"],
            "source_url": item["source_url"],
            "source_sha256": item["source_sha256"],
            "byte_size": item["byte_size"],
            "acquired_at": item["acquired_at"],
            "access_status": item["access_status"],
            "access_basis": item["access_basis"],
            "access_reason": item["access_reason"],
            "access_evidence_ref": item["access_evidence_ref"],
            "amendment_status": amendment_status,
            "amendment_details_json": amendment_details_json,
            "cover_page": cover_page,
        }
        return resolved, None

    from spicy_docs.sources.executive_disclosures import UnsupportedDisclosure, read_form278e_cover

    cover = None
    cover_unsupported_reason: str | None = None
    try:
        cover = read_form278e_cover(body, cover_page=cover_page)
    except UnsupportedDisclosure as error:
        cover_unsupported_reason = str(error)

    if cover is not None and cover.status == "readable":
        native_name = _collapse_ws(cover.filer_name or "")
        declared_name = _collapse_ws(item["official_name"])
        if native_name != declared_name:
            raise ValueError(
                f"declared official_name ({item['official_name']!r}) differs from native cover filer_name ({cover.filer_name!r})"
            )

        raw_native: dict[str, Any] = {
            "position_raw": cover.position_raw,
            "agency_raw": cover.agency_raw,
            "report_type": cover.report_type,
            "report_period_raw": cover.report_period_raw,
            "report_year": cover.report_year,
            "form_type": "278e",
        }
        resolved_cover: dict[str, Any] = {}
        for key, native_val in raw_native.items():
            native_blank = native_val is None or native_val == ""
            if key in override:
                ov_val = override[key]
                if not native_blank and ov_val != native_val:
                    raise ValueError(
                        f"cover_override.{key} ({ov_val!r}) conflicts with readable native cover ({native_val!r})"
                    )
                if native_blank and ov_val is not None:
                    resolved_cover[key] = ov_val
                elif key in ("position_raw", "agency_raw", "report_period_raw"):
                    resolved_cover[key] = native_val if native_val is not None else ""
                else:
                    resolved_cover[key] = native_val
            elif key in ("position_raw", "agency_raw", "report_period_raw"):
                resolved_cover[key] = native_val if native_val is not None else ""
            else:
                resolved_cover[key] = native_val

        if (
            "oge_certification_date" in override
            and override["oge_certification_date"] is not None
            and cover.oge_digital_sig_date is not None
            and override["oge_certification_date"] != cover.oge_digital_sig_date
        ):
            raise ValueError(
                f"cover_override.oge_certification_date ({override['oge_certification_date']!r}) conflicts "
                f"with native cover digital signature date ({cover.oge_digital_sig_date!r})"
            )

        position_raw = resolved_cover["position_raw"]
        if item["official_id"] == position_raw:
            raise ValueError("person identity cannot be an office")
        _validate_report_identity(
            position_raw=position_raw,
            agency_raw=resolved_cover["agency_raw"],
            report_type=resolved_cover["report_type"],
            report_period_raw=resolved_cover["report_period_raw"],
            report_year=resolved_cover["report_year"],
            form_type=resolved_cover["form_type"],
            cover_page=cover.cover_page,
        )
        review_ref = item.get("metadata_review_ref") or f"native-cover:p{cover.cover_page}:{item['source_sha256']}"
        return {
            "filing_id": item["filing_id"],
            "official_id": item["official_id"],
            "official_name": item["official_name"],
            "position_raw": position_raw,
            "agency_raw": resolved_cover["agency_raw"],
            "report_type": resolved_cover["report_type"],
            "report_period_raw": resolved_cover["report_period_raw"],
            "report_year": resolved_cover["report_year"],
            "form_type": resolved_cover["form_type"],
            "filing_date": override.get("filing_date"),
            "filer_signed_date": override.get("filer_signed_date"),
            "agency_certification_date": override.get("agency_certification_date"),
            "oge_certification_date": override.get("oge_certification_date") or cover.oge_digital_sig_date,
            "metadata_review_ref": review_ref,
            "source_url": item["source_url"],
            "source_sha256": item["source_sha256"],
            "byte_size": item["byte_size"],
            "acquired_at": item["acquired_at"],
            "access_status": item["access_status"],
            "access_basis": item["access_basis"],
            "access_reason": item["access_reason"],
            "access_evidence_ref": item["access_evidence_ref"],
            "amendment_status": amendment_status,
            "amendment_details_json": amendment_details_json,
            "cover_page": cover.cover_page,
        }, cover

    # Unreadable, partial, or unsupported cover (e.g. scanned/amended cover or Form 278-T):
    # require explicit human-reviewed cover_override + metadata_review_ref rather than guessing or running OCR.
    reason = cover_unsupported_reason or (f"{cover.status}:{cover.reason}" if cover is not None else "unreadable")
    if not isinstance(override, dict) or not (COVER_OVERRIDE_REQUIRED_FIELDS <= set(override)):
        raise ValueError(
            f"filing {item['filing_id']!r} has non-readable cover ({reason}) and requires a complete cover_override"
        )
    if not isinstance(item.get("metadata_review_ref"), str) or not item["metadata_review_ref"].strip():
        raise ValueError(
            f"filing {item['filing_id']!r} has non-readable cover ({reason}) and requires metadata_review_ref"
        )
    if item["official_id"] == override["position_raw"]:
        raise ValueError("person identity cannot be an office")
    _validate_report_identity(
        position_raw=override["position_raw"],
        agency_raw=override["agency_raw"],
        report_type=override["report_type"],
        report_period_raw=override["report_period_raw"],
        report_year=override["report_year"],
        form_type=override["form_type"],
        cover_page=cover_page,
    )
    return {
        "filing_id": item["filing_id"],
        "official_id": item["official_id"],
        "official_name": item["official_name"],
        "position_raw": override["position_raw"],
        "agency_raw": override["agency_raw"],
        "report_type": override["report_type"],
        "report_period_raw": override["report_period_raw"],
        "report_year": override["report_year"],
        "form_type": override["form_type"],
        "filing_date": override.get("filing_date"),
        "filer_signed_date": override.get("filer_signed_date"),
        "agency_certification_date": override.get("agency_certification_date"),
        "oge_certification_date": override.get("oge_certification_date"),
        "metadata_review_ref": item["metadata_review_ref"],
        "source_url": item["source_url"],
        "source_sha256": item["source_sha256"],
        "byte_size": item["byte_size"],
        "acquired_at": item["acquired_at"],
        "access_status": item["access_status"],
        "access_basis": item["access_basis"],
        "access_reason": item["access_reason"],
        "access_evidence_ref": item["access_evidence_ref"],
        "amendment_status": amendment_status,
        "amendment_details_json": amendment_details_json,
        "cover_page": cover_page,
    }, None


class _StreamingParquetTable:
    """Bounded zstd Parquet writer flushing in ``batch_size`` Arrow batches with atomic replace."""

    def __init__(self, temp_path: Path, destination: Path, schema: pa.Schema, *, batch_size: int = DEFAULT_BATCH_SIZE):
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        from spicy_regs.native_types import extra_field_checker

        self.temp_path = temp_path
        self.destination = destination
        self.schema = schema
        self.batch_size = batch_size
        self._check_fields = extra_field_checker(schema)
        self._buffer: list[dict[str, Any]] = []
        self.row_count = 0
        self._writer = pq.ParquetWriter(temp_path, schema, compression="zstd")
        self._closed = False

    def append(self, row: dict[str, Any]) -> None:
        self._check_fields(row)
        self._buffer.append(row)
        self.row_count += 1
        if len(self._buffer) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self._writer.write_table(pa.Table.from_pylist(self._buffer, schema=self.schema))
        self._buffer.clear()

    def abort(self) -> None:
        """Drop any unflushed rows and close the writer without re-flushing on teardown."""
        self._buffer.clear()
        if not self._closed:
            try:
                self._writer.close()
            finally:
                self._closed = True

    def commit(self) -> Path:
        if not self._closed:
            try:
                self.flush()
            finally:
                self._writer.close()
                self._closed = True
        self.temp_path.replace(self.destination)
        return self.destination


def _read_verified_original(store, item: Mapping[str, Any], max_bytes: int) -> bytes:
    if item["byte_size"] > max_bytes:
        raise ValueError("selected original exceeds the source reader byte bound")
    with store.open(item["source_sha256"]) as stream:
        body = stream.read(max_bytes + 1)
    if len(body) != item["byte_size"] or "sha256:" + hashlib.sha256(body).hexdigest() != item["source_sha256"]:
        raise ValueError("retained original differs from selected source version")
    return body


def build_tables(
    selection: dict,
    output_dir: Path,
    *,
    evidence,
    store,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[Path]:
    """Stream bounded originals one filing at a time into 4 zstd Parquet tables; replace outputs atomically."""
    from spicy_docs.sources.executive_disclosures import (
        DEFAULT_SECTIONS,
        MAX_BYTES,
        REPORT_TYPES,
        UnsupportedDisclosure,
        read_excel_278e,
        read_form278e_cover,
    )

    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    validate_selection(selection)
    v2 = is_v2_manifest(selection)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(dir=output_dir) as scratch:
        scratch_dir = Path(scratch)
        writers: dict[str, _StreamingParquetTable] = {}
        with ExitStack() as stack:
            for name in TABLES:
                writer = _StreamingParquetTable(
                    scratch_dir / f"{name}.parquet",
                    output_dir / f"{name}.parquet",
                    SCHEMAS[name],
                    batch_size=batch_size,
                )
                writers[name] = writer
                stack.callback(writer.abort)

            filings_writer = writers[TABLES[0]]
            facts_writer = writers[TABLES[1]]
            transactions_writer = writers[TABLES[2]]
            pages_writer = writers[TABLES[3]]

            for item in selection["filings"]:
                body: bytes | None = None
                if item["access_status"] == "available":
                    body = _read_verified_original(store, item, MAX_BYTES)
                    evidence.retain_bytes(
                        body,
                        stage="executive-filing-replay",
                        raw_source=True,
                        original_location=item["source_url"],
                        filing_id=item["filing_id"],
                    )

                native_cover = None
                if v2:
                    resolved, native_cover = _resolve_v2_filing_metadata(item, body)
                else:
                    resolved = dict(item)
                filing: dict[str, Any] = {
                    **resolved,
                    "cohort_id": selection["cohort_id"],
                    "coverage_note": selection["coverage_note"],
                    "metadata_json": json.dumps(resolved, ensure_ascii=False, sort_keys=True),
                    "facts_status": "not_available",
                    "facts_reason": resolved["access_reason"],
                    "qualified_sections_json": "[]",
                }
                if body is None:
                    filings_writer.append(filing)
                    continue

                try:
                    if resolved["amendment_status"] != "none_observed_in_retained_document":
                        raise UnsupportedDisclosure(
                            "amendment status requires review; original and amendment remain available"
                        )
                    if resolved["form_type"] != "278e":
                        raise UnsupportedDisclosure(
                            f"form_type {resolved['form_type']!r} requires separate layout qualification; "
                            "only qualified Form 278e sections are supported"
                        )
                    if resolved["report_type"] not in REPORT_TYPES:
                        raise UnsupportedDisclosure(
                            f"report_type {resolved['report_type']!r} is unsupported for Form 278e extraction"
                        )
                    effective_cover_override = None
                    if resolved["cover_page"] != 1:
                        if not v2:
                            try:
                                probed_cover = read_form278e_cover(body, cover_page=resolved["cover_page"])
                            except UnsupportedDisclosure:
                                probed_cover = None
                            if probed_cover is not None and probed_cover.status == "readable":
                                native_cover = probed_cover
                        if native_cover is not None and native_cover.status == "readable":
                            effective_cover_override = native_cover
                    result = read_excel_278e(
                        body,
                        filer_name=resolved["official_name"],
                        report_year=resolved["report_year"],
                        sections=DEFAULT_SECTIONS,
                        report_types=REPORT_TYPES,
                        cover_override=effective_cover_override,
                    )
                except UnsupportedDisclosure as error:
                    filing.update(
                        facts_status="unsupported",
                        facts_reason=str(error),
                        qualified_sections_json="[]",
                    )
                    evidence.event("interpretation-refusal", filing_id=resolved["filing_id"], reason=str(error))
                    filings_writer.append(filing)
                    body = None
                    continue

                matched_sections = sorted({page["section_code"] for page in result.pages})
                if set(matched_sections) == set(DEFAULT_SECTIONS):
                    facts_status = "qualified_parts_2_5_6_7"
                    facts_reason = "Qualified Form 278e Parts 2, 5, 6, and 7; other sections unsupported"
                else:
                    facts_status = "qualified_partial"
                    facts_reason = (
                        f"Qualified Form 278e sections {', '.join(matched_sections)}; "
                        "other sections absent or unsupported"
                    )
                filing.update(
                    facts_status=facts_status,
                    facts_reason=facts_reason,
                    qualified_sections_json=json.dumps(matched_sections, ensure_ascii=False),
                )
                filings_writer.append(filing)

                shared = {
                    "cohort_id": filing["cohort_id"],
                    "filing_id": filing["filing_id"],
                    "source_url": filing["source_url"],
                    "source_sha256": filing["source_sha256"],
                    "parser_revision": result.parser_revision,
                    "backend_version": result.backend_version,
                }

                seen_pages: set[int] = set()
                for page in result.pages:
                    page_num = page["page"]
                    if page_num in seen_pages:
                        raise ValueError(f"duplicate page within filing {resolved['filing_id']!r}: {page_num}")
                    seen_pages.add(page_num)
                    pages_writer.append(
                        {
                            **shared,
                            "source_locator": page["source_locator"],
                            "page_text": page["page_text"],
                            "filer_identity_json": page["filer_identity_json"],
                            "table_bbox_json": page["table_bbox_json"],
                            "page": page_num,
                            "source_slot_count": page["source_slot_count"],
                            "blank_slot_count": page["blank_slot_count"],
                            "section": page["section"],
                            "layout_profile": page["layout_profile"],
                            "endnotes_json": page["endnotes_json"],
                        }
                    )

                seen_fact_ids: set[str] = set()
                for raw in result.rows:
                    if raw["blank_slot"]:
                        continue
                    section_code = raw["section_code"]
                    fact_id = f"{resolved['filing_id']}:{result.source_sha256}:{section_code}:p{raw['page']}:{raw['row_number_raw']}"
                    if fact_id in seen_fact_ids:
                        raise ValueError(f"duplicate fact_id within filing: {fact_id}")
                    seen_fact_ids.add(fact_id)

                    value_status, value_lower, value_upper, value_exact_usd = amount_range(raw["value_raw"])
                    income_amount_raw, income_status, income_lower, income_upper, income_exact_usd = (
                        resolve_income_amount(raw["income_amount_raw"], raw.get("income_exact_raw") or "")
                    )
                    facts_writer.append(
                        {
                            **shared,
                            "fact_id": fact_id,
                            "section": raw["section"],
                            "section_code": section_code,
                            "row_number_raw": raw["row_number_raw"],
                            "description_raw": raw["description_raw"],
                            "eif_raw": raw["eif_raw"],
                            "value_raw": raw["value_raw"],
                            "income_type_raw": raw["income_type_raw"],
                            "income_amount_raw": income_amount_raw,
                            "ownership_label": _extract_ownership_label(raw["description_raw"]),
                            "value_status": value_status,
                            "income_status": income_status,
                            "currency": (
                                "USD"
                                if (value_status in USD_AMOUNT_STATUSES or income_status in USD_AMOUNT_STATUSES)
                                else None
                            ),
                            "source_locator": raw["source_locator"],
                            "raw_cells_json": raw["raw_cells_json"],
                            "cell_boxes_json": raw["cell_boxes_json"],
                            "page": raw["page"],
                            "value_lower": value_lower,
                            "value_upper": value_upper,
                            "income_lower": income_lower,
                            "income_upper": income_upper,
                            "value_exact_usd": value_exact_usd,
                            "income_exact_usd": income_exact_usd,
                        }
                    )

                seen_txn_ids: set[str] = set()
                for raw in result.transactions:
                    if raw["blank_slot"]:
                        continue
                    section_code = raw["section_code"]
                    txn_id = f"{resolved['filing_id']}:{result.source_sha256}:{section_code}:p{raw['page']}:{raw['row_number_raw']}"
                    if txn_id in seen_txn_ids:
                        raise ValueError(f"duplicate transaction_id within filing: {txn_id}")
                    seen_txn_ids.add(txn_id)

                    txn_date, txn_date_status = parse_transaction_date(raw["transaction_date_raw"])
                    amount_status, amount_lower, amount_upper, _amount_exact = amount_range(raw["amount_raw"])
                    transactions_writer.append(
                        {
                            **shared,
                            "transaction_id": txn_id,
                            "form_type": resolved["form_type"],
                            "section": raw["section"],
                            "section_code": section_code,
                            "row_number_raw": raw["row_number_raw"],
                            "description_raw": raw["description_raw"],
                            "ownership_label": _extract_ownership_label(raw["description_raw"]),
                            "transaction_type_raw": raw["transaction_type_raw"],
                            "transaction_date_raw": raw["transaction_date_raw"],
                            "transaction_date_status": txn_date_status,
                            "notification_over_30_days_raw": raw.get("notification_over_30_days_raw"),
                            "amount_raw": raw["amount_raw"],
                            "amount_status": amount_status,
                            "currency": "USD" if amount_status in USD_AMOUNT_STATUSES else None,
                            "source_locator": raw["source_locator"],
                            "raw_cells_json": raw["raw_cells_json"],
                            "cell_boxes_json": raw["cell_boxes_json"],
                            "page": raw["page"],
                            "amount_lower": amount_lower,
                            "amount_upper": amount_upper,
                            "transaction_date": txn_date,
                        }
                    )

                # Release per-filing buffers immediately before reading the next filing.
                result = None
                body = None

            return [writers[name].commit() for name in TABLES]
