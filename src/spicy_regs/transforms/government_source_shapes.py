"""Explicit domain shapes for government-source datasets.

The source shapers and incremental mergers use the literal source forms. This
module converts their final rows to queryable native values. Original conversion
inputs and acquisition/processing fields belong to the shared ETL receipts.
No network access or publication occurs here.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Mapping

import pyarrow as pa

RECEIPT_FIELDS = {
    "crs_reports": ("url",),
    "fcc_filings": ("filing_url", "native_fields_sha256", "pdf_extraction_results_json"),
    "gao_decisions": ("listing_page", "source"),
    "gao_reports": ("url", "source"),
    "gao_recommendations": ("first_seen", "last_seen"),
    "lobbying_filings": ("url",),
    "usaspending_recipients": ("observed_at", "source_capture_sha256"),
}

INTEGER_FIELDS = {
    "crs_reports": ("version",),
    "fcc_filings": ("total_page_count",),
    "lobbying_filings": ("filing_year",),
    "lobbying_activities": ("activity_index",),
    "lobbying_activity_lobbyists": ("activity_index", "lobbyist_index"),
}

BOOLEAN_FIELDS = {
    "fcc_filings": ("express_comment",),
    "gao_recommendations": ("priority", "listed_open"),
    "lobbying_activity_lobbyists": ("new",),
}

MONEY_FIELDS = {"lobbying_filings": ("income", "expenses"), "usaspending_recipients": ("total_award_amount",)}

DATE_FIELDS = {
    "gao_reports": ("published_date",),
    "gao_decisions": ("decision_date",),
    "gao_recommendations": ("publication_date",),
    "sam_entities": ("registration_date", "registration_expiration_date"),
}

TIMESTAMP_FIELDS = {"crs_reports": ("published_date", "update_date"), "lobbying_filings": ("dt_posted",)}

IDENTITY_FIELDS = {
    "crs_reports": ("report_id",),
    "fcc_filings": ("id_submission",),
    "fcc_proceedings": ("name",),
    "gao_reports": ("report_id",),
    "gao_decisions": ("decision_number", "url"),
    "gao_recommendations": ("recommendation_id",),
    "lobbying_filings": ("filing_uuid",),
    "lobbying_activities": ("filing_uuid", "activity_index"),
    "lobbying_activity_lobbyists": ("filing_uuid", "activity_index", "lobbyist_index"),
    "sam_entities": ("uei", "entity_eft_indicator"),
    "usaspending_recipients": ("recipient_id",),
}

LEGACY_COLUMNS = {
    "crs_reports": ("report_id", "title", "report_type", "status", "published_date", "update_date", "version", "url"),
    "fcc_filings": (
        "id_submission",
        "proceeding_names_json",
        "submission_type",
        "express_comment",
        "date_received",
        "date_submission",
        "date_disseminated",
        "filing_status",
        "viewing_status",
        "exparte_or_late_filed",
        "filers_json",
        "authors_json",
        "lawfirms_json",
        "bureaus_json",
        "text_data",
        "total_page_count",
        "documents_json",
        "filing_url",
        "native_fields_json",
        "native_fields_sha256",
        "pdf_extraction_results_json",
    ),
    "fcc_proceedings": (
        "name",
        "id_proceeding",
        "description",
        "bureau_code",
        "bureau_name",
        "rulemaking_or_docket",
        "filing_status",
        "date_created",
        "date_closed",
        "comment_start_date",
        "comment_end_date",
        "reply_comment_start_date",
        "reply_comment_end_date",
        "filed_by",
    ),
    "gao_decisions": (
        "decision_number",
        "b_numbers_json",
        "decision_type",
        "title",
        "decision_date",
        "topics_json",
        "url",
        "listing_page",
        "source",
    ),
    "gao_recommendations": (
        "recommendation_id",
        "report_id",
        "publication_number",
        "publication_title",
        "publication_date",
        "director_name",
        "agency",
        "recommendation",
        "recommendation_kind",
        "recommendation_number",
        "status",
        "priority",
        "comments",
        "topics",
        "first_seen",
        "last_seen",
        "listed_open",
        "status_as_of",
    ),
    "gao_reports": (
        "report_id",
        "title",
        "report_type",
        "published_date",
        "abstract",
        "agencies_json",
        "topics_json",
        "url",
        "source",
        "product_type",
        "report_number",
        "requester_type",
        "requester_committees_json",
        "requester_members_json",
        "recommendation_count",
        "matters_for_congress_count",
        "page_count",
        "subject_terms_json",
    ),
    "lobbying_activities": (
        "filing_uuid",
        "activity_index",
        "general_issue_code",
        "general_issue_code_display",
        "description",
        "foreign_entity_issues",
        "government_entities_json",
    ),
    "lobbying_activity_lobbyists": (
        "filing_uuid",
        "activity_index",
        "lobbyist_index",
        "lobbyist_id",
        "prefix",
        "first_name",
        "nickname",
        "middle_name",
        "last_name",
        "suffix",
        "covered_position",
        "new",
    ),
    "lobbying_filings": (
        "filing_uuid",
        "filing_type",
        "filing_year",
        "filing_period",
        "dt_posted",
        "registrant_name",
        "registrant_id",
        "client_name",
        "client_id",
        "income",
        "expenses",
        "lobbying_activities_json",
        "government_entities_json",
        "url",
    ),
    "sam_entities": (
        "uei",
        "entity_eft_indicator",
        "cage_code",
        "legal_business_name",
        "dba_name",
        "entity_structure_desc",
        "entity_type_desc",
        "profit_structure_desc",
        "state",
        "city",
        "zip_code",
        "congressional_district",
        "primary_naics",
        "registration_status",
        "registration_date",
        "registration_expiration_date",
        "exclusion_status_flag",
        "purpose_of_registration_desc",
        "entity_url",
    ),
    "usaspending_recipients": (
        "recipient_id",
        "uei",
        "duns",
        "name",
        "recipient_level",
        "total_award_amount",
        "observed_at",
        "source_capture_sha256",
    ),
}

# Additional held GAO editions rename the release day and retain the actual
# decision day and the publisher's status. Accept both release-day spellings;
# the original spelling remains a receipt conversion input.
LEGACY_COLUMNS["gao_decisions"] += (
    "released_date",
    "decided_date",
    "decision_status",
    "outcome",
    "outcome_rule",
    "b_numbers_truncated",
)
RECEIPT_FIELDS["gao_decisions"] += ("outcome_rule",)
# Whether GAO's listing cut the row's number list is a fact about the decision as held, which a reader needs
# beside b_numbers; it is not how the row was processed (owner decision, 2026-10-04).
BOOLEAN_FIELDS["gao_decisions"] = ("b_numbers_truncated",)
DATE_FIELDS["gao_decisions"] = ("released_date", "decided_date")

# FCC native observations carry more than the older name-only arrays. IDs remain
# text: ECFS serves both strings and integers, and identifiers are not measures.
FCC_ARRAYS = {
    "proceedings": (
        ("name", "name"),
        ("id_proceeding", "id_proceeding"),
        ("bureau_code", "bureau_code"),
        ("bureau_name", "bureau_name"),
        ("created_date", "created_date"),
        ("date_closed", "date_closed"),
        ("description", "description"),
        ("description_display", "description_display"),
        ("sunshine_start_date", "sunshine_start_date"),
        ("sunshine_end_date", "sunshine_end_date"),
        ("filing_status", "filingStatus"),
    ),
    "filers": (("name", "name"),),
    "authors": (("name", "name"),),
    "lawfirms": (("name", "name"),),
    "bureaus": (("name", "name"), ("code", "code"), ("edocs_bureau_code", "edocs_bureau_code")),
    "documents": (("filename", "filename"), ("description", "description")),
}
FCC_LEGACY_ARRAYS = {
    "proceeding_names_json": "proceedings",
    "filers_json": "filers",
    "authors_json": "authors",
    "lawfirms_json": "lawfirms",
    "bureaus_json": "bureaus",
    "documents_json": "documents",
}
STRUCT_ARRAYS = {
    "government_entities_json": ("id", "name"),
    "lobbying_activities_json": ("general_issue_code", "general_issue_code_display", "description"),
}


class GovernmentShapeError(ValueError):
    """A source value cannot be represented without losing its meaning."""


def _list_type(fields: tuple[str, ...]) -> pa.DataType:
    return pa.list_(pa.struct([(field, pa.string()) for field in fields]))


def subject_schema(dataset: str) -> pa.Schema:
    """The family-owned declaration; no parser status or source locator is a subject column."""
    fields = []
    for name in LEGACY_COLUMNS[dataset]:
        if dataset == "gao_decisions" and name == "decision_date":
            continue
        if name in RECEIPT_FIELDS.get(dataset, ()) or name == "native_fields_json":
            continue
        target, dtype = name, pa.string()
        if dataset == "fcc_filings" and name in FCC_LEGACY_ARRAYS:
            target = FCC_LEGACY_ARRAYS[name]
            dtype = _list_type(tuple(field for field, _ in FCC_ARRAYS[target]))
        elif name in STRUCT_ARRAYS:
            target, dtype = name.removesuffix("_json"), _list_type(STRUCT_ARRAYS[name])
        elif name.endswith("_json"):
            target, dtype = name.removesuffix("_json"), pa.list_(pa.string())
        elif name in INTEGER_FIELDS.get(dataset, ()) or name in (
            "recommendation_count",
            "matters_for_congress_count",
            "page_count",
        ):
            dtype = pa.int64()
        elif name in BOOLEAN_FIELDS.get(dataset, ()):
            dtype = pa.bool_()
        elif name in MONEY_FIELDS.get(dataset, ()):
            dtype = pa.decimal128(38, 6)
        elif name in DATE_FIELDS.get(dataset, ()):
            dtype = pa.date32()
        elif name in TIMESTAMP_FIELDS.get(dataset, ()):
            dtype = pa.timestamp("us", tz="UTC")
        fields.append((target, dtype))
    return pa.schema(fields, metadata={b"government_etl_policy": b"1"})


SUBJECT_SCHEMAS = {dataset: subject_schema(dataset) for dataset in LEGACY_COLUMNS}


def _text(value: Any, *, identifier: bool = False) -> str | None:
    if value is None or isinstance(value, str):
        return value
    if identifier and type(value) is int:
        return str(value)
    raise GovernmentShapeError(f"Expected text, got {type(value).__name__}")


def _integer(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is int:
        number = value
    elif isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
        number = int(value)
    else:
        raise GovernmentShapeError("Expected a literal whole number")
    if not -(2**63) <= number < 2**63:
        raise GovernmentShapeError("Whole number exceeds BIGINT")
    return number


def _boolean(value: Any) -> bool | None:
    if value is None or type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value in ("true", "True", "1", "false", "False", "0"):
        return value in ("true", "True", "1")
    raise GovernmentShapeError("Unsupported source boolean spelling")


def _money(value: Any) -> Decimal | None:
    if value is None:
        return None
    if not isinstance(value, str | Decimal | int) or isinstance(value, bool):
        raise GovernmentShapeError("Money requires an exact decimal input, never a binary float")
    try:
        amount = Decimal(value)
        with localcontext() as context:
            context.prec = 50
            scaled = amount.quantize(Decimal("0.000001"))
        if not amount.is_finite() or scaled != amount or abs(amount) >= Decimal(10) ** 32:
            raise GovernmentShapeError("Money cannot fit DECIMAL(38,6) without rounding")
        return amount
    except InvalidOperation as error:
        raise GovernmentShapeError("Invalid exact decimal amount") from error


def _json(value: Any, kind: type) -> Any:
    if value is None:
        return None
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (ValueError, TypeError) as error:
        raise GovernmentShapeError("Invalid source JSON") from error
    if parsed is None:
        return None
    if not isinstance(parsed, kind):
        raise GovernmentShapeError(f"Expected {kind.__name__} source JSON")
    return parsed


def _structs(values: Any, fields: tuple[tuple[str, str], ...]) -> list | None:
    if values is None:
        return None
    if not isinstance(values, list):
        raise GovernmentShapeError("Expected a source array")
    result = []
    for value in values:
        if value is None:
            result.append(None)
        elif isinstance(value, dict):
            result.append(
                {name: _text(value.get(source), identifier=name in ("id", "id_proceeding")) for name, source in fields}
            )
        else:
            raise GovernmentShapeError("Unsupported array element; raw value belongs in refusal receipt")
    return result


def _fcc_arrays(row: Mapping[str, Any]) -> dict[str, Any]:
    literal = row.get("native_fields_json")
    native = _json(literal, dict)
    if literal is not None and row.get("native_fields_sha256") is not None:
        digest = "sha256:" + hashlib.sha256(literal.encode()).hexdigest()
        if digest != row["native_fields_sha256"]:
            raise GovernmentShapeError("FCC native fields differ from their retained digest")
    result = {}
    for old, field in FCC_LEGACY_ARRAYS.items():
        if native is not None:
            values = native.get(field)
        else:
            values = _json(row.get(old), list)
            if field != "documents" and values is not None:
                values = [None if name is None else {"name": _text(name)} for name in values]
        result[field] = _structs(values, FCC_ARRAYS[field])
    return result


def map_subject(dataset: str, row: Mapping[str, Any]) -> dict[str, Any]:
    """Map one declared legacy row, refusing unclassified top-level fields and lossy coercions.

    The caller retains the entire original row as a receipt input before invoking
    this mapper, including failed inputs. No malformed value silently becomes an
    empty array, zero amount, or successful subject.
    """
    unknown = set(row) - set(LEGACY_COLUMNS[dataset])
    if unknown:
        raise GovernmentShapeError(f"{dataset}: unclassified fields {sorted(unknown)}")
    if dataset == "gao_decisions":
        if row.get("released_date") is not None and row.get("decision_date") is not None:
            if row["released_date"] != row["decision_date"]:
                raise GovernmentShapeError("Conflicting GAO release-day spellings")
        row = dict(row)
        if row.get("released_date") is None:
            row["released_date"] = row.get("decision_date")
    fcc = _fcc_arrays(row) if dataset == "fcc_filings" else {}
    subject: dict[str, Any] = {}
    for field in SUBJECT_SCHEMAS[dataset]:
        name = field.name
        value = row.get(name)
        if name in fcc:
            value = fcc[name]
        elif pa.types.is_list(field.type):
            old = name + "_json"
            values = _json(row.get(old), list)
            if old in STRUCT_ARRAYS:
                value = _structs(values, tuple((k, k) for k in STRUCT_ARRAYS[old]))
            else:
                value = None if values is None else [_text(v) for v in values]
        elif pa.types.is_integer(field.type):
            value = _integer(value)
        elif pa.types.is_boolean(field.type):
            value = _boolean(value)
        elif pa.types.is_decimal(field.type):
            value = _money(value)
        elif pa.types.is_date(field.type) and value is not None:
            try:
                value = date.fromisoformat(value) if isinstance(value, str) else value
                if not isinstance(value, date) or isinstance(value, datetime):
                    raise ValueError("Expected an ISO day")
            except ValueError as error:
                raise GovernmentShapeError(f"{name}: invalid calendar date") from error
        elif pa.types.is_timestamp(field.type) and value is not None:
            try:
                value = datetime.fromisoformat(value) if isinstance(value, str) else value
                if not isinstance(value, datetime) or value.utcoffset() is None:
                    raise ValueError("Expected an offset timestamp")
                value = value.astimezone(timezone.utc)
            except ValueError as error:
                raise GovernmentShapeError(f"{name}: invalid offset timestamp") from error
        elif pa.types.is_string(field.type):
            value = _text(value)
        subject[name] = value
    if subject.get(IDENTITY_FIELDS[dataset][0]) in (None, ""):
        raise GovernmentShapeError(f"{dataset}: missing stable identity")
    return subject
