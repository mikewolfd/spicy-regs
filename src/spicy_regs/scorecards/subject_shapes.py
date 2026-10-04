"""Explicit scorecard domain shapes; upstream acquisition rows retain their evidence."""

from __future__ import annotations
import json
import re
from decimal import Decimal
from collections.abc import Mapping
import pyarrow as pa

POLICY_VERSION = "scorecards-etl-v1"
RATING_POLICY_VERSION = "scorecards-etl-ratings-v2"
SOURCE_COLUMNS = {
    "scorecard_item_links": (
        "scorecard_id",
        "item_id",
        "reference_id",
        "source_reference_id",
        "congress",
        "chamber",
        "session",
        "roll_number",
        "vote_id",
        "bill_id",
        "amendment_id",
        "source_snapshot_id",
        "resolution_status",
        "resolution_rule",
        "rule_version",
        "candidate_count",
        "candidates_json",
        "reason",
        "source_context_json",
        "input_pins_json",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_items": (
        "scorecard_id",
        "item_id",
        "publisher_item_id",
        "item_kind_text",
        "title",
        "item_date_text",
        "congress_text",
        "chamber_text",
        "session_text",
        "roll_number_text",
        "bill_citation_text",
        "amendment_citation_text",
        "references_json",
        "source_url",
        "source_path",
        "publisher_position_text",
        "position_basis",
        "position_source_path",
        "snapshot_id",
        "capture_id",
    ),
    "scorecard_member_item_results": (
        "scorecard_id",
        "result_id",
        "metric_id",
        "item_id",
        "participation_id",
        "publisher_member_key",
        "action_text",
        "result_text",
        "contribution_text",
        "contribution_number",
        "eligibility_text",
        "counts_toward_metric",
        "adjustment_text",
        "reason_text",
        "snapshot_id",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_member_links": (
        "scorecard_id",
        "publisher_member_key",
        "bioguide_id",
        "term_candidates_json",
        "override_version",
        "source_snapshot_id",
        "resolution_status",
        "resolution_rule",
        "rule_version",
        "candidate_count",
        "candidates_json",
        "reason",
        "source_context_json",
        "input_pins_json",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_member_ratings": (
        "scorecard_id",
        "metric_id",
        "publisher_member_key",
        "value_text",
        "value_number",
        "value_status_text",
        "rank_text",
        "notes_text",
        "source_url",
        "source_path",
        "snapshot_id",
        "capture_id",
    ),
    "scorecard_members": (
        "scorecard_id",
        "publisher_member_key",
        "publisher_member_id",
        "identifiers_json",
        "member_name",
        "chamber_text",
        "state",
        "district",
        "party",
        "period_text",
        "eligibility_text",
        "notes_text",
        "snapshot_id",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_methodologies": (
        "scorecard_id",
        "methodology_id",
        "chamber_text",
        "cohort_text",
        "effective_at_text",
        "methodology_text",
        "methodology_url",
        "disclosure_status",
        "capture_id",
        "source_path",
        "snapshot_id",
        "source_url",
    ),
    "scorecard_metric_components": (
        "scorecard_id",
        "parent_metric_id",
        "component_metric_id",
        "weight_text",
        "weight_number",
        "combination_rule_text",
        "snapshot_id",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_metric_items": (
        "scorecard_id",
        "metric_id",
        "item_id",
        "participation_id",
        "methodology_id",
        "publisher_position_text",
        "position_basis",
        "weight_text",
        "weight_number",
        "contribution_rule_text",
        "counts_toward_metric",
        "snapshot_id",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_metrics": (
        "scorecard_id",
        "metric_id",
        "methodology_id",
        "name",
        "value_unit_text",
        "chamber_text",
        "cohort_text",
        "period_text",
        "periods_json",
        "is_primary",
        "scale_text",
        "rank_population_text",
        "snapshot_id",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_publishers": (
        "publisher_id",
        "name",
        "abbreviation",
        "aliases_json",
        "homepage_url",
        "scorecard_index_url",
        "observed_at",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecard_snapshots": (
        "snapshot_id",
        "scorecard_id",
        "observed_at",
        "capture_ids_json",
        "parser_version",
        "methodology_digest",
        "completeness_status",
        "completeness_rule",
        "source_declared_counts_json",
        "parsed_counts_json",
        "evidence_policy",
        "capture_roles_json",
        "rendition_selection_rule",
        "identity_rule_version",
        "capture_id",
        "source_url",
        "source_path",
    ),
    "scorecards": (
        "scorecard_id",
        "publisher_id",
        "series_id",
        "publisher_edition_key",
        "publisher_name_text",
        "title",
        "edition_label_text",
        "chamber_scope_text",
        "year_text",
        "congress_text",
        "session_text",
        "timespan_text",
        "periods_json",
        "source_url",
        "methodology_url",
        "published_at_text",
        "updated_at_text",
        "observed_at",
        "snapshot_id",
        "capture_id",
        "source_path",
    ),
}

IDENTITIES = {
    "scorecard_publishers": ("publisher_id",),
    "scorecards": ("scorecard_id",),
    "scorecard_snapshots": ("snapshot_id",),
    "scorecard_methodologies": ("scorecard_id", "methodology_id"),
    "scorecard_metrics": ("scorecard_id", "metric_id"),
    "scorecard_items": ("scorecard_id", "item_id"),
    "scorecard_metric_items": ("scorecard_id", "metric_id", "item_id", "participation_id"),
    "scorecard_metric_components": ("scorecard_id", "parent_metric_id", "component_metric_id"),
    "scorecard_members": ("scorecard_id", "publisher_member_key"),
    "scorecard_member_ratings": ("scorecard_id", "metric_id", "publisher_member_key"),
    "scorecard_member_item_results": ("scorecard_id", "item_id", "publisher_member_key", "result_id"),
    "scorecard_member_links": ("scorecard_id", "publisher_member_key"),
    "scorecard_item_links": ("scorecard_id", "item_id", "reference_id"),
}

DOMAIN_COLUMNS = {
    "scorecard_item_links": (
        "scorecard_id",
        "item_id",
        "reference_id",
        "source_reference_id",
        "congress",
        "chamber",
        "session",
        "roll_number",
        "vote_id",
        "bill_id",
        "amendment_id",
    ),
    "scorecard_items": (
        "scorecard_id",
        "item_id",
        "publisher_item_id",
        "item_kind_text",
        "title",
        "item_date_text",
        "congress_text",
        "chamber_text",
        "session_text",
        "roll_number_text",
        "bill_citation_text",
        "amendment_citation_text",
        "references_json",
        "publisher_position_text",
    ),
    "scorecard_member_item_results": (
        "scorecard_id",
        "result_id",
        "metric_id",
        "item_id",
        "participation_id",
        "publisher_member_key",
        "action_text",
        "result_text",
        "contribution_text",
        "contribution_number",
        "eligibility_text",
        "counts_toward_metric",
        "adjustment_text",
        "reason_text",
    ),
    "scorecard_member_links": ("scorecard_id", "publisher_member_key", "bioguide_id"),
    "scorecard_member_ratings": (
        "scorecard_id",
        "metric_id",
        "publisher_member_key",
        "value_text",
        "value_number",
        "value_status_text",
        "rank_text",
        "notes_text",
    ),
    "scorecard_members": (
        "scorecard_id",
        "publisher_member_key",
        "publisher_member_id",
        "identifiers_json",
        "member_name",
        "chamber_text",
        "state",
        "district",
        "party",
        "period_text",
        "eligibility_text",
        "notes_text",
    ),
    "scorecard_methodologies": (
        "scorecard_id",
        "methodology_id",
        "chamber_text",
        "cohort_text",
        "effective_at_text",
        "methodology_text",
        "methodology_url",
        "disclosure_status",
    ),
    "scorecard_metric_components": (
        "scorecard_id",
        "parent_metric_id",
        "component_metric_id",
        "weight_text",
        "weight_number",
        "combination_rule_text",
    ),
    "scorecard_metric_items": (
        "scorecard_id",
        "metric_id",
        "item_id",
        "participation_id",
        "methodology_id",
        "publisher_position_text",
        "weight_text",
        "weight_number",
        "contribution_rule_text",
        "counts_toward_metric",
    ),
    "scorecard_metrics": (
        "scorecard_id",
        "metric_id",
        "methodology_id",
        "name",
        "value_unit_text",
        "chamber_text",
        "cohort_text",
        "period_text",
        "periods_json",
        "is_primary",
        "scale_text",
        "rank_population_text",
    ),
    "scorecard_publishers": (
        "publisher_id",
        "name",
        "abbreviation",
        "aliases_json",
        "homepage_url",
        "scorecard_index_url",
    ),
    "scorecard_snapshots": (),
    "scorecards": (
        "scorecard_id",
        "publisher_id",
        "series_id",
        "publisher_edition_key",
        "publisher_name_text",
        "title",
        "edition_label_text",
        "chamber_scope_text",
        "year_text",
        "congress_text",
        "session_text",
        "timespan_text",
        "periods_json",
        "methodology_url",
        "published_at_text",
        "updated_at_text",
    ),
}

NATIVE_LISTS = {
    "aliases_json": ("aliases", pa.list_(pa.string())),
    "identifiers_json": ("identifiers", pa.list_(pa.struct([(k, pa.string()) for k in ("scheme", "value")]))),
    "periods_json": (
        "periods",
        pa.list_(
            pa.struct(
                [
                    (k, pa.string())
                    for k in (
                        "occurrence_id",
                        "period_text",
                        "kind",
                        "congress_text",
                        "chamber_text",
                        "session_text",
                        "year_text",
                    )
                ]
            )
        ),
    ),
    "references_json": (
        "references",
        pa.list_(
            pa.struct(
                [
                    (k, pa.string())
                    for k in (
                        "occurrence_id",
                        "citation_text",
                        "kind",
                        "congress_text",
                        "chamber_text",
                        "session_text",
                        "roll_number_text",
                        "bill_citation_text",
                        "amendment_citation_text",
                    )
                ]
            )
        ),
    ),
}
BOOLEAN_FIELDS = frozenset({"is_primary", "counts_toward_metric"})
INTEGER_FIELDS = frozenset({"congress", "session", "roll_number"})
DECIMAL_FIELDS = frozenset({"value_number", "weight_number", "contribution_number"})
# Both types preserve exact source decimals in Arrow and DuckDB. The retained
# ILA detail ratings require 19 fractional digits; ratings permit 19 integer
# digits. Weights and contributions keep their original 18-digit scale.
# See the pinned census in receipts/scorecards-expansion-20261004/etl-conversion-review/.
# Values outside either declared bound refuse; no numeric rounding is allowed.
DECIMAL_TYPE = pa.decimal128(38, 18)
RATING_DECIMAL_TYPE = pa.decimal128(38, 19)


def subject_schema(name: str) -> pa.Schema:
    fields = []
    for column in DOMAIN_COLUMNS[name]:
        target, dtype = NATIVE_LISTS.get(column, (column, pa.string()))
        if column in BOOLEAN_FIELDS:
            dtype = pa.bool_()
        elif column in INTEGER_FIELDS:
            dtype = pa.int32()
        elif column in DECIMAL_FIELDS:
            dtype = RATING_DECIMAL_TYPE if column == "value_number" else DECIMAL_TYPE
        fields.append(pa.field(target, dtype, nullable=column not in IDENTITIES[name]))
    return pa.schema(fields)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _constant(value):
    raise ValueError(f"Non-finite JSON value: {value}")


def native_list(field: str, raw: str | None):
    if raw is None:
        return None
    entries = json.loads(raw, object_pairs_hook=_unique, parse_constant=_constant)
    if not isinstance(entries, list):
        raise ValueError(f"{field}: expected array, not null or scalar")
    if field == "aliases_json":
        if any(not isinstance(entry, str) or not entry for entry in entries):
            raise ValueError("Aliases require nonempty literal names")
        return entries
    keys = {f.name for f in NATIVE_LISTS[field][1].value_type}
    mandatory = (
        {"scheme", "value"}
        if field == "identifiers_json"
        else {
            "occurrence_id",
            "kind",
            "source_path",
            "period_text" if field == "periods_json" else "citation_text",
        }
    )
    allowed = keys if field == "identifiers_json" else keys | {"source_path"}
    seen = set()
    result = []
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or not mandatory <= entry.keys() <= allowed
            or any(not isinstance(entry[k], str) or not entry[k] for k in mandatory)
            or any(v is not None and not isinstance(v, str) for v in entry.values())
        ):
            raise ValueError(f"{field}: incomplete or unknown source occurrence fields")
        if field != "identifiers_json":
            if entry["occurrence_id"] in seen:
                raise ValueError(f"{field}: duplicate occurrence identity")
            seen.add(entry["occurrence_id"])
        if field == "periods_json" and entry["kind"] not in {"explicit", "relative"}:
            raise ValueError("Unknown source period kind")
        result.append({k: entry.get(k) for k in keys})
    return result


def map_source_row(name: str, row: Mapping) -> dict:
    """Classify every input and retain original conversion values for exact replay."""
    if set(row) != set(SOURCE_COLUMNS[name]):
        raise ValueError(f"{name}: source schema changed; classify every field before migration")
    if any(v is not None and not isinstance(v, str) for v in row.values()):
        raise ValueError(f"{name}: source rows must retain their string/null shape")
    mapped = dict(row)
    originals = {}
    for column in DOMAIN_COLUMNS[name]:
        value = row[column]
        if column in NATIVE_LISTS:
            mapped[NATIVE_LISTS[column][0]] = native_list(column, value)
        elif column in BOOLEAN_FIELDS:
            if value not in {None, "true", "false"}:
                raise ValueError(f"{column}: expected true, false or NULL")
            originals[column] = value
            mapped[column] = None if value is None else value == "true"
        elif column in INTEGER_FIELDS:
            if value is not None and not re.fullmatch(r"(?:0|[1-9][0-9]*)", value):
                raise ValueError(f"{column}: not a canonical resolved integer")
            originals[column] = value
            mapped[column] = None if value is None else int(value)
        elif column in DECIMAL_FIELDS:
            if value is not None and not re.fullmatch(r"[+-]?(?:\d+(?:\.\d+)?|\.\d+)", value):
                raise ValueError(f"{column}: not exact decimal text")
            originals[column] = value
            mapped[column] = None if value is None else Decimal(value)
    if originals:
        mapped["conversion_inputs"] = originals
    # Arrow rejects overflow or loss of fractional digits; never cast via float.
    if DOMAIN_COLUMNS[name]:
        mapped.update(pa.Table.from_pylist([mapped], schema=subject_schema(name)).to_pylist()[0])
    return mapped


def restore_source_row(name: str, row: Mapping) -> dict:
    """Restore the provider row after a receipt-checked internal read."""
    result = {field: row[field] for field in SOURCE_COLUMNS[name]}
    for field, value in row.get("conversion_inputs", {}).items():
        if field not in BOOLEAN_FIELDS | DECIMAL_FIELDS | INTEGER_FIELDS:
            raise ValueError("Unknown scorecard conversion input")
        result[field] = value
    return result
