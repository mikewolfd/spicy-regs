"""Interpret one full USAspending transaction snapshot without adding award totals twice."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

MONEY = pa.decimal128(38, 2)
TEXT = pa.string()
COMMON = [
    ("archive_sha256", TEXT),
    ("archive_url", TEXT),
    ("archive_version", TEXT),
    ("award_family", TEXT),
    ("fiscal_year", pa.int32()),
    ("awarding_agency_code", TEXT),
]
SCHEMAS = {
    "usaspending_transactions": pa.schema(
        COMMON
        + [
            ("transaction_key", TEXT),
            ("award_key", TEXT),
            ("action_date", pa.date32()),
            ("federal_action_obligation", MONEY),
            ("publisher_overall_award_obligated_amount", MONEY),
            ("publisher_overall_award_outlayed_amount", MONEY),
            ("recipient_uei", TEXT),
            ("recipient_name", TEXT),
            ("recipient_state_code", TEXT),
            ("place_of_performance_state_name", TEXT),
            ("funding_agency_code", TEXT),
            ("award_type_code", TEXT),
            ("modification_number", TEXT),
            ("last_modified_date", TEXT),
            ("correction_delete_indicator_code", TEXT),
            ("source_member", TEXT),
            ("source_member_sha256", TEXT),
            ("record_number", pa.int64()),
            ("line_start", pa.int64()),
            ("line_end", pa.int64()),
            ("raw_cells_json", TEXT),
        ]
    ),
    "usaspending_awards": pa.schema(
        COMMON
        + [
            ("award_key", TEXT),
            ("selected_transaction_count", pa.int64()),
            ("selected_fy_obligations", MONEY),
            ("publisher_overall_award_obligated_amount", MONEY),
            ("publisher_overall_award_outlayed_amount", MONEY),
            ("first_selected_action_date", pa.date32()),
            ("last_selected_action_date", pa.date32()),
        ]
    ),
    "usaspending_archive_members": pa.schema(
        COMMON
        + [
            ("source_member", TEXT),
            ("source_member_sha256", TEXT),
            ("row_count", pa.int64()),
            ("header_json", TEXT),
            ("added_columns_json", TEXT),
            ("missing_columns_json", TEXT),
        ]
    ),
}


def money(raw: str, *, required: bool = False) -> Decimal | None:
    """Exact signed USD, retaining blank as unknown; no exponent or rounding."""
    if raw == "" and not required:
        return None
    if not re.fullmatch(r"-?[0-9]+(?:\.[0-9]{1,2})?", raw):
        raise ValueError("USAspending money must be a literal decimal with at most two fractional digits")
    value = Decimal(raw)
    if value.copy_abs() >= Decimal("1e36"):
        raise ValueError("USAspending money exceeds DECIMAL(38,2)")
    return value


def build_tables(archive, output: Path, *, selection: dict) -> tuple[Path, ...]:
    """Replace one declared full cohort; never append a delta or select revisions by date.

    A duplicate transaction key is ambiguous across files/revisions and refuses,
    even when its cells match. Award-wide totals must agree across contributing
    rows, including known/blank distinctions. Funding lists remain native cells;
    no funding/subaward join can multiply the financial rows.
    """
    if selection["snapshot_kind"] != "full":
        raise ValueError(
            "Only a complete full snapshot can replace this selected cohort; deltas require reconciliation"
        )
    if archive.sha256 != selection["archive"]["sha256"] or archive.award_family != selection["award_family"]:
        raise ValueError("Archive differs from the selected identity or award family")
    year = selection["fiscal_year"]
    if type(year) is not int or not 2001 <= year <= 9999:
        raise ValueError("Select a supported fiscal year")
    base = {
        "archive_sha256": archive.sha256,
        "archive_url": selection["archive"]["url"],
        "archive_version": selection["archive"]["updated_date"],
        "award_family": archive.award_family,
        "fiscal_year": year,
        "awarding_agency_code": selection["agency_code"],
    }
    members = {m.name: m for m in archive.members}
    required = {
        "total_obligated_amount",
        "total_outlayed_amount_for_overall_award",
        "recipient_uei",
        "recipient_name",
        "recipient_state_code",
        "primary_place_of_performance_state_name",
        "funding_agency_code",
        "modification_number",
        "correction_delete_indicator_code",
        "assistance_type_code" if archive.award_family == "assistance" else "award_type_code",
    }
    if any(not required <= set(m.header) for m in archive.members):
        raise ValueError("Transaction header lacks a field needed for the financial tables")
    if archive.award_family == "assistance" and any("record_type_code" not in m.header for m in archive.members):
        raise ValueError("Assistance header must state its native aggregation level")
    transactions, by_award, seen = [], defaultdict(list), set()
    prefix = "assistance" if archive.award_family == "assistance" else "contract"
    for row in archive.rows:
        member = members[row.member]
        raw = dict(zip(member.header, row.cells, strict=True))
        key, award = raw[f"{prefix}_transaction_unique_key"], raw[f"{prefix}_award_unique_key"]
        if not key.strip() or not award.strip() or key in seen:
            raise ValueError("Missing or repeated transaction/award identity; do not combine archive revisions")
        seen.add(key)
        action = date.fromisoformat(raw["action_date"])
        if (
            raw["action_date_fiscal_year"] != str(year)
            or action.year + (action.month >= 10) != year
            or raw["awarding_agency_code"] != base["awarding_agency_code"]
        ):
            raise ValueError("Transaction lies outside the declared awarding agency/fiscal year")
        code = raw["assistance_type_code"] if prefix == "assistance" else raw["award_type_code"]
        permitted = {f"{n:02d}" for n in range(2, 12)} if prefix == "assistance" else {"A", "B", "C", "D"}
        if code not in permitted:
            raise ValueError("Transaction type differs from the selected award family")
        if raw["correction_delete_indicator_code"] not in {"", "C"}:
            raise ValueError("A deleted or unknown correction code requires reconciliation before full-snapshot use")
        if prefix == "assistance" and raw["record_type_code"] != "2":
            raise ValueError("Aggregate or unknown assistance record types require separate population interpretation")
        tx = {
            **base,
            "transaction_key": key,
            "award_key": award,
            "action_date": action,
            "federal_action_obligation": money(raw["federal_action_obligation"], required=True),
            "publisher_overall_award_obligated_amount": money(raw["total_obligated_amount"]),
            "publisher_overall_award_outlayed_amount": money(raw["total_outlayed_amount_for_overall_award"]),
            "recipient_uei": raw["recipient_uei"],
            "recipient_name": raw["recipient_name"],
            "recipient_state_code": raw["recipient_state_code"],
            "place_of_performance_state_name": raw["primary_place_of_performance_state_name"],
            "funding_agency_code": raw["funding_agency_code"],
            "award_type_code": code,
            "modification_number": raw["modification_number"],
            "last_modified_date": raw["last_modified_date"],
            "correction_delete_indicator_code": raw["correction_delete_indicator_code"],
            "source_member": row.member,
            "source_member_sha256": member.sha256,
            "record_number": row.record_number,
            "line_start": row.line_start,
            "line_end": row.line_end,
            "raw_cells_json": json.dumps(raw, ensure_ascii=False, separators=(",", ":")),
        }
        transactions.append(tx)
        by_award[award].append(tx)
    awards = []
    for award, rows in by_award.items():
        totals = {}
        for column in ("publisher_overall_award_obligated_amount", "publisher_overall_award_outlayed_amount"):
            values = {r[column] for r in rows}
            if len(values) != 1:
                raise ValueError(
                    "Award-wide totals disagree across transactions; preserve the conflict before publication"
                )
            totals[column] = values.pop()
        with localcontext() as context:
            context.prec = 50
            obligations = sum((r["federal_action_obligation"] for r in rows), Decimal(0))
        awards.append(
            {
                **base,
                "award_key": award,
                "selected_transaction_count": len(rows),
                "selected_fy_obligations": obligations,
                **totals,
                "first_selected_action_date": min(r["action_date"] for r in rows),
                "last_selected_action_date": max(r["action_date"] for r in rows),
            }
        )
    counts = defaultdict(int)
    for row in transactions:
        counts[row["source_member"]] += 1
    member_rows = [
        {
            **base,
            "source_member": m.name,
            "source_member_sha256": m.sha256,
            "row_count": counts[m.name],
            "header_json": json.dumps(m.header),
            "added_columns_json": json.dumps(m.added_columns),
            "missing_columns_json": json.dumps(m.missing_columns),
        }
        for m in archive.members
    ]
    # Validate every value and relationship before creating any table output.
    tables = [
        pa.Table.from_pylist(rows, schema=SCHEMAS[name])
        for name, rows in zip(SCHEMAS, (transactions, awards, member_rows), strict=True)
    ]
    output.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, table in zip(SCHEMAS, tables, strict=True):
        path = output / f"{name}.parquet"
        if path.exists():
            raise ValueError("Use a fresh candidate directory")
        paths.append(path)
    for path, table in zip(paths, tables, strict=True):
        pq.write_table(table, path, compression="zstd")
    return tuple(paths)
