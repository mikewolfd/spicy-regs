"""CMS Part B manufacturer and drug-use associations at non-multiplying workbook grain.

The CMS Part B spending table (``cms_part_b_drug_spending``) reports spending at
HCPCS billing-code grain without a manufacturer column. CMS publishes a separate
supporting workbook containing two worksheets:

* ``Manufacturer Summary`` — multiple brand/generic/manufacturer association rows
  per HCPCS code (e.g. HCPCS ``90371`` has four source rows), with no
  manufacturer-attributed spending amounts.
* ``Drug Use Information`` — one HCPCS-level drug-use monograph row per billing
  code, carrying literal ``*``/``**`` name markers and CMS's clinical disclaimer.

This transform maps those two sheets into separate, receipts-free tables carrying
exact worksheet names, 1-based row locators, workbook SHA-256, and pinned Part B
release identity so analysts can inspect associations alongside spending without
ever repeating or allocating ``cms_part_b_drug_spending`` totals.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
import hashlib
import re
from pathlib import Path
from typing import Any

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.cms_drug_spending import json_value

FAMILY = "cms-part-b-associations"
MANUFACTURER_TABLE = "cms_part_b_manufacturer_associations"
DRUG_USE_TABLE = "cms_part_b_drug_use_information"
MANUFACTURER_OUTPUT = MANUFACTURER_TABLE + ".parquet"
DRUG_USE_OUTPUT = DRUG_USE_TABLE + ".parquet"

MANUFACTURER_SHEET = "Manufacturer Summary"
DRUG_USE_SHEET = "Drug Use Information"
MANUFACTURER_HEADER_ROW = 4
DRUG_USE_HEADER_ROW = 6
COLUMNS = ("A", "B", "C", "D", "E")
MANUFACTURER_HEADERS = (
    "Medicare Billing Code (HCPCS Code)",
    "Drug Description",
    "Brand Name",
    "Generic Name",
    "Manufacturer",
)
DRUG_USE_HEADERS = (
    "Medicare Billing Code (HCPCS Code)",
    "Drug Description",
    "Brand Name",
    "Generic Name",
    "Drug Uses***",
)
DRUG_USES_NOT_AVAILABLE = "Drug uses not available"
SPENDING_ALLOCATION_STATUS = "not_allocated_in_source"

MAX_WORKBOOK_BYTES = 10 * 1024 * 1024
MAX_RESOURCES_BYTES = 1024 * 1024
MAX_WORKBOOK_ROWS = 50_000
MAX_WORKBOOK_CELLS = 250_000

_COORD_RE = re.compile(r"^([A-Z]+)([1-9]\d*)$")
_MERGE_RE = re.compile(r"^[A-Z]+([1-9]\d*):[A-Z]+([1-9]\d*)$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

MANUFACTURER_SCHEMA = pa.schema(
    [
        ("association_id", pa.string()),
        ("program", pa.string()),
        ("benefit_year", pa.int32()),
        ("hcpcs_code", pa.string()),
        ("drug_description", pa.string()),
        ("brand_name", pa.string()),
        ("generic_name", pa.string()),
        ("manufacturer_name", pa.string()),
        ("spending_allocation_status", pa.string()),
        ("value_states_json", pa.string()),
        ("source_sheet", pa.string()),
        ("source_row_number", pa.int32()),
        ("source_workbook_sha256", pa.string()),
        ("source_workbook_url", pa.string()),
        ("source_resources_url", pa.string()),
        ("source_release_logical_id", pa.string()),
        ("source_release_digest", pa.string()),
        ("source_selection_json", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_row_json", pa.string()),
    ]
)

DRUG_USE_SCHEMA = pa.schema(
    [
        ("drug_use_id", pa.string()),
        ("program", pa.string()),
        ("benefit_year", pa.int32()),
        ("hcpcs_code", pa.string()),
        ("drug_description", pa.string()),
        ("brand_name", pa.string()),
        ("generic_name", pa.string()),
        ("drug_uses", pa.string()),
        ("drug_use_status", pa.string()),
        ("value_states_json", pa.string()),
        ("source_sheet", pa.string()),
        ("source_row_number", pa.int32()),
        ("source_workbook_sha256", pa.string()),
        ("source_workbook_url", pa.string()),
        ("source_resources_url", pa.string()),
        ("source_release_logical_id", pa.string()),
        ("source_release_digest", pa.string()),
        ("source_selection_json", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_row_json", pa.string()),
    ]
)

SCHEMAS = {
    MANUFACTURER_TABLE: MANUFACTURER_SCHEMA,
    DRUG_USE_TABLE: DRUG_USE_SCHEMA,
}


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(json_value(value).encode("utf-8")).hexdigest()


def _cell_state(cell) -> tuple[str | None, str]:
    if cell is None:
        return None, "absent"
    if cell.formula is not None or cell.formula_attributes is not None:
        raise ValueError("CMS Part B supporting workbook must not contain formulas")
    if cell.kind == "e":
        raise ValueError("CMS Part B supporting workbook must not contain error cells")
    if cell.value is None:
        return None, "null"
    if cell.kind not in {"s", "inlineStr"} or not isinstance(cell.value, str):
        raise ValueError(f"CMS Part B supporting workbook cell must remain a literal string, got kind={cell.kind!r}")
    if cell.value == "":
        return "", "empty"
    return cell.value, "value"


def _sheet_rows(sheet, *, sheet_name: str, header_row: int, expected_headers: tuple[str, ...]):
    for ref in sheet.merged_ranges:
        match = _MERGE_RE.fullmatch(ref)
        if match is None or max(int(match.group(1)), int(match.group(2))) >= header_row:
            raise ValueError(
                f"CMS Part B sheet {sheet_name!r} contains unexpected merged range {ref!r} "
                f"at or below header row {header_row}"
            )
    rows: dict[int, dict[str, Any]] = {}
    for coord, cell in sheet.cells.items():
        if cell.formula is not None or cell.formula_attributes is not None:
            raise ValueError(f"CMS Part B sheet {sheet_name!r} contains a formula at {coord}")
        if cell.kind == "e":
            raise ValueError(f"CMS Part B sheet {sheet_name!r} contains an error cell at {coord}")
        match = _COORD_RE.fullmatch(coord)
        if match is None:
            raise ValueError(f"CMS Part B sheet {sheet_name!r} has invalid cell coordinate {coord!r}")
        col, row_num = match.group(1), int(match.group(2))
        rows.setdefault(row_num, {})[col] = cell

    if not rows:
        raise ValueError(f"CMS Part B sheet {sheet_name!r} is empty")
    if header_row not in rows:
        raise ValueError(f"CMS Part B sheet {sheet_name!r} is missing header row {header_row}")

    # Validate preamble rows above header: only column A may carry non-empty notes.
    notes: list[dict[str, Any]] = []
    for row_num in sorted(r for r in rows if r < header_row):
        for col, cell in rows[row_num].items():
            if col != "A" and cell.value not in (None, ""):
                raise ValueError(
                    f"CMS Part B sheet {sheet_name!r} preamble row {row_num} has unexpected text in column {col}"
                )
        cell_a = rows[row_num].get("A")
        if cell_a is not None and cell_a.value not in (None, ""):
            notes.append({"row": row_num, "text": cell_a.value})

    # Validate exact header row layout and labels.
    header_cells = rows[header_row]
    extra_header_cols = set(header_cells) - set(COLUMNS)
    if extra_header_cols:
        raise ValueError(
            f"CMS Part B sheet {sheet_name!r} header row {header_row} has unexpected columns {sorted(extra_header_cols)}"
        )
    actual_headers = tuple(header_cells[col].value if col in header_cells else None for col in COLUMNS)
    if actual_headers != expected_headers:
        raise ValueError(
            f"CMS Part B sheet {sheet_name!r} headers changed at row {header_row}: "
            f"expected {expected_headers!r}, got {actual_headers!r}"
        )

    max_row = max(rows)
    if max_row <= header_row:
        raise ValueError(f"CMS Part B sheet {sheet_name!r} has no data rows after row {header_row}")

    parsed: list[tuple[int, dict[str, str | None], dict[str, str], dict[str, str]]] = []
    for row_num in range(header_row + 1, max_row + 1):
        row_cells = rows.get(row_num)
        if not row_cells:
            raise ValueError(f"CMS Part B sheet {sheet_name!r} row {row_num} is blank (missing HCPCS key)")
        extra_cols = set(row_cells) - set(COLUMNS)
        if extra_cols:
            raise ValueError(
                f"CMS Part B sheet {sheet_name!r} row {row_num} has unexpected columns {sorted(extra_cols)}"
            )
        raw_fields: dict[str, str | None] = {}
        states: dict[str, str] = {}
        cell_coords: dict[str, str] = {}
        for col, header in zip(COLUMNS, expected_headers, strict=True):
            cell = row_cells.get(col)
            value, state = _cell_state(cell)
            raw_fields[header] = value
            states[header] = state
            if cell is not None:
                cell_coords[col] = f"{col}{row_num}"
        hcpcs = raw_fields[expected_headers[0]]
        if not isinstance(hcpcs, str) or not hcpcs.strip():
            raise ValueError(f"CMS Part B sheet {sheet_name!r} row {row_num} is missing its literal HCPCS key")
        parsed.append((row_num, raw_fields, states, cell_coords))

    return notes, parsed


def parse_workbook(
    workbook_bytes: bytes,
    *,
    year: int,
    source_workbook_url: str,
    source_resources_url: str,
    source_release_logical_id: str,
    source_release_digest: str,
    source_workbook_sha256: str | None = None,
    source_resources_sha256: str | None = None,
    observed_at: str | None = None,
    selection_metadata: Mapping[str, Any] | None = None,
    expected_hcpcs_codes: Collection[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate and map both sheets of a CMS Part B supporting workbook into literal row lists."""
    from spicy_docs.sources.scorecards.workbooks import literal_worksheets

    if type(year) is not int or year < 2000:
        raise ValueError("CMS Part B associations require an explicit integer benefit year")
    if not isinstance(workbook_bytes, (bytes, bytearray)) or not 0 < len(workbook_bytes) <= MAX_WORKBOOK_BYTES:
        raise ValueError("CMS Part B supporting workbook bytes are empty or exceed the bounded size limit")

    computed_sha = "sha256:" + hashlib.sha256(workbook_bytes).hexdigest()
    if source_workbook_sha256 is not None:
        if not _SHA256_RE.fullmatch(source_workbook_sha256) or source_workbook_sha256 != computed_sha:
            raise ValueError("CMS Part B supporting workbook digest differs from its pinned SHA-256")
    workbook_sha = computed_sha

    for label, value in (
        ("source_workbook_url", source_workbook_url),
        ("source_resources_url", source_resources_url),
        ("source_release_logical_id", source_release_logical_id),
        ("source_release_digest", source_release_digest),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"CMS Part B associations require a non-empty {label}")

    all_sheets = literal_worksheets(
        bytes(workbook_bytes),
        max_rows=MAX_WORKBOOK_ROWS,
        max_cells=MAX_WORKBOOK_CELLS,
    )
    expected_sheets = {MANUFACTURER_SHEET, DRUG_USE_SHEET}
    if set(all_sheets) != expected_sheets:
        raise ValueError(
            f"CMS Part B supporting workbook sheets changed: expected {sorted(expected_sheets)}, "
            f"got {sorted(all_sheets)}"
        )
    sheets = literal_worksheets(
        bytes(workbook_bytes),
        sheet_names=(MANUFACTURER_SHEET, DRUG_USE_SHEET),
        max_rows=MAX_WORKBOOK_ROWS,
        max_cells=MAX_WORKBOOK_CELLS,
    )

    ms_notes, ms_parsed = _sheet_rows(
        sheets[MANUFACTURER_SHEET],
        sheet_name=MANUFACTURER_SHEET,
        header_row=MANUFACTURER_HEADER_ROW,
        expected_headers=MANUFACTURER_HEADERS,
    )
    du_notes, du_parsed = _sheet_rows(
        sheets[DRUG_USE_SHEET],
        sheet_name=DRUG_USE_SHEET,
        header_row=DRUG_USE_HEADER_ROW,
        expected_headers=DRUG_USE_HEADERS,
    )

    du_codes: set[str] = set()
    for row_num, raw_fields, _states, _coords in du_parsed:
        code = raw_fields[DRUG_USE_HEADERS[0]]
        assert code is not None
        if code in du_codes:
            raise ValueError(f"Duplicate HCPCS code {code!r} in {DRUG_USE_SHEET!r} at row {row_num}")
        du_codes.add(code)

    ms_codes = {raw_fields[MANUFACTURER_HEADERS[0]] for _r, raw_fields, _s, _c in ms_parsed}
    if ms_codes != du_codes:
        only_ms = sorted(c for c in (ms_codes - du_codes) if c is not None)[:5]
        only_du = sorted(c for c in (du_codes - ms_codes) if c is not None)[:5]
        raise ValueError(
            f"CMS Part B workbook HCPCS keys differ across sheets: "
            f"only_in_manufacturer={only_ms}, only_in_drug_use={only_du}"
        )

    if expected_hcpcs_codes is not None:
        expected_set = set(expected_hcpcs_codes)
        if ms_codes != expected_set:
            missing = sorted(expected_set - ms_codes)[:5]
            extra = sorted(c for c in (ms_codes - expected_set) if c is not None)[:5]
            raise ValueError(
                f"CMS Part B workbook HCPCS keys differ from selected spending population: "
                f"missing={missing}, extra={extra}"
            )

    selection_payload = {
        "benefit_year": year,
        "program": "Medicare Part B",
        "source_release_logical_id": source_release_logical_id,
        "source_release_digest": source_release_digest,
        "source_resources_url": source_resources_url,
        "source_resources_sha256": source_resources_sha256,
        "source_workbook_url": source_workbook_url,
        "source_workbook_sha256": workbook_sha,
        "source_workbook_byte_size": len(workbook_bytes),
        "manufacturer_sheet": MANUFACTURER_SHEET,
        "manufacturer_header_row": MANUFACTURER_HEADER_ROW,
        "manufacturer_headers": list(MANUFACTURER_HEADERS),
        "manufacturer_notes": ms_notes,
        "drug_use_sheet": DRUG_USE_SHEET,
        "drug_use_header_row": DRUG_USE_HEADER_ROW,
        "drug_use_headers": list(DRUG_USE_HEADERS),
        "drug_use_notes": du_notes,
        "manufacturer_row_count": len(ms_parsed),
        "drug_use_row_count": len(du_parsed),
        "distinct_hcpcs_count": len(du_codes),
        **({"selection_metadata": dict(selection_metadata)} if selection_metadata else {}),
    }
    selection_json = json_value(selection_payload)

    def locator(sheet_name: str, row_num: int, cell_coords: dict[str, str]) -> str:
        payload: dict[str, Any] = {
            "workbook_url": source_workbook_url,
            "workbook_sha256": workbook_sha,
            "resources_url": source_resources_url,
            "sheet": sheet_name,
            "row": row_num,
            "cells": cell_coords,
        }
        if source_resources_sha256 is not None:
            payload["resources_sha256"] = source_resources_sha256
        if observed_at is not None:
            payload["observed_at"] = observed_at
        return json_value(payload)

    manufacturer_rows: list[dict[str, Any]] = []
    for row_num, raw_fields, states, cell_coords in ms_parsed:
        col_states = {
            "hcpcs_code": states[MANUFACTURER_HEADERS[0]],
            "drug_description": states[MANUFACTURER_HEADERS[1]],
            "brand_name": states[MANUFACTURER_HEADERS[2]],
            "generic_name": states[MANUFACTURER_HEADERS[3]],
            "manufacturer_name": states[MANUFACTURER_HEADERS[4]],
        }
        manufacturer_rows.append(
            {
                "association_id": _digest([workbook_sha, year, MANUFACTURER_SHEET, row_num]),
                "program": "Medicare Part B",
                "benefit_year": year,
                "hcpcs_code": raw_fields[MANUFACTURER_HEADERS[0]],
                "drug_description": raw_fields[MANUFACTURER_HEADERS[1]],
                "brand_name": raw_fields[MANUFACTURER_HEADERS[2]],
                "generic_name": raw_fields[MANUFACTURER_HEADERS[3]],
                "manufacturer_name": raw_fields[MANUFACTURER_HEADERS[4]],
                "spending_allocation_status": SPENDING_ALLOCATION_STATUS,
                "value_states_json": json_value(col_states),
                "source_sheet": MANUFACTURER_SHEET,
                "source_row_number": row_num,
                "source_workbook_sha256": workbook_sha,
                "source_workbook_url": source_workbook_url,
                "source_resources_url": source_resources_url,
                "source_release_logical_id": source_release_logical_id,
                "source_release_digest": source_release_digest,
                "source_selection_json": selection_json,
                "source_locator_json": locator(MANUFACTURER_SHEET, row_num, cell_coords),
                "raw_row_json": json_value(raw_fields),
            }
        )

    drug_use_rows: list[dict[str, Any]] = []
    for row_num, raw_fields, states, cell_coords in du_parsed:
        col_states = {
            "hcpcs_code": states[DRUG_USE_HEADERS[0]],
            "drug_description": states[DRUG_USE_HEADERS[1]],
            "brand_name": states[DRUG_USE_HEADERS[2]],
            "generic_name": states[DRUG_USE_HEADERS[3]],
            "drug_uses": states[DRUG_USE_HEADERS[4]],
        }
        uses_text = raw_fields[DRUG_USE_HEADERS[4]]
        if uses_text is None or uses_text == "":
            use_status = "empty"
        elif uses_text == DRUG_USES_NOT_AVAILABLE:
            use_status = "not_available"
        else:
            use_status = "summary_provided"
        drug_use_rows.append(
            {
                "drug_use_id": _digest([workbook_sha, year, DRUG_USE_SHEET, row_num]),
                "program": "Medicare Part B",
                "benefit_year": year,
                "hcpcs_code": raw_fields[DRUG_USE_HEADERS[0]],
                "drug_description": raw_fields[DRUG_USE_HEADERS[1]],
                "brand_name": raw_fields[DRUG_USE_HEADERS[2]],
                "generic_name": raw_fields[DRUG_USE_HEADERS[3]],
                "drug_uses": uses_text,
                "drug_use_status": use_status,
                "value_states_json": json_value(col_states),
                "source_sheet": DRUG_USE_SHEET,
                "source_row_number": row_num,
                "source_workbook_sha256": workbook_sha,
                "source_workbook_url": source_workbook_url,
                "source_resources_url": source_resources_url,
                "source_release_logical_id": source_release_logical_id,
                "source_release_digest": source_release_digest,
                "source_selection_json": selection_json,
                "source_locator_json": locator(DRUG_USE_SHEET, row_num, cell_coords),
                "raw_row_json": json_value(raw_fields),
            }
        )

    return manufacturer_rows, drug_use_rows


def build_cms_part_b_associations(
    workbook_bytes: bytes,
    output_dir: Path,
    *,
    year: int,
    source_workbook_url: str,
    source_resources_url: str,
    source_release_logical_id: str,
    source_release_digest: str,
    source_workbook_sha256: str | None = None,
    source_resources_sha256: str | None = None,
    observed_at: str | None = None,
    selection_metadata: Mapping[str, Any] | None = None,
    expected_hcpcs_codes: Collection[str] | None = None,
) -> list[Path]:
    """Validate completely before writing both Parquet tables atomically; prior output survives any refusal."""
    manufacturer_rows, drug_use_rows = parse_workbook(
        workbook_bytes,
        year=year,
        source_workbook_url=source_workbook_url,
        source_resources_url=source_resources_url,
        source_release_logical_id=source_release_logical_id,
        source_release_digest=source_release_digest,
        source_workbook_sha256=source_workbook_sha256,
        source_resources_sha256=source_resources_sha256,
        observed_at=observed_at,
        selection_metadata=selection_metadata,
        expected_hcpcs_codes=expected_hcpcs_codes,
    )
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return [
        write_rows(manufacturer_rows, output_dir / MANUFACTURER_OUTPUT, MANUFACTURER_SCHEMA),
        write_rows(drug_use_rows, output_dir / DRUG_USE_OUTPUT, DRUG_USE_SCHEMA),
    ]
