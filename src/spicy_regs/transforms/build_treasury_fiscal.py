"""Exact Treasury stocks and Table 1 budget actuals from admitted source rows."""
from __future__ import annotations

import calendar
import json
import re
from datetime import date
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

DEBT = "treasury_debt_observations"
STATEMENTS = "treasury_statement_periods"
DEBT_FIELDS = {
    "total_debt_usd": "tot_pub_debt_out_amt",
    "public_debt_usd": "debt_held_public_amt",
    "intragovernmental_debt_usd": "intragov_hold_amt",
}
BUDGET_FIELDS = {
    "budget_receipts_usd": "current_month_gross_rcpt_amt",
    "budget_outlays_usd": "current_month_gross_outly_amt",
    "budget_deficit_usd": "current_month_dfct_sur_amt",
}
_MONTHS = {name: index for index, name in enumerate(
    ("January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"), 1)}
_SOURCE_COLUMNS = [
    ("source_record_id", pa.string()), ("source_release_logical_id", pa.string()),
    ("source_release_digest", pa.string()), ("source_page_sha256", pa.string()),
    ("source_locator_json", pa.string()), ("source_selection_json", pa.string()),
    ("field_metadata_json", pa.string()), ("raw_row_json", pa.string()),
]
SCHEMAS = {
    DEBT: pa.schema([
        ("record_date", pa.date32()),
        *((name, pa.decimal128(38, 2)) for name in DEBT_FIELDS),
        ("amount_statuses_json", pa.string()), *_SOURCE_COLUMNS,
    ]),
    STATEMENTS: pa.schema([
        ("report_date", pa.date32()), ("economic_fiscal_year", pa.int32()),
        ("period_kind", pa.string()), ("period_start", pa.date32()), ("period_end", pa.date32()),
        *((name, pa.decimal128(38, 2)) for name in BUDGET_FIELDS),
        ("amount_statuses_json", pa.string()), ("classification_id", pa.string()),
        ("parent_id_literal", pa.string()), ("classification_desc", pa.string()),
        ("sequence_number_cd_literal", pa.string()), ("parent_context_json", pa.string()),
        ("qualification_report_sha256", pa.string()), ("qualification_report_url", pa.string()),
        *_SOURCE_COLUMNS,
    ]),
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _amount(raw):
    if raw is None:
        return None, "json_null"
    if raw == "null":
        return None, "publisher_null"
    if raw == "":
        return None, "blank"
    if not isinstance(raw, str) or not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", raw):
        raise ValueError("Treasury amount must be a literal decimal or an explicit null")
    value = Decimal(raw)
    with localcontext() as context:
        context.prec = 42
        if value != value.quantize(Decimal(".01")) or abs(value) >= Decimal("1e36"):
            raise ValueError("Treasury amount exceeds exact DECIMAL(38,2)")
    return value, "value"


def _amounts(raw, fields):
    pairs = {name: _amount(raw[key]) for name, key in fields.items()}
    return {name: pair[0] for name, pair in pairs.items()} | {
        "amount_statuses_json": _json({name: pair[1] for name, pair in pairs.items()})
    }


def _sum(values):
    with localcontext() as context:
        context.prec = 42
        return sum(values, Decimal(0))


def _source(reader, item):
    row = item["record"]
    return {
        "source_record_id": item["sourceRecordId"],
        "source_release_logical_id": reader.pin.logical_id,
        "source_release_digest": reader.pin.artifact_digest,
        "source_page_sha256": row["source_locator"]["response_sha256"],
        "source_locator_json": _json(row["source_locator"]),
        "source_selection_json": _json(row["selection"]),
        "field_metadata_json": _json(row["field_metadata"]),
        "raw_row_json": _json(row["raw"]),
    }


def _statement_rows(reader, items, report, qualification):
    """Support complete prior FYs and current October–report-month rows plus FYTD.

    This shape is qualified against the retained September 2026 issue. Other
    layouts refuse; these date rules are not a claim about every historical MTS.
    """
    parents, children = {}, {}
    for item in items:
        raw = item["record"]["raw"]
        identity = raw["classification_id"]
        if identity in parents or identity in children:
            raise ValueError("Repeated Treasury classification within a report release")
        if raw["table_nbr"] != "1":
            raise ValueError("Treasury Table 1 record has a different table number")
        if raw["parent_id"] in (None, "null"):
            match = re.fullmatch(r"FY ([0-9]{4})", raw["classification_desc"])
            if (not match or raw["data_type_cd"] != "S"
                    or int(match[1]) in {year for year, _ in parents.values()}):
                raise ValueError("Unsupported Treasury fiscal-year header")
            parents[identity] = (int(match[1]), item)
        else:
            children[identity] = item
    report_fy = report.year + (report.month >= 10)
    periods, output = {}, []
    for item in children.values():
        raw = item["record"]["raw"]
        if raw["parent_id"] not in parents:
            raise ValueError("Treasury period has no fiscal-year parent")
        fiscal_year, parent = parents[raw["parent_id"]]
        if not 1900 <= fiscal_year <= report_fy:
            raise ValueError("Treasury economic fiscal year exceeds the report issue")
        year_end = date(fiscal_year, 9, 30) if fiscal_year < report_fy else report
        if year_end.day != calendar.monthrange(year_end.year, year_end.month)[1]:
            raise ValueError("Treasury statement issue must name a month end")
        if raw["classification_desc"] in _MONTHS and raw["record_type_cd"] == "MTH":
            month = _MONTHS[raw["classification_desc"]]
            year = fiscal_year - (month >= 10)
            start, end = date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])
            kind = "month"
        elif raw["classification_desc"] == "Year-to-Date" and raw["data_type_cd"] == "T":
            start, end, kind = date(fiscal_year - 1, 10, 1), year_end, "fiscal_year_to_date"
        else:
            raise ValueError("Unsupported Treasury Table 1 period row")
        key = fiscal_year, kind, start
        if key in periods or end > year_end:
            raise ValueError("Repeated or future Treasury economic period")
        amounts = _amounts(raw, BUDGET_FIELDS)
        a, b, c = (amounts[name] for name in BUDGET_FIELDS)
        if all(value is not None for value in (a, b, c)) and _sum((a, c)) != b:
            raise ValueError("Treasury deficit differs from outlays minus receipts")
        periods[key] = amounts
        output.append({
            "report_date": report, "economic_fiscal_year": fiscal_year,
            "period_kind": kind, "period_start": start, "period_end": end, **amounts,
            "classification_id": raw["classification_id"], "parent_id_literal": raw["parent_id"],
            "classification_desc": raw["classification_desc"],
            "sequence_number_cd_literal": raw["sequence_number_cd"],
            "parent_context_json": _json(parent),
            "qualification_report_sha256": qualification["sha256"],
            "qualification_report_url": qualification["url"],
            **_source(reader, item),
        })
    for fiscal_year, _ in parents.values():
        end = date(fiscal_year, 9, 30) if fiscal_year < report_fy else report
        months = [date(fiscal_year - (month >= 10), month, 1)
                  for month in (*range(10, 13), *range(1, 10))
                  if date(fiscal_year - (month >= 10), month, 1) <= end]
        keys = [(fiscal_year, "month", start) for start in months]
        ytd = (fiscal_year, "fiscal_year_to_date", date(fiscal_year - 1, 10, 1))
        if any(key not in periods for key in (*keys, ytd)):
            raise ValueError("Unsupported Treasury report shape: missing required monthly or fiscal-year-to-date period")
        for field in BUDGET_FIELDS:
            values = [periods[key][field] for key in keys]
            total = periods[ytd][field]
            if total is not None and all(value is not None for value in values) and _sum(values) != total:
                raise ValueError("Treasury monthly actuals do not reconcile to the reported fiscal-year total")
    return output


def build_treasury_fiscal(readers, output_dir: Path, *, debt_dates, report_dates, qualifications):
    """Build one explicit set of source releases, retaining issue/revision identity."""
    debt_dates, report_dates = set(debt_dates), set(report_dates)
    if not debt_dates or not report_dates or set(qualifications) != report_dates:
        raise ValueError("Declare debt/report dates and a qualified report for every issue")
    rows, seen_debt, seen_reports, releases = {DEBT: [], STATEMENTS: []}, set(), set(), set()
    controls = set()
    for reader in readers:
        if reader.pin in releases:
            raise ValueError("A Treasury source release was supplied twice")
        releases.add(reader.pin)
        statements, seen_records = {}, set()
        for item in reader.iter_records():
            row, raw = item["record"], item["record"]["raw"]
            dataset, when = row["dataset"], date.fromisoformat(raw["record_date"])
            if item["sourceRecordId"] in seen_records:
                raise ValueError("Repeated Treasury source record")
            seen_records.add(item["sourceRecordId"])
            if row["selection"]["dataset"] != dataset or row["selection"]["record_date"] != when.isoformat():
                raise ValueError("Treasury row differs from its declared source selection")
            if dataset == "debt_to_penny":
                if when not in debt_dates:
                    raise ValueError("Unexpected Treasury debt observation date")
                amounts = _amounts(raw, DEBT_FIELDS)
                total, public, intra = (amounts[name] for name in DEBT_FIELDS)
                if all(value is not None for value in (total, public, intra)) and _sum((public, intra)) != total:
                    raise ValueError("Treasury debt components do not sum to total debt")
                rows[DEBT].append({"record_date": when, **amounts, **_source(reader, item)})
                seen_debt.add(when)
            elif dataset in ("mts_table_1", "mts_table_2"):
                if when not in report_dates:
                    raise ValueError("Unexpected Treasury report issue")
                if dataset == "mts_table_1":
                    statements.setdefault(when, []).append(item)
                else:
                    controls.add(when)
            else:
                raise ValueError("Unsupported Treasury dataset")
        for report, items in statements.items():
            derived = _statement_rows(reader, items, report, qualifications[report])
            if not derived:
                raise ValueError("Treasury report contains no budget periods")
            rows[STATEMENTS].extend(derived)
            seen_reports.add(report)
    if seen_debt != debt_dates or seen_reports != report_dates or controls != report_dates:
        raise ValueError("Requested Treasury period is unavailable; no latest-period substitution")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # These explicitly bounded selections are validated before either table is written.
    return [write_rows(rows[name], output_dir / (name + ".parquet"), schema)
            for name, schema in SCHEMAS.items()]
