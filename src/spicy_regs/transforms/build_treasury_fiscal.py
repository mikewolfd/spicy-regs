"""Exact Treasury stocks, Table 1 budget actuals and Table 2 classification cells from admitted source rows."""
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
CELLS = "treasury_statement_classification_cells"
#: Reviewed per-field units and per-edition 0.00-estimate determinations; see the file's note.
UNIT_RULES = json.loads(Path(__file__).with_name("treasury_units.json").read_text())
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
#: Native Table 2 lines: (classification_desc, data_type_cd, parent src_line_nbr). The identities
#: below name these lines, so a different layout refuses instead of checking the wrong rows.
TABLE2_LAYOUT = {
    1: ("Total On-Budget and Off-Budget Results:", "S", None),
    2: ("Total Receipts", "T", 1), 3: ("On-Budget Receipts", "D", 2), 4: ("Off-Budget Receipts", "D", 2),
    5: ("Total Outlays", "T", 1), 6: ("On-Budget Outlays", "D", 5), 7: ("Off-Budget Outlays", "D", 5),
    8: ("Total Surplus (+) or Deficit (-)", "T", 1),
    9: ("On-Budget Surplus (+) or Deficit (-)", "D", 8), 10: ("Off-Budget Surplus (+) or Deficit (-)", "D", 8),
    11: ("Total On-Budget and Off-Budget Financing", "T", 1), 12: ("Means of Financing:", "S", None),
    13: ("Borrowing from the Public", "D", 12), 14: ("Reduction of Operating Cash, Increase (-)", "D", 12),
    15: ("By Other Means", "D", 12),
}
#: (result line, signed operand lines) per measure: on plus off equals total, receipts minus outlays
#: equals surplus, financing equals minus surplus, and the means of financing sum to financing.
TABLE2_IDENTITIES = (
    (2, ((3, 1), (4, 1))), (5, ((6, 1), (7, 1))), (8, ((9, 1), (10, 1))),
    (8, ((2, 1), (5, -1))), (9, ((3, 1), (6, -1))), (10, ((4, 1), (7, -1))),
    (11, ((8, -1),)), (11, ((13, 1), (14, 1), (15, 1))),
)
#: Table 2 actual columns against Table 1: (Table 2 line, Table 1 column, sign).
TABLE2_TABLE1_LINES = ((2, "budget_receipts_usd", 1), (5, "budget_outlays_usd", 1), (8, "budget_deficit_usd", -1))
_ARITHMETIC = ("value", "zero")
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
    CELLS: pa.schema([
        ("report_date", pa.date32()), ("src_line_nbr", pa.int32()), ("parent_src_line_nbr", pa.int32()),
        ("print_order_nbr", pa.int32()), ("sequence_level_nbr", pa.int32()),
        ("sequence_number_cd_literal", pa.string()), ("classification_id", pa.string()),
        ("parent_id_literal", pa.string()), ("classification_desc", pa.string()),
        ("data_type_cd", pa.string()), ("record_type_cd", pa.string()),
        ("measure", pa.string()), ("measure_fiscal_year", pa.int32()), ("amount_literal", pa.string()),
        ("amount", pa.decimal128(38, 2)), ("amount_unit", pa.string()), ("marker_kind", pa.string()),
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


def _cell(raw, field, line, review):
    """Type one Table 2 cell; a 0.00 estimate needs the edition's page-5 determination.

    The publisher prints some 0.00 estimates as '......' (No Transactions) and
    others as a projected zero, so the field name alone cannot decide.
    """
    if field not in raw:
        raise ValueError("Treasury Table 2 row omits an amount field")
    value, status = _amount(raw[field])
    if status != "value" or value != 0:
        return value, status
    if not UNIT_RULES["datasets"]["mts_table_2"][field]["estimate"]:
        return value, "zero"
    if line in review.get("no_transactions", {}).get(field, ()):
        return None, "no_transactions"
    if line in review.get("zero", {}).get(field, ()):
        return value, "zero"
    raise ValueError("Treasury 0.00 estimate has no page-review determination")


def _require_unit_rules(row):
    rules = UNIT_RULES["datasets"].get(row["dataset"], {})
    types = row["field_metadata"].get("dataTypes", {})
    if any((types.get(field) == "CURRENCY" or field.endswith("_amt")) and field not in rules
           for field in set(types) | set(row["raw"])):
        raise ValueError("Treasury amount field has no committed unit rule")


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


def _native_int(raw, key):
    if not isinstance(raw[key], str) or not raw[key].isascii() or not raw[key].isdigit():
        raise ValueError("Treasury native order field is not an integer literal")
    return int(raw[key])


def _table2_rows(reader, items, report, qualification):
    """Emit every native line and measure of one Table 2 issue, with its cell markers."""
    report_fy = report.year + (report.month >= 10)
    lines, identities = {}, {}
    for item in items:
        raw = item["record"]["raw"]
        line = _native_int(raw, "src_line_nbr")
        if raw["table_nbr"] != "2" or raw["record_fiscal_year"] != str(report_fy):
            raise ValueError("Treasury Table 2 record has a different table or fiscal year")
        if line in lines or raw["classification_id"] in identities:
            raise ValueError("Repeated Treasury Table 2 line within a report release")
        lines[line], identities[raw["classification_id"]] = item, line
    pages = {item["record"]["source_locator"]["response_sha256"] for item in items}
    if set(lines) != set(TABLE2_LAYOUT) or len(pages) != 1:
        raise ValueError("Unsupported Treasury Table 2 layout")
    review = UNIT_RULES["zero_estimates"].get(pages.pop(), {})
    rules = UNIT_RULES["datasets"]["mts_table_2"]
    output, cells = [], {}
    for line, item in sorted(lines.items()):
        raw = item["record"]["raw"]
        parent = None if raw["parent_id"] in (None, "null") else identities.get(raw["parent_id"], "unresolved")
        if (raw["classification_desc"], raw["data_type_cd"], parent) != TABLE2_LAYOUT[line]:
            raise ValueError("Unsupported Treasury Table 2 layout")
        for field, rule in rules.items():
            amount, marker = cells[line, field] = _cell(raw, field, line, review)
            output.append({
                "report_date": report, "src_line_nbr": line, "parent_src_line_nbr": parent,
                "print_order_nbr": _native_int(raw, "print_order_nbr"),
                "sequence_level_nbr": _native_int(raw, "sequence_level_nbr"),
                "sequence_number_cd_literal": raw["sequence_number_cd"],
                "classification_id": raw["classification_id"], "parent_id_literal": raw["parent_id"],
                "classification_desc": raw["classification_desc"], "data_type_cd": raw["data_type_cd"],
                "record_type_cd": raw["record_type_cd"], "measure": field,
                "measure_fiscal_year": report_fy + rule["fiscal_year_offset"], "amount_literal": raw[field],
                "amount": amount, "amount_unit": rule["unit"], "marker_kind": marker,
                "qualification_report_sha256": qualification["sha256"],
                "qualification_report_url": qualification["url"],
                **_source(reader, item),
            })
    reviewed = {(kind, line, field) for kind in ("no_transactions", "zero")
                for field, listed in review.get(kind, {}).items() for line in listed}
    if reviewed - {(marker, line, field) for (line, field), (_, marker) in cells.items()}:
        raise ValueError("Treasury page review names a cell that is not a 0.00 estimate")
    return output, cells


def _check_identities(cells):
    """Evaluate each identity where every operand is a number; markers without a number skip."""
    evaluated = skipped = 0
    for field in UNIT_RULES["datasets"]["mts_table_2"]:
        for result, terms in TABLE2_IDENTITIES:
            if any(cells[line, field][1] not in _ARITHMETIC for line in (result, *(line for line, _ in terms))):
                skipped += 1
                continue
            evaluated += 1
            total = _sum(cells[line, field][0] * sign for line, sign in terms)
            if total != cells[result, field][0]:
                raise ValueError(f"Treasury Table 2 identity fails for line {result} {field}")
    return {"evaluated": evaluated, "skipped": skipped}


def _table1_failures(cells, periods, report):
    """Count Table 2 actual cells that differ from one Table 1 release of the same issue."""
    fy = report.year + (report.month >= 10)
    by_key = {(row["economic_fiscal_year"], row["period_kind"], row["period_start"]): row for row in periods}
    months = [month for month in (*range(10, 13), *range(1, 10))
              if date(fy - 1 - (month >= 10), month, 1) <= date(report.year - 1, report.month, 1)]
    try:
        expected = {
            "current_month_budget_amt": [by_key[fy, "month", date(report.year, report.month, 1)]],
            "current_fytd_budget_amt": [by_key[fy, "fiscal_year_to_date", date(fy - 1, 10, 1)]],
            "prior_fytd_budget_amt": [by_key[fy - 1, "month", date(fy - 1 - (month >= 10), month, 1)]
                                      for month in months],
        }
    except KeyError:
        return None
    failures = evaluated = 0
    for field, sources in expected.items():
        for line, column, sign in TABLE2_TABLE1_LINES:
            amount, marker = cells[line, field]
            values = [row[column] for row in sources]
            if marker not in _ARITHMETIC or any(value is None for value in values):
                continue
            evaluated += 1
            failures += _sum(values) * sign != amount
    return failures, evaluated


def build_treasury_fiscal(readers, output_dir: Path, *, debt_dates, report_dates, qualifications):
    """Build one explicit set of source releases, retaining issue/revision identity.

    Returns the written tables and one check summary per Table 2 release.
    """
    debt_dates, report_dates = set(debt_dates), set(report_dates)
    if not debt_dates or not report_dates or set(qualifications) != report_dates:
        raise ValueError("Declare debt/report dates and a qualified report for every issue")
    rows, seen_debt, seen_reports, releases = {DEBT: [], STATEMENTS: [], CELLS: []}, set(), set(), set()
    table1, table2 = {}, {}
    for reader in readers:
        if reader.pin in releases:
            raise ValueError("A Treasury source release was supplied twice")
        releases.add(reader.pin)
        statements, classifications, seen_records = {}, {}, set()
        for item in reader.iter_records():
            row, raw = item["record"], item["record"]["raw"]
            dataset, when = row["dataset"], date.fromisoformat(raw["record_date"])
            if item["sourceRecordId"] in seen_records:
                raise ValueError("Repeated Treasury source record")
            seen_records.add(item["sourceRecordId"])
            if row["selection"]["dataset"] != dataset or row["selection"]["record_date"] != when.isoformat():
                raise ValueError("Treasury row differs from its declared source selection")
            _require_unit_rules(row)
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
                (statements if dataset == "mts_table_1" else classifications).setdefault(when, []).append(item)
            else:
                raise ValueError("Unsupported Treasury dataset")
        for report, items in statements.items():
            derived = _statement_rows(reader, items, report, qualifications[report])
            if not derived:
                raise ValueError("Treasury report contains no budget periods")
            rows[STATEMENTS].extend(derived)
            table1.setdefault(report, []).append(derived)
            seen_reports.add(report)
        for report, items in classifications.items():
            derived, cells = _table2_rows(reader, items, report, qualifications[report])
            rows[CELLS].extend(derived)
            table2.setdefault(report, []).append((reader.pin, cells))
    if seen_debt != debt_dates or seen_reports != report_dates or set(table2) != report_dates:
        raise ValueError("Requested Treasury period is unavailable; no latest-period substitution")
    summary = []
    for report, releases_of_issue in sorted(table2.items()):
        for pin, cells in releases_of_issue:
            # Same-issue revisions stay separate releases: each Table 2 release must
            # reconcile with at least one Table 1 release of its issue, not with all.
            pairs = [_table1_failures(cells, periods, report) for periods in table1[report]]
            matched = [pair for pair in pairs if pair is not None and pair[0] == 0]
            if not matched:
                raise ValueError("Treasury Table 2 actuals differ from every Table 1 release of the issue")
            summary.append({"report_date": report.isoformat(), "source_release_digest": pin.artifact_digest,
                            "identities": _check_identities(cells), "table1_cells_evaluated": matched[0][1]})
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    # These explicitly bounded selections are validated before any table is written.
    files = [write_rows(rows[name], output_dir / (name + ".parquet"), schema) for name, schema in SCHEMAS.items()]
    return files, summary
