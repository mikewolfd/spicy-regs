"""CMS-2552-10 report/cell facts and explicitly selected worksheet measurements."""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.cms_drug_spending import measurement

FAMILY = "hospital-cost-reports"
REPORTS, CELLS, MEASUREMENTS = "hospital_cost_reports", "hospital_cost_report_cells", "hospital_cost_report_measures"
# CMS Hospital Cost Report Data Dictionary: selected cells, no rollups or ratios.
MEASURES = {
    "hospital_beds": ("S300001", "01400", "00200", "beds", "hospital"),
    "total_costs": ("C000001", "20000", "00300", "USD", "worksheet_C_part_I_total"),
    "total_charges": ("C000001", "20000", "00800", "USD", "worksheet_C_part_I_total"),
    "net_patient_revenue": ("G300000", "00300", "00100", "USD", "financial_statement_reporting_entity"),
}
LOCATOR_FIELDS = [
    ("source_archive_sha256", pa.string()), ("source_member", pa.string()),
    ("source_member_sha256", pa.string()), ("source_record_number", pa.int64()),
    ("source_byte_offset", pa.int64()), ("source_byte_length", pa.int64()),
]
REPORT_SCHEMA = pa.schema([
    ("report_id", pa.string()), ("ccn", pa.string()), ("form", pa.string()),
    ("cohort_year", pa.int32()), ("report_status_raw", pa.string()), ("transmittal_raw", pa.string()),
    ("fiscal_begin", pa.date32()), ("fiscal_end", pa.date32()), ("period_days", pa.int32()),
    ("period_status", pa.string()), ("cohort_status", pa.string()),
    ("selected_report_policy", pa.string()), ("raw_report_json", pa.string()), *LOCATOR_FIELDS,
])
CELL_SCHEMA = pa.schema([
    ("report_id", pa.string()), ("cell_kind", pa.string()), ("worksheet", pa.string()),
    ("line", pa.string()), ("column", pa.string()), ("value_raw", pa.string()), *LOCATOR_FIELDS,
])
MEASURE_SCHEMA = pa.schema([
    ("report_id", pa.string()), ("ccn", pa.string()), ("fiscal_begin", pa.date32()),
    ("fiscal_end", pa.date32()), ("measure", pa.string()), ("unit", pa.string()),
    ("provider_scope", pa.string()), ("worksheet", pa.string()), ("line", pa.string()),
    ("column", pa.string()), ("value_raw", pa.string()), ("value", pa.decimal128(38, 12)),
    ("value_status", pa.string()), *LOCATOR_FIELDS,
])
SCHEMAS = {REPORTS: REPORT_SCHEMA, CELLS: CELL_SCHEMA, MEASUREMENTS: MEASURE_SCHEMA}
POLICY = "all_filings_in_pinned_CMS_highest_status_extract; no_additional_revision_selection"


def locator(item):
    source = item["source_locator"]
    return dict(zip((name for name, _ in LOCATOR_FIELDS), (
        source["archive_sha256"], source["member"], source["member_sha256"],
        source["record_number"], source["byte_offset"], source["byte_length"],
    ), strict=True))


def source_date(raw):
    if raw == "":
        return None
    try:
        return datetime.strptime(raw, "%m/%d/%Y").date()
    except ValueError as error:
        raise ValueError("Hospital reporting date has an unsupported format") from error


def report(item, reader, validate_ccn):
    raw = item["raw"]
    ccn = validate_ccn(raw["prvdr_num"])
    if ccn != raw["prvdr_num"] or len(ccn) != 6:
        raise ValueError("Hospital CCN must retain its native six-character identity")
    if raw["rpt_stus_cd"] not in {"1", "2", "3", "4", "5"}:
        raise ValueError("Hospital report has an unsupported status")
    begin, end = source_date(raw["fy_bgn_dt"]), source_date(raw["fy_end_dt"])
    days = None if begin is None or end is None else (end - begin).days + 1
    if days is not None and days <= 0:
        raise ValueError("Hospital reporting period ends before it begins")
    return {
        "report_id": raw["rpt_rec_num"], "ccn": ccn, "form": reader.form,
        "cohort_year": reader.year, "report_status_raw": raw["rpt_stus_cd"],
        "transmittal_raw": raw["trnsmtl_num"], "fiscal_begin": begin, "fiscal_end": end,
        "period_days": days,
        "period_status": "unknown" if days is None else "short" if days < 365 else "annual" if days <= 366 else "long",
        "cohort_status": "unknown_begin" if begin is None else "in_cohort" if begin.year == reader.year else "outside_declared_year",
        "selected_report_policy": POLICY,
        "raw_report_json": json.dumps(raw, sort_keys=True, separators=(",", ":")), **locator(item),
    }


def selected_measures(names):
    names = tuple(names)
    if not names or len(set(names)) != len(names) or set(names) - MEASURES.keys():
        raise ValueError("Unsupported or repeated hospital measure; select an explicit supported set")
    return {name: MEASURES[name] for name in names}


def build_hospital_cost_reports(reader, output_dir: Path, *, measures=tuple(MEASURES), validate_ccn=None):
    """Write native observations before selected measures; never combine alternate reports.

    Only small report and selected-cell maps are retained. Worksheet rows stream
    through the maintained atomic Parquet writer. Missing cells get an explicit
    absent status; literal blanks and numeric zero retain distinct states.
    """
    definitions = selected_measures(measures)
    if validate_ccn is None:
        try:
            from refspec.registry.nppes_npi_identifiers import validate_ccn
        except ImportError as error:
            raise RuntimeError("Hospital builds require RefSpec's CCN validator on the Python path") from error
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    reports = {}
    for item in reader.rows("rpt"):
        row = report(item, reader, validate_ccn)
        if row["report_id"] in reports:
            raise ValueError("Repeated hospital report identity")
        reports[row["report_id"]] = row
    files = [write_rows(reports.values(), output_dir / f"{REPORTS}.parquet", REPORT_SCHEMA)]
    by_coordinate = {value[:3]: name for name, value in definitions.items()}
    chosen = {}

    def cells():
        for kind in ("nmrc", "alphnmrc"):
            for item in reader.rows(kind):
                raw = item["raw"]
                identity = raw["rpt_rec_num"]
                if identity not in reports:
                    raise ValueError("Hospital worksheet references an absent report")
                coordinate = (raw["wksht_cd"], raw["line_num"], raw["clmn_num"])
                row = {
                    "report_id": identity, "cell_kind": kind, "worksheet": coordinate[0],
                    "line": coordinate[1], "column": coordinate[2], "value_raw": raw["value_raw"], **locator(item),
                }
                if kind == "nmrc" and coordinate in by_coordinate:
                    key = (identity, by_coordinate[coordinate])
                    if key in chosen:
                        raise ValueError("Ambiguous repeated hospital measurement cell")
                    chosen[key] = row
                yield row
    files.append(write_rows(cells(), output_dir / f"{CELLS}.parquet", CELL_SCHEMA))
    reader.verify_unchanged()

    def measurements():
        for identity, parent in reports.items():
            for name, (worksheet, line, column, unit, scope) in definitions.items():
                cell = chosen.get((identity, name))
                raw = None if cell is None else cell["value_raw"]
                value, status = (None, "absent_cell") if cell is None else measurement(raw)
                # Numeric suppression/overflow is explicit, never converted to zero.
                if name == "hospital_beds" and value is not None and (value < 0 or value != value.to_integral()):
                    value, status = None, "invalid_bed_count"
                yield {
                    "report_id": identity, "ccn": parent["ccn"], "fiscal_begin": parent["fiscal_begin"],
                    "fiscal_end": parent["fiscal_end"], "measure": name, "unit": unit, "provider_scope": scope,
                    "worksheet": worksheet, "line": line, "column": column, "value_raw": raw, "value": value,
                    "value_status": status,
                    **({field: None for field, _ in LOCATOR_FIELDS} if cell is None else {field: cell[field] for field, _ in LOCATOR_FIELDS}),
                    "source_archive_sha256": reader.sha256,
                }
    files.append(write_rows(measurements(), output_dir / f"{MEASUREMENTS}.parquet", MEASURE_SCHEMA))
    # Verify that the writing path consumed the complete cell iterator.
    return files, {path.stem: pq.ParquetFile(path).metadata.num_rows for path in files}
