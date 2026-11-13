"""Interpret a declared Form 990 XML cohort without merging organizations or amended returns."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

# Qualify each version against retained publisher XML before extending this set.
SUPPORTED_VERSIONS = frozenset({"2021v4.2"})
MONEY = pa.decimal128(38, 0)
FILINGS = "irs990_filings"
OFFICERS = "irs990_officers"
GRANTS = "irs990_grants"


def _schema(strings, typed=()):
    return pa.schema([(name, pa.string()) for name in strings.split()] + list(typed))


SCHEMAS = {
    FILINGS: _schema(
        """filing_id ein organization_name return_id tax_period return_type return_version
        tax_year tax_period_begin tax_period_end amended_raw amended_status
        revenue_raw expenses_raw grants_paid_raw revenue_status expenses_status grants_paid_status
        revenue_reconciliation expenses_reconciliation schedule_i_status
        index_native_json source_locator_json native_xml_json""",
        [
            ("submission_year", pa.int32()),
            ("revenue_usd", MONEY),
            ("expenses_usd", MONEY),
            ("grants_paid_usd", MONEY),
        ],
    ),
    OFFICERS: _schema(
        """filing_id ein source_xpath person_name title compensation_raw
        compensation_status related_compensation_raw related_compensation_status other_compensation_raw
        other_compensation_status native_entry_json source_locator_json""",
        [
            ("entry_ordinal", pa.int32()),
            ("compensation_usd", MONEY),
            ("related_compensation_usd", MONEY),
            ("other_compensation_usd", MONEY),
        ],
    ),
    GRANTS: _schema(
        """filing_id ein source_xpath recipient_ein recipient_name purpose
        cash_raw cash_status noncash_raw noncash_status native_entry_json source_locator_json""",
        [
            ("entry_ordinal", pa.int32()),
            ("cash_usd", MONEY),
            ("noncash_usd", MONEY),
        ],
    ),
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _child(node, name):
    from spicy_docs.sources.irs_form990 import child

    return child(node, name) if node is not None else None


def _text(node, *path):
    from spicy_docs.sources.irs_form990 import text_at

    return text_at(node, *path) if node is not None else None


def _name(node):
    if node is None:
        return None
    lines = [_text(node, "BusinessNameLine1Txt"), _text(node, "BusinessNameLine2Txt")]
    return (
        " ".join(value for value in lines if value is not None) if any(value is not None for value in lines) else None
    )


def _amount(node, *path):
    for name in path:
        node = _child(node, name)
    if node is None:
        return None, None, "absent"
    raw = node["text"]
    if raw is None or raw == "":
        return raw, None, "empty"
    if re.fullmatch(r"[+-]?\d{1,38}", raw) is None:
        raise ValueError("IRS amount is not an exact whole US dollar value")
    return raw, Decimal(raw), "exact"


def _money(row, prefix, node, *path):
    raw, amount, status = _amount(node, *path)
    row[prefix + "_raw"], row[prefix + "_usd"], row[prefix + "_status"] = raw, amount, status
    return amount


def _reconcile(summary, detail):
    if summary is None or detail is None:
        return "missing_field"
    return "matched" if summary == detail else "different"


def interpret(record):
    """Return filing, officer and Schedule I domestic-organization grant observations.

    Amounts are reported whole US dollars, never scaled or converted. Missing
    values remain NULL with an explicit status. Absent schedules establish no
    claim that an organization paid no grants.
    """
    index, root = record["native_index"], record["native_xml"]
    if index["RETURN_TYPE"] != "990" or record["return_version"] not in SUPPORTED_VERSIONS:
        raise ValueError("Unsupported IRS form/version cohort; native acquisition does not qualify interpretation")
    header, data = _child(root, "ReturnHeader"), _child(root, "ReturnData")
    form = _child(data, "IRS990")
    amended_node = _child(form, "AmendedReturnInd")
    amended = amended_node["text"] if amended_node is not None else None
    if amended not in {None, "", "true", "false", "1", "0", "X"}:
        raise ValueError("IRS amended-return indicator changed")
    locator = _json(record["source_locator"])
    row = {
        "filing_id": record["object_id"],
        "ein": index["EIN"],
        "organization_name": index["TAXPAYER_NAME"],
        "return_id": index["RETURN_ID"],
        "tax_period": index["TAX_PERIOD"],
        "submission_year": record["submission_year"],
        "return_type": index["RETURN_TYPE"],
        "return_version": record["return_version"],
        "tax_year": _text(header, "TaxYr"),
        "tax_period_begin": _text(header, "TaxPeriodBeginDt"),
        "tax_period_end": _text(header, "TaxPeriodEndDt"),
        "amended_raw": amended,
        "amended_status": "absent" if amended_node is None else "empty" if amended in {None, ""} else "reported",
        "index_native_json": _json(index),
        "source_locator_json": locator,
        "native_xml_json": _json(root),
    }
    revenue = _money(row, "revenue", form, "CYTotalRevenueAmt")
    expenses = _money(row, "expenses", form, "CYTotalExpensesAmt")
    _money(row, "grants_paid", form, "CYGrantsAndSimilarPaidAmt")
    row["revenue_reconciliation"] = _reconcile(revenue, _amount(form, "TotalRevenueGrp", "TotalRevenueColumnAmt")[1])
    row["expenses_reconciliation"] = _reconcile(expenses, _amount(form, "TotalFunctionalExpensesGrp", "TotalAmt")[1])
    officers, grants = [], []
    ns = "{http://www.irs.gov/efile}"
    for node in form["children"]:
        if node["tag"] != ns + "Form990PartVIISectionAGrp":
            continue
        officer = {
            "filing_id": row["filing_id"],
            "ein": row["ein"],
            "entry_ordinal": len(officers) + 1,
            "source_xpath": node["source_xpath"],
            "person_name": _text(node, "PersonNm"),
            "title": _text(node, "TitleTxt"),
            "native_entry_json": _json(node),
            "source_locator_json": locator,
        }
        for prefix, field in [
            ("compensation", "ReportableCompFromOrgAmt"),
            ("related_compensation", "ReportableCompFromRltdOrgAmt"),
            ("other_compensation", "OtherCompensationAmt"),
        ]:
            _money(officer, prefix, node, field)
        officers.append(officer)
    schedule = _child(data, "IRS990ScheduleI")
    row["schedule_i_status"] = "absent" if schedule is None else "present"
    for node in schedule["children"] if schedule is not None else []:
        if node["tag"] != ns + "RecipientTable":
            continue
        grant = {
            "filing_id": row["filing_id"],
            "ein": row["ein"],
            "entry_ordinal": len(grants) + 1,
            "source_xpath": node["source_xpath"],
            "recipient_ein": _text(node, "RecipientEIN"),
            "recipient_name": _name(_child(node, "RecipientBusinessName")),
            "purpose": _text(node, "PurposeOfGrantTxt"),
            "native_entry_json": _json(node),
            "source_locator_json": locator,
        }
        _money(grant, "cash", node, "CashGrantAmt")
        _money(grant, "noncash", node, "NonCashAssistanceAmt")
        grants.append(grant)
    return row, officers, grants


def build_irs_form990(records, output_dir: Path):
    """Validate all selected filings before replacing any maintained table output."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any((output_dir / (name + ".parquet")).exists() for name in SCHEMAS):
        raise ValueError("Use fresh output paths for each IRS generation")
    rows = {name: [] for name in SCHEMAS}
    seen = set()
    for record in records:
        filing, officers, grants = interpret(record)
        if filing["filing_id"] in seen:
            raise ValueError("Duplicate IRS filing identity in selected slice")
        seen.add(filing["filing_id"])
        rows[FILINGS].append(filing)
        rows[OFFICERS].extend(officers)
        rows[GRANTS].extend(grants)
    if not seen:
        raise ValueError("IRS filing slice must contain an explicitly selected return")
    with TemporaryDirectory(dir=output_dir) as scratch:
        built = [
            write_rows(rows[name], Path(scratch) / (name + ".parquet"), schema) for name, schema in SCHEMAS.items()
        ]
        outputs = [(path, output_dir / path.name) for path in built]
        try:
            for source, destination in outputs:
                destination.hardlink_to(source)
        except BaseException:
            for source, destination in outputs:
                try:
                    if not destination.is_symlink() and destination.samefile(source):
                        destination.unlink()
                except FileNotFoundError:
                    pass
            raise
    return [destination for _, destination in outputs]
