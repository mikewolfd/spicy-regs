"""Interpret qualified IRS Form 990 XML cohorts into submission_year-partitioned Parquet tables."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
import gc
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.parquet_rows import write_rows

MONEY = pa.decimal128(38, 0)
FILINGS = "irs990_filings"
OFFICERS = "irs990_officers"
GRANTS = "irs990_grants"
PARTITION_COLUMNS = ("submission_year",)
PARTITIONED_TABLES: dict[str, tuple[str, ...]] = {
    f"{name}.parquet": PARTITION_COLUMNS for name in (FILINGS, OFFICERS, GRANTS)
}

DEFAULT_BATCH_SIZE = 10_000
DEFAULT_MAX_QUARANTINED_ROWS = 500
DEFAULT_MAX_QUARANTINED_RATIO = 0.001

_VERSION_FAMILY_RE = re.compile(r"^(20\d{2}v\d+)\.\d+$", re.ASCII)
_WHOLE_DOLLAR_RE = re.compile(r"[+-]?\d{1,38}", re.ASCII)
_VALID_AMENDED_VALUES = frozenset({None, "", "true", "false", "1", "0", "X"})
_VALID_DOCUMENT_CNT_STATUS = frozenset({"matched", "redacted_fewer", "absent"})
_FATAL_ARCHIVE_ANOMALIES = frozenset({"unsafe_member_path", "encrypted_member"})


@dataclass(frozen=True, slots=True)
class VersionSpec:
    """Visually qualified IRS Form 990 XML schema cohort."""

    family: str
    profile: str
    visually_reviewed: tuple[str, ...]
    notes: str


# Qualify each major schema family against rendered original publisher XML before extending this registry.
# Pre-2020 (2018v3.*, 2019v4.*) and future (2025v5.*, 2026v*) cohorts stay unqualified until visual review.
QUALIFIED_VERSION_SPECS: dict[str, VersionSpec] = {
    "2020v4": VersionSpec(
        family="2020v4",
        profile="mef-990-standard-v1",
        visually_reviewed=("2020v4.0", "2020v4.1"),
        notes="Visually reviewed 2026-11-28 (2020v4.0, 2020v4.1; 2020v4.2 audited 123/123 identical tags).",
    ),
    "2021v4": VersionSpec(
        family="2021v4",
        profile="mef-990-standard-v1",
        visually_reviewed=("2021v4.1", "2021v4.2"),
        notes="Visually reviewed 2026-11-13 & 2026-11-28 (2021v4.1, 2021v4.2; 2021v4.0 audited 3,421/3,421 identical).",
    ),
    "2022v4": VersionSpec(
        family="2022v4",
        profile="mef-990-standard-v1",
        visually_reviewed=("2022v4.1",),
        notes="Visually reviewed 2026-11-28 (2022v4.1).",
    ),
    "2022v5": VersionSpec(
        family="2022v5",
        profile="mef-990-standard-v1",
        visually_reviewed=("2022v5.0",),
        notes="Visually reviewed 2026-11-28 (2022v5.0 from official 2025_TEOS_XML_01A.zip).",
    ),
    "2023v5": VersionSpec(
        family="2023v5",
        profile="mef-990-standard-v1",
        visually_reviewed=("2023v5.0", "2023v5.1"),
        notes="Visually reviewed 2026-11-28 (2023v5.0, 2023v5.1 from official 2025_TEOS_XML_01A.zip).",
    ),
    "2023v6": VersionSpec(
        family="2023v6",
        profile="mef-990-standard-v1",
        visually_reviewed=("2023v6.0",),
        notes="Visually reviewed 2026-11-28 (2023v6.0, dominant 2023 schema in 2025_TEOS_XML_01A.zip).",
    ),
    "2024v5": VersionSpec(
        family="2024v5",
        profile="mef-990-standard-v1",
        visually_reviewed=("2024v5.0", "2024v5.1", "2024v5.2", "2024v5.5"),
        notes="Visually reviewed 2026-11-28 (2024v5.0, 2024v5.1, 2024v5.2, 2024v5.5).",
    ),
    "2025v4": VersionSpec(
        family="2025v4",
        profile="mef-990-standard-v1",
        visually_reviewed=("2025v4.0",),
        notes="Visually reviewed 2026-11-28 (2025v4.0 from official 2026_TEOS_XML_01A.zip).",
    ),
}
SUPPORTED_VERSION_FAMILIES = frozenset(QUALIFIED_VERSION_SPECS)


class UnsupportedReturnVersionError(ValueError):
    """Raised when an IRS Form 990 XML returnVersion is outside QUALIFIED_VERSION_SPECS."""


def resolve_version_spec(return_version: str | None) -> VersionSpec | None:
    """Return the visually qualified VersionSpec for ``return_version``, or ``None`` if unqualified."""
    if not isinstance(return_version, str):
        return None
    match = _VERSION_FAMILY_RE.fullmatch(return_version)
    if match is None:
        return None
    return QUALIFIED_VERSION_SPECS.get(match.group(1))


def is_qualified_version(return_version: str | None) -> bool:
    """Whether ``return_version`` belongs to a visually qualified Form 990 schema family."""
    return resolve_version_spec(return_version) is not None


def _schema(strings: str, typed: Sequence[tuple[str, pa.DataType]] = ()) -> pa.Schema:
    return pa.schema([(name, pa.string()) for name in strings.split()] + list(typed))


SCHEMAS: dict[str, pa.Schema] = {
    FILINGS: _schema(
        """filing_id ein organization_name return_id tax_period return_type return_version
        tax_year tax_period_begin tax_period_end amended_raw amended_status document_cnt_status
        revenue_raw expenses_raw grants_paid_raw revenue_status expenses_status grants_paid_status
        revenue_reconciliation expenses_reconciliation schedule_i_status
        index_native_json source_locator_json""",
        [
            ("submission_year", pa.int32()),
            ("revenue_usd", MONEY),
            ("expenses_usd", MONEY),
            ("grants_paid_usd", MONEY),
        ],
    ),
    OFFICERS: _schema(
        """filing_id ein source_xpath person_name business_name title compensation_raw
        compensation_status related_compensation_raw related_compensation_status other_compensation_raw
        other_compensation_status native_entry_json source_locator_json""",
        [
            ("submission_year", pa.int32()),
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
            ("submission_year", pa.int32()),
            ("entry_ordinal", pa.int32()),
            ("cash_usd", MONEY),
            ("noncash_usd", MONEY),
        ],
    ),
}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _child(node: Mapping[str, Any] | None, name: str) -> dict[str, Any] | None:
    from spicy_docs.sources.irs_form990 import child

    return child(node, name) if node is not None else None


def _text(node: Mapping[str, Any] | None, *path: str) -> str | None:
    from spicy_docs.sources.irs_form990 import text_at

    return text_at(node, *path) if node is not None else None


def _name(node: Mapping[str, Any] | None) -> str | None:
    if node is None:
        return None
    lines = [_text(node, "BusinessNameLine1Txt"), _text(node, "BusinessNameLine2Txt")]
    return (
        " ".join(value for value in lines if value is not None) if any(value is not None for value in lines) else None
    )


def _parse_whole_dollar(raw: str | None) -> tuple[str | None, Decimal | None, str]:
    if raw is None or raw == "":
        return raw, None, "empty"
    if _WHOLE_DOLLAR_RE.fullmatch(raw) is None:
        raise ValueError("IRS amount is not an exact whole US dollar value")
    return raw, Decimal(raw), "exact"


def _amount_from_node(node: Mapping[str, Any] | None, *path: str) -> tuple[str | None, Decimal | None, str]:
    for name in path:
        node = _child(node, name)
    if node is None:
        return None, None, "absent"
    return _parse_whole_dollar(node["text"])


def _amount_from_map(fields: Mapping[str, str | None], key: str) -> tuple[str | None, Decimal | None, str]:
    if key not in fields:
        return None, None, "absent"
    return _parse_whole_dollar(fields[key])


def _set_money(row: dict[str, Any], prefix: str, raw: str | None, amount: Decimal | None, status: str) -> Decimal | None:
    row[prefix + "_raw"], row[prefix + "_usd"], row[prefix + "_status"] = raw, amount, status
    return amount


def _money_from_node(row: dict[str, Any], prefix: str, node: Mapping[str, Any] | None, *path: str) -> Decimal | None:
    raw, amount, status = _amount_from_node(node, *path)
    return _set_money(row, prefix, raw, amount, status)


def _money_from_map(row: dict[str, Any], prefix: str, fields: Mapping[str, str | None], key: str) -> Decimal | None:
    raw, amount, status = _amount_from_map(fields, key)
    return _set_money(row, prefix, raw, amount, status)


def _reconcile(summary: Decimal | None, detail: Decimal | None) -> str:
    if summary is None or detail is None:
        return "missing_field"
    return "matched" if summary == detail else "different"


def _legacy_document_cnt_status(data: Mapping[str, Any] | None) -> str:
    if data is None:
        return "absent"
    declared = data.get("attributes", {}).get("documentCnt")
    if declared is None:
        return "absent"
    if not (isinstance(declared, str) and declared.isascii() and declared.isdigit()):
        raise ValueError("IRS declared return document count is malformed")
    observed = len(data.get("children", ()))
    declared_int = int(declared)
    if observed > declared_int:
        raise ValueError("IRS observed return document count exceeds declared documentCnt")
    return "matched" if observed == declared_int else "redacted_fewer"


def interpret(record: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Return filing, officer and Schedule I domestic-organization grant observations.

    Accepts both streaming ``scan_form990_return()`` dicts (preferred, zero DOM
    retention) and legacy ``parse_return()`` dicts. Amounts are reported whole US
    dollars, never scaled or converted. Missing values remain NULL with an explicit
    status. Absent schedules establish no claim that an organization paid no grants.
    """
    index = record["native_index"]
    if index["RETURN_TYPE"] != "990":
        raise ValueError(f"Unsupported IRS form type for Form 990 interpretation: {index['RETURN_TYPE']!r}")
    return_version = record.get("return_version")
    if resolve_version_spec(return_version) is None:
        raise UnsupportedReturnVersionError(
            f"Unsupported IRS Form 990 returnVersion {return_version!r}; visual qualification required"
        )

    submission_year = record["submission_year"]
    locator = _json(record["source_locator"])

    if "form_fields" in record:
        header_fields = record["header"]
        form_fields = record["form_fields"]
        tax_year = header_fields.get("TaxYr")
        tax_period_begin = header_fields.get("TaxPeriodBeginDt")
        tax_period_end = header_fields.get("TaxPeriodEndDt")
        if "AmendedReturnInd" not in form_fields:
            amended = None
            amended_status = "absent"
        else:
            amended = form_fields["AmendedReturnInd"]
            if amended not in _VALID_AMENDED_VALUES:
                raise ValueError("IRS amended-return indicator changed")
            amended_status = "empty" if amended in {None, ""} else "reported"
        document_cnt_status = record.get("document_cnt_status", "absent")
        if document_cnt_status not in _VALID_DOCUMENT_CNT_STATUS:
            raise ValueError(f"Unexpected IRS document_cnt_status: {document_cnt_status!r}")

        row: dict[str, Any] = {
            "filing_id": record["object_id"],
            "ein": index["EIN"],
            "organization_name": index["TAXPAYER_NAME"],
            "return_id": index["RETURN_ID"],
            "tax_period": index["TAX_PERIOD"],
            "submission_year": submission_year,
            "return_type": index["RETURN_TYPE"],
            "return_version": return_version,
            "tax_year": tax_year,
            "tax_period_begin": tax_period_begin,
            "tax_period_end": tax_period_end,
            "amended_raw": amended,
            "amended_status": amended_status,
            "document_cnt_status": document_cnt_status,
            "index_native_json": _json(index),
            "source_locator_json": locator,
        }
        revenue = _money_from_map(row, "revenue", form_fields, "CYTotalRevenueAmt")
        expenses = _money_from_map(row, "expenses", form_fields, "CYTotalExpensesAmt")
        _money_from_map(row, "grants_paid", form_fields, "CYGrantsAndSimilarPaidAmt")
        row["revenue_reconciliation"] = _reconcile(
            revenue, _amount_from_map(form_fields, "TotalRevenueGrp/TotalRevenueColumnAmt")[1]
        )
        row["expenses_reconciliation"] = _reconcile(
            expenses, _amount_from_map(form_fields, "TotalFunctionalExpensesGrp/TotalAmt")[1]
        )
        row["schedule_i_status"] = "present" if record.get("schedule_i_present") else "absent"
        part_vii_nodes: Sequence[Mapping[str, Any]] = record.get("part_vii_entries", ())
        schedule_i_nodes: Sequence[Mapping[str, Any]] = record.get("schedule_i_grants", ())
    else:
        root = record["native_xml"]
        header, data = _child(root, "ReturnHeader"), _child(root, "ReturnData")
        form = _child(data, "IRS990")
        if form is None:
            raise ValueError("IRS XML is missing IRS990 body")
        amended_node = _child(form, "AmendedReturnInd")
        amended = amended_node["text"] if amended_node is not None else None
        if amended not in _VALID_AMENDED_VALUES:
            raise ValueError("IRS amended-return indicator changed")
        document_cnt_status = record.get("document_cnt_status") or _legacy_document_cnt_status(data)
        if document_cnt_status not in _VALID_DOCUMENT_CNT_STATUS:
            raise ValueError(f"Unexpected IRS document_cnt_status: {document_cnt_status!r}")
        row = {
            "filing_id": record["object_id"],
            "ein": index["EIN"],
            "organization_name": index["TAXPAYER_NAME"],
            "return_id": index["RETURN_ID"],
            "tax_period": index["TAX_PERIOD"],
            "submission_year": submission_year,
            "return_type": index["RETURN_TYPE"],
            "return_version": return_version,
            "tax_year": _text(header, "TaxYr"),
            "tax_period_begin": _text(header, "TaxPeriodBeginDt"),
            "tax_period_end": _text(header, "TaxPeriodEndDt"),
            "amended_raw": amended,
            "amended_status": "absent" if amended_node is None else "empty" if amended in {None, ""} else "reported",
            "document_cnt_status": document_cnt_status,
            "index_native_json": _json(index),
            "source_locator_json": locator,
        }
        revenue = _money_from_node(row, "revenue", form, "CYTotalRevenueAmt")
        expenses = _money_from_node(row, "expenses", form, "CYTotalExpensesAmt")
        _money_from_node(row, "grants_paid", form, "CYGrantsAndSimilarPaidAmt")
        row["revenue_reconciliation"] = _reconcile(
            revenue, _amount_from_node(form, "TotalRevenueGrp", "TotalRevenueColumnAmt")[1]
        )
        row["expenses_reconciliation"] = _reconcile(
            expenses, _amount_from_node(form, "TotalFunctionalExpensesGrp", "TotalAmt")[1]
        )
        ns = "{http://www.irs.gov/efile}"
        part_vii_nodes = [node for node in form["children"] if node["tag"] == ns + "Form990PartVIISectionAGrp"]
        schedule = _child(data, "IRS990ScheduleI")
        row["schedule_i_status"] = "absent" if schedule is None else "present"
        schedule_i_nodes = (
            [node for node in schedule["children"] if node["tag"] == ns + "RecipientTable"]
            if schedule is not None
            else []
        )

    officers: list[dict[str, Any]] = []
    for node in part_vii_nodes:
        officer: dict[str, Any] = {
            "filing_id": row["filing_id"],
            "ein": row["ein"],
            "submission_year": submission_year,
            "entry_ordinal": len(officers) + 1,
            "source_xpath": node["source_xpath"],
            "person_name": _text(node, "PersonNm"),
            "business_name": _name(_child(node, "BusinessName")),
            "title": _text(node, "TitleTxt"),
            "native_entry_json": _json(node),
            "source_locator_json": locator,
        }
        for prefix, field_name in (
            ("compensation", "ReportableCompFromOrgAmt"),
            ("related_compensation", "ReportableCompFromRltdOrgAmt"),
            ("other_compensation", "OtherCompensationAmt"),
        ):
            _money_from_node(officer, prefix, node, field_name)
        officers.append(officer)

    grants: list[dict[str, Any]] = []
    for node in schedule_i_nodes:
        grant: dict[str, Any] = {
            "filing_id": row["filing_id"],
            "ein": row["ein"],
            "submission_year": submission_year,
            "entry_ordinal": len(grants) + 1,
            "source_xpath": node["source_xpath"],
            "recipient_ein": _text(node, "RecipientEIN"),
            "recipient_name": _name(_child(node, "RecipientBusinessName")),
            "purpose": _text(node, "PurposeOfGrantTxt"),
            "native_entry_json": _json(node),
            "source_locator_json": locator,
        }
        _money_from_node(grant, "cash", node, "CashGrantAmt")
        _money_from_node(grant, "noncash", node, "NonCashAssistanceAmt")
        grants.append(grant)

    return row, officers, grants


def build_irs_form990_year(
    submission_year: int,
    archive_iter: Iterable[Any],
    output_dir: Path,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_quarantined_rows: int = DEFAULT_MAX_QUARANTINED_ROWS,
    max_quarantined_ratio: float = DEFAULT_MAX_QUARANTINED_RATIO,
    strict_versions: bool = False,
    index_summary: Mapping[str, Any] | None = None,
    archive_dispositions: Sequence[Mapping[str, Any]] | None = None,
    summary_out: Path | None = None,
    quarantined_out: Path | None = None,
) -> dict[str, Any]:
    """Stream one submission year of Form 990 returns directly into partitioned Zstd Parquet files.

    Writes ``<output_dir>/<table>/submission_year=<YYYY>/part-000000.parquet`` atomically
    through a sibling temporary directory. Deduplicates byte-identical reposted ``OBJECT_ID``
    members within the year, quarantines conflicting-bytes duplicates and bounded anomalies
    (including unqualified ``returnVersion`` values up to ``max_quarantined_ratio``), and
    never collapses distinct-``OBJECT_ID`` amended filings.
    """
    from spicy_docs.sources.irs_form990 import IRS990Anomaly, IRS990ParsedReturn, IRS990SkippedMember

    if type(submission_year) is not int or not 2011 <= submission_year <= 2099:
        raise ValueError(f"Invalid submission_year: {submission_year!r}")
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if type(max_quarantined_rows) is not int or max_quarantined_rows < 0:
        raise ValueError("max_quarantined_rows must be a non-negative integer")
    if (
        not isinstance(max_quarantined_ratio, (int, float))
        or isinstance(max_quarantined_ratio, bool)
        or not 0.0 <= max_quarantined_ratio <= 1.0
    ):
        raise ValueError("max_quarantined_ratio must be between 0.0 and 1.0")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destinations = {
        name: output_dir / name / f"submission_year={submission_year}" / "part-000000.parquet"
        for name in SCHEMAS
    }
    if any(dest.exists() for dest in destinations.values()):
        raise ValueError(f"Use fresh output paths for IRS Form 990 submission_year={submission_year}")

    seen_oids: dict[str, str] = {}
    duplicate_identical_oids = 0
    quarantined: list[dict[str, Any]] = []
    quarantined_by_reason: Counter[str] = Counter()
    excluded_by_return_type: Counter[str] = Counter()
    skipped_by_reason: Counter[str] = Counter()
    return_version_counts: Counter[str] = Counter()
    document_cnt_status_counts: Counter[str] = Counter()
    amended_status_counts: Counter[str] = Counter()
    schedule_i_status_counts: Counter[str] = Counter()
    revenue_reconciliation_counts: Counter[str] = Counter()
    expenses_reconciliation_counts: Counter[str] = Counter()
    row_counts: dict[str, int] = {name: 0 for name in SCHEMAS}

    def record_quarantine(entry: dict[str, Any]) -> None:
        reason = str(entry["reason"])
        if len(quarantined) >= max_quarantined_rows:
            raise ValueError(
                f"IRS Form 990 submission_year={submission_year} quarantined anomaly count exceeded "
                f"max_quarantined_rows={max_quarantined_rows} ({reason}: {entry.get('detail')})"
            )
        quarantined.append(entry)
        quarantined_by_reason[reason] += 1

    with TemporaryDirectory(dir=output_dir, prefix=f".irs990-{submission_year}-") as scratch:
        scratch_dir = Path(scratch)
        scratch_files = {name: scratch_dir / f"{name}.parquet" for name in SCHEMAS}
        writers = {
            name: pq.ParquetWriter(
                scratch_files[name],
                schema,
                compression="zstd",
                use_dictionary=[
                    col for col in schema.names if not col.endswith("_json") and col != "source_xpath"
                ],
            )
            for name, schema in SCHEMAS.items()
        }
        buffers: dict[str, dict[str, list[Any]]] = {
            name: {col: [] for col in schema.names} for name, schema in SCHEMAS.items()
        }
        buffered_counts: dict[str, int] = {name: 0 for name in SCHEMAS}
        try:
            def flush(name: str) -> None:
                if buffered_counts[name]:
                    table = pa.Table.from_pydict(buffers[name], schema=SCHEMAS[name])
                    for values in buffers[name].values():
                        values.clear()
                    buffered_counts[name] = 0
                    writers[name].write_table(table, row_group_size=batch_size)
                    del table
                    gc.collect()
                    pa.default_memory_pool().release_unused()

            def append_row(name: str, row_dict: Mapping[str, Any]) -> None:
                cols = buffers[name]
                for col, values in cols.items():
                    values.append(row_dict[col])
                buffered_counts[name] += 1
                if buffered_counts[name] >= batch_size:
                    flush(name)

            for item in archive_iter:
                if isinstance(item, IRS990SkippedMember):
                    skipped_by_reason[item.reason] += 1
                    if item.reason == "excluded_return_type" and item.return_type:
                        excluded_by_return_type[item.return_type] += 1
                    continue

                if isinstance(item, IRS990Anomaly):
                    if item.reason in _FATAL_ARCHIVE_ANOMALIES:
                        raise ValueError(
                            f"Fatal IRS Form 990 archive anomaly ({item.reason}) on {item.member_name}: {item.detail}"
                        )
                    record_quarantine({
                        "submission_year": submission_year,
                        "reason": item.reason,
                        "detail": item.detail,
                        "object_id": item.object_id,
                        "member_name": item.member_name,
                        "return_type": item.return_type,
                        "archive_sha256": item.archive_sha256,
                        "member_sha256": item.member_sha256,
                    })
                    continue

                record = item.record if isinstance(item, IRS990ParsedReturn) else item
                if not isinstance(record, Mapping):
                    raise TypeError(f"Unexpected item in IRS Form 990 archive stream: {type(item).__name__}")

                if record.get("submission_year") != submission_year:
                    raise ValueError(
                        f"IRS Form 990 record submission_year={record.get('submission_year')!r} "
                        f"differs from target partition submission_year={submission_year}"
                    )

                oid = str(record["object_id"])
                locator = record.get("source_locator") or {}
                member_sha = locator.get("member_sha256")
                archive_sha = locator.get("archive_sha256")
                member_name = locator.get("member_name")

                if oid in seen_oids:
                    if member_sha and seen_oids[oid] == member_sha:
                        duplicate_identical_oids += 1
                        continue
                    record_quarantine({
                        "submission_year": submission_year,
                        "reason": "conflicting_duplicate_oid",
                        "detail": "Duplicate OBJECT_ID with differing XML bytes within submission_year",
                        "object_id": oid,
                        "member_name": member_name,
                        "archive_sha256": archive_sha,
                        "member_sha256": member_sha,
                        "existing_member_sha256": seen_oids[oid],
                    })
                    continue

                try:
                    filing, officers, grants = interpret(record)
                except UnsupportedReturnVersionError as error:
                    if strict_versions:
                        raise
                    record_quarantine({
                        "submission_year": submission_year,
                        "reason": "unsupported_return_version",
                        "detail": str(error),
                        "object_id": oid,
                        "return_version": record.get("return_version"),
                        "member_name": member_name,
                        "archive_sha256": archive_sha,
                        "member_sha256": member_sha,
                    })
                    continue
                except ValueError as error:
                    record_quarantine({
                        "submission_year": submission_year,
                        "reason": "interpret_error",
                        "detail": str(error),
                        "object_id": oid,
                        "return_version": record.get("return_version"),
                        "member_name": member_name,
                        "archive_sha256": archive_sha,
                        "member_sha256": member_sha,
                    })
                    continue

                seen_oids[oid] = member_sha or ""
                append_row(FILINGS, filing)
                for officer in officers:
                    append_row(OFFICERS, officer)
                for grant in grants:
                    append_row(GRANTS, grant)
                row_counts[FILINGS] += 1
                row_counts[OFFICERS] += len(officers)
                row_counts[GRANTS] += len(grants)
                return_version_counts[filing["return_version"]] += 1
                document_cnt_status_counts[filing["document_cnt_status"]] += 1
                amended_status_counts[filing["amended_status"]] += 1
                schedule_i_status_counts[filing["schedule_i_status"]] += 1
                revenue_reconciliation_counts[filing["revenue_reconciliation"]] += 1
                expenses_reconciliation_counts[filing["expenses_reconciliation"]] += 1

            if row_counts[FILINGS] == 0:
                raise ValueError(
                    f"IRS Form 990 submission_year={submission_year} produced zero qualified Form 990 filings "
                    f"(quarantined={len(quarantined)})"
                )
            total_evaluated = row_counts[FILINGS] + len(quarantined)
            if quarantined and (len(quarantined) / total_evaluated) > max_quarantined_ratio:
                raise ValueError(
                    f"IRS Form 990 submission_year={submission_year} quarantined anomaly ratio "
                    f"({len(quarantined)}/{total_evaluated}) exceeded max_quarantined_ratio={max_quarantined_ratio}; "
                    f"reasons={dict(sorted(quarantined_by_reason.items()))}"
                )

            for name in SCHEMAS:
                flush(name)
        finally:
            for writer in writers.values():
                writer.close()

        # Merge excluded return-type counts reported by ArchiveDisposition summaries when yielded skips were off.
        disposition_excluded: Counter[str] = Counter()
        disposition_list = [dict(d) for d in (archive_dispositions or ())]
        for disp in disposition_list:
            for rtype, count in (disp.get("excluded_by_return_type") or {}).items():
                disposition_excluded[str(rtype)] += int(count)
        merged_excluded = dict(sorted((disposition_excluded or excluded_by_return_type).items()))

        summary: dict[str, Any] = {
            "format": "spicy-irs-form990-year-summary",
            "version": 1,
            "submission_year": submission_year,
            "rows": dict(row_counts),
            "return_version_counts": dict(sorted(return_version_counts.items())),
            "document_cnt_status_counts": dict(sorted(document_cnt_status_counts.items())),
            "amended_status_counts": dict(sorted(amended_status_counts.items())),
            "schedule_i_status_counts": dict(sorted(schedule_i_status_counts.items())),
            "revenue_reconciliation_counts": dict(sorted(revenue_reconciliation_counts.items())),
            "expenses_reconciliation_counts": dict(sorted(expenses_reconciliation_counts.items())),
            "excluded_by_return_type": merged_excluded,
            "duplicate_identical_oids": duplicate_identical_oids,
            "quarantined_count": len(quarantined),
            "quarantined_by_reason": dict(sorted(quarantined_by_reason.items())),
            "partition_files": {
                name: dest.relative_to(output_dir).as_posix() for name, dest in destinations.items()
            },
        }
        if index_summary is not None:
            summary["index"] = dict(index_summary)
        if disposition_list:
            summary["archives"] = disposition_list

        moved: list[Path] = []
        try:
            for name, dest in destinations.items():
                dest.parent.mkdir(parents=True, exist_ok=True)
                scratch_files[name].replace(dest)
                moved.append(dest)
        except BaseException:
            for dest in moved:
                dest.unlink(missing_ok=True)
            raise

        if quarantined_out is not None and quarantined:
            quarantined_tmp = scratch_dir / "quarantined-anomalies.json"
            quarantined_tmp.write_text(
                json.dumps(
                    {
                        "format": "spicy-irs-form990-quarantined-anomalies",
                        "version": 1,
                        "submission_year": submission_year,
                        "count": len(quarantined),
                        "by_reason": dict(sorted(quarantined_by_reason.items())),
                        "rows": quarantined,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            q_dest = Path(quarantined_out)
            q_dest.parent.mkdir(parents=True, exist_ok=True)
            quarantined_tmp.replace(q_dest)

        if summary_out is not None:
            summary_tmp = scratch_dir / "irs990-population-summary.json"
            summary_tmp.write_text(
                json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            s_dest = Path(summary_out)
            s_dest.parent.mkdir(parents=True, exist_ok=True)
            summary_tmp.replace(s_dest)

        return summary


def build_irs_form990(records: Iterable[Mapping[str, Any]], output_dir: Path) -> list[Path]:
    """Validate all selected filings before replacing any maintained flat table output."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if any((output_dir / (name + ".parquet")).exists() for name in SCHEMAS):
        raise ValueError("Use fresh output paths for each IRS generation")
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in SCHEMAS}
    seen: set[str] = set()
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
