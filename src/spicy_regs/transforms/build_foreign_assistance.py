"""Country/fiscal-period/measure observations from complete retained summaries."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

OBSERVATIONS = "foreign_assistance_observations"
SNAPSHOTS = "foreign_assistance_snapshots"
# Reviewed dictionary, prepared by State on 2026-07-01; pages 1 and 4. It does
# not state a constant-dollar base year. A changed dictionary requires review.
REVIEWED_DICTIONARY = "sha256:3f3d6f03830736a5c190be93920e7e07b444da0cf0752993e3005f8812933ac8"
SCHEMAS = {
    OBSERVATIONS: pa.schema([
        ("snapshot_id", pa.string()), ("recipient_code", pa.string()), ("recipient_name", pa.string()),
        ("recipient_scope", pa.string()), ("region_id", pa.string()), ("region_name", pa.string()),
        ("income_group_acronym", pa.string()), ("income_group_name", pa.string()),
        ("fiscal_period", pa.string()), ("period_start", pa.date32()), ("period_end", pa.date32()),
        ("measure_id", pa.string()), ("measure", pa.string()), ("dollar_basis", pa.string()),
        ("amount", pa.decimal128(38, 6)), ("amount_raw", pa.string()), ("amount_status", pa.string()),
        ("unit", pa.string()), ("constant_dollar_base_year", pa.int32()), ("reporting_status", pa.string()),
        ("agency_scope", pa.string()), ("program_scope", pa.string()),
        ("source_record_id", pa.string()), ("source_ordinal", pa.int64()), ("source_release_digest", pa.string()),
        ("source_locator_json", pa.string()), ("raw_cells_json", pa.string()),
    ]),
    SNAPSHOTS: pa.schema([
        ("snapshot_id", pa.string()), ("observed_at", pa.timestamp("us", tz="UTC")),
        ("source_sha256", pa.string()), ("source_bytes", pa.int64()), ("source_url", pa.string()),
        ("dictionary_sha256", pa.string()), ("dictionary_url", pa.string()),
        ("source_release_logical_id", pa.string()), ("source_release_digest", pa.string()),
        ("source_row_count", pa.int64()), ("publisher_release_date", pa.date32()),
        ("coverage", pa.string()), ("source_scope_json", pa.string()),
    ]),
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def period_dates(value):
    """Keep the 1976 transition quarter separate from either adjacent fiscal year."""
    if value.lower() == "1976tq":
        return date(1976, 7, 1), date(1976, 9, 30)
    if not re.fullmatch(r"(?:19|20)[0-9]{2}", value):
        raise ValueError("Unsupported foreign assistance fiscal period")
    year = int(value)
    return (date(year - 1, 10, 1), date(year, 9, 30)) if year >= 1977 else (
        date(year - 1, 7, 1), date(year, 6, 30),
    )


def amount_cell(raw):
    """Preserve blanks, literal NULL, errors and exact signed dollars independently."""
    if raw == "":
        return None, "blank"
    if raw == "NULL":
        return None, "publisher_null"
    if not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]+)?", raw):
        return None, "unparsed"
    value = Decimal(raw)
    with localcontext() as context:
        context.prec = max(44, len(raw) + 8)
        if abs(value) >= Decimal("1e32") or value != value.quantize(Decimal(".000001")):
            return None, "unrepresentable"
    return value, "numeric"


def _scope(reader):
    outcome = reader.collection_outcome
    if (outcome["failedRecordCount"] or outcome["discardedObservationCount"]
            or outcome["discoveredRecordCount"] != outcome["publishedRecordCount"]):
        raise ValueError("Foreign assistance requires a complete accepted source observation")
    scope = outcome["requestedScope"]
    if scope["dictionary_capture"]["response_sha256"] != REVIEWED_DICTIONARY:
        raise ValueError("Foreign assistance dictionary requires bounded raw review before use")
    return scope


def snapshot_id(scope):
    return "sha256:" + hashlib.sha256(_json(scope).encode()).hexdigest()


def _rows(readers, snapshots):
    seen, instants = set(), set()
    for reader in readers:
        scope = _scope(reader)
        sid = snapshot_id(scope)
        if sid in seen:
            raise ValueError("A foreign assistance snapshot was supplied twice")
        seen.add(sid)
        capture, dictionary = scope["capture"], scope["dictionary_capture"]
        observed = datetime.fromisoformat(capture["observed_at"])
        if observed in instants:
            raise ValueError("Foreign assistance snapshots have ambiguous equal capture times")
        instants.add(observed)
        count = 0
        for item in reader.iter_records():
            record, raw = item["record"], item["record"]["raw"]
            count += 1
            start, end = period_dates(raw["Fiscal Year"])
            # Historical areas and territories remain source-designated recipients;
            # this is not an ISO or present-day sovereign-state crosswalk.
            name = raw["Country Name"]
            recipient_scope = "global" if name == "World" else (
                "regional" if name.endswith(" Region") else "country_or_area"
            )
            for basis in ("current", "constant"):
                amount, status = amount_cell(raw[basis + "_amount"])
                yield {
                    "snapshot_id": sid, "recipient_code": raw["Country Code"], "recipient_name": name,
                    "recipient_scope": recipient_scope, "region_id": raw["Region ID"],
                    "region_name": raw["Region Name"], "income_group_acronym": raw["Income Group Acronym"],
                    "income_group_name": raw["Income Group Name"], "fiscal_period": raw["Fiscal Year"],
                    "period_start": start, "period_end": end, "measure_id": raw["Transaction Type ID"],
                    "measure": raw["Transaction Type Name"], "dollar_basis": basis, "amount": amount,
                    "amount_raw": raw[basis + "_amount"], "amount_status": status, "unit": "USD",
                    "constant_dollar_base_year": None,
                    "reporting_status": "open_fiscal_period" if observed.date() <= end else "completeness_not_asserted",
                    "agency_scope": "all_reporting_agencies_aggregated; identities_not_in_country_summary",
                    "program_scope": "all_reported_programs_aggregated; identities_not_in_country_summary",
                    "source_record_id": item["sourceRecordId"], "source_ordinal": record["ordinal"],
                    "source_release_digest": reader.pin.artifact_digest,
                    "source_locator_json": _json(record["source_locator"]), "raw_cells_json": _json(raw),
                }
        if count != reader.collection_outcome["publishedRecordCount"]:
            raise ValueError("Foreign assistance consumer population differs from its source release")
        snapshots.append({
            "snapshot_id": sid, "observed_at": observed,
            "source_sha256": capture["response_sha256"], "source_bytes": capture["byte_size"],
            "source_url": capture["request_url"], "dictionary_sha256": dictionary["response_sha256"],
            "dictionary_url": dictionary["request_url"], "source_release_logical_id": reader.pin.logical_id,
            "source_release_digest": reader.pin.artifact_digest, "source_row_count": count,
            "publisher_release_date": None,
            "coverage": "Complete observed country-summary file; reporting completeness and finality unasserted; "
                        "agency/program identities and constant-dollar base unavailable in this summary.",
            "source_scope_json": _json(scope),
        })


def build_foreign_assistance(readers, output_dir: Path):
    readers = tuple(readers)
    if not readers:
        raise ValueError("Declare at least one retained foreign assistance snapshot")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshots = []
    observations = write_rows(_rows(readers, snapshots), output_dir / (OBSERVATIONS + ".parquet"), SCHEMAS[OBSERVATIONS])
    metadata = write_rows(snapshots, output_dir / (SNAPSHOTS + ".parquet"), SCHEMAS[SNAPSHOTS])
    return observations, metadata
