"""Named profiles from an explicit pair of retained FEC summary CSV snapshots.

The financial rule owns amount eligibility. This owner only groups equivalent
witnesses and exposes unresolved conflicts; it never sums or selects a latest row.
"""

from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import date, datetime
from decimal import Decimal
import hashlib
from itertools import groupby
import json
from pathlib import Path
import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.contract_types import described_schema
from spicy_regs.fec_financial_rules import IDENTITY_VERSION, VALUE_MAPPING_VERSION
from spicy_regs.parquet_rows import write_rows
from spicy_regs.sources import publication
from spicy_regs.transforms.fec_financial_policy import reported_summary_measure
from spicy_regs.transforms.fec_summaries import SUMMARY_SCHEMA

TABLE = "fec_financial_profiles"
OUTPUT = TABLE + ".parquet"
PARENT = "fec_reported_financial_summaries.parquet"
POLICY = "fec-reported-profile-selection/1"
MEASURES = {
    "candidate": {
        "receipts": "Total_Receipt",
        "disbursements": "Total_Disbursement",
        "cash_on_hand": "Cash_On_Hand_COP",
        "debts_owed": "Debt_Owed_By_Committee",
    },
    "committee": {
        "receipts": "TTL_RECEIPTS",
        "disbursements": "TTL_DISB",
        "cash_on_hand": "COH_COP",
        "debts_owed": "DEBTS_OWED_BY_CMTE",
    },
}
TEXT_FIELDS = (
    "profile_id scope_id entity_kind entity_id entity_name selection_status selection_reason "
    "coverage_status coverage_reasons_json period_start_raw period_start_status period_end_raw period_end_status "
    "amount_basis captured_at source_collection_id source_sha256 source_table_generation "
    "witnesses_json source_url publisher_profile_url selection_policy"
).split()
SCHEMA = pa.schema(
    [(name, pa.string()) for name in TEXT_FIELDS]
    + [
        (name + suffix, pa.string())
        for name in MEASURES["candidate"]
        for suffix in ("", "_status", "_raw", "_native_field")
    ]
    + [
        ("source_cycle", pa.int32()),
        ("witness_count", pa.int32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
    ]
)
_PIN = re.compile(r"sha256:[0-9a-f]{64}\Z")


def _json_default(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Unsupported source value: {type(value).__name__}")


def json_bytes(value):
    return json.dumps(
        value, default=_json_default, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _signature(path):
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def validate_selection(selection, snapshot):
    """Bind both populations, capture facts and the exact managed parent."""
    cycle = selection["cycle"]
    if type(cycle) is not int or not 1976 <= cycle <= 9998 or cycle % 2:
        raise ValueError("Profile selection requires an even FEC election cycle")
    if not isinstance(selection["scope_id"], str) or not selection["scope_id"]:
        raise ValueError("Profile selection requires an explicit scope identity")
    parent = publication.table_pin(snapshot, PARENT)
    if parent.get("family") != "fec-query" or selection["table"]["pin"] != parent:
        raise ValueError("Profile input differs from the selected managed summary table")
    items = selection["collections"]
    if len(items) != 2 or {item["entity_kind"] for item in items} != set(MEASURES):
        raise ValueError("Select exactly one candidate CSV and one committee CSV")
    for item in items:
        kind = item["entity_kind"]
        if item["collection_id"] != f"bulk-{kind}_summary_{cycle}-csv":
            raise ValueError("Profile collection does not identify the declared CSV cycle")
        if not _PIN.fullmatch(str(item["source_sha256"])):
            raise ValueError("Profile source requires an exact digest")
        if type(item["expected_rows"]) is not int or item["expected_rows"] < 0:
            raise ValueError("Profile selection requires complete declared row counts")
        captured = datetime.fromisoformat(item["captured_at"])
        if captured.tzinfo is None or not item["source_url"].startswith("https://"):
            raise ValueError("Profile capture requires an aware time and source URL")
    return parent


def _coverage(row, cycle, captured_at):
    reasons = []
    for field in ("period_start", "period_end"):
        if row[field] is None or row[field + "_status"] != "exact":
            reasons.append(field + ":" + row[field + "_status"])
    start, end = row["period_start"], row["period_end"]
    if start and end and start > end:
        reasons.append("period_start_after_end")
    if end and end > datetime.fromisoformat(captured_at).date():
        reasons.append("period_end_after_capture")
    if start and start < date(cycle - 1, 1, 1) or end and end > date(cycle, 12, 31):
        reasons.append("period_outside_declared_cycle")
    return "unqualified" if reasons else "reported", json_bytes(reasons).decode()


def _equivalence(row, kind):
    # Linked candidates explain duplicate committee witnesses, not distinct
    # committee money. All measures and date spellings still must agree.
    fields = (
        "identity_version mapping_version value_mapping_version mapping_status "
        "source_authority source_representation_role correction_operation correction_applicability_status "
        "current_record_status summary_type source_namespace currency period_basis "
        "period_start period_start_raw period_start_status period_end period_end_raw period_end_status measures"
    ).split()
    return json_bytes({**{key: row[key] for key in fields}, "name": row[kind + "_name"]})


def profile(rows, item, selection):
    """Keep all witnesses; equal financial facts select once, conflicts select none."""
    kind = item["entity_kind"]
    first = rows[0]
    entity_id = first[kind + "_native_id"]
    if not isinstance(entity_id, str) or not entity_id:
        raise ValueError("Selected summary lacks its entity identity; population cannot be qualified")
    first_facts = _equivalence(first, kind)
    equivalent = all(_equivalence(row, kind) == first_facts for row in rows[1:])
    selected = first if equivalent else None
    result = dict.fromkeys(SCHEMA.names)
    result.update(
        profile_id="sha256:"
        + hashlib.sha256(json_bytes([POLICY, selection["scope_id"], kind, entity_id, selection["cycle"]])).hexdigest(),
        scope_id=selection["scope_id"],
        entity_kind=kind,
        entity_id=entity_id,
        entity_name=first[kind + "_name"]
        if all(row[kind + "_name"] == first[kind + "_name"] for row in rows)
        else None,
        source_cycle=selection["cycle"],
        selection_status="selected" if equivalent else "ambiguous",
        selection_reason=("unique_reported_snapshot" if len(rows) == 1 else "equivalent_witnesses")
        if equivalent
        else "conflicting_reported_snapshots",
        amount_basis="source_reported",
        captured_at=item["captured_at"],
        source_collection_id=item["collection_id"],
        source_sha256=item["source_sha256"],
        source_table_generation=selection["table"]["pin"]["artifactDigest"],
        witness_count=len(rows),
        witnesses_json=json_bytes(rows).decode(),
        source_url=item["source_url"],
        publisher_profile_url=f"https://www.fec.gov/data/{kind}/{entity_id}/?cycle={selection['cycle']}",
        selection_policy=POLICY,
    )
    if selected:
        result["coverage_status"], result["coverage_reasons_json"] = _coverage(
            selected, selection["cycle"], item["captured_at"]
        )
        for field in ("period_start", "period_end"):
            for suffix in ("", "_raw", "_status"):
                result[field + suffix] = selected[field + suffix]
    else:
        result.update(coverage_status="ambiguous", coverage_reasons_json='["conflicting_reported_snapshots"]')
        result["period_start_status"] = result["period_end_status"] = "ambiguous"
    for name, native in MEASURES[kind].items():
        result[name + "_native_field"] = native
        result[name + "_status"] = "ambiguous"
        if selected is None:
            continue
        matches = [measure for measure in selected["measures"] if measure["native_field"] == native]
        decision = reported_summary_measure(selected, native)
        raw = matches[0]["raw_value"] if len(matches) == 1 else None
        result[name + "_raw"] = raw
        if decision.status == "eligible":
            result[name] = format(decision.value, "f")
            result[name + "_status"] = "exact"
        elif len(matches) == 1 and matches[0]["value_status"] in {"source_null", "source_empty"}:
            result[name + "_status"] = matches[0]["value_status"]
        else:
            result[name + "_status"] = "refused" if matches else "not_supplied"
    return result


def build_profiles(selection, snapshot, destination):
    """One bounded ordered table read; atomic output follows final membership checks."""
    parent = validate_selection(selection, snapshot)
    path = Path(selection["table"]["path"])
    signature = _signature(path)
    identity = publication.file_identity(path)
    if identity["sha256"] != parent["sha256"] or identity["bytes"] != parent["byteSize"]:
        raise ValueError("Profile parent bytes differ from selected digest")
    if _signature(path) != signature:
        raise ValueError("Profile parent changed during verification")
    footer = pq.ParquetFile(path)
    if not footer.schema_arrow.equals(SUMMARY_SCHEMA, check_metadata=False):
        raise ValueError("Profile parent does not have the complete exact-value summary schema")
    table = snapshot["families"]["fec-query"]["tables"][PARENT]
    if (
        footer.metadata.num_rows != table["rows"]
        or [list(pair) for pair in described_schema(SUMMARY_SCHEMA)] != table["columns"]
    ):
        raise ValueError("Profile parent footer differs from declared population")
    footer.close()
    items = {item["collection_id"]: item for item in selection["collections"]}
    counts, outcomes = Counter(), Counter()
    seen = set()

    def records():
        # No corpus-sized Python list or spill. Sorting is bounded by DuckDB's
        # memory refusal, and only one entity's witnesses stay in Python.
        with duckdb.connect(config={"threads": 1, "memory_limit": "512MB", "max_temp_directory_size": "0B"}) as db:
            reader = db.execute(
                """SELECT * FROM read_parquet(?) WHERE collection_id IN (?, ?)
                   ORDER BY collection_id,
                     CASE WHEN summary_type='candidate-summary-csv/1'
                          THEN candidate_native_id ELSE committee_native_id END, record_id""",
                [str(path), *items],
            ).to_arrow_reader(batch_size=256)

            def source_rows():
                for batch in reader:
                    for row in batch.to_pylist():
                        item = items[row["collection_id"]]
                        kind = item["entity_kind"]
                        if (
                            row["source_sha256"] != item["source_sha256"]
                            or row["source_cycle"] != selection["cycle"]
                            or row["summary_type"] != kind + "-summary-csv/1"
                            or row["value_mapping_version"] != VALUE_MAPPING_VERSION
                            or row["identity_version"] != IDENTITY_VERSION
                            or row["mapping_version"] != "fec-" + kind + "-summary-csv/1"
                            or row["source_namespace"] != "fec-bulk-" + kind + "-summary-csv"
                            or row["source_authority"] != "official-fec"
                            or row["source_representation_role"] != "snapshot"
                            or row["correction_operation"] != "none"
                        ):
                            raise ValueError("Selected profile row differs from its declared source or mapping")
                        if row["record_id"] in seen:
                            raise ValueError("Duplicate physical witness identity in selected profile")
                        seen.add(row["record_id"])
                        counts[row["collection_id"]] += 1
                        if counts[row["collection_id"]] > item["expected_rows"]:
                            raise ValueError("Selected profile population exceeds declaration")
                        yield row

            try:
                for (collection, _entity), group in groupby(
                    source_rows(),
                    key=lambda row: (
                        row["collection_id"],
                        row[items[row["collection_id"]]["entity_kind"] + "_native_id"],
                    ),
                ):
                    rows = list(group)
                    result = profile(rows, items[collection], selection)
                    outcomes[result["selection_status"]] += 1
                    yield result
            finally:
                reader.close()
        if dict(counts) != {key: item["expected_rows"] for key, item in items.items() if item["expected_rows"]}:
            raise ValueError("Selected profile population differs from declaration")
        if _signature(path) != signature:
            raise ValueError("Profile parent changed during reading")

    with closing(records()) as rows:
        write_rows(rows, Path(destination), SCHEMA, batch_size=128)
    return {"source_rows": dict(counts), "profiles": dict(outcomes), "selection_policy": POLICY}
