"""Separate literal API status observations and complete query populations from Grants daily exports."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

STATUS_TABLE = "grants_status_observations"
QUERY_TABLE = "grants_status_queries"
SCHEMAS = {
    STATUS_TABLE: pa.schema([
        ("opportunity_id", pa.string()), ("lifecycle_status", pa.string()), ("status_value_state", pa.string()),
        ("document_type", pa.string()), ("opportunity_number", pa.string()), ("agency_code", pa.string()),
        ("open_date_raw", pa.string()), ("close_date_raw", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")), ("source_record_id", pa.string()),
        ("source_release_logical_id", pa.string()), ("source_release_digest", pa.string()),
        ("source_locator_json", pa.string()), ("raw_fields_json", pa.string()),
    ]),
    QUERY_TABLE: pa.schema([
        ("source_release_logical_id", pa.string()), ("source_release_digest", pa.string()),
        ("selection_json", pa.string()), ("declared_count", pa.int64()), ("observed_count", pa.int64()),
        ("page_count", pa.int64()), ("first_observed_at", pa.timestamp("us", tz="UTC")),
        ("last_observed_at", pa.timestamp("us", tz="UTC")), ("outcome", pa.string()),
        ("captures_json", pa.string()),
    ]),
}
OUTPUTS = tuple(table + ".parquet" for table in SCHEMAS)


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _rows(readers, populations):
    seen = set()
    for reader in readers:
        if reader.pin in seen:
            raise ValueError("A Grants status release was supplied twice")
        seen.add(reader.pin)
        outcome = reader.collection_outcome
        if (outcome["failedRecordCount"] or outcome["discardedObservationCount"]
                or outcome["discoveredRecordCount"] != outcome["publishedRecordCount"]):
            raise ValueError("Grants status requires a complete reconciled source population")
        scope = outcome["requestedScope"]
        captures = scope["captures"]
        count, identities = 0, set()
        for item in reader.iter_records():
            record = item["record"]
            raw, locator = record["raw"], record["source_locator"]
            native = raw["id"]
            if (item["sourceRecordId"] != f"grants-gov-status:{native}" or native in identities
                    or record["selection"] != scope["selection"]):
                raise ValueError("Grants status identity or selection differs")
            if not any(all(locator.get(key) == val for key, val in capture.items()) for capture in captures):
                raise ValueError("Grants status row has no selected response evidence")
            identities.add(native)
            status = raw.get("oppStatus")
            state = "missing" if "oppStatus" not in raw else "null" if status is None else "blank" if status == "" else "value"
            yield {
                "opportunity_id": native, "lifecycle_status": status, "status_value_state": state,
                "document_type": raw.get("docType"), "opportunity_number": raw.get("number"),
                "agency_code": raw.get("agencyCode"), "open_date_raw": raw.get("openDate"),
                "close_date_raw": raw.get("closeDate"), "observed_at": datetime.fromisoformat(locator["observed_at"]),
                "source_record_id": item["sourceRecordId"], "source_release_logical_id": reader.pin.logical_id,
                "source_release_digest": reader.pin.artifact_digest,
                "source_locator_json": _json(locator), "raw_fields_json": _json(raw),
            }
            count += 1
        if count != outcome["publishedRecordCount"]:
            raise ValueError("Grants status rows differ from the admitted population")
        times = [datetime.fromisoformat(capture["observed_at"]) for capture in captures]
        populations.append({
            "source_release_logical_id": reader.pin.logical_id, "source_release_digest": reader.pin.artifact_digest,
            "selection_json": _json(scope["selection"]), "declared_count": outcome["discoveredRecordCount"],
            "observed_count": count, "page_count": len(captures), "first_observed_at": min(times),
            "last_observed_at": max(times), "outcome": "observed-empty" if not count else "observed-complete",
            "captures_json": _json(captures),
        })
    if not seen:
        raise ValueError("Declare at least one Grants status release")


def build_grants_status(readers, output_dir: Path):
    """Write literal observations; zero hits still yield a query row with exact response evidence."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    populations = []
    status = write_rows(_rows(readers, populations), output_dir / OUTPUTS[0], SCHEMAS[STATUS_TABLE])
    queries = write_rows(populations, output_dir / OUTPUTS[1], SCHEMAS[QUERY_TABLE])
    return status, queries
