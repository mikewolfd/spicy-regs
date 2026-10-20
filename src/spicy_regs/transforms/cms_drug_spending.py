"""Common value, provenance and population checks for the two CMS drug programs."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import re

import pyarrow as pa

IDENTITY_FIELDS = ("Brnd_Name", "Gnrc_Name", "Mftr_Name")


def spending_schema(measures):
    """Shared observation fields; each program declares its own measure names."""
    return pa.schema(
        [
            ("observation_id", pa.string()),
            ("benefit_year", pa.int32()),
            ("brand_name", pa.string()),
            ("generic_name", pa.string()),
            ("manufacturer_name", pa.string()),
            ("aggregation_level", pa.string()),
            ("manufacturer_count_raw", pa.string()),
            *((name, pa.decimal128(38, 12)) for name in measures),
            ("outlier_flag_raw", pa.string()),
            ("value_statuses_json", pa.string()),
            ("source_record_id", pa.string()),
            ("source_release_logical_id", pa.string()),
            ("source_release_digest", pa.string()),
            ("source_selection_json", pa.string()),
            ("source_locator_json", pa.string()),
            ("raw_row_json", pa.string()),
        ]
    )


_DECIMAL = re.compile(r"[+-]?[0-9]+(?:\.[0-9]+)?\Z")


def json_value(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def measurement(raw):
    """Preserve missing/unknown cells and refuse precision loss without inventing zero."""
    if raw is None:
        return None, "null"
    if raw == "":
        return None, "blank"
    if not isinstance(raw, str):
        raise ValueError("CMS source measurements must remain literal strings or null")
    if not _DECIMAL.fullmatch(raw):
        return None, "non_numeric"
    value = Decimal(raw)
    with localcontext() as context:
        context.prec = 50
        try:
            if abs(value) >= Decimal("1e26") or value != value.quantize(Decimal("1e-12")):
                return None, "unrepresentable"
        except InvalidOperation:
            return None, "unrepresentable"
    return value, "value"


def observation(item, pin, *, year, measures, label):
    """Map annual measures without recomputing weighted averages or adding populations."""
    record, raw = item["record"], item["record"]["raw"]
    identity = tuple(raw[name] for name in IDENTITY_FIELDS)
    if any(not isinstance(value, str) or not value for value in identity):
        raise ValueError(f"CMS {label} row lacks its literal drug/manufacturer identity")
    if any(value is not None and not isinstance(value, str) for value in raw.values()):
        raise ValueError(f"CMS {label} raw cells must remain literal strings or null")
    fields = {name: f"{prefix}_{year}" for name, prefix in measures.items()}
    required = {*fields.values(), f"Outlier_Flag_{year}", "Tot_Mftr"}
    if not required <= raw.keys():
        raise ValueError(f"CMS {label} row lacks a selected-year field")
    values = {name: measurement(raw[field]) for name, field in fields.items()}
    source_id = item["sourceRecordId"]
    if not isinstance(source_id, str) or not source_id:
        raise ValueError(f"CMS {label} row lacks its admitted source record identity")
    return {
        "observation_id": "sha256:"
        + hashlib.sha256(json_value([pin.artifact_digest, source_id, year]).encode()).hexdigest(),
        "benefit_year": year,
        "brand_name": identity[0],
        "generic_name": identity[1],
        "manufacturer_name": identity[2],
        # The retained annual release includes both Overall and named manufacturer
        # rows for the same drug. They are alternative aggregation levels.
        "aggregation_level": "all_manufacturers" if identity[2] == "Overall" else "manufacturer",
        "manufacturer_count_raw": raw["Tot_Mftr"],
        **{name: pair[0] for name, pair in values.items()},
        "outlier_flag_raw": raw[f"Outlier_Flag_{year}"],
        "value_statuses_json": json_value({name: pair[1] for name, pair in values.items()}),
        "source_record_id": source_id,
        "source_release_logical_id": pin.logical_id,
        "source_release_digest": pin.artifact_digest,
        "source_selection_json": json_value(record["selection"]),
        "source_locator_json": json_value(record["source_locator"]),
        "raw_row_json": json_value(raw),
    }


def observations(reader, *, year, series_title, label, map_observation):
    """Stream a complete selected population; duplicate identities refuse atomically."""
    outcome = reader.collection_outcome
    selection = outcome["requestedScope"]["selection"]
    if (
        type(year) is not int
        or selection["series_title"] != series_title
        or date.fromisoformat(selection["release_date"]) != date(year, 1, 1)
    ):
        raise ValueError(f"Select the {label} release for the explicit benefit year")
    count = selection["total_rows"]
    if (
        type(count) is not int
        or count <= 0
        or outcome["publishedRecordCount"] != count
        or outcome["discoveredRecordCount"] != count
        or outcome["failedRecordCount"] != 0
        or outcome["discardedObservationCount"] != 0
    ):
        raise ValueError(f"CMS {label} requires a complete, nonempty admitted source population")
    seen_ids, identities, overall_drugs, manufacturer_drugs = set(), set(), set(), set()
    for item in reader.iter_records():
        if item["record"]["selection"] != selection:
            raise ValueError(f"CMS {label} record differs from the selected source release")
        row = map_observation(item, reader.pin, year=year)
        identity = row["brand_name"], row["generic_name"], row["manufacturer_name"]
        if row["source_record_id"] in seen_ids or identity in identities:
            raise ValueError(f"Repeated CMS {label} drug/manufacturer or source occurrence")
        seen_ids.add(row["source_record_id"])
        identities.add(identity)
        (overall_drugs if row["aggregation_level"] == "all_manufacturers" else manufacturer_drugs).add(identity[:2])
        yield row
    if len(seen_ids) != count:
        raise ValueError(f"CMS {label} source population differs from its declared row count")
    if not overall_drugs or not manufacturer_drugs <= overall_drugs:
        raise ValueError(f"CMS {label} manufacturer rows lack their Overall comparison rows")
