"""Queryable lookup outcomes and native geography candidates; no unique guess."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from spicy_regs.transforms.parquet_rows import write_rows

LOOKUPS = "census_geocoder_lookups"
GEOGRAPHIES = "census_geocoder_geographies"
REFERENCE = json.loads((Path(__file__).resolve().parents[1] / "reference/refspec/census-county-geoid.json").read_text())
if REFERENCE["row"]["structure"] != "STATE+COUNTY" or REFERENCE["row"]["number_of_digits"] != "2+3=5":
    raise ValueError("Reviewed RefSpec county GEOID definition changed")

_COMMON = [
    (name, pa.string())
    for name in (
        "lookup_id",
        "kind",
        "benchmark",
        "vintage",
        "outcome",
        "reason",
        "request_url",
        "observed_at",
        "response_sha256",
        "source_release_logical_id",
        "source_release_digest",
    )
]
LOOKUP_SCHEMA = pa.schema(
    [
        *_COMMON,
        ("address", pa.string()),
        ("input_x", pa.string()),
        ("input_y", pa.string()),
        ("candidate_count", pa.int32()),
        ("response_json", pa.string()),
    ]
)
GEOGRAPHY_SCHEMA = pa.schema(
    [
        *_COMMON,
        ("candidate_index", pa.int32()),
        ("unique_assignment", pa.bool_()),
        *[
            (name, pa.string())
            for name in (
                "matched_address",
                "x",
                "y",
                "layer",
                "geoid",
                "name",
                "state_code",
                "county_code",
                "county_geoid",
                "tract_code",
                "block_code",
                "json_pointer",
                "geography_json",
                "geography_authority",
                "geography_source_sha256",
            )
        ],
    ]
)
SCHEMAS = {LOOKUPS: LOOKUP_SCHEMA, GEOGRAPHIES: GEOGRAPHY_SCHEMA}


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str, allow_nan=False)


def rows(readers):
    from spicy_docs.reading.json_input import load_bounded_json
    from spicy_docs.sources.census_geocoder import MAX_BYTES, GeocoderSelection, lookup_record, selection_id
    from spicy_docs.transport.captured import CapturedBodyResponse

    lookups, geographies, seen = [], [], set()
    for reader in readers:
        records = list(reader.iter_records())
        if len(records) != 1:
            raise ValueError("Each pinned lookup release must contain exactly one outcome")
        item = records[0]
        r = item["record"]
        selected = GeocoderSelection(**r["selection"])
        lookup_id = selection_id(selected)
        if lookup_id in seen or item["sourceRecordId"] != lookup_id:
            raise ValueError("Lookup identity is repeated or differs from exact input")
        seen.add(lookup_id)
        c = r["capture"]
        captured = CapturedBodyResponse(
            c["request_url"],
            c["resolved_url"],
            c["status_code"],
            c["content_type"],
            c["observed_at"],
            r["response_json"].encode(),
        )
        if lookup_record(selected, captured) != r or captured.sha256 != c["response_sha256"]:
            raise ValueError("Lookup outcome or original response identity differs from source mapping")
        base = {
            "lookup_id": lookup_id,
            "kind": selected.kind,
            "benchmark": selected.benchmark,
            "vintage": selected.vintage,
            "outcome": r["outcome"],
            "reason": r["reason"],
            "request_url": c["request_url"],
            "observed_at": c["observed_at"],
            "response_sha256": c["response_sha256"],
            "source_release_logical_id": reader.pin.logical_id,
            "source_release_digest": reader.pin.artifact_digest,
        }
        lookups.append(
            base
            | {
                "address": selected.address,
                "input_x": selected.x,
                "input_y": selected.y,
                "candidate_count": len(r["candidates"]),
                "response_json": r["response_json"],
            }
        )
        if r["outcome"] not in {"matched", "ambiguous"}:
            continue
        decoded = load_bounded_json(
            captured.body,
            source="Census geocoder table",
            error_type=ValueError,
            number_policy="decimal",
            max_bytes=MAX_BYTES,
        )
        if not isinstance(decoded, dict):
            raise ValueError("Census geocoder response must be an object")
        native = cast(dict[str, Any], decoded)["result"]
        for index, candidate in enumerate(r["candidates"]):
            obj = native["addressMatches"][index] if selected.kind == "onelineaddress" else native
            for layer, layer_rows in obj["geographies"].items():
                for ordinal, geo in enumerate(layer_rows):
                    state, county = geo.get("STATE"), geo.get("COUNTY")
                    geographies.append(
                        base
                        | {
                            "candidate_index": index,
                            "unique_assignment": r["outcome"] == "matched",
                            "matched_address": candidate["matched_address"],
                            "x": candidate["x"],
                            "y": candidate["y"],
                            "layer": layer,
                            "geoid": geo["GEOID"],
                            "name": geo.get("NAME"),
                            "state_code": state,
                            "county_code": county,
                            "county_geoid": state + county if state is not None and county is not None else None,
                            "tract_code": geo.get("TRACT"),
                            "block_code": geo.get("BLOCK"),
                            "json_pointer": candidate["pointer"]
                            + "/geographies/"
                            + layer.replace("~", "~0").replace("/", "~1")
                            + f"/{ordinal}",
                            "geography_json": _json(geo),
                            "geography_authority": REFERENCE["authority_resource_id"],
                            "geography_source_sha256": REFERENCE["source_sha256"],
                        }
                    )
    if not lookups or len(lookups) > 100:
        raise ValueError("A local cohort must contain from one to 100 distinct pinned lookups")
    return {LOOKUPS: lookups, GEOGRAPHIES: geographies}


def build_census_geocoder(readers, output_dir):
    """Validate the complete bounded cohort before writing any candidate tables."""
    data = rows(readers)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    for table, schema in SCHEMAS.items():
        path = output_dir / (table + ".parquet")
        write_rows(data[table], path, schema)
        outputs.append(path)
    return tuple(outputs)
