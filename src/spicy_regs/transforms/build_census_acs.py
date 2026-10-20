"""County comparisons from an admitted, retained ACS five-year source release.

SpicyDocs owns response parsing and geographic membership across requests.
This mapper preserves its literal cells and adds exact decimal measurements.
"""
from __future__ import annotations

import json
import re
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa

from spicy_regs.transforms.parquet_rows import write_rows

TABLE = "census_acs_county_observations"
OUTPUT = TABLE + ".parquet"
MEASURES = {
    "B01003_001": ("population", "persons"),
    "B19013_001": ("median_household_income", "USD; inflation-adjusted to vintage"),
}
GEOID_REFERENCE = json.loads(
    (Path(__file__).resolve().parents[1] / "reference/refspec/census-county-geoid.json").read_text()
)
# This is the County row exported by RefSpec's pinned Census guidance reader.
if GEOID_REFERENCE["row"]["structure"] != "STATE+COUNTY" or GEOID_REFERENCE["row"]["number_of_digits"] != "2+3=5":
    raise ValueError("Reviewed Census county GEOID composition changed")

_DECIMAL = re.compile(r"[+-]?[0-9]+(?:\.[0-9]+)?\Z")
_SENTINELS = {
    Decimal("-999999999"): "insufficient_sample",
    Decimal("-888888888"): "not_applicable_or_unavailable",
    Decimal("-666666666"): "unavailable",
    Decimal("-555555555"): "controlled_estimate",
    Decimal("-333333333"): "open_ended_median",
    Decimal("-222222222"): "insufficient_observations",
}
SCHEMA = pa.schema([
    ("dataset", pa.string()), ("vintage", pa.int32()), ("geography", pa.string()),
    ("state_code", pa.string()), ("county_code", pa.string()), ("county_geoid", pa.string()),
    ("name", pa.string()), ("source_geo_id", pa.string()),
    ("measure", pa.string()), ("unit", pa.string()),
    ("estimate", pa.decimal128(38, 6)), ("margin_of_error", pa.decimal128(38, 6)),
    ("estimate_status", pa.string()), ("margin_status", pa.string()),
    ("estimate_raw", pa.string()), ("margin_raw", pa.string()),
    ("estimate_annotation", pa.string()), ("margin_annotation", pa.string()),
    ("source_record_id", pa.string()), ("source_release_logical_id", pa.string()),
    ("source_release_digest", pa.string()), ("variable_metadata_json", pa.string()),
    ("value_sources_json", pa.string()), ("metadata_sources_json", pa.string()),
    ("geography_authority", pa.string()), ("geography_source_sha256", pa.string()),
])


def _measurement(raw, annotation, *, median=False):
    """Keep missing values and Census sentinel meanings separate from numbers."""
    if raw is None:
        return None, "null"
    if raw == "":
        return None, "blank"
    if not isinstance(raw, str) or not _DECIMAL.fullmatch(raw):
        raise ValueError("ACS numeric cell is not a literal decimal or a documented sentinel")
    value = Decimal(raw)
    if value in _SENTINELS:
        return None, _SENTINELS[value]
    # Refuse rounding rather than silently lose precision at the table boundary.
    with localcontext() as context:
        context.prec = 44
        if value != value.quantize(Decimal("0.000001")):
            raise ValueError("ACS numeric cell exceeds the table's exact decimal scale")
    # A numeric median followed by a sign is a bound. A standalone "-" is
    # an unavailable-data annotation, not an upper bound on this number.
    bound = re.fullmatch(r"([0-9][0-9,]*(?:\.[0-9]+)?)([+-])", annotation or "")
    if median and bound and Decimal(bound[1].replace(",", "")) == value:
        return value, "lower_bound" if bound[2] == "+" else "upper_bound"
    return value, "annotated" if annotation else "numeric"


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _observations(reader, *, vintage, state, counties):
    if type(vintage) is not int or vintage < 2009:
        raise ValueError("An explicit ACS five-year vintage is required")
    if not isinstance(state, str) or not re.fullmatch(r"[0-9]{2}", state):
        raise ValueError("State must be a literal two-digit Census code")
    if (not counties or len(set(counties)) != len(counties)
            or any(not isinstance(c, str) or not re.fullmatch(r"[0-9]{3}", c) for c in counties)):
        raise ValueError("Declare the complete county code set without duplicates")
    expected, seen = set(counties), set()
    variables = {base + suffix for base in MEASURES for suffix in ("E", "M", "EA", "MA")}
    for item in reader.iter_records():
        row = item["record"]
        if (row["dataset"], row["vintage"], row["geography"], row["state"]) != ("acs/acs5", vintage, "county", state):
            raise ValueError("ACS record differs from the declared dataset, vintage or geographic scope")
        county = row["county"]
        if county not in expected or county in seen:
            raise ValueError("ACS county membership is unexpected or repeated")
        seen.add(county)
        geoid = state + county
        if row["geo_id"] != "0500000US" + geoid:
            raise ValueError("ACS GEO_ID differs from the RefSpec county composition")
        values = row["values"]
        for field in ("values", "variables", "value_sources", "metadata_sources"):
            if not variables <= row[field].keys():
                raise ValueError("ACS measurement is missing a selected cell or its evidence")
        if any(value is not None and not isinstance(value, str) for value in values.values()):
            raise ValueError("ACS source cells must remain literal strings or null")
        for base, (measure, unit) in MEASURES.items():
            selected = tuple(base + suffix for suffix in ("E", "M", "EA", "MA"))
            estimate, estimate_status = _measurement(
                values[base + "E"], values[base + "EA"], median=measure == "median_household_income",
            )
            margin, margin_status = _measurement(values[base + "M"], values[base + "MA"])
            yield {
                "dataset": row["dataset"], "vintage": vintage, "geography": "county",
                "state_code": state, "county_code": county, "county_geoid": geoid,
                "name": row["name"], "source_geo_id": row["geo_id"],
                "measure": measure, "unit": unit, "estimate": estimate, "margin_of_error": margin,
                "estimate_status": estimate_status, "margin_status": margin_status,
                "estimate_raw": values[base + "E"], "margin_raw": values[base + "M"],
                "estimate_annotation": values[base + "EA"], "margin_annotation": values[base + "MA"],
                "source_record_id": item["sourceRecordId"],
                "source_release_logical_id": reader.pin.logical_id,
                "source_release_digest": reader.pin.artifact_digest,
                "variable_metadata_json": _json({key: row["variables"][key] for key in selected}),
                "value_sources_json": _json({key: row["value_sources"][key] for key in selected}),
                "metadata_sources_json": _json({key: row["metadata_sources"][key] for key in selected}),
                "geography_authority": GEOID_REFERENCE["authority_resource_id"],
                "geography_source_sha256": GEOID_REFERENCE["source_sha256"],
            }
    if seen != expected:
        raise ValueError("ACS release omitted a county from the declared complete scope")


def build_census_acs(reader, output_dir: Path, *, vintage: int, state: str, counties: tuple[str, ...]) -> Path:
    """Atomically write the complete selected comparison, or preserve prior output."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / OUTPUT
    write_rows(_observations(reader, vintage=vintage, state=state, counties=counties), destination, SCHEMA)
    return destination
