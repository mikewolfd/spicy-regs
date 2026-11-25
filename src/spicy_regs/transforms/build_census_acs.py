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
CD_TABLE = "census_acs_congressional_district_observations"
CD_OUTPUT = CD_TABLE + ".parquet"
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

_STATE_CODE = re.compile(r"[0-9]{2}")
_COUNTY_CODE = re.compile(r"[0-9]{3}")
_COUNTY_GEOID = re.compile(r"[0-9]{5}")
_CD_CODE = re.compile(r"(?:[0-9]{2}|ZZ)")
_CD_GEOID = re.compile(r"[0-9]{2}(?:[0-9]{2}|ZZ)")
_CD_SOURCE_GEO_ID = re.compile(r"(500([0-9]{2})00US)([0-9]{2})([0-9]{2}|ZZ)")
_DECIMAL = re.compile(r"[+-]?[0-9]+(?:\.[0-9]+)?\Z")
_SENTINELS = {
    Decimal("-999999999"): "insufficient_sample",
    Decimal("-888888888"): "not_applicable_or_unavailable",
    Decimal("-666666666"): "unavailable",
    Decimal("-555555555"): "controlled_estimate",
    Decimal("-333333333"): "open_ended_median",
    Decimal("-222222222"): "insufficient_observations",
}
_MEASUREMENT_FIELDS = [
    ("name", pa.string()), ("source_geo_id", pa.string()),
    ("measure", pa.string()), ("unit", pa.string()),
    ("estimate", pa.decimal128(38, 6)), ("margin_of_error", pa.decimal128(38, 6)),
    ("estimate_status", pa.string()), ("margin_status", pa.string()),
    ("estimate_raw", pa.string()), ("margin_raw", pa.string()),
    ("estimate_annotation", pa.string()), ("margin_annotation", pa.string()),
    ("source_record_id", pa.string()), ("source_release_logical_id", pa.string()),
    ("source_release_digest", pa.string()), ("variable_metadata_json", pa.string()),
    ("value_sources_json", pa.string()), ("metadata_sources_json", pa.string()),
]
SCHEMA = pa.schema([
    ("dataset", pa.string()), ("vintage", pa.int32()), ("geography", pa.string()),
    ("state_code", pa.string()), ("county_code", pa.string()), ("county_geoid", pa.string()),
    *_MEASUREMENT_FIELDS,
    ("geography_authority", pa.string()), ("geography_source_sha256", pa.string()),
])
CD_SCHEMA = pa.schema([
    ("dataset", pa.string()), ("vintage", pa.int32()), ("geography", pa.string()),
    ("state_code", pa.string()), ("congressional_district", pa.string()), ("district_geoid", pa.string()),
    ("source_geo_prefix", pa.string()), ("congress_session_code", pa.string()),
    *_MEASUREMENT_FIELDS,
])
SCHEMAS = {TABLE: SCHEMA, CD_TABLE: CD_SCHEMA}


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


def _expected_allowlist(values, *, pattern: re.Pattern[str], label: str) -> set[str] | None:
    if values is None:
        return None
    if (
        not values
        or len(set(values)) != len(values)
        or any(not isinstance(item, str) or pattern.fullmatch(item) is None for item in values)
    ):
        raise ValueError(f"Declare the complete {label} code set without duplicates")
    return set(values)


def _observations(reader, *, vintage, state, counties=None, geography="county", districts=None):
    if type(vintage) is not int or vintage < 2009:
        raise ValueError("An explicit ACS five-year vintage is required")
    if geography not in {"county", "congressional district"}:
        raise ValueError("ACS geography must be 'county' or 'congressional district'")
    if not isinstance(state, str) or (state != "*" and _STATE_CODE.fullmatch(state) is None):
        raise ValueError("State must be '*' or a literal two-digit Census code")
    if geography == "county":
        if districts is not None:
            raise ValueError("districts allowlist applies only to congressional district geography")
        expected = _expected_allowlist(
            counties,
            pattern=_COUNTY_GEOID if state == "*" else _COUNTY_CODE,
            label="county GEOID" if state == "*" else "county",
        )
    else:
        if counties is not None:
            raise ValueError("counties allowlist applies only to county geography")
        expected = _expected_allowlist(
            districts,
            pattern=_CD_GEOID if state == "*" else _CD_CODE,
            label="congressional district GEOID" if state == "*" else "congressional district",
        )
    seen: set[str] = set()
    cd_prefix: str | None = None
    variables = {base + suffix for base in MEASURES for suffix in ("E", "M", "EA", "MA")}
    for item in reader.iter_records():
        row = item["record"]
        row_state = row.get("state")
        if (
            (row.get("dataset"), row.get("vintage"), row.get("geography")) != ("acs/acs5", vintage, geography)
            or not isinstance(row_state, str)
            or (_STATE_CODE.fullmatch(row_state) is None if state == "*" else row_state != state)
        ):
            raise ValueError("ACS record differs from the declared dataset, vintage or geographic scope")
        if geography == "county":
            code = row.get("county")
            if not isinstance(code, str) or _COUNTY_CODE.fullmatch(code) is None:
                raise ValueError("ACS county code differs from supported three-digit format")
            geoid = row_state + code
            if row["geo_id"] != "0500000US" + geoid:
                raise ValueError("ACS GEO_ID differs from the RefSpec county composition")
            key = geoid if state == "*" else code
            extra_geo = {
                "state_code": row_state,
                "county_code": code,
                "county_geoid": geoid,
                "geography_authority": GEOID_REFERENCE["authority_resource_id"],
                "geography_source_sha256": GEOID_REFERENCE["source_sha256"],
            }
        else:
            code = row.get("congressional_district")
            if not isinstance(code, str) or _CD_CODE.fullmatch(code) is None:
                raise ValueError("ACS congressional district code differs from supported two-character format")
            geo_match = _CD_SOURCE_GEO_ID.fullmatch(row["geo_id"])
            if geo_match is None or geo_match[3] != row_state or geo_match[4] != code:
                raise ValueError("ACS congressional district GEO_ID differs from 500<session>00US<state><cd>")
            if cd_prefix is not None and geo_match[1] != cd_prefix:
                raise ValueError("ACS congressional district release mixes Congress session GEO_ID prefixes")
            cd_prefix = geo_match[1]
            geoid = row_state + code
            key = geoid if state == "*" else code
            extra_geo = {
                "state_code": row_state,
                "congressional_district": code,
                "district_geoid": geoid,
                "source_geo_prefix": geo_match[1],
                "congress_session_code": geo_match[2],
            }
        if (expected is not None and key not in expected) or key in seen:
            raise ValueError(f"ACS {geography} membership is unexpected or repeated")
        seen.add(key)
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
                "dataset": row["dataset"], "vintage": vintage, "geography": geography,
                **extra_geo,
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
            }
    if expected is not None and seen != expected:
        raise ValueError(f"ACS release omitted a {geography} from the declared complete scope")


def build_census_acs(
    reader,
    output_dir: Path,
    *,
    vintage: int,
    state: str,
    counties: tuple[str, ...] | None = None,
    geography: str = "county",
    districts: tuple[str, ...] | None = None,
) -> Path:
    """Atomically write the complete selected comparison, or preserve prior output."""
    if geography not in {"county", "congressional district"}:
        raise ValueError("ACS geography must be 'county' or 'congressional district'")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / (OUTPUT if geography == "county" else CD_OUTPUT)
    schema = SCHEMA if geography == "county" else CD_SCHEMA
    write_rows(
        _observations(reader, vintage=vintage, state=state, counties=counties, geography=geography, districts=districts),
        destination,
        schema,
    )
    return destination
