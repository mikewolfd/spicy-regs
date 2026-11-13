"""Map a complete declared GSA IOLP pair into source-traceable property and lease observations."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

FAMILY = "gsa-iolp"
PROPERTIES = "gsa_iolp_properties"
LEASES = "gsa_iolp_leases"
SNAPSHOTS = "gsa_iolp_snapshots"
OWNERSHIP = {"OWNED": "owned", "LEASED": "leased", "F": "owned", "L": "leased"}
TEXT_FIELDS = {
    "location_code": "Location Code",
    "asset_name": "Real Property Asset Name",
    "installation_name": "Installation Name",
    "gsa_region": "GSA Region",
    "street_address": "Street Address",
    "city": "City",
    "state": "State",
    "zip_code": "Zip Code",
    "latitude_raw": "Latitude",
    "longitude_raw": "Longitude",
    "rentable_square_feet_raw": "Building Rentable Square Feet",
    "available_square_feet_raw": "Available Square Feet",
    "congressional_district": "Congressional District",
}
COMMON = [
    ("snapshot_id", pa.string()),
    ("snapshot_date", pa.date32()),
    *((name, pa.string()) for name in TEXT_FIELDS),
    ("ownership_raw", pa.string()),
    ("ownership", pa.string()),
    ("asset_type", pa.string()),
    ("representative", pa.string()),
    ("area_unit", pa.string()),
    ("coordinate_unit", pa.string()),
    ("address_scope", pa.string()),
    ("location_status", pa.string()),
    ("location_issue", pa.string()),
    ("value_states_json", pa.string()),
    ("source_record_id", pa.string()),
    ("source_sha256", pa.string()),
    ("source_url", pa.string()),
    ("observed_at", pa.string()),
    ("source_sheet", pa.string()),
    ("source_row_number", pa.int64()),
    ("source_locator_json", pa.string()),
    ("raw_fields_json", pa.string()),
    ("raw_cells_json", pa.string()),
]
SCHEMAS = {
    PROPERTIES: pa.schema(COMMON + [("construction_year_raw", pa.string()), ("building_status", pa.string())]),
    LEASES: pa.schema(
        COMMON
        + [("lease_number", pa.string()), ("lease_effective_date", pa.date32()), ("lease_expiration_date", pa.date32())]
    ),
    SNAPSHOTS: pa.schema(
        [
            ("snapshot_id", pa.string()),
            ("snapshot_date", pa.date32()),
            ("dataset_id", pa.string()),
            ("catalog_modified_raw", pa.string()),
            ("data_as_of", pa.string()),
            ("coverage", pa.string()),
            ("properties", pa.int64()),
            ("leases", pa.int64()),
            ("owned_properties", pa.int64()),
            ("leased_properties", pa.int64()),
            ("unmatched_lease_locations", pa.int64()),
            ("source_selection_json", pa.string()),
            ("raw_catalog_json", pa.string()),
        ]
    ),
}
# Exact original reviewed in ss-7ihp. No authoritative replacement coordinate/district is established.
# Other rows are explicitly unvalidated; this bounded finding must never imply a full geography audit.
LOCATION_FINDING = {
    (
        "sha256:b4218dd3debed861c9f416160ff250a58bd186d5a7215c7e1f41f3f37636dc89",
        "AZ0715",
    ): "Unresolved source conflict: Arizona address with Texas coordinates and congressional district; "
    "literal values retained. See docs/gsa-iolp.md raw review.",
    (
        "sha256:b4218dd3debed861c9f416160ff250a58bd186d5a7215c7e1f41f3f37636dc89",
        "SC1001",
    ): "Unresolved source conflict: South Carolina address with coordinates near Mississippi; "
    "literal values retained. See docs/gsa-iolp.md raw review.",
}


def _json(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=lambda value: value.isoformat(),
    )


def _date(raw):
    if raw in (None, ""):
        return None, "null" if raw is None else "empty"
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date(), "value"
        except ValueError:
            pass
    return None, "unparsed"


def _location(fields, issue):
    absent = [
        name
        for name in ("Street Address", "City", "State", "Zip Code", "Latitude", "Longitude")
        if fields[name] in (None, "")
    ]
    if issue:
        return "source_conflict"
    if absent:
        return "missing:" + ",".join(absent)
    try:
        lat, lon = Decimal(fields["Latitude"]), Decimal(fields["Longitude"])
        if not lat.is_finite() or not lon.is_finite() or not -90 <= lat <= 90 or not -180 <= lon <= 180:
            return "invalid_coordinates"
    except InvalidOperation:
        return "invalid_coordinates"
    return "present_unvalidated"


def mapped_rows(inventory, capture, snapshot_id, snapshot_date):
    """Preserve literal quantities and null/empty/absent states; interpret only ownership and lease dates."""
    for row in inventory.rows:
        fields = row["fields"]
        ownership_raw = fields["Owned or Leased" if inventory.kind == "buildings" else "Federal Leased Code"]
        ownership = OWNERSHIP.get(ownership_raw)
        if ownership is None or inventory.kind == "leases" and ownership != "leased":
            raise ValueError("IOLP ownership code changed; review before mapping")
        states = {
            key: "absent"
            if key not in row["cells"]
            else "null"
            if value is None
            else "empty"
            if value == ""
            else "value"
            for key, value in fields.items()
        }
        issue = LOCATION_FINDING.get((capture["sha256"], fields["Location Code"]))
        locator = {
            "url": capture["requested_url"],
            "sha256": capture["sha256"],
            "sheet": inventory.sheet,
            "row": row["row_number"],
        }
        mapped = {
            "snapshot_id": snapshot_id,
            "snapshot_date": date.fromisoformat(snapshot_date),
            **{key: fields[name] for key, name in TEXT_FIELDS.items()},
            "ownership_raw": ownership_raw,
            "ownership": ownership,
            "asset_type": fields[
                "Real Property Asset Type" if inventory.kind == "buildings" else "Real Property Asset type"
            ],
            "representative": fields[
                "Congressional District Representative Name"
                if inventory.kind == "buildings"
                else "Congressional District Representative"
            ],
            "area_unit": "square feet",
            "coordinate_unit": "decimal degrees",
            "address_scope": "source building address; no geocoding or standardized-address claim",
            "location_status": _location(fields, issue),
            "location_issue": issue,
            "value_states_json": _json(states),
            "source_record_id": f"gsa-iolp:{inventory.kind}:{row['native_id']}",
            "source_sha256": capture["sha256"],
            "source_url": capture["requested_url"],
            "observed_at": capture["observed_at"],
            "source_sheet": inventory.sheet,
            "source_row_number": row["row_number"],
            "source_locator_json": _json(locator),
            "raw_fields_json": _json(fields),
            "raw_cells_json": _json(row["cells"]),
        }
        if inventory.kind == "buildings":
            mapped.update(construction_year_raw=fields["Construction Date"], building_status=fields["Building Status"])
        else:
            mapped["lease_number"] = fields["Lease Number"]
            for key, source in (
                ("lease_effective_date", "Lease Effective Date"),
                ("lease_expiration_date", "Lease Expiration Date"),
            ):
                value, status = _date(fields[source])
                mapped[key] = value
                states[key] = status
            mapped["value_states_json"] = _json(states)
        yield mapped


def build_tables(selection, selected, inventories, output_dir: Path):
    """Write one complete pair, including valid empty tables and snapshot reconciliation; no prior is erased."""
    if set(inventories) != {"buildings", "leases"} or set(selected) != set(inventories):
        raise ValueError("IOLP requires both selected workbooks")
    snapshot_id = "gsa-iolp:" + hashlib.sha256(_json(selection).encode()).hexdigest()
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for kind, table in (("buildings", PROPERTIES), ("leases", LEASES)):
        files.append(
            write_rows(
                mapped_rows(inventories[kind], selected[kind], snapshot_id, selection["snapshot_date"]),
                output_dir / f"{table}.parquet",
                SCHEMAS[table],
            )
        )
    properties = inventories["buildings"].rows
    leases = inventories["leases"].rows
    locations = {row["fields"]["Location Code"] for row in properties}
    snapshot = {
        "snapshot_id": snapshot_id,
        "snapshot_date": date.fromisoformat(selection["snapshot_date"]),
        "dataset_id": "GSA-4495",
        "catalog_modified_raw": selection["catalog"].get("modified"),
        "data_as_of": None,
        "coverage": "Declared GSA IOLP distribution pair; GSA-controlled properties, not government-wide FRPP. "
        "Snapshot date is the distribution filename label; effective inventory time is unstated.",
        "properties": len(properties),
        "leases": len(leases),
        "owned_properties": sum(OWNERSHIP[row["fields"]["Owned or Leased"]] == "owned" for row in properties),
        "leased_properties": sum(OWNERSHIP[row["fields"]["Owned or Leased"]] == "leased" for row in properties),
        "unmatched_lease_locations": sum(row["fields"]["Location Code"] not in locations for row in leases),
        "source_selection_json": _json(selection),
        "raw_catalog_json": _json(selection["catalog"]),
    }
    files.append(write_rows([snapshot], output_dir / f"{SNAPSHOTS}.parquet", SCHEMAS[SNAPSHOTS]))
    return files
