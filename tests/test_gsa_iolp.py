"""Domain controls for literal values, uncertain locations, snapshot-only links and native lease identities."""

import json
from datetime import date

import duckdb
import pytest
from spicy_docs.sources.gsa_iolp import HEADERS, Inventory

from spicy_regs.relationship_views.gsa_iolp import GSA_IOLP_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.build_gsa_iolp import LOCATION_FINDING, SCHEMAS, build_tables, mapped_rows


def inventory(kind, key="CT0001", **overrides):
    fields = dict.fromkeys(HEADERS[kind])
    fields.update(
        {
            "Location Code": key,
            "Owned or Leased": "LEASED",
            "Federal Leased Code": "L",
            "Lease Number": "LCT00001",
            "Lease Effective Date": "7/27/2019",
            "Lease Expiration Date": "7/26/2034",
            "State": "CT",
            "Zip Code": "06320-6309",
            "Latitude": "41.3542597",
            "Longitude": "-72.094346999999999",
            "Building Rentable Square Feet": "7276",
            "Available Square Feet": None,
        }
    )
    fields = {name: overrides.get(name, fields[name]) for name in HEADERS[kind]}
    cells = {name: {"coordinate": "A2", "value": value} for name, value in fields.items() if name != "City"}
    return Inventory(
        kind,
        "Sheet1",
        HEADERS[kind],
        ({"native_id": key if kind == "buildings" else "LCT00001", "row_number": 2, "fields": fields, "cells": cells},),
    )


CAPTURE = {
    "sha256": "sha256:" + "0" * 64,
    "requested_url": "https://inventory.data.gov/source.xlsx",
    "observed_at": "2026-11-13T11:00:00Z",
}


def test_nulls_zero_source_units_and_lease_dates_are_not_conflated():
    row = next(
        mapped_rows(
            inventory("leases", **{"Installation Name": "", "Building Rentable Square Feet": "0"}),
            CAPTURE,
            "snapshot",
            "2026-11-06",
        )
    )
    assert row["lease_number"] == "LCT00001" and row["zip_code"] == "06320-6309"
    assert row["available_square_feet_raw"] is None and row["rentable_square_feet_raw"] == "0"
    assert row["area_unit"] == "square feet" and row["coordinate_unit"] == "decimal degrees"
    assert row["lease_effective_date"] == date(2019, 7, 27)
    assert row["lease_expiration_date"] == date(2034, 7, 26)
    states = json.loads(row["value_states_json"])
    assert states["Available Square Feet"] == "null" and states["Installation Name"] == "empty"
    assert states["City"] == "absent" and row["location_status"].startswith("missing:")


def test_conflicting_source_location_keeps_original_values_and_unresolved_issue():
    sha, key = next(iter(LOCATION_FINDING))
    row = next(
        mapped_rows(
            inventory("buildings", key, **{"State": "AZ"}), {**CAPTURE, "sha256": sha}, "snapshot", "2026-11-06"
        )
    )
    assert row["state"] == "AZ" and row["longitude_raw"] == "-72.094346999999999"
    assert row["location_status"] == "source_conflict" and row["location_issue"].startswith("Unresolved")


def test_unknown_ownership_refuses_and_invalid_date_remains_unparsed():
    with pytest.raises(ValueError, match="ownership code changed"):
        list(mapped_rows(inventory("buildings", **{"Owned or Leased": "X"}), CAPTURE, "snapshot", "2026-11-06"))
    row = next(
        mapped_rows(inventory("leases", **{"Lease Effective Date": "not-a-date"}), CAPTURE, "snapshot", "2026-11-06")
    )
    assert row["lease_effective_date"] is None
    assert json.loads(row["value_states_json"])["lease_effective_date"] == "unparsed"
    assert json.loads(row["raw_fields_json"])["Lease Effective Date"] == "not-a-date"


def test_complete_tables_reconcile_unmatched_locations_without_dropping_leases(tmp_path):
    selected = {kind: CAPTURE for kind in HEADERS}
    scope = {"snapshot_date": "2026-11-06", "catalog": {"modified": "2026-11-06T00:00:00Z"}}
    inputs = {"buildings": inventory("buildings"), "leases": inventory("leases", "CT9999")}
    files = build_tables(scope, selected, inputs, tmp_path)
    con = duckdb.connect()
    for path in files:
        con.read_parquet(str(path)).create_view(path.stem)
    snapshot = con.execute("SELECT properties, leases, unmatched_lease_locations, data_as_of FROM gsa_iolp_snapshots")
    assert snapshot.fetchone() == (1, 1, 1, None)
    install_sql_views(con, SCHEMAS.keys(), GSA_IOLP_VIEWS)
    assert con.execute("SELECT lease_number, property_matched FROM gsa_iolp_property_leases").fetchall() == [
        ("LCT00001", False)
    ]
    # A native key in a different snapshot cannot satisfy the location join.
    con.execute("CREATE TABLE properties_copy AS SELECT * FROM gsa_iolp_properties")
    con.execute("UPDATE properties_copy SET snapshot_id='older', location_code='CT9999'")
    con.execute("CREATE OR REPLACE VIEW gsa_iolp_properties AS SELECT * FROM properties_copy")
    assert con.execute("SELECT property_matched FROM gsa_iolp_property_leases").fetchone() == (False,)
    con.close()


def test_incomplete_pair_has_no_accepted_snapshot(tmp_path):
    with pytest.raises(ValueError, match="both selected workbooks"):
        build_tables({}, {}, {"buildings": inventory("buildings")}, tmp_path)
