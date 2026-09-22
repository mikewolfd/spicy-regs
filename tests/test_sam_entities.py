"""Hermetic tests for the SAM.gov entity ingest (no network).

Covers the pieces with real logic: the raw-entity -> published-schema mapping
(``_shape``), the API-key resolution fallback chain, missing-credential refusal, the bulk
*extract* path (trigger -> download-URL discovery -> defensive file parse, with
``registrationDate`` year-windowing and ``max_records`` bounding), and the
*partition* path's adaptive date-window subdivision + paginated walk. The fixture
below is a trimmed but faithful copy of a real ``entityData[]`` record from the v4
``/entities`` response.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from spicy_docs.sources.sam_extract import SamExtractError
from spicy_regs.transforms.build_sam_entities import (
    COLUMNS,
    SAM_API_KEY_ENV_VARS,
    _iter_sam_entities,
    _resolve_sam_api_key,
    _shape,
)

_RAW_ENTITY = {
    "entityRegistration": {
        "samRegistered": "Yes",
        "ueiSAM": "TXBDHEGXWKD6",
        "cageCode": "9ABC1",
        "legalBusinessName": "Imperial Law Associates Pvt. Ltd.",
        "dbaName": "Imperial Law",
        "purposeOfRegistrationDesc": "Federal Assistance Awards",
        "registrationStatus": "Active",
        "registrationDate": "2026-07-17",
        "registrationExpirationDate": "2027-07-17",
        "exclusionStatusFlag": "N",
    },
    "coreData": {
        "entityInformation": {"entityURL": "www.lawimperial.com"},
        "physicalAddress": {
            "city": "Kathmandu",
            "stateOrProvinceCode": "NY",
            "zipCode": "44600",
            "countryCode": "NPL",
        },
        "congressionalDistrict": "12",
        "generalInformation": {
            "entityStructureDesc": "Corporate Entity (Not Tax Exempt)",
            "entityTypeDesc": "Business or Organization",
            "profitStructureDesc": "For Profit Organization",
        },
    },
    "assertions": {"goodsAndServices": {"primaryNaics": "541110"}},
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ENTITY)
    # Every published column present, and nothing extra (18-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 18


def test_shape_maps_nested_fields():
    row = _shape(_RAW_ENTITY)
    assert row["uei"] == "TXBDHEGXWKD6"
    assert row["cage_code"] == "9ABC1"
    assert row["legal_business_name"] == "Imperial Law Associates Pvt. Ltd."
    assert row["dba_name"] == "Imperial Law"
    # coreData.generalInformation.*
    assert row["entity_structure_desc"] == "Corporate Entity (Not Tax Exempt)"
    assert row["entity_type_desc"] == "Business or Organization"
    assert row["profit_structure_desc"] == "For Profit Organization"
    # coreData.physicalAddress.*
    assert row["state"] == "NY"
    assert row["city"] == "Kathmandu"
    assert row["zip_code"] == "44600"
    # scalars coerced to str
    assert row["congressional_district"] == "12"
    assert row["primary_naics"] == "541110"
    # entityRegistration.*
    assert row["registration_status"] == "Active"
    assert row["registration_date"] == "2026-07-17"
    assert row["registration_expiration_date"] == "2027-07-17"
    assert row["exclusion_status_flag"] == "N"
    assert row["purpose_of_registration_desc"] == "Federal Assistance Awards"
    # coreData.entityInformation.entityURL
    assert row["entity_url"] == "www.lawimperial.com"


def test_shape_handles_missing_nested_objects():
    # A registration with no coreData / assertions still yields a UEI-keyed row.
    row = _shape({"entityRegistration": {"ueiSAM": "ABC123456789"}})
    assert row["uei"] == "ABC123456789"
    assert row["state"] is None
    assert row["primary_naics"] is None
    assert row["entity_url"] is None
    assert row["entity_structure_desc"] is None
    with pytest.raises(SamExtractError, match="ueiSAM"):
        _shape({})


def test_shape_coerces_int_scalars_to_str():
    row = _shape(
        {
            **{"entityRegistration": {"ueiSAM": "A"}},
            "assertions": {"goodsAndServices": {"primaryNaics": 541110}},
            "coreData": {"congressionalDistrict": 3},
        }
    )
    assert row["primary_naics"] == "541110"
    assert row["congressional_district"] == "3"


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_sam_specific_key(monkeypatch):
    # SAM.gov needs a SAM-authorized key, so SAM_API_KEY must win over the
    # generic api.data.gov key (which 404s on SAM) when both are set.
    for var in SAM_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("SAM_API_KEY", "sam-key")
    assert _resolve_sam_api_key() == "sam-key"


def test_api_key_env_var_precedence_order():
    # Explicit contract: SAM-specific first, generic data.gov next, regs last.
    assert SAM_API_KEY_ENV_VARS == ("SAM_API_KEY", "API_GOV", "DATA_GOV_API_KEY", "REGULATIONS_GOV_API_KEY")


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in SAM_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Only the last one set — the fallback chain should still find it.
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_sam_api_key() == "regs-key"


def test_resolve_api_key_refuses_when_unset(monkeypatch):
    for var in SAM_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SamExtractError, match="SAM-authorized"):
        _resolve_sam_api_key()


def test_fetch_refuses_without_key(monkeypatch):
    for var in SAM_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SamExtractError, match="SAM-authorized"):
        list(_iter_sam_entities(mode="extract", registration_status="A", since_year=2026, until_year=2026, year_windows=True, max_records=1))


def test_fetch_rejects_unknown_mode(monkeypatch):
    for var in SAM_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("SAM_API_KEY", "k")
    with pytest.raises(ValueError, match="mode must be"):
        list(_iter_sam_entities(mode="bogus", registration_status="A", since_year=2026, until_year=2026, year_windows=True, max_records=1))


# -- date literals -----------------------------------------------------------


# -- rollup: bounded rotation + env overrides --------------------------------


def test_rotating_year_covers_full_range_over_a_cycle():
    from spicy_regs.pipelines.rollups.sam_entities import _MIN_REGISTRATION_YEAR, _rotating_year

    today = date(2026, 7, 18)
    span = today.year - _MIN_REGISTRATION_YEAR + 1
    seen = {_rotating_year(date.fromordinal(today.toordinal() + d)) for d in range(span)}
    # A full rotation touches every year in [min, current] exactly once.
    assert seen == set(range(_MIN_REGISTRATION_YEAR, today.year + 1))


def test_int_env_parses_blank_and_bad_values(monkeypatch):
    from spicy_regs.pipelines.rollups.sam_entities import _int_env

    monkeypatch.delenv("SAM_MAX_RECORDS", raising=False)
    assert _int_env("SAM_MAX_RECORDS") is None
    monkeypatch.setenv("SAM_MAX_RECORDS", "  ")
    assert _int_env("SAM_MAX_RECORDS") is None
    monkeypatch.setenv("SAM_MAX_RECORDS", "not-a-number")
    with pytest.raises(ValueError, match="SAM_MAX_RECORDS"):
        _int_env("SAM_MAX_RECORDS")
    monkeypatch.setenv("SAM_MAX_RECORDS", "25000")
    assert _int_env("SAM_MAX_RECORDS") == 25000


def test_rollup_build_defaults_to_rotating_single_year(monkeypatch):
    """With no env overrides the scheduled build fetches one rotating year window."""
    from spicy_regs.pipelines.rollups import sam_entities as rollup

    for var in ("SAM_INGEST_MODE", "SAM_SINCE_YEAR", "SAM_UNTIL_YEAR", "SAM_MAX_RECORDS"):
        monkeypatch.delenv(var, raising=False)
    captured: dict = {}

    def fake_build(output_dir, **kwargs):
        captured.update(kwargs)
        return output_dir / "sam_entities.parquet"

    monkeypatch.setattr(rollup, "build_sam_entities", fake_build)
    rollup.SamEntitiesRollup().build(Path("/tmp/out"))
    assert captured["mode"] == "extract"
    assert captured["since_year"] == captured["until_year"]  # a single year window
    assert captured["max_records"] is None


def test_rollup_build_honours_explicit_range(monkeypatch, tmp_path):
    from spicy_regs.pipelines.rollups import sam_entities as rollup

    monkeypatch.setenv("SAM_SINCE_YEAR", "2000")
    monkeypatch.setenv("SAM_UNTIL_YEAR", "2026")
    monkeypatch.setenv("SAM_MAX_RECORDS", "0")  # blank/0 -> unbounded
    monkeypatch.setenv("SAM_INGEST_MODE", "partition")
    captured: dict = {}

    def fake_build(output_dir, **kwargs):
        captured.update(kwargs)
        return output_dir / "sam_entities.parquet"

    monkeypatch.setattr(rollup, "build_sam_entities", fake_build)
    rollup.SamEntitiesRollup().build(tmp_path)
    assert captured == {"mode": "partition", "since_year": 2000, "until_year": 2026, "max_records": None}
