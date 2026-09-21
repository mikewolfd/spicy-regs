"""CRS field mapping and credential resolution; source refusals are tested in test_reference_source_failures."""

from __future__ import annotations

from spicy_regs.sources.crs_reports import (
    API_KEY_ENV_VARS,
    _resolve_api_key,
)
from spicy_regs.transforms.build_crs_reports import COLUMNS, _shape

_RAW_REPORT = {
    "contentType": "Reports",
    "id": "R48641",
    "publishDate": "2026-07-16T04:00:00Z",
    "status": "Active",
    "title": "Proposals to Limit Member of Congress Financial Activities",
    "updateDate": "2026-07-17T22:38:55Z",
    "url": "https://api.congress.gov/v3/crsreport/R48641",
    "version": 15,
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_REPORT)
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 8


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_REPORT)
    assert row["report_id"] == "R48641"
    assert row["title"] == "Proposals to Limit Member of Congress Financial Activities"
    assert row["report_type"] == "Reports"
    assert row["status"] == "Active"
    assert row["published_date"] == "2026-07-16T04:00:00Z"
    assert row["update_date"] == "2026-07-17T22:38:55Z"
    # version int is stringified.
    assert row["version"] == "15"
    assert row["url"] == "https://api.congress.gov/v3/crsreport/R48641"


def test_shape_handles_missing_scalars():
    row = _shape({"id": "IN12713"})
    assert row["report_id"] == "IN12713"
    assert row["title"] is None
    assert row["report_type"] is None
    assert row["version"] is None


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("CONGRESS_GOV_API_KEY", "congress-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None
