"""Hermetic tests for the OpenFEC committees ingest (no network).

Covers the pieces with real logic: the raw-committee → published-schema mapping
(``_shape`` / ``_json_array``), the API-key resolution fallback chain, and the
keyset-preferred / offset-fallback pagination (which must *not* stop early on a
short page — that was the bug that truncated the backfill at ~26K of ~89K).
"""

from __future__ import annotations

import json
import importlib

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.fec_committees import (
    API_KEY_ENV_VARS,
    FecCommitteesReader,
    _clean_cursor,
    _resolve_api_key,
)
from spicy_regs.transforms.build_fec_committees import COLUMNS, _shape
from spicy_regs.transforms.build_fec_committees import write_fec_committee_rows

_RAW_COMMITTEE = {
    "committee_id": "C00684373",
    "name": "EXAMPLE FOR PRESIDENT",
    "committee_type": "P",
    "committee_type_full": "Presidential",
    "designation": "P",
    "designation_full": "Principal campaign committee",
    "party": "DEM",
    "party_full": "DEMOCRATIC PARTY",
    "state": "MA",
    "treasurer_name": "DOE, JANE",
    "organization_type": None,
    "organization_type_full": None,
    "filing_frequency": "A",
    "first_file_date": "2018-08-03",
    "last_file_date": "2021-02-01",
    "cycles": [2018, 2020, 2022],
    "candidate_ids": ["P00008052"],
    # Fields present in the payload but intentionally not published:
    "affiliated_committee_name": "NONE",
    "designated_agent_name": "DOE, JANE",
}


def test_retained_rows_use_bounded_batches_and_existing_shape(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    real_writer = module.pq.ParquetWriter
    written = []

    class ObservedWriter:
        def __init__(self, *args, **kwargs):
            self.writer = real_writer(*args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *error):
            self.writer.close()

        def write_table(self, table):
            written.append(table.num_rows)
            self.writer.write_table(table)

    monkeypatch.setattr(module.pq, "ParquetWriter", ObservedWriter)

    def records():
        for index in range(5):
            if index >= 2:
                assert sum(written) >= (index // 2) * 2
            yield {**_RAW_COMMITTEE, "committee_id": f"C{index:08}"}

    target = write_fec_committee_rows(records(), tmp_path / "selected.parquet", batch_size=2)
    assert written == [2, 2, 1]
    table = pq.read_table(target)
    assert table.column_names == list(COLUMNS)
    assert table.to_pylist() == [_shape({**_RAW_COMMITTEE, "committee_id": f"C{i:08}"}) for i in range(5)]


def test_failed_retained_input_preserves_previous_output(tmp_path):
    target = write_fec_committee_rows([_RAW_COMMITTEE], tmp_path / "selected.parquet")
    prior = target.read_bytes()

    def failed():
        yield {**_RAW_COMMITTEE, "name": "CHANGED BEFORE FAILURE"}
        raise RuntimeError("retained input failed verification")

    with pytest.raises(RuntimeError, match="failed verification"):
        write_fec_committee_rows(failed(), target, batch_size=1)
    assert target.read_bytes() == prior
    assert list(tmp_path.iterdir()) == [target]


def test_empty_retained_input_keeps_schema_and_duplicate_rows_survive(tmp_path):
    target = write_fec_committee_rows(iter(()), tmp_path / "empty.parquet")
    assert pq.read_table(target).column_names == list(COLUMNS)
    assert pq.ParquetFile(target).metadata.num_rows == 0
    write_fec_committee_rows([_RAW_COMMITTEE, _RAW_COMMITTEE], target)
    assert pq.read_table(target).to_pylist() == [_shape(_RAW_COMMITTEE)] * 2


@pytest.mark.parametrize("value", [0, -1, True])
def test_retained_input_rejects_invalid_batch_bound(tmp_path, value):
    with pytest.raises(ValueError, match="batch_size"):
        write_fec_committee_rows([], tmp_path / "unused.parquet", batch_size=value)


def test_default_builder_uses_the_shared_row_writer(tmp_path, monkeypatch):
    module = importlib.import_module("spicy_regs.transforms.build_fec_committees")
    monkeypatch.setattr(module.r2, "download", lambda *a: False)
    monkeypatch.setattr(module.FecCommitteesReader, "iter_records", lambda self: iter([_RAW_COMMITTEE]))
    real_write = module.write_fec_committee_rows
    calls = []

    def write(records, destination):
        calls.append(destination.name)
        return real_write(records, destination, batch_size=1)

    monkeypatch.setattr(module, "write_fec_committee_rows", write)
    monkeypatch.setenv("FEC_API_KEY", "test-key")
    result = module.build_fec_committees(tmp_path)
    assert calls == ["_fec_new.parquet"]
    assert pq.read_table(result).to_pylist() == [_shape(_RAW_COMMITTEE)]


def test_shape_produces_exact_schema():
    row = _shape(_RAW_COMMITTEE)
    # Every published column present, and nothing extra (16-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 16


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_COMMITTEE)
    assert row["committee_id"] == "C00684373"
    assert row["name"] == "EXAMPLE FOR PRESIDENT"
    assert row["committee_type"] == "P"
    assert row["committee_type_full"] == "Presidential"
    assert row["designation_full"] == "Principal campaign committee"
    assert row["party_full"] == "DEMOCRATIC PARTY"
    assert row["state"] == "MA"
    assert row["treasurer_name"] == "DOE, JANE"
    assert row["first_file_date"] == "2018-08-03"
    assert row["last_file_date"] == "2021-02-01"
    # Array fields are serialized to JSON strings.
    assert json.loads(row["cycles_json"]) == [2018, 2020, 2022]
    assert json.loads(row["candidate_ids_json"]) == ["P00008052"]


def test_shape_handles_missing_fields_and_null_arrays():
    row = _shape({"committee_id": "C99999999"})
    assert row["committee_id"] == "C99999999"
    # Missing scalars degrade to null, not KeyError.
    assert row["name"] is None
    assert row["organization_type_full"] is None
    assert row["party_full"] is None
    # Missing / null array fields serialize to an empty JSON array, not null.
    assert row["cycles_json"] == "[]"
    assert row["candidate_ids_json"] == "[]"


def test_shape_omits_unpublished_organization_type_code():
    # organization_type (the bare code) is not in the pinned schema; only
    # organization_type_full is published.
    assert "organization_type" not in _shape(_RAW_COMMITTEE)


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("FEC_API_KEY", "fec-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Only the last one set — the fallback chain should still find it.
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None


def test_reader_yields_nothing_without_key(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reader = FecCommitteesReader()
    # No key configured: a keyless run is a no-op, not a crash.
    assert list(reader.iter_records()) == []


# -- pagination --------------------------------------------------------------


def _committee(cid: str) -> dict:
    return {"committee_id": cid}


def test_clean_cursor_extracts_keyset_or_empty():
    # A real seek cursor is returned as-is (null-valued keys dropped).
    assert _clean_cursor({"last_index": "42", "last_committee_id": "C9", "x": None}) == {
        "last_index": "42",
        "last_committee_id": "C9",
    }
    # /committees returns last_indexes: null -> no cursor -> offset fallback.
    assert _clean_cursor(None) == {}
    # An all-null cursor is likewise unusable.
    assert _clean_cursor({"last_index": None}) == {}


def test_paginate_offset_walk_stops_only_on_empty_page(monkeypatch):
    """With ``last_indexes: null`` (the /committees case) walk by page to empty."""
    reader = FecCommitteesReader(api_key="test", per_page=2)
    pages = {
        1: {"results": [_committee("C1"), _committee("C2")], "pagination": {"page": 1, "last_indexes": None}},
        2: {"results": [_committee("C3"), _committee("C4")], "pagination": {"page": 2, "last_indexes": None}},
        3: {"results": [_committee("C5")], "pagination": {"page": 3, "last_indexes": None}},
        4: {"results": [], "pagination": {"page": 4, "last_indexes": None}},
    }
    requested: list[tuple[int, dict]] = []

    def stub(page, keyset):
        requested.append((page, dict(keyset)))
        return pages.get(page, {"results": []})

    monkeypatch.setattr(reader, "_get_page", stub)
    got = [c["committee_id"] for c in reader._paginate()]
    assert got == ["C1", "C2", "C3", "C4", "C5"]
    # Offset fallback: page increments, no keyset cursor ever passed.
    assert [p for p, _ in requested] == [1, 2, 3, 4]
    assert all(ks == {} for _, ks in requested)


def test_paginate_does_not_stop_on_short_page(monkeypatch):
    """Regression: a short mid-walk page must NOT terminate the walk (the bug)."""
    reader = FecCommitteesReader(api_key="test", per_page=3)
    pages = {
        1: {"results": [_committee("C1"), _committee("C2")], "pagination": {"last_indexes": None}},  # short!
        2: {"results": [_committee("C3"), _committee("C4"), _committee("C5")], "pagination": {"last_indexes": None}},
        3: {"results": [], "pagination": {"last_indexes": None}},
    }
    monkeypatch.setattr(reader, "_get_page", lambda page, keyset: pages.get(page, {"results": []}))
    got = [c["committee_id"] for c in reader._paginate()]
    # Old code broke after C1/C2 on the short page; the walk must continue.
    assert got == ["C1", "C2", "C3", "C4", "C5"]


def test_paginate_follows_keyset_cursor_when_present(monkeypatch):
    """When ``last_indexes`` is present, carry the cursor forward, don't page."""
    reader = FecCommitteesReader(api_key="test", per_page=2)
    by_cursor = {
        None: {
            "results": [_committee("C1"), _committee("C2")],
            "pagination": {"last_indexes": {"last_index": "C2", "last_committee_id": "C2"}},
        },
        "C2": {
            "results": [_committee("C3"), _committee("C4")],
            "pagination": {"last_indexes": {"last_index": "C4", "last_committee_id": "C4"}},
        },
        "C4": {"results": [], "pagination": {"last_indexes": None}},
    }
    seen_pages: list[int] = []

    def stub(page, keyset):
        seen_pages.append(page)
        return by_cursor.get(keyset.get("last_index"), {"results": []})

    monkeypatch.setattr(reader, "_get_page", stub)
    got = [c["committee_id"] for c in reader._paginate()]
    assert got == ["C1", "C2", "C3", "C4"]
    # Keyset walk: page never advances past 1 (the cursor does the seeking).
    assert seen_pages == [1, 1, 1]


def test_paginate_respects_max_pages_cap(monkeypatch):
    """``max_pages`` caps the number of fetches regardless of more data."""
    reader = FecCommitteesReader(api_key="test", per_page=1, max_pages=1)
    pages = {
        1: {"results": [_committee("C1")], "pagination": {"last_indexes": None}},
        2: {"results": [_committee("C2")], "pagination": {"last_indexes": None}},
    }
    monkeypatch.setattr(reader, "_get_page", lambda page, keyset: pages.get(page, {"results": []}))
    got = [c["committee_id"] for c in reader._paginate()]
    assert got == ["C1"]
