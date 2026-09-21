"""The discovery table must preserve the provider inventory without coverage claims."""

import hashlib
import json
from importlib.resources import files

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_fec_source_catalog import build_fec_source_catalog


def test_all_official_families_preserve_original_metadata(tmp_path):
    raw = files("spicy_docs.sources.fec").joinpath("official_sources.json").read_bytes()
    native = {row["id"]: row for row in json.loads(raw)}
    rows = pq.read_table(build_fec_source_catalog(tmp_path)).to_pylist()
    assert {row["source_family"] for row in rows} == native.keys()
    assert len(rows) == len(native)
    for row in rows:
        original = native[row["source_family"]]
        assert json.loads(row["source_metadata_json"]) == original
        assert json.loads(row["references_json"]) == original.get("references", [])
        assert json.loads(row["indexes_json"]) == original.get("indexes", [])
        assert row["catalog_sha256"] == hashlib.sha256(raw).hexdigest()
        assert "does not establish acquired records" in row["coverage_note"]


def test_unknown_reader_route_preserves_previous_catalog(tmp_path, monkeypatch):
    from spicy_docs.sources.fec import catalog

    target = build_fec_source_catalog(tmp_path)
    previous = target.read_bytes()
    monkeypatch.setattr(catalog, "api_operations", lambda: {})
    with pytest.raises(ValueError, match="no reader operation"):
        build_fec_source_catalog(tmp_path)
    assert target.read_bytes() == previous
