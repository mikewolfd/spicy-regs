"""Inline court scope enrichment preserves native subjects and exact source receipts."""

from __future__ import annotations

import bz2
import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest
import pyarrow as pa
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "backfill_cluster_court_scope",
    Path(__file__).resolve().parents[1] / "scripts" / "backfill_cluster_court_scope.py",
)
assert _SPEC and _SPEC.loader
backfill_module = importlib.util.module_from_spec(_SPEC)
sys.modules["backfill_cluster_court_scope"] = backfill_module
_SPEC.loader.exec_module(backfill_module)


def _fixtures(tmp_path: Path) -> tuple[Path, Path, Path]:
    from spicy_regs.court_receipts import write_court_rows
    witness = dict(source_id='held-clusters', source_uri=None, sha256='sha256:' + 'a' * 64,
                   locator=None, body_version=None)
    clusters = write_court_rows('court_opinion_clusters', [
        {"cluster_id": "1", "cl_docket_id": "10", "case_name": "Federal"},
        {"cluster_id": "2", "cl_docket_id": "11", "case_name": "State"},
        {"cluster_id": "3", "cl_docket_id": "99", "case_name": "Unplaced"},
        {"cluster_id": "4", "cl_docket_id": None, "case_name": "No docket"},
    ], tmp_path, witnesses=[witness])
    docket_map = tmp_path / "docket_courts.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"cl_docket_id": "10", "court_id": "dcd"},
                {"cl_docket_id": "11", "court_id": "nc"},
            ],
            schema=pa.schema([("cl_docket_id", pa.string()), ("court_id", pa.string())]),
        ),
        docket_map,
    )
    courts = tmp_path / "courts-2026-06-30.csv.bz2"
    courts.write_bytes(bz2.compress(b"id,jurisdiction\ndcd,FD\nnc,S\n"))
    return clusters, docket_map, courts


def _run(tmp_path: Path) -> dict:
    clusters, docket_map, courts = _fixtures(tmp_path)
    return backfill_module.backfill(
        clusters=clusters,
        docket_court_map=docket_map,
        courts_dump=courts,
        output_dir=tmp_path / "out",
        dump_date=date(2026, 6, 30),
    )


def test_scope_enrichment_keeps_every_original_column_and_the_published_order(tmp_path: Path, monkeypatch):
    """The scope belongs next to the key it is derived from, not bolted on the end."""
    monkeypatch.setattr(backfill_module, "check_headroom", lambda *a, **k: None)
    receipt = _run(tmp_path)
    table = pq.read_table(tmp_path / "out" / "court_opinion_clusters.parquet")

    assert table.schema == SUBJECT_SCHEMAS['court_opinion_clusters']
    rows = table.to_pylist()
    assert rows[0]["case_name"] == "Federal"
    assert rows[0]["court_jurisdiction"] == "FD"
    assert not (tmp_path / 'out/court_cluster_scope.parquet').exists()
    assert receipt["coverage"]["rows_written"] == 4


def test_refuses_before_crossing_the_disk_floor(tmp_path: Path, monkeypatch):
    def refuse(*args, **kwargs):
        raise RuntimeError("disk floor")
    monkeypatch.setattr(backfill_module, "check_headroom", refuse)
    with pytest.raises(RuntimeError, match="disk floor"):
        _run(tmp_path)
    assert not (tmp_path / "out" / "court_cluster_scope.parquet").exists()
    assert not (tmp_path / "out" / "court_opinion_clusters.parquet").exists()
