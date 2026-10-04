"""Contracts for scoping an already-built cluster table.

Adding three derived columns by re-streaming the 2.3 GiB dump is 23 minutes of
reading for facts the table already has the key for, so the backfill joins the
docket→court map against the table on disk instead. It has one interesting
decision in it: the better artifact — the whole cluster table rewritten with the
columns inline — costs a second copy of a 3.9 GB file, and on the machine this
was written for that crosses the project's free-space floor. So the mode is
chosen by what fits, and the mode actually used is recorded, because a run that
quietly produced the lesser artifact is the shape of degradation this ingest
keeps having to guard against.
"""

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
    clusters = tmp_path / "court_opinion_clusters.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"cluster_id": "1", "cl_docket_id": "10", "case_name": "Federal"},
                {"cluster_id": "2", "cl_docket_id": "11", "case_name": "State"},
                {"cluster_id": "3", "cl_docket_id": "99", "case_name": "Unplaced"},
                {"cluster_id": "4", "cl_docket_id": None, "case_name": "No docket"},
            ],
            schema=pa.schema(
                [
                    ("cluster_id", pa.string()),
                    ("cl_docket_id", pa.string()),
                    ("case_name", pa.string()),
                ]
            ),
        ),
        clusters,
    )
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


def _run(tmp_path: Path, mode: str, monkeypatch) -> dict:
    clusters, docket_map, courts = _fixtures(tmp_path)
    if mode == "full":
        # The real guard refuses this on the machine it was written for; the
        # behaviour under test here is the rewrite, not the arithmetic.
        monkeypatch.setattr(backfill_module, "check_headroom", lambda *a, **k: None)
        monkeypatch.setattr(backfill_module, "_fits", lambda *a, **k: True)
    return backfill_module.backfill(
        clusters=clusters,
        docket_court_map=docket_map,
        courts_dump=courts,
        output_dir=tmp_path / "out",
        dump_date=date(2026, 6, 30),
        mode=mode,
        allow_legacy_input=True,
    )


def test_scope_mode_refuses_an_unnecessary_parallel_subject_table(tmp_path: Path, monkeypatch):
    with pytest.raises(ValueError, match="retired"):
        _run(tmp_path, "scope", monkeypatch)
    assert not (tmp_path / "out" / "court_cluster_scope.parquet").exists()


def test_full_mode_keeps_every_original_column_and_the_published_order(tmp_path: Path, monkeypatch):
    """The scope belongs next to the key it is derived from, not bolted on the end."""
    receipt = _run(tmp_path, "full", monkeypatch)
    table = pq.read_table(tmp_path / "out" / "court_opinion_clusters.parquet")

    assert table.schema == SUBJECT_SCHEMAS['court_opinion_clusters']
    rows = table.to_pylist()
    assert rows[0]["case_name"] == "Federal"
    assert rows[0]["court_jurisdiction"] == "FD"
    assert receipt["mode"] == "full"
    assert receipt["coverage"]["rows_written"] == 4


def test_auto_refuses_before_crossing_the_disk_floor(tmp_path: Path, monkeypatch):
    def refuse(*args, **kwargs):
        raise RuntimeError("disk floor")
    monkeypatch.setattr(backfill_module, "check_headroom", refuse)
    with pytest.raises(RuntimeError, match="disk floor"):
        _run(tmp_path, "auto", monkeypatch)
    assert not (tmp_path / "out" / "court_cluster_scope.parquet").exists()
    assert not (tmp_path / "out" / "court_opinion_clusters.parquet").exists()
