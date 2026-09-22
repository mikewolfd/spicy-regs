"""A pinned local edition must not acquire or mutate a different prior."""

from __future__ import annotations

import bz2
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.courtlistener import bulk
from spicy_regs.sources import r2
from spicy_regs.transforms.build_court_opinion_clusters import build_court_opinion_clusters


@pytest.mark.parametrize("retained_prior", [False, True])
def test_local_edition_uses_pinned_scope_and_preserves_prior(tmp_path: Path, monkeypatch, retained_prior):
    def forbidden(*args, **kwargs):
        pytest.fail("local edition attempted remote acquisition or search")

    monkeypatch.setattr(r2, "download", forbidden)
    monkeypatch.setattr(bulk, "list_bulk_dumps", forbidden)
    monkeypatch.setattr(
        "spicy_regs.sources.courtlistener.CourtListenerOpinionSearchReader.iter_records", forbidden
    )
    prior = tmp_path / "_clusters_prior.parquet"
    if retained_prior:
        # Deliberately unreadable: it must be ignored, not merely lose a merge.
        prior.write_bytes(b"retained caller evidence")
    clusters = tmp_path / "opinion-clusters.csv.bz2"
    clusters.write_bytes(bz2.compress(b'id,docket_id,case_name,slug\n"1","71","Alpha","alpha"\n'))
    courts = tmp_path / "courts.csv.bz2"
    courts.write_bytes(bz2.compress(b'id,jurisdiction\n"dcd","FD"\n'))
    docket_map = tmp_path / "map.parquet"
    pq.write_table(pa.Table.from_pylist([{"cl_docket_id": "71", "court_id": "dcd"}]), docket_map)
    output = build_court_opinion_clusters(
        tmp_path,
        dump_date=date(2026, 6, 30),
        local_file=clusters,
        docket_court_map=docket_map,
        courts_local_file=courts,
        skip_search_catchup=True,
        include_prior=False,
    )
    [row] = pq.read_table(output).to_pylist()
    assert (row["cluster_id"], row["case_name"], row["court_id"], row["court_jurisdiction"], row["court_is_federal"]) == (
        "1", "Alpha", "dcd", "FD", "t"
    )
    assert not (tmp_path / "_clusters_new.parquet").exists()
    if retained_prior:
        assert prior.read_bytes() == b"retained caller evidence"
    else:
        assert not prior.exists()
