"""Contracts for the court-data coverage report's arithmetic.

Every number in ``docs/evidence/court-data-coverage-2026-08-22.md`` comes from
``scripts/verify_court_coverage.py``, which makes a mis-read of its output a
published claim. That has happened once already, and the failure was not a
wrong query — it was subtracting a count of *clusters* from a count of
*opinions* and reporting the remainder as 12,666 opinions whose cluster was
missing from the dump. Every one of those opinions had a cluster. They were
concurrences and dissents sharing a cluster with the opinion ahead of them,
which is the grain the table exists to have.

So the property under test is not "the join works". It is that the report can
distinguish a sibling opinion from an orphaned one, because a coverage report
that cannot is worse than no report.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_SPEC = importlib.util.spec_from_file_location(
    "verify_court_coverage",
    Path(__file__).resolve().parents[1] / "scripts" / "verify_court_coverage.py",
)
assert _SPEC and _SPEC.loader
verify_court_coverage = importlib.util.module_from_spec(_SPEC)
sys.modules["verify_court_coverage"] = verify_court_coverage
_SPEC.loader.exec_module(verify_court_coverage)


def _parquet(path: Path, rows: list[dict[str, str | None]]) -> Path:
    columns = sorted({key for row in rows for key in row})
    table = pa.Table.from_pylist(
        [{column: row.get(column) for column in columns} for row in rows],
        schema=pa.schema([(column, pa.string()) for column in columns]),
    )
    pq.write_table(table, path)
    return path


def test_sibling_opinions_are_not_counted_as_orphans(tmp_path: Path):
    """Five opinions over three clusters, all present. Orphans: zero."""
    clusters = _parquet(
        tmp_path / "clusters.parquet",
        [{"cluster_id": "100"}, {"cluster_id": "200"}, {"cluster_id": "300"}],
    )
    bodies = _parquet(
        tmp_path / "bodies.parquet",
        [
            # One cluster, three separately-authored opinions.
            {"opinion_id": "1", "cluster_id": "100", "available_text_fields": "plain_text",
             "plain_text": "majority", "html_with_citations": None},
            {"opinion_id": "2", "cluster_id": "100", "available_text_fields": "plain_text",
             "plain_text": "concurrence", "html_with_citations": None},
            {"opinion_id": "3", "cluster_id": "100", "available_text_fields": "plain_text",
             "plain_text": "dissent", "html_with_citations": None},
            {"opinion_id": "4", "cluster_id": "200", "available_text_fields": None,
             "plain_text": None, "html_with_citations": None},
            {"opinion_id": "5", "cluster_id": "300", "available_text_fields": "plain_text",
             "plain_text": "lead", "html_with_citations": None},
        ],
    )

    report = verify_court_coverage.body_coverage(bodies, clusters)

    assert report["opinions_ingested"] == 5
    assert report["opinions_resolving_to_a_cluster"] == 5
    # The number that would have been reported as "unexplained orphans".
    assert report["opinions_not_resolving_to_a_cluster"] == 0
    # ...and the number that explains why the two unit counts differ.
    assert report["distinct_clusters_named_by_ingested_opinions"] == 3
    assert report["clusters_resolved_for_ingested_opinions"] == 3
    assert report["sibling_opinions_sharing_a_cluster"] == 2


def test_a_genuine_orphan_is_still_reported_as_one(tmp_path: Path):
    """The fix must not make the orphan measure unable to see an orphan."""
    clusters = _parquet(tmp_path / "clusters.parquet", [{"cluster_id": "100"}])
    bodies = _parquet(
        tmp_path / "bodies.parquet",
        [
            {"opinion_id": "1", "cluster_id": "100", "available_text_fields": "plain_text",
             "plain_text": "present", "html_with_citations": None},
            # Names a cluster the dump does not contain — a real orphan.
            {"opinion_id": "2", "cluster_id": "999", "available_text_fields": "plain_text",
             "plain_text": "withdrawn?", "html_with_citations": None},
        ],
    )

    report = verify_court_coverage.body_coverage(bodies, clusters)

    assert report["opinions_ingested"] == 2
    assert report["opinions_resolving_to_a_cluster"] == 1
    assert report["opinions_not_resolving_to_a_cluster"] == 1
    assert report["sibling_opinions_sharing_a_cluster"] == 0
