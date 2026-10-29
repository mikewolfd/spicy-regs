"""Public inventory states and bounded summaries never invent source custody."""
import json

import pytest

from spicy_regs.source_catalog.exporter import export, period_summary
from spicy_regs.source_catalog.storage import connect, schema


def index_with_collection(tmp_path, *, title="Publication evidence", status=None, periods=()):
    index = tmp_path / "index.sqlite"
    with connect(index) as db:
        schema(db)
        db.executemany("INSERT INTO metadata VALUES (?,?)", [
            ("generation", json.dumps("sha256:" + "a" * 64)),
            ("generatedAt", json.dumps("2026-10-27T00:00:00Z")),
        ])
        db.execute("INSERT INTO collections VALUES (?,?,?)", ("publication", title, "{}"))
        if status:
            db.execute("INSERT INTO evidence VALUES (?,?,?,?)", (
                "availability", "publication", "observations",
                json.dumps({"id": "availability", "status": status,
                            "note": "Explicit source availability observation"})))
        db.executemany("INSERT INTO evidence VALUES (?,?,?,?)", [
            (f"period-{i:05d}", "publication", "periods", json.dumps(period))
            for i, period in enumerate(periods)
        ])
        db.commit()
    return index


@pytest.mark.parametrize("title", ["Publication: comments Mirrulations", "Publication: retained-sec-comments"])
def test_empty_collection_does_not_assert_originals_were_not_acquired(tmp_path, title):
    manifest = export(index_with_collection(tmp_path, title=title), tmp_path / "public")
    summary = manifest["collections"][0]
    assert "inventory-unresolved" in summary["statuses"]
    assert "known-not-acquired" not in summary["statuses"]
    assert "external-by-choice" not in summary["statuses"]
    assert summary["holdings"] == {"aliases": 0, "objects": 0, "decodedBytes": 0, "missing": 0}


@pytest.mark.parametrize("status", ["known-not-acquired", "external-by-choice"])
def test_explicit_source_availability_evidence_is_preserved(tmp_path, status):
    manifest = export(index_with_collection(tmp_path, status=status), tmp_path / "public")
    assert status in manifest["collections"][0]["statuses"]
    assert "inventory-unresolved" not in manifest["collections"][0]["statuses"]


def test_period_detail_is_exhaustive_while_manifest_summary_is_bounded(tmp_path):
    periods = [{"kind": "requested-scope", "label": f"Request {i}", "basis": "A selected request"}
               for i in range(2000)]
    observed = {"kind": "observed-retained-scope", "label": "Saved rows", "start": "1994", "end": "1995",
                "basis": "Exact saved record scope, completeness unknown"}
    periods += [observed, dict(reversed(list(observed.items())))]
    root = tmp_path / "public"
    manifest = export(index_with_collection(tmp_path, periods=periods), root)
    summary = manifest["collections"][0]
    assert manifest["version"] == 3
    assert "periods" not in summary
    assert summary["periodSummary"] == {"count": len(periods), "observedRetained": observed}
    assert len(json.dumps(summary["periodSummary"])) < 300
    detail = json.loads((root / summary["detailPath"]).read_text())
    assert detail["version"] == 3
    assert detail["collection"] == {**summary, "periods": periods}
    assert (root / "manifest.json").stat().st_size < 1000


def test_different_observed_scopes_are_not_combined_into_one_range():
    periods = [{"kind": "observed-retained-scope", "label": str(year), "start": str(year),
                "basis": "Saved records"} for year in (1994, 2026)]
    assert period_summary(periods) == {"count": 2}


def test_period_summary_deduplicates_exact_scopes_independently_of_field_order():
    period = {"kind": "observed-retained-scope", "label": "Saved rows", "start": "1994", "basis": "Saved records"}
    assert period_summary([period, dict(reversed(list(period.items())))]) == {
        "count": 2, "observedRetained": period}
