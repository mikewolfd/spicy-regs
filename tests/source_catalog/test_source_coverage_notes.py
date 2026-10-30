"""Source inventory reviews survive import and stay bound to table versions."""
import json

import pytest

from spicy_regs.source_catalog.exporter import export, source_coverage_note
from spicy_regs.source_catalog.importer import Importer
from spicy_regs.source_catalog.storage import Inputs, connect, schema


VERSION = "sha256:" + "a" * 64


def observation(**changes):
    return {"id": "source-coverage:example", "collection_id": "source",
            "metadata_kind": "source-coverage", "table": "example",
            "table_version": VERSION, "status": "unknown",
            "note": "Source coverage: unknown; expected versions are not fully enumerated.",
            **changes}


def test_registry_review_replays_and_exports_without_processing_qualification(tmp_path):
    config = {"collections": [{"id": "source"}], "observations": [observation()]}
    for attempt, version in enumerate([VERSION, VERSION, "sha256:" + "c" * 64]):
        index = tmp_path / f"{attempt}.sqlite"
        with connect(index) as db:
            schema(db)
            Importer(db, Inputs(tmp_path), config)
            db.executemany("INSERT INTO metadata VALUES(?,?)", [
                ("generation", json.dumps("sha256:" + "b" * 64)),
                ("generatedAt", json.dumps("2026-10-26T00:00:00Z")),
            ])
            db.execute("INSERT INTO tables VALUES(?,?,?,?)", (
                "example", "source", "family", json.dumps({"id": "example", "family": "family",
                    "status": "publication-pinned", "artifact_digest": version})))
            db.commit()
        output = tmp_path / f"public-{attempt}"
        manifest = export(index, output)
        detail = json.loads((output / manifest["collections"][0]["detailPath"]).read_text())
        if version == VERSION:
            assert detail["tables"][0]["coverageNote"] == observation()["note"]
        else:
            assert "coverageNote" not in detail["tables"][0]
        assert detail["observations"] == []
        assert "publication" not in detail["tables"][0]
        assert detail["coverage"]["completenessDenominator"] is None


def test_review_for_another_table_version_does_not_transfer():
    evidence = [("observations", observation())]
    assert source_coverage_note({"id": "example", "artifact_digest": "sha256:" + "c" * 64}, evidence) is None
    assert source_coverage_note({"id": "other", "artifact_digest": VERSION}, evidence) is None


@pytest.mark.parametrize("version", [None, "unversioned", "sha256:abc"])
def test_unversioned_or_malformed_table_cannot_select_a_review(version):
    item = observation(table_version=version)
    assert source_coverage_note({"id": "example", "artifact_digest": version}, [("observations", item)]) is None
    item.pop("table_version")
    assert source_coverage_note({"id": "example"}, [("observations", item)]) is None


def test_conflicting_reviews_refuse_export_selection():
    evidence = [("observations", observation()), ("observations", observation(note="Different source coverage."))]
    with pytest.raises(ValueError, match="Conflicting source coverage"):
        source_coverage_note({"id": "example", "artifact_digest": VERSION}, evidence)
