"""Publication parity checks preserve exact identities and unresolved provenance."""

import json
from typing import Any

import pytest

from spicy_regs.source_catalog.publication import check_publication, import_publications, reconcile
from spicy_regs.source_catalog.storage import Inputs, connect, digest, schema


def write(path, value):
    path.write_text(json.dumps(value))
    return {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}


@pytest.fixture
def fixture(tmp_path):
    original = tmp_path / "original.txt"
    original.write_bytes(b"original")
    body_sha = digest(original)
    version = "sha256:" + "a" * 64
    output_sha = "sha256:" + "b" * 64
    index = tmp_path / "base.sqlite"
    with connect(index) as db:
        schema(db)
        db.execute("INSERT INTO metadata VALUES('generation',?)", (json.dumps("sha256:" + "0" * 64),))
        db.execute("INSERT INTO collections VALUES('family:test','Test',?)", (json.dumps({"families": ["test"]}),))
        db.execute("INSERT INTO objects VALUES(?,8)", (body_sha,))
        db.execute("INSERT INTO files VALUES('raw:one','family:test',NULL,'original','held',?,8,?,?)", (
            body_sha, str(original), json.dumps({"encoded_bytes": 8, "presence_mtime_ns": original.stat().st_mtime_ns}),
        ))
        db.execute("INSERT INTO inputs VALUES('/base/producer.py',?,1)", ("sha256:" + "f" * 64,))
    descriptor = {"sha256": output_sha, "byteSize": 4, "rows": 1, "columns": [["id", "VARCHAR"]]}
    publication = {"format": "spicy-regs-publication", "version": 1, "families": {"test": {
        "prefix": "generations/test/" + "a" * 64, "logicalId": "urn:test", "artifactDigest": version,
        "tables": {"one.parquet": descriptor},
    }}}
    proof = write(tmp_path / "native-proof.json", {"producer_verified": True})
    generation = {"family": "test", "artifact_digest": version, "complete": False, "conflicts": [],
                  "outputs": [{"table": "one", "object_key": "one.parquet", "sha256": output_sha, "bytes": 4}],
                  "sources": [{"sha256": body_sha, "bytes": 8, "provenance": "build-consumption"}],
                  "parents": [], "evidence_inputs": [proof]}
    receipts = {"format": "spicy-publication-inputs", "version": 1, "generations": [generation]}
    selection = {"format": "spicy-publication-reconciliation", "version": 1,
                 "publication": write(tmp_path / "publication.json", publication),
                 "receipts": [write(tmp_path / "receipts.json", receipts)]}
    selection_path = tmp_path / "selection.json"
    write(selection_path, selection)
    return dict(root=tmp_path, index=index, original=original, publication=publication, receipts=receipts,
                generation=generation, selection=selection, selection_path=selection_path)


def update(fixture):
    root = fixture["root"]
    selection = fixture["selection"]
    selection["publication"] = write(root / "publication.json", fixture["publication"])
    selection["receipts"] = [write(root / "receipts.json", fixture["receipts"])]
    write(fixture["selection_path"], selection)


def result(fixture):
    with connect(fixture["index"]) as db:
        report = import_publications(db, Inputs(fixture["root"]), fixture["selection_path"])
        value = json.loads(db.execute("SELECT data FROM tables WHERE id='one'").fetchone()[0])
    return report, value["publication"]


def test_exact_output_and_source_matches_do_not_prove_complete_consumption(fixture):
    report, publication = result(fixture)
    assert report["statuses"] == {"missing_provenance": 1}
    assert publication["sourceFiles"] == publication["matchedSourceFiles"] == 1
    assert publication["localFileIds"] == ["raw:one"]


@pytest.mark.parametrize("fault,status", [
    ("missing", "missing_provenance"), ("incomplete", "missing_provenance"),
    ("provisional", "missing_provenance"), ("output", "conflicting_evidence"),
    ("size", "conflicting_evidence"), ("no-proof", "missing_provenance"),
])
def test_gaps_never_become_verified(fixture, fault, status):
    generation = fixture["generation"]
    if fault == "missing":
        fixture["original"].unlink()
    elif fault == "incomplete":
        generation["complete"] = False
    elif fault == "provisional":
        generation["sources"][0]["provenance"] = "provisional"
    elif fault == "output":
        generation["outputs"][0]["sha256"] = "sha256:" + "c" * 64
    elif fault == "size":
        generation["sources"][0]["bytes"] = 9
    else:
        generation["evidence_inputs"] = []
    update(fixture)
    _, publication = result(fixture)
    assert publication["status"] == status


def test_shared_parent_exact_output_and_transitive_sources(fixture):
    child = fixture["generation"]
    parent: dict[str, Any] = dict(child, family="parent", artifact_digest="sha256:" + "d" * 64,
                  outputs=[dict(child["outputs"][0], table="parent", object_key="parent.parquet")], parents=[])
    child["sources"] = []
    child["parents"] = [{"family": "parent", "artifact_digest": parent["artifact_digest"], "table": "parent",
                         "sha256": parent["outputs"][0]["sha256"], "bytes": 4}]
    fixture["receipts"]["generations"].append(parent)
    update(fixture)
    report, publication = result(fixture)
    assert report["generationsVisited"] == 2
    assert publication["status"] == "missing_provenance"
    child["parents"][0]["sha256"] = "sha256:" + "e" * 64
    update(fixture)
    assert result(fixture)[1]["status"] == "conflicting_evidence"


def test_unknown_family_and_unidentified_sources_remain_accounted(fixture):
    fixture["receipts"]["generations"] = []
    with connect(fixture["index"]) as db:
        db.execute("UPDATE collections SET data='{}'")
    update(fixture)
    report, publication = result(fixture)
    assert report["currentTables"] == 1
    assert publication["status"] == "missing_provenance"
    with connect(fixture["index"], readonly=True) as db:
        assert db.execute("SELECT collection_id FROM tables").fetchone()[0] == "publication:test"


def test_incremental_activation_preserves_base_and_code_pins(fixture):
    output = fixture["root"] / "selected.sqlite"
    before = fixture["index"].read_bytes()
    report = reconcile(fixture["index"], output, fixture["selection_path"])
    assert fixture["index"].read_bytes() == before
    assert report["baseGeneration"] != report["generation"]
    with connect(output, readonly=True) as db:
        assert db.execute("SELECT path FROM inputs").fetchone()[0] == "/base/producer.py"
    assert not check_publication(output, fixture["selection_path"])["verified"]
    assert check_publication(output, fixture["selection_path"])["selectionMatches"]
    fixture["selection_path"].write_text(fixture["selection_path"].read_text() + "\n")
    assert not check_publication(output, fixture["selection_path"])["selectionMatches"]


def test_failed_update_keeps_selected_database_and_removes_candidate(fixture):
    output = fixture["root"] / "selected.sqlite"
    reconcile(fixture["index"], output, fixture["selection_path"])
    before = output.read_bytes()
    fixture["generation"]["complete"] = False
    update(fixture)
    with pytest.raises(ValueError, match="unresolved"):
        reconcile(fixture["index"], output, fixture["selection_path"], require_verified=True)
    assert output.read_bytes() == before
    assert not list(fixture["root"].glob("*.tmp"))
    fixture["selection"]["publication"]["bytes"] += 1
    write(fixture["selection_path"], fixture["selection"])
    with pytest.raises(ValueError, match="pin mismatch"):
        reconcile(fixture["index"], output, fixture["selection_path"])
    assert output.read_bytes() == before


def test_historical_versions_do_not_replace_current_table(fixture):
    historical = json.loads(json.dumps(fixture["publication"]))
    entry = historical["families"]["test"]
    entry["artifactDigest"] = "sha256:" + "e" * 64
    entry["prefix"] = "generations/test/" + "e" * 64
    fixture["selection"]["historical_publications"] = [write(fixture["root"] / "historical.json", historical)]
    update(fixture)
    result(fixture)
    with connect(fixture["index"], readonly=True) as db:
        assert db.execute("SELECT count(*) FROM tables").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM lineage").fetchone()[0] == 2
        assert json.loads(db.execute("SELECT data FROM tables").fetchone()[0])["artifact_digest"] == "sha256:" + "a" * 64


def test_rulemaking_accounts_exact_snapshot_and_rejects_pointer_mismatch(fixture):
    snapshot = "snapshot_1234"
    prefix = "materialized/rulemaking/snapshots/" + snapshot
    pointer = {"format_version": 2, "dataset": "rulemaking", "snapshot_id": snapshot,
               "manifest_key": prefix + "/manifest.json"}
    manifest = {"format_version": 2, "dataset": "rulemaking", "snapshot_id": snapshot, "artifacts": {
        "two.parquet": {"visibility": "public", "sha256": "c" * 64, "bytes": 5, "rows": 2,
                        "remote_key": prefix + "/two.parquet"},
    }}
    fixture["selection"]["rulemaking"] = {"pointer": write(fixture["root"] / "pointer.json", pointer),
                                           "manifest": write(fixture["root"] / "manifest.json", manifest)}
    update(fixture)
    assert result(fixture)[0]["currentTables"] == 2
    pointer["snapshot_id"] = "snapshot_5678"
    fixture["selection"]["rulemaking"]["pointer"] = write(fixture["root"] / "pointer.json", pointer)
    update(fixture)
    with pytest.raises(ValueError, match="differs from selected pointer"):
        result(fixture)


def test_unsupported_complete_claim_is_rejected_even_with_pinned_evidence(fixture):
    fixture["generation"]["complete"] = True
    update(fixture)
    with pytest.raises(ValueError, match="supported native consumption evidence"):
        result(fixture)


def test_reselection_removes_only_stale_managed_current_rows(fixture):
    result(fixture)
    with connect(fixture["index"]) as db:
        db.execute("INSERT INTO tables VALUES('other','family:test','test','{}')")
    descriptor = fixture["publication"]["families"]["test"]["tables"].pop("one.parquet")
    fixture["publication"]["families"]["test"]["tables"]["new.parquet"] = descriptor
    update(fixture)
    with connect(fixture["index"]) as db:
        import_publications(db, Inputs(fixture["root"]), fixture["selection_path"])
        assert {row[0] for row in db.execute("SELECT id FROM tables")} == {"other", "new"}
        assert [json.loads(row[0])["id"] for row in db.execute("SELECT data FROM lineage")] == ["new"]


def test_shared_sources_are_joined_once_for_several_tables(fixture):
    descriptor = fixture["publication"]["families"]["test"]["tables"]["one.parquet"]
    fixture["publication"]["families"]["test"]["tables"]["two.parquet"] = descriptor
    fixture["generation"]["outputs"].append(dict(fixture["generation"]["outputs"][0],
                                                table="two", object_key="two.parquet"))
    update(fixture)
    report, _ = result(fixture)
    assert report["currentTables"] == 2
    assert report["sourceIdentitiesChecked"] == 1


def test_expected_catalog_generation_cannot_change_between_selection_and_reconciliation(fixture):
    with pytest.raises(ValueError, match="generation changed"):
        reconcile(fixture["index"], fixture["root"] / "bad.sqlite", fixture["selection_path"],
                  expected_generation="sha256:" + "f" * 64)
    assert not list(fixture["root"].glob("*.tmp"))


def test_imported_publication_exports_all_required_catalog_reader_fields(fixture):
    from spicy_regs.source_catalog.exporter import export, validate_public

    output = fixture["root"] / "selected.sqlite"
    reconcile(fixture["index"], output, fixture["selection_path"])
    public = fixture["root"] / "public"
    manifest = export(output, public)
    summary = next(item for item in manifest["collections"] if item["id"] == "family:test")
    detail = json.loads((public / summary["detailPath"]).read_text())
    validate_public(detail)
    # parseCatalogDetail requires these strings; the provenance result has a
    # separate status and cannot substitute for the publication's own status.
    table = detail["tables"][0]
    assert all(isinstance(table[field], str) and table[field] for field in ("id", "family", "status"))
    assert table["status"] == "publication-pinned"
    assert table["artifactDigest"] == table["publication"]["version"]
    assert table["publication"]["status"] == "missing_provenance"
    assert set(table) == {"id", "family", "status", "artifactDigest", "publication"}
