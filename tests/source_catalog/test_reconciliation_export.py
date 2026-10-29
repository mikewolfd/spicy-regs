from tests.source_catalog.file_helpers import read_file_page
"""Public reconciliation retains proof identity while withholding local evidence."""

import json

import pytest

from spicy_regs.source_catalog.exporter import export, export_period, public_reconciliation
from spicy_regs.source_catalog.storage import connect, schema


def reconciliation(**changes):
    return {
        "scope": "current", "version": "sha256:" + "a" * 64, "status": "verified",
        "sourceFiles": 2, "matchedSourceFiles": 2, "missingSourceFiles": 0, "capturedSourceFiles": 2, "inputClosureComplete": True,
        "reasons": [], "evidenceLinks": [{"label": "Build receipt", "url": "https://data.example/build.json"}],
        **changes,
    }


def test_period_display_withholds_private_text_without_changing_internal_metadata():
    period = {"kind": "capture", "label": "Selected /home/private/source", "basis": "/Users/private/receipt.json", "start": "2026-10-26", "local_path": "/private/raw"}
    assert export_period(period, internal=False) == {"kind": "capture", "label": "[internal evidence withheld]", "basis": "[internal evidence withheld]", "start": "2026-10-26"}
    assert export_period(period, internal=True) == {key: value for key, value in period.items() if key != "local_path"}
    assert period["label"] == "Selected /home/private/source"
    assert export_period({"kind": "edition", "label": "2026 edition", "basis": "https://publisher.example/edition.json"}, internal=False) == {"kind": "edition", "label": "2026 edition", "basis": "https://publisher.example/edition.json"}


def test_public_reconciliation_strict_allowlist():
    clean = public_reconciliation(reconciliation(
        localFileIds=["private"], evidenceIds=["receipt"], raw={"local_path": "/home/private"},
        reasons=["Missing file /home/private/source.xml"],
        evidenceLinks=[
            {"label": "Build receipt", "url": "https://data.example/build.json", "raw": "/private/local"},
            {"label": "Private", "url": "https://data.example/build.json?token=private"},
            {"label": "Local", "url": "file:///home/private"},
        ],
    ))
    assert set(clean) == {"scope", "version", "status", "sourceFiles", "matchedSourceFiles", "missingSourceFiles", "capturedSourceFiles", "inputClosureComplete", "reasons", "evidenceLinks"}
    assert clean["evidenceLinks"] == [{"label": "Build receipt", "url": "https://data.example/build.json"}]
    assert clean["reasons"] == ["[internal evidence withheld]"]


@pytest.mark.parametrize("changes", [
    {"status": "complete"}, {"scope": "latest"}, {"version": "unversioned"},
    {"missingSourceFiles": 1}, {"sourceFiles": 0, "matchedSourceFiles": 0},
    {"matchedSourceFiles": True}, {"reasons": [None]}, {"inputClosureComplete": False},
])
def test_reconciliation_rejects_unqualified_or_inconsistent_results(changes):
    with pytest.raises(ValueError):
        public_reconciliation(reconciliation(**changes))


def test_export_retains_publication_but_keeps_lineage_private(tmp_path):
    index = tmp_path / "index.sqlite"
    db = connect(index)
    schema(db)
    db.executemany("INSERT INTO metadata VALUES (?,?)", [
        ("generation", json.dumps("sha256:" + "b" * 64)), ("generatedAt", json.dumps("2026-10-26T00:00:00Z")),
    ])
    db.execute("INSERT INTO collections VALUES (?,?,?)", ("source", "Source", "{}"))
    data = {"id": "table", "family": "family", "status": "published", "artifact_digest": reconciliation()["version"],
            "publication": reconciliation(localFileIds=["private"])}
    db.execute("INSERT INTO tables VALUES (?,?,?,?)", ("table", "source", "family", json.dumps(data)))
    db.execute("INSERT INTO lineage VALUES (?,?,?)", ("lineage", "source", json.dumps({"local_path": "/home/private"})))
    db.commit()
    db.close()
    output = tmp_path / "public"
    manifest = export(index, output)
    detail = json.loads((output / manifest["collections"][0]["detailPath"]).read_text())
    assert detail["tables"][0]["publication"] == public_reconciliation(reconciliation())
    assert "internalOnly" not in detail
    assert "private" not in json.dumps(detail)
    internal_output = tmp_path / "internal"
    internal_manifest = export(index, internal_output, internal=True)
    internal_detail = json.loads((internal_output / internal_manifest["collections"][0]["detailPath"]).read_text())
    assert internal_detail["internalOnly"]["lineage"] == [{"local_path": "/home/private"}]


def test_public_pages_omit_private_payloads_and_keep_internal_fields(tmp_path):
    index = tmp_path / "index.sqlite"
    db = connect(index)
    schema(db)
    db.executemany("INSERT INTO metadata VALUES (?,?)", [
        ("generation", json.dumps("sha256:" + "b" * 64)), ("generatedAt", json.dumps("2026-10-26T00:00:00Z")),
    ])
    db.execute("INSERT INTO collections VALUES (?,?,?)", ("source", "Source", "{}"))
    payload = {"raw": "private receipt body" * 1000, "encoded_bytes": 8, "verification": "recorded"}
    db.execute("INSERT INTO files(id,collection_id,name,status,local_path,data) VALUES (?,?,?,?,?,?)",
               ("file", "source", "source.xml", "receipt-only", "/home/private", json.dumps(payload)))
    db.commit()
    db.close()
    output = tmp_path / "public"
    manifest = export(index, output)
    detail = json.loads((output / manifest["collections"][0]["detailPath"]).read_text())
    page = read_file_page(output / detail["files"]["pages"][0]["path"])
    assert page["files"] == [{"id": "file", "name": "source.xml", "status": "receipt-only",
                              "provenance": {"basis": "unknown", "sourceUrls": [], "availability": "local"}}]
    internal = tmp_path / "internal"
    internal_manifest = export(index, internal, internal=True)
    internal_detail = json.loads((internal / internal_manifest["collections"][0]["detailPath"]).read_text())
    internal_page = json.loads((internal / internal_detail["files"]["pages"][0]["path"]).read_text())
    assert internal_page["files"][0]["localPath"] == "/home/private"
    assert internal_page["files"][0]["encodedBytes"] == 8
    assert internal_page["files"][0]["verification"] == "recorded"
