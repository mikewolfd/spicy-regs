"""Selection pins retained response partitions without relaxing member guards."""

import json

import pytest

import test_selection as selection_fixtures
from test_selection import selected_bytes
from spicy_regs.source_catalog import selection
from spicy_regs.source_catalog.exporter import export
from spicy_regs.source_catalog.scope import install_scope
from spicy_regs.source_catalog.storage import connect


prepared = selection_fixtures.prepared


def prepare_retained(prepared):
    candidate = prepared["candidate"]
    with connect(candidate, scope="all") as db:
        for ident, status in [("usable", "held"), ("error", "refused-source"), ("unchecked", "unclassified-source")]:
            db.execute(
                "INSERT INTO files VALUES(?,?,NULL,?,?,NULL,NULL,NULL,?)",
                (ident, "source", ident + ".json", status, json.dumps({"role": "original-response"})),
            )
        install_scope(
            db,
            {
                "format": "spicy-source-scope",
                "version": 2,
                "collections": [{"id": "source", "disposition": "active", "reason": "Published current source."}],
                "excluded_objects": [],
            },
        )
        db.commit()
    root = prepared["staged_public"].with_name("staged-with-retained")
    manifest = export(candidate, root, page_size=1)
    prepared["staged_public"] = root
    summary = manifest["collections"][0]
    path = root / summary["detailPath"]
    detail = json.loads(path.read_text())
    assert detail["files"]["total"] == summary["holdings"]["aliases"] == 1
    assert detail["retainedFiles"]["total"] == 2
    return root, manifest, path, detail


def test_exported_retained_pages_are_pinned_and_selected(prepared):
    root, manifest, _, detail = prepare_retained(prepared)
    members, _ = selection.public_members(root, manifest["generation"])
    retained = {part["path"] for part in detail["retainedFiles"]["pages"]}
    assert retained <= {member["path"] for member in members}
    result = selection.select(**prepared)
    assert result["status"] == "selected"
    selected = json.loads((prepared["cache"] / "selection.json").read_text())
    assert retained <= {member["path"] for member in selected["files"]}
    assert all((prepared["public"] / name).is_file() for name in retained)


@pytest.mark.parametrize("fault", ["unreferenced", "rows", "total", "collection", "duplicate", "oversized", "missing"])
def test_retained_page_faults_reject_selection_without_replacing_current(prepared, fault):
    root, manifest, path, detail = prepare_retained(prepared)
    page = detail["retainedFiles"]["pages"][0]
    page_path = root / page["path"]
    if fault == "unreferenced":
        page_path.with_name("unreferenced-retained.parquet").write_bytes(page_path.read_bytes())
    elif fault == "rows":
        page["rows"] += 1
    elif fault == "total":
        detail["retainedFiles"]["total"] += 1
    elif fault == "duplicate":
        detail["retainedFiles"]["pages"].append(dict(page))
        detail["retainedFiles"]["total"] += page["rows"]
    elif fault == "oversized":
        page["rows"] = 5001
    elif fault == "missing":
        page_path.unlink()
    else:
        import hashlib
        import pyarrow.parquet as pq
        from spicy_regs.source_catalog.file_inventory import encode

        records = pq.read_table(page_path).to_pylist()
        records[0]["dataset_id"] = "other"
        body = encode(records, generation=manifest["generation"], dataset_id="source", inventory_kind="attention")
        page_path.write_bytes(body)
        page["bytes"] = len(body)
        page["sha256"] = "sha256:" + hashlib.sha256(body).hexdigest()
    path.write_text(json.dumps(detail))
    before = selected_bytes(prepared)
    with pytest.raises(ValueError):
        selection.select(**prepared)
    assert selected_bytes(prepared) == before
