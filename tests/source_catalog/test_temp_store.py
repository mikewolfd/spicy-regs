"""Temporary storage is a build resource choice, not a catalog identity choice."""

import json
import sys

import pytest

from spicy_regs.source_catalog import __main__ as cli
from spicy_regs.source_catalog import importer
from spicy_regs.source_catalog.raw_archive import RawImporter
from test_raw_archive import base_archive


def test_temp_store_modes_preserve_catalog_and_select_sqlite_storage(tmp_path, monkeypatch):
    root = tmp_path / "raw"
    base_archive(root)
    schema_modes = []
    cache_modes = []
    original_schema = importer.schema
    original_run = RawImporter.run

    def observe_schema(db):
        schema_modes.append(db.execute("PRAGMA temp_store").fetchone()[0])
        original_schema(db)

    def observe_cache(self):
        original_run(self)
        cache_modes.append(self.db.execute("PRAGMA temp_store").fetchone()[0])
        assert self.db.execute("SELECT count(*) FROM native_metadata_cache").fetchone()[0] == 1

    monkeypatch.setattr(importer, "schema", observe_schema)
    monkeypatch.setattr(RawImporter, "run", observe_cache)
    reports = []
    for name, options in [("default", {}), ("file", {"temp_store": "file"}), ("memory", {"temp_store": "memory"})]:
        output = tmp_path / (name + ".sqlite")
        report = importer.build(root, output, **options)
        stored_report = json.loads(output.with_suffix(".sqlite.reconciliation.json").read_text())
        assert stored_report["temp_store"] == report["temp_store"] == options.get("temp_store", "file")
        reports.append(report)
    assert schema_modes == cache_modes == [1, 1, 2]
    assert len({report["generation"] for report in reports}) == 1
    assert reports[0]["raw_archive"] == reports[1]["raw_archive"] == reports[2]["raw_archive"]


def test_invalid_temp_store_refused_before_output_changes(tmp_path):
    output = tmp_path / "selected.sqlite"
    output.write_bytes(b"previous catalog")
    with pytest.raises(ValueError, match="temp_store"):
        importer.build(tmp_path / "raw", output, temp_store="automatic")
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".sqlite.reconciliation.json").exists()


def test_cli_passes_memory_choice_to_build(tmp_path, monkeypatch, capsys):
    root = tmp_path / "raw"
    base_archive(root)
    output = tmp_path / "catalog.sqlite"
    monkeypatch.setattr(
        sys,
        "argv",
        ["source_catalog", "build", "--corpus", str(root), "--output", str(output), "--temp-store", "memory"],
    )
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert result["temp_store"] == "memory"
    assert result["status"] == "validated"
