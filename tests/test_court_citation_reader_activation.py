"""Serving admits court body keys before the scheduled mapper starts writing them."""
from collections import Counter
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import legislative_receipts, mcp_server
from spicy_regs.court_receipts import local_receipt_selection, write_court_rows
from spicy_regs.court_subjects import SUBJECT_SCHEMAS, normalize_court_row
from spicy_regs.legislative_documents import map_subject, native_document_key
from spicy_regs.selected_generations import SelectedDataset, remember_selection
from spicy_regs.transforms.held_citations import NAMESPACE, Selection, build_held_citations, write_citation_reads
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from tests.test_mcp_server import _tool_data


def selected_cohort(root, monkeypatch, *, body_keys, damage=None):
    fixture = json.loads((Path(__file__).parent / "fixtures/court_citations/derived-text-cohort.json").read_text())
    source = [normalize_court_row("court_opinion_pdf_extractions", row) for row in fixture["rows"]]
    legacy = {row["opinion_body_id"]: json.dumps([row["opinion_id"], row["source_sha256"]], separators=(",", ":"))
              for row in source}
    with duckdb.connect() as con:
        con.register("court_opinion_pdf_extractions", pa.Table.from_pylist(
            source, schema=SUBJECT_SCHEMAS["court_opinion_pdf_extractions"]))
        findings = build_held_citations(
            root / "historical", cursor=con,
            selections=[Selection("court_opinion_derived_pdf", (row["opinion_body_id"],)) for row in source],
            input_pins={"court_opinion_pdf_extractions": {"artifactDigest": fixture["publication"]}},
            download_prior=lambda *_: False,
        )
    # Retain the historical capture spelling in both findings and checkpoints.
    checkpoints = [{**row, "document_key": legacy[row["document_key"]]}
                   for row in read_checkpoints(findings, NAMESPACE)]
    table = pq.read_table(findings)
    table = table.set_column(table.schema.get_field_index("document_key"), "document_key",
                             pa.array([legacy[key] for key in table["document_key"].to_pylist()]))
    pq.write_table(table.replace_schema_metadata(checkpoint_metadata(findings, NAMESPACE, checkpoints)), findings)
    reads = write_citation_reads(findings.parent, findings)

    def future_mapper(dataset, raw):
        mapped = map_subject(dataset, raw)
        if mapped is not None and dataset == "document_citations":
            mapped["document_key"] = native_document_key(raw["document_kind"], raw["document_key"])
            if damage == "body":
                mapped["document_key"] = "court-opinion-body:" + "0" * 64
            elif damage == "field":
                mapped["target_rule"] = "contradicted-rule"
        return mapped

    # Actual family writer emits each layout. The activation override is confined
    # to fixture creation; serving runs with the unchanged production mapper.
    bundle = root / "citations"
    with monkeypatch.context() as producer:
        if body_keys:
            producer.setattr(legislative_receipts, "map_subject", future_mapper)
        manifest = legislative_receipts.write_legislative_outputs([findings, reads], bundle, generation_id="citations")
    for dataset in manifest["datasets"]:
        remember_selection(root, [SelectedDataset(
            dataset, tuple(bundle / key for key in manifest["subjects"].get(dataset, ())),
            bundle / manifest["receipt_file"], "citations")])
    pdf = write_court_rows("court_opinion_pdf_extractions", fixture["rows"], root / "pdf", generation_id="pdf",
                           witnesses=[{"source_id": "retained-court-cohort", "sha256": fixture["publication"]}])
    receipt, generation = local_receipt_selection(pdf)
    remember_selection(root, [SelectedDataset("court_opinion_pdf_extractions", (pdf,), receipt, generation)])
    return source, legacy


@pytest.mark.parametrize("body_keys", [False, True])
@pytest.mark.parametrize("writer_activated", [False, True])
def test_server_reads_old_and_future_court_subjects_without_activating_writer(
    tmp_path, monkeypatch, body_keys, writer_activated,
):
    source, legacy = selected_cohort(tmp_path, monkeypatch, body_keys=body_keys)
    if writer_activated:
        from spicy_regs import legislative_documents

        def activated_mapper(dataset, raw):
            mapped = map_subject(dataset, raw)
            if mapped is not None and dataset == "document_citations":
                mapped["document_key"] = native_document_key(raw.get("document_kind"), raw.get("document_key"))
            return mapped

        monkeypatch.setattr(legislative_documents, "map_subject", activated_mapper)
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    with mcp_server._build_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        findings = Counter()
        for row in source:
            reply = _tool_data(server, "resolve_document_citations", {
                "document_kind": "court_opinion_derived_pdf", "document_key": row["opinion_body_id"],
                "max_occurrences": 100,
            })
            findings[row["opinion_id"]] = len(reply["occurrences"])
            assert reply["source_read"]["status"] == ("read" if findings[row["opinion_id"]] else "read_none_found")
        assert findings == {"11209378": 6, "11264531": 12, "11264529": 0}
        public_keys = {key for key, in con.execute("SELECT DISTINCT document_key FROM document_citations").fetchall()}
        assert public_keys <= (set(legacy) if body_keys else set(legacy.values()))
    raw = pq.read_table(tmp_path / "historical/document_citations.parquet").to_pylist()[0]
    mapped = map_subject("document_citations", raw)
    assert mapped is not None and mapped["document_key"] == raw["document_key"]


@pytest.mark.parametrize("damage", ["body", "field"])
def test_future_court_subject_must_match_its_exact_source_body_and_other_fields(tmp_path, monkeypatch, damage):
    selected_cohort(tmp_path, monkeypatch, body_keys=True, damage=damage)
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    with pytest.raises(ValueError, match="differs from its selected native subject"):
        mcp_server._build_connection()
