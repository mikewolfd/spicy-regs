"""Selected FCC observations remain main data without invented capture evidence."""

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.relationship_views.fcc_native import FCC_NATIVE_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.government_receipts import EARLIER_POLICIES, POLICIES, migrate_outputs
from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS, map_subject


def raw():
    return {
        "id_submission": "filing",
        "documents_json": json.dumps([
            {"src": "https://example.test/a.pdf", "filename": "a", "description": "first"},
            None,
            {"src": "https://example.test/a.pdf", "filename": "different", "description": "repeat"},
            {"src": "bad url", "filename": "a"},
        ]),
        "pdf_extraction_results_json": json.dumps([
            {"url": "https://example.test/a.pdf", "status": "ok", "source_sha256": "sha256:" + "a" * 64,
             "page_count": 2, "error": None},
            {"url": "https://example.test/missing.pdf", "status": "error", "source_sha256": None,
             "page_count": None, "error": "no bytes"},
            {"url": "bad url", "status": "ok", "source_sha256": "bad digest"},
            None,
        ]),
    }


def test_main_results_keep_repeated_positions_and_qualification():
    subject = map_subject("fcc_filings", raw(), generation_id="selected")
    assert subject["generation_id"] == "selected"
    observations = subject["extraction_results"]
    assert observations[0]["offered_ordinals"] == [0, 2]
    assert observations[1]["offered_ordinals"] == []
    assert observations[2]["offered_ordinals"] == [3]
    assert observations[2]["url_status"] == "invalid"
    assert observations[2]["digest_status"] == "invalid"
    assert observations[3] is None
    assert observations[0]["capture_status"] == "unverified"
    assert observations[0]["text_access_status"] == "unverified"
    with duckdb.connect() as connection:
        connection.register("fcc_filings", pa.Table.from_pylist([subject], schema=SUBJECT_SCHEMAS["fcc_filings"]))
        installed = install_sql_views(connection, ["fcc_filings"], FCC_NATIVE_VIEWS)
        assert installed["fcc_document_extraction_results"]["status"] == "available"
        assert connection.execute(
            "SELECT attempt_ordinal, offered_ordinal, filename, extraction_status "
            "FROM fcc_document_extraction_results ORDER BY attempt_ordinal, offered_ordinal"
        ).fetchall() == [(0, 0, "a", "ok"), (0, 2, "different", "ok"),
                         (1, None, None, "error"), (2, 3, "a", "ok"), (3, None, None, None)]
        assert connection.execute("SELECT offered_url FROM fcc_filing_artifacts ORDER BY source_ordinal").fetchall() == [
            ("https://example.test/a.pdf",), (None,), ("https://example.test/a.pdf",), ("bad url",)]


def test_null_empty_and_absent_results_are_distinct():
    for value, expected in [(None, None), ("null", None), ("[]", [])]:
        assert map_subject("fcc_filings", {"id_submission": "f", "pdf_extraction_results_json": value})[
            "extraction_results"] == expected


def test_writer_binds_generation_and_keeps_exact_conversion_input(tmp_path):
    path = tmp_path / "fcc_filings.parquet"
    source = raw()
    pq.write_table(pa.Table.from_pylist([source]), path)
    metadata = migrate_outputs((path,), generation_id="actual-build")
    subject = pq.read_table(path).to_pylist()[0]
    assert subject["generation_id"] == "actual-build"
    assert subject["extraction_results"][0]["status"] == "ok"
    assert metadata["generation_id"] == "actual-build"
    assert pq.read_table(tmp_path / ".government-inputs" / "actual-build" / path.name).to_pylist() == [source]
    assert POLICIES["fcc_filings"].policy_version == "government-sources/3"
    assert {p.policy_version for p in EARLIER_POLICIES["fcc_filings"]} == {
        "government-sources/1", "government-sources/2"}
