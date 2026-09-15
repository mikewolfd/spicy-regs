"""Latest per-file diagnostics survive mixed outcomes and both storage paths."""

from hashlib import sha256
import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from spicy_regs.backfill_derived_text import enrich_comments_with_derived_text
from spicy_regs.enrich_pdf import (
    _PdfAttempt,
    _combine,
    _enrich_comment_agency_in_catalog,
    enrich_comments_with_pdf_text,
    enrich_documents_with_pdf_text,
)
from spicy_regs.schemas import COMMENT, DOCUMENT
from spicy_regs.sources import iceberg
from spicy_regs.transforms.pdf_text import PdfTextResult, PdfTextStatus, extract_pdf_text
from tests.pdf_enrichment_oracle import _combine as old_combine
from tests.pdf_fixtures import make_pdf, make_textless_pdf
from tests.test_pdf_text_shared import _rewrite

FIELD = "pdf_extraction_results_json"
GOOD = Path(__file__).parents[1] / "sample-data/mirrulations/comment-ACF-2025-0038-0004_attachment_1.pdf"


def frame(kind, urls, **overrides):
    formats = [{"url": url, "format": "pdf"} for url in urls]
    attachments = [{"title": "Source files", "formats": formats}] if kind == "comments" else formats
    row = {"attachments_json": json.dumps(attachments), "text_content": None, "text_extraction_status": None}
    row.update({"comment_id": "C1"} if kind == "comments" else {"document_id": "D1", "file_url": None})
    row.update(overrides)
    return pl.DataFrame([row], schema={key: pl.String for key in row})


def enrich(kind, source, **kwargs):
    operation = enrich_comments_with_pdf_text if kind == "comments" else enrich_documents_with_pdf_text
    return operation(source, max_workers=1, **kwargs)


@pytest.mark.parametrize("kind", ["documents", "comments"])
@pytest.mark.parametrize("failure", ["truncated", "empty-bytes", "fetch-none"])
def test_retained_good_pdf_and_failed_attachment_keep_aggregate_and_each_result(kind, failure, tmp_path):
    raw = GOOD.read_bytes()
    failed = raw[:256] if failure == "truncated" else b"" if failure == "empty-bytes" else None
    good_url, bad_url = "https://source.invalid/good.pdf", "https://source.invalid/bad.pdf"
    payloads = {good_url: raw, bad_url: failed}
    output, stats = enrich(kind, frame(kind, [good_url, bad_url]), fetch=payloads.get)
    row = output.row(0, named=True)
    good_result = extract_pdf_text(raw)
    bad_result = extract_pdf_text(failed) if failed is not None else None
    expected = old_combine([(good_result.text, good_result.status.value), (None, "error")])
    assert (row["text_content"], row["text_extraction_status"]) == expected
    assert stats == {"selected": 1, "ok": 1, "empty": 0, "encrypted": 0, "error": 0}
    assert json.loads(row[FIELD]) == [
        {"url": good_url, "source_sha256": sha256(raw).hexdigest(), "status": "ok", "page_count": 2, "error": None},
        {
            "url": bad_url,
            "source_sha256": sha256(failed).hexdigest() if failed is not None else None,
            "status": "error",
            "page_count": bad_result.page_count if bad_result is not None else None,
            "error": bad_result.error if bad_result is not None else "fetch returned no bytes",
        },
    ]
    path = tmp_path / "enriched.parquet"
    output.write_parquet(path)
    assert pl.read_parquet(path).row(0, named=True) == row


def test_distinct_equal_byte_urls_keep_source_order_and_individual_diagnostics():
    raw = make_pdf(["Shared bytes"])
    urls = ["https://source.invalid/z.pdf", "https://source.invalid/a.pdf"]
    calls = []

    def extract(source):
        calls.append(source)
        return extract_pdf_text(source)

    output, _ = enrich("documents", frame("documents", [*urls, urls[0]]), fetch=lambda _: raw, extract=extract)
    entries = json.loads(output[FIELD][0])
    assert [entry["url"] for entry in entries] == urls
    assert [entry["source_sha256"] for entry in entries] == [sha256(raw).hexdigest()] * 2
    assert calls == [raw]  # Sequential workers exercise the existing digest cache.
    assert output["text_content"][0] == "Shared bytes\n\nShared bytes"


@pytest.mark.parametrize(
    "result",
    [
        PdfTextResult(PdfTextStatus.ERROR, "caller-supplied text", 3, "caller diagnostic"),
        PdfTextResult(PdfTextStatus.EMPTY, "", 4, None),
        PdfTextResult(PdfTextStatus.ENCRYPTED, "", 0, "password required"),
    ],
)
def test_metadata_retains_actual_reader_fields_instead_of_recomputing_status_from_text(result):
    output, _ = enrich(
        "documents",
        frame("documents", ["https://source.invalid/a.pdf"]),
        fetch=lambda _: b"source bytes",
        extract=lambda _: result,
    )
    entry = json.loads(output[FIELD][0])[0]
    assert (entry["status"], entry["page_count"], entry["error"]) == (
        result.status.value,
        result.page_count,
        result.error,
    )
    assert (output["text_content"][0], output["text_extraction_status"][0]) == old_combine(
        [(result.text or None, result.status.value)]
    )


@pytest.mark.parametrize(
    "source,status,pages",
    [
        (make_textless_pdf(), "empty", 1),
        (_rewrite(make_pdf(["Secret"]), password="secret"), "encrypted", 0),
        (_rewrite(make_pdf(["First", "Bad"]), broken_page=2), "error", 2),
    ],
)
def test_actual_empty_encrypted_and_failed_page_results_remain_distinct(source, status, pages):
    output, _ = enrich("comments", frame("comments", ["https://source.invalid/a.pdf"]), fetch=lambda _: source)
    entry = json.loads(output[FIELD][0])[0]
    assert entry["status"] == status
    assert entry["page_count"] == pages
    if status == "error":
        assert "page 2" in entry["error"]
    assert output["text_content"][0] is None


@pytest.mark.parametrize("kind", ["documents", "comments"])
def test_skip_retains_prior_attempt_and_failed_overwrite_keeps_old_text_with_new_diagnostics(kind):
    old = '[{"url":"prior","status":"ok"}]'
    source = frame(
        kind,
        ["https://source.invalid/now.pdf"],
        text_content="older text",
        text_extraction_status="ok",
        pdf_extraction_results_json=old,
    )
    skipped, stats = enrich(kind, source, fetch=lambda _: pytest.fail("skipped row fetched"))
    assert skipped.row(0, named=True) == source.row(0, named=True)
    assert stats["selected"] == 0
    output, stats = enrich(kind, source, fetch=lambda _: None, overwrite=True)
    assert output["text_content"][0] == "older text"
    assert output["text_extraction_status"][0] == "error"
    assert json.loads(output[FIELD][0])[0]["error"] == "fetch returned no bytes"
    assert stats["error"] == 1


@pytest.mark.parametrize("kind", ["documents", "comments"])
def test_unselected_rows_have_no_invented_pdf_attempt(kind):
    output, stats = enrich(kind, frame(kind, []), fetch=lambda _: pytest.fail("no URL to fetch"))
    assert output[FIELD][0] is None
    assert stats["selected"] == 0


@pytest.fixture
def catalog():
    with duckdb.connect() as con:
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        yield con


@pytest.mark.parametrize("record_type", [DOCUMENT, COMMENT])
def test_existing_table_adds_only_nullable_pdf_column_and_preserves_rows(catalog, record_type):
    con = catalog
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {iceberg._schema_ref()}")
    columns = [column for column in record_type.schema if column != FIELD]
    definitions = ", ".join(f'"{column}" VARCHAR' for column in columns)
    table = iceberg._qualified(record_type)
    con.execute(f"CREATE TABLE {table} ({definitions})")
    con.execute(f'INSERT INTO {table} ("{record_type.dedup_key}") VALUES (?)', ["retained"])
    iceberg._ensure_table(con, record_type)
    iceberg._ensure_table(con, record_type)
    observed = con.execute(f"DESCRIBE {table}").fetchall()
    assert [column[0] for column in observed] == [*columns, FIELD]
    assert observed[-1][1:3] == ("VARCHAR", "YES")
    assert con.execute(f'SELECT "{record_type.dedup_key}", {FIELD} FROM {table}').fetchall() == [("retained", None)]


def test_existing_incompatible_column_refuses_without_replacing_its_data(catalog):
    table = iceberg._qualified(COMMENT)
    catalog.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
    catalog.execute(f"CREATE TABLE {table} (comment_id VARCHAR, {FIELD} INTEGER)")
    catalog.execute(f"INSERT INTO {table} VALUES ('retained', 7)")
    with pytest.raises(ValueError, match="must be VARCHAR"):
        iceberg._ensure_table(catalog, COMMENT)
    assert catalog.execute(f"SELECT * FROM {table}").fetchall() == [("retained", 7)]


def test_required_avro_install_failure_propagates_before_credentials_or_attach(monkeypatch):
    commands = []
    closed = []
    failure = duckdb.Error("required extension unavailable")

    class RefusingConnection:
        def execute(self, sql):
            commands.append(sql)
            raise failure

        def close(self):
            closed.append(True)

    for name in iceberg._REQUIRED_ENV:
        monkeypatch.setenv(name, "test-placeholder")
    monkeypatch.setattr(duckdb, "connect", lambda: RefusingConnection())
    with pytest.raises(duckdb.Error) as caught:
        iceberg._connect()
    assert caught.value is failure
    assert commands == ["INSTALL avro; LOAD avro;"]
    assert closed == [True]


def test_catalog_mixed_pdf_results_survive_export_and_derived_text_update(catalog, tmp_path):
    raw = GOOD.read_bytes()
    urls = ["https://source.invalid/good.pdf", "https://source.invalid/bad.pdf"]
    iceberg._ensure_table(catalog, COMMENT)
    rows = []
    for agency, identifier in [("ACF", "C1"), ("OTHER", "C2")]:
        values = frame("comments", urls, comment_id=identifier, agency_code=agency).row(0, named=True)
        rows.append({**{column: None for column in COMMENT.schema}, **values})
    catalog.register("_test_seed", pl.DataFrame(rows, schema=COMMENT.schema).to_arrow())
    table = iceberg._qualified(COMMENT)
    catalog.execute(f"INSERT INTO {table} SELECT * FROM _test_seed")
    catalog.unregister("_test_seed")
    stats = _enrich_comment_agency_in_catalog(catalog, COMMENT, "ACF", fetch={urls[0]: raw, urls[1]: raw[:256]}.get)
    assert stats["ok"] == 1
    attempted = catalog.execute(f"SELECT {FIELD} FROM {table} WHERE comment_id='C1'").fetchone()[0]
    assert [entry["status"] for entry in json.loads(attempted)] == ["ok", "error"]
    assert catalog.execute(f"SELECT {FIELD} FROM {table} WHERE comment_id='C2'").fetchone() == (None,)
    derived = pl.DataFrame({"comment_id": ["C1"], "_new_text": ["independent derived text"], "_new_status": ["ok"]})
    iceberg.upsert_comment_text(catalog, COMMENT, "ACF", derived)
    assert catalog.execute(f"SELECT text_content, {FIELD} FROM {table} WHERE comment_id='C1'").fetchone() == (
        "independent derived text",
        attempted,
    )
    exported = pl.read_parquet(iceberg._export_parquet(catalog, COMMENT, tmp_path))
    assert exported.filter(pl.col("comment_id") == "C1")[FIELD][0] == attempted


def test_frame_derived_backfill_preserves_prior_pdf_diagnostics(monkeypatch):
    from spicy_regs import backfill_derived_text

    prior = '[{"url":"prior","status":"error"}]'
    source = frame("comments", [], docket_id="D1", agency_code="ACF", pdf_extraction_results_json=prior)
    updates = pl.DataFrame({"comment_id": ["C1"], "_new_text": ["derived text"], "_new_status": ["ok"]})
    monkeypatch.setattr(backfill_derived_text, "_derived_text_updates", lambda *a, **kw: (updates, {"ok": 1}))
    output, _ = enrich_comments_with_derived_text(source)
    assert output[FIELD][0] == prior
    assert output["text_content"][0] == "derived text"


def test_combined_no_text_precedence_matches_the_frozen_policy():
    outcomes = [PdfTextStatus.EMPTY, PdfTextStatus.ENCRYPTED, PdfTextStatus.ERROR]
    for size in range(4):
        results = [PdfTextResult(status, "", 0) for status in outcomes[:size]]
        assert _combine([_PdfAttempt("digest", result) for result in results]) == old_combine(
            [(None, result.status.value) for result in results]
        )
