"""Citation tools consume exact selected receipts while SQL sees native subjects."""
import hashlib

import duckdb
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server
from spicy_regs.etl_receipts import ReceiptContext, write_dataset
from spicy_regs.legislative_receipts import mapped_record, policy
from spicy_regs.selected_generations import SelectedDataset, remember_selection
from tests.test_legislative_receipts import row
from tests.test_mcp_server import _tool_data


def select(root, dataset, rows):
    context = ReceiptContext("citation-generation", "citation-attempt", "citation-test", [
        {"source_id": dataset, "sha256": "a" * 64}])
    subject, receipts = write_dataset(
        [(mapped_record(dataset, source), context) for source in rows], root / dataset, policy(dataset))
    remember_selection(root, [SelectedDataset(dataset, (subject,) if subject is not None else (),
                                               receipts, context.generation_id)])
    return subject, receipts


def connection(root, monkeypatch):
    monkeypatch.setattr(mcp_server, "DATA_DIR", root)
    monkeypatch.setattr(mcp_server, "TABLES", ())
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    return con, mcp_server.build_server()


def occurrence(digest, **overrides):
    return row("document_citations", document_kind="budget_volume", document_key="BUDGET-2027",
               cite_kind="public_law", target_key="119-public-1", span_start="0", span_end="12",
               matched_text="Public Law 1", text_sha256=digest, target_resolved="false",
               target_rule="public_law", rule_version="test-rule") | overrides


@pytest.mark.parametrize("settled", [False, True])
def test_native_citation_tool_preserves_unsettled_key_and_private_processing(tmp_path, monkeypatch, settled):
    digest = "sha256:" + "c" * 64
    subjects, _ = select(tmp_path, "document_citations", [occurrence(digest, target_resolved=str(settled).lower())])
    select(tmp_path, "budget_volumes", [row("budget_volumes", package_id="BUDGET-2027",
                                          text_sha256=digest, pages_read="1", rule_set_version="test",
                                          citation_rows="1")])
    select(tmp_path, "laws", [row("laws", congress="119", law_type="public", number="1", law_id="119-public-1")])
    assert "target_resolved" not in pq.read_schema(subjects).names
    with connection(tmp_path, monkeypatch)[0] as con:
        server = mcp_server.build_server()
        result = _tool_data(server, "resolve_document_citations", {
            "document_kind": "budget_volume", "document_key": "BUDGET-2027"})
        assert result["source_read"]["status"] == "read"
        assert result["occurrences"][0]["target_status"] == ("found" if settled else "not_checked")
        assert result["occurrences"][0]["reason"] == (None if settled else "unsettled_key")
        assert result["coverage"]["distinct_target_keys_read"] == int(settled)
        assert "target_resolved" not in {r[0] for r in con.execute("DESCRIBE document_citations").fetchall()}
        with pytest.raises(ToolError, match="Internal"):
            _tool_data(server, "query_sql", {"sql": "SELECT * FROM _spicy_citation_processing_document_citations"})
        with pytest.raises(ToolError, match="Internal"):
            _tool_data(server, "query_sql", {"sql": "SELECT * FROM _spicy_citation_inputs"})


def test_held_text_zero_result_comes_from_receipt_only_read(tmp_path, monkeypatch):
    text = "No cited legal text."
    digest = "sha256:" + hashlib.sha256(text.encode()).hexdigest()
    select(tmp_path, "document_citations", [])
    select(tmp_path, "report_sections", [row("report_sections", package_id="CRPT-119hrpt1",
        part_id="1", seq="0", body=text)])
    key = '["CRPT-119hrpt1","1","0"]'
    select(tmp_path, "document_citation_reads", [row("document_citation_reads",
        document_kind="report_section", document_key=key, text_sha256=digest,
        citation_rows="0", read_at="2026-10-04T00:00:00Z", rule_set_version="test")])
    with connection(tmp_path, monkeypatch)[0] as con:
        server = mcp_server.build_server()
        reply = _tool_data(server, "resolve_document_citations", {
            "document_kind": "report_section", "document_key": key})
        assert reply["source_read"]["status"] == "read_none_found"
        # The private cache must keep only identities and hashes, never a second
        # copy of a source body (comments and full document text can be large).
        cache = "_spicy_citation_digests_report_sections"
        assert {name for name, *_ in con.execute(f"DESCRIBE {cache}").fetchall()} == {
            "package_id", "part_id", "seq", "sha256_body"}
        assert con.execute(f"SELECT sha256_body FROM {cache}").fetchone() == (digest,)
        assert reply["occurrences"] == []
        assert "document_citation_reads" not in mcp_server._available_tables(con)
        assert reply["publication"]["document_citation_reads"]["generation_id"] == "citation-generation"


def test_receiptless_citation_table_is_not_a_tool_fallback(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE document_citations(document_kind VARCHAR)")
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        with pytest.raises(ToolError, match="requires selected native ETL receipts"):
            _tool_data(server, "resolve_document_citations", {
                "document_kind": "budget_volume", "document_key": "BUDGET-2027"})
