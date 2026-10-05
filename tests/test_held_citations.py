"""Held-field scope, replay, failure preservation and the real MCP boundary."""

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server
from spicy_regs.citation_sources import document_key, key_values, source_digests
from spicy_regs.transforms import held_citations as held
from spicy_regs.transforms.read_checkpoints import read_checkpoints
from tests.test_mcp_server import _tool_data

PIN = {"comments": {"sha256": "sha256:" + "a" * 64}}


def connection():
    con = duckdb.connect()
    con.execute("CREATE TABLE comments(comment_id VARCHAR, comment VARCHAR)")
    return con


def run(con, out, ids, **kwargs):
    return held.build_held_citations(
        out, cursor=con, selections=[held.Selection("comment_inline", (key,)) for key in ids],
        input_pins=PIN, download_prior=lambda *_: False, **kwargs,
    )


def test_exact_field_spans_zero_checkpoint_and_unchanged_skip(tmp_path, monkeypatch):
    with connection() as con:
        body = "<p>See 5 U.S.C. 552 and H.R. 12; the latter has no stated Congress. $1 million in FY 2027.</p>"
        con.execute("INSERT INTO comments VALUES ('a', ?), ('empty','No references here.')", [body])
        path = run(con, tmp_path, ["a", "empty"])
        rows = pq.read_table(path).to_pylist()
        assert rows and all(r["document_kind"] == "comment_inline" for r in rows)
        assert {r["cite_kind"] for r in rows} == {"usc_section", "bill_number"}
        assert all(body[int(r["span_start"]):int(r["span_end"])] == r["matched_text"] for r in rows)
        assert all(r["text_sha256"] == "sha256:" + hashlib.sha256(body.encode()).hexdigest() for r in rows)
        bill = next(r for r in rows if r["cite_kind"] == "bill_number")
        assert bill["target_resolved"] == "false"  # never borrow the current Congress
        states = read_checkpoints(path, held.NAMESPACE)
        assert next(s for s in states if s["document_key"] == "empty")["findings"] == 0
        monkeypatch.setattr(held, "find_citations", lambda *_a, **_k: pytest.fail("unchanged field reparsed"))
        run(con, tmp_path, ["a", "empty"])
        assert pq.read_table(path).to_pylist() == rows
        assert all(r["status"] == "unchanged_complete_field" for r in json.loads((tmp_path / "held-citation-reads.json").read_text()))


def test_changed_rule_zero_clears_only_same_kind_key_text_rule(tmp_path, monkeypatch):
    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('same', '5 U.S.C. 552'), ('other', '5 U.S.C. 552')")
        path = run(con, tmp_path, ["same", "other"], kinds=("usc_section",))
        table = pq.read_table(path)
        rows = table.to_pylist()
        # A different document kind may legitimately use the same native ID.
        foreign = {**rows[0], "document_kind": "budget_volume"}
        pq.write_table(pa.Table.from_pylist(rows + [foreign], schema=table.schema), path)
        monkeypatch.setattr(held, "ADAPTER_VERSION", "held-fields/corrected")
        monkeypatch.setattr(held, "find_citations", lambda *_a, **_k: [])
        run(con, tmp_path, ["same"], kinds=("usc_section",))
        result = pq.read_table(path).to_pylist()
        assert {(r["document_kind"], r["document_key"]) for r in result} == {
            ("comment_inline", "other"), ("budget_volume", foreign["document_key"]),
        }


@pytest.mark.parametrize("failure", ["missing", "null", "duplicate", "cap", "read_failure"])
def test_failed_read_preserves_prior_rows_and_checkpoint(tmp_path, failure):
    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('a', '5 U.S.C. 552')")
        path = run(con, tmp_path, ["a"])
        before, checkpoints = pq.read_table(path).to_pylist(), read_checkpoints(path, held.NAMESPACE)
        kwargs = {}
        if failure == "missing":
            con.execute("DELETE FROM comments")
        elif failure == "null":
            con.execute("UPDATE comments SET comment=NULL")
        elif failure == "duplicate":
            con.execute("INSERT INTO comments SELECT * FROM comments")
        elif failure == "cap":
            kwargs["max_field_bytes"] = 1
        else:
            con.execute("DROP TABLE comments")
        run(con, tmp_path, ["a"], **kwargs)
        assert pq.read_table(path).to_pylist() == before
        assert read_checkpoints(path, held.NAMESPACE) == checkpoints


def test_composite_full_identity_and_actual_field_digest():
    keys = ('BILL-1";--', "ih", "congress.gov", "2")
    key = document_key("bill_section", keys)
    assert key_values("bill_section", key) == keys
    with duckdb.connect() as con:
        con.execute("CREATE TABLE bill_sections(bill_id VARCHAR,version_code VARCHAR,printing_id VARCHAR,seq INTEGER,body VARCHAR)")
        con.execute("INSERT INTO bill_sections VALUES (?,?,?,?,?), (?,?,?,?,?)", [*keys, "right", *keys[:3], "3", "wrong"])
        assert source_digests(con, "bill_section", key) == [("sha256:" + hashlib.sha256(b"right").hexdigest(),)]
        con.execute("INSERT INTO bill_sections SELECT * FROM bill_sections WHERE seq=2")
        assert len(source_digests(con, "bill_section", key)) == 2  # duplicate parents are ambiguous
    with pytest.raises(ValueError):
        key_values("bill_section", '["missing keys"]')


def test_mcp_checks_literal_field_and_refuses_stale_or_duplicate_parent(tmp_path, monkeypatch):
    from tests.citation_fixtures import prepare_citation_inputs

    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('a', 'Public Law 114-254')")
        path = run(con, tmp_path, ["a"])
        con.execute("CREATE TABLE document_citations AS SELECT * FROM read_parquet(?)", [str(path)])
        con.execute("CREATE TABLE laws(law_id VARCHAR,congress VARCHAR,law_type VARCHAR,number VARCHAR)")
        con.execute("INSERT INTO laws VALUES ('114-public-254','114','public','254')")
        con.execute("CREATE TABLE _spicy_publication(snapshot VARCHAR)")
        con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps({"families": {
            "laws": {"artifactDigest": "sha256:" + "a" * 64, "tables": {"laws.parquet": {}}},
        }})])
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: prepare_citation_inputs(con))
        server = mcp_server.build_server()
        args = {"document_kind": "comment_inline", "document_key": "a"}
        found = _tool_data(server, "resolve_document_citations", args)
        assert found["source_read"]["status"] == "read"
        assert found["occurrences"][0]["target_status"] == "found"
        con.execute("UPDATE comments SET comment='changed'")
        assert _tool_data(server, "resolve_document_citations", args)["occurrences"][0]["target_status"] == "not_checked"
        con.execute("INSERT INTO comments SELECT * FROM comments")
        with pytest.raises(ToolError, match="Duplicate"):
            _tool_data(server, "resolve_document_citations", args)


def test_selection_requires_full_keys_and_immutable_input(tmp_path):
    with pytest.raises(ValueError):
        held.parse_selections([{"kind": "comment_inline", "keys": ["a"]}] * 2)
    with connection() as con, pytest.raises(ValueError, match="immutable"):
        held.build_held_citations(tmp_path, cursor=con, selections=[held.Selection("comment_inline", ("a",))],
                                  input_pins={}, download_prior=lambda *_: False)


def test_native_comment_negative_fields_remain_complete(tmp_path):
    # Real retained native inputs, not an attachment or whole-document claim.
    fixtures = Path(__file__).parent / "fixtures/comments_null_dockets"
    bodies = [json.loads(p.read_text())["data"]["attributes"]["comment"] for p in sorted(fixtures.glob("*.source.json"))]
    assert len(bodies) == 3
    with connection() as con:
        con.executemany("INSERT INTO comments VALUES (?, ?)", [(str(i), body) for i, body in enumerate(bodies)])
        path = run(con, tmp_path, [str(i) for i in range(len(bodies))])
        assert pq.read_table(path).num_rows == 0
        assert len(read_checkpoints(path, held.NAMESPACE)) == len(bodies)


class _Recording:
    """Cursor stand-in that records statements and forwards them."""

    def __init__(self, con):
        self.con, self.statements = con, []

    def execute(self, sql, parameters=None):
        self.statements.append(sql)
        return self.con.execute(sql, parameters or [])

    def interrupt(self):
        self.con.interrupt()


def test_selected_fields_read_with_two_scans_per_kind_on_native_key_types():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE bill_sections(bill_id VARCHAR,version_code VARCHAR,printing_id VARCHAR,seq INTEGER,body VARCHAR)")
        con.executemany("INSERT INTO bill_sections VALUES ('B','ih','congress.gov',?,?)",
                        [(n, f"section {n}") for n in range(1, 6)])
        selections = [held.Selection("bill_section", ("B", "ih", "congress.gov", key)) for key in ("2", "02", "5", "9")]
        cursor = _Recording(con)
        reads = held._read_fields(cursor, selections, held.MAX_FIELD_BYTES)
    assert [(text, status) for text, status, _ in reads] == [
        ("section 2", "complete_field"), (None, "missing"), ("section 5", "complete_field"), (None, "missing"),
    ]  # "02" keeps the literal-spelling rule even though it casts to 2
    scans = [sql for sql in cursor.statements if "JOIN" in sql]
    assert len(scans) == 2 and all('t."seq" = TRY_CAST' in sql and 'CAST(t."seq" AS VARCHAR) =' in sql for sql in scans)


def test_run_byte_budget_applies_in_selection_order(monkeypatch):
    monkeypatch.setattr(held, "MAX_TOTAL_FIELD_BYTES", 10)
    with connection() as con:
        con.executemany("INSERT INTO comments VALUES (?, ?)",
                        [("a", "x" * 6), ("b", "y" * 6), ("c", "z" * 3), ("d", "w"), ("e", "v")])
        reads = held._read_fields(con, [held.Selection("comment_inline", (k,)) for k in "abcde"], held.MAX_FIELD_BYTES)
    assert [status for _, status, _ in reads] == [
        "complete_field", "field_byte_cap", "complete_field", "complete_field", "run_byte_cap",
    ]


def test_a_held_read_that_finds_nothing_is_a_document_citation_reads_row(tmp_path):
    """document_citation_reads states each held-field read, so a field read with no citation is told from an unread one.

    Until now a zero-result read was only a checkpoint in document_citations' Parquet metadata, which the server
    does not read: every held field with no rows answered "not read" (round 5 S5-2, the server's open question 1).
    """
    from spicy_docs.interpretation.citations import CITATION_RULE_SET_VERSION

    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('a', '5 U.S.C. 552'), ('empty', 'No references here.')")
        citations = run(con, tmp_path, ["a", "empty", "missing"])
        reads = held.write_citation_reads(tmp_path, citations)
        rows = {row["document_key"]: row for row in pq.read_table(reads).to_pylist()}
    assert pq.read_schema(reads).names == list(held.READS_COLUMNS)
    assert set(rows) == {"a", "empty"}, "a selection that read no field is no read"
    assert (rows["empty"]["citation_rows"], rows["a"]["citation_rows"]) == ("0", "1")
    assert {row["rule_set_version"] for row in rows.values()} == {CITATION_RULE_SET_VERSION}
    assert all(row["read_at"] and row["text_sha256"].startswith("sha256:") for row in rows.values())
    assert {row["document_kind"] for row in rows.values()} == {"comment_inline"}


def test_each_read_states_the_input_generation_it_read(tmp_path):
    """The checkpoint pins the source table each read came from; the reads table keeps that pin, not only the
    citation table's Parquet metadata, which the server never reads (round 6, H2)."""
    pin = {"family": "comments-text", "artifactDigest": "sha256:" + "b" * 64, "sha256": "sha256:" + "a" * 64,
           "byteSize": 1234}
    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('empty', 'No references here.')")
        citations = held.build_held_citations(tmp_path, cursor=con, selections=[held.Selection("comment_inline", ("empty",))],
                                              input_pins={"comments": pin}, download_prior=lambda *_: False)
    [row] = pq.read_table(held.write_citation_reads(tmp_path, citations)).to_pylist()
    assert (row["source_table"], row["input_family"], row["input_generation"], row["input_sha256"]) == (
        "comments", "comments-text", "sha256:" + "b" * 64, "sha256:" + "a" * 64)


def test_a_read_from_before_the_reads_table_states_no_time_or_rule_set(tmp_path):
    """A checkpoint written before read_at and rule_set_version existed is still a read, with those two NULL."""
    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('empty', 'No references here.')")
        citations = run(con, tmp_path, ["empty"])
    from spicy_regs.transforms.read_checkpoints import checkpoint_metadata

    states = [{k: v for k, v in state.items() if k not in ("read_at", "rule_set_version")}
              for state in read_checkpoints(citations, held.NAMESPACE)]
    table = pq.read_table(citations)
    pq.write_table(table.replace_schema_metadata(checkpoint_metadata(citations, held.NAMESPACE, states)), citations)
    [row] = pq.read_table(held.write_citation_reads(tmp_path, citations)).to_pylist()
    assert (row["document_key"], row["citation_rows"], row["read_at"], row["rule_set_version"]) == (
        "empty", "0", None, None)


def _native_print_family(tmp_path, monkeypatch):
    from spicy_docs.schemas import TABLE_CONTRACTS
    from spicy_regs.contract_types import arrow_schema
    from spicy_regs.pipelines.rollups import print_citations as rollup
    import shutil

    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('empty', 'No references here.')")
        citations = run(con, tmp_path, ["empty"])

    def build(output_dir, **_):
        paths = []
        for name in ("house_activity_reports", "budget_volumes", "bill_committee_actions"):
            path = output_dir / (name + ".parquet")
            pq.write_table(arrow_schema(TABLE_CONTRACTS[name]).empty_table(), path)
            paths.append(path)
        target = output_dir / "document_citations.parquet"
        shutil.copyfile(citations, target)
        return (*paths, target)

    monkeypatch.setattr(rollup, "build_print_citations", build)
    return rollup.PrintCitationsRollup(output_dir=tmp_path).build(tmp_path)


def test_the_print_citations_family_rebuilds_the_reads_from_its_merged_citation_table(tmp_path, monkeypatch):
    """Successful empty reads survive as shared receipts beside the complete native subject family."""
    from spicy_regs.pipelines.rollups.print_citations import PrintCitationsRollup
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors

    _native_print_family(tmp_path, monkeypatch)
    reads = SelectedPriors(tmp_path / "verify", root=tmp_path).get("document_citation_reads")
    assert [row["document_key"] for row in pq.read_table(reads).to_pylist()] == ["empty"]
    assert "document_citation_reads.parquet" not in PrintCitationsRollup.outputs
    assert "document_citation_reads.parquet" in PrintCitationsRollup.source_outputs
    assert next(p for p in PrintCitationsRollup.receipt_policies if p.dataset == "document_citation_reads").receipt_only


def test_the_held_citations_rollup_records_its_familys_citation_table_as_no_parent(tmp_path, monkeypatch):
    """Its own verified input belongs to prior-generation evidence, never the dependency parents."""
    from spicy_regs.pipelines.rollups.held_citations import HeldCitationsRollup

    _native_print_family(tmp_path, monkeypatch)
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"selections": [{"kind": "comment_inline", "keys": ["a"]}],
                                     "input_generations": {"comments": "sha256:" + "c" * 64}}))
    assert HeldCitationsRollup(selection=selection, output_dir=tmp_path)._prime(tmp_path) == {}


def test_retained_held_field_bytes_have_claimed_evidence_members(tmp_path):
    from spicy_regs.source_evidence import CaptureEvidence, verify_evidence

    evidence = CaptureEvidence(tmp_path, "held-citations")
    with connection() as con:
        con.execute("INSERT INTO comments VALUES ('a', 'See 5 U.S.C. 552.')")
        run(con, tmp_path, ["a"], evidence=evidence)
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    retained = next(row for row in journal if row["event"] == "retained-file")
    assert retained["stage"] == "held-citation-field"
    assert retained["source_field"] == "comment"
    assert any(row["event"] == "held-citation-read" for row in journal)
