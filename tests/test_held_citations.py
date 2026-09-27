"""Held-field scope, replay, failure preservation and the real MCP boundary."""

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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
        con.execute("CREATE TABLE bill_sections(bill_id VARCHAR,version_code VARCHAR,source VARCHAR,seq INTEGER,body VARCHAR)")
        con.execute("INSERT INTO bill_sections VALUES (?,?,?,?,?), (?,?,?,?,?)", [*keys, "right", *keys[:3], "3", "wrong"])
        assert source_digests(con, "bill_section", key) == [("sha256:" + hashlib.sha256(b"right").hexdigest(),)]
        con.execute("INSERT INTO bill_sections SELECT * FROM bill_sections WHERE seq=2")
        assert len(source_digests(con, "bill_section", key)) == 2  # duplicate parents are ambiguous
    with pytest.raises(ValueError):
        key_values("bill_section", '["missing keys"]')


def test_mcp_checks_literal_field_and_refuses_stale_or_duplicate_parent(tmp_path, monkeypatch):
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
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        args = {"document_kind": "comment_inline", "document_key": "a"}
        found = _tool_data(server, "resolve_document_citations", args)
        assert found["source_read"]["status"] == "read"
        assert found["occurrences"][0]["target_status"] == "found"
        con.execute("UPDATE comments SET comment='changed'")
        assert _tool_data(server, "resolve_document_citations", args)["occurrences"][0]["target_status"] == "not_checked"
        con.execute("INSERT INTO comments SELECT * FROM comments")
        assert _tool_data(server, "resolve_document_citations", args)["source_read"]["status"] == "ambiguous"


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
