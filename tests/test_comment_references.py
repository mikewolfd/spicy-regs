"""Source-stated parent references survive native subject and receipt storage."""

import json
from pathlib import Path

import duckdb
import polars as pl

from spicy_regs.schemas.regulations import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources.regulatory_catalog import processing_table


FIXTURE = Path(__file__).parent / "fixtures/comments_null_dockets/ODNI-2009-0004-0002.source.json"


def test_native_comment_parent_is_separate_from_docket_and_object_reference(tmp_path):
    raw = json.loads(FIXTURE.read_text())
    row = COMMENT.extract(raw)
    assert row["comment_id"] == "ODNI-2009-0004-0002"
    assert row["docket_id"] is None
    assert row["comment_on_document_id"] == "ODNI-2009-0004-0001"
    assert row["comment_on_object_id"] == "0900006480a18cfe"
    assert row["original_document_id"] == "ODNI_FRDOC_0001-0004"
    assert json.loads(row["comment_reference_values_json"]) == {
        key: raw["data"]["attributes"][key] for key in ("commentOnDocumentId", "commentOn", "originalDocumentId")
    }
    output = tmp_path / "comments.parquet"
    pl.DataFrame([row], schema=COMMENT.schema).write_parquet(output)
    with duckdb.connect() as con:
        assert con.read_parquet(output).project("comment_on_document_id, docket_id").fetchall() == [
            ("ODNI-2009-0004-0001", None)
        ]


def test_reference_observations_distinguish_absent_null_empty_and_unread():
    rows = [
        COMMENT.extract({"data": {"attributes": value}})
        for value in (
            {},
            {"commentOnDocumentId": None},
            {"commentOnDocumentId": ""},
        )
    ]
    assert [json.loads(row["comment_reference_values_json"]) for row in rows] == [
        {},
        {"commentOnDocumentId": None},
        {"commentOnDocumentId": ""},
    ]
    assert [row["comment_on_document_id"] for row in rows] == [None, None, ""]
    # Receipts retain whether the source field was absent, null, or empty.
    assert all(row["comment_reference_values_json"] is not None for row in rows)




def test_native_parent_and_exact_source_observations_survive_catalog(tmp_path):
    row = COMMENT.extract(json.loads(FIXTURE.read_text()))
    with duckdb.connect() as con:
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        con.register("source", pl.DataFrame([row], schema=COMMENT.schema).to_arrow())
        iceberg.replace_rows(con, COMMENT, "source")
        restored = con.execute(f"SELECT * FROM {processing_table(con, COMMENT)}").to_arrow_table().to_pylist()
        assert restored == [row]
        output = iceberg._export_parquet(con, COMMENT, tmp_path / "out")
        native = pl.read_parquet(output)
        assert native["docket_id"].to_list() == [None]
        assert native["comment_on_document_id"].to_list() == ["ODNI-2009-0004-0001"]
        assert "comment_reference_values_json" not in native.columns
