"""Source-stated parent references survive a null docket and schema evolution."""

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
    # Nullable added columns in a legacy table express unread source fields.
    assert all(row["comment_reference_values_json"] is not None for row in rows)


def test_legacy_catalog_adds_nullable_fields_and_preserves_old_rows(tmp_path, monkeypatch):
    """Exercise the actual migration/export SQL across reopened connections."""
    catalog_path = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{catalog_path}' AS {iceberg._CATALOG_ALIAS}")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    with connect() as con:
        con.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
        columns = [column for column in COMMENT.schema if column not in iceberg._COMMENT_ADDED_COLUMNS]
        con.execute(
            f'CREATE TABLE {iceberg._schema_ref()}."comments" (' + ", ".join(f'"{c}" VARCHAR' for c in columns) + ")"
        )
        con.execute(
            f'INSERT INTO {iceberg._schema_ref()}."comments" (comment_id, docket_id) VALUES (?, NULL)', ["prior"]
        )
    with iceberg._connect_for_table(COMMENT) as con:
        fields = ", ".join(iceberg._COMMENT_ADDED_COLUMNS)
        assert con.execute(f"SELECT {fields} FROM {processing_table(con, COMMENT)}").fetchall() == [
            (None,) * len(iceberg._COMMENT_ADDED_COLUMNS)
        ]
        output = iceberg._export_parquet(con, COMMENT, tmp_path / "output")
        assert con.read_parquet(output).columns == list(COMMENT.schema)
        assert con.read_parquet(output).project("comment_reference_values_json").fetchall() == [(None,)]
    with iceberg._connect_for_table(COMMENT) as con:
        assert con.execute(f"SELECT comment_id FROM {processing_table(con, COMMENT)}").fetchall() == [("prior",)]
