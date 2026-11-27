"""Tests for regulations_native comment reference views, including empty original_document_id guard (ss-otwa)."""

from __future__ import annotations

import duckdb

from spicy_regs.relationship_views.regulations_native import (
    install_native_regulations_views,
    native_regulations_views,
)


def test_comment_native_references_excludes_empty_and_whitespace_original_document_id():
    with duckdb.connect() as con:
        con.execute(
            """CREATE TABLE comments (
                comment_id VARCHAR,
                docket_id VARCHAR,
                comment_on_document_id VARCHAR,
                comment_on_object_id VARCHAR,
                original_document_id VARCHAR
            )"""
        )
        con.executemany(
            "INSERT INTO comments VALUES (?, ?, ?, ?, ?)",
            [
                ("C-VALID", "D-1", "DOC-1", "OBJ-1", "ORIG-1"),
                ("C-EMPTY", "D-1", "DOC-2", "OBJ-2", ""),
                ("C-SPACE", "D-1", None, None, "   "),
                ("C-TABS", "D-1", None, None, "\t\n "),
                ("C-NULL", "D-1", "DOC-3", None, None),
            ],
        )
        installed = install_native_regulations_views(con, ["comments"])
        assert installed["comment_native_references"]["status"] == "available"
        assert installed["comment_document_references"]["status"] == "available"
        assert installed["comment_document_references_pairs"]["status"] == "available"

        rows = con.execute(
            "SELECT comment_id, docket_id, target_kind, target_key "
            "FROM comment_native_references ORDER BY comment_id, target_kind"
        ).fetchall()
        assert rows == [
            ("C-EMPTY", "D-1", "regulations_document", "DOC-2"),
            ("C-EMPTY", "D-1", "regulations_object", "OBJ-2"),
            ("C-NULL", "D-1", "regulations_document", "DOC-3"),
            ("C-VALID", "D-1", "regulations_document", "DOC-1"),
            ("C-VALID", "D-1", "regulations_object", "OBJ-1"),
            ("C-VALID", "D-1", "regulations_original_document", "ORIG-1"),
        ]
        # Zero blank/whitespace edges emitted for original_document_id.
        assert con.execute(
            "SELECT count(*) FROM comment_native_references "
            "WHERE target_kind = 'regulations_original_document' AND trim(coalesce(target_key, '')) = ''"
        ).fetchone() == (0,)
        # Non-empty original_document_id still preserves exact lineage and value.
        assert con.execute(
            "SELECT comment_id, target_key FROM comment_native_references "
            "WHERE target_kind = 'regulations_original_document'"
        ).fetchall() == [("C-VALID", "ORIG-1")]
        # comment_document_references_pairs remains unchanged.
        assert con.execute(
            "SELECT comment_id, target_key FROM comment_document_references_pairs ORDER BY comment_id"
        ).fetchall() == [
            ("C-EMPTY", "DOC-2"),
            ("C-NULL", "DOC-3"),
            ("C-VALID", "DOC-1"),
        ]
        specs = {spec.name: spec for spec in native_regulations_views(["comments"])}
        assert "original_document_id" in specs["comment_native_references"].required["comments"]
