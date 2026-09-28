"""Synthetic state and duplicate-role controls; native replay lives in provider tests."""

import json
import duckdb
from spicy_regs.relationship_views import install_relationship_views
from spicy_regs.relationship_views.artifacts_topics import ARTIFACT_SQL_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views


def test_attachment_roles_restrictions_and_unread():
    con = duckdb.connect()
    con.execute("CREATE TABLE documents(document_id VARCHAR,attachments_json VARCHAR,attachment_records_json VARCHAR)")
    records = [
        {
            "id": "a",
            "type": "attachments",
            "attributes": {"fileFormats": [{"fileUrl": "https://example.gov/a", "format": "pdf", "size": 2}]},
        },
        {
            "id": "b",
            "type": "attachments",
            "attributes": {"fileFormats": None, "restrictReasonType": "Confidential Business Information"},
        },
    ]
    con.executemany(
        "INSERT INTO documents VALUES (?,?,?)",
        [
            ("positive", '[{"url":"https://example.gov/a"}]', json.dumps(records)),
            ("empty", None, "[]"),
            ("unread", None, None),
        ],
    )
    install_relationship_views(con, ["documents"])
    install_sql_views(con, ["documents"], ARTIFACT_SQL_VIEWS)
    assert con.execute(
        "SELECT document_id,field_state FROM document_attachment_records_field_states ORDER BY document_id"
    ).fetchall() == [("empty", "empty_array"), ("positive", "populated_array"), ("unread", "sql_null")]
    assert con.execute(
        "SELECT target_key,restriction FROM document_attachment_records_occurrences ORDER BY source_ordinal"
    ).fetchall() == [("a", None), ("b", "Confidential Business Information")]
    assert con.execute(
        "SELECT attachment_id,attachment_ordinal,format_ordinal,artifact_role,offered_url FROM document_attachment_renditions"
    ).fetchall() == [("a", 0, 0, "attachment", "https://example.gov/a")]
    assert con.execute("SELECT count(*) FROM document_artifacts_occurrences").fetchone() == (1,)
    assert con.execute("SELECT acquisition_status,retained_digest FROM document_attachment_renditions").fetchone() == (
        "not_checked",
        None,
    )


def test_old_schema_retains_content_view_and_reports_attachment_unsupported():
    con = duckdb.connect()
    con.execute("CREATE TABLE documents(document_id VARCHAR,attachments_json VARCHAR)")
    metadata = install_relationship_views(con, ["documents"])
    assert metadata["document_artifacts_occurrences"]["status"] == "available"
    assert metadata["document_attachment_records_occurrences"]["status"] == "unsupported"
    assert (
        install_sql_views(con, ["documents"], ARTIFACT_SQL_VIEWS)["document_attachment_renditions"]["status"]
        == "unsupported"
    )


def test_default_document_shaping_does_not_import_provider(monkeypatch):
    import builtins
    from spicy_regs.schemas.regulations import _extract_document

    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.startswith("spicy_docs"):
            raise AssertionError("default consumer shaping imported provider")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    result = _extract_document({"data": {"id": "native-id", "attributes": {}}})
    assert result["attachment_records_json"] is None
