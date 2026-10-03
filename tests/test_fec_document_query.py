"""Document query views preserve source grain and unusual/invalid values."""

import json

import duckdb
import pytest

from spicy_regs.relationship_views.fec_document_query import FEC_DOCUMENT_QUERY_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views

SPECS = {spec.name: spec for spec in FEC_DOCUMENT_QUERY_VIEWS}


def source(connection, view, rows, types=None):
    spec = SPECS[view]
    table, columns = next(iter(spec.required.items()))
    types = types or {}
    connection.execute(f"CREATE TABLE {table} (" + ",".join(f'"{c}" {types.get(c, "VARCHAR")}' for c in columns) + ")")
    for row in rows:
        values = [row.get(c) for c in columns]
        connection.execute(f"INSERT INTO {table} VALUES (" + ",".join("?" for _ in values) + ")", values)
    result = install_sql_views(connection, [table], [spec], {table: {"sha256": "source-generation"}})
    assert result[view]["status"] == "available"
    return table


@pytest.fixture
def connection():
    with duckdb.connect() as connection:
        yield connection


def test_measures_keep_exact_values_order_and_uncertainty(connection):
    measures = [
        {
            "native_position": 17,
            "native_label": "Total",
            "value": "12345678901234567890.123456789",
            "value_status": "exact",
            "measure_role": "reported-total",
        },
        {
            "native_position": 18,
            "native_label": "Subtotal",
            "value": "0.000000001",
            "value_status": "exact",
            "measure_role": "reported-subtotal",
        },
        {"native_position": 19, "native_label": "Missing", "value": None, "value_status": "source_empty"},
    ]
    source(
        connection,
        "fec_filing_report_measures",
        [
            {
                "record_id": "r",
                "collection_id": "capture",
                "source_record_id": "src",
                "filing_link_status": "unresolved",
                "reported_measures": json.dumps(measures),
            },
            {"record_id": "empty", "reported_measures": "[]"},
            {"record_id": "null", "reported_measures": None},
            {"record_id": "invalid", "reported_measures": "[null, 17]"},
        ],
        {"reported_measures": "JSON"},
    )
    rows = connection.execute(
        "SELECT source_ordinal, exact_value, value_status, measure_role, filing_link_status "
        "FROM fec_filing_report_measures WHERE record_id = 'r' ORDER BY source_ordinal"
    ).fetchall()
    assert rows == [
        (0, measures[0]["value"], "exact", "reported-total", "unresolved"),
        (1, "0.000000001", "exact", "reported-subtotal", "unresolved"),
        (2, None, "source_empty", None, "unresolved"),
    ]
    assert connection.execute("SELECT count(*) FROM fec_filing_report_measures WHERE record_id='empty'").fetchone() == (
        0,
    )
    assert connection.execute(
        "SELECT collection_status, parsing_status FROM fec_filing_report_measures WHERE record_id='null'"
    ).fetchone() == ("source_null", "collection_unavailable")
    assert connection.execute(
        "SELECT parsing_status FROM fec_filing_report_measures WHERE record_id='invalid' ORDER BY source_ordinal"
    ).fetchall() == [("json_null",), ("unsupported_shape",)]


def test_narrative_expands_multiple_fragments_without_financial_boilerplate(connection):
    source(
        connection,
        "fec_filing_text",
        [
            {
                "record_id": "r",
                "filing_header_record_id": "header",
                "text_fragments": json.dumps(
                    [
                        {"text": "Heading", "text_status": "resolved_source_body"},
                        {"text": "body\n", "text_status": "reported"},
                    ]
                ),
            }
        ],
        {"text_fragments": "JSON"},
    )
    assert connection.execute(
        "SELECT source_ordinal,text,text_status,filing_header_record_id FROM fec_filing_text ORDER BY source_ordinal"
    ).fetchall() == [(0, "Heading", "resolved_source_body", "header"), (1, "body\n", "reported", "header")]
    columns = {row[0] for row in connection.execute("DESCRIBE fec_filing_text").fetchall()}
    assert not columns & {"currency", "amount_kind", "value_mapping_version", "native_fields"}


def test_legal_citations_keep_namespaces_repeats_and_unknown_values(connection):
    source(
        connection,
        "fec_legal_citations",
        [
            {
                "record_id": "r",
                "matter_id": "m",
                "native_facts_json": json.dumps(
                    {
                        "citations": {
                            "regulations": [{"text": "11 CFR", "url": "/relative"}, {"text": "11 CFR"}],
                            "future_namespace": [17],
                            "broken": "unexpected",
                        },
                        "ao_citations": [{"no": "2025-01", "name": "Name"}],
                        "statutory_citations": [{"title": 52, "section": "30101"}],
                        "aos_cited_by": [],
                    }
                ),
            }
        ],
    )
    assert connection.execute("SELECT count(*) FROM fec_legal_citations").fetchone() == (6,)
    assert connection.execute("SELECT DISTINCT source_field FROM fec_legal_citations").fetchall() == [
        ("native_facts_json",)
    ]
    assert connection.execute(
        "SELECT citation_text,url FROM fec_legal_citations WHERE source_pointer='/citations/regulations/0'"
    ).fetchone() == ("11 CFR", "/relative")
    assert connection.execute(
        "SELECT title,section FROM fec_legal_citations WHERE citation_kind='statutory_citations'"
    ).fetchone() == ("52", "30101")
    assert connection.execute(
        "SELECT raw_value_json,parsing_status FROM fec_legal_citations WHERE citation_kind='citations/future_namespace'"
    ).fetchone() == ("17", "unsupported_shape")
    assert connection.execute(
        "SELECT raw_value_json,collection_status FROM fec_legal_citations WHERE citation_kind='citations/broken'"
    ).fetchone() == ('"unexpected"', "unsupported_shape")


def test_subject_tree_preserves_hierarchy_sibling_order_and_invalid_children(connection):
    source(
        connection,
        "fec_legal_subjects",
        [
            {
                "record_id": "r",
                "native_facts_json": json.dumps(
                    {
                        "subject": [
                            {
                                "text": "Parent",
                                "children": [{"text": "Repeated"}, {"text": "Repeated", "children": [None, 42]}],
                            },
                            {"text": "Broken", "children": "oops"},
                        ],
                        "subjects": [{"subject": "Flat", "primary_subject_id": "2"}],
                    }
                ),
            }
        ],
    )
    rows = connection.execute(
        "SELECT source_pointer,parent_pointer,source_ordinal,depth,subject,parsing_status,subject_attributes_json "
        "FROM fec_legal_subjects ORDER BY source_pointer"
    ).fetchall()
    assert len(rows) == 8
    assert connection.execute("SELECT DISTINCT source_field FROM fec_legal_subjects").fetchall() == [
        ("native_facts_json",)
    ]
    assert rows[2][:6] == ("/subject/0/children/1", "/subject/0", 1, 1, "Repeated", "reported")
    assert rows[4][:6] == ("/subject/0/children/1/children/1", "/subject/0/children/1", 1, 2, None, "unsupported_shape")
    assert rows[4][6] == "42"
    assert rows[6][:6] == ("/subject/1/children", "/subject/1", None, 1, "oops", "collection_unavailable")
    assert "children" not in rows[0][6]


def test_install_binds_without_reading_source_rows(connection):
    spec = SPECS["fec_legal_subjects"]
    cols = spec.required["fec_legal_matters"]
    connection.execute(
        "CREATE VIEW fec_legal_matters AS SELECT "
        + ",".join(f"error('must not read source rows')::VARCHAR AS {col}" for col in cols)
    )
    assert install_sql_views(connection, ["fec_legal_matters"], [spec])[spec.name]["status"] == "available"


def test_missing_columns_are_unsupported(connection):
    connection.execute("CREATE TABLE fec_legal_matters(record_id VARCHAR)")
    result = install_sql_views(connection, ["fec_legal_matters"], [SPECS["fec_legal_citations"]])
    assert result["fec_legal_citations"]["status"] == "unsupported"


def test_only_distinct_document_children_add_query_views():
    assert set(SPECS) == {"fec_filing_report_measures", "fec_filing_text", "fec_legal_citations", "fec_legal_subjects"}
