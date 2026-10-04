"""Native FEC business values retain their grain and exact interpretation."""

from decimal import Decimal
import json

import pyarrow as pa
import pytest

from spicy_regs.transforms.fec_native_subjects import prepare_subject, subject_schema


def test_legal_hierarchy_ids_status_and_duplicate_citations():
    facts = dict(
        rm_id=3472947,
        rm_number="REG 2024-08",
        case_serial=9,
        ao_citations=[dict(name="A", no="2025-01"), dict(name="A", no="2025-01"), None],
        subject=[dict(text="Contributions", children=[dict(text="Limits"), dict(text="Limits", children=[])])],
        subjects=[dict(primary_subject_id="1", secondary_subject_id="2", subject="A")],
    )
    row = dict(
        record_id="id",
        status="Closed",
        civil_penalty_payment_status="Paid In Full",
        native_facts_json=json.dumps(facts),
        mapping_status="mapped",
    )
    subject, inputs = prepare_subject("fec_legal_matters", row)
    assert subject["rm_id"] == "3472947"
    assert subject["rm_number"] == "REG 2024-08"
    assert subject["status"] == "Closed"
    assert subject["civil_penalty_payment_status"] == "Paid In Full"
    assert subject["ao_citations"][0] == subject["ao_citations"][1]
    assert subject["ao_citations"][2] is None
    assert [n["path"] for n in subject["subject"]] == [[0], [0, 0], [0, 1]]
    assert subject["subject"][1]["node"]["children"] is None
    assert subject["subject"][2]["node"]["children"] == []
    assert subject["subjects"][0]["secondary_subject_id"] == "2"
    assert inputs["native_facts_json"] == row["native_facts_json"]
    assert "mapping_status" not in subject


@pytest.mark.parametrize("value", [None, [], [None]])
def test_lists_keep_null_empty_and_null_element(value):
    row = dict(record_id="id", reported_measures=value)
    subject, inputs = prepare_subject("fec_filing_report_observations", row)
    assert subject["reported_measures"] == value
    assert inputs["reported_measures"] == value


def test_report_values_are_native_decimals_with_distinct_roles():
    value = Decimal("12345678901234567890.123456789")
    measure = dict(
        native_position=1,
        native_label="Total",
        definition_cell="B1",
        raw_value=str(value),
        value=value,
        value_status="exact",
        quantity_kind="reported-money",
        measure_role="reported-total",
        period_basis="calendar-year",
    )
    row = dict(record_id="id", reported_measures=[measure, None, {**measure, "measure_role": "reported-subtotal"}])
    subject, inputs = prepare_subject("fec_filing_report_observations", row)
    schema = subject_schema(
        "fec_filing_report_observations", pa.schema([("record_id", pa.string()), ("reported_measures", pa.string())])
    )
    restored = pa.Table.from_pylist([subject], schema=schema).to_pylist()[0]
    assert restored["reported_measures"][0]["value"] == value
    assert [v["measure_role"] if v else None for v in restored["reported_measures"]] == [
        "reported-total",
        None,
        "reported-subtotal",
    ]
    assert "raw_value" not in restored["reported_measures"][0]
    assert inputs["reported_measures"] == row["reported_measures"]


def test_receipt_only_definition_does_not_produce_empty_subject():
    row = dict(
        record_id="layout",
        family="electronic",
        version="8.5",
        form="F3",
        workbook_sha256="sha256:a",
        layout_json='{"fields":[]}',
    )
    subject, inputs = prepare_subject("fec_filing_definitions", row)
    assert subject is None
    assert inputs == row


def test_unknown_input_and_nested_property_refuse_without_silent_loss():
    with pytest.raises(ValueError, match="Unclassified"):
        prepare_subject("fec_receipts", dict(record_id="id", new_money=Decimal("5")))
    with pytest.raises(ValueError, match="Unsupported"):
        prepare_subject("fec_legal_matters", dict(record_id="id", native_facts_json='{"new_meaning":1}'))
    with pytest.raises(ValueError, match="Unsupported"):
        prepare_subject(
            "fec_filing_text_observations", dict(record_id="id", text_fragments=[dict(text="a", new_meaning="b")])
        )


def test_agency_organizations_and_context_preserve_order_and_repeats():
    row = dict(
        record_id="r",
        organizations_json=json.dumps(
            [dict(id="O1", element=1, names=[dict(value="A", element=2), dict(value="A", element=3)], abbreviations=[])]
        ),
        metadata_json="{}",
    )
    subject, _ = prepare_subject("fec_agency_reports", row)
    assert subject["organizations"][0]["names"] == ["A", "A"]
    assert subject["organizations"][0]["abbreviations"] == []
    assert "element" not in subject["organizations"][0]


def test_financial_source_corrections_are_data_and_qualification_is_receipt():
    subject, inputs = prepare_subject(
        "fec_receipts",
        dict(
            record_id="r",
            correction_operation="deletion",
            amount=Decimal("1"),
            amount_status="exact",
            source_representation_role="deletion",
            current_record_status="unqualified",
        ),
    )
    assert subject == dict(record_id="r", correction_operation="deletion", amount=Decimal("1"))
    assert inputs["amount_status"] == "exact"
    assert inputs["source_representation_role"] == "deletion"


def test_native_child_views_keep_decimal_order_and_hierarchy():
    import duckdb
    from spicy_regs.relationship_views.fec_native_document_query import FEC_NATIVE_DOCUMENT_QUERY_VIEWS
    from spicy_regs.relationship_views.sql_views import install_sql_views

    specs = {s.name: s for s in FEC_NATIVE_DOCUMENT_QUERY_VIEWS}
    row = dict(
        record_id="r",
        filing_key=None,
        currency="USD",
        reported_measures=[
            dict(
                native_position=2,
                native_label="Total",
                value=Decimal("999999999999999999.123456789"),
                quantity_kind="money",
                measure_role="total",
                period_basis="year",
            )
        ]
        * 2,
    )
    schema = subject_schema(
        "fec_filing_report_observations",
        pa.schema(
            [
                ("record_id", pa.string()),
                ("filing_key", pa.string()),
                ("currency", pa.string()),
                ("reported_measures", pa.string()),
            ]
        ),
    )
    with duckdb.connect() as con:
        con.register("fec_filing_report_observations", pa.Table.from_pylist([row], schema=schema))
        spec = specs["fec_filing_report_measures"]
        install_sql_views(con, ["fec_filing_report_observations"], [spec], {})
        assert con.execute(
            "SELECT source_ordinal, exact_value FROM fec_filing_report_measures ORDER BY source_ordinal"
        ).fetchall() == [(0, Decimal("999999999999999999.123456789")), (1, Decimal("999999999999999999.123456789"))]
        legal, _ = prepare_subject(
            "fec_legal_matters",
            dict(
                record_id="m",
                matter_id="matter",
                native_facts_json=json.dumps(
                    dict(
                        subject=[dict(text="A", children=[dict(text="B"), dict(text="B")])],
                        citations=dict(regulations=[dict(text="11 CFR", url="u")] * 2),
                    )
                ),
            ),
        )
        schema = subject_schema(
            "fec_legal_matters",
            pa.schema([("record_id", pa.string()), ("matter_id", pa.string()), ("native_facts_json", pa.string())]),
        )
        con.register("fec_legal_matters", pa.Table.from_pylist([legal], schema=schema))
        for name in ("fec_legal_citations", "fec_legal_subjects"):
            install_sql_views(con, ["fec_legal_matters"], [specs[name]], {})
        assert con.execute("SELECT count(*) FROM fec_legal_citations").fetchone() == (2,)
        assert con.execute("SELECT subject FROM fec_legal_subjects ORDER BY ordinal_path").fetchall() == [
            ("A",),
            ("B",),
            ("B",),
        ]


def test_publisher_report_type_does_not_overwrite_family_report_type():
    row = dict(
        record_id="r",
        report_type="oversight_report",
        organizations_json="[]",
        metadata_json=json.dumps(dict(fields=[dict(native_field="field-report-type", value="Audit")])),
    )
    subject, _ = prepare_subject("fec_agency_reports", row)
    assert subject["report_type"] == "oversight_report"
    assert subject["published_report_type"] == "Audit"


def test_legal_null_node_differs_from_null_text_node():
    raw = [None, dict(text=None), dict(text="A", children=[None])]
    subject, _ = prepare_subject(
        "fec_legal_matters", dict(record_id="id", native_facts_json=json.dumps(dict(subject=raw)))
    )
    nodes = subject["subject"]
    assert nodes[0] == dict(path=[0], node=None)
    assert nodes[1] == dict(path=[1], node=dict(text=None, children=None))
    assert nodes[3] == dict(path=[2, 0], node=None)


def test_agency_text_keeps_reading_order_without_source_locator():
    row = dict(record_id="r", text="Paragraph", source_locator_json='{"ordinal":17}')
    subject, inputs = prepare_subject("fec_agency_report_text", row)
    assert subject["text_ordinal"] == 17
    assert "source_locator_json" not in subject
    assert inputs["source_locator_json"] == row["source_locator_json"]
