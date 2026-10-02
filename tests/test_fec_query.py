"""Exact financial values, physical identity and kind-specific evidence endpoints."""

from dataclasses import replace
from datetime import date
from decimal import Decimal
import json

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

from spicy_regs.relationship_views.fec_typed import typed_record_evidence

from spicy_regs.transforms.fec_query import (
    EVIDENCE_SCHEMA,
    RECEIPT_SCHEMA,
    ReceiptSelection,
    bulk_date,
    bulk_receipt,
    exact_amount,
    observation_id,
    record_evidence,
)

PIN = "sha256:" + "a" * 64


@pytest.mark.parametrize(
    ("raw", "value", "status"),
    [
        ("-0.0100", Decimal("-0.01"), "exact"),
        (".01", Decimal("0.01"), "exact"),
        ("-.99", Decimal("-0.99"), "exact"),
        ("+.0100", Decimal("0.01"), "exact"),
        (".", None, "unsupported_spelling"),
        ("-.", None, "unsupported_spelling"),
        ("99999999999999999999999999999.999999999", Decimal("99999999999999999999999999999.999999999"), "exact"),
        ("100000000000000000000000000000", None, "overflow"),
        ("0.0000000001", None, "excess_precision"),
        ("1.2300000000", Decimal("1.23"), "exact"),
        (None, None, "source_null"),
        ("", None, "source_empty"),
        (0.1, None, "unsupported_spelling"),
        ("1e3", None, "unsupported_spelling"),
        ("NaN", None, "unsupported_spelling"),
        (" 5", None, "unsupported_spelling"),
    ],
)
def test_exact_money_never_rounds_or_uses_float(raw, value, status):
    assert exact_amount(raw) == (value, status)


def test_date_preserves_missing_invalid_and_exact_states():
    assert bulk_date("02292024") == (date(2024, 2, 29), "exact")
    assert bulk_date("02292025") == (None, "invalid_date")
    assert bulk_date("2292024") == (None, "unsupported_spelling")
    assert bulk_date(None) == (None, "source_null")
    assert bulk_date("") == (None, "source_empty")


def source_row():
    fields = dict(
        CMTE_ID="C00000001",
        AMNDT_IND="A",
        RPT_TP="M4",
        TRANSACTION_PGI="P2026",
        IMAGE_NUM="0000000000000000000001",
        TRANSACTION_TP="15",
        ENTITY_TP="IND",
        NAME="Example",
        CITY="City",
        STATE="DC",
        ZIP_CODE="00123",
        EMPLOYER="",
        OCCUPATION="",
        TRANSACTION_DT="03052026",
        TRANSACTION_AMT="5700",
        OTHER_ID="",
        TRAN_ID="0001",
        FILE_NUM="1970601",
        MEMO_CD="",
        MEMO_TEXT="",
        SUB_ID="1234567890123456789012",
        future_field={"unknown": True},
    )
    return dict(
        collection_id="deletions",
        source_record_id="source/ordinal/7",
        source_sha256=PIN,
        source_locator_json=json.dumps(
            dict(
                collection_id="deletions",
                source_record_id="source/ordinal/7",
                member={"name": "itcont.txt", "ordinal": 0},
                ordinal=7,
            )
        ),
        metadata_json=json.dumps(fields),
    )


def selection(role="deletion"):
    return ReceiptSelection("deletions", PIN, "sha256:" + "b" * 64, "official-fec", 2026, role, "sha256:" + "c" * 64)


@pytest.mark.parametrize(
    ("transaction_id", "raw_amount"),
    [("9141314", "50"), ("9144996", "250"), ("9149847", "500")],
)
def test_audited_receipt_values_keep_literal_raw_fields(transaction_id, raw_amount):
    """Independent publisher witnesses retained by the 2026-10-01 Nina audit."""
    row = source_row()
    native = json.loads(row["metadata_json"])
    native.update(TRAN_ID=transaction_id, TRANSACTION_AMT=raw_amount, TRANSACTION_DT="10222025")
    row["metadata_json"] = json.dumps(native)
    before = dict(row)
    typed, evidence = bulk_receipt(row, selection())
    stored = pa.Table.from_pylist([typed], schema=RECEIPT_SCHEMA).to_pylist()[0]
    assert stored["amount_raw"] == raw_amount
    assert stored["transaction_date_raw"] == "10222025"
    assert stored["transaction_date_status"] == stored["amount_status"] == "exact"
    assert stored["amount"] == Decimal(raw_amount)
    assert stored["transaction_date"] == date(2025, 10, 22)
    assert stored["mapping_version"] == "fec-bulk-individual-receipt/2"
    assert row == before and stored["record_id"] == observation_id("fec_receipts", row, "official-fec")
    assert len(evidence) == 1 and stored["correction_operation"] == "deletion"


@pytest.mark.parametrize(
    ("raw_amount", "raw_date", "amount_status", "date_status"),
    [(None, None, "source_null", "source_null"), ("", "", "source_empty", "source_empty"),
     ("-.0100", "02292024", "exact", "exact"),
     ("0.0000000001", "02292025", "excess_precision", "invalid_date")],
)
def test_receipt_raw_values_survive_missing_empty_and_refused_conversion(
    raw_amount, raw_date, amount_status, date_status,
):
    row = source_row()
    native = json.loads(row["metadata_json"])
    native.update(TRANSACTION_AMT=raw_amount, TRANSACTION_DT=raw_date)
    row["metadata_json"] = json.dumps(native)
    typed, _ = bulk_receipt(row, selection())
    assert typed["amount_raw"] == raw_amount and typed["transaction_date_raw"] == raw_date
    assert typed["amount_status"] == amount_status and typed["transaction_date_status"] == date_status
    assert typed["memo_indicator"] == "" and typed["amendment_indicator"] == "A"
    if amount_status != "exact":
        assert typed["amount"] is None and typed["transaction_date"] is None


def test_positive_deletion_stays_a_deletion_and_ids_keep_native_spelling():
    row = source_row()
    typed, evidence = bulk_receipt(row, selection())
    assert typed["amount"] == Decimal("5700")
    assert typed["correction_operation"] == "deletion"
    assert typed["correction_applicability_status"] == typed["current_record_status"] == "unqualified"
    assert typed["source_record_identifier"] == "1234567890123456789012"
    assert typed["transaction_id"] == "0001"
    assert typed["image_number"] == "0000000000000000000001"
    assert typed["contributor_zip"] == "00123"
    assert typed["report_number"] == "1970601" and typed["filing_key"] is None
    assert evidence[0]["witness_generation_pin"] == selection().source_generation_pin
    assert evidence[0]["target_generation_scope"] == "self"
    assert "target_generation_pin" not in evidence[0]
    assert json.loads(row["metadata_json"])["future_field"] == {"unknown": True}
    # Same amount/amendment flag, different evidence-backed collection role.
    assert bulk_receipt(row, selection("snapshot"))[0]["correction_operation"] == "none"


def test_typed_parquet_roundtrip_keeps_decimal_and_evidence_nulls(tmp_path):
    row = source_row()
    native = json.loads(row["metadata_json"])
    native["TRANSACTION_AMT"] = "99999999999999999999999999999.999999999"
    row["metadata_json"] = json.dumps(native)
    typed, evidence = bulk_receipt(row, selection())
    assert set(typed) == set(RECEIPT_SCHEMA.names)
    assert set(evidence[0]) == set(EVIDENCE_SCHEMA.names)
    for name, rows, schema in (("receipts", [typed], RECEIPT_SCHEMA), ("evidence", evidence, EVIDENCE_SCHEMA)):
        path = tmp_path / (name + ".parquet")
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        assert pq.read_table(path).to_pylist() == rows


def test_identity_uses_physical_occurrence_but_not_mapping_or_generation():
    row = source_row()
    identity = observation_id("fec_receipts", row, "official-fec")
    locator = json.loads(row["source_locator_json"])
    row["source_locator_json"] = json.dumps({**locator, "field_mapping": {"new_definition": True}}, indent=2)
    assert observation_id("fec_receipts", row, "official-fec") == identity
    row["source_locator_json"] = json.dumps({**locator, "ordinal": 8})
    assert observation_id("fec_receipts", row, "official-fec") != identity
    assert observation_id("fec_receipts", row, "unofficial-archive") != identity


def test_wrong_collection_digest_or_locator_refuses_before_mapping():
    row = source_row()
    with pytest.raises(ValueError, match="pinned selected collection"):
        bulk_receipt(row, replace(selection(), source_sha256="sha256:" + "d" * 64))
    row["source_locator_json"] = json.dumps({"collection_id": "elsewhere", "source_record_id": row["source_record_id"]})
    with pytest.raises(ValueError, match="locator"):
        bulk_receipt(row, selection())


def test_dictionary_evidence_uses_real_collection_context_without_record_id():
    row = source_row()
    locator = json.loads(row["source_locator_json"])
    locator["field_mapping"] = dict(
        kind="official-html-dictionary",
        definitions_collection_id="summary",
        definitions_pointer="/tableFieldDefinitions",
        source_sha256=PIN,
        table_locator={"byte_start": 19633, "table_ordinal": 0},
    )
    row["source_locator_json"] = json.dumps(locator)
    endpoints = record_evidence("fec_reported_financial_summaries", PIN, row, PIN)
    context = endpoints[1]
    assert context["endpoint_kind"] == "collection_context"
    assert context["source_record_id"] is None
    assert context["collection_id"] == "summary"
    assert context["context_column"] == "collection_outcome_json"
    assert context["context_pointer"] == "/tableFieldDefinitions"
    assert json.loads(context["witness_locator_json"]) == {"byte_start": 19633, "table_ordinal": 0}


def test_header_evidence_checks_the_literal_record_endpoint():
    row = source_row()
    locator = json.loads(row["source_locator_json"])
    locator["field_mapping"] = dict(
        collection_id="header",
        source_record_id="header/0",
        source_sha256=PIN,
        source_locator={"collection_id": "header", "source_record_id": "header/0", "ordinal": 0},
    )
    row["source_locator_json"] = json.dumps(locator)
    assert record_evidence("fec_receipts", PIN, row, PIN)[1]["source_record_id"] == "header/0"
    locator["field_mapping"]["source_locator"]["source_record_id"] = "header/1"
    row["source_locator_json"] = json.dumps(locator)
    with pytest.raises(ValueError, match="header locator"):
        record_evidence("fec_receipts", PIN, row, PIN)


@pytest.mark.parametrize("definition", [None, "explicit_null", "header", "dictionary"])
def test_derived_evidence_equals_stored_oracle_without_losing_multiplicity(definition):
    row = source_row()
    locator = json.loads(row["source_locator_json"])
    if definition == "explicit_null":
        locator["field_mapping"] = None
    elif definition == "header":
        locator["field_mapping"] = dict(
            collection_id="header",
            source_record_id="header/0",
            source_sha256=PIN,
            source_locator={"collection_id": "header", "source_record_id": "header/0", "ordinal": 0},
        )
    elif definition == "dictionary":
        locator["field_mapping"] = dict(
            kind="official-html-dictionary",
            definitions_collection_id="summary",
            definitions_pointer="/tableFieldDefinitions",
            source_sha256=PIN,
            table_locator={"byte_start": 19633, "table_ordinal": 0},
        )
    row["source_locator_json"] = json.dumps(locator)
    typed, expected = bulk_receipt(row, selection())
    with duckdb.connect() as con:
        con.register("fec_receipts", pa.Table.from_pylist([typed, typed], schema=RECEIPT_SCHEMA))
        actual = (
            con.execute(typed_record_evidence("fec_receipts", selection().source_generation_pin))
            .to_arrow_table()
            .to_pylist()
        )

    def as_json(e):
        return json.dumps(e, sort_keys=True)

    assert sorted(map(as_json, actual)) == sorted(map(as_json, expected + expected))


def test_derived_evidence_requires_an_explicit_source_pin():
    with pytest.raises(ValueError, match="selected source-generation pin"):
        typed_record_evidence("fec_receipts", "latest")
