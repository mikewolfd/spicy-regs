"""Filing mappings preserve physical positions and distinct financial meanings."""

from copy import deepcopy
from dataclasses import replace
from datetime import date
from decimal import Decimal
import json

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.relationship_views.fec_typed import filing_record_evidence
from spicy_regs.transforms.fec_filing_definitions import bind_definition, definition_contexts_from_collections
from spicy_regs.transforms.fec_filing_financial import (
    ALLOCATED_DISBURSEMENTS,
    DEBTS,
    GUARANTORS,
    INAUGURAL_DONATIONS,
    INAUGURAL_REFUNDS,
    LOANS,
    MAPPINGS,
    RECEIPTS,
    WORKBOOK,
    load_layout,
    map_filing_financial,
)
from spicy_regs.transforms.fec_query import CollectionSelection, EVIDENCE_SCHEMA

PIN = "sha256:" + "a" * 64
GEN = "sha256:" + "b" * 64
SELECTION = CollectionSelection("filing", PIN, GEN, "official-fec", None, "snapshot", PIN)


def source(native, ordinal):
    return dict(
        collection_id="filing",
        source_record_id=str(ordinal),
        source_sha256=PIN,
        source_locator_json=json.dumps(
            dict(collection_id="filing", source_record_id=str(ordinal), ordinal=ordinal, member=None)
        ),
        metadata_json=json.dumps(native),
    )


def header(version="8.5"):
    return source(dict(kind="header", format_version=version, fields=["HDR", "FEC", version]), 0)


def native(mapping, record_type, updates=(), *, width=None):
    layout = load_layout(mapping)
    count = len(layout["fields"]) if width is None else width
    fields = dict.fromkeys(map(str, range(count)), "")
    fields.update(dict(updates))
    fields["0"] = record_type
    return dict(kind="record", record_type=record_type, field_count=count, fields=fields)


def mapped(mapping, record_type, updates=(), *, width=None):
    return map_filing_financial(
        source(native(mapping, record_type, updates, width=width), 1),
        SELECTION,
        mapping,
        layout=load_layout(mapping),
        header_row=header(),
    )


@pytest.mark.parametrize("mapping", MAPPINGS)
def test_every_definition_is_pinned_and_every_position_has_one_destination(mapping):
    layout = load_layout(mapping)
    assert layout["source"]["sha256"] == WORKBOOK
    assert len(mapping.schema.names) == len(set(mapping.schema.names))


def test_receipt_amount_is_not_aggregate_and_source_id_is_not_filing_id():
    row, evidence = mapped(
        RECEIPTS,
        "SA11AI",
        {
            "1": "C00000001",
            "2": "0000123",
            "7": "Smith",
            "16": "00123",
            "19": "20240229",
            "20": "-.99",
            "21": "10000.50",
            "42": "X",
        },
    )
    assert row["amount"] == Decimal("-.99") and row["reported_aggregate_amount"] == Decimal("10000.50")
    assert row["transaction_date"] == date(2024, 2, 29) and row["transaction_id"] == "0000123"
    assert row["contributor_last_name"] == "Smith" and row["contributor_zip"] == "00123"
    assert row["memo_indicator"] == "X" and row["current_record_status"] == "unqualified"
    assert row["filing_key"] is None and row["filing_link_status"] == "unresolved"
    assert row["correction_operation"] == "none" and [e["role"] for e in evidence] == ["primary", "filing_header"]
    assert pa.Table.from_pylist([row], schema=RECEIPTS.schema).to_pylist() == [row]


def test_short_rows_blank_fields_and_body_references_remain_distinct():
    row, _ = mapped(RECEIPTS, "SA12", {"20": "0"}, width=21)
    assert row["amount"] == 0 and row["reported_aggregate_amount_status"] == "source_missing"
    assert row["transaction_date_status"] == "source_empty" and row["memo_indicator"] is None
    assert json.loads(row["mapping_reason_json"])["absent_positions"] == list(range(21, 45))
    n = native(RECEIPTS, "SA11AI")
    del n["fields"]["43"]
    n["embedded_bodies"] = [{"field_index": 43, "body_id": "text-1"}]
    row, _ = map_filing_financial(source(n, 1), SELECTION, RECEIPTS, layout=load_layout(RECEIPTS), header_row=header())
    assert row["memo_text"] is None and json.loads(row["mapping_reason_json"])["body_positions"] == [43]
    assert row["mapping_status"] == "partial"


def test_extra_values_remain_visible_and_do_not_shift_amounts():
    row, _ = mapped(RECEIPTS, "SA12", {"20": "15.000000001", "45": "unknown value"}, width=46)
    assert row["amount"] == Decimal("15.000000001")
    assert json.loads(row["mapping_reason_json"])["extra_positions"] == {"45": "unknown value"}
    assert row["mapping_status"] == "partial"


@pytest.mark.parametrize("version", ["8.5 ", "8.4", "v8.5"])
def test_header_versions_are_not_trimmed_or_rounded(version):
    with pytest.raises(ValueError, match="exact version"):
        map_filing_financial(
            source(native(RECEIPTS, "SA11AI"), 1),
            SELECTION,
            RECEIPTS,
            layout=load_layout(RECEIPTS),
            header_row=header(version),
        )


@pytest.mark.parametrize("form", ["SB17", "SA3L", "SA", "SA11AIextra"])
def test_unrelated_or_bundled_records_do_not_become_itemized_receipts(form):
    # The generic pattern identifies field layout, not code validity. A source
    # still needs financial policy; exclude the three semantically different forms.
    if form == "SA11AIextra":
        row, _ = mapped(RECEIPTS, form)
        assert row["submission_conformance_status"] == "not-qualified"
    else:
        with pytest.raises(ValueError, match="record type"):
            mapped(RECEIPTS, form)


def test_changed_definition_or_header_source_refuses_mapping():
    layout = load_layout(RECEIPTS)
    layout["fields"][20]["label"] = "wrong amount"
    with pytest.raises(ValueError, match="reviewed layout"):
        map_filing_financial(
            source(native(RECEIPTS, "SA12"), 1), SELECTION, RECEIPTS, layout=layout, header_row=header()
        )
    other = header()
    other["source_sha256"] = GEN
    with pytest.raises(ValueError, match="selected collection"):
        map_filing_financial(
            source(native(RECEIPTS, "SA12"), 1), SELECTION, RECEIPTS, layout=load_layout(RECEIPTS), header_row=other
        )


def test_loans_debts_and_allocations_keep_stock_and_flow_separate():
    loan, _ = mapped(LOANS, "SC/10", {"18": "1000", "19": "100", "20": "900", "22": "ON DEMAND"})
    assert (loan["original_loan_amount"], loan["payments_to_date"], loan["outstanding_balance"]) == (1000, 100, 900)
    assert loan["due_date_terms"] == "ON DEMAND"
    debt, _ = mapped(DEBTS, "SD10", {"16": "900", "17": "50", "18": "20", "19": "930"})
    assert (debt["opening_balance"], debt["incurred_in_period"], debt["paid_in_period"], debt["closing_balance"]) == (
        900,
        50,
        20,
        930,
    )
    allocation, _ = mapped(ALLOCATED_DISBURSEMENTS, "H4", {"19": "100", "20": "61", "21": "38.99", "22": "500"})
    assert (allocation["total_amount"], allocation["federal_share"], allocation["nonfederal_share"]) == (
        100,
        61,
        Decimal("38.99"),
    )
    assert allocation["event_amount_year_to_date"] == 500  # Never force shares to sum by changing source money.
    guarantor, _ = mapped(GUARANTORS, "SC2/12", {"3": "LOAN1", "19": "1000"})
    assert guarantor["back_reference_transaction_id"] == "LOAN1" and guarantor["loan_link_status"] == "unresolved"


def test_inaugural_refund_is_explicit_without_changing_its_sign():
    donation, _ = mapped(INAUGURAL_DONATIONS, "F132", {"18": "100", "19": "1000"})
    refund, _ = mapped(INAUGURAL_REFUNDS, "F133", {"18": "100"})
    assert donation["activity_kind"] == "reported-donation" and refund["activity_kind"] == "reported-refund"
    assert donation["amount"] == refund["amount"] == 100
    assert "reported_aggregate_amount" not in refund


def fixture_contexts():
    """Minimal retained native cell facts; malformed variants exercise refusal."""
    layout = load_layout(RECEIPTS)
    facts = [
        dict(kind="worksheet", sheet_ordinal=0, title="HDR"),
        dict(kind="cell", sheet_ordinal=0, coordinate="E7", value="8.5"),
        dict(kind="worksheet", sheet_ordinal=1, title="Sch A"),
    ]
    for f in layout["fields"]:
        facts.append(dict(kind="cell", sheet_ordinal=1, coordinate=f["cell"], value=f["label"]))
        for i, v in enumerate(f["specification"]):
            if isinstance(v, str):
                facts.append(dict(kind="cell", sheet_ordinal=1, coordinate=chr(67 + i) + f["cell"][1:], value=v))
    return {"definitions": dict(source_capture=dict(original_sha256=WORKBOOK), parsing=dict(facts=facts))}


def test_definition_facts_and_exact_serving_context_pointer():
    contexts = fixture_contexts()
    raw = dict(receiverDisposition=dict(callerContext=dict(facts=contexts["definitions"])))
    collections = [dict(collection_id="definitions", collection_outcome_json=json.dumps(raw))]
    assert definition_contexts_from_collections(collections) == contexts
    definition, evidence = bind_definition(RECEIPTS, contexts, GEN)
    assert definition["record_id"] == RECEIPTS.layout_pin
    value = raw
    for token in evidence[0]["context_pointer"].split("/")[1:]:
        value = value[token]
    assert value == contexts["definitions"]["parsing"]["facts"]
    broken = deepcopy(contexts)
    broken["definitions"]["parsing"]["facts"][1]["value"] = "8.4"
    with pytest.raises(ValueError, match="differs"):
        bind_definition(RECEIPTS, broken, GEN)
    with pytest.raises(ValueError, match="ambiguous"):
        bind_definition(RECEIPTS, {**contexts, "duplicate": contexts["definitions"]}, GEN)


def test_one_source_definition_is_shared_by_distinct_consumer_mappings():
    """A dictionary identity must not contain whichever consumer used it first."""
    specialised = replace(
        RECEIPTS,
        record_pattern="SA3L",
        fields=replace(RECEIPTS.fields, key="bundling/1", table="fec_bundled_contributions"),
    )
    regular = bind_definition(RECEIPTS, fixture_contexts(), GEN)
    bundling = bind_definition(specialised, fixture_contexts(), GEN)
    assert regular == bundling


def test_derived_evidence_equals_record_header_and_normalized_definition_oracle():
    row, direct = mapped(RECEIPTS, "SA11AI", {"20": "100"})
    _, definitions = bind_definition(RECEIPTS, fixture_contexts(), GEN)
    expected = direct + [
        {**e, "target_table": "fec_receipts", "target_record_id": row["record_id"]} for e in definitions
    ]
    with duckdb.connect() as db:
        db.register("fec_receipts", pa.Table.from_pylist([row], schema=RECEIPTS.schema))
        db.register("fec_filing_definition_evidence", pa.Table.from_pylist(definitions, schema=EVIDENCE_SCHEMA))
        actual = db.sql(filing_record_evidence("fec_receipts", GEN)).to_arrow_table().to_pylist()

    def canonical(rows):
        return sorted(json.dumps(x, sort_keys=True) for x in rows)

    assert canonical(actual) == canonical(expected)


def test_archived_workbook_uses_member_digest_instead_of_container_digest():
    contexts = fixture_contexts()
    capture = contexts["definitions"]["source_capture"]
    capture.update(original_sha256=PIN, member=dict(name="definitions.xlsx", ordinal=18), member_sha256=WORKBOOK)
    raw = dict(receiverDisposition=dict(callerContext=dict(facts=contexts["definitions"])))
    collections = [dict(collection_id="definitions", collection_outcome_json=json.dumps(raw))]
    assert definition_contexts_from_collections(collections) == contexts
    definition, evidence = bind_definition(RECEIPTS, contexts, GEN)
    assert definition["workbook_sha256"] == evidence[0]["witness_sha256"] == WORKBOOK
    capture["member_sha256"] = PIN
    with pytest.raises(ValueError, match="workbook digest"):
        bind_definition(RECEIPTS, contexts, GEN)


def test_mixed_table_emits_headers_only_for_rows_with_header_evidence():
    filing, direct = mapped(RECEIPTS, "SA11AI", {"20": "100"})
    other = {
        **filing,
        "record_id": "sha256:" + "c" * 64,
        "filing_header_record_id": None,
        "filing_header_locator_json": None,
        "definition_set_id": None,
    }
    _, definitions = bind_definition(RECEIPTS, fixture_contexts(), GEN)
    with duckdb.connect() as db:
        db.register("fec_receipts", pa.Table.from_pylist([filing, other], schema=RECEIPTS.schema))
        db.register("fec_filing_definition_evidence", pa.Table.from_pylist(definitions, schema=EVIDENCE_SCHEMA))
        rows = db.sql(filing_record_evidence("fec_receipts", GEN)).to_arrow_table().to_pylist()
        headers = [row for row in rows if row["role"] == "filing_header"]
        assert headers == [edge for edge in direct if edge["role"] == "filing_header"]
        other_edges = [row for row in rows if row["target_record_id"] == other["record_id"]]
        assert len(other_edges) == 1 and other_edges[0]["role"] == "primary"
        assert all(row["source_record_id"] for row in rows if row["endpoint_kind"] == "source_record")

        # Partial header evidence stays visible for release rejection; do not
        # silently turn a malformed filing witness into an absent header.
        other["filing_header_record_id"] = filing["filing_header_record_id"]
        db.register("fec_receipts", pa.Table.from_pylist([other], schema=RECEIPTS.schema))
        rows = db.sql(filing_record_evidence("fec_receipts", GEN)).to_arrow_table().to_pylist()
        (header,) = [row for row in rows if row["role"] == "filing_header"]
        assert header["witness_locator_json"] is None
