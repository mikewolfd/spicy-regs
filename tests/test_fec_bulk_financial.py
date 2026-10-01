"""Financial families keep their source grain, amounts, dates and evidence."""

from dataclasses import replace
from datetime import date
from decimal import Decimal
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import duckdb

from spicy_regs.relationship_views.fec_typed import spending_targets, typed_record_evidence
from spicy_regs.transforms.fec_bulk_financial import (
    CANDIDATE_TRANSACTIONS,
    COMMUNICATION_COSTS,
    ELECTIONEERING,
    INDEPENDENT_EXPENDITURES,
    INTERCOMMITTEE,
    OPERATING_EXPENSES,
    OPPEXP_FIELDS,
    financial_date,
    map_bulk_financial,
    operating_expense,
)
from spicy_regs.transforms.fec_query import CollectionSelection, observation_id

PIN = "sha256:" + "a" * 64
SELECTION = CollectionSelection("financial", PIN, "sha256:" + "b" * 64, "official-fec", 2026, "snapshot", PIN)


def source(native):
    return dict(
        collection_id="financial",
        source_record_id="original/0",
        source_sha256=PIN,
        source_locator_json=json.dumps(dict(collection_id="financial", source_record_id="original/0", ordinal=0)),
        metadata_json=json.dumps(native),
    )


def empty_fields(mapping):
    return dict.fromkeys(
        [v for _, v in mapping.text] + [v for _, v in mapping.amounts] + [v for _, v, _ in mapping.dates], ""
    )


def test_date_formats_do_not_depend_on_locale_or_implicit_year_pivots():
    assert financial_date("20161018", "YYYYMMDD") == (date(2016, 10, 18), "exact")
    assert financial_date("03/28/2025", "MM/DD/YYYY") == (date(2025, 3, 28), "exact")
    assert financial_date("30-OCT-24", "DD-MON-YY") == (None, "unresolved_century")
    assert financial_date("30-OCT-24", "DD-MON-YY", year_bounds=(1900, 2099)) == (None, "unresolved_century")
    assert financial_date("30-OCT-24", "DD-MON-YY", year_bounds=(2000, 2099)) == (
        date(2024, 10, 30),
        "exact_with_year_bounds",
    )
    assert financial_date("29-FEB-24", "DD-MON-YY", year_bounds=(2000, 2023)) == (None, "outside_selected_year_bounds")
    assert financial_date("31-FEB-24", "DD-MON-YY", year_bounds=(2000, 2099)) == (None, "invalid_date")
    assert financial_date("30-XYZ-24", "DD-MON-YY", year_bounds=(2000, 2099)) == (None, "invalid_date")
    assert financial_date("3/28/2025", "MM/DD/YYYY") == (None, "unsupported_spelling")


def test_committee_transactions_keep_unknown_direction_and_native_ids():
    fields = empty_fields(CANDIDATE_TRANSACTIONS)
    fields.update(
        CMTE_ID="C00777706",
        CAND_ID="H2TX31044",
        OTHER_ID="C00371203",
        ENTITY_TP="CCM",
        TRANSACTION_AMT="2500",
        TRANSACTION_DT="12122022",
        TRAN_ID="SB23.4778",
        AMNDT_IND="T",
        SUB_ID="4011820231709508201",
        FILE_NUM="001675104",
        ZIP_CODE="00001",
        TRANSACTION_TP="24K",
    )
    row, _ = map_bulk_financial(source(fields), SELECTION, CANDIDATE_TRANSACTIONS)
    assert row["amount"] == Decimal("2500") and row["transaction_date"] == date(2022, 12, 12)
    assert row["reported_direction"] == "unresolved"
    assert row["other_native_id"] == "C00371203" and row["candidate_id"] == "H2TX31044"
    assert row["report_number"] == "001675104" and row["counterparty_zip"] == "00001"
    assert row["source_record_identifier"] == "4011820231709508201"
    assert row["correction_operation"] == "none" and row["current_record_status"] == "unqualified"


def test_independent_spending_keeps_aggregate_separate_and_missing_date():
    fields = empty_fields(INDEPENDENT_EXPENDITURES)
    fields.update(
        exp_amo="-150.05",
        agg_amo="9000",
        exp_date="",
        dissem_dt="30-OCT-24",
        receipt_dat="31-OCT-24",
        spe_id="C90012121",
        spe_nam="Individual or group",
        pay="Payee",
        cand_id="H4CO08034",
        sup_opp="S",
    )
    row, _ = map_bulk_financial(source(fields), SELECTION, INDEPENDENT_EXPENDITURES)
    assert row["amount"] == Decimal("-150.05") and row["reported_aggregate_amount"] == Decimal("9000")
    assert row["expenditure_date"] is None and row["expenditure_date_status"] == "source_empty"
    assert row["dissemination_date"] is None and row["dissemination_date_status"] == "unresolved_century"
    assert row["dissemination_date_raw"] == "30-OCT-24" and row["mapping_status"] == "partial"
    assert row["spender_native_id"] == "C90012121" and "reporting_committee_id" not in row


def test_electioneering_never_invents_a_shared_payment_id_or_divides_an_amount():
    fields = empty_fields(ELECTIONEERING)
    fields.update(
        REPORTED_DISBURSEMENT_AMOUNT="15000",
        CALCULATED_CANDIDATE_SHARE="7400.01",
        NUMBER_OF_CANDIDATES="2",
        CANDIDATE_ID="S4IA00087",
        SB_IMAGE_NUM="000123",
    )
    row, _ = map_bulk_financial(source(fields), SELECTION, ELECTIONEERING)
    assert row["amount"] == Decimal("15000") and row["allocated_candidate_amount"] == Decimal("7400.01")
    assert row["reported_candidate_count"] == "2"
    assert row["observation_grain"] == "candidate-associated-disbursement"
    assert row["event_equivalence_status"] == "unresolved"
    assert row["amount_aggregation_status"] == "requires-event-deduplication"


def oppexp_inputs(tail):
    native = [
        "C00696948",
        "N",
        "2025",
        "M4",
        "202504189756130114",
        "23",
        "F3P",
        "SB",
        "ADP, LLC",
        "EL PASO",
        "TX",
        "799128023",
        "03/28/2025",
        "3716.38",
        "P2020",
        "PROCESSING FEES",
        "",
        "",
        "",
        "",
        "ORG",
        "4042220251186031739",
        "1889009",
        "500201474",
        "",
    ]
    header = source({"kind": "row", "fields": list(OPPEXP_FIELDS)})
    header.update(
        collection_id="header",
        source_record_id="header/0",
        source_sha256="sha256:" + "c" * 64,
        source_locator_json=json.dumps(dict(collection_id="header", source_record_id="header/0", ordinal=0)),
    )
    return source({"kind": "row", "fields": native + tail}), header


def test_oppexp_labels_known_positions_and_retains_extra_empty_position():
    raw, header = oppexp_inputs([""])
    row, evidence = operating_expense(raw, SELECTION, header)
    assert row["record_id"] == observation_id("fec_disbursements", raw, "official-fec")
    assert row["payee_name"] == "ADP, LLC" and row["amount"] == Decimal("3716.38")
    assert row["transaction_date"] == date(2025, 3, 28)
    assert row["back_reference_transaction_id"] == "" and row["source_record_identifier"] == "4042220251186031739"
    assert json.loads(row["mapping_reason_json"])["unlabelled_positions"] == {"25": ""}
    assert row["mapping_status"] == "partial"
    assert evidence[1]["collection_id"] == "header" and evidence[1]["witness_sha256"] == header["source_sha256"]
    raw, header = oppexp_inputs([])
    assert operating_expense(raw, SELECTION, header)[0]["mapping_status"] == "mapped"


@pytest.mark.parametrize("tail", [["unexpected"], ["", ""]])
def test_oppexp_refuses_new_nonempty_or_multiple_unlabelled_positions(tail):
    raw, header = oppexp_inputs(tail)
    with pytest.raises(ValueError):
        operating_expense(raw, SELECTION, header)


def test_mismatched_header_cannot_label_operating_expenses():
    raw, header = oppexp_inputs([""])
    names = list(OPPEXP_FIELDS)
    names[12], names[13] = names[13], names[12]
    header["metadata_json"] = json.dumps({"kind": "row", "fields": names})
    with pytest.raises(ValueError, match="ordered definition"):
        operating_expense(raw, SELECTION, header)


@pytest.mark.parametrize(
    "mapping",
    [
        INTERCOMMITTEE,
        CANDIDATE_TRANSACTIONS,
        COMMUNICATION_COSTS,
        INDEPENDENT_EXPENDITURES,
        ELECTIONEERING,
        OPERATING_EXPENSES,
    ],
)
def test_all_bulk_schemas_preserve_exact_values_and_refusals_in_parquet(mapping, tmp_path):
    fields = empty_fields(mapping)
    fields[mapping.amounts[0][1]] = "99999999999999999999999999999.999999999"
    fields["unrecognized_future_field"] = {"preserved": ["001", None]}
    row, evidence = map_bulk_financial(source(fields), replace(SELECTION, representation_role="deletion"), mapping)
    assert row["correction_operation"] == "deletion" and row["amount"] > 0
    assert set(row) == set(mapping.schema.names)
    path = tmp_path / "typed.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema=mapping.schema), path)
    assert pq.read_table(path).to_pylist() == [row]
    assert json.loads(row["mapping_reason_json"])["uninterpreted_native_fields"] == ["unrecognized_future_field"]
    assert evidence[0]["source_record_id"] == "original/0"
    fields[mapping.amounts[0][1]] = "1e5"
    refused, _ = map_bulk_financial(source(fields), SELECTION, mapping)
    assert refused["amount"] is None and refused["amount_status"] == "unsupported_spelling"
    assert refused["mapping_status"] == "partial"


def test_target_view_keeps_only_reported_associations_and_supplied_allocations():
    fields = empty_fields(ELECTIONEERING)
    fields.update(
        REPORTED_DISBURSEMENT_AMOUNT="15000",
        CALCULATED_CANDIDATE_SHARE="7400.01",
        NUMBER_OF_CANDIDATES="2",
        CANDIDATE_ID="S4IA00087",
    )
    first, _ = map_bulk_financial(source(fields), SELECTION, ELECTIONEERING)
    fields.update(CANDIDATE_ID="", CALCULATED_CANDIDATE_SHARE="", CANDIDATE_NAME="Named only")
    second, _ = map_bulk_financial(source(fields), SELECTION, ELECTIONEERING)
    second["record_id"] = "sha256:" + "d" * 64
    fields["CANDIDATE_NAME"] = ""
    no_target, _ = map_bulk_financial(source(fields), SELECTION, ELECTIONEERING)
    with duckdb.connect() as con:
        con.register(
            ELECTIONEERING.table, pa.Table.from_pylist([first, second, no_target], schema=ELECTIONEERING.schema)
        )
        targets = con.execute(spending_targets(ELECTIONEERING.table)).to_arrow_table()
        rows = targets.to_pylist()
        assert len(rows) == 2
        assert rows[0]["allocated_amount"] == Decimal("7400.01")
        assert rows[1]["allocated_amount"] is None and rows[1]["target_status"] == "name_only"
        assert rows[0]["record_id"] != rows[1]["record_id"]
        assert rows[0]["spending_record_id"] == first["record_id"]
        assert all(r["spending_generation_scope"] == "self" for r in rows)
        con.register("fec_spending_targets", targets)
        endpoints = (
            con.execute(typed_record_evidence("fec_spending_targets", SELECTION.source_generation_pin))
            .to_arrow_table()
            .to_pylist()
        )
        assert len(endpoints) == 2 and endpoints[0]["target_record_id"] == rows[0]["record_id"]


@pytest.mark.parametrize("mapping", [INDEPENDENT_EXPENDITURES, COMMUNICATION_COSTS])
def test_target_view_does_not_copy_full_spending_as_an_allocation(mapping):
    fields = empty_fields(mapping)
    candidate_field = "cand_id" if mapping == INDEPENDENT_EXPENDITURES else "CAND_ID"
    fields[candidate_field] = "H4CO08034"
    fields[mapping.amounts[0][1]] = "100"
    row, _ = map_bulk_financial(source(fields), SELECTION, mapping)
    with duckdb.connect() as con:
        con.register(mapping.table, pa.Table.from_pylist([row], schema=mapping.schema))
        targets = con.execute(spending_targets(mapping.table)).to_arrow_table().to_pylist()
        assert len(targets) == 1 and targets[0]["allocated_amount"] is None
        assert targets[0]["allocated_amount_status"] == "not_reported"
