"""API result scope, exact values, balances and allocations stay distinct."""

from datetime import date
from decimal import Decimal
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import fec_api_financial as a
from spicy_regs.transforms.fec_query import CollectionSelection

PIN = "sha256:" + "a" * 64
SELECTED = CollectionSelection("api", PIN, PIN, "official-fec", None, "snapshot", PIN)


def native(mapping):
    f = mapping.fields
    return dict.fromkeys({v for _, v in f.text} | {v for _, v in f.amounts} | {v for _, v, _ in f.dates}, None)


def source(mapping, fields):
    return dict(
        collection_id="api",
        source_record_id="api/result/0",
        source_sha256=PIN,
        source_url="https://api.open.fec.gov" + mapping.route,
        source_locator_json=json.dumps(
            dict(collection_id="api", source_record_id="api/result/0", pointer="/results/0")
        ),
        metadata_json=json.dumps(
            dict(kind="api-record-observation", metadata=fields, query_completeness="not-asserted")
        ),
    )


def test_loan_state_does_not_treat_balances_as_new_receipts():
    n = native(a.LOANS)
    n.update(
        original_loan_amount="43.01",
        loan_balance="43.01",
        payment_to_date="0.0",
        file_number=2006298,
        incurred_date="2026-08-07",
        sub_id="4082720261585241047",
        action_code="A",
        transaction_id="SC/9.4238",
        cycle=2026,
    )
    row, _ = a.map_api_financial(source(a.LOANS, n), SELECTED, a.LOANS)
    assert row["original_loan_amount"] == row["outstanding_balance"] == Decimal("43.01")
    assert row["payments_to_date"] == 0 and row["incurred_date"] == date(2026, 8, 7)
    assert row["report_number"] == "2006298" and row["source_record_identifier"] == "4082720261585241047"
    assert row["correction_operation"] == "none" and row["current_record_status"] == "unqualified"
    assert row["query_completeness"] == "not-asserted" and "amount" not in row


def test_debt_activity_is_separate_from_opening_and_closing_balances():
    n = native(a.DEBTS)
    n.update(
        outstanding_balance_beginning_of_period="100",
        outstanding_balance_close_of_period="75",
        amount_incurred_period="25",
        payment_period="50",
        coverage_start_date="2026-07-01",
        coverage_end_date="2026-09-30",
    )
    row, _ = a.map_api_financial(source(a.DEBTS, n), SELECTED, a.DEBTS)
    assert (row["opening_balance"], row["incurred_in_period"], row["paid_in_period"], row["closing_balance"]) == (
        100,
        25,
        50,
        75,
    )


def test_allocation_preserves_supplied_parts_without_forcing_arithmetic_agreement():
    n = native(a.ALLOCATED_DISBURSEMENTS)
    n.update(
        disbursement_amount="12000", federal_share="8000", nonfederal_share="3900", event_amount_year_to_date="25000"
    )
    row, _ = a.map_api_financial(source(a.ALLOCATED_DISBURSEMENTS, n), SELECTED, a.ALLOCATED_DISBURSEMENTS)
    assert row["total_amount"] == 12000 and row["federal_share"] == 8000 and row["nonfederal_share"] == 3900
    assert row["event_amount_year_to_date"] == 25000 and "amount" not in row


def test_coordinated_date_keeps_native_datetime_and_distinct_aggregate():
    n = native(a.COORDINATED_EXPENDITURES)
    n.update(
        expenditure_amount=1566,
        aggregate_general_election_expenditure="3132.84",
        expenditure_date="2023-01-13T23:59:58",
    )
    row, _ = a.map_api_financial(source(a.COORDINATED_EXPENDITURES, n), SELECTED, a.COORDINATED_EXPENDITURES)
    assert row["amount"] == 1566 and row["aggregate_general_election_amount"] == Decimal("3132.84")
    assert (
        row["expenditure_date"] == date(2023, 1, 13) and row["expenditure_date_status"] == "exact_local_date_component"
    )
    assert row["expenditure_date_raw"] == "2023-01-13T23:59:58"


def test_api_null_missing_empty_and_boolean_amount_remain_distinct():
    n = native(a.INAUGURAL_AGGREGATES)
    n.update(committee_id="C00629584", contributor_name=None, cycle=2018, total_donation=None)
    row, _ = a.map_api_financial(source(a.INAUGURAL_AGGREGATES, n), SELECTED, a.INAUGURAL_AGGREGATES)
    assert row["amount_status"] == "source_null" and row["contributor_name"] is None
    n.pop("total_donation")
    row, _ = a.map_api_financial(source(a.INAUGURAL_AGGREGATES, n), SELECTED, a.INAUGURAL_AGGREGATES)
    assert row["amount_status"] == "source_missing" and row["mapping_status"] == "partial"
    n["total_donation"] = ""
    assert (
        a.map_api_financial(source(a.INAUGURAL_AGGREGATES, n), SELECTED, a.INAUGURAL_AGGREGATES)[0]["amount_status"]
        == "source_empty"
    )
    n["total_donation"] = True
    row, _ = a.map_api_financial(source(a.INAUGURAL_AGGREGATES, n), SELECTED, a.INAUGURAL_AGGREGATES)
    assert row["amount"] is None and row["amount_status"] == "unsupported_spelling" and row["amount_raw"] == "true"


def test_controls_wrong_routes_and_inexact_identifier_shapes_refuse():
    raw = source(a.LOANS, native(a.LOANS))
    raw["metadata_json"] = json.dumps(dict(kind="api-response-field", field="pagination", value={}))
    with pytest.raises(ValueError, match="response controls"):
        a.map_api_financial(raw, SELECTED, a.LOANS)
    raw = source(a.LOANS, native(a.LOANS))
    with pytest.raises(ValueError, match="source route"):
        a.map_api_financial(raw, SELECTED, a.DEBTS)
    n = native(a.LOANS)
    n["file_number"] = 2006298.5
    with pytest.raises(ValueError, match="API scalar"):
        a.map_api_financial(source(a.LOANS, n), SELECTED, a.LOANS)


@pytest.mark.parametrize("mapping", a.MAPPINGS)
def test_api_schema_parquet_roundtrip_and_exact_json_decimal(mapping, tmp_path):
    n = native(mapping)
    n[mapping.fields.amounts[0][1]] = "99999999999999999999999999999.999999999"
    row, _ = a.map_api_financial(source(mapping, n), SELECTED, mapping)
    assert set(row) == set(mapping.fields.schema.names)
    p = tmp_path / "api.parquet"
    pq.write_table(pa.Table.from_pylist([row], schema=mapping.fields.schema), p)
    assert pq.read_table(p).to_pylist() == [row]
    raw = source(mapping, n)
    raw["metadata_json"] = raw["metadata_json"].replace('"99999999999999999999999999999.999999999"', "123.4500")
    exact, _ = a.map_api_financial(raw, SELECTED, mapping)
    assert exact[mapping.fields.amounts[0][0]] == Decimal("123.45")
    assert exact[mapping.fields.amounts[0][0] + "_raw"] == "123.4500"
