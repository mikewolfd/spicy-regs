"""Summary periods, native measures and aggregate bins retain their meanings."""

from dataclasses import replace
from datetime import date
from decimal import Decimal
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.relationship_views.fec_typed import reported_summary_measures, typed_record_evidence
from spicy_regs.transforms import fec_summaries as s
from spicy_regs.transforms.fec_query import CollectionSelection

PIN = "sha256:" + "a" * 64
SELECTED = CollectionSelection("summary", PIN, PIN, "official-fec", 2024, "snapshot", PIN)


def source(native):
    return dict(
        collection_id="summary",
        source_record_id="row/0",
        source_sha256=PIN,
        source_locator_json=json.dumps(dict(collection_id="summary", source_record_id="row/0", ordinal=0)),
        metadata_json=json.dumps(native),
    )


def fields(mapping):
    names = set(mapping.money_fields) | set(mapping.attributes) | {v for _, v in mapping.identity_fields}
    names |= {v for v in [mapping.period_start_field, mapping.period_end_field] if v}
    return dict.fromkeys(names, "")


def test_summary_keeps_null_blank_zero_and_stock_separate():
    native = fields(s.COMMITTEE_CSV)
    native.update(
        CMTE_ID="C00059907",
        CMTE_NM="Committee",
        CVG_START_DT="20230101",
        CVG_END_DT="20241231",
        TTL_RECEIPTS="155193.28",
        COH_BOP="53003.66",
        COH_COP="50195.94",
        INDV_REF="1",
        FED_CAND_CONTB_REF="-5",
        COH_BOY="",
        COH_COY=None,
        INDT_EXP="0",
    )
    row, _ = s.map_summary(source(native), SELECTED, s.COMMITTEE_CSV)
    measures = {m["native_field"]: m for m in row["measures"]}
    assert row["period_start"] == date(2023, 1, 1) and row["period_end"] == date(2024, 12, 31)
    assert measures["COH_BOP"]["value"] == Decimal("53003.66")
    assert measures["COH_COP"]["value"] == Decimal("50195.94")
    assert measures["FED_CAND_CONTB_REF"]["value"] == Decimal("-5")
    assert measures["INDT_EXP"]["value"] == 0 and measures["INDT_EXP"]["value_status"] == "exact"
    assert measures["COH_BOY"]["value"] is None and measures["COH_BOY"]["value_status"] == "source_empty"
    assert measures["COH_COY"]["value"] is None and measures["COH_COY"]["value_status"] == "source_null"


def test_bundling_totals_never_invent_bundlers_or_add_overlapping_periods():
    native = fields(s.BUNDLING_RECIPIENT)
    native.update(
        Committee_Id="C00903690",
        Quarterly_Contribution="100",
        Semi_Annual_Contribution="250",
        Coverage_Start_Date="01-JUL-26",
        Coverage_End_Date="22-JUL-26",
    )
    row, _ = s.map_summary(source(native), replace(SELECTED, source_cycle=None), s.BUNDLING_RECIPIENT)
    assert row["source_cycle"] is None
    assert row["period_basis"] == "quarterly-and-semiannual-measures-overlap"
    assert {m["native_field"]: m["value"] for m in row["measures"]} == {
        "Quarterly_Contribution": Decimal("100"),
        "Semi_Annual_Contribution": Decimal("250"),
    }
    assert row["period_end"] is None and row["period_end_status"] == "unresolved_century"
    assert "bundler_id" not in row


def test_aggregate_names_do_not_become_resolved_candidate_identities():
    raw = source(
        dict(
            CANDIDATE_ID="P00000001",
            CANDIDATE_NAME="All candidates,",
            CONTRIB_RECEIPT_AMOUNT="250",
            CONTRIB_STATE="AL",
            STATE_NAME="ALABAMA",
            ZIP_3="004",
        )
    )
    row, _ = s.map_contribution_aggregate(raw, SELECTED, kind="three-digit-zip")
    assert row["entity_reference_status"] == "source_identifier_unresolved"
    assert row["candidate_native_id"] == "P00000001" and row["candidate_name"] == "All candidates,"
    assert json.loads(row["dimensions_json"])["ZIP_3"] == "004"
    assert row["reported_count"] is None and row["reported_count_status"] == "not_reported"
    assert set(row) == set(s.AGGREGATE_SCHEMA.names)


@pytest.mark.parametrize(
    "mapping",
    [
        s.CANDIDATE_CSV,
        s.COMMITTEE_CSV,
        s.CANDIDATE_WEB,
        s.COMMITTEE_WEB,
        s.PRESIDENTIAL_OVERALL,
        s.LEADERSHIP,
        s.BUNDLING_RECIPIENT,
    ],
)
def test_summary_schema_and_derived_measure_evidence_roundtrip(mapping, tmp_path):
    native = fields(mapping)
    native[mapping.money_fields[0]] = "99999999999999999999999999999.999999999"
    row, _ = s.map_summary(source(native), SELECTED, mapping)
    assert len(mapping.money_fields) == len(set(mapping.money_fields))
    assert set(row) == set(s.SUMMARY_SCHEMA.names)
    path = tmp_path / "summary.parquet"
    table = pa.Table.from_pylist([row], schema=s.SUMMARY_SCHEMA)
    pq.write_table(table, path)
    assert pq.read_table(path).to_pylist() == [row]
    with duckdb.connect() as con:
        con.register("fec_reported_financial_summaries", table)
        measures = con.execute(reported_summary_measures()).to_arrow_table()
        values = measures.to_pylist()
        assert len(values) == len(mapping.money_fields)
        assert len({v["record_id"] for v in values}) == len(values)
        assert {v["native_measure_name"]: v["amount"] for v in values} == {
            m["native_field"]: m["value"] for m in row["measures"]
        }
        assert all(
            v["summary_record_id"] == row["record_id"] and v["summary_generation_scope"] == "self" for v in values
        )
        con.register("fec_reported_financial_summary_metrics", measures)
        evidence = (
            con.execute(typed_record_evidence("fec_reported_financial_summary_metrics", PIN))
            .to_arrow_table()
            .to_pylist()
        )
        assert len(evidence) == len(values) and all(v["source_record_id"] == "row/0" for v in evidence)


def test_invalid_measure_remains_queryable_without_rounding():
    native = fields(s.LEADERSHIP)
    native.update(Cash_on_Hand="0.0000000001", Total_Receipt="0", Total_Disbursement="")
    row, _ = s.map_summary(source(native), SELECTED, s.LEADERSHIP)
    m = row["measures"][0]
    assert m["value"] is None and m["raw_value"] == "0.0000000001" and m["value_status"] == "excess_precision"
    assert row["mapping_status"] == "partial"
