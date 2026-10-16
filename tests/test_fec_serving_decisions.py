"""Cross-consumer fixtures stay tied to the existing SQL filing rules."""

from copy import deepcopy
import json

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.fec_serving_decisions import decision_program, evaluate
from spicy_regs.relationship_views.fec_filing_associations import (
    financial_header_association_sql,
    financial_number_association_sql,
)
from tests.test_fec_filing_associations import financial, filing, headers, GEN, EVIDENCE, NAMESPACE

NEW = "sha256:" + "b" * 64


def population():
    filings = [
        dict(filing(rid=rid, number=number), mapping_version="fec-identity-observations/1", **changes)
        for rid, number, changes in [
            ("one", "123", {}),
            ("two", "123", {}),
            ("paper", "-123", {}),
            ("duplicate", "124", {}),
            ("duplicate", "124", {}),
            ("filer-a", "125", {}),
            ("filer-b", "125", {"native_filer_id": "C00000002"}),
            ("wrong-authority", "126", {"source_authority": "unofficial"}),
        ]
    ]
    hs = [
        dict(
            h,
            policy_version="fec-retained-filing-association/1",
            filing_observation_count=2,
            filing_observation_ids=["one", "two"],
        )
        for h in headers()
    ]
    return filings, hs


def fixture_cases():
    filings, hs = population()
    cases = [
        dict(name="number-" + str(i), row=financial(**change), kind="number")
        for i, change in enumerate(
            [
                {},
                {"report_number": "-123"},
                {"report_number": None},
                {"report_number": ""},
                {"report_number": "0"},
                {"report_number": "00123"},
                {"report_number": " 123"},
                {"report_number": "123.0"},
                {"report_number": "900"},
                {"report_number": "124"},
                {"report_number": "125"},
                {"report_number": "126"},
                {"source_authority": "unofficial"},
                {"mapping_version": "unknown"},
                {"source_namespace": "unknown"},
                {"reporting_committee_id": "C00000002"},
            ]
        )
    ]
    for name, changes in [
        ("match", {}),
        ("locator", {"filing_header_locator_json": "{}"}),
        ("missing", {"filing_header_record_id": "absent"}),
    ]:
        row = financial(
            filing_header_record_id=hs[0]["header_record_id"], filing_header_locator_json=hs[0]["header_locator_json"]
        )
        row.update(changes)
        cases.append(dict(name="header-" + name, row=row, kind="header"))
    cases.append(dict(name="header-record-itself", row=hs[0], kind="header-self"))
    return filings, hs, cases


def sql_expectations(filings, hs, cases):
    from spicy_regs.transforms.fec_filing_associations import ASSOCIATION_SCHEMA

    results = []
    with duckdb.connect(config={"threads": 1, "max_temp_directory_size": "0B"}) as con:
        con.register("fec_filings", pa.Table.from_pylist(filings))
        con.register("fec_filing_header_associations", pa.Table.from_pylist(hs, schema=ASSOCIATION_SCHEMA))
        for case in cases:
            row = case["row"]
            if case["kind"] == "header-self":
                row = {**row, "filing_header_record_id": row["header_record_id"],
                       "filing_header_locator_json": row["header_locator_json"]}
            con.register("fec_receipts", pa.Table.from_pylist([row]))
            sql = (
                financial_header_association_sql("fec_receipts", GEN, legacy=True)
                if case["kind"].startswith("header")
                else financial_number_association_sql(
                    "fec_receipts", GEN, {NAMESPACE: EVIDENCE}, columns=tuple(row), legacy=True
                )
            )
            actual = con.sql(sql).to_arrow_table().to_pylist()[0]
            results.append(
                dict(
                    name=case["name"],
                    row=case["row"],
                    expected={
                        "filing_key": actual["filing_key"],
                        "filing_association_status": actual["association_status"],
                    },
                )
            )
    return results


def exported_fixture():
    filings, hs, cases = fixture_cases()
    program = decision_program(filings, hs, {NAMESPACE: EVIDENCE}, historical_pin=GEN, observations_pin=NEW)
    return dict(format="fec-serving-semantic-fixtures/1", program=program, cases=sql_expectations(filings, hs, cases))


def test_portable_program_matches_maintained_sql():
    fixture = exported_fixture()
    for case in fixture["cases"]:
        result = evaluate(fixture["program"], case["row"])
        assert {k: result[k] for k in case["expected"]} == case["expected"], case["name"]
        assert result["filing_association_source_generation_pin"] == NEW
        assert result["filing_association_evidence_generation_pin"] == GEN
        assert result["filing_association_target_generation_scope"] == "self"


@pytest.mark.parametrize(
    "changes",
    [
        {"source_locator_json": "{}"},
        {"source_locator_json": "null"},
        {"source_locator_json": "{bad"},
        {"source_record_id": None},
        {"source_sha256": "not-a-pin"},
        {"selection_evidence_sha256": None},
    ],
)
def test_bad_source_context_never_becomes_a_link(changes):
    f, h = population()
    p = decision_program(f, h, {NAMESPACE: EVIDENCE}, historical_pin=GEN, observations_pin=NEW)
    result = evaluate(p, financial(**changes))
    assert result["filing_key"] is None
    assert result["filing_association_status"] == "unresolved_source_context"


@pytest.mark.parametrize(
    "change,status",
    [
        ("duplicate", "unresolved_header_association_ambiguous"),
        ("policy", "unresolved_header_policy_version"),
        ("semantics", "unresolved_header_policy_semantics"),
    ],
)
def test_header_failures_take_precedence_over_a_valid_number(change, status):
    f, h = population()
    if change == "duplicate":
        h.append(deepcopy(h[0]))
    elif change == "policy":
        h[0]["policy_version"] = "unknown"
    else:
        h[0]["association_basis"] = "inferred"
    row = financial(filing_header_record_id=h[0]["header_record_id"], filing_header_locator_json="different")
    p = decision_program(f, h, {NAMESPACE: EVIDENCE}, historical_pin=GEN, observations_pin=NEW)
    result = evaluate(p, row)
    assert result["filing_key"] is None and result["filing_association_status"] == status


def test_missing_namespace_proof_and_no_targets_stay_unresolved():
    f, h = population()
    p = decision_program(f, h, {}, historical_pin=GEN, observations_pin=NEW)
    assert evaluate(p, financial())["filing_association_status"] == "unresolved_namespace_evidence"
    p = decision_program([], [], {NAMESPACE: EVIDENCE}, historical_pin=GEN, observations_pin=NEW)
    assert evaluate(p, financial())["filing_association_status"] == "unresolved_target_not_retained"
    assert json.loads(json.dumps(p)) == p


def test_header_record_itself_uses_its_exact_existing_association():
    filings, hs = population()
    program = decision_program(filings, hs, {NAMESPACE: EVIDENCE}, historical_pin=GEN, observations_pin=NEW)
    result = evaluate(program, hs[0])
    assert result["filing_association_status"] == "resolved_native_filing_key"
    assert result["filing_key"] == hs[0]["filing_key"]
    assert result["filing_association_target_count"] == 2
