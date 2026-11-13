"""Exact retained inputs and planted failures exercise the named FEC calculations."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from spicy_docs.sources.fec.metadata import parse_api
from spicy_regs.pipelines.fec_research_profiles import filing_rows
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.fec_committee_profiles import committee_profiles
from spicy_regs.transforms.fec_contribution_profiles import contribution_profiles
from spicy_regs.transforms.fec_outside_profiles import outside_profiles
from spicy_regs.transforms.fec_query import CollectionSelection

FIXTURES = Path(__file__).parent / "fixtures/fec-research-profiles"
SELECTION = json.loads((FIXTURES / "selection.json").read_text())


def api(query):
    return [row for page in query["pages"] for row in
            parse_api((FIXTURES / page["sha256"].removeprefix("sha256:")).read_bytes())["results"]]


@pytest.fixture(scope="module")
def cohort(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("fec-contribution-cohort")
    capture = SELECTION["contribution"]["original"]
    evidence = CaptureEvidence(tmp_path / "evidence", "fec-test", archive_root=tmp_path / "raw")
    evidence.retain_file(FIXTURES / capture["response_sha256"].removeprefix("sha256:"),
                         stage="source-fixture", raw_source=True, source_url=capture["request_url"])
    rows = filing_rows(evidence, capture)
    evidence.finish()
    queries = SELECTION["contribution"]["queries"]
    context = {q["name"]: api(q) for q in queries}
    pin = capture["response_sha256"]
    selection = CollectionSelection("filing-1943202", pin, pin, "official-fec", 2026, "snapshot", pin)
    return rows, selection, dict(report=context["reports"][0], filing_id="1943202", source_url=capture["request_url"],
        captured_at=None, candidate_id="S6AL00484", committee_history=context["committee-history"][0],
        candidates=context["candidate-search"], evidence=queries)


def test_real_report_reconciles_pac_individual_refund_memo_and_signed_adjustments(cohort):
    rows, selection, kwargs = cohort
    output = contribution_profiles(rows, selection, **kwargs)
    profiles = {r["contribution_family"]: r for r in output["fec_contribution_profiles"]}
    assert Decimal(profiles["pac"]["signed_itemized_receipts"]) == Decimal("12000")
    assert Decimal(profiles["individual"]["signed_itemized_receipts"]) == Decimal("80188.58")
    assert Decimal(profiles["individual"]["itemized_refunds"]) == Decimal("500")
    assert profiles["individual"]["memo_records"] == 69
    decisions = output["fec_contribution_decisions"]
    assert len(decisions) == 404
    assert any(r["transaction_date_scope"] == "outside_report_coverage" and r["disposition"] == "included" for r in decisions)
    assert {r["memo_field_status"] for r in decisions} == {"missing", "empty", "reported"}
    assert all(r["donor_id"] is None for r in decisions if r["contribution_family"] == "individual")
    assert any(r["disposition_reason"] == "negative_receipt_adjustment" for r in decisions)


@pytest.mark.parametrize("change", ["most_recent", "amended", "money", "cover_period", "future_candidate", "duplicate_native_id"])
def test_report_membership_money_and_period_identity_failures_refuse(cohort, change):
    rows, selection, kwargs = deepcopy(cohort)
    if change == "most_recent":
        kwargs["report"]["most_recent_file_number"] = Decimal("2000000")
    elif change == "amended":
        kwargs["report"]["is_amended"] = True
    elif change == "money":
        kwargs["report"]["individual_itemized_contributions_period"] = "80188.59"
    elif change == "cover_period":
        kwargs["report"]["coverage_end_date"] = "2025-12-30T00:00:00"
    elif change == "future_candidate":
        kwargs["candidate_id"] = "H6AL07190"  # Same committee's later House candidacy does not qualify this period.
    elif change == "duplicate_native_id":
        target = next(r for r in rows if json.loads(r["metadata_json"]).get("record_type") == "SA11C")
        duplicate = deepcopy(target)
        duplicate["source_record_id"] += "duplicate"
        locator = json.loads(duplicate["source_locator_json"])
        locator["source_record_id"] = duplicate["source_record_id"]
        duplicate["source_locator_json"] = json.dumps(locator)
        rows.append(duplicate)
    with pytest.raises(ValueError):
        contribution_profiles(rows, selection, **kwargs)


def outside(**kwargs):
    queries = SELECTION["outside"]
    selected = {q["name"]: api(q) for q in queries}
    return outside_profiles(selected["by-spender"], selected["totals"], candidate_id="S6OH00163", cycle=2024,
                            evidence=queries, **kwargs)


def test_real_spender_totals_reconcile_and_count_source_filers():
    result = outside()
    profiles = {r["support_oppose"]: r for r in result["fec_outside_spending_profiles"]}
    assert Decimal(profiles["O"]["total"]) == Decimal("115071914.82")
    assert Decimal(profiles["S"]["total"]) == Decimal("24303523.34")
    assert len(result["fec_outside_spending_spenders"]) == 73
    for indicator in profiles:
        ids = {r["spender_id"] for r in result["fec_outside_spending_spenders"] if r["support_oppose"] == indicator}
        assert profiles[indicator]["reported_spender_count"] == len(ids)


def test_specific_election_is_not_inferred_from_cycle_total():
    with pytest.raises(ValueError, match="election-specific"):
        outside(election="G2024")


def test_unknown_filer_and_form5_namespaces_remain_distinct_from_committees():
    queries = SELECTION["outside"]
    by = {q["name"]: api(q) for q in queries}
    by["by-spender"][0]["committee_id"] = None
    by["by-spender"][1]["committee_id"] = "C99999888"
    output = outside_profiles(by["by-spender"], by["totals"], candidate_id="S6OH00163", cycle=2024, evidence=queries)
    assert sum(r["unknown_spender_rows"] for r in output["fec_outside_spending_profiles"]) == 1
    assert any(r["spender_namespace"] == "unresolved" and r["spender_id"] is None for r in output["fec_outside_spending_spenders"])
    assert any(r["spender_namespace"] == "fec_form5_filer" and r["spender_id"] == "C99999888" for r in output["fec_outside_spending_spenders"])


@pytest.mark.parametrize("change", ["duplicate", "money", "cycle", "indicator", "float"])
def test_publisher_aggregate_alternative_representations_and_bad_values_refuse(change):
    queries = SELECTION["outside"]
    by = {q["name"]: api(q) for q in queries}
    rows, totals = by["by-spender"], by["totals"]
    if change == "duplicate":
        rows.append(deepcopy(rows[0]))
    elif change == "money":
        totals[0]["total"] += Decimal("0.01")
    elif change == "cycle":
        rows[0]["cycle"] = 2022
    elif change == "indicator":
        rows[0]["support_oppose_indicator"] = None
    else:
        rows[0]["total"] = float(rows[0]["total"])
    with pytest.raises(ValueError):
        outside_profiles(rows, totals, candidate_id="S6OH00163", cycle=2024, evidence=queries)


def test_relationships_preserve_multiple_sponsors_and_unknown_affiliation_states():
    capture = dict(response_sha256="sha256:" + "a" * 64, request_url="https://api.open.fec.gov/v1/committees/",
                   observed_at="2026-11-13T12:00:00Z")
    row = dict(committee_id="C00000001", name="Source committee", sponsor_candidate_ids=["S0XX00001", "S0XX00002"],
               affiliated_committee_name="Unresolved organization")
    profiles = committee_profiles([row, {"committee_id": "C00000002"},
                                   {"committee_id": "C00000003", "affiliated_committee_name": None}], capture)
    relationships = json.loads(profiles[0]["relationships_json"])
    assert [r["object_id"] for r in relationships if r["object_id"]] == ["S0XX00001", "S0XX00002"]
    assert relationships[0]["object_name"] == "Unresolved organization" and relationships[0]["object_id"] is None
    assert json.loads(profiles[1]["relationships_json"])[0]["value_status"] == "missing_field"
    assert json.loads(profiles[2]["relationships_json"])[0]["value_status"] == "null"
    conflicting = committee_profiles([{**row, "affiliated_committee_name": "Different disclosure"}],
                                    {**capture, "response_sha256": "sha256:" + "b" * 64})
    assert conflicting[0]["profile_id"] != profiles[0]["profile_id"]
