"""Source-local classifications cannot multiply money or hide unresolved coverage."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pytest

from spicy_regs.transforms.sector_comparisons import compare, issue_views

FIXTURE = json.loads((Path(__file__).parent / "fixtures/fec-research-profiles/sector-observations.json").read_text())


def run(data):
    return compare(data["fec"], data["lda"], mapping=data["mapping"], scope=data["scope"])


def totals(rows):
    result = {}
    for r in rows:
        key = (r["source_family"], r["measure"])
        result[key] = result.get(key, Decimal(0)) + Decimal(r["known_reported_amount"] or "0")
    return result


def test_two_real_source_comparisons_and_unclassified_denominators():
    rows = run(FIXTURE)["sector_financial_comparisons"]
    for code in ["62", "81"]:
        assert any(r["sector_code"] == code and r["source_family"] == "fec" and r["qualified_measure_profiles"] == 1 for r in rows)
        assert any(r["sector_code"] == code and r["source_family"] == "lda" and r["measure"] == "income" and r["known_reported_amount"] is not None for r in rows)
    assert all(r["population_profiles"] == 26 for r in rows if r["source_family"] == "lda")
    assert any(r["unclassified_population_profiles"] > 0 for r in rows)
    assert any(r["unresolved_measure_profiles"] > 0 for r in rows)
    assert any(r["known_reported_amount"] is None for r in rows)
    evidence = run(FIXTURE)["sector_classification_evidence"]
    assert any(r["classification_status"] == "unresolved_identity" and r["method"] == "not_used_for_money" for r in evidence)


def test_repeated_industry_labels_do_not_repeat_additive_money():
    original = run(FIXTURE)["sector_financial_comparisons"]
    data = deepcopy(FIXTURE)
    data["mapping"]["assignments"] *= 2
    repeated = run(data)["sector_financial_comparisons"]
    assert [(r["sector_code"], r["source_family"], r["measure"], r["known_reported_amount"]) for r in repeated] == [
        (r["sector_code"], r["source_family"], r["measure"], r["known_reported_amount"]) for r in original]


def test_mapping_change_moves_classification_without_changing_source_money():
    original = run(FIXTURE)["sector_financial_comparisons"]
    data = deepcopy(FIXTURE)
    data["mapping"]["version"] += "/changed"
    data["mapping"]["assignments"][2]["sector_codes"] = ["62", "81"]
    changed = run(data)["sector_financial_comparisons"]
    assert totals(original) == totals(changed)
    assert {r["record_id"] for r in original}.isdisjoint({r["record_id"] for r in changed})
    assert any(r["uncertain_population_profiles"] > 0 for r in changed)


@pytest.mark.parametrize("change", ["period", "population", "duplicate", "name_only", "issue_code", "evidence"])
def test_unsupported_comparisons_and_classifications_refuse(change):
    data = deepcopy(FIXTURE)
    if change == "period":
        data["lda"][0]["period_end"] = "2026-03-31"
    elif change == "population":
        data["fec"][0]["entity_kind"] = "candidate"
    elif change == "duplicate":
        data["lda"].append(data["lda"][0])
    elif change == "evidence":
        data["mapping"]["assignments"][0]["evidence"] = []
    else:
        data["mapping"]["assignments"][0]["method"] = change
    with pytest.raises(ValueError):
        run(data)


def test_multi_issue_views_preserve_whole_filing_amount_and_ignore_activity_multiplicity():
    original = issue_views(FIXTURE["lda"], FIXTURE["activities"], source_generation="sha256:" + "a" * 64)
    repeated = issue_views(FIXTURE["lda"], FIXTURE["activities"] * 3, source_generation="sha256:" + "a" * 64)
    assert original == repeated
    filings = {r["filing_uuid"] for r in original}
    assert any(sum(r["filing_uuid"] == uid for r in original) > 1 for uid in filings)
    assert all(r["amount_basis"] == "whole_selected_filing_not_allocated_by_issue" for r in original)


def test_same_client_number_under_another_registrant_does_not_inherit_industry():
    data = deepcopy(FIXTURE)
    row = next(r for r in data["lda"] if r["client_id"] == "64160")
    row["registrant_id"] = "99999"
    before = run(FIXTURE)["sector_financial_comparisons"]
    after = run(data)["sector_financial_comparisons"]
    assert totals(before) == totals(after)
    prior = next(r for r in before if r["sector_code"] == "62" and r["source_family"] == "lda" and r["measure"] == "income")
    current = next(r for r in after if r["sector_code"] == "62" and r["source_family"] == "lda" and r["measure"] == "income")
    assert current["group_profiles"] == prior["group_profiles"] - 1
