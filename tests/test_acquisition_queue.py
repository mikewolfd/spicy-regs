from copy import deepcopy

import pytest

from spicy_regs.acquisition_queue import build_missing_target_queue

PIN = {"status": "versioned", "publication_id": "fixture-1"}


def occurrence(key="118-public-5", kind="public_law", number=1):
    return {"target_status": "missing", "match_count": 0, "source_status": "current_text",
            "target_kind": kind, "normalized_key": key, "target_snapshot": PIN,
            "occurrence_key": f"occ-{number}", "document_kind": "govinfo_package", "document_key": "report-1",
            "matched_text": "Public Law 118–5", "text_sha256": "held-digest", "resolution_rule": "selected-target-lookup/1"}


def queue(rows, **kwargs):
    return build_missing_target_queue({"occurrences": rows}, input_snapshots={"govinfo_package": PIN}, **kwargs)


def test_deduplicates_requests_preserving_spelling_and_pins():
    first, second = occurrence(), occurrence(number=2)
    second["matched_text"] = "P.L. 118-5"
    result = queue([first, first, second], retained_targets={("public_law", "118-public-5"): {"status": "absent"}})
    item, = result["items"]
    assert item["affected_occurrences"] == 2 and item["affected_records"] == 1
    assert item["status"] == "planned" and item["acquisition_outcome"] == "not_attempted"
    assert item["provider_route"]["arguments"] == {"congress": 118, "kind": "public", "number": 5}
    assert [r["matched_text"] for r in item["requesting_occurrences"]] == ["Public Law 118–5", "P.L. 118-5"]
    assert item["requesting_occurrences"][0]["input_snapshot"] == PIN


@pytest.mark.parametrize("change", [{"target_status": "ambiguous"}, {"source_status": "unread_source"},
                                    {"source_status": "stale_source"}, {"target_snapshot": {}},
                                    {"match_count": None}, {"occurrence_key": None}])
def test_never_queues_unqualified_observation(change):
    row = occurrence() | change
    assert not queue([row])["items"]


def test_retained_lookup_required_and_unsupported_usc_explicit():
    law, gao, usc = occurrence(), occurrence("GAO-24-123", "gao_product_id", 2), occurrence("5-801", "usc_section", 3)
    result = queue([law, gao, usc])
    items = {r["target_kind"]: r for r in result["items"]}
    assert items["public_law"]["status"] == "needs_retained_lookup"
    assert items["gao_product_id"]["provider_route"]["api"].endswith(".acquire_report_pdf")
    assert items["gao_product_id"]["provider_route"]["arguments"] == {"product_id": "gao-24-123"}
    assert items["usc_section"]["status"] == "unsupported"
    assert "edition_unselected" in items["usc_section"]["provider_route"]["reason"]
    retained = queue([law], retained_targets={("public_law", law["normalized_key"]): {"status": "retained", "sha256": "digest", "receipt": "receipt.json"}})
    assert retained["items"][0]["status"] == "retained"


def test_distinct_target_pins_and_bounded_ranking():
    old, new = occurrence(), deepcopy(occurrence(number=2))
    new["target_snapshot"]["publication_id"] = "fixture-2"
    result = queue([old, new, occurrence("118-public-6", number=3)], max_items=1)
    assert result["coverage"]["qualified_target_groups"] == 3
    assert result["coverage"]["partial"]
    assert len(result["items"]) == 1
