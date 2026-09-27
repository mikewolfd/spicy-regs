"""Real vendored REF-038 evidence; no new source identity adjudications."""
from spicy_regs.vocabulary_mapping import FEDERAL_REGISTER, REGULATIONS, lookup_agency


def test_native_identifier_mapping_carries_actual_review_and_capture_pins():
    result = lookup_agency(FEDERAL_REGISTER, "406")
    assert result["status"] == "reviewed_mapping"
    row, = result["candidates"]
    assert row["source_value"] == "OPM"
    evidence, = row["evidence_records"]
    assert evidence["decision"] == "approved" and evidence["reviewer"] == "urn:ref:reviewer:refspec-owner"
    assert evidence["source_record"]["source_digest"].startswith("sha256:")
    assert evidence["target_record"]["resource"] == "urn:ref:federal-register-agency:406"
    assert result["publication"]["decision"] == "REF-038"


def test_native_parent_child_are_not_identity_and_labels_are_not_keys():
    faa, = lookup_agency(REGULATIONS, "FAA")["candidates"]
    dot, = lookup_agency(REGULATIONS, "DOT")["candidates"]
    assert faa["org"] != dot["org"] and faa["parent_org"] == dot["org"]
    assert lookup_agency(REGULATIONS, "Federal Aviation Administration")["status"] == "unmatched"
    assert lookup_agency(FEDERAL_REGISTER, "OPM")["status"] == "unmatched"


def test_retained_different_office_abstention_and_unqualified_history():
    result = lookup_agency(REGULATIONS, "ARCTICGAS")
    assert result["status"] == "unmatched" and not result["candidates"]
    record, = result["abstentions"]
    assert record["closest_non_adopted_candidate"]["reason"] == "different office and role"
    assert "different entity" in record["reasoning"]
    historical = lookup_agency(REGULATIONS, "OPM", on_date="1990-01-01")
    assert historical["status"] == "temporal_scope_unqualified"
    assert historical["candidates"]  # Evidence retained, but not an assertion for that date.
    assert lookup_agency("topic_label", "Transportation")["status"] == "unsupported_namespace"
