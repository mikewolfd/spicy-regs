"""Real vendored REF-038 evidence; no new source identity adjudications."""

from spicy_regs.vocabulary_mapping import FEDERAL_REGISTER, REGULATIONS, lookup_agency


def test_native_identifier_mapping_carries_actual_review_and_capture_pins():
    result = lookup_agency(FEDERAL_REGISTER, "406")
    assert result["status"] == "reviewed_mapping"
    (row,) = result["candidates"]
    assert row["source_value"] == "OPM"
    (evidence,) = row["evidence_records"]
    assert evidence["decision"] == "approved" and evidence["reviewer"] == "urn:ref:reviewer:refspec-owner"
    assert evidence["source_record"]["source_digest"].startswith("sha256:")
    assert evidence["target_record"]["resource"] == "urn:ref:federal-register-agency:406"
    assert result["publication"]["decision"] == "REF-038"


def test_native_parent_child_are_not_identity_and_labels_are_not_keys():
    (faa,) = lookup_agency(REGULATIONS, "FAA")["candidates"]
    (dot,) = lookup_agency(REGULATIONS, "DOT")["candidates"]
    assert faa["org"] != dot["org"] and faa["parent_org"] == dot["org"]
    assert lookup_agency(REGULATIONS, "Federal Aviation Administration")["status"] == "unmatched"
    assert lookup_agency(FEDERAL_REGISTER, "OPM")["status"] == "unmatched"


def test_retained_different_office_abstention_and_unqualified_history():
    result = lookup_agency(REGULATIONS, "ARCTICGAS")
    assert result["status"] == "unmatched" and not result["candidates"]
    (record,) = result["abstentions"]
    assert record["closest_non_adopted_candidate"]["reason"] == "different office and role"
    assert "different entity" in record["reasoning"]
    historical = lookup_agency(REGULATIONS, "OPM", on_date="1990-01-01")
    assert historical["status"] == "temporal_scope_unqualified"
    assert historical["candidates"]  # Evidence retained, but not an assertion for that date.
    assert lookup_agency("topic_label", "Transportation")["status"] == "unsupported_namespace"


def test_registry_bridge_and_current_successor_are_distinct_from_historical_identity():
    bridge = lookup_agency(FEDERAL_REGISTER, "136")
    assert bridge["status"] == "reviewed_bridge"
    assert bridge["registry_evidence"]["current_lineage"]["code"] == "DOE"
    assert bridge["registry_evidence"]["bridges"]
    old = lookup_agency(FEDERAL_REGISTER, "559")
    assert old["status"] == "succession_evidence"
    assert old["registry_evidence"]["current_lineage"]["code"] == "CMS"
    assert all(row["effective_date"] for row in old["registry_evidence"]["events"])
    historical = lookup_agency(FEDERAL_REGISTER, "559", on_date="1998-01-01")
    assert historical["status"] == "temporal_scope_unqualified"
    assert historical["registry_evidence"]["current_lineage"]["historical_identity_qualified"] is False
    assert historical["registry_evidence"]["publication"]["release"]["decisionRecord"].endswith("ref-072")


def test_registry_split_and_non_emission_are_not_silently_mapped():
    split = lookup_agency(FEDERAL_REGISTER, "232")
    assert split["status"] == "succession_evidence"
    assert len({row["result"] for row in split["registry_evidence"]["events"]}) > 1
    assert split["registry_evidence"]["current_lineage"]["code"] is None
    cisa = lookup_agency(REGULATIONS, "CISA")
    assert cisa["status"] == "reviewed_mapping"
    assert cisa["registry_evidence"]["non_emissions"][0]["reason"] == "noCounterpartInHeldFRRoster"
    assert lookup_agency(FEDERAL_REGISTER, "0406")["registry_evidence"]["current_lineage"]["code"] is None
