"""Real vendored REF-038 evidence; no new source identity adjudications."""

import pytest

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
    # Round 6: another namespace is refused, not answered with a status a caller can mistake for a lookup.
    with pytest.raises(ValueError, match="the namespaces are regulations.gov:agency and federal_register_agency"):
        lookup_agency("topic_label", "Transportation")


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


def test_the_mapping_reads_refspec_through_the_agencies_modules_public_names():
    """DRY scout S4: no reach into agencies' private names, and the Federal Register URN prefix stated once."""
    import ast
    import inspect

    from spicy_regs import vocabulary_mapping
    from spicy_regs.ontology import agencies

    source = inspect.getsource(vocabulary_mapping)
    private = [node.attr for node in ast.walk(ast.parse(source))
               if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
               and node.value.id == "agencies" and node.attr.startswith("_")]
    assert private == [] and agencies.FR_AGENCY_URN not in source
    result = lookup_agency("regulations.gov:agency", "OPM")
    assert result["registry_evidence"]["publication"] == agencies.registry_publication()
    assert result["publication"]["unresolved_sha256"] == "sha256:" + agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256
    assert agencies.unresolved_rows() and all("source_value" in row for row in agencies.unresolved_rows())
