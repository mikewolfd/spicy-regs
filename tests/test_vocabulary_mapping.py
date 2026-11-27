"""Real vendored REF-038 evidence; no new source identity adjudications."""

import pytest

from spicy_regs.vocabulary_mapping import FEDERAL_REGISTER, NAME, REGULATIONS, lookup_agency


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
    # Round 6 + View 1.4: another namespace is refused, not answered with a status a caller can mistake for a lookup.
    with pytest.raises(
        ValueError, match="the namespaces are regulations.gov:agency, federal_register_agency, and name"
    ):
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
    assert result["publication"]["schema_version"] == "1.4"
    assert result["publication"]["manifest_sha256"] == "sha256:" + agencies.AGENCY_REGISTRY_MANIFEST_SHA256
    assert result["publication"]["unresolved_sha256"] == "sha256:" + agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256
    assert agencies.unresolved_rows() and all("source_value" in row for row in agencies.unresolved_rows())


def test_all_157_view_14_aliases_and_rejected_names_resolve_accurately():
    from spicy_regs.ontology import agencies

    aliases = agencies.alias_rows()
    assert len(aliases) == 157
    for alias in aliases:
        res = lookup_agency(NAME, alias["name_key"])
        assert res["status"] == "reviewed_alias", alias["alias_id"]
        assert res["candidates"] == []
        assert len(res["registry_evidence"]["aliases"]) == 1
        assert res["registry_evidence"]["aliases"][0]["alias_id"] == alias["alias_id"]
        assert res["registry_evidence"]["current_lineage"]["status"] == "not_applicable"
        if alias["fr_resource"] is not None:
            assert res["registry_evidence"]["organization"]["resource_iri"] == alias["fr_resource"]
        else:
            assert res["registry_evidence"]["organization"] is None

    # Normalized name_key lookup (punctuation/qualifier variants) + partOf stays non-identity.
    usmc = lookup_agency(NAME, "The U.S. Marine Corps (USMC)")
    assert usmc["status"] == "reviewed_alias"
    assert usmc["registry_evidence"]["aliases"][0]["fr_relation"] == "partOf"
    assert usmc["registry_evidence"]["organization"]["fr_id"] == 378
    # Non-FR statutory entity alias held in FH/OPM (fr_resource is None).
    capitol_police = lookup_agency(NAME, "United States Capitol Police")
    assert capitol_police["status"] == "reviewed_alias"
    assert capitol_police["registry_evidence"]["aliases"][0]["target"] == "urn:ref:federal-hierarchy-org:500193110"
    assert capitol_police["registry_evidence"]["organization"] is None
    # Pinned FR organization direct name match (when not an alias).
    epa = lookup_agency(NAME, "U.S. Environmental Protection Agency")
    assert epa["status"] == "reviewed_organization"
    assert epa["registry_evidence"]["organization"]["fr_id"] == 145
    # Owner-rejected alias returns unmatched with its non-emission reasoning.
    rejected = lookup_agency(NAME, "Energy")
    assert rejected["status"] == "unmatched"
    assert rejected["registry_evidence"]["non_emissions"][0]["reason"] == "aliasRejectedByOwner"
    # Temporal abstention on name lookup preserves alias evidence.
    dated = lookup_agency(NAME, "Bureau of Land Management", on_date="1995-01-01")
    assert dated["status"] == "temporal_scope_unqualified"
    assert dated["registry_evidence"]["aliases"][0]["fr_resource"] == "urn:ref:federal-register-agency:275"
