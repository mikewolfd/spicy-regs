"""Retained main-key navigation preserves source scope and all occurrences."""

import json
import base64
import io
from pathlib import Path

import pyarrow as pa
import pytest

from spicy_regs.explorer_navigation import published_navigation, target_keys, validate_navigation
from spicy_regs.explorer_retained_navigation import declarations
from spicy_regs.navigation_measurements import MeasurementCache
from tests.test_remaining_main_navigation import selected


def recipe(name):
    return next(spec for spec in declarations() if spec["id"] == name)


def digest(letter):
    return "sha256:" + letter * 64


def measure(tmp_path, name, rows, targets, identity, schema=None):
    spec = recipe(name)
    index, paths = selected(tmp_path, [
        (spec["source"], rows, schema), (spec["targets"][0]["table"], targets, None),
    ])
    return MeasurementCache(tmp_path / "cache").measure(index, paths, spec, source_identity=identity)


def test_declarations_use_only_explicit_main_fields_and_are_independent():
    specs = declarations()
    validate_navigation(specs)
    assert len({s["id"] for s in specs}) == len(specs)
    assert all(not s["receiptFields"] for s in specs)
    assert all(not t["table"].startswith("@receipt:") for s in specs for t in s["targets"])
    specs[0]["targets"][0]["keys"].clear()
    assert declarations()[0]["targets"][0]["keys"]


@pytest.mark.parametrize("name,field,target_column", [
    ("sec_file_rulemaking", "rulemaking_id", "rulemaking_id"),
    ("supreme_court_opinion_index", "index_observation_id", "observation_id"),
    ("ecfr_content_snapshot", "snapshot_id", "snapshot_id"),
    ("uscode_content_edition_member", "edition_id", "edition_id"),
    ("uscode_structure_edition_member", "edition_id", "edition_id"),
])
def test_content_bound_parent_references_do_not_cross_capture_versions(tmp_path, name, field, target_column):
    proof = measure(tmp_path, name,
                    [{"id": "first", field: digest("a")}, {"id": "other-version", field: digest("b")}],
                    [{target_column: digest("a"), "member_name": "same.xml"}], ("id",))
    assert (proof["eligible"], proof["matched"], proof["missing"], proof["ambiguous"]) == (2, 1, 1, 0)
    assert proof["reverse"]["matchedTargetRows"] == 1
    assert proof["reverse"]["maximumDistinctSourceRecordsPerTarget"] == 1


def test_cftc_comment_link_uses_native_ids_with_exact_rule_observation(tmp_path):
    proof = measure(tmp_path, "cftc_file_comment", [
        {"observation_id": digest("c"), "rule_observation_id": digest("a"), "rule_id": 12, "comment_id": 34},
        {"observation_id": digest("d"), "rule_observation_id": digest("b"), "rule_id": 12, "comment_id": 34},
    ], [{"rule_observation_id": digest("a"), "rule_id": 12, "comment_id": 34}], ("observation_id",))
    assert (proof["matched"], proof["missing"]) == (1, 1)
    assert proof["reverse"]["maximumDistinctSourceRecordsPerTarget"] == 1


def test_edis_attachment_link_preserves_multiple_metadata_versions_and_rejects_wrong_scope(tmp_path):
    row = {"document_observation_id": digest("a"), "document_id": 5, "attachment_id": 9}
    proof = measure(tmp_path, "edis_file_attachment", [
        {"observation_id": digest("b"), **row},
        {"observation_id": digest("c"), **row, "document_observation_id": digest("d")},
    ], [{"observation_id": digest(letter), **row} for letter in ("e", "f")], ("observation_id",))
    assert (proof["eligible"], proof["matched"], proof["missing"], proof["ambiguous"]) == (2, 1, 1, 1)
    assert proof["targetPopulation"]["maximumRowsPerKey"] == 2
    assert proof["reverse"]["matchedTargetRows"] == 2


def test_ferc_collection_does_not_select_one_capture_or_cross_accessions(tmp_path):
    native = {"accession": "20240418-4000", "file_guid": "12345678-1234-1234-1234-1234567890ab"}
    proof = measure(tmp_path, "ferc_original_file_observations", [
        {"observation_id": digest(letter), **native} for letter in ("a", "b")
    ] + [{"observation_id": digest("c"), **native, "accession": "20240419-4000"}], [
        {"observation_id": digest(letter), **native} for letter in ("d", "e")
    ], ("observation_id",))
    assert (proof["eligible"], proof["matched"], proof["missing"], proof["ambiguous"]) == (3, 2, 1, 2)
    assert proof["reverse"]["matchedTargetRows"] == 2
    assert proof["reverse"]["maximumDistinctSourceRecordsPerTarget"] == 2


def test_ferc_accession_navigation_retains_repeated_search_captures(tmp_path):
    proof = measure(tmp_path, "ferc_file_search_observations", [
        {"observation_id": digest("a"), "accession": "20240418-4000"},
    ], [{"observation_id": digest(letter), "accession": "20240418-4000"} for letter in ("b", "c")]
        + [{"observation_id": digest("d"), "accession": "20240419-4000"}], ("observation_id",))
    assert (proof["matched"], proof["ambiguous"]) == (1, 1)
    assert proof["reverse"]["matchedTargetRows"] == 2
    assert proof["reverse"]["unmatchedTargetRows"] == 1


def test_supreme_court_pdf_declarations_keep_revisions_repeats_and_index_scope(tmp_path):
    occurrence = {"index_observation_id": digest("a"), "opinion_index": 0, "role": "opinion"}
    entries = [occurrence, {**occurrence, "role": "revision", "revision_index": 1},
               {**occurrence, "index_observation_id": digest("b")}, {**occurrence, "role": "unknown"}]
    proof = measure(tmp_path, "supreme_court_pdf_declarations", [
        {"observation_id": digest("c"), "declaring_occurrences_json": json.dumps(entries)},
    ], [{"index_observation_id": digest("a"), "opinion_index": 0}], ("observation_id",))
    assert (proof["eligible"], proof["matched"], proof["missing"]) == (3, 2, 1)
    assert proof["repeatedReferences"] == 1
    assert proof["unsupportedReferences"] == 1
    assert proof["reverse"]["maximumReferencesPerTarget"] == 2
    assert proof["reverse"]["maximumDistinctSourceRecordsPerTarget"] == 1


def test_administration_links_keep_repeated_mentions_and_do_not_infer_positions(tmp_path):
    proof = measure(tmp_path, "administration_policy_statement", [
        {"link_id": digest(letter), "statement_id": digest("a"), "source_link_index": i}
        for i, letter in enumerate(("b", "c"))
    ] + [{"link_id": digest("d"), "statement_id": digest("e"), "source_link_index": 0}],
        [{"statement_id": digest("a"), "rescinded": False}], ("link_id",))
    assert (proof["matched"], proof["missing"]) == (2, 1)
    assert proof["reverse"]["maximumDistinctSourceRecordsPerTarget"] == 2
    bill = recipe("administration_policy_bill")["targets"][0]
    assert target_keys(bill, {}, {"bill_id": "118-hr-5", "congress": 119}) == ["118-hr-5"]
    assert target_keys(bill, {}, {"bill": "hr5", "congress": 119}) is None


@pytest.mark.parametrize("value", [None, "", True, False, {}, [], 1.0, "other-observation"])
def test_parent_keys_refuse_invalid_shape_even_when_a_native_name_matches(value):
    target = recipe("uscode_content_edition_member")["targets"][0]
    assert target_keys(target, {}, {"edition_id": value, "member_name": "title5.xml"}) is None


@pytest.mark.parametrize("value", [None, True, 1.0, -1, "0", "12.0"])
def test_native_document_identity_requires_positive_integer_spelling(value):
    target = recipe("edis_file_document")["targets"][0]
    assert target_keys(target, {}, {"document_observation_id": digest("a"), "document_id": value}) is None


def test_typed_array_shape_and_complete_main_keys_gate_availability():
    spec = recipe("supreme_court_pdf_declarations")
    schemas = {
        spec["source"]: [["declaring_occurrences_json", "STRUCT(index_observation_id VARCHAR, opinion_index BIGINT)[]"]],
        "supreme_court_opinion_observations": [["index_observation_id", "VARCHAR"], ["opinion_index", "BIGINT"]],
    }
    target = published_navigation([spec], schemas)[0]["targets"][0]
    assert not target["sourceAvailable"]  # Declaring role is required, even with both key fields present.
    schemas[spec["source"]] = [["declaring_occurrences_json", "VARCHAR"]]
    assert published_navigation([spec], schemas)[0]["targets"][0]["sourceAvailable"]
    del schemas["supreme_court_opinion_observations"]
    assert not published_navigation([spec], schemas)[0]["targets"][0]["available"]


def test_every_retained_recipe_binds_to_its_maintained_native_main_schema():
    from spicy_regs.native_types import described_schema
    from spicy_regs.subject_catalog import descriptors
    policies = descriptors()
    schemas = {table: described_schema(pa.ipc.read_schema(io.BytesIO(base64.b64decode(p["subject_schema"]))))
               for table, p in policies.items() if not p["receipt_only"]}
    identities = {table: p["identity_fields"] for table, p in policies.items() if not p["receipt_only"]}
    specs = published_navigation(declarations(), schemas, identities)
    assert len(specs) == len(declarations())
    for spec in specs:
        for target in spec["targets"]:
            assert target["sourceAvailable"], spec["id"]
            assert target["available"], spec["id"]


def test_all_retained_collections_have_original_source_attribution_without_false_main_inputs():
    registry = json.loads((Path(__file__).parents[1] / "src/spicy_regs/explorer_sources.json").read_text())
    sources = {
        "administration_policy_statements": "administration-policy-archive",
        "administration_policy_bill_links": "administration-policy-archive",
        "uscode_editions": "uscode", "uscode_edition_structure": "uscode", "uscode_edition_sections": "uscode",
        "sec_rulemakings": "sec-comments", "sec_comment_files": "sec-comments",
        "cftc_rule_observations": "cftc-comments", "cftc_comments": "cftc-comments",
        "cftc_comment_files": "cftc-comments",
        "edis_documents": "edis", "edis_attachments": "edis", "edis_attachment_files": "edis",
        "ferc_search_documents": "ferc", "ferc_accession_files": "ferc", "ferc_original_files": "ferc",
        "supreme_court_index_observations": "supreme-court",
        "supreme_court_opinion_observations": "supreme-court", "supreme_court_pdf_observations": "supreme-court",
        "inspectors_general_report_metadata": "reports-archive", "federal_register_issues": "govinfo-register",
        "ecfr_snapshots": "ecfr", "ecfr_snapshot_sections": "ecfr", "table3_historical_records": "uscode",
    }
    for table, source in sources.items():
        origin = registry["tables"][table]
        assert origin["sources"] == [source]
        assert origin["inputs"] == []  # Sibling main tables were not the mapper's input.
        assert not origin["modelGenerated"]
        assert (Path(__file__).parents[1] / origin["evidence"]).is_file()
    assert registry["sources"]["administration-policy-archive"]["kind"] == "community"
    assert registry["sources"]["reports-archive"]["kind"] == "community"


def test_public_metadata_binds_source_attribution_to_the_actual_main_table():
    from spicy_regs.explorer_metadata import build_bundle

    table = "uscode_editions"
    result = build_bundle({"format": "spicy-regs-publication", "version": 2, "families": {
        "caller-selected-family": {"artifactDigest": digest("a"), "tables": {
            table + ".parquet": {"columns": [["edition_id", "VARCHAR"]], "rows": 1, "byteSize": 100},
        }},
    }}, descriptions={table: {"label": "Edition members", "identity_columns": ["edition_id"], "columns": []}},
        join_record={"joins": []}, audit={}, generated_at="2026-10-13T00:00:00Z")
    metadata = result["tables"][table]
    assert metadata["sourceStatus"] == "documented"
    assert [s["id"] for s in metadata["sources"]] == ["uscode"]
    assert metadata["inputs"] == []
