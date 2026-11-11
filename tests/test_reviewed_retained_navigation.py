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
        "federal_register_issues": "govinfo-register",
        "ecfr_snapshots": "ecfr", "ecfr_snapshot_sections": "ecfr", "table3_historical_records": "uscode",
    }
    for table, source in sources.items():
        origin = registry["tables"][table]
        assert origin["sources"] == [source]
        assert origin["inputs"] == []  # Sibling main tables were not the mapper's input.
        assert not origin["modelGenerated"]
        assert (Path(__file__).parents[1] / origin["evidence"]).is_file()
    assert registry["sources"]["administration-policy-archive"]["kind"] == "community"


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
