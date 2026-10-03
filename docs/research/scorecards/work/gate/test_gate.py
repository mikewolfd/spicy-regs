"""Counterexamples for the research gate; these are synthetic schema fixtures.

Literal examples exercise observed shapes, not production completeness. No test
acquires publisher data, changes the schema, or enables ingestion.
"""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("scorecard_gate", Path(__file__).with_name("validate_gate.py"))
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


@pytest.fixture
def schema():
    return json.loads(gate.DEFAULT_SCHEMA.read_text())


def empty_bundle(schema):
    return {table["name"]: [] for table in schema["tables"]}


def add(schema, bundle, table, **values):
    definition = next(t for t in schema["tables"] if t["name"] == table)
    row = dict.fromkeys(definition["fields"])
    defaults = {
        "scorecard_id": "fixture:edition",
        "snapshot_id": "fixture:snapshot",
        "capture_id": "fixture:capture",
        "source_url": "https://example.invalid/research-fixture",
        "source_path": "synthetic fixture location",
    }
    row.update({key: value for key, value in defaults.items() if key in row})
    assert set(values) <= set(row)
    row.update(values)
    bundle[table].append(row)
    return row


def metric_free(schema):
    bundle = empty_bundle(schema)
    add(schema, bundle, "scorecard_publishers", publisher_id="fixture:nrf", name="Synthetic NRF-shaped publisher")
    add(schema, bundle, "scorecards", publisher_id="fixture:nrf", chamber_scope_text="Senate")
    add(
        schema,
        bundle,
        "scorecard_snapshots",
        capture_ids_json='["fixture:capture"]',
        capture_roles_json='[{"capture_id":"fixture:capture","role":"primary","field_groups":["items","results"]}]',
        parser_version="fixture/1",
        completeness_status="complete",
        completeness_rule="Complete synthetic fixture; no source acquisition claim",
        evidence_policy="hash_only",
        rendition_selection_rule="Single synthetic rendition",
        identity_rule_version="fixture-key/1",
    )
    add(schema, bundle, "scorecard_members", publisher_member_key="MD/68368", member_name="Alsobrooks, Angela")
    add(
        schema,
        bundle,
        "scorecard_items",
        item_id="S-2391-12552",
        title="S. 1404",
        publisher_position_text="SUPPORT",
        position_basis="explicit",
        position_source_path="item target paragraph",
    )
    add(
        schema,
        bundle,
        "scorecard_member_item_results",
        item_id="S-2391-12552",
        publisher_member_key="MD/68368",
        result_id="fixture:cell-1",
        result_text="supports nrf's position",
    )
    return bundle


def add_metric(schema, bundle, metric_id="annual"):
    return add(schema, bundle, "scorecard_metrics", metric_id=metric_id, name=metric_id)


def accepted_qualification(bundle):
    return {
        "handoffs": {name: {"accepted": True} for name in gate.HANDOFFS},
        "requirements": [
            {"id": key, "accepted": True, "evidence": ["synthetic acceptance fixture"]} for key in gate.REQUIREMENTS
        ],
        "bundles": [
            {
                "sample_id": "synthetic-nrf",
                "fixture_only": True,
                "fixture_scope": "Bounded synthetic structure; not complete source capture",
                "tables": bundle,
                "snapshot_captures": {"fixture:snapshot": ["fixture:capture"]},
            }
        ],
    }


def test_metric_free_results_and_item_target_validate_without_invented_metric(schema):
    bundle = metric_free(schema)
    original = deepcopy(bundle)
    report = gate.validate_bundle(schema, bundle, {"fixture:snapshot": ["fixture:capture"]})
    assert report["table_rows"]["scorecard_member_item_results"] == 1
    assert bundle["scorecard_metrics"] == bundle["scorecard_member_ratings"] == []
    assert bundle == original


@pytest.mark.parametrize("field", ["metric_id", "participation_id"])
def test_metric_context_cannot_be_half_populated(schema, field):
    bundle = metric_free(schema)
    bundle["scorecard_member_item_results"][0][field] = "unproven-context"
    with pytest.raises(ValueError, match="partial metric participation"):
        gate.validate_bundle(schema, bundle)


def test_explicit_metric_participation_requires_the_exact_item(schema):
    bundle = metric_free(schema)
    add_metric(schema, bundle)
    add(
        schema,
        bundle,
        "scorecard_metric_items",
        metric_id="annual",
        item_id="S-2391-12552",
        participation_id="ordinary",
    )
    bundle["scorecard_member_item_results"][0].update(metric_id="annual", participation_id="ordinary")
    gate.validate_bundle(schema, bundle)
    bundle["scorecard_member_item_results"][0]["participation_id"] = "unknown"
    with pytest.raises(ValueError, match="orphan reference to scorecard_metric_items"):
        gate.validate_bundle(schema, bundle)


def test_item_target_without_locator_is_refused(schema):
    bundle = metric_free(schema)
    bundle["scorecard_items"][0]["position_source_path"] = None
    with pytest.raises(ValueError, match="target needs basis and locator"):
        gate.validate_bundle(schema, bundle)


def test_second_snapshot_for_same_edition_refuses_even_with_unique_snapshot_id(schema):
    bundle = metric_free(schema)
    second = deepcopy(bundle["scorecard_snapshots"][0])
    second["snapshot_id"] = "fixture:old-snapshot"
    bundle["scorecard_snapshots"].append(second)
    with pytest.raises(ValueError, match="Exactly one accepted snapshot"):
        gate.validate_bundle(schema, bundle)


def test_resolving_foreign_keys_does_not_allow_wrong_snapshot(schema):
    bundle = metric_free(schema)
    for table in ("scorecards", "scorecard_snapshots"):
        second = deepcopy(bundle[table][0])
        second.update(scorecard_id="fixture:other-edition", snapshot_id="fixture:other-snapshot")
        bundle[table].append(second)
    # Both snapshot IDs exist; ordinary identity references alone would pass.
    bundle["scorecard_members"][0]["snapshot_id"] = "fixture:other-snapshot"
    with pytest.raises(ValueError, match="fact snapshot differs"):
        gate.validate_bundle(schema, bundle)


def test_fact_capture_and_independent_capture_membership_are_checked(schema):
    bundle = metric_free(schema)
    with pytest.raises(ValueError, match="External snapshot capture membership differs"):
        gate.validate_bundle(schema, bundle, {"fixture:snapshot": ["unrecorded-capture"]})
    bundle["scorecard_items"][0]["capture_id"] = "unrecorded-capture"
    with pytest.raises(ValueError, match="capture is not in selected snapshot"):
        gate.validate_bundle(schema, bundle)


def test_repeated_hr4_actions_remain_distinct_and_duplicate_keys_refuse(schema):
    bundle = metric_free(schema)
    bundle["scorecard_items"] = []
    bundle["scorecard_member_item_results"] = []
    for suffix in ("hr-4-rescissions-act-2025", "hr-4-rescissions-act-2025-0"):
        add(
            schema,
            bundle,
            "scorecard_items",
            item_id=suffix,
            publisher_item_id=suffix,
            title="H.R. 4, Rescissions Act of 2025",
            bill_citation_text="H.R. 4",
        )
        add(
            schema,
            bundle,
            "scorecard_member_item_results",
            item_id=suffix,
            publisher_member_key="MD/68368",
            result_id="fixture:cell",
            result_text="ｘ",
        )
    gate.validate_bundle(schema, bundle)
    assert len(bundle["scorecard_items"]) == 2
    assert bundle["scorecard_member_item_results"][0]["result_text"] == "ｘ"
    bundle["scorecard_items"][1]["item_id"] = bundle["scorecard_items"][0]["item_id"]
    with pytest.raises(ValueError, match="duplicate identity"):
        gate.validate_bundle(schema, bundle)


def test_lcv_selected_csv_literal_is_not_replaced_with_html_display(schema):
    bundle = metric_free(schema)
    snapshot = bundle["scorecard_snapshots"][0]
    snapshot["capture_ids_json"] = '["fixture:capture","fixture:html"]'
    snapshot["capture_roles_json"] = json.dumps(
        [
            {"capture_id": "fixture:capture", "role": "primary", "field_groups": ["ratings", "membership"]},
            {"capture_id": "fixture:html", "role": "corroboration", "field_groups": ["membership"]},
        ]
    )
    snapshot["rendition_selection_rule"] = "CSV rating values; HTML validates membership"
    add_metric(schema, bundle, "lifetime")
    rating = add(
        schema,
        bundle,
        "scorecard_member_ratings",
        metric_id="lifetime",
        publisher_member_key="MD/68368",
        value_text="2",
        value_number="2",
    )
    assertions = [
        {
            "table": "scorecard_member_ratings",
            "key": ["fixture:edition", "lifetime", "MD/68368"],
            "field": "value_text",
            "expected": "2",
        }
    ]
    original = deepcopy(bundle)
    gate.validate_bundle(schema, bundle, literal_assertions=assertions)
    assert bundle == original
    rating["value_text"] = "2%"
    with pytest.raises(ValueError, match="Literal source assertion differs"):
        gate.validate_bundle(schema, bundle, literal_assertions=assertions)


def test_two_primary_renditions_for_same_field_group_refuse(schema):
    bundle = metric_free(schema)
    snapshot = bundle["scorecard_snapshots"][0]
    roles = json.loads(snapshot["capture_roles_json"])
    roles.append(deepcopy(roles[0]))
    snapshot["capture_roles_json"] = json.dumps(roles)
    with pytest.raises(ValueError, match="exactly one primary rendition"):
        gate.validate_bundle(schema, bundle)


@pytest.mark.parametrize("token", ["✓", "ｘ", "◯", ""])
def test_source_result_tokens_and_state_typo_survive_unchanged(schema, token):
    bundle = metric_free(schema)
    bundle["scorecard_member_item_results"][0]["result_text"] = token
    bundle["scorecard_members"][0]["state"] = "NES"
    original = deepcopy(bundle)
    gate.validate_bundle(schema, bundle)
    assert bundle == original


@pytest.mark.parametrize("bad_value", [False, 2, 0.5])
def test_no_implicit_value_coercion(schema, bad_value):
    bundle = metric_free(schema)
    bundle["scorecard_member_item_results"][0]["result_text"] = bad_value
    with pytest.raises(ValueError, match="string or null"):
        gate.validate_bundle(schema, bundle)


def test_absent_field_and_missing_parent_do_not_become_empty_success(schema):
    bundle = metric_free(schema)
    del bundle["scorecard_members"][0]["notes_text"]
    with pytest.raises(ValueError, match="exact fields required"):
        gate.validate_bundle(schema, bundle)
    bundle = metric_free(schema)
    bundle["scorecard_members"] = []
    with pytest.raises(ValueError, match="orphan reference to scorecard_members"):
        gate.validate_bundle(schema, bundle)


def test_metric_component_cycles_refuse(schema):
    bundle = metric_free(schema)
    add_metric(schema, bundle, "overall")
    add_metric(schema, bundle, "leadership")
    add(schema, bundle, "scorecard_metric_components", parent_metric_id="overall", component_metric_id="leadership")
    gate.validate_bundle(schema, bundle)
    add(schema, bundle, "scorecard_metric_components", parent_metric_id="leadership", component_metric_id="overall")
    with pytest.raises(ValueError, match="Metric component cycle"):
        gate.validate_bundle(schema, bundle)


def test_schema_rejects_nonidentity_foreign_reference(schema):
    table = next(t for t in schema["tables"] if t["name"] == "scorecard_items")
    table["references"][0]["parent_columns"].append("snapshot_id")
    table["references"][0]["child_columns"].append("snapshot_id")
    with pytest.raises(ValueError, match="must target parent identity"):
        gate.validate_schema(schema)


def test_schema_requires_physical_common_fields_and_varchar(schema):
    table = next(t for t in schema["tables"] if t["name"] == "scorecard_items")
    table["types"]["item_date_text"] = "DATE"
    with pytest.raises(ValueError, match="all fields must declare VARCHAR"):
        gate.validate_schema(schema)
    table["types"]["item_date_text"] = "VARCHAR"
    table["fields"].remove("capture_id")
    del table["types"]["capture_id"]
    with pytest.raises(ValueError, match="must be physical columns"):
        gate.validate_schema(schema)


def test_gate_stays_closed_without_each_accepted_handoff(schema):
    qualification = accepted_qualification(metric_free(schema))
    assert gate.validate_gate(schema, None)["allows"] is False
    for name in gate.HANDOFFS:
        changed = deepcopy(qualification)
        changed["handoffs"][name]["accepted"] = False
        report = gate.validate_gate(schema, changed)
        assert report["allows"] is False
        assert f"handoff not accepted: {name}" in report["pending"]


def test_gate_needs_separate_source_proof_acceptance_and_cannot_authorize_publication(schema):
    qualification = accepted_qualification(metric_free(schema))
    report = gate.validate_gate(schema, qualification)
    assert report["allows"] is True
    assert report["allows_production_publication"] is False
    proof = next(r for r in qualification["requirements"] if r["id"] == "selected_scope_boundaries_proven")
    proof["evidence"] = []
    assert gate.validate_gate(schema, qualification)["allows"] is False


def test_unlabeled_fixture_cannot_imply_production_completeness(schema):
    qualification = accepted_qualification(metric_free(schema))
    qualification["bundles"][0]["fixture_only"] = False
    report = gate.validate_gate(schema, qualification)
    assert report["allows"] is False
    assert any("fixture_only" in error for error in report["errors"])


def test_empty_table_set_does_not_qualify_as_a_representative_source_fixture(schema):
    qualification = accepted_qualification(empty_bundle(schema))
    qualification["bundles"][0]["snapshot_captures"] = {}
    report = gate.validate_gate(schema, qualification)
    assert report["allows"] is False
    assert any("must contain an edition" in error for error in report["errors"])


def test_repeated_references_need_distinct_occurrence_ids(schema):
    bundle = metric_free(schema)
    references = [
        {"occurrence_id": "first", "citation_text": "H.R. 4", "kind": "bill", "source_path": "first source mention"},
        {"occurrence_id": "second", "citation_text": "H.R. 4", "kind": "bill", "source_path": "second source mention"},
    ]
    item = bundle["scorecard_items"][0]
    item["references_json"] = json.dumps(references)
    gate.validate_bundle(schema, bundle)
    references[1]["occurrence_id"] = "first"
    item["references_json"] = json.dumps(references)
    with pytest.raises(ValueError, match="duplicate occurrence_id"):
        gate.validate_bundle(schema, bundle)


@pytest.mark.parametrize("table", ["scorecard_items", "scorecard_publishers"])
@pytest.mark.parametrize("field", ["capture_id", "source_url", "source_path"])
def test_every_source_observation_needs_capture_and_locator(schema, table, field):
    bundle = metric_free(schema)
    bundle[table][0][field] = None
    with pytest.raises(ValueError, match=f"source observation needs {field}"):
        gate.validate_bundle(schema, bundle)


@pytest.mark.parametrize(
    "field,text_field,kind",
    [("periods_json", "period_text", "relative"), ("references_json", "citation_text", "bill")],
)
@pytest.mark.parametrize("missing", ["text", "locator"])
def test_source_occurrences_need_literal_text_and_locator(schema, field, text_field, kind, missing):
    bundle = metric_free(schema)
    occurrence = {"occurrence_id": "source-occurrence", text_field: "source text", "kind": kind, "source_path": "p1"}
    occurrence[text_field if missing == "text" else "source_path"] = ""
    row = bundle["scorecards" if field == "periods_json" else "scorecard_items"][0]
    row[field] = json.dumps([occurrence])
    with pytest.raises(ValueError, match="occurrence needs source text and locator"):
        gate.validate_bundle(schema, bundle)
