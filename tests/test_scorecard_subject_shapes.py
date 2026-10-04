"""Domain conversion preserves publisher meaning and exact conversion inputs."""

from decimal import Decimal
import json

import pyarrow as pa
import pytest
from spicy_docs.schemas.scorecard_tables import SCORECARD_TABLES
from spicy_regs.scorecards.resolution import LINK_COLUMNS
from spicy_regs.scorecards.subject_shapes import (
    SOURCE_COLUMNS,
    DOMAIN_COLUMNS,
    map_source_row,
    restore_source_row,
    subject_schema,
)


def row(name, **values):
    result = dict.fromkeys(SOURCE_COLUMNS[name])
    from spicy_regs.scorecards.subject_shapes import IDENTITIES

    result.update({key: key for key in IDENTITIES[name]})
    return result | values


def test_every_provider_and_resolver_field_is_explicitly_classified():
    actual = {name: set(c.columns) for name, c in SCORECARD_TABLES.items()}
    actual.update({name: set(columns) for name, columns in LINK_COLUMNS.items()})
    assert actual == {name: set(columns) for name, columns in SOURCE_COLUMNS.items()}
    assert not DOMAIN_COLUMNS["scorecard_snapshots"]


@pytest.mark.parametrize("value", [None, "[]", '["alias","alias","last"]'])
def test_alias_order_repeats_and_null_empty_distinction(value):
    before = row("scorecard_publishers", aliases_json=value)
    mapped = map_source_row("scorecard_publishers", before)
    assert mapped["aliases"] == (json.loads(value) if value is not None else None)
    assert restore_source_row("scorecard_publishers", mapped) == before


def test_identifiers_and_occurrences_preserve_structure_and_receipt_locators():
    identifiers = '[{"scheme":"id","value":"a"},{"scheme":"id","value":"a"}]'
    mapped = map_source_row("scorecard_members", row("scorecard_members", identifiers_json=identifiers))
    assert mapped["identifiers"] == json.loads(identifiers)
    references = json.dumps(
        [
            dict(occurrence_id="a", citation_text="H.R. 1", kind="bill", source_path="p1", chamber_text=None),
            dict(occurrence_id="b", citation_text="H.R. 1", kind="bill", source_path="p2"),
        ]
    )
    before = row("scorecard_items", references_json=references)
    mapped = map_source_row("scorecard_items", before)
    assert [r["occurrence_id"] for r in mapped["references"]] == ["a", "b"]
    assert [r["citation_text"] for r in mapped["references"]] == ["H.R. 1", "H.R. 1"]
    assert all("source_path" not in r for r in mapped["references"])
    assert restore_source_row("scorecard_items", mapped) == before


@pytest.mark.parametrize("value", ["null", "{}", "[null]", '[{"scheme":"a","scheme":"b","value":"1"}]'])
def test_invalid_lists_refuse_without_coercion(value):
    with pytest.raises(ValueError):
        map_source_row("scorecard_members", row("scorecard_members", identifiers_json=value))


def test_exact_numeric_and_status_facts():
    before = row(
        "scorecard_member_ratings",
        value_text="97.123456789012345",
        value_number="+097.123456789012345",
        value_status_text="not graded",
    )
    mapped = map_source_row("scorecard_member_ratings", before)
    assert mapped["value_number"] == Decimal("97.123456789012345")
    assert mapped["value_status_text"] == "not graded"
    assert restore_source_row("scorecard_member_ratings", mapped) == before
    assert subject_schema("scorecard_member_ratings").field("value_number").type == pa.decimal128(38, 18)
    before = row("scorecard_metric_items", counts_toward_metric="false", weight_number="-2.00")
    mapped = map_source_row("scorecard_metric_items", before)
    assert mapped["counts_toward_metric"] is False
    assert restore_source_row("scorecard_metric_items", mapped) == before
    assert "disclosure_status" in subject_schema("scorecard_methodologies").names
    assert "eligibility_text" in subject_schema("scorecard_members").names


@pytest.mark.parametrize("value", ["NaN", "1e2", "0.0000000000000000001", "100000000000000000000"])
def test_numeric_overflow_and_rounding_refuse(value):
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        map_source_row("scorecard_member_ratings", row("scorecard_member_ratings", value_number=value))


def test_unknown_fields_refuse():
    with pytest.raises(ValueError, match="classify every field"):
        map_source_row("scorecards", row("scorecards", new_field="must audit"))


def test_period_variants_and_empty_component_table_have_declared_native_types():
    periods = json.dumps(
        [
            dict(
                occurrence_id="period-1",
                period_text="118th Congress",
                kind="explicit",
                source_path="p1",
                congress_text="118",
            ),
            dict(occurrence_id="period-2", period_text="Lifetime", kind="relative", source_path="p2"),
        ]
    )
    before = row("scorecards", periods_json=periods)
    mapped = map_source_row("scorecards", before)
    assert [entry["kind"] for entry in mapped["periods"]] == ["explicit", "relative"]
    assert mapped["periods"][1]["congress_text"] is None
    assert restore_source_row("scorecards", mapped) == before
    assert subject_schema("scorecard_metric_components").field("weight_number").type == pa.decimal128(38, 18)


@pytest.mark.parametrize("literal,native", [(None, None), ("true", True), ("false", False)])
def test_boolean_null_is_not_false(literal, native):
    before = row("scorecard_metrics", is_primary=literal)
    mapped = map_source_row("scorecard_metrics", before)
    assert mapped["is_primary"] is native
    assert restore_source_row("scorecard_metrics", mapped) == before


def test_resolved_legislative_numbers_are_native_and_replay_exactly():
    before = row("scorecard_item_links", congress="119", session="1", roll_number="597")
    mapped = map_source_row("scorecard_item_links", before)
    assert (mapped["congress"], mapped["session"], mapped["roll_number"]) == (119, 1, 597)
    assert restore_source_row("scorecard_item_links", mapped) == before
