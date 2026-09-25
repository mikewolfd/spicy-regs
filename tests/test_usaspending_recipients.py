"""USAspending field mapping; page failure and selection bound tests are in test_reference_source_failures."""

from __future__ import annotations

from spicy_regs.transforms.build_usaspending_recipients import COLUMNS, _shape

_RAW_RECIPIENT = {
    "id": "b97d19b0-833c-8d8f-3a2c-157d04ea55ef-P",
    "duns": "834951691",
    "uei": "ZFN2JJXBLZT3",
    "name": "LOCKHEED MARTIN CORP",
    "recipient_level": "P",
    "amount": 63465270734.15,
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_RECIPIENT)
    # Every published column present, and nothing extra (including observation metadata).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 8


def test_shape_maps_and_stringifies_fields():
    row = _shape(_RAW_RECIPIENT)
    assert row["recipient_id"] == "b97d19b0-833c-8d8f-3a2c-157d04ea55ef-P"
    assert row["uei"] == "ZFN2JJXBLZT3"
    assert row["duns"] == "834951691"
    assert row["name"] == "LOCKHEED MARTIN CORP"
    assert row["recipient_level"] == "P"
    # Numeric amount is coerced to a string, not left as a float.
    assert row["total_award_amount"] == "63465270734.15"


def test_shape_handles_missing_fields():
    row = _shape({"id": "x-R"})
    assert row["recipient_id"] == "x-R"
    # Missing scalars degrade to None, not KeyError.
    assert row["uei"] is None
    assert row["duns"] is None
    assert row["name"] is None
    assert row["recipient_level"] is None
    # Missing amount stays None (not the string "None").
    assert row["total_award_amount"] is None
