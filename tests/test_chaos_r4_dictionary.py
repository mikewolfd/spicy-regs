"""Declared-metadata repairs from the 2026-10-03 blind persona test (round 4, scout A).

Hermetic: the interim-note expiry, the regression guard that binds key prose to
the declared identity, and the join declarations, against the committed
``descriptions.yaml`` and the in-code schema. The claims that need the
published data are in ``test_chaos_r4_dictionary_live.py``.
"""

from __future__ import annotations

import re

import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs import table_joins

#: A local build the notes below name, standing for whichever build a real note would.
VENDORED = "0.53.0+votes.8d7f3fe3b489"
NOTE = f"The vendored column sentence is wrong. (interim until spicy-docs > {VENDORED})"


# --------------------------------------------------------------------------- #
# Interim notes expire with the wheel they stand in for.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("installed", [VENDORED, "0.53.0", "0.52.9", "0.52.9+laws.ffffffffffff"])
def test_an_interim_note_stands_while_the_wheel_is_the_stated_one_or_older(installed):
    assert dd.interim_note_errors("laws", NOTE, installed) == []


@pytest.mark.parametrize("installed", ["0.54.0", "0.53.1", "0.53.0+billslane.c681b7704d2d"])
def test_an_interim_note_expires_once_the_wheel_moves_past_its_version(installed):
    """A local label names a branch, not an order, so another build of the same release counts as past it."""
    errors = dd.interim_note_errors("laws", NOTE, installed)
    assert len(errors) == 1 and installed in errors[0] and VENDORED in errors[0], errors


@pytest.mark.parametrize(
    "note",
    [
        "Wrong until the wheel lands (interim until the bills-lane release).",
        "Wrong (interim until spicy-docs > not-a-version).",
        "Wrong (Interim until spicy-docs >= 0.54.0).",
    ],
)
def test_an_interim_note_whose_expiry_cannot_be_evaluated_is_refused(note):
    """An expiry the check cannot read would let the note stand for ever, which is what the marker exists to stop."""
    assert dd.interim_note_errors("laws", note, VENDORED), note


def test_the_check_refuses_an_expired_note_in_the_dictionary(monkeypatch):
    broken = {table: dict(entry) for table, entry in dd.load_descriptions().items()}
    broken["laws"]["data_quality"] = NOTE
    monkeypatch.setattr(dd, "installed_spicy_docs", lambda: "0.54.0")
    errors = [error for error in dd.check_descriptions(dd.expected_schemas(), broken) if error.startswith("[laws]")]
    assert len(errors) == 1 and "interim" in errors[0], errors


@pytest.mark.parametrize("table", ["congress_bills", "laws", "law_sections", "roll_call_votes"])
def test_no_round4_stand_in_outlives_the_wheel_that_fixed_it(table):
    """W1 (stage), W3 (latest action), B2 (margin notes) and B3 (party totals) ship in the adopted 0.54.0 build.

    Their contract prose now states each rule (party_totals_json, the enactment-date pointer, the v2 margin-note
    rule keyed on reader_version), so no interim paragraph stands in for them any more.
    """
    note = dd.load_descriptions()[table].get("data_quality") or ""
    assert not dd.INTERIM_MARKER.findall(" ".join(note.split())), table


# --------------------------------------------------------------------------- #
# Regression guard: key prose and the declared identity say the same thing.
# --------------------------------------------------------------------------- #
#: A column's own description calling it the key, or part of it. "The table's primary key remains name" names
#: another column and is lowercase, so it is not matched.
_KEY_PROSE = re.compile(r"\b(?:Primary key|Primary/dedup key|primary/dedup key|dedup key)\b")


def test_a_column_described_as_the_key_is_in_the_declared_identity():
    """Regression guard, not a claim check: a unique key can still merge two entities or split one (W4)."""
    metadata = dd.build_mcp_metadata(dd.load_descriptions(), dd.expected_schemas())
    offenders = [
        f"{table}.{column['column_name']}"
        for table, entry in metadata.items()
        for column in entry["columns"]
        if _KEY_PROSE.search(column["description"] or "") and column["column_name"] not in entry["identity_columns"]
    ]
    assert offenders == [], offenders


# --------------------------------------------------------------------------- #
# Join declarations.
# --------------------------------------------------------------------------- #
def test_rule_targets_reach_cfr_sections_on_the_structural_part():
    """The printed cfr_ref missed parts held only as sections (50 CFR 622); rule_targets keys are part-level."""
    (join,) = [join for join in table_joins.JOINS if join.child == "rule_targets" and join.parent == "cfr_sections"]
    assert (join.child_columns, join.parent_columns, join.kind) == (("cfr_title", "cfr_part"), ("title", "part"), "scope")
    assert "chapter" in join.reason and "Title 41" in join.reason


@pytest.mark.parametrize(("child", "parent"), [("lobbying_activities", "lobbying_filings"),
                                               ("lobbying_activity_lobbyists", "lobbying_activities")])
def test_the_lobbying_joins_carry_a_full_measurement_not_the_migration_population(child, parent):
    """247 and 522 were the activity tables' first-migration populations, served as baselines on 2026-10-03."""
    (join,) = [join for join in table_joins.JOINS if join.child == child and join.parent == parent]
    assert join.measurement is not None and join.baseline_keys > 100_000 and join.baseline_missing == 0
    assert join.measurement["parent_duplicate_keys"] == 0
