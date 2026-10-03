"""Declared-metadata repairs from the 2026-10-03 blind persona test (round 4, scout A).

Hermetic: the interim-note expiry, against the committed ``descriptions.yaml``
and the in-code schema.
"""

from __future__ import annotations

import pytest

from spicy_regs import data_dictionary as dd

VENDORED = "0.53.0+laws.f8431033f626"
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
def test_each_stand_in_for_an_unreleased_spicy_docs_fix_carries_its_expiry(table):
    """W1 (stage), W3 (latest action), B2 (margin notes) and B3 (party totals) are fixed only on a spicy-docs branch."""
    note = dd.load_descriptions()[table].get("data_quality") or ""
    stated = dd.INTERIM_MARKER.findall(" ".join(note.split()))
    assert stated and all(version == dd.installed_spicy_docs() for version in stated), (table, stated)
