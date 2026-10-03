"""Declared-metadata repairs from the 2026-10-02 blind persona test (round 3, scout A).

Hermetic: the lint rules, the coverage-kind vocabulary and the join declarations,
against the committed ``descriptions.yaml`` and the in-code schema. The live
counterparts (an ``empty`` table publishes no rows, a coverage statement states no
end bound the data has passed) are in ``test_chaos_r3_dictionary_live.py``.
"""

from __future__ import annotations

import re

import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs import table_joins


def _entries() -> dict:
    return {table: dict(entry) for table, entry in dd.load_descriptions().items()}


def _coverage_errors(table: str, coverage: str, measured_on: str) -> list[str]:
    """Run the dictionary check with one table's coverage statement replaced."""
    broken = _entries()
    broken[table]["coverage"] = coverage
    broken[table]["measured_on"] = measured_on
    return [error for error in dd.check_descriptions(dd.expected_schemas(), broken) if error.startswith(f"[{table}]")]


# --------------------------------------------------------------------------- #
# Coverage prose: a measurement date or a count presented as coverage decays.
# --------------------------------------------------------------------------- #
def test_coverage_prose_may_not_restate_its_own_measurement_date():
    """19 of 155 statements on 2026-10-02 named their measured_on inside the sentence; readers took the date as the live end bound."""
    errors = _coverage_errors("comments", "True range. Posted dates run from 1990-01-01 to 2026-09-06.", "2026-09-06")
    assert len(errors) == 1 and "2026-09-06" in errors[0] and "measured_on" in errors[0], errors


def test_a_measurement_date_inside_a_receipt_name_is_a_citation_not_a_claim():
    prose = "Derived. Counts by agency and month. Receipt: fork-execution-2026-09-21/document-derivatives/MANUAL-AUDIT.md."
    assert _coverage_errors("agency_monthly_volume", prose, "2026-09-21") == []


def test_a_scope_date_that_is_not_the_measurement_date_stays_legal():
    assert _coverage_errors("fcc_filings", "Window: filings received on or after 2026-08-24, extended daily.", "2026-09-28") == []


@pytest.mark.parametrize(
    "prose",
    [
        "Window. Every active registration: 792,846 registrations of 787,293 UEIs.",
        "True range. On that day 74 whole days (8,683 documents) matched the API.",
        "True range: reports running about 1,100 to 1,350 a year in recent years.",
    ],
)
def test_coverage_prose_refuses_a_count_in_any_unit(prose):
    """The 2026-09-28 rule caught `rows|records` only; registrations, documents and per-year rates slipped past it."""
    errors = dd.coverage_prose_errors("gao_reports", prose)
    assert errors and all("a count" in error for error in errors), errors


def test_a_row_count_is_still_reported_once():
    """The widened rule must not double-report what the row-count rule already refuses."""
    assert len(dd.coverage_prose_errors("gao_reports", "Window. Measured 319,501 rows on the candidate.")) == 1


def test_a_per_run_cap_passes_only_through_its_declared_exception():
    """A code constant quoted as a rule is not a measurement; it is admitted by exact phrase, per table."""
    assert "at most 1,000 a run" in dd.COVERAGE_PROSE_EXCEPTIONS["committee_meetings"]
    assert dd.coverage_prose_errors("committee_meetings", "Sampled. Details are read, at most 1,000 a run.") == []
    assert dd.coverage_prose_errors("record_issues", "Sampled. Details are read, at most 1,000 a run.")


# --------------------------------------------------------------------------- #
# Data-quality prose: a measurement is fine, an unanchored one is not.
# --------------------------------------------------------------------------- #
def _data_quality_errors(table: str, note: str) -> list[str]:
    broken = _entries()
    broken[table]["data_quality"] = note
    return [error for error in dd.check_descriptions(dd.expected_schemas(), broken) if error.startswith(f"[{table}]")]


def test_a_count_in_data_quality_must_name_when_or_on_what_it_was_measured():
    """`33,373 rows carry a posted_date before 1990` on a table rebuilt daily is a number nothing re-measures."""
    errors = _data_quality_errors("comments", "33,373 rows carry a `posted_date` before 1990. Filter on the date.")
    assert len(errors) == 1 and "33,373 rows" in errors[0], errors


@pytest.mark.parametrize(
    "note",
    [
        "On 2026-09-06 33,373 rows carried a `posted_date` before 1990. Filter on the date.",
        "On snapshot_9b2c770e's inputs 262 lifecycles anchor on such a document.",
        "Generation `55671b43…` holds 18 enrolled-to-introduced comparisons and 3,185 refusals.",
        "Measured on the cold-start run (receipt `d1-measured-run-2026-09-19/`): 3,185 refusals.",
    ],
)
def test_a_dated_or_pinned_measurement_in_data_quality_is_the_honest_form(note):
    assert _data_quality_errors("comments", note) == []


def test_data_quality_refuses_a_publication_status_claim():
    assert _data_quality_errors("comments", "The candidate is not yet published.")


# --------------------------------------------------------------------------- #
# Coverage kinds: a table that publishes no rows says so.
# --------------------------------------------------------------------------- #
def test_empty_is_a_coverage_kind():
    """Six 0-row tables read `Sampled` or `Not a range` on 2026-10-02; a count over them is zero by construction."""
    assert dd.coverage_kind("Empty by owner decision 34: no row is produced.") == "empty"
    assert dd.COVERAGE_KINDS["Empty"] == "empty"


def test_the_committed_dictionary_passes_the_widened_lint():
    assert dd.check_descriptions(dd.expected_schemas(), dd.load_descriptions()) == []


def test_the_kind_is_held_to_the_index_where_the_index_is_read():
    """Zero published rows means Empty and Empty means zero rows; the prose prefix is only the offline fallback."""
    descriptions = dd.load_descriptions()
    rows = {"financial_changes": 0, "bill_summaries": 7, "dockets": 279_406, "not_a_dictionary_table": 0}
    errors = dd.kind_index_errors(descriptions, rows)
    assert len(errors) == 1 and errors[0].startswith("[bill_summaries]") and "7 rows" in errors[0], errors
    assert dd.kind_index_errors(descriptions, {"financial_changes": 0, "dockets": 1}) == []


# --------------------------------------------------------------------------- #
# Join declarations.
# --------------------------------------------------------------------------- #
def test_law_sections_join_is_measured_not_declared_empty():
    """law_sections published 3,852 rows on 2026-10-02 while its join still read `empty … not yet baselined`."""
    (join,) = table_joins.joins_for("law_sections")["outgoing"]
    assert (join["parent"], join["kind"], join["expected_cardinality"]) == ("laws", "complete", "one")
    measurement = join["measurement"]
    assert measurement["keys"] > 0 and measurement["missing"] == 0
    # No NULL law_id, and one parent per key: every section row survives the inner join exactly once.
    assert measurement["child_nonnull_rows"] == measurement["child_input_rows"] == measurement["inner_join_rows"]
    assert measurement["parent_duplicate_keys"] == 0
    assert all(url.startswith("https://data.spicygov.ai/generations/laws/") for url in measurement["child_urls"] + measurement["parent_urls"])


def test_join_reasons_carry_no_operator_detail():
    """An environment-variable name in a public join reason is deployment detail, not a reason (round-3 S2)."""
    offenders = [join.name for join in table_joins.JOINS if re.search(r"\b[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+\b", join.reason)]
    assert offenders == [], offenders


def test_an_empty_join_names_the_decision_that_keeps_it_empty():
    """The four model/pairing tables are empty by owner decision 34, not for want of a key."""
    for child in ("bill_summaries", "diff_summaries", "section_classifications", "financial_changes"):
        (join,) = [join for join in table_joins.JOINS if join.child == child]
        assert join.kind == "empty" and "decision 34" in join.reason and "no rows" in join.reason, join.name
