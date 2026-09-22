"""Hermetic tests for the shared Congress scoping rule.

The boundary cases are the point: a Congress convenes on 3 January of an odd
year, so a run on 1 or 2 January of an odd year still belongs to the previous
one. Getting that wrong points a January run at a Congress with no bills in it.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from spicy_regs.transforms.congress_scope import (
    CONGRESS_BOUNDARY_OVERLAP_DAYS,
    DEFAULT_BILL_TYPES,
    bill_types_from_env,
    congresses_from_env,
    current_congress,
    default_congresses,
    session_of,
    sessions_of,
)


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 9, 19), 119),
        (date(2025, 1, 3), 119),  # the day the 119th convened
        (date(2025, 1, 2), 118),  # one day earlier is still the 118th
        (date(2024, 12, 31), 118),
        (date(2027, 1, 3), 120),
        (date(1789, 6, 1), 1),
    ],
)
def test_current_congress_at_the_boundaries(day, expected):
    assert current_congress(day) == expected


def test_session_of_is_one_in_the_odd_year_and_two_in_the_even():
    assert session_of(119, 2025) == 1
    assert session_of(119, 2026) == 2


def test_session_of_refuses_a_year_outside_the_congress():
    with pytest.raises(ValueError, match="not in Congress"):
        session_of(119, 2027)


def test_sessions_of_reports_only_those_that_have_begun():
    assert sessions_of(119, date(2025, 5, 1)) == (1,)
    assert sessions_of(119, date(2026, 5, 1)) == (1, 2)
    assert sessions_of(120, date(2026, 5, 1)) == ()


def test_scope_defaults_to_the_current_congress_and_all_types(monkeypatch):
    monkeypatch.delenv("BILL_FAMILY_CONGRESSES", raising=False)
    monkeypatch.delenv("BILL_FAMILY_BILL_TYPES", raising=False)
    assert congresses_from_env(today=date(2026, 9, 19)) == (119,)
    assert bill_types_from_env() == DEFAULT_BILL_TYPES
    assert len(DEFAULT_BILL_TYPES) == 8


def test_the_default_names_both_congresses_across_the_boundary():
    """A December correction is still re-read after 3 January: the outgoing Congress stays in scope."""
    assert default_congresses(date(2025, 1, 3)) == (119, 118)  # the day the 119th convened
    assert default_congresses(date(2025, 1, 2)) == (118,)  # still the 118th's own tail
    last = date(2025, 1, 3) + timedelta(days=CONGRESS_BOUNDARY_OVERLAP_DAYS - 1)
    assert default_congresses(last) == (119, 118)  # last day of the window
    assert default_congresses(last + timedelta(days=1)) == (119,)  # the window closes


def test_the_overlap_window_does_not_name_a_congress_before_the_first():
    assert default_congresses(date(1789, 6, 1)) == (1,)


def test_the_overlap_never_overrides_an_explicit_scope(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    assert congresses_from_env(today=date(2025, 1, 15)) == (119,)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119, 118")
    assert congresses_from_env(today=date(2025, 1, 15)) == (119, 118)


@pytest.mark.parametrize("blank", ["", "   ", ","])
def test_a_blank_input_means_the_default_never_nothing(monkeypatch, blank):
    """A cron passes an empty string when no input is given; that is not 'no Congresses'."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", blank)
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", blank)
    assert congresses_from_env(today=date(2026, 9, 19)) == (119,)
    assert bill_types_from_env() == DEFAULT_BILL_TYPES


def test_explicit_scope_is_read_in_order(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118, 119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "HR , s")
    assert congresses_from_env() == (118, 119)
    assert bill_types_from_env() == ("hr", "s")


def test_a_non_numeric_congress_is_refused(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119,next")
    with pytest.raises(ValueError, match="Congress numbers"):
        congresses_from_env()
