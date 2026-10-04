"""Rulemaking lifecycles: pairing, withdrawals, censoring, Agenda signals, strata, Aalen-Johansen and re-derivation."""

from __future__ import annotations

import json
import math
import tomllib
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs import mcp_server
from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.ontology.federal_register import document_form
from spicy_regs.ontology.rins import docket_side_holder, specific_rin_holders
from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline
from spicy_regs.transforms.build_agency_lifecycle_stats import (
    MIN_RULES,
    aalen_johansen,
    build_agency_lifecycle_stats,
    quantile,
)
from spicy_regs.transforms.build_lifecycles import (
    EVENTS_OUTPUT,
    LIFECYCLE_SCHEMA,
    LIFECYCLES_OUTPUT,
    ROUTINE_FAMILIES,
    AgendaEntry,
    Event,
    Lifecycle,
    agenda_signal,
    build_lifecycles,
    lifecycle,
    routine_family,
)
from spicy_regs.transforms.build_proceedings import build_proceedings
from spicy_regs.transforms.build_regulatory_agenda import build_regulatory_agenda
from spicy_regs.transforms.build_rule_targets import build_rule_targets

REPO_ROOT = Path(__file__).resolve().parents[1]
CENSOR = date(2026, 9, 25)
REGISTER, REGULATIONS_GOV = "federal_register", "regulations_gov"


def _event(
    day: str,
    stage: str,
    document: str,
    source: str = REGISTER,
    joined_by: str = "docket",
    dated_by: str = REGISTER,
    title: str | None = None,
) -> Event:
    return Event(
        date.fromisoformat(day),
        document,
        stage,
        source,
        document,
        joined_by,
        document_form(stage, title),
        dated_by,
        title,
    )


# --- pairing (decision 54) ---------------------------------------------------------------


def test_a_final_strictly_after_the_proposal_pairs_with_the_earliest_such_final():
    result = lifecycle(
        [
            _event("2020-03-01", "proposed", "P2"),
            _event("2020-01-01", "proposed", "P1"),
            _event("2021-06-01", "final", "F2"),
            _event("2021-01-01", "final", "F1"),
        ],
        CENSOR,
    )
    assert result.kind == "finalized"
    assert (result.outcome, result.duration_days, result.finals_before_proposal) == ("final", 366, 0)
    assert result.anchors() == {"P1": "proposal", "F1": "final"}


def test_a_final_the_register_dates_on_the_proposals_day_is_a_companion_and_pairs_nothing():
    result = lifecycle([_event("2020-01-01", "proposed", "P"), _event("2020-01-01", "final", "DFR")], CENSOR)
    assert (result.kind, result.anchors()) == ("companion", {"P": "proposal", "DFR": "final"})
    assert (result.outcome, result.duration_days) == (None, None)


@pytest.mark.parametrize(
    ("proposal_dated_by", "final_dated_by", "kind", "anchors", "outcome"),
    [
        (REGISTER, REGISTER, "companion", {"P": "proposal", "SAME": "final"}, None),
        # Decision 54b: an upload-dated "final" beside a Register proposal pairs nothing and makes nothing.
        (REGISTER, REGULATIONS_GOV, "open", {"P": "proposal"}, "censored"),
        # Decision 54c: an upload-dated proposal with a final that day is an upload pair, outside survival.
        (REGULATIONS_GOV, REGULATIONS_GOV, "upload_pair", {"P": "proposal", "SAME": "final"}, None),
        (REGULATIONS_GOV, REGISTER, "upload_pair", {"P": "proposal", "SAME": "final"}, None),
    ],
)
def test_who_dates_a_same_day_pair_decides_its_kind(proposal_dated_by, final_dated_by, kind, anchors, outcome):
    events = [
        _event("2020-01-01", "proposed", "P", dated_by=proposal_dated_by),
        _event("2020-01-01", "final", "SAME", dated_by=final_dated_by),
        # A withdrawal after the day withdraws no companion or upload pair, and does an open proposal.
        _event("2020-06-01", "withdrawn", "W"),
    ]
    alone = lifecycle(events, CENSOR)
    if kind == "open":
        kind, anchors, outcome = "withdrawn", {**anchors, "W": "withdrawal"}, "withdrawn"
    assert (alone.kind, alone.anchors(), alone.outcome) == (kind, anchors, outcome)
    assert alone.duration_days is None if outcome is None else alone.duration_days == 152
    # A real later final pairs whoever dated the same-day documents.
    later = lifecycle([*events, _event("2021-01-01", "final", "F")], CENSOR)
    assert (later.kind, later.anchors(), later.duration_days) == ("finalized", {"P": "proposal", "F": "final"}, 366)


def test_a_same_day_final_and_a_later_final_pair_with_the_later_one():
    result = lifecycle(
        [
            _event("2020-01-01", "proposed", "P"),
            _event("2020-01-01", "final", "DFR"),
            _event("2020-09-01", "final", "F"),
        ],
        CENSOR,
    )
    assert (result.kind, result.anchors(), result.duration_days) == ("finalized", {"P": "proposal", "F": "final"}, 244)


#: Decision 54d's cohort, by example: CMS-2026-2377-0001, an unnumbered "display" upload posted on the
#: public-inspection day, and 2026-14327, the Register's proposal published two days later.
UPLOAD, REGISTER_PROPOSAL, FINAL = "CMS-2026-0001-0001", "2026-14327@2026-01-16", "2026-20000@2026-05-04"


def _upload_proposal(day: str = "2026-01-14", document: str = UPLOAD) -> Event:
    return _event(day, "proposed", document, REGULATIONS_GOV, dated_by=REGULATIONS_GOV)


def _proposal_of(result: Lifecycle) -> str:
    assert result.proposal is not None
    return result.proposal.document_id


def test_a_register_dated_proposal_is_time_zero_and_the_earlier_upload_anchors_nothing():
    """Decision 54d: the display upload is dated by its posting, the Register's copy by its publication."""
    result = lifecycle(
        [
            _upload_proposal(),
            _event("2026-01-16", "proposed", REGISTER_PROPOSAL, joined_by="fr_copy"),
            _event("2026-05-04", "final", FINAL),
        ],
        CENSOR,
    )
    assert result.anchors() == {REGISTER_PROPOSAL: "proposal", FINAL: "final"}
    assert (result.kind, result.duration_days) == ("finalized", 108)


def test_the_register_dates_time_zero_however_long_after_the_upload():
    # No window: an upload day says when a document was posted, not when the rule was proposed.
    result = lifecycle(
        [
            _upload_proposal("2024-01-14"),
            _event("2026-01-16", "proposed", REGISTER_PROPOSAL),
            _event("2026-05-04", "final", FINAL),
        ],
        CENSOR,
    )
    assert (_proposal_of(result), result.duration_days) == (REGISTER_PROPOSAL, 108)


def test_an_upload_dated_proposal_anchors_only_where_the_register_dates_none():
    result = lifecycle([_upload_proposal(), _event("2026-05-04", "final", FINAL)], CENSOR)
    assert (_proposal_of(result), result.kind, result.duration_days) == (UPLOAD, "finalized", 110)


def test_a_same_day_register_proposal_wins_by_rule_not_by_id_order():
    # A legacy-numbered Register id sorts after the upload's; the rule, not the sort, picks the Register's.
    register = "R1-2026-10679@2026-01-14"
    result = lifecycle([_upload_proposal(), _event("2026-01-14", "proposed", register)], CENSOR)
    assert _proposal_of(result) == register


def test_the_earlier_of_two_register_proposals_in_the_window_anchors():
    result = lifecycle(
        [
            _upload_proposal(),
            _event("2026-01-20", "proposed", "2026-15000@2026-01-20"),
            _event("2026-01-16", "proposed", REGISTER_PROPOSAL),
        ],
        CENSOR,
    )
    assert _proposal_of(result) == REGISTER_PROPOSAL


@pytest.mark.parametrize("final_day", ["2026-01-15", "2026-01-16"])
def test_a_final_between_the_upload_and_the_registers_proposal_keeps_the_upload_anchored(final_day):
    """The upload's proposal was finalized first (on the Register proposal's own day included): the Register's is a later one."""
    result = lifecycle(
        [
            _upload_proposal(),
            _event(final_day, "final", "F0"),
            _event("2026-01-16", "proposed", REGISTER_PROPOSAL),
            _event("2026-05-04", "final", FINAL),
        ],
        CENSOR,
    )
    assert result.anchors() == {UPLOAD: "proposal", "F0": "final"}
    assert (result.kind, result.finals_before_proposal, result.duration_days) == (
        "finalized",
        0,
        date.fromisoformat(final_day).day - 14,
    )


def test_a_final_on_the_uploads_own_day_is_not_between_and_an_upload_pair_still_yields():
    # A same-day final is the upload-pair shape (54c), not a final of the upload's proposal; the Register's proposal anchors.
    result = lifecycle(
        [
            _upload_proposal(),
            _event("2026-01-14", "final", "F0", REGULATIONS_GOV, dated_by=REGULATIONS_GOV),
            _event("2026-01-16", "proposed", REGISTER_PROPOSAL),
            _event("2026-05-04", "final", FINAL),
        ],
        CENSOR,
    )
    assert (_proposal_of(result), result.kind, result.finals_before_proposal) == (REGISTER_PROPOSAL, "finalized", 1)


@pytest.mark.parametrize(
    "title",
    [
        "Payment Policies; Extension of Comment Period",
        "Payment Policies; Reopening of Comment Period",
        "Payment Policies; Correction",
    ],
)
def test_a_register_extension_or_correction_presupposes_the_published_proposal_and_leaves_the_upload_anchored(title):
    result = lifecycle(
        [
            _upload_proposal(),
            _event("2026-02-20", "proposed", REGISTER_PROPOSAL, title=title),
            _event("2026-05-04", "final", FINAL),
        ],
        CENSOR,
    )
    assert (_proposal_of(result), result.duration_days) == (UPLOAD, 110)
    # The same document titled as a proposal is the publication, and anchors.
    plain = lifecycle(
        [
            _upload_proposal(),
            _event("2026-02-20", "proposed", REGISTER_PROPOSAL, title="Payment Policies"),
            _event("2026-05-04", "final", FINAL),
        ],
        CENSOR,
    )
    assert (_proposal_of(plain), plain.duration_days) == (REGISTER_PROPOSAL, 73)


def test_a_final_before_the_proposal_is_counted_not_paired():
    result = lifecycle([_event("2019-01-01", "final", "F0"), _event("2020-01-01", "proposed", "P")], CENSOR)
    assert (result.kind, result.final, result.finals_before_proposal) == ("open", None, 1)
    paired = lifecycle(
        [
            _event("2019-01-01", "final", "F0"),
            _event("2020-01-01", "proposed", "P"),
            _event("2020-05-01", "final", "F"),
        ],
        CENSOR,
    )
    assert (paired.kind, paired.anchors(), paired.finals_before_proposal) == (
        "finalized",
        {"P": "proposal", "F": "final"},
        1,
    )


def test_a_proceeding_with_no_proposal_is_anchored_by_its_first_final_or_by_nothing():
    only_finals = lifecycle([_event("2021-01-01", "final", "F2"), _event("2020-01-01", "final", "F1")], CENSOR)
    assert (only_finals.kind, only_finals.anchors(), only_finals.outcome) == (
        "final_without_observed_proposal",
        {"F1": "final"},
        None,
    )
    assert lifecycle([_event("2020-01-01", "supplemental", "S")], CENSOR).kind == "no_anchor"
    assert lifecycle([], CENSOR).kind == "no_anchor"


# --- withdrawals and censoring ------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "kind"),
    [(REGISTER, "withdrawn"), ("unified_agenda", "withdrawn"), (REGULATIONS_GOV, "open")],
)
def test_withdrawals_count_only_from_the_register_or_the_agenda(source, kind):
    result = lifecycle([_event("2020-01-01", "proposed", "P"), _event("2020-07-01", "withdrawn", "W", source)], CENSOR)
    assert result.kind == kind
    if kind == "withdrawn":
        assert (result.anchors(), result.outcome, result.duration_days) == (
            {"P": "proposal", "W": "withdrawal"},
            "withdrawn",
            182,
        )


def test_a_withdrawal_before_the_proposal_or_after_a_later_final_withdraws_nothing():
    assert (
        lifecycle([_event("2019-01-01", "withdrawn", "W"), _event("2020-01-01", "proposed", "P")], CENSOR).kind
        == "open"
    )
    finalized = lifecycle(
        [
            _event("2020-01-01", "proposed", "P"),
            _event("2020-02-01", "withdrawn", "W"),
            _event("2021-01-01", "final", "F"),
        ],
        CENSOR,
    )
    assert (finalized.kind, finalized.withdrawal) == ("finalized", None)


def test_an_open_proposal_is_right_censored_at_the_censor_date():
    result = lifecycle([_event("2026-01-01", "proposed", "P")], CENSOR)
    assert (result.kind, result.outcome, result.duration_days) == ("open", "censored", (CENSOR - date(2026, 1, 1)).days)


# --- open_signal ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stage", "actions", "edition", "signal"),
    [
        ("Completed Actions", ["nprm", "withdrawn"], "202410", "agenda_completed_withdrawn"),
        # A completed entry stating both a final rule and a withdrawal reads as withdrawn.
        ("Completed Actions", ["nprm", "final rule", "withdrawn"], "202510", "agenda_completed_withdrawn"),
        ("Completed Actions", ["nprm", "final rule"], "202510", "agenda_completed_final"),
        ("Completed Actions", ["final action"], "202510", "agenda_completed_final"),
        ("Completed Actions", ["nprm"], "202510", "agenda_completed_other"),
        ("Long-Term Actions", ["nprm"], "201910", "agenda_long_term"),
        ("Proposed Rule Stage", ["nprm"], "202510", "agenda_active"),
        ("Final Rule Stage", ["nprm"], "202404", "agenda_dropped_off"),
    ],
)
def test_each_agenda_signal(stage, actions, edition, signal):
    assert agenda_signal(stage, actions, edition, "202510") == signal


def _entry(rin: str, edition: str, signal: str) -> AgendaEntry:
    return AgendaEntry(rin, edition, signal, None, None, None)


def test_the_latest_edition_decides_then_the_strongest_signal():
    older_but_stronger = _entry("2060-AA01", "202404", "agenda_completed_withdrawn")
    newer = _entry("2060-AA02", "202510", "agenda_active")
    assert max([older_but_stronger, newer], key=AgendaEntry.rank) is newer
    # Within one edition the strongest signal wins, whichever RIN sorts last.
    strong = _entry("2060-AA01", "202510", "agenda_completed_final")
    weak = _entry("2060-AA02", "202510", "agenda_active")
    assert max([strong, weak], key=AgendaEntry.rank) is strong


# --- routine families (decision 55) --------------------------------------------------------


@pytest.mark.parametrize(
    ("agency", "title", "family"),
    [
        ("FAA", "Airworthiness Directives; The Boeing Company Airplanes", "airworthiness_directive"),
        ("FAA", "Amendment of Class E Airspace; Walden, CO", "airspace"),
        ("FAA", "Standard Instrument Approach Procedures; Miscellaneous Amendments", "airspace"),
        ("EPA", "Approval and Promulgation of Implementation Plans; Ohio; Regional Haze", "state_air_plan"),
        ("EPA", "Air Plan Approval; Texas; Infrastructure for the 2015 Ozone NAAQS", "state_air_plan"),
        ("EPA", "Fluopyram; Pesticide Tolerances", "pesticide_tolerance"),
        # A petition receipt is the tolerance pathway at time zero (decision 55a): its final is "...; Pesticide Tolerances".
        (
            "EPA",
            "Receipt of Several Pesticide Petitions Filed for Residues of Pesticide Chemicals in or on Various Commodities",
            "pesticide_tolerance",
        ),
        (
            "EPA",
            "Notice of Filing of Several Pesticide Petitions Filed for Residues of Pesticide Chemicals",
            "pesticide_tolerance",
        ),
        ("USCG", "Safety Zone; Fireworks Display, Lake Michigan, Chicago, IL", "coast_guard_local"),
        ("USCG", "Drawbridge Operation Regulation; Hackensack River, NJ", "coast_guard_local"),
        ("USCG", "Anchorage Regulations; Port of New York", "coast_guard_local"),
        ("USCG", "Regattas and Marine Parades; Great Lakes Annual Marine Events", "coast_guard_local"),
    ],
)
def test_each_routine_family(agency, title, family):
    assert routine_family(agency, title) == family


@pytest.mark.parametrize(
    ("agency", "title"),
    [
        ("FDA", "Tolerances for Residues of New Animal Drugs in Food"),
        ("APHIS", "Lacey Act Implementation Plan; De Minimis Exception"),
        ("EPA", "National Emission Standards for Hazardous Air Pollutants"),
        # EPA's own plans are not a state's.
        ("EPA", "Approval and Promulgation of Federal Implementation Plan for Oil and Natural Gas Well Production"),
        ("EPA", "Revisions to the CAIR Federal Implementation Plan and CAMR Proposed Federal Plan"),
        # Flight prohibitions and special federal aviation regulations are not airspace designations.
        ("FAA", "Prohibition Against Certain Flights Within the Territory and Airspace of Iraq"),
        ("FAA", "Airspace and Flight Operations Requirements for the 2002 Winter Olympic Games, Salt Lake City, UT"),
        ("FAA", "Special Federal Aviation Regulation No. 60; Airspace Restrictions"),
        ("FAA", "New York Class B Airspace Hudson River and East River Exclusion Special Flight Rules Area"),
    ],
)
def test_a_phrase_under_another_agency_or_with_a_keep_out_phrase_is_no_family(agency, title):
    assert routine_family(agency, title) is None


def test_a_keep_out_phrase_in_the_title_vetoes_and_the_families_are_stated_once():
    assert (
        routine_family("EPA", "Approval and Promulgation of Implementation Plans; Arizona; Federal Implementation Plan")
        is None
    )
    assert {family for family, *_ in ROUTINE_FAMILIES} == {
        "airworthiness_directive",
        "airspace",
        "state_air_plan",
        "pesticide_tolerance",
        "coast_guard_local",
    }


@pytest.mark.parametrize(
    ("stage", "title", "form"),
    [
        ("final", "Air Plan Approval; Ohio", "final"),
        ("final", "Hazardous Waste; Interim Final Rule", "interim_final"),
        ("final", "Visas; Interim rule with request for comments", "interim_final"),
        ("final", "Air Plan Approval; Direct Final Rule", "direct_final"),
        ("final", "Airworthiness Directives; Correction", "correction"),
        ("final", "Medicare Program; Correcting Amendment", "correction"),
        ("final", "Technical Corrections", "correction"),
        ("final", "Pesticide Tolerance Technical Correction", "correction"),
        ("final", "Correction of Effective Date Under Congressional Review Act (CRA)", "correction"),
        ("final", "U.S. DOT/FAA - Correction", "correction"),
        ("proposed", "Standards; Correction and Extension of Comment Period", "correction"),
        ("final", "Hazardous Waste Management System; Land Disposal Restrictions Correction", "correction"),
        ("proposed", "Special Areas; Roadless Area Conservation; Proposed Correction", "correction"),
        ("final", "Drinking Water Regulations: Public Notification Rule; Correction [W-99-10-I-A-2]", "correction"),
        ("final", "U.S. DOT/FAA - Correction -Airworthiness Directives; McDonnell Douglas Model MD-11", "correction"),
        (
            "final",
            "Vessel Traffic Service Lower Mississippi River; Correction (Federal Register Publication)",
            "correction",
        ),
        ("final", "Technical Corrections Relating to Issuance of Notices To Appear", "correction"),
        # Rules whose titles say "correction" without being corrections.
        ("final", "Standards for Correctional Agencies", "final"),
        ("final", "Voluntary Fiduciary Correction Program", "final"),
        ("proposed", "Correction Action for SWMUs", "proposed"),
        ("final", "Medical Devices; Reports of Corrections and Removals", "final"),
        ("final", "Air Plan Approval; Alabama; Determinations Concerning Need for Error Correction", "final"),
        ("final", "Export Controls; Updates and Corrections; and Export Controls on Semiconductor Items", "final"),
        ("proposed", "Emission Standards; Extension of Comment Period", "comment_period"),
        ("proposed", "Energy Conservation; Advance Notice of Proposed Rulemaking", "advance_proposed"),
        ("proposed", "Emission Standards", "proposed"),
        ("withdrawn", "Emission Standards; Withdrawal", "withdrawn"),
    ],
)
def test_the_document_form_refines_the_stage_by_title_markers(stage, title, form):
    assert document_form(stage, title) == form


def test_a_docket_holds_a_rule_targets_rin_only_through_its_own_evidence():
    assert docket_side_holder("docket_rin") == "docket_rin"
    assert docket_side_holder("document_rin") == "document_rin"
    assert docket_side_holder("document_fr_doc") == "rule_targets:document_fr_doc"
    assert docket_side_holder("fr_cfr_ref") is None
    assert docket_side_holder("document_rin", feed_docket=True) is None
    assert docket_side_holder("docket_rin", feed_docket=True) == "docket_rin"


def test_a_rin_is_specific_to_the_one_proceeding_that_holds_it():
    held = {"A": {"2060-AA01", "2060-AA09", "0648-XA01"}, "B": {"2060-AA09"}}
    assert specific_rin_holders(held) == {"2060-AA01": "A"}


# --- Aalen-Johansen, withdrawal competing (decision 54a) ------------------------------------

#: Ten rules: finals on days 3, 5, 8, 12 and 15; withdrawn on 6 and 10; censored on 5, 9 and 12.
TIMES = [3, 5, 5, 6, 8, 9, 10, 12, 12, 15]
OUTCOMES = ["final", "final", "censored", "withdrawn", "final", "censored", "withdrawn", "final", "censored", "final"]


def test_aalen_johansen_matches_a_hand_computed_example_and_r():
    """By hand, with P the chance a proposal is still pending and F the incidence of a final.

    Day 3 (10 at risk, 1 final): F = 1/10, P = 9/10. Day 5 (9 at risk, 1 final, 1 censored that
    day): F = 1/10 + 9/10 · 1/9 = 1/5, P = 4/5. Day 6 (7, 1 withdrawn): P = 4/5 · 6/7 = 24/35, F
    stays. Day 8 (6, 1 final): F = 1/5 + 24/35 · 1/6 = 11/35. Day 10 (4, 1 withdrawn): P = 3/7.
    Day 12 (3, 1 final): F = 11/35 + 3/7 · 1/3 = 16/35. Day 15 (1, 1 final): F = 26/35. On day 3 the
    standard error is the binomial √(0.1 · 0.9 / 10). The standard errors and the log-log band
    are R survival 3.8.6's multi-state survfit(Surv(t, s) ~ 1, conf.type = "log-log") on these rules.
    """
    steps = aalen_johansen(TIMES, OUTCOMES)
    assert [(s.day, s.at_risk, s.finals) for s in steps] == [(3, 10, 1), (5, 9, 1), (8, 6, 1), (12, 3, 1), (15, 1, 1)]
    assert [s.incidence for s in steps] == pytest.approx([1 / 10, 1 / 5, 11 / 35, 16 / 35, 26 / 35])
    assert steps[0].std_err == pytest.approx(math.sqrt(0.1 * 0.9 / 10))
    assert [s.std_err for s in steps] == pytest.approx([0.09486833, 0.12649111, 0.15149402, 0.17427066, 0.15702222])
    assert [(s.lower, s.upper) for s in steps] == [
        pytest.approx(pair, abs=1e-7)
        for pair in [
            (0.005723456, 0.3581275),
            (0.030909024, 0.4747147),
            (0.072939664, 0.5994748),
            (0.130917307, 0.7398149),
            (0.301832061, 0.9288927),
        ]
    ]
    # The median is the first day F reaches 1/2 (15); the band's upper edge first reaches it on
    # 8 and its lower edge never does, so the interval is open above.
    assert quantile(steps, 0.5) == (15, 8, None)
    assert quantile(steps, 0.25) == (8, 3, 15)


def test_a_withdrawal_competes_and_is_neither_a_final_nor_censoring():
    """Withdrawn on 5 and 6, final on 10 and 20: F(10) = 1/4 and F(20) = 1/2, so the median is 20.

    Counting withdrawals as censoring makes it 10 (every rule left at 10 is final), and counting
    them as finals makes it 6. The median lands exactly on 1/2, so a strict comparison misses it.
    """
    steps = aalen_johansen([5, 6, 10, 20], ["withdrawn", "withdrawn", "final", "final"])
    assert [(s.day, s.incidence) for s in steps] == [(10, pytest.approx(0.25)), (20, pytest.approx(0.5))]
    assert quantile(steps, 0.5)[0] == 20
    with pytest.raises(ValueError, match="unknown lifecycle outcome"):
        aalen_johansen([5], ["finalized"])


def test_a_quantile_the_incidence_never_reaches_is_none():
    steps = aalen_johansen([10, 20, 30, 40], ["final", "censored", "censored", "withdrawn"])
    assert steps[-1].incidence == pytest.approx(0.25)
    assert quantile(steps, 0.5)[0] is None


def test_a_curve_that_reaches_one_has_no_variance_left_and_a_real_small_one_is_kept():
    """R reports a standard error of exactly 0, and the band [F, F], once every rule has finalized;
    the influence sums get there only to ~1e-19. A real standard error near 1e-4 must survive."""
    reaches_one = aalen_johansen(list(range(1, 31)), ["final"] * 30)[-1]
    assert (reaches_one.std_err, reaches_one.lower, reaches_one.upper) == (
        0.0,
        reaches_one.incidence,
        reaches_one.incidence,
    )
    assert reaches_one.incidence == pytest.approx(1.0)
    # One final among 10,000 rules: F = 1e-4, se = √(F(1 - F)/n) ≈ 1e-4.
    small = aalen_johansen([1] + [2] * 9_999, ["final"] + ["censored"] * 9_999)[0]
    assert small.std_err == pytest.approx(math.sqrt(1e-4 * (1 - 1e-4) / 10_000))
    assert small.lower < small.incidence < small.upper


def _lifecycles_file(root: Path, rows: list[dict]) -> None:
    full = [{name: row.get(name) for name in LIFECYCLE_SCHEMA.names} for row in rows]
    pq.write_table(pa.Table.from_pylist(full, schema=LIFECYCLE_SCHEMA), root / LIFECYCLES_OUTPUT)


def _rows(agency: str, outcomes: list[tuple[int, str]], family: str | None = None) -> list[dict]:
    return [
        {
            "proceeding_id": f"{agency}{n}",
            "agency_code": agency,
            "outcome": outcome,
            "duration_days": days,
            "routine_family": family,
            "censor_date": CENSOR,
        }
        for n, (days, outcome) in enumerate(outcomes)
    ]


def test_the_stats_hold_withdrawn_proposals_in_the_denominator_as_never_final(tmp_path):
    pattern = [(5, "withdrawn"), (6, "withdrawn"), (10, "final"), (20, "final")] * 8
    _lifecycles_file(tmp_path, _rows("AAA", pattern) + _rows("FAA", [(30, "final")] * MIN_RULES, "airspace"))
    cells = {
        (r["agency_code"], r["stratum"]): r for r in pq.read_table(build_agency_lifecycle_stats(tmp_path)).to_pylist()
    }
    cell = cells[("AAA", "all")]
    assert (cell["rules"], cell["finals"], cell["withdrawals"], cell["censored"]) == (32, 16, 16, 0)
    assert (cell["q1_days"], cell["median_days"], cell["q3_days"]) == (10, 20, None)
    # A routine rule is in its agency's routine cell and its family's.
    assert cells[("FAA", "routine")]["rules"] == cells[("FAA", "airspace")]["rules"] == MIN_RULES
    assert cells[(None, "routine")]["median_days"] == 30
    assert b"Aalen-Johansen" in pq.read_schema(tmp_path / "agency_lifecycle_stats.parquet").metadata[b"estimator"]


def test_a_cell_under_thirty_rules_keeps_its_row_suppressed_with_no_estimates(tmp_path):
    rows = _rows("AAA", [(n, "final") for n in range(1, MIN_RULES + 1)]) + _rows(
        "BBB", [(n, "final") for n in range(1, MIN_RULES)]
    )
    rows.append({"proceeding_id": "C", "agency_code": "AAA", "kind": "companion", "censor_date": CENSOR})
    rows.append({"proceeding_id": "U", "agency_code": "AAA", "kind": "upload_pair", "censor_date": CENSOR})
    _lifecycles_file(tmp_path, rows)
    stats = pq.read_table(build_agency_lifecycle_stats(tmp_path)).to_pylist()
    cells = {(row["agency_code"], row["stratum"]): row for row in stats}
    assert set(cells) == {(agency, stratum) for agency in (None, "AAA", "BBB") for stratum in ("all", "non_routine")}
    kept, suppressed = cells[("AAA", "all")], cells[("BBB", "all")]
    assert (kept["rules"], kept["suppressed"], kept["median_days"]) == (MIN_RULES, False, 15)
    assert (suppressed["rules"], suppressed["suppressed"]) == (MIN_RULES - 1, True)
    assert all(suppressed[column] is None for column in suppressed if column.endswith("_days"))
    assert cells[(None, "all")]["rules"] == 2 * MIN_RULES - 1  # the companion and upload pair stay out


# --- the stage, end to end ------------------------------------------------------------------

AGENCY = "EPA"


def _write(root: Path, name: str, rows: list[dict]) -> None:
    columns = sorted({column for row in rows for column in row}) or ["id"]
    write_parquet_rows(root / f"{name}.parquet", columns=columns, rows=rows)


def _document(document_id: str, docket: str, kind: str, posted: str | None, title: str = "", **extra) -> dict:
    return {
        "document_id": document_id,
        "docket_id": docket,
        "document_type": kind,
        "title": title or f"{kind} {document_id}",
        "agency_code": extra.pop("agency_code", AGENCY),
        "posted_date": posted,
        "additional_rins": "[]",
        **extra,
    }


def _register(number: str, published: str, kind: str, title: str, rins: list[str] | None = None) -> dict:
    return {
        "document_number": number,
        "publication_date": published,
        "document_type": kind,
        "title": title,
        "regulation_id_numbers_json": json.dumps(rins or []),
        "agencies_json": "[]",
    }


def _agenda(rin: str, edition: str, stage: str, timetable: list[tuple[str, str]], **extra) -> dict:
    return {
        "rin": rin,
        "agenda_edition": edition,
        "rule_stage": stage,
        "timetable_json": json.dumps(
            [{"action": action, "date": day, "fr_citation": None} for action, day in timetable]
        ),
        **extra,
    }


def _docket(
    docket: str,
    rin: str | None = None,
    agency: str = AGENCY,
    title: str = "",
    modified: str = "",
    docket_type: str = "Rulemaking",
) -> dict:
    return {
        "docket_id": docket,
        "rin": rin,
        "docket_type": docket_type,
        "title": title or f"Docket {docket}",
        "agency_code": agency,
        "modify_date": modified or None,
    }


def _build_inputs(root: Path) -> None:
    """Every docket a separate rulemaking, each exercising one rule; built through the real upstream stages."""
    _write(
        root,
        "dockets",
        [
            # A: a Regulations.gov copy of a Register proposal, then a Regulations.gov final; specific RIN.
            _docket("EPA-HQ-OAR-2020-0001", "2060-AA01"),
            # B: a Register proposal and a Register final the same day (companion).
            _docket("EPA-HQ-OAR-2020-0002"),
            # C: an open proposal whose specific RIN the Agenda completed as withdrawn.
            _docket("EPA-HQ-OAR-2020-0003", "2060-AA03"),
            # D: an open proposal a Regulations.gov-typed withdrawal cannot withdraw.
            _docket("EPA-HQ-OAR-2020-0004"),
            # E: a Register withdrawal, through the Register's own docket link.
            _docket("EPA-HQ-OAR-2020-0005"),
            # F: a final before the proposal.
            _docket("EPA-HQ-OAR-2020-0006"),
            # G: only a Regulations.gov copy of a Register final that links another docket (L) holds.
            _docket("EPA-HQ-OAR-2020-0007"),
            _docket("EPA-HQ-OAR-2020-0012"),
            # H1, H2: a RIN two dockets hold is nobody's specific RIN.
            _docket("EPA-HQ-OAR-2020-0008", "2060-AA09"),
            _docket("EPA-HQ-OAR-2020-0009", "2060-AA09"),
            # X: an X-pattern code is never specific.
            _docket("NOAA-NMFS-2020-0010", "0648-XA01"),
            # N: a docket with no rule document at all; with no anchor, its own title names the family.
            _docket("EPA-HQ-OAR-2020-0011", "2060-AA11", title="Air Plan Approval; Iowa; No Rule Document Yet"),
            # S: a Register final attached by the docket's specific RIN, which the Agenda never lists.
            _docket("EPA-HQ-OAR-2020-0013", "2060-AA13"),
            # B2: a proposal and a "final" both dated by their Regulations.gov upload the same day.
            _docket("EPA-HQ-OAR-2020-0014"),
            # B4: an upload pair uploaded before 2008; B5: an upload-dated proposal and a Register final that day.
            _docket("EPA-HQ-OAR-2020-0022"),
            _docket("EPA-HQ-OAR-2020-0023"),
            # B3: a Register proposal, an upload-dated "final" that day, and a later final.
            _docket("EPA-HQ-OAR-2020-0015"),
            # P1, P2: proposals on either side of 2008-01-01.
            _docket("EPA-HQ-OAR-2020-0016"),
            _docket("EPA-HQ-OAR-2020-0017"),
            # U: an undated proposal; V: one dated after the run's day.
            _docket("EPA-HQ-OAR-2020-0018"),
            _docket("EPA-HQ-OAR-2020-0019"),
            # T: a Regulations.gov copy of a Register row that states no type (decision 60a).
            _docket("EPA-HQ-OAR-2020-0020"),
            # R: an AD proposal in a docket whose latest title names airspace.
            _docket(
                "FAA-2020-0021", agency="FAA", title="Establishment of Class E Airspace; Mesa", modified="2025-01-01"
            ),
            # D1: an unnumbered "display" upload of a proposal, the Register's numbered copy two days later (54d).
            _docket("CMS-2021-0001", agency="CMS"),
            # Q1: an open pesticide-petition receipt whose docket title names no tolerance (decision 55a).
            _docket("EPA-HQ-OPP-2021-0002", title="Petition docket"),
            # Q2: a generically titled proposal whose final is a state air plan: no family at time zero (55a).
            _docket("EPA-HQ-OAR-2021-0003"),
        ],
    )
    documents = [
        _document(
            "EPA-HQ-OAR-2020-0001-0001", "EPA-HQ-OAR-2020-0001", "Proposed Rule", "2020-03-05", fr_doc_num="2020-01000"
        ),
        _document("EPA-HQ-OAR-2020-0001-0002", "EPA-HQ-OAR-2020-0001", "Rule", "2021-01-10"),
        _document("EPA-HQ-OAR-2020-0003-0001", "EPA-HQ-OAR-2020-0003", "Proposed Rule", "2019-01-01"),
        _document("EPA-HQ-OAR-2020-0004-0001", "EPA-HQ-OAR-2020-0004", "Proposed Rule", "2019-01-01"),
        _document(
            "EPA-HQ-OAR-2020-0004-0002", "EPA-HQ-OAR-2020-0004", "Proposed Rule", "2019-06-01", "Withdrawal of proposal"
        ),
        _document("EPA-HQ-OAR-2020-0005-0001", "EPA-HQ-OAR-2020-0005", "Proposed Rule", "2019-02-01"),
        _document("EPA-HQ-OAR-2020-0006-0001", "EPA-HQ-OAR-2020-0006", "Rule", "2018-01-01"),
        _document("EPA-HQ-OAR-2020-0006-0002", "EPA-HQ-OAR-2020-0006", "Proposed Rule", "2018-06-01"),
        _document("EPA-HQ-OAR-2020-0007-0001", "EPA-HQ-OAR-2020-0007", "Rule", "2021-02-10", fr_doc_num="2021-03000"),
        _document("EPA-HQ-OAR-2020-0008-0001", "EPA-HQ-OAR-2020-0008", "Proposed Rule", "2022-01-03"),
        _document("EPA-HQ-OAR-2020-0009-0001", "EPA-HQ-OAR-2020-0009", "Proposed Rule", "2022-01-04"),
        _document("NOAA-NMFS-2020-0010-0001", "NOAA-NMFS-2020-0010", "Proposed Rule", "2022-01-05", agency_code="NOAA"),
        _document("EPA-HQ-OAR-2020-0013-0001", "EPA-HQ-OAR-2020-0013", "Proposed Rule", "2020-02-01"),
        _document("EPA-HQ-OAR-2020-0014-0001", "EPA-HQ-OAR-2020-0014", "Proposed Rule", "2020-05-01"),
        _document("EPA-HQ-OAR-2020-0014-0002", "EPA-HQ-OAR-2020-0014", "Rule", "2020-05-01"),
        _document("EPA-HQ-OAR-2020-0022-0001", "EPA-HQ-OAR-2020-0022", "Proposed Rule", "2003-05-01"),
        _document("EPA-HQ-OAR-2020-0022-0002", "EPA-HQ-OAR-2020-0022", "Rule", "2003-05-01"),
        _document("EPA-HQ-OAR-2020-0023-0001", "EPA-HQ-OAR-2020-0023", "Proposed Rule", "2020-07-01"),
        _document("EPA-HQ-OAR-2020-0015-0001", "EPA-HQ-OAR-2020-0015", "Rule", "2020-06-01"),
        _document("EPA-HQ-OAR-2020-0015-0002", "EPA-HQ-OAR-2020-0015", "Rule", "2021-06-01"),
        _document("EPA-HQ-OAR-2020-0016-0001", "EPA-HQ-OAR-2020-0016", "Proposed Rule", "2007-12-31"),
        _document("EPA-HQ-OAR-2020-0016-0002", "EPA-HQ-OAR-2020-0016", "Rule", "2008-03-01"),
        _document("EPA-HQ-OAR-2020-0017-0001", "EPA-HQ-OAR-2020-0017", "Proposed Rule", "2008-01-01"),
        _document("EPA-HQ-OAR-2020-0018-0001", "EPA-HQ-OAR-2020-0018", "Proposed Rule", None),
        _document("EPA-HQ-OAR-2020-0019-0001", "EPA-HQ-OAR-2020-0019", "Proposed Rule", "2030-01-01"),
        _document("EPA-HQ-OAR-2020-0020-0001", "EPA-HQ-OAR-2020-0020", "Rule", "2004-06-01", fr_doc_num="94-12345"),
        _document(
            "FAA-2020-0021-0001",
            "FAA-2020-0021",
            "Proposed Rule",
            "2021-01-01",
            "Airworthiness Directives; Boeing Airplanes",
            agency_code="FAA",
        ),
        _document(
            "CMS-2021-0001-0001",
            "CMS-2021-0001",
            "Proposed Rule",
            "2021-07-14",
            "Payment Policies (CMS-1848-P Display)",
            agency_code="CMS",
        ),
        _document(
            "CMS-2021-0001-0002",
            "CMS-2021-0001",
            "Proposed Rule",
            "2021-07-16",
            agency_code="CMS",
            fr_doc_num="2021-14327",
        ),
    ]
    _write(root, "documents", documents)
    _write(
        root,
        "federal_register",
        [
            _register("2020-01000", "2020-03-01", "Proposed Rule", "Air Plan Approval; Ohio; Regional Haze"),
            _register("2020-02000", "2020-09-01", "Proposed Rule", "Emission Standards; Withdrawal of Proposed Rule"),
            _register("2021-03000", "2021-02-01", "Rule", "Emission Standards; Final Rule"),
            _register("2020-04000", "2020-05-01", "Proposed Rule", "Air Plan Approval; Iowa; Companion Proposal"),
            _register("2020-04001", "2020-05-01", "Rule", "Air Plan Approval; Iowa; Direct Final Rule"),
            _register("2020-05000", "2020-08-01", "Rule", "Emission Standards", ["2060-AA13"]),
            _register("2020-06000", "2020-06-01", "Proposed Rule", "Emission Standards"),
            _register("94-12345", "1994-06-01", "Uncategorized Document", "Emission Standards Program Changes"),
            _register("2020-07000", "2020-07-01", "Rule", "Emission Standards"),
            _register("2021-14327", "2021-07-16", "Proposed Rule", "Medicare Program; Payment Policies"),
            _register("2021-20000", "2021-11-01", "Rule", "Medicare Program; Payment Policies; Final Rule"),
            _register(
                "2021-30000",
                "2021-03-01",
                "Proposed Rule",
                "Receipt of Several Pesticide Petitions Filed for Residues of Pesticide Chemicals in or on Various Commodities",
            ),
            _register("2021-40000", "2021-04-01", "Proposed Rule", "Notice of Proposed Rulemaking"),
            _register("2021-40001", "2021-10-01", "Rule", "Air Plan Approval; Ohio; Regional Haze"),
        ],
    )
    _write(
        root,
        "fr_docket_links",
        [
            {"docket_id": docket, "document_number": number, "publication_date": published}
            for docket, number, published in (
                ("EPA-HQ-OAR-2020-0005", "2020-02000", "2020-09-01"),
                ("EPA-HQ-OAR-2020-0012", "2021-03000", "2021-02-01"),
                ("EPA-HQ-OAR-2020-0002", "2020-04000", "2020-05-01"),
                ("EPA-HQ-OAR-2020-0002", "2020-04001", "2020-05-01"),
                ("EPA-HQ-OAR-2020-0015", "2020-06000", "2020-06-01"),
                ("EPA-HQ-OAR-2020-0023", "2020-07000", "2020-07-01"),
                ("CMS-2021-0001", "2021-20000", "2021-11-01"),
                ("EPA-HQ-OPP-2021-0002", "2021-30000", "2021-03-01"),
                ("EPA-HQ-OAR-2021-0003", "2021-40000", "2021-04-01"),
                ("EPA-HQ-OAR-2021-0003", "2021-40001", "2021-10-01"),
            )
        ],
    )
    _write(
        root,
        "unified_agenda",
        [
            _agenda(
                "2060-AA01",
                "202404",
                "Final Rule Stage",
                [("NPRM", "03/01/2020")],
                priority_category="Other Significant",
            ),
            _agenda(
                "2060-AA01",
                "202510",
                "Completed Actions",
                [("NPRM", "03/01/2020"), ("Final Rule", "01/10/2021")],
                priority_category="Economically Significant",
                major="Yes",
            ),
            _agenda("2060-AA03", "202510", "Completed Actions", [("NPRM", "01/01/2019"), ("Withdrawn", "06/15/2020")]),
            _agenda("2060-AA09", "202510", "Proposed Rule Stage", [("NPRM", "01/03/2022")]),
            _agenda("2060-AA11", "202510", "Long-Term Actions", [("NPRM", "To Be Determined")]),
        ],
    )
    build_rule_targets(root)
    build_proceedings(root)
    build_regulatory_agenda(root)


def _lifecycles_of(root: Path) -> tuple[dict[str, dict], list[dict]]:
    """The lifecycle stage over ``root``'s built upstream tables: rows by their first docket, and the events."""
    lifecycles, events = build_lifecycles(root, run_id="test-run", asserted_at="2026-09-27T00:00:00Z")
    by_docket = {}
    proceedings = {row["proceeding_id"]: row for row in pq.read_table(root / "proceedings.parquet").to_pylist()}
    for row in pq.read_table(lifecycles).to_pylist():
        by_docket[json.loads(proceedings[row["proceeding_id"]]["docket_ids_json"])[0]] = row
    return by_docket, pq.read_table(events).to_pylist()


def _build(root: Path) -> tuple[dict[str, dict], list[dict]]:
    _build_inputs(root)
    return _lifecycles_of(root)


def _events_of(events: list[dict], row: dict) -> list[dict]:
    return [event for event in events if event["proceeding_id"] == row["proceeding_id"]]


def _titles(root: Path) -> dict[str, str]:
    """Each fixture document's title by the id a lifecycle anchors on: a Register record id or a Regulations.gov id."""
    register = pq.read_table(
        root / "federal_register.parquet", columns=["document_number", "publication_date", "title"]
    )
    documents = pq.read_table(root / "documents.parquet", columns=["document_id", "title"])
    return {f"{r['document_number']}@{r['publication_date']}": r["title"] for r in register.to_pylist()} | {
        r["document_id"]: r["title"] for r in documents.to_pylist()
    }


def test_the_stage_reads_every_rule_end_to_end(tmp_path):
    rows, events = _build(tmp_path)
    assert len(rows) == 26  # every docketed proceeding; each Register document joined a docket
    a = rows["EPA-HQ-OAR-2020-0001"]
    # The copy is the Register's proposal: the Register's id and day win over the posting's.
    assert (a["kind"], a["proposal_document_id"], a["proposal_date"]) == (
        "finalized",
        "2020-01000@2020-03-01",
        date(2020, 3, 1),
    )
    assert (a["final_document_id"], a["duration_days"], a["outcome"]) == ("EPA-HQ-OAR-2020-0001-0002", 315, "final")
    assert (a["routine_family"], a["proposal_form"], a["pre_2008_coverage"]) == ("state_air_plan", "proposed", False)
    assert (a["open_signal"], a["agenda_priority"], a["agenda_major"]) == (
        "agenda_completed_final",
        "Economically Significant",
        "Yes",
    )
    assert (json.loads(a["specific_rins_json"]), a["anchored_by_specific_rin"]) == (["2060-AA01"], False)
    # The Register proposal joined by its copy, and the copy, are one document: the Register's event.
    (proposal,) = [e for e in events if e["document_id"] == "2020-01000@2020-03-01"]
    assert (proposal["source"], proposal["dated_by"], proposal["evidence_id"], proposal["joined_by"]) == (
        "federal_register",
        "federal_register",
        "2020-01000@2020-03-01",
        "fr_copy",
    )
    assert proposal["anchor_role"] == "proposal"
    # A copy alone is the Register document too, dated and typed by the Register.
    g = rows["EPA-HQ-OAR-2020-0007"]
    assert (g["kind"], g["final_document_id"], g["final_date"]) == (
        "final_without_observed_proposal",
        "2021-03000@2021-02-01",
        date(2021, 2, 1),
    )
    (copy,) = [e for e in events if e["evidence_id"] == "EPA-HQ-OAR-2020-0007-0001"]
    assert (copy["source"], copy["dated_by"], copy["event_date"], copy["stage"]) == (
        "federal_register",
        "federal_register",
        date(2021, 2, 1),
        "final",
    )
    assert rows["EPA-HQ-OAR-2020-0012"]["final_document_id"] == "2021-03000@2021-02-01"
    c = rows["EPA-HQ-OAR-2020-0003"]
    assert (c["kind"], c["withdrawal_source"], c["withdrawal_date"], c["open_signal"]) == (
        "withdrawn",
        "unified_agenda",
        date(2020, 6, 15),
        "agenda_completed_withdrawn",
    )
    d = rows["EPA-HQ-OAR-2020-0004"]
    assert (d["kind"], d["outcome"], d["open_signal"]) == ("open", "censored", "no_specific_rin")
    assert d["duration_days"] == (d["censor_date"] - date(2019, 1, 1)).days
    e = rows["EPA-HQ-OAR-2020-0005"]
    assert (e["kind"], e["withdrawal_source"], e["withdrawal_date"]) == (
        "withdrawn",
        "federal_register",
        date(2020, 9, 1),
    )
    f = rows["EPA-HQ-OAR-2020-0006"]
    assert (f["kind"], f["finals_before_proposal"], f["pre_2008_coverage"]) == ("open", 1, False)
    for shared in ("EPA-HQ-OAR-2020-0008", "EPA-HQ-OAR-2020-0009"):
        assert (json.loads(rows[shared]["specific_rins_json"]), rows[shared]["open_signal"]) == ([], "no_specific_rin")
    assert (rows["NOAA-NMFS-2020-0010"]["specific_rins_json"], rows["NOAA-NMFS-2020-0010"]["open_signal"]) == (
        "[]",
        "no_specific_rin",
    )
    n = rows["EPA-HQ-OAR-2020-0011"]
    assert (n["kind"], n["open_signal"], n["pre_2008_coverage"]) == ("no_anchor", "agenda_long_term", None)
    assert {row["actor_id"] for row in rows.values()} == {"spicy-regs:rulemaking-lifecycles:v4"}
    assert {event["actor_id"] for event in events} == {"spicy-regs:lifecycle-events:v3"}


def test_a_companion_needs_the_register_to_date_both_sides_and_an_upload_pair_leaves_survival(tmp_path):
    rows, events = _build(tmp_path)
    b = rows["EPA-HQ-OAR-2020-0002"]
    assert (b["kind"], b["proposal_document_id"], b["final_document_id"], b["final_form"]) == (
        "companion",
        "2020-04000@2020-05-01",
        "2020-04001@2020-05-01",
        "direct_final",
    )
    # Both sides dated by their Regulations.gov upload (decision 54c): an upload pair, with no survival
    # outcome, and not called post-2008 on the strength of a 2020 upload.
    b2 = rows["EPA-HQ-OAR-2020-0014"]
    assert (b2["kind"], b2["final_document_id"], b2["outcome"], b2["duration_days"], b2["pre_2008_coverage"]) == (
        "upload_pair",
        "EPA-HQ-OAR-2020-0014-0002",
        None,
        None,
        None,
    )
    assert {e["dated_by"] for e in _events_of(events, b2)} == {"regulations_gov"}
    # An upload before 2008 still says the rule is that old.
    assert (rows["EPA-HQ-OAR-2020-0022"]["kind"], rows["EPA-HQ-OAR-2020-0022"]["pre_2008_coverage"]) == (
        "upload_pair",
        True,
    )
    # An upload-dated proposal and a Register final that day are an upload pair too.
    b5 = rows["EPA-HQ-OAR-2020-0023"]
    assert (b5["kind"], b5["final_document_id"], b5["outcome"]) == ("upload_pair", "2020-07000@2020-07-01", None)
    # A Register proposal and an upload-dated "final" that day: the later final pairs instead.
    b3 = rows["EPA-HQ-OAR-2020-0015"]
    assert (b3["kind"], b3["final_document_id"], b3["duration_days"]) == (
        "finalized",
        "EPA-HQ-OAR-2020-0015-0002",
        365,
    )


def test_a_final_joined_by_specific_rin_flags_the_lifecycle_and_an_unlisted_rin_is_not_on_the_agenda(tmp_path):
    """Decision 56c flags a proposal *or* a final that arrived by specific RIN; here only the final did."""
    rows, _ = _build(tmp_path)
    s = rows["EPA-HQ-OAR-2020-0013"]
    assert (s["kind"], s["final_document_id"], s["anchored_by_specific_rin"]) == (
        "finalized",
        "2020-05000@2020-08-01",
        True,
    )
    assert (json.loads(s["specific_rins_json"]), s["open_signal"]) == (["2060-AA13"], "not_on_agenda")


def test_pre_2008_coverage_is_read_off_the_proposal_before_the_first_of_2008(tmp_path):
    rows, _ = _build(tmp_path)
    p1, p2 = rows["EPA-HQ-OAR-2020-0016"], rows["EPA-HQ-OAR-2020-0017"]
    assert (p1["kind"], p1["proposal_date"], p1["final_date"], p1["pre_2008_coverage"]) == (
        "finalized",
        date(2007, 12, 31),
        date(2008, 3, 1),
        True,
    )
    assert (p2["proposal_date"], p2["pre_2008_coverage"]) == (date(2008, 1, 1), False)


def test_undated_and_future_dated_events_are_left_out_and_cap_the_censor_date(tmp_path):
    rows, events = _build(tmp_path)
    for docket in ("EPA-HQ-OAR-2020-0018", "EPA-HQ-OAR-2020-0019"):
        assert rows[docket]["kind"] == "no_anchor"
        assert _events_of(events, rows[docket]) == []
    # One censor date for the generation, the last day of any event that could have been observed:
    # the 2030 posting would otherwise have moved it past the run's own day. It is in the metadata too.
    assert {row["censor_date"] for row in rows.values()} == {date(2022, 1, 5)}
    assert pq.read_schema(tmp_path / LIFECYCLES_OUTPUT).metadata[b"censor_date"] == b"2022-01-05"


def test_a_copy_of_an_untyped_register_row_keeps_its_own_type_and_takes_the_registers_day(tmp_path):
    """Decision 60a: the Register types a copy only where it states a type; it always dates it."""
    rows, events = _build(tmp_path)
    t = rows["EPA-HQ-OAR-2020-0020"]
    assert (t["kind"], t["final_document_id"], t["final_date"], t["pre_2008_coverage"]) == (
        "final_without_observed_proposal",
        "94-12345@1994-06-01",
        date(1994, 6, 1),
        True,
    )
    (copy,) = _events_of(events, t)
    assert (copy["source"], copy["dated_by"], copy["evidence_id"]) == (
        "regulations_gov",
        "federal_register",
        "EPA-HQ-OAR-2020-0020-0001",
    )


def test_a_display_upload_is_not_the_anchor_when_the_registers_proposal_follows_within_the_window(tmp_path):
    """Decision 54d, end to end: the upload stays an event of the proceeding, anchoring nothing."""
    rows, events = _build(tmp_path)
    d1 = rows["CMS-2021-0001"]
    assert (d1["kind"], d1["proposal_document_id"], d1["proposal_date"], d1["duration_days"]) == (
        "finalized",
        "2021-14327@2021-07-16",
        date(2021, 7, 16),
        108,
    )
    (upload,) = [e for e in _events_of(events, d1) if e["document_id"] == "CMS-2021-0001-0001"]
    assert (upload["stage"], upload["dated_by"], upload["anchor_role"]) == ("proposed", "regulations_gov", None)


def test_the_routine_family_is_read_at_time_zero_from_the_anchors_own_title(tmp_path):
    """Decision 55a: a stratum is a survival covariate, so the proposal's title decides, the final's without one."""
    rows, _ = _build(tmp_path)
    q1 = rows["EPA-HQ-OPP-2021-0002"]
    assert (q1["kind"], q1["routine_family"]) == ("open", "pesticide_tolerance")
    q2 = rows["EPA-HQ-OAR-2021-0003"]
    assert (q2["kind"], q2["final_document_id"], q2["routine_family"]) == ("finalized", "2021-40001@2021-10-01", None)
    # The AD proposal in an airspace-titled docket (R) reads its own title.
    assert rows["FAA-2020-0021"]["routine_family"] == "airworthiness_directive"
    # With no anchor there is no title of the rule's own, and the proceeding's stands in.
    n = rows["EPA-HQ-OAR-2020-0011"]
    assert (n["kind"], n["routine_family"]) == ("no_anchor", "state_air_plan")


def test_every_survival_stratum_is_the_proposals_own_family(tmp_path):
    rows, _ = _build(tmp_path)
    titles = _titles(tmp_path)
    for docket, row in rows.items():
        if row["outcome"] is not None:
            assert row["routine_family"] == routine_family(row["agency_code"], titles[row["proposal_document_id"]]), (
                docket
            )


def _rewrite(path: Path, rows: list[dict]) -> None:
    write_parquet_rows(path, columns=list(rows[0]), rows=rows)


@pytest.mark.parametrize("change", ["deleted", "retitled"])
def test_the_routine_family_does_not_move_when_the_final_rule_goes_or_is_retitled(tmp_path, change):
    """Decision 55a's invariant: a stratum is fixed at the proposal, and nothing observed later may move it."""
    _build_inputs(tmp_path)
    before, _ = _lifecycles_of(tmp_path)
    if change == "deleted":
        proceedings = pq.read_table(tmp_path / "proceedings.parquet").to_pylist()
        for row in proceedings:
            events = json.loads(row["stage_events_json"])
            row["stage_events_json"] = json.dumps([event for event in events if event["stage"] != "final"])
        _rewrite(tmp_path / "proceedings.parquet", proceedings)
    else:
        register = pq.read_table(tmp_path / "federal_register.parquet").to_pylist()
        for row in register:
            if row["document_type"] == "Rule":
                row["title"] = "Fluopyram; Pesticide Tolerances"
        _rewrite(tmp_path / "federal_register.parquet", register)
    after, _ = _lifecycles_of(tmp_path)
    proposed = {docket for docket, row in before.items() if row["proposal_document_id"] is not None}
    assert {d: after[d]["routine_family"] for d in proposed} == {d: before[d]["routine_family"] for d in proposed}
    # The change was real: a finalized EPA rule lost its final, or its final now says "Tolerances".
    q2 = "EPA-HQ-OAR-2021-0003"
    assert before[q2]["kind"] == "finalized" and before[q2]["routine_family"] is None
    if change == "deleted":
        assert after[q2]["kind"] == "open"
    else:
        assert after[q2]["final_document_id"] == before[q2]["final_document_id"]
        assert routine_family("EPA", "Fluopyram; Pesticide Tolerances") == "pesticide_tolerance"


#: An independent re-derivation of every lifecycle from its events, in SQL rather than the builder's Python.
REDERIVE_SQL = """
WITH e AS (SELECT * FROM events),
p0 AS (
  SELECT proceeding_id, min(event_date) AS d, arg_min(document_id, (event_date, document_id)) AS doc,
         arg_min(joined_by, (event_date, document_id)) AS jb, arg_min(dated_by, (event_date, document_id)) AS dated
  FROM e WHERE stage = 'proposed' GROUP BY 1),
-- Decision 54d: the earliest Register-dated proposal is time zero; an upload-dated one anchors only where the
-- Register dates none that could be its publication (a proposal in form, with no final between the two).
pr AS (
  SELECT proceeding_id, min(event_date) AS d, arg_min(document_id, (event_date, document_id)) AS doc,
         arg_min(joined_by, (event_date, document_id)) AS jb, arg_min(document_form, (event_date, document_id)) AS form
  FROM e WHERE stage = 'proposed' AND dated_by = 'federal_register' GROUP BY 1),
fb AS (
  SELECT e.proceeding_id, count(*) AS n FROM e JOIN p0 USING (proceeding_id) JOIN pr USING (proceeding_id)
  WHERE e.stage = 'final' AND e.event_date > p0.d AND e.event_date <= pr.d GROUP BY 1),
p AS (
  SELECT p0.proceeding_id,
    CASE WHEN r THEN pr.d ELSE p0.d END AS d, CASE WHEN r THEN pr.doc ELSE p0.doc END AS doc,
    CASE WHEN r THEN pr.jb ELSE p0.jb END AS jb, CASE WHEN r THEN 'federal_register' ELSE p0.dated END AS dated
  FROM (SELECT p0.*, pr.d AS prd, pr.doc AS prdoc, pr.jb AS prjb,
          p0.dated = 'regulations_gov' AND pr.d IS NOT NULL AND pr.form IN ('proposed', 'advance_proposed')
            AND coalesce(fb.n, 0) = 0 AS r
        FROM p0 LEFT JOIN pr USING (proceeding_id) LEFT JOIN fb USING (proceeding_id)) p0
  LEFT JOIN pr USING (proceeding_id)),
f AS (
  SELECT e.proceeding_id,
    min(e.event_date) FILTER (WHERE e.event_date > p.d) AS later_d,
    arg_min(e.document_id, (e.event_date, e.document_id)) FILTER (WHERE e.event_date > p.d) AS later_doc,
    arg_min(e.joined_by, (e.event_date, e.document_id)) FILTER (WHERE e.event_date > p.d) AS later_jb,
    arg_min(e.document_id, e.document_id) FILTER (WHERE e.event_date = p.d AND e.dated_by = 'federal_register')
      AS same_doc,
    arg_min(e.joined_by, e.document_id) FILTER (WHERE e.event_date = p.d AND e.dated_by = 'federal_register')
      AS same_jb,
    arg_min(e.document_id, e.document_id) FILTER (WHERE e.event_date = p.d) AS any_same_doc,
    arg_min(e.joined_by, e.document_id) FILTER (WHERE e.event_date = p.d) AS any_same_jb,
    min(e.event_date) AS first_d,
    arg_min(e.document_id, (e.event_date, e.document_id)) AS first_doc,
    arg_min(e.joined_by, (e.event_date, e.document_id)) AS first_jb,
    count(*) FILTER (WHERE e.event_date < p.d) AS before
  FROM e LEFT JOIN p USING (proceeding_id) WHERE e.stage = 'final' GROUP BY 1),
w AS (
  SELECT e.proceeding_id, min(e.event_date) AS d, arg_min(e.document_id, (e.event_date, e.document_id)) AS doc,
         arg_min(e.source, (e.event_date, e.document_id)) AS source
  FROM e JOIN p USING (proceeding_id)
  WHERE e.stage = 'withdrawn' AND e.source IN ('federal_register', 'unified_agenda') AND e.event_date >= p.d
  GROUP BY 1),
k AS (
  SELECT l.proceeding_id, l.censor_date, p.d AS pd, p.doc AS pdoc, p.jb AS pjb, f.*, w.d AS wd, w.doc AS wdoc,
    w.source AS ws,
    CASE WHEN p.d IS NULL AND f.first_d IS NOT NULL THEN 'final_without_observed_proposal'
         WHEN p.d IS NULL THEN 'no_anchor'
         WHEN f.later_d IS NOT NULL THEN 'finalized'
         WHEN f.same_doc IS NOT NULL AND p.dated = 'federal_register' THEN 'companion'
         WHEN f.any_same_doc IS NOT NULL AND p.dated = 'regulations_gov' THEN 'upload_pair'
         WHEN w.d IS NOT NULL THEN 'withdrawn'
         ELSE 'open' END AS kind
  FROM lifecycles l LEFT JOIN p USING (proceeding_id) LEFT JOIN f USING (proceeding_id) LEFT JOIN w USING (proceeding_id))
SELECT proceeding_id, kind, pd AS proposal_date, pdoc AS proposal_document_id,
  CASE kind WHEN 'finalized' THEN later_d WHEN 'companion' THEN pd WHEN 'upload_pair' THEN pd
    WHEN 'final_without_observed_proposal' THEN first_d END AS final_date,
  CASE kind WHEN 'finalized' THEN later_doc WHEN 'companion' THEN same_doc WHEN 'upload_pair' THEN any_same_doc
    WHEN 'final_without_observed_proposal' THEN first_doc END AS final_document_id,
  CASE WHEN pd IS NOT NULL THEN coalesce(before, 0) END AS finals_before_proposal,
  CASE WHEN kind = 'withdrawn' THEN wd END AS withdrawal_date,
  CASE WHEN kind = 'withdrawn' THEN wdoc END AS withdrawal_document_id,
  CASE WHEN kind = 'withdrawn' THEN ws END AS withdrawal_source,
  CASE kind WHEN 'finalized' THEN 'final' WHEN 'withdrawn' THEN 'withdrawn' WHEN 'open' THEN 'censored' END AS outcome,
  CAST(CASE kind WHEN 'finalized' THEN later_d - pd WHEN 'withdrawn' THEN wd - pd WHEN 'open' THEN censor_date - pd END
    AS INTEGER) AS duration_days,
  coalesce(pjb = 'specific_rin', false) OR coalesce(CASE kind WHEN 'finalized' THEN later_jb WHEN 'companion' THEN same_jb
    WHEN 'upload_pair' THEN any_same_jb WHEN 'final_without_observed_proposal' THEN first_jb END = 'specific_rin', false)
    AS anchored_by_specific_rin,
  CASE WHEN kind = 'upload_pair' AND pd >= DATE '2008-01-01' THEN NULL
       ELSE coalesce(pd, first_d) < DATE '2008-01-01' END AS pre_2008_coverage
FROM k ORDER BY proceeding_id
"""
REDERIVED_COLUMNS = (
    "proceeding_id",
    "kind",
    "proposal_date",
    "proposal_document_id",
    "final_date",
    "final_document_id",
    "finals_before_proposal",
    "withdrawal_date",
    "withdrawal_document_id",
    "withdrawal_source",
    "outcome",
    "duration_days",
    "anchored_by_specific_rin",
    "pre_2008_coverage",
)


def rederive(lifecycles_path: Path, events_path: Path) -> tuple[list[tuple], list[tuple], list[tuple]]:
    """Re-derived rows, published rows (the same columns), and anchor roles the events disagree with."""
    con = duckdb.connect()
    con.execute(f"CREATE VIEW lifecycles AS SELECT * FROM read_parquet('{lifecycles_path}')")
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{events_path}')")
    derived = con.execute(REDERIVE_SQL).fetchall()
    published = con.execute(f"SELECT {', '.join(REDERIVED_COLUMNS)} FROM lifecycles ORDER BY proceeding_id").fetchall()
    roles = con.execute(
        """SELECT e.proceeding_id, e.document_id, e.anchor_role FROM events e JOIN lifecycles l USING (proceeding_id)
           WHERE e.anchor_role IS DISTINCT FROM CASE e.document_id WHEN l.proposal_document_id THEN 'proposal'
             WHEN l.final_document_id THEN 'final' WHEN l.withdrawal_document_id THEN 'withdrawal' END"""
    ).fetchall()
    return derived, published, roles


def test_every_lifecycle_row_is_rederived_from_its_events(tmp_path):
    _build(tmp_path)
    derived, published, roles = rederive(tmp_path / LIFECYCLES_OUTPUT, tmp_path / EVENTS_OUTPUT)
    assert derived == published
    assert roles == []
    assert {row[1] for row in derived} >= {"finalized", "companion", "upload_pair", "withdrawn", "open", "no_anchor"}


def test_the_stages_follow_proceedings_and_the_agenda_and_publish_in_its_generation():
    pipeline = RulemakingDatasetPipeline(output_dir=Path("unused"))
    stages = {stage.name: stage for stage in pipeline.stages()}
    assert stages["lifecycles"].depends_on == ("proceedings", "regulatory-agenda")
    assert stages["agency-lifecycle-stats"].depends_on == ("lifecycles",)
    order = [stage.name for stage in pipeline._ordered_stages(pipeline.stages())]
    assert order.index("lifecycles") > max(order.index("proceedings"), order.index("regulatory-agenda"))
    assert {"rulemaking_lifecycles.parquet", "lifecycle_events.parquet", "agency_lifecycle_stats.parquet"} <= set(
        pipeline.published_outputs
    )


# --- withdrawn postings (round 6) -------------------------------------------------------------

LOGBOOK = "Electronic Logbook Reporting in Commercial Fisheries of the Gulf of America and Atlantic"
STAPLE_FOODS = "Updated Staple Food Stocking Standards for Retailers in the Supplemental Nutrition Assistance Program"
WITHDRAWN_POSTINGS = ("FNS-2025-0401-0001", "NOAA-NMFS-2025-0570-0022", "NOAA-NMFS-2025-0570-0023")


def _withdrawn_postings(root: Path, withdrawn: str | None) -> tuple[dict[str, dict], list[dict]]:
    """Two dockets as held on 2026-10-03 (documents generation 066964e8), their postings' ``withdrawn`` set to ``withdrawn``.

    FNS-2025-0401's one posting is a copy of the final rule regulations.gov flags withdrawn ("Duplicate docket");
    NOAA-NMFS-2025-0570 holds the Register's two logbook proposals and two finals of other rules, both flagged
    "Accidentally added to wrong docket.".
    """
    fns, noaa = "FNS-2025-0401", "NOAA-NMFS-2025-0570"
    wrong_docket = {"withdrawn": withdrawn, "reason_withdrawn": "Accidentally added to wrong docket."}
    _write(
        root,
        "dockets",
        [
            _docket(fns, "0584-AF12", agency="FNS", title=STAPLE_FOODS, modified="2026-05-08T16:52:05Z"),
            _docket(noaa, "0648-BN11", agency="NOAA", title=LOGBOOK, modified="2026-06-26T16:51:10Z"),
        ],
    )
    _write(
        root,
        "documents",
        [
            _document(
                f"{fns}-0001", fns, "Rule", "2026-05-08T04:00:00Z", STAPLE_FOODS, agency_code="FNS",
                withdrawn=withdrawn, reason_withdrawn="Duplicate docket",
            ),
            _document(
                f"{noaa}-0001", noaa, "Proposed Rule", "2025-11-20T05:00:00Z", LOGBOOK, agency_code="NOAA",
                fr_doc_num="2025-20491", withdrawn="false",
            ),
            _document(
                f"{noaa}-0022", noaa, "Rule", "2025-09-29T04:00:00Z",
                "Fisheries of the Northeastern United States: Coastal Migratory Pelagic Resources of the Gulf and "
                "Atlantic Region", agency_code="NOAA", **wrong_docket,
            ),
            _document(
                f"{noaa}-0023", noaa, "Rule", "2026-05-26T04:00:00Z",
                "Atlantic Highly Migratory Species: 2025 North Atlantic Albacore Tuna, North and South Atlantic "
                "Swordfish, and Atlantic Bluefin Tuna Category Quotas", agency_code="NOAA", **wrong_docket,
            ),
            _document(
                f"{noaa}-0024", noaa, "Proposed Rule", "2026-05-26T04:00:00Z", LOGBOOK, agency_code="NOAA",
                fr_doc_num="2026-10389", withdrawn="false",
            ),
        ],
    )
    _write(
        root,
        "federal_register",
        [
            _register("2025-20491", "2025-11-20", "Proposed Rule", LOGBOOK, ["0648-BN11"]),
            _register("2026-10389", "2026-05-26", "Proposed Rule", LOGBOOK, ["0648-BN11"]),
        ],
    )
    _write(root, "fr_docket_links", [])
    _write(root, "unified_agenda", [])
    build_rule_targets(root)
    build_proceedings(root)
    build_regulatory_agenda(root)
    return _lifecycles_of(root)


def test_a_posting_regulations_gov_withdrew_is_no_stage_evidence(tmp_path):
    """A withdrawn posting was misfiled, duplicated or moved (round 6, M4-A); the live snapshot paired both."""
    rows, events = _withdrawn_postings(tmp_path, "true")
    assert (rows["FNS-2025-0401"]["kind"], rows["FNS-2025-0401"]["final_document_id"]) == ("no_anchor", None)
    noaa = rows["NOAA-NMFS-2025-0570"]
    assert (noaa["kind"], noaa["proposal_document_id"], noaa["final_document_id"]) == (
        "open",
        "2025-20491@2025-11-20",
        None,
    )
    proceedings = pq.read_table(tmp_path / "proceedings.parquet").to_pylist()
    stage_evidence = {event["evidence_id"] for row in proceedings for event in json.loads(row["stage_events_json"])}
    assert stage_evidence.isdisjoint(WITHDRAWN_POSTINGS)
    assert {event["evidence_id"] for event in events}.isdisjoint(WITHDRAWN_POSTINGS)


@pytest.mark.parametrize("withdrawn", ["false", None])
def test_a_posting_not_flagged_withdrawn_still_counts(tmp_path, withdrawn):
    """``'false'`` and NULL are the flag unset: both postings anchor as the live snapshot published them."""
    rows, _ = _withdrawn_postings(tmp_path, withdrawn)
    assert (rows["FNS-2025-0401"]["kind"], rows["FNS-2025-0401"]["final_document_id"]) == (
        "final_without_observed_proposal",
        "FNS-2025-0401-0001",
    )
    assert (rows["NOAA-NMFS-2025-0570"]["kind"], rows["NOAA-NMFS-2025-0570"]["final_document_id"]) == (
        "finalized",
        "NOAA-NMFS-2025-0570-0023",
    )


# --- the deleted documents-based rollup --------------------------------------------------


def test_the_documents_based_rollup_is_gone_with_no_dangling_reference():
    assert not (REPO_ROOT / "src/spicy_regs/transforms/build_rulemaking_lifecycles.py").exists()
    assert not (REPO_ROOT / "src/spicy_regs/pipelines/rollups/rulemaking_lifecycles.py").exists()
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert "run-rollup-lifecycles" not in scripts
    # The name now belongs to the rulemaking dataset's stage, which MCP and the dictionary expose from the
    # snapshot pointer. The rollup's literal schema, its own columns and its prose are gone from every
    # place that described it.
    assert "rulemaking_lifecycles" in mcp_server.TABLES and "rulemaking_lifecycles" in dd.MCP_QUERYABLE
    assert "rulemaking_lifecycles" not in dd.DERIVED_SCHEMAS
    declared = dd.expected_schemas()["rulemaking_lifecycles"]
    from spicy_regs.native_types import described_schema
    assert dd.expected_schemas()["rulemaking_lifecycles"] == described_schema(dd.subject_policies()["rulemaking_lifecycles"].subject_schema)
    assert not {"docket_id", "title", "proposed_date", "days"} & {column for column, _ in declared}
    rollup_prose = ("build_rulemaking_lifecycles", "Proposed→final rulemaking arcs", "`stuck`", "`pair`")
    for path in (
        "data_dictionary/descriptions.yaml",
        "data_dictionary/catalog.json",
        "src/spicy_regs/table_metadata.json",
        "docs/tables/rulemaking_lifecycles.md",
        "mkdocs.yml",
        "deploy/cloudrun/deploy.sh",
    ):
        text = (REPO_ROOT / path).read_text(encoding="utf-8")
        assert not any(marker in text for marker in rollup_prose), path
    # The new table reuses the name, so the qualification record may name it only as a T17 rulemaking output.
    record = json.loads((REPO_ROOT / "src/spicy_regs/table_qualification.json").read_text(encoding="utf-8"))
    naming = [row["task"] for row in record["rows"] if "rulemaking_lifecycles" in row["tables"]]
    assert naming in ([], ["T17"]), naming
    stale = (
        "build_rulemaking_lifecycles",
        "RulemakingLifecyclesRollup",
        "rollups.rulemaking_lifecycles",
        "run-rollup-lifecycles",
    )
    for root in ("src", "scripts", "deploy", ".github", "plugins", "tests"):
        for path in (REPO_ROOT / root).rglob("*"):
            if path.is_file() and path.suffix in {".py", ".sh", ".yml", ".yaml", ".toml", ".md", ".json"}:
                text = path.read_text(encoding="utf-8", errors="ignore")
                if path.name != Path(__file__).name:
                    assert not any(name in text for name in stale), path
