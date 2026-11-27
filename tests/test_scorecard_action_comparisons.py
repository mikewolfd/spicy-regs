"""Qualified multi-publisher roll-call comparisons: preserve credit and refuse unsupported evidence."""
from copy import deepcopy

import pytest

from spicy_regs.scorecards.action_comparisons import (
    COLUMNS, RESULT_COLUMNS, TABLE, VOTE_COLUMNS, compare_lcv_2025, selected_native_rows,
)
from spicy_regs.scorecards.etl import ANALYSIS_NAMES, SOURCE_NAMES, generation_options, read_family, write_family
from spicy_regs.scorecards.resolution import INPUT_TABLES
from spicy_regs.generations import build_generation
from tests.test_scorecard_native_main_inputs import native_selection

# The votes trio's tables publish no receipts since ss-vse3; here they stand in for the receipt route it last
# published under (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("votes_receipts_unretired")


def fixture():
    source = {"scorecards": [{"scorecard_id": "lcv:2025", "snapshot_id": "s1", "publisher_id": "lcv", "year_text": "2025"}],
              "scorecard_items": []}
    links = {"scorecard_member_links": [], "scorecard_item_links": []}
    results, votes, people, metrics = [], [], [], []
    cases = ["same", "different", "not_voting", "present", "missing", "not_applicable", "target",
             "ambiguous", "repeated", "stale", "duplicate_vote", "unknown_position"]
    for number, case in enumerate(cases):
        item, bio = str(number), f"X{number:06}"
        source["scorecard_items"].append(dict(scorecard_id="lcv:2025", snapshot_id="s1", item_id=item,
            item_kind_text="cosponsorship" if case == "not_applicable" else "Scorecard Vote",
            publisher_position_text="Oppose" if case == "target" else "No"))
        results.append(dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="lcv:2025", snapshot_id="s1", item_id=item,
            result_id="reported", publisher_member_key=bio, metric_id="annual", participation_id="scorecard", result_text="excused",
            capture_id="capture", source_url="https://publisher.example", source_path="CSV/row[1]/Vote Type"))
        metrics.append(dict(scorecard_id="lcv:2025", snapshot_id="s1", metric_id="annual", item_id=item,
                            participation_id="scorecard", publisher_position_text="No"))
        links["scorecard_member_links"].append(dict(scorecard_id="lcv:2025", publisher_member_key=bio,
            source_snapshot_id="s1", bioguide_id=bio, resolution_status="resolved"))
        link: dict[str, str | None] = dict(scorecard_id="lcv:2025", item_id=item, source_snapshot_id="old" if case == "stale" else "s1",
                    reference_id="item:direct", vote_id="119-senate-1-24", resolution_status="resolved")
        if case == "ambiguous":
            link.update(resolution_status="ambiguous", vote_id=None)
        links["scorecard_item_links"].append(link)
        if case == "repeated":
            links["scorecard_item_links"].append(link | {"reference_id": "reference:second", "vote_id": "119-senate-1-25"})
        people.append(dict(bioguide_id=bio, lis_id="S" + item))
        if case != "missing":
            position = {"same": "nay", "different": "yea", "not_voting": "not_voting", "present": "present",
                        "unknown_position": "unknown"}.get(case, "nay")
            vote = dict(vote_id="119-senate-1-24", member_key="lis:S" + item, bioguide_id=None,
                        lis_id="S" + item, position=position, position_normalized=position)
            votes.append(vote)
            if case == "duplicate_vote":
                votes.append(vote | {"member_key": "duplicate"})
    pins = {name: dict(family="scorecards" if name.startswith("scorecard") else "official",
                      artifactDigest="sha256:" + "a" * 64, sha256="sha256:" + "b" * 64, byteSize=1)
            for name in (*INPUT_TABLES, "scorecard_member_item_results", "scorecard_metric_items", "member_votes")}
    return source, links, results, votes, people, pins, metrics


def test_six_statuses_fail_closed_and_native_credit_is_unchanged(tmp_path):
    inputs = fixture()
    original = deepcopy(inputs)
    rows = compare_lcv_2025(*inputs)
    assert [r["comparison_status"] for r in rows] == [
        "same", "different", "not_voting", "present", "unresolved", "not_applicable",
        "unresolved", "unresolved", "unresolved", "unresolved", "unresolved", "unresolved"]
    assert rows[2]["publisher_credit_text"] == "excused"
    assert rows[2]["official_position_normalized"] == "not_voting"
    assert rows[6]["publisher_target_text"] == "Oppose"
    assert rows[8]["vote_id"] is None
    assert rows[9]["reason"] == "link_snapshot_mismatch"
    assert inputs == original
    assert TABLE in ANALYSIS_NAMES and TABLE not in SOURCE_NAMES
    directory = tmp_path / "comparison"
    files = write_family(directory, {TABLE: rows})
    assert read_family(directory, [TABLE])[TABLE] == rows
    build_generation(tmp_path / "generation", family="scorecard-analysis", files=files,
                     expected_keys=[TABLE + ".parquet"], **generation_options(directory, [TABLE]))


def test_changed_source_snapshot_refuses():
    inputs = fixture()
    inputs[2][0]["snapshot_id"] = "old"
    with pytest.raises(ValueError, match="selected snapshot"):
        compare_lcv_2025(*inputs)


def test_metric_specific_target_and_lis_conflicts_stay_unresolved():
    inputs = fixture()
    inputs[6][0]["publisher_position_text"] = "Yes"
    assert compare_lcv_2025(*inputs)[0]["reason"] == "missing_or_conflicting_metric_target"
    inputs = fixture()
    inputs[4].append(dict(bioguide_id="other", lis_id="S0"))
    assert compare_lcv_2025(*inputs)[0]["comparison_status"] == "unresolved"


def test_native_vote_filter_keeps_only_requested_roll_call(tmp_path):
    index, subject, _ = native_selection(tmp_path, "member_votes", [
        {"vote_id": "119-senate-1-24", "member_key": "lis:S1", "position": "Nay", "position_normalized": "nay"},
        {"vote_id": "118-house-1-1", "member_key": "X000001", "position": "Yea", "position_normalized": "yea"},
    ])
    rows = selected_native_rows(index, "member_votes", [subject], VOTE_COLUMNS, "vote_id", {"119-senate-1-24"})
    assert len(rows) == 1 and rows[0]["member_key"] == "lis:S1"
    assert selected_native_rows(index, "member_votes", [subject], VOTE_COLUMNS, "vote_id", set()) == []


def test_retained_zeldin_item_literal_examples():
    # Independently selected source51536 / analysis25d74 / official f26f rows:
    # item36294, CSV member-result cells. No publisher body is copied here.
    source, links, _, _, _, pins, _ = fixture()
    snapshot = "scorecard-snapshot:22a84305-68e5-4a7b-bc3d-1e1a49a504c5"
    source["scorecards"][0]["snapshot_id"] = snapshot
    source["scorecard_items"] = [dict(scorecard_id="lcv:2025", snapshot_id=snapshot, item_id="36294",
        item_kind_text="Scorecard Vote", publisher_position_text="No")]
    links = {"scorecard_member_links": [], "scorecard_item_links": [dict(scorecard_id="lcv:2025", item_id="36294",
        source_snapshot_id=snapshot, reference_id="item:direct", resolution_status="resolved", vote_id="119-senate-1-24")]}
    results, votes, people, metrics = [], [], [], []
    for key, bio, lis, credit, position, normalized in [
        ("angela-alsobrooks", "A000382", "S428", "pro", "Nay", "nay"),
        ("lisa-a-murkowski", "M001153", "S288", "anti", "Yea", "yea"),
        ("cory-anthony-booker", "B001288", "S370", "excused", "Not Voting", "not_voting"),
    ]:
        results.append(dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="lcv:2025", result_id="reported",
            snapshot_id=snapshot, item_id="36294", publisher_member_key=key, metric_id="annual", participation_id="scorecard", result_text=credit))
        links["scorecard_member_links"].append(dict(scorecard_id="lcv:2025", publisher_member_key=key,
            source_snapshot_id=snapshot, resolution_status="resolved", bioguide_id=bio))
        people.append(dict(bioguide_id=bio, lis_id=lis))
        votes.append(dict(vote_id="119-senate-1-24", member_key="lis:" + lis, bioguide_id=bio, lis_id=lis,
                          position=position, position_normalized=normalized))
    metrics = [dict(scorecard_id="lcv:2025", snapshot_id=snapshot, metric_id="annual", item_id="36294",
                    participation_id="scorecard", publisher_position_text="No")]
    rows = compare_lcv_2025(source, links, results, votes, people, pins, metrics)
    assert [r["comparison_status"] for r in rows] == ["same", "different", "not_voting"]
    assert [r["publisher_credit_text"] for r in rows] == ["pro", "anti", "excused"]
    assert all(tuple(r) == COLUMNS and "input_pins_json" not in r for r in rows)


def test_multi_publisher_target_rules_corroborating_links_and_explicit_refusals():
    from spicy_regs.scorecards.action_comparisons import compare_roll_calls

    _, _, _, _, _, pins, _ = fixture()
    source = {
        "scorecards": [
            dict(scorecard_id="peace_action:2024", snapshot_id="s1", publisher_id="peace_action", year_text="2024"),
            dict(scorecard_id="frc_action:2025", snapshot_id="s2", publisher_id="frc_action", year_text="2025"),
            dict(scorecard_id="cpac:2023", snapshot_id="s3", publisher_id="cpac", year_text="2023"),
            dict(scorecard_id="dav:109", snapshot_id="s4", publisher_id="dav", year_text=None),
            dict(scorecard_id="hrc:118", snapshot_id="s5", publisher_id="hrc", year_text=None),
        ],
        "scorecard_items": [
            dict(scorecard_id="peace_action:2024", snapshot_id="s1", item_id="p1", item_kind_text="vote", publisher_position_text="favor"),
            dict(scorecard_id="frc_action:2025", snapshot_id="s2", item_id="f1", item_kind_text="roll call", publisher_position_text="FRC Action opposed this Bill."),
            dict(scorecard_id="cpac:2023", snapshot_id="s3", item_id="c1", item_kind_text="roll_call", publisher_position_text="supported this amendment"),
            dict(scorecard_id="dav:109", snapshot_id="s4", item_id="d1", item_kind_text="roll_call", publisher_position_text=None),
            dict(scorecard_id="hrc:118", snapshot_id="s5", item_id="h1", item_kind_text="cosponsorship", publisher_position_text="HRC opposed the bill."),
            dict(scorecard_id="hrc:118", snapshot_id="s5", item_id="h2", item_kind_text=None, publisher_position_text="HRC opposed the bill."),
        ],
    }
    links = {
        "scorecard_member_links": [
            dict(scorecard_id="peace_action:2024", publisher_member_key="m1", source_snapshot_id="s1", bioguide_id="A000001", resolution_status="resolved"),
            dict(scorecard_id="frc_action:2025", publisher_member_key="m1", source_snapshot_id="s2", bioguide_id="A000001", resolution_status="resolved"),
            dict(scorecard_id="cpac:2023", publisher_member_key="m1", source_snapshot_id="s3", bioguide_id="A000001", resolution_status="resolved"),
            dict(scorecard_id="dav:109", publisher_member_key="m1", source_snapshot_id="s4", bioguide_id="A000001", resolution_status="resolved"),
            dict(scorecard_id="hrc:118", publisher_member_key="m1", source_snapshot_id="s5", bioguide_id="A000001", resolution_status="resolved"),
        ],
        "scorecard_item_links": [
            # Peace Action corroborating item:direct + reference:vote agreeing on same vote_id
            dict(scorecard_id="peace_action:2024", item_id="p1", source_snapshot_id="s1", reference_id="item:direct", vote_id="118-house-2-10", resolution_status="resolved"),
            dict(scorecard_id="peace_action:2024", item_id="p1", source_snapshot_id="s1", reference_id="reference:vote", vote_id="118-house-2-10", resolution_status="resolved"),
            # FRC Action roll call + separate bill reference (vote_id=None)
            dict(scorecard_id="frc_action:2025", item_id="f1", source_snapshot_id="s2", reference_id="item:direct", vote_id="119-house-1-10", resolution_status="resolved"),
            dict(scorecard_id="frc_action:2025", item_id="f1", source_snapshot_id="s2", reference_id="reference:bill", vote_id=None, bill_id="119-hr-1", resolution_status="resolved"),
            # CPAC 2023
            dict(scorecard_id="cpac:2023", item_id="c1", source_snapshot_id="s3", reference_id="item:direct", vote_id="118-house-1-5", resolution_status="resolved"),
            # DAV without publisher target
            dict(scorecard_id="dav:109", item_id="d1", source_snapshot_id="s4", reference_id="item:direct", vote_id="109-house-2-1", resolution_status="resolved"),
            # HRC cosponsorship (non-floor) & unlinked vote
            dict(scorecard_id="hrc:118", item_id="h1", source_snapshot_id="s5", reference_id="item:direct", vote_id=None, bill_id="118-hr-1", resolution_status="resolved", reason="no_roll_selected_from_bill_or_amendment_alone"),
            dict(scorecard_id="hrc:118", item_id="h2", source_snapshot_id="s5", reference_id="item:direct", vote_id=None, bill_id=None, resolution_status="unresolved", reason="incomplete_roll_identifiers"),
        ],
    }
    results = [
        dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="peace_action:2024", snapshot_id="s1", item_id="p1", result_id="r1", publisher_member_key="m1", result_text="wpc_voted_with_us"),
        dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="frc_action:2025", snapshot_id="s2", item_id="f1", result_id="r1", publisher_member_key="m1", result_text="-"),
        dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="cpac:2023", snapshot_id="s3", item_id="c1", result_id="r1", publisher_member_key="m1", result_text="+"),
        dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="dav:109", snapshot_id="s4", item_id="d1", result_id="r1", publisher_member_key="m1", result_text="+"),
        dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="hrc:118", snapshot_id="s5", item_id="h1", result_id="r1", publisher_member_key="m1", result_text="●"),
        dict.fromkeys(RESULT_COLUMNS) | dict(scorecard_id="hrc:118", snapshot_id="s5", item_id="h2", result_id="r1", publisher_member_key="m1", result_text="○"),
    ]
    votes = [
        dict(vote_id="118-house-2-10", member_key="A000001", bioguide_id="A000001", lis_id=None, position="Yea", position_normalized="yea"),
        dict(vote_id="119-house-1-10", member_key="A000001", bioguide_id="A000001", lis_id=None, position="Yea", position_normalized="yea"),
        dict(vote_id="118-house-1-5", member_key="A000001", bioguide_id="A000001", lis_id=None, position="Not Voting", position_normalized="not_voting"),
    ]
    people = [dict(bioguide_id="A000001", lis_id=None)]
    rows = compare_roll_calls(source, links, results, votes, people, pins, [])
    assert [(r["scorecard_id"], r["comparison_status"], r["reason"], r["publisher_credit_text"]) for r in rows] == [
        ("peace_action:2024", "same", "exact_roll_call_position", "wpc_voted_with_us"),
        ("frc_action:2025", "different", "exact_roll_call_position", "-"),
        ("cpac:2023", "not_voting", "explicit_official_position", "+"),
        ("dav:109", "unresolved", "missing_publisher_target", "+"),
        ("hrc:118", "not_applicable", "non_floor_vote_item_kind", "●"),
        ("hrc:118", "unresolved", "unresolved_member_or_roll_call", "○"),
    ]
    assert rows[0]["reference_id"] == "item:direct" and rows[0]["vote_id"] == "118-house-2-10"


def test_active_cosponsor_with_normalized_null_withdrawn_date_resolves_cleanly():
    """FINDING 6 (ss-kj95): normalized None sponsorship_withdrawn_date evaluates as active, whereas '' degrades."""
    from tests.test_scorecard_historical_sponsorship import compare, historical_fixture

    normalized = historical_fixture()
    normalized[3][0].update(sponsorship_date="2022-08-26", sponsorship_withdrawn_date=None)
    row = compare(normalized)[0]
    assert (row["comparison_status"], row["official_position_normalized"], row["reason"]) == (
        "same",
        "cosponsor_at_cutoff",
        "dated_official_cosponsorship_at_source_cutoff",
    )

    unnormalized = historical_fixture()
    unnormalized[3][0].update(sponsorship_date="2022-08-26", sponsorship_withdrawn_date="")
    degraded = compare(unnormalized)[0]
    assert (degraded["comparison_status"], degraded["reason"]) == (
        "unresolved",
        "missing_or_invalid_official_sponsorship_dates",
    )
