"""Exact resolution does not turn uncertainty into invented source identities."""

from copy import deepcopy
import json

import pytest

from spicy_regs.scorecards.resolution import INPUT_TABLES, LINK_COLUMNS, OFFICIAL_COLUMNS, resolve_scorecard_links


def pins():
    return {
        name: {
            "sha256": "sha256:" + "a" * 64,
            "byteSize": 321,
            "family": "test-family",
            "artifactDigest": "sha256:" + "b" * 64,
            "layout": {"kind": "single"},
        }
        for name in INPUT_TABLES
    }


def person(key="A000001", first="Alice", last="Smith", **extra):
    return {"bioguide_id": key, "name_first": first, "name_last": last, **extra}


def term(key="A000001", index="0", state="CA", district="7", kind="rep", start="2023-01-03", end="2025-01-03"):
    return {
        "bioguide_id": key,
        "term_index": index,
        "term_state": state,
        "term_district": district,
        "term_type": kind,
        "term_start": start,
        "term_end": end,
    }


def official(members=None, terms=None):
    return {
        "members": members if members is not None else [person()],
        "member_terms": terms if terms is not None else [term()],
        "congress_bills": [{"bill_id": "118-hr-1"}, {"bill_id": "118-s-2"}],
        "amendments": [{"amendment_id": "118-hamdt-3", "amended_bill_id": "118-hr-1"}],
        "roll_call_votes": [
            {"vote_id": "118-house-1-4", "bill_id": "118-hr-1"},
            {"vote_id": "118-house-1-5", "bill_id": "118-hr-1"},
        ],
    }


def fact(**extra):
    return {
        "scorecard_id": "publisher:118",
        "snapshot_id": "publisher:118:snapshot",
        "capture_id": "capture",
        "source_url": "https://publisher.example/118",
        "source_path": "table/member",
        **extra,
    }


def member(**extra):
    return fact(publisher_member_key="source-person", member_name="Alice Smith", state="CA", district="7", **extra)


def sources(members=(), items=(), **edition):
    return {
        "scorecards": [fact(congress_text="118th Congress", chamber_scope_text="House", **edition)],
        "scorecard_members": list(members),
        "scorecard_items": list(items),
    }


def link(row, data=None, **kwargs):
    return resolve_scorecard_links(sources([row]), data or official(), pins(), **kwargs)["scorecard_member_links"][0]


def item_link(**fields):
    return resolve_scorecard_links(sources(items=[fact(item_id="source-item", **fields)]), official(), pins())[
        "scorecard_item_links"
    ]


def test_exact_historical_context_uses_terms_not_latest_summary():
    data = official([person(current_term_state="NY", current_term_district="9")], [term()])
    resolved = link(member(), data)
    assert resolved["bioguide_id"] == "A000001"
    assert resolved["resolution_rule"] == "exact_historical_name_context"
    assert json.loads(resolved["term_candidates_json"])["A000001"][0]["term_district"] == "7"


def test_bioguide_and_crosswalk_orders_and_literal_state_not_overwritten():
    data = official([person(lis_id="S001", govtrack_id="123", fec_ids_json='["H1CA00001"]')])
    for scheme, value, rule in [
        ("bioguide", "A000001", "explicit_bioguide"),
        ("lis", "S001", "exact_crosswalk"),
        ("govtrack", "123", "exact_crosswalk"),
        ("fec", "H1CA00001", "exact_crosswalk"),
    ]:
        row = member(identifiers_json=json.dumps([{"scheme": scheme, "value": value}]))
        row["state"] = "California"
        result = link(row, data)
        assert result["bioguide_id"] == "A000001"
        assert result["resolution_rule"] == rule
        assert json.loads(result["source_context_json"])["state"] == "California"


@pytest.mark.parametrize("scheme", ["votesmart", "votesmart_id"])
def test_votesmart_exact_crosswalk_preserves_source_literal_and_string_output(scheme):
    row = member(identifiers_json=json.dumps([{"scheme": scheme, "value": "123456"}]))
    row["member_name"] = "Publisher spelling without a name match"
    before = deepcopy(row)
    result = link(row, official([person(votesmart_id="123456")]))
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_rule"] == "exact_crosswalk"
    assert result["candidate_count"] == "1"
    assert "votesmart_id" in OFFICIAL_COLUMNS["members"]
    assert json.loads(result["source_context_json"])["identifiers_json"] == row["identifiers_json"]
    assert all(value is None or isinstance(value, str) for value in result.values())
    assert row == before


def test_votesmart_crosswalk_collision_retains_all_candidates_without_name_tiebreak():
    data = official(
        [person(votesmart_id="123456"), person("B000001", "Bob", "Jones", votesmart_id="123456")],
        [term(), term("B000001")],
    )
    result = link(member(identifiers_json='[{"scheme":"votesmart","value":"123456"}]'), data)
    assert result["resolution_status"] == "ambiguous"
    assert result["bioguide_id"] is None
    assert result["resolution_rule"] == "exact_crosswalk"
    assert result["candidate_count"] == "2"
    assert [row["bioguide_id"] for row in json.loads(result["candidates_json"])] == ["A000001", "B000001"]


def test_votesmart_duplicate_can_be_narrowed_by_exact_historical_context_with_candidates_retained():
    data = official(
        [person(votesmart_id="123456"), person("B000001", "Bob", "Jones", votesmart_id="123456")],
        [term(), term("B000001", state="NY")],
    )
    result = link(member(identifiers_json='[{"scheme":"votesmart","value":"123456"}]'), data)
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_status"] == "resolved"
    assert result["candidate_count"] == "2"
    assert json.loads(result["term_candidates_json"])["B000001"] == []


def test_previous_bioguide_resolves_exactly_and_retains_the_stated_id():
    row = member(identifiers_json='[{"scheme":"bioguide","value":"P000001"}]')
    data = official([person(bioguide_previous_json='["P000001"]')])
    result = link(row, data)
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_rule"] == "explicit_bioguide"
    assert "bioguide_previous" in json.loads(result["candidates_json"])[0]["matched_by"]
    assert json.loads(result["source_context_json"])["identifiers_json"] == row["identifiers_json"]


def test_previous_bioguide_collisions_and_contradictions_do_not_fall_through():
    data = official(
        [person(bioguide_previous_json='["P000001"]'), person("P000001", "Bob", "Jones")], [term(), term("P000001")]
    )
    row = member(identifiers_json='[{"scheme":"bioguide","value":"P000001"}]')
    result = link(row, data)
    assert result["resolution_status"] == "ambiguous"
    assert result["candidate_count"] == "2"
    assert result["bioguide_id"] is None
    row["state"] = "NY"
    assert link(row, data)["resolution_status"] == "conflict"
    data = official(
        [person(bioguide_previous_json='["P000001"]'), person("B000001", "Bob", "Jones")], [term(), term("B000001")]
    )
    row = member(identifiers_json='[{"scheme":"bioguide","value":"P000001"},{"scheme":"bioguide","value":"B000001"}]')
    assert link(row, data)["resolution_status"] == "conflict"


def test_dated_alias_partial_patch_uses_exact_historical_context_and_preserves_evidence():
    alias = {"last": "Earlier", "middle": None, "end": "2024-01-01"}
    data = official([person(other_names_json=json.dumps([alias]))])
    row = member(period_text="2023-12-31")
    row["member_name"] = "Alice Earlier"
    result = link(row, data)
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_rule"] == "exact_historical_name_context"
    candidate = json.loads(result["candidates_json"])[0]
    assert candidate["alias_matches"][0]["source_patch"] == alias
    assert candidate["alias_matches"][0]["alias_index"] == 0
    row.update(state=None, district=None, member_name="EARLIER, ALICE")
    assert link(row, data)["resolution_rule"] == "unique_normalized_name_historical_term"
    row["member_name"] = "Alic Earlier"
    assert link(row, data)["resolution_status"] == "unresolved"


@pytest.mark.parametrize(
    "alias",
    [
        {"last": "Earlier"},
        {"last": "Earlier", "end": None},
        {"last": "Earlier", "end": "2023-02-30"},
        {"last": "Earlier", "end": "unknown"},
        {"last": "Earlier", "start": "2024-02-01", "end": "2024-01-01"},
        {"last": "Earlier", "end": "2025-01-01", "unknown_qualifier": "conditional"},
    ],
)
def test_undated_invalid_or_unqualified_aliases_cannot_match_unrestricted(alias):
    data = official([person(other_names_json=json.dumps([alias]))])
    row = member(period_text="2024")
    row["member_name"] = "Alice Earlier"
    result = link(row, data)
    assert result["resolution_status"] == "unresolved"
    assert result["bioguide_id"] is None
    assert result["candidate_count"] == "1"
    assert not json.loads(result["candidates_json"])[0]["alias_matches"][0]["context_match"]


@pytest.mark.parametrize("period", ["2023-12-31", "2024-02-01", "2024-02-02", "Lifetime"])
def test_alias_stated_interval_never_expands_to_other_source_periods(period):
    data = official([person(other_names_json='[{"last":"Earlier","start":"2024-01-01","end":"2024-02-01"}]')])
    row = member(period_text=period)
    row["member_name"] = "Alice Earlier"
    assert link(row, data)["resolution_status"] == "unresolved"
    row["period_text"] = "2024-01-01"
    assert link(row, data)["bioguide_id"] == "A000001"


def test_alias_must_overlap_the_same_term_and_period_and_keep_collisions():
    alias = '[{"last":"Earlier","start":"2023-01-01","end":"2024-01-01"}]'
    data = official([person(other_names_json=alias)], [term(start="2024-01-03", end="2025-01-03")])
    row = member(period_text="118th Congress")
    row["member_name"] = "Alice Earlier"
    assert link(row, data)["resolution_status"] == "unresolved"
    data = official(
        [person(other_names_json=alias), person("B000001", other_names_json=alias)], [term(), term("B000001")]
    )
    result = link(row, data)
    assert result["resolution_status"] == "ambiguous"
    assert result["candidate_count"] == "2"
    row["identifiers_json"] = '[{"scheme":"bioguide","value":"MISSING"}]'
    assert link(row, data)["resolution_status"] == "conflict"


def test_alias_term_end_fallback_updates_candidate_evidence_without_extending_alias_end():
    data = official([person(other_names_json='[{"last":"Earlier","end":"2025-02-01"}]')])
    row = member(period_text="2025-01-03")
    row["member_name"] = "Alice Earlier"
    result = link(row, data)
    assert result["bioguide_id"] == "A000001"
    assert json.loads(result["candidates_json"])[0]["alias_matches"][0]["context_match"]
    assert "historical_alias_outside_context" not in result["reason"]
    data["members"][0]["other_names_json"] = '[{"last":"Earlier","end":"2025-01-03"}]'
    assert link(row, data)["resolution_status"] == "unresolved"


@pytest.mark.parametrize(
    "ids",
    [
        [{"scheme": "votesmart", "value": "missing"}],
        [{"scheme": "votesmart", "value": "0123456"}],
        [{"scheme": "votesmart", "value": 123456}],
        [{"scheme": "bioguide", "value": "A000001"}, {"scheme": "votesmart", "value": "654321"}],
    ],
)
def test_unknown_malformed_or_conflicting_votesmart_id_refuses_weaker_name_match(ids):
    data = official(
        [person(votesmart_id="123456"), person("B000001", "Bob", "Jones", votesmart_id="654321")],
        [term(), term("B000001")],
    )
    result = link(member(identifiers_json=json.dumps(ids)), data)
    assert result["resolution_status"] == "conflict"
    assert result["bioguide_id"] is None
    assert result["resolution_rule"] == "conflicting_or_unknown_identifiers"


def test_unknown_id_or_conflicting_ids_do_not_fall_through_to_name_or_override():
    data = official(
        [person(lis_id="S001"), person("B000001", "Bob", "Jones", lis_id="S002")], [term(), term("B000001")]
    )
    override = {
        "scorecard_id": "publisher:118",
        "publisher_member_key": "source-person",
        "bioguide_id": "A000001",
        "version": "review-1",
        "reason": "reviewed source spelling",
    }
    for ids in [
        [{"scheme": "bioguide", "value": "MISSING"}],
        [{"scheme": "bioguide", "value": "A000001"}, {"scheme": "lis", "value": "S002"}],
    ]:
        result = link(member(identifiers_json=json.dumps(ids)), data, member_overrides=[override])
        assert result["resolution_status"] == "conflict"
        assert result["bioguide_id"] is None
        assert result["override_version"] is None
    assert result["candidate_count"] == "2"


@pytest.mark.parametrize(
    "change", [{"state": "NY"}, {"district": "8"}, {"chamber_text": "Senate"}, {"period_text": "2010"}]
)
def test_explicit_id_with_contradictory_term_context_is_a_conflict(change):
    row = member(identifiers_json='[{"scheme":"bioguide","value":"A000001"}]')
    row.update(change)
    result = link(row)
    assert result["resolution_status"] == "conflict"
    assert result["resolution_rule"] == "identifier_context_conflict"
    assert result["bioguide_id"] is None


def test_redistricting_and_chamber_change_use_source_period():
    data = official(
        terms=[
            term(end="2023-01-03", start="2021-01-03", district="3"),
            term(index="1", start="2023-01-03", end="2025-01-03", district="7"),
            term(index="2", start="2025-01-03", end="2031-01-03", district=None, kind="sen"),
        ]
    )
    row = member(period_text="2022")
    row["district"] = "3"
    assert link(row, data)["bioguide_id"] == "A000001"
    row.update(period_text="2024", district="3")
    assert link(row, data)["resolution_status"] == "unresolved"
    row.update(period_text="2025-01-03", district=None, chamber_text="Senate")
    assert link(row, data)["bioguide_id"] == "A000001"
    row.update(chamber_text="House", district="7")
    result = link(row, data)
    assert "unique_inclusive_term_end_fallback" in result["reason"]


def test_unique_normalized_name_is_exact_not_fuzzy_and_needs_historical_period():
    row = member()
    row.update(member_name="SMITH, ALICE", state=None, district=None)
    result = link(row)
    assert result["resolution_rule"] == "unique_normalized_name_historical_term"
    row["member_name"] = "Alic Smith"
    assert link(row)["resolution_status"] == "unresolved"
    row.update(member_name="Alice Smith", period_text="Lifetime")
    assert link(row)["resolution_status"] == "unresolved"


def test_source_last_first_order_is_exact_and_does_not_hide_reversed_name_ambiguity():
    row = member()
    row["member_name"] = "Smith Alice"
    assert link(row)["resolution_rule"] == "exact_historical_name_context"
    data = official([person(), person("B000001", "Smith", "Alice")], [term(), term("B000001")])
    assert link(row, data)["resolution_status"] == "ambiguous"


def test_ambiguous_names_preserve_candidates_and_versioned_override_is_last():
    data = official([person(), person("B000001")], [term(), term("B000001")])
    result = link(member(), data)
    assert result["resolution_status"] == "ambiguous"
    assert result["candidate_count"] == "2"
    assert [row["bioguide_id"] for row in json.loads(result["candidates_json"])] == ["A000001", "B000001"]
    override = {
        "scorecard_id": "publisher:118",
        "publisher_member_key": "source-person",
        "bioguide_id": "B000001",
        "version": "review-7",
        "reason": "source publisher ID manually crosswalked",
    }
    result = link(member(), data, member_overrides=[override])
    assert (result["bioguide_id"], result["override_version"]) == ("B000001", "review-7")
    assert result["resolution_rule"] == "versioned_override"
    assert result["candidate_count"] == "2"
    del override["version"]
    with pytest.raises(ValueError, match="version"):
        link(member(), data, member_overrides=[override])


def test_unknown_nes_state_is_preserved_and_explained_not_repaired_to_ne():
    row = member()
    row["state"] = "NES"
    result = link(row)
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_rule"] == "unique_normalized_name_historical_term"
    assert "unknown_state_literal_ignored:NES" in result["reason"]
    assert json.loads(result["source_context_json"])["state"] == "NES"


def test_explicit_source_periods_precede_release_year_and_unreadable_period_blocks_fallback():
    data = official(terms=[term(start="2019-01-03", end="2021-01-03")])
    periods = [{"occurrence_id": "p1", "kind": "explicit", "period_text": "116th Congress", "source_path": "coverage"}]
    source = sources([member()], year_text="2024", periods_json=json.dumps(periods))
    result = resolve_scorecard_links(source, data, pins())["scorecard_member_links"][0]
    assert result["bioguide_id"] == "A000001"
    periods[0]["period_text"] = "2019 through part of 2023"
    source["scorecards"][0]["periods_json"] = json.dumps(periods)
    result = resolve_scorecard_links(source, official(), pins())["scorecard_member_links"][0]
    assert result["resolution_status"] == "unresolved"
    assert "no_exact_historical_period" in (result["reason"] or "")


def test_heritage_cong_id_remains_an_unqualified_scheme_not_a_bioguide_crosswalk():
    row = member(identifiers_json='[{"scheme":"cong_id","value":"A000001"}]')
    row.update(member_name="Unmatched publisher spelling")
    result = link(row)
    assert result["resolution_status"] == "unresolved"
    assert result["bioguide_id"] is None
    assert "unrecognized_identifier_scheme:cong_id" in result["reason"]


def test_senate_zero_district_is_not_applicable_but_house_zero_remains_at_large():
    row = member(chamber_text="Senate")
    row["district"] = "00"
    data = official(terms=[term(kind="sen", district=None)])
    result = link(row, data)
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_rule"] == "exact_historical_name_context"
    assert "senate_district_not_applicable_literal:00" in result["reason"]
    assert json.loads(result["source_context_json"])["district"] == "00"
    row["chamber_text"] = "House"
    assert link(row)["resolution_status"] == "unresolved"
    assert link(row, official(terms=[term(district="0")]))["bioguide_id"] == "A000001"


def test_crosswalk_collision_is_ambiguous_unless_historical_context_selects_one():
    data = official(
        [person(lis_id="S001"), person("B000001", "Bob", "Jones", lis_id="S001")], [term(), term("B000001")]
    )
    row = member(identifiers_json='[{"scheme":"lis","value":"S001"}]')
    assert link(row, data)["resolution_status"] == "ambiguous"
    data["member_terms"][1]["term_state"] = "NY"
    result = link(row, data)
    assert result["bioguide_id"] == "A000001"
    assert result["candidate_count"] == "2"


def test_exact_roll_call_and_multiple_actions_on_one_bill_stay_separate():
    source_items = [
        fact(
            item_id=f"motion-{roll}",
            item_kind_text="roll_call",
            congress_text="118",
            chamber_text="house",
            session_text="1",
            roll_number_text=roll,
            bill_citation_text="H.R. 1",
        )
        for roll in ["4", "5"]
    ]
    results = resolve_scorecard_links(sources(items=source_items), official(), pins())["scorecard_item_links"]
    assert [row["vote_id"] for row in results] == ["118-house-1-4", "118-house-1-5"]
    assert all(row["bill_id"] == "118-hr-1" for row in results)


def test_bill_or_cosponsorship_never_selects_a_roll_call():
    for kind in ["cosponsorship", "bill", "sponsorship"]:
        row = item_link(
            item_kind_text=kind,
            congress_text="118",
            chamber_text="house",
            session_text="1",
            roll_number_text="4",
            bill_citation_text="H.R. 1",
        )[0]
        assert row["bill_id"] == "118-hr-1"
        assert row["vote_id"] is row["session"] is row["roll_number"] is None
    row = item_link(bill_citation_text="118-hr-1")[0]
    assert row["vote_id"] is None
    assert row["resolution_rule"] == "exact_bill"


def test_niac_old_action_does_not_inherit_edition_congress():
    row = item_link(item_date_text="2015", bill_citation_text="H.R. 1")[0]
    assert row["resolution_status"] == "unresolved"
    assert row["congress"] is row["bill_id"] is None
    assert "missing_source_congress" in row["reason"]


def test_exact_amendment_and_unresolved_citation():
    row = item_link(congress_text="118", amendment_citation_text="H.Amdt. 3")[0]
    assert row["amendment_id"] == "118-hamdt-3"
    assert row["vote_id"] is None
    row = item_link(congress_text="118", bill_citation_text="the climate measure")[0]
    assert row["resolution_status"] == "unresolved"
    assert "unsupported_bill_citation_text" in row["reason"]


def test_sampled_upstream_bill_dialect_resolves_only_as_an_explicit_citation():
    # Heritage publishes identifier=hr4274-115 and current s382-119; the
    # type-number-Congress grammar is pinned in the ecosystem research memo.
    row = item_link(item_kind_text="cosponsorship", bill_citation_text="hr1-118")[0]
    assert row["bill_id"] == "118-hr-1"
    assert row["resolution_rule"] == "exact_bill"
    assert row["vote_id"] is row["session"] is row["roll_number"] is None
    assert json.loads(row["source_context_json"])["item"]["bill_citation_text"] == "hr1-118"
    row = item_link(publisher_item_id="hr1-118")[0]
    assert row["resolution_status"] == "unresolved"
    assert row["bill_id"] is None
    row = item_link(congress_text="119", bill_citation_text="hr1-118")[0]
    assert row["resolution_status"] == "conflict"
    assert row["bill_id"] is None


@pytest.mark.parametrize(
    ("kind", "citation"), [("amendment", "hamdt3-118-extra"), ("roll_call", "h4-118.2023"), ("roll_call", "h4-2023")]
)
def test_unqualified_identifier_dialects_remain_literal_and_unresolved(kind, citation):
    reference = {"occurrence_id": "source-id", "kind": kind, "citation_text": citation, "source_path": "item/id"}
    row = item_link(references_json=json.dumps([reference]))[0]
    assert row["resolution_status"] == "unresolved"
    assert row["amendment_id"] is row["vote_id"] is row["session"] is None
    assert json.loads(row["source_context_json"])["reference"]["citation_text"] == citation


def test_upstream_amendment_dialect_is_anchored_and_measure_only():
    references = [
        {
            "occurrence_id": "amendment",
            "kind": "amendment",
            "citation_text": "hamdt3-118",
            "source_path": "amendment/id",
        }
    ]
    row = item_link(references_json=json.dumps(references))[0]
    assert row["resolution_status"] == "resolved"
    assert row["amendment_id"] == "118-hamdt-3"
    assert row["vote_id"] is None
    row = item_link(congress_text="119", amendment_citation_text="hamdt3-118")[0]
    assert row["resolution_status"] == "conflict"


def upstream_vote_link(data, citation="h4-118.2023", **context):
    reference = {
        "occurrence_id": "vote",
        "kind": "roll_call",
        "citation_text": citation,
        "source_path": "vote/id",
        **context,
    }
    source = sources(items=[fact(item_id="source-vote", references_json=json.dumps([reference]))])
    return resolve_scorecard_links(source, data, pins())["scorecard_item_links"][0]


def calendar_item_link(data, **fields):
    source = sources(items=[fact(item_id="source-item", chamber_text="House", roll_number_text="4", **fields)])
    before = deepcopy(source)
    result = resolve_scorecard_links(source, data, pins())["scorecard_item_links"][0]
    assert source == before
    return result


def test_literal_item_year_joins_official_dates_without_edition_or_calendar_inference():
    data = official()
    data["roll_call_votes"] = [{"vote_id": "119-house-3-4", "bill_id": None, "vote_date": "3-Jan-2025"}]
    row = calendar_item_link(data, item_date_text="2025")
    assert row["vote_id"] == "119-house-3-4"
    assert row["congress"] == "119"
    assert row["session"] == "3"
    assert row["resolution_status"] == "resolved"
    assert json.loads(row["candidates_json"])[0]["vote_date"] == "3-Jan-2025"
    for year in (None, "", "unknown", "2024"):
        assert calendar_item_link(data, item_date_text=year)["vote_id"] is None
    source = sources(items=[fact(item_id="i", chamber_text="House", roll_number_text="4")], year_text="2025")
    assert resolve_scorecard_links(source, data, pins())["scorecard_item_links"][0]["vote_id"] is None


def test_literal_item_year_preserves_candidates_and_rejects_conflicting_context():
    data = official()
    data["roll_call_votes"] = [
        {"vote_id": f"{congress}-house-{session}-4", "bill_id": None, "vote_date": "3-Jan-2025"}
        for congress, session in ((118, 2), (119, 1))
    ]
    row = calendar_item_link(data, item_date_text="2025")
    assert row["resolution_status"] == "ambiguous"
    assert row["candidate_count"] == "2"
    assert row["vote_id"] is None
    assert calendar_item_link(data, item_date_text="2025", congress_text="119")["vote_id"] == "119-house-1-4"
    for context in ({"congress_text": "120"}, {"congress_text": "119", "session_text": "2"}):
        row = calendar_item_link(data, item_date_text="2025", **context)
        assert row["resolution_status"] == "conflict"
        assert row["vote_id"] is None


@pytest.mark.parametrize("literal", [None, "", "2025-01-03", "30-Feb-2025"])
def test_literal_item_year_requires_valid_official_chamber_date(literal):
    data = official()
    data["roll_call_votes"][0]["vote_date"] = literal
    row = calendar_item_link(data, item_date_text="2025", congress_text="118", session_text="1")
    assert row["resolution_status"] == "unresolved"
    assert row["vote_id"] is None


@pytest.mark.parametrize("citation_year,item_year", [("2023", "2024"), ("2024", "2023")])
def test_item_year_and_typed_vote_year_conflict_clears_all_targets(citation_year, item_year):
    data = official()
    data["roll_call_votes"][0]["vote_date"] = "3-Jan-2023"
    reference = {
        "occurrence_id": "vote",
        "kind": "roll_call",
        "citation_text": f"h4-118.{citation_year}",
        "source_path": "vote/id",
    }
    source = sources(
        items=[fact(item_id="source-vote", item_date_text=item_year, references_json=json.dumps([reference]))]
    )
    row = resolve_scorecard_links(source, data, pins())["scorecard_item_links"][0]
    assert row["resolution_status"] == "conflict"
    assert "contradictory_source_vote_years" in (row["reason"] or "")
    assert row["vote_id"] is row["session"] is row["bill_id"] is None


@pytest.mark.parametrize("kind", ["bill", "sponsorship", "cosponsorship", "committee_action", "committee_vote"])
def test_literal_item_year_cannot_turn_non_floor_action_into_roll(kind):
    data = official()
    data["roll_call_votes"][0]["vote_date"] = "3-Jan-2025"
    row = calendar_item_link(data, item_date_text="2025", item_kind_text=kind)
    assert row["vote_id"] is None
    assert row["session"] is None


def test_upstream_vote_year_uses_actual_official_date_and_session_without_arithmetic():
    data = official()
    # A special-session fixture deliberately differs from modern year arithmetic.
    data["roll_call_votes"] = [{"vote_id": "118-house-3-4", "bill_id": "118-hr-1", "vote_date": "3-Jan-2023"}]
    row = upstream_vote_link(data)
    assert row["resolution_status"] == "resolved"
    assert row["vote_id"] == "118-house-3-4"
    assert row["session"] == "3"
    assert "upstream_vote_year_matched_official_date_and_session" in row["reason"]
    assert json.loads(row["candidates_json"])[0]["vote_date"] == "3-Jan-2023"
    assert upstream_vote_link(data, "h4-118.2024")["resolution_status"] == "unresolved"
    assert upstream_vote_link(data, "h4-118.2023suffix")["resolution_status"] == "unresolved"
    data["roll_call_votes"] = [
        {"vote_id": "118-senate-2-4", "bill_id": None, "vote_date": "January 3, 2023,  02:54 PM"}
    ]
    assert upstream_vote_link(data, "s4-118.2023")["vote_id"] == "118-senate-2-4"


def test_upstream_vote_candidates_preserve_ambiguity_and_explicit_conflicts():
    data = official()
    data["roll_call_votes"] = [
        {"vote_id": f"118-house-{session}-4", "bill_id": "118-hr-1", "vote_date": "3-Jan-2023"} for session in (1, 3)
    ]
    row = upstream_vote_link(data)
    assert row["resolution_status"] == "ambiguous"
    assert row["candidate_count"] == "2"
    assert row["session"] is row["vote_id"] is None
    row = upstream_vote_link(data, session_text="3")
    assert row["vote_id"] == "118-house-3-4"
    assert row["candidate_count"] == "2"
    for context in (
        {"session_text": "2"},
        {"congress_text": "119"},
        {"chamber_text": "Senate"},
        {"roll_number_text": "5"},
        {"bill_citation_text": "118-s-2"},
    ):
        row = upstream_vote_link(data, **({"session_text": "3"} | context))
        assert row["resolution_status"] == "conflict"
        assert row["vote_id"] is row["session"] is None


@pytest.mark.parametrize("literal", [None, "", "unknown", "2023-01-03", "30-Feb-2023"])
def test_upstream_vote_without_qualified_chamber_date_cannot_guess_session(literal):
    data = official()
    data["roll_call_votes"][0]["vote_date"] = literal
    row = upstream_vote_link(data)
    assert row["resolution_status"] == "unresolved"
    assert row["vote_id"] is row["session"] is None
    assert "no_official_vote_with_qualified_source_year" in row["reason"]


@pytest.mark.parametrize("native_citation", [False, True])
def test_calendar_year_never_becomes_an_ordinal_session(native_citation):
    data = official()
    # Even an incorrect input-table row cannot make the year an ordinal.
    data["roll_call_votes"].append({"vote_id": "118-house-2023-4", "bill_id": "118-hr-1"})
    fields = {"congress_text": "118", "chamber_text": "House", "session_text": "2023", "roll_number_text": "4"}
    if native_citation:
        fields = {
            "references_json": json.dumps(
                [
                    {
                        "occurrence_id": "source-id",
                        "kind": "roll_call",
                        "citation_text": "118-house-2023-4",
                        "source_path": "id",
                    }
                ]
            )
        }
    source = sources(items=[fact(item_id="source-item", **fields)])
    row = resolve_scorecard_links(source, data, pins())["scorecard_item_links"][0]
    assert row["resolution_status"] == "unresolved"
    assert row["session"] is row["vote_id"] is None
    assert "calendar_year_session_not_ordinal" in (row["reason"] or "")


@pytest.mark.parametrize(
    "fields",
    [
        {"congress_text": "119", "bill_citation_text": "118-hr-1"},
        {"congress_text": "118", "bill_citation_text": "S.2", "amendment_citation_text": "H.Amdt.3"},
        {
            "congress_text": "118",
            "bill_citation_text": "S.2",
            "chamber_text": "house",
            "session_text": "1",
            "roll_number_text": "4",
        },
    ],
)
def test_conflicting_legislative_identifiers_select_no_target(fields):
    row = item_link(**fields)[0]
    assert row["resolution_status"] == "conflict"
    assert row["bill_id"] is row["amendment_id"] is row["vote_id"] is None
    assert row["congress"] is row["chamber"] is row["session"] is row["roll_number"] is None
    assert json.loads(row["candidates_json"])


def test_references_preserve_occurrences_and_namespaces_and_do_not_borrow_item_context():
    references = [
        {
            "occurrence_id": identity,
            "kind": "bill",
            "citation_text": "H.R. 1",
            "congress_text": "118",
            "source_path": f"paragraph/{identity}",
        }
        for identity in ["item:direct", "second"]
    ]
    references.append({"occurrence_id": "older", "kind": "bill", "citation_text": "H.R. 1", "source_path": "older"})
    rows = item_link(congress_text="118", bill_citation_text="H.R.1", references_json=json.dumps(references))
    assert [row["reference_id"] for row in rows] == [
        "item:direct",
        "reference:item:direct",
        "reference:second",
        "reference:older",
    ]
    assert rows[1]["source_reference_id"] == "item:direct"
    assert rows[1]["source_path"] == "paragraph/item:direct"
    assert rows[3]["bill_id"] is None
    assert rows[3]["congress"] is None


def test_exact_natural_roll_reference_and_committee_boundary():
    refs = [
        {"occurrence_id": "r1", "kind": "roll_call", "citation_text": "118-house-1-4", "source_path": "reference/1"}
    ]
    assert item_link(references_json=json.dumps(refs))[0]["vote_id"] == "118-house-1-4"
    row = item_link(
        item_kind_text="committee_action",
        congress_text="118",
        bill_citation_text="H.R.1",
        chamber_text="house",
        session_text="1",
        roll_number_text="4",
    )[0]
    assert row["vote_id"] is None
    assert "committee_action_not_mapped_to_floor_roll" in row["reason"]


def test_missing_or_mutable_pins_and_snapshot_mismatch_refuse():
    pinned = pins()
    pinned["members"]["sha256"] = "https://example.org/current/members.parquet"
    with pytest.raises(ValueError, match="sha256"):
        resolve_scorecard_links(sources([member()]), official(), pinned)
    row = member()
    row["snapshot_id"] = "wrong"
    with pytest.raises(ValueError, match="snapshot"):
        link(row)


def test_partitioned_descriptor_pin_is_retained_without_inventing_file_sha256():
    pinned = pins()
    split = pinned["congress_bills"]
    split["tableDescriptorDigest"] = split.pop("sha256")
    split["layout"] = {"kind": "partitioned", "members": [{"key": "immutable/bill-1.parquet", "byteSize": 31}]}
    output = resolve_scorecard_links(sources([member()]), official(), pinned)
    retained_text = output["scorecard_member_links"][0]["input_pins_json"]
    assert isinstance(retained_text, str)
    retained = json.loads(retained_text)
    assert retained == pinned
    assert "sha256" not in retained["congress_bills"]
    split["sha256"] = "sha256:" + "a" * 64
    with pytest.raises(ValueError, match="exactly one"):
        resolve_scorecard_links(sources([member()]), official(), pinned)


def test_explicit_id_uses_unique_inclusive_term_end_fallback():
    row = member(period_text="2025-01-03", identifiers_json='[{"scheme":"bioguide","value":"A000001"}]')
    result = link(row)
    assert result["bioguide_id"] == "A000001"
    assert result["resolution_rule"] == "explicit_bioguide"
    assert "unique_inclusive_term_end_fallback" in result["reason"]


def test_outputs_are_stable_all_varchar_and_inputs_and_ratings_stay_unchanged():
    source = sources([member()], [fact(item_id="bill", congress_text="118", bill_citation_text="H.R.1")])
    source["scorecard_member_ratings"] = [fact(value_text="A+", publisher_member_key="source-person")]
    data, pinned = official(), pins()
    before = deepcopy((source, data, pinned))
    a = resolve_scorecard_links(source, data, pinned)
    b = resolve_scorecard_links(source, data, pinned)
    assert a == b
    assert (source, data, pinned) == before
    for table, rows in a.items():
        for row in rows:
            assert tuple(row) == LINK_COLUMNS[table]
            assert all(value is None or isinstance(value, str) for value in row.values())
            retained_text = row["input_pins_json"]
            assert isinstance(retained_text, str)
            assert json.loads(retained_text) == pinned
            assert row["source_snapshot_id"] == "publisher:118:snapshot"


def test_duplicate_source_ids_or_official_ids_refuse():
    with pytest.raises(ValueError, match="repeated identity"):
        resolve_scorecard_links(sources([member(), member()]), official(), pins())
    with pytest.raises(ValueError, match="repeated identity"):
        link(member(), official([person(), person()]))


def edition_measure(fields, *, edition=None, data=None):
    source = sources(items=[fact(item_id="edition-measure", **fields)])
    source["scorecards"][0].update(edition or {})
    before = deepcopy(source)
    result = resolve_scorecard_links(source, data or official(), pins())["scorecard_item_links"]
    assert source == before
    return result


@pytest.mark.parametrize(
    ("field", "citation", "target", "identifier"),
    [
        ("bill_citation_text", "H.R. 1", "bill_id", "118-hr-1"),
        ("amendment_citation_text", "H.Amdt. 3", "amendment_id", "118-hamdt-3"),
    ],
)
def test_explicit_edition_congress_qualifies_only_the_unstated_measure_context(field, citation, target, identifier):
    row = edition_measure({field: citation, "item_date_text": "March 28, 2023"})[0]
    assert row[target] == identifier and row["congress"] == "118"
    assert row["vote_id"] is row["session"] is row["roll_number"] is None
    assert row["resolution_rule"].endswith("_edition_congress")
    assert row["rule_version"] == "scorecard-resolution-v1.4"
    assert "explicit_edition_congress_for_measure" in row["reason"]
    context = json.loads(row["source_context_json"])
    assert context["item"].get("congress_text") is None
    assert context["reference"].get("congress_text") is None
    assert context["edition"]["congress_text"] == "118th Congress"


def test_edition_context_preserves_independent_citation_occurrences():
    references = [
        {"occurrence_id": "bill", "kind": "bill", "citation_text": "H.R. 1", "source_path": "items/0/bill"},
        {"occurrence_id": "amend", "kind": "amendment", "citation_text": "H.Amdt. 3", "source_path": "items/0/amend"},
    ]
    rows = edition_measure({"references_json": json.dumps(references)})
    assert [r["reference_id"] for r in rows] == ["reference:bill", "reference:amend"]
    assert rows[0]["bill_id"] == "118-hr-1" and rows[1]["amendment_id"] == "118-hamdt-3"
    assert all(r["vote_id"] is None for r in rows)
    assert [json.loads(r["source_context_json"])["reference"] for r in rows] == references


@pytest.mark.parametrize("literal", ["117-hr-1", "hr1-117"])
def test_edition_context_never_replaces_an_embedded_historical_congress(literal):
    data = official()
    data["congress_bills"].append({"bill_id": "117-hr-1"})
    row = edition_measure({"bill_citation_text": literal}, data=data)[0]
    assert row["bill_id"] == "117-hr-1" and row["congress"] == "117"
    assert row["resolution_rule"] == "exact_bill"
    assert "explicit_edition_congress_for_measure" not in row["reason"]


@pytest.mark.parametrize("context", ["117", "unknown", "117 / 118"])
def test_reference_cannot_use_edition_over_explicit_item_congress(context):
    reference = {"occurrence_id": "bill", "kind": "bill", "citation_text": "H.R. 1", "source_path": "bill"}
    rows = edition_measure({"congress_text": context, "references_json": json.dumps([reference])})
    row = next(r for r in rows if r["reference_id"] == "reference:bill")
    assert row["bill_id"] is None and "explicit_edition_congress_for_measure" not in row["reason"]


@pytest.mark.parametrize("context", ["unknown", "117 / 118"])
def test_edition_cannot_repair_malformed_reference_congress(context):
    reference = {
        "occurrence_id": "bill",
        "kind": "bill",
        "citation_text": "H.R. 1",
        "source_path": "bill",
        "congress_text": context,
    }
    row = edition_measure({"references_json": json.dumps([reference])})[0]
    assert row["bill_id"] is None and row["congress"] is None


@pytest.mark.parametrize(
    "edition",
    [
        {"congress_text": None},
        {"congress_text": "117 and 118"},
        {"congress_text": "0"},
        {"timespan_text": "Lifetime"},
        {"year_text": "2015"},
        {"periods_json": '[{"kind":"relative","period_text":"current and prior six sessions"}]'},
        {"periods_json": '[{"kind":"explicit","period_text":"117th Congress"}]'},
        {"periods_json": '[{"kind":"explicit","period_text":"118th Congress","congress_text":"117"}]'},
    ],
)
def test_edition_fallback_requires_single_consistent_explicit_scope(edition):
    row = edition_measure({"bill_citation_text": "H.R. 1"}, edition=edition)[0]
    assert row["bill_id"] is None and row["congress"] is None


@pytest.mark.parametrize("period", ["2015", "2015-04-01", "April 1, 2015", "March 32, 2023", "yesterday", "2025-01-03"])
def test_historical_or_malformed_item_dates_prevent_edition_fallback(period):
    row = edition_measure({"bill_citation_text": "H.R. 1", "item_date_text": period})[0]
    assert row["bill_id"] is None and row["congress"] is None
    assert "historical_or_unqualified_period" in row["reason"]


def test_edition_context_never_supplies_roll_or_calendar_session():
    for extra in ({"chamber_text": "House", "roll_number_text": "4", "session_text": "1"}, {"session_text": "2023"}):
        row = edition_measure({"bill_citation_text": "H.R. 1", **extra})[0]
        assert row["bill_id"] is None and row["vote_id"] is None
        assert "explicit_edition_congress_for_measure" not in row["reason"]


def test_mixed_historical_reference_context_prevents_guessing_for_unnumbered_citation():
    refs = [
        {"occurrence_id": "current-unknown", "kind": "bill", "citation_text": "H.R. 1", "source_path": "a"},
        {"occurrence_id": "historical", "kind": "bill", "citation_text": "117-hr-1", "source_path": "b"},
    ]
    data = official()
    data["congress_bills"].append({"bill_id": "117-hr-1"})
    rows = edition_measure({"references_json": json.dumps(refs)}, data=data)
    assert rows[0]["bill_id"] is None and rows[1]["bill_id"] == "117-hr-1"


@pytest.mark.parametrize("citation", ["117-house-1-4", "h4-117.2021", "unknown"])
def test_edition_context_does_not_qualify_measure_beside_a_separate_roll_reference(citation):
    refs = [
        {"occurrence_id": "bill", "kind": "bill", "citation_text": "H.R. 1", "source_path": "a"},
        {"occurrence_id": "roll", "kind": "roll_call", "citation_text": citation, "source_path": "b"},
    ]
    rows = edition_measure({"references_json": json.dumps(refs)})
    assert rows[0]["bill_id"] is None
    assert "edition_congress_not_used_with_sibling_vote_context" in rows[0]["reason"]
