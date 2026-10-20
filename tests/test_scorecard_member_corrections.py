"""Only the reviewed publisher error receives a correction before strict matching."""

from copy import deepcopy
import json

import pytest

from spicy_regs.scorecards.member_corrections import reviewed_member_context
from spicy_regs.scorecards.resolution import INPUT_TABLES, _Members, resolve_scorecard_links

# Exact selected source rows: original publisher values, promoted capture context.
# Original HTML 93d623f5..., GPO 103rd directory 5715936a..., PDF page 3.
SOURCE = {
    "scorecard_id": "lugar_mccourt:103-senate-html",
    "publisher_member_key": "Senate:Domenici:Pete:NH:R",
    "publisher_member_id": None,
    "identifiers_json": "[]",
    "member_name": "Domenici Pete",
    "chamber_text": "Senate",
    "state": "NH",
    "district": None,
    "party": "R",
    "period_text": None,
    "eligibility_text": None,
    "notes_text": '{"native_cells": [{"text": "7", "native_sheet_value": null}, {"text": "Domenici", '
    '"native_sheet_value": null}, {"text": "Pete", "native_sheet_value": null}, {"text": "NH", '
    '"native_sheet_value": null}, {"text": "R", "native_sheet_value": null}, {"text": "0.947105", '
    '"native_sheet_value": null}]}',
    "snapshot_id": "scorecard-snapshot:c919dccf-ae18-4765-a032-c68971d9a9bc",
    "capture_id": "315de095b665463eb19270a3e518a998",
    "source_url": "https://www.thelugarcenter.org/ourwork-Bipartisan-Index.html",
    "source_path": "html:#tab23-group/score-table[9]/tr[8]",
}
EDITION = {
    "scorecard_id": "lugar_mccourt:103-senate-html",
    "publisher_id": "lugar_mccourt",
    "series_id": "lugar_mccourt",
    "publisher_edition_key": "103-senate-html",
    "publisher_name_text": "The Lugar Center and Georgetown University McCourt School of Public Policy",
    "title": "Bipartisan Index Senate Scores — 103rd Congress",
    "edition_label_text": "103-senate-html",
    "chamber_scope_text": "Senate",
    "year_text": None,
    "congress_text": "103",
    "session_text": None,
    "timespan_text": "103rd Congress",
    "periods_json": '[{"occurrence_id": "edition", "kind": "explicit", "period_text": "103rd Congress", '
    '"source_path": "html:#tab23-group/score-table[9]"}]',
    "source_url": "https://www.thelugarcenter.org/ourwork-Bipartisan-Index.html",
    "methodology_url": "https://www.thelugarcenter.org/ourwork-44.html",
    "published_at_text": None,
    "updated_at_text": None,
    "observed_at": "2026-10-04T11:26:43.822441+00:00",
    "snapshot_id": "scorecard-snapshot:c919dccf-ae18-4765-a032-c68971d9a9bc",
    "capture_id": "315de095b665463eb19270a3e518a998",
    "source_path": "/",
}


def official():
    return {
        "members": [{"bioguide_id": "D000407", "name_first": "Pete", "name_last": "Domenici"}],
        "member_terms": [
            {
                "bioguide_id": "D000407",
                "term_index": "3",
                "term_type": "sen",
                "term_start": "1991-01-03",
                "term_end": "1997-01-03",
                "term_state": "NM",
                "term_district": None,
            }
        ],
        "congress_bills": [],
        "amendments": [],
        "roll_call_votes": [],
    }


def resolve(data=None, source=None):
    return resolve_scorecard_links(
        {
            "scorecards": [deepcopy(EDITION)],
            "scorecard_members": [deepcopy(SOURCE if source is None else source)],
            "scorecard_items": [],
        },
        official() if data is None else data,
        {
            name: {
                "sha256": "sha256:" + "a" * 64,
                "byteSize": 321,
                "family": "fixture",
                "artifactDigest": "sha256:" + "b" * 64,
                "layout": {"kind": "single"},
            }
            for name in INPUT_TABLES
        },
    )["scorecard_member_links"][0]


def test_domenici_correction_preserves_raw_and_round_trips_native_evidence(tmp_path):
    from spicy_regs.scorecards.etl import LINK_NAMES, read_family, write_family

    data = official()
    original = _Members(data["members"], data["member_terms"]).resolve(SOURCE, EDITION, None)
    assert original["resolution_status"] == "unresolved"
    assert original["bioguide_id"] is None
    source, edition = deepcopy(SOURCE), deepcopy(EDITION)
    adjusted, evidence = reviewed_member_context(source, edition)
    assert evidence is not None
    assert source == SOURCE and edition == EDITION
    assert adjusted == {**SOURCE, "state": "NM"}
    result = resolve()
    assert result["bioguide_id"] == "D000407"
    assert result["resolution_status"] == "resolved"
    assert result["resolution_rule"] == "exact_historical_name_context"
    assert result["normalized_state"] == "NM"
    assert result["state_status"] == "corrected_from_reviewed_evidence"
    assert result["normalized_district"] is None
    assert result["district_status"] == "not_applicable"
    assert result["publisher_member_key"] == SOURCE["publisher_member_key"]
    assert json.loads(result["source_context_json"]) == SOURCE
    assert json.loads(result["correction_evidence_json"]) == evidence
    assert evidence["status"] == "applied"
    assert evidence["original_value"] == "NH" and evidence["corrected_value"] == "NM"
    assert evidence["official_evidence"] == {
        "document": "GPO-CDOC-108hdoc222-3-103.pdf",
        "sha256": "sha256:5715936a0af738b437607714426b481604cb0122a0a64e0c7d008c1d0b8ef40f",
        "byte_size": 172686,
        "pdf_page": 3,
        "printed_page": "506",
        "locator": "NEW MEXICO / SENATORS / Pete V. Domenici, Albuquerque",
    }
    assert json.loads(result["term_candidates_json"])["D000407"] == data["member_terms"]
    rows = {"scorecard_member_links": [result], "scorecard_item_links": []}
    write_family(tmp_path, rows, generation_id="reviewed-state-fixture")
    assert read_family(tmp_path, LINK_NAMES, generation_id="reviewed-state-fixture") == rows


@pytest.mark.parametrize(
    "target,field,value",
    [
        ("source", "state", "NM"),
        ("source", "capture_id", "different-capture"),
        ("source", "snapshot_id", "different-snapshot"),
        ("source", "source_path", "different-row"),
        ("source", "source_url", "https://elsewhere.example/"),
        ("source", "member_name", "Someone Else"),
        ("source", "identifiers_json", '[{"scheme":"bioguide","value":"OTHER"}]'),
        ("source", "notes_text", None),
        ("edition", "congress_text", "104"),
        ("edition", "capture_id", "different-edition-capture"),
        ("edition", "periods_json", "[]"),
    ],
)
def test_changed_reviewed_record_skips_stale_correction(target, field, value):
    source, edition = deepcopy(SOURCE), deepcopy(EDITION)
    (source if target == "source" else edition)[field] = value
    adjusted, evidence = reviewed_member_context(source, edition)
    assert evidence is not None
    assert adjusted is source
    assert (source if target == "source" else edition)[field] == value
    assert evidence["status"] == "not_applied_source_changed"
    assert (
        evidence["actual_record_sha256"] != evidence["expected_record_sha256"]
        or evidence["actual_edition_sha256"] != evidence["expected_edition_sha256"]
    )


def test_stale_correction_preserves_other_members_and_publisher_fixed_state():
    data = official()
    data["members"].append({"bioguide_id": "G000445", "name_first": "Judd", "name_last": "Gregg"})
    data["member_terms"].append({**data["member_terms"][0], "bioguide_id": "G000445", "term_state": "NH"})
    changed = {**SOURCE, "capture_id": "fresh-capture"}
    other = {**SOURCE, "publisher_member_key": "Senate:Gregg:Judd:NH:R", "member_name": "Gregg Judd"}
    pins = json.loads(resolve()["input_pins_json"])
    results = resolve_scorecard_links(
        {"scorecards": [EDITION], "scorecard_members": [changed, other], "scorecard_items": []},
        data,
        pins,
    )["scorecard_member_links"]
    first, second = results
    assert first["resolution_status"] == "unresolved"
    assert first["bioguide_id"] is None
    assert first["source_context_json"] is not None and first["correction_evidence_json"] is not None
    assert json.loads(first["source_context_json"]) == changed
    assert json.loads(first["correction_evidence_json"])["status"] == "not_applied_source_changed"
    assert (second["bioguide_id"], second["normalized_state"], second["state_status"]) == ("G000445", "NH", "confirmed")
    assert second["correction_evidence_json"] is None
    fixed = resolve(source={**SOURCE, "state": "NM"})
    assert (fixed["bioguide_id"], fixed["normalized_state"], fixed["state_status"]) == ("D000407", "NM", "confirmed")
    assert json.loads(fixed["correction_evidence_json"])["status"] == "not_applied_source_changed"
    conflict = resolve(source={**SOURCE, "identifiers_json": '[{"scheme":"bioguide","value":"MISSING"}]'})
    assert conflict["resolution_status"] == "conflict"
    assert conflict["bioguide_id"] is None


def test_unrelated_scope_and_new_hampshire_records_are_not_corrected():
    for change in (
        {"scorecard_id": "lugar_mccourt:104-senate-html"},
        {"publisher_member_key": "Senate:Gregg:Judd:NH:R", "member_name": "Gregg Judd"},
    ):
        source = {**SOURCE, **change}
        adjusted, evidence = reviewed_member_context(source, EDITION)
        assert adjusted == source
        assert adjusted is source
        assert adjusted["state"] == "NH"
        assert evidence is None


@pytest.mark.parametrize("change", ["missing_term", "wrong_state", "wrong_period", "ambiguous_name"])
def test_correction_does_not_bypass_normal_official_matching(change):
    data = official()
    if change == "missing_term":
        data["member_terms"] = []
    elif change == "wrong_state":
        data["member_terms"][0]["term_state"] = "NH"
    elif change == "wrong_period":
        data["member_terms"][0].update(term_start="2001-01-03", term_end="2007-01-03")
    else:
        data["members"].append({**data["members"][0], "bioguide_id": "OTHER"})
        data["member_terms"].append({**data["member_terms"][0], "bioguide_id": "OTHER"})
    result = resolve(data)
    assert result["bioguide_id"] is None
    assert result["normalized_state"] is None
    assert result["state_status"] != "corrected_from_reviewed_evidence"
    assert result["resolution_status"] == ("ambiguous" if change == "ambiguous_name" else "unresolved")
    assert json.loads(result["source_context_json"])["state"] == "NH"
    assert json.loads(result["correction_evidence_json"])["corrected_value"] == "NM"
