"""Unit tests for LDA-witness matching, normalization guards, archive alias binding, and decisions rollup."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs import archive_release
from spicy_regs.identity_review import (
    PERSON_RULE,
    check_person_batch_sheet,
    make_decision,
    person_candidate_digest,
    render_person_batch_sheet,
)
from spicy_regs.local_publication import LOCAL_FAMILIES
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.pipelines.rollups.person_identity_decisions import (
    PersonIdentityDecisionsRollup,
    build_person_identity_decisions,
    verify_decisions_parquet,
)
from spicy_regs.transforms.person_identity_candidates import (
    HYPHEN_POLICY,
    build_precision_read_batch,
    check_middle_initial_guard,
    check_suffix_guard,
    compute_date_overlap,
    match_lda_witness_candidates,
    normalize_lda_lobbyist_name,
    normalize_organization,
    normalize_person_name,
)


# ---------------------------------------------------------------------------
# Name normalization (contract-review.md:21-26, 113-123)
# ---------------------------------------------------------------------------


def test_name_normalization_accent_folding_and_explicit_hyphen_policy():
    assert HYPHEN_POLICY == "space_before_punctuation"
    assert normalize_person_name("José García-López").name_key == normalize_person_name("Jose Garcia Lopez").name_key
    assert normalize_person_name("François Müller").name_key == "francois muller"
    assert normalize_person_name("Søren Kierkegaard").name_key == "soren kierkegaard"
    # Explicit hyphen-to-space before punctuation-to-space + apostrophe removal.
    assert normalize_person_name("Douglas Holtz-Eakin").name_key == "douglas holtz eakin"
    assert normalize_person_name("Douglas Holtz Eakin").name_key == "douglas holtz eakin"
    assert normalize_person_name("Tim O'Reilly").name_key == "tim oreilly"
    assert normalize_person_name("Tim O’Reilly").name_key == "tim oreilly"


def test_name_normalization_surname_first_and_compound_surnames():
    w1 = normalize_person_name("Williams, James A.")
    assert w1.surname_first is True
    assert w1.name_key == "james williams"
    assert w1.middle_initial == "a"
    assert w1.suffix == ""

    w2 = normalize_person_name("Williams, James A., Jr.")
    assert w2.surname_first is True
    assert w2.name_key == "james williams"
    assert w2.middle_initial == "a"
    assert w2.suffix == "jr"

    vh1 = normalize_person_name("Chris Van Hollen")
    vh2 = normalize_person_name("Van Hollen, Chris")
    assert vh1.name_key == "chris van hollen"
    assert vh2.name_key == "chris van hollen"
    assert vh2.surname_first is True

    he = normalize_person_name("Holtz-Eakin, Douglas")
    assert he.surname_first is True
    assert he.name_key == "douglas holtz eakin"

    # Credentials/suffixes after comma do NOT trigger surname-first inversion.
    cred = normalize_person_name("Dr. James A. Williams, Jr., Ph.D., M.D.")
    assert cred.surname_first is False
    assert cred.name_key == "james williams"
    assert cred.middle_initial == "a"
    assert cred.suffix == "jr"

    # Leading titles stripped without mangling real surnames like "Major" or "Justice".
    assert normalize_person_name("Maj. Gen. Jane Doe, Ph.D.").name_key == "jane doe"
    assert normalize_person_name("John Major").name_key == "john major"
    assert normalize_person_name("Mary Justice").name_key == "mary justice"


def test_suffix_and_middle_initial_guards_block_conflicts_and_allow_single_stated():
    assert check_suffix_guard("jr", "sr")[0] is False
    assert check_suffix_guard("ii", "iii")[0] is False
    assert check_suffix_guard("jr", "ii")[0] is False
    assert check_suffix_guard("jr", "jr") == (True, "both_match")
    assert check_suffix_guard("jr", "") == (True, "one_side_unstated")
    assert check_suffix_guard("", "") == (True, "both_unstated")

    assert check_middle_initial_guard("a", "b")[0] is False
    assert check_middle_initial_guard("a", "a") == (True, "both_match")
    assert check_middle_initial_guard("a", "") == (True, "one_side_unstated")
    assert check_middle_initial_guard("", "") == (True, "both_unstated")

    # LDA structured fields extract initial from full middle name and preserve compound last name.
    lda = normalize_lda_lobbyist_name(
        first_name="JAMES",
        middle_name="ALLEN",
        last_name="WILLIAMS",
        suffix="JR.",
    )
    assert lda.name_key == "james williams"
    assert lda.middle_initial == "a"
    assert lda.suffix == "jr"

    lda_compound = normalize_lda_lobbyist_name(
        first_name="CHRIS",
        middle_name=None,
        last_name="VAN HOLLEN",
    )
    assert lda_compound.name_key == "chris van hollen"


# ---------------------------------------------------------------------------
# Organization normalization (contract-review.md:22, 113-116)
# ---------------------------------------------------------------------------


def test_organization_normalization_never_cuts_at_first_comma_and_strips_only_trailing_places():
    # Crucial invariant: OIG, DoD != OIG, HHS (never cut at first comma!).
    assert normalize_organization("OIG, DoD") != normalize_organization("OIG, HHS")
    assert normalize_organization("OIG, DoD") == "oig dod"
    assert normalize_organization("OIG, HHS") == "oig hhs"
    assert (
        normalize_organization("Office of Inspector General, Department of Defense")
        != normalize_organization("Office of Inspector General, Department of Health and Human Services")
    )
    assert (
        normalize_organization("University of California, Berkeley")
        != normalize_organization("University of California, Los Angeles")
    )

    # Trailing place names stripped cleanly while preserving comma-separated sub-units.
    assert normalize_organization("American Enterprise Institute, Washington, DC") == "american enterprise institute"
    assert normalize_organization("Department of Justice, Washington, D.C") == "department of justice"
    assert normalize_organization("Harvard Law School, Cambridge, Massachusetts") == "harvard law school"
    assert (
        normalize_organization("Criminal Division, U.S. Department of Justice, Washington, DC")
        == "criminal division u s department of justice"
    )
    assert normalize_organization("The Boeing Company:") == "boeing company"
    assert normalize_organization("Café Policy Institute, Austin, TX") == "cafe policy institute"

    # Bare place names, empty orgs, bare titles, and private-citizen markers never merge.
    for unmergeable in ("", "   ", "Washington, DC", "New York, NY", "Private Citizen", "Self", "Director"):
        assert normalize_organization(unmergeable) == ""


# ---------------------------------------------------------------------------
# Date-overlap scoring & deterministic LDA-witness matching
# ---------------------------------------------------------------------------


def test_date_overlap_scoring_strata():
    exact = compute_date_overlap(date(2024, 5, 10), [(2024, "second_quarter")])
    assert exact.status == "exact_period_overlap" and exact.score == 1.0 and exact.min_abs_days_gap == 0

    same_yr = compute_date_overlap(date(2024, 2, 10), [(2024, "fourth_quarter")])
    assert same_yr.status == "same_year_overlap" and same_yr.score == 0.9

    span = compute_date_overlap(date(2022, 6, 1), [(2020, "first_quarter"), (2024, "fourth_quarter")])
    assert span.status == "span_overlap" and span.score == 0.85

    adj = compute_date_overlap(date(2024, 5, 1), [(2023, "fourth_quarter")])
    assert adj.status == "adjacent_1y" and adj.score == 0.7

    near = compute_date_overlap(date(2024, 5, 1), [(2021, "fourth_quarter")])
    assert near.status == "near_3y" and near.score == 0.4

    distant = compute_date_overlap(date(2025, 3, 1), [(1999, "mid_year")])
    assert distant.status == "distant" and distant.score == 0.1

    undated = compute_date_overlap(None, [(2024, "first_quarter")])
    assert undated.status == "undated" and undated.score == 0.0 and undated.min_abs_days_gap is None


def test_match_lda_witness_candidates_blocks_conflicts_and_preserves_digest_across_releases():
    witnesses = [
        # Valid match via accent folding + trailing place stripping + client match:
        {
            "witness_key": "w_jose",
            "source": "mods",
            "record_sha": bytes.fromhex("11" * 32),
            "source_locator": "mods:witness[1]",
            "name": "José García-López",
            "organization": "Acme Energy Alliance, Washington, DC",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 5, 20),
        },
        # Blocked by differing generational suffix (Jr. vs Sr.):
        {
            "witness_key": "w_suf_block",
            "source": "house_xml",
            "record_sha": bytes.fromhex("22" * 32),
            "source_locator": "house_xml:witness[2]",
            "name": "Williams, James A., Jr.",
            "organization": "United Widget Union",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 3, 1),
        },
        # Blocked by differing middle initial (A. vs B.):
        {
            "witness_key": "w_mid_block",
            "source": "senate_page",
            "record_sha": bytes.fromhex("33" * 32),
            "source_locator": "senate_page:witness[1]",
            "name": "Alice B. Carter",
            "organization": "OIG, DoD",
            "witness_type": "Government - Federal",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2023, 8, 10),
        },
        # Distinct org (OIG, HHS != OIG, DoD) must NOT match OIG, DoD lobbyist:
        {
            "witness_key": "w_org_sep",
            "source": "senate_page",
            "record_sha": bytes.fromhex("44" * 32),
            "source_locator": "senate_page:witness[2]",
            "name": "Alice A. Carter",
            "organization": "OIG, HHS",
            "witness_type": "Government - Federal",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2023, 8, 10),
        },
        # Excluded member row:
        {
            "witness_key": "w_member",
            "source": "house_xml",
            "record_sha": bytes.fromhex("55" * 32),
            "source_locator": "house_xml:witness[3]",
            "name": "José García-López",
            "organization": "Acme Energy Alliance",
            "witness_type": "member_appearance",
            "bioguide_id": "G000001",
            "matched_bioguide_id": "G000001",
            "superseded_by": None,
            "hearing_date": date(2024, 5, 20),
        },
    ]

    lda_rows = [
        {
            "filing_uuid": "uuid-1",
            "lobbyist_id": "1001",
            "prefix": None,
            "first_name": "JOSE",
            "middle_name": None,
            "last_name": "GARCIA LOPEZ",
            "suffix": None,
            "covered_position": None,
            "filing_year": 2024,
            "filing_period": "second_quarter",
            "registrant_name": "Capitol Strategies LLC",
            "client_name": "ACME ENERGY ALLIANCE",
        },
        {
            "filing_uuid": "uuid-2",
            "lobbyist_id": "2002",
            "prefix": None,
            "first_name": "JAMES",
            "middle_name": "ALLEN",
            "last_name": "WILLIAMS",
            "suffix": "SR",
            "covered_position": None,
            "filing_year": 2024,
            "filing_period": "first_quarter",
            "registrant_name": "United Widget Union",
            "client_name": "United Widget Union",
        },
        {
            "filing_uuid": "uuid-3",
            "lobbyist_id": "3003",
            "prefix": None,
            "first_name": "ALICE",
            "middle_name": "ANNE",
            "last_name": "CARTER",
            "suffix": None,
            "covered_position": None,
            "filing_year": 2023,
            "filing_period": "third_quarter",
            "registrant_name": "OIG, DoD",
            "client_name": "OIG, DoD",
        },
    ]

    cands_r1, stats = match_lda_witness_candidates(
        witnesses,
        lda_rows,
        ct_release_id="453351a3" + "0" * 56,
        lobbying_generation_digest="sha256:" + "a" * 64,
    )
    cands_r2, _ = match_lda_witness_candidates(
        witnesses,
        lda_rows,
        ct_release_id="99999999" + "f" * 56,
        lobbying_generation_digest="sha256:" + "b" * 64,
    )
    assert len(cands_r1) == 1
    assert stats["blocked_suffix"] == 1
    assert stats["blocked_middle_initial"] == 1
    assert stats["witnesses_skipped_member"] == 1
    cand = cands_r1[0]
    assert cand["witness_key"] == "w_jose"
    assert cand["external_native_id"] == "lda_lobbyist:1001"
    assert cand["date_overlap_status"] == "exact_period_overlap"
    assert cand["date_overlap_score"] == 1.0
    assert cand["content_digest"] == cands_r2[0]["content_digest"] == person_candidate_digest(cand)
    assert "person_key" not in cand


def test_precision_read_batch_includes_planted_wrong_control_and_checks_cleanly():
    # Build 6 distinct synthetic candidates so batch builder can sample real + planted controls.
    witnesses = [
        {
            "witness_key": f"w_{i}",
            "source": "mods",
            "record_sha": bytes([i + 1] * 32),
            "source_locator": f"mods:witness[{i + 1}]",
            "name": f"Pat{i} Example{i}",
            "organization": f"Association Number {i}, Washington, DC",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 1 + i, 15),
        }
        for i in range(6)
    ]
    lda_rows = [
        {
            "filing_uuid": f"filing-{i}",
            "lobbyist_id": f"{5000 + i}",
            "prefix": None,
            "first_name": f"PAT{i}",
            "middle_name": None,
            "last_name": f"EXAMPLE{i}",
            "suffix": None,
            "covered_position": None,
            "filing_year": 2024,
            "filing_period": "first_quarter",
            "registrant_name": f"Association Number {i}",
            "client_name": f"Association Number {i}",
        }
        for i in range(6)
    ]
    cands, stats = match_lda_witness_candidates(
        witnesses,
        lda_rows,
        ct_release_id="453351a3" + "0" * 56,
        lobbying_generation_digest="sha256:" + "e" * 64,
    )
    batch1 = build_precision_read_batch(
        cands,
        seed=20261127,
        total_pairs=5,
        planted_wrong_count=1,
        ct_release_id="453351a3" + "0" * 56,
        lobbying_generation_digest="sha256:" + "e" * 64,
        stats=stats,
    )
    batch2 = build_precision_read_batch(
        cands,
        seed=20261127,
        total_pairs=5,
        planted_wrong_count=1,
        ct_release_id="453351a3" + "0" * 56,
        lobbying_generation_digest="sha256:" + "e" * 64,
        stats=stats,
    )
    assert batch1 == batch2
    assert len(batch1["items"]) == 5
    planted = [item for item in batch1["items"] if item["is_planted_control"]]
    assert len(planted) == 1
    assert planted[0]["expected_owner_answer"] == "different_person"
    rendered = render_person_batch_sheet(batch1)
    check_person_batch_sheet(batch1, rendered)
    assert "PLANTED WRONG CONTROL" in rendered


# ---------------------------------------------------------------------------
# archive_release person bindings & alias resolution
# ---------------------------------------------------------------------------


def test_archive_release_binds_people_and_resolves_unambiguous_aliases():
    assert set(archive_release.PERSON_BOUND_FILES) <= set(archive_release.BOUND_FILES)
    assert "people" in archive_release.BOUND_FILES
    assert "person_aliases" in archive_release.BOUND_FILES
    assert "witness_key_map" in archive_release.BOUND_FILES

    aliases = [
        {"old_person_key": "p_old", "person_key": "p_new", "status": "unambiguous"},
        {"old_person_key": "p_ambig", "person_key": "p_candidate", "status": "ambiguous"},
    ]
    assert archive_release.resolve_person_key(aliases, "p_old") == "p_new"
    assert archive_release.resolve_person_key(aliases, "p_ambig") == "p_ambig"
    assert archive_release.resolve_person_key(aliases, "p_current") == "p_current"

    wk_map = [
        {"old_key": "w_old", "witness_key": "w_new", "status": "unambiguous"},
        {"old_key": "w_ambig", "witness_key": "w_other", "status": "ambiguous"},
    ]
    assert archive_release.resolve_witness_key(wk_map, "w_old") == "w_new"
    assert archive_release.resolve_witness_key(wk_map, "w_ambig") == "w_ambig"


# ---------------------------------------------------------------------------
# PersonIdentityDecisionsRollup (RollupPipeline, NOT LOCAL_FAMILIES, owner gate)
# ---------------------------------------------------------------------------


def _sample_person_history(tmp_path: Path) -> Path:
    c1 = {
        "rule_version": PERSON_RULE,
        "candidate_id": "lda_witness:w_101:lda_lobbyist:9001",
        "subject_kind": "ct_witness_appearance",
        "witness_key": "w_101",
        "relation": "same_person",
        "external_native_id": "lda_lobbyist:9001",
        "stated_name": "Jordan R. Sample",
        "stated_org": "National Widget Council",
        "body_sha256": "sha256:" + "1" * 64,
        "page_or_offset": "mods:witness[1]",
        "quoted_span": "CT witness 'Jordan R. Sample' (National Widget Council) <-> LDA lda_lobbyist:9001",
        "release_id": "453351a3a57bdd28031a43de218832f5cd8b89c30048bc04354aaba37b337c47",
        "source_generation_digest": "sha256:" + "a" * 64,
    }
    c2 = {
        **c1,
        "candidate_id": "lda_witness:w_202:lda_lobbyist:9002",
        "witness_key": "w_202",
        "external_native_id": "lda_lobbyist:9002",
        "stated_name": "Morgan K. Sample",
        "quoted_span": "CT witness 'Morgan K. Sample' (National Widget Council) <-> LDA lda_lobbyist:9002",
    }
    ev = [{
        "kind": "hearing_record",
        "stance": "supporting",
        "locator": "ct:witness:w_101#mods:witness[1]",
        "sha256": "sha256:" + "1" * 64,
        "claim": "Verified hearing witness and LDA filing registrant match",
    }]
    scope = {
        "role": "witness_appearance_lobbyist",
        "relationship": "same_person",
        "valid_from": "2024-01-01",
        "valid_to": "2025-01-01",
        "validity_basis": "LDA 2024 filing period overlaps hearing date",
    }
    e1 = make_decision(
        c1, [],
        expected_candidate_digest=person_candidate_digest(c1),
        expected_event_id=None,
        decision="accepted",
        reviewer="owner-fixture",
        decided_at="2026-11-27T15:00:00Z",
        reason="Owner-approved rule match",
        evidence=ev,
        scope=scope,
    )
    e2 = make_decision(
        c1, [e1],
        expected_candidate_digest=person_candidate_digest(c1),
        expected_event_id=e1["event_id"],
        decision="revoked",
        reviewer="owner-fixture",
        decided_at="2026-11-27T15:05:00Z",
        reason="Revocation drill",
        evidence=ev,
        scope=scope,
    )
    e3 = make_decision(
        c2, [e1, e2],
        expected_candidate_digest=person_candidate_digest(c2),
        expected_event_id=e2["event_id"],
        decision="rejected",
        reviewer="owner-fixture",
        decided_at="2026-11-27T15:10:00Z",
        reason="Different individual confirmed by dates",
        evidence=[{**ev[0], "stance": "conflicting"}],
        scope=scope,
    )
    log_path = tmp_path / "person_decisions.jsonl"
    log_path.write_text("\n".join(json.dumps(e, sort_keys=True) for e in (e1, e2, e3)) + "\n", encoding="utf-8")
    return log_path


def test_person_identity_decisions_rollup_is_rollup_pipeline_not_local_family():
    assert issubclass(PersonIdentityDecisionsRollup, RollupPipeline)
    assert PersonIdentityDecisionsRollup.name not in LOCAL_FAMILIES


def test_person_identity_decisions_rollup_builds_verified_generation_and_enforces_owner_gate(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("PERSON_IDENTITY_OWNER_APPROVAL", raising=False)
    log_path = _sample_person_history(tmp_path)

    # Hard owner-approval gate blocks live upload without explicit owner approval reference.
    with pytest.raises(RuntimeError, match="owner approval"):
        PersonIdentityDecisionsRollup(
            output_dir=tmp_path / "blocked",
            skip_upload=False,
            history_path=log_path,
        ).run()

    # Local skip_upload=True run builds and verifies a complete rulespec generation!
    out_dir = tmp_path / "out"
    PersonIdentityDecisionsRollup(
        output_dir=out_dir,
        skip_upload=True,
        history_path=log_path,
    ).run()

    parquet_path = out_dir / "person_identity_decisions.parquet"
    assert parquet_path.exists()
    table = pq.read_table(parquet_path)
    assert table.num_rows == 3
    assert "witness_key" in table.column_names
    assert "person_key" not in table.column_names
    assert "release_id" in table.column_names
    assert "source_generation_digest" in table.column_names

    verified = verify_decisions_parquet(parquet_path)
    assert [e["decision"] for e in verified] == ["accepted", "revoked", "rejected"]
    assert verified[0]["previous_event_id"] is None
    assert verified[1]["previous_event_id"] == verified[0]["event_id"]
    assert verified[2]["previous_event_id"] == verified[1]["event_id"]

    # Local rulespec_artifacts generation directory was emitted cleanly.
    gen_dirs = [p for p in (out_dir / "generations").iterdir() if p.is_dir()]
    assert len(gen_dirs) == 1
    assert (gen_dirs[0] / "person_identity_decisions.parquet").exists()

    # Direct builder round-trip also verifies cleanly.
    direct = build_person_identity_decisions(tmp_path / "direct", history_path=log_path)
    assert len(verify_decisions_parquet(direct)) == 3
