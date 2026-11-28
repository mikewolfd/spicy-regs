"""Unit tests for LDA-witness matching, normalization guards, archive alias binding, and decisions rollup."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_regs import archive_release
from spicy_regs.identity_review import (
    PERSON_RULE,
    check_person_batch_sheet,
    make_decision,
    person_candidate_digest,
    render_person_batch_sheet,
    replay,
)
from spicy_regs.local_publication import LOCAL_FAMILIES
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.pipelines.rollups.person_identity_decisions import (
    PersonIdentityDecisionsRollup,
    build_person_identity_decisions,
    verify_decisions_parquet,
)
from spicy_regs.transforms.person_identity_candidates import (
    DISPOSITION_MANUAL_REVIEW_QUARANTINE,
    DISPOSITION_QUALIFIED,
    HYPHEN_POLICY,
    QUARANTINE_INITIAL_ONLY_FIRST_NAME,
    QUARANTINE_NO_LDA_DATES,
    QUARANTINE_TEMPORAL_GAP_EXCEEDS_5Y,
    QUARANTINE_UNDATED_UNCORROBORATED,
    build_precision_read_batch,
    check_first_name_guard,
    check_middle_initial_guard,
    check_suffix_guard,
    compute_date_overlap,
    compute_temporal_gap_years,
    congress_year_window,
    extract_hearing_congress,
    is_auto_publish_qualified,
    is_eponymous_firm_match,
    match_lda_witness_candidates,
    normalize_lda_lobbyist_name,
    normalize_organization,
    normalize_person_name,
    partition_lda_witness_candidates,
    query_prefiltered_lda_rows,
    require_auto_publish_qualified,
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
    # Crucial invariant: OIG, DoD != OIG, HHS != OIG, VA (never cut at first comma or strip bare acronyms!).
    assert normalize_organization("OIG, DoD") != normalize_organization("OIG, HHS")
    assert normalize_organization("OIG, DoD") == "oig dod"
    assert normalize_organization("OIG, HHS") == "oig hhs"
    assert normalize_organization("OIG, VA") == "oig va"
    assert normalize_organization("OIG, VA") != normalize_organization("OIG, DoD")
    assert normalize_organization("OIG, VA") != normalize_organization("OIG, HHS")
    assert (
        normalize_organization("Office of Inspector General, Department of Defense")
        != normalize_organization("Office of Inspector General, Department of Health and Human Services")
    )
    assert (
        normalize_organization("University of California, Berkeley")
        != normalize_organization("University of California, Los Angeles")
    )

    # Finding 5 regression: legal/entity abbreviations matching 2-letter US state codes MUST NOT be stripped!
    assert normalize_organization("Smith Law, P.A.") == "smith law p a"
    assert normalize_organization("Smith Law, PA") == "smith law pa"
    assert normalize_organization("Acme, Co.") == "acme co"
    assert normalize_organization("Clinic, M.D.") == "clinic m d"

    # Trailing place names stripped cleanly while preserving comma-separated sub-units.
    assert normalize_organization("American Enterprise Institute, Washington, DC") == "american enterprise institute"
    assert normalize_organization("Department of Justice, Washington, D.C") == "department of justice"
    assert normalize_organization("Department of Defense, D.C.") == "department of defense"
    assert normalize_organization("Acme Corp, VA 22201") == "acme corp"
    assert normalize_organization("Harvard Law School, Cambridge, Massachusetts") == "harvard law school"
    assert (
        normalize_organization("Criminal Division, U.S. Department of Justice, Washington, DC")
        == "criminal division u s department of justice"
    )
    assert normalize_organization("The Boeing Company:") == "boeing company"
    assert normalize_organization("Café Policy Institute, Austin, TX") == "cafe policy institute"

    # Bare place names, empty orgs, bare titles, and private-citizen markers never merge.
    for unmergeable in (
        "", "   ", "Washington, DC", "Washington, D.C", "New York, NY",
        "Philadelphia, Pennsylvania", "Private Citizen", "Self", "Director",
    ):
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
    assert PersonIdentityDecisionsRollup.retain_source_evidence is True


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

    # Local rulespec_artifacts generation directory and source evidence artifact were emitted cleanly.
    gen_dirs = [p for p in (out_dir / "generations").iterdir() if p.is_dir()]
    assert len(gen_dirs) == 1
    assert (gen_dirs[0] / "person_identity_decisions.parquet").exists()
    evidence_journals = list((out_dir / "source-evidence").rglob("journal.jsonl"))
    assert len(evidence_journals) == 1
    journal_events = [json.loads(line) for line in evidence_journals[0].read_text(encoding="utf-8").splitlines()]
    retained = [e for e in journal_events if e.get("event") == "retained-file"]
    assert len(retained) == 1
    assert retained[0]["stage"] == "person-identity-decisions-log"
    assert retained[0]["event_count"] == 3

    # Direct builder round-trip also verifies cleanly.
    direct = build_person_identity_decisions(tmp_path / "direct", history_path=log_path)
    assert len(verify_decisions_parquet(direct)) == 3


# ---------------------------------------------------------------------------
# Independent review regression tests (Findings 1, 3, 4)
# ---------------------------------------------------------------------------


def test_candidate_digest_is_invariant_across_new_quarterly_lda_filings():
    """Finding 1 regression: appending a new quarterly LDA filing must NOT mutate quoted_span or candidate_digest."""
    witnesses = [{
        "witness_key": "w_quarterly",
        "source": "congress_gov",
        "record_sha": bytes.fromhex("ab" * 32),
        "source_locator": "congress_gov:witness[1]",
        "name": "Jordan Example",
        "organization": "American Widget Council, Washington, DC",
        "witness_type": "Non-Governmental",
        "bioguide_id": None,
        "matched_bioguide_id": None,
        "superseded_by": None,
        "hearing_date": date(2024, 5, 10),
    }]
    q2_2024 = [{
        "filing_uuid": "uuid-2024-q2",
        "lobbyist_id": "88001",
        "prefix": None,
        "first_name": "JORDAN",
        "middle_name": None,
        "last_name": "EXAMPLE",
        "suffix": None,
        "covered_position": None,
        "filing_year": 2024,
        "filing_period": "second_quarter",
        "registrant_name": "American Widget Council",
        "client_name": "American Widget Council",
    }]
    later_filings = [
        *q2_2024,
        {
            **q2_2024[0],
            "filing_uuid": "uuid-2024-q3",
            "prefix": "Mr.",
            "filing_period": "third_quarter",
            "registrant_name": "AMERICAN WIDGET COUNCIL",
            "client_name": "AMERICAN WIDGET COUNCIL",
        },
        {
            **q2_2024[0],
            "filing_uuid": "uuid-2025-q1",
            "prefix": "Mr.",
            "filing_year": 2025,
            "filing_period": "first_quarter",
            "registrant_name": "Outside Firm LLC",
            "client_name": "AMERICAN WIDGET COUNCIL",
        },
    ]

    before_cands, _ = match_lda_witness_candidates(
        witnesses, q2_2024, ct_release_id="453351a3" + "0" * 56, lobbying_generation_digest="sha256:" + "1" * 64,
    )
    after_cands, _ = match_lda_witness_candidates(
        witnesses, later_filings, ct_release_id="453351a3" + "0" * 56, lobbying_generation_digest="sha256:" + "2" * 64,
    )
    assert len(before_cands) == 1 and len(after_cands) == 1
    before, after = before_cands[0], after_cands[0]

    # Diagnostic fields update cleanly as new quarterly filings arrive:
    assert before["lda_filing_count"] == 1 and after["lda_filing_count"] == 3
    assert before["lda_filing_years"] == [2024] and after["lda_filing_years"] == [2024, 2025]
    # Immutable quoted_span and content_digest remain BYTE-IDENTICAL:
    assert before["quoted_span"] == after["quoted_span"]
    assert before["content_digest"] == after["content_digest"]

    # An accepted decision made before the new filings stays active (NOT stale) afterward!
    accepted = make_decision(
        before,
        [],
        expected_candidate_digest=before["content_digest"],
        expected_event_id=None,
        decision="accepted",
        reviewer="owner-fixture",
        decided_at="2026-11-27T16:00:00Z",
        reason="Quarterly invariance check",
        evidence=[{
            "kind": "hearing_record",
            "stance": "supporting",
            "locator": "ct:witness:w_quarterly#congress_gov:witness[1]",
            "sha256": before["body_sha256"],
            "claim": "Hearing witness record matches LDA lobbyist",
        }],
        scope={
            "role": "witness_appearance_lobbyist",
            "relationship": "same_person",
            "valid_from": "2024-01-01",
            "valid_to": "2025-01-01",
            "validity_basis": "2024 Q2 filing overlaps hearing date",
        },
    )
    replayed = replay([accepted], [after])[0]
    assert replayed["status"] == "accepted" and replayed["effective"] is True


def test_multi_value_lda_middle_initials_and_suffixes_are_blocked_not_collapsed():
    """Finding 3 regression: competing multi-value LDA initials/suffixes must block instead of collapsing to ''."""
    assert check_middle_initial_guard("b", {"a", "c"})[0] is False
    assert check_middle_initial_guard("", {"a", "c"})[0] is False
    assert check_suffix_guard("sr", {"jr", "ii"})[0] is False
    assert check_suffix_guard("", {"jr", "ii"})[0] is False

    witnesses = [
        {
            "witness_key": "w_mid_b",
            "source": "mods",
            "record_sha": bytes.fromhex("10" * 32),
            "source_locator": "mods:witness[1]",
            "name": "Jane B. Doe",
            "organization": "Widget Policy Group",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 2, 1),
        },
        {
            "witness_key": "w_mid_none",
            "source": "mods",
            "record_sha": bytes.fromhex("11" * 32),
            "source_locator": "mods:witness[2]",
            "name": "Jane Doe",
            "organization": "Widget Policy Group",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 2, 1),
        },
        {
            "witness_key": "w_suf_sr",
            "source": "mods",
            "record_sha": bytes.fromhex("12" * 32),
            "source_locator": "mods:witness[3]",
            "name": "Robert Roe Sr.",
            "organization": "Widget Policy Group",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 2, 1),
        },
        {
            "witness_key": "w_suf_none",
            "source": "mods",
            "record_sha": bytes.fromhex("13" * 32),
            "source_locator": "mods:witness[4]",
            "name": "Robert Roe",
            "organization": "Widget Policy Group",
            "witness_type": "Non-Governmental",
            "bioguide_id": None,
            "matched_bioguide_id": None,
            "superseded_by": None,
            "hearing_date": date(2024, 2, 1),
        },
    ]
    lda_rows = [
        # Two competing middle initials ('a' and 'c') on the same lobbyist_id:
        {
            "filing_uuid": "f1", "lobbyist_id": "7001", "prefix": None,
            "first_name": "JANE", "middle_name": "ALLEN", "last_name": "DOE", "suffix": None,
            "covered_position": None, "filing_year": 2024, "filing_period": "first_quarter",
            "registrant_name": "Widget Policy Group", "client_name": "Widget Policy Group",
        },
        {
            "filing_uuid": "f2", "lobbyist_id": "7001", "prefix": None,
            "first_name": "JANE", "middle_name": "CHARLES", "last_name": "DOE", "suffix": None,
            "covered_position": None, "filing_year": 2024, "filing_period": "second_quarter",
            "registrant_name": "Widget Policy Group", "client_name": "Widget Policy Group",
        },
        # Two competing suffixes ('jr' and 'ii') on the same lobbyist_id:
        {
            "filing_uuid": "f3", "lobbyist_id": "7002", "prefix": None,
            "first_name": "ROBERT", "middle_name": None, "last_name": "ROE", "suffix": "JR.",
            "covered_position": None, "filing_year": 2024, "filing_period": "first_quarter",
            "registrant_name": "Widget Policy Group", "client_name": "Widget Policy Group",
        },
        {
            "filing_uuid": "f4", "lobbyist_id": "7002", "prefix": None,
            "first_name": "ROBERT", "middle_name": None, "last_name": "ROE", "suffix": "II",
            "covered_position": None, "filing_year": 2024, "filing_period": "second_quarter",
            "registrant_name": "Widget Policy Group", "client_name": "Widget Policy Group",
        },
    ]
    cands, stats = match_lda_witness_candidates(witnesses, lda_rows)
    assert cands == []
    assert stats["blocked_middle_initial"] == 2
    assert stats["blocked_suffix"] == 2


def test_duckdb_sql_prefilter_handles_honorifics_parentheticals_credentials_and_transliterations(tmp_path):
    """Finding 4 regression: SQL pre-filter aligns with Python normalization across edge cases."""
    lob_parquet = tmp_path / "lobbying_activity_lobbyists.parquet"
    fil_parquet = tmp_path / "lobbying_filings.parquet"
    with duckdb.connect() as con:
        con.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('u1', '101', NULL::VARCHAR, 'Dr. Søren', NULL::VARCHAR, 'Kierkegaard, Ph.D.', NULL::VARCHAR, NULL::VARCHAR),
                    ('u2', '102', NULL::VARCHAR, '(Hon.) José', NULL::VARCHAR, 'García-López (Ret.), Jr.', NULL::VARCHAR, NULL::VARCHAR),
                    ('u3', '103', NULL::VARCHAR, 'Lt. Gen. Patrick', NULL::VARCHAR, 'O''Connor, Esq.', NULL::VARCHAR, NULL::VARCHAR),
                    ('u4', '104', NULL::VARCHAR, 'John', NULL::VARCHAR, 'Major', NULL::VARCHAR, NULL::VARCHAR),
                    ('u5', '999', NULL::VARCHAR, 'Unrelated', NULL::VARCHAR, 'NoisePerson', NULL::VARCHAR, NULL::VARCHAR)
                ) t(filing_uuid, lobbyist_id, prefix, first_name, middle_name, last_name, suffix, covered_position)
            ) TO '{lob_parquet}' (FORMAT PARQUET)
        """)
        con.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('u1', 2024::BIGINT, 'first_quarter', 'Nordic Policy Forum', 'Nordic Policy Forum'),
                    ('u2', 2024::BIGINT, 'second_quarter', 'Solar Alliance', 'Solar Alliance'),
                    ('u3', 2024::BIGINT, 'third_quarter', 'Maritime Council', 'Maritime Council'),
                    ('u4', 2024::BIGINT, 'fourth_quarter', 'Civic Standards Institute', 'Civic Standards Institute'),
                    ('u5', 2024::BIGINT, 'fourth_quarter', 'Noise Org', 'Noise Org')
                ) t(filing_uuid, filing_year, filing_period, registrant_name, client_name)
            ) TO '{fil_parquet}' (FORMAT PARQUET)
        """)

        witnesses: list[dict[str, Any]] = [
            {
                "witness_key": "w_soren", "source": "mods", "record_sha": bytes.fromhex("21" * 32),
                "source_locator": "mods:witness[1]", "name": "Soren Kierkegaard",
                "organization": "Nordic Policy Forum", "witness_type": "Non-Governmental",
                "bioguide_id": None, "matched_bioguide_id": None, "superseded_by": None,
                "hearing_date": date(2024, 2, 1),
            },
            {
                "witness_key": "w_jose", "source": "mods", "record_sha": bytes.fromhex("22" * 32),
                "source_locator": "mods:witness[2]", "name": "Jose Garcia Lopez Jr.",
                "organization": "Solar Alliance", "witness_type": "Non-Governmental",
                "bioguide_id": None, "matched_bioguide_id": None, "superseded_by": None,
                "hearing_date": date(2024, 5, 1),
            },
            {
                "witness_key": "w_pat", "source": "mods", "record_sha": bytes.fromhex("23" * 32),
                "source_locator": "mods:witness[3]", "name": "Patrick O'Connor",
                "organization": "Maritime Council", "witness_type": "Non-Governmental",
                "bioguide_id": None, "matched_bioguide_id": None, "superseded_by": None,
                "hearing_date": date(2024, 8, 1),
            },
            {
                "witness_key": "w_major", "source": "mods", "record_sha": bytes.fromhex("24" * 32),
                "source_locator": "mods:witness[4]", "name": "John Major",
                "organization": "Civic Standards Institute", "witness_type": "Non-Governmental",
                "bioguide_id": None, "matched_bioguide_id": None, "superseded_by": None,
                "hearing_date": date(2024, 11, 1),
            },
        ]
        pairs = [
            (norm.tokens[0], norm.tokens[-1])
            for w in witnesses
            if (norm := normalize_person_name(w["name"])).name_key
        ]
        filtered = query_prefiltered_lda_rows(
            con,
            lobbyists_source=f"read_parquet('{lob_parquet}')",
            filings_source=f"read_parquet('{fil_parquet}')",
            witness_name_pairs=pairs,
        )
    assert {r["lobbyist_id"] for r in filtered} == {"101", "102", "103", "104"}
    cands, _ = match_lda_witness_candidates(witnesses, filtered)
    assert {c["witness_key"] for c in cands} == {"w_soren", "w_jose", "w_pat", "w_major"}


# ---------------------------------------------------------------------------
# Blind Neon audit precision guardrails (ss-39zl tightening)
# ---------------------------------------------------------------------------


def _mk_witness(
    witness_key: str,
    name: str,
    organization: str,
    *,
    hearing_date: date | None = date(2024, 5, 10),
    hearing_congress: int | None = None,
    package_id: str | None = None,
    source: str = "mods",
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "witness_key": witness_key,
        "source": source,
        "record_sha": bytes.fromhex("33" * 32),
        "source_locator": f"{source}:witness[1]",
        "name": name,
        "organization": organization,
        "witness_type": "Non-Governmental",
        "bioguide_id": None,
        "matched_bioguide_id": None,
        "superseded_by": None,
        "hearing_date": hearing_date,
        "package_id": package_id,
    }
    if hearing_congress is not None:
        row["hearing_congress"] = hearing_congress
    return row


def _mk_lda(
    lobbyist_id: str,
    first_name: str,
    last_name: str,
    registrant_name: str,
    *,
    middle_name: str | None = None,
    suffix: str | None = None,
    prefix: str | None = None,
    client_name: str | None = None,
    filing_year: int | None = 2024,
    filing_period: str = "second_quarter",
    filing_uuid: str = "uuid-g",
) -> dict[str, Any]:
    return {
        "filing_uuid": f"{filing_uuid}-{lobbyist_id}-{filing_year}",
        "lobbyist_id": lobbyist_id,
        "prefix": prefix,
        "first_name": first_name,
        "middle_name": middle_name,
        "last_name": last_name,
        "suffix": suffix,
        "covered_position": None,
        "filing_year": filing_year,
        "filing_period": filing_period,
        "registrant_name": registrant_name,
        "client_name": client_name if client_name is not None else registrant_name,
    }


def test_guardrail_1_rejects_initial_only_first_names_and_allows_two_letter_names():
    # Direct helper checks on both CT witness and LDA sides.
    assert check_first_name_guard("j", {"j"}) == (False, "quarantine_both_initial_only")
    assert check_first_name_guard("j", {"david"}) == (False, "quarantine_witness_initial_only")
    assert check_first_name_guard("david", {"j"}) == (False, "quarantine_lda_initial_only")
    assert check_first_name_guard("al", {"al"}) == (True, "both_full")

    witnesses = [
        # Pair #37 shape: initial-only first name AND 17-year gap.
        _mk_witness(
            "w_j_grossman",
            "J. Grossman",
            "Consumer Technology Association",
            hearing_date=date(2026, 6, 4),
            source="congress_gov",
        ),
        # Initial-only with honorific and zero temporal gap (still quarantined by Guardrail 1 alone!).
        _mk_witness(
            "w_dr_j",
            "Dr. J. Grossman",
            "Consumer Technology Association",
            hearing_date=date(2008, 6, 4),
        ),
        # Surname-first initial-only ("Grossman, J.").
        _mk_witness(
            "w_sf_j",
            "Grossman, J.",
            "Consumer Technology Association",
            hearing_date=date(2008, 6, 4),
        ),
        # Leading initial followed by middle name ("H. Frederick Dylla").
        _mk_witness(
            "w_h_dylla",
            "H. Frederick Dylla",
            "American Institute of Physics",
            hearing_date=date(2008, 5, 1),
        ),
        # Genuine 2-letter first name ("Al Gore", "Ed Markey") MUST pass.
        _mk_witness(
            "w_al",
            "Al Example",
            "Clean Grid Association",
            hearing_date=date(2024, 5, 1),
        ),
    ]
    lda_rows = [
        _mk_lda("7816", "J", "GROSSMAN", "CONSUMER TECHNOLOGY ASSOCIATION", middle_name="DAVID", filing_year=2006),
        _mk_lda("7816", "J", "GROSSMAN", "CONSUMER TECHNOLOGY ASSOCIATION", middle_name="DAVID", filing_year=2009),
        _mk_lda("7817", "H FREDERICK", "DYLLA", "AMERICAN INSTITUTE OF PHYSICS", prefix="mr", filing_year=2008),
        _mk_lda("7818", "AL", "EXAMPLE", "CLEAN GRID ASSOCIATION", filing_year=2024),
    ]

    qualified, quarantined, stats = partition_lda_witness_candidates(
        witnesses,
        lda_rows,
        ct_release_id="453351a3" + "0" * 56,
        lobbying_generation_digest="sha256:" + "a" * 64,
    )
    assert [c["witness_key"] for c in qualified] == ["w_al"]
    assert is_auto_publish_qualified(qualified[0]) is True
    require_auto_publish_qualified(qualified[0])

    by_wk = {c["witness_key"]: c for c in quarantined}
    assert set(by_wk) == {"w_j_grossman", "w_dr_j", "w_sf_j", "w_h_dylla"}

    # #37 triggers BOTH Guardrail 1 (initial-only first name) AND Guardrail 2 (17y gap).
    j_grossman = by_wk["w_j_grossman"]
    assert j_grossman["auto_publish_disposition"] == DISPOSITION_MANUAL_REVIEW_QUARANTINE
    assert j_grossman["quarantine_reasons"] == [
        QUARANTINE_INITIAL_ONLY_FIRST_NAME,
        QUARANTINE_TEMPORAL_GAP_EXCEEDS_5Y,
    ]
    assert j_grossman["temporal_gap_years"] == 17
    assert is_auto_publish_qualified(j_grossman) is False

    # Same-year initial-only cases still fail Guardrail 1 alone.
    for wk in ("w_dr_j", "w_sf_j", "w_h_dylla"):
        assert by_wk[wk]["quarantine_reasons"] == [QUARANTINE_INITIAL_ONLY_FIRST_NAME]
        assert by_wk[wk]["auto_publish_disposition"] == DISPOSITION_MANUAL_REVIEW_QUARANTINE

    assert stats["quarantined_total"] == 4
    assert stats[f"quarantined_{QUARANTINE_INITIAL_ONLY_FIRST_NAME}"] == 4
    assert stats[f"quarantined_{QUARANTINE_TEMPORAL_GAP_EXCEEDS_5Y}"] == 1
    assert stats["quarantined_multiple_reasons"] == 1

    # Quarantined pair retains valid content-only digest invariant across release_id, is refused for
    # auto-publish / batch inclusion, and can still enter manual identity_review adjudication.
    with pytest.raises(ValueError, match="refused for auto-publish"):
        require_auto_publish_qualified(j_grossman)
    with pytest.raises(ValueError, match="refused for auto-publish"):
        build_precision_read_batch([qualified[0], j_grossman], total_pairs=2, planted_wrong_count=1)
    j_grossman_r2 = {**j_grossman, "release_id": "f" * 64, "source_generation_digest": "sha256:" + "f" * 64}
    assert j_grossman["content_digest"] == person_candidate_digest(j_grossman) == person_candidate_digest(j_grossman_r2)
    replayed = replay([], [j_grossman])[0]
    assert replayed["status"] == "pending" and replayed["in_review_queue"] is True

    # match_lda_witness_candidates preserves backward-compatible (qualified, stats) return and populates quarantined_out.
    q_out: list[dict[str, Any]] = []
    matched, matched_stats = match_lda_witness_candidates(witnesses, lda_rows, quarantined_out=q_out)
    assert [c["witness_key"] for c in matched] == ["w_al"]
    assert {c["witness_key"] for c in q_out} == set(by_wk)
    assert matched_stats["quarantined_total"] == 4


def test_guardrail_2_temporal_gap_cap_at_5_years():
    # Direct interval gap math checks.
    assert compute_temporal_gap_years((2024, 2024), [2019, 2020]) == 4
    assert compute_temporal_gap_years((2024, 2024), [2019]) == 5
    assert compute_temporal_gap_years((2014, 2014), [2019]) == 5
    assert compute_temporal_gap_years((2024, 2024), [2018]) == 6
    assert compute_temporal_gap_years((2014, 2014), [2007, 2008]) == 6
    assert compute_temporal_gap_years((2024, 2024), [2015, 2016]) == 8
    assert compute_temporal_gap_years((2026, 2026), [2006, 2009]) == 17
    assert compute_temporal_gap_years((2019, 2019), [2013, 2024]) == 0
    assert compute_temporal_gap_years((2024, 2024), []) is None

    witnesses = [
        # 3-year gap (even > 1095d like #47 Anthony F. Earley Jr.): ACCEPTED.
        _mk_witness("w_3y", "Anthony F. Earley Jr.", "DTE Energy", hearing_date=date(2011, 4, 15)),
        # Exact 5-year gap (hearing after LDA): ACCEPTED.
        _mk_witness("w_5y_after", "Taylor FiveAfter", "Policy Forum", hearing_date=date(2024, 9, 1)),
        # Exact 5-year gap (hearing before LDA): ACCEPTED.
        _mk_witness("w_5y_before", "Morgan FiveBefore", "Policy Forum", hearing_date=date(2014, 2, 1)),
        # 6-year gap (#25 / #50 boundary): QUARANTINED.
        _mk_witness("w_6y", "Dean Brenner", "Qualcomm Incorporated", hearing_date=date(2013, 6, 27)),
        # 6-year gap (hearing before LDA): QUARANTINED.
        _mk_witness("w_6y_before", "Casey SixBefore", "Policy Forum", hearing_date=date(2013, 1, 15)),
        # 8-year gap (#17 Emily Martin): QUARANTINED.
        _mk_witness("w_8y", "Emily Martin", "National Women's Law Center, Washington, DC", hearing_date=date(2024, 7, 23)),
        # 17-year gap with full first name: QUARANTINED by Guardrail 2 only.
        _mk_witness("w_17y", "Jordan Seventeen", "Policy Forum", hearing_date=date(2026, 6, 4)),
        # Missing LDA filing years: QUARANTINED fail-closed.
        _mk_witness("w_no_lda_yr", "Robin NoYears", "Policy Forum", hearing_date=date(2024, 5, 1)),
    ]
    lda_rows = [
        _mk_lda("41428", "ANTHONY", "EARLEY", "DTE ENERGY", suffix="jr", filing_year=2008),
        _mk_lda("5001", "TAYLOR", "FIVEAFTER", "POLICY FORUM", filing_year=2019),
        _mk_lda("5002", "MORGAN", "FIVEBEFORE", "POLICY FORUM", filing_year=2019),
        _mk_lda("17271", "DEAN", "BRENNER", "QUALCOMM, INCORPORATED", filing_year=2004),
        _mk_lda("17271", "DEAN", "BRENNER", "QUALCOMM, INCORPORATED", filing_year=2007),
        _mk_lda("5003", "CASEY", "SIXBEFORE", "POLICY FORUM", filing_year=2019),
        _mk_lda("62416", "EMILY", "MARTIN", "NATIONAL WOMEN'S LAW CENTER", filing_year=2015),
        _mk_lda("62416", "EMILY", "MARTIN", "NATIONAL WOMEN'S LAW CENTER", filing_year=2016),
        _mk_lda("5004", "JORDAN", "SEVENTEEN", "POLICY FORUM", filing_year=2009),
        _mk_lda("5005", "ROBIN", "NOYEARS", "POLICY FORUM", filing_year=None),
    ]

    qualified, quarantined, stats = partition_lda_witness_candidates(witnesses, lda_rows)
    assert {c["witness_key"] for c in qualified} == {"w_3y", "w_5y_after", "w_5y_before"}
    q_by_wk = {c["witness_key"]: c for c in quarantined}
    assert set(q_by_wk) == {"w_6y", "w_6y_before", "w_8y", "w_17y", "w_no_lda_yr"}
    assert q_by_wk["w_6y"]["temporal_gap_years"] == 6
    assert q_by_wk["w_6y_before"]["temporal_gap_years"] == 6
    assert q_by_wk["w_8y"]["temporal_gap_years"] == 8
    assert q_by_wk["w_17y"]["temporal_gap_years"] == 17
    for wk in ("w_6y", "w_6y_before", "w_8y", "w_17y"):
        assert q_by_wk[wk]["quarantine_reasons"] == [QUARANTINE_TEMPORAL_GAP_EXCEEDS_5Y]
    assert q_by_wk["w_no_lda_yr"]["quarantine_reasons"] == [QUARANTINE_NO_LDA_DATES]
    assert stats[f"quarantined_{QUARANTINE_TEMPORAL_GAP_EXCEEDS_5Y}"] == 4
    assert stats[f"quarantined_{QUARANTINE_NO_LDA_DATES}"] == 1


def test_guardrail_3_undated_congress_window_and_unbounded_corroboration():
    # Congress window conversion and metadata extraction checks.
    assert congress_year_window(107) == (2001, 2002)
    assert congress_year_window(116) == (2019, 2020)
    assert congress_year_window(119) == (2025, 2026)
    assert extract_hearing_congress({"hearing_congress": 116}) == 116
    assert extract_hearing_congress({"congress": "118"}) == 118
    assert extract_hearing_congress({"package_id": "CHRG-107shrg87708"}) == 107
    assert extract_hearing_congress({"package_id": None}) is None
    assert extract_hearing_congress({"hearing_congress": True}) is None

    # Eponymous firm matching checks (registrant firms vs universities/foundations/generic words/client-only).
    assert is_eponymous_firm_match(("schwartz",), "schwartz ballen llp", matched_roles={"registrant"}) is True
    assert is_eponymous_firm_match(("greenwood",), "greenwood environmental counsel pllc", matched_roles={"registrant"}) is True
    assert is_eponymous_firm_match(("holtz", "eakin"), "holtz eakin policy group llc", matched_roles={"registrant"}) is True
    assert is_eponymous_firm_match(("brown",), "brown university", matched_roles={"client", "registrant"}) is False
    assert is_eponymous_firm_match(("ford",), "ford foundation", matched_roles={"client", "registrant"}) is False
    assert is_eponymous_firm_match(("law",), "national law center", matched_roles={"registrant"}) is False
    assert is_eponymous_firm_match(("schwartz",), "schwartz ballen llp", matched_roles={"client"}) is False
    assert is_eponymous_firm_match(("li",), "li associates llc", matched_roles={"registrant"}) is False

    witnesses = [
        # 1. Undated with CHRG package_id -> 107th Congress (2001-2002), LDA 1999-2001 (gap=0): ACCEPTED.
        _mk_witness(
            "w_cong_ok",
            "Howard Metzenbaum",
            "Consumer Federation of America",
            hearing_date=None,
            package_id="CHRG-107shrg87708",
        ),
        # 2. Undated with explicit hearing_congress=107 (2001-2002), LDA 2007 (gap=5): ACCEPTED.
        _mk_witness(
            "w_cong_5y",
            "Dana Boundary",
            "Civic Forum",
            hearing_date=None,
            hearing_congress=107,
        ),
        # 3. Undated with hearing_congress=107 (2001-2002), LDA 2008 (gap=6) EVEN WITH matching middle initial: QUARANTINED!
        _mk_witness(
            "w_cong_6y",
            "Andrew J. Imparato",
            "American Association of People with Disabilities",
            hearing_date=None,
            package_id="CHRG-107shrg99999",
        ),
        # 4. Completely unbounded undated, corroborated by matching middle initial: ACCEPTED.
        _mk_witness(
            "w_unb_mid",
            "Alan P. Larson",
            "Covington & Burling LLP",
            hearing_date=None,
            source="senate_page",
        ),
        # 5. Completely unbounded undated, corroborated by matching multi-letter middle name: ACCEPTED.
        _mk_witness(
            "w_unb_midname",
            "Otto Juan Reich",
            "Foreign Policy Partners",
            hearing_date=None,
            source="senate_page",
        ),
        # 6. Completely unbounded undated, corroborated by matching generational suffix: ACCEPTED.
        _mk_witness(
            "w_unb_suf",
            "Ernest Stevens Jr.",
            "National Indian Gaming Association",
            hearing_date=None,
            source="senate_page",
        ),
        # 7. Completely unbounded undated, corroborated by eponymous registrant firm: ACCEPTED.
        _mk_witness(
            "w_unb_epon",
            "Gilbert T. Schwartz",
            "Schwartz & Ballen LLP",
            hearing_date=None,
            source="senate_page",
        ),
        # 8. Completely unbounded undated at university matching surname (NOT eponymous): QUARANTINED.
        _mk_witness(
            "w_unb_univ",
            "Sarah Brown",
            "Brown University",
            hearing_date=None,
            source="senate_page",
        ),
        # 9. Completely unbounded undated without corroboration (#31 James Stansel shape): QUARANTINED.
        _mk_witness(
            "w_unb_none",
            "James Stansel",
            "Pharmaceutical Research and Manufacturers of America",
            hearing_date=None,
            source="senate_page",
        ),
    ]
    lda_rows = [
        _mk_lda("17653", "HOWARD", "METZENBAUM", "CONSUMER FEDERATION OF AMERICA", filing_year=2001),
        _mk_lda("6001", "DANA", "BOUNDARY", "CIVIC FORUM", filing_year=2007),
        _mk_lda(
            "6002",
            "ANDREW",
            "IMPARATO",
            "AMERICAN ASSOCIATION OF PEOPLE WITH DISABILITIES",
            middle_name="J",
            filing_year=2008,
        ),
        _mk_lda("6003", "ALAN", "LARSON", "COVINGTON & BURLING LLP", middle_name="P", filing_year=2015),
        _mk_lda("6004", "OTTO JUAN", "REICH", "FOREIGN POLICY PARTNERS", filing_year=2014),
        _mk_lda("6005", "ERNEST", "STEVENS", "NATIONAL INDIAN GAMING ASSOCIATION", suffix="JR", filing_year=2018),
        _mk_lda(
            "42801",
            "GILBERT",
            "SCHWARTZ",
            "SCHWARTZ & BALLEN LLP",
            client_name="OTHER CLIENT CORP",
            filing_year=2007,
        ),
        _mk_lda("6006", "SARAH", "BROWN", "BROWN UNIVERSITY", filing_year=2019),
        _mk_lda(
            "66469",
            "JAMES",
            "STANSEL",
            "PHARMACEUTICAL RESEARCH AND MANUFACTURERS OF AMERICA",
            middle_name="C.",
            filing_year=2019,
        ),
    ]

    qualified, quarantined, stats = partition_lda_witness_candidates(witnesses, lda_rows)
    qual_by_wk = {c["witness_key"]: c for c in qualified}
    quar_by_wk = {c["witness_key"]: c for c in quarantined}

    assert set(qual_by_wk) == {"w_cong_ok", "w_cong_5y", "w_unb_mid", "w_unb_midname", "w_unb_suf", "w_unb_epon"}
    assert set(quar_by_wk) == {"w_cong_6y", "w_unb_univ", "w_unb_none"}

    # Verify Congress-window diagnostics.
    assert qual_by_wk["w_cong_ok"]["temporal_bound_source"] == "congress_window"
    assert qual_by_wk["w_cong_ok"]["hearing_congress"] == 107
    assert qual_by_wk["w_cong_ok"]["hearing_year_window"] == [2001, 2002]
    assert qual_by_wk["w_cong_ok"]["temporal_gap_years"] == 0
    assert qual_by_wk["w_cong_5y"]["temporal_gap_years"] == 5

    # Congress-bounded >5y gap is quarantined even with matching middle initial.
    assert quar_by_wk["w_cong_6y"]["temporal_bound_source"] == "congress_window"
    assert quar_by_wk["w_cong_6y"]["temporal_gap_years"] == 6
    assert quar_by_wk["w_cong_6y"]["quarantine_reasons"] == [QUARANTINE_TEMPORAL_GAP_EXCEEDS_5Y]

    # Verify unbounded corroboration diagnostics.
    assert qual_by_wk["w_unb_mid"]["undated_corroboration"] == ["middle_match"]
    assert qual_by_wk["w_unb_midname"]["undated_corroboration"] == ["middle_match"]
    assert qual_by_wk["w_unb_suf"]["undated_corroboration"] == ["suffix_match"]
    assert qual_by_wk["w_unb_epon"]["undated_corroboration"] == ["eponymous_firm"]

    # Verify uncorroborated unbounded quarantines.
    assert quar_by_wk["w_unb_univ"]["quarantine_reasons"] == [QUARANTINE_UNDATED_UNCORROBORATED]
    assert quar_by_wk["w_unb_none"]["quarantine_reasons"] == [QUARANTINE_UNDATED_UNCORROBORATED]
    assert stats["qualified_undated_congress_bounded"] == 2
    assert stats["qualified_undated_corroborated"] == 4
    assert stats[f"quarantined_{QUARANTINE_UNDATED_UNCORROBORATED}"] == 2

    # Verify batch-sheet rendering handles Congress-bounded and corroborated undated rows cleanly.
    batch = build_precision_read_batch(
        qualified,
        seed=20261127,
        total_pairs=5,
        planted_wrong_count=1,
        ct_release_id="453351a3" + "0" * 56,
        lobbying_generation_digest="sha256:" + "a" * 64,
        stats=stats,
    )
    rendered = render_person_batch_sheet(batch)
    check_person_batch_sheet(batch, rendered)
    assert "population_quarantined_total" in rendered
    assert "Cong. 107: 2001–2002" in rendered or "corrob=" in rendered
    for c in qualified:
        assert c["auto_publish_disposition"] == DISPOSITION_QUALIFIED
