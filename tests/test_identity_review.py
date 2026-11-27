"""Synthetic workflow controls; these are not real identity adjudications."""
import json
import os
import subprocess
import sys
from copy import deepcopy

import pytest

from spicy_regs.identity_review import (
    PERSON_RULE,
    PrivacyGuardError,
    candidate_digest,
    check_person_batch_sheet,
    make_decision,
    person_candidate_digest,
    render_person_batch_sheet,
    replay,
    verify_person_history,
)


def candidate():
    return {"candidate_id": "org_candidate_fixture", "candidate_id_status": "publication_scoped",
            "observed_name": "Example Organization", "target_namespace": "fec_committee",
            "candidate_target_id": "C00000000", "source_publication_json": {"artifact_digest": "sha256:" + "a" * 64},
            "matcher_evidence_json": {"method": "name_only"}}


def claim(c, decision="accepted", prior=None):
    return {"expected_candidate_digest": candidate_digest(c), "expected_event_id": prior,
            "decision": decision, "reviewer": "fixture-reviewer", "decided_at": "2026-09-27T12:00:00Z",
            "reason": "Synthetic workflow control, no real adjudication",
            "evidence": [{"kind": "reviewed_source", "stance": "supporting", "locator": "fixture://source",
                          "sha256": "sha256:" + "b" * 64, "claim": "Synthetic identity assertion"}],
            "scope": {"role": "acting_organization", "relationship": "same_entity", "valid_from": None,
                      "valid_to": None, "validity_basis": "Fixture undated assertion"}}


def person_candidate(**overrides):
    base = {
        "rule_version": PERSON_RULE,
        "candidate_id": "lda_witness:w_abc123:lda_lobbyist:100042",
        "subject_kind": "ct_witness_appearance",
        "witness_key": "w_abc123",
        "relation": "same_person",
        "external_native_id": "lda_lobbyist:100042",
        "stated_name": "Jane Q. Example",
        "stated_org": "American Widget Association",
        "body_sha256": "sha256:" + "1" * 64,
        "page_or_offset": "mods:witness[1]",
        "quoted_span": "CT witness 'Jane Q. Example' (American Widget Association) <-> LDA lda_lobbyist:100042",
        # Provenance columns strictly OUTSIDE candidate_digest:
        "release_id": "453351a3a57bdd28031a43de218832f5cd8b89c30048bc04354aaba37b337c47",
        "source_generation_digest": "sha256:" + "9" * 64,
        "hearing_date": "2024-05-15",
    }
    base.update(overrides)
    return base


def person_claim(c, decision="accepted", prior=None, **overrides):
    base = {
        "expected_candidate_digest": candidate_digest(c),
        "expected_event_id": prior,
        "decision": decision,
        "reviewer": "owner-fixture",
        "decided_at": "2026-11-27T14:00:00Z",
        "reason": "Synthetic LDA lobbyist and witness appearance control",
        "evidence": [{
            "kind": "hearing_record",
            "stance": "supporting",
            "locator": "ct:witness:w_abc123#mods:witness[1]",
            "sha256": "sha256:" + "1" * 64,
            "claim": "Hearing witness record matches LDA filing registrant and name",
        }],
        "scope": {
            "role": "witness_appearance_lobbyist",
            "relationship": "same_person",
            "valid_from": "2024-01-01",
            "valid_to": "2025-01-01",
            "validity_basis": "LDA filing year 2024 overlaps hearing date 2024-05-15",
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Existing publication-scoped-identity-review/1 organization tests (untouched)
# ---------------------------------------------------------------------------


def test_accept_revoke_reject_replay_keeps_history_and_never_publishes():
    c = candidate()
    accepted = make_decision(c, [], **claim(c))
    revoked = make_decision(c, [accepted], **claim(c, "revoked", accepted["event_id"]))
    rejected = make_decision(c, [accepted, revoked], **claim(c, "rejected", revoked["event_id"]))
    assert replay([], [c])[0]["status"] == "pending"
    assert replay([accepted], [c])[0]["status"] == "accepted"
    assert replay([accepted, revoked], [c])[0]["status"] == "revoked"
    result = replay([accepted, revoked, rejected], [c])[0]
    assert result["status"] == "rejected"
    assert not result["accepted_mapping_published"] and not result["money_attribution_permitted"]
    assert replay([accepted], [])[0]["status"] == "stale_candidate"


def test_stale_candidate_history_and_tampering_refuse():
    c = candidate()
    accepted = make_decision(c, [], **claim(c))
    changed = deepcopy(c)
    changed["matcher_evidence_json"]["method"] = "changed"
    with pytest.raises(ValueError, match="Stale candidate"):
        make_decision(changed, [], **claim(c))
    assert replay([accepted], [changed])[0]["status"] == "stale_candidate"
    with pytest.raises(ValueError, match="Stale history"):
        make_decision(c, [accepted], **claim(c, "revoked"))
    tampered = deepcopy(accepted)
    tampered["reason"] = "changed"
    with pytest.raises(ValueError, match="digest"):
        replay([tampered], [c])
    with pytest.raises(ValueError, match="Only an accepted"):
        make_decision(c, [], **claim(c, "revoked"))
    with pytest.raises(ValueError, match="Duplicate candidate"):
        replay([], [c, c])


def test_evidence_and_explicit_scope_required():
    c = candidate()
    for changes in ({"evidence": []}, {"scope": {}}, {"decided_at": "2026-09-27"}):
        with pytest.raises(ValueError):
            make_decision(c, [], **(claim(c) | changes))
    c["source_publication_json"] = {"status": "legacy_unversioned"}
    with pytest.raises(ValueError, match="SHA-256"):
        candidate_digest(c)


def test_cli_bounded_end_to_end_decision_replay_and_stale_refusal(tmp_path):
    c = candidate()
    candidates, review, history = (tmp_path / name for name in ("candidates.json", "review.json", "history.jsonl"))
    candidates.write_text(json.dumps([c]))
    review.write_text(json.dumps(claim(c)))
    base = [sys.executable, "-m", "spicy_regs.identity_review"]
    env = {**os.environ, "PYTHONPATH": "src"}
    result = subprocess.run([*base, "decide", "--candidates", str(candidates), "--history", str(history),
                             "--candidate-id", c["candidate_id"], "--review", str(review)], env=env,
                            check=True, capture_output=True, text=True)
    accepted = json.loads(result.stdout)
    again = subprocess.run([*base, "replay", "--candidates", str(candidates), "--history", str(history)],
                           env=env, check=True, capture_output=True, text=True)
    assert json.loads(again.stdout) == accepted
    before = history.read_bytes()
    rejected = subprocess.run([*base, "decide", "--candidates", str(candidates), "--history", str(history),
                               "--candidate-id", c["candidate_id"], "--review", str(review)],
                              env=env, capture_output=True)
    assert rejected.returncode != 0 and history.read_bytes() == before


def test_revocation_cannot_change_the_reviewed_scope():
    c = candidate()
    event = make_decision(c, [], **claim(c))
    wrong = claim(c, "revoked", event["event_id"])
    wrong["scope"]["relationship"] = "parent_of"
    with pytest.raises(ValueError, match="accepted decision scope"):
        make_decision(c, [event], **wrong)


# ---------------------------------------------------------------------------
# person-identity-review/1 tests (ss-39zl)
# ---------------------------------------------------------------------------


def test_person_digest_is_content_only_and_invariant_across_release_changes():
    c1 = person_candidate(
        release_id="453351a3a57bdd28031a43de218832f5cd8b89c30048bc04354aaba37b337c47",
        source_generation_digest="sha256:" + "a" * 64,
    )
    c2 = person_candidate(
        release_id="ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        source_generation_digest="sha256:" + "f" * 64,
        hearing_date="2024-05-16",
    )
    d1 = person_candidate_digest(c1)
    d2 = person_candidate_digest(c2)
    assert d1 == d2 == candidate_digest(c1) == candidate_digest(c2)

    # An accepted decision made on release 1 remains active (NOT stale) on release 2!
    accepted = make_decision(c1, [], **person_claim(c1))
    replayed = replay([accepted], [c2])[0]
    assert replayed["status"] == "accepted"
    assert replayed["effective"] is True
    assert replayed["in_review_queue"] is False

    # Mutating any of the 7 content fields MUST change candidate_digest.
    for field, new_val in (
        ("stated_name", "Jane R. Example"),
        ("stated_org", "American Widget Institute"),
        ("body_sha256", "sha256:" + "2" * 64),
        ("page_or_offset", "mods:witness[2]"),
        ("quoted_span", "Altered quoted span"),
        ("external_native_id", "lda_lobbyist:999999"),
    ):
        mutated = person_candidate(**{field: new_val})
        assert person_candidate_digest(mutated) != d1


def test_person_candidates_must_key_on_witness_key_and_reject_person_key():
    with pytest.raises(ValueError, match="never person_key"):
        person_candidate_digest(person_candidate(person_key="p_1234567890abcdef"))
    with pytest.raises(ValueError, match="never person_key"):
        person_candidate_digest(person_candidate(subject_person_key="p_1234567890abcdef"))
    with pytest.raises(ValueError, match="ct_witness_appearance"):
        person_candidate_digest(person_candidate(subject_kind="ct_person"))
    with pytest.raises(ValueError, match="witness_key"):
        person_candidate_digest(person_candidate(witness_key=""))
    with pytest.raises(ValueError, match="same_person"):
        person_candidate_digest(person_candidate(relation="possibly_same_person"))


def test_person_revoke_before_replace_stale_no_effect_and_rejection_blocks_repropose():
    c = person_candidate()
    accepted = make_decision(c, [], **person_claim(c))
    assert accepted["policy_version"] == PERSON_RULE
    assert accepted["witness_key"] == "w_abc123"
    assert accepted["previous_event_id"] is None

    # Cannot replace accepted decision without revoking first.
    with pytest.raises(ValueError, match="Revoke the accepted decision before replacing it"):
        make_decision(c, [accepted], **person_claim(c, decision="accepted", prior=accepted["event_id"]))
    with pytest.raises(ValueError, match="Revoke the accepted decision before replacing it"):
        make_decision(c, [accepted], **person_claim(c, decision="rejected", prior=accepted["event_id"]))

    # Stale decision (content changed) has NO effect and returns to the queue!
    changed = person_candidate(stated_name="Jane Updated Example")
    stale_replay = replay([accepted], [changed])[0]
    assert stale_replay["status"] == "stale_candidate"
    assert stale_replay["effective"] is False
    assert stale_replay["in_review_queue"] is True

    # Revoke accepted, then reject identical digest.
    revoked = make_decision(c, [accepted], **person_claim(c, decision="revoked", prior=accepted["event_id"]))
    assert revoked["previous_event_id"] == accepted["event_id"]
    rev_replay = replay([accepted, revoked], [c])[0]
    assert rev_replay["status"] == "revoked"
    assert rev_replay["effective"] is False
    assert rev_replay["in_review_queue"] is True

    rejected = make_decision(c, [accepted, revoked], **person_claim(c, decision="rejected", prior=revoked["event_id"]))
    assert rejected["previous_event_id"] == revoked["event_id"]
    history = [accepted, revoked, rejected]
    assert len(verify_person_history(history)) == 3

    # Rejection blocks re-proposing the identical digest!
    with pytest.raises(ValueError, match="Rejected candidate digest cannot be re-proposed"):
        make_decision(c, history, **person_claim(c, decision="accepted", prior=rejected["event_id"]))

    # Even under a brand-new candidate_id, an identical rejected digest is blocked in replay!
    reproposed_alias = person_candidate(candidate_id="lda_witness:w_abc123:reproposed")
    alias_replay = {r["candidate_id"]: r for r in replay(history, [reproposed_alias])}
    assert alias_replay["lda_witness:w_abc123:reproposed"]["status"] == "rejected"
    assert alias_replay["lda_witness:w_abc123:reproposed"]["blocked_by_rejection"] is True
    assert alias_replay["lda_witness:w_abc123:reproposed"]["in_review_queue"] is False
    assert alias_replay["lda_witness:w_abc123:reproposed"]["effective"] is False


@pytest.mark.parametrize("bad_kwargs", [
    {"external_native_id": "regulations_comment:EPA-HQ-2024-0001-0001"},
    {"external_native_id": "commenter:99812"},
    {"external_native_id": "fec_donor:D00012345"},
    {"external_native_id": "fec_contributor:987654"},
    {"external_native_id": "fec_schedule_a:SA11AI-1"},
    {"external_native_id": "linkedin:jane-example"},
    {"external_native_id": "third_party_profile:123"},
    {"source_kind": "regulations_commenter"},
    {"witness_type": "private_individual"},
    {"stated_org": "Private Citizen"},
    {"stated_org": "Self"},
    {"email": "jane@example.com"},
    {"phone_number": "202-555-0199"},
    {"birth_date": "1975-04-01"},
    {"family": "Spouse Name"},
    {"health": "redacted"},
    {"linkedin_url": "https://www.linkedin.com/in/jane-example"},
    {"quoted_span": "Contact jane@example.com for testimony copy"},
    {"quoted_span": "Call 202-555-0199 regarding lobbying disclosure"},
    {"quoted_span": "Matched via https://www.linkedin.com/in/jane-example"},
])
def test_person_privacy_denylist_rejects_prohibited_sources_and_fields(bad_kwargs):
    with pytest.raises(PrivacyGuardError):
        person_candidate_digest(person_candidate(**bad_kwargs))


def test_person_privacy_rejects_refusals_naming_private_individuals_and_bad_evidence():
    c = person_candidate()
    # Refusal mentioning private individual is rejected.
    with pytest.raises(PrivacyGuardError):
        make_decision(
            c,
            [],
            **person_claim(c, decision="rejected", reason="Refusal naming private individual John Doe"),
        )
    # Evidence pointing to comments.parquet or LinkedIn or private: is rejected.
    for bad_locator in (
        "r2://comments.parquet#row=10",
        "https://www.linkedin.com/in/jane-example",
        "private:sha256:" + "c" * 64,
        "fec_schedule_a://row/55",
    ):
        bad_ev = [{
            "kind": "reviewed_source",
            "stance": "supporting",
            "locator": bad_locator,
            "sha256": "sha256:" + "1" * 64,
            "claim": "External check",
        }]
        with pytest.raises(PrivacyGuardError):
            make_decision(c, [], **person_claim(c, evidence=bad_ev))


def test_person_hash_chain_continuity_and_tamper_detection():
    c1 = person_candidate(candidate_id="lda_witness:w1:lda_lobbyist:1", witness_key="w1", external_native_id="lda_lobbyist:1")
    c2 = person_candidate(candidate_id="lda_witness:w2:lda_lobbyist:2", witness_key="w2", external_native_id="lda_lobbyist:2")
    e1 = make_decision(c1, [], **person_claim(c1))
    e2 = make_decision(c2, [e1], **person_claim(c2, prior=e1["event_id"]))
    assert e1["previous_event_id"] is None
    assert e2["previous_event_id"] == e1["event_id"]
    assert len(verify_person_history([e1, e2])) == 2

    # Break hash chain by clearing previous_event_id.
    broken = deepcopy(e2)
    broken["previous_event_id"] = None
    with pytest.raises(ValueError, match="digest or sequence mismatch"):
        verify_person_history([e1, broken])


def test_person_batch_sheet_render_check_and_stale_marker(tmp_path):
    c = person_candidate()
    digest = person_candidate_digest(c)
    batch = {
        "metadata": {
            "title": "Test Precision Sheet",
            "rule_version": PERSON_RULE,
            "publication_status": "awaiting-owner-approval",
            "seed": 42,
            "ct_release_id": c["release_id"],
            "lobbying_generation_digest": c["source_generation_digest"],
            "population_appearances": 10,
            "population_distinct_names": 5,
            "population_distinct_lobbyists": 5,
            "real_pair_count": 1,
            "planted_control_count": 0,
            "summary": {"sample_total_pairs": 1},
        },
        "items": [{
            **c,
            "content_digest": digest,
            "lda_stated_name": "JANE Q EXAMPLE",
            "lda_matched_org": "AMERICAN WIDGET ASSOCIATION",
            "lda_matched_role": "registrant",
            "lda_filing_years": [2024],
            "lda_filing_count": 2,
            "date_overlap_status": "exact_period_overlap",
            "date_overlap_score": 1.0,
            "min_abs_days_gap": 0,
            "middle_guard": "both_match",
            "suffix_guard": "both_unstated",
            "source": "mods",
            "is_planted_control": False,
        }],
    }
    sheet = render_person_batch_sheet(batch)
    check_person_batch_sheet(batch, sheet)
    assert c["candidate_id"] in sheet and digest in sheet

    with pytest.raises(ValueError, match="out of sync"):
        check_person_batch_sheet(batch, sheet + "\nextra")

    stale_sheet = render_person_batch_sheet(
        batch,
        decisions={c["candidate_id"]: {
            "answer": "same_person",
            "date": "2026-11-27",
            "channel": "questionTool",
            "content_digest": "sha256:" + "0" * 64,
        }},
    )
    assert "**STALE**" in stale_sheet

    # Verify CLI batch-sheet write and --check.
    batch_path = tmp_path / "batch.json"
    sheet_path = tmp_path / "sheet.md"
    batch_path.write_text(json.dumps(batch))
    base = [sys.executable, "-m", "spicy_regs.identity_review", "batch-sheet",
            "--batch", str(batch_path), "--output", str(sheet_path)]
    env = {**os.environ, "PYTHONPATH": "src"}
    subprocess.run(base, env=env, check=True, capture_output=True)
    subprocess.run([*base, "--check"], env=env, check=True, capture_output=True)


def test_person_cli_stale_candidate_revoke_and_replace_no_deadlock(tmp_path):
    """Finding 2 regression: stale accepted decision can be revoked & replaced via CLI without deadlock."""
    c1 = person_candidate(stated_name="Jane Q. Example")
    c2 = person_candidate(stated_name="Jane Q. Example-Updated")
    d1 = person_candidate_digest(c1)
    d2 = person_candidate_digest(c2)
    assert d1 != d2

    candidates_path = tmp_path / "candidates.json"
    history_path = tmp_path / "history.jsonl"
    review_path = tmp_path / "review.json"
    base = [sys.executable, "-m", "spicy_regs.identity_review"]
    env = {**os.environ, "PYTHONPATH": "src"}

    # Step 1: accept c1 via CLI.
    candidates_path.write_text(json.dumps([c1]))
    review_path.write_text(json.dumps(person_claim(c1, decision="accepted", prior=None)))
    r1 = subprocess.run(
        [*base, "decide", "--candidates", str(candidates_path), "--history", str(history_path),
         "--candidate-id", c1["candidate_id"], "--review", str(review_path)],
        env=env, check=True, capture_output=True, text=True,
    )
    out1 = json.loads(r1.stdout)[0]
    assert out1["status"] == "accepted" and out1["effective"] is True
    e1_id = out1["latest_event"]["event_id"]

    # Step 2: content changes -> candidates.json now carries c2 under the same candidate_id.
    candidates_path.write_text(json.dumps([c2]))
    r_stale = subprocess.run(
        [*base, "replay", "--candidates", str(candidates_path), "--history", str(history_path)],
        env=env, check=True, capture_output=True, text=True,
    )
    stale_out = json.loads(r_stale.stdout)[0]
    assert stale_out["status"] == "stale_candidate"
    assert stale_out["effective"] is False
    assert stale_out["in_review_queue"] is True

    # Direct accept of c2 without revoking e1 must still fail (revoke-before-replace).
    review_path.write_text(json.dumps(person_claim(c2, decision="accepted", prior=e1_id)))
    direct = subprocess.run(
        [*base, "decide", "--candidates", str(candidates_path), "--history", str(history_path),
         "--candidate-id", c2["candidate_id"], "--review", str(review_path)],
        env=env, capture_output=True, text=True,
    )
    assert direct.returncode != 0

    # Step 3: revoke stale e1 via CLI while candidates.json holds c2 (must NOT deadlock!).
    review_path.write_text(json.dumps(person_claim(c1, decision="revoked", prior=e1_id)))
    r_rev = subprocess.run(
        [*base, "decide", "--candidates", str(candidates_path), "--history", str(history_path),
         "--candidate-id", c2["candidate_id"], "--review", str(review_path)],
        env=env, check=True, capture_output=True, text=True,
    )
    rev_out = json.loads(r_rev.stdout)[0]
    assert rev_out["latest_event"]["decision"] == "revoked"
    assert rev_out["in_review_queue"] is True
    e2_id = rev_out["latest_event"]["event_id"]

    # Step 4: accept c2 cleanly on top of the revocation.
    review_path.write_text(json.dumps(person_claim(c2, decision="accepted", prior=e2_id)))
    r_acc2 = subprocess.run(
        [*base, "decide", "--candidates", str(candidates_path), "--history", str(history_path),
         "--candidate-id", c2["candidate_id"], "--review", str(review_path)],
        env=env, check=True, capture_output=True, text=True,
    )
    acc2_out = json.loads(r_acc2.stdout)[0]
    assert acc2_out["status"] == "accepted"
    assert acc2_out["effective"] is True
    assert acc2_out["candidate_digest"] == d2
    e3_id = acc2_out["latest_event"]["event_id"]

    # Step 5: revoking via CLI also works when the candidate has vanished completely from candidates.json.
    candidates_path.write_text(json.dumps([]))
    review_path.write_text(json.dumps(person_claim(c2, decision="revoked", prior=e3_id)))
    r_rev_empty = subprocess.run(
        [*base, "decide", "--candidates", str(candidates_path), "--history", str(history_path),
         "--candidate-id", c2["candidate_id"], "--review", str(review_path)],
        env=env, check=True, capture_output=True, text=True,
    )
    rev_empty_out = json.loads(r_rev_empty.stdout)[0]
    assert rev_empty_out["latest_event"]["decision"] == "revoked"
    assert len(verify_person_history([json.loads(line) for line in history_path.read_text().splitlines()])) == 4
