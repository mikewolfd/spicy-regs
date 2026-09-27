"""Synthetic workflow controls; these are not real identity adjudications."""
import json
import os
import subprocess
import sys
from copy import deepcopy

import pytest

from spicy_regs.identity_review import candidate_digest, make_decision, replay


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
