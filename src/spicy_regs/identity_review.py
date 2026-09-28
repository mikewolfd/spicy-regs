"""Explicit local decisions over exact published identity candidates.

Run ``python -m spicy_regs.identity_review --help``. The append-only JSONL history
is a local review record, not an accepted RefSpec mapping or a money-attribution
edge. Replays retain stale/revoked decisions and never apply them to new inputs.

``inspect --candidates candidates.json`` prints each exact review digest.
``decide --candidates candidates.json --history decisions.jsonl --candidate-id ID
--review review.json`` records an explicit decision. The review JSON supplies
``expected_candidate_digest``, ``expected_event_id`` (null for empty history),
``decision`` (accepted/rejected/revoked), ``reviewer``, timezone-aware
``decided_at``, ``reason``, ``evidence`` and ``scope``. Each evidence object names
kind (source_identifier/reviewed_source), stance (supporting/conflicting), locator,
sha256 and claim. Scope names role, relationship, valid_from/valid_to (nullable,
half-open ISO dates) and validity_basis. ``replay`` takes candidates and history
and prints current dispositions plus the exact last event for each candidate.
These assertions document the reviewer claim; digests alone do not prove it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path

RULE = "publication-scoped-identity-review/1"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
ROLES = {"unknown", "acting_organization", "represented_client", "employer", "recipient"}
RELATIONS = {"same_entity", "parent_of", "subsidiary_of", "represents", "employs"}


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def candidate_digest(candidate: Mapping) -> str:
    """Bind all observed candidate fields, including competing match evidence."""
    if not candidate.get("candidate_id") or candidate.get("candidate_id_status") != "publication_scoped":
        raise ValueError("A publication-scoped candidate is required")
    pin = candidate.get("source_publication_json")
    if isinstance(pin, str):
        pin = json.loads(pin)
    if not isinstance(pin, dict) or not DIGEST.fullmatch(str(pin.get("artifact_digest") or pin.get("sha256") or "")):
        raise ValueError("Candidate source publication needs an exact SHA-256 pin")
    for field in ("observed_name", "target_namespace", "candidate_target_id"):
        if not isinstance(candidate.get(field), str) or not candidate[field]:
            raise ValueError(f"Candidate needs {field}")
    return _digest(dict(candidate))


def _validate_claim(event: Mapping) -> None:
    if event.get("rule") != RULE or event.get("decision") not in {"accepted", "rejected", "revoked"}:
        raise ValueError("Unsupported review rule or decision")
    if not event.get("reviewer") or not event.get("reason"):
        raise ValueError("Reviewer and reason are required")
    when = datetime.fromisoformat(event["decided_at"])
    if when.tzinfo is None or when.utcoffset() is None:
        raise ValueError("Decision time requires a timezone")
    scope = event["scope"]
    if scope.get("role") not in ROLES or scope.get("relationship") not in RELATIONS:
        raise ValueError("An explicit supported role and relationship are required")
    if not {"valid_from", "valid_to", "validity_basis"} <= scope.keys() or not scope["validity_basis"]:
        raise ValueError("Explicit validity bounds and basis are required, including undated claims")
    start, end = (date.fromisoformat(scope[k]) if scope[k] is not None else None for k in ("valid_from", "valid_to"))
    if start and end and start >= end:
        raise ValueError("Validity is a nonempty half-open interval")
    evidence = event["evidence"]
    if not isinstance(evidence, list) or not 1 <= len(evidence) <= 100:
        raise ValueError("One to 100 retained evidence references are required")
    for item in evidence:
        if (item.get("stance") not in {"supporting", "conflicting"} or
                item.get("kind") not in {"source_identifier", "reviewed_source"} or
                not item.get("locator") or not item.get("claim") or
                not DIGEST.fullmatch(str(item.get("sha256", "")))):
            raise ValueError("Evidence needs a locator, digest, claim, kind and stance")
    if event["decision"] == "accepted" and not any(e["stance"] == "supporting" for e in evidence):
        raise ValueError("Acceptance requires supporting evidence")


def replay(history: Sequence[Mapping], candidates: Sequence[Mapping]) -> list[dict]:
    """Verify ordered event hashes and state transitions, then check current pins."""
    if len(history) > 10_000 or len(candidates) > 10_000:
        raise ValueError("Review scope exceeds the bounded local workflow")
    current = {}
    for candidate in candidates:
        identity = candidate["candidate_id"]
        if identity in current:
            raise ValueError("Duplicate candidate identity")
        current[identity] = candidate_digest(candidate)
    latest: dict[str, dict] = {}
    previous = None
    for raw in history:
        event = dict(raw)
        identifier = event.pop("event_id")
        if _digest(event) != identifier or event.get("previous_event_id") != previous:
            raise ValueError("Review history digest or sequence mismatch")
        _validate_claim(event)
        candidate = event["candidate"]
        identity = candidate["candidate_id"]
        fingerprint = candidate_digest(candidate)
        if fingerprint != event["candidate_digest"]:
            raise ValueError("Candidate evidence differs from the reviewed bytes")
        prior = latest.get(identity)
        if event["decision"] == "revoked" and (not prior or prior["decision"] != "accepted"):
            raise ValueError("Only an accepted decision can be revoked")
        if event["decision"] == "revoked" and prior and event["scope"] != prior["scope"]:
            raise ValueError("Revocation must name the accepted decision scope")
        if prior and prior["candidate_digest"] != fingerprint:
            raise ValueError("A candidate identity cannot be reused for changed evidence")
        if prior and prior["decision"] == "accepted" and event["decision"] != "revoked":
            raise ValueError("Revoke the accepted decision before replacing it")
        latest[identity] = {**event, "event_id": identifier}
        previous = identifier
    result = []
    for identity in sorted(set(current) | set(latest)):
        event = latest.get(identity)
        status = "pending" if event is None else event["decision"]
        if event and current.get(identity) != event["candidate_digest"]:
            status = "stale_candidate"
        result.append({"candidate_id": identity, "status": status, "latest_event": event,
                       "accepted_mapping_published": False, "money_attribution_permitted": False})
    return result


def make_decision(candidate: Mapping, history: Sequence[Mapping], *, expected_candidate_digest: str,
                  expected_event_id: str | None, decision: str, reviewer: str, decided_at: str,
                  reason: str, evidence: list[dict], scope: dict) -> dict:
    """Construct a decision only for the exact reviewed candidate and history tip."""
    fingerprint = candidate_digest(candidate)
    if fingerprint != expected_candidate_digest:
        raise ValueError("Stale candidate: reviewed digest differs from current candidate")
    last = history[-1]["event_id"] if history else None
    if expected_event_id != last:
        raise ValueError("Stale history: review the latest decision before appending")
    event = {"rule": RULE, "candidate": dict(candidate), "candidate_digest": fingerprint,
             "previous_event_id": last, "decision": decision, "reviewer": reviewer,
             "decided_at": decided_at, "reason": reason, "evidence": evidence, "scope": scope}
    event["event_id"] = _digest(event)
    replay([*history, event], [candidate])
    return event


def _read_json(path: Path):
    if path.stat().st_size > 10 * 1024 * 1024:
        raise ValueError("Input exceeds 10 MiB review bound")
    return json.loads(path.read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "decide", "replay"))
    parser.add_argument("--candidates", type=Path, required=True, help="JSON list exported from org_identity_candidates")
    parser.add_argument("--history", type=Path)
    parser.add_argument("--candidate-id")
    parser.add_argument("--review", type=Path, help="JSON make_decision keyword arguments, including expected digests")
    args = parser.parse_args()
    candidates = _read_json(args.candidates)
    if not isinstance(candidates, list) or len(candidates) > 10_000:
        raise ValueError("Candidates must be a bounded JSON list")
    if args.command == "inspect":
        print(json.dumps([{ "candidate_id": c["candidate_id"], "candidate_digest": candidate_digest(c)} for c in candidates], indent=2))
        return
    if args.history is None:
        parser.error("--history is required")
    # Local POSIX file locking makes the read-tip/check/append one writer action.
    import fcntl
    with args.history.open("a+" if args.command == "decide" else "r", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX if args.command == "decide" else fcntl.LOCK_SH)
        stream.seek(0)
        raw = stream.read(10 * 1024 * 1024 + 1)
        if len(raw.encode()) > 10 * 1024 * 1024:
            raise ValueError("History exceeds 10 MiB review bound")
        if raw and not raw.endswith("\n"):
            raise ValueError("Review history has an incomplete final line")
        history = [json.loads(line) for line in raw.splitlines()]
        if args.command == "decide":
            if args.review is None or args.candidate_id is None:
                parser.error("--review and --candidate-id are required for a decision")
            candidate, = [c for c in candidates if c["candidate_id"] == args.candidate_id]
            event = make_decision(candidate, history, **_read_json(args.review))
            stream.seek(0, os.SEEK_END)
            stream.write(_canonical(event).decode() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
            history.append(event)
        print(json.dumps(replay(history, candidates), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
