"""Explicit local decisions over exact published identity candidates.

Supports two independent review rules through one replay owner:

* ``publication-scoped-identity-review/1`` (:data:`RULE`): organization candidates bound to
  one source publication pin. Kept untouched for organization identity review.
* ``person-identity-review/1`` (:data:`PERSON_RULE`): cross-source person identity decisions
  keyed strictly on stable Congressional Archive appearance ``witness_key`` (never CT
  ``person_key``), with content-only ``candidate_digest`` hashes (keeping ``release_id`` and
  source generation digests strictly outside the hash as provenance) and hard privacy guards
  excluding Regulations.gov commenters, FEC donors (52 U.S.C. 30111(a)(4)), LinkedIn /
  third-party scraped profiles, contact/birth/family/health fields, and refusals naming
  private individuals.

Run ``python -m spicy_regs.identity_review --help``. The append-only JSONL history
is a local review record, not an accepted RefSpec mapping or a money-attribution
edge. Replays retain stale/revoked decisions and never apply them to new inputs.

``inspect --candidates candidates.json`` prints each exact review digest.
``decide --candidates candidates.json --history decisions.jsonl --candidate-id ID
--review review.json`` records an explicit decision. The review JSON supplies
``expected_candidate_digest``, ``expected_event_id`` (null for empty history),
``decision`` (accepted/rejected/revoked), ``reviewer``, timezone-aware
``decided_at``, ``reason``, ``evidence`` and ``scope``. Each evidence object names
kind (source_identifier/reviewed_source, or hearing_record/lda_filing for person rules),
stance (supporting/conflicting), locator, sha256 and claim. Scope names role, relationship,
valid_from/valid_to (nullable, half-open ISO dates) and validity_basis. ``replay`` takes
candidates and history and prints current dispositions plus the exact last event for each
candidate. ``batch-sheet`` renders or verifies (``--check``) an owner precision-review
sheet keyed by candidate ID, content digest, channel, and date.
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
from typing import Any

RULE = "publication-scoped-identity-review/1"
PERSON_RULE = "person-identity-review/1"
SUPPORTED_RULES = frozenset({RULE, PERSON_RULE})

DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
ROLES = {"unknown", "acting_organization", "represented_client", "employer", "recipient"}
RELATIONS = {"same_entity", "parent_of", "subsidiary_of", "represents", "employs"}

PERSON_ROLES = frozenset({"hearing_witness", "registered_lobbyist", "witness_appearance_lobbyist"})
PERSON_RELATIONS = frozenset({"same_person"})
PERSON_EVIDENCE_KINDS = frozenset({"source_identifier", "reviewed_source", "hearing_record", "lda_filing"})
#: Strict allowlist of external person-record identifier schemes permitted under ``person-identity-review/1``.
ALLOWED_PERSON_SCHEMES = frozenset({"lda_lobbyist"})
#: Exact content-only fields hashed into ``candidate_digest`` for ``person-identity-review/1``.
#: Provenance locators (``release_id``, ``source_generation_digest``, etc.) stay strictly outside.
PERSON_DIGEST_FIELDS = (
    "stated_name",
    "stated_org",
    "body_sha256",
    "page_or_offset",
    "quoted_span",
    "external_native_id",
    "rule_version",
)
#: Bounded replay ceiling for organization logs (/1) and person logs (person-identity-review/1).
ORG_MAX_EVENTS = 10_000
PERSON_MAX_EVENTS = 100_000

_SCHEME_ID_RE = re.compile(r"([a-z][a-z0-9_]*):(.+)\Z")
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}(?!\d)")
_LINKEDIN_RE = re.compile(r"linkedin\.com|rapidapi|brightdata|serpapi", re.IGNORECASE)

#: Hard privacy denylist of field names prohibited anywhere in person candidates or decisions.
FORBIDDEN_PRIVACY_FIELDS = frozenset({
    "email",
    "email_address",
    "phone",
    "phone_number",
    "mobile",
    "telephone",
    "home_address",
    "residential_address",
    "street_address",
    "personal_address",
    "contact_details",
    "contact_info",
    "birth_date",
    "date_of_birth",
    "dob",
    "birthday",
    "age",
    "ssn",
    "family",
    "family_members",
    "spouse",
    "children",
    "relatives",
    "health",
    "health_info",
    "medical",
    "diagnosis",
    "disability",
    "linkedin",
    "linkedin_url",
    "linkedin_profile",
    "social_profile",
    "third_party_profile",
    "scraped_profile",
    "private_evidence",
    "donor_id",
    "contributor_id",
    "fec_donor_id",
    "commenter_id",
    "comment_id",
    "private_individual_name",
    "refusal_private_individual",
})

#: Prohibited source/scheme/type tokens covering commenters, FEC donors (52 U.S.C. 30111(a)(4)),
#: LinkedIn / third-party scraped profiles, private evidence, and private individuals.
FORBIDDEN_SOURCE_TOKENS = frozenset({
    "commenter",
    "regulations_comment",
    "regulations_commenter",
    "comment_author",
    "comments.parquet",
    "regulations.gov/comment",
    "fec_donor",
    "fec_contributor",
    "fec_individual",
    "fec_schedule_a",
    "schedule_a",
    "small_donor",
    "donor",
    "contributor",
    "individual_contribution",
    "linkedin",
    "scraped_profile",
    "third_party_profile",
    "private_evidence",
    "private_refusal",
    "private_individual",
})

#: Non-institutional organization markers that indicate a private individual rather than a public org.
_PRIVATE_ORG_MARKERS = frozenset({
    "",
    "self",
    "private citizen",
    "private individual",
    "individual",
    "citizen",
    "none",
    "n/a",
    "na",
})

_PRIVATE_REFUSAL_PHRASES = (
    "private individual",
    "private citizen",
    "commenter",
    "small donor",
    "fec donor",
    "fec contributor",
)


class PrivacyGuardError(ValueError):
    """Raised when a person-identity candidate or decision violates hard privacy exclusions."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def is_person_candidate(candidate: Mapping[str, Any]) -> bool:
    """Return True when ``candidate`` belongs to ``person-identity-review/1``."""
    return candidate.get("rule_version") == PERSON_RULE or candidate.get("rule") == PERSON_RULE


def _contains_forbidden_source_token(value: str) -> str | None:
    lowered = value.casefold()
    if _LINKEDIN_RE.search(lowered):
        return "third-party/LinkedIn profile source"
    if lowered.startswith("private:"):
        return "private evidence locator"
    for token in FORBIDDEN_SOURCE_TOKENS:
        if re.search(rf"(?:^|[^a-z0-9]){re.escape(token)}(?:$|[^a-z0-9])", lowered):
            return token
    return None


def _scan_forbidden_fields(obj: Any, *, path: str = "") -> None:
    if isinstance(obj, Mapping):
        for key, value in obj.items():
            key_str = str(key)
            key_lower = key_str.casefold()
            if key_lower in FORBIDDEN_PRIVACY_FIELDS:
                raise PrivacyGuardError(f"Prohibited privacy field '{path}{key_str}' is forbidden in person identity records")
            _scan_forbidden_fields(value, path=f"{path}{key_str}.")
    elif isinstance(obj, list | tuple):
        for idx, item in enumerate(obj):
            _scan_forbidden_fields(item, path=f"{path}[{idx}].")


def _check_no_pii_text(text: str, *, field_name: str) -> None:
    if _EMAIL_RE.search(text):
        raise PrivacyGuardError(f"Contact detail (email) is forbidden in {field_name}")
    if _PHONE_RE.search(text):
        raise PrivacyGuardError(f"Contact detail (phone number) is forbidden in {field_name}")
    if _LINKEDIN_RE.search(text):
        raise PrivacyGuardError(f"Third-party/LinkedIn profile reference is forbidden in {field_name}")


def validate_external_person_id(external_native_id: Any) -> tuple[str, str]:
    """Validate ``scheme:native-id`` against the strict allowlist and privacy denylist."""
    if not isinstance(external_native_id, str) or not external_native_id.strip():
        raise ValueError("Person candidate needs a non-empty external_native_id")
    match = _SCHEME_ID_RE.fullmatch(external_native_id)
    if match is None or not match.group(2).strip():
        raise ValueError("external_native_id must use '<scheme>:<native-id>' format")
    scheme, native_id = match.group(1), match.group(2).strip()
    if forbidden := _contains_forbidden_source_token(external_native_id):
        raise PrivacyGuardError(
            f"External identifier '{external_native_id}' rejected by privacy denylist ({forbidden})"
        )
    if scheme not in ALLOWED_PERSON_SCHEMES:
        raise PrivacyGuardError(
            f"External scheme '{scheme}' is not in the person-identity allowlist {sorted(ALLOWED_PERSON_SCHEMES)}"
        )
    return scheme, native_id


def validate_person_privacy(payload: Mapping[str, Any], *, context: str = "candidate") -> None:
    """Enforce hard privacy exclusions for ``person-identity-review/1``.

    Rejects Regulations.gov commenters, FEC donors (52 U.S.C. 30111(a)(4)),
    LinkedIn / third-party scraped profiles, contact/birth/family/health fields,
    and refusals naming private individuals.
    """
    _scan_forbidden_fields(payload)

    if "external_native_id" in payload:
        validate_external_person_id(payload["external_native_id"])

    for source_key in ("external_scheme", "source_kind", "subject_source", "witness_type", "source"):
        val = payload.get(source_key)
        if isinstance(val, str) and val.strip():
            if forbidden := _contains_forbidden_source_token(val):
                raise PrivacyGuardError(
                    f"Prohibited source '{val}' in {context}.{source_key} ({forbidden})"
                )

    stated_org = payload.get("stated_org")
    if isinstance(stated_org, str):
        norm_org = re.sub(r"\s+", " ", stated_org.strip().casefold())
        if norm_org in _PRIVATE_ORG_MARKERS:
            raise PrivacyGuardError(
                f"Private-individual or empty stated_org '{stated_org}' is forbidden in {context}"
            )
        _check_no_pii_text(stated_org, field_name=f"{context}.stated_org")

    for text_field in ("stated_name", "quoted_span", "reason"):
        val = payload.get(text_field)
        if isinstance(val, str):
            _check_no_pii_text(val, field_name=f"{context}.{text_field}")

    scope = payload.get("scope")
    if isinstance(scope, Mapping):
        if scope.get("private_individual") is True:
            raise PrivacyGuardError("Refusals or decisions naming private individuals are forbidden")
        basis = scope.get("validity_basis")
        if isinstance(basis, str):
            _check_no_pii_text(basis, field_name=f"{context}.scope.validity_basis")

    evidence = payload.get("evidence")
    if isinstance(evidence, list):
        for idx, raw_item in enumerate(evidence):
            if not isinstance(raw_item, Mapping):
                continue
            item = dict(raw_item)
            for ev_key in ("kind", "locator", "claim"):
                ev_val = item.get(ev_key)
                if isinstance(ev_val, str):
                    if ev_key in ("kind", "locator"):
                        if forbidden := _contains_forbidden_source_token(ev_val):
                            raise PrivacyGuardError(
                                f"Prohibited evidence {ev_key} '{ev_val}' at {context}.evidence[{idx}] ({forbidden})"
                            )
                    _check_no_pii_text(ev_val, field_name=f"{context}.evidence[{idx}].{ev_key}")

    decision = payload.get("decision")
    if decision in {"rejected", "revoked"}:
        if payload.get("private_individual") is True:
            raise PrivacyGuardError("Refusals naming private individuals are forbidden")
        reason_text = str(payload.get("reason") or "").casefold()
        for phrase in _PRIVATE_REFUSAL_PHRASES:
            if phrase in reason_text:
                raise PrivacyGuardError(
                    f"Refusal naming private individuals/protected sources ('{phrase}') is forbidden"
                )
        if isinstance(evidence, list):
            for item in evidence:
                if isinstance(item, Mapping):
                    claim_text = str(item.get("claim") or "").casefold()
                    for phrase in _PRIVATE_REFUSAL_PHRASES:
                        if phrase in claim_text:
                            raise PrivacyGuardError(
                                f"Refusal evidence naming private individuals/protected sources ('{phrase}') is forbidden"
                            )


def person_candidate_digest(candidate: Mapping[str, Any]) -> str:
    """Compute the content-only SHA-256 digest for a ``person-identity-review/1`` candidate.

    Hashes strictly canonical JSON over ``(stated_name, stated_org, body_sha256,
    page_or_offset, quoted_span, external_native_id, rule_version)``. Provenance
    locators such as ``release_id`` and source generation digests are excluded so
    re-running on a newer CT release or lobbying generation never stales a decision.
    """
    rule_version = candidate.get("rule_version")
    if rule_version != PERSON_RULE:
        raise ValueError(f"Expected rule_version={PERSON_RULE!r}")
    if "rule" in candidate and candidate["rule"] != PERSON_RULE:
        raise ValueError(f"Conflicting rule={candidate['rule']!r} for person candidate")
    if not isinstance(candidate.get("candidate_id"), str) or not candidate["candidate_id"].strip():
        raise ValueError("Person candidate needs candidate_id")

    # Subject must be CT witness_key, NEVER CT person_key.
    if "person_key" in candidate or "subject_person_key" in candidate:
        raise ValueError("Person identity decisions must key on CT witness_key, never person_key")
    subject_kind = candidate.get("subject_kind")
    if subject_kind is not None and subject_kind != "ct_witness_appearance":
        raise ValueError("Person identity subject_kind must be 'ct_witness_appearance', never person_key")
    witness_key = candidate.get("witness_key")
    if not isinstance(witness_key, str) or not witness_key.strip():
        raise ValueError("Person candidate needs CT witness_key")

    relation = candidate.get("relation")
    if relation is not None and relation not in PERSON_RELATIONS:
        raise ValueError(f"Unsupported person relation {relation!r}; only 'same_person' is permitted")

    for field in PERSON_DIGEST_FIELDS:
        value = candidate.get(field)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Person candidate needs non-empty string field '{field}'")
    if not DIGEST.fullmatch(candidate["body_sha256"]):
        raise ValueError("Person candidate body_sha256 needs an exact SHA-256 digest ('sha256:<64 hex>')")

    validate_person_privacy(candidate, context="candidate")

    content_only = {field: candidate[field] for field in PERSON_DIGEST_FIELDS}
    return _digest(content_only)


def candidate_digest(candidate: Mapping[str, Any]) -> str:
    """Bind observed candidate fields; dispatches between org (/1) and person-identity-review/1."""
    if is_person_candidate(candidate):
        return person_candidate_digest(candidate)
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


def _validate_common_claim(
    event: Mapping[str, Any],
    *,
    roles: set[str] | frozenset[str],
    relations: set[str] | frozenset[str],
    evidence_kinds: set[str] | frozenset[str],
) -> None:
    if event.get("decision") not in {"accepted", "rejected", "revoked"}:
        raise ValueError("Unsupported review rule or decision")
    if not event.get("reviewer") or not event.get("reason"):
        raise ValueError("Reviewer and reason are required")
    when = datetime.fromisoformat(event["decided_at"])
    if when.tzinfo is None or when.utcoffset() is None:
        raise ValueError("Decision time requires a timezone")
    scope = event["scope"]
    if scope.get("role") not in roles or scope.get("relationship") not in relations:
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
        if (
            item.get("stance") not in {"supporting", "conflicting"}
            or item.get("kind") not in evidence_kinds
            or not item.get("locator")
            or not item.get("claim")
            or not DIGEST.fullmatch(str(item.get("sha256", "")))
        ):
            raise ValueError("Evidence needs a locator, digest, claim, kind and stance")
    if event["decision"] == "accepted" and not any(e["stance"] == "supporting" for e in evidence):
        raise ValueError("Acceptance requires supporting evidence")


def _validate_claim(event: Mapping[str, Any]) -> None:
    rule = event.get("rule")
    if rule == RULE:
        _validate_common_claim(
            event,
            roles=ROLES,
            relations=RELATIONS,
            evidence_kinds={"source_identifier", "reviewed_source"},
        )
        return
    if rule == PERSON_RULE:
        if event.get("policy_version") != PERSON_RULE:
            raise ValueError(f"Person review events require policy_version={PERSON_RULE!r}")
        if "person_key" in event or "subject_person_key" in event:
            raise ValueError("Person review events must key on CT witness_key, never person_key")
        candidate = event.get("candidate")
        if not isinstance(candidate, Mapping) or not is_person_candidate(candidate):
            raise ValueError("Person review event requires a person-identity-review/1 candidate")
        if event.get("witness_key") != candidate.get("witness_key"):
            raise ValueError("Event witness_key must match candidate witness_key")
        if event.get("external_native_id") != candidate.get("external_native_id"):
            raise ValueError("Event external_native_id must match candidate external_native_id")
        _validate_common_claim(
            event,
            roles=PERSON_ROLES,
            relations=PERSON_RELATIONS,
            evidence_kinds=PERSON_EVIDENCE_KINDS,
        )
        validate_person_privacy(event, context="event")
        return
    raise ValueError("Unsupported review rule or decision")


def replay(history: Sequence[Mapping[str, Any]], candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Verify ordered event hashes and state transitions, then check current candidates.

    Preserves exact ``publication-scoped-identity-review/1`` output for organization workflows
    and adds content-only digest invariance, ``witness_key`` keying, rejection digest blocking,
    and stale-returns-to-queue semantics for ``person-identity-review/1``.
    """
    person_mode = any(is_person_candidate(c) for c in candidates) or any(
        e.get("rule") == PERSON_RULE for e in history
    )
    max_events = PERSON_MAX_EVENTS if person_mode else ORG_MAX_EVENTS
    if len(history) > max_events or len(candidates) > max_events:
        raise ValueError("Review scope exceeds the bounded local workflow")

    current: dict[str, str] = {}
    current_candidates: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        identity = candidate["candidate_id"]
        if identity in current:
            raise ValueError("Duplicate candidate identity")
        current[identity] = candidate_digest(candidate)
        current_candidates[identity] = candidate

    latest: dict[str, dict[str, Any]] = {}
    rejected_digests: dict[str, dict[str, Any]] = {}
    previous: str | None = None
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
        if event["rule"] == RULE:
            if prior and prior["candidate_digest"] != fingerprint:
                raise ValueError("A candidate identity cannot be reused for changed evidence")
            if prior and prior["decision"] == "accepted" and event["decision"] != "revoked":
                raise ValueError("Revoke the accepted decision before replacing it")
        else:
            if event["decision"] == "revoked" and prior and prior["candidate_digest"] != fingerprint:
                raise ValueError("Revocation must target the accepted candidate digest")
            if event["decision"] in {"accepted", "rejected"} and fingerprint in rejected_digests:
                raise ValueError("Rejected candidate digest cannot be re-proposed")
            if prior and prior["decision"] == "accepted" and event["decision"] != "revoked":
                raise ValueError("Revoke the accepted decision before replacing it")
            if event["decision"] == "rejected":
                rejected_digests[fingerprint] = {**event, "event_id": identifier}

        latest[identity] = {**event, "event_id": identifier}
        previous = identifier

    result: list[dict[str, Any]] = []
    for identity in sorted(set(current) | set(latest)):
        event = latest.get(identity)
        cand = current_candidates.get(identity)
        is_person = (cand is not None and is_person_candidate(cand)) or (
            event is not None and event.get("rule") == PERSON_RULE
        )
        if not is_person:
            status = "pending" if event is None else event["decision"]
            if event and current.get(identity) != event["candidate_digest"]:
                status = "stale_candidate"
            result.append({
                "candidate_id": identity,
                "status": status,
                "latest_event": event,
                "accepted_mapping_published": False,
                "money_attribution_permitted": False,
            })
            continue

        cur_digest = current.get(identity)
        blocked_by_rejection = bool(cur_digest and cur_digest in rejected_digests)
        if blocked_by_rejection:
            status = "rejected"
            effective = False
            in_review_queue = False
            effective_event = event if (event and event["candidate_digest"] == cur_digest) else rejected_digests[cur_digest]
        elif event is None:
            status = "pending"
            effective = False
            in_review_queue = True
            effective_event = None
        elif cur_digest != event["candidate_digest"]:
            # Stale decision has NO effect and its current candidate returns to the queue.
            status = "stale_candidate"
            effective = False
            in_review_queue = identity in current
            effective_event = event
        else:
            status = event["decision"]
            effective = status == "accepted"
            in_review_queue = status == "revoked"
            effective_event = event

        ref_cand = cand if cand is not None else (event["candidate"] if event is not None else {})
        result.append({
            "candidate_id": identity,
            "rule": PERSON_RULE,
            "witness_key": ref_cand.get("witness_key"),
            "external_native_id": ref_cand.get("external_native_id"),
            "candidate_digest": cur_digest,
            "status": status,
            "effective": effective,
            "in_review_queue": in_review_queue,
            "blocked_by_rejection": blocked_by_rejection,
            "latest_event": effective_event,
            "accepted_mapping_published": False,
            "money_attribution_permitted": False,
        })
    return result


def make_decision(
    candidate: Mapping[str, Any],
    history: Sequence[Mapping[str, Any]],
    *,
    expected_candidate_digest: str,
    expected_event_id: str | None,
    decision: str,
    reviewer: str,
    decided_at: str,
    reason: str,
    evidence: list[dict[str, Any]],
    scope: dict[str, Any],
) -> dict[str, Any]:
    """Construct a decision only for the exact reviewed candidate and history tip."""
    fingerprint = candidate_digest(candidate)
    if fingerprint != expected_candidate_digest:
        raise ValueError("Stale candidate: reviewed digest differs from current candidate")
    last = history[-1]["event_id"] if history else None
    if expected_event_id != last:
        raise ValueError("Stale history: review the latest decision before appending")

    if is_person_candidate(candidate):
        event: dict[str, Any] = {
            "rule": PERSON_RULE,
            "policy_version": PERSON_RULE,
            "witness_key": candidate["witness_key"],
            "external_native_id": candidate["external_native_id"],
            "candidate": dict(candidate),
            "candidate_digest": fingerprint,
            "previous_event_id": last,
            "decision": decision,
            "reviewer": reviewer,
            "decided_at": decided_at,
            "reason": reason,
            "evidence": evidence,
            "scope": scope,
        }
    else:
        event = {
            "rule": RULE,
            "candidate": dict(candidate),
            "candidate_digest": fingerprint,
            "previous_event_id": last,
            "decision": decision,
            "reviewer": reviewer,
            "decided_at": decided_at,
            "reason": reason,
            "evidence": evidence,
            "scope": scope,
        }
    event["event_id"] = _digest(event)
    replay([*history, event], [candidate])
    return event


def verify_person_history(history: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Verify a complete ``person-identity-review/1`` hash-chained decision log end-to-end.

    Returns the verified events in log order (with ``event_id`` and ``previous_event_id`` intact).
    Refuses any non-person rule event, broken hash chain, privacy violation, or invalid transition.
    """
    for raw in history:
        if raw.get("rule") != PERSON_RULE:
            raise ValueError(f"Expected only {PERSON_RULE!r} events in person decision log")
    # Replay against all candidates referenced in the log (keyed by candidate_id -> latest seen candidate).
    latest_candidates: dict[str, Mapping[str, Any]] = {}
    for raw in history:
        cand = raw.get("candidate")
        if isinstance(cand, Mapping) and isinstance(cand.get("candidate_id"), str):
            latest_candidates[cand["candidate_id"]] = cand
    replay(history, list(latest_candidates.values()))
    return [dict(raw) for raw in history]


def render_person_batch_sheet(
    batch: Mapping[str, Any],
    decisions: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    """Render a deterministic Markdown owner-review batch sheet from a precision-read JSON payload.

    Follows the RefSpec adjudication-sheet precedent (``--check`` fails if the rendered sheet
    disagrees with the batch JSON or decisions): every row is keyed by ``candidate_id`` +
    ``content_digest`` + ``channel`` + ``date``.
    """
    items = batch.get("items", [])
    meta = batch.get("metadata", {})
    decisions_map = decisions or {}

    def _cell(value: Any) -> str:
        text = "" if value is None else str(value)
        return text.replace("|", "\\|").replace("\n", " ").strip()

    lines = [
        "<!-- markdownlint-disable MD013 -->",
        "",
        f"# {meta.get('title', 'Person Identity Precision Read — Owner Adjudication Sheet')}",
        "",
        f"- **Rule version:** `{meta.get('rule_version', PERSON_RULE)}`",
        f"- **Status:** `{meta.get('publication_status', 'awaiting-owner-approval')}` (live publication blocked until explicit owner approval)",
        f"- **Seed:** `{meta.get('seed')}`",
        f"- **CT release bound:** `{meta.get('ct_release_id')}`",
        f"- **Lobbying generation:** `{meta.get('lobbying_generation_digest')}`",
        f"- **Batch digest:** `{_digest({'metadata': meta, 'items': items})}`",
        f"- **Population matched:** {meta.get('population_appearances', 0)} appearances across {meta.get('population_distinct_names', 0)} normalized names ({meta.get('population_distinct_lobbyists', 0)} LDA lobbyists)",
        f"- **Sample size:** {len(items)} pairs ({meta.get('real_pair_count', 0)} sampled real pairs + {meta.get('planted_control_count', 0)} planted negative control(s))",
        "",
        "> **Do not edit this sheet directly.** Record rule or item decisions keyed by `candidate_id`, `content_digest`, `channel`, and `date`; re-run with `--check` to verify exact parity.",
        "",
        "## Summary Breakdown",
        "",
        "| Metric | Value |",
        "| --- | --- |",
    ]
    for key, val in sorted((meta.get("summary") or {}).items()):
        lines.append(f"| {_cell(key)} | {_cell(val)} |")

    lines.extend([
        "",
        "## Seeded Precision-Read Pairs",
        "",
        "| # | Sample Kind | Candidate ID | CT Witness (`witness_key`, Name, Org, Hearing Date) | LDA Lobbyist (`external_native_id`, Name, Matched Org/Role, Filing Years) | Date Overlap | Guards | Quoted Evidence | Owner Decision / Date / Channel |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    for idx, item in enumerate(items, start=1):
        cid = item["candidate_id"]
        digest = item["content_digest"]
        dec = decisions_map.get(cid)
        if dec is None:
            dec_cell = "pending"
        elif dec.get("content_digest") != digest:
            dec_cell = f"**STALE** ({_cell(dec.get('answer'))}, {_cell(dec.get('date'))}, {_cell(dec.get('channel'))})"
        else:
            dec_cell = f"**{_cell(dec.get('answer'))}** ({_cell(dec.get('date'))}, {_cell(dec.get('channel'))})"

        ct_desc = (
            f"`{_cell(item['witness_key'])}` — {_cell(item['stated_name'])} "
            f"({_cell(item['stated_org'])}; hearing: {_cell(item.get('hearing_date') or 'undated')}; "
            f"source: `{_cell(item.get('source'))}`, `{_cell(item.get('page_or_offset'))}`)"
        )
        years = item.get("lda_filing_years") or []
        year_span = f"{min(years)}–{max(years)}" if years else "n/a"
        lda_desc = (
            f"`{_cell(item['external_native_id'])}` — {_cell(item.get('lda_stated_name'))} "
            f"({_cell(item.get('lda_matched_role'))}: {_cell(item.get('lda_matched_org'))}; "
            f"years: {year_span}, filings: {item.get('lda_filing_count', 0)})"
        )
        gap_val = item.get("min_abs_days_gap")
        gap_str = f"{gap_val}d" if gap_val is not None else "n/a"
        overlap_desc = (
            f"`{_cell(item.get('date_overlap_status'))}` "
            f"(score={item.get('date_overlap_score')}, gap={gap_str})"
        )
        guards_desc = (
            f"mid=`{_cell(item.get('middle_guard'))}`, suf=`{_cell(item.get('suffix_guard'))}`"
        )
        kind_label = "**PLANTED WRONG CONTROL**" if item.get("is_planted_control") else "real"
        lines.append(
            f"| {idx} | {kind_label} | `{_cell(cid)}` | {ct_desc} | {lda_desc} | "
            f"{overlap_desc} | {guards_desc} | {_cell(item['quoted_span'])} | {dec_cell} |"
        )

    lines.extend([
        "",
        "## Decision Keys",
        "",
        "Record each owner decision under its `candidate_id` with `answer` (`same_person` / `different_person` / `uncertain`), `date`, `channel`, and the exact `content_digest` below.",
        "",
        "| Candidate ID | Control Flag | Content Digest |",
        "| --- | --- | --- |",
    ])
    for item in items:
        flag = "planted_wrong_control" if item.get("is_planted_control") else "sampled_real"
        lines.append(f"| `{_cell(item['candidate_id'])}` | `{flag}` | `{_cell(item['content_digest'])}` |")
    lines.append("")
    return "\n".join(lines)


def check_person_batch_sheet(
    batch: Mapping[str, Any],
    sheet_text: str,
    decisions: Mapping[str, Mapping[str, Any]] | None = None,
) -> None:
    """Fail closed if ``sheet_text`` disagrees with ``batch`` or ``decisions``."""
    expected = render_person_batch_sheet(batch, decisions)
    if sheet_text != expected:
        raise ValueError("Precision-review batch sheet is out of sync with batch JSON/decisions")


def _read_json(path: Path) -> Any:
    if path.stat().st_size > 10 * 1024 * 1024:
        raise ValueError("Input exceeds 10 MiB review bound")
    return json.loads(path.read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "decide", "replay", "batch-sheet"))
    parser.add_argument("--candidates", type=Path, help="JSON list of identity candidates")
    parser.add_argument("--history", type=Path)
    parser.add_argument("--candidate-id")
    parser.add_argument("--review", type=Path, help="JSON make_decision keyword arguments, including expected digests")
    parser.add_argument("--batch", type=Path, help="JSON precision-read batch for batch-sheet rendering/checking")
    parser.add_argument("--decisions", type=Path, help="Optional JSON mapping of owner batch decisions")
    parser.add_argument("--output", type=Path, help="Markdown sheet path for batch-sheet command")
    parser.add_argument("--check", action="store_true", help="Verify existing batch sheet matches batch JSON exactly")
    args = parser.parse_args()

    if args.command == "batch-sheet":
        if args.batch is None or args.output is None:
            parser.error("batch-sheet requires --batch and --output")
        batch = _read_json(args.batch)
        decisions = _read_json(args.decisions) if args.decisions is not None else None
        rendered = render_person_batch_sheet(batch, decisions)
        if args.check:
            check_person_batch_sheet(batch, args.output.read_text(encoding="utf-8"), decisions)
            print(json.dumps({"status": "ok", "checked": str(args.output), "items": len(batch.get("items", []))}))
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
            print(json.dumps({"status": "written", "output": str(args.output), "items": len(batch.get("items", []))}))
        return

    if args.candidates is None:
        parser.error("--candidates is required")
    candidates = _read_json(args.candidates)
    if not isinstance(candidates, list) or len(candidates) > PERSON_MAX_EVENTS:
        raise ValueError("Candidates must be a bounded JSON list")
    if args.command == "inspect":
        print(json.dumps([{"candidate_id": c["candidate_id"], "candidate_digest": candidate_digest(c)} for c in candidates], indent=2))
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
            (candidate,) = [c for c in candidates if c["candidate_id"] == args.candidate_id]
            event = make_decision(candidate, history, **_read_json(args.review))
            stream.seek(0, os.SEEK_END)
            stream.write(_canonical(event).decode() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
            history.append(event)
        print(json.dumps(replay(history, candidates), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
