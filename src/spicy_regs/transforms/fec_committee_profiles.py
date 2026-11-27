"""Profiles of disclosed relationships using the maintained FEC relationship mapper."""

import hashlib
import json

import pyarrow as pa

from spicy_regs.transforms.fec_relationships import api_relationships

TABLE = "fec_committee_relationship_profiles"
POLICY = "fec-disclosed-relationships/1"
SCHEMA = pa.schema([(n, pa.string()) for n in (
    "profile_id committee_id committee_name designation source_sha256 source_url captured_at source_pointer "
    "relationships_json native_fields_json relationship_effective_date_status policy_version coverage_note"
).split()])


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def iter_committee_profiles(records, capture):
    """Yield one profile per native committee occurrence, never latest-by-retrieval.

    Both sponsor arrays remain distinct witnesses in relationships_json. A UI
    may group identical sponsor IDs for navigation, preserving every witness.
    Names, missing fields and conflicting source observations never become IDs.
    """
    digest = capture.get("response_sha256") or capture["sha256"]
    request_url = capture["request_url"]
    observed_at = capture.get("observed_at")
    for index, committee in enumerate(records):
        pointer = f"/results/{index}"
        cid = committee.get("committee_id")
        if not isinstance(cid, str) or not cid:
            raise ValueError("Committee profile lacks a native committee ID")
        evidence = {
            "sha256": digest,
            "url": request_url,
            "observed_at": observed_at,
            "locator": {"json_pointer": pointer},
        }
        relationships = list(api_relationships(committee, evidence=evidence))
        identity = [POLICY, digest, pointer, cid]
        yield dict(
            profile_id="sha256:" + hashlib.sha256(json_text(identity).encode()).hexdigest(),
            committee_id=cid,
            committee_name=committee.get("name"),
            designation=committee.get("designation"),
            source_sha256=digest,
            source_url=request_url,
            captured_at=observed_at,
            source_pointer=pointer,
            relationships_json=json_text(relationships),
            native_fields_json=json_text(committee),
            relationship_effective_date_status="not_supplied; filing and retrieval dates are not relationship dates",
            policy_version=POLICY,
            coverage_note="Selected retained committee occurrences. Disclosures do not prove ownership, transfers "
            "or candidate authorization. Unknown affiliation is not no affiliation.",
        )


def committee_profiles(records, capture):
    """One profile per native committee occurrence, never latest-by-retrieval."""
    return list(iter_committee_profiles(records, capture))
