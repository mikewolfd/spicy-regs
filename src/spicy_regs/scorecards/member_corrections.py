"""Reviewed source errors, corrected only for their exact admitted record.

The resolver still owns identity and period-qualified term matching. This rule
changes a copy of the geographic context, never the publisher's key or raw row.
"""

from collections.abc import Mapping
import hashlib
import json


_DOMENICI_KEY = ("lugar_mccourt:103-senate-html", "Senate:Domenici:Pete:NH:R")
_SOURCE_SHA256 = "bafadfe81fe4d4b8cd419177e26e9f0ad061cd8784a3fe6561d3dc4e5519932c"
_EDITION_SHA256 = "a8f3ffca9bc648e3c08408ae9efb30f12fe8fbe8e4e82fb23ec1d416617d8921"


def _digest(row: Mapping) -> str:
    return hashlib.sha256(
        json.dumps(dict(row), sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def reviewed_member_context(source: Mapping, edition: Mapping) -> tuple[Mapping, dict | None]:
    """Apply the reviewed Lugar 103rd Senate correction or leave context alone.

    Hashes bind the entire source row and edition, including raw state, name,
    identifiers, snapshot, capture, locator and period. A changed known record
    skips this correction and follows ordinary strict resolution; a spelling or
    name match alone cannot trigger a repair. No Bioguide identifier is supplied.
    """
    if (source.get("scorecard_id"), source.get("publisher_member_key")) != _DOMENICI_KEY:
        return source, None
    source_sha256, edition_sha256 = _digest(source), _digest(edition)
    if source_sha256 != _SOURCE_SHA256 or edition_sha256 != _EDITION_SHA256:
        return source, {
            "rule_id": "lugar-mccourt-103-domenici-state-v1",
            "status": "not_applied_source_changed",
            "reason": "The source row or edition differs from the reviewed correction; use ordinary strict matching.",
            "expected_record_sha256": "sha256:" + _SOURCE_SHA256,
            "expected_edition_sha256": "sha256:" + _EDITION_SHA256,
            "actual_record_sha256": "sha256:" + source_sha256,
            "actual_edition_sha256": "sha256:" + edition_sha256,
        }
    adjusted = dict(source)
    adjusted["state"] = "NM"
    return adjusted, {
        "rule_id": "lugar-mccourt-103-domenici-state-v1",
        "status": "applied",
        "field": "state",
        "original_value": "NH",
        "corrected_value": "NM",
        "reason": "The official 103rd Congress directory lists Pete V. Domenici under New Mexico senators.",
        "record_sha256": "sha256:" + _SOURCE_SHA256,
        "edition_sha256": "sha256:" + _EDITION_SHA256,
        "hash_encoding": "UTF-8 JSON; sorted keys; no whitespace; literal Unicode",
        "source_sha256": "sha256:93d623f58ab6aee8717d1411d97f553d614b6509c73375e298975c774cdb95ed",
        "reference_sha256": "sha256:1332031e48a71f90f591a2bed6d51d4352e7850ea8b86dfe8b9fd095e317433f",
        "official_evidence": {
            "document": "GPO-CDOC-108hdoc222-3-103.pdf",
            "sha256": "sha256:5715936a0af738b437607714426b481604cb0122a0a64e0c7d008c1d0b8ef40f",
            "byte_size": 172686,
            "pdf_page": 3,
            "printed_page": "506",
            "locator": "NEW MEXICO / SENATORS / Pete V. Domenici, Albuquerque",
        },
    }
