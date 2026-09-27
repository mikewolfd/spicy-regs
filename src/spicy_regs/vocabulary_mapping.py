"""Bounded exact-identifier access to the vendored, reviewed RefSpec projection.

REF-038 asserts undated roster identity. It does not supply historical succession,
label-equivalence rules, or cross-source topic identity. No new mapping is made.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date
from importlib.resources import files

from spicy_regs.ontology import agencies

REGULATIONS = "regulations.gov:agency"
FEDERAL_REGISTER = "federal_register_agency"  # Existing occurrence-view namespace.


def lookup_agency(namespace: str, identifier: str, *, on_date: str | None = None) -> dict:
    """Return reviewed candidates and abstentions; never match labels or parents."""
    if not isinstance(identifier, str) or not identifier or len(identifier) > 256:
        raise ValueError("A bounded exact native identifier is required")
    if on_date is not None:
        date.fromisoformat(on_date)
    root = files("spicy_regs").joinpath("reference/refspec")
    raw = root.joinpath("view-manifest.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != agencies.VIEW_MANIFEST_SHA256:
        raise ValueError("RefSpec manifest differs from its pinned bytes")
    manifest = json.loads(raw)
    rows = agencies.projection_rows()
    unresolved = agencies._read_pinned(root.joinpath("agency-projection-unresolved.parquet"),
                                      agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256)
    candidates = []
    abstentions = []
    if namespace == REGULATIONS:
        candidates = [dict(row) for row in rows if row["source_value"] == identifier]
        abstentions = [dict(row) for row in unresolved if row["source_value"] == identifier]
    elif namespace == FEDERAL_REGISTER:
        urn = "urn:ref:federal-register-agency:" + identifier
        candidates = [dict(row) for row in rows if row["org"] == urn]
    status = "unsupported_namespace" if namespace not in {REGULATIONS, FEDERAL_REGISTER} else (
        "contested" if candidates and abstentions else "ambiguous" if len(candidates) > 1 else
        "reviewed_mapping" if candidates else "unmatched")
    if on_date is not None and status == "reviewed_mapping":
        status = "temporal_scope_unqualified"
    return {"namespace": namespace, "identifier": identifier, "on_date": on_date,
            "status": status, "candidates": candidates, "abstentions": abstentions,
            "mapping_scope": "undated reviewed roster identity; parent_org is a separate relationship",
            "temporal_policy": "REF-072 succession artifacts are not installed; adjudication dates are not validity dates",
            "publication": {"view_id": manifest["viewId"], "manifest_sha256": "sha256:" + agencies.VIEW_MANIFEST_SHA256,
                            "projection_sha256": "sha256:" + agencies.AGENCY_PROJECTION_SHA256,
                            "unresolved_sha256": "sha256:" + agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256,
                            "decision": manifest["agencyProjection"]["decision"],
                            "projection_digest": manifest["agencyProjection"]["digest"]}}
