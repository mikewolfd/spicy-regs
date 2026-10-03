"""Bounded exact-identifier access to the vendored, reviewed RefSpec projection.

REF-038 asserts undated roster identity. REF-072 supplies separately reviewed
bridges and succession evidence. Current-lineage lookup is not a date-qualified
identity assertion. No new mapping is made.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from importlib.resources import files

from spicy_regs.ontology import agencies

REGULATIONS = "regulations.gov:agency"
FEDERAL_REGISTER = "federal_register_agency"  # Existing occurrence-view namespace.


def _registry_evidence(namespace: str, identifier: str, candidates: list[dict]) -> dict:
    publication = agencies.registry_publication()  # refuses a manifest that is not the pinned one, first
    tables = {
        name: [dict(row) for row in agencies.registry_rows(name)] for name in ("bridges", "events", "non-emissions")
    }
    anchors = {row["org"] for row in candidates}
    if namespace == FEDERAL_REGISTER:
        anchors.add(agencies.FR_AGENCY_URN + identifier)
    # Retain the connected source evidence, without turning succession into identity.
    while True:
        previous = set(anchors)
        for row in tables["bridges"]:
            if row["subject"] in anchors or row["object"] in anchors:
                anchors.update((row["subject"], row["object"]))
        for row in tables["events"]:
            members = {*row["originals"], row["result"]}
            if members & anchors:
                anchors.update(members)
        if anchors == previous:
            break
    bridges = [row for row in tables["bridges"] if row["subject"] in anchors or row["object"] in anchors]
    events = [row for row in tables["events"] if {*row["originals"], row["result"]} & anchors]
    non_emissions = [
        row
        for row in tables["non-emissions"]
        if row.get("subject") in anchors
        or row.get("object") in anchors
        or (namespace == REGULATIONS and row.get("subject_value") == identifier)
    ]
    code = None
    applicable = (
        namespace == FEDERAL_REGISTER
        and identifier.isascii()
        and identifier.isdecimal()
        and str(int(identifier)) == identifier
    )
    if applicable:
        code = agencies.fr_agency_code(int(identifier))
    return {
        "bridges": bridges,
        "events": events,
        "non_emissions": non_emissions,
        "current_lineage": {
            "code": code,
            "status": "unique_code" if code else "no_unique_code" if applicable else "not_applicable",
            "policy": "Owner fr_agency_code follows current successors and bridges regardless of document date; a current successor's code wins over the agency's own; splits require all successors to agree.",
            "historical_identity_qualified": False,
        },
        "publication": publication,
    }


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
    unresolved = agencies.unresolved_rows()
    candidates = []
    abstentions = []
    if namespace == REGULATIONS:
        candidates = [dict(row) for row in rows if row["source_value"] == identifier]
        abstentions = [dict(row) for row in unresolved if row["source_value"] == identifier]
    elif namespace == FEDERAL_REGISTER:
        urn = agencies.FR_AGENCY_URN + identifier
        candidates = [dict(row) for row in rows if row["org"] == urn]
    status = (
        "unsupported_namespace"
        if namespace not in {REGULATIONS, FEDERAL_REGISTER}
        else (
            "contested"
            if candidates and abstentions
            else "ambiguous"
            if len(candidates) > 1
            else "reviewed_mapping"
            if candidates
            else "unmatched"
        )
    )
    registry = _registry_evidence(namespace, identifier, candidates)
    if status == "unmatched" and registry["events"]:
        status = "succession_evidence"
    elif status == "unmatched" and registry["bridges"]:
        status = "reviewed_bridge"
    if on_date is not None and status in {"reviewed_mapping", "reviewed_bridge", "succession_evidence"}:
        status = "temporal_scope_unqualified"
    return {
        "namespace": namespace,
        "identifier": identifier,
        "on_date": on_date,
        "status": status,
        "candidates": candidates,
        "abstentions": abstentions,
        "mapping_scope": "undated reviewed roster identity; parent_org is a separate relationship",
        "temporal_policy": "REF-072 event dates are retained as evidence; current-lineage lookup ignores dates and does not establish requested-date identity. Adjudication dates are not validity dates.",
        "registry_evidence": registry,
        "publication": {
            "view_id": manifest["viewId"],
            "manifest_sha256": "sha256:" + agencies.VIEW_MANIFEST_SHA256,
            "projection_sha256": "sha256:" + agencies.AGENCY_PROJECTION_SHA256,
            "unresolved_sha256": "sha256:" + agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256,
            "decision": manifest["agencyProjection"]["decision"],
            "projection_digest": manifest["agencyProjection"]["digest"],
        },
    }
