"""Bounded exact-identifier and reviewed-alias access to RefSpec Agency Registry View 1.4.

REF-038 asserts undated roster identity. REF-072 supplies separately reviewed bridges and
succession evidence. REF-075 and REF-077 unify REF-038, all 472 pinned Federal Register
organizations, and 157 owner-reviewed free-text aliases (keyed by RefSpec ``name_key`` v1) into one
digest-pinned view. Current-lineage lookup is not a date-qualified identity assertion, and a
``partOf`` alias claims no identity. No new mapping is made.
"""

from __future__ import annotations

from datetime import date
from functools import cache
from typing import Any, Literal, Mapping, get_args

#: The namespaces lookup_agency accepts: a regulations.gov agency code, a Federal Register agency
#: id (the occurrence views' namespace), or a free-text agency/sub-unit name matched strictly by
#: RefSpec's published ``name_key`` against reviewed aliases and pinned FR organizations.
Namespace = Literal["regulations.gov:agency", "federal_register_agency", "name"]
REGULATIONS, FEDERAL_REGISTER, NAME = get_args(Namespace)

_COMPACT_ALIAS_FIELDS = (
    "alias_id",
    "name_key",
    "target",
    "target_release_key",
    "target_publisher_name",
    "fr_resource",
    "fr_publisher_name",
    "fr_relation",
    "basis",
)


def _compact_alias(row: Mapping[str, Any]) -> dict[str, Any]:
    """Bounded alias summary for identifier lookups; full passages and decisions come from ``name`` lookup."""
    return {field: row[field] for field in _COMPACT_ALIAS_FIELDS}


@cache
def _names(view_path: Any) -> dict[str, str]:
    """Each organization the vendored View 1.4 rows name, with that name; none is written here.

    RefSpec's preferred label for a projected organization or abstention comes first, then the
    publisher's name a row records for a resource: a mapping's source or target record, either end
    of a bridge, an event's result, or the pinned Federal Register organization table.
    """
    del view_path
    from spicy_regs.ontology import agencies

    names: dict[str, str] = {}
    for row in agencies.projection_rows():
        names.setdefault(row["org"], row["pref_label"])
    for row in agencies.unresolved_rows():
        names.setdefault(row["source_org"], row["pref_label"])
    for row in agencies.projection_rows():
        for record in row["evidence_records"] or ():
            for side in (record.get("source_record") or {}, record.get("target_record") or {}):
                if side.get("resource") and side.get("publisher_name"):
                    names.setdefault(side["resource"], side["publisher_name"])
    for row in agencies.registry_rows("bridges"):
        names.setdefault(row["subject"], row["subject_publisher_name"])
        names.setdefault(row["object"], row["object_publisher_name"])
    for row in agencies.registry_rows("events"):
        names.setdefault(row["result"], row["result_publisher_name"])
    for row in agencies.organization_rows():
        names.setdefault(row["resource_iri"], row["name"])
    return names


def _parent_labels(candidates: list[dict], abstentions: list[dict], registry: dict) -> dict[str, str | None]:
    """Each parent organization the reply names, with the name the vendored rows give it; null where none does."""
    from spicy_regs.ontology import agencies

    parents = [row["parent_org"] for row in candidates] + [row["source_parent_org"] for row in abstentions]
    parents += [row[end] for row in registry["bridges"] for end in ("subject_parent", "object_parent")]
    parents += [parent for row in registry["events"] for parent in row["original_parents"]]
    org = registry.get("organization")
    if isinstance(org, Mapping) and org.get("parent"):
        parents.append(org["parent"])
    known = _names(agencies.AGENCY_REGISTRY_VIEW_PATH)
    return {parent: known.get(parent) for parent in sorted(set(parents) - {None})}


def _exact_decimal_fr_id(identifier: str) -> int | None:
    if identifier.isascii() and identifier.isdecimal() and str(int(identifier)) == identifier:
        return int(identifier)
    return None


def _registry_evidence(
    namespace: str,
    identifier: str,
    candidates: list[dict],
    publication: dict[str, Any],
    *,
    matched_alias: dict[str, Any] | None = None,
    matched_org: dict[str, Any] | None = None,
    matched_name_key: str | None = None,
) -> dict[str, Any]:
    from spicy_regs.ontology import agencies  # here, not at import: the MCP server imports Namespace at start

    tables = {
        name: [dict(row) for row in agencies.registry_rows(name)] for name in ("bridges", "events", "non-emissions")
    }
    anchors = {row["org"] for row in candidates}
    primary_fr_id: int | None = None
    if namespace == FEDERAL_REGISTER:
        anchors.add(agencies.FR_AGENCY_URN + identifier)
        primary_fr_id = _exact_decimal_fr_id(identifier)
    elif namespace == REGULATIONS:
        fr_orgs = [
            row["org"]
            for row in candidates
            if isinstance(row.get("org"), str) and row["org"].startswith(agencies.FR_AGENCY_URN)
        ]
        if len(set(fr_orgs)) == 1:
            primary_fr_id = _exact_decimal_fr_id(fr_orgs[0].removeprefix(agencies.FR_AGENCY_URN))
    elif namespace == NAME:
        if matched_alias is not None:
            if matched_alias.get("fr_resource"):
                anchors.add(matched_alias["fr_resource"])
                primary_fr_id = _exact_decimal_fr_id(
                    matched_alias["fr_resource"].removeprefix(agencies.FR_AGENCY_URN)
                )
            if matched_alias.get("target"):
                anchors.add(matched_alias["target"])
        elif matched_org is not None:
            anchors.add(matched_org["resource_iri"])
            primary_fr_id = int(matched_org["fr_id"])

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
        or (
            namespace == NAME
            and matched_name_key
            and row.get("reason") == "aliasRejectedByOwner"
            and row.get("subject_value") == matched_name_key
        )
    ]

    if matched_alias is not None:
        aliases = [matched_alias]
    elif anchors:
        aliases = [
            _compact_alias(row)
            for row in agencies.alias_rows()
            if row.get("fr_resource") in anchors or row.get("target") in anchors
        ]
    else:
        aliases = []

    organization = matched_org
    if organization is None and primary_fr_id is not None:
        org_row = agencies.organization_by_fr_id(primary_fr_id)
        organization = dict(org_row) if org_row is not None else None

    code = None
    applicable = namespace == FEDERAL_REGISTER and primary_fr_id is not None
    if applicable and primary_fr_id is not None:
        code = agencies.fr_agency_code(primary_fr_id)

    evidence: dict[str, Any] = {
        "organization": organization,
        "aliases": aliases,
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
    if namespace == NAME:
        evidence["matched_name_key"] = matched_name_key
    return evidence


def lookup_agency(namespace: str, identifier: str, *, on_date: str | None = None) -> dict[str, Any]:
    """Return reviewed candidates, aliases, organizations, and abstentions from View 1.4.

    Labels are never keys under ``regulations.gov:agency`` or ``federal_register_agency``. Under
    ``name``, free text is keyed strictly with RefSpec's published ``name_key`` (never fuzzy)
    against the 157 owner-reviewed aliases and 472 pinned Federal Register organizations. Any other
    namespace is refused.
    """
    from spicy_regs.ontology import agencies

    if namespace not in (REGULATIONS, FEDERAL_REGISTER, NAME):
        raise ValueError(
            f"Unsupported namespace {namespace!r}; the namespaces are {REGULATIONS}, {FEDERAL_REGISTER}, and {NAME}"
        )
    if not isinstance(identifier, str) or not identifier or len(identifier) > 256:
        raise ValueError("A bounded exact native identifier is required")
    if on_date is not None:
        date.fromisoformat(on_date)

    registry_pub = agencies.registry_publication()  # refuses a manifest that is not the pinned View 1.4 bytes
    rows = agencies.projection_rows()
    unresolved = agencies.unresolved_rows()
    candidates: list[dict[str, Any]] = []
    abstentions: list[dict[str, Any]] = []
    matched_alias: dict[str, Any] | None = None
    matched_org: dict[str, Any] | None = None
    matched_name_key: str | None = None

    if namespace == REGULATIONS:
        candidates = [dict(row) for row in rows if row["source_value"] == identifier]
        abstentions = [dict(row) for row in unresolved if row["source_value"] == identifier]
    elif namespace == FEDERAL_REGISTER:
        urn = agencies.FR_AGENCY_URN + identifier
        candidates = [dict(row) for row in rows if row["org"] == urn]
    else:
        matched_name_key = agencies.name_key(identifier)
        alias_row = agencies.lookup_alias(identifier)
        if alias_row is not None:
            matched_alias = dict(alias_row)
        else:
            org_row = agencies.organization_by_name(identifier)
            if org_row is not None:
                matched_org = dict(org_row)

    status = (
        "contested"
        if candidates and abstentions
        else "ambiguous"
        if len(candidates) > 1
        else "reviewed_mapping"
        if candidates
        else "reviewed_alias"
        if matched_alias is not None
        else "reviewed_organization"
        if matched_org is not None
        else "unmatched"
    )
    registry = _registry_evidence(
        namespace,
        identifier,
        candidates,
        registry_pub,
        matched_alias=matched_alias,
        matched_org=matched_org,
        matched_name_key=matched_name_key,
    )
    if status == "unmatched" and registry["events"]:
        status = "succession_evidence"
    elif status == "unmatched" and registry["bridges"]:
        status = "reviewed_bridge"
    if on_date is not None and status in {
        "reviewed_mapping",
        "reviewed_bridge",
        "succession_evidence",
        "reviewed_alias",
        "reviewed_organization",
    }:
        status = "temporal_scope_unqualified"
    return {
        "namespace": namespace,
        "identifier": identifier,
        "on_date": on_date,
        "status": status,
        "candidates": candidates,
        "abstentions": abstentions,
        "mapping_scope": (
            "undated reviewed roster identity and RefSpec View 1.4 reviewed aliases; "
            "parent_org and partOf aliases are separate relationships and claim no identity"
        ),
        "temporal_policy": "REF-072 event dates are retained as evidence; current-lineage lookup ignores dates and does not establish requested-date identity. Adjudication dates are not validity dates.",
        "registry_evidence": registry,
        "parent_labels": _parent_labels(candidates, abstentions, registry),
        "publication": {
            "view_id": registry_pub["view_id"],
            "schema_version": registry_pub["schema_version"],
            "manifest_sha256": registry_pub["manifest_sha256"],
            "projection_sha256": "sha256:" + agencies.AGENCY_PROJECTION_SHA256,
            "unresolved_sha256": "sha256:" + agencies.AGENCY_PROJECTION_UNRESOLVED_SHA256,
            "decision": "REF-038",
            "projection_digest": registry_pub["release"]["agencyProjectionDigest"],
        },
    }
