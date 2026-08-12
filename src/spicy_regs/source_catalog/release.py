"""Build and publish one sealed ``SourceCatalogRelease`` v1 bundle.

The bundle is six immutable files::

    release.json                            the root, identity stamped last
    manifests/global.json                   every member, sorted by objectKey
    data/source-items.json                  the requested universe U, one array
    schemas/source-catalog-release-v1.schema.json
    schemas/member-manifest-v1.schema.json
    schemas/source-items-v1.schema.json

Identity is content-derived and excludes the act of publishing::

    urn:spicy-regs:source-catalog-release:v1:
      SHA-256(canonical({format, formatVersion, content}))

``annotations`` — ``publishedAt``, ``releaseStatus``, ``buildRunId`` — sits
outside that preimage for the same reason DocumentRelease v3 keeps ``createdAt``
out of its own: a wall-clock stamp inside identity makes a reproducible build
unreproducible.  Two publishes of one selection therefore share one identity.

Order of construction is forced by the digests: rows, then member bytes, then
the manifest over those bytes, then the content over that manifest, then the
identity over that content.  The bundle is built whole in memory, gated against
the pinned schemas, written into a private directory, and exposed with one
atomic rename.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from spicy_regs.document_release_v3 import canonical_json_bytes, sha256_bytes
from spicy_regs.source_catalog.discovery import (
    DiscoveredItem,
    SourceOutcome,
    require_unique_source_item_ids,
)
from spicy_regs.source_catalog.records import (
    FORMAT,
    FORMAT_VERSION,
    GLOBAL_MANIFEST_OBJECT_KEY,
    MEMBER_MANIFEST_FORMAT,
    MEMBER_MANIFEST_VERSION,
    REFSPEC_IDENTIFIER_PREFIXES,
    RELEASE_ID_PREFIX,
    RELEASE_STATUSES,
    ROOT_OBJECT_KEY,
    SCHEMA_OBJECT_KEY_PREFIX,
    SOURCE_ITEMS_OBJECT_KEY,
    derive_counts,
    derive_coverage,
    disposition_of,
    release_identity,
    selected_ids,
    set_digest,
    universe_ids,
)
from spicy_regs.source_catalog.schema_pins import (
    SCHEMA_ROLES,
    pinned_schemas,
    schema_descriptors,
    schema_id,
    schema_set_identity,
)
from spicy_regs.source_catalog.universe import INSTANT_RE, SourceCatalogError, UniverseSpec
from spicy_regs.source_catalog.validate import validate_bundle_records


def select_source_items(spec: UniverseSpec, discovered: Iterable[DiscoveredItem]) -> list[dict[str, Any]]:
    """Apply the universe policy and return the wire rows, sorted by identity.

    Every discovered item becomes exactly one row carrying exactly one
    disposition, so every member of ``U`` is accounted for once and only once.
    A non-selected row always carries a machine-legible ``reasonCode`` and a
    human ``reason``.
    """

    ordered = sorted(require_unique_source_item_ids(discovered), key=lambda item: item.source_item_id)
    budget = spec.scope.max_items
    selected_document_ids: set[str] = set()
    rows: list[dict[str, Any]] = []

    for item in ordered:
        disposition, reason = _decide(spec, item, budget_remaining=budget)
        if disposition == "selected":
            if item.document_id in selected_document_ids:
                # For the MVP one selected sourceItemId maps to one documentId.
                # Grouping several source items into one document is out of
                # scope, so the second claimant is refused rather than merged.
                raise SourceCatalogError(
                    f"{item.source_item_id}: documentId {item.document_id!r} is already claimed by a "
                    "selected item; the MVP admits no many-to-one source-to-document mapping"
                )
            selected_document_ids.add(item.document_id)
            if budget is not None:
                budget -= 1
        rows.append(_row(spec, item, disposition, reason))
    return rows


def _decide(
    spec: UniverseSpec, item: DiscoveredItem, *, budget_remaining: int | None
) -> tuple[str, tuple[str, str] | None]:
    """Return one disposition and, when non-selected, its reason pair."""

    if item.outcome is not SourceOutcome.AVAILABLE:
        # The source already settled this one; the policy does not re-decide it.
        return str(item.outcome), (str(item.outcome_reason_code), str(item.outcome_reason))
    draft = item.normalized
    if draft is None:
        return "failed", (
            "source.metadata-unusable",
            "The source served no usable metadata record, so no normalized view exists.",
        )
    out_of_scope = spec.scope.admits(draft, location_key=item.location_key)
    if out_of_scope is not None:
        return "excluded", out_of_scope
    missing = draft.missing_required_field()
    if missing is not None:
        # The schema admits no null here and this producer invents no
        # placeholder, so the item states its gap instead of hiding it.
        return "failed", (
            "source.normalized-field-missing",
            f"The source states no usable {missing}; the normalized field set requires one.",
        )
    _, agency_failure = spec.normalization.agencies(draft.agency_ids)
    if agency_failure is not None:
        return "failed", agency_failure
    if not item.renditions:
        return "unavailable", (
            "source.no-candidate-rendition",
            "The source offers no rendition to capture, so this item cannot become a document.",
        )
    if budget_remaining is not None and budget_remaining <= 0:
        return "excluded", (
            "policy.item-budget-exhausted",
            f"The universe admits at most {spec.scope.max_items} items and that budget is spent.",
        )
    return "selected", None


def _normalized_metadata(spec: UniverseSpec, item: DiscoveredItem) -> dict[str, Any] | None:
    """The ten normalized MVP fields, or ``None`` when the source falls short.

    ``null`` here is the schema's own answer for a non-selected item whose
    source never served usable metadata.  A selected item never reaches it:
    ``_decide`` has already given any such item a non-selected disposition.
    """

    draft = item.normalized
    if draft is None or draft.missing_required_field() is not None:
        return None
    agencies, failure = spec.normalization.agencies(draft.agency_ids)
    if failure is not None or agencies is None:
        return None
    return {
        "agencies": agencies,
        "commentCloseDate": draft.comment_close_date,
        "docketIds": list(draft.docket_ids),
        "documentType": draft.document_type,
        "language": spec.normalization.language,
        "lastUpdatedDate": draft.last_updated_date,
        "publicationDate": draft.publication_date,
        "regulationIdentifierNumbers": list(draft.regulation_identifier_numbers),
        "sourceUrl": draft.source_url,
        "title": draft.title,
    }


def _row(spec: UniverseSpec, item: DiscoveredItem, disposition: str, reason: tuple[str, str] | None) -> dict[str, Any]:
    selection: dict[str, Any] = {"disposition": disposition}
    if disposition != "selected":
        if reason is None:  # pragma: no cover - _decide always supplies one
            raise SourceCatalogError(f"{item.source_item_id}: {disposition} requires a reason")
        selection["reasonCode"], selection["reason"] = reason
    for topic in item.observed_topics:
        for value in (topic.observed_topic_id, topic.observed_topic_scheme):
            if value.lower().startswith(REFSPEC_IDENTIFIER_PREFIXES):
                raise SourceCatalogError(
                    f"{item.source_item_id}: a source-observed topic must not carry a RefSpec concept "
                    f"identifier, got {value!r}"
                )
    return {
        "candidateRenditions": [rendition.as_record() for rendition in item.renditions],
        "documentId": item.document_id,
        "normalizedMetadata": _normalized_metadata(spec, item),
        "selection": selection,
        "sourceIssuedVersion": item.source_issued_version,
        "sourceItemId": item.source_item_id,
        "sourceNativeMetadata": dict(item.source_native_metadata),
        "sourceObservations": [observation.as_record() for observation in item.observations],
        "sourceObservedTopics": [topic.as_record() for topic in item.observed_topics],
    }


@dataclass(frozen=True)
class SourceCatalogBundle:
    """One complete bundle in memory, already gated, ready to write."""

    release_id: str
    root: Mapping[str, Any]
    manifest: Mapping[str, Any]
    items: tuple[Mapping[str, Any], ...]
    files: Mapping[str, bytes]

    def write(self, output_dir: Path | str) -> Path:
        """Materialize the bundle and expose it with one atomic rename."""

        return _write_bundle(self, Path(output_dir))


def build_source_catalog_release(
    spec: UniverseSpec,
    discovered: Iterable[DiscoveredItem],
    *,
    published_at: str,
    release_status: str | None = None,
    build_run_id: str | None = None,
) -> SourceCatalogBundle:
    """Build one complete, gated bundle, or refuse."""

    if INSTANT_RE.fullmatch(published_at) is None:
        raise SourceCatalogError(f"publishedAt must be a UTC instant, got {published_at!r}")
    if release_status is not None and release_status not in RELEASE_STATUSES:
        raise SourceCatalogError(f"releaseStatus must be one of {list(RELEASE_STATUSES)}, got {release_status!r}")

    items = select_source_items(spec, discovered)
    schemas = pinned_schemas()

    files: dict[str, bytes] = {SOURCE_ITEMS_OBJECT_KEY: canonical_json_bytes(items)}
    members = [
        _member(
            SOURCE_ITEMS_OBJECT_KEY,
            files[SOURCE_ITEMS_OBJECT_KEY],
            role="source-items",
            record_count=len(items),
            member_schema_id=schema_id("source-items"),
        )
    ]
    for role in SCHEMA_ROLES:
        schema = schemas[role]
        object_key = f"{SCHEMA_OBJECT_KEY_PREFIX}{schema.file_name}"
        files[object_key] = schema.payload
        members.append(
            _member(
                object_key,
                schema.payload,
                role="schema",
                record_count=None,
                member_schema_id=schema.schema_id,
            )
        )
    members.sort(key=lambda member: member["objectKey"])
    total_member_byte_size = sum(int(member["byteSize"]) for member in members)

    manifest = {
        "counts": {
            "memberCount": len(members),
            "totalByteSize": total_member_byte_size,
            "totalRecordCount": sum(member["recordCount"] or 0 for member in members),
        },
        "format": MEMBER_MANIFEST_FORMAT,
        "formatVersion": MEMBER_MANIFEST_VERSION,
        "manifestId": "global:global",
        "members": members,
        "scope": {"id": "global", "kind": "global"},
    }
    manifest_bytes = canonical_json_bytes(manifest)
    files[GLOBAL_MANIFEST_OBJECT_KEY] = manifest_bytes

    content = {
        "catalogId": spec.catalog_id,
        "counts": derive_counts(items, member_count=len(members), total_member_byte_size=total_member_byte_size),
        "coverage": derive_coverage(items),
        "globalManifest": {
            "byteSize": len(manifest_bytes),
            "manifestId": "global:global",
            "objectKey": GLOBAL_MANIFEST_OBJECT_KEY,
            "scopeId": "global",
            "scopeKind": "global",
            "sha256": sha256_bytes(manifest_bytes),
        },
        "requestedUniverseSetDigest": set_digest(universe_ids(items)),
        "schemaSet": {"schemaSetId": schema_set_identity(schemas), "schemas": schema_descriptors(schemas)},
        "selectedSourceSetDigest": set_digest(selected_ids(items)),
        "selectionPolicy": spec.selection_policy_record(),
        "sourceSystem": spec.source_system_record(),
    }
    annotations: dict[str, Any] = {"publishedAt": published_at}
    if release_status is not None:
        annotations["releaseStatus"] = release_status
    if build_run_id is not None:
        annotations["buildRunId"] = build_run_id

    root: dict[str, Any] = {
        "annotations": annotations,
        "content": content,
        "format": FORMAT,
        "formatVersion": FORMAT_VERSION,
    }
    root["releaseId"] = release_identity(root)
    files[ROOT_OBJECT_KEY] = canonical_json_bytes(root)

    # The producer-side gate; see `validate.py` for why it is not the contract
    # authority. Nothing reaches the filesystem until it passes.
    validate_bundle_records(root=root, manifest=manifest, items=items)

    return SourceCatalogBundle(
        release_id=str(root["releaseId"]),
        root=root,
        manifest=manifest,
        items=tuple(items),
        files=files,
    )


def _member(
    object_key: str, payload: bytes, *, role: str, record_count: int | None, member_schema_id: str
) -> dict[str, Any]:
    return {
        "byteSize": len(payload),
        "mediaType": "application/schema+json" if role == "schema" else "application/json",
        "objectKey": object_key,
        "recordCount": record_count,
        "role": role,
        "schemaId": member_schema_id,
        "sha256": sha256_bytes(payload),
    }


def _write_bundle(bundle: SourceCatalogBundle, output_dir: Path) -> Path:
    output_dir = output_dir.resolve()
    if output_dir.exists() or output_dir.is_symlink():
        raise SourceCatalogError(f"refusing to replace existing output: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    work_root = output_dir.parent / f".{output_dir.name}.building-{uuid.uuid4().hex}"
    work_root.mkdir()
    try:
        for object_key, payload in sorted(bundle.files.items()):
            path = work_root.joinpath(*object_key.split("/"))
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(payload)
        _atomic_publish(work_root, output_dir)
    finally:
        if work_root.exists():
            _remove_tree(work_root)
    return output_dir


def _atomic_publish(work_root: Path, output_dir: Path) -> None:
    """Rename the private build directory under the v3 publication lock.

    Imported here rather than at module scope so building a bundle in memory
    costs nothing from the Parquet writer stack; the lock itself is generic
    over bundles, so this reuses it instead of spelling a second rename dance.
    """

    from spicy_regs.document_release_v3 import DocumentReleaseV3Error
    from spicy_regs.document_release_v3_writer import atomic_publish_directory

    try:
        atomic_publish_directory(work_root, output_dir)
    except DocumentReleaseV3Error as error:
        raise SourceCatalogError(str(error)) from error


def _remove_tree(root: Path) -> None:
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_dir() and not path.is_symlink():
            path.rmdir()
        else:
            path.unlink(missing_ok=True)
    root.rmdir()


def publish_source_catalog_release(
    spec: UniverseSpec,
    discovered: Iterable[DiscoveredItem],
    output_dir: Path | str,
    *,
    published_at: str,
    release_status: str | None = None,
    build_run_id: str | None = None,
) -> dict[str, Any]:
    """Build, gate, and publish one bundle; return its publication receipt."""

    bundle = build_source_catalog_release(
        spec,
        discovered,
        published_at=published_at,
        release_status=release_status,
        build_run_id=build_run_id,
    )
    path = bundle.write(output_dir)
    content = bundle.root["content"]
    return {
        "catalogId": content["catalogId"],
        "counts": dict(content["counts"]),
        "coverage": dict(content["coverage"]),
        "output": str(path),
        "releaseId": bundle.release_id,
        "requestedUniverseSetDigest": content["requestedUniverseSetDigest"],
        "selectedSourceSetDigest": content["selectedSourceSetDigest"],
        "selectionPolicy": dict(content["selectionPolicy"]),
    }


__all__ = [
    "FORMAT",
    "FORMAT_VERSION",
    "RELEASE_ID_PREFIX",
    "SourceCatalogBundle",
    "build_source_catalog_release",
    "disposition_of",
    "publish_source_catalog_release",
    "select_source_items",
]
