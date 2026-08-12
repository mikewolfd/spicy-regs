"""The producer-side gate: no bundle is published that this refuses.

Two layers run over every produced bundle.

1. **Closed shape.**  The root, the member manifest, and every source-item row
   are validated with ``jsonschema`` Draft 2020-12 against the pinned Rulespec
   schema bytes.
2. **The rules a JSON Schema cannot state.**  A selected item needs a candidate
   rendition; a non-selected disposition needs both a machine-legible reason
   code and a human reason; identifiers are unique; a source-observed topic is
   not a RefSpec concept; and the identity, set digests, counts, and coverage
   must re-derive from the rows.

This gate is not the contract authority.  Cross-product verdict agreement
(SpicySearch `PLAN.md` "Execution order" step 7) later runs Rulespec's own
validator over shared fixtures, and that validator decides whether a bundle
conforms.  What lives here is the part SpicyRegs owns: refusing to publish a
bundle it already knows is wrong.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import jsonschema

from spicy_regs.source_catalog.records import (
    FORMAT,
    FORMAT_VERSION,
    GLOBAL_MANIFEST_OBJECT_KEY,
    MEMBER_MANIFEST_FORMAT,
    MEMBER_MANIFEST_VERSION,
    NON_SELECTED_DISPOSITIONS,
    REFSPEC_IDENTIFIER_PREFIXES,
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
    pinned_schema_document,
    pinned_schemas,
    schema_descriptors,
    schema_set_identity,
)
from spicy_regs.source_catalog.universe import SourceCatalogError


def _validate_against(value: Any, *, role: str, path: str) -> None:
    validator = jsonschema.Draft202012Validator(pinned_schema_document(role))
    errors = sorted(validator.iter_errors(value), key=lambda error: list(error.path))
    if errors:
        first = errors[0]
        location = "".join(f"/{part}" for part in first.path)
        raise SourceCatalogError(f"{path}{location} violates the pinned {role} schema: {first.message}")


def validate_release_root(root: Mapping[str, Any]) -> None:
    _validate_against(root, role="release-root", path="release.json")


def validate_member_manifest(manifest: Mapping[str, Any]) -> None:
    _validate_against(manifest, role="member-manifest", path=GLOBAL_MANIFEST_OBJECT_KEY)


def validate_source_items(items: Sequence[Mapping[str, Any]]) -> None:
    document = pinned_schema_document("source-items")
    validator = jsonschema.Draft202012Validator(document)
    for index, item in enumerate(items):
        errors = sorted(validator.iter_errors(item), key=lambda error: list(error.path))
        if errors:
            first = errors[0]
            location = "".join(f"/{part}" for part in first.path)
            raise SourceCatalogError(
                f"{SOURCE_ITEMS_OBJECT_KEY}/{index}{location} violates the pinned source-items schema: {first.message}"
            )


def validate_bundle_records(
    *,
    root: Mapping[str, Any],
    manifest: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
) -> None:
    """Refuse the bundle unless every shape, rule, and derivation holds."""

    if root.get("format") != FORMAT or root.get("formatVersion") != FORMAT_VERSION:
        raise SourceCatalogError(f"release.json must declare {FORMAT!r} version {FORMAT_VERSION!r}")
    if manifest.get("format") != MEMBER_MANIFEST_FORMAT or manifest.get("formatVersion") != MEMBER_MANIFEST_VERSION:
        raise SourceCatalogError(
            f"{GLOBAL_MANIFEST_OBJECT_KEY} must declare {MEMBER_MANIFEST_FORMAT!r} version {MEMBER_MANIFEST_VERSION!r}"
        )

    validate_release_root(root)
    validate_member_manifest(manifest)
    validate_source_items(items)

    _validate_identity(root)
    _validate_manifest_members(manifest)
    _validate_schema_set(root, manifest)
    _validate_items(items)
    _validate_derivations(root, manifest, items)


def _validate_identity(root: Mapping[str, Any]) -> None:
    expected = release_identity(root)
    if root.get("releaseId") != expected:
        raise SourceCatalogError(
            f"release.json/releaseId is {root.get('releaseId')!r}, but the content derives {expected}"
        )


def _validate_manifest_members(manifest: Mapping[str, Any]) -> None:
    members = list(manifest.get("members") or ())
    keys = [str(member.get("objectKey")) for member in members]
    if keys != sorted(keys):
        raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY}/members must be sorted by objectKey")
    if len(set(keys)) != len(keys):
        raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY}/members names one objectKey twice")
    for key in keys:
        if key.startswith("/") or "\\" in key or ".." in key.split("/"):
            raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY}/members holds an unsafe path {key!r}")
    data_members = [member for member in members if member.get("role") == "source-items"]
    if len(data_members) != 1:
        raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY} must declare exactly one source-items member")
    if data_members[0].get("objectKey") != SOURCE_ITEMS_OBJECT_KEY:
        raise SourceCatalogError(
            f"{GLOBAL_MANIFEST_OBJECT_KEY} must place the source items at {SOURCE_ITEMS_OBJECT_KEY}"
        )
    expected_counts = {
        "memberCount": len(members),
        "totalByteSize": sum(int(member.get("byteSize") or 0) for member in members),
        "totalRecordCount": sum(int(member.get("recordCount") or 0) for member in members),
    }
    if manifest.get("counts") != expected_counts:
        raise SourceCatalogError(
            f"{GLOBAL_MANIFEST_OBJECT_KEY}/counts is {manifest.get('counts')}, but the members derive {expected_counts}"
        )


def _validate_schema_set(root: Mapping[str, Any], manifest: Mapping[str, Any]) -> None:
    content = root.get("content")
    schema_set = content.get("schemaSet") if isinstance(content, Mapping) else None
    if not isinstance(schema_set, Mapping):
        raise SourceCatalogError("release.json/content/schemaSet is absent")
    schemas = pinned_schemas()
    expected_descriptors = schema_descriptors(schemas)
    if list(schema_set.get("schemas") or ()) != expected_descriptors:
        raise SourceCatalogError(
            "release.json/content/schemaSet/schemas must name the three pinned schemas, sorted by schemaId"
        )
    expected_id = schema_set_identity(schemas)
    if schema_set.get("schemaSetId") != expected_id:
        raise SourceCatalogError(
            f"release.json/content/schemaSet/schemaSetId is {schema_set.get('schemaSetId')!r}, "
            f"but the descriptors derive {expected_id}"
        )
    members_by_id = {
        str(member.get("schemaId")): member
        for member in manifest.get("members") or ()
        if member.get("role") == "schema"
    }
    for role in SCHEMA_ROLES:
        schema = schemas[role]
        member = members_by_id.get(schema.schema_id)
        if member is None:
            raise SourceCatalogError(f"the bundle declares no schema member for role {role!r}")
        if member.get("sha256") != schema.sha256 or member.get("byteSize") != len(schema.payload):
            raise SourceCatalogError(f"the {role!r} schema member differs from the pinned bytes")


def _validate_items(items: Sequence[Mapping[str, Any]]) -> None:
    seen_source_item_ids: set[str] = set()
    seen_selected_document_ids: set[str] = set()
    for index, item in enumerate(items):
        path = f"{SOURCE_ITEMS_OBJECT_KEY}/{index}"
        source_item_id = str(item.get("sourceItemId"))
        if source_item_id in seen_source_item_ids:
            raise SourceCatalogError(f"{path}/sourceItemId repeats {source_item_id!r}")
        seen_source_item_ids.add(source_item_id)

        disposition = disposition_of(item)
        selection = item.get("selection")
        if disposition in NON_SELECTED_DISPOSITIONS:
            for field_name in ("reasonCode", "reason"):
                if not (isinstance(selection, Mapping) and selection.get(field_name)):
                    raise SourceCatalogError(
                        f"{path}/selection/{field_name} is required for disposition {disposition!r}"
                    )
        elif isinstance(selection, Mapping) and (selection.get("reasonCode") or selection.get("reason")):
            raise SourceCatalogError(f"{path}/selection carries a reason for a selected item")

        renditions = list(item.get("candidateRenditions") or ())
        rendition_ids = [str(rendition.get("renditionId")) for rendition in renditions]
        if len(set(rendition_ids)) != len(rendition_ids):
            raise SourceCatalogError(f"{path}/candidateRenditions names one renditionId twice")

        if disposition == "selected":
            if not renditions:
                raise SourceCatalogError(f"{path}/candidateRenditions: a selected item requires at least one")
            if item.get("normalizedMetadata") is None:
                raise SourceCatalogError(f"{path}/normalizedMetadata: a selected item requires the full field set")
            document_id = str(item.get("documentId"))
            if document_id in seen_selected_document_ids:
                raise SourceCatalogError(f"{path}/documentId repeats selected {document_id!r}")
            seen_selected_document_ids.add(document_id)

        for topic_index, topic in enumerate(item.get("sourceObservedTopics") or ()):
            for field_name in ("observedTopicId", "observedTopicScheme"):
                value = topic.get(field_name)
                if isinstance(value, str) and value.lower().startswith(REFSPEC_IDENTIFIER_PREFIXES):
                    raise SourceCatalogError(
                        f"{path}/sourceObservedTopics/{topic_index}/{field_name}: a source-observed topic "
                        "must not carry a RefSpec concept identifier"
                    )


def _validate_derivations(
    root: Mapping[str, Any],
    manifest: Mapping[str, Any],
    items: Sequence[Mapping[str, Any]],
) -> None:
    content = root.get("content")
    if not isinstance(content, Mapping):
        raise SourceCatalogError("release.json/content is absent")
    for field_name, ids in (
        ("requestedUniverseSetDigest", universe_ids(items)),
        ("selectedSourceSetDigest", selected_ids(items)),
    ):
        expected = set_digest(ids)
        if content.get(field_name) != expected:
            raise SourceCatalogError(
                f"release.json/content/{field_name} is {content.get(field_name)!r}, but the "
                f"{len(set(ids))} member identifiers derive {expected}"
            )
    members = list(manifest.get("members") or ())
    expected_counts = derive_counts(
        items,
        member_count=len(members),
        total_member_byte_size=sum(int(member.get("byteSize") or 0) for member in members),
    )
    if content.get("counts") != expected_counts:
        raise SourceCatalogError(
            f"release.json/content/counts is {content.get('counts')}, but the members derive {expected_counts}"
        )
    expected_coverage = derive_coverage(items)
    if content.get("coverage") != expected_coverage:
        raise SourceCatalogError(
            f"release.json/content/coverage is {content.get('coverage')}, but the rows derive {expected_coverage}"
        )


__all__ = [
    "validate_bundle_records",
    "validate_member_manifest",
    "validate_release_root",
    "validate_source_items",
]
