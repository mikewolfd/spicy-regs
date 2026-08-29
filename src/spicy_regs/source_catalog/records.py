"""The wire constants and the derivations both the builder and the gate use.

Everything here is a function of the bundle's own rows: the identity, the set
digests, the diagnostic counts, the coverage accounting.  Keeping them in one
place is what lets the producer compute a value and the gate recompute it
independently without either becoming the other's mirror.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from spicy_regs.source_catalog.universe import SourceCatalogError, canonical_digest

FORMAT = "spicy-regs-source-catalog-release"
FORMAT_VERSION = "1.0"
RELEASE_ID_PREFIX = "urn:spicy-regs:source-catalog-release:v1:"
MEMBER_MANIFEST_FORMAT = "spicy-artifact-member-manifest"
MEMBER_MANIFEST_VERSION = "1.0"

ROOT_OBJECT_KEY = "release.json"
GLOBAL_MANIFEST_OBJECT_KEY = "manifests/global.json"
SOURCE_ITEMS_OBJECT_KEY = "data/source-items.json"
SCHEMA_OBJECT_KEY_PREFIX = "schemas/"

RELEASE_STATUSES: tuple[str, ...] = ("fixture", "candidate", "published")

# Exactly the five dispositions the contract admits.  The order is the
# reporting order of the derived counts, not a ranking.
SELECTION_DISPOSITIONS: tuple[str, ...] = ("selected", "excluded", "deleted", "unavailable", "failed")
NON_SELECTED_DISPOSITIONS = frozenset(SELECTION_DISPOSITIONS) - {"selected"}

# A source-observed topic is not a RefSpec concept.  Refusing the vocabulary
# owner's URN space keeps that boundary mechanical rather than advisory.
REFSPEC_IDENTIFIER_PREFIXES: tuple[str, ...] = ("urn:ref:", "urn:refspec:")


def set_digest(source_item_ids: Iterable[str]) -> str:
    """Canonical SET digest over a deduplicated, sorted identifier list.

    A repeated identifier does not move it.  Duplicates are a separate defect
    with a separate refusal; folding them in would let one defect mask another.
    """

    return f"sha256:{canonical_digest(sorted(set(source_item_ids)))}"


def release_identity(root: Mapping[str, Any]) -> str:
    """Derive the release identity from the identity-bearing payload alone.

    ``annotations`` is excluded, and that is where every fact about the ACT of
    publishing lives.  Two publishes of identical selection content therefore
    share one identity, and no wall-clock stamp can make a reproducible build
    unreproducible — the rule DocumentRelease v3 already applies to
    ``createdAt``.
    """

    payload = {
        "content": root.get("content"),
        "format": root.get("format"),
        "formatVersion": root.get("formatVersion"),
    }
    return f"{RELEASE_ID_PREFIX}{canonical_digest(payload)}"


def disposition_of(item: Mapping[str, Any]) -> str:
    selection = item.get("selection")
    disposition = selection.get("disposition") if isinstance(selection, Mapping) else None
    if disposition not in SELECTION_DISPOSITIONS:
        raise SourceCatalogError(f"row {item.get('sourceItemId')!r} carries no recognized disposition")
    return str(disposition)


def derive_counts(
    items: Sequence[Mapping[str, Any]], *, member_count: int, total_member_byte_size: int
) -> dict[str, int]:
    """Recompute the diagnostic counts from the rows alone."""

    tally = dict.fromkeys(SELECTION_DISPOSITIONS, 0)
    for item in items:
        tally[disposition_of(item)] += 1
    return {
        "deletedCount": tally["deleted"],
        "discoveredCount": len(items),
        "excludedCount": tally["excluded"],
        "failedCount": tally["failed"],
        "memberCount": member_count,
        "selectedCount": tally["selected"],
        "totalMemberByteSize": total_member_byte_size,
        "unavailableCount": tally["unavailable"],
    }


def derive_coverage(items: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    """Recompute the accounting proof from the rows alone."""

    with_rendition = 0
    selected_document_ids: set[str] = set()
    accounted = 0
    for item in items:
        # `disposition_of` refuses an unrecognized disposition, so a row that
        # reaches this line is accounted for by construction.
        disposition = disposition_of(item)
        accounted += 1
        if disposition != "selected":
            continue
        if item.get("candidateRenditions"):
            with_rendition += 1
        selected_document_ids.add(str(item.get("documentId")))
    return {
        "accountedCount": accounted,
        "distinctSelectedDocumentIdCount": len(selected_document_ids),
        "selectedWithCandidateRenditionCount": with_rendition,
        "unaccountedCount": len(items) - accounted,
    }


def universe_ids(items: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(item["sourceItemId"]) for item in items]


def selected_ids(items: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(item["sourceItemId"]) for item in items if disposition_of(item) == "selected"]


__all__ = [
    "FORMAT",
    "FORMAT_VERSION",
    "GLOBAL_MANIFEST_OBJECT_KEY",
    "MEMBER_MANIFEST_FORMAT",
    "MEMBER_MANIFEST_VERSION",
    "NON_SELECTED_DISPOSITIONS",
    "REFSPEC_IDENTIFIER_PREFIXES",
    "RELEASE_ID_PREFIX",
    "RELEASE_STATUSES",
    "ROOT_OBJECT_KEY",
    "SCHEMA_OBJECT_KEY_PREFIX",
    "SELECTION_DISPOSITIONS",
    "SOURCE_ITEMS_OBJECT_KEY",
    "derive_counts",
    "derive_coverage",
    "disposition_of",
    "release_identity",
    "selected_ids",
    "set_digest",
    "universe_ids",
]
