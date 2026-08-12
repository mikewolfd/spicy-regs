"""Re-read a published bundle from disk and re-derive everything it claims.

The builder computes; this reads back.  Nothing is carried over from the build:
the bytes on disk are parsed, required to be canonical, hashed, compared with
the digests the manifest and root declare, and put through the same gate a
freshly built bundle passes.  A bundle that survives a round trip through the
filesystem is one a consumer can open.

As with `validate.py`, this is the producer's own check.  Rulespec's validator
is the contract authority and closes at SpicySearch `PLAN.md` step 7.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from spicy_regs.document_release_v3 import DocumentReleaseV3Error, parse_canonical_json, sha256_bytes
from spicy_regs.source_catalog.records import (
    GLOBAL_MANIFEST_OBJECT_KEY,
    ROOT_OBJECT_KEY,
    SOURCE_ITEMS_OBJECT_KEY,
)
from spicy_regs.source_catalog.universe import SourceCatalogError
from spicy_regs.source_catalog.validate import validate_bundle_records


def _read_canonical(path: Path, *, label: str) -> Any:
    try:
        return parse_canonical_json(path.read_bytes(), label=label)
    except (OSError, DocumentReleaseV3Error) as error:
        raise SourceCatalogError(f"{label} is unreadable or not canonical: {error}") from error


def verify_bundle_directory(bundle_dir: Path | str) -> dict[str, Any]:
    """Verify one materialized bundle and return its root, or refuse."""

    bundle = Path(bundle_dir)
    root_path = bundle / ROOT_OBJECT_KEY
    if root_path.is_symlink() or not root_path.is_file():
        raise SourceCatalogError(f"{ROOT_OBJECT_KEY} is absent from {bundle}")
    root = _read_canonical(root_path, label=ROOT_OBJECT_KEY)
    if not isinstance(root, dict):
        raise SourceCatalogError(f"{ROOT_OBJECT_KEY} must contain an object")

    content = root.get("content")
    if not isinstance(content, dict):
        raise SourceCatalogError("release.json/content is absent")
    reference = content.get("globalManifest")
    if not isinstance(reference, dict) or reference.get("objectKey") != GLOBAL_MANIFEST_OBJECT_KEY:
        raise SourceCatalogError(f"release.json/content/globalManifest must point at {GLOBAL_MANIFEST_OBJECT_KEY}")
    manifest_path = bundle / GLOBAL_MANIFEST_OBJECT_KEY
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY} is absent from {bundle}")
    manifest_bytes = manifest_path.read_bytes()
    if len(manifest_bytes) != reference.get("byteSize") or sha256_bytes(manifest_bytes) != reference.get("sha256"):
        raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY} size or digest differs from the root reference")
    manifest = _read_canonical(manifest_path, label=GLOBAL_MANIFEST_OBJECT_KEY)
    if not isinstance(manifest, dict):
        raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY} must contain an object")

    declared = {ROOT_OBJECT_KEY, GLOBAL_MANIFEST_OBJECT_KEY}
    for index, member in enumerate(manifest.get("members") or ()):
        object_key = member.get("objectKey")
        if not isinstance(object_key, str) or not object_key:
            raise SourceCatalogError(f"{GLOBAL_MANIFEST_OBJECT_KEY}/members/{index}/objectKey is invalid")
        declared.add(object_key)
        path = bundle.joinpath(*object_key.split("/"))
        if path.is_symlink():
            raise SourceCatalogError(f"{object_key} is a symlink; a bundle carries files only")
        if not path.is_file():
            raise SourceCatalogError(f"{object_key} is declared but absent")
        payload = path.read_bytes()
        if len(payload) != member.get("byteSize") or sha256_bytes(payload) != member.get("sha256"):
            raise SourceCatalogError(f"{object_key} size or digest differs from its descriptor")

    materialized = {
        item.relative_to(bundle).as_posix() for item in bundle.rglob("*") if item.is_file() or item.is_symlink()
    }
    undeclared = sorted(materialized - declared)
    if undeclared:
        raise SourceCatalogError(f"the bundle carries undeclared files: {undeclared}")
    absent = sorted(declared - materialized)
    if absent:
        raise SourceCatalogError(f"the bundle declares absent files: {absent}")

    items = _read_canonical(bundle / SOURCE_ITEMS_OBJECT_KEY, label=SOURCE_ITEMS_OBJECT_KEY)
    if not isinstance(items, list):
        raise SourceCatalogError(f"{SOURCE_ITEMS_OBJECT_KEY} must contain an array")
    data_member = next(
        (member for member in manifest.get("members") or () if member.get("role") == "source-items"), None
    )
    if data_member is None or data_member.get("recordCount") != len(items):
        raise SourceCatalogError(f"{SOURCE_ITEMS_OBJECT_KEY} row count differs from its descriptor")

    validate_bundle_records(root=root, manifest=manifest, items=items)
    return root


__all__ = ["verify_bundle_directory"]
