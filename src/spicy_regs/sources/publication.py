"""Immutable table generations and a single conditional publication pointer.

Writers verify all remote bytes before changing the pointer. Readers resolve
the pointer once per operation and use immutable table URLs. Legacy bare URLs
remain readable only for tables not yet present in the publication index.
Source-evidence blobs are content-addressed and stored once for all artifacts.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Callable

import httpx
from loguru import logger

if TYPE_CHECKING:
    from botocore.exceptions import ClientError

INDEX_KEY = "publication.json"
INDEX_LIMIT = 1024 * 1024
PART_BYTES = 64 * 1024 * 1024
EVIDENCE_PREFIX = "source-evidence"
_IMMUTABLE_HEADERS = {"ContentType": "application/octet-stream", "CacheControl": "public, max-age=31536000, immutable"}
_BLOB_KEY = re.compile(r"blobs/sha256/([0-9a-f]{64})\Z")
_POINTER_ATTEMPTS = 8
_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_snapshot: ContextVar[tuple[str, dict] | None] = ContextVar("publication_snapshot", default=None)


class PublicationError(RuntimeError):
    """Publication or resolution cannot establish one complete generation."""


def empty_index() -> dict:
    return {"format": "spicy-regs-publication", "version": 1, "families": {}}


def _pairs(pairs):
    """JSON object-pairs hook that refuses a repeated key.

    Readers such as the MCP server parse the index without ETL dependencies, so
    this cannot use Rulespec's parser.
    """
    result = {}
    for key, value in pairs:
        if key in result:
            raise PublicationError("Publication index repeats a key")
        result[key] = value
    return result


def parse_index(raw: bytes) -> dict:
    """Validate the small mutable pointer without claiming payload verification."""
    if len(raw) > INDEX_LIMIT:
        raise PublicationError("Publication index exceeds its byte limit")
    try:
        value = json.loads(raw, object_pairs_hook=_pairs)
        if (
            set(value) != {"format", "version", "families"}
            or value["format"] != "spicy-regs-publication"
            or type(value["version"]) is not int
            or value["version"] != 1
            or not isinstance(value["families"], dict)
        ):
            raise ValueError("invalid publication index")
        seen = set()
        for family, entry in value["families"].items():
            if not _NAME.fullmatch(family) or set(entry) != {"prefix", "logicalId", "artifactDigest", "tables"}:
                raise ValueError("invalid family")
            digest = entry["artifactDigest"]
            if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
                raise ValueError("invalid artifact pin")
            if entry["prefix"] != f"generations/{family}/{digest.removeprefix('sha256:')}":
                raise ValueError("prefix differs from artifact identity")
            if not isinstance(entry["logicalId"], str) or not entry["logicalId"].startswith("urn:"):
                raise ValueError("invalid logical identity")
            if not isinstance(entry["tables"], dict) or not entry["tables"]:
                raise ValueError("empty family")
            for key, table in entry["tables"].items():
                if not key.endswith(".parquet") or not _NAME.fullmatch(key[:-8]) or key in seen:
                    raise ValueError("invalid or multiply owned table")
                seen.add(key)
                if set(table) != {"sha256", "byteSize", "rows", "columns"}:
                    raise ValueError("invalid table descriptor")
                if not _DIGEST.fullmatch(table["sha256"]):
                    raise ValueError("invalid table digest")
                if any(type(table[field]) is not int or table[field] < 0 for field in ("byteSize", "rows")):
                    raise ValueError("invalid table counts")
                columns = table["columns"]
                if not isinstance(columns, list) or not columns:
                    raise ValueError("missing columns")
                if any(
                    not isinstance(c, list) or len(c) != 2 or not all(isinstance(x, str) and x for x in c)
                    for c in columns
                ):
                    raise ValueError("invalid columns")
                if len({c[0] for c in columns}) != len(columns):
                    raise ValueError("duplicate columns")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid publication index") from exc
    return value


def _bounded_get(url: str, *, allow_missing: bool, headers: Mapping[str, str] | None = None) -> bytes | None:
    """GET at most ``INDEX_LIMIT`` bytes; a 404 is ``None`` only when ``allow_missing``."""
    with httpx.stream("GET", url, headers={"User-Agent": "spicy-regs", **(headers or {})},
                      follow_redirects=True, timeout=60) as response:
        if allow_missing and response.status_code == 404:
            return None
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_bytes():
            raw.extend(chunk)
            if len(raw) > INDEX_LIMIT:
                raise PublicationError(f"{url} exceeds its byte limit")
    return bytes(raw)


def load_index(base_url: str) -> dict:
    """Read one bounded pointer; only a 404 permits legacy table resolution."""
    raw = _bounded_get(f"{base_url.rstrip('/')}/{INDEX_KEY}", allow_missing=True, headers={"Cache-Control": "no-cache"})
    return empty_index() if raw is None else parse_index(raw)


def current_index(base_url: str) -> dict:
    """Return the index bound by the active ``snapshot`` for ``base_url``, otherwise load it."""
    held = _snapshot.get()
    return held[1] if held is not None and held[0] == base_url.rstrip("/") else load_index(base_url)


def load_family_root(base_url: str, entry: Mapping) -> tuple[bytes, dict]:
    """Read only the pinned prior root for lineage; this does not re-admit its tables."""
    from rulespec_artifacts import ArtifactVerificationError, expected_artifact_digest, parse_canonical_json

    raw = _bounded_get(f"{base_url.rstrip('/')}/{entry['prefix']}/artifact.json", allow_missing=False)
    assert raw is not None
    try:
        root = parse_canonical_json(raw)
    except ArtifactVerificationError as exc:
        raise PublicationError("Prior generation root is not canonical artifact JSON") from exc
    if (not isinstance(root, dict) or root.get("artifactDigest") != entry["artifactDigest"]
            or expected_artifact_digest(root) != entry["artifactDigest"]
            or root.get("logicalId") != entry["logicalId"] or not isinstance(root.get("inputs"), list)):
        raise PublicationError("Prior generation root differs from its captured pin")
    return raw, root


@contextmanager
def snapshot(base_url: str) -> Iterator[dict]:
    """Bind all member reads in this operation to one pointer observation."""
    selected = current_index(base_url)
    token = _snapshot.set((base_url.rstrip("/"), selected))
    try:
        yield selected
    finally:
        _snapshot.reset(token)


def parquet_tables(index: Mapping) -> tuple[str, ...]:
    """Active Parquet table names, independent of any consumer's supported-table list."""
    return tuple(sorted({key.removesuffix(".parquet")
                         for family in index["families"].values()
                         for key in family["tables"] if key.endswith(".parquet")}))


def table_location(index: Mapping, key: str) -> tuple[str, dict | None]:
    """Resolve a table key to ``(generation path, descriptor)``, or ``(key, None)`` when unpublished.

    The bare-key fallback is what keeps legacy table URLs readable.
    """
    for entry in index["families"].values():
        if key in entry["tables"]:
            return f"{entry['prefix']}/{key}", entry["tables"][key]
    return key, None


def table_owner(index: Mapping, key: str) -> tuple[str, Mapping] | None:
    """The family publishing ``key`` in ``index`` and its entry, or ``None`` for a legacy table."""
    return next(((name, entry) for name, entry in index["families"].items() if key in entry["tables"]), None)


def _missing(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}


def _precondition(error: ClientError) -> bool:
    """A refused conditional write; S3 reports a concurrent conditional write as ``ConditionalRequestConflict``."""
    return error.response.get("Error", {}).get("Code") in {"412", "PreconditionFailed", "ConditionalRequestConflict"}


def _head(client, bucket: str, key: str) -> dict | None:
    """HEAD one object; only absence is ``None``, so permission and 5xx errors propagate."""
    from botocore.exceptions import ClientError

    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if _missing(exc):
            return None
        raise


def file_identity(path: Path, *, part_bytes: int = PART_BYTES) -> dict:
    """Size, SHA-256 and the S3/R2 ETag of ``path`` uploaded in ``part_bytes`` parts, in one read.

    A single-part upload's ETag is the file's MD5; a multipart one is the MD5 of
    the part MD5s plus the part count. ``_put_immutable`` uses ``PART_BYTES``;
    boto3's managed transfer uses 8 MiB parts.
    """
    digest = hashlib.sha256()
    parts = []
    with path.open("rb") as stream:
        while chunk := stream.read(part_bytes):
            digest.update(chunk)
            parts.append(hashlib.md5(chunk, usedforsecurity=False).digest())
    if len(parts) <= 1:
        etag = (parts[0] if parts else hashlib.md5(b"", usedforsecurity=False).digest()).hex()
    else:
        etag = f"{hashlib.md5(b''.join(parts), usedforsecurity=False).hexdigest()}-{len(parts)}"
    return {"bytes": path.stat().st_size, "sha256": "sha256:" + digest.hexdigest(), "etag": f'"{etag}"'}


def _get_bounded(client, bucket: str, key: str) -> tuple[bytes, str] | None:
    """Read a small control object and its ETag; only absence is ``None`` and oversize refuses."""
    from botocore.exceptions import ClientError

    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if _missing(exc):
            return None
        raise
    body = response["Body"]
    try:
        raw = body.read(INDEX_LIMIT + 1)
    finally:
        body.close()
    if len(raw) > INDEX_LIMIT:
        raise PublicationError(f"{key} exceeds its byte limit")
    return raw, response["ETag"]


def _stored_index(client, bucket: str) -> tuple[dict, str | None]:
    """Read the stored index and its conditional-write token; absent means empty and unpinned.

    A response without an ETag refuses.
    """
    stored = _get_bounded(client, bucket, INDEX_KEY)
    if stored is None:
        return empty_index(), None
    raw, etag = stored
    if not isinstance(etag, str) or not etag:
        raise PublicationError("Publication index has no conditional-write token")
    return parse_index(raw), etag


def _copy_unchanged_member(client, bucket: str, source_key: str, destination_key: str) -> None:
    """Copy an unchanged member server-side instead of re-uploading its bytes.

    Destination creation stays conditional: it HEADs first and skips when the
    object already exists, so a resumed publication never overwrites. The
    caller re-verifies the whole prefix afterwards (``admit_artifact``), so a
    copy can never publish wrong bytes — a wrong source digest fails
    verification before the pointer moves.
    """
    if _head(client, bucket, destination_key) is None:
        client.copy_object(Bucket=bucket, Key=destination_key, CopySource={"Bucket": bucket, "Key": source_key})


def _create_multipart(client, bucket: str, key: str, upload_parts: Callable[[str], list[dict]]) -> None:
    """Create-only multipart lifecycle: any failure aborts; a refused completion means the key already exists.

    The caller admits the destination bytes afterwards, so an existing object is
    only reused once it proves to hold the expected bytes.
    """
    from botocore.exceptions import ClientError

    args = {"Bucket": bucket, "Key": key}
    upload_id = client.create_multipart_upload(**args, **_IMMUTABLE_HEADERS)["UploadId"]
    completing = False
    try:
        parts = upload_parts(upload_id)
        completing = True
        client.complete_multipart_upload(**args, UploadId=upload_id, MultipartUpload={"Parts": parts},
                                         IfNoneMatch="*")
    except BaseException as exc:
        client.abort_multipart_upload(**args, UploadId=upload_id)
        if not completing or not isinstance(exc, ClientError) or not _precondition(exc):
            raise


def _put_immutable(client, bucket: str, key: str, path: Path, *, sha256: str | None = None) -> None:
    """Conditional creation, including multipart completion for large tables.

    Every request carries Content-MD5, so storage refuses bytes damaged in
    transit. With ``sha256``, the bytes actually sent must hash to it before the
    object is created, so a content-addressed key never holds other bytes.
    """
    from botocore.exceptions import ClientError

    size = path.stat().st_size
    if size > PART_BYTES * 10_000:
        raise PublicationError("Generation member exceeds the bounded multipart size")
    digest = hashlib.sha256()

    def checked(chunk: bytes) -> dict:
        digest.update(chunk)
        return {"Body": chunk, "ContentMD5": base64.b64encode(hashlib.md5(chunk, usedforsecurity=False).digest()).decode()}

    def assert_content_address() -> None:
        if sha256 is not None and "sha256:" + digest.hexdigest() != sha256:
            raise PublicationError(f"Bytes sent for {key} differ from their content address")

    if size <= PART_BYTES:
        body = checked(path.read_bytes())
        assert_content_address()
        try:
            client.put_object(Bucket=bucket, Key=key, **_IMMUTABLE_HEADERS, **body, ContentLength=size,
                              IfNoneMatch="*")
        except ClientError as exc:
            if not _precondition(exc):
                raise
        return

    def upload_parts(upload_id: str) -> list[dict]:
        parts = []
        with path.open("rb") as stream:
            while chunk := stream.read(PART_BYTES):
                part = len(parts) + 1
                result = client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part,
                                            **checked(chunk))
                parts.append({"PartNumber": part, "ETag": result["ETag"]})
        assert_content_address()
        return parts

    _create_multipart(client, bucket, key, upload_parts)


class _S3Members:
    """Rulespec storage adapter: list exact membership and stream actual bytes."""

    def __init__(self, client, bucket: str, prefix: str):
        self.client, self.bucket, self.prefix = client, bucket, prefix + "/"

    def keys(self):
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix)
        for page in pages:
            for entry in page.get("Contents", []):
                yield entry["Key"][len(self.prefix) :]

    @contextmanager
    def open(self, object_key: str) -> Iterator[BinaryIO]:
        from botocore.exceptions import ClientError
        from rulespec_artifacts import MemberNotFoundError

        try:
            result = self.client.get_object(Bucket=self.bucket, Key=self.prefix + object_key)
        except ClientError as exc:
            if _missing(exc):
                raise MemberNotFoundError(object_key) from exc
            raise
        body = result["Body"]
        try:
            yield body
        finally:
            body.close()


class _EvidenceMembers:
    """Admission source for one evidence artifact whose blobs live in the shared prefix.

    Metadata members come from ``source-evidence/<digest>/``; each
    ``blobs/sha256/<hex>`` member comes from ``source-evidence/blobs/…``, or
    from the locally admitted copy when the stored object's size and ETag
    already equal those bytes (``trusted``).
    """

    def __init__(self, client, bucket: str, prefix: str, local, blobs: set[str], trusted: set[str]):
        self.artifact = _S3Members(client, bucket, prefix)
        self.shared = _S3Members(client, bucket, EVIDENCE_PREFIX)
        self.local, self.blobs, self.trusted = local, blobs, trusted

    def keys(self):
        yield from sorted({*self.artifact.keys(), *self.blobs})

    @contextmanager
    def open(self, object_key: str) -> Iterator[BinaryIO]:
        source = (self.local if object_key in self.trusted
                  else self.shared if object_key in self.blobs else self.artifact)
        with source.open(object_key) as stream:
            yield stream


def _publish_evidence(client, bucket: str, path: Path, artifact) -> None:
    """Upload one locally admitted evidence artifact, sending and reading back only new blob bytes.

    Blob members are content-addressed, so each is stored once at
    ``source-evidence/blobs/sha256/<hex>`` for every artifact that cites it.
    A new blob is created from bytes that hash to its key and is read back once
    by admission. An existing blob whose size and ETag equal the local bytes is
    not transferred again; any other existing object is read back and refused
    unless its bytes match. Small metadata stays under the artifact's prefix.
    """
    from rulespec_artifacts import LocalMemberSource, admit_artifact, iter_member_descriptors

    local = LocalMemberSource(path)
    prefix = f"{EVIDENCE_PREFIX}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    declared = {member.object_key: member for member in iter_member_descriptors(artifact, local)}
    blobs, trusted = set(), set()
    for key in sorted(local.keys()):
        match, member = _BLOB_KEY.fullmatch(key), declared.get(key)
        if match is None or member is None:
            _put_immutable(client, bucket, f"{prefix}/{key}", path / key)
            continue
        if member.sha256 != "sha256:" + match[1]:
            raise PublicationError(f"Evidence blob {key} is not addressed by its content")
        blobs.add(key)
        stored = _head(client, bucket, f"{EVIDENCE_PREFIX}/{key}")
        if stored is None:
            _put_immutable(client, bucket, f"{EVIDENCE_PREFIX}/{key}", path / key, sha256=member.sha256)
            continue
        identity = file_identity(path / key)
        if (stored["ContentLength"], stored.get("ETag")) == (identity["bytes"], identity["etag"]):
            trusted.add(key)
        else:
            logger.warning("Stored evidence blob {} differs from its expected identity; reading it back", key)
    admit_artifact(_EvidenceMembers(client, bucket, prefix, local, blobs, trusted), expected_pin=artifact.pin)


def publish_generation(directory: Path, *, client, bucket: str, prior_index: Mapping,
                       evidence_directories: tuple[Path, ...] = ()) -> dict:
    """Verify/upload/verify, then compare-and-swap the publication pointer.

    Validation and conditional-write refusals preserve the current pointer.
    A transport failure after the pointer request can have an uncertain result;
    reread the index to establish whether the complete generation is current.
    Unreferenced immutable uploads may remain and are safe to reuse. A pointer
    moved by another family's writer is reread and this family's entry merged
    onto it, which equals a first attempt made a moment later; a change to this
    family or its tables refuses as stale and never overwrites the other writer.
    Input provenance and semantic quality are separate checks.
    """
    from rulespec_artifacts import LocalMemberSource
    from spicy_regs.generations import verify_generation

    artifact = verify_generation(directory)
    return _publish_verified_generation(
        artifact, LocalMemberSource(directory), client=client, bucket=bucket, prior_index=prior_index,
        evidence_directories=evidence_directories,
        upload_member=lambda prefix, key: _put_immutable(client, bucket, prefix + "/" + key, directory / key),
    )


def _publish_verified_generation(
    artifact, source, *, upload_member, client, bucket: str, prior_index: Mapping,
    evidence_directories: tuple[Path, ...] = (),
) -> dict:
    """Shared publication gates; both callers fully verify their source first."""
    from botocore.exceptions import ClientError
    from rulespec_artifacts import admit_artifact, iter_member_descriptors
    from spicy_regs.sources.r2 import _assert_upload_safe, _get_remote_size
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, verify_evidence

    if artifact.root["spec"]["publicationStatus"] != "complete-family":
        raise PublicationError("A local partial candidate cannot be published")
    family = artifact.root["spec"]["family"]
    if not _NAME.fullmatch(family):
        raise PublicationError("Invalid family name")
    index, etag = _stored_index(client, bucket)
    _assert_family_unchanged(index, prior_index, family)
    prefix = f"generations/{family}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    tables = {}
    for member in iter_member_descriptors(artifact, source):
        key = member.object_key
        assert key is not None
        _, prior = table_location(index, key)
        old_size = prior["byteSize"] if prior else _get_remote_size(client, bucket, key)
        _assert_upload_safe(member.byte_size, old_size, key)
        tables[key] = {
            "sha256": member.sha256,
            "byteSize": member.byte_size,
            **artifact.root["spec"]["tables"][key],
        }
    old_family = index["families"].get(family)
    if old_family is not None and set(old_family["tables"]) != set(tables):
        raise PublicationError("Family membership changed; explicit migration is required")
    entry = {
        "prefix": prefix,
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
        "tables": tables,
    }
    _merge_family(index, family, entry)
    evidence = [verify_evidence(path) for path in evidence_directories]
    declared = [item for item in artifact.root["inputs"] if item["role"] == INPUT_ROLE]
    actual = [{"role": INPUT_ROLE, **item.pin.as_dict()} for item in evidence]
    if actual != declared:
        raise PublicationError("Generation source evidence is missing or differs from its input pins")
    for path, item in zip(evidence_directories, evidence, strict=True):
        if item.root["spec"]["family"] != family:
            raise PublicationError("Source evidence belongs to a different family")
        if item.root["inputs"] != [value for value in artifact.root["inputs"] if value["role"] == PRIOR_ROLE]:
            raise PublicationError("Source evidence and generation name different inherited inputs")
        _publish_evidence(client, bucket, path, item)
    # Members whose bytes the prior generation already holds under the same
    # digest are copied server-side instead of re-uploaded. The copy can never
    # publish wrong bytes: ``admit_artifact`` below re-verifies every member of
    # the new prefix against the artifact pin before the pointer moves.
    prior_tables = old_family.get("tables", {}) if old_family is not None else {}
    prior_prefix = old_family.get("prefix") if old_family is not None else None
    reused = 0
    for key in sorted(source.keys()):
        prior_entry = prior_tables.get(key)
        if (
            prior_prefix is not None
            and prior_entry is not None
            and prior_entry.get("sha256") == tables[key]["sha256"]
            and prior_entry.get("byteSize") == tables[key]["byteSize"]
        ):
            _copy_unchanged_member(client, bucket, f"{prior_prefix}/{key}", f"{prefix}/{key}")
            reused += 1
        else:
            upload_member(prefix, key)
    if reused:
        logger.info("publication: reused {} unchanged member(s) across generation prefixes", reused)
    # S3 success or caller-supplied metadata is not byte-verification evidence.
    admit_artifact(_S3Members(client, bucket, prefix), expected_pin=artifact.pin)
    # Each lost race means another writer's pointer write succeeded, so
    # contention resolves in at most one round per concurrent writer.
    for _ in range(_POINTER_ATTEMPTS):
        updated, raw = _merge_family(index, family, entry)
        condition = {"IfMatch": etag} if etag is not None else {"IfNoneMatch": "*"}
        try:
            client.put_object(
                Bucket=bucket,
                Key=INDEX_KEY,
                Body=raw,
                ContentType="application/json",
                CacheControl="no-store, no-cache, must-revalidate",
                **condition,
            )
            return updated
        except ClientError as exc:
            if not _precondition(exc):
                raise
        logger.info("publication: pointer moved concurrently; merging {} onto the reread index", family)
        index, etag = _stored_index(client, bucket)
        _assert_family_unchanged(index, prior_index, family)
    raise PublicationError("Publication changed concurrently; retry from a fresh snapshot")


def _assert_family_unchanged(index: Mapping, prior_index: Mapping, family: str) -> None:
    if index["families"].get(family) != prior_index["families"].get(family):
        raise PublicationError("Family changed since the build read its inputs; rebuild before publishing")


def _merge_family(index: dict, family: str, entry: dict) -> tuple[dict, bytes]:
    """Return ``index`` with ``family`` set to ``entry``, refusing a table another family owns."""
    from rulespec_artifacts import canonical_json_bytes

    for owner, other in index["families"].items():
        for key in entry["tables"]:
            if owner != family and key in other["tables"]:
                raise PublicationError(f"{key} already belongs to family {owner}")
    updated = deepcopy(index)
    updated["families"][family] = entry
    raw = canonical_json_bytes(updated)
    parse_index(raw)
    return updated, raw
