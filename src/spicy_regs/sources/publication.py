"""Immutable table generations and a single conditional publication pointer.

Writers verify all remote bytes before changing the pointer. Readers resolve
the pointer once per operation and use immutable table URLs. Legacy bare URLs
remain readable only for tables not yet present in the publication index.
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
from typing import BinaryIO

import httpx
from botocore.exceptions import ClientError

INDEX_KEY = "publication.json"
INDEX_LIMIT = 1024 * 1024
PART_BYTES = 64 * 1024 * 1024
_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_snapshot: ContextVar[tuple[str, dict] | None] = ContextVar("publication_snapshot", default=None)


class PublicationError(RuntimeError):
    """Publication or resolution cannot establish one complete generation."""


def empty_index() -> dict:
    return {"format": "spicy-regs-publication", "version": 1, "families": {}}


def _pairs(pairs):
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


def load_index(base_url: str) -> dict:
    """Read one bounded pointer; only a 404 permits legacy table resolution."""
    with httpx.stream(
        "GET",
        f"{base_url.rstrip('/')}/{INDEX_KEY}",
        headers={"User-Agent": "spicy-regs", "Cache-Control": "no-cache"},
        follow_redirects=True,
        timeout=60,
    ) as response:
        if response.status_code == 404:
            return empty_index()
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_bytes():
            raw.extend(chunk)
            if len(raw) > INDEX_LIMIT:
                raise PublicationError("Publication index exceeds its byte limit")
    return parse_index(bytes(raw))


def current_index(base_url: str) -> dict:
    held = _snapshot.get()
    return held[1] if held is not None and held[0] == base_url.rstrip("/") else load_index(base_url)


@contextmanager
def snapshot(base_url: str) -> Iterator[dict]:
    """Bind all member reads in this operation to one pointer observation."""
    selected = current_index(base_url)
    token = _snapshot.set((base_url.rstrip("/"), selected))
    try:
        yield selected
    finally:
        _snapshot.reset(token)


def table_location(index: Mapping, key: str) -> tuple[str, dict | None]:
    for entry in index["families"].values():
        if key in entry["tables"]:
            return f"{entry['prefix']}/{key}", entry["tables"][key]
    return key, None


def _missing(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}


def _precondition(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") in {"412", "PreconditionFailed"}


def _stored_index(client, bucket: str) -> tuple[dict, str | None]:
    try:
        response = client.get_object(Bucket=bucket, Key=INDEX_KEY)
    except ClientError as exc:
        if _missing(exc):
            return empty_index(), None
        raise
    body = response["Body"]
    try:
        raw = body.read(INDEX_LIMIT + 1)
    finally:
        body.close()
    etag = response["ETag"]
    if not isinstance(etag, str) or not etag:
        raise PublicationError("Publication index has no conditional-write token")
    return parse_index(raw), etag


def _put_immutable(client, bucket: str, key: str, path: Path) -> None:
    """Conditional creation, including multipart completion for large tables."""
    size = path.stat().st_size
    args = {"Bucket": bucket, "Key": key}
    headers = {"ContentType": "application/octet-stream", "CacheControl": "public, max-age=31536000, immutable"}
    if size <= PART_BYTES:
        try:
            with path.open("rb") as body:
                client.put_object(**args, **headers, Body=body, ContentLength=size, IfNoneMatch="*")
        except ClientError as exc:
            if not _precondition(exc):
                raise
        return
    if size > PART_BYTES * 10_000:
        raise PublicationError("Generation member exceeds the bounded multipart size")
    upload_id = client.create_multipart_upload(**args, **headers)["UploadId"]
    try:
        parts = []
        with path.open("rb") as stream:
            while chunk := stream.read(PART_BYTES):
                part = len(parts) + 1
                digest = base64.b64encode(hashlib.md5(chunk, usedforsecurity=False).digest()).decode()
                result = client.upload_part(
                    **args,
                    UploadId=upload_id,
                    PartNumber=part,
                    Body=chunk,
                    ContentMD5=digest,
                )
                parts.append({"PartNumber": part, "ETag": result["ETag"]})
        client.complete_multipart_upload(
            **args,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
            IfNoneMatch="*",
        )
    except BaseException as exc:
        client.abort_multipart_upload(**args, UploadId=upload_id)
        if not isinstance(exc, ClientError) or not _precondition(exc):
            raise


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


def publish_generation(directory: Path, *, client, bucket: str, prior_index: Mapping) -> dict:
    """Verify/upload/verify, then compare-and-swap the publication pointer.

    Validation and conditional-write refusals preserve the current pointer.
    A transport failure after the pointer request can have an uncertain result;
    reread the index to establish whether the complete generation is current.
    Unreferenced immutable uploads may remain and are safe to reuse. A concurrent update refuses this
    attempt; it must not overwrite the other writer or silently retry a stale
    family build. Input provenance and semantic quality are separate checks.
    """
    from rulespec_artifacts import LocalMemberSource, admit_artifact, canonical_json_bytes, iter_member_descriptors
    from spicy_regs.generations import verify_generation
    from spicy_regs.sources.r2 import _assert_upload_safe, _get_remote_size

    artifact = verify_generation(directory)
    family = artifact.root["spec"]["family"]
    if not _NAME.fullmatch(family):
        raise PublicationError("Invalid family name")
    index, etag = _stored_index(client, bucket)
    if index["families"].get(family) != prior_index["families"].get(family):
        raise PublicationError("Family changed since the build read its inputs; rebuild before publishing")
    prefix = f"generations/{family}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    source = LocalMemberSource(directory)
    tables = {}
    for member in iter_member_descriptors(artifact, source):
        key = member.object_key
        assert key is not None
        for owner, entry in index["families"].items():
            if owner != family and key in entry["tables"]:
                raise PublicationError(f"{key} already belongs to family {owner}")
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
    updated = deepcopy(index)
    updated["families"][family] = {
        "prefix": prefix,
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
        "tables": tables,
    }
    raw = canonical_json_bytes(updated)
    parse_index(raw)
    for key in sorted(source.keys()):
        _put_immutable(client, bucket, prefix + "/" + key, directory / key)
    # S3 success or caller-supplied metadata is not byte-verification evidence.
    admit_artifact(_S3Members(client, bucket, prefix), expected_pin=artifact.pin)
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
    except ClientError as exc:
        if _precondition(exc):
            raise PublicationError("Publication changed concurrently; retry from a fresh snapshot") from exc
        raise
    return updated
