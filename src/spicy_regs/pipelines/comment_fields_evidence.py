"""Conditional Mirrulations reads and durable checkpoints for a pinned comments plan."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from io import BytesIO
from typing import NamedTuple
from uuid import uuid4

import polars as pl
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from spicy_docs.sources.mirrulations import DEFAULT_MAX_OBJECT_BYTES, _raise_if_access_refused

SOURCE_PIN_COLUMNS = ("etag", "size", "version_id", "raw_sha256")
REFETCH_SUFFIX = r"( *\([0-9]+\))+$"
EVIDENCE_SCHEMA = {
    "key": pl.String, "expected_etag": pl.String, "expected_size": pl.Int64,
    "expected_version_id": pl.String, "expected_sha256": pl.String, "etag": pl.String, "size": pl.Int64,
    "version_id": pl.String, "last_modified": pl.String, "observed_at": pl.String,
    "sha256": pl.String, "status": pl.String, "reason": pl.String, "custody": pl.String,
}


def file_sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def comment_key_identity(key):
    return re.sub(REFETCH_SUFFIX, "", key.rsplit("/", 1)[-1].removesuffix(".json"))


def key_sha256(keys):
    digest = hashlib.sha256()
    for key in sorted(keys):
        digest.update(key.encode())
        digest.update(b"\0")
    return digest.hexdigest()


class SourcePin(NamedTuple):
    etag: str | None
    size: int | None
    version_id: str | None
    raw_sha256: str | None


class _PinnedObject:
    def __init__(self, fetch, key, pin, observations, archive, observe_source):
        self.fetch = fetch
        self.key = key
        self.pin = pin
        self.observations = observations
        self.archive = archive
        self.observe_source = observe_source

    def get(self, **options):
        pin = self.pin
        previous = self.observations.get(self.key)
        if previous and previous.get("status") in {
            "unlisted", "oversized", "source-missing", "source-changed", "invalid-source-metadata",
        }:
            raise ValueError(previous["reason"])
        if pin.etag is None and not self.observe_source:
            self.observations[self.key] = {"status": "unlisted", "reason": "The pinned inventory has no listing for the key"}
            raise ValueError("No source version in the pinned inventory")
        if pin.size is not None and pin.size > DEFAULT_MAX_OBJECT_BYTES:
            self.observations[self.key] = {"status": "oversized", "reason": "The pinned source exceeds the object byte cap"}
            raise ValueError("Pinned source exceeds the object byte cap")
        if pin.raw_sha256 is not None:
            with self.archive.open(f"sha256:{pin.raw_sha256}") as stream:
                content = stream.read(pin.size + 1)
            if len(content) != pin.size or hashlib.sha256(content).hexdigest() != pin.raw_sha256:
                raise ValueError("Retained original differs from its pinned size or SHA-256")
            self.observations[self.key] = {
                "etag": pin.etag, "size": pin.size, "version_id": pin.version_id,
                "sha256": pin.raw_sha256, "custody": "central-raw-archive",
            }
            return {"ETag": pin.etag, "ContentLength": pin.size, "VersionId": pin.version_id,
                    "Body": StreamingBody(BytesIO(content), len(content))}
        if pin.etag is not None:
            options = {**options, "IfMatch": pin.etag}
        if pin.version_id is not None:
            options["VersionId"] = pin.version_id
        try:
            response = self.fetch(**options)
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status in (401, 403):
                self.observations[self.key] = {"status": "access-refused", "reason": f"Source GET returned HTTP {status}"}
            if status in (404, 412):
                label = "source-missing" if status == 404 else "source-changed"
                self.observations[self.key] = {"status": label, "reason": f"Conditional source GET returned HTTP {status}"}
            raise

        body = response["Body"]
        modified = response.get("LastModified")
        observed = {
            "etag": response.get("ETag"), "size": response.get("ContentLength"), "version_id": response.get("VersionId"),
            "last_modified": modified.isoformat() if modified is not None else None,
        }
        try:
            if (not isinstance(observed["etag"], str) or not observed["etag"]
                    or type(observed["size"]) is not int or not 0 <= observed["size"] <= DEFAULT_MAX_OBJECT_BYTES):
                self.observations[self.key] = {
                    **observed, "status": "invalid-source-metadata", "reason": "Source GET lacks a usable ETag or bounded size",
                }
                raise ValueError("Source GET lacks a usable ETag or bounded size")
            if ((pin.etag is not None and (observed["etag"] != pin.etag or observed["size"] != pin.size))
                    or pin.version_id is not None and observed["version_id"] != pin.version_id):
                self.observations[self.key] = {
                    **observed, "status": "source-changed", "reason": "Source response differs from the pinned version or size",
                }
                raise ValueError("Source response differs from the pinned version or size")
            content = body.read(observed["size"] + 1)
            if len(content) != observed["size"]:
                raise ValueError("Source body does not match its pinned size")
        finally:
            body.close()
        self.observations[self.key] = {
            **observed, "sha256": hashlib.sha256(content).hexdigest(), "custody": "external",
        }
        return {**response, "Body": StreamingBody(BytesIO(content), len(content))}


class PinnedChunk:
    """One bounded resource adapter. The reader still owns JSON parsing and outcomes."""

    def __init__(self, resource, pins, archive, *, observe_source=False):
        self.resource = resource
        self.pins = pins
        self.observations = {}
        self.archive = archive
        self.observe_source = observe_source

    def Object(self, bucket, key):  # noqa: N802 - boto3 resource API
        def fetch(**options):
            return self.resource.Object(bucket, key).get(**options)

        return _PinnedObject(fetch, key, self.pins[key], self.observations, self.archive, self.observe_source)

    def evidence(self, accepted, failures):
        observed_at = datetime.now(UTC).isoformat()
        for key, pin in self.pins.items():
            outcome = self.observations.get(key, {})
            failure = failures.get(key, {})
            status = outcome.get("status") or ("read" if key in accepted else failure.get("status", "unattempted"))
            yield {
                "key": key, "expected_etag": pin.etag, "expected_size": pin.size, "expected_version_id": pin.version_id,
                "expected_sha256": pin.raw_sha256, "observed_at": observed_at, **outcome, "status": status,
                "reason": outcome.get("reason", failure.get("reason")),
            }


def list_source_pins(resource, bucket, prefix):
    """Yield only comment object pins, preserving each refetch key from the listing."""
    pages = resource.meta.client.get_paginator("list_objects_v2")
    try:
        for page in pages.paginate(Bucket=bucket, Prefix=prefix):
            for entry in page.get("Contents", ()):
                key = entry["Key"]
                if "/comments/" in key and key.endswith(".json"):
                    yield {"key": key, "etag": entry["ETag"], "size": entry["Size"], "version_id": None}
    except ClientError as error:
        _raise_if_access_refused(error, prefix)
        raise


def write_source_pins(resource, bucket, prefix, output):
    import pyarrow as pa

    from spicy_regs.transforms.parquet_rows import write_rows

    if output.exists():
        raise FileExistsError(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}-{uuid4().hex}.tmp")
    schema = pa.schema([("key", pa.string()), ("etag", pa.string()), ("size", pa.int64()), ("version_id", pa.string())])
    try:
        write_rows(list_source_pins(resource, bucket, prefix), temporary, schema, batch_size=20_000)
        temporary.replace(output)
    finally:
        temporary.unlink(missing_ok=True)
    return {"prefix": prefix, "path": str(output), "sha256": file_sha256(output)}


def checkpoint_path(part):
    return part.with_suffix(".checkpoint.json")


def verified_part(part, plan_sha256, expected_keys):
    checkpoint = checkpoint_path(part)
    if not checkpoint.exists():
        return False
    selected = json.loads(checkpoint.read_text())
    evidence = part.parent / selected["evidence_file"]
    if (selected["plan_sha256"] != plan_sha256 or selected["part_file"] != part.name
            or not part.exists() or not evidence.is_file()
            or evidence.parent.resolve() != (part.parent / "source-evidence").resolve()
            or file_sha256(part) != selected["part_sha256"]
            or file_sha256(evidence) != selected["evidence_sha256"]):
        raise ValueError(f"Pinned comments checkpoint differs from its files or plan: {checkpoint}")
    evidence_rows = pl.read_parquet(evidence, columns=["key", "status"])
    outcome_keys = evidence_rows["key"].to_list()
    staged_keys = pl.read_parquet(part, columns=["key"])["key"].to_list()
    read_keys = evidence_rows.filter(pl.col("status") == "read")["key"].to_list()
    if (len(expected_keys) != selected["keys"] or key_sha256(expected_keys) != key_sha256(outcome_keys)
            or len(outcome_keys) - len(read_keys) != selected["unresolved"]
            or key_sha256(read_keys) != key_sha256(staged_keys)):
        raise ValueError(f"Pinned comments checkpoint counts differ from its evidence: {checkpoint}")
    return selected


def write_evidence(part, rows):
    """Keep evidence outside the staged parts glob, including interrupted attempts."""
    folder = part.parent / "source-evidence"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{part.stem}-{uuid4().hex}.parquet"
    temporary = target.with_suffix(".tmp")
    pl.DataFrame(rows, schema=EVIDENCE_SCHEMA).write_parquet(temporary, compression="zstd")
    temporary.replace(target)
    return target


def commit_checkpoint(part, plan_sha256, evidence, keys, unresolved):
    checkpoint = checkpoint_path(part)
    temporary = checkpoint.with_suffix(".tmp")
    record = {
        "plan_sha256": plan_sha256,
        "part_file": part.name,
        "part_sha256": file_sha256(part),
        "evidence_file": str(evidence.relative_to(part.parent)),
        "evidence_sha256": file_sha256(evidence),
        "keys": keys,
        "unresolved": unresolved,
    }
    with temporary.open("w") as handle:
        json.dump(record, handle)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(checkpoint)
