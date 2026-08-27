"""Publish and read the one SpicyRegs source-native release.

Rulespec owns byte identity, manifests, and structural admission.  This module
owns source-release roles and the bounded product verifier. Source-specific
meaning arrives through one injected profile.
"""

from __future__ import annotations

import hashlib
import heapq
import os
import shutil
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import zip_longest
from importlib.resources import files
from pathlib import Path
from typing import Any, BinaryIO, Callable, Final, cast

from jsonschema import Draft202012Validator

from rulespec_artifacts import (
    ROOT_OBJECT_KEY,
    ArtifactPin,
    BlobSource,
    FramedSection,
    LocalMemberSource,
    MemberDescriptor,
    MemberManifestReference,
    MemberSource,
    Producer,
    Supersedes,
    VerifiedArtifact,
    admit_artifact,
    build_artifact_root,
    canonical_json_bytes,
    describe_member_from_receipt,
    framed_section_digest,
    iter_member_descriptors,
    parse_canonical_json,
    schema_bundle_digest,
)

from spicy_regs.source_native_profile import (
    AcquisitionCheck,
    SourceNativePage,
    SourceNativeProfile,
    TraversalCheck,
)
from spicy_regs.source_native_store import SourceNativeBlobStore
from spicy_regs.publication import publish_directory_once, write_bytes_once

KIND: Final = "spicyregs-source-native-release"
FORMAT: Final = "spicyregs-source-native-release"
FORMAT_VERSION: Final = "1.0"
RELEASE_SCHEMA_ID: Final = "urn:spicy-regs:schema:source-native-release:1.0"
VERIFIER_ID: Final = "urn:spicy-regs:source-native-release-verifier"
VERIFIER_VERSION: Final = "1.0"
MAX_EVIDENCE_BYTES: Final = 24 * 1024 * 1024
MAX_ROW_BYTES: Final = 4 * 1024 * 1024
PARTITION_BUCKET_COUNT: Final = 64
PARTITION_ALGORITHM: Final = "sha256-utf8-modulo"

PARTITION_RECORDS: Final = "records"
PARTITION_RENDITIONS: Final = "renditions"
PARTITION_LEDGER: Final = "acquisition-records"
PARTITION_PAGES: Final = "acquisition-pages"
PARTITION_KINDS: Final = (
    PARTITION_LEDGER,
    PARTITION_PAGES,
    PARTITION_RECORDS,
    PARTITION_RENDITIONS,
)

ROLE_SCOPES: Final = "source-native-scopes"
ROLE_SCHEMA: Final = "source-native-schema"
ROLE_RECORDS: Final = "source-native-records"
ROLE_RENDITIONS: Final = "rendition-index"
ROLE_RELEASE_SCHEMA: Final = "release-schema"
ROLE_RECEIPT: Final = "source-publication-receipt"
ROLE_LEDGER: Final = "source-acquisition-ledger"
ROLE_EVIDENCE: Final = "source-acquisition-evidence"
PARTITION_ROLES: Final = {
    PARTITION_RECORDS: ROLE_RECORDS,
    PARTITION_RENDITIONS: ROLE_RENDITIONS,
    PARTITION_LEDGER: ROLE_LEDGER,
    PARTITION_PAGES: ROLE_LEDGER,
}
REQUIRED_ROLES: Final = frozenset(
    {
        ROLE_SCOPES,
        ROLE_SCHEMA,
        ROLE_RECORDS,
        ROLE_RENDITIONS,
        ROLE_RELEASE_SCHEMA,
        ROLE_RECEIPT,
        ROLE_LEDGER,
        ROLE_EVIDENCE,
    }
)
ALWAYS_REQUIRED_ROLES: Final = REQUIRED_ROLES - {ROLE_RECORDS, ROLE_RENDITIONS}

SCOPES_KEY: Final = "records/scopes.jsonl"
RELEASE_SCHEMA_KEY: Final = "schemas/source-native-release-1.0.json"
RECEIPT_KEY: Final = "receipts/publication.json"
MANIFEST_KEY: Final = "manifests/source-native.json"

SPEC_FIELDS: Final = frozenset(
    {
        "acquisitionPolicyDigest",
        "acquisitionPolicyId",
        "acquisitionPolicyVersion",
        "releaseSchemaDigest",
        "sourceNativeSchemaSetDigest",
        "sourceStateDigest",
        "sourceStateScope",
        "sourceSystemId",
        "sourceSystemVersion",
    }
)
class SourceNativeReleaseError(ValueError):
    """The source-native product rules refuse a release."""


@dataclass(frozen=True, slots=True)
class SourceNativeReleaseBuild:
    """Publication evidence supplied by the caller, not inferred from a worktree."""

    query_scope: Mapping[str, Any]
    producer: Producer
    started_at: str
    supersedes: Supersedes | None = None

    def __post_init__(self) -> None:
        _utc(self.started_at, "started_at")
        if self.producer.product != "spicy-regs":
            raise SourceNativeReleaseError("producer product must be spicy-regs")
        if self.producer.verifier_id != VERIFIER_ID or self.producer.verifier_version != VERIFIER_VERSION:
            raise SourceNativeReleaseError("producer names an unsupported source-native verifier")


@dataclass(frozen=True, slots=True)
class PublishedSourceNativeRelease:
    root: Path
    artifact: VerifiedArtifact


@dataclass(slots=True)
class _ByteAccounting:
    payload_bytes_read: int = 0
    payload_bytes_reused: int = 0
    payload_bytes_written: int = 0

    def add(self, *, byte_size: int, reused: bool, bytes_written: int) -> None:
        self.payload_bytes_read += byte_size
        if reused:
            self.payload_bytes_reused += byte_size
        self.payload_bytes_written += bytes_written


@dataclass(frozen=True, slots=True)
class _PayloadPartition:
    partition_kind: str
    partition_id: str
    member: MemberDescriptor

    def receipt(self) -> dict[str, object]:
        if self.member.blob_ref is None or self.member.record_count is None:
            raise RuntimeError("source-native payload partition is incomplete")
        return {
            "blobRef": self.member.blob_ref,
            "byteSize": self.member.byte_size,
            "partitionId": self.partition_id,
            "partitionKind": self.partition_kind,
            "recordCount": self.member.record_count,
        }


def _utc(value: str, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise SourceNativeReleaseError(f"{label} must be a canonical UTC instant")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise SourceNativeReleaseError(f"{label} is not a valid UTC instant") from error
    if parsed.isoformat().replace("+00:00", "Z") != value:
        raise SourceNativeReleaseError(f"{label} must use canonical UTC spelling")
    return parsed


def _now() -> datetime:
    return datetime.now(UTC)


def _instant(clock: Callable[[], datetime]) -> str:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise SourceNativeReleaseError("publisher clock must return a timezone-aware instant")
    return (
        value.astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _closed_schema(properties: Mapping[str, Any], *, required: Sequence[str] | None = None) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": False,
        "properties": dict(properties),
        "required": list(required or properties),
        "type": "object",
    }


@dataclass(frozen=True, slots=True)
class _ClosedObjectShape:
    """One declaration for schema generation, writing, and structural parsing."""

    name: str
    properties: Mapping[str, Any]

    @property
    def fields(self) -> frozenset[str]:
        return frozenset(self.properties)

    @property
    def schema(self) -> dict[str, Any]:
        return _closed_schema(self.properties)

    def parse(self, value: object) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise SourceNativeReleaseError(f"{self.name} is not an object")
        errors = sorted(
            Draft202012Validator(self.schema).iter_errors(value),
            key=lambda error: tuple(str(part) for part in error.absolute_path),
        )
        if errors:
            where = "/".join(str(part) for part in errors[0].absolute_path)
            suffix = f" at {where}" if where else ""
            raise SourceNativeReleaseError(
                f"{self.name} structure differs{suffix}: {errors[0].message}"
            )
        return dict(cast(Mapping[str, Any], value))

    def build(self, **values: Any) -> dict[str, Any]:
        return self.parse(values)


_NULLABLE_TEXT_SCHEMA: Final = {"type": ["string", "null"]}
_UINT_SCHEMA: Final = {"minimum": 0, "type": "integer"}
_DIGEST_SCHEMA: Final = {"pattern": "^sha256:[0-9a-f]{64}$", "type": "string"}

_BYTE_MEASUREMENTS_SCHEMA: Final = _closed_schema(
    {
        "payloadBytesRead": _UINT_SCHEMA,
        "payloadBytesReused": _UINT_SCHEMA,
        "payloadBytesWritten": _UINT_SCHEMA,
        "publicationBytesWritten": _UINT_SCHEMA,
    }
)
_PARTITION_POLICY_SCHEMA: Final = _closed_schema(
    {
        "algorithm": {"const": PARTITION_ALGORITHM},
        "bucketCount": {"const": PARTITION_BUCKET_COUNT},
        "identityEncoding": {"const": "utf-8"},
    }
)
_PAYLOAD_PARTITION_SCHEMA: Final = _closed_schema(
    {
        "blobRef": _DIGEST_SCHEMA,
        "byteSize": _UINT_SCHEMA,
        "partitionId": {"pattern": "^[0-9]{2}$", "type": "string"},
        "partitionKind": {"enum": list(PARTITION_KINDS)},
        "recordCount": _UINT_SCHEMA,
    }
)

_PAGE_SHAPE: Final = _ClosedObjectShape(
    "acquisition page",
    {
        "accepted": {"type": "boolean"},
        "discoveredRecords": {"type": "array"},
        "evidenceMediaType": {"minLength": 1, "type": "string"},
        "evidenceBlobRef": _DIGEST_SCHEMA,
        "pageIndex": _UINT_SCHEMA,
        "requestKey": {"type": "string"},
        "recordsIncluded": {"type": "boolean"},
        "responseDigest": _DIGEST_SCHEMA,
        "sourceCursor": _NULLABLE_TEXT_SCHEMA,
        "terminal": {"type": "boolean"},
        "traversalIndex": _UINT_SCHEMA,
        "windowIndex": _UINT_SCHEMA,
        "windowPageIndex": _UINT_SCHEMA,
    },
)

_RECEIPT_SHAPE: Final = _ClosedObjectShape(
    "publication receipt",
    {
        "acquisitionEvidenceCount": _UINT_SCHEMA,
        "acquisitionLedgerDigest": _DIGEST_SCHEMA,
        "acquisitionPolicyDigest": _DIGEST_SCHEMA,
        "byteMeasurements": _BYTE_MEASUREMENTS_SCHEMA,
        "completedAt": {"type": "string"},
        "discoveredRecordCount": _UINT_SCHEMA,
        "discardedObservationCount": _UINT_SCHEMA,
        "failedRecordCount": _UINT_SCHEMA,
        "format": {"type": "string"},
        "formatVersion": {"type": "string"},
        "inputObservationCount": _UINT_SCHEMA,
        "inputObservationDigest": _DIGEST_SCHEMA,
        "partitionPolicy": _PARTITION_POLICY_SCHEMA,
        "payloadPartitions": {
            "items": _PAYLOAD_PARTITION_SCHEMA,
            "maxItems": len(PARTITION_KINDS) * PARTITION_BUCKET_COUNT,
            "type": "array",
        },
        "publishedRecordCount": _UINT_SCHEMA,
        "reconciliationDigest": _DIGEST_SCHEMA,
        "reconciliationPassCount": _UINT_SCHEMA,
        "releaseSchemaDigest": _DIGEST_SCHEMA,
        "releaseSchemaId": {"type": "string"},
        "renditionIndexCount": _UINT_SCHEMA,
        "semanticVerdict": {"const": "pass"},
        "sourceNativeSchemaSetDigest": _DIGEST_SCHEMA,
        "sourceStateDigest": _DIGEST_SCHEMA,
        "sourceStateScope": {"type": "string"},
        "sourceSystemId": {"type": "string"},
        "startedAt": {"type": "string"},
        "verifierId": {"type": "string"},
        "verifierImplementationId": {"type": "string"},
        "verifierVersion": {"type": "string"},
        "warnings": {"type": "array"},
    },
)


def release_schema_bundle() -> dict[str, Mapping[str, Any]]:
    """Return the installed, closed product schema family used by the publisher."""

    nullable_text = _NULLABLE_TEXT_SCHEMA
    digest = _DIGEST_SCHEMA
    schemas: dict[str, Mapping[str, Any]] = {
        "source-native-record.schema.json": _closed_schema(
            {
                "fieldDiagnostics": {"type": "array"},
                "record": {"type": "object"},
                "schemaDigest": digest,
                "schemaName": {"type": "string"},
                "schemaVersion": {"type": "string"},
                "scopeId": {"type": "string"},
                "sourceRecordId": {"type": "string"},
            }
        ),
        "scope.schema.json": _closed_schema(
            {
                "fields": {"type": "object"},
                "scopeId": {"type": "string"},
                "scopeKind": {"type": "string"},
                "sourceSystemId": {"type": "string"},
            }
        ),
        "source-schema-declaration.schema.json": _closed_schema(
            {
                "schemaDigest": digest,
                "schemaName": {"type": "string"},
                "schemaVersion": {"type": "string"},
            }
        ),
        "rendition-index.schema.json": _closed_schema(
            {
                "expectedByteSize": {"type": ["integer", "null"]},
                "expectedSha256": {"type": ["string", "null"]},
                "locator": nullable_text,
                "mediaType": {"type": "string"},
                "renditionId": {"type": "string"},
                "sourceField": {"type": "string"},
                "sourceRecordId": {"type": "string"},
            }
        ),
        "acquisition-ledger.schema.json": _closed_schema(
            {
                "evidenceBlobRef": digest,
                "failure": {"type": "null"},
                "observationRef": {"type": "object"},
                "sourceRecordId": {"type": "string"},
            }
        ),
        "acquisition-page.schema.json": _PAGE_SHAPE.schema,
        "failure.schema.json": _closed_schema(
            {
                "code": {"type": "string"},
                "evidenceDigest": digest,
                "stage": {"type": "string"},
            }
        ),
        "publication-receipt.schema.json": _RECEIPT_SHAPE.schema,
        "release.schema.json": _closed_schema(
            {
                "acquisitionPolicyDigest": digest,
                "acquisitionPolicyId": {"type": "string"},
                "acquisitionPolicyVersion": {"type": "string"},
                "releaseSchemaDigest": digest,
                "sourceNativeSchemaSetDigest": digest,
                "sourceStateDigest": digest,
                "sourceStateScope": {"type": "string"},
                "sourceSystemId": {"type": "string"},
                "sourceSystemVersion": {"type": "string"},
            }
        ),
    }
    return schemas


def installed_release_schema_bundle() -> dict[str, Mapping[str, Any]]:
    """Load the shipped schemas and refuse drift from their sole generator."""

    expected = release_schema_bundle()
    root = files("spicy_regs").joinpath("schemas/source_native_release/1.0")
    try:
        observed_names = {entry.name for entry in root.iterdir() if entry.is_file()}
    except FileNotFoundError as error:
        raise SourceNativeReleaseError("installed source-native schema bundle is missing") from error
    if observed_names != set(expected):
        raise SourceNativeReleaseError("installed source-native schema membership differs")
    installed: dict[str, Mapping[str, Any]] = {}
    for name, generated in expected.items():
        payload = root.joinpath(name).read_bytes()
        if payload != canonical_json_bytes(generated):
            raise SourceNativeReleaseError(f"installed source-native schema differs at {name}")
        value = parse_canonical_json(payload, path=f"installed/{name}")
        if not isinstance(value, Mapping):
            raise SourceNativeReleaseError(f"installed source-native schema is not an object at {name}")
        installed[name] = value
    return installed


def _jsonl_rows(stream: BinaryIO, *, label: str) -> Iterator[Mapping[str, Any]]:
    while raw := stream.readline(MAX_ROW_BYTES + 2):
        if len(raw) > MAX_ROW_BYTES + 1:
            raise SourceNativeReleaseError(f"{label} contains an oversized row")
        if not raw.endswith(b"\n"):
            raise SourceNativeReleaseError(f"{label} contains an unterminated row")
        value = parse_canonical_json(raw[:-1], path=label)
        if not isinstance(value, Mapping):
            raise SourceNativeReleaseError(f"{label} row is not an object")
        yield value


def _read_jsonl(source: MemberSource, object_key: str) -> Iterator[Mapping[str, Any]]:
    with source.open(object_key) as stream:
        yield from _jsonl_rows(stream, label=object_key)


@contextmanager
def _open_descriptor(
    source: MemberSource,
    blob_source: BlobSource | None,
    member: MemberDescriptor,
) -> Iterator[BinaryIO]:
    if member.object_key is not None:
        with source.open(member.object_key) as stream:
            yield stream
        return
    if member.blob_ref is None or blob_source is None:
        raise SourceNativeReleaseError(
            "source-native external members require an injected blob source"
        )
    with blob_source.open(member.blob_ref) as stream:
        yield stream


def _descriptor_rows(
    source: MemberSource,
    blob_source: BlobSource | None,
    member: MemberDescriptor,
) -> Iterator[Mapping[str, Any]]:
    label = member.object_key or member.blob_ref or "external-member"
    with _open_descriptor(source, blob_source, member) as stream:
        yield from _jsonl_rows(stream, label=label)


def _read_one_json(source: MemberSource, object_key: str, byte_limit: int = MAX_ROW_BYTES) -> Mapping[str, Any]:
    with source.open(object_key) as stream:
        raw = stream.read(byte_limit + 1)
    if len(raw) > byte_limit:
        raise SourceNativeReleaseError(f"{object_key} exceeds its product limit")
    value = parse_canonical_json(raw, path=object_key)
    if not isinstance(value, Mapping):
        raise SourceNativeReleaseError(f"{object_key} is not an object")
    return value


def _partition_id(identity: str) -> str:
    if not isinstance(identity, str) or not identity:
        raise SourceNativeReleaseError("source-native partition identity must be nonempty text")
    digest = hashlib.sha256(identity.encode("utf-8")).digest()
    return f"{int.from_bytes(digest, 'big') % PARTITION_BUCKET_COUNT:02d}"


def _partition_policy() -> dict[str, object]:
    return {
        "algorithm": PARTITION_ALGORITHM,
        "bucketCount": PARTITION_BUCKET_COUNT,
        "identityEncoding": "utf-8",
    }


def _file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            yield block


def _stage_partition(
    scratch: Path,
    *,
    blob_store: SourceNativeBlobStore,
    accounting: _ByteAccounting,
    partition_kind: str,
    partition_id: str,
    rows: Iterable[Mapping[str, Any]],
) -> _PayloadPartition | None:
    path = scratch / f"{partition_kind}-{partition_id}.jsonl"
    digest = hashlib.sha256()
    byte_size = 0
    record_count = 0
    try:
        with path.open("xb") as stream:
            for value in rows:
                payload = canonical_json_bytes(value)
                if len(payload) > MAX_ROW_BYTES:
                    raise SourceNativeReleaseError(
                        f"source-native row exceeds {MAX_ROW_BYTES} bytes"
                    )
                for chunk in (payload, b"\n"):
                    stream.write(chunk)
                    digest.update(chunk)
                    byte_size += len(chunk)
                record_count += 1
            stream.flush()
            os.fsync(stream.fileno())
        if record_count == 0:
            return None
        blob_ref = "sha256:" + digest.hexdigest()
        write = blob_store.put_blob(blob_ref, byte_size, _file_chunks(path))
        accounting.add(
            byte_size=byte_size,
            reused=write.reused,
            bytes_written=write.bytes_written,
        )
        return _PayloadPartition(
            partition_kind,
            partition_id,
            describe_member_from_receipt(
                blob_ref=blob_ref,
                role=PARTITION_ROLES[partition_kind],
                media_type="application/x-ndjson",
                byte_size=byte_size,
                record_count=record_count,
            ),
        )
    finally:
        path.unlink(missing_ok=True)


def _policy(build: SourceNativeReleaseBuild, profile: SourceNativeProfile) -> dict[str, Any]:
    return _policy_for_scope(build.query_scope, profile)


def _policy_for_scope(
    query_scope: Mapping[str, Any],
    profile: SourceNativeProfile,
) -> dict[str, Any]:
    policy = profile.acquisition_policy(query_scope)
    if not isinstance(policy, Mapping):
        raise SourceNativeReleaseError(
            f"{profile.name} acquisition policy builder did not return an object"
        )
    return dict(policy)


def _policy_digest(build: SourceNativeReleaseBuild, profile: SourceNativeProfile) -> str:
    return framed_section_digest(
        "spicyregs-acquisition-policy/1",
        (FramedSection("policy", 1, (_policy(build, profile),)),),
    )


def _digest_records(domain: str, section: str, values: Sequence[Mapping[str, Any]]) -> str:
    return framed_section_digest(domain, (FramedSection(section, len(values), values),))


def _source_state_digest(
    scopes: tuple[int, Iterable[Mapping[str, Any]]],
    schemas: tuple[int, Iterable[Mapping[str, Any]]],
    records: tuple[int, Iterable[Mapping[str, Any]]],
    renditions: tuple[int, Iterable[Mapping[str, Any]]],
) -> str:
    return framed_section_digest(
        "spicyregs-source-state/1",
        (
            FramedSection("scopes", *scopes),
            FramedSection("schemas", *schemas),
            FramedSection("records", *records),
            FramedSection("renditions", *renditions),
        ),
    )


def _same_traversal(connection: sqlite3.Connection, left: int, right: int) -> bool:
    left_count = connection.execute("SELECT count(*) FROM observations WHERE traversal = ?", (left,)).fetchone()[0]
    right_count = connection.execute("SELECT count(*) FROM observations WHERE traversal = ?", (right,)).fetchone()[0]
    if left_count != right_count:
        return False
    difference = connection.execute(
        "SELECT 1 FROM observations l JOIN observations r ON r.traversal = ? AND r.ordinal = l.ordinal "
        "WHERE l.traversal = ? AND (l.source_record_id != r.source_record_id OR l.record_digest != r.record_digest) "
        "LIMIT 1",
        (right, left),
    ).fetchone()
    return difference is None


def _observation_version(
    profile: SourceNativeProfile,
    record: Mapping[str, Any],
) -> str | None:
    if profile.observation_version is None:
        return None
    value = profile.observation_version(record)
    if value is not None and (not isinstance(value, str) or not value):
        raise SourceNativeReleaseError(
            f"{profile.name} observation version must be nonempty text or null"
        )
    return value


def _select_observations(
    connection: sqlite3.Connection,
    *,
    profile: SourceNativeProfile,
) -> None:
    """Select one record per source id using the disk-backed SQLite index."""

    connection.execute("UPDATE observations SET selected = 0")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS observations_selection "
        "ON observations (traversal, source_record_id, source_version, record_digest, ordinal)"
    )
    if profile.observation_version is None:
        duplicate = connection.execute(
            "SELECT traversal, source_record_id FROM observations "
            "GROUP BY traversal, source_record_id HAVING count(*) > 1 "
            "ORDER BY traversal, source_record_id LIMIT 1"
        ).fetchone()
        if duplicate is not None:
            raise SourceNativeReleaseError(
                f"{profile.name} traversal repeats {str(duplicate[1])!r}"
            )
    else:
        duplicate_condition = (
            "count(*) > 1"
            if profile.refuse_equal_observation_versions
            else "count(DISTINCT record_digest) > 1"
        )
        duplicate = connection.execute(
            "SELECT traversal, source_record_id, source_version FROM observations "
            "GROUP BY traversal, source_record_id, source_version HAVING "
            f"{duplicate_condition} "
            "ORDER BY traversal, source_record_id, source_version IS NULL, "
            "source_version DESC LIMIT 1"
        ).fetchone()
        if duplicate is not None:
            raise SourceNativeReleaseError(
                f"{profile.name} has an unresolved source-version tie for "
                f"{str(duplicate[1])!r} at {duplicate[2]!r}"
            )

    # The correlated lookup uses the file-backed index above. It marks the
    # newest non-null normalized version, or the sole null version, without a
    # Python collection proportional to the number of source identities.
    connection.execute(
        "UPDATE observations AS candidate SET selected = 1 WHERE NOT EXISTS ("
        "SELECT 1 FROM observations AS preferred "
        "WHERE preferred.traversal = candidate.traversal "
        "AND preferred.source_record_id = candidate.source_record_id "
        "AND ((preferred.source_version IS NOT NULL "
        "AND (candidate.source_version IS NULL "
        "OR preferred.source_version > candidate.source_version)) "
        "OR (preferred.source_version IS candidate.source_version "
        "AND preferred.record_digest = candidate.record_digest "
        "AND preferred.ordinal < candidate.ordinal)))"
    )


def _accepted_traversal(
    connection: sqlite3.Connection,
    *,
    traversal_count: int,
    profile: SourceNativeProfile,
) -> int:
    if profile.traversal_acceptance in {
        "single-observed-traversal",
        "source-enumeration",
    }:
        if traversal_count != 1:
            raise SourceNativeReleaseError(
                f"{profile.name} acceptance requires exactly one traversal"
            )
        return 0
    for right in range(1, traversal_count):
        if _same_traversal(connection, right - 1, right):
            return right
    raise SourceNativeReleaseError(
        "observed crawl lacks two stable consecutive traversals"
    )


def _validate_evidence_media_type(page: SourceNativePage) -> None:
    if page.evidence_media_type not in {"application/json", "application/zip"}:
        raise SourceNativeReleaseError("source-native evidence media type is unsupported")


def _query_mappings(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...] = (),
) -> Iterator[Mapping[str, Any]]:
    for (payload,) in connection.execute(query, parameters):
        value = parse_canonical_json(bytes(payload))
        if not isinstance(value, Mapping):
            raise SourceNativeReleaseError("indexed source-native record is not an object")
        yield value


def _query_renditions(
    connection: sqlite3.Connection,
    accepted_traversal: int,
    partition_id: str | None = None,
) -> Iterator[Mapping[str, Any]]:
    query = (
        "SELECT rendition_payload FROM observations "
        "WHERE traversal = ? AND selected = 1 "
        + ("AND partition_id = ? " if partition_id is not None else "")
        + "ORDER BY source_record_id"
    )
    parameters: tuple[object, ...] = (
        (accepted_traversal, partition_id)
        if partition_id is not None
        else (accepted_traversal,)
    )
    for (payload,) in connection.execute(query, parameters):
        values = parse_canonical_json(bytes(payload))
        if not isinstance(values, list):
            raise SourceNativeReleaseError("indexed rendition group is not an array")
        for value in values:
            if not isinstance(value, Mapping):
                raise SourceNativeReleaseError("indexed rendition row is not an object")
            yield value


def _ordered_rendition_rows(
    profile: SourceNativeProfile,
    record: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    values = tuple(profile.rendition_rows(record))
    keys: list[tuple[str, str]] = []
    for value in values:
        source_record_id = value.get("sourceRecordId")
        rendition_id = value.get("renditionId")
        if (
            not isinstance(source_record_id, str)
            or not source_record_id
            or not isinstance(rendition_id, str)
            or not rendition_id
        ):
            raise SourceNativeReleaseError(
                f"{profile.name} rendition row lacks its closed identity"
            )
        keys.append((source_record_id, rendition_id))
    if len(set(keys)) != len(keys):
        raise SourceNativeReleaseError(f"{profile.name} rendition identity is repeated")
    return tuple(value for _, value in sorted(zip(keys, values, strict=True)))

def _ledger_rows(
    connection: sqlite3.Connection,
    accepted_traversal: int,
    partition_id: str | None = None,
) -> Iterator[Mapping[str, Any]]:
    query = (
        "SELECT observations.source_record_id, observations.evidence_ref "
        "FROM observations "
        "WHERE observations.traversal = ? AND observations.selected = 1 "
        + ("AND observations.partition_id = ? " if partition_id is not None else "")
        +
        "ORDER BY observations.source_record_id"
    )
    parameters: tuple[object, ...] = (
        (accepted_traversal, partition_id)
        if partition_id is not None
        else (accepted_traversal,)
    )
    for source_record_id, evidence_ref in connection.execute(query, parameters):
        yield {
            "evidenceBlobRef": evidence_ref,
            "failure": None,
            "observationRef": {"sourceRecordId": source_record_id},
            "sourceRecordId": source_record_id,
        }


def _page_rows(
    connection: sqlite3.Connection,
    accepted_traversal: int,
    *,
    accepted_only: bool = False,
    partition_id: str | None = None,
) -> Iterator[Mapping[str, Any]]:
    conditions: list[str] = []
    parameters: list[object] = []
    if accepted_only:
        conditions.append("traversal = ?")
        parameters.append(accepted_traversal)
    if partition_id is not None:
        conditions.append("partition_id = ?")
        parameters.append(partition_id)
    query = (
        "SELECT traversal, page, window_index, window_page, records_included, request_key, "
        "source_cursor, next_cursor, evidence_ref, evidence_media_type "
        "FROM pages "
        + (("WHERE " + " AND ".join(conditions) + " ") if conditions else "")
        + "ORDER BY traversal, page"
    )
    for (
        traversal,
        page,
        window_index,
        window_page,
        records_included,
        request_key,
        source_cursor,
        next_cursor,
        evidence_ref,
        evidence_media_type,
    ) in connection.execute(query, tuple(parameters)):
        discovered = [
            {"recordDigest": record_digest, "sourceRecordId": source_record_id}
            for source_record_id, record_digest in connection.execute(
                "SELECT source_record_id, record_digest FROM observations "
                "WHERE traversal = ? AND page = ? ORDER BY ordinal",
                (traversal, page),
            )
        ]
        yield _PAGE_SHAPE.build(
            accepted=traversal == accepted_traversal,
            discoveredRecords=discovered,
            evidenceMediaType=evidence_media_type,
            evidenceBlobRef=evidence_ref,
            pageIndex=page,
            requestKey=request_key,
            recordsIncluded=bool(records_included),
            responseDigest=evidence_ref,
            sourceCursor=source_cursor,
            terminal=next_cursor is None,
            traversalIndex=traversal,
            windowIndex=window_index,
            windowPageIndex=window_page,
        )


class SourceNativeReleasePublisher:
    """Build one source release through injected source-specific semantics."""

    def __init__(
        self,
        profile: SourceNativeProfile,
        *,
        blob_store: SourceNativeBlobStore,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        self._profile = profile
        self._blob_store = blob_store
        self._clock = clock

    def publish(
        self,
        pages: Iterable[SourceNativePage],
        *,
        build: SourceNativeReleaseBuild,
        destination: Path,
    ) -> PublishedSourceNativeRelease:
        profile = self._profile
        canonical_scope = dict(profile.validate_query_scope(build.query_scope))
        if canonical_scope != dict(build.query_scope):
            raise SourceNativeReleaseError(f"{profile.name} query scope is not canonical")
        destination = Path(destination)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"refusing to replace immutable release: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", suffix=".staging", dir=destination.parent))
        scratch = Path(tempfile.mkdtemp(prefix="source-native-index-", dir=destination.parent))
        accounting = _ByteAccounting()
        evidence_members: dict[str, MemberDescriptor] = {}
        try:
            connection = sqlite3.connect(scratch / "release.sqlite3")
            try:
                self._index_pages(
                    connection,
                    pages,
                    blob_store=self._blob_store,
                    accounting=accounting,
                    evidence_members=evidence_members,
                    query_scope=canonical_scope,
                    profile=profile,
                )
                traversal_count = int(connection.execute("SELECT count(DISTINCT traversal) FROM pages").fetchone()[0])
                accepted = _accepted_traversal(
                    connection,
                    traversal_count=traversal_count,
                    profile=profile,
                )
                return self._publish_indexed(
                    connection,
                    staging=staging,
                    scratch=scratch,
                    destination=destination,
                    build=build,
                    accepted_traversal=accepted,
                    traversal_count=traversal_count,
                    profile=profile,
                    blob_store=self._blob_store,
                    accounting=accounting,
                    evidence_descriptors=tuple(
                        evidence_members[key] for key in sorted(evidence_members)
                    ),
                )
            finally:
                connection.close()
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
            shutil.rmtree(staging, ignore_errors=True)

    @staticmethod
    def _index_pages(
        connection: sqlite3.Connection,
        pages: Iterable[SourceNativePage],
        *,
        blob_store: SourceNativeBlobStore,
        accounting: _ByteAccounting,
        evidence_members: dict[str, MemberDescriptor],
        query_scope: Mapping[str, Any],
        profile: SourceNativeProfile,
    ) -> None:
        connection.executescript(
            "CREATE TABLE pages (traversal INTEGER, page INTEGER, window_index INTEGER, window_page INTEGER, "
            "records_included INTEGER, request_key TEXT, source_cursor TEXT, next_cursor TEXT, "
            "evidence_ref TEXT, evidence_media_type TEXT, partition_id TEXT, "
            "PRIMARY KEY (traversal, page), UNIQUE (traversal, window_index, window_page));"
            "CREATE TABLE observations (traversal INTEGER, page INTEGER, ordinal INTEGER, source_record_id TEXT, "
            "source_version TEXT, selected INTEGER NOT NULL DEFAULT 0, record_digest TEXT, "
            "record_payload BLOB, rendition_payload BLOB, evidence_ref TEXT, partition_id TEXT, "
            "PRIMARY KEY (traversal, ordinal));"
        )
        previous: SourceNativePage | None = None
        previous_next: str | None = None
        inventory = None
        seen_urls: set[str] = set()
        current_window: object | None = None
        acquisition_checks: dict[int, AcquisitionCheck] = {}
        ordinal = 0
        saw_page = False
        for page in pages:
            saw_page = True
            if len(page.response_bytes) > MAX_EVIDENCE_BYTES:
                raise SourceNativeReleaseError(f"{profile.name} page exceeds the evidence bound")
            if page.traversal_index >= profile.max_traversals:
                raise SourceNativeReleaseError(f"{profile.name} acquisition exceeds its traversal bound")
            starts_window = page.window_page_index == 0
            if previous is None:
                if (
                    page.traversal_index != 0
                    or page.page_index != 0
                    or page.window_index != 0
                    or not starts_window
                ):
                    raise SourceNativeReleaseError(
                        f"{profile.name} acquisition must start at traversal 0 page 0"
                    )
            elif page.traversal_index == previous.traversal_index:
                if page.page_index != previous.page_index + 1:
                    raise SourceNativeReleaseError(f"{profile.name} page indexes are not contiguous")
                if starts_window:
                    if previous_next is not None or page.window_index != previous.window_index + 1:
                        raise SourceNativeReleaseError(
                            f"{profile.name} window chain is missing or forked"
                        )
                elif (
                    page.window_index != previous.window_index
                    or page.window_page_index != previous.window_page_index + 1
                    or page.source_cursor != previous_next
                    or page.request_key != page.source_cursor
                ):
                    raise SourceNativeReleaseError(f"{profile.name} page chain is missing or forked")
            else:
                if previous_next is not None:
                    raise SourceNativeReleaseError(
                        f"{profile.name} traversal ended before its terminal page"
                    )
                if (
                    page.traversal_index != previous.traversal_index + 1
                    or page.page_index != 0
                    or page.window_index != 0
                    or not starts_window
                ):
                    raise SourceNativeReleaseError(
                        f"{profile.name} traversal indexes are not contiguous"
                    )
                ordinal = 0
            if starts_window:
                inventory = profile.traversal_check()
                seen_urls = {page.request_key}
                if profile.page_window is None:
                    if page.window_index != 0:
                        raise SourceNativeReleaseError(
                            f"{profile.name} acquisition declares an unsupported nested window"
                        )
                    current_window = None
                else:
                    current_window = profile.page_window(page.request_key)
            response = profile.parse_page_response(page.response_bytes)
            records_included = profile.records_included(
                response,
                query_scope=query_scope,
                page_window=current_window,
            )
            if not isinstance(records_included, bool):
                raise SourceNativeReleaseError(
                    f"{profile.name} page disposition is not boolean"
                )
            if starts_window:
                check = acquisition_checks.setdefault(
                    page.traversal_index,
                    profile.acquisition_check(),
                )
                check.add_window(
                    response,
                    page_window=current_window,
                    records_included=records_included,
                    response_bytes=page.response_bytes,
                )
            elif not records_included:
                raise SourceNativeReleaseError(
                    f"{profile.name} non-record evidence cannot continue a page chain"
                )
            if records_included:
                if inventory is None:
                    raise RuntimeError(f"{profile.name} page inventory was not initialized")
                inventory.add(response, page_index=page.window_page_index)
                next_cursor = profile.next_page(response, seen_urls=seen_urls)
            else:
                next_cursor = None
            _validate_evidence_media_type(page)
            evidence_ref = "sha256:" + hashlib.sha256(page.response_bytes).hexdigest()
            evidence_descriptor = evidence_members.get(evidence_ref)
            if evidence_descriptor is None:
                write = blob_store.put_blob(
                    evidence_ref,
                    len(page.response_bytes),
                    (page.response_bytes,),
                )
                accounting.add(
                    byte_size=len(page.response_bytes),
                    reused=write.reused,
                    bytes_written=write.bytes_written,
                )
                evidence_descriptor = describe_member_from_receipt(
                    blob_ref=evidence_ref,
                    role=ROLE_EVIDENCE,
                    media_type=page.evidence_media_type,
                    byte_size=len(page.response_bytes),
                    record_count=0,
                )
                evidence_members[evidence_ref] = evidence_descriptor
            elif (
                evidence_descriptor.byte_size != len(page.response_bytes)
                or evidence_descriptor.media_type != page.evidence_media_type
            ):
                raise SourceNativeReleaseError(
                    "source-native evidence content has conflicting declarations"
                )
            connection.execute(
                "INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    page.traversal_index,
                    page.page_index,
                    page.window_index,
                    page.window_page_index,
                    int(records_included),
                    page.request_key,
                    page.source_cursor,
                    next_cursor,
                    evidence_ref,
                    page.evidence_media_type,
                    _partition_id(f"{page.traversal_index}:{page.page_index}"),
                ),
            )
            for raw_record in response["results"] if records_included else ():
                record = profile.classify_record(raw_record)
                profile.validate_record_scope(
                    record,
                    query_scope=query_scope,
                    page_window=current_window,
                )
                wrapped = profile.wrap_record(
                    record,
                    schema_digest=profile.source_schema_digest(),
                )
                renditions = _ordered_rendition_rows(profile, record)
                source_record_id = wrapped.get("sourceRecordId")
                if not isinstance(source_record_id, str) or not source_record_id:
                    raise SourceNativeReleaseError(
                        f"{profile.name} wrapped record lacks sourceRecordId"
                    )
                try:
                    connection.execute(
                        "INSERT INTO observations VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)",
                        (
                            page.traversal_index,
                            page.page_index,
                            ordinal,
                            source_record_id,
                            _observation_version(profile, record),
                            profile.record_digest(record),
                            canonical_json_bytes(wrapped),
                            canonical_json_bytes(renditions),
                            evidence_ref,
                            _partition_id(source_record_id),
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    raise SourceNativeReleaseError(
                        f"{profile.name} observation index repeats ordinal {ordinal}"
                    ) from error
                ordinal += 1
            previous = page
            previous_next = next_cursor if isinstance(next_cursor, str) else None
            if records_included and previous_next is None:
                if inventory is None:
                    raise RuntimeError(f"{profile.name} page inventory was not initialized")
                inventory.finish()
        if not saw_page:
            raise SourceNativeReleaseError(f"{profile.name} acquisition has no evidence pages")
        if previous_next is not None:
            raise SourceNativeReleaseError(f"{profile.name} acquisition has no terminal page")
        for traversal in sorted(acquisition_checks):
            acquisition_checks[traversal].finish(query_scope=query_scope)
        _select_observations(connection, profile=profile)
        connection.commit()

    def _publish_indexed(
        self,
        connection: sqlite3.Connection,
        *,
        staging: Path,
        scratch: Path,
        destination: Path,
        build: SourceNativeReleaseBuild,
        accepted_traversal: int,
        traversal_count: int,
        profile: SourceNativeProfile,
        blob_store: SourceNativeBlobStore,
        accounting: _ByteAccounting,
        evidence_descriptors: tuple[MemberDescriptor, ...],
    ) -> PublishedSourceNativeRelease:
        scopes = [
            {
                "fields": dict(build.query_scope),
                "scopeId": profile.scope_id,
                "scopeKind": "source-collection",
                "sourceSystemId": profile.source_system_id,
            }
        ]
        schema_declarations = [profile.source_schema_declaration()]
        record_query = "SELECT record_payload FROM observations "
        selected_record_query = (
            record_query + "WHERE traversal = ? AND selected = 1 ORDER BY source_record_id"
        )
        observation_query = (
            "SELECT record_payload FROM observations WHERE traversal = ? "
            "ORDER BY source_record_id, source_version IS NULL, source_version DESC"
        )
        published_record_count = int(
            connection.execute(
                "SELECT count(*) FROM observations WHERE traversal = ? AND selected = 1",
                (accepted_traversal,),
            ).fetchone()[0]
        )
        input_observation_count = int(
            connection.execute(
                "SELECT count(*) FROM observations WHERE traversal = ?",
                (accepted_traversal,),
            ).fetchone()[0]
        )

        def records() -> Iterator[Mapping[str, Any]]:
            return _query_mappings(connection, selected_record_query, (accepted_traversal,))

        def observations() -> Iterator[Mapping[str, Any]]:
            return _query_mappings(connection, observation_query, (accepted_traversal,))

        def renditions() -> Iterator[Mapping[str, Any]]:
            return _query_renditions(connection, accepted_traversal)

        rendition_count = sum(1 for _ in renditions())
        release_schemas = installed_release_schema_bundle()
        page_count = int(connection.execute("SELECT count(*) FROM pages").fetchone()[0])
        accepted_page_count = int(
            connection.execute(
                "SELECT count(*) FROM pages WHERE traversal = ?", (accepted_traversal,)
            ).fetchone()[0]
        )
        partitions: list[_PayloadPartition] = []
        for partition_id in (f"{index:02d}" for index in range(PARTITION_BUCKET_COUNT)):
            rows_by_kind: tuple[tuple[str, Iterable[Mapping[str, Any]]], ...] = (
                (
                    PARTITION_LEDGER,
                    _ledger_rows(connection, accepted_traversal, partition_id),
                ),
                (
                    PARTITION_PAGES,
                    _page_rows(
                        connection,
                        accepted_traversal,
                        partition_id=partition_id,
                    ),
                ),
                (
                    PARTITION_RECORDS,
                    _query_mappings(
                        connection,
                        record_query
                        + "WHERE traversal = ? AND selected = 1 AND partition_id = ? "
                        "ORDER BY source_record_id",
                        (accepted_traversal, partition_id),
                    ),
                ),
                (
                    PARTITION_RENDITIONS,
                    _query_renditions(connection, accepted_traversal, partition_id),
                ),
            )
            for partition_kind, rows in rows_by_kind:
                partition = _stage_partition(
                    scratch,
                    blob_store=blob_store,
                    accounting=accounting,
                    partition_kind=partition_kind,
                    partition_id=partition_id,
                    rows=rows,
                )
                if partition is not None:
                    partitions.append(partition)
        partitions.sort(key=lambda value: (value.partition_kind, value.partition_id))
        partition_counts = {
            kind: sum(
                partition.member.record_count or 0
                for partition in partitions
                if partition.partition_kind == kind
            )
            for kind in PARTITION_KINDS
        }
        if (
            partition_counts[PARTITION_RECORDS] != published_record_count
            or partition_counts[PARTITION_RENDITIONS] != rendition_count
            or partition_counts[PARTITION_LEDGER] != published_record_count
            or partition_counts[PARTITION_PAGES] != page_count
        ):
            raise SourceNativeReleaseError("partitioned source-native accounting differs")

        schema_set_digest = _digest_records(
            "spicyregs-source-schema-set/1", "schemas", schema_declarations
        )
        state_digest = _source_state_digest(
            (len(scopes), scopes),
            (len(schema_declarations), schema_declarations),
            (published_record_count, records()),
            (rendition_count, renditions()),
        )
        input_digest = framed_section_digest(
            "spicyregs-input-observations/1",
            (
                FramedSection(
                    "observations",
                    input_observation_count,
                    observations(),
                ),
            ),
        )
        ledger_digest = framed_section_digest(
            "spicyregs-acquisition-ledger/1",
            (
                FramedSection(
                    "entries",
                    published_record_count,
                    _ledger_rows(connection, accepted_traversal),
                ),
            ),
        )
        reconciliation_digest = framed_section_digest(
            "spicyregs-source-reconciliation/1",
            (
                FramedSection(
                    "pages",
                    accepted_page_count,
                    _page_rows(
                        connection,
                        accepted_traversal,
                        accepted_only=True,
                    ),
                ),
            ),
        )
        release_schema_digest = schema_bundle_digest(release_schemas)
        completed_at = _instant(self._clock)
        if _utc(completed_at, "completed_at") < _utc(build.started_at, "started_at"):
            raise SourceNativeReleaseError("publisher completion precedes acquisition start")
        spec = {
            "acquisitionPolicyDigest": _policy_digest(build, profile),
            "acquisitionPolicyId": profile.acquisition_policy_id,
            "acquisitionPolicyVersion": profile.acquisition_policy_version,
            "releaseSchemaDigest": release_schema_digest,
            "sourceNativeSchemaSetDigest": schema_set_digest,
            "sourceStateDigest": state_digest,
            "sourceStateScope": profile.source_state_scope,
            "sourceSystemId": profile.source_system_id,
            "sourceSystemVersion": profile.source_system_version,
        }
        receipt: dict[str, Any] = {
            "acquisitionEvidenceCount": len(evidence_descriptors),
            "acquisitionLedgerDigest": ledger_digest,
            "acquisitionPolicyDigest": _policy_digest(build, profile),
            "byteMeasurements": {
                "payloadBytesRead": accounting.payload_bytes_read,
                "payloadBytesReused": accounting.payload_bytes_reused,
                "payloadBytesWritten": accounting.payload_bytes_written,
                "publicationBytesWritten": 0,
            },
            "completedAt": completed_at,
            "discoveredRecordCount": input_observation_count,
            "discardedObservationCount": (
                input_observation_count - published_record_count
            ),
            "failedRecordCount": 0,
            "format": FORMAT,
            "formatVersion": FORMAT_VERSION,
            "inputObservationCount": input_observation_count,
            "inputObservationDigest": input_digest,
            "partitionPolicy": _partition_policy(),
            "payloadPartitions": [partition.receipt() for partition in partitions],
            "publishedRecordCount": published_record_count,
            "reconciliationDigest": reconciliation_digest,
            "reconciliationPassCount": traversal_count,
            "releaseSchemaDigest": release_schema_digest,
            "releaseSchemaId": RELEASE_SCHEMA_ID,
            "renditionIndexCount": rendition_count,
            "semanticVerdict": "pass",
            "sourceNativeSchemaSetDigest": schema_set_digest,
            "sourceStateDigest": state_digest,
            "sourceStateScope": profile.source_state_scope,
            "sourceSystemId": profile.source_system_id,
            "startedAt": build.started_at,
            "verifierId": build.producer.verifier_id,
            "verifierImplementationId": build.producer.verifier_implementation_id,
            "verifierVersion": build.producer.verifier_version,
            "warnings": [],
        }
        scopes_bytes = b"".join(
            chunk
            for value in scopes
            for chunk in (canonical_json_bytes(value), b"\n")
        )
        source_schema_bytes = canonical_json_bytes(profile.source_schema)
        release_schema_bytes = canonical_json_bytes(release_schemas)
        external_members = (
            *evidence_descriptors,
            *(partition.member for partition in partitions),
        )
        refs = [member.blob_ref for member in external_members]
        if None in refs or len(set(refs)) != len(refs):
            raise SourceNativeReleaseError(
                "source-native external payload members must have distinct content identities"
            )
        publication_bytes = -1
        for _ in range(8):
            receipt["byteMeasurements"]["publicationBytesWritten"] = max(
                publication_bytes, 0
            )
            receipt = _RECEIPT_SHAPE.parse(receipt)
            receipt_bytes = canonical_json_bytes(receipt)
            local_members = (
                describe_member_from_receipt(
                    object_key=SCOPES_KEY,
                    sha256="sha256:" + hashlib.sha256(scopes_bytes).hexdigest(),
                    role=ROLE_SCOPES,
                    media_type="application/x-ndjson",
                    byte_size=len(scopes_bytes),
                    record_count=1,
                ),
                describe_member_from_receipt(
                    object_key=RECEIPT_KEY,
                    sha256="sha256:" + hashlib.sha256(receipt_bytes).hexdigest(),
                    role=ROLE_RECEIPT,
                    media_type="application/json",
                    byte_size=len(receipt_bytes),
                ),
                describe_member_from_receipt(
                    object_key=RELEASE_SCHEMA_KEY,
                    sha256="sha256:" + hashlib.sha256(release_schema_bytes).hexdigest(),
                    role=ROLE_RELEASE_SCHEMA,
                    media_type="application/schema+json",
                    byte_size=len(release_schema_bytes),
                    schema_id=RELEASE_SCHEMA_ID,
                ),
                describe_member_from_receipt(
                    object_key=profile.source_schema_key,
                    sha256="sha256:" + hashlib.sha256(source_schema_bytes).hexdigest(),
                    role=ROLE_SCHEMA,
                    media_type="application/schema+json",
                    byte_size=len(source_schema_bytes),
                    schema_id=str(profile.source_schema["$id"]),
                ),
            )
            manifest, manifest_bytes = MemberManifestReference.for_members(
                scope_kind="global",
                scope_id="source-native",
                object_key=MANIFEST_KEY,
                members=(*local_members, *external_members),
            )
            root = build_artifact_root(
                kind=KIND,
                spec=spec,
                producer=build.producer,
                manifests=(manifest,),
                supersedes=build.supersedes,
            )
            root_bytes = canonical_json_bytes(root)
            measured = (
                len(scopes_bytes)
                + len(receipt_bytes)
                + len(release_schema_bytes)
                + len(source_schema_bytes)
                + len(manifest_bytes)
                + len(root_bytes)
            )
            if measured == publication_bytes:
                break
            publication_bytes = measured
        else:
            raise SourceNativeReleaseError(
                "source-native publication byte accounting did not stabilize"
            )
        write_bytes_once(staging / SCOPES_KEY, scopes_bytes)
        write_bytes_once(staging / profile.source_schema_key, source_schema_bytes)
        write_bytes_once(staging / RELEASE_SCHEMA_KEY, release_schema_bytes)
        write_bytes_once(staging / RECEIPT_KEY, receipt_bytes)
        write_bytes_once(staging / MANIFEST_KEY, manifest_bytes)
        write_bytes_once(staging / ROOT_OBJECT_KEY, root_bytes)
        artifact = admit_artifact(
            LocalMemberSource(staging),
            blob_source=blob_store,
            semantic_verifier=lambda artifact, source: verify_source_native_release(
                artifact,
                source,
                profile=profile,
                blob_source=blob_store,
            ),
            scratch_directory=scratch / "verify",
        )
        publish_directory_once(staging, destination)
        return PublishedSourceNativeRelease(destination, artifact)


def _member_index(
    artifact: VerifiedArtifact,
    source: MemberSource,
) -> tuple[
    dict[str, MemberDescriptor],
    dict[str, MemberDescriptor],
    dict[str, list[MemberDescriptor]],
]:
    by_key: dict[str, MemberDescriptor] = {}
    by_ref: dict[str, MemberDescriptor] = {}
    by_role: dict[str, list[MemberDescriptor]] = {}
    for member in iter_member_descriptors(artifact, source):
        if member.object_key is not None:
            by_key[member.object_key] = member
        elif member.blob_ref is not None:
            by_ref[member.blob_ref] = member
        else:
            raise SourceNativeReleaseError("source-native member has no location")
        by_role.setdefault(member.role, []).append(member)
    roles = frozenset(by_role)
    if not ALWAYS_REQUIRED_ROLES <= roles <= REQUIRED_ROLES:
        raise SourceNativeReleaseError(
            f"source-native roles differ; missing={sorted(ALWAYS_REQUIRED_ROLES - roles)}, "
            f"unknown={sorted(roles - REQUIRED_ROLES)}"
        )
    for role in (ROLE_RECORDS, ROLE_RENDITIONS, ROLE_LEDGER, ROLE_EVIDENCE):
        if any(member.blob_ref is None for member in by_role.get(role, ())):
            raise SourceNativeReleaseError(
                f"source-native {role} payload members must use external blobRef locations"
            )
    for role in (ROLE_SCOPES, ROLE_SCHEMA, ROLE_RELEASE_SCHEMA, ROLE_RECEIPT):
        if any(member.object_key is None for member in by_role[role]):
            raise SourceNativeReleaseError(
                f"source-native {role} publication members must be local"
            )
    return by_key, by_ref, by_role


def _partition_row_identity(
    partition_kind: str,
    row: Mapping[str, Any],
) -> tuple[int, int, int, str, str]:
    if partition_kind == PARTITION_PAGES:
        traversal = row.get("traversalIndex")
        page = row.get("pageIndex")
        if (
            isinstance(traversal, bool)
            or not isinstance(traversal, int)
            or traversal < 0
            or isinstance(page, bool)
            or not isinstance(page, int)
            or page < 0
        ):
            raise SourceNativeReleaseError("acquisition-page partition key is invalid")
        return (0, traversal, page, "", "")
    source_record_id = row.get("sourceRecordId")
    if not isinstance(source_record_id, str) or not source_record_id:
        raise SourceNativeReleaseError(
            f"source-native {partition_kind} row lacks sourceRecordId"
        )
    if partition_kind == PARTITION_RENDITIONS:
        rendition_id = row.get("renditionId")
        if not isinstance(rendition_id, str) or not rendition_id:
            raise SourceNativeReleaseError("rendition partition row lacks renditionId")
        return (1, 0, 0, source_record_id, rendition_id)
    return (1, 0, 0, source_record_id, "")


def _identity_bucket_for_row(partition_kind: str, row: Mapping[str, Any]) -> str:
    key = _partition_row_identity(partition_kind, row)
    if partition_kind == PARTITION_PAGES:
        return _partition_id(f"{key[1]}:{key[2]}")
    return _partition_id(key[3])


def _partition_rows(
    source: MemberSource,
    blob_source: BlobSource | None,
    partitions: Sequence[_PayloadPartition],
) -> Iterator[Mapping[str, Any]]:
    if not partitions:
        return
    selected = tuple(sorted(partitions, key=lambda value: value.partition_id))
    partition_kind = selected[0].partition_kind
    if any(value.partition_kind != partition_kind for value in selected):
        raise SourceNativeReleaseError("source-native partition stream mixes kinds")
    with ExitStack() as stack:
        iterators: list[Iterator[Mapping[str, Any]]] = []
        heap: list[
            tuple[tuple[int, int, int, str, str], int, Mapping[str, Any]]
        ] = []
        previous_by_partition: list[tuple[int, int, int, str, str] | None] = []
        for index, partition in enumerate(selected):
            stream = stack.enter_context(
                _open_descriptor(source, blob_source, partition.member)
            )
            iterator = _jsonl_rows(
                stream,
                label=partition.member.blob_ref or "external-member",
            )
            iterators.append(iterator)
            previous_by_partition.append(None)
            try:
                row = next(iterator)
            except StopIteration:
                if partition.member.record_count != 0:
                    raise SourceNativeReleaseError(
                        "source-native partition record count differs"
                    )
                continue
            key = _partition_row_identity(partition_kind, row)
            if _identity_bucket_for_row(partition_kind, row) != partition.partition_id:
                raise SourceNativeReleaseError(
                    "source-native row is assigned to the wrong identity bucket"
                )
            previous_by_partition[index] = key
            heapq.heappush(heap, (key, index, row))
        observed_counts = [0 for _ in selected]
        previous_key: tuple[int, int, int, str, str] | None = None
        while heap:
            key, index, row = heapq.heappop(heap)
            if previous_key is not None and key <= previous_key:
                raise SourceNativeReleaseError(
                    f"source-native {partition_kind} rows are repeated or unordered"
                )
            previous_key = key
            observed_counts[index] += 1
            yield row
            try:
                next_row = next(iterators[index])
            except StopIteration:
                continue
            next_key = _partition_row_identity(partition_kind, next_row)
            previous_in_partition = previous_by_partition[index]
            if previous_in_partition is not None and next_key <= previous_in_partition:
                raise SourceNativeReleaseError(
                    f"source-native {partition_kind} partition is unordered"
                )
            if _identity_bucket_for_row(partition_kind, next_row) != selected[index].partition_id:
                raise SourceNativeReleaseError(
                    "source-native row is assigned to the wrong identity bucket"
                )
            previous_by_partition[index] = next_key
            heapq.heappush(heap, (next_key, index, next_row))
        for partition, observed in zip(selected, observed_counts, strict=True):
            if partition.member.record_count != observed:
                raise SourceNativeReleaseError(
                    "source-native partition record count differs"
                )


def _payload_partitions(
    receipt: Mapping[str, Any],
    by_ref: Mapping[str, MemberDescriptor],
) -> dict[str, tuple[_PayloadPartition, ...]]:
    if receipt.get("partitionPolicy") != _partition_policy():
        raise SourceNativeReleaseError("source-native partition policy differs")
    raw_partitions = receipt.get("payloadPartitions")
    if not isinstance(raw_partitions, list):
        raise SourceNativeReleaseError("source-native payload partitions are absent")
    result: dict[str, list[_PayloadPartition]] = {kind: [] for kind in PARTITION_KINDS}
    previous: tuple[str, str] | None = None
    seen_refs: set[str] = set()
    for raw in raw_partitions:
        if not isinstance(raw, Mapping):
            raise SourceNativeReleaseError("source-native payload partition is not an object")
        partition = dict(raw)
        kind = partition.get("partitionKind")
        partition_id = partition.get("partitionId")
        blob_ref = partition.get("blobRef")
        if (
            not isinstance(kind, str)
            or kind not in PARTITION_ROLES
            or not isinstance(partition_id, str)
            or len(partition_id) != 2
            or not partition_id.isascii()
            or not partition_id.isdigit()
            or int(partition_id) >= PARTITION_BUCKET_COUNT
            or not isinstance(blob_ref, str)
        ):
            raise SourceNativeReleaseError("source-native payload partition identity is invalid")
        order_key = (kind, partition_id)
        if previous is not None and order_key <= previous:
            raise SourceNativeReleaseError(
                "source-native payload partitions are repeated or unordered"
            )
        previous = order_key
        member = by_ref.get(blob_ref)
        if (
            member is None
            or member.role != PARTITION_ROLES[kind]
            or member.media_type != "application/x-ndjson"
            or member.byte_size != partition.get("byteSize")
            or member.record_count != partition.get("recordCount")
            or not isinstance(member.record_count, int)
            or member.record_count <= 0
            or blob_ref in seen_refs
        ):
            raise SourceNativeReleaseError(
                "source-native payload partition differs from its manifest member"
            )
        seen_refs.add(blob_ref)
        result[kind].append(_PayloadPartition(kind, partition_id, member))
    partition_roles = {ROLE_RECORDS, ROLE_RENDITIONS, ROLE_LEDGER}
    manifest_partition_refs = {
        blob_ref for blob_ref, member in by_ref.items() if member.role in partition_roles
    }
    if seen_refs != manifest_partition_refs:
        raise SourceNativeReleaseError(
            "source-native receipt does not account for every payload partition"
        )
    return {kind: tuple(values) for kind, values in result.items()}


def verify_source_native_admission(
    artifact: VerifiedArtifact,
    source: MemberSource,
    *,
    profile: SourceNativeProfile,
    blob_source: BlobSource | None,
) -> None:
    """Check bounded receipt/root agreement for consumer open."""

    by_key, by_ref, by_role = _member_index(artifact, source)
    if by_ref and blob_source is None:
        raise SourceNativeReleaseError(
            "source-native external members require an injected blob source"
        )
    if artifact.root.get("kind") != KIND:
        raise SourceNativeReleaseError("artifact is not a SpicyRegs source-native release")
    spec = artifact.root.get("spec")
    if not isinstance(spec, Mapping):
        raise SourceNativeReleaseError("source-native root spec is not an object")
    if set(spec) != SPEC_FIELDS:
        raise SourceNativeReleaseError("source-native root spec fields differ")
    if (
        spec.get("sourceSystemId") != profile.source_system_id
        or spec.get("sourceSystemVersion") != profile.source_system_version
        or spec.get("acquisitionPolicyId") != profile.acquisition_policy_id
        or spec.get("acquisitionPolicyVersion") != profile.acquisition_policy_version
        or spec.get("sourceStateScope") != profile.source_state_scope
    ):
        raise SourceNativeReleaseError(
            f"source-native root names an unsupported {profile.name} profile"
        )
    receipts = by_role[ROLE_RECEIPT]
    if len(receipts) != 1 or receipts[0].object_key != RECEIPT_KEY:
        raise SourceNativeReleaseError("source-native release must carry one publication receipt")
    singleton_members = {
        ROLE_RELEASE_SCHEMA: RELEASE_SCHEMA_KEY,
        ROLE_SCHEMA: profile.source_schema_key,
        ROLE_SCOPES: SCOPES_KEY,
    }
    for role, object_key in singleton_members.items():
        members = by_role[role]
        if len(members) != 1 or members[0].object_key != object_key:
            raise SourceNativeReleaseError(f"source-native release must carry one {role} member")
    receipt = _RECEIPT_SHAPE.parse(_read_one_json(source, RECEIPT_KEY))
    partitions = _payload_partitions(receipt, by_ref)
    for field in (
        "acquisitionPolicyDigest",
        "releaseSchemaDigest",
        "sourceNativeSchemaSetDigest",
        "sourceStateDigest",
        "sourceStateScope",
        "sourceSystemId",
    ):
        if receipt.get(field) != spec.get(field):
            raise SourceNativeReleaseError(f"receipt differs from root spec at {field}")
    producer = artifact.root.get("producer")
    if not isinstance(producer, Mapping):
        raise SourceNativeReleaseError("source-native producer record is absent")
    if (
        producer.get("product") != "spicy-regs"
        or producer.get("verifierId") != VERIFIER_ID
        or producer.get("verifierVersion") != VERIFIER_VERSION
    ):
        raise SourceNativeReleaseError("source-native producer names an unsupported verifier")
    for receipt_field, producer_field in (
        ("verifierId", "verifierId"),
        ("verifierVersion", "verifierVersion"),
        ("verifierImplementationId", "verifierImplementationId"),
    ):
        if receipt.get(receipt_field) != producer.get(producer_field):
            raise SourceNativeReleaseError(f"receipt differs from producer at {receipt_field}")
    if receipt.get("semanticVerdict") != "pass" or receipt.get("failedRecordCount") != 0:
        raise SourceNativeReleaseError("source-native receipt is not publishable")
    if (
        receipt.get("format") != FORMAT
        or receipt.get("formatVersion") != FORMAT_VERSION
        or receipt.get("releaseSchemaId") != RELEASE_SCHEMA_ID
    ):
        raise SourceNativeReleaseError("source-native receipt format is unsupported")
    started = _utc(str(receipt.get("startedAt")), "receipt.startedAt")
    completed = _utc(str(receipt.get("completedAt")), "receipt.completedAt")
    if completed < started:
        raise SourceNativeReleaseError("source-native receipt completion precedes start")
    for field in (
        "acquisitionEvidenceCount",
        "discoveredRecordCount",
        "discardedObservationCount",
        "failedRecordCount",
        "inputObservationCount",
        "publishedRecordCount",
        "reconciliationPassCount",
        "renditionIndexCount",
    ):
        value = receipt.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SourceNativeReleaseError(f"source-native receipt count is invalid at {field}")
    if (
        receipt["inputObservationCount"]
        != receipt["publishedRecordCount"] + receipt["discardedObservationCount"]
        or receipt["discoveredRecordCount"]
        != receipt["inputObservationCount"] + receipt["failedRecordCount"]
    ):
        raise SourceNativeReleaseError("source-native receipt count equations differ")
    partition_counts = {
        kind: sum(value.member.record_count or 0 for value in partitions[kind])
        for kind in PARTITION_KINDS
    }
    if (
        partition_counts[PARTITION_RECORDS] != receipt["publishedRecordCount"]
        or partition_counts[PARTITION_RENDITIONS] != receipt["renditionIndexCount"]
        or partition_counts[PARTITION_LEDGER] != receipt["publishedRecordCount"]
        or receipt["acquisitionEvidenceCount"] != len(by_role[ROLE_EVIDENCE])
    ):
        raise SourceNativeReleaseError(
            "source-native receipt counts differ from payload membership"
        )
    measurements = receipt["byteMeasurements"]
    payload_bytes = sum(member.byte_size for member in by_ref.values())
    if (
        measurements["payloadBytesRead"] != payload_bytes
        or measurements["payloadBytesReused"] > payload_bytes
        or measurements["payloadBytesWritten"] > payload_bytes
    ):
        raise SourceNativeReleaseError(
            "source-native payload byte measurements do not reconcile"
        )
    with source.open(ROOT_OBJECT_KEY) as stream:
        root_bytes = stream.read(MAX_ROW_BYTES + 1)
    if len(root_bytes) > MAX_ROW_BYTES:
        raise SourceNativeReleaseError("source-native root exceeds its product limit")
    publication_roles = {
        ROLE_SCOPES,
        ROLE_SCHEMA,
        ROLE_RELEASE_SCHEMA,
        ROLE_RECEIPT,
    }
    publication_bytes = (
        sum(
            member.byte_size
            for role in publication_roles
            for member in by_role[role]
        )
        + sum(manifest.byte_size for manifest in artifact.manifests)
        + len(root_bytes)
    )
    if measurements["publicationBytesWritten"] != publication_bytes:
        raise SourceNativeReleaseError(
            "source-native publication byte measurements do not reconcile"
        )
    warnings = receipt.get("warnings")
    if not isinstance(warnings, list) or any(
        not isinstance(value, Mapping)
        or set(value) != {"code", "message"}
        or not isinstance(value.get("code"), str)
        or not value.get("code")
        or not isinstance(value.get("message"), str)
        or not value.get("message")
        for value in warnings
    ):
        raise SourceNativeReleaseError("source-native receipt warnings are not closed")
    if warnings != sorted(warnings, key=lambda value: (value["code"], value["message"])):
        raise SourceNativeReleaseError("source-native receipt warnings are not sorted")
    if profile.source_schema_key not in by_key or RELEASE_SCHEMA_KEY not in by_key:
        raise SourceNativeReleaseError("source-native schema members are absent")
    release_schemas = _read_one_json(source, RELEASE_SCHEMA_KEY)
    if (
        release_schemas != installed_release_schema_bundle()
        or schema_bundle_digest(release_schemas) != spec["releaseSchemaDigest"]
    ):
        raise SourceNativeReleaseError("installed release schema bundle differs")
    source_schema = _read_one_json(source, profile.source_schema_key)
    if source_schema != profile.source_schema:
        raise SourceNativeReleaseError(f"installed {profile.name} source schema differs")
    schema_set_digest = _digest_records(
        "spicyregs-source-schema-set/1",
        "schemas",
        [profile.source_schema_declaration()],
    )
    if schema_set_digest != spec["sourceNativeSchemaSetDigest"]:
        raise SourceNativeReleaseError("source-native schema-set digest differs")


def _replay_acquisition(
    connection: sqlite3.Connection,
    *,
    source: MemberSource,
    blob_source: BlobSource | None,
    evidence_members: Mapping[str, MemberDescriptor],
    page_partitions: Sequence[_PayloadPartition],
    query_scope: Mapping[str, Any],
    profile: SourceNativeProfile,
) -> tuple[int, int]:
    """Replay sorted page/evidence streams with only one active window in memory."""

    connection.executescript(
        "CREATE TABLE pages (traversal INTEGER, page INTEGER, window_index INTEGER, window_page INTEGER, "
        "records_included INTEGER, accepted INTEGER, request_key TEXT, source_cursor TEXT, next_cursor TEXT, "
        "evidence_ref TEXT, payload BLOB, PRIMARY KEY (traversal, page), "
        "UNIQUE (traversal, window_index, window_page));"
        "CREATE TABLE observations (traversal INTEGER, ordinal INTEGER, source_record_id TEXT, "
        "source_version TEXT, selected INTEGER NOT NULL DEFAULT 0, record_digest TEXT, "
        "record_payload BLOB, evidence_ref TEXT, PRIMARY KEY (traversal, ordinal));"
    )
    pages = _partition_rows(source, blob_source, page_partitions)
    seen_evidence: set[str] = set()
    previous_traversal = -1
    previous_page = -1
    previous_window = -1
    previous_window_page = -1
    previous_next: str | None = None
    current_window: object | None = None
    current_inventory: TraversalCheck | None = None
    current_seen_urls: set[str] = set()
    current_check: AcquisitionCheck | None = None
    ordinal = 0
    saw_page = False
    for raw_page_row in pages:
        page_row = _PAGE_SHAPE.parse(raw_page_row)
        traversal = page_row.get("traversalIndex")
        page_index = page_row.get("pageIndex")
        window_index = page_row.get("windowIndex")
        window_page_index = page_row.get("windowPageIndex")
        declared_records_included = page_row.get("recordsIncluded")
        accepted_flag = page_row.get("accepted")
        assert isinstance(traversal, int)
        assert isinstance(page_index, int)
        assert isinstance(window_index, int)
        assert isinstance(window_page_index, int)
        assert isinstance(declared_records_included, bool)
        assert isinstance(accepted_flag, bool)
        starts_traversal = traversal != previous_traversal
        starts_window = window_page_index == 0
        if starts_traversal:
            if current_check is not None:
                current_check.finish(query_scope=query_scope)
            if (
                traversal != previous_traversal + 1
                or page_index != 0
                or window_index != 0
                or not starts_window
                or previous_next is not None
            ):
                raise SourceNativeReleaseError(
                    "acquisition traversal indexes are missing or forked"
                )
            current_check = profile.acquisition_check()
            previous_page = -1
            previous_window = -1
            previous_window_page = -1
            ordinal = 0
        elif page_index != previous_page + 1:
            raise SourceNativeReleaseError("acquisition page indexes are not contiguous")
        source_cursor = page_row.get("sourceCursor")
        request_key = page_row.get("requestKey")
        if (
            source_cursor is not None
            and (not isinstance(source_cursor, str) or not source_cursor)
            or not isinstance(request_key, str)
            or not request_key
        ):
            raise SourceNativeReleaseError("acquisition page cursor or request key is invalid")
        if starts_window:
            if (
                source_cursor is not None
                or previous_next is not None
                or window_index != previous_window + 1
            ):
                raise SourceNativeReleaseError(
                    "acquisition window indexes are missing or forked"
                )
            if profile.page_window is None:
                if window_index != 0:
                    raise SourceNativeReleaseError(
                        f"{profile.name} acquisition declares an unsupported nested window"
                    )
                current_window = None
            else:
                current_window = profile.page_window(request_key)
            current_inventory = profile.traversal_check()
            current_seen_urls = {request_key}
        elif (
            window_index != previous_window
            or window_page_index != previous_window_page + 1
            or source_cursor != previous_next
            or request_key != source_cursor
        ):
            raise SourceNativeReleaseError(
                "acquisition continuation request differs from its cursor chain"
            )
        evidence_ref = page_row.get("evidenceBlobRef")
        evidence_media_type = page_row.get("evidenceMediaType")
        member = evidence_members.get(evidence_ref) if isinstance(evidence_ref, str) else None
        if (
            member is None
            or member.blob_ref != evidence_ref
            or evidence_ref != page_row.get("responseDigest")
            or member.media_type != evidence_media_type
        ):
            raise SourceNativeReleaseError("acquisition page evidence pin differs")
        assert isinstance(evidence_ref, str)
        seen_evidence.add(evidence_ref)
        with _open_descriptor(source, blob_source, member) as stream:
            response_bytes = stream.read(MAX_EVIDENCE_BYTES + 1)
        if len(response_bytes) > MAX_EVIDENCE_BYTES:
            raise SourceNativeReleaseError("acquisition evidence exceeds its product bound")
        response = profile.parse_page_response(response_bytes)
        records_included = profile.records_included(
            response,
            query_scope=query_scope,
            page_window=current_window,
        )
        if records_included is not declared_records_included:
            raise SourceNativeReleaseError(
                "acquisition page disposition differs from source evidence"
            )
        if starts_window:
            assert current_check is not None
            current_check.add_window(
                response,
                page_window=current_window,
                records_included=records_included,
                response_bytes=response_bytes,
            )
        elif not records_included:
            raise SourceNativeReleaseError(
                f"{profile.name} non-record evidence cannot continue a page chain"
            )
        if current_inventory is None:
            raise SourceNativeReleaseError("acquisition window begins without an explicit boundary")
        if records_included:
            current_inventory.add(response, page_index=window_page_index)
            next_cursor = profile.next_page(response, seen_urls=current_seen_urls)
        else:
            next_cursor = None
        terminal = next_cursor is None
        if records_included and terminal:
            current_inventory.finish()
        if page_row.get("terminal") is not terminal:
            raise SourceNativeReleaseError("acquisition terminal marker differs from source evidence")
        discovered = page_row.get("discoveredRecords")
        if not isinstance(discovered, list):
            raise SourceNativeReleaseError("acquisition page discoveredRecords is not an array")
        expected_discovered = []
        for raw in response["results"] if records_included else ():
            classified = profile.classify_record(raw)
            profile.validate_record_scope(
                classified,
                query_scope=query_scope,
                page_window=current_window,
            )
            wrapped = profile.wrap_record(
                classified,
                schema_digest=profile.source_schema_digest(),
            )
            identity = wrapped.get("sourceRecordId")
            if not isinstance(identity, str) or not identity:
                raise SourceNativeReleaseError(
                    f"{profile.name} wrapped record lacks sourceRecordId"
                )
            digest = profile.record_digest(classified)
            expected_discovered.append({"recordDigest": digest, "sourceRecordId": identity})
            try:
                connection.execute(
                    "INSERT INTO observations VALUES (?, ?, ?, ?, 0, ?, ?, ?)",
                    (
                        traversal,
                        ordinal,
                        identity,
                        _observation_version(profile, classified),
                        digest,
                        canonical_json_bytes(wrapped),
                        evidence_ref,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise SourceNativeReleaseError(
                    f"accepted acquisition repeats observation ordinal {ordinal}"
                ) from error
            ordinal += 1
        if discovered != expected_discovered:
            raise SourceNativeReleaseError("acquisition page record inventory differs")
        connection.execute(
            "INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                traversal,
                page_index,
                window_index,
                window_page_index,
                int(records_included),
                int(accepted_flag),
                request_key,
                source_cursor,
                next_cursor,
                evidence_ref,
                canonical_json_bytes(page_row),
            ),
        )
        saw_page = True
        previous_traversal = traversal
        previous_page = page_index
        previous_window = window_index
        previous_window_page = window_page_index
        previous_next = next_cursor if isinstance(next_cursor, str) else None
    if not saw_page or current_check is None:
        raise SourceNativeReleaseError("acquisition has no evidence pages")
    if seen_evidence != set(evidence_members):
        raise SourceNativeReleaseError(
            "acquisition pages do not account for exact evidence membership"
        )
    current_check.finish(query_scope=query_scope)
    if previous_next is not None:
        raise SourceNativeReleaseError("acquisition traversal has no terminal page")
    traversal_count = previous_traversal + 1
    if traversal_count > profile.max_traversals:
        raise SourceNativeReleaseError("acquisition traversal count exceeds its bound")
    _select_observations(connection, profile=profile)
    accepted = _accepted_traversal(
        connection,
        traversal_count=traversal_count,
        profile=profile,
    )
    wrong_flag = connection.execute(
        "SELECT 1 FROM pages WHERE accepted != (traversal = ?) LIMIT 1",
        (accepted,),
    ).fetchone()
    if wrong_flag is not None:
        raise SourceNativeReleaseError("acquisition accepted-traversal flags differ")
    connection.commit()
    return accepted, traversal_count


def verify_source_native_release(
    artifact: VerifiedArtifact,
    source: MemberSource,
    *,
    profile: SourceNativeProfile,
    blob_source: BlobSource | None,
) -> None:
    """Recompute the selected source profile at the producer gate."""

    verify_source_native_admission(
        artifact,
        source,
        profile=profile,
        blob_source=blob_source,
    )
    _, by_ref, by_role = _member_index(artifact, source)
    spec = artifact.root["spec"]
    if not isinstance(spec, Mapping):
        raise SourceNativeReleaseError("source-native root spec is not an object")
    receipt = _RECEIPT_SHAPE.parse(_read_one_json(source, RECEIPT_KEY))
    partitions = _payload_partitions(receipt, by_ref)
    scopes = list(_read_jsonl(source, SCOPES_KEY))
    if len(scopes) != 1 or set(scopes[0]) != {"fields", "scopeId", "scopeKind", "sourceSystemId"}:
        raise SourceNativeReleaseError(f"{profile.name} source scope differs")
    if (
        scopes[0].get("scopeId") != profile.scope_id
        or scopes[0].get("scopeKind") != "source-collection"
        or scopes[0].get("sourceSystemId") != profile.source_system_id
        or not isinstance(scopes[0].get("fields"), Mapping)
    ):
        raise SourceNativeReleaseError(f"{profile.name} source scope values differ")
    query_scope = dict(profile.validate_query_scope(scopes[0]["fields"]))
    if query_scope != dict(scopes[0]["fields"]):
        raise SourceNativeReleaseError(f"{profile.name} source scope is not canonical")
    policy_digest = framed_section_digest(
        "spicyregs-acquisition-policy/1",
        (
            FramedSection(
                "policy",
                1,
                (_policy_for_scope(query_scope, profile),),
            ),
        ),
    )
    if policy_digest != spec["acquisitionPolicyDigest"]:
        raise SourceNativeReleaseError("acquisition-policy digest differs")
    schema_declarations = [profile.source_schema_declaration()]
    schema_set_digest = _digest_records("spicyregs-source-schema-set/1", "schemas", schema_declarations)
    if schema_set_digest != spec["sourceNativeSchemaSetDigest"]:
        raise SourceNativeReleaseError("source-native schema-set digest differs")
    evidence_members = {
        member.blob_ref: member
        for member in by_role[ROLE_EVIDENCE]
        if member.blob_ref is not None
    }
    with tempfile.TemporaryDirectory(prefix="source-native-verify-") as directory:
        connection = sqlite3.connect(Path(directory) / "replay.sqlite3")
        try:
            accepted, traversal_count = _replay_acquisition(
                connection,
                source=source,
                blob_source=blob_source,
                evidence_members=evidence_members,
                page_partitions=partitions[PARTITION_PAGES],
                query_scope=query_scope,
                profile=profile,
            )
            if receipt["reconciliationPassCount"] != traversal_count:
                raise SourceNativeReleaseError("reconciliation pass count differs")
            published_record_count = int(
                connection.execute(
                    "SELECT count(*) FROM observations "
                    "WHERE traversal = ? AND selected = 1",
                    (accepted,),
                ).fetchone()[0]
            )
            input_observation_count = int(
                connection.execute(
                    "SELECT count(*) FROM observations WHERE traversal = ?",
                    (accepted,),
                ).fetchone()[0]
            )

            def replayed_records() -> Iterator[Mapping[str, Any]]:
                return _query_mappings(
                    connection,
                    "SELECT record_payload FROM observations "
                    "WHERE traversal = ? AND selected = 1 ORDER BY source_record_id",
                    (accepted,),
                )

            def replayed_observations() -> Iterator[Mapping[str, Any]]:
                return _query_mappings(
                    connection,
                    "SELECT record_payload FROM observations WHERE traversal = ? "
                    "ORDER BY source_record_id, source_version IS NULL, source_version DESC",
                    (accepted,),
                )

            def admitted_records() -> Iterator[Mapping[str, Any]]:
                return _partition_rows(
                    source,
                    blob_source,
                    partitions[PARTITION_RECORDS],
                )

            def expected_renditions() -> Iterator[Mapping[str, Any]]:
                for row in replayed_records():
                    record = row.get("record")
                    if not isinstance(record, Mapping):
                        raise SourceNativeReleaseError(
                            f"published {profile.name} record payload is invalid"
                        )
                    yield from _ordered_rendition_rows(
                        profile,
                        profile.classify_record(record),
                    )

            rendition_count = sum(1 for _ in expected_renditions())

            sentinel = object()
            observed_count = 0
            for actual, expected in zip_longest(admitted_records(), replayed_records(), fillvalue=sentinel):
                if actual is sentinel or expected is sentinel or actual != expected:
                    raise SourceNativeReleaseError("published records differ from replayed source evidence")
                observed_count += 1
            if observed_count != published_record_count:
                raise SourceNativeReleaseError("published record count differs from replayed evidence")

            observed_rendition_count = 0
            for actual, expected in zip_longest(
                _partition_rows(
                    source,
                    blob_source,
                    partitions[PARTITION_RENDITIONS],
                ),
                expected_renditions(),
                fillvalue=sentinel,
            ):
                if actual is sentinel or expected is sentinel or actual != expected:
                    raise SourceNativeReleaseError("rendition index differs from source-stated locators")
                observed_rendition_count += 1
            if observed_rendition_count != rendition_count:
                raise SourceNativeReleaseError("rendition count differs from replayed evidence")

            def expected_ledger() -> Iterator[Mapping[str, Any]]:
                query = (
                    "SELECT observations.source_record_id, observations.evidence_ref "
                    "FROM observations "
                    "WHERE observations.traversal = ? AND observations.selected = 1 "
                    "ORDER BY observations.source_record_id"
                )
                for source_record_id, evidence_ref in connection.execute(
                    query, (accepted,)
                ):
                    yield {
                        "evidenceBlobRef": evidence_ref,
                        "failure": None,
                        "observationRef": {"sourceRecordId": source_record_id},
                        "sourceRecordId": source_record_id,
                    }

            def admitted_ledger() -> Iterator[Mapping[str, Any]]:
                return _partition_rows(
                    source,
                    blob_source,
                    partitions[PARTITION_LEDGER],
                )

            observed_ledger_count = 0
            for actual, expected in zip_longest(
                admitted_ledger(), expected_ledger(), fillvalue=sentinel
            ):
                if actual is sentinel or expected is sentinel or actual != expected:
                    raise SourceNativeReleaseError("acquisition ledger differs from replayed evidence")
                observed_ledger_count += 1
            if observed_ledger_count != published_record_count:
                raise SourceNativeReleaseError("acquisition-ledger count differs")

            state_digest = _source_state_digest(
                (len(scopes), scopes),
                (len(schema_declarations), schema_declarations),
                (published_record_count, admitted_records()),
                (
                    rendition_count,
                    _partition_rows(
                        source,
                        blob_source,
                        partitions[PARTITION_RENDITIONS],
                    ),
                ),
            )
            if state_digest != spec["sourceStateDigest"]:
                raise SourceNativeReleaseError("source-state digest differs")
            input_digest = framed_section_digest(
                "spicyregs-input-observations/1",
                (
                    FramedSection(
                        "observations",
                        input_observation_count,
                        replayed_observations(),
                    ),
                ),
            )
            if input_digest != receipt["inputObservationDigest"]:
                raise SourceNativeReleaseError("input-observation digest differs")
            ledger_digest = framed_section_digest(
                "spicyregs-acquisition-ledger/1",
                (
                    FramedSection(
                        "entries",
                        published_record_count,
                        admitted_ledger(),
                    ),
                ),
            )
            if ledger_digest != receipt["acquisitionLedgerDigest"]:
                raise SourceNativeReleaseError("acquisition-ledger digest differs")
            accepted_page_count = int(
                connection.execute("SELECT count(*) FROM pages WHERE accepted = 1").fetchone()[0]
            )
            accepted_pages = _query_mappings(
                connection,
                "SELECT payload FROM pages WHERE accepted = 1 ORDER BY traversal, page",
            )
            reconciliation_digest = framed_section_digest(
                "spicyregs-source-reconciliation/1",
                (FramedSection("pages", accepted_page_count, accepted_pages),),
            )
            if reconciliation_digest != receipt["reconciliationDigest"]:
                raise SourceNativeReleaseError("source-reconciliation digest differs")
            counts = {
                "acquisitionEvidenceCount": len(evidence_members),
                "discoveredRecordCount": input_observation_count,
                "discardedObservationCount": (
                    input_observation_count - published_record_count
                ),
                "failedRecordCount": 0,
                "inputObservationCount": input_observation_count,
                "publishedRecordCount": published_record_count,
                "renditionIndexCount": rendition_count,
            }
            for name, expected in counts.items():
                if receipt.get(name) != expected:
                    raise SourceNativeReleaseError(f"receipt count differs at {name}")
        finally:
            connection.close()


class SourceNativeReleaseReader:
    """Structurally admit once, then stream records through an injected source."""

    def __init__(
        self,
        source: MemberSource,
        *,
        blob_source: BlobSource,
        profile: SourceNativeProfile,
        accepted_verifier_implementation_ids: frozenset[str],
        expected_pin: ArtifactPin | None = None,
    ) -> None:
        if not accepted_verifier_implementation_ids:
            raise SourceNativeReleaseError("at least one verifier implementation must be accepted")
        self._source = source
        self._blob_source = blob_source
        self._artifact = admit_artifact(
            source,
            blob_source=blob_source,
            expected_pin=expected_pin,
            semantic_verifier=lambda artifact, source: verify_source_native_admission(
                artifact,
                source,
                profile=profile,
                blob_source=blob_source,
            ),
        )
        producer = self._artifact.root["producer"]
        if producer["verifierImplementationId"] not in accepted_verifier_implementation_ids:
            raise SourceNativeReleaseError("source-native verifier implementation is not accepted")
        _, by_ref, _ = _member_index(self._artifact, source)
        receipt = _RECEIPT_SHAPE.parse(_read_one_json(source, RECEIPT_KEY))
        partitions = _payload_partitions(receipt, by_ref)
        self._record_partitions = partitions[PARTITION_RECORDS]
        self._rendition_partitions = partitions[PARTITION_RENDITIONS]
        spec = self._artifact.root["spec"]
        self.source_state_scope = str(spec["sourceStateScope"])
        self.source_system_id = str(spec["sourceSystemId"])
        self.source_system_version = str(spec["sourceSystemVersion"])
        self.source_state_digest = str(spec["sourceStateDigest"])
        self.source_native_schema_set_digest = str(spec["sourceNativeSchemaSetDigest"])

    @property
    def pin(self) -> ArtifactPin:
        return self._artifact.pin

    def iter_records(self) -> Iterator[Mapping[str, Any]]:
        yield from _partition_rows(
            self._source,
            self._blob_source,
            self._record_partitions,
        )

    def iter_renditions(self) -> Iterator[Mapping[str, Any]]:
        yield from _partition_rows(
            self._source,
            self._blob_source,
            self._rendition_partitions,
        )


__all__ = [
    "KIND",
    "PublishedSourceNativeRelease",
    "SourceNativeReleaseBuild",
    "SourceNativeReleaseError",
    "SourceNativeReleasePublisher",
    "SourceNativeReleaseReader",
    "installed_release_schema_bundle",
    "release_schema_bundle",
    "verify_source_native_admission",
    "verify_source_native_release",
]
