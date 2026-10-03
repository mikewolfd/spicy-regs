"""Immutable table generations and a single conditional publication pointer.

Writers verify all remote bytes before changing the pointer. Readers resolve
the pointer once per operation and use immutable table URLs. The materialized
rulemaking dataset publishes under its own pointer (``SNAPSHOT_POINTER``),
which readers resolve the same way. Legacy bare URLs remain readable only for
tables neither pointer names.

A table stored as several files (a split table) is listed only in the version-2
index, ``INDEX_V2_KEY``; the version-1 index lists every other table unchanged
for readers that predate splitting (``docs/research/multi-file-tables-2026-09-26.md``).
Readers resolve any table's files through :func:`table_members`.
Source-evidence blobs are content-addressed and stored once for all artifacts.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Callable, NamedTuple

import httpx
from loguru import logger

if TYPE_CHECKING:
    from botocore.exceptions import ClientError

INDEX_KEY = "publication.json"
#: The index that also lists split tables; readers prefer it when it exists.
INDEX_V2_KEY = "publication.v2.json"
#: The materialized rulemaking dataset's pointer to its current snapshot manifest.
SNAPSHOT_POINTER = "materialized/rulemaking/latest.json"
#: A materialized snapshot id: safe in an object key and inlined SQL. ``pipelines.materialized`` writes and reads by it.
SNAPSHOT_ID = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
#: Snapshot format versions a reader accepts. A published snapshot outlives the
#: code that wrote it, and the prior generation a run restores from was written
#: before version 2 made ``visibility`` required, so refusing version 1 outright
#: would strand every existing dataset's state at its next build.
SNAPSHOT_FORMAT_VERSIONS = (1, 2)
INDEX_LIMIT = 1024 * 1024
PART_BYTES = 64 * 1024 * 1024
EVIDENCE_PREFIX = "source-evidence"
#: Concurrent blob requests while publishing evidence, as r2.py bounds its uploads.
EVIDENCE_WORKERS = 8
#: An evidence manifest lists every retained body and its journal every
#: capture, so both grow with a run's reads (house communications' 1,043
#: captures: a 261 KB manifest and a 588 KB journal, 2026-09-26). Rulespec
#: bounds a manifest at the same 64 MiB.
EVIDENCE_CONTROL_LIMIT = 64 * 1024 * 1024
_IMMUTABLE_HEADERS = {"ContentType": "application/octet-stream", "CacheControl": "public, max-age=31536000, immutable"}
_BLOB_KEY = re.compile(r"blobs/sha256/([0-9a-f]{64})\Z")
_POINTER_ATTEMPTS = 8
_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_PARTITION_VALUE = re.compile(r"[A-Za-z0-9_.-]+\Z")
_PART = re.compile(r"part-\d{6}\.parquet\Z")
_MEMBER_PATH = re.compile(r"generations/[a-z][a-z0-9_-]*/[0-9a-f]{64}/(.+)\Z")
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


def _counts(value: Mapping) -> bool:
    return all(type(value[field]) is int and value[field] >= 0 for field in ("byteSize", "rows"))


def _split_table(key: str, table: Mapping) -> None:
    """A split table: its members partition it by declared columns, and their counts sum to the table's.

    A partition column is a plain name, since it spells a directory of every member key.
    """
    if set(table) != {"byteSize", "rows", "columns", "partitionColumns", "members"} or not _counts(table):
        raise ValueError("invalid split table descriptor")
    by = table["partitionColumns"]
    if (not isinstance(by, list) or not by or len(set(by)) != len(by)
            or not all(isinstance(column, str) and _NAME.fullmatch(column) for column in by)
            or not set(by) <= {column[0] for column in table["columns"]}):
        raise ValueError("invalid partition columns")
    members, seen = table["members"], set()
    if not isinstance(members, list) or not members:
        raise ValueError("split table has no members")
    for member in members:
        if set(member) != {"key", "sha256", "byteSize", "rows", "partition"} or not _counts(member):
            raise ValueError("invalid member descriptor")
        partition = member["partition"]
        if (not isinstance(partition, dict) or list(partition) != by
                or not all(isinstance(v, str) and _PARTITION_VALUE.fullmatch(v) for v in partition.values())):
            raise ValueError("invalid member partition")
        directory = "/".join([key[:-8], *(f"{column}={value}" for column, value in partition.items())])
        name = member["key"].removeprefix(directory + "/")
        if name == member["key"] or not _PART.fullmatch(name) or member["key"] in seen:
            raise ValueError("member key differs from its partition")
        if not _DIGEST.fullmatch(member["sha256"]):
            raise ValueError("invalid member digest")
        seen.add(member["key"])
    if (sum(m["rows"] for m in members) != table["rows"]
            or sum(m["byteSize"] for m in members) != table["byteSize"]):
        raise ValueError("member counts differ from their table's")


def member_table(key: str) -> str:
    """The table a member key belongs to: the key itself for a single file, else ``<first directory>.parquet``."""
    return key if "/" not in key else key.split("/", 1)[0] + ".parquet"


def member_partition(key: str) -> dict[str, str]:
    """A split member's ``col=value`` directories, in order; a malformed one raises ``ValueError``."""
    partition = {}
    for part in key.split("/")[1:-1]:
        column, separator, value = part.partition("=")
        if not separator:
            raise ValueError(f"member directory is not col=value: {part!r}")
        partition[column] = value
    return partition


def table_entries(tables: Mapping, members) -> dict[str, dict]:
    """Index descriptors for a generation's declared ``tables`` from its member descriptors, in the index's shape.

    A table declaring ``partitionColumns`` is split: its members are ``<table>/<col>=<value>/part-NNNNNN.parquet``
    and must add up to it. Any other table is exactly one member at its own key. ``ValueError`` names a mismatch.
    """
    grouped: dict[str, list] = {}
    for member in members:
        grouped.setdefault(member_table(member.object_key), []).append(member)
    if not tables or set(grouped) != set(tables):
        raise ValueError("Generation membership differs from its table declarations")
    entries = {}
    for key, declared in tables.items():
        found = sorted(grouped[key], key=lambda member: member.object_key)
        if "partitionColumns" not in declared:
            if [member.object_key for member in found] != [key]:
                raise ValueError("Generation membership differs from its table declarations")
            entries[key] = {"sha256": found[0].sha256, "byteSize": found[0].byte_size, **declared}
            continue
        entries[key] = {**declared, "byteSize": sum(member.byte_size for member in found), "members": [
            {"key": member.object_key, "sha256": member.sha256, "byteSize": member.byte_size,
             "rows": member.record_count, "partition": member_partition(member.object_key)} for member in found]}
        _split_table(key, entries[key])
    return entries


def parse_index(raw: bytes) -> dict:
    """Validate the small mutable pointer without claiming payload verification.

    Version 1 lists only single-file tables. Version 2 may also list split tables, each with its members.
    """
    if len(raw) > INDEX_LIMIT:
        raise PublicationError("Publication index exceeds its byte limit")
    try:
        value = json.loads(raw, object_pairs_hook=_pairs)
        if (
            set(value) != {"format", "version", "families"}
            or value["format"] != "spicy-regs-publication"
            or type(value["version"]) is not int
            or value["version"] not in (1, 2)
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
                columns = table.get("columns")
                if not isinstance(columns, list) or not columns:
                    raise ValueError("missing columns")
                if any(
                    not isinstance(c, list) or len(c) != 2 or not all(isinstance(x, str) and x for x in c)
                    for c in columns
                ):
                    raise ValueError("invalid columns")
                if len({c[0] for c in columns}) != len(columns):
                    raise ValueError("duplicate columns")
                if "members" in table and value["version"] == 2:
                    _split_table(key, table)
                    continue
                if set(table) != {"sha256", "byteSize", "rows", "columns"}:
                    raise ValueError("invalid table descriptor")
                if not _DIGEST.fullmatch(table["sha256"]):
                    raise ValueError("invalid table digest")
                if not _counts(table):
                    raise ValueError("invalid table counts")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid publication index") from exc
    return value


def _bounded_get(
    url: str, *, allow_missing: bool, headers: Mapping[str, str] | None = None, limit: int = INDEX_LIMIT
) -> bytes | None:
    """GET at most ``limit`` bytes; a 404 is ``None`` only when ``allow_missing``."""
    with httpx.stream("GET", url, headers={"User-Agent": "spicy-regs", **(headers or {})},
                      follow_redirects=True, timeout=60) as response:
        if allow_missing and response.status_code == 404:
            return None
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_bytes():
            raw.extend(chunk)
            if len(raw) > limit:
                raise PublicationError(f"{url} exceeds its byte limit")
    return bytes(raw)


def load_index(base_url: str) -> dict:
    """Read the version-2 pointer, or the version-1 pointer while no version 2 is published.

    Only a 404 on both permits legacy table resolution.
    """
    for key in (INDEX_V2_KEY, INDEX_KEY):
        raw = _bounded_get(f"{base_url.rstrip('/')}/{key}", allow_missing=True, headers={"Cache-Control": "no-cache"})
        if raw is not None:
            return parse_index(raw)
    return empty_index()


def load_rulemaking_snapshot(base_url: str) -> dict | None:
    """Read the rulemaking pointer and the manifest it names, once; ``None`` while no snapshot is published.

    Returns the snapshot id, its manifest key and, by table key, the record of
    each artifact the manifest marks public. As ``pipelines.materialized``
    reads its prior generation, both documents must be a readable format
    version, the same one, of this dataset and one snapshot; each public
    artifact must also sit at its own name under that snapshot's prefix,
    because readers inline these keys into URLs and SQL. An artifact that does
    not say it is public is left out.
    """
    base = base_url.rstrip("/")
    raw = _bounded_get(f"{base}/{SNAPSHOT_POINTER}", allow_missing=True, headers={"Cache-Control": "no-cache"})
    if raw is None:
        return None
    root = SNAPSHOT_POINTER.removesuffix("latest.json")
    dataset = root.removeprefix("materialized/").rstrip("/")
    try:
        pointer = json.loads(raw, object_pairs_hook=_pairs)
        snapshot_id = pointer["snapshot_id"]
        prefix = f"{root}snapshots/{snapshot_id}"
        if pointer["format_version"] not in SNAPSHOT_FORMAT_VERSIONS or pointer["dataset"] != dataset:
            raise ValueError("pointer is not a readable rulemaking pointer")
        if not SNAPSHOT_ID.fullmatch(snapshot_id) or pointer["manifest_key"] != f"{prefix}/manifest.json":
            raise ValueError("pointer names no manifest of its own snapshot")
        raw = _bounded_get(f"{base}/{prefix}/manifest.json", allow_missing=False)
        assert raw is not None
        manifest = json.loads(raw, object_pairs_hook=_pairs)
        if (manifest["format_version"], manifest["dataset"], manifest["snapshot_id"]) != (
            pointer["format_version"], dataset, snapshot_id
        ):
            raise ValueError("pointer and manifest name different snapshots")
        tables = {key: record for key, record in manifest["artifacts"].items() if record.get("visibility") == "public"}
        for key, record in tables.items():
            if not key.endswith(".parquet") or not _NAME.fullmatch(key[:-8]) or record["remote_key"] != f"{prefix}/{key}":
                raise ValueError(f"public artifact {key!r} is not its snapshot's member")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid rulemaking snapshot") from exc
    return {"snapshot_id": snapshot_id, "manifest_key": f"{prefix}/manifest.json", "tables": tables,
            "manifest": manifest}


#: The comment files the export receipt identifies at fixed public URLs, as table names; the receipt's agency
#: partitions are downloads, not served tables.
COMMENTS_EXPORT_TABLES = ("comments", "comments_index")


def load_comments_publication(base_url: str) -> dict | None:
    """Read the catalog export receipt; fixed member URLs still require version checks.

    This receipt is written only after both public comment files and all agency
    partitions have passed readback. It identifies one catalog snapshot, not an
    immutable generation URL. Readers must check member ETags around their read.
    """
    raw = _bounded_get(f"{base_url.rstrip('/')}/comments-publication.json", allow_missing=True,
                       headers={"Cache-Control": "no-cache"})
    if raw is None:
        return None
    try:
        receipt = json.loads(raw, object_pairs_hook=_pairs)
        source = receipt["source"]
        if receipt["format_version"] != 1 or not source["table_uuid"]:
            raise ValueError("unknown receipt format or table identity")
        if any(type(source[key]) is not int or source[key] < 0 for key in ("snapshot_id", "schema_id")):
            raise ValueError("invalid catalog version")
        for table in COMMENTS_EXPORT_TABLES:
            record = receipt["files"][table + ".parquet"]
            if not re.fullmatch(r"[0-9a-f]{64}", record["sha256"]) or not record["etag"]:
                raise ValueError("invalid public file identity")
            if any(type(record[name]) is not int or record[name] < 0 for name in ("rows", "bytes")):
                raise ValueError("invalid public file size")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PublicationError("Invalid comments publication receipt") from exc
    return {"receipt_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(), "receipt": receipt}


def comments_export_pins(base_url: str, comments: Mapping, tables: Iterable[str]) -> dict[str, dict]:
    """Each named export table's pin from a loaded receipt: its file record, fixed URL and the receipt's identity."""
    base = base_url.rstrip("/")
    receipt = comments["receipt"]
    pins = {}
    for table in tables:
        record = receipt["files"][table + ".parquet"]
        pins[table] = {**record, "sha256": "sha256:" + record["sha256"], "kind": "comments-mirror",
                       "family": "comments-mirror", "receipt_sha256": comments["receipt_sha256"],
                       "source": receipt["source"], "urls": [f"{base}/{table}.parquet"]}
    return pins


def mutable_versions_match(pins: Mapping[str, Mapping | None]) -> bool:
    """Fixed comment URLs are usable only while their published object versions match the receipt.

    One HEAD per comments-mirror pin; other pins are immutable and pass.
    """
    for pin in pins.values():
        if pin is None or pin.get("kind") != "comments-mirror":
            continue
        response = httpx.head(pin["urls"][0], follow_redirects=True, timeout=30,
                              headers={"Cache-Control": "no-cache"})
        response.raise_for_status()
        if response.headers.get("etag") != pin["etag"] or int(response.headers.get("content-length", -1)) != pin["bytes"]:
            return False
    return True


def published_urls(base_url: str) -> dict[str, list[str]]:
    """Each table the publisher's two pointers name, by name, to its immutable file URLs.

    The publication index's generations come first, then the rulemaking
    snapshot's public artifacts. A table neither names is legacy: its URL is
    the bare ``<name>.parquet`` key, which the caller adds.
    """
    base = base_url.rstrip("/")
    index, snapshot = load_index(base), load_rulemaking_snapshot(base)
    urls = {table: [f"{base}/{member.path}" for member in table_members(index, f"{table}.parquet")]
            for table in parquet_tables(index)}
    for key, record in (snapshot or {"tables": {}})["tables"].items():
        urls.setdefault(key.removesuffix(".parquet"), [f"{base}/{record['remote_key']}"])
    return urls


def fetch_member(base_url: str, member: Member, local_path: Path, label: str | None = None, *,
                 headers: Mapping[str, str] | None = None, timeout: float | None = None) -> bool:
    """Stream one table file to ``local_path`` through a sibling temp file, checked against its pin when it has one.

    ``False`` only when an unpinned (legacy) file is absent (HTTP 404). A missing pinned member, any other status, a
    transport error or bytes that differ from the pin raise, leaving ``local_path`` untouched and no temp file.
    ``headers`` and ``timeout`` pass through to the request when given.
    """
    label = label or member.path
    url = f"{base_url.rstrip('/')}/{member.path}"
    temp_path = local_path.with_suffix(local_path.suffix + ".tmp")
    request: dict = {name: value for name, value in (("headers", headers), ("timeout", timeout)) if value is not None}
    digest, byte_size = hashlib.sha256(), 0
    try:
        with httpx.stream("GET", url, follow_redirects=True, **request) as response:
            if response.status_code == 404:
                if member.sha256 is not None:
                    raise RuntimeError(f"Published generation member is missing: {label}")
                logger.info("{} not found on R2 (404)", label)
                return False
            if response.status_code != 200:
                raise RuntimeError(f"Failed to download {label} from R2: HTTP {response.status_code}")
            with temp_path.open("wb") as out:
                for chunk in response.iter_bytes():
                    out.write(chunk)
                    digest.update(chunk)
                    byte_size += len(chunk)
        if member.sha256 is not None and (
                member.byte_size != byte_size or member.sha256 != "sha256:" + digest.hexdigest()):
            raise RuntimeError(f"Published generation member differs from its pin: {label}")
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise
    temp_path.replace(local_path)
    logger.info("Downloaded {} from R2", label)
    return True


def parquet_scan(paths: Sequence[str]) -> str:
    """``read_parquet`` over one table's files as a SQL expression.

    Hive partitioning is off: a split table stores its partition column inside each file, so a path's ``col=value``
    must not add a second one.
    """
    listed = ", ".join("'" + path.replace("'", "''") + "'" for path in paths)
    return f"read_parquet([{listed}], hive_partitioning = false)"


def current_index(base_url: str) -> dict:
    """Return the index bound by the active ``snapshot`` for ``base_url``, otherwise load it."""
    held = _snapshot.get()
    return held[1] if held is not None and held[0] == base_url.rstrip("/") else load_index(base_url)


def family_root(raw: bytes, entry: Mapping) -> dict:
    """Parse an artifact root and require it to be the one ``entry`` pins; the caller chooses where ``raw`` came from."""
    from rulespec_artifacts import ArtifactVerificationError, expected_artifact_digest, parse_canonical_json

    try:
        root = parse_canonical_json(raw)
    except ArtifactVerificationError as exc:
        raise PublicationError("Pinned root is not canonical artifact JSON") from exc
    if (not isinstance(root, dict) or root.get("artifactDigest") != entry["artifactDigest"]
            or expected_artifact_digest(root) != entry["artifactDigest"]
            or root.get("logicalId") != entry["logicalId"] or not isinstance(root.get("inputs"), list)):
        raise PublicationError("Pinned root differs from its captured pin")
    return root


def load_family_root(base_url: str, entry: Mapping) -> tuple[bytes, dict]:
    """Read only the pinned prior root for lineage over HTTPS; this does not re-admit its tables."""
    raw = _bounded_get(f"{base_url.rstrip('/')}/{entry['prefix']}/artifact.json", allow_missing=False)
    assert raw is not None
    return raw, family_root(raw, entry)


def load_evidence_journal(base_url: str, pin: Mapping) -> bytes:
    """Read one published source-evidence journal, each hop checked against the digest above it; no blob is read.

    The root must be the one ``pin`` names, its member manifest the bytes the
    root declares, and the journal the bytes that manifest declares.
    """
    from rulespec_artifacts import ROOT_OBJECT_KEY, parse_admitted_json, sha256_digest

    from spicy_regs.source_evidence import JOURNAL

    prefix = f"{base_url.rstrip('/')}/{EVIDENCE_PREFIX}/{pin['artifactDigest'].removeprefix('sha256:')}"

    def pinned(key: str, digest: str) -> bytes:
        raw = _bounded_get(f"{prefix}/{key}", allow_missing=False, limit=EVIDENCE_CONTROL_LIMIT)
        if raw is None or sha256_digest(raw) != digest:
            raise PublicationError(f"Source evidence {key} differs from its declared digest")
        return raw

    raw_root = _bounded_get(f"{prefix}/{ROOT_OBJECT_KEY}", allow_missing=False)
    assert raw_root is not None
    for reference in family_root(raw_root, pin)["memberManifests"]:
        manifest = parse_admitted_json(pinned(reference["objectKey"], reference["sha256"]))
        for member in manifest["members"]:
            if member["objectKey"] == JOURNAL:
                return pinned(JOURNAL, member["sha256"])
    raise PublicationError("Source evidence declares no journal")


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


class Member(NamedTuple):
    """One file of a table: its path under the public base and, when published, its pin."""

    path: str
    sha256: str | None
    byte_size: int | None
    rows: int | None

    @property
    def key(self) -> str:
        """The file's key within its generation, ``<table>.parquet`` or a split member's; a legacy file's own path.

        A downloaded table keeps this layout locally. A pinned member's path must be
        ``generations/<family>/<digest>/<key>``.
        """
        if self.sha256 is None:
            return self.path
        match = _MEMBER_PATH.fullmatch(self.path)
        if match is None:
            raise PublicationError(f"Member path is not under a generation prefix: {self.path}")
        return match[1]


def table_members(index: Mapping, key: str) -> tuple[Member, ...]:
    """Every file of table ``key``, in index order; one unpinned member at the bare key when no family publishes it.

    The bare-key fallback is what keeps legacy table URLs readable.
    """
    owner = table_owner(index, key)
    if owner is None:
        return (Member(key, None, None, None),)
    prefix, table = owner[1]["prefix"], owner[1]["tables"][key]
    return tuple(Member(f"{prefix}/{m['key']}", m["sha256"], m["byteSize"], m["rows"])
                 for m in table.get("members", [{"key": key, **table}]))


def table_descriptor(index: Mapping, key: str) -> dict | None:
    """The index's descriptor for table ``key`` (rows, bytes, columns and, when split, members), or ``None``."""
    owner = table_owner(index, key)
    return None if owner is None else owner[1]["tables"][key]


def single_member(index: Mapping, key: str) -> Member:
    """The one file of table ``key``; a split table refuses, naming the resolver its reader needs."""
    members = table_members(index, key)
    if len(members) != 1:
        raise PublicationError(f"{key} is published as {len(members)} files; read it through table_members")
    return members[0]


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


def derive_v1(index: Mapping) -> dict:
    """The version-1 view of ``index``: split tables omitted, and a family left with none omitted too."""
    families = {}
    for name, entry in index["families"].items():
        tables = {key: table for key, table in entry["tables"].items() if "members" not in table}
        if tables:
            families[name] = {**entry, "tables": tables}
    return {**empty_index(), "families": families}


#: The version-1 index as read while version 2 was absent: its bytes and ETag, each ``None`` when it was absent too.
Bootstrap = tuple[bytes | None, str | None]


def _stored_index(client, bucket: str) -> tuple[dict, str | None, Bootstrap | None]:
    """Read the stored version-2 index, its conditional-write token and, while version 2 is absent, the bootstrap.

    Until version 2 is first written, the version-1 index stands in with no token, so that write creates version 2
    from it, and the version 1 read is returned as the bootstrap for :func:`_write_v1`; absent both, empty and
    unpinned. A response without an ETag refuses.
    """
    for key in (INDEX_V2_KEY, INDEX_KEY):
        stored = _get_bounded(client, bucket, key)
        if stored is not None:
            raw, etag = stored
            if not isinstance(etag, str) or not etag:
                raise PublicationError("Publication index has no conditional-write token")
            index = {**parse_index(raw), "version": 2}
            return (index, etag, None) if key == INDEX_V2_KEY else (index, None, stored)
    return {**empty_index(), "version": 2}, None, (None, None)


def _put_pointer(client, bucket: str, key: str, raw: bytes, etag: str | None) -> bool:
    """Replace pointer ``key`` only if it still has ``etag`` (``None``: only if absent); ``False`` when it moved."""
    from botocore.exceptions import ClientError

    condition = {"IfMatch": etag} if etag is not None else {"IfNoneMatch": "*"}
    try:
        client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType="application/json",
                          CacheControl="no-store, no-cache, must-revalidate", **condition)
    except ClientError as exc:
        if not _precondition(exc):
            raise
        return False
    return True


def _write_v1(client, bucket: str, bootstrap: Bootstrap | None = None) -> None:
    """Write ``publication.json`` as :func:`derive_v1` of the stored version 2, under its own conditional write.

    Version 1's token is read before version 2, so a writer that loses this race rederives from the newer version 2.
    With no version 2 there is nothing to derive, and version 1 stays the pointer.

    ``bootstrap`` is the version 1 a writer read while version 2 was absent, so its write is conditional on that ETag.
    When version 1 changed since, :func:`_fold_v1` first moves into version 2 what a writer predating version 2 wrote
    to version 1 alone, so deriving cannot erase it. Nothing folds once version 2 exists: a writer predating version 2
    must not publish after the merge that introduces it, which is why that merge waits until no publish is in flight.
    """
    from rulespec_artifacts import canonical_json_bytes

    for _ in range(_POINTER_ATTEMPTS):
        stored = _get_bounded(client, bucket, INDEX_KEY)
        if bootstrap is not None and (stored or (None, None))[1] != bootstrap[1]:
            _fold_v1(client, bucket, bootstrap[0], None if stored is None else stored[0])
        index, _, absent = _stored_index(client, bucket)
        if absent is not None:
            return
        raw = canonical_json_bytes(derive_v1(index))
        if stored is not None and stored[0] == raw:
            return
        if _put_pointer(client, bucket, INDEX_KEY, raw, None if stored is None else stored[1]):
            return
    raise PublicationError(f"{INDEX_KEY} kept changing; derive it again from {INDEX_V2_KEY}")


def _fold_v1(client, bucket: str, before: bytes | None, after: bytes | None) -> None:
    """Move into version 2 each family written to version 1 alone between the version-1 reads ``before`` and ``after``.

    A family whose version-1 entry changed while version 2's view of it still equals ``before`` was published by a
    writer predating version 2. One whose version-2 view changed too was published through version 2, which wins.
    """
    was, now = ({} if raw is None else parse_index(raw)["families"] for raw in (before, after))
    for _ in range(_POINTER_ATTEMPTS):
        index, etag, _ = _stored_index(client, bucket)
        view = derive_v1(index)["families"]
        folded = sorted(name for name, entry in now.items() if entry != was.get(name) and view.get(name) == was.get(name))
        if not folded:
            return
        raw = b""
        for name in folded:
            index, raw = _merge_family(index, name, now[name])
        logger.warning("publication: folding {} from {} into {}; a writer predating {} published them",
                       ", ".join(folded), INDEX_KEY, INDEX_V2_KEY, INDEX_V2_KEY)
        if _put_pointer(client, bucket, INDEX_V2_KEY, raw, etag):
            return
    raise PublicationError(f"{INDEX_V2_KEY} kept changing while {INDEX_KEY} was folded into it")


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

    Metadata members come from ``source-evidence/<digest>/``. Each
    ``blobs/sha256/<hex>`` member comes from the locally admitted copy when the
    stored object's size and ETag already equal those bytes (``trusted``), and
    otherwise directly from the shared remote prefix.
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
    before admission. An existing blob whose size and ETag equal the local
    bytes is not transferred again; any other existing object is read back and
    refused unless its bytes match. Small metadata stays under the artifact's
    prefix. Uploads run ``EVIDENCE_WORKERS`` at a time. After every blob exists
    and metadata is written, admission streams untrusted remote blobs directly
    without retaining local verification copies.
    """
    from concurrent.futures import ThreadPoolExecutor
    from rulespec_artifacts import LocalMemberSource, admit_artifact, iter_member_descriptors

    local = LocalMemberSource(path)
    prefix = f"{EVIDENCE_PREFIX}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    declared = {member.object_key: member for member in iter_member_descriptors(artifact, local)}
    blobs, metadata = [], []
    for key in sorted(local.keys()):
        match, member = _BLOB_KEY.fullmatch(key), declared.get(key)
        if match is None or member is None:
            metadata.append(key)
            continue
        if member.sha256 != "sha256:" + match[1]:
            raise PublicationError(f"Evidence blob {key} is not addressed by its content")
        blobs.append(key)

    def store(key: str) -> bool:
        """Create the blob if absent; ``True`` when the stored object already equals the local bytes."""
        stored = _head(client, bucket, f"{EVIDENCE_PREFIX}/{key}")
        if stored is None:
            _put_immutable(client, bucket, f"{EVIDENCE_PREFIX}/{key}", path / key, sha256=declared[key].sha256)
            return False
        identity = file_identity(path / key)
        if (stored["ContentLength"], stored.get("ETag")) == (identity["bytes"], identity["etag"]):
            return True
        logger.warning("Stored evidence blob {} differs from its expected identity; reading it back", key)
        return False

    with ThreadPoolExecutor(max_workers=EVIDENCE_WORKERS) as pool:
        trusted = {key for key, same in zip(blobs, pool.map(store, blobs), strict=True) if same}
    for key in metadata:
        _put_immutable(client, bucket, f"{prefix}/{key}", path / key)
    admit_artifact(
        _EvidenceMembers(client, bucket, prefix, local, set(blobs), trusted),
        expected_pin=artifact.pin,
    )


def publish_generation(directory: Path, *, client, bucket: str, prior_index: Mapping,
                       evidence_directories: tuple[Path, ...] = (), added_tables: frozenset[str] = frozenset()) -> dict:
    """Verify/upload/verify, then compare-and-swap the publication pointer.

    Validation and conditional-write refusals preserve the current pointer.
    A transport failure after the pointer request can have an uncertain result;
    reread the index to establish whether the complete generation is current.
    ``added_tables`` is an explicit migration: the family's table set may grow by
    exactly those tables, and never shrink. Unreferenced immutable uploads may
    remain and are safe to reuse. A pointer
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
        evidence_directories=evidence_directories, added_tables=added_tables,
        upload_member=lambda prefix, key: _put_immutable(client, bucket, prefix + "/" + key, directory / key),
    )


def _publish_verified_generation(
    artifact, source, *, upload_member, client, bucket: str, prior_index: Mapping,
    evidence_directories: tuple[Path, ...] = (), added_tables: frozenset[str] = frozenset(),
) -> dict:
    """Shared publication gates; both callers fully verify their source first."""
    from botocore.exceptions import BotoCoreError, ClientError
    from rulespec_artifacts import admit_artifact, iter_member_descriptors
    from spicy_regs.sources.r2 import _assert_upload_safe, _get_remote_size
    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, verify_evidence

    if artifact.root["spec"]["publicationStatus"] != "complete-family":
        raise PublicationError("A local partial candidate cannot be published")
    family = artifact.root["spec"]["family"]
    if not _NAME.fullmatch(family):
        raise PublicationError("Invalid family name")
    index, etag, bootstrap = _stored_index(client, bucket)
    _assert_family_unchanged(index, prior_index, family)
    prefix = f"generations/{family}/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    members = list(iter_member_descriptors(artifact, source))
    try:
        tables = table_entries(artifact.root["spec"]["tables"], members)
    except ValueError as exc:
        raise PublicationError(str(exc)) from exc
    # The guards run per table: a partition of a split table may legitimately shrink, but none may vanish,
    # since a build that forgot a partition would otherwise pass whenever the rest outweighs the shrink ratio.
    for key, table in tables.items():
        prior = table_descriptor(index, key)
        if missing := _partitions(prior) - _partitions(table):
            raise PublicationError(f"{key} lacks partitions its prior generation holds: "
                                   + ", ".join("/".join(f"{c}={v}" for c, v in p) for p in sorted(missing)))
        old_size = prior["byteSize"] if prior else _get_remote_size(client, bucket, key)
        _assert_upload_safe(table["byteSize"], old_size, key)
    old_family = index["families"].get(family)
    # The table set may change only by an explicit migration: exactly the declared added tables join, none leave.
    # A union, so the declaration is inert once those tables are published.
    if old_family is not None and set(tables) != set(old_family["tables"]) | set(added_tables):
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
    # A split table's members are compared one by one, so an untouched partition is copied, never re-uploaded.
    prior_members = {
        member["key"]: (member["sha256"], member["byteSize"])
        for key, table in (old_family or {}).get("tables", {}).items()
        for member in table.get("members", [{"key": key, **table}])
    }
    prior_prefix = old_family.get("prefix") if old_family is not None else None
    reused = 0
    current = {member.object_key: (member.sha256, member.byte_size) for member in members}
    for key in sorted(source.keys()):
        if prior_prefix is not None and key in current and prior_members.get(key) == current[key]:
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
    # Version 2 is the pointer; version 1 is derived from it afterwards (multi-file design §4.1).
    # Once version 2 is written the generation is published: a failed version-1 write is logged, not raised, and
    # the next publish, or retention before it deletes anything, derives version 1 again. A writer that first read
    # the index while version 2 was absent keeps that bootstrap read, so ``_write_v1`` can fold what a writer
    # predating version 2 published meanwhile.
    for _ in range(_POINTER_ATTEMPTS):
        updated, raw = _merge_family(index, family, entry)
        if _put_pointer(client, bucket, INDEX_V2_KEY, raw, etag):
            try:
                _write_v1(client, bucket, bootstrap)
            except (BotoCoreError, ClientError, OSError, PublicationError) as exc:
                logger.error("publication: {} is behind {}; the next publish rederives it ({}: {})",
                             INDEX_KEY, INDEX_V2_KEY, type(exc).__name__, exc)
            return updated
        logger.info("publication: pointer moved concurrently; merging {} onto the reread index", family)
        index, etag, _ = _stored_index(client, bucket)
        _assert_family_unchanged(index, prior_index, family)
    raise PublicationError("Publication changed concurrently; retry from a fresh snapshot")


def _partitions(table: Mapping | None) -> set[tuple[tuple[str, str], ...]]:
    """The partitions a split table's members hold, as ``(column, value)`` tuples; none for a single file."""
    return {tuple(member["partition"].items()) for member in (table or {}).get("members", ())}


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
