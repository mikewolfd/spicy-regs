"""Describe separately published files without reading their record bodies.

Publication receipts own membership and file identity. Exact Parquet footers
own schemas. A schema already verified for the same file identity is reusable;
mutable comment URLs still require their receipt's ETag and byte size.
"""
from __future__ import annotations

import hashlib
import io
import re
import struct

import httpx
import pyarrow.parquet as pq

from spicy_regs.explorer_metadata import canonical_bytes, published_tables
from spicy_regs.native_types import described_schema
from spicy_regs.sources.publication import load_comments_publication, load_rulemaking_snapshot

FOOTER_LIMIT = 64 * 1024 * 1024
_DIGEST = re.compile(r"(?:sha256:)?([a-f0-9]{64})\Z")


def file_identity(kind: str, family: str, path: str, record: dict, *, snapshot_id: str | None = None) -> dict:
    """Normalize one pointer-owned file for the publisher and browser to compare."""
    match = _DIGEST.fullmatch(record.get("sha256", ""))
    if not match or any(type(record.get(k)) is not int or record[k] < 0 for k in ("bytes", "rows")):
        raise ValueError(f"Invalid public file identity: {path}")
    member = {"path": path, "sha256": "sha256:" + match[1],
              "byteSize": record["bytes"], "rows": record["rows"]}
    if kind == "comments":
        if not isinstance(record.get("etag"), str) or not record["etag"]:
            raise ValueError("Mutable comments require an object ETag")
        member["etag"] = record["etag"]
    identity = {"kind": kind, "family": family, "members": [member]}
    if snapshot_id is not None:
        identity["snapshotId"] = snapshot_id
    return identity


def identity_digest(identity: dict) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(identity)).hexdigest()


def _version(client, url: str, member: dict) -> str:
    response = client.head(url, headers={"Cache-Control": "no-cache"})
    response.raise_for_status()
    etag = response.headers.get("etag")
    if (not etag or int(response.headers.get("content-length", -1)) != member["byteSize"]
            or member.get("etag", etag) != etag):
        raise ValueError(f"Public file version differs from its publication: {member['path']}")
    return etag


def _range(client, url: str, start: int, end: int, total: int, etag: str) -> bytes:
    headers = {"Range": f"bytes={start}-{end}", "If-Match": etag, "Accept-Encoding": "identity"}
    with client.stream("GET", url, headers=headers) as response:
        response.raise_for_status()
        if (response.status_code != 206 or response.headers.get("content-range") != f"bytes {start}-{end}/{total}"
                or response.headers.get("etag") != etag):
            raise ValueError("Parquet schema read did not return the exact pinned byte range")
        parts, size = [], 0
        for part in response.iter_bytes():
            size += len(part)
            if size > end - start + 1:
                raise ValueError("Parquet schema response exceeds the requested range")
            parts.append(part)
        if size != end - start + 1:
            raise ValueError("Parquet schema response is truncated")
    return b"".join(parts)


def parquet_schema(client, base_url: str, member: dict) -> list[list[str]]:
    """Inspect only the footer; refuse object changes, ignored ranges and wrong counts."""
    total = member["byteSize"]
    if total < 12:
        raise ValueError("Public Parquet file is too short")
    url = base_url.rstrip("/") + "/" + member["path"]
    etag = _version(client, url, member)
    tail = _range(client, url, total - 8, total - 1, total, etag)
    footer_size = struct.unpack("<I", tail[:4])[0]
    if tail[4:] != b"PAR1" or not 0 < footer_size <= min(FOOTER_LIMIT, total - 12):
        raise ValueError("Public Parquet footer is invalid or exceeds its reader limit")
    footer = _range(client, url, total - 8 - footer_size, total - 1, total, etag)
    with pq.ParquetFile(io.BytesIO(b"PAR1" + footer)) as parquet:
        if parquet.metadata.num_rows != member["rows"]:
            raise ValueError("Parquet footer rows differ from the publication")
        columns = [list(pair) for pair in described_schema(parquet.schema_arrow)]
    if _version(client, url, member) != etag:
        raise ValueError("Public Parquet changed while its schema was read")
    return columns


def other_tables(index: dict, base_url: str, *, read=None, previous: dict | None = None, client=None) -> dict:
    """Current separate publications, with main-index precedence and exact schema reuse."""
    core = published_tables(index)
    snapshot = load_rulemaking_snapshot(base_url, read=read)
    comments = load_comments_publication(base_url, read=read)
    identities = {}
    for filename, record in (snapshot or {}).get("tables", {}).items():
        name = filename.removesuffix(".parquet")
        if name not in core:
            assert snapshot is not None
            identities[name] = file_identity("rulemaking", "rulemaking", record["remote_key"], record,
                                             snapshot_id=snapshot["snapshot_id"])
    for name in ("comments", "comments_index"):
        if comments and name not in core:
            identities[name] = file_identity("comments", "comments", name + ".parquet",
                                             comments["receipt"]["files"][name + ".parquet"])
    if client is None:
        with httpx.Client(timeout=60, follow_redirects=True) as http:
            return _describe(identities, base_url, previous or {}, http)
    return _describe(identities, base_url, previous or {}, client)


def _describe(identities: dict, base_url: str, previous: dict, client) -> dict:
    result = {}
    if (not isinstance(previous, dict) or previous.get("format") != "spicy-regs-explorer-metadata"
            or previous.get("version") != 1):
        previous = {}
    cached_tables = previous.get("extra_tables", {})
    if not isinstance(cached_tables, dict):
        cached_tables = {}
    for name, identity in sorted(identities.items()):
        digest = identity_digest(identity)
        cached = cached_tables.get(name, {})
        if not isinstance(cached, dict):
            cached = {}
        columns = cached.get("publicationSchema")
        metadata_tables = previous.get("tables", {})
        metadata = metadata_tables.get(name, {}) if isinstance(metadata_tables, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        if (cached.get("publicationIdentity") == digest and cached.get("descriptor") == identity
                and metadata.get("publicationIdentity") == digest and metadata.get("publicationSchema") == columns
                and metadata.get("family") == identity["family"] == cached.get("family")
                and isinstance(columns, list) and columns
                and all(isinstance(c, list) and len(c) == 2 and all(isinstance(v, str) and v for v in c) for c in columns)
                and len({c[0] for c in columns}) == len(columns)):
            if identity["kind"] == "comments":
                member = identity["members"][0]
                _version(client, base_url.rstrip("/") + "/" + member["path"], member)
        else:
            columns = parquet_schema(client, base_url, identity["members"][0])
        result[name] = {"family": identity["family"], "publicationSchema": columns,
                        "publicationIdentity": digest, "descriptor": identity}
    return result
