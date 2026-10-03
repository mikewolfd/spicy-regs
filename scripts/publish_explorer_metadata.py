#!/usr/bin/env python3
"""Validate and optionally publish explorer metadata, never source data.

Without --publish this is a public, read-only build. Publishing reads the
authoritative index from R2, not a potentially cached HTTP response.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spicy_regs.explorer_metadata import KEY, build_bundle, canonical_bytes, public_url, same_metadata  # noqa: E402
from spicy_regs.public_url import resolve_r2_base_url  # noqa: E402
from spicy_regs.sources.publication import INDEX_LIMIT, INDEX_V2_KEY, parse_index, table_members  # noqa: E402

METADATA_LIMIT = 8 * 1024 * 1024
PUBLISHER_LIMIT = 4 * 1024 * 1024


def get_public(url: str, limit: int) -> bytes:
    import httpx
    with httpx.stream("GET", url, timeout=60, headers={"User-Agent": "spicy-regs-explorer-metadata/1"}) as response:
        response.raise_for_status()
        chunks, size = [], 0
        for chunk in response.iter_bytes():
            size += len(chunk)
            if size > limit:
                raise ValueError(f"Public metadata input exceeds {limit} bytes")
            chunks.append(chunk)
    return b"".join(chunks)


def scorecard_sources(index: dict, base_url: str) -> list[dict]:
    """Publisher names/links follow the current tiny table, never a copied list."""
    import pyarrow.parquet as pq
    key = "scorecard_publishers.parquet"
    if not any(key in f["tables"] for f in index["families"].values()):
        return []
    publishers = {}
    total = 0
    for member in table_members(index, key):
        raw = get_public(f"{base_url.rstrip('/')}/{member.path}", PUBLISHER_LIMIT)
        total += len(raw)
        if total > PUBLISHER_LIMIT:
            raise ValueError("Published scorecard publisher table exceeds metadata-reader limit")
        if member.byte_size is not None and len(raw) != member.byte_size:
            raise ValueError("Published scorecard publisher bytes differ from the publication size")
        if member.sha256 and member.sha256 != "sha256:" + hashlib.sha256(raw).hexdigest():
            raise ValueError("Published scorecard publisher bytes differ from the publication digest")
        table = pq.read_table(io.BytesIO(raw), columns=["publisher_id", "name", "scorecard_index_url", "homepage_url"])
        if table.num_rows > 10_000:
            raise ValueError("Unexpectedly large scorecard publisher table")
        for row in table.to_pylist():
            url = row.get("scorecard_index_url") or row.get("homepage_url")
            if not row.get("publisher_id") or not row.get("name") or not url or not public_url(url):
                raise ValueError("Scorecard publisher lacks a name, identifier or valid public URL")
            entry = {"id": "scorecard-" + row["publisher_id"], "name": row["name"], "url": url,
                     "kind": "publisher", "note": "Ratings express this publisher’s position; each scorecard names its publisher."}
            if entry["id"] in publishers and publishers[entry["id"]] != entry:
                raise ValueError("Conflicting scorecard publisher identity")
            publishers[entry["id"]] = entry
    return sorted(publishers.values(), key=lambda p: p["name"])


def read_object(client, bucket: str, key: str, limit: int, *, optional: bool = False) -> bytes | None:
    from botocore.exceptions import ClientError
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if optional and exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    try:
        raw = obj["Body"].read(limit + 1)
    finally:
        obj["Body"].close()
    if len(raw) > limit:
        raise ValueError(f"R2 metadata object exceeds {limit} bytes")
    return raw


def publish_bundle(client, bucket: str, bundle: dict) -> str:
    """Write one complete JSON object after validation; failures stay visible."""
    encoded = canonical_bytes(bundle)
    if len(encoded) > METADATA_LIMIT:
        raise ValueError("Explorer metadata exceeds its reader limit")
    before = read_object(client, bucket, KEY, METADATA_LIMIT, optional=True)
    if before and same_metadata(json.loads(before), bundle):
        return "unchanged"
    client.put_object(Bucket=bucket, Key=KEY, Body=encoded,
                      ContentType="application/json; charset=utf-8",
                      CacheControl="public, max-age=60, must-revalidate")
    if read_object(client, bucket, KEY, METADATA_LIMIT) != encoded:
        raise ValueError("R2 metadata readback differs from the validated bundle")
    return "published"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true", help="Write only explorer-metadata.v1.json to R2")
    parser.add_argument("--index", type=Path, help="Local version-2 publication index for an offline/read-only build")
    parser.add_argument("--output", type=Path, default=Path("output") / KEY)
    parser.add_argument("--base-url", default=resolve_r2_base_url())
    args = parser.parse_args()
    if args.publish and args.index:
        parser.error("--publish always reads the current authoritative R2 index; do not pass --index")
    if not public_url(args.base_url):
        parser.error("--base-url must be HTTPS")
    client = None
    bucket = None
    if args.publish:
        from spicy_regs.sources.r2 import get_r2_client, require_credentials
        require_credentials("publish explorer metadata")
        client, bucket = get_r2_client(), os.environ["R2_BUCKET_NAME"]
        raw = read_object(client, bucket, INDEX_V2_KEY, INDEX_LIMIT)
    else:
        raw = args.index.read_bytes() if args.index else get_public(args.base_url.rstrip("/") + "/" + INDEX_V2_KEY, INDEX_LIMIT)
    assert raw is not None
    index = parse_index(raw)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    bundle = build_bundle(index, scorecard_publishers=scorecard_sources(index, args.base_url), source_revision=revision)
    encoded = canonical_bytes(bundle)
    if len(encoded) > METADATA_LIMIT:
        raise ValueError("Explorer metadata exceeds its reader limit")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(encoded + b"\n")
    action = "validated"
    if client:
        assert bucket is not None
        action = publish_bundle(client, bucket, bundle)
        if action == "published":
            from spicy_regs.sources.cloudflare import purge_urls
            purge_urls([args.base_url.rstrip("/") + "/" + KEY])
    print(json.dumps({"action": action, "key": KEY, "tables": len(bundle["tables"]), "joins": len(bundle["joins"]),
                      "undocumentedTables": [n for n, t in bundle["tables"].items() if t["metadataStatus"] == "unknown"],
                      "undocumentedSources": [n for n, t in bundle["tables"].items() if t["sourceStatus"] == "unknown"],
                      "bytes": len(encoded), "sourceRevision": revision}))


if __name__ == "__main__":
    main()
