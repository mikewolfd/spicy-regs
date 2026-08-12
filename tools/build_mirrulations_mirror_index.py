#!/usr/bin/env python
"""Freeze, fetch, and verify the Mirrulations renditions behind a document set.

The published catalog states a `file_url` for 2.5% of its rows. The mirror
holds a rendition for most of the rest, but a locator is only worth carrying if
the producer knows the bytes it serves — `CandidateRendition.expectedSha256` is
never a guess. So this tool does what
`corpora/mirrulations_document_corpus.py` does for a bounded draw, at the scale
of a publication window, in three phases that each leave a resumable artifact:

`draw`
    List `raw-data/<agency>/<docket>/text-<docket>/documents/` once per docket
    and record every non-JSON object whose name belongs to a document in the
    input set, with the exact `size`, `etag`, and `lastModified` the listing
    reported. Nothing is fetched. The result is an immutable statement of what
    the mirror held at that moment.

`fetch`
    Download each drawn object pinned to its listed ETag, so the mirror
    replacing an object after the listing makes S3 refuse the GET rather than
    hand back different bytes under the old key, and record the SHA-256 of the
    bytes actually read. Appends one JSON line per object, so an interrupted
    run resumes instead of restarting.

`seal`
    Fold the verified lines into one canonical-JSON index keyed by document
    identifier and print its digest. That digest is what a universe pins as
    the mirror's `sourceSystemVersion`: it names these exact bytes at these
    exact keys, and any drift moves it.

The index is derived data and lives under `output/`; the universe records only
its digest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import threading
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Objects larger than this are recorded but not fetched; a rendition that big
#: is not something this producer should pull into a listing pass by surprise.
MAX_OBJECT_BYTES = 64 * 1024 * 1024

INDEX_SCHEMA_VERSION = "mirrulations-mirror-index-v1"

#: The mirror names a rendition `<documentId>_content.<ext>` and an attachment
#: `<documentId>_attachment_<n>.<ext>`. The non-greedy head stops at the first
#: role marker rather than the first underscore, because a document identifier
#: may hold underscores of its own (`EPA_FRDOC_0001-11807`).
RENDITION_NAME_RE = re.compile(r"^(?P<document_id>.+?)_(?:content|attachment)[._]")


def _canonical_bytes(value: Any) -> bytes:
    from spicy_regs.document_release_v3 import canonical_json_bytes

    return canonical_json_bytes(value)


def _document_ids(catalog: Path, window: tuple[str, str]) -> dict[str, tuple[str, str]]:
    """Return ``{documentId: (agencyCode, docketId)}`` for the window."""

    import duckdb

    connection = duckdb.connect()
    rows = connection.execute(
        f"""
        SELECT document_id, agency_code, docket_id
        FROM read_parquet('{catalog}')
        WHERE substr(posted_date, 1, 10) BETWEEN ? AND ?
          AND docket_id IS NOT NULL AND agency_code IS NOT NULL
        """,
        [window[0], window[1]],
    ).fetchall()
    return {str(row[0]): (str(row[1]), str(row[2])) for row in rows}


def draw(catalog: Path, window: tuple[str, str], output: Path, *, workers: int) -> Path:
    """List every docket in the window and freeze the rendition objects."""

    from spicy_regs.sources.mirrulations import BUCKET, PREFIX, s3_client

    wanted = _document_ids(catalog, window)
    dockets = sorted({(agency, docket) for agency, docket in wanted.values()})
    print(f"draw: {len(wanted):,} documents across {len(dockets):,} dockets", flush=True)

    client = s3_client()
    found: dict[str, list[dict[str, Any]]] = {}
    lock = threading.Lock()
    listed = 0

    def one(pair: tuple[str, str]) -> None:
        nonlocal listed
        agency, docket = pair
        prefix = f"{PREFIX}/{agency}/{docket}/text-{docket}/documents/"
        objects: list[tuple[str, dict[str, Any]]] = []
        try:
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
                for entry in page.get("Contents", ()):
                    key = str(entry["Key"])
                    name = key.rsplit("/", 1)[-1]
                    if name.endswith(".json"):
                        continue
                    # `<documentId>_content.htm`, `<documentId>_attachment_1.pdf`.
                    # The identifier itself may hold underscores — `EPA_FRDOC_0001-11807`
                    # is one document id — so the split is on the first role
                    # marker, never on the first underscore.
                    matched = RENDITION_NAME_RE.match(name)
                    if matched is None:
                        continue
                    document_id = matched.group("document_id")
                    if document_id not in wanted:
                        continue
                    objects.append(
                        (
                            document_id,
                            {
                                # Verbatim, quotes and all: `download_object_bytes`
                                # compares it against the response's own ETag, and
                                # `mirrulations_document_corpus` records it the same
                                # way. Normalizing it here would break both.
                                "etag": str(entry["ETag"]),
                                "key": key,
                                "lastModified": entry["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "size": int(entry["Size"]),
                            },
                        )
                    )
        except Exception as error:  # noqa: BLE001 - one unreachable docket must not end the pass
            with lock:
                print(f"draw: {agency}/{docket} listing failed: {error}", flush=True)
            return
        with lock:
            for document_id, record in objects:
                found.setdefault(document_id, []).append(record)
            listed += 1
            if listed % 5000 == 0:
                print(f"draw: {listed:,}/{len(dockets):,} dockets, {len(found):,} documents", flush=True)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, dockets))

    manifest = {
        "documents": {
            document_id: sorted(records, key=lambda record: record["key"])
            for document_id, records in sorted(found.items())
        },
        "schemaVersion": INDEX_SCHEMA_VERSION,
        "source": {"bucket": "mirrulations", "prefix": "raw-data"},
        "window": {"from": window[0], "to": window[1]},
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(_canonical_bytes(manifest))
    objects = sum(len(records) for records in found.values())
    print(f"draw: {len(found):,} documents, {objects:,} rendition objects -> {output}", flush=True)
    return output


def _already_fetched(receipts: Path) -> set[str]:
    if not receipts.exists():
        return set()
    done: set[str] = set()
    with receipts.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                done.add(str(json.loads(line)["key"]))
            except (ValueError, KeyError):
                continue
    return done


def fetch(draw_path: Path, receipts: Path, *, workers: int) -> Path:
    """Download every drawn object pinned to its ETag and digest the bytes."""

    from spicy_regs.sources.mirrulations import BUCKET, download_object_bytes, s3_resource

    manifest = json.loads(draw_path.read_text(encoding="utf-8"))
    pending = [
        (document_id, record)
        for document_id, records in manifest["documents"].items()
        for record in records
        if record["size"] <= MAX_OBJECT_BYTES
    ]
    done = _already_fetched(receipts)
    pending = [item for item in pending if item[1]["key"] not in done]
    print(f"fetch: {len(pending):,} objects outstanding ({len(done):,} already verified)", flush=True)

    resource = s3_resource()
    lock = threading.Lock()
    receipts.parent.mkdir(parents=True, exist_ok=True)
    stream = receipts.open("a", encoding="utf-8")
    completed = 0
    failed = 0

    def one(item: tuple[str, Mapping[str, Any]]) -> None:
        nonlocal completed, failed
        document_id, record = item
        try:
            downloaded = download_object_bytes(
                resource, BUCKET, record["key"], if_match=record["etag"], max_bytes=MAX_OBJECT_BYTES
            )
            payload = downloaded.content
        except Exception as error:  # noqa: BLE001 - a lost object is recorded, not fatal
            with lock:
                failed += 1
                stream.write(
                    json.dumps({"error": str(error)[:200], "key": record["key"], "documentId": document_id}) + "\n"
                )
            return
        digest = hashlib.sha256(payload).hexdigest()
        with lock:
            completed += 1
            stream.write(
                json.dumps(
                    {
                        "documentId": document_id,
                        "key": record["key"],
                        "sha256": digest,
                        "size": len(payload),
                    }
                )
                + "\n"
            )
            if completed % 5000 == 0:
                stream.flush()
                print(f"fetch: {completed:,}/{len(pending):,} verified, {failed:,} failed", flush=True)

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(one, pending))
    finally:
        stream.close()
    print(f"fetch: {completed:,} verified, {failed:,} failed -> {receipts}", flush=True)
    return receipts


def seal(draw_path: Path, receipts: Path, output: Path) -> Path:
    """Fold the verified lines into one canonical index and digest it."""

    manifest = json.loads(draw_path.read_text(encoding="utf-8"))
    verified: dict[str, dict[str, Any]] = {}
    errors = 0
    with receipts.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if "sha256" not in record:
                errors += 1
                continue
            verified[str(record["key"])] = record

    documents: dict[str, list[dict[str, Any]]] = {}
    for document_id, records in manifest["documents"].items():
        entries = []
        for record in records:
            confirmed = verified.get(record["key"])
            if confirmed is None:
                continue
            entries.append(
                {
                    "key": record["key"],
                    "sha256": f"sha256:{confirmed['sha256']}",
                    "size": int(confirmed["size"]),
                }
            )
        if entries:
            documents[document_id] = sorted(entries, key=lambda entry: entry["key"])

    index = {
        "documents": dict(sorted(documents.items())),
        "schemaVersion": INDEX_SCHEMA_VERSION,
        "source": manifest["source"],
        "window": manifest["window"],
    }
    payload = _canonical_bytes(index)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    objects = sum(len(entries) for entries in documents.values())
    print(f"seal: {len(documents):,} documents, {objects:,} verified renditions, {errors:,} unreadable")
    print(f"seal: {output} ({len(payload):,} bytes)")
    print(f"sourceSystemVersion sha256:{digest}")
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("phase", choices=("draw", "fetch", "seal", "all"))
    parser.add_argument("--catalog", type=Path, required=False)
    parser.add_argument("--from", dest="window_from", default="2021-01-01")
    parser.add_argument("--to", dest="window_to", default="2025-12-31")
    parser.add_argument("--work-dir", type=Path, default=REPO_ROOT / "output/mirrulations-mirror-index")
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args(argv)

    draw_path = args.work_dir / "mirror-draw.json"
    receipts = args.work_dir / "mirror-fetch.jsonl"
    index = args.work_dir / "mirror-index.json"
    window = (args.window_from, args.window_to)

    if args.phase in ("draw", "all"):
        if args.catalog is None:
            parser.error("--catalog is required for the draw phase")
        draw(args.catalog, window, draw_path, workers=args.workers)
    if args.phase in ("fetch", "all"):
        fetch(draw_path, receipts, workers=args.workers)
    if args.phase in ("seal", "all"):
        seal(draw_path, receipts, index)
    return 0


def read_mirror_index(path: Path | str) -> dict[str, list[dict[str, Any]]]:
    """Read one sealed index as ``{documentId: [{key, sha256, size}, ...]}``."""

    document = json.loads(Path(path).read_text(encoding="utf-8"))
    documents = document.get("documents")
    if document.get("schemaVersion") != INDEX_SCHEMA_VERSION or not isinstance(documents, dict):
        raise ValueError(f"not a {INDEX_SCHEMA_VERSION} index: {path}")
    return documents


def index_digest(path: Path | str) -> str:
    return f"sha256:{hashlib.sha256(Path(path).read_bytes()).hexdigest()}"


__all__ = ["INDEX_SCHEMA_VERSION", "draw", "fetch", "index_digest", "read_mirror_index", "seal"]


if __name__ == "__main__":  # pragma: no cover - console entry point
    sys.exit(main())
