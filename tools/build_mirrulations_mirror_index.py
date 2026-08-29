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

#: The fetch phase refuses a draw containing anything larger than this; a
#: rendition that big must become an explicit policy decision, not disappear
#: silently from the sealed index.
MAX_OBJECT_BYTES = 64 * 1024 * 1024

INDEX_SCHEMA_VERSION = "mirrulations-mirror-index-v1"

#: The mirror names a rendition `<documentId>_content.<ext>` and an attachment
#: `<documentId>_attachment_<n>.<ext>`. The non-greedy head stops at the first
#: role marker rather than the first underscore, because a document identifier
#: may hold underscores of its own (`EPA_FRDOC_0001-11807`).
RENDITION_NAME_RE = re.compile(r"^(?P<document_id>.+?)_(?:content|attachment)[._]")


class MirrorIndexError(RuntimeError):
    """The mirror index cannot prove that it covers the complete frozen draw."""


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
    failures: list[tuple[str, str, str]] = []
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
        except Exception as error:  # noqa: BLE001 - report every failed docket after the parallel pass
            with lock:
                failures.append((agency, docket, str(error)))
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

    if failures:
        examples = ", ".join(f"{agency}/{docket}" for agency, docket, _ in failures[:5])
        raise MirrorIndexError(
            f"draw is incomplete: {len(failures):,} of {len(dockets):,} docket listings failed"
            f" (first failures: {examples})"
        )

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


def _already_fetched(receipts: Path, expected: Mapping[str, tuple[str, int]] | None = None) -> set[str]:
    if not receipts.exists():
        return set()
    done: set[str] = set()
    with receipts.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if _valid_success_receipt(record):
                key = record["key"]
                if expected is None or expected.get(key) == (record["documentId"], record["size"]):
                    done.add(key)
    return done


def _valid_success_receipt(record: Any) -> bool:
    """Return whether one JSON line proves the bytes for one drawn object."""

    return (
        isinstance(record, Mapping)
        and isinstance(record.get("documentId"), str)
        and bool(record["documentId"])
        and isinstance(record.get("key"), str)
        and bool(record["key"])
        and isinstance(record.get("sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", record["sha256"]) is not None
        and isinstance(record.get("size"), int)
        and not isinstance(record["size"], bool)
        and record["size"] >= 0
    )


def _drawn_objects(manifest: Any) -> list[tuple[str, Mapping[str, Any]]]:
    """Validate and flatten the object set frozen by ``draw``."""

    if not isinstance(manifest, Mapping) or manifest.get("schemaVersion") != INDEX_SCHEMA_VERSION:
        raise MirrorIndexError(f"draw is not a {INDEX_SCHEMA_VERSION} manifest")
    documents = manifest.get("documents")
    if not isinstance(documents, Mapping):
        raise MirrorIndexError("draw.documents must be an object")

    flattened: list[tuple[str, Mapping[str, Any]]] = []
    seen: set[str] = set()
    for document_id, records in documents.items():
        if not isinstance(document_id, str) or not document_id or not isinstance(records, list):
            raise MirrorIndexError("draw.documents must map document identifiers to object arrays")
        for record in records:
            if not isinstance(record, Mapping):
                raise MirrorIndexError(f"draw record for {document_id} is not an object")
            key = record.get("key")
            size = record.get("size")
            etag = record.get("etag")
            if not isinstance(key, str) or not key:
                raise MirrorIndexError(f"draw record for {document_id} has no key")
            if key in seen:
                raise MirrorIndexError(f"draw contains object key twice: {key}")
            if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                raise MirrorIndexError(f"draw record for {key} has an invalid size")
            if not isinstance(etag, str) or not etag:
                raise MirrorIndexError(f"draw record for {key} has no ETag")
            seen.add(key)
            flattened.append((document_id, record))
    return flattened


def fetch(draw_path: Path, receipts: Path, *, workers: int) -> Path:
    """Download every drawn object pinned to its ETag and digest the bytes."""

    manifest = json.loads(draw_path.read_text(encoding="utf-8"))
    pending = _drawn_objects(manifest)
    oversized = [(document_id, record) for document_id, record in pending if record["size"] > MAX_OBJECT_BYTES]
    if oversized:
        first_key = oversized[0][1]["key"]
        raise MirrorIndexError(
            f"fetch refuses {len(oversized):,} drawn objects larger than {MAX_OBJECT_BYTES:,} bytes; first: {first_key}"
        )

    from spicy_regs.sources.mirrulations import BUCKET, download_object_bytes, s3_resource

    expected = {record["key"]: (document_id, record["size"]) for document_id, record in pending}
    done = _already_fetched(receipts, expected)
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
    if failed:
        raise MirrorIndexError(f"fetch is incomplete: {failed:,} objects failed and remain eligible for retry")
    return receipts


def seal(draw_path: Path, receipts: Path, output: Path) -> Path:
    """Fold the verified lines into one canonical index and digest it."""

    manifest = json.loads(draw_path.read_text(encoding="utf-8"))
    drawn = _drawn_objects(manifest)
    expected = {record["key"]: (document_id, record) for document_id, record in drawn}
    verified: dict[str, dict[str, Any]] = {}
    receipt_errors: set[str] = set()
    problems: list[str] = []
    with receipts.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                record = json.loads(line)
            except ValueError:
                problems.append(f"receipt line {line_number} is not JSON")
                continue
            if not isinstance(record, Mapping) or not isinstance(record.get("key"), str):
                problems.append(f"receipt line {line_number} has no object key")
                continue
            key = record["key"]
            if key not in expected:
                problems.append(f"receipt line {line_number} names an object outside the draw: {key}")
                continue
            if not _valid_success_receipt(record):
                receipt_errors.add(key)
                continue
            if key in verified:
                problems.append(f"receipt contains more than one successful record for {key}")
                continue
            document_id, drawn_record = expected[key]
            if record["documentId"] != document_id:
                problems.append(f"receipt assigns {key} to {record['documentId']}, not {document_id}")
                continue
            if record["size"] != drawn_record["size"]:
                problems.append(
                    f"receipt size for {key} is {record['size']:,}, not the drawn size {drawn_record['size']:,}"
                )
                continue
            verified[key] = dict(record)

    missing = sorted(set(expected) - set(verified))
    unresolved_errors = sorted(receipt_errors - set(verified))
    if missing:
        problems.append(f"{len(missing):,} drawn objects have no valid successful receipt; first: {missing[0]}")
    if unresolved_errors:
        problems.append(f"{len(unresolved_errors):,} drawn objects have unresolved error receipts")
    if problems:
        raise MirrorIndexError("seal refuses an incomplete or inconsistent receipt set: " + "; ".join(problems[:8]))

    documents: dict[str, list[dict[str, Any]]] = {}
    for document_id, records in manifest["documents"].items():
        entries = []
        for record in records:
            confirmed = verified[record["key"]]
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
    print(f"seal: {len(documents):,} documents, {objects:,} verified renditions")
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


__all__ = ["INDEX_SCHEMA_VERSION", "MirrorIndexError", "draw", "fetch", "index_digest", "read_mirror_index", "seal"]


if __name__ == "__main__":  # pragma: no cover - console entry point
    sys.exit(main())
