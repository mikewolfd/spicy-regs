"""Exchange committed hosted acquisitions, separately from table publication.

Receipts retain the central archive's content-addressed layout. Originals use
the existing source-evidence CAS: a successful generation, a refusal and a
probe can all refer to the same bytes without storing another R2 copy.
"""

import hashlib
import json
import re
from collections.abc import Iterator
from pathlib import Path

from rulespec_artifacts import canonical_json_bytes
from spicy_docs.storage.raw_archive import ACQUISITION_FORMAT, ACQUISITION_PREFIX, CentralRawArchiveStore
from spicy_docs.storage.raw_archive import iter_acquisition_receipts

from spicy_regs.sources import publication, r2

HOSTED_PREFIX = "raw-source-originals/"
_RECEIPTS = HOSTED_PREFIX + ACQUISITION_PREFIX + "/"
_RECEIPT_LIMIT = 16 * 1024 * 1024
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_CHUNK = 1024 * 1024


def _blob_key(digest: str) -> str:
    return f"{publication.EVIDENCE_PREFIX}/blobs/sha256/{digest}"


def _persist(client, bucket: str, key: str, open_body, size: int, digest: str) -> None:
    """Create only absent members; independently check pre-existing and uploaded bytes."""
    head = publication._head(client, bucket, key)
    if head is None:
        publication._put_immutable_stream(client, bucket, key, open_body, size, sha256=digest)
    else:
        with open_body() as stream:
            local = publication._stream_identity(stream)
        if (local["bytes"], local["etag"]) == (head["ContentLength"], head.get("ETag")):
            return
    if not publication._stored_bytes_match(client, bucket, key, expected_size=size, expected_sha256=digest):
        raise publication.PublicationError(f"Hosted acquisition member differs from its pin: {key}")


def persist_hosted_receipts(root: Path, *, client, bucket: str) -> int:
    """Commit receipts only after every referenced original is in the shared CAS."""
    if not root.exists():
        return 0
    store = CentralRawArchiveStore(root, create=False)
    originals: set[str] = set()
    count = 0
    for path, document in iter_acquisition_receipts(store.root):
        for obj in document["objects"]:
            digest = obj["sha256"]
            if digest not in originals:
                _persist(client, bucket, _blob_key(digest), lambda: store.open(digest), obj["bytes"], "sha256:" + digest)
                originals.add(digest)
        relative = document["receipt_object"]
        _persist(client, bucket, HOSTED_PREFIX + relative, lambda: store.open_member(relative),
                 path.stat().st_size, "sha256:" + path.name)
        count += 1
    return count


def _receipt_document(client, bucket: str, key: str) -> dict:
    digest = key.removeprefix(_RECEIPTS)
    if len(digest) != 67 or not _SHA.fullmatch(digest[3:]) or digest[:3] != digest[3:5] + "/":
        raise ValueError(f"Noncanonical hosted acquisition key: {key}")
    obj = client.get_object(Bucket=bucket, Key=key)
    with obj["Body"] as stream:
        raw = stream.read(_RECEIPT_LIMIT + 1)
    if len(raw) > _RECEIPT_LIMIT or hashlib.sha256(raw).hexdigest() != digest[3:]:
        raise ValueError(f"Hosted acquisition receipt differs from its pin: {key}")
    document = json.loads(raw)
    if (
        not isinstance(document, dict)
        or document.get("format") != ACQUISITION_FORMAT
        or set(document) != {"format", "files", "objects", "receipt"}
        or not isinstance(document["receipt"], dict)
        or not isinstance(document["objects"], list)
        or not isinstance(document["files"], list)
        or canonical_json_bytes(document) != raw
    ):
        raise ValueError(f"Invalid hosted acquisition receipt: {key}")
    return document


def _originals(document: dict) -> dict[str, int]:
    """Validate closure and the original/storage identities before downloading."""
    originals = {}
    descriptors = {}
    for item in document["objects"]:
        digest, size, encoding = item["sha256"], item["bytes"], item["encoding"]
        if not _SHA.fullmatch(digest) or digest in originals or type(size) is not int or size < 0:
            raise ValueError("Invalid hosted original identity")
        key = f"objects/sha256/{digest[:2]}/{digest}"
        if encoding == "gzip":
            key += ".gz"
        if (
            encoding not in {"raw", "gzip"}
            or item["object"] != key
            or not _SHA.fullmatch(item["stored_sha256"])
            or type(item["stored_bytes"]) is not int
            or item["stored_bytes"] < 0
            or (encoding == "raw" and (item["stored_bytes"], item["stored_sha256"]) != (size, digest))
        ):
            raise ValueError("Invalid hosted original storage descriptor")
        originals[digest] = size
        descriptors[digest] = (size, key)
    claimed = set()
    for item in document["files"]:
        digest = item["sha256"]
        if descriptors.get(digest) != (item["bytes"], item["object"]):
            raise ValueError("Hosted acquisition file lacks its matching original")
        claimed.add(digest)
    if claimed != originals.keys():
        raise ValueError("Hosted acquisition receipt has unclaimed originals")
    return originals


def _read_original(client, bucket: str, digest: str) -> Iterator[bytes]:
    result = client.get_object(Bucket=bucket, Key=_blob_key(digest))
    with result["Body"] as stream:
        while block := stream.read(_CHUNK):
            yield block


def import_hosted_receipts(root: Path, *, client, bucket: str) -> int:
    """Hydrate hosted receipts into the local archive before rebuilding the catalog.

    Restore missing originals once and verify them by content address. The
    maintained writer reuses an existing raw or lossless gzip original; the
    local receipt differs from the hosted key only if its storage descriptor
    needs to name that local encoding. Run IDs, outcomes, and file evidence
    are never rewritten. No table or publication pointer is touched.
    """
    store = CentralRawArchiveStore(root)
    checked: dict[str, int] = {}
    count = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=_RECEIPTS):
        for item in page.get("Contents", ()):
            document = _receipt_document(client, bucket, item["Key"])
            for digest, size in _originals(document).items():
                if digest in checked and checked[digest] != size:
                    raise ValueError("Conflicting hosted original sizes")
                if digest not in checked:
                    store.put_blob(digest, size, _read_original(client, bucket, digest))
                    checked[digest] = size
            store.record_receipt(files=document["files"], receipt=document["receipt"])
            count += 1
    return count


def hosted_connection():
    """Use explicit authenticated R2 settings; a refused listing is not an empty archive."""
    import os

    if not all(os.environ.get(key) for key in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME")):
        raise ValueError("Hosted acquisitions require R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_BUCKET_NAME")
    return r2.get_r2_client(), os.environ["R2_BUCKET_NAME"]


def main() -> None:
    """Internal workflow entry point for publishing the runner's raw archive."""
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    if not (args.root / ACQUISITION_PREFIX).exists():
        print("No committed hosted acquisition receipts")
        return
    client, bucket = hosted_connection()
    count = persist_hosted_receipts(args.root, client=client, bucket=bucket)
    print(f"Durable hosted acquisition receipts: {count}")


if __name__ == "__main__":
    main()
