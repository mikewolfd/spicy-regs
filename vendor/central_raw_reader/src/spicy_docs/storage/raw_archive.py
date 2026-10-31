"""One raw-source archive: immutable original bytes and acquisition receipts.

The existing gzip indexes are a frozen base. New acquisitions append small,
content-addressed receipts after their originals are durable; readers replay
both. Catalog databases and run artifacts belong outside this directory.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import stat
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from pathlib import Path

from rulespec_artifacts import (
    BlobIntegrityError,
    BlobLimitError,
    LocalBlobWriter,
    LocalMemberSource,
    PinnedLocalDirectory,
    canonical_json_bytes,
    validate_object_key,
)

from spicy_docs.storage.blobs import SourceNativeBlobWrite
from spicy_docs.storage.publication import ImmutablePublicationError

ACQUISITION_FORMAT = "spicygov-acquisition-1"
ACQUISITION_PREFIX = "acquisitions/sha256"
_HEX = re.compile(r"[0-9a-f]{64}\Z")
_CHUNK = 1024 * 1024
_RECEIPT_LIMIT = 16 * 1024 * 1024


def default_raw_archive_root() -> Path:
    """Use the explicit archive setting or the shared local corpus location."""
    return Path(os.environ.get("SPICYGOV_RAW_ROOT", Path.home() / "Work/corpora/spicyregs/raw")).expanduser()


def _hex(value: str) -> str:
    if not isinstance(value, str) or not _HEX.fullmatch(value.removeprefix("sha256:")):
        raise ValueError("expected a SHA-256 identity")
    return value.removeprefix("sha256:")


def _size(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("original byte size must be a nonnegative integer")


def _state(value: os.stat_result) -> tuple[int, int, int, int]:
    # Concurrent identical publication may change ctime through hardlinks.
    # Device, inode, size and mtime are the byte-changing fields.
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


class CentralRawArchiveStore:
    """The source-native blob interface over the central raw archive layout.

    New originals use the maintained conditional blob writer. Existing gzip
    wrappers are decoded and verified before reuse. ``create=False`` is a
    read-only open: no directories or pending files are created. A receipt is
    the commit record; a crash before it leaves unclaimed, recoverable bytes.
    """

    def __init__(self, root: Path | None = None, *, create: bool = True) -> None:
        selected = Path(root) if root is not None else default_raw_archive_root()
        if create:
            selected.mkdir(parents=True, exist_ok=True)
        self._reader = LocalMemberSource(selected)
        self.root = self._reader.root
        root_stat = self.root.lstat()
        self._identity = root_stat.st_dev, root_stat.st_ino
        self._writable = create

    def _descriptor(self, key: str) -> int:
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            state = os.fstat(directory)
            if (state.st_dev, state.st_ino) != self._identity:
                raise BlobIntegrityError("raw archive root changed after opening")
            for part in key.split("/")[:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                os.close(directory)
                directory = child
            return os.open(key.split("/")[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        finally:
            os.close(directory)

    @contextmanager
    def open_member(self, object_key: str) -> Iterator[io.BufferedReader]:
        """Read an ordinary contained file, refusing symlinks and path changes.

        Descriptor-relative opens also refuse a raced-in FIFO without blocking.
        This adapter is needed for the archive's gzip storage representation.
        """
        key = validate_object_key(object_key, path="raw archive member")
        with os.fdopen(self._descriptor(key), "rb") as stream:
            assert isinstance(stream, io.BufferedReader)
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise BlobIntegrityError("raw archive member is not an ordinary file")
            yield stream
            after = os.fstat(stream.fileno())
            current_fd = self._descriptor(key)
            try:
                current = os.fstat(current_fd)
            finally:
                os.close(current_fd)
            if _state(before) != _state(after) or _state(after) != _state(current):
                raise BlobIntegrityError("raw archive member changed while being read")

    @contextmanager
    def _stored(self, blob_ref: str) -> Iterator[tuple[io.BufferedReader, str, str]]:
        digest = _hex(blob_ref)
        key = f"objects/sha256/{digest[:2]}/{digest}"
        with ExitStack() as stack:
            try:
                stream = stack.enter_context(self.open_member(key))
            except FileNotFoundError:
                key += ".gz"
                stream = stack.enter_context(self.open_member(key))
                encoding = "gzip"
            else:
                encoding = "raw"
            yield stream, key, encoding

    @contextmanager
    def open(self, blob_ref: str) -> Iterator[io.BufferedReader | gzip.GzipFile]:
        """Open original bytes; consumers requiring integrity exhaust a verified read."""
        with self._stored(blob_ref) as (stream, _key, encoding):
            if encoding == "gzip":
                with gzip.GzipFile(fileobj=stream, mode="rb") as original:
                    yield original
            else:
                yield stream

    def describe_blob(self, blob_ref: str, byte_size: int | None = None) -> dict:
        """Verify stored and original bytes and return a base-index-compatible row."""
        digest = _hex(blob_ref)
        if byte_size is not None:
            _size(byte_size)
        with self._stored(blob_ref) as (stored, key, encoding):
            stored_size = os.fstat(stored.fileno()).st_size
            if encoding == "raw" and byte_size is not None and stored_size != byte_size:
                raise BlobIntegrityError("raw archive object differs from its original identity")
            stored_digest = hashlib.file_digest(stored, "sha256").hexdigest()
            if encoding == "raw":
                size, observed = stored_size, stored_digest
            else:
                stored.seek(0)
                size, hasher = 0, hashlib.sha256()
                with gzip.GzipFile(fileobj=stored, mode="rb") as original:
                    while chunk := original.read(_CHUNK if byte_size is None else min(_CHUNK, byte_size - size + 1)):
                        size += len(chunk)
                        if byte_size is not None and size > byte_size:
                            raise BlobIntegrityError("decoded original exceeds its declared size")
                        hasher.update(chunk)
                observed = hasher.hexdigest()
            if observed != digest or (byte_size is not None and size != byte_size):
                raise BlobIntegrityError("raw archive object differs from its original identity")
            return {
                "sha256": digest,
                "bytes": size,
                "object": key,
                "encoding": encoding,
                "stored_sha256": stored_digest,
                "stored_bytes": stored_size,
            }

    def _writer(self, prefix: str) -> LocalBlobWriter:
        if not self._writable:
            raise ValueError("raw archive was opened read-only")
        return LocalBlobWriter(
            self.root, object_prefix=prefix, shard_digits=2, expected_root=self._identity, create=True
        )

    def put_blob(self, blob_ref: str, byte_size: int, chunks: Iterable[bytes]) -> SourceNativeBlobWrite:
        """Conditionally write original bytes, independently verifying any reuse."""
        digest = _hex(blob_ref)
        _size(byte_size)
        if not self._writable:
            raise ValueError("raw archive was opened read-only")
        try:
            self.describe_blob(digest, byte_size)
        except FileNotFoundError:
            pass
        else:
            return SourceNativeBlobWrite("sha256:" + digest, byte_size, True, 0)
        try:
            result = self._writer("objects/sha256").put(
                chunks, max_bytes=byte_size, expected_digest="sha256:" + digest, expected_size=byte_size
            )
        except (BlobIntegrityError, BlobLimitError) as error:
            raise ImmutablePublicationError(str(error)) from error
        return SourceNativeBlobWrite(result.digest, result.byte_size, result.reused, result.bytes_written)

    def record_receipt(self, *, files: Sequence[Mapping], receipt: Mapping) -> dict:
        """Commit original-source references only after their bytes are verified.

        Each file requires ``sha256`` and ``bytes``. Caller-supplied native IDs,
        source URLs, periods and provenance are retained without inference.
        An empty file list records an unsuccessful or non-retaining acquisition.
        """
        objects, normalized = {}, []
        for row in files:
            descriptor = self.describe_blob(row["sha256"], row["bytes"])
            objects[descriptor["sha256"]] = descriptor
            normalized.append({**row, "sha256": descriptor["sha256"], "object": descriptor["object"]})
        document = {
            "format": ACQUISITION_FORMAT,
            "objects": sorted(objects.values(), key=lambda item: item["sha256"]),
            "files": normalized,
            "receipt": dict(receipt),
        }
        payload = canonical_json_bytes(document)
        if len(payload) > _RECEIPT_LIMIT:
            raise ValueError("acquisition receipt exceeds its metadata size limit")
        result = self._writer(ACQUISITION_PREFIX).put((payload,), max_bytes=_RECEIPT_LIMIT)
        return {
            "sha256": result.digest.removeprefix("sha256:"),
            "bytes": result.byte_size,
            "object": result.object_key,
            "encoding": "raw",
            "stored_sha256": result.digest.removeprefix("sha256:"),
            "stored_bytes": result.byte_size,
        }


def _check_object(store: CentralRawArchiveStore, obj: dict, *, deep: bool) -> str:
    digest = _hex(obj["sha256"])
    _size(obj["bytes"])
    _size(obj["stored_bytes"])
    _hex(obj["stored_sha256"])
    expected = f"objects/sha256/{digest[:2]}/{digest}"
    if obj["encoding"] == "gzip":
        expected += ".gz"
    elif obj["encoding"] != "raw":
        raise ValueError("unsupported original storage encoding")
    if obj["object"] != expected:
        raise ValueError("invalid original object reference")
    if obj["encoding"] == "raw" and (obj["stored_sha256"] != digest or obj["stored_bytes"] != obj["bytes"]):
        raise BlobIntegrityError("raw object storage differs from original identity")
    if deep:
        if store.describe_blob(digest, obj["bytes"]) != obj:
            raise BlobIntegrityError("retained original differs from its receipt")
    else:
        with store.open_member(obj["object"]) as stream:
            if os.fstat(stream.fileno()).st_size != obj["stored_bytes"]:
                raise BlobIntegrityError("retained original stored size differs from its receipt")
    return digest


def iter_acquisition_receipts(root: Path, *, verify_objects: bool = False,
                              receipt_keys: Iterable[str] | None = None) -> Iterator[tuple[Path, dict]]:
    """Replay committed additions, checking receipt hashes and contained references.

    Yields (receipt path, document), with ``receipt_object`` added for provenance.
    An unfinished write has no receipt and therefore makes no acquisition claim.
    Explicit keys are validated before opening any receipt or referenced object;
    omitted keys retain full archive iteration for initial builds and audits.
    """
    selected = None
    if receipt_keys is not None:
        selected = tuple(receipt_keys)
        if (len(set(selected)) != len(selected)
                or any(not isinstance(key, str) or not re.fullmatch(
                    r"acquisitions/sha256/([0-9a-f]{2})/([0-9a-f]{64})", key)
                    or key.split("/")[-2] != key.split("/")[-1][:2] for key in selected)):
            raise ValueError("selected acquisition receipt keys must be unique canonical archive paths")
        if not selected:
            return
    store = CentralRawArchiveStore(root, create=False)
    try:
        descriptor = store._descriptor(ACQUISITION_PREFIX)
    except FileNotFoundError:
        if selected:
            raise
        return
    else:
        os.close(descriptor)
    source = PinnedLocalDirectory(store.root, expected_identity=store._identity).member_source(ACQUISITION_PREFIX)
    keys = source.keys() if selected is None else (key.removeprefix(ACQUISITION_PREFIX + "/") for key in sorted(selected))
    for relative in keys:
        parts = relative.split("/")
        if len(parts) != 2 or not _HEX.fullmatch(parts[-1]) or parts[0] != parts[-1][:2]:
            raise ValueError("unexpected acquisition receipt path")
        key = f"{ACQUISITION_PREFIX}/{relative}"
        with store.open_member(key) as stream:
            payload = stream.read(_RECEIPT_LIMIT + 1)
        if len(payload) > _RECEIPT_LIMIT or hashlib.sha256(payload).hexdigest() != parts[-1]:
            raise BlobIntegrityError("acquisition receipt differs from its immutable identity")
        document = json.loads(payload)
        if (
            not isinstance(document, dict)
            or document.get("format") != ACQUISITION_FORMAT
            or not isinstance(document.get("receipt"), dict)
            or not isinstance(document.get("files"), list)
            or not isinstance(document.get("objects"), list)
        ):
            raise ValueError("invalid acquisition receipt")
        objects = {}
        for obj in document["objects"]:
            digest = _check_object(store, obj, deep=verify_objects)
            if digest in objects:
                raise ValueError("duplicate original object reference")
            objects[digest] = obj
        claimed = set()
        for row in document["files"]:
            obj = objects.get(_hex(row["sha256"]))
            if obj is None or row["bytes"] != obj["bytes"] or row["object"] != obj["object"]:
                raise ValueError("acquisition file has no matching original object")
            claimed.add(row["sha256"])
        if claimed != set(objects):
            raise ValueError("acquisition receipt has unclaimed original objects")
        yield store.root / key, {**document, "receipt_object": key}


def iter_base_records(root: Path, index_name: str) -> Iterator[dict]:
    """Read a frozen base index only after checking its snapshot checksum."""
    store = CentralRawArchiveStore(root, create=False)
    with store.open_member("snapshot.json") as stream:
        snapshot = json.load(stream)
    if index_name not in snapshot["indexes"] or not index_name.endswith(".jsonl.gz"):
        raise ValueError("index is not selected by the raw archive snapshot")
    with store.open_member(index_name) as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != snapshot["indexes"][index_name]:
            raise BlobIntegrityError("raw archive index differs from its snapshot checksum")
        stream.seek(0)
        with gzip.GzipFile(fileobj=stream, mode="rb") as decoded, io.TextIOWrapper(decoded, encoding="utf-8") as text:
            for line in text:
                yield json.loads(line)


def verify_archive(root: Path, *, deep: bool = False) -> dict:
    """Check the frozen base and committed receipts without changing the archive.

    The default checks index pins, references, safe paths and stored lengths.
    ``deep=True`` additionally hashes every stored and decoded original.
    Uncommitted objects and interrupted temporary writes are reported, retained,
    and excluded from claimed acquisition coverage.
    """
    store = CentralRawArchiveStore(root, create=False)
    objects, paths = {}, set()
    base_records, acquisitions = 0, 0
    try:
        with store.open_member("snapshot.json") as stream:
            snapshot = json.load(stream)
    except FileNotFoundError:
        snapshot = None
    if snapshot is not None:
        for name, expected in snapshot["indexes"].items():
            with store.open_member(name) as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
                    raise BlobIntegrityError("raw archive index differs from snapshot: " + name)
        for obj in iter_base_records(root, "objects.jsonl.gz"):
            digest = _check_object(store, obj, deep=deep)
            if digest in objects:
                raise ValueError("duplicate object identity in base index")
            objects[digest] = obj
            paths.add(obj["object"])
        if len(objects) != snapshot["objects"]:
            raise ValueError("base object count differs from snapshot")
        for name in ("files.jsonl.gz", "receipts.jsonl.gz", "provisional-receipts.jsonl.gz"):
            for row in iter_base_records(root, name):
                digest = row.get("sha256", row.get("raw_sha256"))
                obj = objects.get(digest)
                if obj is None or ("object" in row and row["object"] != obj["object"]):
                    raise ValueError("unresolved original reference in " + name)
                if row.get("receipt_object") and row["receipt_object"] not in paths:
                    raise ValueError("unresolved acquisition receipt in " + name)
                base_records += 1
    for _path, document in iter_acquisition_receipts(root, verify_objects=deep):
        acquisitions += 1
        for obj in document["objects"]:
            previous = objects.setdefault(obj["sha256"], obj)
            if previous != obj:
                raise BlobIntegrityError("conflicting retained original descriptions")
            paths.add(obj["object"])
    uncommitted, pending = [], []
    for key in store._reader.keys():  # noqa: SIM118 - MemberSource exposes keys(), not iteration.
        if key.startswith("objects/") and key not in paths:
            uncommitted.append(key)
        elif key.startswith(".pending/"):
            pending.append(key)
    return {
        "verified": True,
        "verification": "all-original-and-stored-hashes" if deep else "index-pins-references-paths-stored-sizes",
        "objects": len(objects),
        "base_records": base_records,
        "acquisition_receipts": acquisitions,
        "uncommitted_objects": uncommitted,
        "pending_files": pending,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["verify"])
    parser.add_argument("--root", type=Path, default=default_raw_archive_root())
    parser.add_argument("--deep", action="store_true", help="hash all original and stored bytes")
    args = parser.parse_args()
    print(json.dumps(verify_archive(args.root, deep=args.deep), sort_keys=True))


__all__ = [
    "ACQUISITION_FORMAT",
    "CentralRawArchiveStore",
    "default_raw_archive_root",
    "iter_acquisition_receipts",
    "iter_base_records",
    "verify_archive",
]


if __name__ == "__main__":
    main()
