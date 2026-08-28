"""Explicit local content-addressed storage for source-native payloads."""

from __future__ import annotations

import hashlib
import os
import stat
import secrets
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol, runtime_checkable

from rulespec_artifacts import LocalBlobSource

from spicy_regs.publication import ImmutablePublicationError


@dataclass(frozen=True, slots=True)
class SourceNativeBlobWrite:
    """Observed effect of one conditional CAS write."""

    blob_ref: str
    byte_size: int
    reused: bool
    bytes_written: int


@runtime_checkable
class SourceNativeBlobStore(Protocol):
    """The one injected read/write boundary used by source-native publication."""

    def put_blob(
        self,
        blob_ref: str,
        byte_size: int,
        chunks: Iterable[bytes],
    ) -> SourceNativeBlobWrite: ...

    def open(self, blob_ref: str) -> AbstractContextManager[BinaryIO]: ...


def _digest_name(blob_ref: str) -> str:
    if (
        not isinstance(blob_ref, str)
        or not blob_ref.startswith("sha256:")
        or len(blob_ref) != 71
        or any(character not in "0123456789abcdef" for character in blob_ref[7:])
    ):
        raise ValueError("source-native blob reference must be a qualified SHA-256 digest")
    return blob_ref[7:]


def _open_directory(path: Path, *, label: str) -> int:
    try:
        return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except (FileNotFoundError, NotADirectoryError, OSError) as error:
        raise ValueError(
            f"{label} is missing or is not a present non-symlink directory"
        ) from error


def _open_child_directory(parent: int, name: str, *, label: str) -> int:
    try:
        return os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            dir_fd=parent,
        )
    except (FileNotFoundError, NotADirectoryError, OSError) as error:
        raise ValueError(
            f"{label} is missing or is not a present non-symlink directory"
        ) from error


def _directory_identity(descriptor: int, *, label: str) -> tuple[int, int]:
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{label} is not a directory")
    return metadata.st_dev, metadata.st_ino


def _require_directory_identity(
    descriptor: int,
    expected: tuple[int, int],
    *,
    label: str,
) -> None:
    if _directory_identity(descriptor, label=label) != expected:
        raise ValueError(f"{label} changed after admission")


def _prepare_child_directory(parent: int, name: str, *, create: bool) -> int:
    if create:
        try:
            os.mkdir(name, dir_fd=parent)
        except FileExistsError:
            pass
    return _open_child_directory(
        parent,
        name,
        label="source-native blob store layout",
    )


def _verify_blob(
    directory: int,
    digest_name: str,
    *,
    blob_ref: str,
    byte_size: int,
) -> None:
    try:
        descriptor = os.open(
            digest_name,
            os.O_RDONLY | os.O_NOFOLLOW,
            dir_fd=directory,
        )
    except FileNotFoundError as error:
        raise ImmutablePublicationError(f"source-native blob is missing: {blob_ref}") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ImmutablePublicationError(
                f"source-native blob is not a regular file: {blob_ref}"
            )
        digest = hashlib.sha256()
        observed_size = 0
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            while block := stream.read(1024 * 1024):
                digest.update(block)
                observed_size += len(block)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if observed_size != byte_size or "sha256:" + digest.hexdigest() != blob_ref:
        raise ImmutablePublicationError(
            f"source-native blob differs from its content identity: {blob_ref}"
        )


def _blob_exists(directory: int, digest_name: str) -> bool:
    try:
        metadata = os.stat(digest_name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(metadata.st_mode):
        raise ImmutablePublicationError(
            "source-native blob destination is not a regular file"
        )
    return True


def _pending_file(directory: int) -> tuple[int, str]:
    for _ in range(128):
        name = f"blob-{secrets.token_hex(16)}"
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory,
            )
        except FileExistsError:
            continue
        return descriptor, name
    raise ImmutablePublicationError("source-native blob pending name space is exhausted")


class LocalSourceNativeBlobStore:
    """One append-only local CAS selected explicitly by the composition root."""

    def __init__(self, root: Path, *, create: bool = True) -> None:
        selected = Path(root)
        if create:
            selected.mkdir(parents=True, exist_ok=True)
        root_descriptor = _open_directory(
            selected,
            label="source-native blob store",
        )
        try:
            resolved = selected.resolve(strict=True)
            root_identity = _directory_identity(
                root_descriptor,
                label="source-native blob store",
            )
            resolved_state = os.stat(resolved, follow_symlinks=False)
            if root_identity != (
                resolved_state.st_dev,
                resolved_state.st_ino,
            ):
                raise ValueError("source-native blob store changed during admission")
            self.root = resolved
            child_identities: dict[str, tuple[int, int]] = {}
            for name in ("sha256", ".pending"):
                child = _prepare_child_directory(root_descriptor, name, create=create)
                try:
                    child_identities[name] = _directory_identity(
                        child,
                        label="source-native blob store layout",
                    )
                finally:
                    os.close(child)
        finally:
            os.close(root_descriptor)
        self._root_identity = root_identity
        self._child_identities = child_identities
        self._reader = LocalBlobSource(self.root)

    @contextmanager
    def _layout(self) -> Iterator[tuple[int, int, int]]:
        with ExitStack() as stack:
            root = _open_directory(self.root, label="source-native blob store")
            stack.callback(os.close, root)
            _require_directory_identity(
                root,
                self._root_identity,
                label="source-native blob store",
            )
            sha_root = _open_child_directory(
                root,
                "sha256",
                label="source-native blob store layout",
            )
            stack.callback(os.close, sha_root)
            _require_directory_identity(
                sha_root,
                self._child_identities["sha256"],
                label="source-native blob store SHA-256 directory",
            )
            pending_root = _open_child_directory(
                root,
                ".pending",
                label="source-native blob store layout",
            )
            stack.callback(os.close, pending_root)
            _require_directory_identity(
                pending_root,
                self._child_identities[".pending"],
                label="source-native blob store pending directory",
            )
            yield root, sha_root, pending_root

    @contextmanager
    def open(self, blob_ref: str) -> Iterator[BinaryIO]:
        _digest_name(blob_ref)
        with self._reader.open(blob_ref) as stream:
            yield stream

    def put_blob(
        self,
        blob_ref: str,
        byte_size: int,
        chunks: Iterable[bytes],
    ) -> SourceNativeBlobWrite:
        """Create absent bytes, verify EEXIST, and retain safe race/orphan state."""

        digest_name = _digest_name(blob_ref)
        if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 0:
            raise ValueError("source-native blob byte size must be a non-negative integer")
        with self._layout() as (root, sha_root, pending_root):
            if _blob_exists(sha_root, digest_name):
                _verify_blob(
                    sha_root,
                    digest_name,
                    blob_ref=blob_ref,
                    byte_size=byte_size,
                )
                return SourceNativeBlobWrite(blob_ref, byte_size, True, 0)

            descriptor, pending_name = _pending_file(pending_root)
            digest = hashlib.sha256()
            observed_size = 0
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    for chunk in chunks:
                        if not isinstance(chunk, bytes):
                            raise TypeError("source-native blob writes require bytes")
                        stream.write(chunk)
                        digest.update(chunk)
                        observed_size += len(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
                if observed_size != byte_size or "sha256:" + digest.hexdigest() != blob_ref:
                    raise ImmutablePublicationError(
                        "source-native blob write differs from its declared receipt"
                    )
                try:
                    os.link(
                        pending_name,
                        digest_name,
                        src_dir_fd=pending_root,
                        dst_dir_fd=sha_root,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    _verify_blob(
                        sha_root,
                        digest_name,
                        blob_ref=blob_ref,
                        byte_size=byte_size,
                    )
                    return SourceNativeBlobWrite(
                        blob_ref,
                        byte_size,
                        True,
                        observed_size,
                    )
                _verify_blob(
                    sha_root,
                    digest_name,
                    blob_ref=blob_ref,
                    byte_size=byte_size,
                )
                os.fsync(sha_root)
                os.fsync(root)
                return SourceNativeBlobWrite(
                    blob_ref,
                    byte_size,
                    False,
                    observed_size,
                )
            finally:
                try:
                    os.unlink(pending_name, dir_fd=pending_root)
                except FileNotFoundError:
                    pass


__all__ = [
    "LocalSourceNativeBlobStore",
    "SourceNativeBlobStore",
    "SourceNativeBlobWrite",
]
