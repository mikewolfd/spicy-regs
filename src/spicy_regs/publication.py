"""Small durable-write primitives shared by immutable SpicyRegs publishers."""

from __future__ import annotations

import ctypes
import errno
import os
import sys
from collections.abc import Iterable
from pathlib import Path


class ImmutablePublicationError(RuntimeError):
    """An immutable path could not be written or published exactly once."""


def write_bytes_once(path: Path, payload: bytes) -> None:
    """Create and durably flush one file without replacing existing bytes."""

    write_chunks_once(path, (payload,))


def write_chunks_once(path: Path, chunks: Iterable[bytes]) -> None:
    """Stream and durably flush one file without replacing existing bytes."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            for chunk in chunks:
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as error:
        raise ImmutablePublicationError(f"refusing to replace immutable file: {path}") from error


def publish_directory_once(working: Path, destination: Path) -> None:
    """Rename a private directory while holding one exclusive publication lock."""

    working = Path(working).absolute()
    destination = Path(destination).absolute()
    lock_path = destination.parent / f".{destination.name}.publish.lock"
    try:
        descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ImmutablePublicationError(f"publication is already in progress for: {destination}") from error
    try:
        with os.fdopen(descriptor, "wb") as lock:
            lock.write(working.name.encode("utf-8"))
            lock.flush()
            os.fsync(lock.fileno())
        if destination.exists() or destination.is_symlink():
            raise ImmutablePublicationError(f"refusing to replace immutable directory: {destination}")
        for directory in sorted(
            (path for path in working.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            _fsync_directory(directory)
        _fsync_directory(working)
        _rename_directory_noreplace(working, destination)
        _fsync_directory(destination.parent)
    finally:
        lock_path.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_directory_noreplace(source: Path, destination: Path) -> None:
    """Atomically rename a directory only when the destination is absent."""

    if os.name == "nt":
        try:
            os.rename(source, destination)
        except FileExistsError as error:
            raise ImmutablePublicationError(
                f"refusing to replace immutable directory: {destination}"
            ) from error
        return

    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform == "darwin":
        rename = libc.renamex_np
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, 0x4)  # RENAME_EXCL
    elif sys.platform.startswith("linux") and hasattr(libc, "renameat2"):
        rename = libc.renameat2
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        rename.restype = ctypes.c_int
        result = rename(-100, source_bytes, -100, destination_bytes, 1)  # RENAME_NOREPLACE
    else:
        raise ImmutablePublicationError("this platform lacks an atomic no-replace directory rename")
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ImmutablePublicationError(f"refusing to replace immutable directory: {destination}")
    raise OSError(error_number, os.strerror(error_number), destination)


__all__ = [
    "ImmutablePublicationError",
    "publish_directory_once",
    "write_bytes_once",
    "write_chunks_once",
]
