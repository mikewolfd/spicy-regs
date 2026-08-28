"""Small durable-write primitives shared by immutable SpicyRegs publishers."""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

from rulespec_artifacts import (
    ArtifactVerificationError,
    MemberSourceError,
    publish_directory_no_replace,
)


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
    """Publish one private directory through the shared immutable primitive."""

    try:
        publish_directory_no_replace(working, destination)
    except FileExistsError as error:
        raise ImmutablePublicationError(
            f"refusing to replace immutable directory: {destination}"
        ) from error
    except BlockingIOError as error:
        raise ImmutablePublicationError(
            f"publication is already in progress for: {destination}"
        ) from error
    except (ArtifactVerificationError, MemberSourceError, OSError, ValueError) as error:
        raise ImmutablePublicationError(
            f"immutable directory publication failed for {destination}: {error}"
        ) from error


__all__ = [
    "ImmutablePublicationError",
    "publish_directory_once",
    "write_bytes_once",
    "write_chunks_once",
]
