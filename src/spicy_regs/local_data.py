"""Resolve one local download selection without fetching remote data."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from spicy_regs.sources.publication import empty_index, parse_index, table_location

_NAME = re.compile(r"[a-z][a-z0-9_-]*\Z")


@dataclass(frozen=True)
class LocalSelection:
    directory: Path
    files: dict[str, tuple[Path, str]]
    publication: dict
    is_download: bool


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("Current download metadata repeats a key")
        result[key] = value
    return result


def local_selection(output_dir: Path, *, include_legacy: bool = False) -> LocalSelection:
    """Capture current once; a download batch exposes only its explicit selection.

    CLI callers can retain their historical root-file fallback with include_legacy.
    This reads receipt metadata, not proof that local member bytes are unchanged.
    """
    root = output_dir.expanduser().absolute()
    current = root if root.name == "current" and root.is_symlink() else root / "current"
    if current.is_symlink():
        directory = current.resolve(strict=True)
        if directory.parent != (current.parent / "download-runs").resolve():
            raise RuntimeError("Current download points outside download-runs")
    elif current.exists():
        raise RuntimeError("Current download must be a symlink")
    else:
        directory = root.resolve(strict=True)
    metadata_path = directory / "download.json"
    is_download = metadata_path.is_file()
    if current.is_symlink() and not is_download:
        raise RuntimeError("Current download is missing download.json")
    index = empty_index()
    result = {}
    if is_download:
        metadata = json.loads(metadata_path.read_text(), object_pairs_hook=_unique_pairs)
        if metadata.get("version") != 1 or metadata.get("status") != "complete":
            raise RuntimeError("Current download is incomplete")
        index = parse_index(json.dumps(metadata["publication"]).encode())
        selected = metadata.get("selected")
        if not isinstance(selected, dict) or not selected:
            raise RuntimeError("Current download has no selected tables")
        for name, selection in selected.items():
            if not _NAME.fullmatch(name):
                raise RuntimeError("Current download contains an invalid table name")
            key, published = table_location(index, f"{name}.parquet")
            expected = {"key": key, "status": "managed" if published is not None else "legacy-unversioned"}
            if selection != expected:
                raise RuntimeError(f"Current download selection differs from its publication snapshot: {name}")
            path = directory / f"{name}.parquet"
            if path.is_symlink() or not path.is_file():
                raise RuntimeError(f"Current download is missing a regular {name}.parquet")
            result[name] = (path, selection["status"])
    if not is_download or include_legacy:
        for path in sorted(root.glob("*.parquet")):
            if _NAME.fullmatch(path.stem) and path.is_file():
                result.setdefault(path.stem, (path.resolve(), "legacy-unversioned"))
    return LocalSelection(directory, result, index, is_download)


def file_signature(path: Path) -> list[int]:
    """Detect ordinary replacement/mutation; this is not a cryptographic pin."""
    stat = path.lstat()
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"Local download member is no longer a regular file: {path.name}")
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def verify_local_members(selection: LocalSelection) -> dict[str, list[int]]:
    """Rehash managed bytes once, returning change guards for the selected batch."""
    signatures = {}
    if not selection.is_download:
        return signatures
    for name, (path, status) in selection.files.items():
        before = file_signature(path)
        if status == "managed":
            _, descriptor = table_location(selection.publication, f"{name}.parquet")
            if descriptor is None:
                raise RuntimeError(f"Local download member has no generation pin: {name}")
            with path.open("rb") as stream:
                digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
            if before[2] != descriptor["byteSize"] or digest != descriptor["sha256"]:
                raise RuntimeError(f"Local download member differs from its generation pin: {name}")
        if file_signature(path) != before:
            raise RuntimeError(f"Local download member changed during verification: {name}")
        signatures[str(path)] = before
    return signatures


def assert_local_members_unchanged(signatures: dict[str, list[int]]) -> None:
    for name, expected in signatures.items():
        try:
            actual = file_signature(Path(name))
        except OSError as exc:
            raise RuntimeError(f"Local download member unavailable: {name}") from exc
        if actual != expected:
            raise RuntimeError(f"Local download member changed after verification: {name}")
