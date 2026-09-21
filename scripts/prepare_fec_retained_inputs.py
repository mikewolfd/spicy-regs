"""Verify and unpack an explicitly transferred FEC input selection, without HTTP."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import tarfile
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

from spicy_regs.transforms.build_fec_observations import _load_manifest

MAX_ARCHIVE_BYTES = 4_000_000_000
MAX_EXTRACTED_BYTES = 8_000_000_000
MAX_MEMBERS = 100_000
MAX_MANIFEST_BYTES = 16 * 1024 * 1024


def _digest(value: str) -> str:
    value = value.removeprefix("sha256:")
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("expected a lowercase SHA-256 digest")
    return value


def _sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _relative(value: str) -> Path:
    path = PurePosixPath(value)
    if not path.parts or path.is_absolute() or ".." in path.parts or "\\" in value or path.as_posix() != value:
        raise ValueError("bundle paths must be normalized relative POSIX paths")
    return Path(*path.parts)


def _path_fields(value: dict):
    for ordinal, item in enumerate(value["collections"]):
        for key in ("blob_root", "release_path"):
            if key in item:
                yield f"/collections/{ordinal}/{key}", item, key
        dictionary = item.get("field_mapping", {}).get("dictionary")
        if dictionary is not None:
            yield f"/collections/{ordinal}/field_mapping/dictionary/blob_root", dictionary, "blob_root"


def _relocations(original: dict, portable: dict, root: Path) -> list[dict]:
    """Only filesystem paths may change; acquisition facts and source pins cannot."""
    before, after = deepcopy(original), deepcopy(portable)
    old_paths = {pointer: parent[key] for pointer, parent, key in _path_fields(before)}
    changes = []
    for pointer, parent, key in _path_fields(after):
        if pointer not in old_paths:
            raise ValueError("portable manifest changes the selected input modes")
        selected = root / _relative(parent[key])
        if not selected.is_dir() or not selected.resolve().is_relative_to(root.resolve()):
            raise ValueError("manifest input roots must be existing directories inside the bundle")
        changes.append({"pointer": pointer, "original": old_paths[pointer], "portable": parent[key]})
        parent[key] = old_paths[pointer]
    if before != after:
        raise ValueError("portable manifest must preserve all source facts and change only filesystem paths")
    return changes


def prepare_inputs(
    archive: Path,
    *,
    archive_sha256: str,
    manifest_sha256: str,
    output_dir: Path,
    audit_dir: Path,
    max_archive_bytes: int = MAX_ARCHIVE_BYTES,
    max_extracted_bytes: int = MAX_EXTRACTED_BYTES,
) -> Path:
    """Install a fresh bounded selection after transfer and path verification.

    Source membership/digest/semantic verification remains with the existing
    retained builder. A transfer receipt is not a new acquisition observation.
    """
    audit_dir.mkdir(parents=True, exist_ok=False)
    receipt = {
        "version": 1,
        "transfer_checked_at": datetime.now(UTC).isoformat(),
        "status": "checking",
        "source_verification": "required-by-build-fec-observations-before-publication",
    }
    try:
        expected_archive, expected_manifest = _digest(archive_sha256), _digest(manifest_sha256)
        receipt.update(archive_sha256=expected_archive, manifest_sha256=expected_manifest)
        if min(max_archive_bytes, max_extracted_bytes) <= 0:
            raise ValueError("transfer and extraction limits must be positive")
        if output_dir.exists():
            raise FileExistsError("retained inputs require a fresh output directory")
        size = archive.stat().st_size
        if size > max_archive_bytes:
            raise ValueError("archive exceeds the selected transfer byte limit")
        if _sha256(archive) != expected_archive:
            raise ValueError("transferred archive SHA-256 differs")
        receipt["archive_bytes"] = size
        output_dir.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".fec-inputs-", dir=output_dir.parent) as temporary:
            stage = Path(temporary) / "selection"
            stage.mkdir()
            names, total = set(), 0
            with tarfile.open(archive, mode="r|*") as source:
                for member in source:
                    name = member.name.rstrip("/") if member.isdir() else member.name
                    target = stage / _relative(name)
                    if name in names or len(names) >= MAX_MEMBERS:
                        raise ValueError("bundle repeats a path or exceeds the member limit")
                    names.add(name)
                    if member.isdir():
                        target.mkdir(parents=True, exist_ok=True)
                        continue
                    if not member.isfile() or member.issparse():
                        raise ValueError("bundle members must be ordinary files or directories; links are forbidden")
                    total += member.size
                    if member.size < 0 or total > max_extracted_bytes:
                        raise ValueError("bundle exceeds the selected extraction byte limit")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    reader = source.extractfile(member)
                    if reader is None:
                        raise ValueError("bundle file has no readable contents")
                    with reader, target.open("xb") as writer:
                        shutil.copyfileobj(reader, writer, length=1024 * 1024)
            manifest, original = stage / "manifest.json", stage / "source-manifest.json"
            if any(path.stat().st_size > MAX_MANIFEST_BYTES for path in (manifest, original)):
                raise ValueError("input manifests exceed the 16 MiB byte limit")
            if _sha256(manifest) != expected_manifest:
                raise ValueError("portable manifest SHA-256 differs")
            # Retain exact bytes, including on a later structural/source failure.
            for path in (manifest, original):
                shutil.copyfile(path, audit_dir / path.name)
            _load_manifest(manifest)
            _load_manifest(original)
            changes = _relocations(json.loads(original.read_bytes()), json.loads(manifest.read_bytes()), stage)
            (audit_dir / "relocations.json").write_text(json.dumps(changes, indent=2) + "\n")
            receipt.update(
                source_manifest_sha256=_sha256(original),
                extracted_bytes=total,
                member_count=len(names),
                status="verified-transfer",
            )
            if output_dir.exists():
                raise FileExistsError("retained inputs require a fresh output directory")
            stage.rename(output_dir)
        return output_dir / "manifest.json"
    except Exception as error:
        receipt.update(status="refused", error=str(error))
        raise
    finally:
        (audit_dir / "transfer.json").write_text(json.dumps(receipt, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--archive-sha256", required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--max-archive-bytes", type=int, default=MAX_ARCHIVE_BYTES)
    parser.add_argument("--max-extracted-bytes", type=int, default=MAX_EXTRACTED_BYTES)
    print(prepare_inputs(**vars(parser.parse_args())))


if __name__ == "__main__":
    main()
