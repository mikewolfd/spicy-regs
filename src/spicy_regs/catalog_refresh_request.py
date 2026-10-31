"""Opt-in coalesced catalogue notification after durable source/publication work.

The control means all committed inputs may have changed. Its last event is a
diagnostic, never a per-family work list or publication-selection authority.
An existing local runner consumes the full delta through refresh-local-catalog.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat
import tempfile
from uuid import uuid4

REQUEST_ENV = "SPICYGOV_CATALOG_REQUEST"
FORMAT = "spicy-catalog-refresh-request"


def control_path(path: Path, raw_root: Path) -> Path:
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("Catalogue request requires an absolute regular control path")
    resolved = path.resolve()
    if resolved.is_relative_to(raw_root.resolve()):
        raise ValueError("Catalogue request must stay outside the raw archive")
    return resolved


@contextmanager
def locked(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path.with_name(path.name + ".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("Catalogue request lock must be a regular file")
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def _read(path):
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise ValueError("Catalogue request must be a regular file")
    with path.open("rb") as stream:
        body = stream.read(65537)
    if len(body) > 65536:
        raise ValueError("Catalogue request exceeds its control bound")
    value = json.loads(body)
    if (
        not isinstance(value, dict)
        or value.get("format") != FORMAT
        or value.get("version") != 1
        or not re.fullmatch(r"[0-9a-f]{32}", value.get("requestId", ""))
        or value.get("scope") != "all-committed-inputs"
    ):
        raise ValueError("Invalid catalogue refresh request")
    return {"request": value, "sha256": "sha256:" + hashlib.sha256(body).hexdigest()}


def pending(path: Path, raw_root: Path):
    path = control_path(path, raw_root)
    with locked(path):
        item = _read(path)
        if item and Path(item["request"]["rawRoot"]).resolve() != raw_root.resolve():
            raise ValueError("Catalogue request names a different raw archive")
        return item


def notify(reason: str, reference: dict, *, raw_root: Path | None = None):
    """Never change the result of the durable operation that precedes this call."""
    if not (configured := os.environ.get(REQUEST_ENV)):
        return None
    try:
        if raw_root is None:
            if not (configured_raw := os.environ.get("SPICYGOV_RAW_ROOT")):
                raise ValueError("Catalogue notification requires SPICYGOV_RAW_ROOT")
            raw_root = Path(configured_raw)
        path = control_path(Path(configured), raw_root)
        value = {
            "format": FORMAT,
            "version": 1,
            "requestId": uuid4().hex,
            "requestedAt": datetime.now(timezone.utc).isoformat(),
            "rawRoot": str(raw_root.resolve()),
            "scope": "all-committed-inputs",
            "lastEvent": {"reason": reason, "reference": reference},
        }
        with locked(path):
            if path.is_symlink():
                raise ValueError("Catalogue request symlink refused")
            previous = _read(path)
            if previous and Path(previous["request"]["rawRoot"]).resolve() != raw_root.resolve():
                raise ValueError("Catalogue request names a different raw archive")
            publication = reference.get("publicationSha256") or (
                previous["request"].get("publicationSha256") if previous else None
            )
            if publication:
                value["publicationSha256"] = publication
            body = (json.dumps(value, sort_keys=True) + "\n").encode()
            if len(body) > 65536:
                raise ValueError("Catalogue request exceeds its control bound")
            descriptor, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(body)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                Path(temporary).unlink(missing_ok=True)
        return {"status": "pending", "path": str(path), "requestId": value["requestId"]}
    except Exception as error:
        logging.getLogger(__name__).warning("Durable operation completed; catalogue notification failed: %s", error)
        return {"status": "signal-failed", "error": f"{type(error).__name__}: {error}"}


def acknowledge(path: Path, expected_sha256: str, raw_root: Path):
    path = control_path(path, raw_root)
    with locked(path):
        item = _read(path)
        if item is None:
            return {"status": "already-acknowledged"}
        if Path(item["request"]["rawRoot"]).resolve() != raw_root.resolve():
            raise ValueError("Catalogue request names a different raw archive")
        if item["sha256"] != expected_sha256:
            return {"status": "pending-newer-request", "requestId": item["request"]["requestId"]}
        path.unlink()
        return {"status": "acknowledged", "requestId": item["request"]["requestId"]}
