"""The congressional-tech archive release spicy-regs reads, pinned by release_id (L2-A3, L2-A4).

``archive_release_pins.json`` beside this module is both the binding record and
the published pin list. It is the ct-cov3 v1 wire contract
(``{"version": 1, "updated_at", "releases": [64-hex]}``) plus ``bound``, the one
release spicy-regs reads now; readers of the wire contract ignore ``bound``.
The archive's maintenance keeps only ``live`` and ``last_good`` plus external
pins (engine-first-architecture §10.3.7), so it reads this file from GitHub
main by raw URL, as spicy-regs reads DocSpec's pins (``generation_retention``).
The pin is live when the commit merges, before any MCP deploy binds it.

Lifetime rule: when ``bound`` moves to a newer release, keep the superseded id
in ``releases`` until the hosted MCP has redeployed onto its successor, so the
archive cannot prune a release a running server still reads.

Trust: ``read_manifest`` recomputes the release id from the manifest's
``files[]`` (DATA-MODEL §8.3) rather than trusting the id the manifest states.
``schema.json`` is one of those entries, so the id commits to the schema too.
Bytes later served by HTTPS range reads are not hashed here; their trust rests
on the release prefix being create-only and on this recompute.
"""

from __future__ import annotations

import json
import re
from functools import cache
from hashlib import sha256
from importlib.resources import files
from typing import Any

from spicy_regs.sources import publication

ORIGIN = "https://congressional-data.spicygov.ai/viewer-publications"
PINS_URL = "https://raw.githubusercontent.com/mikewolfd/spicy-regs/main/src/spicy_regs/archive_release_pins.json"
#: The archive's own bound on control documents (congressional-tech release.py CONTROL_BYTES).
CONTROL_BYTES = 32 << 20
#: The release files L2-A4 binds; url_state, url_variants and the capture_* controls stay unexposed,
#: and producer ``spicy-regs`` entity files are spicy-regs' own tables republished.
BOUND_FILES = ("list", "detail", "links", "urls", "sources", "recoveries", "meetings", "witnesses", "recordings")
_RELEASE_ID = re.compile(r"[0-9a-f]{64}")
_URL = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*)://([^/?#]*)([^?#]*)(\?[^#]*)?(?:#.*)?")


class ArchiveReleaseError(ValueError):
    """A pin document or release manifest outside its stated contract."""


def parse_pins(raw: bytes | str) -> dict[str, Any]:
    """The pin document, validated: v1, unique lowercase 64-hex ids, ``bound`` among them."""
    try:
        document = json.loads(raw)
        releases = document["releases"]
        if document["version"] != 1 or not isinstance(document["updated_at"], str):
            raise ValueError("not a v1 pin document")
        if not isinstance(releases, list) or not releases or len(set(releases)) != len(releases):
            raise ValueError("releases must be a non-empty list of unique ids")
        if not all(isinstance(item, str) and _RELEASE_ID.fullmatch(item) for item in releases):
            raise ValueError("release ids must be 64-character lowercase hex")
        if document["bound"] not in releases:
            raise ValueError("the bound release must be pinned")
    except (ValueError, TypeError, KeyError) as exc:
        raise ArchiveReleaseError(f"Invalid archive release pins: {exc}") from exc
    return document


def load_pins() -> dict[str, Any]:
    """The repo-held pin document shipped with this package."""
    return parse_pins(files("spicy_regs").joinpath("archive_release_pins.json").read_bytes())


def release_identity(schema_version: Any, entries: list[dict[str, Any]]) -> str:
    """DATA-MODEL §8.3: sha256 of the compact JSON ``[schema_version, [[name, sha256, size, rows], ...]]``."""
    identity = [schema_version, [[item["name"], item["sha256"], item["size"], item["rows"]] for item in entries]]
    return sha256(json.dumps(identity, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def verify_manifest(release_id: str, manifest: dict[str, Any]) -> dict[str, Any]:
    """The manifest when both its stated id and the id recomputed from its files equal ``release_id``."""
    try:
        computed = release_identity(manifest["schema_version"], manifest["files"])
        stated = manifest["release_id"]
    except (TypeError, KeyError) as exc:
        raise ArchiveReleaseError(f"Archive manifest {release_id[:8]} is malformed") from exc
    if stated != release_id or computed != release_id:
        raise ArchiveReleaseError(
            f"Archive manifest for {release_id[:8]} states {str(stated)[:8]} and hashes to {computed[:8]}")
    return manifest


@cache
def read_manifest(release_id: str, origin: str = ORIGIN) -> dict[str, Any]:
    """Fetch and verify one release manifest, once per process: a release prefix is immutable."""
    raw = publication._bounded_get(f"{origin}/releases/{release_id}/manifest.json",
                                   allow_missing=False, limit=CONTROL_BYTES)
    assert raw is not None
    return verify_manifest(release_id, json.loads(raw))


def bound_files(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """The ``BOUND_FILES`` entries by stem; a missing, foreign-producer or sharded entry refuses."""
    entries = {item["name"]: item for item in manifest["files"]}
    found = {}
    for stem in BOUND_FILES:
        entry = entries.get(f"{stem}.parquet")
        if entry is None or entry.get("producer") != "archive":
            raise ArchiveReleaseError(f"Archive release {manifest['release_id'][:8]} has no archive {stem}.parquet")
        # list_row is file_row_number plus the part's offset; unsharded files start at 0 and no
        # offset rule for shards is published yet, so refuse rather than guess one.
        if entry.get("shard") is not None or entry.get("row_range") is not None:
            raise ArchiveReleaseError(f"Archive {stem}.parquet is sharded; sharded offsets are not defined")
        found[stem] = entry
    return found


def lookup_key(url: str) -> bytes | None:
    """DATA-MODEL §8.2's ``urls.parquet`` key: the first 8 bytes of ``sha256('lookup:' || lookup_url(url))``.

    ``lookup_url`` drops the scheme, a leading ``www.``, the default port, the
    fragment and one trailing path slash, and lowercases the host; the query is
    kept byte for byte (congressional-tech archive_format.py lookup_values and
    the site's ``lookupUrl``). A URL that is not absolute has no key.
    """
    match = _URL.fullmatch(url)
    if match is None or not match[2]:
        return None
    scheme, host = match[1].lower(), re.sub(r"^www\.", "", match[2].lower())
    host = re.sub(r":80$", "", host) if scheme == "http" else re.sub(r":443$", "", host) if scheme == "https" else host
    return sha256(f"lookup:{host}{re.sub(r'/$', '', match[3])}{match[4] or ''}".encode()).digest()[:8]
