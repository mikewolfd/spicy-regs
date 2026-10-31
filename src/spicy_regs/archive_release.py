"""The congressional-tech archive release spicy-regs reads, pinned by release_id (L2-A3, L2-A4).

``archive_release_pins.json`` beside this module is the binding record: the
ct-cov3 v1 wire contract (``{"version": 1, "updated_at", "releases": [64-hex]}``)
plus ``bound``, the one release spicy-regs reads now. The archive's maintenance
keeps only ``live`` and ``last_good`` plus external pins
(engine-first-architecture §10.3.7), so spicy-regs publishes the wire keys alone
to its own bucket at ``PINS_URL`` (owner, 2026-10-31: each project writes only
its own bucket; the archive reads it over HTTPS). ``main`` here renders that
document and, with ``--publish``, uploads and reads it back; the publish
workflow runs it when the record changes on main, before any MCP deploy binds it.

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
from typing import Any, NamedTuple

from spicy_regs.sources import publication

ORIGIN = "https://congressional-data.spicygov.ai/viewer-publications"
#: The published pin list, in spicy-regs' own bucket; ct-cov3 reads it.
PINS_KEY = "pins/archive-releases.json"
PINS_URL = f"https://data.spicygov.ai/{PINS_KEY}"
#: The ct-cov3 v1 wire keys; ``bound`` stays in the repo-held record.
WIRE_KEYS = ("version", "updated_at", "releases")
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


def wire_bytes(document: dict[str, Any]) -> bytes:
    """The published pin list: a validated record cut to exactly the ct-cov3 v1 keys."""
    return (json.dumps({key: document[key] for key in WIRE_KEYS}, indent=2) + "\n").encode()


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


def _get(url: str) -> bytes:
    """One control document, bounded like the archive's own reader; a local path reads the file."""
    if not url.startswith(("https://", "http://")):
        with open(url, "rb") as stream:
            raw = stream.read(CONTROL_BYTES + 1)
        if len(raw) > CONTROL_BYTES:
            raise ArchiveReleaseError(f"{url} exceeds its byte limit")
        return raw
    raw = publication._bounded_get(url, allow_missing=False, limit=CONTROL_BYTES)
    assert raw is not None
    return raw


@cache
def read_manifest(release_id: str, origin: str = ORIGIN) -> dict[str, Any]:
    """Fetch and verify one release manifest, once per process: a release prefix is immutable."""
    return verify_manifest(release_id, json.loads(_get(f"{origin}/releases/{release_id}/manifest.json")))


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
    the site's ``lookupUrl``). A URL that is not absolute has no key. Like the producer's SQL, userinfo and
    other schemes are keyed as spelled: the site's refusal of them validates user input, not the key rule.
    """
    match = _URL.fullmatch(url)
    if match is None or not match[2]:
        return None
    scheme, host = match[1].lower(), re.sub(r"^www\.", "", match[2].lower())
    host = re.sub(r":80$", "", host) if scheme == "http" else re.sub(r":443$", "", host) if scheme == "https" else host
    return sha256(f"lookup:{host}{re.sub(r'/$', '', match[3])}{match[4] or ''}".encode()).digest()[:8]


#: SQL view name per bound stem: flat ``main`` names, since the MCP's relation scoping reads only ``main``.
VIEWS = {f"archive_{stem}": stem for stem in BOUND_FILES}
#: What the archive's integrity rests on in a reader that does not hash served bytes; describe_table states it.
REMOTE_TRUST = ("The manifest's release id is recomputed from its files[] and schema.json is checked against its "
                "manifest digest; Parquet bytes read by HTTPS range are not hashed, and rest on the release prefix "
                "being create-only.")
LOCAL_TRUST = "Every bound file and schema.json was hashed against the verified manifest when the connection was built."


class Binding(NamedTuple):
    """One verified archive release as the MCP reads it: where, which files, and the published schema."""

    release_id: str
    origin: str
    files: dict[str, dict[str, Any]]
    schema: dict[str, Any]
    #: schema.json as published: per-file entries, relationships and recipes.
    document: dict[str, Any]
    hashed: bool

    def url(self, stem: str) -> str:
        return f"{self.origin}/releases/{self.release_id}/{stem}.parquet"

    def view_sql(self, stem: str) -> str:
        """The view body: the published columns by name, plus ``list_row`` where the schema declares it virtual.

        Naming each column makes a file that lost one fail at bind time; a column the schema does not
        publish stays hidden. ``list_row`` is the zero-based physical row (DATA-MODEL §8.4); ``bound_files``
        refuses shards, so no offset applies.
        """
        entry = self.schema[stem]
        virtual = [column["name"] for column in entry.get("virtual_columns", [])]
        if virtual not in ([], ["list_row"]):
            raise ArchiveReleaseError(f"Archive {stem}.parquet declares unsupported virtual columns {virtual}")
        names = ", ".join('"' + column["name"].replace('"', '""') + '"' for column in entry["columns"])
        location = self.url(stem).replace("'", "''")
        if virtual:
            return f"SELECT {names}, file_row_number AS list_row FROM read_parquet('{location}', file_row_number = true)"
        return f"SELECT {names} FROM read_parquet('{location}')"


@cache
def _read_schema(release_id: str, origin: str, digest: str) -> dict[str, Any]:
    """schema.json, once per process, when its bytes match the digest the verified manifest states."""
    raw = _get(f"{origin}/releases/{release_id}/schema.json")
    if sha256(raw).hexdigest() != digest:
        raise ArchiveReleaseError(f"Archive schema.json does not match release {release_id[:8]}")
    return json.loads(raw)


def bind(release_id: str, origin: str, *, hash_files: bool) -> Binding:
    """Verify one pinned release: manifest id recomputed, schema.json against its digest, bytes too when local.

    A remote reader passes ``hash_files=False``: hashing would download every file per process.
    """
    manifest = read_manifest(release_id, origin)
    found = bound_files(manifest)
    entries = {item["name"]: item for item in manifest["files"]}
    if "schema.json" not in entries:
        raise ArchiveReleaseError(f"Archive release {release_id[:8]} lists no schema.json")
    document = _read_schema(release_id, origin, entries["schema.json"]["sha256"])
    schema = {item["name"].removesuffix(".parquet"): item for item in document["files"]}
    if missing := [stem for stem in found if stem not in schema]:
        raise ArchiveReleaseError(f"Archive schema.json describes no {missing}")
    if hash_files:
        for stem, entry in found.items():
            digest = sha256()
            with open(f"{origin}/releases/{release_id}/{stem}.parquet", "rb") as stream:
                while chunk := stream.read(1 << 20):
                    digest.update(chunk)
            if digest.hexdigest() != entry["sha256"]:
                raise ArchiveReleaseError(f"Local archive {stem}.parquet does not match release {release_id[:8]}")
    return Binding(release_id, origin, found, schema, document, hash_files)


def relationship_tables(item: dict[str, Any]) -> list[str]:
    """The release tables a published relationship joins; an external side names none."""
    return [side["table"] for side in (item["source"], item["target"]) if "table" in side]


def description(binding: Binding) -> dict[str, Any]:
    """What list_sources and describe_table state about the bound release, pinned with the connection.

    Relationships and recipes are shown as published, with a name map from the release's table names to
    these views; one whose tables are not all bound here (spicy-regs' own committees, members, rosters,
    bills, nominations) is marked ``available_here: false`` rather than rewritten.
    """
    document = binding.document
    bound = set(binding.files)

    def here(tables: list[str]) -> bool:
        return set(tables) <= bound

    return {
        "status": "bound",
        "release_id": binding.release_id,
        "origin": binding.origin if binding.origin.startswith(("https://", "http://")) else "local",
        "pins_url": PINS_URL,
        "trust": LOCAL_TRUST if binding.hashed else REMOTE_TRUST,
        "name_map": {stem: view for view, stem in VIEWS.items()},
        "views": {
            view: {"file": f"{stem}.parquet", **{key: binding.files[stem][key] for key in ("sha256", "size", "rows")},
                   **{key: binding.schema[stem][key] for key in
                      ("purpose", "row_meaning", "identity", "gaps", "notes", "columns", "virtual_columns")
                      if key in binding.schema[stem]}}
            for view, stem in VIEWS.items()
        },
        # A relationship's target can be ``{"external": ...}`` (an identifier resolved outside the release).
        "relationships": [{**item, "available_here": here(relationship_tables(item))}
                          for item in document.get("relationships", [])],
        "recipes": [{**item, "available_here": here(item["requires"])} for item in document.get("recipes", [])],
    }


def main(argv: list[str] | None = None) -> int:
    """Render ``pins/archive-releases.json`` from the shipped record; ``--publish`` uploads it and reads it back."""
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--output", type=Path, default=Path("output") / PINS_KEY)
    parser.add_argument("--publish", action="store_true", help=f"Upload to R2 as {PINS_KEY} and verify the public copy")
    args = parser.parse_args(argv)
    raw = wire_bytes(load_pins())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(raw)
    if args.publish:
        from spicy_regs.sources.r2 import upload_file, verify_public_file

        # Short edge lifetime: a newly pinned release must reach the archive's pruner before it runs.
        upload_file(args.output, PINS_KEY, cache_control="public, max-age=60, must-revalidate")
        # Read back from the origin readers use, not the configured upload base: a pass proves PINS_URL serves it.
        origin = PINS_URL.removesuffix("/" + PINS_KEY)
        print(json.dumps({"published": PINS_KEY, **verify_public_file(args.output, PINS_KEY, origin)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
