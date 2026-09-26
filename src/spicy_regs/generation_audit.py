"""Audit one published table generation, read-only, into separate machine-readable sections.

These are the checks each qualification re-wrote by hand, with the blind spots
their reviews found closed: column types as well as names; duplicate and NULL
identities counted before any row is paired by identity, and multisets compared
where identity cannot pair rows; conservation in both directions with changed
cells enumerated per column; source evidence admitted and bound to the
generation; credentials searched in decoded evidence bytes and table cells;
HTTP 200 bodies sniffed for HTML where JSON or XML was expected; and empty
outputs reported as state, not failure.

Publication, structure (schema, identity, conservation), evidence and state are
separate sections. Source qualification and deployment are named in every
report as not assessed, and ``limits`` says what the run could not see.
Nothing here writes to object storage.

Cost: each member byte is streamed once for admission and scanning; Parquet is
aggregated by DuckDB over range reads (identity columns once per side, one full
outer join for conservation, one pass over string columns for credentials), so
each table is O(rows) with bounded samples.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any, BinaryIO

import duckdb
import httpx

from spicy_regs.content_checks import (
    CHUNK,
    CREDENTIAL_PATTERN,
    REDACTED,
    Scan,
    configured_secrets,
    expected_kind,
    redact,
    scan_bytes,
)
from spicy_regs.sources.publication import (
    EVIDENCE_PREFIX,
    INDEX_KEY,
    INDEX_LIMIT,
    PublicationError,
    parse_index,
    table_owner,
)

FORMAT = "spicy-regs-generation-audit"
ROOT = "artifact.json"
JOURNAL = "journal.jsonl"
#: Roots, manifests and evidence journals are held whole; a larger one is refused rather than half-read.
CONTROL_LIMIT = 256 * 1024 * 1024
CHAIN_LIMIT = 64
TEXT_LIMIT = 200
#: Column types that cannot carry text, so the cell credential scan skips them.
_NOT_TEXT = re.compile(r"(U?(BIG|SMALL|TINY|HUGE)?INT(EGER)?|DOUBLE|FLOAT|REAL|DECIMAL|BOOLEAN|DATE|TIME(STAMP)?|INTERVAL)\b")
#: What DuckDB raises for a missing or unreadable file; any other DuckDB error is a defect here and propagates.
_UNREADABLE = (duckdb.IOException, duckdb.HTTPException)
#: What a read or an admission can raise when bytes are missing, malformed or differ from their pins.
_READ_ERRORS = (ValueError, RuntimeError, OSError, httpx.HTTPError, duckdb.Error)

LIMITS = (
    "Anonymous reads cannot list a prefix: objects under the generation or evidence prefix that no manifest declares "
    "are invisible, so undeclared extra objects are not detected.",
    "Parquet pages are not re-decoded here; each member's bytes equal its admitted manifest digest, and the build "
    "verified that digest by decoding every page.",
    "Source qualification is not assessed: no native source record is re-read, re-parsed or re-derived. Conservation "
    "shows what changed against the prior pin, not whether a change is correct, and the prior's own qualification "
    "is not re-established.",
    "Deployment is not assessed: which job produced this generation, and whether readers consume it.",
    "An empty table is reported as state. This audit cannot tell an intentionally uncomputed output from a failed "
    "computation, and counts it neither as a failure nor as a qualification.",
    "Credential detection sees the listed parameter names followed by =, %3D or : and an 8-256 character token, plus "
    "literal configured values; a key under another name or in another encoding is visible only to the literal search.",
    "Body shapes are sniffed from the first 4 KiB of decoded content, past an XML prolog, comments and DOCTYPE.",
    "Samples are bounded and ordered by identity; every count is complete.",
    "Roots, manifests and evidence journals are held whole up to 256 MiB; a larger one refuses rather than being "
    "half-read.",
)


class AuditError(RuntimeError):
    """The audit cannot establish what it was asked to audit (index, family, table or prior pin)."""


# --------------------------------------------------------------------------- #
# Anonymous reads.
# --------------------------------------------------------------------------- #
class _Observed(io.RawIOBase):
    """A readable chunk stream that hashes, counts and shows every byte to its scans exactly once."""

    def __init__(self, chunks: Iterable[bytes], scans: Sequence[Scan] = ()):
        super().__init__()
        self._chunks, self._scans = iter(chunks), scans
        self._pending = bytearray()
        self._digest = hashlib.sha256()
        self.size, self.eof = 0, False

    @property
    def sha256(self) -> str:
        return "sha256:" + self._digest.hexdigest()

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        while not self._pending and not self.eof:
            chunk = next(self._chunks, None)
            if chunk is None:
                self.eof = True
                for scan in self._scans:
                    scan.close()
                break
            self._digest.update(chunk)
            self.size += len(chunk)
            for scan in self._scans:
                scan.update(chunk)
            self._pending += chunk
        count = min(len(target), len(self._pending))
        target[:count] = self._pending[:count]
        del self._pending[:count]
        return count


class PublicBase:
    """Anonymous, read-only access to published objects under an ``https://`` base or a local directory.

    A local directory laid out like the bucket stands in for the public base in
    tests and for retained receipts. Every completed read leaves a receipt of
    exactly what was received; absence raises ``MemberNotFoundError``.
    """

    def __init__(self, location: str, *, client: httpx.Client | None = None):
        self.remote = "://" in location
        self.client: httpx.Client | None = None
        if self.remote:
            from spicy_regs.public_url import resolve_r2_base_url

            self.location = resolve_r2_base_url(location)
            self.client = client or httpx.Client(timeout=120, follow_redirects=True,
                                                 headers={"User-Agent": "spicy-regs-generation-audit"})
        else:
            self.location = str(Path(location).resolve())
        self.receipts: list[dict] = []

    def path(self, key: str) -> str:
        """The URL or file path DuckDB reads ``key`` from."""
        return f"{self.location}/{key}" if self.remote else str(Path(self.location, key))

    @contextmanager
    def _chunks(self, key: str, fresh: bool) -> Iterator[tuple[Iterator[bytes], dict]]:
        from rulespec_artifacts import MemberNotFoundError

        if self.client is None:
            path = Path(self.path(key))
            if not path.is_file():
                raise MemberNotFoundError(key)
            with path.open("rb") as stream:
                yield iter(lambda: stream.read(CHUNK), b""), {}
            return
        headers = {"Cache-Control": "no-cache"} if fresh else {}
        with self.client.stream("GET", self.path(key), headers=headers) as response:
            if response.status_code == 404:
                raise MemberNotFoundError(key)
            response.raise_for_status()
            yield response.iter_bytes(CHUNK), {"status": response.status_code, "etag": response.headers.get("etag"),
                                               "content_type": response.headers.get("content-type")}

    @contextmanager
    def open(self, key: str, *, scans: Sequence[Scan] = (), fresh: bool = False) -> Iterator[BinaryIO]:
        """Stream ``key``; its receipt (bytes, SHA-256, completeness) is recorded when the stream closes."""
        with self._chunks(key, fresh) as (chunks, facts):
            raw = _Observed(chunks, scans)
            yield io.BufferedReader(raw, CHUNK)
        self.receipts.append({"key": key, **facts, "bytes": raw.size, "sha256": raw.sha256, "complete": raw.eof,
                              "observed_at": _now()})

    def read(self, key: str, *, limit: int = CONTROL_LIMIT, scans: Sequence[Scan] = (), fresh: bool = False) -> bytes:
        """The whole of a small object; one larger than ``limit`` refuses."""
        with self.open(key, scans=scans, fresh=fresh) as stream:
            raw = stream.read(limit + 1)
            if len(raw) > limit:
                raise AuditError(f"{key} exceeds the audit's {limit}-byte bound for control objects")
        return raw

    def etag(self, key: str) -> str | None:
        assert self.client is not None
        response = self.client.head(self.path(key), headers={"Cache-Control": "no-cache"})
        response.raise_for_status()
        return response.headers.get("etag")


class _ArtifactSource:
    """Rulespec member source over one published artifact prefix.

    Anonymous reads cannot list a prefix, so ``keys`` yields only the declared
    members: an undeclared extra object is invisible here (a stated limit).
    The root, manifests and journal are read once and kept; payloads stream.
    ``blobs`` redirects ``blobs/…`` keys to the shared evidence prefix.
    """

    def __init__(self, base: PublicBase, prefix: str, *, blobs: str | None = None,
                 scans: Callable[[str], Sequence[Scan]] = lambda key: ()):
        self.base, self.prefix, self.blobs, self.scans = base, prefix, blobs, scans
        self.control: dict[str, bytes] = {}
        self.observed: dict[str, dict] = {}
        self._manifests: list[str] | None = None

    def location(self, key: str) -> str:
        return f"{self.blobs}/{key}" if self.blobs and key.startswith("blobs/") else f"{self.prefix}/{key}"

    def read_control(self, key: str) -> bytes:
        if key not in self.control:
            raw = self.control[key] = self.base.read(self.location(key), scans=self.scans(key))
            self.observed[key] = {"sha256": "sha256:" + hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        return self.control[key]

    def root(self) -> dict | None:
        try:
            value = json.loads(self.read_control(ROOT))
        except _READ_ERRORS:
            return None
        return value if isinstance(value, dict) else None

    def manifests(self) -> list[str]:
        if self._manifests is None:
            refs = (self.root() or {}).get("memberManifests", [])
            self._manifests = [ref["objectKey"] for ref in refs if isinstance(ref, dict) and "objectKey" in ref]
        return self._manifests

    def keys(self) -> Iterator[str]:
        yield ROOT
        for manifest in self.manifests():
            yield manifest
            for member in json.loads(self.read_control(manifest)).get("members", []):
                if "objectKey" in member:
                    yield member["objectKey"]

    @contextmanager
    def open(self, object_key: str) -> Iterator[BinaryIO]:
        if object_key in (ROOT, JOURNAL) or object_key in self.manifests():
            yield io.BytesIO(self.read_control(object_key))
            return
        with self.base.open(self.location(object_key), scans=self.scans(object_key)) as stream:
            yield stream
        receipt = self.base.receipts[-1]
        self.observed[object_key] = {k: receipt[k] for k in ("sha256", "bytes", "complete")}


# --------------------------------------------------------------------------- #
# Declarations, DuckDB helpers and run state.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Declaration:
    """What this checkout declares for one table: ordered ``(name, type)`` columns and its identity columns."""

    columns: tuple[tuple[str, str], ...]
    identity: tuple[str, ...]
    identity_source: str


def declared_tables() -> dict[str, Declaration]:
    """Dictionary schemas; identity from the SpicyDocs contract where one exists, else the dictionary's."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.data_dictionary import build_mcp_metadata, expected_schemas, load_descriptions

    schemas = expected_schemas()
    metadata = build_mcp_metadata(load_descriptions(), schemas)
    result = {}
    for table, columns in schemas.items():
        contract = TABLE_CONTRACTS.get(table)
        identity = contract.identity if contract else tuple(metadata.get(table, {}).get("identity_columns", ()))
        result[table] = Declaration(tuple((name, kind) for name, kind in columns), tuple(identity),
                                    "spicy-docs contract" if contract else "data dictionary")
    return result


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _one(con: duckdb.DuckDBPyConnection, sql: str, params: Sequence | Mapping | None = None) -> tuple:
    row = con.execute(sql, params if params is not None else []).fetchone()
    assert row is not None
    return row


def _parquet(base: PublicBase, prefix: str, key: str) -> str:
    return f"read_parquet({_literal(base.path(f'{prefix}/{key}'))})"


def _table_info(con: duckdb.DuckDBPyConnection, source: str) -> dict:
    """Footer schema and row count in the shape a generation root records; pages are not decoded."""
    columns = con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
    return {"columns": [[row[0], row[1]] for row in columns], "rows": _one(con, f"SELECT count(*) FROM {source}")[0]}


@contextmanager
def _connect(base: PublicBase, memory_limit: str, threads: int) -> Iterator[duckdb.DuckDBPyConnection]:
    from spicy_regs.duckdb_settings import ExportResources

    with tempfile.TemporaryDirectory(prefix="generation-audit-") as spill, duckdb.connect() as con:
        con.execute(f"SET home_directory={_literal(tempfile.gettempdir())}")
        if base.remote:
            con.execute("INSTALL httpfs; LOAD httpfs")
        ExportResources(memory=memory_limit, threads=threads).configure(con, Path(spill))
        yield con


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class _Run:
    """What one audit accumulates while its sections are written."""

    samples: int
    secrets: Mapping[str, bytes]
    findings: list[dict] = field(default_factory=list)
    limits: list[str] = field(default_factory=lambda: list(LIMITS))
    scans: dict[str, Scan] = field(default_factory=dict)

    def show(self, value: object) -> str | None:
        """A sample value as reported: redacted first, then clipped, so a clip can never leave part of a key."""
        if value is None:
            return None
        text = redact(str(value), (secret.decode("utf-8", "replace") for secret in self.secrets.values()))
        return text if len(text) <= TEXT_LIMIT else text[:TEXT_LIMIT] + "…"

    def finding(self, severity: str, section: str, code: str, table: str | None = None, **detail) -> None:
        self.findings.append({"severity": severity, "section": section, "code": code, "table": table, **detail})

    def scanner(self, scope: str) -> Callable[[str], Sequence[Scan]]:
        """Scans for every non-Parquet object read under ``scope``; DuckDB scans Parquet cells instead."""

        def scans(key: str) -> Sequence[Scan]:
            if key.endswith(".parquet"):
                return ()
            scan = self.scans[f"{scope}:{key}"] = Scan(self.secrets)
            return (scan,)

        return scans


# --------------------------------------------------------------------------- #
# Publication and the prior pin.
# --------------------------------------------------------------------------- #
def _publication(con, base: PublicBase, source: _ArtifactSource, entry: Mapping, family: str, run: _Run,
                 infos: dict[str, dict]) -> tuple[dict, dict | None]:
    """Admit every member byte, then reconcile index, manifest, root and observed bytes per table."""
    from rulespec_artifacts import ArtifactPin, iter_member_descriptors

    from spicy_regs.generations import verify_generation_source

    def table_info(key: str) -> dict:
        infos[key] = _table_info(con, _parquet(base, entry["prefix"], key))
        return infos[key]

    section: dict[str, Any] = {"pin": {"artifactDigest": entry["artifactDigest"], "logicalId": entry["logicalId"]},
                               "prefix": entry["prefix"]}
    members: dict[str, dict] = {}
    try:
        artifact = verify_generation_source(source, table_info,
                                      expected_pin=ArtifactPin(entry["logicalId"], entry["artifactDigest"]))
    except _READ_ERRORS as error:
        section["admission"] = {"admitted": False, "error": f"{type(error).__name__}: {error}"}
        run.finding("fail", "publication", "generation-not-admitted", error=section["admission"]["error"])
    else:
        section["admission"] = {
            "admitted": True, "members": artifact.member_count, "bytes": artifact.total_member_byte_size,
            "verifier": "spicy_regs.generations: Rulespec admission of every declared byte, then membership, lineage, "
                        "carried-forward bytes and each table's footer schema and row count",
        }
        members = {m.object_key: {"sha256": m.sha256, "byteSize": m.byte_size, "rows": m.record_count}
                   for m in iter_member_descriptors(artifact, source) if m.object_key}
    root = source.root()
    spec = (root or {}).get("spec", {})
    root_tables = spec.get("tables", {})
    section["root"] = {
        "kind": (root or {}).get("kind"), "family": spec.get("family"), "publicationStatus": spec.get("publicationStatus"),
        "carriedForward": spec.get("carriedForward"), "parents": spec.get("parents"), "packages": spec.get("packages"),
        "inputs": (root or {}).get("inputs"),
        "capturedPrior": ((spec.get("readSnapshot") or {}).get("families", {}).get(family) or {}).get("artifactDigest"),
    }
    if spec.get("family") != family or spec.get("publicationStatus") != "complete-family":
        run.finding("fail", "publication", "root-is-not-this-complete-family", family=spec.get("family"),
                    publication_status=spec.get("publicationStatus"))
    index_only = sorted(set(entry["tables"]) - set(root_tables))
    root_only = sorted(set(root_tables) - set(entry["tables"]))
    section["membership"] = {"index_only": index_only, "root_only": root_only}
    if index_only or root_only:
        run.finding("fail", "publication", "index-and-root-membership-differ", index_only=index_only, root_only=root_only)
    tables = {}
    for key, descriptor in sorted(entry["tables"].items()):
        observed, member = source.observed.get(key, {}), members.get(key)
        checks = {
            "index_equals_manifest": member == {k: descriptor[k] for k in ("sha256", "byteSize", "rows")},
            "index_equals_root": root_tables.get(key) == {"columns": descriptor["columns"], "rows": descriptor["rows"]},
            "observed_bytes_equal_index": None if not observed else (observed["sha256"], observed["bytes"])
            == (descriptor["sha256"], descriptor["byteSize"]),
        }
        tables[key] = {"index": {k: descriptor[k] for k in ("sha256", "byteSize", "rows")}, "manifest": member,
                       "observed": observed or None, **checks}
        for check, passed in checks.items():
            if passed is False:
                run.finding("fail", "publication", check.replace("_", "-") + "-false", key.removesuffix(".parquet"))
    section["tables"] = tables
    return section, root


def _chain(base: PublicBase, root: Mapping, family: str) -> Iterator[dict]:
    """Earlier generations' index entries, newest first, each root checked against the pin that named it."""
    from rulespec_artifacts import expected_artifact_digest, parse_canonical_json

    seen: set[str] = set()
    entry = ((root.get("spec", {}).get("readSnapshot") or {}).get("families") or {}).get(family)
    while entry is not None:
        if entry["artifactDigest"] in seen or len(seen) >= CHAIN_LIMIT:
            raise AuditError(f"{family}'s captured prior chain loops or exceeds {CHAIN_LIMIT} generations")
        seen.add(entry["artifactDigest"])
        yield entry
        prior = parse_canonical_json(base.read(f"{entry['prefix']}/{ROOT}", limit=INDEX_LIMIT))
        # publication.load_family_root makes the same check, but reads only over https.
        if (not isinstance(prior, dict) or expected_artifact_digest(prior) != entry["artifactDigest"]
                or prior.get("logicalId") != entry["logicalId"]):
            raise AuditError(f"The root at {entry['prefix']} differs from the pin that named it")
        entry = ((prior.get("spec", {}).get("readSnapshot") or {}).get("families") or {}).get(family)


def _select_prior(base: PublicBase, requested: str | None, entry: Mapping, root: Mapping | None,
                  family: str) -> tuple[dict | None, str]:
    """The prior generation to conserve against and how it was chosen; a pin outside the chain raises."""
    if requested == "none":
        return None, "not requested (--prior none)"
    if root is None:
        return None, "unresolvable: the generation root is unreadable"
    if requested is None:
        first = next(_chain(base, root, family), None)
        return first, "the generation's own captured prior (readSnapshot)" if first else "none captured"
    wanted = requested.lower().removeprefix("sha256:")
    if not re.fullmatch(r"[0-9a-f]{8,64}", wanted):
        raise AuditError(f"--prior must be 8 to 64 hex digits or sha256:<hex>, not {requested!r}")
    if entry["artifactDigest"].removeprefix("sha256:").startswith(wanted):
        return dict(entry), "argument: the audited generation itself"
    for candidate in _chain(base, root, family):
        if candidate["artifactDigest"].removeprefix("sha256:").startswith(wanted):
            return candidate, "argument, found in the generation's captured prior chain"
    raise AuditError(f"Prior {requested} is not in {family}'s captured prior chain")


def _admit_prior(base: PublicBase, prior: dict | None, entry: Mapping, run: _Run,
                 sources: list[_ArtifactSource]) -> str | None:
    """Byte admission of a distinct prior; its shape rules may predate this checkout's verifier, so only bytes."""
    from rulespec_artifacts import ArtifactPin, admit_artifact

    if prior is None:
        return None
    if prior["artifactDigest"] == entry["artifactDigest"]:
        return "same generation as audited"
    source = _ArtifactSource(base, prior["prefix"])
    sources.append(source)
    try:
        admit_artifact(source, expected_pin=ArtifactPin(prior["logicalId"], prior["artifactDigest"]))
    except _READ_ERRORS as error:
        run.finding("fail", "conservation", "prior-not-admitted", error=f"{type(error).__name__}: {error}")
        return f"not admitted: {type(error).__name__}: {error}"
    return "admitted: every declared byte matched its manifest"


# --------------------------------------------------------------------------- #
# Schema, identity and conservation.
# --------------------------------------------------------------------------- #
def _schema(observed: list, index_columns: list, declaration: Declaration | None, table: str, run: _Run) -> dict:
    """Observed names and types against the complete declaration and against the index descriptor."""
    section: dict[str, Any] = {"observed": observed, "index": index_columns, "matches_index": observed == index_columns}
    if observed != index_columns:
        run.finding("fail", "schema", "observed-schema-differs-from-index", table)
    if declaration is None:
        run.finding("review", "schema", "undeclared-table", table)
        return {**section, "status": "undeclared", "declared": None}
    declared = [list(column) for column in declaration.columns]
    declared_types, observed_types = dict(declaration.columns), {name: kind for name, kind in observed}
    detail = {
        "missing_columns": [name for name, _ in declared if name not in observed_types],
        "extra_columns": [name for name, _ in observed if name not in declared_types],
        "type_mismatches": [{"column": name, "declared": kind, "observed": observed_types[name]}
                            for name, kind in declared if name in observed_types and observed_types[name] != kind],
        "order_matches": ([name for name, _ in observed if name in declared_types]
                          == [name for name, _ in declared if name in observed_types]),
    }
    matches = observed == declared
    if not matches:
        run.finding("fail", "schema", "schema-differs-from-declaration", table, **detail)
    return {**section, "status": "matches-declaration" if matches else "differs-from-declaration",
            "declared": declared, **detail}


def _identity_profile(con, view: str, identity: Sequence[str], samples: int) -> dict:
    """Duplicate and NULL identities over the identity columns only, counted before any row is paired."""
    key = "struct_pack(" + ", ".join(f"{_q(c)} := {_q(c)}" for c in identity) + ")"
    nulls = " OR ".join(f"{_q(c)} IS NULL" for c in identity)
    per_column = ", ".join(f"coalesce(sum(n) FILTER (WHERE identity.{_q(c)} IS NULL), 0)" for c in identity)
    row = _one(con, f"""
        WITH g AS (SELECT {key} AS identity, count(*) AS n, bool_or({nulls}) AS has_null FROM {view} GROUP BY ALL)
        SELECT coalesce(sum(n), 0), count(*), count_if(n > 1), coalesce(sum(n) FILTER (WHERE n > 1), 0),
               coalesce(sum(n) FILTER (WHERE has_null), 0),
               min_by(struct_pack(identity := identity, rows := n), identity, {samples}) FILTER (WHERE n > 1),
               min_by(identity, identity, {samples}) FILTER (WHERE has_null), {per_column}
        FROM g""")
    return {
        "rows": row[0], "distinct_identities": row[1], "duplicate_identities": row[2],
        "rows_in_duplicate_identities": row[3], "null_identity_rows": row[4],
        "null_rows_by_column": dict(zip(identity, row[7:], strict=True)),
        "duplicate_sample": row[5] or [], "null_sample": row[6] or [],
    }


def _identity(con, info: Mapping, declaration: Declaration | None, table: str, run: _Run) -> tuple[dict, dict | None]:
    """The current table's identity profile, or why none applies."""
    identity = list(declaration.identity) if declaration else []
    section: dict[str, Any] = {"columns": identity, "source": declaration.identity_source if declaration else None}
    missing = [c for c in identity if c not in {name for name, _ in info["columns"]}]
    if not identity:
        return {**section, "status": "no-declared-identity"}, None
    if missing:
        run.finding("fail", "identity", "identity-columns-missing", table, missing=missing)
        return {**section, "status": "identity-columns-missing", "missing": missing}, None
    if info["rows"] == 0:
        return {**section, "status": "not-applicable-empty"}, None
    profile = _identity_profile(con, "audit_current", identity, run.samples)
    duplicates, nulls = profile["duplicate_identities"], profile["null_identity_rows"]
    if duplicates:
        run.finding("fail", "identity", "duplicate-identities", table, identities=duplicates,
                    rows=profile["rows_in_duplicate_identities"])
    if nulls:
        run.finding("fail", "identity", "null-identity-parts", table, rows=nulls)
    status = "duplicates" if duplicates else "null-identity" if nulls else "unique"
    return {**section, "status": status, **profile}, profile


def _cast(side: str, column: str, retyped: set[str]) -> str:
    expression = f"{side}.{_q(column)}"
    return f"CAST({expression} AS VARCHAR)" if column in retyped else expression


def _paired(con, identity: Sequence[str], common: Sequence[str], retyped: set[str], run: _Run) -> dict:
    """One full outer join on identity: removed and added identities, and changed cells per column with samples."""
    def key(side: str) -> str:
        return "struct_pack(" + ", ".join(f"{_q(c)} := {_cast(side, c, retyped)}" for c in identity) + ")"

    on = " AND ".join(f"{_cast('p', c, retyped)} IS NOT DISTINCT FROM {_cast('c', c, retyped)}" for c in identity)
    columns = [c for c in common if c not in identity]
    select = [f"{key('p')} AS pid", f"{key('c')} AS cid", "p.__audit_side AS in_p", "c.__audit_side AS in_c"]
    both, differs, aggregates, k = "in_p AND in_c", [], [], run.samples
    for i, column in enumerate(columns):
        select += [f"{_cast('p', column, retyped)} AS p{i}", f"{_cast('c', column, retyped)} AS c{i}"]
        differs.append(f"p{i} IS DISTINCT FROM c{i}")
        aggregates += [
            f"count_if({both} AND {differs[-1]})",
            f"min_by(struct_pack(identity := pid, prior := CAST(p{i} AS VARCHAR), current := CAST(c{i} AS VARCHAR)), "
            f"pid, {k}) FILTER (WHERE {both} AND {differs[-1]})",
        ]
    row = _one(con, f"""
        WITH j AS (SELECT {', '.join(select)}
                   FROM (SELECT *, true AS __audit_side FROM audit_prior) p
                   FULL OUTER JOIN (SELECT *, true AS __audit_side FROM audit_current) c ON {on})
        SELECT count_if(in_p AND in_c IS NULL), count_if(in_c AND in_p IS NULL), count_if({both}),
               min_by(pid, pid, {k}) FILTER (WHERE in_c IS NULL), min_by(cid, cid, {k}) FILTER (WHERE in_p IS NULL),
               count_if({both} AND ({' OR '.join(differs) or 'false'})) {''.join(', ' + a for a in aggregates)}
        FROM j""")
    changed = {
        column: {"rows": row[6 + 2 * i], "sample": [
            {"identity": item["identity"], "prior": run.show(item["prior"]), "current": run.show(item["current"])}
            for item in row[7 + 2 * i] or []]}
        for i, column in enumerate(columns) if row[6 + 2 * i]
    }
    return {"identities_removed": row[0], "identities_added": row[1], "identities_paired": row[2],
            "removed_sample": row[3] or [], "added_sample": row[4] or [], "rows_changed": row[5],
            "changed_cells_by_column": changed}


def _multiset(con, common: Sequence[str], identity: Sequence[str], retyped: set[str], run: _Run) -> dict:
    """Both directions in one pass: rows one side holds more often than the other, and identities only one holds."""
    select = ", ".join(f"CAST({_q(c)} AS VARCHAR) AS {_q(c)}" if c in retyped else _q(c) for c in common)
    shown = "struct_pack(" + ", ".join(f"{_q(c)} := CAST({_q(c)} AS VARCHAR)" for c in common) + ")"
    k = run.samples
    identities, identity_columns = "", ""
    if identity:
        key = "struct_pack(" + ", ".join(f"{_q(c)} := {_q(c)}" for c in identity) + ")"
        identities = f""", i AS (SELECT {key} AS identity, max(__np) > 0 AS in_p, max(__nc) > 0 AS in_c
                               FROM g GROUP BY ALL)"""
        identity_columns = "".join(
            f", (SELECT count_if({a} AND NOT {b}) FROM i), (SELECT min_by(identity, identity, {k}) FILTER "
            f"(WHERE {a} AND NOT {b}) FROM i)" for a, b in (("in_p", "in_c"), ("in_c", "in_p")))
    row = _one(con, f"""
        WITH u AS (SELECT {select}, 1 AS __p, 0 AS __c FROM audit_prior
                   UNION ALL SELECT {select}, 0, 1 FROM audit_current),
             g AS (SELECT * EXCLUDE (__p, __c), sum(__p) AS __np, sum(__c) AS __nc FROM u
                   GROUP BY ALL){identities}
        SELECT coalesce(sum(greatest(__np - __nc, 0)), 0), coalesce(sum(greatest(__nc - __np, 0)), 0),
               min_by(struct_pack(record := {shown}, surplus := __np - __nc), {shown}, {k}) FILTER (WHERE __np > __nc),
               min_by(struct_pack(record := {shown}, surplus := __nc - __np), {shown}, {k}) FILTER (WHERE __nc > __np)
               {identity_columns}
        FROM g""")

    def sample(items) -> list:
        return [{"record": {c: run.show(v) for c, v in item["record"].items()}, "surplus": item["surplus"]}
                for item in items or []]

    result: dict[str, Any] = {"prior_rows_not_in_current": {"rows": row[0], "sample": sample(row[2])},
                              "current_rows_not_in_prior": {"rows": row[1], "sample": sample(row[3])}}
    if identity:
        result.update(identities_removed=row[4], removed_sample=row[5] or [], identities_added=row[6],
                      added_sample=row[7] or [])
    return result


def _conservation(con, table: str, current: Mapping, prior: Mapping, info: Mapping, prior_info: Mapping,
                  declaration: Declaration | None, profile: dict | None, run: _Run) -> dict:
    """Both directions against the prior generation's table: identities, rows, columns and changed cells."""
    result: dict[str, Any] = {"rows": current["rows"], "prior_rows": prior["rows"],
                              "bytes_equal": current["sha256"] == prior["sha256"]}
    if result["bytes_equal"]:
        return {**result, "status": "bytes-equal"}
    types = {name: kind for name, kind in info["columns"]}
    prior_types = {name: kind for name, kind in prior_info["columns"]}
    common = [name for name in prior_types if name in types]
    retyped = {name for name in common if prior_types[name] != types[name]}
    result.update(columns_removed=[c for c in prior_types if c not in types],
                  columns_added=[c for c in types if c not in prior_types],
                  columns_retyped=[{"column": c, "prior": prior_types[c], "current": types[c]} for c in sorted(retyped)])
    if result["columns_removed"]:
        run.finding("review", "conservation", "columns-removed", table, columns=result["columns_removed"])
    identity = list(declaration.identity) if declaration and all(c in common for c in declaration.identity) else []
    prior_profile = _identity_profile(con, "audit_prior", identity, run.samples) if identity and prior["rows"] else None
    if prior_profile:
        result["prior_identity"] = {k: prior_profile[k] for k in (
            "rows", "distinct_identities", "duplicate_identities", "null_identity_rows")}
    if identity and not any(p and p["duplicate_identities"] for p in (profile, prior_profile)):
        result["method"] = "identity-pairing (NULL-safe equality on the declared identity)"
        result.update(_paired(con, identity, common, retyped, run))
        removed = result["identities_removed"]
        differs = removed or result["identities_added"] or result["rows_changed"]
    else:
        reason = "identity is not unique on both sides" if identity else "no declared identity shared by both sides"
        run.limits.append(f"{table}: {reason}, so changed cells are not paired; multiset differences are reported in "
                          "both directions instead.")
        result.update(method="multiset (rows grouped over common columns, multiplicity kept)", reason=reason,
                      **_multiset(con, common, identity, retyped, run))
        removed = result.get("identities_removed", result["prior_rows_not_in_current"]["rows"])
        differs = result["prior_rows_not_in_current"]["rows"] or result["current_rows_not_in_prior"]["rows"]
    if removed:
        run.finding("review", "conservation", "prior-identities-or-rows-removed", table, count=removed)
    changed = differs or retyped or result["columns_removed"] or result["columns_added"]
    return {**result, "status": "changed" if changed else "equal"}


# --------------------------------------------------------------------------- #
# Evidence: admission, binding, body shapes and credentials.
# --------------------------------------------------------------------------- #
def _cell_credentials(con, columns: Sequence, secrets: Mapping[str, bytes]) -> dict:
    """Rows per column carrying a credential parameter value or a configured key value.

    Every column that can hold text is read as text, lists, structs and JSON included; only numeric, boolean and
    temporal columns are skipped.
    """
    strings = [name for name, kind in columns if not _NOT_TEXT.match(kind)]
    markers = ", ".join(_literal(marker) for marker in REDACTED)
    expressions, labels = [], []
    for column in strings:
        text = f"CAST({_q(column)} AS VARCHAR)"
        expressions.append(
            f"count_if(len(list_filter(regexp_extract_all({text}, $pattern, 2), lambda v: lower(v) NOT IN "
            f"({markers}) AND NOT starts_with(lower(v), '%3c'))) > 0)")
        labels.append((column, "parameter"))
        for i, name in enumerate(secrets):
            expressions.append(f"count_if(contains({text}, $s{i}))")
            labels.append((column, name))
    if not expressions:
        return {"columns_scanned": 0}
    params = {"pattern": CREDENTIAL_PATTERN, **{f"s{i}": v.decode("utf-8", "replace")
                                                 for i, v in enumerate(secrets.values())}}
    hits: dict[str, dict] = {}
    for (column, detector), count in zip(labels, _one(con, f"SELECT {', '.join(expressions)} FROM audit_current",
                                                      params), strict=True):
        if count:
            hits.setdefault(column, {})[detector] = count
    return {"columns_scanned": len(strings), "rows_with_hits": hits}


def _evidence(base: PublicBase, root: Mapping | None, family: str, run: _Run,
              sources: list[_ArtifactSource]) -> dict:
    """Admit the generation's source evidence, check its binding, and judge each capture's body shape."""
    from rulespec_artifacts import ArtifactPin

    from spicy_regs.source_evidence import INPUT_ROLE, PRIOR_ROLE, verify_evidence

    inputs = (root or {}).get("inputs") or []
    declared = [item for item in inputs if item.get("role") == INPUT_ROLE]
    if not declared:
        run.limits.append("The generation declares no source-evidence input, so no source response was admitted, "
                          "scanned or sniffed for it.")
        return {"status": "absent", "inputs": inputs}
    if len(declared) != 1:
        run.finding("fail", "evidence", "multiple-evidence-inputs", count=len(declared))
    pin = declared[0]
    source = _ArtifactSource(base, f"{EVIDENCE_PREFIX}/{pin['artifactDigest'].removeprefix('sha256:')}",
                             blobs=EVIDENCE_PREFIX, scans=run.scanner("evidence"))
    sources.append(source)
    section: dict[str, Any] = {"status": "declared", "pin": pin, "prefix": source.prefix}
    try:
        artifact = verify_evidence(source, expected_pin=ArtifactPin(pin["logicalId"], pin["artifactDigest"]))
    except _READ_ERRORS as error:
        section["admission"] = {"admitted": False, "error": f"{type(error).__name__}: {error}"}
        run.finding("fail", "evidence", "evidence-not-admitted", error=section["admission"]["error"])
    else:
        section["admission"] = {"admitted": True, "members": artifact.member_count,
                                "bytes": artifact.total_member_byte_size, "outcome": artifact.root["spec"]["outcome"],
                                "verifier": "spicy_regs.source_evidence.verify_evidence"}
        section["binding"] = {
            "family_matches": artifact.root["spec"].get("family") == family,
            "inherited_inputs_match_generation": artifact.root["inputs"]
            == [item for item in inputs if item.get("role") == PRIOR_ROLE],
        }
        for check, passed in section["binding"].items():
            if not passed:
                run.finding("fail", "evidence", "evidence-" + check.replace("_", "-") + "-false")
    try:
        journal = source.read_control(JOURNAL)
    except _READ_ERRORS as error:
        journal = b""
        run.finding("fail", "evidence", "journal-unreadable", error=f"{type(error).__name__}: {error}")
    events = []
    for number, line in enumerate(journal.splitlines(), 1):
        try:
            event = json.loads(line)
        except ValueError:
            event = None
        if not isinstance(event, dict):
            run.finding("fail", "evidence", "journal-line-not-an-object", line=number)
            continue
        events.append(event)
    section["journal"] = {"events": len(events), "by_event": dict(Counter(str(e.get("event")) for e in events))}
    try:
        members = {member["objectKey"] for manifest in source.manifests()
                   for member in json.loads(source.read_control(manifest)).get("members", []) if "objectKey" in member}
    except _READ_ERRORS:
        members = None
    section["body_shapes"] = _body_shapes(events, members, run)
    return section


def _body_shapes(events: Sequence[Mapping], members: set[str] | None, run: _Run) -> dict:
    """Each capture's sniffed body against what it was requested as; a 2xx body of another kind is listed.

    A capture whose URL and media type state no kind takes its stage's kind when every other capture in that
    stage that states one agrees, so an HTML error page answered as ``text/html`` by a JSON API is still judged.
    """
    captures = [e for e in events if e.get("event") == "capture" and isinstance(e.get("sha256"), str)]
    stated = [(e, expected_kind(str(e.get("requested_url") or ""), e.get("content_type"))) for e in captures]
    by_stage: dict[object, set[str]] = {}
    for event, kind in stated:
        if kind:
            by_stage.setdefault(event.get("stage"), set()).add(kind)
    kinds: Counter[str] = Counter()
    encodings: Counter[str] = Counter()
    unexpected, unjudged, unread = [], 0, 0
    for event, expected in stated:
        key = "blobs/sha256/" + event["sha256"].removeprefix("sha256:")
        if members is not None and key not in members:
            run.finding("fail", "evidence", "capture-body-not-a-member", stage=event.get("stage"), sha256=event["sha256"])
        scan = run.scans.get("evidence:" + key)
        observed = scan.kind if scan else None
        unread += observed is None
        source = "capture" if expected else None
        if expected is None and len(stage := by_stage.get(event.get("stage"), set())) == 1:
            expected, source = next(iter(stage)), "stage"
        kinds[f"{expected or 'unstated'}->{observed or 'unread'}"] += 1
        if event.get("content_encoding") not in (None, "identity", "gzip"):
            encodings[str(event.get("content_encoding"))] += 1
        status = event.get("status_code")
        if not (isinstance(status, int) and 200 <= status < 300):
            continue
        if expected is None:
            unjudged += 1
        elif observed is not None and observed != expected:
            unexpected.append({"stage": event.get("stage"), "requested_url": event.get("requested_url"),
                               "status_code": status, "content_type": event.get("content_type"),
                               "sha256": event["sha256"], "expected": expected, "expected_from": source,
                               "observed": observed})
    if unread:
        run.limits.append(f"{unread} captures' bodies were not read (admission stopped early or the body is not a "
                          "member); their shapes and credentials are unchecked.")
    if unjudged:
        run.limits.append(f"{unjudged} 2xx captures state no expected kind (format parameter, path suffix, JSON/XML "
                          "media type or their stage's); their sniffed kinds are counted, not judged.")
    if encodings:
        run.limits.append(f"Captures declaring content encodings other than gzip ({dict(encodings)}) were scanned and "
                          "sniffed as stored bytes; an encoded key or HTML body inside them is invisible.")
    return {"expected_to_observed": dict(sorted(kinds.items())), "unexpected_2xx": unexpected}


def _cited(con, base: PublicBase, prefix: str, infos: Mapping[str, dict], digests: Sequence[str]) -> dict:
    """For each digest, the tables and ``*sha256*`` string columns whose cells cite it, with or without the prefix."""
    wanted = {value: digest for digest in digests for value in (digest, digest.removeprefix("sha256:"))}
    cited: dict[str, dict] = {}
    for key, info in infos.items():
        for column in [name for name, kind in info["columns"] if kind == "VARCHAR" and "sha256" in name.lower()]:
            rows = con.execute(f"SELECT {_q(column)}, count(*) FROM {_parquet(base, prefix, key)} "
                               f"WHERE {_q(column)} IN (SELECT unnest($values)) GROUP BY 1", {"values": list(wanted)})
            for value, count in rows.fetchall():
                cited.setdefault(wanted[value], {}).setdefault(key.removesuffix(".parquet"), {})[column] = count
    return cited


def _credentials(run: _Run, cells: Mapping[str, dict], metadata: Mapping[str, Scan]) -> dict:
    """Credential hits in every scanned object, table cell and Parquet footer; configured values by name only."""
    objects = []
    for key, scan in sorted({**run.scans, **{f"parquet-metadata:{t}": s for t, s in metadata.items()}}.items()):
        if scan.pattern_count or scan.configured:
            objects.append({"object": key, "parameter_matches": scan.pattern_count, "samples": scan.pattern_hits,
                            "configured_values": sorted(scan.configured)})
            run.finding("fail", "evidence", "credential-in-object", object=key, parameter_matches=scan.pattern_count,
                        configured_values=sorted(scan.configured))
    for table, result in cells.items():
        for column, hits in result.get("rows_with_hits", {}).items():
            run.finding("fail", "evidence", "credential-in-table-cells", table, column=column, rows=hits)
    encodings = Counter(scan.encoding for scan in run.scans.values())
    kinds = Counter(scan.kind for scan in run.scans.values())
    opaque = {kind: kinds[kind] for kind in ("zip", "bzip2", "xz", "zstd") if kinds[kind]}
    if opaque:
        run.limits.append(f"Scanned objects stored as archives or compression this audit does not expand ({opaque}) "
                          "were searched only as stored bytes.")
    if encodings["gzip-undecodable"]:
        run.limits.append(f"{encodings['gzip-undecodable']} gzip objects could not be fully decoded; the remainder "
                          "of each was not scanned.")
    if not run.secrets:
        run.limits.append("No configured key values were supplied (environment or --env-file); only the parameter-name "
                          "detector ran, and a bare key without its parameter name is invisible to it.")
    return {"detectors": {"parameter": CREDENTIAL_PATTERN, "configured_values": sorted(run.secrets)},
            "objects_scanned": len(run.scans), "decoded_bytes_scanned": sum(s.decoded_bytes for s in run.scans.values()),
            "by_encoding": dict(encodings), "objects_with_hits": objects, "table_cells": dict(cells)}


# --------------------------------------------------------------------------- #
# The audit.
# --------------------------------------------------------------------------- #
def audit(base: PublicBase, *, family: str | None = None, table: str | None = None, prior: str | None = None,
          index_raw: bytes | None = None, declarations: Mapping[str, Declaration] | None = None,
          secrets: Mapping[str, str] | None = None, samples: int = 5, memory_limit: str = "2GB", threads: int = 2,
          retain: Path | None = None) -> dict:
    """Audit one family's (or one table's) current generation against an optional prior pin; never writes remotely."""
    run = _Run(samples=samples, secrets={name: value.encode() for name, value in (secrets or {}).items()})
    started = _now()
    raw_index = index_raw if index_raw is not None else base.read(INDEX_KEY, limit=INDEX_LIMIT, fresh=True)
    index = parse_index(raw_index)
    family, keys = _select(index, family, table)
    entry = index["families"][family]
    declarations = declared_tables() if declarations is None else declarations
    sources = [_ArtifactSource(base, entry["prefix"], scans=run.scanner("generation"))]
    infos: dict[str, dict] = {}
    with _connect(base, memory_limit, threads) as con:
        publication, root = _publication(con, base, sources[0], entry, family, run, infos)
        prior_entry, reason = _select_prior(base, prior, entry, root, family)
        prior_section: dict[str, Any] = {"requested": prior, "selected_by": reason,
                                         "admission": _admit_prior(base, prior_entry, entry, run, sources)}
        if prior_entry is not None:
            prior_section.update({k: prior_entry[k] for k in ("artifactDigest", "logicalId", "prefix")})
            if table is None:
                prior_section["tables_removed"] = sorted(set(prior_entry["tables"]) - set(entry["tables"]))
                for key in prior_section["tables_removed"]:
                    run.finding("review", "conservation", "table-removed", key.removesuffix(".parquet"))
        sections: dict[str, dict] = {"publication": publication, "prior": prior_section, "schema": {}, "identity": {},
                                     "conservation": {}, "evidence": {}, "state": {}}
        cells, metadata = {}, {}
        for key in keys:
            name = key.removesuffix(".parquet")
            try:
                info = infos.get(key) or _table_info(con, _parquet(base, entry["prefix"], key))
                con.execute(f"CREATE OR REPLACE VIEW audit_current AS SELECT * FROM "
                            f"{_parquet(base, entry['prefix'], key)}")
            except _UNREADABLE as error:
                run.finding("fail", "publication", "table-unreadable", name, error=str(error))
                for part in ("schema", "identity", "conservation"):
                    sections[part][name] = {"status": "unreadable"}
                sections["state"][name] = {"rows": None, "state": "unreadable"}
                continue
            infos[key] = info
            descriptor = entry["tables"][key]
            declaration = declarations.get(name)
            sections["schema"][name] = _schema(info["columns"], descriptor["columns"], declaration, name, run)
            sections["state"][name] = {"rows": info["rows"], "state": "empty" if info["rows"] == 0 else "populated"}
            sections["identity"][name], profile = _identity(con, info, declaration, name, run)
            try:
                sections["conservation"][name] = _conserve(con, base, name, key, descriptor, info, prior_entry,
                                                           declaration, profile, run)
            except _UNREADABLE as error:
                run.finding("fail", "conservation", "prior-table-unreadable", name, error=str(error))
                sections["conservation"][name] = {"status": "prior-unreadable"}
            if info["rows"]:
                cells[name] = _cell_credentials(con, info["columns"], run.secrets)
            metadata[name] = scan_bytes(b"\n".join(bytes(k) + b"=" + bytes(v) for k, v in con.execute(
                f"SELECT key, value FROM parquet_kv_metadata({_literal(base.path(entry['prefix'] + '/' + key))})"
            ).fetchall()), run.secrets)
        evidence = sections["evidence"] = _evidence(base, root, family, run, sources)
        unexpected = evidence.get("body_shapes", {}).get("unexpected_2xx", [])
        if unexpected:
            cited = _cited(con, base, entry["prefix"], {k: infos[k] for k in keys if k in infos},
                           [item["sha256"] for item in unexpected])
            for item in unexpected:
                item["cited_by"] = cited.get(item["sha256"], {})
                severity = "fail" if item["cited_by"] and item["observed"] == "html" else "review"
                run.finding(severity, "evidence", "2xx-body-is-not-the-expected-kind", expected=item["expected"],
                            observed=item["observed"], stage=item["stage"], sha256=item["sha256"],
                            cited_by=item["cited_by"])
        evidence["credentials"] = _credentials(run, cells, metadata)
    sections["consistency"] = _consistency(base, entry, prior_entry, keys, run)
    if retain is not None:
        _retain(retain, raw_index, sources)
    severities = Counter(finding["severity"] for finding in run.findings)
    report = {
        "format": FORMAT, "version": 1, "started_at": started, "finished_at": _now(),
        "tool": {"implementation": _implementation(), "packages": {
            name: version(name) for name in ("spicy-regs", "spicy-docs", "rulespec-artifacts", "duckdb")}},
        "base": base.location,
        "index": {"source": "supplied" if index_raw is not None else "live", "bytes": len(raw_index),
                  "sha256": "sha256:" + hashlib.sha256(raw_index).hexdigest()},
        "family": family, "tables": [key.removesuffix(".parquet") for key in keys],
        "summary": {"fail": severities["fail"], "review": severities["review"],
                    "empty_tables": sorted(n for n, s in sections["state"].items() if s["state"] == "empty")},
        "scope": {
            "publication_verification": "assessed: index entry, admission of every member byte, descriptor agreement",
            "structure": "assessed: schema against the declaration, identity duplicates and NULLs, conservation",
            "evidence": "assessed: admission, binding, credential and body-shape scans",
            "empty_or_uncomputed_state": "reported per table as state, never as a failure or a qualification",
            "source_qualification": "not assessed",
            "deployment": "not assessed",
        },
        "dispositions": _dispositions(sections, run),
        "findings": run.findings, "sections": sections, "limits": list(dict.fromkeys(run.limits)),
        "receipts": base.receipts,
    }
    return _scrubbed(report, [secret.decode("utf-8", "replace") for secret in run.secrets.values()])


def _conserve(con, base: PublicBase, name: str, key: str, descriptor: Mapping, info: Mapping, prior: Mapping | None,
              declaration: Declaration | None, profile: dict | None, run: _Run) -> dict:
    if prior is None:
        return {"status": "no-prior"}
    if key not in prior["tables"]:
        return {"status": "table-added"}
    source = _parquet(base, prior["prefix"], key)
    con.execute(f"CREATE OR REPLACE VIEW audit_prior AS SELECT * FROM {source}")
    return _conservation(con, name, descriptor, prior["tables"][key], info, _table_info(con, source), declaration,
                         profile, run)


def _dispositions(sections: Mapping[str, dict], run: _Run) -> dict:
    """Per output, one word per section; source qualification and deployment are never assessed here."""
    failed = {f["table"] for f in run.findings if f["section"] == "publication"}
    admitted = sections["publication"]["admission"]["admitted"]
    return {
        name: {"publication": "verified" if admitted and not failed & {None, name} else "failed",
               "schema": sections["schema"][name]["status"], "identity": sections["identity"][name]["status"],
               "conservation": sections["conservation"][name]["status"], "state": sections["state"][name]["state"],
               "source_qualification": "not-assessed", "deployment": "not-assessed"}
        for name in sections["schema"]
    }


def _select(index: Mapping, family: str | None, table: str | None) -> tuple[str, list[str]]:
    if table is not None:
        key = table.removesuffix(".parquet") + ".parquet"
        owner = table_owner(index, key)
        if owner is None:
            raise AuditError(f"{key} is not in the publication index; only managed generations can be audited")
        return owner[0], [key]
    if family not in index["families"]:
        raise AuditError(f"Family {family!r} is not in the publication index")
    return family, sorted(index["families"][family]["tables"])


def _consistency(base: PublicBase, entry: Mapping, prior: Mapping | None, keys: Sequence[str], run: _Run) -> dict:
    """For an https base, each table's ETag after analysis against the ETag of its digest stream."""
    if not base.remote:
        run.limits.append("The base is a local directory; HTTP transport and ETag stability were not exercised.")
        return {"checked": False}
    run.limits.append("DuckDB range reads are separate requests from the digest stream; table ETags were re-read after "
                      "analysis and compared with the digest stream's.")
    streamed = {receipt["key"]: receipt.get("etag") for receipt in base.receipts if receipt.get("complete")}
    stable = {}
    for prefix in [entry["prefix"], *([prior["prefix"]] if prior else [])]:
        for location in (f"{prefix}/{key}" for key in keys if f"{prefix}/{key}" in streamed):
            stable[location] = base.etag(location) == streamed[location]
            if not stable[location]:
                run.finding("fail", "consistency", "etag-changed-during-audit", location=location)
    return {"checked": True, "etag_stable": stable}


def _retain(directory: Path, raw_index: bytes, sources: Sequence[_ArtifactSource]) -> None:
    """Write the index and every control object read (roots, manifests, journal) under their published keys."""
    objects = {INDEX_KEY: raw_index}
    for source in sources:
        objects.update({source.location(key): raw for key, raw in source.control.items()})
    for key, raw in objects.items():
        target = directory / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)


def _scrubbed(value: Any, secrets: Sequence[str]) -> Any:
    """``value`` with configured values and credential parameters redacted from every string, by this module's
    spellings and then by the owner's scrubber."""
    from spicy_docs.transport.credentials import scrub_credential

    if isinstance(value, str):
        return scrub_credential(redact(value, secrets), *secrets)
    if isinstance(value, dict):
        return {key: _scrubbed(item, secrets) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrubbed(item, secrets) for item in value]
    return value


def _implementation() -> str:
    from spicy_regs.generations import implementation_id

    return implementation_id()


def main(argv: Sequence[str] | None = None) -> int:
    """Print or write one report; exit 1 when any finding is a failure, 2 when the audit could not run."""
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--family", help="Family in the publication index")
    target.add_argument("--table", help="One table; its family is the index entry that owns it")
    parser.add_argument("--prior", help="Prior pin (8+ hex digits or sha256:...) from the generation's captured chain; "
                                        "'none' skips conservation (default: the captured prior)")
    parser.add_argument("--base", help="Public base URL or a local directory laid out like the bucket "
                                       "(default: SPICY_REGS_R2_URL, R2_PUBLIC_URL, then the project default)")
    parser.add_argument("--index", type=Path, help="Audit against this frozen publication.json, not the live one")
    parser.add_argument("--output", type=Path, help="Write the JSON report here (default: stdout)")
    parser.add_argument("--retain", type=Path, help="Write the index, roots, manifests and journal read here")
    parser.add_argument("--env-file", type=Path, action="append", default=[],
                        help="Also search for every value in this env file except URLs and paths (reported by "
                             "name only); repeatable")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--memory-limit", default="2GB")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args(argv)
    try:
        from spicy_regs.public_url import resolve_r2_base_url

        report = audit(PublicBase(args.base or resolve_r2_base_url()), family=args.family, table=args.table,
                       prior=args.prior, index_raw=args.index.read_bytes() if args.index else None,
                       secrets=configured_secrets(args.env_file), samples=args.samples,
                       memory_limit=args.memory_limit, threads=args.threads, retain=args.retain)
    except (AuditError, PublicationError, OSError, httpx.HTTPError, duckdb.Error, ValueError) as error:
        print(f"Generation audit could not run: {error}", file=sys.stderr)
        return 2
    text = json.dumps(report, indent=1, default=str) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    summary = report["summary"]
    print(f"{report['family']} {report['sections']['publication']['pin']['artifactDigest'][7:15]}: "
          f"fail={summary['fail']} review={summary['review']} empty={summary['empty_tables']}", file=sys.stderr)
    return 1 if summary["fail"] else 0
