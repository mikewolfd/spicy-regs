"""Regulations.gov agency codes, Federal Register organizations, and reviewed aliases from RefSpec View 1.4.

Agency Registry View 1.4 (REF-072 / REF-075 / REF-077, release ``agency-registry-2026-09-26``)
is the single digest-pinned agency artifact vendored under ``reference/refspec/agency-registry-view/``.
It unifies:

- REF-038's one-way projection from Regulations.gov agency codes onto Federal Register, eCFR, and
  Federal Hierarchy organizations (321 resolved rows and 10 documented abstentions);
- 13 owner-reviewed identity bridges (``sameEntityAs``) from Federal Register agencies to eCFR or
  Federal Hierarchy organizations;
- 10 dated succession events (15 result rows) plus RefSpec's sealed current-successors table (15 rows);
- 11 recorded non-emissions (4 bridge/no-FR items plus 7 owner-rejected aliases);
- 157 owner-reviewed free-text aliases (63 batch 1 + 94 batch 2, keyed by RefSpec ``name_key`` v1);
- all 472 pinned Federal Register organizations (names, short names, parents, REF-038 codes, successors).

The lineage rule reads no date, the view's effective dates included: every document takes today's
lineage whatever its own date, so a 1998 Health Care Finance Administration notice is CMS. All
files are RefSpec's bytes, vendored in ``reference/refspec/agency-registry-view/`` and never edited
here; the loader refuses any copy whose sha256 is not the pinned one.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import defaultdict
from functools import cache, lru_cache
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Any, Iterable, Mapping, NamedTuple, Sequence

import duckdb

#: Published match-key contract version for View 1.4 aliases (RefSpec REF-075 D6).
NAME_KEY_VERSION = "1"

#: Pinned digests of the vendored Agency Registry View 1.4 (RefSpec REF-077, commit a3e7f5a6 / PR #2
#: 96c62e00, schema 1.4); the manifest's is the pin RefSpec's build tool names. See
#: ``reference/refspec/README.md``.
AGENCY_REGISTRY_MANIFEST_SHA256 = "c89935debe320e35aaf748ea0519457c1618b49e13d79ddb33cbc30051b5a118"
AGENCY_REGISTRY_BRIDGES_SHA256 = "2e33905b475c6a1adf27960ecf170a1b4c82df2baf20ac13df9307bb687dd898"
AGENCY_REGISTRY_EVENTS_SHA256 = "fce2b80a184fd0de98e2d15ba2d3a68d47f84eb9b9ba1c2678bc8787af3e0c07"
AGENCY_REGISTRY_NON_EMISSIONS_SHA256 = "7819b99bd132cd48a08f4c918e8faf20ae78cac8c0ec9ad41a6cb6327b8f2f3e"
AGENCY_REGISTRY_CURRENT_SUCCESSORS_SHA256 = "857abd0ca7c42a50bab7225d1036df2e498d91e1840a2007440fd10acdf97160"
AGENCY_REGISTRY_ALIASES_SHA256 = "74bba3bdccaf3e2b6111b8320d695f74c777d3a405de73e04a0475c28df834a9"
AGENCY_REGISTRY_ORGANIZATIONS_SHA256 = "426f3082277f07f8b75d3e154d3065d6f478f09de789393ecdcd647116d76177"
AGENCY_PROJECTION_SHA256 = "9b0296b69a8d009199607929841fe8dc9a20f528319988e8d70eaaeab88c5303"
AGENCY_PROJECTION_UNRESOLVED_SHA256 = "7aa01a441ba254575aac427d72f0c593ace5a1f3d177b6bc8f927c1967a43ca6"

#: The vendored Agency Registry View 1.4 directory (manifest plus all 8 tables under ``tables/``).
AGENCY_REGISTRY_VIEW_PATH = files("spicy_regs").joinpath("reference/refspec/agency-registry-view")

_REGISTRY_TABLE_FILES: Mapping[str, tuple[str, str]] = {
    "bridges": ("tables/agency-registry-bridges.parquet", AGENCY_REGISTRY_BRIDGES_SHA256),
    "events": ("tables/agency-registry-events.parquet", AGENCY_REGISTRY_EVENTS_SHA256),
    "non-emissions": ("tables/agency-registry-non-emissions.parquet", AGENCY_REGISTRY_NON_EMISSIONS_SHA256),
    "current-successors": ("tables/agency-registry-current-successors.parquet", AGENCY_REGISTRY_CURRENT_SUCCESSORS_SHA256),
    "aliases": ("tables/agency-registry-aliases.parquet", AGENCY_REGISTRY_ALIASES_SHA256),
    "organizations": ("tables/agency-registry-organizations.parquet", AGENCY_REGISTRY_ORGANIZATIONS_SHA256),
    "projection": ("tables/agency-projection.parquet", AGENCY_PROJECTION_SHA256),
    "projection-unresolved": ("tables/agency-projection-unresolved.parquet", AGENCY_PROJECTION_UNRESOLVED_SHA256),
}

#: The RefSpec URN prefix of a Federal Register agency, followed by its FR agency id.
FR_AGENCY_URN = "urn:ref:federal-register-agency:"


def name_key(text: str) -> str:
    """RefSpec's published alias match key (version ``1``, REF-075 D6).

    NFKC-normalized and case-folded; ``&`` read as ``and``; parentheticals dropped; every run of
    non-alphanumeric characters collapsed to one space; then ``the``, ``u s``, ``us``, ``united
    states``, and ``of america`` dropped wherever they stand. Two strings name the same alias
    exactly when their keys are equal.
    """
    folded = unicodedata.normalize("NFKC", text).casefold().replace("&", " and ")
    folded = " ".join(re.sub(r"[^a-z0-9]+", " ", re.sub(r"\([^)]*\)", " ", folded)).split())
    return " ".join(re.sub(r"\b(the|u s|us|united states|of america)\b", " ", folded).split())


@cache
def _read_pinned(path: Any, sha256: str) -> tuple[Mapping[str, Any], ...]:
    """Read a vendored RefSpec table, raising unless the bytes match the pinned digest."""
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != sha256:
        raise ValueError(f"{path.name} is not RefSpec's file: sha256 {digest}, pinned {sha256}")
    # DuckDB, not pyarrow: the MCP image installs no pyarrow (deploy/cloudflare/Dockerfile;
    # tests/test_mcp_lightweight_imports.py), and lookup_agency reads these files. It reads the
    # admitted bytes, not the caller's path again.
    with TemporaryDirectory(prefix="spicy-regs-agency-") as directory:
        admitted = Path(directory) / "table.parquet"
        admitted.write_bytes(data)
        with duckdb.connect() as connection:
            cursor = connection.execute("SELECT * FROM read_parquet(?)", [str(admitted)])
            columns = [column[0] for column in cursor.description]
            return tuple(MappingProxyType(dict(zip(columns, row, strict=True))) for row in cursor.fetchall())


def registry_publication() -> dict[str, Any]:
    """The vendored Agency Registry View 1.4 identity: view id, schema version, release, digest, and table pins.

    The manifest is read and its bytes held to the pinned digest on every call, as a lookup always did.
    """
    raw = AGENCY_REGISTRY_VIEW_PATH.joinpath("view-manifest.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != AGENCY_REGISTRY_MANIFEST_SHA256:
        raise ValueError("RefSpec registry manifest differs from its pinned bytes")
    manifest = json.loads(raw)
    return {
        "view_id": manifest["viewId"],
        "schema_version": manifest["schemaVersion"],
        "manifest_sha256": "sha256:" + AGENCY_REGISTRY_MANIFEST_SHA256,
        "release": manifest["release"],
        "digest": manifest["digest"],
        "counts": manifest["counts"],
        "table_sha256": {name: "sha256:" + sha256 for name, (_, sha256) in _REGISTRY_TABLE_FILES.items()},
    }


def registry_rows(table: str) -> tuple[Mapping[str, Any], ...]:
    """Read one vendored View 1.4 table (cached and SHA-256 verified): ``bridges``, ``events``,
    ``non-emissions``, ``current-successors``, ``aliases``, ``organizations``, ``projection``, or
    ``projection-unresolved``."""
    relpath, sha256 = _REGISTRY_TABLE_FILES[table]
    path = AGENCY_REGISTRY_VIEW_PATH.joinpath(relpath)
    return _read_pinned(path, sha256)


def projection_rows() -> tuple[Mapping[str, Any], ...]:
    """The vendored REF-038 projection rows inside View 1.4, read once and cached."""
    return registry_rows("projection")


def unresolved_rows() -> tuple[Mapping[str, Any], ...]:
    """The vendored REF-038 abstentions inside View 1.4, read once and cached."""
    return registry_rows("projection-unresolved")


def alias_rows() -> tuple[Mapping[str, Any], ...]:
    """The 157 owner-reviewed free-text aliases in View 1.4, read once and cached."""
    return registry_rows("aliases")


def organization_rows() -> tuple[Mapping[str, Any], ...]:
    """All 472 pinned Federal Register organizations in View 1.4, read once and cached."""
    return registry_rows("organizations")


@cache
def _alias_index(view_path: Any) -> MappingProxyType[str, Mapping[str, Any]]:
    del view_path
    return MappingProxyType({str(row["name_key"]): row for row in alias_rows()})


@cache
def _org_by_fr_id_index(view_path: Any) -> MappingProxyType[int, Mapping[str, Any]]:
    del view_path
    return MappingProxyType({int(row["fr_id"]): row for row in organization_rows()})


@cache
def _org_by_name_key_index(view_path: Any) -> MappingProxyType[str, Mapping[str, Any]]:
    del view_path
    return MappingProxyType({name_key(str(row["name"])): row for row in organization_rows()})


def lookup_alias(name: str) -> Mapping[str, Any] | None:
    """Return the owner-reviewed View 1.4 alias row matching ``name_key(name)``, or ``None``."""
    key = name_key(name)
    if not key:
        return None
    return _alias_index(AGENCY_REGISTRY_VIEW_PATH).get(key)


def organization_by_fr_id(fr_agency_id: int) -> Mapping[str, Any] | None:
    """Return the pinned Federal Register organization row for ``fr_agency_id``, or ``None``."""
    return _org_by_fr_id_index(AGENCY_REGISTRY_VIEW_PATH).get(fr_agency_id)


def organization_by_name(name: str) -> Mapping[str, Any] | None:
    """Return the pinned Federal Register organization row matching ``name_key(name)``, or ``None``."""
    key = name_key(name)
    if not key:
        return None
    return _org_by_name_key_index(AGENCY_REGISTRY_VIEW_PATH).get(key)


class _Projection(NamedTuple):
    """The reverse lookup: FR agency id -> its one code, and the ancestor orgs of each org whose parent is stated."""

    code_by_fr_id: MappingProxyType[int, str]
    ancestors_by_org: MappingProxyType[str, frozenset[str]]


def _build_projection(
    rows: Iterable[Mapping[str, Any]],
    bridges: Iterable[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
    successors: Iterable[Mapping[str, Any]],
) -> _Projection:
    """Reverse REF-038's rows, then follow the registry's bridges and each original's current successors.

    A bridge adds to its FR subject every code that selects its object, with the subject's
    roster parent. ``successors`` is the view's sealed current-successors table, one row per
    (original, successor): RefSpec walks each original's events to the results no later event
    replaced, so a chain reaches its end and a split keeps every result. An original whose current
    successors are all coded takes the codes that select them, bridged codes included, **in place
    of** any code that selects the original itself: the current successor wins (owner rule, round 5),
    so the Food and Nutrition Service, which REF-038 codes FNS and which is now the Food and
    Nutrition Administration (FNA), is FNA. An uncoded successor is unknown, not absent, so the
    original then takes no code at all. An org resolves only where exactly one code selects it, so
    a split resolves only when all its successors agree on one code. An original takes the roster
    parent its event states in ``original_parents``, None for a top-level agency, so every coded
    org's parent is stated; a second source stating another parent for an org is refused.
    """
    codes: dict[str, set[str]] = defaultdict(set)
    parents: dict[str, str | None] = {}

    def state_parent(org: str, parent: str | None, source: str) -> None:
        if parents.setdefault(org, parent) != parent:
            raise ValueError(f"{source} states another parent for {org}")

    for row in rows:
        codes[row["org"]].add(row["source_value"])
        parents[row["org"]] = row["parent_org"]
    for bridge in bridges:  # an FR subject, an eCFR or Federal Hierarchy object: no bridge reaches another
        state_parent(bridge["subject"], bridge["subject_parent"], f"bridge {bridge['candidate_id']}")
        codes[bridge["subject"]] |= codes.get(bridge["object"], set())
    for row in events:
        for original, parent in zip(row["originals"], row["original_parents"], strict=True):
            state_parent(original, parent, f"event {row['event_id']}")
    current: dict[str, set[str]] = defaultdict(set)
    for row in successors:
        current[row["original"]].add(row["successor"])
    unknown: set[str] = set()
    for original, found_successors in current.items():  # a current successor is no original, so order is free
        found = [codes.get(successor, set()) for successor in found_successors]
        if all(found):
            codes[original] = set().union(*found)
        else:
            unknown.add(original)
    ancestors: dict[str, frozenset[str]] = {}

    def ancestors_of(org: str) -> frozenset[str]:
        chain = ancestors.get(org)
        if chain is None:
            parent = parents.get(org)
            chain = frozenset({parent, *ancestors_of(parent)}) if parent else frozenset()
            ancestors[org] = chain
        return chain

    return _Projection(
        MappingProxyType(
            {
                int(org.removeprefix(FR_AGENCY_URN)): next(iter(selecting))
                for org, selecting in codes.items()
                if org.startswith(FR_AGENCY_URN) and len(selecting) == 1 and org not in unknown
            }
        ),
        MappingProxyType({org: ancestors_of(org) for org in parents}),
    )


@lru_cache(maxsize=1)
def _projection() -> _Projection:
    """The reverse lookup over the vendored View 1.4 projection and registry tables, built once."""
    return _build_projection(
        projection_rows(), registry_rows("bridges"), registry_rows("events"), registry_rows("current-successors")
    )


def fr_agency_code(fr_agency_id: int) -> str | None:
    """The one Regulations.gov code for Federal Register agency ``fr_agency_id``, or ``None``.

    A code selects the agency directly (REF-038), through an identity bridge (Energy Department
    is DOE), or through its current successors when every one is coded and they give one code
    between them (the Health Care Finance Administration is CMS); a current successor's code wins
    over the agency's own (the Food and Nutrition Service is FNA). ``None`` when no code selects
    it, or several do: INS, split to three agencies with three codes, has none, and so would a
    split with any uncoded successor.
    """
    return _projection().code_by_fr_id.get(fr_agency_id)


def agency_code_for_fr_agencies(agencies: Sequence[dict[str, Any]]) -> str | None:
    """The one Regulations.gov code for an FR row's parsed ``agencies_json`` entries, or ``None``.

    Each entry's own code is ``fr_agency_code``'s. An org named alongside its descendant is dropped by the parent chain, however many levels
    apart (Transportation Department with FAA is FAA; Agriculture Department with GIPSA, its
    grandchild, is GIPSA). The chain is the roster's, as the vendored files state it: REF-038's
    ``parent_org``, a bridge's ``subject_parent`` or an event's ``original_parents`` (Health and
    Human Services with the Health Care Finance Administration is CMS); a coded agency's entry
    ``parent_id`` never moves it. Every agency on the document, resolved or not, must then be
    compatible with the chosen org C: it is C or an ancestor of C by that chain, or its FR
    ``parent_id`` is C or an org whose projection chain contains C — a department signing with
    one of its own unresolved bureaus (Treasury with the Bureau of the Fiscal Service) is the
    department. Anything else is a joint document and gives ``None`` (EPA with Interior's Bureau
    of Mines). Malformed entries — a non-dict, a missing or non-integer ``id`` — are skipped.
    """
    projection = _projection()
    ancestors_of = projection.ancestors_by_org  # it holds every coded org: each one's parent is stated
    code_by_org: dict[str, str] = {}
    named: list[tuple[int, int | None]] = []
    for entry in agencies:
        fr_id = entry.get("id") if isinstance(entry, dict) else None
        if not isinstance(fr_id, int) or isinstance(fr_id, bool):
            continue
        parent_id = entry.get("parent_id")
        parent_id = parent_id if isinstance(parent_id, int) and not isinstance(parent_id, bool) else None
        named.append((fr_id, parent_id))
        if (code := projection.code_by_fr_id.get(fr_id)) is not None:
            code_by_org[f"{FR_AGENCY_URN}{fr_id}"] = code
    specific = {
        org for org in code_by_org if not any(org in ancestors_of[other] for other in code_by_org if other != org)
    }
    if len(specific) != 1:
        return None
    chosen = next(iter(specific))
    chosen_id = int(chosen.removeprefix(FR_AGENCY_URN))
    allowed = {chosen, *ancestors_of[chosen]}
    for fr_id, parent_id in named:
        if f"{FR_AGENCY_URN}{fr_id}" in allowed:
            continue
        if parent_id is not None and (
            parent_id == chosen_id or chosen in ancestors_of.get(f"{FR_AGENCY_URN}{parent_id}", frozenset())
        ):
            continue
        return None
    return code_by_org[chosen]
