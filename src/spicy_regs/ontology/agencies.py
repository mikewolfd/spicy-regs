"""Regulations.gov agency codes for Federal Register agencies, through RefSpec's projection and agency registry.

REF-038 projects each code onto one roster organization. The agency registry view (REF-072,
batch 1) adds identity bridges from Federal Register agencies to organizations codes select,
and successions from defunct agencies to the organizations holding their functions now. The
rule reads no date, the view's effective dates included: every document takes today's lineage
whatever its own date, so a 1998 Health Care Finance Administration notice is CMS.
Both are RefSpec's bytes, vendored in ``reference/refspec/`` and never edited here; the loader
refuses any copy whose sha256 is not the pinned one.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from functools import cache, lru_cache
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Any, Iterable, Mapping, NamedTuple, Sequence

import duckdb

#: Pinned digests of the three vendored REF-038 files; see ``reference/refspec/README.md``.
AGENCY_PROJECTION_SHA256 = "c9ec0fde1bf5fda17402983880bc091e9caa417845178f232214606e264c049f"
AGENCY_PROJECTION_UNRESOLVED_SHA256 = "e32e814c3c7489d82df6dbdbc00fd6e16694628c08e7300a9473bcaa0659b065"
VIEW_MANIFEST_SHA256 = "991acd29368b17fb66eea770f36c71385c5faee1a368008a4240281e4c8536d8"

#: Pinned digests of the vendored agency registry view (RefSpec cd78e476, schema 1.1); the
#: manifest's is the pin RefSpec's build tool names. See ``reference/refspec/README.md``.
AGENCY_REGISTRY_MANIFEST_SHA256 = "c7dc9310f9c11cd346245d7cf882f9eaf69b70b25f59841ae6004dca4944866e"
AGENCY_REGISTRY_BRIDGES_SHA256 = "2e33905b475c6a1adf27960ecf170a1b4c82df2baf20ac13df9307bb687dd898"
AGENCY_REGISTRY_EVENTS_SHA256 = "72f35636f9b5c93d708a364352122fb724e872f619a518c5e49ebb19518322b7"
AGENCY_REGISTRY_NON_EMISSIONS_SHA256 = "da863e467f00f16a6b7a9ff1a3e1182fb8e7488f8b7d33f0d7f0c403a0663e24"

#: The vendored projection table and its abstentions, and the registry view's directory (manifest plus ``tables/``).
AGENCY_PROJECTION_PATH = files("spicy_regs").joinpath("reference/refspec/agency-projection.parquet")
AGENCY_PROJECTION_UNRESOLVED_PATH = files("spicy_regs").joinpath("reference/refspec/agency-projection-unresolved.parquet")
AGENCY_REGISTRY_VIEW_PATH = files("spicy_regs").joinpath("reference/refspec/agency-registry-view")

_REGISTRY_TABLE_SHA256 = {
    "bridges": AGENCY_REGISTRY_BRIDGES_SHA256,
    "events": AGENCY_REGISTRY_EVENTS_SHA256,
    "non-emissions": AGENCY_REGISTRY_NON_EMISSIONS_SHA256,
}

#: The RefSpec URN prefix of a Federal Register agency, followed by its FR agency id.
FR_AGENCY_URN = "urn:ref:federal-register-agency:"


@cache
def _read_pinned(path: Any, sha256: str) -> tuple[Mapping[str, Any], ...]:
    """Read a vendored RefSpec table, raising unless the bytes match the pinned digest."""
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != sha256:
        raise ValueError(f"{path.name} is not RefSpec's file: sha256 {digest}, pinned {sha256}")
    # DuckDB, not pyarrow: the MCP image installs no pyarrow (deploy/cloudflare/Dockerfile;
    # tests/test_mcp_lightweight_imports.py), and lookup_agency reads these files. It reads the
    # admitted bytes, not the caller's path again. Rows equal pyarrow's on all four pinned files
    # (2026-09-27); pyarrow's single-thread read, which avoided its CPU pool hanging at exit, is gone.
    with TemporaryDirectory(prefix="spicy-regs-agency-") as directory:
        admitted = Path(directory) / "projection.parquet"
        admitted.write_bytes(data)
        with duckdb.connect() as connection:
            cursor = connection.execute("SELECT * FROM read_parquet(?)", [str(admitted)])
            columns = [column[0] for column in cursor.description]
            return tuple(MappingProxyType(dict(zip(columns, row, strict=True))) for row in cursor.fetchall())


def projection_rows() -> tuple[Mapping[str, Any], ...]:
    """The vendored REF-038 projection rows, read once and cached."""
    return _read_pinned(AGENCY_PROJECTION_PATH, AGENCY_PROJECTION_SHA256)


def unresolved_rows() -> tuple[Mapping[str, Any], ...]:
    """The vendored REF-038 abstentions (codes the projection declined to map), read once and cached."""
    return _read_pinned(AGENCY_PROJECTION_UNRESOLVED_PATH, AGENCY_PROJECTION_UNRESOLVED_SHA256)


def registry_publication() -> dict[str, Any]:
    """The vendored registry view's identity: its view id, release and digest, and every pinned table digest.

    The manifest is read and its bytes held to the pinned digest on every call, as a lookup always did.
    """
    raw = AGENCY_REGISTRY_VIEW_PATH.joinpath("view-manifest.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != AGENCY_REGISTRY_MANIFEST_SHA256:
        raise ValueError("RefSpec registry manifest differs from its pinned bytes")
    manifest = json.loads(raw)
    return {
        "view_id": manifest["viewId"],
        "manifest_sha256": "sha256:" + AGENCY_REGISTRY_MANIFEST_SHA256,
        "release": manifest["release"],
        "digest": manifest["digest"],
        "table_sha256": {name: "sha256:" + digest for name, digest in _REGISTRY_TABLE_SHA256.items()},
    }


def registry_rows(table: str) -> tuple[Mapping[str, Any], ...]:
    """The vendored registry view's ``bridges``, ``events`` or ``non-emissions`` rows, read once and cached."""
    path = AGENCY_REGISTRY_VIEW_PATH.joinpath(f"tables/agency-registry-{table}.parquet")
    return _read_pinned(path, _REGISTRY_TABLE_SHA256[table])


def _current_successors(events: Iterable[Mapping[str, Any]]) -> dict[str, frozenset[str]]:
    """Each event's original read forward to the results no later event replaced; a cycle is refused.

    RefSpec's ``current_agency_successors()`` over the view's event rows, one per (event,
    result), and its iterative depth-first walk, so no chain depth reaches the recursion limit:
    every chain is walked to its end, a split keeps every result, and each organization's answer
    is settled once, after all its results, and reused along every chain through it.
    """
    results_of: dict[str, set[str]] = defaultdict(set)
    for row in events:
        for original in row["originals"]:
            results_of[original].add(row["result"])
    settled: dict[str, frozenset[str]] = {}
    for root in sorted(results_of):
        if root in settled:
            continue
        path = {root}
        stack = [(root, iter(sorted(results_of[root])))]
        while stack:
            org, pending = stack[-1]
            for result in pending:
                if result in settled or result not in results_of:
                    continue
                if result in path:
                    raise ValueError(f"agency change events form a cycle through {result}")
                path.add(result)
                stack.append((result, iter(sorted(results_of[result]))))
                break
            else:
                stack.pop()
                path.discard(org)
                settled[org] = frozenset().union(
                    *(settled[result] if result in results_of else {result} for result in results_of[org])
                )
    return settled


class _Projection(NamedTuple):
    """The reverse lookup: FR agency id -> its one code, and the ancestor orgs of each org whose parent is stated."""

    code_by_fr_id: MappingProxyType[int, str]
    ancestors_by_org: MappingProxyType[str, frozenset[str]]


def _build_projection(
    rows: Iterable[Mapping[str, Any]], bridges: Iterable[Mapping[str, Any]], events: Sequence[Mapping[str, Any]]
) -> _Projection:
    """Reverse REF-038's rows, then follow the registry's bridges and each original's current successors.

    A bridge adds to its FR subject every code that selects its object, with the subject's
    roster parent. An event's original adds every code that selects any current successor,
    bridged codes included, but only when every current successor is coded: an uncoded
    successor is unknown, not absent, so the original then takes no code at all. An org resolves
    only where exactly one code selects it, so a split resolves only when all its successors
    agree on one code. An original takes the roster parent its event states in
    ``original_parents``, None for a top-level agency, so every coded org's parent is stated;
    a second source stating another parent for an org is refused.
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
    unknown: set[str] = set()
    for original, successors in _current_successors(events).items():  # a current successor is no original
        found = [codes.get(successor, set()) for successor in successors]
        if all(found):
            codes[original] |= set().union(*found)
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
    """The reverse lookup over the vendored projection and registry view, built once."""
    return _build_projection(projection_rows(), registry_rows("bridges"), registry_rows("events"))


def fr_agency_code(fr_agency_id: int) -> str | None:
    """The one Regulations.gov code for Federal Register agency ``fr_agency_id``, or ``None``.

    A code selects the agency directly (REF-038), through an identity bridge (Energy Department
    is DOE), or through its current successors when every one is coded and they give one code
    between them (the Health Care Finance Administration is CMS). ``None`` when no code selects
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
