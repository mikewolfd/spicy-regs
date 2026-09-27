"""Regulations.gov agency codes for Federal Register agencies, through RefSpec's projection and agency registry.

REF-038 projects each code onto one roster organization. The agency registry view (REF-072,
batch 1) adds identity bridges from Federal Register agencies to organizations codes select,
and dated successions from defunct agencies to the organizations holding their functions now.
Both are RefSpec's bytes, vendored in ``reference/refspec/`` and never edited here; the loader
refuses any copy whose sha256 is not the pinned one.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from functools import cache, lru_cache
from importlib.resources import files
from types import MappingProxyType
from typing import Any, Iterable, Mapping, NamedTuple, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

#: Pinned digests of the three vendored REF-038 files; see ``reference/refspec/README.md``.
AGENCY_PROJECTION_SHA256 = "c9ec0fde1bf5fda17402983880bc091e9caa417845178f232214606e264c049f"
AGENCY_PROJECTION_UNRESOLVED_SHA256 = "e32e814c3c7489d82df6dbdbc00fd6e16694628c08e7300a9473bcaa0659b065"
VIEW_MANIFEST_SHA256 = "991acd29368b17fb66eea770f36c71385c5faee1a368008a4240281e4c8536d8"

#: Pinned digests of the vendored agency registry view (RefSpec 0.1.0.dev21); the manifest's
#: is the pin RefSpec's design note names. See ``reference/refspec/README.md``.
AGENCY_REGISTRY_MANIFEST_SHA256 = "77b357cc06fe3e67bcacb0591833884087572727064f89643e10aa2a28ad6b87"
AGENCY_REGISTRY_BRIDGES_SHA256 = "2e33905b475c6a1adf27960ecf170a1b4c82df2baf20ac13df9307bb687dd898"
AGENCY_REGISTRY_EVENTS_SHA256 = "09e35a12adcb16581b131fcb187d4d2e07b8d005d431163431c303f1b6fecf2e"
AGENCY_REGISTRY_NON_EMISSIONS_SHA256 = "da863e467f00f16a6b7a9ff1a3e1182fb8e7488f8b7d33f0d7f0c403a0663e24"

#: The vendored projection table, and the registry view's directory (manifest plus ``tables/``).
AGENCY_PROJECTION_PATH = files("spicy_regs").joinpath("reference/refspec/agency-projection.parquet")
AGENCY_REGISTRY_VIEW_PATH = files("spicy_regs").joinpath("reference/refspec/agency-registry-view")

_REGISTRY_TABLE_SHA256 = {
    "bridges": AGENCY_REGISTRY_BRIDGES_SHA256,
    "events": AGENCY_REGISTRY_EVENTS_SHA256,
    "non-emissions": AGENCY_REGISTRY_NON_EMISSIONS_SHA256,
}

_FR_AGENCY = "urn:ref:federal-register-agency:"


@cache
def _read_pinned(path: Any, sha256: str) -> tuple[Mapping[str, Any], ...]:
    """Read a vendored RefSpec table, raising unless the bytes match the pinned digest."""
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != sha256:
        raise ValueError(f"{path.name} is not RefSpec's file: sha256 {digest}, pinned {sha256}")
    # One thread: decoding the registry's nested columns on pyarrow's CPU pool left the pool's
    # destructor waiting at interpreter exit (macOS, pyarrow 23: 12 of 12 runs hung, 0 of 12 without).
    table = pq.read_table(pa.BufferReader(data), use_threads=False)
    return tuple(MappingProxyType(row) for row in table.to_pylist())


def projection_rows() -> tuple[Mapping[str, Any], ...]:
    """The vendored REF-038 projection rows, read once and cached."""
    return _read_pinned(AGENCY_PROJECTION_PATH, AGENCY_PROJECTION_SHA256)


def registry_rows(table: str) -> tuple[Mapping[str, Any], ...]:
    """The vendored registry view's ``bridges``, ``events`` or ``non-emissions`` rows, read once and cached."""
    path = AGENCY_REGISTRY_VIEW_PATH.joinpath(f"tables/agency-registry-{table}.parquet")
    return _read_pinned(path, _REGISTRY_TABLE_SHA256[table])


def _current_successors(events: Iterable[Mapping[str, Any]]) -> dict[str, frozenset[str]]:
    """Each event's original read forward to the results no later event replaced; a cycle is refused.

    RefSpec's ``current_agency_successors()`` over the view's event rows, one per (event,
    result): every chain is walked to its end, a split keeps every result, and each answer is
    settled once and reused along every chain through it.
    """
    results_of: dict[str, set[str]] = defaultdict(set)
    for row in events:
        for original in row["originals"]:
            results_of[original].add(row["result"])
    settled: dict[str, frozenset[str]] = {}
    walking: set[str] = set()

    def current(org: str) -> frozenset[str]:
        if org not in results_of:
            return frozenset({org})
        if org not in settled:
            if org in walking:
                raise ValueError(f"agency change events form a cycle through {org}")
            walking.add(org)
            settled[org] = frozenset().union(*map(current, results_of[org]))
            walking.discard(org)
        return settled[org]

    return {org: current(org) for org in results_of}


class _Projection(NamedTuple):
    """The reverse lookup: FR agency id -> its one code, and the ancestor orgs of each org whose parent is stated."""

    code_by_fr_id: MappingProxyType[int, str]
    ancestors_by_org: MappingProxyType[str, frozenset[str]]


def _build_projection(
    rows: Iterable[Mapping[str, Any]], bridges: Iterable[Mapping[str, Any]], events: Iterable[Mapping[str, Any]]
) -> _Projection:
    """Reverse REF-038's rows, then follow the registry's bridges and each original's current successors.

    A bridge gives its FR subject every code that selects its object, with the subject's
    roster parent; an event's original gets every code that selects any current successor,
    bridged codes included. An org resolves only where exactly one code selects it. The view
    states no parent for an original, so its chain is left to the FR row that names it.
    """
    codes: dict[str, set[str]] = defaultdict(set)
    parents: dict[str, str | None] = {}
    for row in rows:
        codes[row["org"]].add(row["source_value"])
        parents[row["org"]] = row["parent_org"]
    for bridge in bridges:  # an FR subject, an eCFR or Federal Hierarchy object: no bridge reaches another
        subject = bridge["subject"]
        if parents.setdefault(subject, bridge["subject_parent"]) != bridge["subject_parent"]:
            raise ValueError(f"bridge {bridge['candidate_id']} states another parent for {subject}")
        codes[subject] |= codes.get(bridge["object"], set())
    for original, successors in _current_successors(events).items():  # a current successor is no original
        codes[original] |= set().union(*(codes.get(successor, ()) for successor in successors))
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
                int(org.removeprefix(_FR_AGENCY)): next(iter(selecting))
                for org, selecting in codes.items()
                if org.startswith(_FR_AGENCY) and len(selecting) == 1
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
    is DOE), or through its current successors (the Health Care Finance Administration is CMS).
    ``None`` when no code selects it, or several do: a split to several codes (INS) has none.
    """
    return _projection().code_by_fr_id.get(fr_agency_id)


def agency_code_for_fr_agencies(agencies: Sequence[dict[str, Any]]) -> str | None:
    """The one Regulations.gov code for an FR row's parsed ``agencies_json`` entries, or ``None``.

    An org named alongside its descendant is dropped by the parent chain, however many levels
    apart (Transportation Department with FAA is FAA; Agriculture Department with GIPSA, its
    grandchild, is GIPSA). The chain is REF-038's ``parent_org`` or a bridge's roster parent;
    the view names no parent for a successor's original, so there it is the ``parent_id`` the
    entry names (Health and Human Services with the Health Care Finance Administration is CMS).
    Every agency on the document, resolved or not, must then be compatible with the chosen org
    C: it is C or an ancestor of C by that chain, or its FR ``parent_id`` is C or an org whose
    projection chain contains C — a department signing with one of its own unresolved bureaus
    (Treasury with the Bureau of the Fiscal Service) is the department. Anything else is a
    joint document and gives ``None`` (EPA with Interior's Bureau of Mines). Malformed entries
    — a non-dict, a missing or non-integer ``id`` — are skipped.
    """
    projection = _projection()
    code_by_org: dict[str, str] = {}
    ancestors_of: dict[str, frozenset[str]] = {}
    named: list[tuple[int, int | None]] = []
    for entry in agencies:
        fr_id = entry.get("id") if isinstance(entry, dict) else None
        if not isinstance(fr_id, int) or isinstance(fr_id, bool):
            continue
        parent_id = entry.get("parent_id")
        parent_id = parent_id if isinstance(parent_id, int) and not isinstance(parent_id, bool) else None
        named.append((fr_id, parent_id))
        if (code := projection.code_by_fr_id.get(fr_id)) is not None:
            org = f"{_FR_AGENCY}{fr_id}"
            code_by_org[org] = code
            chain = projection.ancestors_by_org.get(org)
            if chain is None and parent_id is not None:  # a successor's original: its entry names the parent
                parent = f"{_FR_AGENCY}{parent_id}"
                chain = frozenset({parent, *projection.ancestors_by_org.get(parent, frozenset())})
            ancestors_of[org] = chain or frozenset()
    specific = {
        org for org in code_by_org if not any(org in ancestors_of[other] for other in code_by_org if other != org)
    }
    if len(specific) != 1:
        return None
    chosen = next(iter(specific))
    chosen_id = int(chosen.removeprefix(_FR_AGENCY))
    allowed = {chosen, *ancestors_of[chosen]}
    for fr_id, parent_id in named:
        if f"{_FR_AGENCY}{fr_id}" in allowed:
            continue
        if parent_id is not None and (
            parent_id == chosen_id or chosen in projection.ancestors_by_org.get(f"{_FR_AGENCY}{parent_id}", frozenset())
        ):
            continue
        return None
    return code_by_org[chosen]
