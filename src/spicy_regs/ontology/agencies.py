"""Regulations.gov agency codes through RefSpec's REF-038 projection onto roster organizations.

The projection's bytes are RefSpec's, vendored in ``reference/refspec/`` and never edited
here; the loader refuses any copy whose sha256 is not the pinned one.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache
from importlib.resources import files
from types import MappingProxyType
from typing import Any, Mapping, NamedTuple, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

#: Pinned digests of the three vendored REF-038 files; see ``reference/refspec/README.md``.
AGENCY_PROJECTION_SHA256 = "c9ec0fde1bf5fda17402983880bc091e9caa417845178f232214606e264c049f"
AGENCY_PROJECTION_UNRESOLVED_SHA256 = "e32e814c3c7489d82df6dbdbc00fd6e16694628c08e7300a9473bcaa0659b065"
VIEW_MANIFEST_SHA256 = "991acd29368b17fb66eea770f36c71385c5faee1a368008a4240281e4c8536d8"

#: The vendored projection table.
AGENCY_PROJECTION_PATH = files("spicy_regs").joinpath("reference/refspec/agency-projection.parquet")

_FR_AGENCY = "urn:ref:federal-register-agency:"


@lru_cache(maxsize=3)
def _read_pinned(path: Any, sha256: str) -> tuple[Mapping[str, Any], ...]:
    """Read a vendored RefSpec table, raising unless the bytes match the pinned digest."""
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if digest != sha256:
        raise ValueError(f"{path.name} is not RefSpec's file: sha256 {digest}, pinned {sha256}")
    return tuple(MappingProxyType(row) for row in pq.read_table(pa.BufferReader(data)).to_pylist())


def projection_rows() -> tuple[Mapping[str, Any], ...]:
    """The vendored REF-038 projection rows, read once and cached."""
    return _read_pinned(AGENCY_PROJECTION_PATH, AGENCY_PROJECTION_SHA256)


class _Projection(NamedTuple):
    """The projection reversed: FR agency id -> its one code, and each org's ancestor orgs."""

    code_by_fr_id: MappingProxyType[int, str]
    ancestors_by_org: MappingProxyType[str, frozenset[str]]


@lru_cache(maxsize=1)
def _projection() -> _Projection:
    """Build the reverse lookup once: one code per FR agency id, and full ancestor chains."""
    codes: dict[int, list[str]] = {}
    parents: dict[str, str | None] = {}
    for row in projection_rows():
        parents[row["org"]] = row["parent_org"]
        if row["org"].startswith(_FR_AGENCY):
            codes.setdefault(int(row["org"].removeprefix(_FR_AGENCY)), []).append(row["source_value"])
    ancestors: dict[str, frozenset[str]] = {}

    def ancestors_of(org: str) -> frozenset[str]:
        chain = ancestors.get(org)
        if chain is None:
            parent = parents.get(org)
            chain = frozenset({parent, *ancestors_of(parent)}) if parent else frozenset()
            ancestors[org] = chain
        return chain

    for org in parents:
        ancestors_of(org)
    return _Projection(
        MappingProxyType({fr_id: values[0] for fr_id, values in codes.items() if len(values) == 1}),
        MappingProxyType(ancestors),
    )


def fr_agency_code(fr_agency_id: int) -> str | None:
    """The one Regulations.gov code projected onto Federal Register agency ``fr_agency_id``, or ``None``.

    ``None`` when no code projects onto that org, or several do. Codes projected onto non-FR
    organizations do not reverse here: their URNs name no FR agency id, so DOE (an eCFR org)
    and DOL have no ``fr_agency_code`` — a known gap.
    """
    return _projection().code_by_fr_id.get(fr_agency_id)


def agency_code_for_fr_agencies(agencies: Sequence[dict[str, Any]]) -> str | None:
    """The one Regulations.gov code for an FR row's parsed ``agencies_json`` entries, or ``None``.

    An org named alongside its descendant is dropped by the projection's ``parent_org``
    chain, however many levels apart (Transportation Department with FAA is FAA; Agriculture
    Department with GIPSA, its grandchild, is GIPSA). Every agency on the document, resolved
    or not, must then be compatible with the chosen org C: it is C, or an ancestor of C by
    the projection's chain (Energy Department with FERC is FERC), or its FR ``parent_id`` is
    C or an org whose projection chain contains C — a department signing with one of its own
    unresolved bureaus (HHS with its Health Care Finance Administration) is the department.
    Anything else is a joint document and gives ``None`` (Education with Labor). Malformed
    entries — a non-dict, a missing or non-integer ``id`` — are skipped.
    """
    projection = _projection()
    code_by_org: dict[str, str] = {}
    named: list[tuple[int, int | None]] = []
    for entry in agencies:
        fr_id = entry.get("id") if isinstance(entry, dict) else None
        if not isinstance(fr_id, int) or isinstance(fr_id, bool):
            continue
        parent_id = entry.get("parent_id")
        named.append((fr_id, parent_id if isinstance(parent_id, int) and not isinstance(parent_id, bool) else None))
        if (code := projection.code_by_fr_id.get(fr_id)) is not None:
            code_by_org[f"{_FR_AGENCY}{fr_id}"] = code
    specific = {
        org
        for org in code_by_org
        if not any(org in projection.ancestors_by_org[other] for other in code_by_org if other != org)
    }
    if len(specific) != 1:
        return None
    chosen = next(iter(specific))
    chosen_id = int(chosen.removeprefix(_FR_AGENCY))
    allowed = {chosen, *projection.ancestors_by_org[chosen]}
    for fr_id, parent_id in named:
        if f"{_FR_AGENCY}{fr_id}" in allowed:
            continue
        if parent_id is not None and (
            parent_id == chosen_id or chosen in projection.ancestors_by_org.get(f"{_FR_AGENCY}{parent_id}", frozenset())
        ):
            continue
        return None
    return code_by_org[chosen]
