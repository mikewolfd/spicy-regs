#!/usr/bin/env python
"""Publish one sealed SourceCatalogRelease v1 bundle from the published catalog.

Reads the tracked universe specification, streams every row of the published
``documents.parquet`` through the published-catalog discovery adapter, applies
the universe policy, and writes the gated bundle with one atomic rename.

The catalog's digest is checked against the specification's
``sourceSystem.sourceSystemVersion`` before anything is read.  A universe names
the exact bytes it was written for; producing it from other bytes would put a
selection under a policy digest that never described it, so the run refuses
rather than proceeds.

Beyond the receipt, this prints the composition of the requested universe ``U``
and the selected set ``S`` — per disposition, per reason code, per document
type, per publication year, and the agencies that carry the selection — because
those are the facts a register entry has to state and the sealed bundle records
only as counts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = REPO_ROOT / "src/spicy_regs/universes/regulations-gov-published-catalog-2021-2025.json"
CATALOG_SOURCE_ID = "https://data.spicy-regs.dev/documents.parquet"
MIRROR_SOURCE_ID = "s3://mirrulations/raw-data"
FEDERAL_REGISTER_SOURCE_ID = "https://data.spicy-regs.dev/federal_register.parquet"


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _validate_source_inputs(
    spec: Any,
    *,
    catalog: Path,
    mirror_index: Path | None,
    federal_register: Path | None,
) -> None:
    """Prove that every file read by this producer is declared and pinned."""

    from spicy_regs.source_catalog import SourceCatalogError

    supplied = {
        CATALOG_SOURCE_ID: catalog,
        MIRROR_SOURCE_ID: mirror_index,
        FEDERAL_REGISTER_SOURCE_ID: federal_register,
    }
    required_roles = {
        CATALOG_SOURCE_ID: "metadata",
        MIRROR_SOURCE_ID: "rendition",
        FEDERAL_REGISTER_SOURCE_ID: "rendition",
    }

    if not spec.sources:
        if spec.source_system_id != CATALOG_SOURCE_ID:
            raise SourceCatalogError(
                f"this producer reads {CATALOG_SOURCE_ID}, but the universe declares {spec.source_system_id}"
            )
        declared = {CATALOG_SOURCE_ID: (spec.source_system_version, {"metadata"})}
    else:
        declared_versions: dict[str, str] = {}
        declared_roles: dict[str, set[str]] = {}
        for source in spec.sources:
            previous = declared_versions.setdefault(source.source_system_id, source.source_system_version)
            if previous != source.source_system_version:
                raise SourceCatalogError(
                    f"the universe declares two versions for {source.source_system_id}: "
                    f"{previous} and {source.source_system_version}"
                )
            declared_roles.setdefault(source.source_system_id, set()).add(source.role)
        declared = {
            source_id: (version, declared_roles[source_id]) for source_id, version in declared_versions.items()
        }

    if CATALOG_SOURCE_ID not in declared:
        raise SourceCatalogError(f"this producer reads {CATALOG_SOURCE_ID}, but the universe does not declare it")

    for source_id, path in supplied.items():
        if path is not None and source_id not in declared:
            raise SourceCatalogError(f"this run supplies {source_id}, but the universe does not declare it")

    for source_id, (pinned_version, roles) in declared.items():
        path = supplied.get(source_id)
        if path is None:
            raise SourceCatalogError(f"the universe declares {source_id} and this run supplies no file")
        required_role = required_roles.get(source_id)
        if required_role is None:
            raise SourceCatalogError(f"this producer does not know how to read the declared source {source_id}")
        if required_role not in roles:
            raise SourceCatalogError(
                f"this producer requires {source_id} in role {required_role}, but the universe declares {sorted(roles)}"
            )
        observed = file_digest(path)
        if observed != pinned_version:
            raise SourceCatalogError(f"{source_id} is pinned at {pinned_version}, but {path} digests {observed}")


def _federal_register_map(path: Path) -> dict[str, dict[str, Any]]:
    """``{documentNumber: {pdf_url, html_url}}`` from the published FR table.

    Only the two locator columns and the join key are read; the FR table is
    123 MB and this producer wants three of its twenty-two columns.
    """

    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["document_number", "pdf_url", "html_url"])
    resolved: dict[str, dict[str, Any]] = {}
    for row in table.to_pylist():
        number = row.get("document_number")
        if isinstance(number, str) and number:
            resolved[number] = {"html_url": row.get("html_url"), "pdf_url": row.get("pdf_url")}
    return resolved


def _normalized(item: Mapping[str, Any]) -> Mapping[str, Any]:
    value = item.get("normalizedMetadata")
    return value if isinstance(value, Mapping) else {}


def _native(item: Mapping[str, Any]) -> Mapping[str, Any]:
    value = item.get("sourceNativeMetadata")
    return value if isinstance(value, Mapping) else {}


def _document_type(item: Mapping[str, Any]) -> str:
    return str(_normalized(item).get("documentType") or _native(item).get("document_type") or "unstated")


def _agency(item: Mapping[str, Any]) -> str:
    agencies = _normalized(item).get("agencies")
    if isinstance(agencies, Sequence) and agencies:
        return str(agencies[0].get("agencyId"))
    return str(_native(item).get("agency_code") or "unstated")


def _family(rendition_id: str) -> str:
    """The rendition family a rendition id belongs to.

    Ids within a family are numbered (`mirrulations-mirror-1`) or role-suffixed
    (`federal-register-pdf`), so the family is the longest declared prefix.
    """

    for family in ("mirrulations-mirror", "source-file-url", "source-attachment", "federal-register"):
        if rendition_id == family or rendition_id.startswith(f"{family}-"):
            return family
    return rendition_id


def _year(item: Mapping[str, Any]) -> str:
    published = _normalized(item).get("publicationDate")
    if isinstance(published, str) and len(published) >= 4:
        return published[:4]
    posted = _native(item).get("posted_date")
    return posted[:4] if isinstance(posted, str) and len(posted) >= 4 else "unstated"


def composition(items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The per-facet composition of ``U`` and of ``S``, from the rows alone."""

    dispositions: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    universe_types: Counter[str] = Counter()
    universe_years: Counter[str] = Counter()
    selected_types: Counter[str] = Counter()
    selected_years: Counter[str] = Counter()
    selected_agencies: Counter[str] = Counter()
    universe_agencies: set[str] = set()
    renditions = 0
    families: Counter[str] = Counter()
    with_digest = 0

    for item in items:
        selection = item.get("selection") or {}
        disposition = str(selection.get("disposition"))
        dispositions[disposition] += 1
        if disposition != "selected":
            reasons[f"{disposition}/{selection.get('reasonCode')}"] += 1
        universe_types[_document_type(item)] += 1
        universe_years[_year(item)] += 1
        universe_agencies.add(_agency(item))
        if disposition == "selected":
            selected_types[_document_type(item)] += 1
            selected_years[_year(item)] += 1
            selected_agencies[_agency(item)] += 1
            offered = list(item.get("candidateRenditions") or ())
            renditions += len(offered)
            # The preference order means one item carries one family, so the
            # first rendition names which source actually won it.
            if offered:
                families[_family(str(offered[0].get("renditionId")))] += 1
            with_digest += sum(1 for rendition in offered if rendition.get("expectedSha256"))

    return {
        "dispositions": dict(sorted(dispositions.items())),
        "reasonCodes": dict(sorted(reasons.items(), key=lambda pair: (-pair[1], pair[0]))),
        "universe": {
            "agencyCount": len(universe_agencies),
            "documentTypes": dict(sorted(universe_types.items())),
            "publicationYears": dict(sorted(universe_years.items())),
        },
        "selected": {
            "agencyCount": len(selected_agencies),
            "candidateRenditionCount": renditions,
            "renditionFamilies": dict(sorted(families.items())),
            "renditionsWithVerifiedDigest": with_digest,
            "documentTypes": dict(sorted(selected_types.items())),
            "publicationYears": dict(sorted(selected_years.items())),
            "topAgencies": dict(sorted(selected_agencies.items(), key=lambda pair: (-pair[1], pair[0]))[:25]),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-status", default="candidate")
    parser.add_argument("--build-run-id", default=None)
    parser.add_argument("--published-at", default=None, help="UTC instant; defaults to now, truncated to the second.")
    parser.add_argument("--composition", type=Path, default=None, help="Write the composition report here as JSON.")
    parser.add_argument("--mirror-index", type=Path, default=None, help="Sealed mirror index the universe pins.")
    parser.add_argument("--federal-register", type=Path, default=None, help="FR table the universe pins.")
    args = parser.parse_args(argv)

    from spicy_regs.source_catalog import build_source_catalog_release, load_universe_spec
    from spicy_regs.source_catalog.published_catalog import DEFAULT_RENDITION_PREFERENCE, discover_published_catalog

    spec = load_universe_spec(args.spec)
    # Every source the universe declares is checked against the bytes handed
    # to this run before anything is read.  A release produced from bytes its
    # policy never described would rest on a digest that does not fit it.
    _validate_source_inputs(
        spec,
        catalog=args.catalog,
        mirror_index=args.mirror_index,
        federal_register=args.federal_register,
    )

    mirror_index = None
    if args.mirror_index is not None:
        # Run as a script, so this file's own directory is already on the path.
        from build_mirrulations_mirror_index import read_mirror_index

        mirror_index = read_mirror_index(args.mirror_index)
        print(f"mirror index: {len(mirror_index):,} documents", flush=True)
    federal_register = _federal_register_map(args.federal_register) if args.federal_register is not None else None
    if federal_register is not None:
        print(f"federal register: {len(federal_register):,} documents", flush=True)

    published_at = args.published_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bundle = build_source_catalog_release(
        spec,
        discover_published_catalog(
            args.catalog,
            mirror_index=mirror_index,
            federal_register=federal_register,
            preference=spec.rendition_preference or DEFAULT_RENDITION_PREFERENCE,
        ),
        published_at=published_at,
        release_status=args.release_status,
        build_run_id=args.build_run_id,
    )
    path = bundle.write(args.output)

    content = bundle.root["content"]
    report = {
        "catalogId": content["catalogId"],
        "composition": composition(bundle.items),
        "counts": dict(content["counts"]),
        "coverage": dict(content["coverage"]),
        "output": str(path),
        "publishedAt": published_at,
        "releaseId": bundle.release_id,
        "requestedUniverseSetDigest": content["requestedUniverseSetDigest"],
        "schemaSetId": content["schemaSet"]["schemaSetId"],
        "selectedSourceSetDigest": content["selectedSourceSetDigest"],
        "selectionPolicy": dict(content["selectionPolicy"]),
        "sourceSystem": dict(content["sourceSystem"]),
        "universeId": spec.universe_id,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.composition is not None:
        args.composition.parent.mkdir(parents=True, exist_ok=True)
        args.composition.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":  # pragma: no cover - console entry point
    sys.exit(main())
