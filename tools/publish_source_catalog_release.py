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


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


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
            renditions += len(item.get("candidateRenditions") or ())

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
    args = parser.parse_args(argv)

    from spicy_regs.source_catalog import SourceCatalogError, build_source_catalog_release, load_universe_spec
    from spicy_regs.source_catalog.published_catalog import discover_published_catalog

    spec = load_universe_spec(args.spec)
    observed = file_digest(args.catalog)
    if observed != spec.source_system_version:
        raise SourceCatalogError(
            f"the universe was written for sourceSystemVersion {spec.source_system_version}, but "
            f"{args.catalog} digests {observed}"
        )

    published_at = args.published_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bundle = build_source_catalog_release(
        spec,
        discover_published_catalog(args.catalog),
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
