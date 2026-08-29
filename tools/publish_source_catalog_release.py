#!/usr/bin/env python
"""Publish one sealed SourceCatalogRelease v1 bundle from the published catalog.

Reads the tracked universe specification, streams every row of the published
``documents.parquet`` through the published-catalog discovery adapter, joins
the exact pinned docket and Federal Register metadata when declared, applies
the universe policy, and writes the gated bundle with one atomic rename.

Every supplied source digest is checked against the specification's per-source
pins before anything is read. A universe names the exact bytes it was written
for; producing it from other bytes would put a selection under a policy digest
that never described it, so the run refuses rather than proceeds.

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
DEFAULT_SPEC = (
    REPO_ROOT
    / "src/spicy_regs/universes/regulations-gov-published-catalog-2021-2025-metadata-complete.json"
)
CATALOG_SOURCE_ID = "https://data.spicy-regs.dev/documents.parquet"
DOCKET_SOURCE_ID = "https://data.spicy-regs.dev/dockets.parquet"
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
    dockets: Path | None = None,
    mirror_index: Path | None = None,
    federal_register: Path | None = None,
) -> dict[str, frozenset[str]]:
    """Prove that every file read by this producer is declared and pinned."""

    from spicy_regs.source_catalog import SourceCatalogError

    supplied = {
        CATALOG_SOURCE_ID: catalog,
        DOCKET_SOURCE_ID: dockets,
        MIRROR_SOURCE_ID: mirror_index,
        FEDERAL_REGISTER_SOURCE_ID: federal_register,
    }
    supported_roles = {
        CATALOG_SOURCE_ID: {"metadata"},
        DOCKET_SOURCE_ID: {"metadata"},
        MIRROR_SOURCE_ID: {"rendition"},
        FEDERAL_REGISTER_SOURCE_ID: {"metadata", "rendition"},
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
        supported = supported_roles.get(source_id)
        if supported is None:
            raise SourceCatalogError(f"this producer does not know how to read the declared source {source_id}")
        unsupported = roles - supported
        if unsupported:
            raise SourceCatalogError(
                f"this producer supports {source_id} in roles {sorted(supported)}, "
                f"but the universe declares {sorted(roles)}"
            )
        observed = file_digest(path)
        if observed != pinned_version:
            raise SourceCatalogError(f"{source_id} is pinned at {pinned_version}, but {path} digests {observed}")

    return {source_id: frozenset(roles) for source_id, (_, roles) in declared.items()}


def _catalog_link_keys(path: Path) -> tuple[set[str], set[str]]:
    """Collect the exact docket and FR keys the document catalog states."""

    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    required = ("docket_id", "fr_doc_num")
    missing = [column for column in required if column not in parquet.schema_arrow.names]
    if missing:
        from spicy_regs.source_catalog import SourceCatalogError

        raise SourceCatalogError(f"the published catalog is missing link columns: {missing}")
    dockets: set[str] = set()
    federal_register: set[str] = set()
    for batch in parquet.iter_batches(batch_size=50_000, columns=list(required)):
        for row in batch.to_pylist():
            docket_id = row.get("docket_id")
            fr_doc_num = row.get("fr_doc_num")
            if isinstance(docket_id, str) and docket_id:
                dockets.add(docket_id)
            if isinstance(fr_doc_num, str) and fr_doc_num:
                federal_register.add(fr_doc_num)
    return dockets, federal_register


def _metadata_map(
    path: Path,
    *,
    key_column: str,
    columns: Sequence[str],
    include_keys: set[str],
    source_name: str,
    reject_unclassified: bool,
    optional_columns: Sequence[str] = (),
) -> dict[str, dict[str, Any]]:
    """Read complete rows for exact keys, with bounded batch memory."""

    import pyarrow.parquet as pq

    from spicy_regs.source_catalog import SourceCatalogError

    try:
        parquet = pq.ParquetFile(path)
    except (OSError, ValueError) as error:
        raise SourceCatalogError(f"the {source_name} table is unreadable: {path} ({error})") from error
    present = set(parquet.schema_arrow.names)
    optional = set(optional_columns)
    missing = [column for column in columns if column not in present and column not in optional]
    if missing:
        raise SourceCatalogError(f"the {source_name} table is missing metadata columns: {missing}")
    unexpected = sorted(present - set(columns))
    if reject_unclassified and unexpected:
        raise SourceCatalogError(
            f"the {source_name} table has unclassified columns; classify them before publishing: {unexpected}"
        )

    read_columns = tuple(column for column in columns if column in present)
    resolved: dict[str, dict[str, Any]] = {}
    for batch in parquet.iter_batches(batch_size=50_000, columns=list(read_columns)):
        for row in batch.to_pylist():
            key = row.get(key_column)
            if not isinstance(key, str) or not key or key not in include_keys:
                continue
            if key in resolved:
                raise SourceCatalogError(f"the {source_name} table carries duplicate {key_column} {key!r}")
            # ``to_pylist`` already returns every selected column, including
            # nulls.  Re-index it to make that closed shape visible in code.
            resolved[key] = {column: row.get(column) for column in read_columns}
    return resolved


def _validate_composition_path(output: Path, composition: Path | None) -> None:
    """Keep a human report outside the closed sealed bundle membership."""

    if composition is None:
        return
    from spicy_regs.source_catalog import SourceCatalogError

    if composition.resolve().is_relative_to(output.resolve()):
        raise SourceCatalogError(
            f"the composition report {composition} must sit outside the sealed bundle {output}"
        )


def _normalized(item: Mapping[str, Any]) -> Mapping[str, Any]:
    value = item.get("normalizedMetadata")
    return value if isinstance(value, Mapping) else {}


def _native(item: Mapping[str, Any]) -> Mapping[str, Any]:
    value = item.get("sourceNativeMetadata")
    return value if isinstance(value, Mapping) else {}


def _document_native(item: Mapping[str, Any]) -> Mapping[str, Any]:
    """Primary document metadata from either supported source-native shape."""

    native = _native(item)
    scoped = native.get("regulationsGovDocument")
    return scoped if isinstance(scoped, Mapping) else native


def _document_type(item: Mapping[str, Any]) -> str:
    return str(_normalized(item).get("documentType") or _document_native(item).get("document_type") or "unstated")


def _agency(item: Mapping[str, Any]) -> str:
    agencies = _normalized(item).get("agencies")
    if isinstance(agencies, Sequence) and agencies:
        return str(agencies[0].get("agencyId"))
    return str(_document_native(item).get("agency_code") or "unstated")


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
    posted = _document_native(item).get("posted_date")
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
    parser.add_argument("--dockets", type=Path, default=None, help="Docket metadata table the universe pins.")
    parser.add_argument("--federal-register", type=Path, default=None, help="FR table the universe pins.")
    args = parser.parse_args(argv)

    from spicy_regs.source_catalog import build_source_catalog_release, load_universe_spec
    from spicy_regs.source_catalog.published_catalog import (
        DEFAULT_RENDITION_PREFERENCE,
        DOCKET_METADATA_COLUMNS,
        FEDERAL_REGISTER_METADATA_COLUMNS,
        discover_published_catalog,
    )
    from spicy_regs.schemas.federal_register import FEDERAL_REGISTER_OPTIONAL_COLUMNS

    spec = load_universe_spec(args.spec)
    _validate_composition_path(args.output, args.composition)
    # Every source the universe declares is checked against the bytes handed
    # to this run before anything is read.  A release produced from bytes its
    # policy never described would rest on a digest that does not fit it.
    roles = _validate_source_inputs(
        spec,
        catalog=args.catalog,
        dockets=args.dockets,
        mirror_index=args.mirror_index,
        federal_register=args.federal_register,
    )

    mirror_index = None
    if args.mirror_index is not None:
        # Run as a script, so this file's own directory is already on the path.
        from build_mirrulations_mirror_index import read_mirror_index

        mirror_index = read_mirror_index(args.mirror_index)
        print(f"mirror index: {len(mirror_index):,} documents", flush=True)
    docket_keys, federal_register_keys = (
        _catalog_link_keys(args.catalog)
        if args.dockets is not None or args.federal_register is not None
        else (set(), set())
    )
    docket_metadata = None
    if args.dockets is not None:
        docket_metadata = _metadata_map(
            args.dockets,
            key_column="docket_id",
            columns=DOCKET_METADATA_COLUMNS,
            include_keys=docket_keys,
            source_name="Regulations.gov dockets",
            reject_unclassified=True,
        )
        print(f"docket metadata: {len(docket_metadata):,} exact joins", flush=True)

    federal_register = None
    federal_register_metadata = None
    if args.federal_register is not None:
        federal_register_roles = roles[FEDERAL_REGISTER_SOURCE_ID]
        columns = (
            FEDERAL_REGISTER_METADATA_COLUMNS
            if "metadata" in federal_register_roles
            else ("document_number", "pdf_url", "html_url")
        )
        federal_register = _metadata_map(
            args.federal_register,
            key_column="document_number",
            columns=columns,
            include_keys=federal_register_keys,
            source_name="Federal Register",
            reject_unclassified="metadata" in federal_register_roles,
            optional_columns=(
                FEDERAL_REGISTER_OPTIONAL_COLUMNS
                if "metadata" in federal_register_roles
                else ()
            ),
        )
        if "metadata" in federal_register_roles:
            federal_register_metadata = federal_register
        if "rendition" not in federal_register_roles:
            federal_register = None
        print(
            f"federal register metadata: {len(federal_register_metadata or {}):,} exact joins; "
            f"rendition joins: {len(federal_register or {}):,}",
            flush=True,
        )

    published_at = args.published_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bundle = build_source_catalog_release(
        spec,
        discover_published_catalog(
            args.catalog,
            mirror_index=mirror_index,
            federal_register=federal_register,
            docket_metadata=docket_metadata,
            federal_register_metadata=federal_register_metadata,
            native_metadata_profile=spec.native_metadata_profile,
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
