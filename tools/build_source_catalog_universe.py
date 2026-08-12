#!/usr/bin/env python
"""Author the universe specification the SourceCatalogRelease producer runs under.

The universe is configuration, not code: identity, catalog identity, selection
policy identity and version, source system, scope, the stratified sample, and
the normalization policy that declares the facts no Regulations.gov record
states.  This tool writes that configuration deterministically so the tracked
file can be rebuilt and compared byte for byte.

The one part that is derived rather than typed is ``normalization.agencyNames``.
The published catalog states an agency *code* and never a name, while the wire
schema requires both, so the universe declares the crosswalk.  It is taken from
``build_agency_crosswalk_artifact.py``'s ``agency-codes.parquet``: for every
code the artifact resolves at a tier in ``DECLARED_TIERS``, the declared name is
that artifact's ``primary_slug`` — the Federal Register's own identifier for the
agency, carried verbatim.  A code the artifact leaves ``ambiguous`` (its
evidence names more than one agency and no winner) or ``unmapped`` (its evidence
names none) gets no declared name, and the producer gives every item under it
disposition ``failed`` with ``policy.agency-name-undeclared`` rather than
borrowing the code as its own name.

Rebuilding from the same crosswalk artifact reproduces the file byte for byte:
the output is canonical JSON over sorted keys, and no path, clock, or host
value enters it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "src/spicy_regs/universes/regulations-gov-published-catalog-2021-2025.json"
DEFAULT_AGENCY_CODES = REPO_ROOT / "output/agency-crosswalk-2026-08-02/agency-codes.parquet"

#: Crosswalk tiers whose ``primary_slug`` this universe is willing to declare.
#: ``ambiguous`` and ``unmapped`` are deliberately absent: neither resolves to
#: one agency, and declaring a name for them would be a guess inside
#: ``policySha256``.
DECLARED_TIERS: tuple[str, ...] = ("confident", "probable")

UNIVERSE_ID = "urn:spicy-regs:source-universe:regulations-gov-published-catalog-2021-2025"
CATALOG_ID = "urn:spicy-regs:source-catalog:regulations-gov-published-catalog"
POLICY_ID = "urn:spicy-regs:selection-policy:regulations-gov-published-catalog-2021-2025-stratified-sample"
POLICY_VERSION = "1.0"

#: The source system is the published catalog itself, and its version is the
#: digest of the exact bytes the release was produced from.  A reissued
#: catalog is a different version, which moves ``policySha256`` and therefore
#: the release identity.
SOURCE_SYSTEM_ID = "https://data.spicy-regs.dev/documents.parquet"

#: Every document type the published catalog carries.  Naming them makes the
#: universe explicit rather than "whatever the table happens to hold".
DOCUMENT_TYPES: tuple[str, ...] = (
    "Notice",
    "Other",
    "Proposed Rule",
    "Public Submission",
    "Rule",
    "Supporting & Related Material",
)

PUBLICATION_WINDOW = {"from": "2021-01-01", "to": "2025-12-31"}

#: The seed and per-partition cap of the draw proven against this corpus.
SAMPLE_SEED = "spicy-regs-sample-2026-08-12"
SAMPLE_PER_PARTITION_LIMIT = 100_000

#: Regulations.gov publishes one address per document, formed from the
#: identifier the catalog does state.  The catalog does not restate it, so the
#: universe declares its form.
SOURCE_URL_TEMPLATE = "https://www.regulations.gov/document/{documentId}"

LANGUAGE = "en"


def agency_names(agency_codes_path: Path) -> dict[str, str]:
    """Read the crosswalk artifact and return the declarable code/name pairs."""

    table = pq.read_table(agency_codes_path, columns=["agency_code", "tier", "primary_slug"])
    names: dict[str, str] = {}
    for row in table.to_pylist():
        code = row.get("agency_code")
        slug = row.get("primary_slug")
        if row.get("tier") not in DECLARED_TIERS:
            continue
        if not isinstance(code, str) or not code or not isinstance(slug, str) or not slug:
            continue
        names[code] = slug
    return dict(sorted(names.items()))


def universe_document(*, source_system_version: str, names: dict[str, str]) -> dict[str, Any]:
    """The universe specification, exactly as it is written to disk."""

    return {
        "catalogId": CATALOG_ID,
        "normalization": {
            "agencyNames": names,
            "language": LANGUAGE,
            "sourceUrlTemplate": SOURCE_URL_TEMPLATE,
        },
        "sample": {
            "allocation": "sqrt-proportional",
            "orderHash": "md5(documentId:seed)",
            "partitionBy": "documentType",
            "perPartitionLimit": SAMPLE_PER_PARTITION_LIMIT,
            "seed": SAMPLE_SEED,
            "stratifyBy": ["agencyId", "publicationYear"],
        },
        "scope": {
            "documentTypes": list(DOCUMENT_TYPES),
            "publicationWindow": dict(PUBLICATION_WINDOW),
        },
        "selectionPolicy": {"policyId": POLICY_ID, "policyVersion": POLICY_VERSION},
        "sourceSystem": {
            "sourceSystemId": SOURCE_SYSTEM_ID,
            "sourceSystemVersion": source_system_version,
        },
        "universeId": UNIVERSE_ID,
    }


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--agency-codes", type=Path, default=DEFAULT_AGENCY_CODES)
    parser.add_argument(
        "--catalog",
        type=Path,
        required=True,
        help="The published documents.parquet the release is produced from; its digest becomes sourceSystemVersion.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    names = agency_names(args.agency_codes)
    document = universe_document(source_system_version=file_digest(args.catalog), names=names)

    # Loading it back proves the file the producer will read is one this
    # repository's own model accepts, before anything is written.
    from spicy_regs.source_catalog import UniverseSpec

    spec = UniverseSpec.from_mapping(document)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"universeId       {spec.universe_id}")
    print(f"policySha256     {spec.policy_sha256()}")
    print(f"agencyNames      {len(names)} declared from tiers {list(DECLARED_TIERS)}")
    print(f"sourceSystem     {spec.source_system_version}")
    print(f"output           {args.output}")
    return 0


if __name__ == "__main__":  # pragma: no cover - console entry point
    sys.exit(main())
