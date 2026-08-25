"""Publish regulations.gov or Federal Register records as one source catalog."""

from __future__ import annotations

import argparse
from datetime import date
from itertools import chain
from pathlib import Path

from rulespec_conformance.platform_artifact import sha256_digest

from spicy_regs.schemas.regulations import DOCUMENT
from spicy_regs.source_catalog import (
    SourceCatalogBuild,
    SourceCatalogPublisher,
    federal_register_items,
    regulations_gov_items,
)
from spicy_regs.sources import mirrulations
from spicy_regs.sources.federal_register import FederalRegisterReader

_SELECTION_POLICY = {
    "candidateRenditionRequired": True,
    "normalizedMetadataRequired": True,
    "withdrawnExcluded": True,
}


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog-id", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="source", required=True)
    regulations = subparsers.add_parser("regulations-gov")
    regulations.add_argument("--agency", action="append", required=True)
    regulations.add_argument("--since-year", type=int)
    federal = subparsers.add_parser("federal-register")
    federal.add_argument("--since", type=_date)
    federal.add_argument("--until", type=_date)
    args = parser.parse_args(argv)

    if args.source == "regulations-gov":
        make_reader = mirrulations.reader_factory(
            [DOCUMENT],
            since_year=args.since_year,
            bounded=True,
        )
        items = chain.from_iterable(
            regulations_gov_items(make_reader(agency, DOCUMENT), DOCUMENT)
            for agency in sorted(set(args.agency))
        )
        source_id = "urn:spicy-regs:source:regulations-gov-mirrulations"
        source_version = "regulations.gov-v4"
    else:
        items = federal_register_items(FederalRegisterReader(since=args.since, until=args.until))
        source_id = "urn:spicy-regs:source:federal-register"
        source_version = "federalregister.gov-v1"

    published = SourceCatalogPublisher().publish(
        items,
        build=SourceCatalogBuild(
            catalog_id=args.catalog_id,
            source_system_id=source_id,
            source_system_version=source_version,
            selection_policy_id="urn:spicy-regs:selection:complete-source-document",
            selection_policy_version="1",
            selection_policy_digest=sha256_digest(_SELECTION_POLICY),
        ),
        destination=args.destination,
    )
    print(published.artifact.pin.logical_id)
    print(published.artifact.pin.artifact_digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
