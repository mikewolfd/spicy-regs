"""Acquire or replay a declared GSA IOLP snapshot through ordinary source evidence and table generations."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.raw_releases import inherit_read_index, open_raw, raw_original
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_gsa_iolp import FAMILY, SCHEMAS, build_tables

FORMAT = "gsa-iolp-captures-v1"
STAGE = "gsa-iolp-original"


def acquire(catalog_url: str, snapshot_date: str, output_dir: Path, *, transport=None):
    """Retain the catalog and its exact dated pair, returning durable locators without signed credentials."""
    from spicy_docs.sources.gsa_iolp import MAX_WORKBOOK_BYTES, acquire_snapshot

    with acquisition_evidence(FAMILY, output_dir=output_dir) as evidence:
        scope, catalog, captures = acquire_snapshot(
            catalog_url,
            snapshot_date,
            transport=evidence.transport(transport, stage=STAGE, max_bytes=MAX_WORKBOOK_BYTES),
        )

        def descriptor(capture, url):
            # The catalog-stated URL, never its temporary signed S3 redirect.
            return {
                "requested_url": url,
                "sha256": capture.sha256,
                "byte_size": len(capture.body),
                "observed_at": capture.observed_at,
            }

        return {
            "format": FORMAT,
            "snapshot_date": snapshot_date,
            "catalog_capture": descriptor(catalog, catalog_url),
            "workbooks": {
                kind: descriptor(capture, scope["distributions"][kind]["downloadURL"])
                for kind, capture in captures.items()
            },
        }


def build(selection: dict, output_dir: Path, *, archive_root: Path | None = None, read_index: Path | None = None):
    """Revalidate exact retained originals, write the whole snapshot pair, and admit its immutable generation.

    Each new output directory is a separate snapshot candidate. No acquisition or shared selection occurs.
    A missing required header, key or original refuses before any table is admitted.
    """
    from spicy_docs.sources.gsa_iolp import (
        MAX_CATALOG_BYTES,
        MAX_WORKBOOK_BYTES,
        official_url,
        read_inventory,
        select_snapshot,
    )

    if selection.get("format") != FORMAT or set(selection.get("workbooks", {})) != {"buildings", "leases"}:
        raise ValueError("Invalid complete IOLP capture selection")
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh output directory for each IOLP generation")
    store = open_raw(archive_root)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)

    def original(capture, limit, kind):
        if (
            type(capture.get("byte_size")) is not int
            or not 0 < capture["byte_size"] <= limit
            or not isinstance(capture.get("observed_at"), str)
            or not capture["observed_at"]
        ):
            raise ValueError("IOLP capture lacks a bounded size or observation time")
        path = raw_original(store, capture["sha256"], capture["byte_size"])
        retained = evidence.retain_file(
            path,
            stage=STAGE,
            raw_source=True,
            original_location=capture["requested_url"],
            source_url=capture["requested_url"],
            dataset_id="GSA-4495",
            native_id=kind,
            source_version=selection["snapshot_date"],
            covered_period=selection["snapshot_date"],
            observed_at=capture["observed_at"],
        )
        if retained["sha256"] != capture["sha256"]:
            raise ValueError("IOLP retained original differs from its selected digest")
        return path.read_bytes()

    try:
        catalog = selection["catalog_capture"]
        official_url(catalog["requested_url"])
        scope = select_snapshot(original(catalog, MAX_CATALOG_BYTES, "GSA-4495"), selection["snapshot_date"])
        inventories = {}
        for kind, capture in selection["workbooks"].items():
            if capture["requested_url"] != scope["distributions"][kind]["downloadURL"]:
                raise ValueError("IOLP workbook URL differs from its exact catalog selection")
            inventories[kind] = read_inventory(original(capture, MAX_WORKBOOK_BYTES, kind), kind)
        scope["captures"] = selection
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.event(
            "gsa-iolp-snapshot",
            selection=selection,
            observed_counts={kind: len(inventory.rows) for kind, inventory in inventories.items()},
            replacement_semantics="complete selected pair; earlier generations and raw originals retained",
        )
        files = build_tables(scope, selection["workbooks"], inventories, output_dir / "tables")
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=files,
            expected_keys=[f"{table}.parquet" for table in SCHEMAS],
            schemas={table: described_schema(schema) for table, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    acquisition = commands.add_parser("acquire")
    acquisition.add_argument("--catalog-url", required=True)
    acquisition.add_argument("--snapshot-date", required=True)
    acquisition.add_argument("--captures-out", required=True, type=Path)
    acquisition.add_argument("--output-dir", required=True, type=Path)
    candidate = commands.add_parser("build")
    candidate.add_argument("--captures", required=True, type=Path)
    candidate.add_argument("--output-dir", required=True, type=Path)
    candidate.add_argument("--archive-root", type=Path)
    candidate.add_argument("--read-index", type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.catalog_url, args.snapshot_date, args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps(selection, sort_keys=True))
    else:
        artifact, evidence = build(
            json.loads(args.captures.read_bytes()),
            args.output_dir,
            archive_root=args.archive_root,
            read_index=args.read_index,
        )
        print(
            json.dumps(
                {
                    "generation": str(args.output_dir / "generation"),
                    "evidence": str(evidence),
                    "pin": artifact.pin.as_dict(),
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
