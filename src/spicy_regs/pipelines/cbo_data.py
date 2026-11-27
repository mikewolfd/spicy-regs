"""Acquire commit-pinned US-CBO/cbo-data files into the raw archive and build an ordinary generation.

``acquire`` captures ``catalog.json``, each dataset's ``schema.json``, and all
CSV distributions at a pinned 40-character Git commit SHA via
``spicy_docs.sources.cbo_data.CboDataAcquirer``, recording exact URLs,
SHA-256 digests, byte sizes, and observation timestamps in a selection file.
``build`` verifies every retained original from the raw archive offline and
emits all 13 schema-validated dataset Parquet tables plus ``cbo_data_snapshots``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, inherit_read_index
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cbo_data import SCHEMAS, build_cbo_data_streaming

FAMILY = "cbo-data"
STAGE = "cbo-data-github"


def acquire(
    output_dir: Path,
    *,
    commit: str | None = None,
    datasets: tuple[str, ...] | None = None,
    transport=None,
    archive_root: Path | None = None,
    min_request_interval_seconds: float = 0.05,
) -> dict[str, Any]:
    """Acquire ``catalog.json``, all ``schema.json`` files, and all CSV distributions at ``commit``."""
    from spicy_docs.sources.cbo_data import (
        DEFAULT_MAX_REQUESTS,
        MAX_CSV_BYTES,
        PINNED_COMMIT,
        CboDataAcquirer,
        CboDataBudget,
        validate_commit,
    )

    pinned = validate_commit(commit or PINNED_COMMIT)
    output_dir = Path(output_dir)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        retained_transport = evidence.transport(transport, stage=STAGE, max_bytes=MAX_CSV_BYTES)
        budget = CboDataBudget(
            max_requests=DEFAULT_MAX_REQUESTS,
            min_request_interval_seconds=min_request_interval_seconds,
        )
        with CboDataAcquirer(budget=budget, transport=retained_transport) as acquirer:
            snapshot = acquirer.acquire_all(commit=pinned, datasets=datasets)

        # Verify each retained original in evidence.store matches captured bytes.
        for cap in (
            snapshot.catalog_capture,
            *snapshot.schema_captures.values(),
            *snapshot.distribution_captures.values(),
        ):
            with evidence.store.open(cap.sha256) as stream:
                if stream.read(MAX_CSV_BYTES + 1) != cap.body:
                    raise ValueError(f"Retained capture differs from central raw archive for {cap.requested_url}")

        selection = snapshot.receipts()
        evidence.event("selection", commit=pinned, datasets=len(snapshot.schemas), distributions=len(snapshot.distributions))
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection


def _read_verified_original(
    evidence: CaptureEvidence,
    *,
    sha256: str,
    byte_size: int,
    max_bytes: int,
    stage: str,
    original_location: str,
    observed_at: str,
    dataset_id: str,
    native_id: str,
) -> bytes:
    with evidence.store.open(sha256) as stream:
        body = stream.read(max_bytes + 1)
    if len(body) != byte_size or len(body) > max_bytes or "sha256:" + hashlib.sha256(body).hexdigest() != sha256:
        raise ValueError(f"cbo-data original {original_location} differs from pinned size/digest")
    evidence.retain_bytes(
        body,
        stage=stage,
        raw_source=True,
        dataset_id=dataset_id,
        native_id=native_id,
        source_version=sha256,
        original_location=original_location,
        observed_at=observed_at,
    )
    return body


def build(
    selection: dict[str, Any],
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
):
    """Replay pinned raw originals offline and write an immutable generation for all 13 datasets."""
    from spicy_docs.sources.cbo_data import (
        MAX_CATALOG_BYTES,
        MAX_CSV_BYTES,
        MAX_SCHEMA_BYTES,
        parse_catalog,
        parse_dataset_schema,
        parse_distribution_csv,
        validate_commit,
    )

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each cbo-data generation")
    commit = validate_commit(selection["commit"])
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="cbo-data-selection")

        cat_pin = selection["catalog"]
        cat_bytes = _read_verified_original(
            evidence,
            sha256=cat_pin["sha256"],
            byte_size=cat_pin["byte_size"],
            max_bytes=MAX_CATALOG_BYTES,
            stage="cbo-data-catalog-replay",
            original_location=cat_pin["url"],
            observed_at=cat_pin["observed_at"],
            dataset_id=f"family:{FAMILY}",
            native_id="catalog.json",
        )
        catalog = parse_catalog(cat_bytes, commit=commit, max_bytes=MAX_CATALOG_BYTES)
        by_id = catalog.by_identifier()

        schemas = {}
        for dataset, schema_pin in sorted(selection["schemas"].items()):
            entry = by_id[dataset]
            schema_bytes = _read_verified_original(
                evidence,
                sha256=schema_pin["sha256"],
                byte_size=schema_pin["byte_size"],
                max_bytes=MAX_SCHEMA_BYTES,
                stage="cbo-data-schema-replay",
                original_location=schema_pin["url"],
                observed_at=schema_pin["observed_at"],
                dataset_id=f"cbo-data:{dataset}",
                native_id=entry.schema_path,
            )
            schemas[dataset] = parse_dataset_schema(
                schema_bytes, commit=commit, dataset=dataset, max_bytes=MAX_SCHEMA_BYTES
            )

        dist_lookup = {
            dist.download_url: (entry, dist)
            for entry in catalog.datasets
            for dist in entry.distributions
        }

        def _iter_distribution_rows(download_url: str):
            entry, dist = dist_lookup[download_url]
            pin = selection["distributions"][download_url]
            csv_bytes = _read_verified_original(
                evidence,
                sha256=pin["sha256"],
                byte_size=pin["byte_size"],
                max_bytes=MAX_CSV_BYTES,
                stage="cbo-data-csv-replay",
                original_location=pin["url"],
                observed_at=pin["observed_at"],
                dataset_id=f"cbo-data:{entry.identifier}",
                native_id=download_url,
            )
            rows = parse_distribution_csv(
                csv_bytes,
                commit=commit,
                dataset=entry.identifier,
                publication_id=entry.publication_id,
                vintage=dist.vintage,
                file_type=dist.file_type,
                source_file=download_url,
                schema=schemas[entry.identifier],
                max_bytes=MAX_CSV_BYTES,
            )
            return rows, pin["sha256"], pin["observed_at"]

        tables = build_cbo_data_streaming(
            commit=commit,
            catalog=catalog,
            schemas=schemas,
            iter_distribution_rows=_iter_distribution_rows,
            output_dir=output_dir,
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=tables,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("acquire", "build"):
        command = commands.add_parser(name)
        command.add_argument("--output-dir", type=Path, required=True)
        command.add_argument("--archive-root", type=Path)
        command.add_argument(
            "--selection",
            type=Path,
            required=True,
            help="Capture-selection output for acquire; exact input for build",
        )
        if name == "acquire":
            command.add_argument("--commit", help="40-character US-CBO/cbo-data commit SHA")
        if name == "build":
            command.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    args = parser.parse_args()
    if args.command == "acquire":
        if args.selection.exists():
            parser.error("Use a new selection path; dated snapshots are never overwritten")
        selection = acquire(args.output_dir, commit=args.commit, archive_root=args.archive_root)
        args.selection.parent.mkdir(parents=True, exist_ok=True)
        args.selection.write_text(json.dumps(selection, indent=2) + "\n")
    else:
        build(
            json.loads(args.selection.read_bytes()),
            args.output_dir,
            archive_root=args.archive_root,
            read_index=args.read_index,
        )


if __name__ == "__main__":
    main()
