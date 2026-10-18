#!/usr/bin/env python3
"""Rebuild the live native comments catalog in the regulations-native-v2 shape from the published generation.

A dry run unless ``--publish``: it downloads and byte-checks the published subject and receipts, pins the catalog
snapshot, proves the live 21 shared columns equal the published subject (per-key SHA-256), prints the plan (rows,
bytes, expected disk, target namespace) and writes nothing. ``--publish`` then drops the v1 table and its receipts,
creates the v2 table, bulk-copies rows and receipts, writes one v2 marker and reads the result back once.
``--verify-only`` runs just that readback against an already rebuilt catalog. Needs the R2 Data Catalog variables
and ``R2_PUBLIC_URL``. Run it under the heavy-check lock, with ``--work`` on a volume with the printed free space.
See ``spicy_regs.sources.regulatory_catalog.rebuild_native``.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from collections.abc import Sequence
from os import getenv
from pathlib import Path

from loguru import logger

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg, publication
from spicy_regs.sources import regulatory_catalog as native
from spicy_regs.transforms.regulations_receipts import ReceiptInput

GIB = 2**30
#: DuckDB spill and Iceberg encode scratch beyond the inputs and the readback copy (plan section 2l).
SPILL_BYTES = 8 * GIB


def expected_disk(subject_bytes: int, receipt_bytes: int) -> int:
    """Inputs plus the readback copy of both, plus spill."""
    return 2 * (subject_bytes + receipt_bytes) + SPILL_BYTES


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--work", type=Path, required=True, help="Empty or new directory for downloads, spill and the readback copy")
    parser.add_argument("--expect-artifact-digest", required=True,
                        help="The comments generation to rebuild from: its artifactDigest, or an 12+ hex-digit prefix of it")
    parser.add_argument("--publish", action="store_true", help="Write the rebuilt catalog; without it nothing is written")
    parser.add_argument("--dry-run", action="store_true", help="The default; refuses to combine with --publish")
    parser.add_argument("--verify-only", action="store_true", help="Only read an already rebuilt catalog back against the generation")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory", default="12GB")
    args = parser.parse_args(argv)
    if args.publish and (args.dry_run or args.verify_only):
        parser.error("--publish cannot combine with --dry-run or --verify-only")
    base = (getenv("R2_PUBLIC_URL") or "").rstrip("/")
    if not base or not iceberg.is_configured():
        parser.error("R2_PUBLIC_URL and the R2 Data Catalog variables must be set")
    prefix = args.expect_artifact_digest.removeprefix("sha256:")
    if len(prefix) < 12 or any(c not in "0123456789abcdef" for c in prefix):
        parser.error("--expect-artifact-digest needs at least 12 lowercase hex digits")

    record_type = RECORD_TYPES["comments"]
    index = publication.load_index(base)
    owner = publication.table_owner(index, "comments.parquet")
    if owner is None or not owner[1]["artifactDigest"].removeprefix("sha256:").startswith(prefix):
        raise SystemExit(f"The published comments generation is not {args.expect_artifact_digest}: "
                         f"{owner[1]['artifactDigest'] if owner else 'no family publishes comments.parquet'}")
    subject = publication.single_member(index, "comments.parquet")
    (receipts,) = publication.receipt_members(index, dataset="comments")
    if any(m.sha256 is None or m.byte_size is None or m.rows is None for m in (subject, receipts)):
        raise SystemExit("The published comments members are not byte-pinned; refusing")
    assert subject.byte_size is not None and receipts.byte_size is not None  # for the type checker, after the refusal
    needed = expected_disk(subject.byte_size, receipts.byte_size)
    args.work.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(args.work).free
    print(f"Plan: rebuild {native.qualified(record_type)} (namespace {native.namespace()}) from generation "
          f"{owner[1]['artifactDigest']}\n"
          f"  rows {subject.rows:,}; subject {subject.byte_size / GIB:.1f} GiB; receipts {receipts.rows:,} rows, "
          f"{receipts.byte_size / GIB:.1f} GiB\n"
          f"  expected peak disk {needed / GIB:.1f} GiB; free in {args.work}: {free / GIB:.1f} GiB\n"
          f"  mode: {'PUBLISH (writes the live catalog)' if args.publish else 'verify-only' if args.verify_only else 'dry run (writes nothing)'}",
          flush=True)
    if free < needed:
        raise SystemExit("Not enough free disk for the plan; use a larger --work volume")

    inputs = args.work / "published"
    inputs.mkdir(exist_ok=True)
    paths = {}
    for member, name in ((subject, "comments.parquet"), (receipts, "etl_receipts.parquet")):
        paths[name] = inputs / name
        publication.fetch_member(base, member, paths[name], name)  # Raises unless the bytes equal the pin.
    pair = ReceiptInput("comments", (paths["comments.parquet"],), paths["etl_receipts.parquet"], owner[1]["artifactDigest"])

    con = iceberg._connect()
    try:
        ExportResources(memory=args.memory, threads=args.threads).configure(con, args.work / "spill")
        if not args.verify_only:
            state = native.rebuild_state(con, record_type)
            snapshot = iceberg._read_pair_snapshot(con, record_type, namespace=native.namespace()) if state == "v1" else None
            plan = native.rebuild_native(con, record_type, pair, expected_snapshot=snapshot,
                                         expected_prior=pair.subjects[0], dry_run=not args.publish)
            print(f"Checks passed; live state {plan['state']}, {plan['rows']:,} rows"
                  + ("" if args.publish else "; dry run, nothing written"), flush=True)
            if not args.publish:
                return 0
        result = native.verify_rebuilt(con, record_type, pair, args.work / "readback")
        print(f"Readback passed: {result}", flush=True)
        logger.info("Next: dispatch check-comments-freshness and dedupe-comments-catalog with skip_upload")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
