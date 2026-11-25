"""Acquire IRS 527 PolOrgsFullData.zip and build evidenced typed Parquet generations."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.raw_releases import inherit_read_index, open_raw, raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_irs_527 import SCHEMAS, build_irs_527

FAMILY = "irs-527"


def _source_context(evidence):
    from spicy_docs.sources import irs_527

    digest = hashlib.sha256(Path(irs_527.__file__).read_bytes()).hexdigest()
    return evidence.for_source(
        FAMILY,
        "full",
        parser_version="irs-527-v1:sha256:" + digest,
        policy_decision_id="ss-irs527-official-fulldata-zip",
    )


def acquire(
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    transport=None,
    max_bytes: int | None = None,
) -> dict:
    """Stream ``PolOrgsFullData.zip`` via GET into the raw archive and return capture metadata."""
    from spicy_docs.sources.irs_527 import DEFAULT_MAX_ARCHIVE_BYTES, FULL_DATA_URL, Irs527Acquirer

    limit = DEFAULT_MAX_ARCHIVE_BYTES if max_bytes is None else max_bytes
    output_dir = Path(output_dir)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        owner = _source_context(evidence)
        with TemporaryDirectory(dir=output_dir) as scratch:
            dest = Path(scratch) / "PolOrgsFullData.zip"
            with Irs527Acquirer(max_bytes=limit, transport=transport) as acquirer:
                download = acquirer.download_full_data(dest, max_bytes=limit)
            owner.retain_file(
                download.path,
                stage="irs-527-full-data-zip",
                raw_source=True,
                original_location=FULL_DATA_URL,
                observed_at=download.observed_at,
            )
            owner.event(
                "irs-527-captured",
                source_url=download.requested_url,
                sha256=download.sha256,
                byte_size=download.byte_size,
            )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return {
        "format": "spicy-irs-527-captures",
        "version": 1,
        "archive": {
            "url": download.requested_url,
            "sha256": download.sha256,
            "bytes": download.byte_size,
            "observed_at": download.observed_at,
        },
    }


def build(
    selection: dict,
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
):
    """Build an evidenced generation from a pinned ``PolOrgsFullData.zip`` in the raw archive."""
    from spicy_docs.sources.irs_527 import DEFAULT_MAX_ARCHIVE_BYTES, FULL_DATA_URL

    if selection.get("format") != "spicy-irs-527-captures" or selection.get("version") != 1:
        raise ValueError("Unsupported IRS 527 capture selection")
    archive = selection.get("archive")
    if not isinstance(archive, dict) or archive.get("url") != FULL_DATA_URL:
        raise ValueError("IRS 527 archive URL differs from official fullData endpoint")
    size = archive.get("bytes")
    if type(size) is not int or not 0 < size <= DEFAULT_MAX_ARCHIVE_BYTES:
        raise ValueError("IRS 527 archive byte size is invalid or exceeds bound")

    output_dir = Path(output_dir)
    if (output_dir / "candidate").exists() or (output_dir / "generation").exists():
        raise ValueError("Use fresh IRS 527 candidate output paths")

    store = open_raw(archive_root)
    archive_path = raw_original(store, archive["sha256"], size)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        snapshot = inherit_read_index(evidence, read_index)
        owner = _source_context(evidence)
        control = output_dir / "selection.json"
        control.write_text(json.dumps(selection, sort_keys=True, separators=(",", ":")) + "\n")
        evidence.retain_control(control, stage="irs-527-selection")
        owner.retain_file(
            archive_path,
            stage="irs-527-full-data-zip",
            raw_source=True,
            original_location=archive["url"],
            observed_at=archive["observed_at"],
        )
        tables = build_irs_527(
            archive_path,
            output_dir / "candidate",
            source_sha256=archive["sha256"],
            source_url=archive["url"],
            observed_at=archive["observed_at"],
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=tables,
            expected_keys=[f"{name}.parquet" for name in SCHEMAS],
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    acq = commands.add_parser("acquire")
    acq.add_argument("--output-dir", type=Path, required=True)
    acq.add_argument("--captures-out", type=Path, required=True)
    acq.add_argument("--archive-root", type=Path)

    bld = commands.add_parser("build")
    bld.add_argument("--selection", type=Path, required=True)
    bld.add_argument("--output-dir", type=Path, required=True)
    bld.add_argument("--archive-root", type=Path)
    bld.add_argument("--read-index", type=Path)

    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.output_dir, archive_root=args.archive_root)
        args.captures_out.parent.mkdir(parents=True, exist_ok=True)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps(selection))
    else:
        artifact, evidence = build(
            json.loads(args.selection.read_text()),
            args.output_dir,
            archive_root=args.archive_root,
            read_index=args.read_index,
        )
        print(json.dumps({
            "logical_id": artifact.pin.logical_id,
            "artifact_digest": artifact.pin.artifact_digest,
            "evidence": str(evidence),
        }))


if __name__ == "__main__":
    main()
