"""Acquire or replay commit-pinned US-CBO/eval-projections data through ordinary generations."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, inherit_read_index
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cbo_eval_projections import SCHEMAS, build_cbo_eval_projections

FAMILY = "cbo-eval-projections"
STAGE = "cbo-eval-projections-repo"


def acquire(
    output_dir: Path,
    *,
    commit: str | None = None,
    transport=None,
    archive_root: Path | None = None,
) -> dict:
    """Capture all allowlisted US-CBO/eval-projections files at one pinned commit into raw."""
    from spicy_docs.sources.cbo_eval_projections import (
        ALL_FILES,
        MAX_FILE_BYTES,
        PINNED_COMMIT,
        CboEvalProjectionsAcquirer,
        EvalProjectionsBudget,
        check_commit,
    )

    pinned = check_commit(PINNED_COMMIT if commit is None else commit)
    evidence = CaptureEvidence(Path(output_dir), FAMILY, archive_root=archive_root)
    try:
        retained = evidence.transport(
            transport,
            stage=STAGE,
            max_bytes=MAX_FILE_BYTES,
            response_header_names=("etag", "last-modified"),
        )
        with CboEvalProjectionsAcquirer(
            budget=EvalProjectionsBudget(max_requests=len(ALL_FILES) + 2, max_file_bytes=MAX_FILE_BYTES),
            transport=retained,
        ) as acquirer:
            acquired = acquirer.acquire_dataset(pinned, files=ALL_FILES)
        selection = {
            "repository": "US-CBO/eval-projections",
            "commit": pinned,
            "observed_at": max(r.observed_at for r in acquired.receipts.values()),
            "files": {
                rel_path: {
                    "request_url": receipt.url,
                    "response_sha256": receipt.sha256,
                    "observed_at": receipt.observed_at,
                    "byte_size": receipt.byte_size,
                }
                for rel_path, receipt in sorted(acquired.receipts.items())
            },
        }
        evidence.event("selection", selection=selection)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection


def build(
    selection: dict,
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
    require_full_reconciliation: bool = True,
):
    """Replay exact raw bytes offline and package all 5 normalized Parquet tables."""
    from spicy_docs.sources.cbo_eval_projections import (
        ALL_FILES,
        MAX_FILE_BYTES,
        check_commit,
        parse_eval_bundle,
        raw_url,
    )

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each cbo-eval-projections build")
    commit = check_commit(selection["commit"])
    file_entries = selection["files"]
    missing = [rel for rel in ALL_FILES if rel not in file_entries]
    if missing:
        raise ValueError(f"Selection omits required eval-projections files: {missing}")

    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="cbo-eval-selection")
        raw_files: dict[str, bytes] = {}
        expected_hashes: dict[str, str] = {}
        for rel_path in ALL_FILES:
            entry = file_entries[rel_path]
            expected_url = raw_url(commit, rel_path)
            if entry["request_url"] != expected_url:
                raise ValueError(f"Selection URL for {rel_path} differs from pinned commit URL {expected_url}")
            with evidence.store.open(entry["response_sha256"]) as stream:
                body = stream.read(MAX_FILE_BYTES + 1)
            if (
                len(body) != entry["byte_size"]
                or len(body) > MAX_FILE_BYTES
                or "sha256:" + hashlib.sha256(body).hexdigest() != entry["response_sha256"]
            ):
                raise ValueError(f"Retained original for {rel_path} differs from selected bytes/digest")
            evidence.retain_bytes(
                body,
                stage=STAGE,
                raw_source=True,
                dataset_id="family:" + FAMILY,
                native_id=rel_path,
                covered_period="1982-2036",
                source_version=commit,
                original_location=entry["request_url"],
                observed_at=entry["observed_at"],
            )
            raw_files[rel_path] = body
            expected_hashes[rel_path] = entry["response_sha256"]
        bundle = parse_eval_bundle(
            raw_files,
            commit=commit,
            observed_at=selection["observed_at"],
            expected_sha256=expected_hashes,
        )
        tables = build_cbo_eval_projections(
            bundle,
            output_dir,
            require_full_reconciliation=require_full_reconciliation,
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
            command.add_argument("--commit", help="Full 40-char commit SHA (defaults to PINNED_COMMIT)")
        else:
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
