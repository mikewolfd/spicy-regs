"""Capture or replay one Senate veto-summary snapshot through ordinary generations."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import inherit_read_index
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_historical_vetoes import SCHEMAS, build_historical_vetoes

FAMILY = "historical-vetoes"


def acquire(output_dir: Path, *, transport=None, archive_root: Path | None = None):
    """One bounded public GET; retain the original before interpreting it."""
    from spicy_docs.sources.historical_vetoes import MAX_BYTES, SOURCE_URL, read_veto_table
    from spicy_docs.transport.download import BoundedAcquirer

    def validate(url):
        if url != SOURCE_URL:
            raise ValueError("Only the selected Senate veto-summary URL is allowed")
        return url

    evidence = CaptureEvidence(Path(output_dir), FAMILY, archive_root=archive_root)
    try:
        with BoundedAcquirer(
            validate_url=validate,
            max_requests=1,
            transport=evidence.transport(
                transport,
                stage="senate-veto-table",
                max_bytes=MAX_BYTES,
                response_header_names=("last-modified", "etag"),
            ),
        ) as client:
            capture = client.capture(SOURCE_URL, max_bytes=MAX_BYTES)
        if capture.media_type != "text/html":
            raise ValueError("Senate summary response is not HTML")
        # The selection must name the retained wire bytes as well as the parsed
        # body. Refuse a server that ignores Accept-Encoding: identity.
        with evidence.store.open(capture.sha256) as stream:
            if stream.read(MAX_BYTES + 1) != capture.body:
                raise ValueError("Senate summary capture differs from the retained original")
        read_veto_table(capture.body, source_url=SOURCE_URL, observed_at=capture.observed_at)
        selection = {
            "request_url": SOURCE_URL,
            "response_sha256": capture.sha256,
            "observed_at": capture.observed_at,
            "byte_size": len(capture.body),
        }
        evidence.event("selection", selection=selection)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return selection


def build(selection: dict, output_dir: Path, *, archive_root: Path | None = None, read_index: Path | None = None):
    """Replay exact originals offline; never select shared publication state."""
    from spicy_docs.sources.historical_vetoes import MAX_BYTES, read_veto_table

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each veto snapshot build")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="veto-capture-selection")
        with evidence.store.open(selection["response_sha256"]) as stream:
            body = stream.read(MAX_BYTES + 1)
        if (
            len(body) != selection["byte_size"]
            or len(body) > MAX_BYTES
            or "sha256:" + hashlib.sha256(body).hexdigest() != selection["response_sha256"]
        ):
            raise ValueError("Veto original differs from its selected bytes/digest")
        evidence.retain_bytes(
            body,
            stage="senate-veto-replay",
            raw_source=True,
            dataset_id="family:" + FAMILY,
            native_id="vetoCounts",
            covered_period="1789-present",
            source_version=selection["response_sha256"],
            original_location=selection["request_url"],
            observed_at=selection["observed_at"],
        )
        table = read_veto_table(body, source_url=selection["request_url"], observed_at=selection["observed_at"])
        tables = build_historical_vetoes(table, output_dir)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("acquire", "build"):
        command = commands.add_parser(name)
        command.add_argument("--output-dir", type=Path, required=True)
        command.add_argument("--archive-root", type=Path)
        command.add_argument(
            "--selection", type=Path, required=True, help="Capture-selection output for acquire; exact input for build"
        )
        if name == "build":
            command.add_argument("--read-index", type=Path, help="Pinned prior publication index for guarded refresh")
    args = parser.parse_args()
    if args.command == "acquire":
        if args.selection.exists():
            parser.error("Use a new selection path; dated snapshots are never overwritten")
        selection = acquire(args.output_dir, archive_root=args.archive_root)
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
