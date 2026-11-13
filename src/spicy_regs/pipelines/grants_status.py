"""Capture a bounded Grants.gov status query, then build independently timed ordinary status tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    ReleaseBlobs, admit_release, inherit_read_index, journal_snapshot_releases, merged_scopes, open_raw, raw_original,
)
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_grants_status import OUTPUTS, SCHEMAS, build_grants_status

FAMILY = "grants-status"
STAGE = "grants-status-search"


def capture(selection, output_dir, *, transport=None):
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.grants_gov_status import MAX_BYTES, MAX_PAGES, StatusReader
    from spicy_docs.sources.grants_gov_status_profile import status_scope

    captures = []
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        try:
            with StatusReader(budget=PagedJsonBudget(MAX_PAGES, MAX_BYTES, 60, 0.2), transport=transport) as reader:
                for page in reader.read(selection):
                    evidence.capture(page.capture, stage=STAGE)
                    captures.append(page.capture)
            scope = status_scope(selection, captures)
        except Exception as error:
            evidence.refusal(error, stage=STAGE)
            raise
        evidence.event("grants-status-query-complete", scope=scope)
    return {"releases": [scope]}


def build(scopes, output_dir: Path, *, archive_root=None, read_index=None):
    from spicy_docs.sources.grants_gov_status_profile import GRANTS_GOV_STATUS_PROFILE, iter_retained_pages, validate_scope

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each Grants status generation")
    raw = open_raw(archive_root)
    blobs = ReleaseBlobs(raw, output_dir / "source-releases/blobs")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        readers, retained = [], set()
        for index, scope in enumerate(scopes):
            validate_scope(scope)
            readers.append(admit_release(
                GRANTS_GOV_STATUS_PROFILE, iter_retained_pages(scope, blob_source=blobs), scope,
                output_dir / "source-releases" / str(index), blobs=blobs,
                observed=[capture["observed_at"] for capture in scope["captures"]],
            ))
            for capture in scope["captures"]:
                digest = capture["response_sha256"]
                if digest in retained:
                    continue
                original = raw_original(raw, digest, capture["byte_size"])
                saved = evidence.retain_file(original, stage=STAGE, raw_source=True,
                                             original_location=capture["request_url"],
                                             observed_at=capture["observed_at"], request_selection=capture["request_body"])
                if saved["sha256"] != digest:
                    raise ValueError("Grants status original differs from its admitted digest")
                retained.add(digest)
        journal_snapshot_releases(evidence, readers, "grants-status-source-release")
        files = build_grants_status(readers, output_dir)
        artifact = build_generation(
            output_dir / "generation", family=FAMILY, files=files, expected_keys=OUTPUTS,
            schemas={table: described_schema(schema) for table, schema in SCHEMAS.items()},
            inputs=evidence.inputs(), read_snapshot=read_snapshot, owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    from spicy_docs.sources.grants_gov_status import StatusSelection

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    acquire = commands.add_parser("acquire")
    acquire.add_argument("--opportunity-number", required=True)
    acquire.add_argument("--agencies", default="")
    acquire.add_argument("--eligibilities", default="")
    acquire.add_argument("--statuses", default="forecasted|posted|closed|archived")
    acquire.add_argument("--page-size", type=int, default=10)
    acquire.add_argument("--output-dir", type=Path, required=True)
    acquire.add_argument("--captures-out", type=Path, required=True)
    derive = commands.add_parser("build")
    derive.add_argument("--captures", type=Path, action="append", required=True)
    derive.add_argument("--output-dir", type=Path, required=True)
    derive.add_argument("--archive-root", type=Path)
    derive.add_argument("--read-index", type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = capture(StatusSelection(args.opportunity_number, args.agencies, args.eligibilities,
                                             args.statuses, args.page_size), args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps(selection))
        return
    scopes = merged_scopes(json.loads(path.read_text()) for path in args.captures)
    artifact, evidence = build(scopes, args.output_dir, archive_root=args.archive_root, read_index=args.read_index)
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest}))


if __name__ == "__main__":
    main()
