"""Capture bounded country summaries or replay retained snapshots into a local generation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    add_snapshot_build, inherit_read_index, journal_snapshot_releases, open_raw, raw_original, snapshot_inputs,
)
from spicy_regs.source_evidence import JOURNAL, CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_foreign_assistance import SCHEMAS, build_foreign_assistance

FAMILY = "foreign-assistance"
STAGE = "foreign-assistance-original"


def acquire(output_dir: Path, *, transport=None):
    """Fetch only the publisher's summary and dictionary; never its transaction corpus.

    The caller checks holdings before choosing a refresh. These mutable URLs
    identify source routes; the captured SHA-256 values identify versions.
    """
    import httpx
    from spicy_docs.sources.foreign_assistance import (
        DICTIONARY_URL, MAX_BYTES, SUMMARY_URL, read_dictionary, summary_records, validate_scope,
    )

    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        retained = evidence.transport(transport, stage=STAGE, max_bytes=MAX_BYTES,
                                      response_header_names=("etag", "last-modified", "content-length"))
        bodies = {}
        with httpx.Client(transport=retained, timeout=60, headers={"Accept-Encoding": "identity"}) as client:
            for url, media in ((SUMMARY_URL, "text/csv"), (DICTIONARY_URL, "application/pdf")):
                response = client.get(url).raise_for_status()
                if response.headers.get("content-type", "").split(";")[0].lower() != media:
                    raise ValueError("Foreign assistance download media type differs")
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise ValueError("Foreign assistance download must retain identity bytes")
                bodies[url] = response.content
        events = [json.loads(line) for line in (evidence.artifact_dir / JOURNAL).read_text().splitlines()]
        captures = {event["requested_url"]: {
            "request_url": event["requested_url"], "response_sha256": event["sha256"],
            "byte_size": event["byte_size"], "observed_at": event["observed_at"],
        } for event in events if event["event"] == "capture" and event["stage"] == STAGE}
        scope = validate_scope({"capture": captures[SUMMARY_URL], "dictionary_capture": captures[DICTIONARY_URL]})
        read_dictionary(bodies[DICTIONARY_URL], scope["dictionary_capture"])
        for _ in summary_records(bodies[SUMMARY_URL], scope=scope):
            pass
        return {"releases": [scope]}


def build(readers, output_dir: Path, *, archives, archive_root: Path | None = None, read_index: Path | None = None):
    """Use ordinary source evidence and generation admission, preserving every snapshot."""
    from spicy_docs.sources.foreign_assistance import MAX_BYTES, read_dictionary

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each foreign assistance generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    store = open_raw(archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        retained = set()
        for reader in readers:
            scope = reader.collection_outcome["requestedScope"]
            dictionary = scope["dictionary_capture"]
            with store.open(dictionary["response_sha256"]) as stream:
                read_dictionary(stream.read(MAX_BYTES + 1), dictionary)
            for kind in ("capture", "dictionary_capture"):
                capture = scope[kind]
                ref = capture["response_sha256"]
                if ref in retained:
                    continue
                original = Path(archives[ref]) if kind == "capture" else raw_original(store, ref, capture["byte_size"])
                if original.stat().st_size != capture["byte_size"]:
                    raise ValueError("Foreign assistance retained original size differs")
                saved = evidence.retain_file(
                    original, stage=STAGE, raw_source=True, original_location=capture["request_url"],
                    source_url=capture["request_url"], observed_at=capture["observed_at"],
                    dataset_id="foreign-assistance-country-summary", native_id=kind,
                    source_version=ref, covered_period="publisher-country-summary-observed-file",
                )
                if saved["sha256"] != ref:
                    raise ValueError("Foreign assistance retained original digest differs")
                retained.add(ref)
        journal_snapshot_releases(evidence, readers, "foreign-assistance-source-release")
        tables = build_foreign_assistance(readers, output_dir)
        artifact = build_generation(
            output_dir / "generation", family=FAMILY, files=tables,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(), read_snapshot=read_snapshot, owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    from spicy_docs.sources.foreign_assistance_profile import FOREIGN_ASSISTANCE_PROFILE, iter_retained_pages

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire", help="Capture one current country summary and dictionary")
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path)
    add_snapshot_build(commands)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out)}))
        return
    readers, archives = snapshot_inputs(args, FOREIGN_ASSISTANCE_PROFILE, iter_retained_pages)
    artifact, evidence = build(readers, args.output_dir, archives=archives,
                               archive_root=args.archive_root, read_index=args.read_index)
    print(json.dumps({"directory": str(args.output_dir / "generation"), "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest}))


if __name__ == "__main__":
    main()
