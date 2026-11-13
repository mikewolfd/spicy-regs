"""Capture official IRS originals and build one explicitly selected Form 990 filing slice."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import download_original, inherit_read_index, raw_original
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_irs_form990 import SCHEMAS, build_irs_form990

FAMILY = "irs-form990"


def acquire(originals, output_dir, *, transport=None):
    """Acquire caller-selected URL/size/ETag descriptors; never crawl or guess a filing URL."""
    from spicy_docs.sources import irs_form990 as source

    captured = []
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        for selected in originals:
            bound = source.MAX_INDEX_BYTES if selected["url"].endswith(".csv") else source.MAX_ARCHIVE_BYTES
            captured.append(
                download_original(
                    evidence,
                    lambda acquirer, store, item=selected: source.acquire_original(acquirer, store=store, **item),
                    stage="irs-original",
                    max_bytes=bound,
                    validate_url=source.validate_url,
                    transport=transport,
                )
            )
    return captured


def validate_selection(selection):
    from spicy_docs.sources import irs_form990 as source

    if set(selection) != {"submission_year", "index", "archive", "object_ids"}:
        raise ValueError("IRS filing selection fields differ")
    year = selection["submission_year"]
    if type(year) is not int or not 2011 <= year <= 2099:
        raise ValueError("IRS selection requires an explicit index year")
    for kind in ("index", "archive"):
        item = selection[kind]
        if set(item) != {"request_url", "response_sha256", "byte_size", "observed_at"}:
            raise ValueError("IRS original descriptor fields differ")
        source.validate_url(item["request_url"])
        if f"/{year}/" not in item["request_url"] or item["request_url"].endswith(".csv") != (kind == "index"):
            raise ValueError("IRS original differs from the selected year or format")
        if kind == "index" and item["request_url"] != f"{source.BASE_URL}{year}/index_{year}.csv":
            raise ValueError("IRS index URL differs from selected year")
        bound = source.MAX_INDEX_BYTES if kind == "index" else source.MAX_ARCHIVE_BYTES
        if type(item["byte_size"]) is not int or not 0 < item["byte_size"] <= bound:
            raise ValueError("IRS original exceeds byte bound")
        if re.fullmatch(r"sha256:[0-9a-f]{64}", item["response_sha256"]) is None:
            raise ValueError("IRS original digest is malformed")
        if datetime.fromisoformat(item["observed_at"]).utcoffset() is None:
            raise ValueError("IRS capture requires a timezone")
    return selection


def load_records(selection, store):
    """Verify original identities and replay the exact selected membership from the retained index and ZIP."""
    from spicy_docs.sources import irs_form990 as source

    validate_selection(selection)
    originals = {
        name: raw_original(store, item["response_sha256"], item["byte_size"])
        for name, item in ((name, selection[name]) for name in ("index", "archive"))
    }
    with originals["index"].open("rb") as stream:
        filings = source.read_index(
            stream, submission_year=selection["submission_year"], object_ids=selection["object_ids"]
        )
    with originals["archive"].open("rb") as stream:
        records = source.read_selected_returns(
            stream, filings=filings, archive_sha256=selection["archive"]["response_sha256"]
        )
    for record in records:
        record["source_locator"].update({"index": selection["index"], "archive": selection["archive"]})
    return records, originals


def build(selection, output_dir, *, archive_root=None, read_index=None):
    """Write a maintained generation plus original/processing evidence; select no shared state."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each IRS generation")
    store = CentralRawArchiveStore(archive_root or default_raw_archive_root(), create=False)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        records, originals = load_records(selection, store)
        read_snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="irs-selection")
        for name, original in originals.items():
            descriptor = selection[name]
            saved = evidence.retain_file(
                original,
                stage="irs-original",
                raw_source=True,
                original_location=descriptor["request_url"],
                source_url=descriptor["request_url"],
                observed_at=descriptor["observed_at"],
            )
            if saved["sha256"] != descriptor["response_sha256"]:
                raise ValueError("IRS retained original differs from selection")
        paths = build_irs_form990(records, output_dir)
        evidence.declare_used_inputs(
            [
                {
                    "location": selection[name]["request_url"],
                    "sha256": selection[name]["response_sha256"],
                    "bytes": selection[name]["byte_size"],
                    "asset_kind": name,
                }
                for name in ("index", "archive")
            ],
            scope_id="irs-form990-selected-filings",
        )
        evidence.event(
            "irs-selected-filings",
            object_ids=selection["object_ids"],
            submission_year=selection["submission_year"],
            coverage="Explicit selected index membership and XML members only; not a full-year or archive census",
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=paths,
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
    capture = commands.add_parser("acquire")
    capture.add_argument("--originals", type=Path, required=True, help="JSON list of official URL/size/ETag selections")
    capture.add_argument("--output-dir", type=Path, required=True)
    capture.add_argument("--captures-out", type=Path, required=True)
    replay = commands.add_parser("build")
    replay.add_argument("--selection", type=Path, required=True)
    replay.add_argument("--output-dir", type=Path, required=True)
    replay.add_argument("--read-index", type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        captured = acquire(json.loads(args.originals.read_text()), args.output_dir)
        args.captures_out.write_text(json.dumps(captured, indent=2) + "\n")
        print(json.dumps({"captures": str(args.captures_out)}))
    else:
        artifact, evidence = build(json.loads(args.selection.read_text()), args.output_dir, read_index=args.read_index)
        print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "sourceEvidence": str(evidence)}))


if __name__ == "__main__":
    main()
