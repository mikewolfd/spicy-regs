"""Bind a hosted converter invocation to one complete published prior entry."""

import argparse
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version
import json
import os
from pathlib import Path
import re


FAMILIES = frozenset({
    "dockets", "documents", "court-opinions", "roll-call-votes", "member-vote-terms", "bill-family",
})


def entry_digest(entry: dict) -> str:
    """Include timestamps, schemas, receipt descriptors and every table member."""
    return "sha256:" + sha256(json.dumps(entry, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def record_invocation(*, family: str, expected_entry: str, publish: bool, source_run: str,
                      bucket: str, output: Path, public_url: str, read_index) -> dict:
    if family not in FAMILIES or not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_entry):
        raise ValueError("Require one supported family and the SHA-256 of its complete prior entry")
    if publish and not bucket.strip():
        raise ValueError("Publication requires an explicitly named expected bucket")
    if source_run and (not publish or not re.fullmatch(r"[1-9][0-9]*", source_run)):
        raise ValueError("An earlier artifact requires publication mode and a positive source run ID")
    if not public_url.startswith("https://"):
        raise ValueError("Require the explicit HTTPS public data URL")
    index = read_index(public_url)
    entry = index["families"].get(family)
    if entry is None or entry_digest(entry) != expected_entry:
        raise ValueError("The complete published prior entry changed; no conversion was started")
    if "etlReceipts" in entry:
        raise ValueError("The family is already native; do not run a one-time conversion")
    record = {
        "observedAt": datetime.now(UTC).isoformat(), "family": family,
        "expectedEntrySha256": expected_entry, "entry": entry, "publicUrl": public_url,
        "publicationRequested": publish, "expectedBucket": bucket if publish else None,
        "sourceRunId": source_run or None,
        "revision": os.getenv("GITHUB_SHA"), "runId": os.getenv("GITHUB_RUN_ID"),
        "runAttempt": os.getenv("GITHUB_RUN_ATTEMPT"),
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "invocation.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", required=True)
    parser.add_argument("--entry-sha256", required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--source-run-id", default="")
    parser.add_argument("--expect-bucket", default="")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from spicy_regs.sources.publication import current_index

    record_invocation(family=args.family, expected_entry=args.entry_sha256, publish=args.publish,
                      source_run=args.source_run_id, bucket=args.expect_bucket, output=args.output,
                      public_url=os.getenv("R2_PUBLIC_URL", ""), read_index=current_index)
    (args.output / "source-runtime.json").write_text(json.dumps({
        "spicyDocsVersion": version("spicy-docs"),
        "meaning": "The maintained converter separately verifies clean hosted main and the installed pinned wheel.",
    }, indent=2) + "\n")


if __name__ == "__main__":
    main()
