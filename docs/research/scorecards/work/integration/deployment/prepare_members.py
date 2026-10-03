"""Replay qualified retained community rosters with current publication lineage.

No source acquisition or publication. Existing remote table copies must already
match the supplied immutable index; every output is compared to the qualified
retained generation before a new candidate is sealed.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import shutil

import httpx
import pyarrow.parquet as pq

from spicy_docs.sources.legislators import LegislatorsAcquirer, LegislatorsBudget
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_members import build_members


def prepare(args):
    if args.output.exists():
        raise ValueError("Use a new candidate directory")
    args.output.mkdir(parents=True)
    index = publication.parse_index(args.index.read_bytes())
    reference = verify_generation(args.qualified_generation)
    retained = verify_evidence(args.retained_evidence)
    if reference.root["inputs"][0]["artifactDigest"] != retained.pin.artifact_digest:
        raise ValueError("Qualified generation and retained input evidence disagree")
    originals = {}
    for line in (args.retained_evidence / "journal.jsonl").read_text().splitlines():
        event = json.loads(line)
        if event["event"] == "capture" and event["stage"] in {"current", "historical"}:
            raw = (args.retained_evidence / event["blob_member"]).read_bytes()
            if "sha256:" + sha256(raw).hexdigest() != event["sha256"]:
                raise ValueError("Retained roster body does not match its admitted capture")
            originals[event["requested_url"]] = (event, raw)
    if len(originals) != 2:
        raise ValueError("Both qualified original rosters are required")
    names = tuple(reference.root["spec"]["tables"])
    for name in names:
        member = publication.single_member(index, name)
        path = args.current_tables / name
        if (
            path.stat().st_size != member.byte_size
            or "sha256:" + sha256(path.read_bytes()).hexdigest() != member.sha256
        ):
            raise ValueError("Current local member table differs from captured publication pin")
        shutil.copyfile(path, args.output / name)
    evidence = CaptureEvidence(args.output, "members")
    evidence.inherit(index, public_url=args.public_url)
    evidence.event(
        "qualified-retained-replay", source_generation=reference.pin.as_dict(), source_evidence=retained.pin.as_dict()
    )
    seen = []
    observed: list[datetime | None] = [None]

    def serve(request):
        event, raw = originals[str(request.url)]
        if str(request.url) in seen:
            raise ValueError("Reader repeated a retained roster request")
        seen.append(str(request.url))
        observed[0] = datetime.fromisoformat(event["observed_at"])
        evidence.capture(
            CapturedBodyResponse(
                event["requested_url"],
                event["resolved_url"],
                event["status_code"],
                event["content_type"],
                event["observed_at"],
                raw,
            ),
            stage=event["stage"],
        )
        return httpx.Response(200, content=iter((raw,)), headers={"content-type": event["content_type"]})

    def clock():
        if observed[0] is None:
            raise ValueError("No retained observation selected")
        return observed[0]

    try:
        with LegislatorsAcquirer(
            budget=LegislatorsBudget(
                max_requests=2, max_bytes=8 * 1024 * 1024, timeout_seconds=60, min_request_interval_seconds=0
            ),
            transport=httpx.MockTransport(serve),
            clock=clock,
        ) as acquirer:
            files = build_members(args.output, acquirer=acquirer, evidence=evidence)
        if set(seen) != set(originals):
            raise ValueError("Reader did not use both retained rosters")
        counts = {}
        for path in files:
            actual = pq.ParquetFile(path).read().to_pylist()
            expected = pq.ParquetFile(args.qualified_generation / path.name).read().to_pylist()

            def canonical(rows):
                return sorted(json.dumps(row, sort_keys=True) for row in rows)

            if canonical(actual) != canonical(expected):
                raise ValueError(f"Installed reader changed qualified source literals: {path.name}")
            counts[path.stem] = len(actual)
        artifact = build_generation(
            args.output / "generation",
            family="members",
            files=files,
            expected_keys=names,
            schemas=expected_schemas(),
            read_snapshot=index,
            inputs=evidence.inputs(),
        )
        verify_generation(args.output / "generation", expected_pin=artifact.pin)
        verify_evidence(evidence.artifact_dir)
        report = {
            "status": "prepared_not_published",
            "generation": artifact.pin.as_dict(),
            "generation_directory": str(args.output / "generation"),
            "evidence_directory": str(evidence.artifact_dir),
            "read_snapshot_path": str(args.index),
            "source_reference_generation": reference.pin.as_dict(),
            "counts": counts,
            "all_retained_literal_rows_matched": True,
            "source_network_requests": 0,
            "prior_family": index["families"]["members"]["artifactDigest"],
        }
        (args.output / "preparation.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
    except BaseException as error:
        evidence.finish(error)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("index", "qualified-generation", "retained-evidence", "current-tables", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--public-url", default="https://data.spicygov.ai")
    prepare(parser.parse_args())


if __name__ == "__main__":
    main()
