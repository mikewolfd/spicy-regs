"""Acquire selected SEC JSON originals, then build an offline evidenced generation."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence

FAMILY = "sec-edgar"


def source_context(evidence):
    """Bind source interpretation to its actual code, including an unreleased checkout."""
    from spicy_docs.sources import sec_edgar

    digest = hashlib.sha256(Path(sec_edgar.__file__).read_bytes()).hexdigest()
    return evidence.for_source(
        "sec-edgar",
        "full",
        parser_version="sec-edgar-v1:sha256:" + digest,
        policy_decision_id="ss-h9qm-official-public-json",
    )


def acquire(issuers, start, end, output_dir, *, contact_email, transport=None, reuse=()):
    from spicy_docs.sources.sec_edgar import (
        MAX_BYTES,
        EdgarAcquirer,
        _checked_agent,
        cik,
        declared_user_agent,
        iso_date,
    )

    if iso_date(start) > iso_date(end):
        raise ValueError("SEC history window is reversed")
    issuers = [cik(issuer) for issuer in issuers]
    if not issuers or len(issuers) != len(set(issuers)):
        raise ValueError("Declare unique SEC issuer CIKs")
    _checked_agent(declared_user_agent(contact_email), live=transport is None)
    sources = []
    reusable = {(item["cik"], item["kind"], item["continuation"]): item for item in reuse}
    if len(reusable) != len(reuse):
        raise ValueError("Reused SEC source routes must be unique")
    with acquisition_evidence(FAMILY, output_dir=Path(output_dir)) as evidence:
        owner = source_context(evidence)
        retained = owner.transport(transport, stage="sec-edgar-json", max_bytes=MAX_BYTES)
        with EdgarAcquirer(contact_email=contact_email, transport=retained) as reader:

            def get(issuer, kind, continuation=None):
                from spicy_docs.sources.sec_edgar import (
                    companyfacts_url,
                    read_companyfacts,
                    read_submissions,
                    submissions_url,
                )
                from spicy_regs.pipelines.raw_releases import open_raw, raw_original

                source = reusable.get((issuer, kind, continuation))
                if source is None:
                    parsed, capture = reader.acquire(issuer, kind=kind, continuation=continuation)
                    source = {
                        "cik": issuer,
                        "kind": kind,
                        "continuation": continuation,
                        "url": capture.requested_url,
                        "sha256": capture.sha256,
                        "bytes": capture.byte_size,
                        "observed_at": capture.observed_at,
                    }
                else:
                    url = submissions_url(issuer, continuation) if kind == "submissions" else companyfacts_url(issuer)
                    if source["url"] != url or not 0 < source["bytes"] <= MAX_BYTES:
                        raise ValueError("Reused SEC source differs from its selected identity")
                    path = raw_original(open_raw(), source["sha256"], source["bytes"])
                    body = path.read_bytes()
                    parsed = (
                        read_submissions(body, issuer, continuation=continuation)
                        if kind == "submissions"
                        else read_companyfacts(body, issuer)
                    )
                    owner.retain_file(
                        path,
                        stage="sec-edgar-json",
                        raw_source=True,
                        original_location=url,
                        observed_at=source["observed_at"],
                    )
                sources.append(source)
                owner.event(
                    "sec-edgar-identity",
                    dataset_id="sec-edgar-" + kind,
                    native_id=issuer,
                    covered_period=f"{start}/{end}",
                    source_version=source["sha256"],
                    source_url=source["url"],
                    sha256=source["sha256"],
                )
                return parsed

            for issuer in issuers:
                root = get(issuer, "submissions")
                for file in root.continuations:
                    if file["filingFrom"] <= end and file["filingTo"] >= start:
                        get(issuer, "submissions", file["name"])
                get(issuer, "companyfacts")
    return {
        "format": "spicy-sec-edgar-captures",
        "version": 1,
        "filing_start": start,
        "filing_end": end,
        "sources": sources,
    }


def build(selection, concepts, output_dir, *, fact_start, fact_end, archive_root=None, read_index=None):
    from spicy_regs.generations import build_generation
    from spicy_regs.native_types import described_schema
    from spicy_regs.pipelines.raw_releases import inherit_read_index, open_raw
    from spicy_regs.transforms.build_sec_edgar import SCHEMAS, build_tables

    output_dir = Path(output_dir)
    if (output_dir / "candidate").exists() or (output_dir / "generation").exists():
        raise ValueError("Use fresh SEC candidate output paths")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        snapshot = inherit_read_index(evidence, read_index)
        evidence.retain_bytes(
            json.dumps(
                {"captures": selection, "concepts": concepts, "fact_start": fact_start, "fact_end": fact_end},
                sort_keys=True,
            ).encode(),
            stage="sec-edgar-selection",
        )
        tables = build_tables(
            selection,
            concepts,
            output_dir / "candidate",
            fact_start=fact_start,
            fact_end=fact_end,
            store=open_raw(archive_root),
            evidence=source_context(evidence),
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=tables,
            expected_keys=[name + ".parquet" for name in SCHEMAS],
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
    capture = commands.add_parser("acquire")
    capture.add_argument("--cik", action="append", required=True, dest="issuers")
    capture.add_argument("--start", required=True)
    capture.add_argument("--end", required=True)
    capture.add_argument("--contact-email", required=True)
    capture.add_argument("--captures-out", type=Path, required=True)
    capture.add_argument(
        "--reuse-captures", type=Path, help="Prior capture selection whose exact raw originals are reused"
    )
    capture.add_argument("--output-dir", type=Path, required=True)
    candidate = commands.add_parser("build")
    candidate.add_argument("--captures", type=Path, required=True)
    candidate.add_argument("--concept", action="append", required=True, help="taxonomy:concept, repeatable")
    candidate.add_argument("--fact-start", required=True)
    candidate.add_argument("--fact-end", required=True)
    candidate.add_argument("--output-dir", type=Path, required=True)
    candidate.add_argument("--archive-root", type=Path)
    candidate.add_argument("--read-index", type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        reuse = json.loads(args.reuse_captures.read_text())["sources"] if args.reuse_captures else ()
        selection = acquire(
            args.issuers, args.start, args.end, args.output_dir, contact_email=args.contact_email, reuse=reuse
        )
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps(selection))
    else:
        concepts = [name.split(":", 1) for name in args.concept]
        artifact, evidence = build(
            json.loads(args.captures.read_text()),
            concepts,
            args.output_dir,
            fact_start=args.fact_start,
            fact_end=args.fact_end,
            archive_root=args.archive_root,
            read_index=args.read_index,
        )
        print(
            json.dumps(
                {
                    "logical_id": artifact.pin.logical_id,
                    "digest": artifact.pin.artifact_digest,
                    "evidence": str(evidence),
                }
            )
        )


if __name__ == "__main__":
    main()
