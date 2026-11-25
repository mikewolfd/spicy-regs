"""Acquire House and Senate history pages and build evidenced congress-sessions generations."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.raw_releases import inherit_read_index, open_raw
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_congress_sessions import PARTY_CONTROL, SCHEMAS, SESSIONS, build_congress_sessions

FAMILY = "congress-sessions"
_REQUIRED_SOURCES = ("house_session_dates", "house_party_divisions", "senate_party_division")


def _source_context(evidence):
    from spicy_docs.sources.congress import party_divisions

    digest = hashlib.sha256(Path(party_divisions.__file__).read_bytes()).hexdigest()
    return evidence.for_source(
        FAMILY,
        "full",
        parser_version="congress-sessions-v1:sha256:" + digest,
        policy_decision_id="ss-e9qj-congress-sessions-party-divisions",
    )


def acquire(
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    transport=None,
    max_requests: int = 6,
    max_bytes: int | None = None,
    timeout_seconds: float = 30.0,
    min_request_interval_seconds: float = 0.5,
) -> dict:
    """Capture the three official House and Senate history pages into the raw archive."""
    from spicy_docs.sources.congress.party_divisions import (
        DEFAULT_MAX_BYTES,
        PartyDivisionAcquirer,
        PartyDivisionBudget,
    )

    limit = DEFAULT_MAX_BYTES if max_bytes is None else max_bytes
    budget = PartyDivisionBudget(
        max_requests=max_requests,
        max_bytes=limit,
        timeout_seconds=timeout_seconds,
        min_request_interval_seconds=min_request_interval_seconds,
    )
    evidence = CaptureEvidence(Path(output_dir), FAMILY, archive_root=archive_root)
    try:
        owner = _source_context(evidence)
        retained = owner.transport(transport, stage="congress-sessions-html", max_bytes=limit)
        with PartyDivisionAcquirer(budget=budget, transport=retained) as acquirer:
            house_sessions = acquirer.acquire_house_session_dates()
            house_divisions = acquirer.acquire_house_divisions()
            senate_division = acquirer.acquire_senate_division()
        pages = {
            "house_session_dates": house_sessions,
            "house_party_divisions": house_divisions,
            "senate_party_division": senate_division,
        }
        sources = {}
        for key, page in pages.items():
            cap = page.capture
            sources[key] = {
                "url": cap.requested_url,
                "sha256": cap.sha256,
                "bytes": cap.byte_size,
                "observed_at": cap.observed_at,
            }
            owner.event(
                "congress-sessions-captured",
                source_key=key,
                source_url=cap.requested_url,
                sha256=cap.sha256,
                byte_size=cap.byte_size,
            )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return {
        "format": "spicy-congress-sessions-captures",
        "version": 1,
        "sources": sources,
    }


def _load_original(store, source: dict, expected_url: str, *, max_bytes: int) -> bytes:
    if source.get("url") != expected_url:
        raise ValueError(f"Congress sessions source URL differs from expected {expected_url}")
    size = source.get("bytes")
    if type(size) is not int or not 0 < size <= max_bytes:
        raise ValueError("Congress sessions source size is invalid or exceeds bound")
    with store.open(source["sha256"]) as stream:
        body = stream.read(size + 1)
    if len(body) != size or "sha256:" + hashlib.sha256(body).hexdigest() != source["sha256"]:
        raise ValueError("Congress sessions original differs from pinned size or SHA-256")
    return body


def build(
    selection: dict,
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
):
    """Build an offline evidenced generation from pinned House and Senate history pages."""
    from spicy_docs.sources.congress.party_divisions import (
        HOUSE_PARTY_DIVISIONS_URL,
        HOUSE_SESSION_DATES_URL,
        MAX_PAGE_BYTES,
        SENATE_PARTY_DIVISION_URL,
    )

    if selection.get("format") != "spicy-congress-sessions-captures" or selection.get("version") != 1:
        raise ValueError("Unsupported congress-sessions capture selection")
    sources = selection.get("sources")
    if not isinstance(sources, dict) or set(sources) != set(_REQUIRED_SOURCES):
        raise ValueError("Congress sessions selection must name all three official history pages")

    output_dir = Path(output_dir)
    if (output_dir / "candidate").exists() or (output_dir / "generation").exists():
        raise ValueError("Use fresh congress-sessions candidate output paths")

    store = open_raw(archive_root)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        snapshot = inherit_read_index(evidence, read_index)
        owner = _source_context(evidence)
        control = output_dir / "selection.json"
        control.write_text(json.dumps(selection, sort_keys=True, separators=(",", ":")) + "\n")
        evidence.retain_control(control, stage="congress-sessions-selection")

        expected_urls = {
            "house_session_dates": HOUSE_SESSION_DATES_URL,
            "house_party_divisions": HOUSE_PARTY_DIVISIONS_URL,
            "senate_party_division": SENATE_PARTY_DIVISION_URL,
        }
        bodies = {}
        for key, url in expected_urls.items():
            src = sources[key]
            body = _load_original(store, src, url, max_bytes=MAX_PAGE_BYTES)
            owner.retain_bytes(
                body,
                stage="congress-sessions-html",
                raw_source=True,
                original_location=url,
                source_version=src["sha256"],
                observed_at=src["observed_at"],
            )
            bodies[key] = body

        sessions_path, control_path = build_congress_sessions(
            house_sessions_body=bodies["house_session_dates"],
            house_divisions_body=bodies["house_party_divisions"],
            senate_division_body=bodies["senate_party_division"],
            house_sessions_source=sources["house_session_dates"],
            house_divisions_source=sources["house_party_divisions"],
            senate_division_source=sources["senate_party_division"],
            output_dir=output_dir / "candidate",
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=(sessions_path, control_path),
            expected_keys=(f"{SESSIONS}.parquet", f"{PARTY_CONTROL}.parquet"),
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
