"""Acquire publicly posted executive disclosures and build local evidenced generations."""

from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re

import pyarrow.parquet as pq

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, inherit_read_index, open_raw
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_executive_disclosures import (
    ACCESS_BASES,
    ACCESS_STATES,
    DEFAULT_BATCH_SIZE,
    SCHEMAS,
    TABLES,
    build_tables,
    is_v2_manifest,
    validate_selection,
)

FAMILY = "executive-disclosures"
POLICY_DECISION_ID = "ss-rhsd-public-executive-disclosures"
EVIDENCE_POLICIES = frozenset({"hash_only", "full"})
_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")


def _source_context(evidence: CaptureEvidence, *, policy: str = "hash_only"):
    from spicy_docs.sources import executive_disclosures as source_mod

    if policy not in EVIDENCE_POLICIES:
        raise ValueError(f"unsupported executive-disclosures evidence policy {policy!r}; expected hash_only or full")
    digest = hashlib.sha256(Path(source_mod.__file__).read_bytes()).hexdigest()
    return evidence.for_source(
        FAMILY,
        policy,
        parser_version=f"{source_mod.PARSER_REVISION}:sha256:{digest}",
        policy_decision_id=POLICY_DECISION_ID,
    )


def _json_line(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _table_summary(tables: list[Path]) -> dict:
    rows = {path.stem: pq.read_metadata(path).num_rows for path in tables}
    filings_path = next(path for path in tables if path.stem == TABLES[0])
    filings_tbl = pq.read_table(filings_path, columns=["filing_id", "access_status", "facts_status"])
    return {
        "family": FAMILY,
        "rows": rows,
        "filings": filings_tbl.to_pylist(),
    }


def build(
    selection: dict,
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
    evidence_policy: str = "hash_only",
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    """Build an evidenced 4-table generation from digest-pinned originals in CentralRawArchiveStore."""
    if evidence_policy not in EVIDENCE_POLICIES:
        raise ValueError(f"unsupported evidence_policy {evidence_policy!r}; expected hash_only or full")
    validate_selection(selection)
    output_dir = Path(output_dir).resolve()
    if output_dir.exists():
        raise ValueError("use a fresh output directory; prior candidates remain intact")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        snapshot = inherit_read_index(evidence, read_index)
        owner = _source_context(evidence, policy=evidence_policy)
        control = output_dir / "selection.json"
        control.write_text(json.dumps(selection, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        evidence.retain_control(control, stage="reviewed-filing-selection")
        tables = build_tables(
            selection,
            output_dir / "candidate",
            evidence=owner,
            store=open_raw(archive_root),
            batch_size=batch_size,
        )
        summary = _table_summary(tables)
        summary["evidence_policy"] = evidence_policy
        owner.event("executive-disclosures-summary", **summary)
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=tables,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return artifact, evidence.artifact_dir


def acquire(
    url: str,
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    accepted_terms: bool = False,
    transport=None,
) -> dict:
    """Acquire one explicitly selected public posting into CentralRawArchiveStore; Form 201 always refuses."""
    from spicy_docs.sources.executive_disclosures import ExecutiveDisclosureAcquirer

    output_dir = Path(output_dir)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        # Acquisition retains the exact raw PDF in local CentralRawArchiveStore for offline replay & visual review.
        owner = _source_context(evidence, policy="full")
        with ExecutiveDisclosureAcquirer(accepted_terms=accepted_terms, transport=transport) as reader:
            try:
                capture = reader.acquire_pdf(url)
            except Exception as error:
                owner.refusal(error, stage="public-filing")
                raise
        owner.capture(capture, stage="public-filing")
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return {
        "source_url": url,
        "source_sha256": capture.sha256,
        "byte_size": capture.byte_size,
        "acquired_at": capture.observed_at,
    }


def _try_reuse_retained(store, item: dict, *, max_bytes: int) -> tuple[str, int] | None:
    """Return ``(sha256, byte_size)`` if the exact retained original is already verified in CentralRawArchiveStore."""
    digest = item.get("source_sha256")
    if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
        return None
    expected_size = item.get("byte_size")
    if expected_size is not None and (type(expected_size) is not int or expected_size <= 0 or expected_size > max_bytes):
        return None
    try:
        with store.open(digest) as stream:
            body = stream.read(max_bytes + 1)
    except FileNotFoundError:
        return None
    if not body or len(body) > max_bytes:
        return None
    if expected_size is not None and len(body) != expected_size:
        return None
    if "sha256:" + hashlib.sha256(body).hexdigest() != digest:
        return None
    from spicy_docs.reading.pdf_bytes import check_pdf_bytes
    from spicy_docs.sources.executive_disclosures import ExecutiveDisclosureError

    check_pdf_bytes(body, error_type=ExecutiveDisclosureError, label="public OGE filing")
    return digest, len(body)


def acquire_manifest(
    manifest: dict,
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    max_requests: int = 16,
    accepted_terms: bool = False,
    transport=None,
) -> tuple[dict, dict]:
    """Acquire missing public postings declared in a v1 or v2 cohort manifest, reusing retained originals first.

    Route B (OGE Level I/II) remains disabled unless ``accepted_terms=True``; Form 201, agency requests,
    and non-public entries are never requested.
    """
    from spicy_docs.sources.executive_disclosures import (
        ExecutiveDisclosureAcquirer,
        MAX_BYTES,
        classify_filing_url,
    )

    if not isinstance(manifest, dict) or not manifest.get("cohort_id") or not manifest.get("coverage_note"):
        raise ValueError("manifest needs cohort_id and coverage_note")
    if is_v2_manifest(manifest) and (
        manifest.get("format") != "executive-disclosures-cohort" or manifest.get("version") != 2
    ):
        raise ValueError("unsupported v2 cohort manifest format/version")
    filings = manifest.get("filings")
    if not isinstance(filings, list) or not filings:
        raise ValueError("manifest requires a nonempty filings list")

    # Pre-validate filing identity, access_basis, and access_status up front before any HTTP request.
    seen_filing_ids: set[str] = set()
    for item in filings:
        if not isinstance(item, dict) or not isinstance(item.get("filing_id"), str) or not item["filing_id"].strip():
            raise ValueError("each manifest filing requires a nonempty filing_id")
        filing_id = item["filing_id"]
        if filing_id in seen_filing_ids:
            raise ValueError(f"duplicate filing_id in manifest: {filing_id!r}")
        seen_filing_ids.add(filing_id)
        access_basis = item.get("access_basis")
        if access_basis not in ACCESS_BASES:
            raise ValueError(f"unknown access_basis {access_basis!r} for filing {filing_id!r}")
        access_status = item.get("access_status")
        if access_status is not None and access_status not in ACCESS_STATES:
            raise ValueError(f"unknown access_status {access_status!r} for filing {filing_id!r}")

    updated = deepcopy(manifest)
    output_dir = Path(output_dir)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    reused = 0
    acquired = 0
    skipped = 0
    try:
        owner = _source_context(evidence, policy="full")
        store = evidence.store
        with ExecutiveDisclosureAcquirer(
            max_requests=max_requests,
            accepted_terms=accepted_terms,
            transport=transport,
        ) as reader:
            for item in updated["filings"]:
                access_basis = item["access_basis"]
                access_status = item.get("access_status")
                source_url = item.get("source_url")

                if access_basis != "public_posting" or access_status in {
                    "not_requested",
                    "requested",
                    "unavailable",
                    "expired",
                    "access_refused",
                }:
                    skipped += 1
                    continue

                if not isinstance(source_url, str) or not source_url.strip():
                    raise ValueError(f"filing {item['filing_id']!r} requires a public source_url")
                classify_filing_url(source_url)

                held = _try_reuse_retained(store, item, max_bytes=MAX_BYTES)
                if held is not None:
                    acquired_at = item.get("acquired_at")
                    if not isinstance(acquired_at, str) or not acquired_at.strip():
                        raise ValueError(
                            f"reused retained filing {item['filing_id']!r} requires its original acquired_at receipt timestamp"
                        )
                    sha256, byte_size = held
                    item["source_sha256"] = sha256
                    item["byte_size"] = byte_size
                    item["access_status"] = "available"
                    owner.event(
                        "executive-filing-reused",
                        filing_id=item["filing_id"],
                        source_url=source_url,
                        source_sha256=sha256,
                        byte_size=byte_size,
                    )
                    reused += 1
                    continue

                expected_sha256 = item.get("source_sha256")
                expected_size = item.get("byte_size")
                try:
                    capture = reader.acquire_pdf(source_url)
                except Exception as error:
                    owner.refusal(error, stage="public-filing")
                    raise
                if isinstance(expected_sha256, str) and expected_sha256 and capture.sha256 != expected_sha256:
                    raise ValueError(
                        f"acquired PDF digest for {item['filing_id']!r} ({capture.sha256}) "
                        f"differs from manifest source_sha256 ({expected_sha256})"
                    )
                if type(expected_size) is int and capture.byte_size != expected_size:
                    raise ValueError(
                        f"acquired PDF size for {item['filing_id']!r} ({capture.byte_size}) "
                        f"differs from manifest byte_size ({expected_size})"
                    )
                owner.capture(capture, stage="public-filing", filing_id=item["filing_id"])
                item["source_sha256"] = capture.sha256
                item["byte_size"] = capture.byte_size
                if not item.get("acquired_at"):
                    item["acquired_at"] = capture.observed_at
                item["access_status"] = "available"
                acquired += 1

        summary = {
            "cohort_id": updated["cohort_id"],
            "filings": len(updated["filings"]),
            "reused": reused,
            "acquired": acquired,
            "skipped": skipped,
        }
        owner.event("executive-manifest-acquisition", **summary)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return updated, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    capture = commands.add_parser("acquire", help="Acquire one explicitly selected public PDF into the raw archive")
    capture.add_argument("--url", required=True)
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--archive-root", type=Path, help="Raw archive directory (defaults to shared archive)")
    capture.add_argument(
        "--accepted-terms",
        action="store_true",
        default=False,
        help="Explicit caller affirmation of 5 U.S.C. § 13107 restrictions for Route B OGE Level I/II PDFs",
    )

    batch = commands.add_parser(
        "acquire-manifest",
        help="Acquire missing public postings in a v1/v2 manifest, reusing retained archive digests first",
    )
    batch.add_argument("--manifest", required=True, type=Path)
    batch.add_argument("--output-dir", required=True, type=Path)
    batch.add_argument("--manifest-out", type=Path, help="Optional path to write updated manifest JSON")
    batch.add_argument("--archive-root", type=Path, help="Raw archive directory (defaults to shared archive)")
    batch.add_argument("--max-requests", type=int, default=16, help="Bounded HTTP request budget (1-128)")
    batch.add_argument(
        "--accepted-terms",
        action="store_true",
        default=False,
        help="Explicit caller affirmation of 5 U.S.C. § 13107 restrictions for Route B OGE Level I/II PDFs",
    )

    candidate = commands.add_parser("build", help="Build an evidenced 4-table generation offline from retained PDFs")
    candidate.add_argument("--selection", required=True, type=Path)
    candidate.add_argument("--output-dir", required=True, type=Path)
    candidate.add_argument("--archive-root", type=Path, help="Raw archive holding the originals (read only)")
    candidate.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    candidate.add_argument(
        "--evidence-policy",
        choices=sorted(EVIDENCE_POLICIES),
        default="hash_only",
        help="Source evidence retention policy for R2 publication (default: hash_only)",
    )

    args = parser.parse_args()
    if args.command == "acquire":
        print(
            _json_line(
                acquire(
                    args.url,
                    args.output_dir,
                    archive_root=args.archive_root,
                    accepted_terms=args.accepted_terms,
                )
            )
        )
        return
    if args.command == "acquire-manifest":
        updated, summary = acquire_manifest(
            json.loads(args.manifest.read_text(encoding="utf-8")),
            args.output_dir,
            archive_root=args.archive_root,
            max_requests=args.max_requests,
            accepted_terms=args.accepted_terms,
        )
        if args.manifest_out is not None:
            args.manifest_out.parent.mkdir(parents=True, exist_ok=True)
            args.manifest_out.write_text(
                json.dumps(updated, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            summary = {**summary, "manifest_out": str(args.manifest_out)}
        print(_json_line(summary))
        return

    artifact, evidence_dir = build(
        json.loads(args.selection.read_text(encoding="utf-8")),
        args.output_dir,
        archive_root=args.archive_root,
        read_index=args.read_index,
        evidence_policy=args.evidence_policy,
    )
    summary = json.loads((args.output_dir / "summary.json").read_text(encoding="utf-8"))
    print(
        _json_line(
            {
                "artifactDigest": artifact.pin.artifact_digest,
                "artifact_digest": artifact.pin.artifact_digest,
                "directory": str(args.output_dir / "generation"),
                "sourceEvidence": str(evidence_dir),
                "evidence": str(evidence_dir),
                "evidencePolicy": args.evidence_policy,
                "rows": summary["rows"],
            }
        )
    )


if __name__ == "__main__":
    main()
