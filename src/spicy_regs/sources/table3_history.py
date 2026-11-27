"""Replay OLRC Table III into a separate historical, source-versioned destination.

The original compressed archive and decoded member remain bounded in memory
through the maintained SpicyDocs reader. Record output is written in batches;
no network acquisition or public-law/Congress filter runs here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import stat
import sys
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import asdict
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq
from spicy_docs.schemas.law_tables import usc_section_place
from spicy_docs.sources.uscode import iter_act_fragments, read_table3_bulk_member
from spicy_docs.sources.uscode.table3 import parse_act_fragment

from spicy_regs.etl_receipts import DatasetPolicy, RECEIPT_KEY, RECEIPT_SCHEMA, ReceiptContext, write_dataset

FAMILY = "table3-history"
READER_VERSION = "table3-historical-public/2"
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_MEMBER_BYTES = 192 * 1024 * 1024
MAX_FRAGMENT_BYTES = 4 * 1024 * 1024
STRING_FIELDS = (
    "id",
    "source_sha256",
    "member_sha256",
    "member_name",
    "release_point",
    "act_key",
    "act_native_id",
    "act_number",
    "congress",
    "act_date",
    "statutes_at_large_volume",
    "act_native_sequence",
    "act_insertion",
    "act_format",
    "act_print_in_supplement",
    "act_include_in_online_release_point",
    "act_public_law",
    "record_native_id",
    "record_native_sequence",
    "usc_key",
    "act_section",
    "record_page",
    "usc_title",
    "usc_section",
    "status",
    "record_print_in_supplement",
    "usc_section_key",
    "usc_place",
    "reader_version",
    "raw_source_row",
)
POSITION_FIELDS = ("act_record_position", "fragment_position", "fragment_record_position")
SCHEMA = pa.schema([(key, pa.string()) for key in STRING_FIELDS] + [(key, pa.int64()) for key in POSITION_FIELDS])
SCHEMAS = {"table3_historical_records": SCHEMA}
IDENTITY_FIELDS = ("member_sha256", "release_point", "act_key", "act_record_position")
POLICY = DatasetPolicy(
    "table3_historical_records", SCHEMA, IDENTITY_FIELDS, ("source_position",), policy_version=READER_VERSION
)


def digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def receipt_generation_id(*, expected_sha256: str, release_point: str) -> str:
    """Deterministic receipt generation identity for one pinned Table III replay."""
    return digest(json.dumps([READER_VERSION, expected_sha256, release_point], separators=(",", ":")).encode())


def historical_records(body: bytes, *, expected_sha256: str, release_point: str) -> Iterator[dict]:
    """Yield provisional source-versioned records; only exhaustion qualifies the read.

    Failed identity, bounds, CRC, unknown vocabulary or late malformed fragments
    raise. Native sequence values are retained but never used as position keys.
    """
    if digest(body) != expected_sha256:
        raise ValueError("Table III archive digest mismatch")
    member_name, stated_point, member = read_table3_bulk_member(
        body, max_bytes=MAX_ARCHIVE_BYTES, max_member_bytes=MAX_MEMBER_BYTES
    )
    if stated_point != release_point:
        raise ValueError("Table III member states a different release point")
    member_sha = digest(member)
    positions: Counter[str] = Counter()
    count = 0
    for fragment_position, fragment_bytes in enumerate(iter_act_fragments(member)):
        act = parse_act_fragment(fragment_bytes, max_bytes=MAX_FRAGMENT_BYTES)
        raw_act = asdict(act)
        raw_act.pop("records")
        for fragment_record_position, record in enumerate(act.records):
            position = positions[act.search_key]
            positions[act.search_key] += 1
            section_key, place = usc_section_place(record.usc_section)
            identity = json.dumps([member_sha, release_point, act.search_key, position], separators=(",", ":"))
            yield {
                "id": "table3-history:" + hashlib.sha256(identity.encode()).hexdigest(),
                "source_sha256": expected_sha256,
                "member_sha256": member_sha,
                "member_name": member_name,
                "release_point": release_point,
                "act_key": act.search_key,
                "act_native_id": act.act_id,
                "act_number": act.num,
                "congress": act.congress,
                "act_date": act.date,
                "statutes_at_large_volume": act.statutes_at_large_volume,
                "act_native_sequence": act.sequence,
                "act_insertion": act.insertion,
                "act_format": act.format,
                "act_print_in_supplement": act.print_in_supplement,
                "act_include_in_online_release_point": act.include_in_online_release_point,
                "act_public_law": act.public_law,
                "record_native_id": record.record_id,
                "record_native_sequence": record.sequence,
                "usc_key": record.usc_key,
                "act_section": record.act_section,
                "record_page": record.statutes_at_large_page,
                "usc_title": record.usc_title,
                "usc_section": record.usc_section,
                "status": record.usc_status,
                "record_print_in_supplement": record.print_in_supplement,
                "usc_section_key": section_key,
                "usc_place": place,
                "reader_version": READER_VERSION,
                "act_record_position": position,
                "fragment_position": fragment_position,
                "fragment_record_position": fragment_record_position,
                "raw_source_row": json.dumps({"act": raw_act, "record": asdict(record)}, sort_keys=True),
            }
            count += 1
    if count == 0:
        raise ValueError("Table III historical replay has no records")


def _read_bounded_archive(archive: Path, *, expected_sha256: str) -> bytes:
    """Read a regular, non-symlink archive file within byte bounds and verify its SHA-256."""
    if any(part.is_symlink() for part in (archive, *archive.parents)):
        raise ValueError("Table III archive must not use symlink paths")
    info = archive.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("Table III archive must be a regular file")
    with archive.open("rb") as stream:
        body = stream.read(MAX_ARCHIVE_BYTES + 1)
    if len(body) > MAX_ARCHIVE_BYTES:
        raise ValueError("Table III archive exceeds byte bound")
    if digest(body) != expected_sha256:
        raise ValueError("Table III archive digest mismatch")
    return body


def _verify_constant_columns(path: Path, expected: Mapping[str, str], *, label: str) -> int:
    """Verify every row of ``path`` holds the exact non-null values in ``expected``; return row count."""
    with pq.ParquetFile(path) as parquet:
        metadata = parquet.metadata
        total = metadata.num_rows
        if total <= 0:
            raise ValueError(f"{label} has no rows")
        names = parquet.schema_arrow.names
        indices = {col: names.index(col) for col in expected}
        for rg_index in range(metadata.num_row_groups):
            rg = metadata.row_group(rg_index)
            if rg.num_rows == 0:
                continue
            needs_scan: list[str] = []
            for col, value in expected.items():
                stats = rg.column(indices[col]).statistics
                if (
                    stats is not None
                    and stats.has_min_max
                    and stats.null_count == 0
                    and stats.min == value
                    and stats.max == value
                ):
                    continue
                needs_scan.append(col)
            if needs_scan:
                chunk = parquet.read_row_group(rg_index, columns=needs_scan)
                for col in needs_scan:
                    values = set(chunk[col].to_pylist())
                    if values != {expected[col]}:
                        raise ValueError(
                            f"{label} column {col!r} differs from expected {expected[col]!r}: {sorted(map(repr, values))[:3]}"
                        )
        return total


def replay(
    archive: Path,
    output: Path,
    *,
    expected_sha256: str,
    release_point: str,
    validation_backend: str = "reference",
) -> dict:
    """Write a fresh local candidate, or retain a refusal receipt without data output."""
    output.mkdir(parents=True, exist_ok=False)
    staged = output / ".dataset"
    receipt: dict = {
        "format": "spicy-table3-historical-replay",
        "version": 1,
        "status": "refused",
        "source_path": str(archive.resolve()),
        "source_sha256": expected_sha256,
        "release_point": release_point,
        "reader_version": READER_VERSION,
        "destination": "table3_historical_records",
        "identity_fields": list(IDENTITY_FIELDS),
        "scope": "All records in this exact retained member; no publisher completeness or publication claim",
    }
    generation = receipt_generation_id(expected_sha256=expected_sha256, release_point=release_point)
    receipt["generation"] = generation
    try:
        with archive.open("rb") as stream:
            body = stream.read(MAX_ARCHIVE_BYTES + 1)
        if len(body) > MAX_ARCHIVE_BYTES:
            raise ValueError("Table III archive exceeds byte bound")
        count = 0
        acts = set()
        metadata = None

        def records():
            nonlocal count, metadata
            for row in historical_records(body, expected_sha256=expected_sha256, release_point=release_point):
                metadata = {key: row[key] for key in ("member_name", "member_sha256")}
                count += 1
                acts.add(row["act_key"])
                position = {
                    key: row[key]
                    for key in (
                        *IDENTITY_FIELDS,
                        "member_name",
                        "fragment_position",
                        "fragment_record_position",
                        "record_native_id",
                    )
                }
                context = ReceiptContext(
                    generation,
                    row["id"],
                    READER_VERSION,
                    [
                        {
                            "source_id": expected_sha256,
                            "source_uri": expected_sha256,
                            "sha256": expected_sha256,
                            "locator": json.dumps(position, sort_keys=True),
                            "body_version": release_point,
                        },
                        {
                            "source_id": row["member_sha256"],
                            "sha256": row["member_sha256"],
                            "locator": row["member_name"],
                        },
                    ],
                )
                yield {**row, "source_position": position}, context

        candidate, etl_receipts = write_dataset(
            records(),
            staged,
            POLICY,
            batch_size=2048,
            validation_backend=validation_backend,
            generation_id=generation,
        )
        assert candidate is not None
        output_digest = hashlib.sha256()
        with candidate.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                output_digest.update(chunk)
        target = output / "table3_historical_records.parquet"
        candidate.rename(target)
        etl_receipts.rename(output / RECEIPT_KEY)
        staged.rmdir()
        receipt.update(status="candidate-verified", records=count, acts=len(acts), **(metadata or {}))
        receipt["output_sha256"] = "sha256:" + output_digest.hexdigest()
        receipt["schema"] = str(SCHEMA)
        receipt["policy"] = POLICY.descriptor()
    except Exception as error:
        import shutil

        shutil.rmtree(staged, ignore_errors=True)
        receipt.update(error_type=type(error).__name__, error=str(error))
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        raise
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def prepare_generation(
    archive: Path,
    output: Path,
    *,
    expected_sha256: str,
    release_point: str,
    candidate: Path | None = None,
    read_snapshot: Mapping | None = None,
    validation_backend: str = "reference",
) -> dict:
    """Package one verified Table III historical replay into an admitted ``table3-history`` generation.

    When ``candidate`` is provided, reuses its ``table3_historical_records.parquet`` and
    ``etl_receipts.parquet`` after verifying the archive digest, member digest, release
    point, schema, and constant provenance pins across every row group. Otherwise runs
    :func:`replay` first in a private staging directory. Never publishes to R2; pass the
    printed ``generation`` and ``evidence`` paths to ``publish-local-generation --family table3-history``.
    """
    from rulespec_artifacts import ArtifactInput

    from spicy_regs import generations
    from spicy_regs.native_types import described_schema
    from spicy_regs.source_evidence import CaptureEvidence, PRIOR_ROLE
    from spicy_regs.sources import publication

    if archive.is_symlink():
        raise ValueError("Table III archive must not be a symlink")
    archive = archive.resolve()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(f"Output directory already exists: {output}")
    if any(part.is_symlink() for part in (output, *output.parents)):
        raise ValueError("Output must not use symlink paths")
    if candidate is not None:
        candidate = candidate.resolve()
        if output.is_relative_to(candidate):
            raise ValueError("Output must be outside the candidate directory")

    parsed_snapshot = (
        publication.parse_index(json.dumps(read_snapshot).encode()) if read_snapshot is not None else None
    )

    body = _read_bounded_archive(archive, expected_sha256=expected_sha256)
    member_name, stated_point, member = read_table3_bulk_member(
        body, max_bytes=MAX_ARCHIVE_BYTES, max_member_bytes=MAX_MEMBER_BYTES
    )
    if stated_point != release_point:
        raise ValueError("Table III member states a different release point")
    member_sha = digest(member)
    del member, body

    gen_id = receipt_generation_id(expected_sha256=expected_sha256, release_point=release_point)

    output.mkdir(parents=True, exist_ok=False)
    with TemporaryDirectory(dir=output.parent, prefix=".table3-history-stage-") as scratch:
        if candidate is None:
            replay_dir = Path(scratch) / "replay"
            replay(
                archive,
                replay_dir,
                expected_sha256=expected_sha256,
                release_point=release_point,
                validation_backend=validation_backend,
            )
            subject_src = replay_dir / "table3_historical_records.parquet"
            receipt_src = replay_dir / RECEIPT_KEY
        else:
            subject_src = candidate / "table3_historical_records.parquet"
            receipt_src = candidate / RECEIPT_KEY
            for path in (subject_src, receipt_src):
                if path.is_symlink() or not path.is_file():
                    raise ValueError(f"Candidate is missing regular file {path.name}")

        if not pq.read_schema(subject_src).equals(SCHEMA):
            raise ValueError("Candidate table3_historical_records.parquet schema differs from POLICY")
        if not pq.read_schema(receipt_src).equals(RECEIPT_SCHEMA):
            raise ValueError("Candidate etl_receipts.parquet schema differs from RECEIPT_SCHEMA")

        subject_rows = _verify_constant_columns(
            subject_src,
            {
                "source_sha256": expected_sha256,
                "member_sha256": member_sha,
                "member_name": member_name,
                "release_point": release_point,
                "reader_version": READER_VERSION,
            },
            label="table3_historical_records.parquet",
        )
        receipt_rows = _verify_constant_columns(
            receipt_src,
            {
                "dataset": POLICY.dataset,
                "policy_version": READER_VERSION,
                "generation_id": gen_id,
                "outcome": "accepted",
            },
            label="etl_receipts.parquet",
        )
        if receipt_rows != subject_rows:
            raise ValueError(
                f"Candidate receipt rows ({receipt_rows}) differ from subject rows ({subject_rows})"
            )

        evidence = CaptureEvidence(output, FAMILY)
        if parsed_snapshot is not None:
            evidence.read_snapshot = parsed_snapshot
            prior = parsed_snapshot["families"].get(FAMILY)
            if prior is not None:
                evidence.prior_input = ArtifactInput(PRIOR_ROLE, prior["logicalId"], prior["artifactDigest"])
        retained = evidence.retain_file(
            archive,
            stage="table3:bulk",
            raw_source=True,
            original_location=expected_sha256,
            release_point=release_point,
            member_name=member_name,
            member_sha256=member_sha,
            reader_version=READER_VERSION,
        )
        if retained["sha256"] != expected_sha256:
            raise ValueError("Retained Table III archive differs from expected digest")
        evidence.event(
            "table3-historical-generation",
            release_point=release_point,
            source_sha256=expected_sha256,
            member_name=member_name,
            member_sha256=member_sha,
            reader_version=READER_VERSION,
            receipt_generation_id=gen_id,
            records=subject_rows,
            reused_candidate=candidate is not None,
        )

        artifact = generations.build_generation(
            output / "generation",
            family=FAMILY,
            files=[subject_src],
            expected_keys=["table3_historical_records.parquet"],
            schemas={"table3_historical_records": described_schema(SCHEMA)},
            receipt_path=receipt_src,
            receipt_policies=[POLICY],
            receipt_generation_id=gen_id,
            read_snapshot=parsed_snapshot,
            inputs=evidence.inputs(),
        )
        evidence.finish()

    prepared = {
        "status": "prepared-table3-history-generation",
        "family": FAMILY,
        "generation": str(output / "generation"),
        "artifactDigest": artifact.pin.artifact_digest,
        "logicalId": artifact.pin.logical_id,
        "evidence": str(evidence.artifact_dir),
        "release_point": release_point,
        "source_sha256": expected_sha256,
        "member_name": member_name,
        "member_sha256": member_sha,
        "reader_version": READER_VERSION,
        "receipt_generation_id": gen_id,
        "records": subject_rows,
        "reused_candidate": candidate is not None,
        "published": False,
    }
    (output / "prepared.json").write_text(json.dumps(prepared, indent=2, sort_keys=True) + "\n")
    return prepared


def main(argv: list[str] | None = None) -> None:
    args_in = sys.argv[1:] if argv is None else list(argv)
    parser = argparse.ArgumentParser(description=__doc__)
    if args_in[:1] == ["prepare"]:
        parser.add_argument("command", choices=["prepare"])
        parser.add_argument("--archive", type=Path, required=True)
        parser.add_argument("--sha256", required=True)
        parser.add_argument("--release-point", required=True)
        parser.add_argument("--output", type=Path, required=True)
        parser.add_argument("--candidate", type=Path, default=None)
        parser.add_argument("--read-index", type=Path, default=None)
        parser.add_argument("--validation-backend", choices=["reference", "bulk"], default="reference")
        args = parser.parse_args(args_in)
        snapshot = json.loads(args.read_index.read_bytes()) if args.read_index is not None else None
        print(
            json.dumps(
                prepare_generation(
                    args.archive,
                    args.output,
                    expected_sha256=args.sha256,
                    release_point=args.release_point,
                    candidate=args.candidate,
                    read_snapshot=snapshot,
                    validation_backend=args.validation_backend,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--release-point", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--validation-backend", choices=["reference", "bulk"], default="reference")
    args = parser.parse_args(args_in)
    print(
        json.dumps(
            replay(
                args.archive,
                args.output,
                expected_sha256=args.sha256,
                release_point=args.release_point,
                validation_backend=args.validation_backend,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
