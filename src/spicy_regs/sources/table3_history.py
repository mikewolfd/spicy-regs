"""Replay OLRC Table III into a separate historical, source-versioned destination.

The original compressed archive and decoded member remain bounded in memory
through the maintained SpicyDocs reader. Record output is written in batches;
no network acquisition or public-law/Congress filter runs here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path

import pyarrow as pa
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, write_dataset
from spicy_docs.schemas.law_tables import usc_section_place
from spicy_docs.sources.uscode import iter_act_fragments, read_table3_bulk_member
from spicy_docs.sources.uscode.table3 import parse_act_fragment

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
IDENTITY_FIELDS = ("member_sha256", "release_point", "act_key", "act_record_position")
POLICY = DatasetPolicy(
    "table3_historical_records", SCHEMA, IDENTITY_FIELDS, ("source_position",), policy_version=READER_VERSION
)


def digest(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


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


def replay(archive: Path, output: Path, *, expected_sha256: str, release_point: str) -> dict:
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
    generation = digest(json.dumps([READER_VERSION, expected_sha256, release_point], separators=(",", ":")).encode())
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

        candidate, etl_receipts = write_dataset(records(), staged, POLICY, batch_size=2048)
        assert candidate is not None
        output_digest = hashlib.sha256()
        with candidate.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                output_digest.update(chunk)
        target = output / "table3_historical_records.parquet"
        candidate.rename(target)
        etl_receipts.rename(output / "etl_receipts.parquet")
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--release-point", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            replay(args.archive, args.output, expected_sha256=args.sha256, release_point=args.release_point), indent=2
        )
    )


if __name__ == "__main__":
    main()
