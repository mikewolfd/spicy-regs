"""Build House floor feed history from explicitly selected retained originals.

This command is offline. It neither retrieves publisher URLs nor follows entry
links. Each capture and revision survives in a local candidate generation; the
pipeline does not select a latest week, infer legislative action, or publish.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_docs.schemas.house_floor_tables import HOUSE_FLOOR_TABLES, shape_house_floor_feed
from spicy_docs.sources.congress.house_floor import FEED_URL, MAX_BYTES, parse_house_floor_feed
from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.parquet_rows import write_rows
from spicy_regs.pipelines.raw_releases import inherit_read_index, open_raw
from spicy_regs.source_evidence import CaptureEvidence

FAMILY = "house-floor-feed"
SCHEMAS = {
    name: pa.schema([(column, pa.string()) for column in table.columns]) for name, table in HOUSE_FLOOR_TABLES.items()
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _captures(selection):
    if selection.get("format") != "house-floor-feed-captures" or selection.get("version") != 1:
        raise ValueError("Unsupported House floor capture selection")
    captures = selection.get("captures")
    if not isinstance(captures, list) or not captures:
        raise ValueError("Select at least one complete retained capture, including an explicitly empty feed")
    seen = set()
    for capture in captures:
        if capture.get("source_uri") not in {FEED_URL, FEED_URL.replace("https:", "http:")}:
            raise ValueError("Selected source URI is not the House floor feed")
        if type(capture.get("bytes")) is not int or not 0 < capture["bytes"] <= MAX_BYTES:
            raise ValueError("Selected original requires a bounded byte size")
        stamp = datetime.fromisoformat(capture["observed_at"])
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError("Capture timestamp must include its time zone")
        digest = capture.get("sha256", "")
        if (
            len(digest) != 71
            or not digest.startswith("sha256:")
            or any(c not in "0123456789abcdef" for c in digest[7:])
        ):
            raise ValueError("Selected original requires a SHA-256 digest")
        identity = (capture["source_uri"], capture["observed_at"], digest)
        if identity in seen:
            raise ValueError("Selection repeats an identical feed observation")
        seen.add(identity)
    return captures


def _original(capture, *, archive_root):
    # Read only the selected local original or the existing central raw store.
    stream = (
        Path(capture["local_path"]).open("rb")
        if capture.get("local_path")
        else open_raw(archive_root).open(capture["sha256"])
    )
    with stream as opened:
        body = opened.read(capture["bytes"] + 1)
    if len(body) != capture["bytes"] or "sha256:" + hashlib.sha256(body).hexdigest() != capture["sha256"]:
        raise ValueError("House floor original differs from its selected size or SHA-256")
    return body


def candidate(selection, directory, evidence, *, archive_root=None):
    captures = _captures(selection)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    counts = dict.fromkeys(SCHEMAS, 0)
    # One original and its rows at a time. Retain capture-sized intermediate
    # tables so histories do not accumulate XML trees or HTML in memory.
    for index, capture in enumerate(captures):
        body = _original(capture, archive_root=archive_root)
        evidence.retain_bytes(
            body,
            stage="retained-house-floor-feed",
            raw_source=True,
            original_location=capture["source_uri"],
            observed_at=capture["observed_at"],
        )
        tables = shape_house_floor_feed(
            parse_house_floor_feed(body), source_uri=capture["source_uri"], observed_at=capture["observed_at"]
        )
        part = directory / "captures" / f"{index:06d}"
        part.mkdir(parents=True)
        for name, schema in SCHEMAS.items():
            write_rows(tables[name], part / (name + ".parquet"), schema)
            counts[name] += len(tables[name])
        del body, tables

    def records(name):
        for index in range(len(captures)):
            parquet = pq.ParquetFile(directory / "captures" / f"{index:06d}" / (name + ".parquet"))
            for batch in parquet.iter_batches(batch_size=2000):
                yield from batch.to_pylist()

    for name, schema in SCHEMAS.items():
        write_rows(records(name), directory / (name + ".parquet"), schema)
    return {
        "rows": counts,
        "selected_captures": len(captures),
        "scope": selection.get("scope"),
        "publication_status": "local-partial",
        "publisher_fetches": 0,
    }


def build(selection, output_dir, *, archive_root=None, read_index=None):
    output_dir = Path(output_dir)
    if (output_dir / "candidate").exists() or (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each House floor generation")
    _captures(selection)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        snapshot = inherit_read_index(evidence, read_index)
        control = output_dir / "selection.json"
        control.write_text(_json(selection) + "\n")
        evidence.retain_control(control, stage="house-floor-selection")
        summary = candidate(selection, output_dir / "candidate", evidence, archive_root=archive_root)
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=[output_dir / "candidate" / (name + ".parquet") for name in SCHEMAS],
            expected_keys=[name + ".parquet" for name in SCHEMAS],
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=snapshot,
            publication_status="local-partial",
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    (output_dir / "summary.json").write_text(_json(summary) + "\n")
    return artifact, evidence.artifact_dir, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path)
    parser.add_argument("--read-index", type=Path)
    args = parser.parse_args()
    artifact, evidence, summary = build(
        json.loads(args.selection.read_text()),
        args.output_dir,
        archive_root=args.archive_root,
        read_index=args.read_index,
    )
    print(_json({"artifact_digest": artifact.pin.artifact_digest, "evidence": str(evidence), **summary}))


if __name__ == "__main__":
    main()
