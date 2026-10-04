"""Build retained FEC identity/context subjects and shared ETL receipts locally.

The explicit manifest selects byte-pinned mapped tables or native source rows.
The latter calls maintained mappers. No network, current-record selection or
publication occurs. Input files are rehashed after complete bounded reads.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq

from .fec_identity_context_fields import REGISTRY
from .fec_identity_dispatch import prepare_identity_job, map_identity_record, map_context_rows
from .fec_identity_receipts import IdentityReceiptWriter


def _sha(path):
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _input_rows(item, base):
    path = Path(item["path"])
    if not path.is_absolute():
        path = base / path
    if path.is_symlink() or not path.is_file() or _sha(path) != item["sha256"]:
        raise ValueError("FEC input is not the selected regular-file bytes")
    count = 0
    with pq.ParquetFile(path) as source:
        if source.metadata.num_rows != item["rows"]:
            raise ValueError("FEC input row membership differs from selection")
        for batch in source.iter_batches(batch_size=256, use_threads=False):
            for row in batch.to_pylist():
                count += 1
                yield row
    if count != item["rows"] or _sha(path) != item["sha256"]:
        raise ValueError("FEC input changed during mapping")


def _emit_mapping(writer, tables, evidence, input_witness, source_input):
    for table, rows in tables.items():
        if not rows:
            continue
        if table not in REGISTRY:
            raise ValueError(f"Context emitted another owned family: {table}")
        for row in rows:
            links = [
                e for e in evidence if e["target_table"] == table and e["target_record_id"] == row.get("record_id")
            ]
            writer.emit(table, row, input_witness=input_witness, evidence=links, source_input=source_input)
    if "fec_record_evidence" in writer.policies:
        for row in evidence:
            writer.emit("fec_record_evidence", row, input_witness=input_witness)


def build_fec_identity_context(manifest, output_dir):
    """Consume a local version-1 selection, admitting every subject with receipts.

    A mapped input names its exact table. A source input supplies explicit jobs
    (kind plus native entry and optional retained header_row). Context inputs
    select collection IDs and explicitly request single or multipart RSS mapping.
    Unsupported native rows become refusal receipts; corrupt inputs abort the
    directory. No schema or namespace guessing is permitted.
    """
    manifest = Path(manifest)
    original = manifest.read_bytes()
    spec = json.loads(original)
    if set(spec) != {"version", "generation_id", "tables", "inputs"} or spec["version"] != 1:
        raise ValueError("Unsupported FEC identity build manifest")
    if not isinstance(spec["inputs"], list) or not spec["inputs"]:
        raise ValueError("FEC identity build needs explicit input membership")
    if not isinstance(spec["tables"], list) or len(set(spec["tables"])) != len(spec["tables"]):
        raise ValueError("FEC identity output selection repeats a table")
    with IdentityReceiptWriter(output_dir, generation_id=spec["generation_id"], tables=spec["tables"]) as writer:
        for item in spec["inputs"]:
            mode = item["mode"]
            witness = dict(
                source_id=item["table"],
                source_uri=str(item["path"]),
                sha256=item["sha256"],
                locator=None,
                body_version=None,
            )
            rows = _input_rows(item, manifest.parent)
            if mode == "mapped":
                for row in rows:
                    writer.emit(item["table"], row, input_witness=witness)
            elif mode == "source_records":
                if item["table"] != "fec_source_records":
                    raise ValueError("Native jobs require source records")
                jobs = {}
                for job in item["jobs"]:
                    prepared = prepare_identity_job(
                        job["kind"],
                        job["entry"],
                        source_generation_pin=item["source_generation_pin"],
                        header_row=job.get("header_row"),
                    )
                    if prepared.collection_id in jobs:
                        raise ValueError("Duplicate collection mapping selection")
                    jobs[prepared.collection_id] = prepared
                selected = set()
                for row in rows:
                    if row["collection_id"] not in jobs:
                        continue
                    selected.add(row["collection_id"])
                    try:
                        tables, evidence = map_identity_record(row, jobs[row["collection_id"]])
                    except (ValueError, KeyError, TypeError) as error:
                        writer.emit(
                            "fec_source_records",
                            row,
                            input_witness=witness,
                            outcome="refused",
                            diagnostic={"error": str(error), "kind": type(error).__name__},
                        )
                        continue
                    writer.emit("fec_source_records", row, input_witness=witness)
                    _emit_mapping(writer, tables, evidence, witness, None)
                if selected != set(jobs):
                    raise ValueError("Selected collection has no input observations")
            elif mode == "context":
                if item["table"] != "fec_collections":
                    raise ValueError("Context mapping requires collections")
                selected = set(item["collection_ids"])
                seen = set()
                feed_parts = []
                for row in rows:
                    if row["collection_id"] not in selected:
                        continue
                    if row["collection_id"] in seen:
                        raise ValueError("Repeated context collection identity")
                    seen.add(row["collection_id"])
                    writer.emit("fec_collections", row, input_witness=witness)
                    if item.get("filing_feed", False):
                        feed_parts.append(row)
                        continue
                    try:
                        mappings = list(map_context_rows([row], source_generation_pin=item["source_generation_pin"]))
                    except (ValueError, KeyError, TypeError) as error:
                        writer.emit(
                            "fec_collections",
                            row,
                            input_witness=witness,
                            outcome="refused",
                            diagnostic={"error": str(error), "kind": type(error).__name__},
                        )
                        continue
                    for mapped in mappings:
                        _emit_mapping(writer, mapped.tables, mapped.evidence, witness, None)
                if seen != selected:
                    raise ValueError("Selected context membership is incomplete")
                if item.get("filing_feed", False):
                    try:
                        mappings = list(
                            map_context_rows(
                                feed_parts, source_generation_pin=item["source_generation_pin"], filing_feed=True
                            )
                        )
                    except (ValueError, KeyError, TypeError) as error:
                        for row in feed_parts:
                            writer.emit(
                                "fec_collections",
                                row,
                                input_witness=witness,
                                outcome="refused",
                                diagnostic={"error": str(error), "kind": type(error).__name__},
                            )
                        continue
                    for mapped in mappings:
                        _emit_mapping(writer, mapped.tables, mapped.evidence, witness, None)
            else:
                raise ValueError(f"Unsupported identity build mode: {mode}")
        if manifest.read_bytes() != original:
            raise ValueError("FEC manifest changed during build")
    return tuple(
        Path(output_dir) / name
        for name in [
            *(t + ".parquet" for t in spec["tables"] if not REGISTRY[t]["receipt_only"]),
            "etl_receipts.parquet",
        ]
    )
