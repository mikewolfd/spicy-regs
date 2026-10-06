"""Generation-bound subject/receipt writer and internal reads for FEC identity.

Every field decision lives in the family registry. Receipt encoding, row-version
binding and admission use the shared ETL implementation.
"""

from __future__ import annotations

from contextlib import ExitStack
from functools import cache
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import (
    DatasetPolicy,
    ReceiptContext,
    RECEIPT_SCHEMA,
    split_record,
    failure_receipt,
    validate_receipt_bundle,
    select_receipts,
    read_with_receipts,
)
from .fec_identity_context_fields import REGISTRY, VERSION, normalize_record, subject_schema


@cache
def dataset_policy(table):
    rules = REGISTRY[table]
    return DatasetPolicy(
        table,
        subject_schema(table),
        tuple(rules["identity_fields"]),
        tuple(rules["receipt_fields"]),
        policy_version=VERSION,
        receipt_only=rules["receipt_only"],
    )


def witnesses_for(row, evidence, input_witness):
    """Preserve all ordered witnesses, including duplicate and split RSS parts."""
    witnesses = [dict(input_witness)]
    if row.get("source_sha256"):
        witnesses.append(
            dict(
                source_id=row.get("source_record_id") or row.get("record_id") or row.get("collection_id"),
                source_uri=row.get("source_url"),
                sha256=row["source_sha256"],
                locator=row.get("source_locator_json") or row.get("source_context_pointer"),
                body_version=None,
            )
        )
    for link in evidence:
        witnesses.append(
            dict(
                source_id=link.get("source_record_id") or link.get("collection_id"),
                source_uri=None,
                sha256=link.get("witness_sha256"),
                locator=json.dumps(link, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                body_version=link.get("witness_generation_pin"),
            )
        )
    return witnesses


class IdentityReceiptWriter:
    """Bounded family build; failed admission never replaces an existing output."""

    def __init__(self, output_dir, *, generation_id, tables, batch_size=512):
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        self.output_dir = Path(output_dir)
        self.generation_id = generation_id
        self.policies = {name: dataset_policy(name) for name in tables}
        if not self.policies:
            raise ValueError("Explicit nonempty table selection required")
        self.batch_size = batch_size
        self.counts = {name: 0 for name in self.policies}
        self.attempts = 0
        self.table_attempts = {name: 0 for name in self.policies}
        self._stack = ExitStack()

    def __enter__(self):
        if self.output_dir.exists():
            raise FileExistsError(self.output_dir)
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._stack.enter_context(TemporaryDirectory(prefix=".fec-identity-", dir=self.output_dir.parent))
        self.stage = Path(temporary) / "tables"
        self.stage.mkdir()
        self.subject_writers = {
            name: self._stack.enter_context(
                pq.ParquetWriter(self.stage / (name + ".parquet"), policy.subject_schema, compression="zstd")
            )
            for name, policy in self.policies.items()
            if not policy.receipt_only
        }
        self.receipt_writer = self._stack.enter_context(
            pq.ParquetWriter(self.stage / "etl_receipts.parquet", RECEIPT_SCHEMA, compression="zstd")
        )
        self.buffers = {name: [] for name in self.subject_writers}
        self.receipts = []
        self.buffer_bytes = 0
        return self

    def emit(
        self, table, row, *, input_witness, evidence=(), source_input=None, outcome=None, diagnostic=None, lineage=None
    ):
        if table not in self.policies:
            raise ValueError(f"Output outside explicitly selected identity tables: {table}")
        policy = self.policies[table]
        ordinal = self.table_attempts[table]
        self.table_attempts[table] += 1
        mapped = normalize_record(table, row, source_input=source_input, observation_ordinal=ordinal)
        if table == "fec_committees":
            mapped["conversion_inputs"] = dict(row)
        self.attempts += 1
        diagnostics = dict(mapped["conversion_diagnostics"])
        if diagnostic is not None:
            diagnostics["mapping"] = diagnostic
        context = ReceiptContext(
            self.generation_id,
            f"{table}/{self.attempts}",
            VERSION,
            witnesses_for(mapped, evidence, input_witness),
            diagnostics,
        )
        if lineage is not None and not policy.receipt_only:
            context = lineage.inherit(context, policy, mapped)
        if table == "fec_research_source_pages" and row.get("content_status") != "body_extracted":
            outcome = "refused"
            context = replace(context, diagnostics={**diagnostics, "content_status": row.get("content_status")})
        if outcome is not None:
            subject = None
            receipt = failure_receipt(policy, context, outcome=outcome, raw_fields=mapped)
        else:
            subject, receipt = split_record(policy, mapped, context)
        if subject is not None:
            self.buffers[table].append(subject)
            self.counts[table] += 1
        self.receipts.append(receipt)
        self.buffer_bytes += len(receipt["processing_json"]) + len(receipt["diagnostic_json"])
        if len(self.receipts) >= self.batch_size or self.buffer_bytes >= 8 * 1024 * 1024:
            self.flush()
        return subject, receipt

    def flush(self):
        for name, rows in self.buffers.items():
            if rows:
                self.subject_writers[name].write_table(
                    pa.Table.from_pylist(rows, schema=self.policies[name].subject_schema)
                )
                rows.clear()
        if self.receipts:
            self.receipt_writer.write_table(pa.Table.from_pylist(self.receipts, schema=RECEIPT_SCHEMA))
            self.receipts.clear()
        self.buffer_bytes = 0

    def __exit__(self, kind, error, traceback):
        try:
            if kind is None:
                self.flush()
                for writer in self.subject_writers.values():
                    writer.close()
                self.receipt_writer.close()
                subjects = {
                    name: [] if policy.receipt_only else [self.stage / (name + ".parquet")]
                    for name, policy in self.policies.items()
                }
                validate_receipt_bundle(
                    subjects,
                    [self.stage / "etl_receipts.parquet"],
                    list(self.policies.values()),
                    generation_id=self.generation_id,
                )
                (self.stage / "identity-context.json").write_text(
                    json.dumps(
                        {
                            "generation_id": self.generation_id,
                            "policies": [p.descriptor() for p in self.policies.values()],
                            "subject_rows": self.counts,
                            "receipt_rows": self.attempts,
                        },
                        indent=2,
                    )
                    + "\n"
                )
                from rulespec_artifacts import publish_directory_no_replace

                publish_directory_no_replace(self.stage, self.output_dir)
        finally:
            self._stack.close()
        return False


def read_identity_rows(directory, table, *, generation_id):
    """Internal callers recover qualification fields only after exact receipt admission."""
    directory = Path(directory)
    policy = dataset_policy(table)
    if policy.receipt_only:
        raise ValueError("Processing-only datasets require read_identity_processing")
    with TemporaryDirectory(prefix="fec-identity-read-") as temporary:
        receipts = select_receipts(
            directory / "etl_receipts.parquet", Path(temporary) / "receipts.parquet", dataset=table
        )
        yield from read_with_receipts(
            [directory / (table + ".parquet")], [receipts], policy, generation_id=generation_id
        )


def read_identity_processing(directory, table, *, generation_id):
    """Read receipt-only source inputs for internal mapping, never a public view."""
    from spicy_regs.etl_receipts import _unpack

    policy = dataset_policy(table)
    if not policy.receipt_only:
        raise ValueError("Subject datasets require read_identity_rows")
    with TemporaryDirectory(prefix="fec-processing-read-") as temporary:
        path = select_receipts(
            Path(directory) / "etl_receipts.parquet", Path(temporary) / "receipts.parquet", dataset=table
        )
        validate_receipt_bundle({table: []}, [path], [policy], generation_id=generation_id)
        with pq.ParquetFile(path) as source:
            for batch in source.iter_batches(batch_size=512):
                for receipt in batch.to_pylist():
                    if receipt["outcome"] == "observed":
                        yield _unpack(json.loads(receipt["processing_json"]))


def seal_identity_context(directory, destination):
    """Seal the exact local subject/receipt set as a verified partial generation."""
    from spicy_regs.generations import build_generation

    directory = Path(directory)
    metadata = json.loads((directory / "identity-context.json").read_text())
    policies = [DatasetPolicy.from_descriptor(p) for p in metadata["policies"]]
    files = [directory / (p.dataset + ".parquet") for p in policies if not p.receipt_only]
    return build_generation(
        Path(destination),
        family="fec-identity-context",
        files=files,
        expected_keys=tuple(p.name for p in files),
        publication_status="local-partial",
        receipt_path=directory / "etl_receipts.parquet",
        receipt_policies=policies,
        receipt_generation_id=metadata["generation_id"],
    )
