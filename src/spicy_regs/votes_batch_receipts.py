"""Opt-in vote batching with the current Congress policy and exact row decisions."""
from __future__ import annotations
import json
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from collections.abc import Mapping, Sequence
import pyarrow as pa
import pyarrow.parquet as pq
from spicy_regs import congress_bulk, etl_bulk
from spicy_regs.congress_receipts import PROCESSOR, CongressInput, _digest, _rows, policy
from spicy_regs.congress_subjects import map_record
from spicy_regs.etl_receipts import (RECEIPT_SCHEMA, ReceiptContext, _unpack,
    failure_receipt, observation_receipt, read_attempts, read_with_receipts,
    select_receipts, split_record, validate_receipt_bundle)
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.current_receipt_history import inherit_current_receipts

def _admit(subjects, receipts, policies, *, generation_id, bulk=True):
    """Use bulk proof where eligible; the exact row validator decides every fallback."""
    if bulk:
        try:
            return etl_bulk.validate_bundle(subjects, receipts, policies, generation_id=generation_id)
        except etl_bulk.NotBulkEligible:
            pass
    return validate_receipt_bundle(subjects, receipts, policies, generation_id=generation_id, bulk=False)

def congress_attempt(dataset: str, raw: Mapping, context: ReceiptContext) -> tuple[dict | None, dict]:
    """The reference attempt for one raw Congress row, shared with qualification."""
    selected = policy(dataset)
    try:
        mapped = map_record(dataset, raw)
        processing = {"source_fields": mapped.source_fields, "entry_kind": "row"}
        if mapped.subject is None and not selected.receipt_only:
            return None, failure_receipt(selected, replace(context, diagnostics={"reason": "no_domain_subject"}),
                                         outcome="rejected", raw_fields=processing)
        return split_record(selected, (mapped.subject or {}) | processing, context)
    except (ValueError, TypeError, OverflowError, pa.ArrowException) as error:
        return None, failure_receipt(selected, replace(context, diagnostics={"reason": "conversion_refused",
                                     "error_type": type(error).__name__}), outcome="refused",
                                     raw_fields={"source_fields": raw, "entry_kind": "row"})

def write_congress_dataset(
    source: Path,
    directory: Path,
    *,
    dataset: str,
    generation_id: str,
    witnesses: Sequence[Mapping[str, Any]] = (),
    prior: CongressInput | None = None,
    bulk: bool = True,
) -> tuple[Path | None, Path]:
    """Retain one complete shaped output, including rejected rows and its exact footer.

    The source file remains in place. Its digest and row ordinal identify the
    conversion input; callers can additionally retain ordered native capture
    witnesses. The first witness never asserts that a shaped file is an original
    publisher response.

    A bulk-eligible dataset is written in one scan. Selected prior evidence is
    inherited through the maintained current receipt function, including its
    original witnesses and retained processing payloads. ``bulk=False`` delegates
    to the current public row writer as the independent reference.
    """
    from rulespec_artifacts import publish_directory_no_replace

    if not bulk:
        from spicy_regs.congress_receipts import write_congress_dataset as row_writer
        return row_writer(source, directory, dataset=dataset, generation_id=generation_id,
                          witnesses=witnesses, prior=prior, bulk=False)
    if directory.exists():
        raise FileExistsError(directory)
    selected = policy(dataset)
    digest = _digest(source)
    directory.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".congress-", dir=directory.parent) as temporary:
        stage = Path(temporary) / "bundle"
        stage.mkdir()
        subjects = None if selected.receipt_only else stage / (dataset + ".parquet")
        receipts = stage / "etl_receipts.parquet"
        base_witness = {
            "source_id": "shaped-observation:" + dataset,
            "source_uri": str(source.resolve()),
            "sha256": digest,
            "body_version": None,
        }
        footer = dict(pq.read_schema(source).metadata or {})
        in_bulk = bulk and subjects is not None and congress_bulk.eligible(dataset, pq.read_schema(source))
        prior_paths = []
        if prior is not None:
            scoped = select_receipts(prior.receipts, Path(temporary) / "prior.parquet", dataset=dataset)
            _admit({dataset: prior.subjects}, [scoped], [selected], generation_id=prior.generation_id, bulk=bulk)
            prior_paths.append(scoped)

        def context(ordinal, diagnostics=None, subject=None, processing=None):
            current = ReceiptContext(
                generation_id,
                f"{dataset}:{digest}:{ordinal}",
                PROCESSOR,
                [{**base_witness, "locator": str(ordinal)}, *witnesses],
                diagnostics or {},
            )
            return current

        def attempt(ordinal, raw):
            return congress_attempt(dataset, raw, context(ordinal))

        metadata = {
            "source_metadata": list(footer.items()),
            "source_schema": pq.read_schema(source).serialize().to_pybytes(),
            "entry_kind": "table_metadata",
        }
        first = observation_receipt(selected, context("metadata", processing=metadata), processing_fields=metadata)
        # One scan for a flat dataset; the row loop below is its reference.
        if in_bulk:
            assert subjects is not None
            try:
                congress_bulk.write_bundle(
                    source, subjects, receipts, dataset=dataset, policy=selected, generation_id=generation_id,
                    processor=PROCESSOR, digest=digest, first_receipts=[first], witnesses=witnesses,
                    row_receipt=lambda text, ordinal: attempt(ordinal, _unpack(json.loads(text))["source_fields"]),
                )
            except (etl_bulk.duckdb.Error, pa.ArrowInvalid):
                # The same staged row reader establishes the exact first source
                # decoding error. Count/order guard failures still abort directly.
                in_bulk = False
        if not in_bulk:
            with pq.ParquetWriter(receipts, RECEIPT_SCHEMA, compression="zstd") as rw:
                sw = None if subjects is None else pq.ParquetWriter(subjects, selected.subject_schema, compression="zstd")
                try:
                    receipt_batch, subject_batch = [first], []
                    for ordinal, raw in enumerate(_rows(source)):
                        subject, receipt = attempt(ordinal, raw)
                        if subject is not None:
                            subject_batch.append(subject)
                        receipt_batch.append(receipt)
                        if len(receipt_batch) >= 2000:
                            if subject_batch:
                                assert sw is not None
                                sw.write_table(pa.Table.from_pylist(subject_batch, schema=selected.subject_schema))
                                subject_batch.clear()
                            rw.write_table(pa.Table.from_pylist(receipt_batch, schema=RECEIPT_SCHEMA))
                            receipt_batch.clear()
                    if subject_batch:
                        assert sw is not None
                        sw.write_table(pa.Table.from_pylist(subject_batch, schema=selected.subject_schema))
                    if receipt_batch:
                        rw.write_table(pa.Table.from_pylist(receipt_batch, schema=RECEIPT_SCHEMA))
                finally:
                    if sw is not None:
                        sw.close()
        if prior_paths:
            try:
                carried = inherit_current_receipts(receipts, prior_paths, stage / "inherited-receipts.parquet")
            except (ValueError, TypeError, OverflowError, pa.ArrowException):
                # Inheritance can refuse a single accepted subject. The current
                # row writer determines those precise refusals and subject omissions.
                # No staged batch output has been published before this fallback.
                from spicy_regs.congress_receipts import write_congress_dataset as row_writer
                return row_writer(source, directory, dataset=dataset, generation_id=generation_id,
                                  witnesses=witnesses, prior=prior, bulk=False)
            carried.replace(receipts)
        _admit(
            {dataset: [] if subjects is None else [subjects]}, [receipts], [selected], generation_id=generation_id, bulk=bulk
        )
        publish_directory_no_replace(stage, directory)
    return (None if subjects is None else directory / subjects.name, directory / receipts.name)

def restore_processing_input(
    subject: Path | tuple[Path, ...] | None,
    receipts: Path,
    destination: Path,
    *,
    dataset: str,
    generation_id: str,
    bulk: bool = True,
) -> Path:
    """Restore retained original source fields and schema after exact receipt admission.

    Rejected conversion attempts remain available for retry. Technical tables
    and processing events remain receipt-only in the public generation. Footer
    metadata includes bill-family completion scopes and per-reader refusal state.

    A dataset ``congress_bulk.eligible`` accepts is decoded in one scan after the
    one admission. ``bulk=False`` is the row readers for any dataset: the
    reference a bulk restore is checked against.
    """
    selected = policy(dataset)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=".congress-read-", dir=destination.parent) as temp:
        scoped = select_receipts(receipts, Path(temp) / "receipts.parquet", dataset=dataset)
        paths = () if subject is None else ((subject,) if isinstance(subject, Path) else tuple(subject))
        _admit({dataset: paths}, [scoped], [selected], generation_id=generation_id, bulk=bulk)

        def source_fields(data, diagnostic):
            kept = data.get("entry_kind") == "row" and diagnostic.get("reason") != "conversion_refused"
            return data["source_fields"] if kept else None

        def retained_schema(metadata_rows):
            if not metadata_rows or any(row != metadata_rows[0] for row in metadata_rows[1:]):
                raise ValueError("Congress input requires consistent retained source footers")
            return pa.ipc.read_schema(pa.BufferReader(metadata_rows[0]["source_schema"]))

        if bulk and congress_bulk.eligible(dataset):
            # The bundle is admitted above. One scan finds its footers and one decodes its rows; each reader
            # below admits the whole bundle again before it yields anything.
            footers = (_unpack(json.loads(text)) for text in congress_bulk.footer_candidates(scoped, dataset=dataset))
            schema = retained_schema([data for data in footers if data.get("entry_kind") == "table_metadata"])
            if congress_bulk.eligible(dataset, schema):
                congress_bulk.restore_input(
                    scoped, destination, dataset=dataset, schema=schema,
                    row_fields=lambda processing, diagnostic: source_fields(
                        _unpack(json.loads(processing)), _unpack(json.loads(diagnostic))
                    ),
                )
                return destination
        # Exercise the shared exact matching reader before reconstructing original source rows.
        for _ in read_with_receipts(paths, [scoped], selected, generation_id=generation_id):
            pass
        schema = retained_schema([
            receipt["processing_fields"]
            for receipt in read_attempts([scoped], selected, generation_id=generation_id)
            if receipt["processing_fields"].get("entry_kind") == "table_metadata"
        ])

        # Source shapers use strings. Preserve discovered source columns and
        # absent columns by reading the exact retained input dictionaries.
        def original_rows():
            for receipt in read_attempts([scoped], selected, generation_id=generation_id):
                fields = source_fields(receipt["processing_fields"], receipt["diagnostics"])
                if fields is not None:
                    yield fields

        return write_rows(original_rows(), destination, schema)
