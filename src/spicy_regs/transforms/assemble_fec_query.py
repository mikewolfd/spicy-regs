"""Combine explicitly selected, verified FEC tables without reinterpreting values.

This step only fills absent fields with NULL and combines compatible schemas.
The existing generation publisher owns sealing and publication. The caller owns
the complete input selection, resource budget and final evidence/identity checks.
"""

from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq

from .fec_query import _digest
from .fec_typed_batch import typed_batch


@dataclass(frozen=True)
class TypedInput:
    path: Path
    sha256: str
    rows: int
    source_namespace: str | None = None


@dataclass(frozen=True)
class FilingAssociationInputs:
    """Explicit selected dependencies for the maintained association rules.

    These pins describe source and filing publications, not current-record or
    amendment selection. Namespace digests must be admitted definition evidence
    by the caller's existing release checks, as for the original mapper.
    """
    source_generation_pin: str
    filing_generation_pin: str
    filings: Sequence[TypedInput]
    namespace_evidence: dict[str, str]
    header_associations: Sequence[TypedInput] = ()

    def prepare(self, table, schema, check_resources):
        from .fec_filing_associations import association_records
        from .fec_native_subjects import FIELD_RULES

        _digest(self.source_generation_pin)
        _digest(self.filing_generation_pin)
        namespace_evidence = dict(self.namespace_evidence)
        for evidence in namespace_evidence.values():
            _digest(evidence)
        if (table not in FIELD_RULES or "filing_key" not in FIELD_RULES[table]["keep"]
                or "filing_key" not in schema.names or not self.filings):
            raise ValueError("Filing association requires an applicable source schema and selected filing inputs")
        if schema.field("filing_key").type != pa.string():
            raise ValueError("Filing association requires the declared string filing key type")

        def read_selected(items):
            rows, proofs = [], []
            paths = [item.path.resolve() for item in items]
            if len(paths) != len(set(paths)):
                raise ValueError("The same filing dependency member was selected twice")
            for item in items:
                check_resources()
                _digest(item.sha256)
                if type(item.rows) is not int or item.rows < 0:
                    raise ValueError("Filing dependency requires explicit integer row membership")
                before = item.path.stat()
                if item.path.is_symlink() or not item.path.is_file() or _sha(item.path) != item.sha256:
                    raise ValueError("Filing dependency differs from its selected bytes")
                source = pq.ParquetFile(item.path)
                if source.metadata.num_rows != item.rows or item.rows > 100_000 or item.path.stat().st_size > 64 * 1024**2:
                    raise ValueError("Filing dependency membership or bounded metadata size refused")
                if len(rows) + item.rows > 100_000:
                    raise ValueError("Combined filing metadata exceeds the bounded selection")
                rows.extend(source.read(use_threads=False).to_pylist())
                stamp = dict(inode=before.st_ino, bytes=before.st_size, mtime_ns=before.st_mtime_ns)
                after = item.path.stat()
                if (_sha(item.path) != item.sha256
                        or stamp != dict(inode=after.st_ino, bytes=after.st_size, mtime_ns=after.st_mtime_ns)
                        or len(rows) != sum(p["rows"] for p in proofs) + item.rows):
                    raise ValueError("Filing dependency changed during complete read")
                proofs.append(dict(path=str(item.path), sha256=item.sha256, rows=item.rows, stat=stamp))
            return rows, proofs

        filings, filing_proofs = read_selected(self.filings)
        headers, header_proofs = read_selected(self.header_associations)
        return (
            lambda rows: association_records(
                rows, table=table, schema=schema, filings=filings, headers=headers,
                source_generation_pin=self.source_generation_pin,
                target_generation_pin=self.filing_generation_pin,
                namespace_evidence=namespace_evidence, check_resources=check_resources,
            ),
            dict(source_generation_pin=self.source_generation_pin, filing_generation_pin=self.filing_generation_pin,
                 filings=filing_proofs, headers=header_proofs, namespace_evidence=namespace_evidence),
        )


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _align(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Refuse field loss/type promotion, including nested source fields."""
    if not pa.unify_schemas([schema, batch.schema]).equals(schema, check_metadata=False):
        raise ValueError("FEC assembly target would discard source fields")
    return pa.RecordBatch.from_arrays(
        [
            batch.column(batch.schema.get_field_index(field.name)).cast(field.type, safe=True)
            if field.name in batch.schema.names else pa.nulls(batch.num_rows, type=field.type)
            for field in schema
        ],
        schema=schema,
    )


def _batches(inputs: Sequence[TypedInput], schema: pa.Schema, batch_size: int,
             check_resources: Callable[[], None], partitioned: bool) -> Iterator[pa.RecordBatch]:
    for item in inputs:
        count = 0
        with pq.ParquetFile(item.path) as source:
            for batch in source.iter_batches(batch_size=batch_size, use_threads=False):
                check_resources()
                batch = _align(batch, schema)
                if partitioned:
                    values = batch.column(schema.get_field_index("source_namespace"))
                    if values.null_count or values.unique().to_pylist() != [item.source_namespace]:
                        raise ValueError("FEC input differs from its declared source namespace")
                count += batch.num_rows
                yield batch
        if count != item.rows:
            raise ValueError("FEC input body count differs from its declared membership")
        if _sha(item.path) != item.sha256:
            raise ValueError("FEC input bytes changed during assembly or readback")


def _equal_streams(expected: Iterator[pa.RecordBatch], actual: Iterator[pa.RecordBatch]) -> int:
    """Compare every cell independently of physical row-group/batch boundaries."""
    left, right = next(expected, None), next(actual, None)
    count = 0
    while left is not None and right is not None:
        size = min(left.num_rows, right.num_rows)
        if not left.slice(0, size).equals(right.slice(0, size)):
            raise ValueError("Combined FEC output differs from selected input values")
        count += size
        left = next(expected, None) if size == left.num_rows else left.slice(size)
        right = next(actual, None) if size == right.num_rows else right.slice(size)
    if left is not None or right is not None:
        raise ValueError("Combined FEC output has missing or additional rows")
    return count


def assemble_table(
    inputs: Sequence[TypedInput], *, schema: pa.Schema, output: Path,
    check_resources: Callable[[], None], partitioned: bool = False,
    read_batch_rows: int = 8192, row_group_rows: int = 32768, part_rows: int = 1048576,
) -> dict:
    """Write and fully compare one table, then create its output exclusively.

    Large tables use actual ``source_namespace`` values as physical partitions;
    each partition retains the exact selected input order and one common schema.
    ``output`` is a table directory for partitions, or a Parquet file otherwise.
    Empty success requires explicit verified zero-row input, never an empty list.
    Failed staged or partially promoted files remain available for diagnosis;
    only the returned receipt admits a successful table. The generation publisher
    owns atomic publication. The resource callback must raise on budget limits.
    """
    if (not inputs or any(type(value) is not int or value <= 0 for value in
                          (read_batch_rows, row_group_rows, part_rows)) or part_rows % row_group_rows):
        raise ValueError("FEC assembly needs selected inputs and positive, aligned batch limits")
    typed_batch([], schema)  # Apply the shared recursive schema checks once.
    paths = [item.path.resolve() for item in inputs]
    if len(paths) != len(set(paths)):
        raise ValueError("The same FEC input file was selected twice")
    staging = output.with_name(output.name + ".partial")
    if output.exists() or staging.exists() or output.is_symlink() or staging.is_symlink():
        raise ValueError("FEC assembly output already exists")
    groups: dict[str | None, list[TypedInput]] = defaultdict(list)
    for item in inputs:
        check_resources()
        _digest(item.sha256)
        if type(item.rows) is not int or item.rows < 0 or item.path.is_symlink() or not item.path.is_file():
            raise ValueError("FEC input must be a regular file with explicit row membership")
        if partitioned and (not isinstance(item.source_namespace, str)
                            or not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", item.source_namespace)):
            raise ValueError("FEC namespace partition requires its exact supported source value")
        if _sha(item.path) != item.sha256:
            raise ValueError("FEC input bytes differ from the qualified digest")
        with pq.ParquetFile(item.path) as source:
            if source.metadata.num_rows != item.rows:
                raise ValueError("FEC input footer differs from declared row membership")
            if not pa.unify_schemas([schema, source.schema_arrow]).equals(schema, check_metadata=False):
                raise ValueError("FEC assembly target would discard source fields")
        groups[item.source_namespace if partitioned else None].append(item)
    if partitioned and "source_namespace" not in schema.names:
        raise ValueError("FEC namespace partition is absent from the actual table schema")
    output.parent.mkdir(parents=True, exist_ok=True)
    if partitioned:
        staging.mkdir()
    results = []
    for namespace, members in groups.items():
        directory = staging / f"source_namespace={namespace}" if partitioned else staging.parent
        if partitioned:
            directory.mkdir()
        part, rows_in_part, writer, stream = 0, 0, None, None
        buffers, buffered_rows, written = [], 0, []

        def flush():
            nonlocal part, rows_in_part, writer, stream, buffers, buffered_rows
            if writer is None:
                target = directory / f"part-{part:06d}.parquet" if partitioned else staging
                stream = target.open("xb")
                writer = pq.ParquetWriter(stream, schema, compression="zstd")
                written.append(target)
            if buffers:
                writer.write_table(pa.Table.from_batches(buffers, schema=schema), row_group_size=row_group_rows)
                rows_in_part += buffered_rows
                buffers, buffered_rows = [], 0
            check_resources()
            if partitioned and rows_in_part == part_rows:
                writer.close()
                assert stream is not None
                stream.close()
                stream = None
                writer, rows_in_part, part = None, 0, part + 1

        try:
            for batch in _batches(members, schema, read_batch_rows, check_resources, partitioned):
                offset = 0
                while offset < batch.num_rows:
                    size = min(batch.num_rows - offset, row_group_rows - buffered_rows)
                    buffers.append(batch.slice(offset, size))
                    buffered_rows += size
                    offset += size
                    if buffered_rows == row_group_rows:
                        flush()
            if buffers or not written:
                flush()
        finally:
            try:
                if writer is not None:
                    writer.close()
            finally:
                if stream is not None:
                    stream.close()

        def stored():
            for path in written:
                with pq.ParquetFile(path) as table:
                    for batch in table.iter_batches(batch_size=read_batch_rows, use_threads=False):
                        check_resources()
                        yield batch

        # Bind compared values to exact bytes. A later hash must not silently
        # adopt a changed file that did not participate in the comparison.
        compared_pins = {path: _sha(path) for path in written}
        checked_rows = _equal_streams(_batches(members, schema, read_batch_rows, check_resources, partitioned), stored())
        if checked_rows != sum(item.rows for item in members):
            raise ValueError("Combined FEC row count differs from selected membership")
        for path in written:
            if _sha(path) != compared_pins[path]:
                raise ValueError("Staged FEC output bytes changed during readback")
            results.append(dict(path=str(path.relative_to(staging)) if partitioned else output.name,
                                sha256=compared_pins[path], bytes=path.stat().st_size,
                                rows=pq.ParquetFile(path).metadata.num_rows))
    check_resources()
    # A rename can replace a destination created since the first exists check.
    # mkdir and hard links refuse EEXIST, including dangling symlinks. Never
    # fall back to a copying function that can overwrite an existing path.
    if partitioned:
        output.mkdir()
    for member in results:
        source = staging / member['path'] if partitioned else staging
        destination = output / member['path'] if partitioned else output
        if partitioned:
            destination.parent.mkdir(exist_ok=True)
        os.link(source, destination)
        if _sha(destination) != member['sha256']:
            raise ValueError("Promoted FEC output differs from its compared bytes")
    # These are only this invocation's staged links. An unexpected directory
    # member stops cleanup instead of being removed recursively.
    for member in results:
        source = staging / member['path'] if partitioned else staging
        source.unlink()
    if partitioned:
        for namespace in groups:
            (staging / f'source_namespace={namespace}').rmdir()
        staging.rmdir()
    return dict(rows=sum(item.rows for item in inputs), members=results,
                partition_columns=["source_namespace"] if partitioned else [],
                every_stored_cell_compared=True, source_files_rehashed_after_readback=True)


def assemble_subject_table(
    inputs: Sequence[TypedInput], *, table: str, schema: pa.Schema, output: Path,
    generation_id: str, context_for, check_resources: Callable[[], None],
    source_partitioned: bool = False, read_batch_rows: int = 8192,
    filing_associations: FilingAssociationInputs | None = None,
) -> dict:
    """Assemble this FEC family's subjects and receipts from qualified mapper files.

    Namespace partitions are checked on the input processing rows. Subject output
    uses ordinary Parquet members whether the table keeps source_namespace as a
    subject column or moves it to receipts; moving it never bypasses input
    partition validation. The original assembly entry point remains available
    for legacy, unmigrated datasets.
    """
    from .fec_subject_receipts import write_fec_subjects, read_fec_with_receipts
    from .fec_native_subjects import FIELD_RULES

    if table not in FIELD_RULES or not inputs or read_batch_rows <= 0:
        raise ValueError("Subject assembly requires an owned dataset and selected inputs")
    paths = [item.path.resolve() for item in inputs]
    if len(paths) != len(set(paths)):
        raise ValueError("The same FEC input file was selected twice")
    for item in inputs:
        check_resources()
        _digest(item.sha256)
        if type(item.rows) is not int or item.rows < 0 or not item.path.is_file() or item.path.is_symlink():
            raise ValueError("FEC input must be a regular file with explicit row membership")
        if _sha(item.path) != item.sha256:
            raise ValueError("FEC input bytes differ from the qualified digest")
        with pq.ParquetFile(item.path) as source:
            if source.metadata.num_rows != item.rows:
                raise ValueError("FEC input footer differs from declared row membership")
            if not pa.unify_schemas([schema, source.schema_arrow]).equals(schema, check_metadata=False):
                raise ValueError("FEC assembly target would discard source fields")
        if source_partitioned and not isinstance(item.source_namespace, str):
            raise ValueError("FEC namespace partition requires its exact supported source value")
    if source_partitioned and "source_namespace" not in schema.names:
        raise ValueError("Namespace validation requires processing rows reconstructed from receipts")

    def records():
        for batch in _batches(inputs, schema, read_batch_rows, check_resources, source_partitioned):
            yield from batch.to_pylist()

    associations_for, association_inputs = (None, None)
    if filing_associations is not None:
        associations_for, association_inputs = filing_associations.prepare(table, schema, check_resources)
    paths, policy = write_fec_subjects(records(), output, table=table, input_schema=schema,
                                      generation_id=generation_id, context_for=context_for,
                                      batch_size=read_batch_rows, associations_for=associations_for)
    subject, receipts = paths
    refused = 0
    for batch in pq.ParquetFile(receipts).iter_batches(columns=["outcome"]):
        refused += sum(v in {"refused", "error", "rejected"} for v in batch.column(0).to_pylist())
    # Compare exact prior mapper rows when the source is fully admitted. A refused
    # input remains in the receipt and explicitly prevents a qualified full rebuild.
    checked = None
    if subject is not None and not refused:
        def restored():
            values = []
            for row in read_fec_with_receipts([subject], [receipts], policy, generation_id=generation_id):
                values.append(row)
                if len(values) == read_batch_rows:
                    yield pa.RecordBatch.from_pylist(values, schema=schema)
                    values.clear()
            if values:
                yield pa.RecordBatch.from_pylist(values, schema=schema)
        checked = _equal_streams(_batches(inputs, schema, read_batch_rows, check_resources, source_partitioned), restored())
    if association_inputs is not None:
        for member in association_inputs["filings"] + association_inputs["headers"]:
            check_resources()
            path = Path(member["path"])
            stamp = path.stat()
            if (member["stat"] != dict(inode=stamp.st_ino, bytes=stamp.st_size, mtime_ns=stamp.st_mtime_ns)
                    or _sha(path) != member["sha256"]):
                raise ValueError("Selected filing dependency changed during subject assembly")
    return dict(dataset=table, subject_path=None if subject is None else str(subject), receipt_path=str(receipts),
                policy=policy.descriptor(), input_rows=sum(i.rows for i in inputs),
                subject_rows=0 if subject is None else pq.ParquetFile(subject).metadata.num_rows,
                refused_rows=refused, exact_mapper_rows_compared=checked,
                source_partition_columns=["source_namespace"] if source_partitioned else [],
                subject_partition_columns=[], full_rebuild_qualified=not refused,
                filing_association_inputs=association_inputs)
