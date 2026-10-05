"""Flat Congress datasets a column at a time: the bundle the row writer writes and the table it restores.

``congress_receipts.write_congress_dataset`` maps, splits, serializes and validates one row at a time, and
``restore_processing_input`` decodes one receipt at a time, over several passes. For ``member_votes`` and
``member_vote_terms`` (10.56M rows each on 2026-10-05) that was about 3.3 hours a table against a 60-minute job.
:func:`write_bundle` and :func:`restore_input` give the same receipts and the same restored rows from one DuckDB scan
each (measured on the first million rows of both tables: every receipt equal and at the same position, the restored
table equal to its source; 5.5 to 8.4 microseconds a row against 577 to 692).

The row code stays the reference and keeps the rows SQL cannot own: a row whose text ``etl_bulk`` spells differently
from Python, and a row the mapper would refuse. Those come back from the ``row_receipt`` and ``row_fields`` callables
the caller passes, which are the row code's own functions, so each rule lives in one place. Such a row's own text
rides along in the one scan. Nothing here runs a second statement on a connection that is still streaming: that ends
the stream without an error, and a bundle cut short that way is consistent with itself (2026-10-05: 199,999 of
1,000,000 rows restored, nothing raised). So each scan also checks that its rows arrive in file order and that it read
as many as the file's own footer counts.

A dataset is taken only when :func:`eligible` says every rule the mapper applies to it is one of the three this
module states in SQL: a column passed through as text, a digit string read as an integer, and a row given no subject.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.congress_subjects import (
    BOOLEANS,
    FLATTEN,
    INPUT_COLUMNS,
    INTEGERS,
    JOINED,
    NATIVE,
    PROCESSING,
    RECEIPT_ONLY,
    subject_schema,
    target_name,
)
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, WITNESS_TYPE, DatasetPolicy

#: ``congress_subjects.integer`` reads an ASCII digit string; 18 digits always fit the subject's int64.
_DIGITS = "'[0-9]{1,18}'"
#: The rows ``congress_subjects.map_record`` gives no subject, as SQL over the source columns. A test holds each to
#: the mapper's own answer.
NO_SUBJECT = {"member_vote_terms": '"term_index" IS NULL AND "term_match" IS NOT NULL'}
#: Receipt columns worth a footer bound; the payload and witness columns are never filtered on.
_STATISTICS = ["dataset", "record_id", "receipt_id", "outcome", "generation_id", "attempt_id", "policy_version"]
_GROUP = 2000
#: Rows held in memory at a time; a receipt is about a kilobyte.
_BATCH = 100_000


def eligible(dataset: str, source: pa.Schema | None = None) -> bool:
    """Whether every rule the mapper applies to ``dataset`` is one this module states in SQL.

    Without ``source`` the answer is for the dataset alone, before its source schema is known.
    """
    source = pa.schema([]) if source is None else source
    if dataset not in INPUT_COLUMNS or dataset in RECEIPT_ONLY or dataset == "public_activity_events":
        return False
    special = {name for table, name in (*NATIVE, *FLATTEN, *JOINED) if table == dataset} | set(BOOLEANS.get(dataset, ()))
    inputs = INPUT_COLUMNS[dataset]
    subject = subject_schema(dataset)
    integers = {field.name for field in subject if pa.types.is_integer(field.type)}
    return (
        not special
        and set(source.names) <= set(inputs)
        and all(pa.types.is_string(field.type) for field in source)
        and all(target_name(dataset, name) == name for name in inputs)
        and all(pa.types.is_string(field.type) or pa.types.is_integer(field.type) for field in subject)
        and integers == set(INTEGERS.get(dataset, ())) & set(subject.names)
        and set(subject.names) == set(inputs) - set(PROCESSING.get(dataset, ()))
    )


def _text(value: Any) -> str:
    return "CAST(NULL AS VARCHAR)" if value is None else "'" + str(value).replace("'", "''") + "'"


def _witnesses_sql(first: Mapping[str, Any], rest: Sequence[Mapping[str, Any]]) -> str:
    """A list of witness structs: the row's own, whose locator is its ordinal, then the caller's."""
    stored = pa.array([list(rest)], type=pa.list_(WITNESS_TYPE)).to_pylist()[0]
    own = ", ".join(f"'{name}': " + ("CAST(_ordinal AS VARCHAR)" if name == "locator" else _text(first.get(name))) for name in WITNESS_TYPE.names)
    others = ["{" + ", ".join(f"'{name}': {_text(witness[name])}" for name in WITNESS_TYPE.names) + "}" for witness in stored]
    return "[" + ", ".join(["{" + own + "}", *others]) + "]"


def _source_sql(source: Path, file_schema: pa.Schema, dataset: str, policy: DatasetPolicy, context_reference: bool = False) -> str:
    """The source rows, each with its ordinal, its exact text, whether it has no subject, and whether it is the row code's."""
    integers = [field.name for field in policy.subject_schema if pa.types.is_integer(field.type) and field.name in file_schema.names]
    bad_integer = " OR ".join(f'("{name}" IS NOT NULL AND NOT regexp_full_match("{name}", {_DIGITS}))' for name in integers) or "false"
    no_identity = " OR ".join(
        f'"{name}" IS NULL' if name in file_schema.names else "true" for name in policy.identity_fields if name not in policy.nullable_identity_fields
    ) or "false"
    no_subject = NO_SUBJECT.get(dataset, "false")
    for name in INPUT_COLUMNS[dataset]:
        if name not in file_schema.names:
            no_subject = no_subject.replace('"' + name + '"', 'CAST(NULL AS VARCHAR)')
    raw = etl_bulk.record_json_sql((name, file_schema.field(name).type) for name in file_schema.names)
    return f"""SELECT *, ((NOT _rejected AND (({no_identity}) OR ({bad_integer}))) OR {etl_bulk.needs_reference_sql("_raw")} OR {str(context_reference).lower()}) AS _reference
               FROM (SELECT *, file_row_number AS _ordinal, {raw} AS _raw, ({no_subject}) AS _rejected
                     FROM read_parquet({_text(source)}, file_row_number = true))"""


def _context_reference(source, dataset, policy, generation_id, processor, witnesses) -> bool:
    """Context text with a control escape SQL spells differently routes every row."""
    controls = "\x0b\x0e\x0f\x1a\x1b\x1c\x1d\x1e\x1f"
    values = [str(source), dataset, policy.policy_version, generation_id, processor,
              *(value for witness in witnesses for value in witness.values())]
    return any(isinstance(value, str) and any(char in value for char in controls) for value in values)


def iter_routed_ordinals(source: Path, *, dataset: str, policy: DatasetPolicy,
                         generation_id: str | None = None, processor: str | None = None,
                         witnesses: Sequence[Mapping[str, Any]] = ()) -> Iterator[int]:
    """Every routed source ordinal in file order, with bounded memory.

    Supply the write's context to include routes caused by witness or publisher text.
    """
    source = Path(source).resolve()
    reference = _context_reference(source, dataset, policy, generation_id, processor, witnesses)
    with etl_bulk.bulk_connection() as (con, _):
        query = f"SELECT _ordinal FROM ({_source_sql(source, pq.read_schema(source), dataset, policy, reference)}) WHERE _reference ORDER BY _ordinal"
        for batch in con.execute(query).to_arrow_reader(_BATCH):
            yield from batch.column(0).to_pylist()


def routed_ordinals(source: Path, *, dataset: str, policy: DatasetPolicy, **context) -> list[int]:
    """List wrapper for bounded samples; large qualification uses ``iter_routed_ordinals``."""
    return list(iter_routed_ordinals(source, dataset=dataset, policy=policy, **context))


def _rows_in(path: Path, dataset: str | None = None) -> int:
    """How many rows the file itself counts, or how many of ``dataset``'s receipts: read apart from the scan it checks."""
    import pyarrow.dataset as ds

    if dataset is None:
        return pq.ParquetFile(path).metadata.num_rows
    return ds.dataset(path, format="parquet").count_rows(filter=ds.field("dataset") == dataset)


def _with_insertions(kept: pa.Table, mask: list[bool], insertions: Mapping[int, dict | None]) -> pa.Table:
    """Merge reference rows in original order using one Arrow table per batch."""
    positions = [index for index, flag in enumerate(mask) if flag]
    replacements = [(index, row) for index, row in insertions.items() if row is not None]
    if not replacements:
        return kept
    added = pa.Table.from_pylist([row for _, row in replacements], schema=kept.schema)
    positions.extend(index for index, _ in replacements)
    order = sorted(range(len(positions)), key=positions.__getitem__)
    return pa.concat_tables([kept, added]).take(order)


def write_bundle(
    source: Path,
    subjects_path: Path,
    receipts_path: Path,
    *,
    dataset: str,
    policy: DatasetPolicy,
    generation_id: str,
    processor: str,
    digest: str,
    first_receipts: Sequence[Mapping[str, Any]],
    row_receipt: Callable[[str, int], tuple[dict | None, dict]],
    witnesses: Sequence[Mapping[str, Any]] = (),
) -> int:
    """Write ``dataset``'s subject table and receipts to the two paths; return how many rows went to the row code.

    ``digest`` is the source file's, as the row writer computes it. ``first_receipts`` are written ahead of the rows
    (the table-metadata receipt). ``row_receipt(processing_json, ordinal)`` is the row writer's own step for one
    source row, which it reads back out of the processing text built here.
    """
    source = Path(source).resolve()
    file_schema = pq.read_schema(source)
    subject = policy.subject_schema
    integers = {field.name for field in subject if pa.types.is_integer(field.type)}
    columns = ", ".join(
        f'CAST(NULL AS VARCHAR) AS "{field.name}"' if field.name not in file_schema.names
        else f'CASE WHEN regexp_full_match("{field.name}", {_DIGITS}) THEN CAST("{field.name}" AS BIGINT) END AS "{field.name}"' if field.name in integers
        else f'"{field.name}"'
        for field in subject
    )
    record_id, version, identity = etl_bulk.identity_sql(policy)
    own_witness = {"source_id": "shaped-observation:" + dataset, "source_uri": str(source), "sha256": digest, "body_version": None}
    reference = _context_reference(source, dataset, policy, generation_id, processor, witnesses)
    query = f"""
    WITH read AS ({_source_sql(source, file_schema, dataset, policy, reference)}),
    shaped AS (SELECT {columns}, _ordinal, _raw, _rejected, _reference FROM read),
    keyed AS (SELECT *, CASE WHEN NOT _rejected THEN {record_id} END AS record_id, CASE WHEN NOT _rejected THEN {version} END AS subject_version,
                     CASE WHEN NOT _rejected THEN {identity} END AS identity_json,
                     '["dict",[["entry_kind",["str","row"]],["source_fields",' || _raw || ']]]' AS processing_json FROM shaped),
    receipt AS (SELECT * EXCLUDE (_raw), {_text(dataset)} AS dataset, {_text(policy.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
                       {_text(dataset + ":" + digest + ":")} || _ordinal AS attempt_id, CASE WHEN _rejected THEN 'rejected' ELSE 'accepted' END AS outcome,
                       {_text(processor)} AS processor, {_witnesses_sql(own_witness, witnesses)} AS witnesses,
                       CASE WHEN _rejected THEN '["dict",[["reason",["str","no_domain_subject"]]]]' ELSE '["dict",[]]' END AS diagnostic_json FROM keyed)
    SELECT {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id, * EXCLUDE (_reference),
           (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
           NOT (_rejected OR _reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _accepted FROM receipt"""
    expected, seen, left = _rows_in(source), 0, 0
    with etl_bulk.bulk_connection() as (con, _), pq.ParquetWriter(subjects_path, subject, compression="zstd") as subjects, \
            pq.ParquetWriter(receipts_path, RECEIPT_SCHEMA, compression="zstd", write_statistics=_STATISTICS) as receipts:
        pending = pa.Table.from_pylist(list(first_receipts), schema=RECEIPT_SCHEMA)
        for batch in con.execute(query).to_arrow_reader(_BATCH):
            table = pa.Table.from_batches([batch])
            table.validate(full=True)
            ordinals = table["_ordinal"].to_pylist()
            if ordinals != list(range(seen, seen + len(ordinals))):
                raise RuntimeError(f"{dataset}: source rows did not arrive in file order")
            seen += len(ordinals)
            flagged = table["_reference"].to_pylist()
            written = table.filter(pa.array([not flag for flag in flagged])).select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA)
            kept = table.filter(table["_accepted"]).select(subject.names).cast(subject)
            if any(flagged):
                texts, row_subjects, row_receipts = table["processing_json"].to_pylist(), {}, {}
                for index in (index for index, flag in enumerate(flagged) if flag):
                    row_subject, receipt = row_receipt(texts[index], ordinals[index])
                    row_subjects[index] = row_subject
                    row_receipts[index] = receipt
                left += len(row_receipts)
                written = _with_insertions(written, [not flag for flag in flagged], row_receipts)
                kept = _with_insertions(kept, table["_accepted"].to_pylist(), row_subjects)
            pending = pa.concat_tables([pending, written])
            if whole := pending.num_rows - pending.num_rows % _GROUP:
                receipts.write_table(pending.slice(0, whole), row_group_size=_GROUP)
                pending = pending.slice(whole)
            if kept.num_rows:
                subjects.write_table(kept)
        if pending.num_rows:
            receipts.write_table(pending, row_group_size=_GROUP)
    if seen != expected:
        raise RuntimeError(f"{dataset}: read {seen} of the source's {expected} rows")
    return left


#: How ``exact_json`` spells a table-metadata entry, at any depth.
_FOOTER_ENTRY = '["entry_kind",["str","table_metadata"]]'
#: How it opens a row attempt that holds nothing but its source fields.
_ROW_OPENING = '["dict",[["entry_kind",["str","row"]],["source_fields",["dict",['


def footer_candidates(receipts: Path, *, dataset: str) -> list[str]:
    """The processing text of every receipt of ``dataset`` that could be a table-metadata entry, in file order.

    A superset: the text is matched at any depth, so the caller decodes each and keeps the ones that are.
    """
    with etl_bulk.bulk_connection() as (con, _):
        return [text for (text,) in con.execute(
            f"SELECT processing_json FROM read_parquet({_text(receipts)}, file_row_number = true) "
            f"WHERE dataset = {_text(dataset)} AND contains(processing_json, {_text(_FOOTER_ENTRY)}) ORDER BY file_row_number"
        ).fetchall()]


def source_fields_sql(schema: pa.Schema) -> tuple[str, str]:
    """SQL over ``processing_json``: the retained source columns, and whether the text is the plain shape they read.

    Plain is a row attempt holding only ``source_fields``, with exactly ``schema``'s columns, each text or NULL.
    """
    names = sorted(schema.names)
    pairs = "json_extract(processing_json, '$[1][1][1][1]')"
    plain = " AND ".join(
        [f"starts_with(processing_json, {_text(_ROW_OPENING)})", "json_array_length(processing_json, '$[1]') = 2",
         f"json_array_length({pairs}) = {len(names)}"]
        + [f"json_extract_string({pairs}, '$[{i}][0]') = {_text(name)}" for i, name in enumerate(names)]
        + [f"json_extract_string({pairs}, '$[{i}][1][0]') IN ('str', 'null')" for i in range(len(names))]
    )
    columns = ", ".join(f"json_extract_string({pairs}, '$[{names.index(field.name)}][1][1]') AS \"{field.name}\"" for field in schema)
    return columns, plain


def restore_input(
    receipts: Path,
    destination: Path,
    *,
    dataset: str,
    schema: pa.Schema,
    row_fields: Callable[[str, str], Mapping[str, Any] | None],
) -> int:
    """Write the source rows ``dataset``'s receipts retain, in builder order, in the retained ``schema``.

    Equal to the rows ``restore_processing_input`` yields: every ``row`` attempt's ``source_fields`` except a refused
    conversion's. The caller has validated the bundle and read ``schema`` from its table-metadata receipt.
    ``row_fields(processing_json, diagnostic_json)`` is the row code's decoding of one receipt. It decides every
    receipt that is not the plain shape or whose diagnostics mention a refused conversion, and returns None to leave
    the row out. Returns how many receipts went to it.
    """
    from spicy_regs.native_types import reject_extra_fields

    if not all(pa.types.is_string(field.type) for field in schema):
        raise NotImplementedError("Only text source columns are decoded here")
    columns, plain = source_fields_sql(schema)
    query = f"""
    SELECT {columns}, _position, _reference, CASE WHEN _reference THEN processing_json END AS _processing,
           CASE WHEN _reference THEN diagnostic_json END AS _diagnostic
    FROM (SELECT *, file_row_number AS _position,
                 (NOT coalesce({plain}, false) OR contains(diagnostic_json, 'conversion_refused')) AS _reference
          FROM read_parquet({_text(receipts)}, file_row_number = true) WHERE dataset = {_text(dataset)})"""
    expected, seen, last, left = _rows_in(receipts, dataset), 0, -1, 0
    # As ``write_rows`` does: the destination appears only once every row is written.
    with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(dir=destination.parent) as scratch:
        temporary = Path(scratch) / "rows.parquet"
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            for batch in con.execute(query).to_arrow_reader(_BATCH):
                table = pa.Table.from_batches([batch])
                table.validate(full=True)
                positions = table["_position"].to_pylist()
                if positions != sorted(set(positions)) or (positions and positions[0] <= last):
                    raise RuntimeError(f"{dataset}: receipts did not arrive in file order")
                seen, last = seen + len(positions), positions[-1] if positions else last
                flagged = table["_reference"].to_pylist()
                rows = table.filter(pa.array([not flag for flag in flagged])).select(schema.names).cast(schema)
                if any(flagged):
                    processing, diagnostic, decoded = table["_processing"].to_pylist(), table["_diagnostic"].to_pylist(), {}
                    for index in (index for index, flag in enumerate(flagged) if flag):
                        fields = row_fields(processing[index], diagnostic[index])
                        if fields is not None:
                            reject_extra_fields(fields, schema)
                        decoded[index] = None if fields is None else dict(fields)
                    left += len(decoded)
                    rows = _with_insertions(rows, [not flag for flag in flagged], decoded)
                if rows.num_rows:
                    writer.write_table(rows)
        if seen != expected:
            raise RuntimeError(f"{dataset}: read {seen} of the file's {expected} receipts")
        temporary.replace(destination)
    return left
