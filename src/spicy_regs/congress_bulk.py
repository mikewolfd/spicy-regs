"""Flat Congress datasets a column at a time: the bundle the row writer writes and the table it restores.

``congress_receipts.write_congress_dataset`` maps, splits, serializes and validates one row at a time, and
``restore_processing_input`` decodes one receipt at a time, over several passes. For ``member_votes`` and
``member_vote_terms`` (10.56M rows each on 2026-10-05) that was about 3.3 hours a table against a 60-minute job.
:func:`write_bundle` and :func:`restore_input` give the same receipts and the same restored rows from one DuckDB scan
each (measured on the first million rows of both tables: every receipt equal and at the same position, the restored
table equal to its source; 5.5 to 8.4 microseconds a row against 577 to 692).

The row code stays the reference and keeps the rows SQL cannot own: a row whose text ``etl_bulk`` spells differently
from Python, and a row the mapper would refuse. Those come back from the ``row_receipt`` and ``row_fields`` callables
the caller passes, which are the row code's own functions, so each rule lives in one place.

A dataset is taken only when :func:`eligible` says every rule the mapper applies to it is one of the three this
module states in SQL: a column passed through as text, a digit string read as an integer, and a row given no subject.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
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


def eligible(dataset: str, source: pa.Schema) -> bool:
    """Whether every rule the mapper applies to ``dataset`` is one this module states in SQL."""
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
    row_receipt: Callable[[Mapping[str, Any], int], tuple[dict | None, dict]],
    witnesses: Sequence[Mapping[str, Any]] = (),
) -> int:
    """Write ``dataset``'s subject table and receipts to the two paths; return how many rows went to the row code.

    ``digest`` is the source file's, as the row writer computes it. ``first_receipts`` are written ahead of the rows
    (the table-metadata receipt). ``row_receipt(raw, ordinal)`` is the row writer's own step for one source row.
    """
    import duckdb

    source = Path(source).resolve()
    file_schema = pq.read_schema(source)
    subject = policy.subject_schema
    integers = [field.name for field in subject if pa.types.is_integer(field.type)]
    columns = ", ".join(
        f'CAST(NULL AS VARCHAR) AS "{field.name}"' if field.name not in file_schema.names
        else f'CASE WHEN regexp_full_match("{field.name}", {_DIGITS}) THEN CAST("{field.name}" AS BIGINT) END AS "{field.name}"' if field.name in integers
        else f'"{field.name}"'
        for field in subject
    )
    present = [name for name in integers if name in file_schema.names]
    bad_integer = " OR ".join(f'("{name}" IS NOT NULL AND NOT regexp_full_match("{name}", {_DIGITS}))' for name in present) or "false"
    no_identity = " OR ".join(
        f'"{name}" IS NULL' if name in file_schema.names else "true" for name in policy.identity_fields if name not in policy.nullable_identity_fields
    ) or "false"
    no_subject = NO_SUBJECT.get(dataset, "false")
    record_id, version, identity = etl_bulk.identity_sql(policy)
    raw = etl_bulk.record_json_sql((name, file_schema.field(name).type) for name in file_schema.names)
    own_witness = {"source_id": "shaped-observation:" + dataset, "source_uri": str(source), "sha256": digest, "body_version": None}
    query = f"""
    WITH read AS (SELECT *, file_row_number AS _ordinal, {raw} AS _raw, ({no_subject}) AS _rejected, ({bad_integer}) AS _bad_integer,
                         ({no_identity}) AS _no_identity FROM read_parquet({_text(source)}, file_row_number = true)),
    shaped AS (SELECT {columns}, _ordinal, _raw, _rejected, _bad_integer, _no_identity FROM read),
    keyed AS (SELECT *, CASE WHEN NOT _rejected THEN {record_id} END AS record_id, CASE WHEN NOT _rejected THEN {version} END AS subject_version,
                     CASE WHEN NOT _rejected THEN {identity} END AS identity_json,
                     '["dict",[["entry_kind",["str","row"]],["source_fields",' || _raw || ']]]' AS processing_json FROM shaped),
    receipt AS (SELECT * EXCLUDE (_raw), {_text(dataset)} AS dataset, {_text(policy.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
                       {_text(dataset + ":" + digest + ":")} || _ordinal AS attempt_id, CASE WHEN _rejected THEN 'rejected' ELSE 'accepted' END AS outcome,
                       {_text(processor)} AS processor, {_witnesses_sql(own_witness, witnesses)} AS witnesses,
                       CASE WHEN _rejected THEN '["dict",[["reason",["str","no_domain_subject"]]]]' ELSE '["dict",[]]' END AS diagnostic_json FROM keyed)
    , marked AS (SELECT {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id, *,
                       ((NOT _rejected AND (_no_identity OR _bad_integer)) OR {etl_bulk.needs_reference_sql("processing_json")}) AS _reference
                FROM receipt)
    SELECT *, NOT (_rejected OR _reference) AS _accepted FROM marked ORDER BY _ordinal"""
    left = 0
    with duckdb.connect() as con, pq.ParquetWriter(subjects_path, subject, compression="zstd") as subjects, \
            pq.ParquetWriter(receipts_path, RECEIPT_SCHEMA, compression="zstd", write_statistics=_STATISTICS) as receipts:
        pending = pa.Table.from_pylist(list(first_receipts), schema=RECEIPT_SCHEMA)
        for batch in con.execute(query).to_arrow_reader(100_000):
            table = pa.Table.from_batches([batch])
            accepted = table.filter(table["_accepted"])
            written, kept = table.select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA), accepted.select(subject.names).cast(subject)
            flagged = table.filter(table["_reference"])["_ordinal"].to_pylist()
            if flagged:
                left += len(flagged)
                written, kept = _with_row_code(con, source, table, written, flagged, row_receipt, subject)
            pending = pa.concat_tables([pending, written])
            whole = pending.num_rows - pending.num_rows % _GROUP
            receipts.write_table(pending.slice(0, whole), row_group_size=_GROUP)
            pending = pending.slice(whole)
            subjects.write_table(kept)
        if pending.num_rows:
            receipts.write_table(pending, row_group_size=_GROUP)
    return left


def _with_row_code(con, source, table, written, flagged, row_receipt, subject):
    """Replace the flagged rows' receipts, in place, with the row code's, and rebuild the batch's subjects in order."""
    placeholders = ", ".join(str(int(ordinal)) for ordinal in flagged)
    raws = con.execute(
        f"SELECT * EXCLUDE (file_row_number), file_row_number FROM read_parquet({_text(source)}, file_row_number = true) "
        f"WHERE file_row_number IN ({placeholders})"
    ).to_arrow_table().to_pylist()
    replaced = {}
    for raw in raws:
        ordinal = raw.pop("file_row_number")
        replaced[ordinal] = row_receipt(raw, ordinal)
    receipts, subjects = written.to_pylist(), []
    ordinals, rejected = table["_ordinal"].to_pylist(), table["_rejected"].to_pylist()
    stored = table.select(subject.names).cast(subject).to_pylist()
    for index, ordinal in enumerate(ordinals):
        if ordinal in replaced:
            row_subject, receipts[index] = replaced[ordinal]
            if row_subject is not None:
                subjects.append(row_subject)
        elif not rejected[index]:
            subjects.append(stored[index])
    return pa.Table.from_pylist(receipts, schema=RECEIPT_SCHEMA), pa.Table.from_pylist(subjects, schema=subject)


def restore_input(
    receipts: Path,
    destination: Path,
    *,
    dataset: str,
    schema: pa.Schema,
    row_fields: Callable[[Mapping[str, Any]], Mapping[str, Any] | None],
) -> int:
    """Write the source rows ``dataset``'s receipts retain, in builder order, in the retained ``schema``.

    Equal to the rows ``restore_processing_input`` yields: every ``row`` attempt's ``source_fields`` except a refused
    conversion's. The caller has validated the bundle and read ``schema`` from its table-metadata receipt.
    ``row_fields(receipt)`` is the row code's decoding of one receipt, used for a receipt whose copy is not the plain
    shape read here; it returns None to leave the row out. Returns how many receipts went to the row code.
    """
    import duckdb

    with duckdb.connect() as con:
        scope = f"dataset = {_text(dataset)}"
        names = sorted(schema.names)
        pairs = "json_extract(processing_json, '$[1][1][1][1]')"
        plain = " AND ".join(
            ["json_extract_string(processing_json, '$[1][1][0]') = 'source_fields'", "json_array_length(processing_json, '$[1]') = 2",
             f"json_array_length({pairs}) = {len(names)}"]
            + [f"json_extract_string({pairs}, '$[{i}][0]') = {_text(name)}" for i, name in enumerate(names)]
            + [f"json_extract_string({pairs}, '$[{i}][1][0]') IN ('str', 'null')" for i in range(len(names))]
        )
        columns = ", ".join(f"json_extract_string({pairs}, '$[{names.index(field.name)}][1][1]') AS \"{field.name}\"" for field in schema)
        # A row attempt's processing begins with its entry_kind, the first key in sorted order. A refused conversion
        # states its reason at the top of its diagnostics; the row code decides any receipt that mentions one.
        query = f"""
        SELECT {columns}, file_row_number AS _position,
               (NOT ({plain}) OR contains(diagnostic_json, 'conversion_refused')) AS _reference
        FROM read_parquet({_text(receipts)}, file_row_number = true)
        WHERE {scope} AND processing_json LIKE '["dict",[["entry_kind",["str","row"]]%'
        ORDER BY file_row_number"""
        left = 0
        # As ``write_rows`` does: the destination appears only once every row is written.
        with TemporaryDirectory(dir=destination.parent) as scratch:
            temporary = Path(scratch) / "rows.parquet"
            with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
                for batch in con.execute(query).to_arrow_reader(200_000):
                    table = pa.Table.from_batches([batch])
                    flagged = table.filter(table["_reference"])["_position"].to_pylist()
                    rows = table.select(schema.names).cast(schema)
                    if flagged:
                        left += len(flagged)
                        rows = _decoded_by_row_code(con, receipts, table, rows, flagged, row_fields, schema)
                    writer.write_table(rows)
            temporary.replace(destination)
    return left


def _decoded_by_row_code(con, receipts, table, rows, flagged, row_fields, schema):
    placeholders = ", ".join(str(int(position)) for position in flagged)
    selected = con.execute(
        f"SELECT * EXCLUDE (file_row_number), file_row_number FROM read_parquet({_text(receipts)}, file_row_number = true) "
        f"WHERE file_row_number IN ({placeholders})"
    ).to_arrow_table().to_pylist()
    decoded = {}
    for receipt in selected:
        position = receipt.pop("file_row_number")
        decoded[position] = row_fields(receipt)
    out = []
    for position, row in zip(table["_position"].to_pylist(), rows.to_pylist()):
        if position not in decoded:
            out.append(row)
        elif decoded[position] is not None:
            out.append(dict(decoded[position]))
    return pa.Table.from_pylist(out, schema=schema)
