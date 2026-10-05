"""Bulk comments shaping with exact raw rows and row-writer fallback.

The shared regulations mapper remains the authority for attachment parsing and
HTML text. SQL handles pass-through fields, receipt encoding and hashing. Every
unsupported row goes back to the caller's reference writer in its source order.
This module writes private files; catalog commits and publication stay with the
catalog owner.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.congress_bulk import _text, _with_insertions
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, DatasetPolicy
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.html_text import register_html_text
from spicy_regs.transforms.regulations_shape import COMMENT_ATTACHMENTS, SOURCE_COLUMNS, TYPES, _native, _validate

_BATCH = 10_000
_ATTACHMENT_RESULT = pa.struct([('native', pa.string()), ('reference', pa.bool_())])


def eligible(schema: pa.Schema) -> bool:
    """Only declared text columns and the original integer count are handled."""
    known = dict(SOURCE_COLUMNS['comments'])
    return all(field.name in known and (
        pa.types.is_string(field.type) or
        field.name == 'duplicate_comments' and pa.types.is_integer(field.type)
    ) for field in schema) and len(set(schema.names)) == len(schema.names)


def _attachments(values):
    """Use the shared strict parser; a refusal belongs to the reference attempt."""
    rows = []
    for value in values.to_pylist():
        try:
            native = _native('comments', 'attachments_json', value)
            _validate(native, COMMENT_ATTACHMENTS, 'comments.attachments')
            stored = pa.array([native], type=COMMENT_ATTACHMENTS).to_pylist()[0]
            rows.append({'native': None if stored is None else json.dumps(stored, ensure_ascii=False, separators=(',', ':')), 'reference': False})
        except (ValueError, TypeError, OverflowError, pa.ArrowException):
            rows.append({'native': None, 'reference': True})
    return pa.array(rows, type=_ATTACHMENT_RESULT)


def write_bundle(source: Path, subjects: Path, receipts: Path, *, policy: DatasetPolicy,
                 generation_id: str, source_label: str,
                 row_attempt: Callable[[Mapping, int], tuple[dict | None, dict]]) -> int:
    """Write a private catalog pair, preserving full original and normalized input.

    ``row_attempt`` runs the existing catalog normalizer and regulations writer.
    The returned number counts routed rows. The caller admits the completed pair
    and decides whether any refused row stops the catalog transaction.
    """
    schema = pq.read_schema(source)
    if not eligible(schema):
        raise etl_bulk.NotBulkEligible('Comments bulk writer requires declared text/integer source columns')
    raw = etl_bulk.record_json_sql((field.name, field.type) for field in schema)
    columns = []
    for name, typename in SOURCE_COLUMNS['comments']:
        expression = '"' + name + '"' if name in schema.names else 'NULL'
        if name == 'duplicate_comments':
            expression = f'TRY_CAST({expression} AS INTEGER)'
        columns.append(f'{expression} AS "{name}"')
    attachment_type = dict(described_schema(policy.subject_schema))['attachments']
    fields = ', '.join('"' + field.name + '"' for field in policy.subject_schema)
    normalized = etl_bulk.record_json_sql((name, TYPES[kind]) for name, kind in SOURCE_COLUMNS['comments'])
    processing = [(name, pa.string()) for name in policy.receipt_fields
                  if name not in {'raw_conversion_inputs', 'input_metadata', 'raw_source_record'}]
    # These field names are explicit policy decisions, and their order is exact_json's.
    pairs = [(name, etl_bulk.exact_json_sql('"' + name + '"', dtype)) for name, dtype in processing]
    pairs.extend([('raw_conversion_inputs', '_normalized'), ('raw_source_record', '_raw'),
                  ('input_metadata', "'[\"dict\",[]]'" )])
    payload = "'[\"dict\",[' || " + " || ',' || ".join(
        _text('[' + json.dumps(name) + ',') + ' || ' + value + " || ']'" for name, value in sorted(pairs)
    ) + " || ']]'"
    integer = '"duplicate_comments"'
    if 'duplicate_comments' in schema.names:
        integer_bad = f'(_count_raw IS NOT NULL AND (NOT regexp_full_match(CAST(_count_raw AS VARCHAR), \'[+-]?[0-9]+\') OR {integer} IS NULL))'
        count_raw = '"duplicate_comments"'
    else:
        integer_bad, count_raw = 'false', 'NULL'
    record_id, version, identity = etl_bulk.identity_sql(policy)
    subject_text = etl_bulk.record_json_sql((field.name, field.type) for field in policy.subject_schema)
    witness = "[{\"source_id\": " + _text(source_label) + ", 'source_uri': NULL::VARCHAR, 'sha256': sha256(_raw), " + \
              "'locator': 'receipt.values.raw_source_record (canonical exact_json)', 'body_version': " + _text(generation_id) + '}]'
    query = f'''
    WITH read AS (SELECT *, file_row_number AS _ordinal, {raw} AS _raw, {count_raw} AS _count_raw
                  FROM read_parquet({_text(str(source.resolve()))}, file_row_number=true)),
    normalized AS (SELECT {', '.join(columns)}, _ordinal, _raw, _count_raw FROM read),
    shaped AS (SELECT *, {normalized} AS _normalized, comments_attachments(attachments_json) AS _attachment,
               html_text(comment) AS comment_text FROM normalized),
    subjects AS (SELECT *, CAST(CAST(_attachment.native AS JSON) AS {attachment_type}) AS attachments FROM shaped),
    receipt AS (SELECT {fields}, _ordinal, _raw,
               (_attachment.reference OR {integer_bad} OR comment_id IS NULL OR NOT comments_identity(comment_id)
                OR {etl_bulk.needs_reference_sql("_raw")} OR {etl_bulk.needs_reference_sql(subject_text)}) AS _reference,
               {record_id} AS record_id, {version} AS subject_version, {identity} AS identity_json,
               {payload} AS processing_json, {_text(policy.dataset)} AS dataset,
               {_text(policy.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
               'row:' || _ordinal AS attempt_id, 'accepted' AS outcome, 'regulatory-catalog-native-v1' AS processor,
               {witness} AS witnesses, '["dict",[]]' AS diagnostic_json FROM subjects)
    SELECT * EXCLUDE (_reference), (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
           {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id FROM receipt'''
    expected, seen, routed = pq.ParquetFile(source).metadata.num_rows, 0, 0
    with etl_bulk.bulk_connection() as (con, work):
        register_html_text(con)
        con.create_function('comments_attachments', _attachments, ['VARCHAR'], 'STRUCT(native VARCHAR, reference BOOLEAN)',
                            type='arrow', null_handling='special')
        con.create_function('comments_identity', lambda values: pa.array([isinstance(v, str) and bool(v.strip())
                            for v in values.to_pylist()], type=pa.bool_()), ['VARCHAR'], 'BOOLEAN',
                            type='arrow', null_handling='special')
        refused_path = work / 'refused.parquet'
        with pq.ParquetWriter(subjects, policy.subject_schema, compression='zstd') as sw, \
                pq.ParquetWriter(receipts, RECEIPT_SCHEMA, compression='zstd') as rw, \
                pq.ParquetWriter(refused_path, RECEIPT_SCHEMA, compression='zstd') as fw:
            for batch in con.execute(query).to_arrow_reader(_BATCH):
                table = pa.Table.from_batches([batch])
                table.validate(full=True)
                ordinals = table['_ordinal'].to_pylist()
                if ordinals != list(range(seen, seen + len(ordinals))):
                    raise RuntimeError('comments: source rows did not arrive in file order')
                seen += len(ordinals)
                flags = table['_reference'].to_pylist()
                mask = [not flag for flag in flags]
                kept_subjects = table.filter(pa.array(mask)).select(policy.subject_schema.names).cast(policy.subject_schema)
                kept_receipts = table.filter(pa.array(mask)).select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA)
                added_subjects, added_receipts, refused = {}, {}, []
                if any(flags):
                    raw_texts = table['_raw'].to_pylist()
                    from spicy_regs.etl_receipts import _unpack
                    for index, flag in enumerate(flags):
                        if flag:
                            subject, receipt = row_attempt(_unpack(json.loads(raw_texts[index])), ordinals[index])
                            added_subjects[index] = subject
                            if receipt["outcome"] == "accepted":
                                added_receipts[index] = receipt
                            else:
                                refused.append(receipt)
                            routed += 1
                if refused:
                    fw.write_table(pa.Table.from_pylist(refused, schema=RECEIPT_SCHEMA))
                sw.write_table(_with_insertions(kept_subjects, mask, added_subjects))
                rw.write_table(_with_insertions(kept_receipts, mask, added_receipts))
            fw.close()
            for batch in pq.ParquetFile(refused_path).iter_batches(batch_size=_BATCH):
                rw.write_batch(batch)
    if seen != expected:
        raise RuntimeError(f'comments: read {seen} of the source file\'s {expected} rows')
    return routed


def restore_catalog(subjects: Path, receipts: Path, destination: Path, *, generation_id: str) -> int:
    """Restore catalog comments after complete admission and shared mapper proof.

    The catalog retains normalized processor inputs plus complete original rows.
    Batch shaping uses the same mapper as _processor_input, followed by one Arrow
    conversion per batch. Any unproven value returns the whole pair to that row
    reader so it decides the exact first error. No destination appears until all
    joins, input values, and source-order checks pass.
    """
    from tempfile import TemporaryDirectory
    from spicy_regs.etl_receipts import decode_exact_json, exact_json
    from spicy_regs.transforms.regulations_shape import RECEIPT_COLUMNS, shape_record
    from spicy_regs.transforms.regulations_receipts import policy as selected_policy

    selected = selected_policy('comments')
    etl_bulk.validate_bundle({'comments': [subjects]}, [receipts], [selected], generation_id=generation_id)
    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS['comments']])
    expected = pq.ParquetFile(subjects).metadata.num_rows
    # Admission already checked the exact canonical identity and accepted joins.
    # Decode its sole comment_id value to join; SQL spelling of control escapes
    # never becomes a second identity calculation here.
    identity = "json_extract_string(r.identity_json, '$[1][0][1][1][1]')"
    query = f"""SELECT n.* EXCLUDE(file_row_number), n.file_row_number AS _ordinal,
                       r.processing_json AS _processing
        FROM read_parquet({_text(subjects)}, file_row_number=true) n
        JOIN read_parquet({_text(receipts)}) r ON n.comment_id={identity}
        WHERE r.dataset='comments' AND r.outcome='accepted' ORDER BY n.file_row_number"""
    destination.parent.mkdir(parents=True, exist_ok=True)
    seen = 0
    try:
        with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(prefix='comments-restore-', dir=destination.parent) as scratch:
            temporary = Path(scratch)/'processing.parquet'
            with pq.ParquetWriter(temporary, schema, compression='zstd') as writer:
                for batch in con.execute(query).to_arrow_reader(min(_BATCH, 1000)):
                    table = pa.Table.from_batches([batch])
                    table.validate(full=True)
                    if table['_ordinal'].to_pylist() != list(range(seen, seen+batch.num_rows)):
                        raise RuntimeError('comments: restored subject order or membership changed')
                    held = [decode_exact_json(value) for value in table['_processing'].to_pylist()]
                    if any(values.get('input_metadata') != {} for values in held):
                        raise etl_bulk.NotBulkEligible('Comments catalog restore requires empty processor metadata')
                    raw = [values.get('raw_conversion_inputs') for values in held]
                    if any(not isinstance(value, Mapping) for value in raw):
                        raise etl_bulk.NotBulkEligible('Comments exact retained processor input is required')
                    shaped = [shape_record('comments', value) for value in raw]
                    reproduced = pa.Table.from_pylist(shaped, schema=selected.subject_schema)
                    actual = table.select(selected.subject_schema.names).cast(selected.subject_schema)
                    if not reproduced.equals(actual):
                        raise etl_bulk.NotBulkEligible('Comments processor input differs from selected native subjects')
                    names = RECEIPT_COLUMNS['comments']
                    if any(exact_json({name: value.get(name) for name in names}) !=
                           exact_json({name: values.get(name) for name in names})
                           for value, values in zip(shaped, held)):
                        raise etl_bulk.NotBulkEligible('Comments processor input differs from selected receipt values')
                    writer.write_table(pa.Table.from_pylist(raw, schema=schema))
                    seen += batch.num_rows
            if seen != expected:
                raise RuntimeError(f'comments: restored {seen} of {expected} native subjects')
            temporary.replace(destination)
    except (etl_bulk.duckdb.Error, pa.ArrowException, ValueError, TypeError, OverflowError) as error:
        raise etl_bulk.NotBulkEligible('Comments batch processing proof requires the row reader') from error
    return seen
