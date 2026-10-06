"""Private batch adapters for the original dockets and documents inputs.

Shared parsers and row attempts decide unproven values. SQL handles declared
strings and exact receipt hashing. Completed pairs undergo public admission
before becoming visible; restoration proves every retained processor input.
"""
from collections.abc import Mapping
from dataclasses import replace
from hashlib import sha256
import json
from pathlib import Path
from typing import Any
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA, WITNESS_TYPE, ReceiptContext,
    decode_exact_json, exact_json, observation_receipt, select_receipts, validate_receipt_bundle,
)
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.regulations_shape import NATIVE_FIELDS, RECEIPT_COLUMNS, SOURCE_COLUMNS, TYPES, _native, _validate

def _text(value: Any) -> str:
    return "CAST(NULL AS VARCHAR)" if value is None else "'" + str(value).replace("'", "''") + "'"


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


_BATCH = 1000
_PARSED = pa.struct([('native', pa.string()), ('reference', pa.bool_())])
DATASETS = frozenset({'dockets', 'documents'})


def eligible(dataset, schema):
    known = dict(SOURCE_COLUMNS.get(dataset, ()))
    return dataset in DATASETS and len(set(schema.names)) == len(schema.names) and all(
        field.name in known and pa.types.is_string(field.type) for field in schema)


def _parser(dataset, name, dtype):
    def parse(values):
        result = []
        for value in values.to_pylist():
            try:
                native = _native(dataset, name, value)
                _validate(native, dtype, f'{dataset}.{name}')
                stored = pa.array([native], type=dtype).to_pylist()[0]
                result.append({'native': None if stored is None else json.dumps(stored, ensure_ascii=False, separators=(',', ':')),
                               'reference': False})
            except (ValueError, TypeError, OverflowError, pa.ArrowException):
                result.append({'native': None, 'reference': True})
        return pa.array(result, type=_PARSED)
    return parse


def write_held_dataset(dataset, source, destination, *, generation_id, processor, witnesses,
                       include_source_witness, prior_receipts):
    from spicy_regs.transforms.regulations_receipts import map_regulations_attempt, policy
    from rulespec_artifacts import publish_directory_no_replace

    if len(prior_receipts) > 1:
        raise etl_bulk.NotBulkEligible("Ordered multiple regulatory prior bundles require the original row writer")
    schema = pq.read_schema(source)
    if not eligible(dataset, schema):
        raise etl_bulk.NotBulkEligible('Regulatory base bulk requires declared string input columns')
    declared = policy(dataset)
    metadata = {key.decode('utf-8'): value for key, value in (schema.metadata or {}).items()}
    label = f'retained:{dataset}'
    def own_witness(raw, field):
        return {'source_id': label, 'source_uri': None, 'sha256': sha256(exact_json(raw).encode()).hexdigest(),
                'locator': f'receipt.values.{field} (canonical exact_json)', 'body_version': None}
    observed = ReceiptContext(generation_id, f'{dataset}:input-file', processor,
                              ([own_witness(metadata, 'input_metadata')] if include_source_witness else []) + list(witnesses),
                              {'kind': 'input_file_metadata', 'rows': pq.ParquetFile(source).metadata.num_rows})
    def attempt(raw, ordinal):
        context = ReceiptContext(generation_id, f'{dataset}:row:{ordinal}', processor,
                                 ([own_witness(raw, 'raw_conversion_inputs')] if include_source_witness else []) + list(witnesses),
                                 {} if include_source_witness else {'output_row_ordinal': ordinal})
        return map_regulations_attempt(dataset, raw, context, input_metadata=metadata)

    raw = etl_bulk.record_json_sql((field.name, field.type) for field in schema)
    source_columns = ', '.join((f'"{name}"' if name in schema.names else 'NULL::VARCHAR') + f' AS "{name}"'
                               for name, _ in SOURCE_COLUMNS[dataset])
    processing = [(name, etl_bulk.exact_json_sql(f'"{name}"', pa.string()))
                  for name in RECEIPT_COLUMNS[dataset] if name in schema.names]
    processing.extend([('raw_conversion_inputs', '_raw'), ('input_metadata', _text(exact_json(metadata)))])
    payload = "'[\"dict\",[' || " + " || ',' || ".join(
        _text('[' + json.dumps(name) + ',') + ' || ' + value + " || ']'" for name, value in sorted(processing)
    ) + " || ']]'"
    fields, parsed, native, flags = [], [], [], []
    sql_types = dict(described_schema(declared.subject_schema))
    for field in declared.subject_schema:
        name = field.name
        fields.append(f'"{name}"')
        mapping = next(((source_name, dtype) for source_name, (target, dtype) in NATIVE_FIELDS.get(dataset, {}).items()
                        if target == name), None)
        if mapping is not None:
            source_name, dtype = mapping
            parsed.append((source_name, dtype))
            native.append(f'CAST(CAST(_parsed_{source_name}.native AS JSON) AS {sql_types[name]}) AS "{name}"')
            flags.append(f'_parsed_{source_name}.reference')
        elif name == 'withdrawn':
            native.append('CASE WHEN withdrawn IN (\'true\',\'True\') THEN true WHEN withdrawn IN (\'false\',\'False\') THEN false END AS withdrawn')
            flags.append("(withdrawn IS NOT NULL AND withdrawn NOT IN ('true','True','false','False'))")
        else:
            native.append(f'"{name}"')
    key = declared.identity_fields[0]
    flags.append(f'"{key}" IS NULL')
    own = "[{'source_id': " + _text(label) + ", 'source_uri': NULL::VARCHAR, 'sha256': sha256(_raw), " + \
          "'locator': 'receipt.values.raw_conversion_inputs (canonical exact_json)', 'body_version': NULL::VARCHAR}]"
    witness_sql = f'list_concat({own}, _extra)' if include_source_witness else '_extra'
    diagnostics = "'[\"dict\",[]]'" if include_source_witness else "'[\"dict\",[[\"output_row_ordinal\",[\"int\",' || _ordinal || ']]]]'"
    record_id, version, identity = etl_bulk.identity_sql(declared)
    subject_json = etl_bulk.record_json_sql((field.name, field.type) for field in declared.subject_schema)
    parse_sql = ', '.join(f'parse_{name}("{name}") AS _parsed_{name}' for name, _ in parsed)
    reference = ' OR '.join(flags)
    # Keep raw evidence and scalar processing fields separately from native fields.
    retained = ', '.join(f'"{name}"' for name in RECEIPT_COLUMNS[dataset])
    native_select = ', '.join(native + ( [retained] if retained else []))
    query = f'''
    WITH original AS (SELECT *, file_row_number AS _ordinal, {raw} AS _raw
        FROM read_parquet({_text(str(Path(source).resolve()))}, file_row_number=true)),
    input AS (SELECT {source_columns}, _ordinal, _raw FROM original),
    parsed AS (SELECT * {', ' + parse_sql if parse_sql else ''} FROM input),
    native AS (SELECT {native_select}, _ordinal, _raw,
        ({reference}) AS _reference FROM parsed),
    receipts AS (SELECT {', '.join(fields)}, _ordinal, _raw,
        (_reference OR {etl_bulk.needs_reference_sql('_raw')} OR {etl_bulk.needs_reference_sql(subject_json)}) AS _reference,
        {record_id} AS record_id, {version} AS subject_version, {identity} AS identity_json,
        {payload} AS processing_json, {_text(dataset)} AS dataset,
        {_text(declared.policy_version)} AS policy_version, {_text(generation_id)} AS generation_id,
        {_text(dataset + ':row:')} || _ordinal AS attempt_id, 'accepted' AS outcome,
        {_text(processor)} AS processor, {witness_sql} AS witnesses, {diagnostics} AS diagnostic_json
        FROM native CROSS JOIN held_constants)
    SELECT * EXCLUDE (_reference), (_reference OR {etl_bulk.needs_reference_sql(etl_bulk.receipt_json_sql())}) AS _reference,
        {etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} AS receipt_id FROM receipts'''
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix='.regulatory-base-', dir=destination.parent) as temporary:
        stage = Path(temporary)/'bundle'
        stage.mkdir()
        subject_path, receipt_path = stage/f'{dataset}.parquet', stage/'etl_receipts.parquet'
        expected, seen = pq.ParquetFile(source).metadata.num_rows, 0
        try:
            with etl_bulk.bulk_connection() as (con, work):
                con.register('held_constants', pa.table({'_extra': pa.array([list(witnesses)], type=pa.list_(WITNESS_TYPE))}))
                for name, dtype in parsed:
                    con.create_function(f'parse_{name}', _parser(dataset, name, dtype), ['VARCHAR'],
                                        'STRUCT(native VARCHAR, reference BOOLEAN)', type='arrow', null_handling='special')
                refusals = work/'refused.parquet'
                with pq.ParquetWriter(subject_path, declared.subject_schema, compression='zstd') as sw, \
                        pq.ParquetWriter(receipt_path, RECEIPT_SCHEMA, compression='zstd') as rw, \
                        pq.ParquetWriter(refusals, RECEIPT_SCHEMA, compression='zstd') as fw:
                    for batch in con.execute(query).to_arrow_reader(_BATCH):
                        table = pa.Table.from_batches([batch])
                        table.validate(full=True)
                        ordinals = table['_ordinal'].to_pylist()
                        if ordinals != list(range(seen, seen+batch.num_rows)):
                            raise RuntimeError(f'{dataset}: source order changed')
                        seen += batch.num_rows
                        flags = table['_reference'].to_pylist()
                        mask = [not flag for flag in flags]
                        kept_subjects = table.filter(pa.array(mask)).select(declared.subject_schema.names).cast(declared.subject_schema)
                        kept_receipts = table.filter(pa.array(mask)).select(RECEIPT_SCHEMA.names).cast(RECEIPT_SCHEMA)
                        added_subjects, added_receipts, refused = {}, {}, []
                        if any(flags):
                            from spicy_regs.etl_receipts import _unpack
                            texts = table['_raw'].to_pylist()
                            for index, flag in enumerate(flags):
                                if flag:
                                    subject, receipt = attempt(_unpack(json.loads(texts[index])), ordinals[index])
                                    added_subjects[index] = subject
                                    if receipt['outcome'] == 'accepted':
                                        added_receipts[index] = receipt
                                    else:
                                        refused.append(receipt)
                        if refused:
                            fw.write_table(pa.Table.from_pylist(refused, schema=RECEIPT_SCHEMA))
                        sw.write_table(_with_insertions(kept_subjects, mask, added_subjects))
                        rw.write_table(_with_insertions(kept_receipts, mask, added_receipts))
                    rw.write_table(pa.Table.from_pylist([observation_receipt(declared, observed, processing_fields={'input_metadata': metadata})], schema=RECEIPT_SCHEMA))
                    fw.close()
                    for batch in pq.ParquetFile(refusals).iter_batches(batch_size=_BATCH):
                        rw.write_batch(batch)
        except (etl_bulk.duckdb.Error, pa.ArrowException) as error:
            raise etl_bulk.NotBulkEligible('Regulatory base query requires the original row writer') from error
        if seen != expected:
            raise RuntimeError(f'{dataset}: source population changed')
        if prior_receipts:
            from spicy_regs.current_receipt_history import inherit_current_receipts
            try:
                inherited = inherit_current_receipts(
                    receipt_path, prior_receipts, stage / "inherited.parquet",
                    inherit_observations=False, require_unique_prior=True,
                )
            except (ValueError, TypeError, OverflowError, pa.ArrowException) as error:
                raise etl_bulk.NotBulkEligible("Regulatory prior inheritance requires the original row writer") from error
            inherited.replace(receipt_path)
        validate_receipt_bundle({dataset: [subject_path]}, [receipt_path], [declared], generation_id=generation_id)
        publish_directory_no_replace(stage, destination)
    return destination/subject_path.name, destination/receipt_path.name


def materialize_internal(selected, destination, *, source_schema=None):
    if (selected.dataset not in DATASETS and selected.dataset != 'federal_register') or len(selected.subjects) != 1:
        raise etl_bulk.NotBulkEligible('Regulatory base restore requires one selected member')
    with TemporaryDirectory(prefix='regulatory-base-selected-') as temporary:
        scoped = select_receipts(selected.receipts, Path(temporary)/'receipts.parquet', dataset=selected.dataset)
        return _materialize_selected(replace(selected, receipts=scoped), destination, source_schema=source_schema)


def _materialize_selected(selected, destination, *, source_schema=None):
    from spicy_regs.transforms.regulations_receipts import policy
    from spicy_regs.transforms.regulations_shape import shape_record

    declared = policy(selected.dataset)
    subject, receipts = selected.subjects[0], selected.receipts
    validate_receipt_bundle({selected.dataset: [subject]}, [receipts], [declared], generation_id=selected.generation_id)
    expected = pq.ParquetFile(subject).metadata.num_rows
    if not expected:
        raise etl_bulk.NotBulkEligible('Empty observed input metadata stays with the row reader')
    key = declared.identity_fields[0]
    join = f'n."{key}"=json_extract_string(r.identity_json, \'$[1][0][1][1][1]\')'
    if len(declared.identity_fields) > 1:
        # Compare each typed identity value, including nulls, to its exact
        # retained encoding. A printed identifier may recur in another edition.
        join = ' AND '.join(
            etl_bulk.exact_json_sql('n."' + name.replace('"', '""') + '"', declared.subject_schema.field(name).type)
            + f"=CAST(json_extract(r.identity_json, '$[1][{index}][1][1]') AS VARCHAR)"
            for index, name in enumerate(declared.identity_fields)
        )
    query = f'''SELECT n.* EXCLUDE(file_row_number), n.file_row_number AS _ordinal, r.processing_json AS _processing
        FROM read_parquet({_text(subject)}, file_row_number=true) n
        JOIN read_parquet({_text(receipts)}) r ON {join}
        WHERE r.dataset={_text(selected.dataset)} AND r.outcome='accepted' ORDER BY n.file_row_number'''
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    schema = (source_schema.remove_metadata() if source_schema is not None else
              pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[selected.dataset]]))
    metadata, seen = None, 0
    try:
        with etl_bulk.bulk_connection() as (con, _), TemporaryDirectory(prefix='.regulatory-restore-', dir=destination.parent) as temporary:
            output = Path(temporary)/'processing.parquet'
            writer = None
            try:
                for batch in con.execute(query).to_arrow_reader(_BATCH):
                    table = pa.Table.from_batches([batch])
                    table.validate(full=True)
                    if table['_ordinal'].to_pylist() != list(range(seen, seen+batch.num_rows)):
                        raise etl_bulk.NotBulkEligible('Regulatory base restored membership or order differs')
                    held = [decode_exact_json(value) for value in table['_processing'].to_pylist()]
                    if metadata is None:
                        metadata = held[0].get('input_metadata', {})
                        writer = pq.ParquetWriter(output, schema.with_metadata(metadata) if metadata else schema, compression='zstd')
                    if any(values.get('input_metadata', {}) != metadata for values in held):
                        raise etl_bulk.NotBulkEligible('Regulatory base input metadata differs across receipts')
                    raw = [values.get('raw_conversion_inputs') for values in held]
                    if any(not isinstance(value, Mapping) for value in raw):
                        raise etl_bulk.NotBulkEligible('Regulatory base exact processor input is required')
                    if source_schema is not None and any(set(value) != set(source_schema.names) for value in raw):
                        raise etl_bulk.NotBulkEligible("Exact regulatory source field presence differs from retained input")
                    shaped = [shape_record(selected.dataset, value) for value in raw]
                    reproduced = pa.Table.from_pylist(shaped, schema=declared.subject_schema)
                    if not reproduced.equals(table.select(declared.subject_schema.names).cast(declared.subject_schema)):
                        raise etl_bulk.NotBulkEligible('Regulatory base retained processor input differs from subject')
                    names = RECEIPT_COLUMNS[selected.dataset]
                    if any(exact_json({name: row.get(name) for name in names}) != exact_json({name: values.get(name) for name in names})
                           for row, values in zip(shaped, held)):
                        raise etl_bulk.NotBulkEligible('Regulatory base retained processing evidence differs')
                    assert writer is not None
                    writer.write_table(pa.Table.from_pylist(raw, schema=schema))
                    seen += batch.num_rows
            finally:
                if writer is not None:
                    writer.close()
            if seen != expected:
                raise etl_bulk.NotBulkEligible('Regulatory base restored population differs')
            output.replace(destination)
    except (etl_bulk.duckdb.Error, pa.ArrowException, ValueError, TypeError, OverflowError, RecursionError) as error:
        raise etl_bulk.NotBulkEligible('Regulatory base processing proof requires the original row reader') from error
    return destination
