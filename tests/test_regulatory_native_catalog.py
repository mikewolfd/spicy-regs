"""Native catalog writes admit subject/receipt pairs with rollback and exact reads."""

import duckdb
import polars as pl
import pytest

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources import regulatory_catalog as native


@pytest.fixture
def con():
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        yield connection


def source(con, *, identity='c1', text='first', attachments='[]'):
    row = {c: None for c in COMMENT.schema}
    row.update(comment_id=identity, docket_id='EPA-1', agency_code='EPA', modify_date='2026-10-03',
               text_content=text, text_extraction_status='complete', attachments_json=attachments)
    con.register('incoming', pl.DataFrame([row], schema=COMMENT.schema).to_arrow())
    con.execute('CREATE OR REPLACE TEMP TABLE source AS SELECT * FROM incoming')
    return row


def test_native_pair_and_processing_read(con):
    expected = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    columns = {r[0] for r in con.execute(f'DESCRIBE {iceberg._qualified(COMMENT)}').fetchall()}
    assert 'attachments' in columns and 'attachments_json' not in columns
    assert 'text_extraction_status' not in columns
    assert con.execute(f"SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1").fetchall() == [('accepted', 1), ('observed', 1)]
    processing = native.processing_table(con, COMMENT)
    actual = con.execute(f'SELECT * FROM {processing}').to_arrow_table().to_pylist()[0]
    assert actual == expected


def test_replacement_retires_prior_receipt_and_checks_stale_prior(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    prior = native.processing_table(con, COMMENT)
    source(con, text='second')
    iceberg.replace_rows(con, COMMENT, 'source', expected_prior=prior)
    assert con.execute(f"SELECT count(*) FROM {native.receipts_table()} WHERE outcome='accepted'").fetchone() == (1,)
    source(con, text='third')
    with pytest.raises(RuntimeError, match='changed after preparation'):
        iceberg.replace_rows(con, COMMENT, 'source', expected_prior=prior)
    assert con.execute(f'SELECT text_content FROM {iceberg._qualified(COMMENT)}').fetchone() == ('second',)


def test_bad_receipt_refuses_processing_read(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    con.execute(f"UPDATE {native.receipts_table()} SET subject_version='wrong'")
    with pytest.raises(ValueError, match='digest'):
        native.processing_table(con, COMMENT)


def test_only_initialized_native_storage_is_read_authority(con):
    source(con)
    con.execute('CREATE SCHEMA reg_catalog."default"')
    con.execute('CREATE TABLE reg_catalog."default".comments AS SELECT * FROM source')
    with pytest.raises(ValueError, match='initialization receipt'):
        native.processing_table(con, COMMENT)
    assert not native._exists(con, native.qualified(COMMENT))
    native.ensure_native(con, COMMENT)
    assert con.execute(f'SELECT count(*) FROM {native.processing_table(con, COMMENT)}').fetchone() == (0,)
    assert con.execute('SELECT count(*) FROM reg_catalog."default".comments').fetchone() == (1,)


def test_receipt_insert_failure_rolls_back_subject_update(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    before = con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall()
    source(con, text='second')

    class Fault:
        def execute(self, sql, *args):
            if sql.startswith(f'INSERT INTO {native.receipts_table()} SELECT * FROM _receipt_replacement_'):
                raise RuntimeError('injected receipt write failure')
            return con.execute(sql, *args)

    with pytest.raises(RuntimeError, match='injected receipt'):
        iceberg.replace_rows(Fault(), COMMENT, 'source')
    assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('first',)
    assert con.execute(f'SELECT * FROM {native.receipts_table()}').fetchall() == before


def test_pair_export_preserves_generation_and_processing(con, tmp_path):
    from spicy_regs.transforms.regulations_receipts import read_internal
    expected = source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    selected = native.export_pair(con, COMMENT, tmp_path, generation_id='export-1')
    assert list(read_internal(selected)) == [expected]
    assert 'export-1' in (tmp_path / 'generation.json').read_text()


def test_explicit_scope_replacement_retains_removed_evidence(con):
    from spicy_regs.etl_receipts import resolve_receipt_witness
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, identity='c2', text='replacement')
    native.replace_native(con, COMMENT, 'source', scope={'agency_code': 'EPA'}, delete_scope=True)
    assert con.execute(f'SELECT comment_id FROM {native.qualified(COMMENT)}').fetchall() == [('c2',)]
    retired = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE attempt_id LIKE '%:retired'").to_arrow_table().to_pylist()
    assert len(retired) == 1 and retired[0]['outcome'] == 'observed'
    references = [w for w in retired[0]['witnesses'] if (w['source_uri'] or '').startswith('receipt-processing:')]
    assert references and all(resolve_receipt_witness(retired[0], w) for w in references)
    native.processing_table(con, COMMENT)


def test_prepared_storage_is_not_a_clean_selected_catalog(con):
    con.execute(f'CREATE SCHEMA reg_catalog."{native.namespace()}"')
    con.execute(f'CREATE TABLE {native.qualified(COMMENT)} ({native._ddl(native.policy("comments").subject_schema)})')
    with pytest.raises(ValueError, match='initialization receipt'):
        iceberg.audit_duplicates(con, COMMENT)
    native.ensure_native(con, COMMENT)
    assert iceberg.audit_duplicates(con, COMMENT) == []


def test_failed_conversion_keeps_refusal_and_prior_subject(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, attachments='not-json')
    with pytest.raises(ValueError, match='conversion refused'):
        iceberg.replace_rows(con, COMMENT, 'source')
    assert con.execute(f'SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1').fetchall() == [('accepted', 1), ('observed', 1), ('refused', 1)]
    assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('first',)
    native.processing_table(con, COMMENT)


def test_merge_keeps_unselected_observations_without_full_prior_scan(con, tmp_path):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    other = source(con, identity='other-agency')
    other['agency_code'] = 'FAA'
    con.register('other', pl.DataFrame([other | {'comment_id': f'other-{i}'} for i in range(257)], schema=COMMENT.schema).to_arrow())
    iceberg.replace_rows(con, COMMENT, 'other')
    older = source(con, text='older')
    older['modify_date'] = '2020-01-01'
    path = tmp_path / 'staged.parquet'
    pl.DataFrame([older, older], schema=COMMENT.schema).write_parquet(path)
    reads = []
    copied_subject_counts = []
    class Observe:
        def execute(self, sql, *args):
            reads.append(sql)
            result = con.execute(sql, *args)
            if sql.startswith(f'COPY (SELECT * FROM {native.qualified(COMMENT)}'):
                import re
                import pyarrow.parquet as pq
                destination = re.search(r" TO '([^']+)'", sql)
                assert destination is not None
                copied_subject_counts.append(pq.ParquetFile(destination.group(1)).metadata.num_rows)
            return result
    assert iceberg._merge(Observe(), [path], COMMENT) == 0
    assert con.execute(f'SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1').fetchall() == [('accepted', 258), ('observed', 1), ('rejected', 2)]
    payload_reads = [sql for sql in reads if f'SELECT * FROM {native.qualified(COMMENT)}' in sql and 'COPY' in sql]
    assert payload_reads and all('WHERE' in sql for sql in payload_reads)
    assert max(copied_subject_counts) == 1  # 257 unrelated agency rows stay unread.
    assert not any('SELECT witnesses' in sql for sql in reads)  # no per-row catalog lookups


def test_unpaired_subject_delete_refuses_whole_dataset_read(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    con.execute(f'DELETE FROM {native.qualified(COMMENT)}')
    with pytest.raises(ValueError, match='Accepted receipt has no matching subject'):
        native.processing_table(con, COMMENT)


def test_replaced_and_exported_witnesses_resolve_exact_retained_payloads(con, tmp_path):
    from hashlib import sha256
    import pyarrow.parquet as pq
    from spicy_regs.etl_receipts import resolve_receipt_witness
    for text in ('first', 'second', 'third'):
        source(con, text=text)
        iceberg.replace_rows(con, COMMENT, 'source')
    selected = native.export_pair(con, COMMENT, tmp_path, generation_id='exported')
    receipt = next(r for r in pq.read_table(selected.receipts).to_pylist() if r['outcome'] == 'accepted')
    assert len(receipt['witnesses']) == 1
    from spicy_regs.etl_receipts import decode_exact_json
    diagnostic = decode_exact_json(receipt['diagnostic_json'])
    assert set(diagnostic['prior_receipt']) == {'receipt_id', 'generation_id', 'processing_sha256'}
    assert 'prior_receipts' not in diagnostic and 'retained_processing' not in diagnostic
    for witness in receipt['witnesses']:
        payload = resolve_receipt_witness(receipt, witness)
        assert sha256(payload).hexdigest() == witness['sha256'].removeprefix('sha256:')


def test_refusal_persistence_failure_preserves_recoverable_spool(con):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, attachments='not-json')
    class Unavailable:
        def execute(self, sql, *args):
            if sql.startswith(f'INSERT INTO {native.receipts_table()} SELECT * FROM read_parquet'):
                raise RuntimeError('catalog unavailable while retaining refusal')
            return con.execute(sql, *args)
    with pytest.raises(native.CatalogConversionRefused) as refusal:
        iceberg.replace_rows(Unavailable(), COMMENT, 'source')
    path = refusal.value.receipt_path
    try:
        import pyarrow.parquet as pq
        assert path.is_file()
        assert any(r['outcome'] == 'refused' for r in pq.read_table(path).to_pylist())
        assert str(path) in ' '.join(refusal.value.__notes__)
        assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('first',)
        native.retain_refusal(con, refusal.value)
        assert not path.exists()
    finally:
        path.unlink(missing_ok=True)


def test_historical_refusal_and_stale_attempt_do_not_poison_later_export(con, tmp_path):
    from spicy_regs.pipelines.regulatory_publication import finish_dataset
    row = source(con, text='current')
    iceberg.replace_rows(con, COMMENT, 'source')
    source(con, attachments='not-json')
    with pytest.raises(native.CatalogConversionRefused):
        iceberg.replace_rows(con, COMMENT, 'source')
    # A later valid write remains selectable while the failed attempt is retained.
    row = source(con, text='recovered')
    iceberg.replace_rows(con, COMMENT, 'source')
    staged = tmp_path / 'stale.parquet'
    pl.DataFrame([{**row, 'modify_date': '2020-01-01'}], schema=COMMENT.schema).write_parquet(staged)
    assert iceberg._merge(con, [staged], COMMENT) == 0
    source_path = iceberg._export_parquet(con, COMMENT, tmp_path / 'export')
    result = finish_dataset(tmp_path, 'comments', source_path, publish=False)
    assert pl.read_parquet(result)['text_content'].to_list() == ['recovered']


def test_resealed_catalog_receipt_cannot_restore_inconsistent_source_input(con):
    import json
    from hashlib import sha256
    import pyarrow as pa
    from spicy_regs.etl_receipts import exact_json

    source(con, text='selected')
    iceberg.replace_rows(con, COMMENT, 'source')
    [receipt] = con.execute(f"SELECT * FROM {native.receipts_table()} WHERE outcome='accepted'").to_arrow_table().to_pylist()
    from spicy_regs.etl_receipts import _unpack
    processing = _unpack(json.loads(receipt['processing_json']))
    processing['raw_conversion_inputs'].pop('text_content')
    receipt['processing_json'] = exact_json(processing)
    receipt['receipt_id'] = 'sha256:' + sha256(exact_json({k: v for k, v in receipt.items() if k != 'receipt_id'}).encode()).hexdigest()
    con.register('altered_receipt', pa.Table.from_pylist([receipt], schema=native.RECEIPT_SCHEMA))
    con.execute(f"DELETE FROM {native.receipts_table()} WHERE outcome='accepted'")
    con.execute(f'INSERT INTO {native.receipts_table()} SELECT * FROM altered_receipt')
    with pytest.raises(ValueError, match='retained processor input differs'):
        native.processing_table(con, COMMENT)
