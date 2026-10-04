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


def test_migration_retains_legacy_source(con):
    expected = source(con)
    con.execute('CREATE SCHEMA reg_catalog."default"')
    con.execute('CREATE TABLE reg_catalog."default".comments AS SELECT * FROM source')
    native.ensure_native(con, COMMENT)
    assert con.execute('SELECT count(*) FROM reg_catalog."default".comments').fetchone() == (1,)
    result = native.processing_table(con, COMMENT)
    assert con.execute(f'SELECT * FROM {result}').to_arrow_table().to_pylist() == [expected]


def test_legacy_preview_does_not_create_native_catalog(con):
    source(con)
    con.execute('CREATE SCHEMA reg_catalog."default"')
    con.execute('CREATE TABLE reg_catalog."default".comments AS SELECT * FROM source')
    result = native.processing_table(con, COMMENT)
    assert con.execute(f'SELECT count(*) FROM {result}').fetchone() == (1,)
    assert not native._exists(con, native.qualified(COMMENT))


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


def test_seed_scope_replaces_subjects_and_receipts_together(con, tmp_path):
    source(con)
    iceberg.replace_rows(con, COMMENT, 'source')
    replacement = source(con, identity='c2', text='seeded')
    file = tmp_path / 'seed.parquet'
    pl.DataFrame([replacement], schema=COMMENT.schema).write_parquet(file)
    assert iceberg.seed_comments_from_parquet(con, str(file), COMMENT, 'EPA', replace=True) == 1
    assert con.execute(f'SELECT comment_id FROM {native.qualified(COMMENT)}').fetchall() == [('c2',)]
    assert con.execute(f"SELECT count(*) FROM {native.receipts_table()} WHERE outcome='accepted'").fetchone() == (1,)
    assert con.execute(f'SELECT comment_id FROM {native.processing_table(con, COMMENT)}').fetchall() == [('c2',)]


def test_backfill_keeps_newer_catalog_row_and_only_inserts_missing(con, tmp_path):
    from spicy_regs.schemas import DOCKET
    native.ensure_native(con, DOCKET)
    rows = [{**dict.fromkeys(DOCKET.schema), 'docket_id': 'd1', 'title': 'newer'},
            {**dict.fromkeys(DOCKET.schema), 'docket_id': 'd2', 'title': 'missing'}]
    con.register('docket_source', pl.DataFrame(rows[:1], schema=DOCKET.schema).to_arrow())
    iceberg.replace_rows(con, DOCKET, 'docket_source')
    rows[0]['title'] = 'older'
    path = tmp_path / 'dockets.parquet'
    pl.DataFrame(rows, schema=DOCKET.schema).write_parquet(path)
    assert iceberg.backfill_missing_from_parquet(con, str(path), DOCKET) == (1, 2)
    assert con.execute(f'SELECT title FROM {native.qualified(DOCKET)} ORDER BY docket_id').fetchall() == [('newer',), ('missing',)]
    native.processing_table(con, DOCKET)


def test_dedup_migration_keeps_losing_source_as_rejected_receipt(con):
    source(con)
    con.execute('CREATE SCHEMA reg_catalog."default"')
    con.execute('CREATE TABLE reg_catalog."default".comments AS SELECT * FROM source')
    source(con, text='newest')
    con.execute("UPDATE source SET modify_date='2026-10-04'")
    con.execute('INSERT INTO reg_catalog."default".comments SELECT * FROM source')
    assert iceberg.dedupe_table(con, COMMENT) == (2, 1)
    assert con.execute('SELECT count(*) FROM reg_catalog."default".comments').fetchone() == (2,)
    assert con.execute(f'SELECT outcome,count(*) FROM {native.receipts_table()} GROUP BY 1 ORDER BY 1').fetchall() == [('accepted', 1), ('observed', 1), ('rejected', 1)]
    assert con.execute(f'SELECT text_content FROM {native.qualified(COMMENT)}').fetchone() == ('newest',)


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
