"""Court preparation separates native domain types from exact original literals."""
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication
from tests.test_native_conversion import BASE, BUCKET, MAIN, STATE, WHEEL, bucket as _bucket, convert, publish_old

bucket = _bucket


def opinions():
    from spicy_regs.court_subjects import LEGACY_COLUMNS
    result = []
    for identity, boolean, count in [('7', 't', '007'), ('2', 'False', None)]:
        row = dict.fromkeys(LEGACY_COLUMNS['court_opinions'])
        row.update(opinion_id=identity, cluster_id='0004', opinion_type='010combined',
                   per_curiam=boolean, page_count=count, author_str=' literal  author ')
        result.append(row)
    return result


def test_original_court_opinions_prepare_and_publish_same_exact_native_artifact(tmp_path, monkeypatch, bucket):
    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': opinions()})
    work = tmp_path / 'prepared'
    prepared = convert('court-opinions', work)
    generation = Path(prepared['generation']['directory'])
    subject = pq.read_table(generation / 'court_opinions.parquet')
    assert subject['page_count'].type == pa.int64() and subject['page_count'].to_pylist() == [7, None]
    assert subject['per_curiam'].type == pa.bool_() and subject['per_curiam'].to_pylist() == [True, False]
    assert prepared['tables']['court_opinions']['type_changes'] == {}
    restored = pq.read_table(work / 'restored/court_opinions.parquet')
    assert restored.to_pylist() == opinions()
    sealed = {p.relative_to(generation): p.read_bytes() for p in generation.rglob('*') if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError('Prepared publication must reuse the qualified court generation')

    monkeypatch.setattr(conversion, '_convert_court', no_writer)
    published = conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinions'],
        expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE))
    assert published['generation'] == prepared['generation']
    assert published['read_back']['anonymous_read_rows'] == {'court_opinions': 2}
    assert sealed == {p.relative_to(generation): p.read_bytes() for p in generation.rglob('*') if p.is_file()}
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)['families']['court-opinions'] == old


@pytest.mark.parametrize('change', ['order', 'value', 'metadata'])
def test_court_preparation_refuses_changed_original_processing_before_publication(tmp_path, monkeypatch, bucket, change):
    from spicy_regs import court_receipts
    publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': opinions()})
    written = list(bucket.writes)
    restore = court_receipts.restore_processing_input

    def changed(*args, **kwargs):
        path = restore(*args, **kwargs)
        table = pq.read_table(path)
        if change == 'order':
            table = table.take([1, 0])
        elif change == 'value':
            index = table.schema.get_field_index('page_count')
            table = table.set_column(index, table.schema.field(index), pa.array(['7', None]))
        else:
            table = table.replace_schema_metadata({b'changed': b'yes'})
        pq.write_table(table, path)
        return path

    monkeypatch.setattr(court_receipts, 'restore_processing_input', changed)
    with pytest.raises(conversion.ConversionRefused, match='Court processing'):
        convert('court-opinions', tmp_path / 'prepared', publish=True)
    assert bucket.writes == written
