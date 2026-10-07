"""Qualified scalar rows skip redundant groups; arrays retain occurrence counts."""
from datetime import date

import duckdb
import pytest

from spicy_regs.navigation_measurements import MeasurementCache, scalar_navigation
from spicy_regs.sources.publication import table_members
from tests.test_navigation_measurements import member, selection


def typed_selection(tmp_path, kind, *, source_duplicate=False, target_duplicate=False, target_null=False):
    values = {'string': ['  A', 'A', 'missing', 'unused'],
              'integer': [9007199254740993, 9007199254740994, 9007199254740995, 9007199254740996],
              'date': [date(2024, 2, 29), date(2024, 3, 1), date(2024, 3, 2), date(2024, 3, 3)]}[kind]
    source = [{'id': 1, 'fk': values[0]}, {'id': 1 if source_duplicate else 2, 'fk': values[0]},
              {'id': 3, 'fk': values[2]}, {'id': 4, 'fk': None}, {'id': 5, 'fk': values[1]}]
    targets: list[dict] = [{'id': value} for value in (values[0], values[1], values[3])]
    if target_duplicate:
        targets.append({'id': values[0]})
    if target_null:
        targets.append({'id': None})
    src, descriptor = member(tmp_path, 'source', source)
    tgt, target = member(tmp_path, 'target', targets)
    index = {'format': 'spicy-regs-publication', 'version': 2, 'families': {'fixture': {
        'prefix': 'generations/fixture/' + 'a' * 64, 'logicalId': 'urn:fixture',
        'artifactDigest': 'sha256:' + 'a' * 64,
        'tables': {'source.parquet': descriptor, 'target.parquet': target}}}}
    paths = {table_members(index, 'source.parquet')[0].path: str(src),
             table_members(index, 'target.parquet')[0].path: str(tgt)}
    spec = scalar_navigation({'child': 'source', 'child_columns': ['fk'], 'parent': 'target', 'parent_columns': ['id']})
    return index, paths, spec, src, tgt


def sql_oracle(source, target):
    with duckdb.connect() as con:
        con.read_parquet(str(source)).create_view('source')
        con.read_parquet(str(target)).create_view('target')
        con.execute('CREATE VIEW keys AS SELECT id,count(*) n FROM target WHERE id IS NOT NULL GROUP BY id')
        counts = con.execute('''SELECT count(*) FILTER(WHERE s.fk IS NOT NULL),
            count(*) FILTER(WHERE t.n IS NOT NULL),count(*) FILTER(WHERE s.fk IS NOT NULL AND t.n IS NULL),
            count(*) FILTER(WHERE t.n>1),count(DISTINCT s.id) FILTER(WHERE t.n IS NOT NULL),
            count(DISTINCT s.id) FILTER(WHERE s.fk IS NOT NULL),
            count(DISTINCT s.id) FILTER(WHERE s.fk IS NOT NULL AND t.n IS NULL),
            count(DISTINCT s.id) FILTER(WHERE t.n>1) FROM source s LEFT JOIN keys t ON s.fk=t.id''').fetchone()
        reverse = con.execute('''WITH sources AS (SELECT fk,count(*) n,count(DISTINCT id) identities
            FROM source WHERE fk IS NOT NULL GROUP BY fk)
            SELECT coalesce(sum(t.n) FILTER(WHERE s.n IS NOT NULL),0),
            coalesce(sum(t.n) FILTER(WHERE s.n IS NULL),0),coalesce(max(s.n),0),
            coalesce(max(s.identities),0) FROM keys t LEFT JOIN sources s ON t.id=s.fk''').fetchone()
        assert counts is not None and reverse is not None
        return counts, reverse


@pytest.mark.parametrize('kind', ['string', 'integer', 'date'])
@pytest.mark.parametrize('target_state', ['unique', 'duplicate', 'null'])
def test_exact_native_types_match_independent_distinct_sql_oracle(tmp_path, kind, target_state):
    index, paths, spec, source, target = typed_selection(tmp_path, kind,
        target_duplicate=target_state == 'duplicate', target_null=target_state == 'null')
    cache = MeasurementCache(tmp_path / 'cache')
    result = cache.measure(index, paths, spec, source_identity=('id',))
    counts, reverse = sql_oracle(source, target)
    assert tuple(result[key] for key in ('eligible', 'matched', 'missing', 'ambiguous',
        'distinctMatchedSourceRecords', 'distinctEligibleSourceRecords',
        'distinctMissingSourceRecords', 'distinctAmbiguousSourceRecords')) == counts
    assert tuple(result['reverse'][key] for key in ('matchedTargetRows', 'unmatchedTargetRows',
        'maximumReferencesPerTarget', 'maximumDistinctSourceRecordsPerTarget')) == reverse
    assert result['reverse']['maximumPhysicalSourceRowsPerTarget'] == reverse[2]
    assert result['aggregationMethods']['sourceRecords'] == 'unique_native_row'
    assert result['aggregationMethods']['targetKeys'] == ('unique_native_keys' if target_state == 'unique' else 'grouped_keys')
    assert cache.work['unique_row_aggregations'] == 1


def test_duplicate_source_identity_refuses_record_shortcut(tmp_path):
    index, paths, spec, source, target = typed_selection(tmp_path, 'string', source_duplicate=True)
    cache = MeasurementCache(tmp_path / 'cache')
    result = cache.measure(index, paths, spec, source_identity=('id',))
    counts, reverse = sql_oracle(source, target)
    assert tuple(result[key] for key in ('eligible', 'matched', 'missing', 'ambiguous')) == counts[:4]
    assert result['distinctMatchedSourceRecords'] is None
    assert result['reverse']['maximumPhysicalSourceRowsPerTarget'] == reverse[2]
    assert result['aggregationMethods']['sourceRecords'] == 'grouped_occurrences'
    assert not cache.work['unique_row_aggregations']


def test_arrays_with_repeated_references_keep_distinct_and_physical_row_groups(tmp_path):
    index, paths, spec = selection(tmp_path, split=True)
    cache = MeasurementCache(tmp_path / 'cache')
    result = cache.measure(index, paths, spec, source_identity=('id',))
    assert result['repeatedReferences'] == 1
    assert (result['matched'], result['distinctMatchedSourceRecords']) == (3, 2)
    assert result['reverse']['maximumReferencesPerTarget'] == 2
    assert result['reverse']['maximumPhysicalSourceRowsPerTarget'] == 1
    assert result['reverse']['maximumDistinctSourceRecordsPerTarget'] == 1
    assert result['aggregationMethods']['sourceRecords'] == 'grouped_occurrences'
    assert not cache.work['unique_row_aggregations']


def test_lossy_float_type_cannot_borrow_native_uniqueness(tmp_path):
    index, paths, spec, _, _ = typed_selection(tmp_path, 'string')
    source, descriptor = member(tmp_path, 'floats-source', [{'id': 1, 'fk': 1.25}])
    target, target_descriptor = member(tmp_path, 'floats-target', [{'id': 1.25}])
    index['families']['fixture']['tables']['source.parquet'] = descriptor
    index['families']['fixture']['tables']['target.parquet'] = target_descriptor
    paths = {table_members(index, 'source.parquet')[0].path: str(source),
             table_members(index, 'target.parquet')[0].path: str(target)}
    with pytest.raises(ValueError, match='Native scalar key types are incompatible or unsupported'):
        MeasurementCache(tmp_path / 'cache').measure(index, paths, spec, source_identity=('id',))


def test_complete_source_key_skips_reverse_grouping_and_preserves_missing_rows(tmp_path):
    index, paths, _, source, target = typed_selection(tmp_path, 'integer')
    spec = scalar_navigation({'child': 'source', 'child_columns': ['id'], 'parent': 'target', 'parent_columns': ['id']})
    cache = MeasurementCache(tmp_path / 'cache')
    result = cache.measure(index, paths, spec, source_identity=('id',))
    assert result['eligible'] == result['missing'] == 5
    assert result['matched'] == 0
    assert result['distinctMissingSourceRecords'] == 5
    assert result['aggregationMethods']['reverseSourceKeys'] == 'unique_native_keys'
    assert result['reverse']['maximumReferencesPerTarget'] == 0
    assert result['reverse']['maximumDistinctSourceRecordsPerTarget'] == 0
