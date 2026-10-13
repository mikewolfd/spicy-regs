"""The complete family builder publishes resolver decisions and restores originals."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.fec_native_conversion import build_retained_family, filing_reference_subjects, staged_snapshot
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
from spicy_regs.transforms.fec_identity_context_fields import normalize_record
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, dataset_policy
from spicy_regs.transforms.fec_identity_observations import filing_key
from tests.test_fec_native_conversion import old_family, fec_bucket
from tests.test_fec_filing_associations import filing, pin
from tests.test_fec_identity_receipts import WITNESS


@pytest.fixture(name='bucket')
def local_bucket(monkeypatch):
    factory = getattr(fec_bucket, "__wrapped__", None)
    if not callable(factory):
        raise RuntimeError("The shared publication fixture has no callable factory")
    return factory(monkeypatch)


def metadata(record, number, **changes):
    return {**filing(pin(record), number), 'filing_link_status': 'native_file_number',
            'collection_id': 'filings', 'source_record_id': record,
            'source_sha256': WITNESS['sha256'], 'source_locator_json': '{"pointer":"/' + record + '"}',
            **changes}


def reference(label, target, *, source='source', source_key='11'):
    return dict(record_id=pin(label), filing_key=filing_key(source_key), source_filing_record_id=pin(source),
                target_filing_key=filing_key(target) if target is not None else None,
                native_target_file_number=target, relation_type='amendment_chain_member',
                identity_version='fec-typed-observation/1', mapping_version='fec-identity-observations/2',
                source_authority='official-fec', source_namespace='fec-openfec-file-number',
                filing_link_status='native_file_number', target_resolution_status='native_reference_unresolved',
                source_sha256=WITNESS['sha256'], collection_id='filings', source_record_id=source,
                source_locator_json='{"array_index":0}', source_pointer='/amendment_chain/0',
                reference_status='reported_reference')


@pytest.mark.parametrize('duplicate_identity', [False, True])
def test_actual_complete_query_builder_publishes_statuses_and_preserves_order_duplicates_and_originals(tmp_path, monkeypatch, bucket, duplicate_identity):
    filings = [metadata('source', '11'), metadata('target-a', '22'), metadata('target-b', '22'),
               metadata('conflict-a', '33'), metadata('conflict-b', '33', native_filer_id='C00000002')]
    if duplicate_identity:
        filings.append(metadata('source', '11'))
    links = [reference('good', '22'), reference('repeated-reference', '22'), reference('conflict', '33'),
             reference('missing', '99'), reference('no-number', None),
             reference('absent-source', '22', source='absent'),
             reference('source-conflict', '22', source_key='99')]
    old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations')
    snapshot, retained = old_family(tmp_path / 'query', monkeypatch, bucket, 'fec-query',
                                   rows_by_table={'fec_filings': filings, 'fec_filing_links': links})
    original_filings = pq.ParquetFile(retained['fec_filings.parquet']).read().to_pylist()
    native_path = tmp_path / 'admitted-native-filings.parquet'
    pq.write_table(pa.Table.from_pylist([normalize_record('fec_filings', row) for row in original_filings],
                                      schema=dataset_policy('fec_filings').subject_schema), native_path)
    adopted = []
    emit = IdentityReceiptWriter.emit

    def actual_emit(self, table, row, **kwargs):
        if table == 'fec_filing_links':
            assert kwargs['native_link_subject'] is not None
            adopted.append(kwargs['native_link_subject']['target_resolution_status'])
        return emit(self, table, row, **kwargs)
    monkeypatch.setattr(IdentityReceiptWriter, 'emit', actual_emit)
    with ConversionReadOperation() as operation:
        source = build_retained_family('fec-observations', snapshot['families']['fec-observations'], snapshot,
                                      {key: tmp_path / 'source' / 'original' / key
                                       for key in snapshot['families']['fec-observations']['tables']},
                                      tmp_path / 'native-source', operation=operation)
        staged = staged_snapshot(snapshot, source, operation)
        selection = FilingAssociationInputs(staged['families']['fec-observations']['artifactDigest'], None,
                                           [TypedInput(native_path, _sha(native_path), len(filings))], {},
                                           filing_generation_scope='self')
        if duplicate_identity:
            with pytest.raises(ValueError, match='Duplicate or ambiguous receipt join'):
                build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
                                      tmp_path / 'native-query', operation=operation, read_snapshot=staged,
                                      filing_associations=selection, observation_generation=source)
            assert adopted[0] == 'unresolved_source_observation_ambiguous'
            assert not (tmp_path / 'native-query/build/generation/artifact.json').exists()
            return
        result = build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
                                      tmp_path / 'native-query', operation=operation, read_snapshot=staged,
                                      filing_associations=selection, observation_generation=source)
        _, reader, _ = operation.admit_generation(result)
        rows = list(reader.read_subjects('fec_filing_links'))
        expected = ['resolved_native_filing_key', 'resolved_native_filing_key',
                    'unresolved_target_identity_conflict',
                    'unresolved_target_not_retained', 'unresolved_no_file_number',
                    'unresolved_source_observation_absent', 'unresolved_source_identity_mismatch']
        assert adopted == expected == [row['target_resolution_status'] for row in rows]
        assert [row['record_id'] for row in rows] == [row['record_id'] for row in links]
        assert rows[0]['target_filing_key'] == rows[1]['target_filing_key']  # Repeated references survive.
        assert rows[0]['record_id'] != rows[1]['record_id']  # Their source occurrence IDs remain distinct.
        original = pq.ParquetFile(retained['fec_filing_links.parquet']).read().to_pylist()
        columns = pq.read_schema(retained['fec_filing_links.parquet']).names
        restored = [{name: row['source_input']['fec_retained_overrides'].get(
                     name, row['conversion_inputs'].get(name, row.get(name))) for name in columns} for row in rows]
        assert restored == original  # Full values and order, not just set equality.
        compared = reader.compare_original('fec_filing_links', retained['fec_filing_links.parquet'])
        assert compared['rows_only_in_retained'] == compared['rows_only_in_restored'] == 0


def test_external_filing_target_is_not_implicitly_admitted(tmp_path):
    with pytest.raises(ValueError, match='complete admitted self-scoped'):
        with filing_reference_subjects([], [], filing_generation_scope='external', check_resources=lambda: None):
            pytest.fail('External selection must refuse before a reader is created')


def test_native_link_adoption_refuses_partial_or_changed_source_fields(tmp_path):
    row = reference('source', '22')
    policy = dataset_policy('fec_filing_links')
    mapped = normalize_record('fec_filing_links', row)
    native = {name: mapped[name] for name in policy.subject_schema.names}
    with IdentityReceiptWriter(tmp_path / 'writer', generation_id='test', tables=['fec_filing_links']) as writer:
        with pytest.raises(ValueError, match='complete declared native subject'):
            writer.emit('fec_filing_links', row, input_witness=WITNESS,
                        native_link_subject={'target_resolution_status': 'resolved_native_filing_key'})
        with pytest.raises(ValueError, match='changed ordered source fields'):
            writer.emit('fec_filing_links', row, input_witness=WITNESS,
                        native_link_subject={**native, 'source_filing_record_id': pin('foreign')})


def test_resolver_keeps_explicit_member_order_and_reports_duplicate_target_observations(tmp_path):
    original = [reference('member-first', '22'), reference('member-second', '22')]
    paths = [tmp_path / 'z-first.parquet', tmp_path / 'a-second.parquet']
    selected = []
    for path, row in zip(paths, original, strict=True):
        pq.write_table(pa.Table.from_pylist([row], schema=dataset_policy('fec_filing_links').subject_schema), path)
        selected.append(TypedInput(path, _sha(path), 1))
    # A duplicate physical target is a refusal decision, not an arbitrary pick.
    targets = [metadata('source', '11'), metadata('target', '22'), metadata('target', '22')]
    checks = []
    with filing_reference_subjects(selected, targets, filing_generation_scope='self',
                                   check_resources=lambda: checks.append(True)) as stream:
        resolved = list(stream)
    assert [row['record_id'] for row in resolved] == [row['record_id'] for row in original]
    assert [row['target_resolution_status'] for row in resolved] == ['unresolved_target_observation_ambiguous'] * 2
    assert len(checks) >= 5


def test_resource_guard_stops_resolver_before_python_materialization(tmp_path):
    path = tmp_path / 'links.parquet'
    pq.write_table(pa.Table.from_pylist([reference('first', '22')],
                                      schema=dataset_policy('fec_filing_links').subject_schema), path)
    count = 0

    def check():
        nonlocal count
        count += 1
        if count == 4:
            raise TimeoutError('bounded resolution stop')
    with pytest.raises(TimeoutError, match='bounded resolution stop'):
        with filing_reference_subjects([TypedInput(path, _sha(path), 1)],
                                       [metadata('source', '11'), metadata('target', '22')],
                                       filing_generation_scope='self', check_resources=check) as stream:
            list(stream)
    assert count == 4


def test_selected_link_bytes_cannot_change_before_resolution_closes(tmp_path):
    path = tmp_path / 'links.parquet'
    row = reference('first', '22')
    schema = dataset_policy('fec_filing_links').subject_schema
    pq.write_table(pa.Table.from_pylist([row], schema=schema), path)
    selected = [TypedInput(path, _sha(path), 1)]
    with filing_reference_subjects(selected, [metadata('source', '11'), metadata('target', '22')],
                                   filing_generation_scope='self', check_resources=lambda: None) as stream:
        assert next(stream)['target_resolution_status'] == 'resolved_native_filing_key'
        pq.write_table(pa.Table.from_pylist([{**row, 'target_resolution_status': 'changed-source'}], schema=schema), path)
        with pytest.raises(ValueError, match='changed during resolution'):
            next(stream)
