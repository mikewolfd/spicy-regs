"""Court preparation separates native domain types from exact original literals."""
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_regs import native_conversion as conversion
from spicy_regs.sources import publication
from tests.test_native_conversion import BASE, BUCKET, MAIN, STATE, WHEEL, bucket as _bucket, convert, publish_old

bucket = _bucket


def observed_processing(monkeypatch):
    """Observe exact literals and schema consumed by the real comparison stream."""
    from spicy_regs import court_receipts

    calls, processing = [], court_receipts.processing_rows

    def observed(*args, **kwargs):
        rows = list(processing(*args, **kwargs))
        calls.append((kwargs['schema'], rows))
        yield from rows

    monkeypatch.setattr(court_receipts, 'processing_rows', observed)
    return calls


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
    compared = observed_processing(monkeypatch)
    prepared = convert('court-opinions', work)
    generation = Path(prepared['generation']['directory'])
    subject = pq.read_table(generation / 'court_opinions.parquet')
    assert subject['page_count'].type == pa.int64() and subject['page_count'].to_pylist() == [7, None]
    assert subject['per_curiam'].type == pa.bool_() and subject['per_curiam'].to_pylist() == [True, False]
    assert prepared['tables']['court_opinions']['type_changes'] == {}
    assert compared and all(rows == opinions() for _, rows in compared)
    assert all(schema.equals(pq.read_schema(work / 'retained/court_opinions.parquet'),
                             check_metadata=True) for schema, _ in compared)
    sealed = {p.relative_to(generation): p.read_bytes() for p in generation.rglob('*') if p.is_file()}

    def no_writer(*args, **kwargs):
        raise AssertionError('Prepared publication must reuse the qualified court generation')

    monkeypatch.setattr(conversion, '_convert_court', no_writer)
    published = conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinions'],
        expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE))
    assert published['generation'] == prepared['generation']
    assert published['read_back']['anonymous_read_rows'] == {'court_opinions': 2}
    assert len(compared) >= 3  # Preparation, fresh publication proof, and actual anonymous bytes.
    assert all(rows == opinions() for _, rows in compared)
    assert sealed == {p.relative_to(generation): p.read_bytes() for p in generation.rglob('*') if p.is_file()}
    conversion.rollback(work / conversion.RECEIPT, expect_bucket=BUCKET)
    assert publication.current_index(BASE)['families']['court-opinions'] == old


@pytest.mark.parametrize('change', ['order', 'value', 'metadata'])
def test_court_preparation_refuses_changed_original_processing_before_publication(tmp_path, monkeypatch, bucket, change):
    from spicy_regs import court_receipts
    publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': opinions()})
    written = list(bucket.writes)
    intercepted = []
    if change == 'metadata':
        from spicy_regs.conversion_reads import _FamilyReader
        compare = _FamilyReader.compare_original

        def changed_metadata(reader, dataset, original, **kwargs):
            intercepted.append(change)
            # Change the actual retained Arrow metadata at the comparison boundary.
            # The real streamed comparator must refuse it before publication.
            table = pq.read_table(original).replace_schema_metadata({b'changed': b'yes'})
            pq.write_table(table, original)
            return compare(reader, dataset, original, **kwargs)

        monkeypatch.setattr(_FamilyReader, 'compare_original', changed_metadata)
        reason = 'schema or metadata'
    else:
        processing = court_receipts.processing_rows

        def changed_rows(*args, **kwargs):
            restored = list(processing(*args, **kwargs))
            intercepted.append(change)
            if change == 'order':
                restored.reverse()
            else:
                restored[0]['page_count'] = '7'
            yield from restored

        monkeypatch.setattr(court_receipts, 'processing_rows', changed_rows)
        reason = 'values, order or repetitions'
    with pytest.raises(conversion.ConversionRefused, match=f'court_opinions does not restore.*{reason}'):
        convert('court-opinions', tmp_path / 'prepared', publish=True)
    assert intercepted == [change]
    assert bucket.writes == written


def large_processing_opinions():
    """A remaining receipt-only literal deliberately exercises the shrink guard."""
    import hashlib
    from spicy_regs.court_subjects import RECEIPT_FIELDS, SUBJECT_SCHEMAS

    assert 'local_path' in RECEIPT_FIELDS['court_opinions']
    assert 'local_path' not in SUBJECT_SCHEMAS['court_opinions'].names
    rows = opinions()
    for number, row in enumerate(rows):
        row['local_path'] = ''.join(hashlib.sha256(f'{number}:{i}'.encode()).hexdigest() for i in range(300))
        row['download_url'] = f'https://court.invalid/opinions/{row["opinion_id"]}.pdf'
    return rows


@pytest.mark.parametrize('prepared_publication', [False, True])
def test_complete_conversion_replaces_size_heuristic_with_exact_restoration(
        tmp_path, monkeypatch, bucket, prepared_publication):
    monkeypatch.delenv('R2_ALLOW_SHRINK', raising=False)
    monkeypatch.delenv('R2_MIN_SIZE_RATIO', raising=False)
    rows = large_processing_opinions()
    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': rows})
    work = tmp_path / 'prepared'
    compared = observed_processing(monkeypatch)
    if prepared_publication:
        prepared = convert('court-opinions', work)
        generation = Path(prepared['generation']['directory'])
        before = list(bucket.writes)
        # Ordinary publication still refuses these same valid, smaller native bytes.
        with pytest.raises(RuntimeError, match='shrink'):
            publication.publish_generation(generation, client=bucket, bucket=BUCKET,
                                           prior_index=publication.current_index(BASE))
        assert bucket.writes == before
        sealed = {p.name: p.read_bytes() for p in generation.iterdir() if p.is_file()}
        receipt = conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinions'],
            expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE))
        assert sealed == {p.name: p.read_bytes() for p in generation.iterdir() if p.is_file()}
    else:
        receipt = convert('court-opinions', work, publish=True)
    entry = publication.current_index(BASE)['families']['court-opinions']
    assert entry['tables']['court_opinions.parquet']['byteSize'] < old['tables']['court_opinions.parquet']['byteSize'] / 2
    subject = pq.read_table(Path(receipt['generation']['directory']) / 'court_opinions.parquet')
    assert 'local_path' not in subject.column_names
    assert subject['download_url'].to_pylist() == [row['download_url'] for row in rows]
    assert receipt['read_back']['anonymous_read_rows'] == {'court_opinions': len(rows)}
    assert len(compared) >= 2  # Local proof and actual anonymous read-back both decode exact originals.
    assert all(restored == rows for _, restored in compared)
    assert all(schema.equals(pq.read_schema(work / 'retained/court_opinions.parquet'),
                             check_metadata=True) for schema, _ in compared)


@pytest.mark.parametrize('damage', ['saved-report', 'candidate', 'logical-id', 'prior', 'incomplete', 'extra'])
def test_publication_refuses_mismatched_or_saved_conversion_proof(tmp_path, monkeypatch, bucket, damage):
    import json
    from rulespec_artifacts import canonical_json_bytes

    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': large_processing_opinions()})
    work = tmp_path / 'prepared'
    convert('court-opinions', work)
    before = list(bucket.writes)
    publish = publication._publish_verified_generation
    intercepted = []

    def changed(*args, **kwargs):
        intercepted.append(damage)
        proof = kwargs['conversion_proof']
        if damage == 'saved-report':
            proof = json.loads((work / conversion.RECEIPT).read_text())
        elif damage == 'candidate':
            proof = proof._replace(artifact_digest='sha256:' + '00' * 32)
        elif damage == 'logical-id':
            proof = proof._replace(logical_id=old['logicalId'])
        elif damage == 'prior':
            proof = proof._replace(prior_entry=canonical_json_bytes(old | {'publishedAt': '2000-01-01T00:00:00Z'}))
        elif damage == 'incomplete':
            proof = proof._replace(restored_tables=frozenset())
        else:
            proof = proof._replace(restored_tables=proof.restored_tables | {'other.parquet'})
        return publish(*args, **kwargs | {'conversion_proof': proof})

    monkeypatch.setattr(publication, '_publish_verified_generation', changed)
    with pytest.raises(conversion.ConversionRefused, match='Native conversion proof differs'):
        conversion.publish_prepared(work / conversion.RECEIPT, allowed=['court-opinions'],
            expected_main=MAIN, expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE))
    assert intercepted == [damage]
    assert bucket.writes == before
    assert publication.current_index(BASE)['families']['court-opinions'] == old


def test_conversion_proof_cannot_be_reused_after_native_publication(tmp_path, monkeypatch, bucket):
    publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': large_processing_opinions()})
    publish = publication.publish_generation
    verified_publish = publication._publish_verified_generation
    proofs = []

    def capture(*args, **kwargs):
        proofs.append(kwargs['conversion_proof'])
        return verified_publish(*args, **kwargs)

    monkeypatch.setattr(publication, '_publish_verified_generation', capture)
    receipt = convert('court-opinions', tmp_path / 'work', publish=True)
    assert len(proofs) == 1
    before = list(bucket.writes)
    current = publication.current_index(BASE)['families']['court-opinions']
    with pytest.raises(publication.PublicationError, match='Native conversion proof differs'):
        publish(Path(receipt['generation']['directory']), client=bucket, bucket=BUCKET,
                prior_index=publication.current_index(BASE), conversion_proof=proofs[0])
    assert len(proofs) == 2
    assert bucket.writes == before
    assert publication.current_index(BASE)['families']['court-opinions'] == current


def test_conversion_keeps_complete_prior_on_conditional_pointer_retry(tmp_path, monkeypatch, bucket):
    from rulespec_artifacts import canonical_json_bytes

    old = publish_old(bucket, monkeypatch, tmp_path, 'court-opinions', {'court_opinions': large_processing_opinions()})
    changed = old | {'publishedAt': '2000-01-01T00:00:00Z'}

    def concurrent(key):
        if key == publication.INDEX_V2_KEY:
            current = publication.current_index(BASE)
            current['families']['court-opinions'] = changed
            bucket.objects[key] = canonical_json_bytes(current)

    bucket.before_put = concurrent
    with pytest.raises(conversion.ConversionRefused, match='changed|stale'):
        convert('court-opinions', tmp_path / 'work', publish=True)
    assert publication.current_index(BASE)['families']['court-opinions'] == changed
