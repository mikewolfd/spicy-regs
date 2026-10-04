"""Court domain values survive normalization without source-evidence columns."""
import hashlib
import json

import pytest

from spicy_regs.court_subjects import SUBJECT_SCHEMAS, normalize_court_row, opinion_body_id


def test_native_names_keep_null_empty_order_and_duplicate_elements():
    values = ['Agency', None, '', 'Agency', 'Law Firm, PLLC']
    row = normalize_court_row('court_dockets', {
        'cl_docket_id': '12', 'parties_json': json.dumps(values),
        'attorneys_json': None, 'firms_json': '[]',
    })
    assert row['parties'] == values
    assert row['attorneys'] is None
    assert row['firms'] == []
    assert row['parties_json'] == json.dumps(values)
    assert not any(name.endswith('_json') for name in SUBJECT_SCHEMAS['court_dockets'].names)


@pytest.mark.parametrize('value', ['{broken', '{}', 'null', '[12]', '[{}]', '"Attorney"'])
def test_unsupported_name_arrays_refuse_instead_of_becoming_empty(value):
    with pytest.raises(ValueError):
        normalize_court_row('court_dockets', {'cl_docket_id': '12', 'parties_json': value})


def test_counsel_prose_is_one_unsplit_element_and_empty_stays_explicit():
    for value in ('Smith, Jones & Co.', ''):
        row = normalize_court_row('court_opinion_clusters', {'cluster_id': '1', 'attorneys': value})
        assert row['attorneys'] == [value]
        assert row['conversion_inputs']['attorneys'] == value


def test_domain_status_and_negative_publisher_count_survive():
    row = normalize_court_row('court_opinion_clusters', {
        'cluster_id': '1', 'blocked': 't', 'date_blocked': '2018-01-01',
        'precedential_status': 'Unpublished', 'disposition': 'Dismissed',
        'date_filed_is_approximate': 'f', 'citation_count': '-1', 'ingest_source': 'bulk',
    })
    assert row['blocked'] is True
    assert row['date_filed_is_approximate'] is False
    assert row['citation_count'] == -1  # Held data uses this literal; do not invent a replacement.
    assert row['disposition'] == 'Dismissed'
    assert row['precedential_status'] == 'Unpublished'
    assert row['date_blocked'] == '2018-01-01'
    assert 'ingest_source' not in SUBJECT_SCHEMAS['court_opinion_clusters'].names


@pytest.mark.parametrize('field,value', [('blocked', 'unknown'), ('citation_count', '1.5'), ('citation_count', str(2**64))])
def test_invalid_scalar_conversion_refuses(field, value):
    with pytest.raises(ValueError):
        normalize_court_row('court_opinion_clusters', {'cluster_id': '1', field: value})


def test_body_identity_distinguishes_bytes_and_ignores_extractor_version():
    def digest(body):
        return 'sha256:' + hashlib.sha256(body).hexdigest()
    a = opinion_body_id('1', digest(b'A'))
    assert a != opinion_body_id('1', digest(b'B'))
    assert a != opinion_body_id('2', digest(b'A'))
    rows = [normalize_court_row('court_opinion_pdf_extractions', {
        'opinion_id': '1', 'cluster_id': '2', 'source_sha256': digest(b'A'),
        'extractor_version': version, 'text_content': text,
    }) for version, text in [('1', 'A'), ('2', 'corrected A')]]
    assert [r['opinion_body_id'] for r in rows] == [a, a]


def test_every_extra_mapper_field_requires_a_decision():
    with pytest.raises(ValueError, match='unclassified'):
        normalize_court_row('court_opinions', {'opinion_id': '1', 'new_publisher_flag': 'x'})
