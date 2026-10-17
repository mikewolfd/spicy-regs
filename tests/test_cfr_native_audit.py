"""Controls ported from D's executable-request-v3 semantic runner, against the maintained module API."""
import dataclasses
import gzip
import json
import os
from pathlib import Path

import pytest

from spicy_regs import cfr_native_audit as audit
from tests.operational_helpers import cfr_capture


@dataclasses.dataclass
class Node:
    name: str
    attributes: tuple = ()
    namespace_declarations: tuple = ()
    content: tuple = ()


BODY = b'<x xmlns="urn:x" a="1">before<q>one</q><q>two</q>after</x>'
TREE = Node('{urn:x}x', (('a', '1'),), (('', 'urn:x'),),
            ('before', Node('{urn:x}q', content=('one',)), Node('{urn:x}q', content=('two',)), 'after'))
URL = 'https://example.test/annual'
CONTEXT = {'kind': 'annual', 'year': 2025, 'expected_final_url': URL}
SPEC = {'decoded_sha256': audit.digest(BODY), 'decoded_bytes': len(BODY)}


class Backend:
    @staticmethod
    def read(body, context, url):
        return TREE, {'fixture': True, 'year': context['year']}


def new_spelling(body=BODY, status=200, mime='application/xml'):
    return {'requested_url': URL, 'resolved_url': URL, 'status_code': status, 'content_type': mime,
            'observed_at': 'fixture-time', 'sha256': audit.digest(body)[7:], 'byte_size': len(body)}


def old_spelling(body=BODY, status=200, mime='application/xml', status_key='http_status'):
    return {'url': URL, 'final_url': URL, status_key: status, 'content_type': mime,
            'observed_at': 'fixture-time', 'sha256': audit.digest(body)[7:], 'bytes': len(body)}


def raw(record):
    return json.dumps(record).encode()


def distinct(index):
    """A trailing comment leaves the event list unchanged but makes each body's digest distinct."""
    return BODY + b'<!-- ' + str(index).encode() + b' -->'


def test_complete_events():
    assert audit.independent_events(BODY) == audit.maintained_events(TREE)
    assert sum(event[0] == 'start' for event in audit.independent_events(BODY)) == 3


def test_mapping_loss_refused():
    class Bad:
        @staticmethod
        def read(*args):
            return Node('x'), {}

    with pytest.raises(ValueError, match='mapping differs'):
        audit.observe(BODY, raw(new_spelling()), SPEC, CONTEXT, Bad)


@pytest.mark.parametrize('spelling', [new_spelling, old_spelling], ids=['requested_url-spelling', 'url-spelling'])
def test_both_retained_capture_spellings_are_accepted_verbatim(spelling):
    record = spelling()
    result = audit.capture_association(raw(record), SPEC, CONTEXT)
    assert result['source_record'] == record
    assert (result['requested_url'], result['final_url'], result['status'], result['decoded_bytes']) == (URL, URL, 200, len(BODY))


def test_status_alias_is_accepted_for_the_404_record():
    html = b'<html>404</html>'
    result = audit.capture_association(raw(old_spelling(html, 404, 'text/html', 'status')),
                                       {'decoded_sha256': audit.digest(html), 'decoded_bytes': len(html)},
                                       {'kind': 'failure', 'expected_final_url': URL})
    assert result['status'] == 404


@pytest.mark.parametrize('extra', [{'status': 200}, {'status_code': 200}, {'requested_url': URL}, {'resolved_url': URL},
                                   {'byte_size': len(BODY)}])
def test_a_record_carrying_two_spellings_of_one_field_is_ambiguous(extra):
    record = {**old_spelling(), **extra}
    with pytest.raises(ValueError, match='ambiguous'):
        audit.capture_association(raw(record), SPEC, CONTEXT)


@pytest.mark.parametrize('field', ['url', 'final_url', 'http_status', 'bytes', 'content_type', 'observed_at', 'sha256'])
def test_a_record_missing_a_field_is_refused(field):
    record = old_spelling()
    del record[field]
    with pytest.raises(ValueError, match='missing'):
        audit.capture_association(raw(record), SPEC, CONTEXT)


@pytest.mark.parametrize('change', [{'bytes': 1}, {'sha256': '0' * 64}, {'http_status': 404}, {'final_url': 'https://else.test/'},
                                    {'url': 'https://example.test/other'}, {'http_status': True}, {'content_type': 'text/html'}])
def test_capture_field_failures_refuse(change):
    with pytest.raises(ValueError):
        audit.capture_association(raw({**old_spelling(), **change}), SPEC, CONTEXT)


def test_404_is_retained_and_never_parsed():
    html = b'<html>404</html>'

    class NoParser:
        @staticmethod
        def read(*args):
            raise AssertionError('404 parsed')

    out = audit.observe(html, raw(old_spelling(html, 404, 'text/html')),
                        {'decoded_sha256': audit.digest(html), 'decoded_bytes': len(html)},
                        {'kind': 'failure', 'expected_final_url': URL}, NoParser)
    assert out['native'] is None and out['status'] == 'retained404-failure'


@pytest.mark.parametrize('document', [b'{"a":1,"a":2}', b'{"a":NaN}'])
def test_json_duplicate_and_nonfinite_values_refuse(document):
    with pytest.raises(ValueError):
        audit.decode(document)


def test_xml_entity_and_depth_bounds(monkeypatch):
    for document in (b'<!DOCTYPE x [<!ENTITY a "value">]><x>&a;</x>', b'<x>' * 65 + b'</x>' * 65):
        with pytest.raises(ValueError):
            audit.independent_events(document)
    monkeypatch.setattr(audit, 'MAX_ELEMENTS', 1)
    with pytest.raises(ValueError):
        audit.independent_events(b'<x><q/></x>')


def spec_for(path, body, encoding='identity'):
    stored = gzip.compress(body, mtime=0) if encoding == 'gzip' else body
    path.write_bytes(stored)
    return {'encoding': encoding, 'decoded_sha256': audit.digest(body), 'decoded_bytes': len(body),
            'stored': {'path': str(path), 'bytes': len(stored), 'sha256': audit.digest(stored)}}


def test_gzip_is_read_and_stored_or_decoded_digest_drift_refuses(tmp_path):
    path = tmp_path / 'tiny.gz'
    spec = spec_for(path, BODY, 'gzip')
    assert audit.read_admitted(spec) == BODY
    path.write_bytes(gzip.compress(b'<changed/>'))
    with pytest.raises(ValueError, match='drift|pin differs'):
        audit.read_admitted(spec)
    other = spec_for(tmp_path / 'other.gz', BODY, 'gzip')
    other['decoded_sha256'] = audit.digest(b'something else')
    with pytest.raises(ValueError, match='decoded hash differs'):
        audit.read_admitted(other)
    plain = spec_for(tmp_path / 'plain', b'404')
    plain['decoded_sha256'] = 'sha256:' + '0' * 64
    with pytest.raises(ValueError, match='decoded hash differs'):
        audit.read_admitted(plain)


def test_inputs_bind_by_content_not_inode_ctime_or_link_count(tmp_path):
    path = tmp_path / 'body.gz'
    spec = spec_for(path, BODY, 'gzip')
    spec['stored']['identity'] = {'inode': 1, 'ctime_ns': 1, 'nlink': 7}  # Recorded identity is ignored.
    os.link(path, tmp_path / 'deduplicated-copy')  # Changes ctime and link count only.
    os.utime(path)
    assert audit.read_admitted(spec) == BODY


def test_complete_input_context_scope_with_both_spellings(tmp_path):
    config = cfr_capture(tmp_path, distinct)
    out = json.loads(audit.run(config, Backend))
    assert len(out['observations']) == 6
    assert out['comparisons'][0]['differences'] == [] and out['comparisons'][1]['differences'] == []
    assert out['observations']['4']['typed_summary']['year'] == 2023
    assert out['observations']['5']['status'] == 'retained404-failure'
    assert out['historical_selection_qualified'] is False


def test_run_refuses_an_incomplete_scope(tmp_path):
    config = cfr_capture(tmp_path, distinct)
    config['contexts'] = config['contexts'][:-1]
    with pytest.raises(ValueError, match='scope differs'):
        audit.run(config, Backend)


def test_custody_chain_and_alias_binding(tmp_path):
    config = cfr_capture(tmp_path, distinct)
    audit.verify_custody(config)
    saved = config['bodies']['0']
    config['bodies']['0'] = config['bodies']['1']
    with pytest.raises(ValueError, match='coverage differs'):
        audit.verify_custody(config)
    config['bodies']['0'] = {**saved, 'aliases': [*saved['aliases'], {'manifest': 'fixture-manifest', 'selector': 'invented'}]}
    with pytest.raises(ValueError, match='aliases differ'):
        audit.verify_custody(config)
    config['bodies']['0'] = saved
    proof = Path(config['custody_acceptance'][1]['path'])
    proof.write_bytes(b'{"fixture": false}')
    with pytest.raises(ValueError, match='drift|pin differs'):
        audit.verify_custody(config)
