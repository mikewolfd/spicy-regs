"""Image qualification must reject a plausible wrong release or interrupted protocol."""

from copy import deepcopy
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys

import pytest


@pytest.fixture
def qualification():
    return runpy.run_path(str(Path(__file__).parents[1] / 'deploy/cloudflare/qualify_reader.py'))


@pytest.fixture
def replies(qualification):
    q = qualification
    release = {'status': 'compatible', 'receipt_sha256': 'sha256:' + 'a' * 64,
               'sql_sha256': q['SQL_SHA'], 'evidence_generations': q['EVIDENCE_GENERATIONS'],
               'population': 'retained observations', 'as_of': 'retained source date',
               'consumer': {'image_digest': 'sha256:' + 'b' * 64, 'code_sha256': 'sha256:' + 'c' * 64,
                            'package_versions': {'spicy-regs': 'qualified'}}}
    receipt = {'views': {q['VIEW']: deepcopy(release)}, 'consumer': release['consumer']}
    publication = {'release_compatibility': release, 'input_publications': {
        name: {'status': 'managed_generation', 'family': 'fec-query', 'artifact_digest': q['QUERY_GENERATION']}
        for name in ['fec_filing_links', 'fec_filings']}}
    query = {'columns': q['COLUMNS'], 'rows': q['EXPECTED_ROWS'], 'row_count_shown': 1,
             'truncated': False, 'truncated_cells': [], 'publication': {q['VIEW']: publication}}
    def tool(value):
        return {'isError': False, 'structuredContent': value}
    return receipt, {
        'initialize': {'protocolVersion': '2025-06-18', 'serverInfo': {'name': 'spicy-regs'}},
        'initialized': None,
        'tools-list': {'tools': [{'name': name} for name in ['list_sources', 'describe_table', 'query_sql']]},
        'sources': tool({'subjects': [], 'fec_release': {'status_counts': {'compatible': 71}}}),
        'fec-known': tool(query),
        'fec-describe': tool({'publication': {'status': 'unavailable'}, 'relationship': {'release_compatibility': release}}),
        'expected-error': {'isError': True, 'content': [{'type': 'text', 'text': q['MISSING'] + ' does not exist'}]},
        'fec-recovery': tool(deepcopy(query)),
    }


def client_with_replies(q, tmp_path, replies):
    class Response(io.BytesIO):
        status = 200
        headers = {'Content-Type': 'text/event-stream', 'Mcp-Session-Id': 'qualified-session'}

    class Transport:
        calls = []

        def open(self, request, timeout):
            body = json.loads(request.data)
            label = body.get('id', 'initialized')
            if self.calls:
                assert request.get_header('Mcp-session-id') == 'qualified-session'
            self.calls.append(label)
            if label == 'initialized':
                response = Response(b'')
                response.status = 202
                return response
            return Response(('event: message\ndata: ' + json.dumps({
                'jsonrpc': '2.0', 'id': label, 'result': replies[label]}) + '\n\n').encode())

    client = q['Client'](tmp_path)
    client.opener = Transport()
    return client


def test_complete_protocol_retains_expected_error_and_recovers(qualification, replies, tmp_path):
    receipt, responses = replies
    client = client_with_replies(qualification, tmp_path, responses)
    qualification['exercise'](client, receipt, 'sha256:' + 'a' * 64)
    assert client.opener.calls == list(responses)
    assert json.loads((tmp_path / 'expected-error.response.json').read_text())[0]['result']['isError']
    assert json.loads((tmp_path / 'fec-recovery.meta.json').read_text())['attempts'] == 1


@pytest.mark.parametrize('failure', ['discovery', 'value', 'generation', 'consumer', 'recovery', 'unexpected_success'])
def test_qualification_stops_at_first_wrong_outcome(qualification, replies, tmp_path, failure):
    receipt, responses = replies
    q = qualification
    responses = deepcopy(responses)
    failed_label = 'fec-known'
    if failure == 'discovery':
        responses['sources'] = {'isError': True, 'content': [{'text': "No module named 'marko'"}]}
        failed_label = 'sources'
    elif failure == 'value':
        responses['fec-known']['structuredContent']['rows'][0][2] = 4
    elif failure == 'generation':
        responses['fec-known']['structuredContent']['publication'][q['VIEW']]['input_publications']['fec_filings']['artifact_digest'] = 'sha256:' + '0' * 64
    elif failure == 'consumer':
        responses['fec-describe']['structuredContent']['relationship']['release_compatibility']['consumer']['image_digest'] = 'sha256:' + '0' * 64
        failed_label = 'fec-describe'
    elif failure == 'recovery':
        responses['fec-recovery']['structuredContent']['truncated'] = True
        failed_label = 'fec-recovery'
    else:
        responses['expected-error'] = {'isError': False, 'structuredContent': {}}
        failed_label = 'expected-error'
    client = client_with_replies(q, tmp_path, responses)
    with pytest.raises(ValueError):
        q['exercise'](client, receipt, 'sha256:' + 'a' * 64)
    assert client.opener.calls[-1] == failed_label
    assert len(set(client.opener.calls)) == len(client.opener.calls)
    assert (tmp_path / f'{failed_label}.response.txt').exists()


def test_transport_refuses_oversize_before_decoding(qualification, tmp_path):
    class Response(io.BytesIO):
        status = 200
        headers = {}

    class Transport:
        calls = 0
        def open(self, request, timeout):
            self.calls += 1
            return Response(b'x' * (qualification['MAX_BYTES'] + 20))

    client = qualification['Client'](tmp_path)
    client.opener = Transport()
    with pytest.raises(ValueError, match='byte bound'):
        client.request('oversize', 'tools/list')
    assert client.opener.calls == 1
    assert (tmp_path / 'oversize.response.txt').stat().st_size == qualification['MAX_BYTES'] + 1


def test_image_smoke_catches_missing_request_time_package():
    script = Path(__file__).parents[1] / 'deploy/cloudflare/runtime_readers.py'
    code = '''
import importlib.abc, runpy, sys
class MissingImagePackage(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'marko':
            raise ModuleNotFoundError("No module named 'marko'")
sys.meta_path.insert(0, MissingImagePackage())
runpy.run_path(sys.argv[1], run_name='__main__')
'''
    result = subprocess.run([sys.executable, '-c', code, str(script)], capture_output=True, text=True, timeout=20)
    assert result.returncode != 0
    assert "Error executing tool list_sources: No module named 'marko'" in result.stderr
