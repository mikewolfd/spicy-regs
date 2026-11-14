"""Qualify this container's ordinary loopback MCP against its selected FEC release.

Run after publishing the image and its matching receipt, before deployment.
Requests are one-shot; a failure leaves its transcript and stops the sequence.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import time
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

URL = 'http://127.0.0.1:8080/mcp'
MAX_BYTES = 2_000_000
TIMEOUT = 20
VIEW = 'fec_filing_reference_resolution'
RECORD = 'sha256:008bf9db0c2af575d4c39f07f26044ca89c7cb9391eb3c3114e51918687059ed'
QUERY_GENERATION = 'sha256:784b23da0ea9ee7807e09715d38508cd8e0cd654af1047ae6a5b1fc63efb7300'
EVIDENCE_GENERATIONS = {'fec-observations': 'sha256:4cb0dffa91c278eb4e7a6048e266a5e4389748666428a82bc58c35ee3fe77f2f'}
SQL_SHA = 'sha256:176d654ca9a85f408dd49e841288d397757f98acc2ff7a4262aa947022862305'
COLUMNS = ['record_id', 'relation_type', 'target_observation_count', 'target_resolution_status']
EXPECTED_ROWS = [[RECORD, 'amendment_chain_member', 3, 'resolved_native_filing_key']]
SQL = f"SELECT {', '.join(COLUMNS)} FROM {VIEW} WHERE record_id = '{RECORD}' LIMIT 2"
MISSING = 'runtime_qualification_missing_table'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('Loopback qualification refuses redirects')


class Client:
    def __init__(self, output):
        self.output = output
        self.session = None
        self.opener = build_opener(NoRedirect())

    def request(self, label, method, params=None):
        body = {'jsonrpc': '2.0', 'method': method}
        notification = method == 'notifications/initialized'
        if not notification:
            body['id'] = label
        if params is not None:
            body['params'] = params
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream',
                   'MCP-Protocol-Version': '2025-06-18', 'User-Agent': 'spicy-regs-image-qualification/1.0'}
        if self.session:
            headers['Mcp-Session-Id'] = self.session
        save(self.output / f'{label}.request.json', {'url': URL, 'headers': headers, 'body': body})
        started = time.monotonic()
        metadata: dict[str, object] = {'started_at': datetime.now(timezone.utc).isoformat(), 'timeout_seconds': TIMEOUT,
                                      'max_response_bytes': MAX_BYTES, 'attempts': 1}
        try:
            try:
                response = self.opener.open(Request(URL, data=json.dumps(body).encode(), headers=headers), timeout=TIMEOUT)
            except HTTPError as error:
                response = error
            with response:
                raw = response.read(MAX_BYTES + 1)
                metadata.update(status=response.status, headers=dict(response.headers), bytes=len(raw),
                                sha256=hashlib.sha256(raw).hexdigest())
                returned_session = response.headers.get('Mcp-Session-Id')
            (self.output / f'{label}.response.txt').write_bytes(raw)
            require(len(raw) <= MAX_BYTES, 'Response exceeded qualification byte bound')
            require(metadata['status'] == (202 if notification else 200), 'Unexpected MCP HTTP status')
            if returned_session:
                require(self.session in (None, returned_session), 'MCP session changed during qualification')
                self.session = returned_session
            if notification:
                require(not raw, 'Initialized notification returned an unexpected body')
                return None
            try:
                decoded = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                decoded = [json.loads(line[5:]) for line in raw.decode().splitlines() if line.startswith('data:')]
            save(self.output / f'{label}.response.json', decoded)
            replies = decoded if isinstance(decoded, list) else [decoded]
            matching = [item for item in replies if item.get('id') == label]
            require(len(matching) == 1, 'Expected one matching MCP response')
            require('error' not in matching[0], 'Unexpected MCP protocol error')
            return matching[0]['result']
        except Exception as error:
            metadata.update(error_type=type(error).__name__, error=str(error))
            raise
        finally:
            metadata.update(seconds=time.monotonic() - started, finished_at=datetime.now(timezone.utc).isoformat())
            save(self.output / f'{label}.meta.json', metadata)

    def tool(self, label, name, arguments, *, expected_error=False):
        result = self.request(label, 'tools/call', {'name': name, 'arguments': arguments})
        require(bool(result.get('isError')) == expected_error, f'Unexpected tool outcome: {name}')
        if expected_error:
            text = '\n'.join(item.get('text', '') for item in result['content'])
            require(MISSING in text and 'does not exist' in text, 'Expected the missing-table catalog error')
            return result
        require(isinstance(result.get('structuredContent'), dict), f'Missing structured result: {name}')
        return result['structuredContent']


def check_release(release, receipt, receipt_sha, *, consumer=False):
    expected = receipt['views'][VIEW]
    require(release['status'] == 'compatible' and release['receipt_sha256'] == receipt_sha, 'FEC release is not compatible')
    for key in ['sql_sha256', 'evidence_generations', 'population', 'as_of']:
        require(release[key] == expected[key], f'FEC release differs: {key}')
    require(release['sql_sha256'] == SQL_SHA and release['evidence_generations'] == EVIDENCE_GENERATIONS,
            'The bounded witness requires renewed review for this FEC selection')
    if consumer:
        require(release['consumer'] == receipt['consumer'], 'Served consumer identity differs from the image-bound receipt')


def check_query(result, receipt, receipt_sha):
    require(result['columns'] == COLUMNS and result['rows'] == EXPECTED_ROWS, 'Protected FEC witness value differs')
    require(result['row_count_shown'] == 1 and not result['truncated'] and not result['truncated_cells'], 'FEC result is incomplete')
    publication = result['publication'][VIEW]
    check_release(publication['release_compatibility'], receipt, receipt_sha)
    require(set(publication['input_publications']) == {'fec_filing_links', 'fec_filings'}, 'FEC input set differs')
    for value in publication['input_publications'].values():
        require(value['status'] == 'managed_generation' and value['family'] == 'fec-query'
                and value['artifact_digest'] == QUERY_GENERATION, 'FEC input generation differs')


def exercise(client, receipt, receipt_sha):
    initialized = client.request('initialize', 'initialize', {
        'protocolVersion': '2025-06-18', 'capabilities': {},
        'clientInfo': {'name': 'spicy-regs-image-qualification', 'version': '1.0'},
    })
    require(initialized['protocolVersion'] == '2025-06-18' and initialized['serverInfo']['name'] == 'spicy-regs',
            'Unexpected MCP initialization')
    client.request('initialized', 'notifications/initialized')
    discovered = client.request('tools-list', 'tools/list', {})
    require({item['name'] for item in discovered['tools']} >= {'list_sources', 'describe_table', 'query_sql'}, 'Required MCP tools absent')
    sources = client.tool('sources', 'list_sources', {})
    require(isinstance(sources['subjects'], list), 'Discovery returned no subject list')
    require(sources['fec_release']['status_counts'].get('disabled', 0) == 0
            and sources['fec_release']['status_counts'].get('compatible', 0) > 0, 'Discovery disabled the FEC release')
    first = client.tool('fec-known', 'query_sql', {'sql': SQL, 'max_rows': 2})
    check_query(first, receipt, receipt_sha)
    description = client.tool('fec-describe', 'describe_table', {'table': VIEW})
    release = description['publication'].get('release_compatibility') or description['relationship']['release_compatibility']
    check_release(release, receipt, receipt_sha, consumer=True)
    client.tool('expected-error', 'query_sql', {'sql': f'SELECT * FROM {MISSING} LIMIT 1'}, expected_error=True)
    recovered = client.tool('fec-recovery', 'query_sql', {'sql': SQL, 'max_rows': 2})
    check_query(recovered, receipt, receipt_sha)
    require(first['publication'] == recovered['publication'], 'Protected release changed during recovery')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    result = {'status': 'failed', 'scope': 'Ordinary loopback MCP in the selected image; not deployed-service acceptance'}
    try:
        from spicy_regs.fec_release import load_receipt, runtime_consumer
        from spicy_regs.runtime_bounds import RequestBound
        receipt_sha = os.environ['SPICY_REGS_FEC_RELEASE_SHA256']
        image_sha = os.environ['SPICY_REGS_CONSUMER_IMAGE_DIGEST']
        with RequestBound(TIMEOUT, 'Qualification receipt deadline').scope():
            receipt = load_receipt(receipt_sha, os.environ['SPICY_REGS_R2_URL'])
        require(receipt['consumer'] == runtime_consumer(image_sha), 'Image packages/code differ from the selected receipt')
        save(args.output / 'selected-receipt.json', receipt)
        result.update(receipt_sha256=receipt_sha, consumer=receipt['consumer'])
        deadline = time.monotonic() + 65
        while True:
            try:
                with socket.create_connection(('127.0.0.1', 8080), timeout=.25):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError('Image did not open its MCP port after bounded startup') from None
                time.sleep(.25)
        exercise(Client(args.output), receipt, receipt_sha)
        result['status'] = 'pass'
    except Exception as error:
        result.update(error_type=type(error).__name__, error=str(error))
        raise
    finally:
        save(args.output / 'qualification.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
