"""Actual legacy columns retain meanings without redeclaring native subject fields."""
import asyncio
import copy
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from mcp.types import CallToolResult

from spicy_regs import mcp_server
from spicy_regs.etl_policy_registry import installed_policies


LEGACY = json.loads((Path(__file__).parent / 'fixtures/lobbying_filings_legacy_publication.json').read_text())
HISTORICAL = {'url', 'lobbying_activities_json', 'government_entities_json'}


def describe(monkeypatch, *, native=False, extra_url=False, local_native=False):
    con = duckdb.connect()
    index = copy.deepcopy(LEGACY)
    entry = index['families']['lobbying-filings']
    if native:
        schema = installed_policies()['lobbying_filings'].subject_schema
        con.register('subjects', pa.Table.from_batches([], schema=schema))
        con.execute('CREATE TABLE lobbying_filings AS SELECT * FROM subjects')
        entry['etlReceipts'] = dict(key='etl_receipts.parquet', rows=0, sha256='sha256:' + '0' * 64,
                                   byteSize=0, generationId='native-fixture', datasets=['lobbying_filings'])
    else:
        columns = entry['tables']['lobbying_filings.parquet']['columns']
        con.execute('CREATE TABLE lobbying_filings (' + ','.join(f'"{name}" {dtype}' for name, dtype in columns) + ')')
    if extra_url:
        con.execute('ALTER TABLE lobbying_filings ADD COLUMN url VARCHAR')
    monkeypatch.setattr(mcp_server, '_get_connection', lambda: con)
    monkeypatch.setattr(mcp_server, '_connection_index', lambda cursor: index)
    monkeypatch.setattr(mcp_server, '_input_lineage', lambda *args: {})
    if local_native:
        monkeypatch.setattr(mcp_server, '_connection_local_selection',
                            lambda cursor: {'native': {'lobbying_filings': {}}})
    mcp_server._table_metadata.cache_clear()
    result = asyncio.run(mcp_server.build_server().call_tool('describe_table', {'table': 'lobbying_filings'}))
    con.close()
    assert isinstance(result, CallToolResult)
    assert not result.is_error
    return result.structured_content


def test_pinned_legacy_lobbying_columns_keep_original_meanings(monkeypatch):
    result = describe(monkeypatch)
    columns = {c['column_name']: c for c in result['columns']}
    assert result['publication']['artifact_digest'] == LEGACY['families']['lobbying-filings']['artifactDigest']
    assert 'page on lda.gov' in columns['url']['description']
    assert 'JSON array of lobbying activities' in columns['lobbying_activities_json']['description']
    assert 'JSON array of the distinct government entities' in columns['government_entities_json']['description']
    assert 'not an organization identity' in columns['client_id']['description']
    assert 'drop registrations (`RR`, `RA`)' in columns['filing_type']['description']
    assert result['metadata']['identity_columns'] == ['filing_uuid']
    assert HISTORICAL <= set(result['schema_differences']['unexpected_columns'])
    assert 'legacy_column_descriptions' not in result['metadata']
    assert len(json.dumps(result, separators=(',', ':'))) < 12000


def test_native_lobbying_subject_does_not_reintroduce_historical_fields(monkeypatch):
    result = describe(monkeypatch, native=True)
    assert HISTORICAL.isdisjoint(c['column_name'] for c in result['columns'])
    assert result['schema_matches_declared'] is True
    assert result['metadata']['identity_columns'] == ['filing_uuid']
    assert 'legacy_column_descriptions' not in result['metadata']
    assert len(json.dumps(result, separators=(',', ':'))) < 12000


@pytest.mark.parametrize('layout', ['published-native', 'local-native'])
def test_historical_meaning_is_withheld_when_selected_native_has_extra_column(monkeypatch, layout):
    result = describe(monkeypatch, native=layout == 'published-native', extra_url=layout == 'published-native',
                      local_native=layout == 'local-native')
    assert next(c for c in result['columns'] if c['column_name'] == 'url')['description'] is None
