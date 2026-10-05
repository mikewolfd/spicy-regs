"""Freshness compares admitted native members, never their private restored files."""
from spicy_regs.mcp_server import _snapshot_lineage
from spicy_regs.sources.publication import table_pin


def test_native_snapshot_reports_actual_selected_source_pin_and_stale_or_absent_inputs():
    index = {'families': {'agenda': {'artifactDigest': 'sha256:' + 'a' * 64, 'tables': {
        'unified_agenda.parquet': {'sha256': 'sha256:' + 'b' * 64, 'byteSize': 1}}}}}
    pin = table_pin(index, 'unified_agenda.parquet')
    manifest = {'inputs': {'sources': {'unified_agenda.parquet': {'sha256': '0' * 64}},
                           'native_inputs': {'unified_agenda': {'publication': pin}}}}
    result = _snapshot_lineage(index, manifest)
    assert result['inputs_current'] is True
    assert result['snapshot_inputs'][0]['built_from'] == 'sha256:' + 'a' * 64
    index['families']['agenda']['tables']['unified_agenda.parquet']['sha256'] = 'sha256:' + 'c' * 64
    assert _snapshot_lineage(index, manifest)['snapshot_inputs'][0]['input_table_current'] is False
    assert _snapshot_lineage({'families': {}}, manifest)['snapshot_inputs'][0]['input_table_current'] is None


def test_native_split_source_freshness_uses_complete_descriptor():
    descriptor = {'byteSize': 1, 'members': [{'path': 'part-000001.parquet', 'sha256': 'sha256:' + 'b' * 64}]}
    index = {'families': {'agenda': {'artifactDigest': 'sha256:' + 'a' * 64,
                                    'tables': {'unified_agenda.parquet': descriptor}}}}
    manifest = {'inputs': {'native_inputs': {'unified_agenda': {'publication': table_pin(index, 'unified_agenda.parquet')}}}}
    assert _snapshot_lineage(index, manifest)['inputs_current'] is True
    descriptor['members'][0]['sha256'] = 'sha256:' + 'c' * 64
    assert _snapshot_lineage(index, manifest)['snapshot_inputs'][0]['input_table_current'] is False
