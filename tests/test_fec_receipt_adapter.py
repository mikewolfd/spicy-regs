"""Qualified SQL reads verified receipts without changing native public rows."""
import base64

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.fec_receipt_adapter import ReceiptAdapter, processing_declarations
from spicy_regs.relationship_views.sql_views import SQLView
from spicy_regs.sources.publication import file_identity
from spicy_regs.transforms.fec_subject_receipts import write_fec_subjects
from tests.test_fec_subject_receipts import context, source_row


def fixture(tmp_path):
    declaration = processing_declarations()["fec_receipts"]
    schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declaration["arrow_schema"])))
    (subject, receipt), policy = write_fec_subjects(
        [source_row()], tmp_path / "bundle", table="fec_receipts", input_schema=schema,
        generation_id="generation-a", context_for=context)
    def descriptor(path):
        identity = file_identity(path)
        return {"sha256": identity["sha256"], "byteSize": identity["bytes"], "rows": 1}
    index = {"families": {"fec-query": {
        "prefix": "generations/fec-query/" + "a" * 64, "artifactDigest": "sha256:" + "a" * 64,
        "tables": {"fec_receipts.parquet": descriptor(subject)},
        "etlReceipts": {**descriptor(receipt), "key": "etl_receipts.parquet", "datasets": ["fec_receipts"],
                        "generationId": "generation-a"}}}}
    return subject, receipt, index


def test_restoration_preserves_subject_and_exact_financial_processing(tmp_path):
    subject, receipt, index = fixture(tmp_path)
    with duckdb.connect() as con:
        con.register("fec_receipts", pq.read_table(subject))
        adapter = ReceiptAdapter(con, index, "unused", local_directory=subject.parent)
        spec = SQLView("receipt_eligibility", {"fec_receipts": ("record_id", "amount_status")},
                       lambda _: "SELECT record_id, amount_status FROM fec_receipts", "test", ("record_id",))
        prepared = adapter.prepare(spec, spec.query({}))
        assert con.execute(prepared.query({})).fetchall() == [(source_row()["record_id"], "exact")]
        assert "amount_status" not in {r[0] for r in con.execute("DESCRIBE fec_receipts").fetchall()}
        assert adapter.restore("fec_receipts") == adapter.restore("fec_receipts")
        qualified = adapter.prepare(spec, "SELECT fec_receipts.amount_status FROM fec_receipts")
        assert con.execute(qualified.query({})).fetchall() == [("exact",)]
        cte = adapter.prepare(spec, "WITH fec_receipts AS (SELECT amount_status FROM main.fec_receipts) SELECT * FROM fec_receipts")
        assert con.execute(cte.query({})).fetchall() == [("exact",)]


@pytest.mark.parametrize("fault", ["generation", "bytes", "subject_version", "missing_receipt"])
def test_bad_receipts_never_create_restored_relation(tmp_path, fault):
    subject, receipt, index = fixture(tmp_path)
    if fault == "generation":
        index["families"]["fec-query"]["etlReceipts"]["generationId"] = "other"
    elif fault == "bytes":
        receipt.write_bytes(b"bad")
    elif fault == "missing_receipt":
        receipt.unlink()
    else:
        rows = pq.read_table(subject).to_pylist()
        rows[0]["currency"] = "different"
        pq.write_table(pa.Table.from_pylist(rows, schema=pq.read_schema(subject)), subject)
        identity = file_identity(subject)
        index["families"]["fec-query"]["tables"]["fec_receipts.parquet"].update(
            sha256=identity["sha256"], byteSize=identity["bytes"])
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, index, "unused", local_directory=subject.parent)
        with pytest.raises((ValueError, OSError)):
            adapter.restore("fec_receipts")
        assert con.execute("SHOW TABLES").fetchall() == []


def test_navigation_restores_receipt_only_context_and_exact_relationship_witness(tmp_path):
    import json

    from spicy_regs.relationship_views.fec import FEC_VIEWS
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = tmp_path / 'bundle'
    tables = ('fec_collections', 'fec_source_records', 'fec_relationships')
    locator = json.dumps({'collection_id': 'collection-a', 'source_record_id': 'record-a'})
    with IdentityReceiptWriter(bundle, generation_id='g-nav', tables=tables) as writer:
        writer.emit('fec_collections', {
            'collection_id': 'collection-a', 'source_family': 'bulk', 'profile': 'bulk',
            'record_count': 1, 'relationship_count': 1,
            'requested_scope_json': '["https://www.fec.gov/files/bulk-downloads/2026/cm.zip"]',
        }, input_witness=WITNESS)
        writer.emit('fec_source_records', {
            'collection_id': 'collection-a', 'source_record_id': 'record-a',
            'source_sha256': WITNESS['sha256'], 'source_locator_json': locator,
        }, input_witness=WITNESS)
        writer.emit('fec_relationships', {
            'subject_id': 'C00000001', 'subject_type': 'committee', 'object_id': 'H0CA00001',
            'object_type': 'candidate', 'relationship_type': 'supports', 'value_status': 'reported',
            'source_sha256': WITNESS['sha256'], 'source_locator_json': locator,
            'source_fields_json': '{}', 'cycle': '2026',
        }, input_witness=WITNESS)
    def descriptor(path):
        value = file_identity(path)
        return {'sha256': value['sha256'], 'byteSize': value['bytes'], 'rows': pq.read_metadata(path).num_rows}
    index = {'families': {'fec-source': {
        'prefix': 'generations/fec-source/' + 'b' * 64, 'artifactDigest': 'sha256:' + 'b' * 64,
        'tables': {'fec_relationships.parquet': descriptor(bundle / 'fec_relationships.parquet')},
        'etlReceipts': {**descriptor(bundle / 'etl_receipts.parquet'), 'key': 'etl_receipts.parquet',
                        'generationId': 'g-nav', 'datasets': list(tables)},
    }}}
    with duckdb.connect() as con:
        con.register('fec_relationships', pq.read_table(bundle / 'fec_relationships.parquet'))
        adapter = ReceiptAdapter(con, index, 'unused', local_directory=bundle)
        for spec in FEC_VIEWS:
            prepared = adapter.prepare(spec, spec.query({}))
            con.execute(f'CREATE VIEW {spec.name} AS {prepared.query({})}')
        assert con.execute('SELECT cycle, cycle_status FROM fec_collection_cycles').fetchall() == [('2026', 'bulk_directory')]
        assert con.execute('SELECT target_status, recorded_digest_status FROM fec_relationship_evidence').fetchall() == [('found', 'matches')]
        assert not (bundle / 'fec_collections.parquet').exists()
        assert 'source_locator_json' not in pq.read_schema(bundle / 'fec_relationships.parquet').names


def test_local_discovery_does_not_restore_unselected_published_fec_receipts(monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.sources.publication import empty_index

    index = empty_index()
    index['families']['fec-source'] = {
        'prefix': 'generations/fec-source/' + 'a' * 64, 'tables': {},
        'etlReceipts': {'key': 'etl_receipts.parquet', 'datasets': ['fec_collections', 'fec_source_records']},
    }
    with duckdb.connect() as con:
        monkeypatch.setattr(mcp_server, '_publication_status', lambda _: {'tables': [], 'publication': {}})
        monkeypatch.setattr(mcp_server, '_connection_index', lambda _: index)
        monkeypatch.setattr(mcp_server, '_connection_local_selection', lambda _: {
            'directory': '/not-used', 'receipt_members': {},
        })
        monkeypatch.setattr(mcp_server, '_fec_release_configuration', lambda *_: None)
        monkeypatch.setattr(ReceiptAdapter, 'restore', lambda *_: pytest.fail('unselected receipt read'))
        mcp_server._install_relationship_views(con)
        assert con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='fec_collection_cycles'").fetchall() == [(0,)]


def test_mixed_native_and_old_dependency_refuses_before_any_restoration(tmp_path):
    from spicy_regs.relationship_views.fec import FEC_VIEWS
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = tmp_path / 'native'
    with IdentityReceiptWriter(bundle, generation_id='native', tables=['fec_relationships']) as writer:
        writer.emit('fec_relationships', {'subject_id': 'C00000001', 'subject_type': 'committee',
            'object_id': 'H0CA00001', 'object_type': 'candidate', 'relationship_type': 'supports',
            'value_status': 'reported', 'source_sha256': WITNESS['sha256'],
            'source_locator_json': '{"collection_id":"c","source_record_id":"r"}',
            'source_fields_json': '{}', 'cycle': '2026'}, input_witness=WITNESS)
    selected = {'fec_relationships': {'subjects': [str(bundle / 'fec_relationships.parquet')],
                'receipts': str(bundle / 'etl_receipts.parquet'), 'generation_id': 'native'}}
    with duckdb.connect() as con:
        con.register('fec_relationships', pq.read_table(bundle / 'fec_relationships.parquet'))
        con.register('fec_source_records', pa.Table.from_pylist([{
            'collection_id': 'c', 'source_record_id': 'r', 'source_sha256': WITNESS['sha256']}]))
        adapter = ReceiptAdapter(con, {'families': {}}, 'unused', local_native=selected)
        spec = next(view for view in FEC_VIEWS if view.name == 'fec_relationship_evidence')
        with pytest.raises(ValueError, match='require selected native receipts: fec_source_records'):
            adapter.prepare(spec, spec.query({}))
        assert con.execute('SHOW TABLES').fetchall() == [('fec_relationships',), ('fec_source_records',)]
        with pytest.raises(ValueError, match='require selected native receipts'):
            adapter.restore('fec_source_records')


def test_mcp_reports_mixed_native_receipt_dependencies_before_install(monkeypatch):
    from spicy_regs import mcp_server
    from spicy_regs.sources.publication import empty_index
    import spicy_regs.relationship_views as relationships
    with duckdb.connect() as con:
        monkeypatch.setattr(mcp_server, '_publication_status', lambda _: {
            'tables': ['fec_relationships', 'fec_source_records'], 'publication': {}})
        monkeypatch.setattr(mcp_server, '_connection_index', lambda _: empty_index())
        monkeypatch.setattr(mcp_server, '_connection_local_selection', lambda _: {
            'directory': '/unused', 'receipt_members': {}, 'native': {'fec_relationships': {}}})
        monkeypatch.setattr(relationships, 'install_relationship_views',
                            lambda *a, **k: pytest.fail('installed views before dependency validation'))
        with pytest.raises(ValueError, match='require selected native receipts: fec_source_records'):
            mcp_server._install_relationship_views(con)
        assert con.execute('SHOW TABLES').fetchall() == []


@pytest.mark.parametrize('conversion', [None, [], 'invalid', 'missing', {}, 'valid'])
def test_identity_conversion_inputs_require_exact_scalar_source(tmp_path, conversion):
    import json
    from spicy_regs.etl_receipts import _digest, _unpack, exact_json, RECEIPT_SCHEMA
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS

    bundle = tmp_path / 'identity'
    with IdentityReceiptWriter(bundle, generation_id='identity', tables=['fec_relationships']) as writer:
        writer.emit('fec_relationships', {'subject_id': 'C', 'object_id': 'H', 'relationship_type': 'supports',
                    'source_sha256': WITNESS['sha256'], 'source_locator_json': '{}',
                    'source_fields_json': '{}', 'cycle': '02026'}, input_witness=WITNESS)
    receipts = bundle / 'etl_receipts.parquet'
    [receipt] = pq.read_table(receipts).to_pylist()
    fields = _unpack(json.loads(receipt['processing_json']))
    if conversion == 'missing':
        del fields['conversion_inputs']
    elif conversion != 'valid':
        fields['conversion_inputs'] = conversion
    receipt['processing_json'] = exact_json(fields)
    receipt['receipt_id'] = _digest({k: v for k, v in receipt.items() if k != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist([receipt], schema=RECEIPT_SCHEMA), receipts)
    with duckdb.connect() as con:
        adapter = ReceiptAdapter(con, {'families': {}}, 'unused', local_native={'fec_relationships': {
            'subjects': [str(bundle / 'fec_relationships.parquet')], 'receipts': str(receipts), 'generation_id': 'identity'}})
        if conversion == 'valid':
            restored = adapter.restore('fec_relationships')
            assert con.execute(f'SELECT cycle FROM {restored}').fetchall() == [('02026',)]
            assert pq.read_table(bundle / 'fec_relationships.parquet')['cycle'].to_pylist() == [2026]
        else:
            with pytest.raises(ValueError, match='conversion_inputs must be a mapping|missing retained conversion input'):
                adapter.restore('fec_relationships')
            assert con.execute('SHOW TABLES').fetchall() == []
