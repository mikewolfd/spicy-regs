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
