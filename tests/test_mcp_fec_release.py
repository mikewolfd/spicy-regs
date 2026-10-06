"""Exact-pin refusal through MCP with real native subjects and receipt-backed inputs."""

from copy import deepcopy
from dataclasses import replace
import json
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

import duckdb
import pytest
from starlette.testclient import TestClient

from spicy_regs import fec_release as release, mcp_server as server
from tests.test_fec_release import capture, digest, fixture
from tests.test_mcp_server import _records, _tool_data
from tests.test_mcp_query_results import call


def native_fixture(tmp_path, *, partition_count=1):
    import base64
    from spicy_regs.etl_receipts import combine_receipts, ReceiptContext
    from spicy_regs.fec_receipt_adapter import processing_declarations
    from spicy_regs.relationship_views.sql_views import SQLView
    from spicy_regs.sources.publication import file_identity
    from spicy_regs.transforms.fec_subject_receipts import write_fec_subjects
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter

    base_specs, _, receipt, consumer = fixture(tmp_path)
    specs = (
        replace(base_specs[0], view=SQLView('fec_test_money_filings',
            {'fec_receipts': ('record_id', 'amount'), 'fec_filings': ('record_id',)},
            lambda _: 'SELECT CAST(a.amount AS INTEGER) AS id FROM fec_receipts a JOIN fec_filings b ON a.record_id=b.record_id',
            'Receipt and filing observations', ('id',), 'test-native/1')),
        replace(base_specs[1], view=SQLView('fec_test_disbursements', {'fec_disbursements': ('amount',)},
            lambda _: 'SELECT CAST(amount AS INTEGER) AS id FROM fec_disbursements',
            'Independent disbursement observations', ('id',), 'test-native/1')),
    )
    index, locations = {'families': {}}, {}
    def descriptor(path):
        identity = file_identity(path)
        return {'sha256': identity['sha256'], 'byteSize': identity['bytes'], 'rows': pq.read_metadata(path).num_rows}
    for family, tables in {'fec-query': ['fec_receipts', 'fec_disbursements'], 'filings': ['fec_filings']}.items():
        directory = tmp_path / family
        directory.mkdir()
        prefix = 'generations/' + family + '/' + digest(family)[7:]
        generation = family + '-native'
        descriptors, shards = {}, []
        for table in tables:
            declaration = processing_declarations()[table]
            schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(declaration['arrow_schema'])))
            count = partition_count if table == 'fec_receipts' else 1
            rows = [dict(record_id=f'row-{i + 1}', **({'amount': Decimal('1'), 'amount_status': 'exact'}
                    if table != 'fec_filings' else {})) for i in range(count)]
            def context(row, ordinal):
                return ReceiptContext(generation, f'{table}-{ordinal}', 'test-native',
                    [{'source_id': table, 'sha256': digest(table), 'locator': str(ordinal)}])
            if table == 'fec_filings':
                with IdentityReceiptWriter(directory / table, generation_id=generation, tables=[table]) as writer:
                    for row in rows:
                        writer.emit(table, row, input_witness={'source_id': table, 'sha256': digest(table)})
                subject, etl = directory / table / (table + '.parquet'), directory / table / 'etl_receipts.parquet'
            else:
                (subject, etl), _ = write_fec_subjects(rows, directory / table, table=table, input_schema=schema,
                                                    generation_id=generation, context_for=context)
            assert subject is not None
            shards.append(etl)
            with duckdb.connect() as con:
                columns = [[r[0], r[1]] for r in con.execute('DESCRIBE SELECT * FROM read_parquet(?)', [str(subject)]).fetchall()]
            if count > 1:
                native, parts = pq.read_table(subject), []
                for i in range(count):
                    key = f'{table}/record_id=row-{i + 1}/part-{i:06}.parquet'
                    path = directory / key
                    path.parent.mkdir(parents=True, exist_ok=True)
                    pq.write_table(native.slice(i, 1), path)
                    parts.append({'key': key, **descriptor(path), 'partition': {'record_id': f'row-{i + 1}'}})
                    locations[prefix + '/' + key] = path
                descriptors[table + '.parquet'] = {'columns': columns, 'rows': count,
                    'byteSize': sum(part['byteSize'] for part in parts), 'partitionColumns': ['record_id'], 'members': parts}
            else:
                descriptors[table + '.parquet'] = {'columns': columns, **descriptor(subject)}
                locations[prefix + '/' + table + '.parquet'] = subject
        shared = combine_receipts(shards, directory / 'etl_receipts.parquet')
        locations[prefix + '/etl_receipts.parquet'] = shared
        index['families'][family] = {'artifactDigest': digest(family), 'prefix': prefix, 'tables': descriptors,
            'etlReceipts': {'key': 'etl_receipts.parquet', **descriptor(shared),
                            'datasets': tables, 'generationId': generation}}
    config = release.capture_configuration(specs, receipt_digest=None, image_digest=None, base_url='unused', consumer=consumer)
    receipt['output_membership'] = {name: release.captured_table(index, name) for name in ('fec_receipts', 'fec_disbursements')}
    receipt['recovery']['retained_generations'] = {family: [entry['artifactDigest']] for family, entry in index['families'].items()}
    receipt['views'] = {spec.view.name: {
        'dependencies': {name: release.captured_table(index, name) for name in spec.view.required},
        **{key: config['views'][spec.view.name][key] for key in ('sql_sha256', 'interpretation', 'population', 'as_of', 'evidence_generations')},
        'acceptance_receipts': [digest('test-native-acceptance')]}
        for spec in specs}
    return specs, index, receipt, consumer, locations


def configure(tmp_path, monkeypatch, *, partition_count=1):
    from spicy_regs.sources import publication
    specs, index, receipt, consumer, locations = native_fixture(tmp_path, partition_count=partition_count)
    def fetch(base, member, destination, *args, **kwargs):
        source = locations[member.path]
        identity = publication.file_identity(source)
        assert (member.sha256, member.byte_size) == (identity['sha256'], identity['bytes'])
        shutil.copyfile(source, destination)
        return True
    monkeypatch.setattr(publication, 'fetch_member', fetch)
    raw = json.dumps(receipt, sort_keys=True).encode()
    path = tmp_path / "selected-receipt.json"
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_FILE", str(path))
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", release.sha256(raw))
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    monkeypatch.setattr(server, "FEC_QUALIFIED_VIEWS", specs)
    monkeypatch.setattr(server, "DATA_DIR", None)
    monkeypatch.setattr(server, "TABLES", ("fec_receipts", "fec_disbursements", "fec_filings"))
    monkeypatch.setattr(release, "runtime_consumer", lambda image_digest: {**deepcopy(consumer), "image_digest": image_digest})
    return specs, index, receipt, consumer, path


def connection(index):
    from spicy_regs.sources import publication
    con = duckdb.connect()
    con.execute("CREATE TABLE _spicy_publication(snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
    with TemporaryDirectory() as temporary:
        for name in ("fec_receipts", "fec_disbursements", "fec_filings"):
            parts = []
            for ordinal, member in enumerate(publication.table_members(index, name + '.parquet')):
                path = Path(temporary) / f'{name}-{ordinal}.parquet'
                publication.fetch_member('fixture', member, path)
                parts.append(pq.ParquetFile(path).read())
            con.register('_native_input', pa.concat_tables(parts))
            con.execute(f'CREATE TABLE {name} AS SELECT * FROM _native_input')
            con.unregister('_native_input')
    server._install_relationship_views(con)
    server._apply_security_settings(con)
    return con


def test_compatible_mcp_responses_include_exact_receipt_dependency_and_consumer_ids(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        discovery = _tool_data(mcp, "list_sources", {})
        name = specs[0].view.name
        assert discovery["fec_release"]["status_counts"] == {"compatible": len(specs)}
        described = _tool_data(mcp, "describe_table", {"table": name, "detail": True})
        assert described["available"]
        result = _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
        attestation = result["publication"][name]["release_compatibility"]
        assert _records(result) == [{"id": 1}]
        assert attestation["receipt_sha256"] == discovery["fec_release"]["receipt_sha256"]
        full = described["publication"]["release_compatibility"]
        assert full["consumer"] == consumer
        assert full["dependencies"]["fec_filings"] == release.captured_table(index, "fec_filings")
        assert attestation["sql_sha256"] == full["sql_sha256"] and full["interpretation"]["policies"]
        assert "consumer" not in attestation and "dependencies" not in attestation
        raw = _tool_data(mcp, "query_sql", {"sql": "SELECT CAST(amount AS INTEGER) AS id FROM fec_receipts"})
        assert "release_compatibility" not in raw["publication"]["fec_receipts"]
        qualified = _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM memory.{name}"})
        assert qualified["publication"][name]["release_compatibility"] == attestation


@pytest.mark.parametrize("sql,reason", [
    ("SELECT * FROM _spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SELECT * FROM main._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SELECT * FROM memory._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SELECT * FROM MEMORY._SPICY_FEC_PROCESSING_FEC_RECEIPTS", "Internal relations"),
    ("SELECT * FROM memory.main._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("WITH visible AS (SELECT * FROM memory._spicy_fec_processing_fec_receipts) SELECT * FROM visible", "Internal relations"),
    ("TABLE memory._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("PRAGMA version; SELECT * FROM memory._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SELECT * FROM pragma_storage_info('_spicy_fec_processing_fec_receipts')", "Dynamic query functions"),
    ("PRAGMA storage_info('_spicy_fec_processing_fec_receipts')", "Dynamic query functions"),
    ("TABLE _spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SUMMARIZE main._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("DESCRIBE main._spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SHOW _spicy_fec_processing_fec_receipts", "Internal relations"),
    ("SELECT * FROM query('SELECT * FROM _spicy_fec_processing_fec_receipts')", "Dynamic query functions"),
    ("SELECT * FROM query_table('_spicy_fec_processing_fec_receipts')", "Dynamic query functions"),
    ("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM _spicy_fec_processing_fec_receipts'))", "Dynamic query functions"),
    ("SELECT * FROM main.JSON_EXECUTE_SERIALIZED_SQL(json_serialize_sql('SELECT * FROM _spicy_fec_processing_fec_receipts'))", "Dynamic query functions"),
    ("SELECT * FROM main.QUERY('SELECT * FROM _spicy_fec_processing_fec_receipts')", "Dynamic query functions"),
    ("SELECT * FROM main.QUERY_TABLE('_spicy_fec_processing_fec_receipts')", "Dynamic query functions"),
])
def test_mcp_cannot_read_private_restored_processing_relations(tmp_path, monkeypatch, sql, reason):
    _, index, _, _, _ = configure(tmp_path, monkeypatch)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with pytest.raises(Exception, match=reason):
            _tool_data(server.build_server(), "query_sql", {"sql": sql})


def test_discovery_fits_sse_limit_with_many_views_and_partitioned_dependencies(tmp_path, monkeypatch):
    registry_size = len(server.FEC_QUALIFIED_VIEWS)
    specs, index, receipt, _, path = configure(tmp_path, monkeypatch, partition_count=32)
    base = specs[0]
    specs = tuple(replace(base, view=replace(base.view, name=f"fec_test_partitioned_{i}"))
                  for i in range(registry_size))
    receipt["output_membership"]["fec_receipts"] = release.captured_table(index, "fec_receipts")
    view_receipt = receipt["views"][base.view.name]
    view_receipt["dependencies"]["fec_receipts"] = release.captured_table(index, "fec_receipts")
    receipt["views"] = {s.view.name: deepcopy(view_receipt) for s in specs}
    # Keep this SSE discovery case below the unchanged release-receipt byte bound.
    # Main qualification fields enlarge each repeated descriptor. The separate
    # 40-partition control below verifies that oversized release receipts refuse.
    raw = json.dumps(receipt, separators=(",", ":")).encode()
    assert len(raw) <= release.LIMIT
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", release.sha256(raw))
    monkeypatch.setattr(server, "FEC_QUALIFIED_VIEWS", specs)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with TestClient(server.build_app()) as client:
            listed = call(client, "list_sources", {})
            # MCP sends both text and structured content in the same SSE event.
            assert len(json.dumps(listed).encode()) < 1024 * 1024
            summary = listed["structuredContent"]["fec_release"]
            assert summary["status_counts"] == {"compatible": len(specs)}
            assert "views" not in summary
            # Release discovery is bounded independently of view/partition detail.
            assert len(json.dumps(summary).encode()) < 2048
            names = {name for group in listed["structuredContent"]["relationship_views"] for name in group["views"]}
            assert names.issuperset(s.view.name for s in specs)
            described = call(client, "describe_table", {"table": specs[0].view.name, "detail": True})["structuredContent"]
            assert described["publication"]["release_compatibility"]["dependencies"] == view_receipt["dependencies"]


def test_large_partitioned_registry_receipt_keeps_the_existing_byte_refusal(tmp_path):
    specs, index, receipt, _, _ = native_fixture(tmp_path, partition_count=40)
    base = specs[0]
    record = receipt["views"][base.view.name]
    record["dependencies"]["fec_receipts"] = release.captured_table(index, "fec_receipts")
    receipt["views"] = {f"fec_test_partitioned_{i}": deepcopy(record)
                        for i in range(len(server.FEC_QUALIFIED_VIEWS))}
    raw = json.dumps(receipt, separators=(",", ":")).encode()
    assert len(raw) > release.LIMIT
    with pytest.raises(ValueError, match="receipt byte limit"):
        release.parse_receipt(raw, release.sha256(raw))


def test_parent_advancement_refuses_affected_query_but_keeps_old_connection_and_unrelated_views(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    old = connection(index)
    changed = deepcopy(index)
    changed["families"]["filings"]["artifactDigest"] = digest("new-parent")
    with connection(changed) as new:
        monkeypatch.setattr(server, "_get_connection", lambda: new)
        mcp = server.build_server()
        name = specs[0].view.name
        description = _tool_data(mcp, "describe_table", {"table": name})
        assert not description["available"]
        assert "exact_table_pin_mismatch" in description["relationship"]["reason"]
        with pytest.raises(Exception, match="disabled.*exact_table_pin_mismatch"):
            _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
        assert _records(_tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {specs[1].view.name}"})) == [{"id": 1}]
        assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT record_id FROM fec_filings"})) == [{"record_id": "row-1"}]
        monkeypatch.setattr(server, "_get_connection", lambda: old)
        prior = _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
        assert _records(prior) == [{"id": 1}]
        assert prior["publication"][name]["input_publications"]["fec_filings"]["artifact_digest"] == index["families"]["filings"]["artifactDigest"]
    old.close()


@pytest.mark.parametrize("damage", ["missing_pin", "bad_bytes", "unreadable", "image"])
def test_receipt_or_deployment_failure_leaves_raw_tables_queryable(tmp_path, monkeypatch, damage):
    specs, index, _, _, path = configure(tmp_path, monkeypatch)
    if damage == "missing_pin":
        monkeypatch.delenv("SPICY_REGS_FEC_RELEASE_SHA256")
    elif damage == "bad_bytes":
        path.write_text("{}")
    elif damage == "unreadable":
        monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_FILE", str(tmp_path / "absent"))
    else:
        monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", digest("another-image"))
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        listed = _tool_data(mcp, "list_sources", {})
        assert listed["fec_release"]["status_counts"] == {"disabled": len(specs)}
        for spec in specs:
            assert spec.view.name in listed["unavailable_tables"]
            described = _tool_data(mcp, "describe_table", {"table": spec.view.name})
            assert described["relationship"]["release_compatibility"]["status"] == "disabled"
            assert described["relationship"]["release_compatibility"]["reasons"]
        assert _records(_tool_data(mcp, "query_sql", {"sql": "SELECT CAST(amount AS INTEGER) AS id FROM fec_receipts"})) == [{"id": 1}]


def test_refresh_checks_configuration_when_index_unchanged_and_rollback_restores_matched_set(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    old = connection(index)
    publication = server._Publication(index, None)
    monkeypatch.setattr(server, "_read_publication", lambda: publication)
    built = []

    def build(selected=None):
        assert selected == publication
        con = connection(index)
        built.append(con)
        return con

    monkeypatch.setattr(server, "_build_connection", build)
    assert server._refreshed(old) is old
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", digest("wrong-image"))
    incompatible = server._refreshed(old)
    assert incompatible is not old
    name = specs[0].view.name
    assert server._connection_relationships(incompatible)[name]["release_compatibility"]["status"] == "disabled"
    assert old.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    restored = server._refreshed(incompatible)
    assert restored.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    assert server._refreshed(restored) is restored
    for con in [old, *built]:
        con.close()


def test_application_sql_cannot_hide_unlisted_dependency_or_write_statement(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    con = duckdb.connect()
    for name in ["fec_receipts", "fec_reports", "members"]:
        con.execute(f"CREATE TABLE {name}(id INTEGER)")
    for sql in ["SELECT a.id FROM fec_receipts a, members b, fec_reports c", "DROP TABLE members"]:
        modified = replace(specs[0], view=replace(specs[0].view, query=lambda _, sql=sql: sql))
        # Even a matching synthetic receipt cannot enlarge the trusted declaration.
        receipt["views"][modified.view.name]["sql_sha256"] = release.sha256(sql.encode())
        config = capture(tmp_path, (modified,), receipt, consumer)
        result = release.install_views(con, (modified,), config, index, ["fec_receipts", "fec_reports", "members"], {},
                                       read_tables=server._tables_named)
        assert result[modified.view.name]["release_compatibility"]["status"] == "disabled"
        assert con.execute("SELECT count(*) FROM members").fetchone() == (0,)
    con.close()


def test_receipt_cannot_register_extra_sql_or_unknown_view(tmp_path, monkeypatch):
    specs, index, receipt, _, path = configure(tmp_path, monkeypatch)
    receipt["views"]["unregistered"] = {**deepcopy(receipt["views"][specs[0].view.name]), "sql": "SELECT * FROM fec_filings"}
    raw = json.dumps(receipt).encode()
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", release.sha256(raw))
    with connection(index) as con:
        assert "unregistered" not in server._connection_relationships(con)
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT * FROM unregistered")
        assert con.execute(f"SELECT * FROM {specs[1].view.name}").fetchall() == [(1,)]


def test_runtime_measurement_failure_disables_qualified_views_but_preserves_raw_access(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)

    def unreadable(_):
        raise OSError("installation unreadable")

    monkeypatch.setattr(release, "runtime_consumer", unreadable)
    with connection(index) as con:
        state = server._connection_relationships(con)[specs[0].view.name]
        assert "consumer_measurement_unavailable" in state["reason"]
        assert con.execute("SELECT CAST(amount AS INTEGER) AS id FROM fec_receipts").fetchall() == [(1,)]


def test_refresh_parent_change_and_partial_rollback_wait_for_all_pins(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    old = connection(index)
    live = deepcopy(index)
    live["families"]["filings"]["artifactDigest"] = digest("advanced-parent")
    monkeypatch.setattr(server, "_read_publication", lambda: server._Publication(live, None))

    def build(selected=None):
        assert selected is not None
        return connection(selected.index)

    monkeypatch.setattr(server, "_build_connection", build)
    advanced = server._refreshed(old)
    name = specs[0].view.name
    assert server._connection_relationships(advanced)[name]["release_compatibility"]["status"] == "disabled"
    # Data rollback alone cannot admit a mismatched running image.
    live = index
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", digest("wrong-image"))
    intermediate = server._refreshed(advanced)
    assert server._connection_relationships(intermediate)[name]["release_compatibility"]["status"] == "disabled"
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    restored = server._refreshed(intermediate)
    assert restored.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    assert old.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    for con in [old, advanced, intermediate, restored]:
        con.close()
