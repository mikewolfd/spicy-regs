"""Discovery avoids row restoration; one request budget also bounds cold admission."""
import asyncio
from time import monotonic, sleep
from threading import Event, Timer

import pytest

from spicy_regs import etl_bulk, local_data, mcp_server as server, selected_generations
from spicy_regs.runtime_bounds import RequestBound, checkpoint
from tests.test_local_native_selection import selected_family
from tests.test_mcp_server import _records, _tool_data


@pytest.fixture(autouse=True)
def reset_connections():
    server._reset_connection_cache()
    yield
    for con in (server._cached_connection, server._cached_discovery_connection):
        if con is not None:
            con.close()
    server._reset_connection_cache()


def test_relationship_declarations_do_not_touch_source_schemas():
    from spicy_regs.relationship_views import describe_relationship_views

    class NoDatabaseWork:
        def execute(self, *args, **kwargs):
            raise AssertionError('Discovery must not bind relationship SQL or read source schemas')

    pin = {'status': 'managed_generation', 'artifact_digest': 'sha256:' + 'a' * 64}
    registry = describe_relationship_views(NoDatabaseWork(), ['members'], {'members': pin})
    member_views = [item for item in registry.values() if item['dependencies'] == ['members']]
    assert member_views
    for entry in member_views:
        assert entry['status'] == 'deferred'
        assert entry['metadata']['query_admission'] == 'pending_relationship_binding'
        assert entry['metadata']['input_publications'] == {'members': pin}
        assert 'column_lineage' not in entry['metadata']
        assert entry['metadata']['availability_basis'] == 'Declared dependencies only; runtime schema binding and query admission remain pending.'
    assert registry['comment_document_references']['status'] == 'unavailable'


def test_general_discovery_defers_binding_but_query_preserves_schema_refusal(monkeypatch):
    import duckdb

    def connection(publication=None):
        con = duckdb.connect()
        con.execute('CREATE TABLE members (bioguide_id VARCHAR, fec_ids_json VARCHAR, observed_at VARCHAR, roster VARCHAR)')
        con.execute("INSERT INTO members VALUES ('X', '[\"S8WA00194\"]', 'today', 'current')")
        con.execute('CREATE TABLE comments (comment_id VARCHAR, docket_id VARCHAR)')
        server._install_relationship_views(con)
        server._apply_security_settings(con)
        return con

    monkeypatch.setattr(server, '_build_connection', connection)
    mcp = server.build_server()
    listed = _tool_data(mcp, 'list_sources', {})
    # Find the maintained name rather than assuming one from the source field.
    discovery_connection = server._cached_discovery_connection
    assert discovery_connection is not None
    registry = server._connection_relationships(discovery_connection)
    name = next(name for name, item in registry.items()
                if item['dependencies'] == ['members'] and name.endswith('_occurrences'))
    assert name in {name for group in listed['deferred_views'] for name in group['views']}
    described = _tool_data(mcp, 'describe_table', {'table': name})
    assert not described['available'] and described['columns'] == []
    assert described['metadata']['query_admission'] == 'pending_relationship_binding'
    assert server._cached_connection is None
    result = _tool_data(mcp, 'query_sql', {'sql': f'SELECT target_key FROM "{name}"'})
    assert _records(result) == [{'target_key': 'S8WA00194'}]
    with pytest.raises(Exception, match='comment_document_references'):
        _tool_data(mcp, 'query_sql', {'sql': 'SELECT * FROM comment_document_references'})
    query_connection = getattr(server, '_cached_connection')
    assert query_connection is not None
    query_registry = server._connection_relationships(query_connection)
    assert query_registry['comment_document_references']['status'] == 'unsupported'
    assert server._cached_connection is not server._cached_discovery_connection


def test_queries_bind_only_requested_relationships_and_expand_same_pins(monkeypatch):
    import duckdb

    builds = []
    selected = {'families': {}, 'test_generation': 'first'}

    def connection(publication=None):
        index = selected if publication is None else publication.index
        builds.append((index, server._requested_relations.get()))
        con = duckdb.connect()
        con.execute('CREATE TABLE _spicy_publication (snapshot VARCHAR)')
        import json
        con.execute('INSERT INTO _spicy_publication VALUES (?)', [json.dumps(index)])
        con.execute('CREATE TABLE members (bioguide_id VARCHAR, fec_ids_json VARCHAR, observed_at VARCHAR, roster VARCHAR)')
        con.execute("INSERT INTO members VALUES ('X', '[\"S8WA00194\"]', 'today', 'current')")
        con.execute('CREATE TABLE comments (comment_id VARCHAR, docket_id VARCHAR, comment_reference_values_json VARCHAR, '
                    'comment_on_document_id VARCHAR, comment_on_object_id VARCHAR, original_document_id VARCHAR)')
        server._install_relationship_views(con)
        server._apply_security_settings(con)
        return con

    monkeypatch.setattr(server, '_build_connection', connection)
    mcp = server.build_server()
    assert _records(_tool_data(mcp, 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})) == [{'bioguide_id': 'X'}]
    con = getattr(server, '_cached_connection')
    assert con is not None
    registry = server._connection_relationships(con)
    assert all(item['status'] != 'available' for item in registry.values())
    names = [name for name, item in registry.items()
             if item['dependencies'] == ['members'] and name.endswith('_pairs')]
    assert names
    first = con
    for name in names:
        _tool_data(mcp, 'query_sql', {'sql': f'SELECT * FROM "{name}"'})
    con = getattr(server, '_cached_connection')
    assert con is not None and con is not first
    assert builds[1][0] == selected
    assert all(item['status'] != 'available' for item in server._connection_relationships(con).values()
               if item['dependencies'] != ['members'])
    assert _records(_tool_data(mcp, 'query_sql', {'sql': 'SELECT * FROM comment_document_references'})) == []
    later = getattr(server, '_cached_connection')
    assert later is not None and later is not con
    assert server._connection_relationships(later)['comment_document_references']['status'] == 'available'
    assert builds[-1][0] == selected
    # A changed selected pointer takes the maintained refresh path, rather
    # than expanding a stale connection with new-generation source views.
    selected = {'families': {}, 'test_generation': 'second'}
    monkeypatch.setattr(server, '_read_publication', lambda: server._Publication(selected, None, None))
    monkeypatch.setattr(server, '_CONNECTION_TTL_SECONDS', 0)
    _tool_data(mcp, 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})
    assert builds[-1][0] == selected
    with pytest.raises(Exception, match='read-only'):
        _tool_data(mcp, 'query_sql', {'sql': 'CREATE TABLE bypass AS SELECT 1'})


@pytest.mark.parametrize('changed_record', ['_spicy_local_selection', '_spicy_fec_release'])
def test_relationship_expansion_refuses_changed_local_selection_or_release(monkeypatch, changed_record):
    import duckdb
    import json

    def connection(version):
        con = duckdb.connect()
        for name, record in {
            '_spicy_relationships': {'needed': {'status': 'deferred'}},
            '_spicy_local_selection': {'signatures': {}, 'generation': version if changed_record == '_spicy_local_selection' else 'held'},
            '_spicy_fec_release': {'receipt': version if changed_record == '_spicy_fec_release' else 'held'},
        }.items():
            con.execute(f'CREATE TABLE {name} (snapshot VARCHAR)')
            con.execute(f'INSERT INTO {name} VALUES (?)', [json.dumps(record)])
        return con

    with connection('first') as current:
        replacement = connection('second')
        monkeypatch.setattr(server, '_build_connection', lambda publication: replacement)
        token = server._requested_relations.set({'needed'})
        try:
            with pytest.raises(RuntimeError, match='publication or release changed'):
                server._expand_query_connection(current)
            assert current.execute('SELECT 1').fetchone() == (1,)
            with pytest.raises(duckdb.ConnectionException, match='closed'):
                replacement.execute('SELECT 1')
        finally:
            server._requested_relations.reset(token)


def test_financial_discovery_defers_exact_rows_but_query_still_admits(tmp_path, monkeypatch):
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from tests.test_mcp_fec_release import configure, connection
    specs, index, *_ = configure(tmp_path, monkeypatch)
    restored = []
    original = ReceiptAdapter.restore_originals

    def restoring(self, table, *args, **kwargs):
        restored.append(table)
        return original(self, table, *args, **kwargs)

    monkeypatch.setattr(ReceiptAdapter, 'restore_originals', restoring)
    monkeypatch.setattr(server, '_build_connection', lambda: connection(index))
    mcp = server.build_server()
    listed = _tool_data(mcp, 'list_sources', {})
    assert not restored
    assert listed['fec_release']['status_counts'] == {'compatible': len(specs)}
    assert {name for entry in listed['deferred_views'] for name in entry['views']} >= {spec.view.name for spec in specs}
    described = _tool_data(mcp, 'describe_table', {'table': specs[0].view.name, 'detail': True})
    assert not restored
    assert specs[0].view.name not in listed['unavailable_tables']
    assert described['available'] is False
    assert described['relationship']['status'] == 'deferred'
    assert described['metadata']['query_admission'] == 'pending_receipt_rows'
    assert described['columns'] == []
    assert server._cached_connection is None
    result = _tool_data(mcp, 'query_sql', {'sql': f'SELECT * FROM {specs[0].view.name}'})
    assert _records(result) == [{'id': 1}]
    assert 'fec_receipts' in restored
    assert server._cached_connection is not server._cached_discovery_connection
    query_connection = getattr(server, '_cached_connection')
    assert query_connection is not None
    registry = server._connection_relationships(query_connection)
    assert registry[specs[0].view.name]['status'] == 'available'
    assert all(registry[spec.view.name]['status'] == 'deferred' for spec in specs[1:])


def test_native_discovery_keeps_pins_without_receipt_row_replay(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    original = etl_receipts.validate_receipt_bundle
    admissions = []
    def validate(*args, **kwargs):
        admissions.append(kwargs['generation_id'])
        return original(*args, **kwargs)
    monkeypatch.setattr(etl_receipts, 'validate_receipt_bundle', validate)
    mcp = server.build_server()
    description = _tool_data(mcp, 'describe_table', {'table': 'members'})
    assert description['available'] and not admissions
    assert 'receipt row joins remain pending' in description['publication']['verification']
    query = _tool_data(mcp, 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})
    assert _records(query) == [{'bioguide_id': 'X'}] and admissions
    # Descriptor-only mode still refuses changed exact bytes.
    server._reset_connection_cache()
    selected = local_data.local_selection(tmp_path)
    selected.native['members'].subjects[0].write_bytes(b'changed')
    with pytest.raises(Exception, match='changed|Parquet'):
        _tool_data(mcp, 'describe_table', {'table': 'members'})


@pytest.mark.parametrize('cancellation', ['deadline', 'external'])
def test_cold_native_hash_admission_cancelled_never_enters_cache(tmp_path, monkeypatch, cancellation):
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    original = selected_generations.stream_sha256
    entered, ended = Event(), Event()
    def slow_hash(stream):
        entered.set()
        try:
            while True:
                sleep(.005)
                checkpoint()
        finally:
            ended.set()
    monkeypatch.setattr(selected_generations, 'stream_sha256', slow_hash)
    bound = RequestBound(.10 if cancellation == 'deadline' else 5, 'cold input admission cancelled')
    timer = Timer(.08, bound.cancel) if cancellation == 'external' else None
    started = monotonic()
    if timer:
        timer.start()
    try:
        with pytest.raises(TimeoutError, match='cold input admission'):
            with bound.scope():
                server._get_connection()
    finally:
        if timer:
            timer.cancel()
    assert entered.is_set() and ended.is_set()
    assert monotonic() - started < 1
    assert server._cached_connection is None
    monkeypatch.setattr(selected_generations, 'stream_sha256', original)
    assert server._get_connection().execute('SELECT bioguide_id FROM members').fetchall() == [('X',)]


def test_request_timeout_covers_cold_build_and_worker_stops(tmp_path, monkeypatch):
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    monkeypatch.setattr(server, 'STATEMENT_TIMEOUT_SECONDS', .10)
    stopped = Event()
    def slow_hash(stream):
        try:
            while True:
                sleep(.005)
                checkpoint()
        finally:
            stopped.set()
    monkeypatch.setattr(selected_generations, 'stream_sha256', slow_hash)
    started = monotonic()
    with pytest.raises(Exception, match='including input admission'):
        _tool_data(server.build_server(), 'query_sql', {'sql': 'SELECT bioguide_id FROM members'})
    assert stopped.wait(1)
    assert monotonic() - started < 1.5
    assert server._cached_connection is None


def test_external_tool_cancellation_stops_cold_worker(tmp_path, monkeypatch):
    selected_family(tmp_path, monkeypatch)
    monkeypatch.setattr(server, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(server, 'TABLES', ())
    monkeypatch.setattr(server, 'STATEMENT_TIMEOUT_SECONDS', 5)
    entered, stopped = Event(), Event()
    def slow_hash(stream):
        entered.set()
        try:
            while True:
                sleep(.005)
                checkpoint()
        finally:
            stopped.set()
    monkeypatch.setattr(selected_generations, 'stream_sha256', slow_hash)
    mcp = server.build_server()
    async def request():
        call = asyncio.create_task(mcp.call_tool('query_sql', {'sql': 'SELECT bioguide_id FROM members'}))
        while not entered.is_set():
            await asyncio.sleep(.005)
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
    asyncio.run(request())
    assert stopped.wait(1)
    assert server._cached_connection is None


def test_bulk_admission_sql_interrupts_and_removes_private_work(tmp_path):
    with pytest.raises(TimeoutError, match='bulk admission cancelled'):
        with RequestBound(.10, 'bulk admission cancelled').scope():
            with etl_bulk.bulk_connection(tmp_path) as (con, _):
                con.execute('SELECT sum(hash(i)) FROM range(10000000000) AS t(i)').fetchone()
    assert list(tmp_path.iterdir()) == []
    with etl_bulk.bulk_connection(tmp_path) as (con, _):
        assert con.execute('SELECT 1').fetchone() == (1,)


def test_discovery_cache_age_does_not_restart_on_every_read(monkeypatch):
    token = server._discovery_only.set(True)
    try:
        values = iter([10., 20., 311., 311.])
        monkeypatch.setattr(server, '_monotonic', lambda: next(values))
        monkeypatch.setattr(server, '_build_connection', lambda: 'old')
        monkeypatch.setattr(server, '_refreshed', lambda old: 'new')
        assert server._get_connection() == 'old'
        assert server._get_connection() == 'old'
        assert server._cached_discovery_at == 10
        assert server._get_connection() == 'new'
    finally:
        server._discovery_only.reset(token)
        server._reset_connection_cache()


def test_old_receipt_only_discovery_is_hidden_without_payload_restore(tmp_path, monkeypatch):
    from spicy_regs import etl_receipts
    from spicy_regs.fec_receipt_adapter import ReceiptAdapter
    from spicy_regs.selected_generations import SelectedDataset, remember_selection
    from tests.test_fec_historical_policy_readback import old_pair
    from mcp.server.mcpserver.exceptions import ToolError

    row = {'collection_id': 'old-collection', 'source_family': 'bulk', 'record_count': 1,
           'conversion_inputs': {'collection_id': 'old-collection', 'source_family': 'bulk', 'record_count': 1}}
    _, subject, receipt = old_pair(tmp_path / 'bundle', 'fec_collections', row)
    assert subject is None
    root = tmp_path / 'state'
    remember_selection(root, [SelectedDataset('fec_collections', (), receipt, 'historical')])
    def refuse_restore(*args, **kwargs):
        pytest.fail('Discovery replayed receipt payloads or restored financial rows')
    monkeypatch.setattr(etl_receipts, 'select_receipts', refuse_restore)
    monkeypatch.setattr(etl_receipts, 'validate_receipt_bundle', refuse_restore)
    monkeypatch.setattr(ReceiptAdapter, 'restore_originals', refuse_restore)
    selection = local_data.local_selection(root, admit_rows=False)
    assert 'fec_collections' not in selection.files
    assert selection.native['fec_collections'].receipts == receipt
    monkeypatch.setattr(server, 'DATA_DIR', root)
    monkeypatch.setattr(server, 'TABLES', ())
    mcp = server.build_server()
    listed = _tool_data(mcp, 'list_sources', {})
    assert 'fec_collections' not in {item['table'] for group in listed['subjects'] for item in group['tables']}
    with pytest.raises(ToolError, match='Unknown table'):
        _tool_data(mcp, 'describe_table', {'table': 'fec_collections'})
    assert server._cached_connection is None
