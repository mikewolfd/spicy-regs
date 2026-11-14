"""R2 maintenance recovery uses mocked table state only; no shared settings change."""
from copy import deepcopy
import json
from pathlib import Path
import runpy

import httpx
import pytest

from spicy_regs import comments_maintenance as guard

PINS = {name: {"table-uuid": name + "-uuid", "current-snapshot-id": 7, "current-schema-id": 3}
        for name in guard.TABLES}
MaintenanceValues = dict[str, str | int | None]
TableConfig = dict[str, str | MaintenanceValues]


class Tables:
    def __init__(self):
        self.configs: dict[str, TableConfig] = {
            "comments": {"compaction": {"state": "enabled", "target_size_mb": "512"}, "interval": "6h",
                         "snapshot_expiration": {"state": "disabled", "max_snapshot_age": "30d",
                                                 "min_snapshots_to_keep": 42}},
            "etl_receipts": {"compaction": {"state": "disabled", "target_size_mb": "64"}, "interval": "2h",
                             "snapshot_expiration": {"state": "enabled", "max_snapshot_age": "14d",
                                                     "min_snapshots_to_keep": 5}},
        }
        self.originals = deepcopy(self.configs)
        self.requests = []
        self.fail_get = None
        self.lose_disable_response = False
        self.fail_restore = False
        self.running = False

    def __call__(self, request):
        assert "/namespaces/default_native/tables/" in request.url.path
        table = request.url.path.split("/")[-2]
        assert table in guard.TABLES
        body = json.loads(request.content) if request.content else None
        self.requests.append((request.method, table, body))
        if request.method == "GET" and self.fail_get == table:
            return httpx.Response(503)
        if request.method == "POST":
            assert isinstance(body, dict)
            assert set(body) == set(self.configs[table]) & set(guard.FIELDS)
            assert all(set(value) <= set(guard.FIELDS[key]) for key, value in body.items())
            disabling = all(value == {"state": "disabled"} for value in body.values())
            if self.fail_restore and table == "comments" and not disabling:
                return httpx.Response(503)
            for key, value in body.items():
                settings = self.configs[table][key]
                assert isinstance(settings, dict)
                settings.update(value)
            if self.lose_disable_response and table == "etl_receipts" and disabling:
                self.lose_disable_response = False
                raise httpx.ReadError("Lost response after update", request=request)
            return httpx.Response(200, json={"success": True, "result": self.configs[table]})
        config = deepcopy(self.configs[table])
        for name in set(guard.FIELDS) & set(config):
            settings = config[name]
            assert isinstance(settings, dict)
            settings["next_eligible_at"] = None if settings["state"] == "disabled" else "2026-11-14T00:00:00Z"
            settings["next_eligible_reason"] = "running" if self.running else settings["state"]
        return httpx.Response(200, json={"success": True, "result": {"maintenance_config": config}})


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.cloudflarestorage.com/account/spicy-regs")
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    monkeypatch.setenv(guard.iceberg.CATALOG_LOCK_ENV, "test-held-lock")
    monkeypatch.setattr(guard, "_selected", lambda: ("default_native", deepcopy(PINS)))
    monkeypatch.setattr(guard, "_uuid", lambda namespace, table: PINS[table]["table-uuid"])
    server = Tables()
    with httpx.Client(transport=httpx.MockTransport(server)) as client:
        yield server, guard.MaintenanceAPI("account", "spicy-regs", client)


def test_both_tables_restore_their_exact_distinct_prior_settings(configured, tmp_path):
    server, api = configured
    state = tmp_path / "maintenance.json"
    guard.suspend(state, api)
    saved = json.loads(state.read_text())
    assert saved["held"] and not saved["restored"]
    assert all(guard._disabled(config) for config in server.configs.values())
    assert saved["tables"]["comments"]["original"]["compaction"]["next_eligible_at"] is not None
    assert saved["tables"]["etl_receipts"]["pin"] == PINS["etl_receipts"]
    assert [(method, name) for method, name, _ in server.requests[:2]] == [
        ("GET", "comments"), ("GET", "etl_receipts")]
    guard.verify(state, api)
    guard.restore(state, api)
    assert server.configs == server.originals
    assert json.loads(state.read_text())["restored"]
    with pytest.raises(ValueError, match="not held"):
        guard.verify(state, api)


def test_capture_failure_changes_neither_table(configured, tmp_path):
    server, api = configured
    server.fail_get = "etl_receipts"
    state = tmp_path / "state.json"
    with pytest.raises(httpx.HTTPStatusError):
        guard.suspend(state, api)
    assert not state.exists()
    assert all(method == "GET" for method, _, _ in server.requests)
    assert server.configs == server.originals


def test_omitted_optional_expiration_stays_absent_as_in_real_table_response(configured, tmp_path):
    server, api = configured
    for config in server.configs.values():
        del config["snapshot_expiration"]
    server.originals = deepcopy(server.configs)
    state = tmp_path / "state.json"
    guard.suspend(state, api)
    guard.verify(state, api)
    guard.restore(state, api)
    assert server.configs == server.originals
    assert all(set(body) == {"compaction"} for method, _, body in server.requests if method == "POST")


def test_partial_disable_with_lost_response_rolls_back_both_tables(configured, tmp_path):
    server, api = configured
    server.lose_disable_response = True
    state = tmp_path / "state.json"
    with pytest.raises(httpx.ReadError):
        guard.suspend(state, api)
    saved = json.loads(state.read_text())
    assert all(entry["disable_attempted"] for entry in saved["tables"].values())
    assert saved["restored"] and not saved["held"]
    assert server.configs == server.originals


def test_restore_continues_after_one_failure_and_can_be_retried(configured, tmp_path):
    server, api = configured
    state = tmp_path / "state.json"
    guard.suspend(state, api)
    server.fail_restore = True
    with pytest.raises(RuntimeError, match="comments"):
        guard.restore(state, api)
    saved = json.loads(state.read_text())
    assert saved["tables"]["etl_receipts"]["restored"]
    assert not saved["tables"]["comments"]["restored"]
    assert server.configs["etl_receipts"] == server.originals["etl_receipts"]
    server.fail_restore = False
    guard.restore(state, api)
    assert server.configs == server.originals


def test_running_operation_refuses_and_restores(configured, tmp_path):
    server, api = configured
    server.running = True
    with pytest.raises(ValueError, match="disabled"):
        guard.suspend(tmp_path / "state.json", api)
    assert server.configs == server.originals


def test_uuid_change_refuses_write_and_restores_only_unchanged_table(configured, tmp_path, monkeypatch):
    server, api = configured
    state = tmp_path / "state.json"
    guard.suspend(state, api)
    changed = deepcopy(PINS)
    changed["comments"]["table-uuid"] = "replacement"
    monkeypatch.setattr(guard, "_selected", lambda: ("default_native", changed))
    monkeypatch.setattr(guard, "_uuid", lambda ns, name: changed[name]["table-uuid"])
    with pytest.raises(ValueError, match="UUID"):
        guard.verify(state, api)
    with pytest.raises(RuntimeError, match="comments"):
        guard.restore(state, api)
    assert server.configs["comments"]["compaction"]["state"] == "disabled"
    assert server.configs["etl_receipts"] == server.originals["etl_receipts"]


def test_no_lock_or_wrong_catalog_never_changes_configuration(configured, tmp_path, monkeypatch):
    server, api = configured
    monkeypatch.delenv(guard.iceberg.CATALOG_LOCK_ENV)
    with pytest.raises(ValueError, match="Hold"):
        guard.suspend(tmp_path / "state.json", api)
    assert server.requests == []
    monkeypatch.setenv(guard.iceberg.CATALOG_LOCK_ENV, "test-held-lock")
    guard.suspend(tmp_path / "state.json", api)
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "unrelated")
    before = len(server.requests)
    with pytest.raises(ValueError, match="selected native"):
        guard.restore(tmp_path / "state.json", api)
    assert len(server.requests) == before


def test_apply_requires_maintenance_state_before_connecting(monkeypatch):
    main = runpy.run_path(str(Path(__file__).parents[1] / "scripts/migrate_comments_attachment_schema.py"))["main"]
    monkeypatch.setattr(guard.iceberg, "is_configured", lambda: True)
    monkeypatch.setenv(guard.iceberg.CATALOG_LOCK_ENV, "test-held-lock")
    monkeypatch.setattr("sys.argv", ["migrate", "--apply", "--table-uuid", "uuid", "--snapshot-id", "1", "--schema-id", "2"])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2


@pytest.fixture
def old_native_catalog(tmp_path, monkeypatch):
    """Real initialized DuckDB v2 table and PyIceberg metadata; only hosted transport is substituted."""
    from types import SimpleNamespace
    from typing import cast

    import duckdb
    import pyarrow as pa
    from pyiceberg.catalog import Catalog as IcebergCatalog
    from pyiceberg.io.pyarrow import PyArrowFileIO, _pyarrow_to_schema_without_ids
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.refs import SnapshotRef
    from pyiceberg.table.snapshots import Snapshot
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.table.update import update_table_metadata

    from spicy_regs.policy_migrations import earlier_policy
    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import regulatory_catalog as native

    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.cloudflarestorage.com/account/spicy-regs")
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "warehouse")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "offline-test")
    monkeypatch.setenv(guard.iceberg.CATALOG_LOCK_ENV, "test-held-lock")
    prior = earlier_policy("comments", "regulations-native-v2").subject_schema
    metadata = new_table_metadata(_pyarrow_to_schema_without_ids(prior), UNPARTITIONED_PARTITION_SPEC,
                                  UNSORTED_SORT_ORDER, tmp_path.as_uri())
    snapshot = Snapshot.model_validate({"snapshot-id": 81, "sequence-number": 1,
                                       "timestamp-ms": 1, "manifest-list": "local"})
    metadata = metadata.model_copy(update={"current_snapshot_id": 81, "snapshots": [snapshot],
        "refs": {"main": SnapshotRef.model_validate({"snapshot-id": 81, "type": "branch"})}})

    class Catalog:
        def __init__(self):
            self.metadata = metadata
            self.commits = []

        def load_table(self, name):
            return Table(name, self.metadata, "local", PyArrowFileIO(), cast(IcebergCatalog, self))

        def commit_table(self, table, requirements, updates):
            for requirement in requirements:
                requirement.validate(self.metadata)
            self.commits.append((requirements, updates))
            self.metadata = update_table_metadata(self.metadata, updates)
            return SimpleNamespace(metadata=self.metadata, metadata_location=table.metadata_location)

        def _delete_old_metadata(self, *args):
            pass

    catalog = Catalog()
    server = Tables()
    original_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: original_client(
        transport=httpx.MockTransport(server), **kwargs))
    monkeypatch.setattr("pyiceberg.catalog.rest.RestCatalog", lambda *args, **kwargs: catalog)
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MiB"}) as con:
        con.execute("ATTACH ':memory:' AS reg_catalog")
        native.ensure_native(con, COMMENT)
        con.execute(f"DROP TABLE {native.qualified(COMMENT)}")
        con.register("prior_shape", pa.Table.from_pylist([], schema=prior))
        con.execute(f"CREATE TABLE {native.qualified(COMMENT)} AS SELECT * FROM prior_shape")
        monkeypatch.setattr(guard.iceberg, "_connect", con.cursor)

        def table_metadata(connection, namespace, name):
            assert namespace == native.namespace()
            if name == "comments":
                return json.loads(catalog.metadata.model_dump_json(by_alias=True))
            assert name == "etl_receipts"
            return deepcopy(PINS[name])

        monkeypatch.setattr(guard.iceberg, "_catalog_metadata", table_metadata)
        yield con, catalog, server


def test_old_schema_guard_requires_migration_pins_then_actual_apply_and_restore(old_native_catalog, tmp_path, monkeypatch):
    from scripts.migrate_comments_attachment_schema import main, table_pin

    con, catalog, server = old_native_catalog
    state = tmp_path / "maintenance.json"
    pin = table_pin(catalog.load_table(("default_native", "comments")))
    assert pin[1] is not None
    expected = (pin[0], pin[1], pin[2])
    # Reproduce the hosted failure through the actual ordinary selection/schema path.
    with pytest.raises(ValueError, match="schema differs from declared policy"):
        guard.run("suspend", state)
    assert not state.exists() and not server.requests
    monkeypatch.setattr("sys.argv", ["maintenance", "suspend", "--state", str(state),
        "--migration-table-uuid", expected[0], "--migration-snapshot-id", str(expected[1]),
        "--migration-schema-id", str(expected[2])])
    guard.main()
    held = json.loads(state.read_text())
    assert held["attachment_migration_pin"] == list(expected)
    assert all(guard._disabled(config) for config in server.configs.values())
    with pytest.raises(ValueError, match="exact migration preview pins"):
        guard.run("verify", state)
    monkeypatch.setattr("sys.argv", ["migrate", "--apply", "--table-uuid", expected[0],
        "--snapshot-id", str(expected[1]), "--schema-id", str(expected[2]), "--maintenance-state", str(state)])
    main()
    assert len(catalog.commits) == 1
    assert catalog.metadata.current_snapshot_id == expected[1]
    assert catalog.metadata.current_schema_id != expected[2]
    with pytest.raises(ValueError, match="migration preview pin"):
        guard.run("verify", state, migration_pin=expected)
    guard.run("restore", state)
    assert server.configs == server.originals
    assert json.loads(state.read_text())["restored"]
    # The independent ordinary writer gate still refuses the unchanged local v2 table.
    with pytest.raises(ValueError, match="schema differs from declared policy"):
        guard.iceberg._selected_catalog_namespace(con, guard.COMMENT)


@pytest.mark.parametrize("broken", ["uuid", "snapshot", "schema", "unknown-layout", "uninitialized"])
def test_migration_selection_refuses_before_maintenance_changes(old_native_catalog, tmp_path, broken):
    from scripts.migrate_comments_attachment_schema import table_pin
    from pyiceberg.types import NestedField, StringType

    con, catalog, server = old_native_catalog
    pin = table_pin(catalog.load_table(("default_native", "comments")))
    assert pin[1] is not None
    expected = (pin[0], pin[1], pin[2])
    if broken == "uuid":
        expected = ("different", expected[1], expected[2])
    elif broken == "snapshot":
        expected = (expected[0], expected[1] + 1, expected[2])
    elif broken == "schema":
        expected = (expected[0], expected[1], expected[2] + 1)
    elif broken == "unknown-layout":
        schema = catalog.metadata.schema()
        schema = schema.model_copy(update={"fields": (*schema.fields, NestedField(9999, "unknown", StringType()))})
        catalog.metadata = catalog.metadata.model_copy(update={"schemas": [schema]})
    else:
        con.execute(f"DELETE FROM {guard.regulatory_catalog.receipts_table()}")
    state = tmp_path / "refused.json"
    with pytest.raises(ValueError):
        guard.run("suspend", state, migration_pin=expected)
    assert not state.exists() and not server.requests and not catalog.commits
    assert server.configs == server.originals


def test_migration_partial_disable_preserves_both_originals(old_native_catalog, tmp_path):
    from scripts.migrate_comments_attachment_schema import table_pin

    _, catalog, server = old_native_catalog
    pin = table_pin(catalog.load_table(("default_native", "comments")))
    assert pin[1] is not None
    expected = (pin[0], pin[1], pin[2])
    server.lose_disable_response = True
    state = tmp_path / "partial.json"
    with pytest.raises(httpx.ReadError):
        guard.run("suspend", state, migration_pin=expected)
    assert server.configs == server.originals
    assert json.loads(state.read_text())["restored"]
    assert not catalog.commits


def test_migration_workflow_supplies_pins_and_always_restores():
    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/migrate-comment-attachments.yml").read_text())
    assert workflow["concurrency"]["group"] == "comments-catalog-write"
    steps = workflow["jobs"]["migrate"]["steps"]
    suspend = next(step for step in steps if step.get("name", "").startswith("Suspend maintenance"))
    assert all(flag in suspend["run"] for flag in (
        "--migration-table-uuid", "--migration-snapshot-id", "--migration-schema-id"))
    restore = next(step for step in steps if step.get("name", "").startswith("Restore exact"))
    assert restore["if"] == "always() && inputs.apply"
    assert "--migration-" not in restore["run"]
