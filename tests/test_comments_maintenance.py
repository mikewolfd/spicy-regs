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


class Tables:
    def __init__(self):
        self.configs = {
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
            assert set(body) == set(guard.FIELDS)
            assert all(set(value) <= set(guard.FIELDS[key]) for key, value in body.items())
            disabling = all(value == {"state": "disabled"} for value in body.values())
            if self.fail_restore and table == "comments" and not disabling:
                return httpx.Response(503)
            for key, value in body.items():
                self.configs[table][key].update(value)
            if self.lose_disable_response and table == "etl_receipts" and disabling:
                self.lose_disable_response = False
                raise httpx.ReadError("Lost response after update", request=request)
            return httpx.Response(200, json={"success": True, "result": self.configs[table]})
        config = deepcopy(self.configs[table])
        for name in guard.FIELDS:
            config[name]["next_eligible_at"] = None if config[name]["state"] == "disabled" else "2026-11-14T00:00:00Z"
            config[name]["next_eligible_reason"] = "running" if self.running else config[name]["state"]
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
