"""Offline unit and workflow tests for guarded stale comment duplicate removal."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from unittest.mock import patch

import duckdb
import pytest
import yaml

from scripts import remove_stale_comment_duplicates as script
from spicy_regs.sources.iceberg import CatalogSnapshot

TABLE_UUID = "01a1bb8e-f307-7c62-bbe2-e57e4c6fbac9"


def _sample_request(*, count: int = 2) -> dict:
    base_targets = [
        {
            "agency_code": "USCIS",
            "comment_id": f"USCIS-2026-0364-{27340 + idx}",
            "drop_modify_date": "2026-11-03T20:10:01Z",
            "drop_title": f"Stale Comment {idx}",
            "keep_modify_date": "2026-11-12T17:02:04Z",
            "keep_title": f"Current Comment {idx}",
        }
        for idx in range(count)
    ]
    return {
        "format": script.FORMAT,
        "expected_table_uuid": TABLE_UUID,
        "targets": base_targets,
    }


def _seed_comments_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE comments (agency_code VARCHAR, comment_id VARCHAR, modify_date VARCHAR, title VARCHAR, body VARCHAR)"
    )
    con.executemany(
        "INSERT INTO comments VALUES (?, ?, ?, ?, ?)",
        [
            ("USCIS", "USCIS-2026-0364-27340", "2026-11-03T20:10:01Z", "Stale Comment 0", "same"),
            ("USCIS", "USCIS-2026-0364-27340", "2026-11-12T17:02:04Z", "Current Comment 0", "same"),
            ("USCIS", "USCIS-2026-0364-27341", "2026-11-03T20:10:01Z", "Stale Comment 1", "same"),
            ("USCIS", "USCIS-2026-0364-27341", "2026-11-12T17:02:04Z", "Current Comment 1", "same"),
            ("EPA", "EPA-2026-0001-0001", "2026-11-10T00:00:00Z", "Clean Row", "untouched"),
        ],
    )
    return con


def _patch_iceberg(monkeypatch, *, snapshot: CatalogSnapshot | None = None, audit_result=None) -> None:
    snap = snapshot or CatalogSnapshot(TABLE_UUID, 100, 1)
    monkeypatch.setattr(script.iceberg, "_qualified", lambda _rt: '"comments"')
    monkeypatch.setattr(script.iceberg, "_read_snapshot", lambda _con, _rt: snap)
    monkeypatch.setattr(script.iceberg, "audit_duplicates", lambda _con, _rt: [] if audit_result is None else audit_result)


def test_validate_request_rejects_malformed_inputs():
    valid = _sample_request()
    assert script.validate_request(valid) == valid

    missing_key = dict(valid)
    del missing_key["expected_table_uuid"]
    with pytest.raises(ValueError, match="keys differ"):
        script.validate_request(missing_key)

    extra_key = {**valid, "extra": True}
    with pytest.raises(ValueError, match="keys differ"):
        script.validate_request(extra_key)

    too_many = _sample_request(count=21)
    with pytest.raises(ValueError, match="1 to 20"):
        script.validate_request(too_many)

    duplicate_id = _sample_request(count=2)
    duplicate_id["targets"][1]["comment_id"] = duplicate_id["targets"][0]["comment_id"]
    with pytest.raises(ValueError, match="Duplicate target"):
        script.validate_request(duplicate_id)

    bad_order = _sample_request(count=1)
    bad_order["targets"][0]["drop_modify_date"] = "2026-11-12T17:02:04Z"
    with pytest.raises(ValueError, match="strictly earlier"):
        script.validate_request(bad_order)


def test_dry_run_verifies_without_mutating(monkeypatch):
    con = _seed_comments_db()
    _patch_iceberg(monkeypatch)
    monkeypatch.delenv(script.iceberg.CATALOG_LOCK_ENV, raising=False)

    receipt = script.remove_stale_duplicates(con, _sample_request(), execute=False)
    assert receipt["status"] == "dry_run_verified"
    assert receipt["targets_verified"] == 2
    assert receipt["removed_rows"] == 0
    assert con.execute("SELECT count(*) FROM comments").fetchone() == (5,)
    con.close()


def test_execute_removes_exact_stale_rows_and_preserves_current(monkeypatch):
    con = _seed_comments_db()
    _patch_iceberg(monkeypatch)
    monkeypatch.setenv(script.iceberg.CATALOG_LOCK_ENV, script.REQUIRED_LOCK)

    receipt = script.remove_stale_duplicates(con, _sample_request(), execute=True)
    assert receipt["status"] == "removed"
    assert receipt["removed_rows"] == 2
    assert receipt["remaining_duplicates"] == []
    assert con.execute("SELECT agency_code, comment_id, modify_date, title FROM comments ORDER BY comment_id").fetchall() == [
        ("EPA", "EPA-2026-0001-0001", "2026-11-10T00:00:00Z", "Clean Row"),
        ("USCIS", "USCIS-2026-0364-27340", "2026-11-12T17:02:04Z", "Current Comment 0"),
        ("USCIS", "USCIS-2026-0364-27341", "2026-11-12T17:02:04Z", "Current Comment 1"),
    ]
    con.close()


def test_execute_refuses_unlocked_write(monkeypatch):
    con = _seed_comments_db()
    _patch_iceberg(monkeypatch)
    monkeypatch.delenv(script.iceberg.CATALOG_LOCK_ENV, raising=False)

    with pytest.raises(ValueError, match=script.iceberg.CATALOG_LOCK_ENV):
        script.remove_stale_duplicates(con, _sample_request(), execute=True)
    assert con.execute("SELECT count(*) FROM comments").fetchone() == (5,)
    con.close()


def test_execute_refuses_table_uuid_mismatch(monkeypatch):
    con = _seed_comments_db()
    _patch_iceberg(monkeypatch, snapshot=CatalogSnapshot("ffffffff-ffff-4fff-8fff-ffffffffffff", 100, 1))
    monkeypatch.setenv(script.iceberg.CATALOG_LOCK_ENV, script.REQUIRED_LOCK)

    with pytest.raises(ValueError, match="table_uuid"):
        script.remove_stale_duplicates(con, _sample_request(), execute=True)
    assert con.execute("SELECT count(*) FROM comments").fetchone() == (5,)
    con.close()


def test_execute_rolls_back_on_single_or_triple_or_mismatched_target(monkeypatch):
    con = _seed_comments_db()
    _patch_iceberg(monkeypatch)
    monkeypatch.setenv(script.iceberg.CATALOG_LOCK_ENV, script.REQUIRED_LOCK)

    # First target is valid, second target has a mismatched title -> full rollback
    req = _sample_request(count=2)
    req["targets"][1]["keep_title"] = "Unexpected Title"
    with pytest.raises(ValueError, match="expected exact 2 rows"):
        script.remove_stale_duplicates(con, req, execute=True)
    assert con.execute("SELECT count(*) FROM comments").fetchone() == (5,)

    # Only 1 row for target -> refused
    con.execute("DELETE FROM comments WHERE comment_id = 'USCIS-2026-0364-27341' AND modify_date = '2026-11-03T20:10:01Z'")
    with pytest.raises(ValueError, match="expected exact 2 rows"):
        script.remove_stale_duplicates(con, _sample_request(count=2), execute=True)
    assert con.execute("SELECT count(*) FROM comments WHERE comment_id = 'USCIS-2026-0364-27340'").fetchone() == (2,)

    # 3 rows for target -> refused
    con.execute(
        "INSERT INTO comments VALUES ('USCIS', 'USCIS-2026-0364-27340', '2026-11-01T00:00:00Z', 'Extra', 'same')"
    )
    with pytest.raises(ValueError, match="expected exact 2 rows"):
        script.remove_stale_duplicates(con, _sample_request(count=1), execute=True)
    assert con.execute("SELECT count(*) FROM comments WHERE comment_id = 'USCIS-2026-0364-27340'").fetchone() == (3,)
    con.close()


def test_execute_fails_loudly_when_audit_detects_remaining_duplicates(monkeypatch, tmp_path):
    con = _seed_comments_db()
    _patch_iceberg(monkeypatch, audit_result=[("EPA", 10, 9)])
    monkeypatch.setattr(script.iceberg, "is_configured", lambda: True)
    monkeypatch.setattr(script.iceberg, "_connect", lambda: con)
    monkeypatch.setenv(script.iceberg.CATALOG_LOCK_ENV, script.REQUIRED_LOCK)

    req_path = tmp_path / "req.json"
    out_path = tmp_path / "out.json"
    req_path.write_text(json.dumps(_sample_request()))

    with patch.object(sys, "argv", ["remove_stale_comment_duplicates.py", "--request", str(req_path), "--output", str(out_path), "--execute"]):
        code = script.main()
    assert code == 1
    receipt = json.loads(out_path.read_text())
    assert receipt["status"] == "failed_remaining_duplicates"
    assert receipt["remaining_duplicates"] == [{"agency_code": "EPA", "rows": 10, "distinct_keys": 9}]


def test_workflow_contract():
    path = Path(__file__).parents[1] / ".github/workflows/remove-stale-comment-duplicates.yml"
    workflow = yaml.safe_load(path.read_text())
    assert "schedule" not in workflow[True]
    assert set(workflow[True]["workflow_dispatch"]["inputs"]) == {"request", "execute"}
    assert workflow[True]["workflow_dispatch"]["inputs"]["execute"]["default"] is False
    assert workflow["concurrency"] == {
        "group": "comments-catalog-write",
        "cancel-in-progress": False,
        "queue": "max",
    }
    assert workflow["env"]["SPICY_REGS_CATALOG_LOCK"] == "comments-catalog-write"
