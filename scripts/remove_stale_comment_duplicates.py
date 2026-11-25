"""Fail-closed removal of exact pinned stale duplicate rows in the live comments Iceberg catalog.

Reads a pinned request JSON naming ``expected_table_uuid`` and explicit duplicate pairs
(``drop_modify_date``/``drop_title`` vs ``keep_modify_date``/``keep_title`` where
``drop_modify_date < keep_modify_date``), verifies every target holds exact expected rows
inside a catalog transaction, and removes only the stale rows when ``--execute`` is set under
``SPICY_REGS_CATALOG_LOCK=comments-catalog-write``.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

from loguru import logger

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg

FORMAT = "remove-stale-comment-duplicates/1"
MAX_REQUEST_BYTES = 16_384
MAX_TARGETS = 20
REQUIRED_LOCK = "comments-catalog-write"
TOP_KEYS = frozenset({"format", "expected_table_uuid", "targets"})
TARGET_KEYS = frozenset({
    "agency_code",
    "comment_id",
    "drop_modify_date",
    "drop_title",
    "keep_modify_date",
    "keep_title",
})


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _parse_aware_timestamp(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} is not a valid ISO-8601 timestamp: {value!r}") from exc
    _require(parsed.tzinfo is not None, f"{label} must carry a timezone: {value!r}")
    return parsed


def validate_request(raw: bytes | str | dict[str, Any]) -> dict[str, Any]:
    """Validate and return the pinned stale-duplicate removal request."""
    if isinstance(raw, bytes):
        _require(len(raw) <= MAX_REQUEST_BYTES, "Request exceeds maximum size")
        document: Any = json.loads(raw.decode("utf-8"))
    elif isinstance(raw, str):
        encoded = raw.encode("utf-8")
        _require(len(encoded) <= MAX_REQUEST_BYTES, "Request exceeds maximum size")
        document = json.loads(raw)
    else:
        document = raw

    _require(isinstance(document, dict) and set(document) == TOP_KEYS, "Request keys differ from schema")
    _require(document["format"] == FORMAT, f"Unsupported request format: {document.get('format')!r}")

    table_uuid = document["expected_table_uuid"]
    _require(isinstance(table_uuid, str) and bool(table_uuid.strip()), "expected_table_uuid must be a UUID string")
    try:
        UUID(table_uuid)
    except ValueError as exc:
        raise ValueError(f"Invalid expected_table_uuid: {table_uuid!r}") from exc

    targets = document["targets"]
    _require(
        isinstance(targets, list) and 1 <= len(targets) <= MAX_TARGETS,
        f"targets must contain 1 to {MAX_TARGETS} entries",
    )

    seen_keys: set[tuple[str, str]] = set()
    for index, target in enumerate(targets):
        _require(
            isinstance(target, dict) and set(target) == TARGET_KEYS,
            f"Target {index} keys differ from schema",
        )
        for field in TARGET_KEYS:
            value = target[field]
            _require(
                isinstance(value, str) and bool(value.strip()),
                f"Target {index} field {field!r} must be a non-empty string",
            )
        key = (target["agency_code"], target["comment_id"])
        _require(key not in seen_keys, f"Duplicate target entry for {key[0]}/{key[1]}")
        seen_keys.add(key)

        drop_dt = _parse_aware_timestamp(target["drop_modify_date"], f"Target {index} drop_modify_date")
        keep_dt = _parse_aware_timestamp(target["keep_modify_date"], f"Target {index} keep_modify_date")
        _require(
            drop_dt < keep_dt,
            f"Target {index} ({key[1]}): drop_modify_date must be strictly earlier than keep_modify_date",
        )

    return document


def _snapshot_dict(snapshot: iceberg.CatalogSnapshot) -> dict[str, Any]:
    return asdict(snapshot)


def remove_stale_duplicates(
    con: Any,
    request: dict[str, Any],
    *,
    execute: bool,
) -> dict[str, Any]:
    """Verify exact duplicate pairs and optionally delete the stale row per target in one transaction."""
    validated = validate_request(request)
    if execute:
        _require(
            os.getenv(iceberg.CATALOG_LOCK_ENV) == REQUIRED_LOCK,
            f"Execute mode requires {iceberg.CATALOG_LOCK_ENV}={REQUIRED_LOCK}",
        )

    record_type = RECORD_TYPES["comments"]
    table_sql = iceberg._qualified(record_type)
    expected_uuid = validated["expected_table_uuid"]
    targets = validated["targets"]

    results: list[dict[str, Any]] = []
    before_snapshot: iceberg.CatalogSnapshot | None = None
    removed_rows = 0

    with iceberg._transaction(con):
        before_snapshot = iceberg._read_snapshot(con, record_type)
        _require(
            before_snapshot.table_uuid == expected_uuid,
            f"Catalog table_uuid {before_snapshot.table_uuid} differs from expected {expected_uuid}",
        )

        for target in targets:
            agency = target["agency_code"]
            comment_id = target["comment_id"]
            drop_date = target["drop_modify_date"]
            drop_title = target["drop_title"]
            keep_date = target["keep_modify_date"]
            keep_title = target["keep_title"]

            observed = con.execute(
                f'SELECT modify_date, title FROM {table_sql} WHERE agency_code = ? AND comment_id = ?',
                [agency, comment_id],
            ).fetchall()
            expected_pair = sorted([(drop_date, drop_title), (keep_date, keep_title)])
            actual_pair = sorted((row[0], row[1]) for row in observed)
            _require(
                len(observed) == 2 and actual_pair == expected_pair,
                f"{agency}/{comment_id}: expected exact 2 rows {expected_pair}, observed {actual_pair}",
            )

        if not execute:
            return {
                "format": FORMAT,
                "status": "dry_run_verified",
                "execute": False,
                "expected_table_uuid": expected_uuid,
                "before_snapshot": _snapshot_dict(before_snapshot),
                "after_snapshot": None,
                "targets_verified": len(targets),
                "removed_rows": 0,
                "remaining_duplicates": None,
                "results": [
                    {
                        "agency_code": t["agency_code"],
                        "comment_id": t["comment_id"],
                        "action": "verified_dry_run",
                        "drop_modify_date": t["drop_modify_date"],
                        "keep_modify_date": t["keep_modify_date"],
                    }
                    for t in targets
                ],
            }

        for target in targets:
            agency = target["agency_code"]
            comment_id = target["comment_id"]
            drop_date = target["drop_modify_date"]
            drop_title = target["drop_title"]
            keep_date = target["keep_modify_date"]
            keep_title = target["keep_title"]

            deleted = con.execute(
                f'DELETE FROM {table_sql} WHERE agency_code = ? AND comment_id = ? '
                'AND modify_date = ? AND title = ?',
                [agency, comment_id, drop_date, drop_title],
            ).fetchone()
            deleted_count = deleted[0] if deleted is not None else None
            _require(
                deleted_count == 1,
                f"{agency}/{comment_id}: expected DELETE count 1, got {deleted_count!r}",
            )

            after_target = con.execute(
                f'SELECT modify_date, title FROM {table_sql} WHERE agency_code = ? AND comment_id = ?',
                [agency, comment_id],
            ).fetchall()
            _require(
                after_target == [(keep_date, keep_title)],
                f"{agency}/{comment_id}: post-delete verification failed, observed {after_target!r}",
            )
            removed_rows += 1
            results.append({
                "agency_code": agency,
                "comment_id": comment_id,
                "action": "deleted_stale",
                "removed_modify_date": drop_date,
                "kept_modify_date": keep_date,
            })

    assert before_snapshot is not None
    after_snapshot = iceberg._read_snapshot(con, record_type)
    remaining = iceberg.audit_duplicates(con, record_type)
    receipt = {
        "format": FORMAT,
        "status": "removed" if not remaining else "failed_remaining_duplicates",
        "execute": True,
        "expected_table_uuid": expected_uuid,
        "before_snapshot": _snapshot_dict(before_snapshot),
        "after_snapshot": _snapshot_dict(after_snapshot),
        "targets_verified": len(targets),
        "removed_rows": removed_rows,
        "remaining_duplicates": [
            {"agency_code": agency, "rows": rows, "distinct_keys": distinct}
            for agency, rows, distinct in remaining
        ],
        "results": results,
    }
    if remaining:
        raise RemainingDuplicatesError(
            f"Catalog still holds {len(remaining)} duplicate agency entry/entries after commit",
            receipt=receipt,
        )
    return receipt


class RemainingDuplicatesError(RuntimeError):
    """Raised after commit if ``audit_duplicates`` still detects duplicate comment identities."""

    def __init__(self, message: str, *, receipt: dict[str, Any]) -> None:
        super().__init__(message)
        self.receipt = receipt


def _write_receipt(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True, help="Pinned stale-duplicate removal JSON")
    parser.add_argument("--output", type=Path, required=True, help="Receipt JSON output path")
    parser.add_argument("--execute", action="store_true", help="Commit deletes; default verifies read-only")
    args = parser.parse_args(argv)

    request_bytes = args.request.read_bytes()
    validated = validate_request(request_bytes)
    if not iceberg.is_configured():
        receipt = {
            "format": FORMAT,
            "status": "failed",
            "execute": args.execute,
            "error": "R2 Data Catalog is not configured",
        }
        _write_receipt(args.output, receipt)
        logger.error("R2 Data Catalog is not configured")
        return 1

    con = iceberg._connect()
    try:
        receipt = remove_stale_duplicates(con, validated, execute=args.execute)
    except RemainingDuplicatesError as exc:
        _write_receipt(args.output, exc.receipt)
        logger.error("{}", exc)
        return 1
    except Exception as exc:
        _write_receipt(
            args.output,
            {
                "format": FORMAT,
                "status": "failed",
                "execute": args.execute,
                "expected_table_uuid": validated["expected_table_uuid"],
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        raise
    finally:
        con.close()

    _write_receipt(args.output, receipt)
    print(json.dumps(receipt, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
