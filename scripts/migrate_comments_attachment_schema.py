#!/usr/bin/env python3
"""Append the nullable attachment fields to an existing native comments catalog."""

from __future__ import annotations

import argparse
import json
from contextlib import closing
from os import environ, getenv
from pathlib import Path

import pyarrow as pa

from spicy_regs.comment_attachment_schema import attachment_additions
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, regulatory_catalog


def table_pin(table) -> tuple[str, int | None, int]:
    metadata = table.metadata
    return str(metadata.table_uuid), metadata.current_snapshot_id, metadata.current_schema_id


def migrate(table, *, expected: tuple[str, int | None, int], apply: bool = False) -> dict:
    """Plan or commit a schema-only update against the named table and snapshot."""
    from pyiceberg.table.update import AssertRefSnapshotId
    from pyiceberg.types import ListType, LongType, StringType

    if table_pin(table) != expected:
        raise ValueError("Comments catalog pin changed")
    if table.metadata.current_snapshot_id is None or table.metadata.current_snapshot_id < 0:
        raise ValueError("Comments catalog has no selected snapshot")

    added = attachment_additions(table.schema())
    plan = {"table_uuid": expected[0], "snapshot_id": expected[1], "schema_id": expected[2], "added": []}
    if not added:
        return plan

    def iceberg_type(dtype):
        if pa.types.is_string(dtype):
            return StringType()
        if pa.types.is_int64(dtype):
            return LongType()
        if pa.types.is_list(dtype) and pa.types.is_string(dtype.value_type) and dtype.value_field.nullable:
            return ListType(element_id=1, element_type=StringType(), element_required=False)
        raise ValueError(f"Unsupported comments migration field type: {dtype}")

    plan["added"] = [field.name for field in added]
    if apply:
        with table.transaction() as transaction:
            with transaction.update_schema() as update:
                for field in added:
                    update.add_column(("attachments", field.name), iceberg_type(field.type))
            # PyIceberg adds UUID and current-schema requirements. Pin the main branch
            # at the REST commit as well, so a concurrent data write refuses the update.
            transaction._stage((), (AssertRefSnapshotId.model_validate({"ref": "main", "snapshot-id": expected[1]}),))
        table.refresh()
        after = table_pin(table)
        if after[:2] != expected[:2] or attachment_additions(table.schema()):
            raise RuntimeError("Comments schema commit needs read-back verification before ingestion")
        plan["new_schema_id"] = after[2]
    return plan


def main() -> None:
    from pyiceberg.catalog.rest import RestCatalog

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply the schema-only update")
    parser.add_argument("--table-uuid")
    parser.add_argument("--snapshot-id", type=int)
    parser.add_argument("--schema-id", type=int)
    parser.add_argument("--maintenance-state", type=Path, help="Saved, verified guard for both native tables")
    args = parser.parse_args()
    if not iceberg.is_configured():
        parser.error("Configure the R2 Data Catalog first")
    pins = (args.table_uuid, args.snapshot_id, args.schema_id)
    if args.apply and (None in pins or not getenv(iceberg.CATALOG_LOCK_ENV) or args.maintenance_state is None):
        parser.error("Apply requires the preview pins, comments catalog lock, and --maintenance-state")
    if any(value is not None for value in pins) and None in pins:
        parser.error("Provide all three catalog pins together")

    guard = None
    if args.apply:
        from spicy_regs.comments_maintenance import run

        guard = run("verify", args.maintenance_state, migration_pin=pins)

    catalog = RestCatalog(
        "comments",
        uri=environ["R2_CATALOG_URI"],
        warehouse=environ["R2_CATALOG_WAREHOUSE"],
        token=environ["R2_CATALOG_TOKEN"],
    )
    with closing(iceberg._connect()) as con:
        if not regulatory_catalog.initialized(con, COMMENT.name):
            raise ValueError("Comments catalog has no qualified initialization receipt")
        expected = iceberg._read_snapshot(con, COMMENT, namespace=regulatory_catalog.namespace())
    table = catalog.load_table((regulatory_catalog.namespace(), COMMENT.name))
    pin = (expected.table_uuid, expected.snapshot_id, expected.schema_id)
    if None not in pins and pins != pin:
        raise ValueError("Comments catalog differs from the supplied preview pin")
    if guard is not None and (
        guard["namespace"] != regulatory_catalog.namespace()
        or guard["tables"]["comments"]["pin"]["table-uuid"] != pin[0]
    ):
        raise ValueError("Maintenance guard differs from the selected comments table")
    print(json.dumps(migrate(table, expected=pin, apply=args.apply)))


if __name__ == "__main__":
    main()
