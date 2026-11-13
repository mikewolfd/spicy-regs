"""Native catalog admission is enforced by the real subject/receipt writer."""
from dataclasses import replace

import duckdb
import pytest

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg


def test_unregistered_dataset_is_refused_before_catalog_mutation():
    fixture = replace(COMMENT, name='fixture_comments')
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        with pytest.raises(ValueError, match="Unsupported regulatory catalog dataset"):
            iceberg._ensure_table(connection, fixture)
        assert connection.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog='reg_catalog'").fetchall() == []


def test_comments_catalog_admits_mixed_receipts_only_at_their_exact_shape(tmp_path):
    import pyarrow as pa

    from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, split_record
    from spicy_regs.policy_migrations import earlier_policy
    from spicy_regs.sources import regulatory_catalog as native
    from spicy_regs.transforms.parquet_rows import write_rows
    from spicy_regs.transforms.regulations_receipts import COMMENTS_PROMOTION_PROCESSOR, policy
    from spicy_regs.transforms.regulations_shape import shape_record

    current = policy("comments")
    prior = earlier_policy("comments", "regulations-native-v2")
    assert native.policy("comments") == current
    raw_old = {"comment_id": "old", "attachments_json": '[{"attachment_id":"a","title":"old"}]'}
    raw_new = {"comment_id": "new", "attachments_json": '[{"attachment_id":"b","authors":["A"],"docOrder":0}]'}
    def prepare(raw, declared, *, promoted=False):
        row = shape_record("comments", raw)
        subject = pa.Table.from_pylist([row], schema=declared.subject_schema).to_pylist()[0]
        processing = {} if promoted else {"raw_source_record": raw, "input_metadata": {}}
        return split_record(
            declared, subject | processing,
            ReceiptContext("selected", raw["comment_id"], COMMENTS_PROMOTION_PROCESSOR if promoted else "test", [
                {"source_id": "fixture", "source_uri": None, "sha256": None, "locator": raw["comment_id"],
                 "body_version": "selected"},
            ]),
        )

    old, old_receipt = prepare(raw_old, prior)
    new, new_receipt = prepare(raw_new, current)
    assert old_receipt["policy_version"] == prior.policy_version
    assert new_receipt["policy_version"] == current.policy_version

    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        native.ensure_native(connection, COMMENT)
        rows = tmp_path / "subjects.parquet"
        receipts = tmp_path / "receipts.parquet"
        write_rows([old, new], rows, current.subject_schema)
        write_rows([old_receipt, new_receipt], receipts, RECEIPT_SCHEMA)
        connection.execute(f"INSERT INTO {native.qualified(COMMENT)} SELECT * FROM read_parquet(?)", [str(rows)])
        connection.execute(f"INSERT INTO {native.receipts_table()} SELECT * FROM read_parquet(?)", [str(receipts)])
        table = native.processing_table(connection, COMMENT)
        assert connection.execute(f"SELECT comment_id, attachments_json FROM {table} ORDER BY comment_id").fetchall() == [
            ("new", raw_new["attachments_json"]), ("old", raw_old["attachments_json"]),
        ]

        promoted, promoted_receipt = prepare(raw_old, prior, promoted=True)
        write_rows([promoted_receipt, new_receipt], receipts, RECEIPT_SCHEMA)
        connection.execute(f"DELETE FROM {native.receipts_table()} WHERE outcome='accepted'")
        connection.execute(f"INSERT INTO {native.receipts_table()} SELECT * FROM read_parquet(?)", [str(receipts)])
        flagged = native.processing_table(connection, COMMENT, include_comment_promotion=True)
        assert connection.execute(f"SELECT comment_id, _promoted_v2 FROM {flagged} ORDER BY comment_id").fetchall() == [
            ("new", False), ("old", True),
        ]
        held = connection.execute(f"SELECT attachments_json FROM {flagged} WHERE comment_id='old'").fetchone()[0]
        from spicy_regs.pipelines.comment_fields_write import only_adds_attachments

        reread = '[{"title": "old", "attachment_id": "a", "docOrder": 1}]'
        assert not only_adds_attachments(reread, held)
        assert only_adds_attachments(reread, held, promoted_v2=True)

        for edit, match in (
            ({"authors": []}, "v2 receipt cannot attest"),
            ({"doc_order": 0}, "v2 receipt cannot attest"),
            ({"title": "changed"}, "Missing, ambiguous or reused subject receipt"),
            ({"attachment_id": "unrelated"}, "Missing, ambiguous or reused subject receipt"),
        ):
            mutated = old | {"attachments": [old["attachments"][0] | edit]}
            write_rows([mutated, new], rows, current.subject_schema)
            connection.execute(f"DELETE FROM {native.qualified(COMMENT)}")
            connection.execute(f"INSERT INTO {native.qualified(COMMENT)} SELECT * FROM read_parquet(?)", [str(rows)])
            with pytest.raises(ValueError, match=match):
                native.processing_table(connection, COMMENT)

        # Old-policy support is limited to the catalog read. Published v3 admission remains exact.
        from spicy_regs.etl_receipts import validate_receipt_bundle

        write_rows([old, new], rows, current.subject_schema)
        with pytest.raises(ValueError, match="Missing, ambiguous or reused subject receipt"):
            validate_receipt_bundle({"comments": [rows]}, [receipts], [current], generation_id="selected")



def test_comments_schema_migration_only_appends_nullable_fields_and_checks_snapshot(tmp_path):
    from types import SimpleNamespace

    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids, schema_to_pyarrow
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
    from pyiceberg.table.snapshots import Snapshot
    from pyiceberg.table.update import update_table_metadata

    from scripts.migrate_comments_attachment_schema import migrate, table_pin
    from spicy_regs.policy_migrations import earlier_policy
    from spicy_regs.transforms.regulations_receipts import policy

    prior = earlier_policy("comments", "regulations-native-v2").subject_schema
    metadata = new_table_metadata(
        _pyarrow_to_schema_without_ids(prior), UNPARTITIONED_PARTITION_SPEC,
        UNSORTED_SORT_ORDER, tmp_path.as_uri(),
    )
    snapshot = Snapshot(snapshot_id=81, sequence_number=1, timestamp_ms=1, manifest_list="local")
    ref = SnapshotRef(snapshot_id=81, snapshot_ref_type=SnapshotRefType.BRANCH)
    metadata = metadata.model_copy(update={"current_snapshot_id": 81, "snapshots": [snapshot], "refs": {"main": ref}})

    class MetadataCatalog:
        def __init__(self):
            self.metadata = metadata
            self.commits = []

        def commit_table(self, table, requirements, updates):
            for requirement in requirements:
                requirement.validate(self.metadata)
            self.commits.append((requirements, updates))
            self.metadata = update_table_metadata(self.metadata, updates)
            return SimpleNamespace(metadata=self.metadata, metadata_location=table.metadata_location)

        def _delete_old_metadata(self, io, old, new):
            pass

        def load_table(self, name):
            return Table(name, self.metadata, "local", None, self)

    catalog = MetadataCatalog()
    table = catalog.load_table(("default_native", "comments"))
    expected = table_pin(table)
    plan = migrate(table, expected=expected)
    assert plan["added"] == ["agency_note", "authors", "description", "doc_abstract", "doc_order", "modify_date", "publication"]
    assert not catalog.commits
    with pytest.raises(ValueError, match="pin changed"):
        migrate(table, expected=("different-uuid", *expected[1:]), apply=True)
    other = ref.model_copy(update={"snapshot_id": 82})
    catalog.metadata = metadata.model_copy(update={"current_snapshot_id": 82, "refs": {"main": other}})
    with pytest.raises(CommitFailedException):
        migrate(table, expected=expected, apply=True)
    assert not catalog.commits

    catalog.metadata = metadata
    applied = migrate(table, expected=expected, apply=True)
    assert table_pin(table) == (*expected[:2], applied["new_schema_id"])
    assert table.schema().find_field("attachments.title").field_id == metadata.schema().find_field("attachments.title").field_id
    assert schema_to_pyarrow(table.schema(), include_field_ids=False).equals(
        schema_to_pyarrow(_pyarrow_to_schema_without_ids(policy("comments").subject_schema), include_field_ids=False)
    )
    assert [entry.action for entry in catalog.commits[0][1]] == ["add-schema", "set-current-schema"]
    assert migrate(table, expected=table_pin(table), apply=True)["added"] == []
    assert len(catalog.commits) == 1
