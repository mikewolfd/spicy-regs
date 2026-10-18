"""The attribute tables' typed bulk writer and restore equal the row writer and row reader."""
import datetime as dt
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, etl_receipts, regulations_bulk
from spicy_regs.schemas.regulations_subjects import SOURCE_COLUMNS
from spicy_regs.transforms import regulations_receipts
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, policy, write_held_dataset
from spicy_regs.transforms.regulations_shape import TYPES

UTC = dt.timezone.utc
DISPLAY = json.dumps([{"name": "n", "label": "l", "tooltip": "t"}])


def _table(dataset, rows):
    schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[dataset]])
    full = [{name: row.get(name) for name in schema.names} for row in rows]
    return pa.Table.from_pylist(full, schema=schema)


def _comment_rows():
    stamp = dt.datetime(2021, 5, 1, 12, 30, 5, tzinfo=UTC)
    return [
        {"comment_id": "plain", "page_count": 3, "withdrawn": False, "postmark_date": stamp,
         "display_properties_json": DISPLAY, "file_formats_json": '[{"fileUrl":"u","format":"pdf","size":3}]'},
        {"comment_id": "micro", "page_count": 0, "withdrawn": True, "postmark_date": stamp.replace(microsecond=5),
         "display_properties_json": DISPLAY},
        {"comment_id": "nulls"},
        {"comment_id": "escape", "address1": "x\x1b[0m", "city": "é😀", "display_properties_json": "null"},
        {"comment_id": "empty-list", "display_properties_json": "[]", "file_formats_json": "[null]"},
        {"comment_id": "bad-json", "display_properties_json": "[bad"},
        {"comment_id": "unknown-key", "display_properties_json": '[{"nope":1}]'},
        {"comment_id": "old", "postmark_date": dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)},
        {"comment_id": "year-1000", "postmark_date": dt.datetime(1000, 1, 1, tzinfo=UTC)},
        {"comment_id": None, "page_count": 1},
        {"comment_id": "last", "page_count": 2**31 - 1, "display_properties_json": DISPLAY},
    ]


def _same(left, right):
    for a, b in zip(left, right):
        assert pq.read_table(a).equals(pq.read_table(b), check_metadata=True)


@pytest.mark.parametrize("dataset, rows", [
    ("comment_attributes", _comment_rows()),
    ("docket_attributes", [
        {"docket_id": "d1", "keywords": ["a", None, "a"], "effective_date": dt.datetime(2020, 2, 29, tzinfo=UTC),
         "display_properties_json": DISPLAY},
        {"docket_id": "d2", "keywords": [], "category": "x\x0b"}, {"docket_id": "d3"}]),
    ("document_attributes", [
        {"document_id": "x1", "allow_late_comments": True, "authors": ["z"], "author_date": dt.datetime(2020, 1, 1, tzinfo=UTC)},
        {"document_id": "x2", "allow_late_comments": None, "page_count": 3} if False else {"document_id": "x2"}]),
])
def test_typed_bulk_pair_and_restore_equal_the_row_writer_and_reader(tmp_path, monkeypatch, dataset, rows):
    source = tmp_path / "source.parquet"
    table = _table(dataset, rows)
    pq.write_table(table, source, row_group_size=3)
    reference = write_held_dataset(dataset, source, tmp_path / "row", bulk=False, generation_id="g")
    monkeypatch.setattr(regulations_bulk, "_BATCH", 4)
    routes = []
    original = regulations_bulk.write_held_dataset
    monkeypatch.setattr(regulations_bulk, "write_held_dataset", lambda *a, **k: routes.append("bulk") or original(*a, **k))
    # The bulk pair must not be redone by row, nor validated by row.
    monkeypatch.setattr(regulations_receipts, "write_records",
                        lambda *a, **k: pytest.fail("whole-pair row fallback"))
    actual = write_held_dataset(dataset, source, tmp_path / "bulk", bulk=True, generation_id="g")
    assert routes == ["bulk"]
    _same(reference, actual)
    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows",
                        lambda *a, **k: pytest.fail("row validation of a bundle the SQL validator proves"))
    etl_receipts.validate_receipt_bundle({dataset: [actual[0]]}, [actual[1]], [policy(dataset)], generation_id="g")

    selected = ReceiptInput(dataset, (actual[0],), actual[1], "g")
    monkeypatch.undo()
    row_restore = materialize_internal(selected, tmp_path / "row-restore.parquet", bulk=False, source_schema=table.schema)
    monkeypatch.setattr(regulations_bulk, "_BATCH", 4)
    restored = []
    restore = regulations_bulk.materialize_internal
    monkeypatch.setattr(regulations_bulk, "materialize_internal", lambda *a, **k: restored.append(1) or restore(*a, **k))
    bulk_restore = materialize_internal(selected, tmp_path / "bulk-restore.parquet", bulk=True, source_schema=table.schema)
    assert restored == [1]
    assert pq.read_table(row_restore).equals(pq.read_table(bulk_restore), check_metadata=True)


def test_a_restore_whose_receipt_names_another_subject_is_refused_not_misread(tmp_path):
    source = tmp_path / "source.parquet"
    table = _table("comment_attributes", [{"comment_id": f"c{i}", "page_count": i} for i in range(5)])
    pq.write_table(table, source)
    subject, receipts = write_held_dataset("comment_attributes", source, tmp_path / "native", bulk=True, generation_id="g")
    # Reorder the subjects: the receipts no longer follow them, so the positional pairing must be refused.
    shuffled = tmp_path / "shuffled.parquet"
    pq.write_table(pq.read_table(subject).take([1, 0, 2, 3, 4]), shuffled)
    with pytest.raises((etl_bulk.NotBulkEligible, ValueError)):
        regulations_bulk.materialize_internal(ReceiptInput("comment_attributes", (shuffled,), receipts, "g"),
                                              tmp_path / "out.parquet", source_schema=table.schema)


def test_a_forged_subject_is_refused_and_leaves_no_file(tmp_path):
    source = tmp_path / "source.parquet"
    table = _table("comment_attributes", [{"comment_id": f"c{i}", "page_count": i, "city": f"town{i}"} for i in range(4)])
    pq.write_table(table, source)
    subject, receipts = write_held_dataset("comment_attributes", source, tmp_path / "native", bulk=True, generation_id="g")
    changed = pq.read_table(subject).to_pylist()
    changed[2]["city"] = "elsewhere"
    forged = tmp_path / "forged.parquet"
    pq.write_table(pa.Table.from_pylist(changed, schema=pq.read_schema(subject)), forged)
    with pytest.raises((etl_bulk.NotBulkEligible, ValueError)):
        regulations_bulk.materialize_internal(ReceiptInput("comment_attributes", (forged,), receipts, "g"),
                                              tmp_path / "out.parquet", source_schema=table.schema)
    assert not (tmp_path / "out.parquet").exists()


def test_the_bulk_validator_proves_datetime_evidence_and_leaves_the_rest_to_the_row_decoder():
    import duckdb
    con = duckdb.connect()

    def proven(value):
        text = json.dumps(["datetime", value]).replace(" ", "")
        return con.execute("SELECT " + etl_bulk.decodes_sql("'" + text.replace("'", "''") + "'")).fetchone()[0]
    assert proven("2021-05-01T12:30:05+00:00") and proven("1000-01-01T00:00:00.000005+00:00")
    for unproven in ("2021-02-30T00:00:00+00:00", "2021-05-01T24:00:00+00:00", "0999-01-01T00:00:00+00:00",
                     "2021-05-01T12:30:05+05:00", "2021-05-01 12:30:05+00:00"):
        assert not proven(unproven)
