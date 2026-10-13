"""Wide retained values remain complete while Python batches have a byte bound."""
import pyarrow as pa

from spicy_regs.fec_native_conversion import _bounded_record_batches


def test_wide_batches_keep_exact_order_and_values():
    source = pa.RecordBatch.from_arrays([pa.array([str(i) + "x" * 256_000 for i in range(128)])], ["value"])
    parts = list(_bounded_record_batches(source))
    assert source.nbytes > 32_000_000
    assert len(parts) > 1
    assert max(part.nbytes for part in parts) <= 8 * 1024 * 1024
    assert [row for part in parts for row in part.to_pylist()] == source.to_pylist()


def test_single_wide_value_is_preserved_without_truncation():
    source = pa.RecordBatch.from_arrays([pa.array(["x" * (9 * 1024 * 1024), "small"])], ["value"])
    parts = list(_bounded_record_batches(source))
    assert parts[0].num_rows == 1 and parts[0].nbytes > 8 * 1024 * 1024
    assert [row for part in parts for row in part.to_pylist()] == source.to_pylist()


def test_nested_null_values_and_odd_batch_boundaries_stay_exact():
    schema = pa.schema([("id", pa.int64()), ("items", pa.list_(pa.struct([("name", pa.string()), ("number", pa.int64())])))])
    rows = [{"id": i if i % 3 else None, "items": None if i % 5 == 0 else [{"name": "é" * i, "number": None if i % 2 else i}]} for i in range(65)]
    source = pa.RecordBatch.from_pylist(rows, schema=schema)
    parts = list(_bounded_record_batches(source, max_bytes=512))
    assert len(parts) > 1
    assert all(part.schema.equals(schema) for part in parts)
    assert [row for part in parts for row in part.to_pylist()] == source.to_pylist()


def test_wide_kept_subject_flush_keeps_exact_subject_receipt_values(tmp_path):
    import pyarrow.parquet as pq
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter

    witness = {"source_id": "fixture", "sha256": "sha256:" + "a" * 64, "source_uri": None, "locator": None}
    expected_subjects, expected_receipts = [], []
    output = tmp_path / "wide"
    with IdentityReceiptWriter(output, generation_id="g", tables=["fec_research_source_pages"]) as writer:
        for i in range(3):
            subject, receipt = writer.emit("fec_research_source_pages",
                {"record_id": str(i), "content_status": "body_extracted", "text": "é" * (3 * 1024 * 1024)},
                input_witness=witness)
            expected_subjects.append(subject)
            expected_receipts.append(receipt)
            assert not writer.receipts  # One wide subject crosses the byte bound, even with empty processing fields.
    assert pq.read_table(output / "fec_research_source_pages.parquet").to_pylist() == expected_subjects
    assert pq.read_table(output / "etl_receipts.parquet").to_pylist() == expected_receipts
