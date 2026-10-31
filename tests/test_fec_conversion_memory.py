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
    with IdentityReceiptWriter(output, generation_id="g", tables=["fec_reference_source_pages"]) as writer:
        for i in range(3):
            subject, receipt = writer.emit("fec_reference_source_pages",
                {"record_id": str(i), "content_status": "body_extracted", "text": "é" * (3 * 1024 * 1024)},
                input_witness=witness)
            expected_subjects.append(subject)
            expected_receipts.append(receipt)
            assert not writer.receipts  # One wide subject crosses the byte bound, even with empty processing fields.
    assert pq.read_table(output / "fec_reference_source_pages.parquet").to_pylist() == expected_subjects
    assert pq.read_table(output / "etl_receipts.parquet").to_pylist() == expected_receipts


def test_batched_source_records_match_complete_ordered_subjects_and_receipts(tmp_path):
    import pyarrow.parquet as pq
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter

    witness = {"source_id": "fixture", "sha256": "sha256:" + "a" * 64, "source_uri": None, "locator": None}
    records = [(dict(collection_id="fixture", source_record_id=str(i % 3), source_family="fixture",
                     source_sha256=witness["sha256"], source_record_json='{"value":"é"}' if i % 2 else None),
                dict(input_witness=witness, source_input={"retained": [i, None, "é"]})) for i in range(65)]
    for mode in ("single", "batch"):
        with IdentityReceiptWriter(tmp_path / mode, generation_id="same", tables=["fec_source_records"]) as writer:
            if mode == "batch":
                writer.emit_batch("fec_source_records", iter(records))
            else:
                for row, kwargs in records:
                    writer.emit("fec_source_records", row, **kwargs)
    for name in ("fec_source_records.parquet", "etl_receipts.parquet"):
        assert pq.read_table(tmp_path / "single" / name).to_pylist() == pq.read_table(tmp_path / "batch" / name).to_pylist()


def test_batch_preparation_keeps_byte_bound_and_complete_single_wide_value(tmp_path, monkeypatch):
    import spicy_regs.transforms.fec_identity_receipts as identity

    original = identity.split_records
    batches = []

    def observed(policy, records):
        batches.append((len(records), sum(identity._buffer_size(row) + identity._buffer_size(context.witnesses)
                       + identity._buffer_size(context.diagnostics) for row, context in records)))
        yield from original(policy, records)

    monkeypatch.setattr(identity, "split_records", observed)
    witness = {"source_id": "fixture", "sha256": "sha256:" + "a" * 64, "source_uri": None, "locator": None}
    records = [(dict(collection_id="fixture", source_record_id=str(i), source_sha256=witness["sha256"],
                     source_record_json="é" * size),
                dict(input_witness=witness)) for i, size in enumerate([200_000] * 12 + [3_000_000] + [1] * 35)]
    with identity.IdentityReceiptWriter(tmp_path / "bounded", generation_id="same", tables=["fec_source_records"]) as writer:
        writer.emit_batch("fec_source_records", records)
        assert writer.attempts == len(records)
    assert all(count <= 32 and (size <= 8 * 1024 * 1024 or count == 1) for count, size in batches)
    assert any(count == 1 and size > 8 * 1024 * 1024 for count, size in batches)


def test_batch_refuses_invalid_native_type_without_exposing_generation(tmp_path):
    import pytest
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter

    output = tmp_path / "invalid"
    witness = {"source_id": "fixture", "sha256": "sha256:" + "a" * 64, "source_uri": None, "locator": None}
    with pytest.raises(ValueError, match="native string required"):
        with IdentityReceiptWriter(output, generation_id="same", tables=["fec_source_records"]) as writer:
            writer.emit_batch("fec_source_records", [(dict(source_record_id=123), dict(input_witness=witness))])
    assert not output.exists()
