"""Typed Arrow dates keep the reference receipt spelling and refusal boundary."""

import calendar
from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, etl_receipts
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, exact_json, subject_identity, write_dataset

WITNESS = {"source_id": "fixture", "source_uri": None, "sha256": "ab" * 32,
           "locator": "row", "body_version": None}


def _encode(values, dtype):
    stored = pa.array(values, type=dtype)
    with duckdb.connect(config={"threads": "1"}) as con:
        con.register("dates", pa.table({"ordinal": range(len(stored)), "value": stored}))
        expression = etl_bulk.exact_json_sql('"value"', dtype)
        got = con.execute(f"SELECT {expression} FROM dates ORDER BY ordinal").fetchall()
    assert got == [(exact_json(value),) for value in stored.to_pylist()]


def test_date32_year_padding_and_gregorian_month_boundaries():
    values = [None, date.max, date(1969, 12, 31), date(1970, 1, 1), date(1970, 1, 2)]
    # Every supported year checks padding; century/leap transitions check all
    # month boundaries, including the proleptic Gregorian calendar before 1582.
    values += [date(year, 1, 1) for year in range(1, 10000)]
    for year in (1, 4, 9, 10, 99, 100, 400, 999, 1000, 1500, 1582, 1600, 1900, 2000, 9996, 9999):
        for month in range(1, 13):
            values.extend([date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])])
    _encode(values, pa.date32())


@pytest.mark.parametrize("list_type", [pa.list_, pa.large_list])
def test_nested_date32_structs_lists_and_nulls(list_type):
    dtype = list_type(pa.struct([("days", list_type(pa.date32())), ("day", pa.date32()), ("name", pa.string())]))
    values = [None, [], [None], [{"days": None, "day": None, "name": None}],
              [{"days": [], "day": date.min, "name": "early"},
               {"days": [None, date(2000, 2, 29), date.max], "day": date.max, "name": 'q"é'}]]
    _encode(values, dtype)


@pytest.mark.parametrize("days", [-2147483648, -719163, 2932897, 2147483647])
@pytest.mark.parametrize("nested", [False, True])
def test_date32_outside_python_range_refuses(days, nested):
    raw = pa.array([days], type=pa.int32()).cast(pa.date32())
    with pytest.raises(OverflowError):
        raw.to_pylist()
    dtype = pa.list_(pa.date32()) if nested else pa.date32()
    values = pa.ListArray.from_arrays(pa.array([0, 1]), raw) if nested else raw
    with duckdb.connect(config={"threads": "1"}) as con:
        con.register("dates", pa.table({"value": values}))
        expression = etl_bulk.exact_json_sql('"value"', dtype)
        with pytest.raises(duckdb.InvalidInputException, match="date32 outside Python date range"):
            con.execute(f"SELECT {expression} FROM dates").fetchall()


@pytest.mark.parametrize("dtype", [pa.date64(), pa.timestamp("s"), pa.timestamp("us", tz="UTC"), pa.time32("s")])
def test_other_temporal_types_remain_unproven(dtype):
    with pytest.raises(NotImplementedError, match="No proven SQL spelling"):
        etl_bulk.exact_json_sql('"value"', dtype)


@pytest.mark.parametrize("days", [-2147483648, -719163, 2932897, 2147483647])
def test_required_bulk_invalid_date_bundle_refuses_without_row_fallback(tmp_path, monkeypatch, days):
    policy = DatasetPolicy("dated", pa.schema([("id", pa.string()), ("day", pa.date32())]), ("id",), ())
    context = ReceiptContext("g", "attempt", "test/1", [WITNESS])
    subject, receipts = write_dataset([({"id": "a", "day": date.min}, context)],
                                     tmp_path / "reference", policy, validation_backend="reference")
    assert subject is not None
    bad_date = pa.array([days], type=pa.int32()).cast(pa.date32())
    pq.write_table(pa.Table.from_arrays([pa.array(["a"]), bad_date], schema=policy.subject_schema), subject)
    args = ({policy.dataset: [subject]}, [receipts], [policy])
    with pytest.raises(OverflowError):
        etl_receipts.validate_receipt_bundle(*args, generation_id="g", bulk=False)

    def forbidden_reference(*args, **kwargs):
        pytest.fail("Required bulk admission must not call the reference validator")

    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows", forbidden_reference)
    # A DuckDB data failure never retries the bundle by row, with or without required-bulk limits. It stays a
    # ValueError (BulkInputError), so writers and the conversion command still refuse it as invalid input.
    with pytest.raises(etl_bulk.BulkInputError, match="no row fallback:.*date32 outside Python date range"):
        etl_receipts.validate_receipt_bundle(*args, generation_id="g")
    with etl_bulk.bulk_limits(threads=1, memory_limit="256MiB", require_bulk=True):
        with pytest.raises(etl_bulk.BulkInputError, match="no row fallback:.*date32 outside Python date range"):
            etl_receipts.validate_receipt_bundle(*args, generation_id="g")


@pytest.mark.parametrize("damage", [False, True])
def test_required_bulk_date_bundle_matches_reference_and_preserves_hashes(tmp_path, monkeypatch, damage):
    policy = DatasetPolicy("dated", pa.schema([
        ("day", pa.date32()), ("days", pa.list_(pa.date32())),
        ("detail", pa.struct([("day", pa.date32())])),
    ]), ("day",), (), nullable_identity_fields=("day",))
    rows = [{"day": date.min, "days": [None, date.max], "detail": {"day": date(2000, 2, 29)}},
            {"day": date.max, "days": [], "detail": None},
            {"day": None, "days": None, "detail": {"day": None}}]
    subject, receipts = write_dataset(
        ((row, ReceiptContext("g", f"attempt-{i}", "test/1", [WITNESS])) for i, row in enumerate(rows)),
        tmp_path / "reference", policy, validation_backend="reference", batch_size=2,
    )
    assert subject is not None
    original = pq.read_table(subject).to_pylist()
    with duckdb.connect(config={"threads": "1"}) as con:
        con.register("subjects", pa.Table.from_pylist(original, schema=policy.subject_schema))
        record_id, version, identity = etl_bulk.identity_sql(policy)
        got = con.execute(f"SELECT {record_id}, {version}, {identity} FROM subjects").fetchall()
    assert got == [subject_identity(policy, row) for row in original]
    if damage:
        original[0]["detail"]["day"] = date(2000, 3, 1)
        pq.write_table(pa.Table.from_pylist(original, schema=policy.subject_schema), subject)
    args = ({policy.dataset: [subject]}, [receipts], [policy])
    if damage:
        with pytest.raises(ValueError) as reference:
            etl_receipts.validate_receipt_bundle(*args, generation_id="g", bulk=False)
    else:
        etl_receipts.validate_receipt_bundle(*args, generation_id="g", bulk=False)

    def forbidden_reference(*args, **kwargs):
        pytest.fail("Required bulk admission must not call the reference validator")

    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows", forbidden_reference)
    with etl_bulk.bulk_limits(threads=1, memory_limit="256MiB", require_bulk=True):
        if damage:
            with pytest.raises(ValueError) as bulk:
                etl_receipts.validate_receipt_bundle(*args, generation_id="g")
            assert str(bulk.value) == str(reference.value)
        else:
            etl_receipts.validate_receipt_bundle(*args, generation_id="g")
