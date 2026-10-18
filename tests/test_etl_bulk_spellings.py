"""The SQL spellings of timestamps and floats equal ``exact_json``; a bundle holding them is validated in bulk, loudly otherwise."""

import datetime as dt
import math
import random
import struct
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from loguru import logger

from spicy_regs import etl_bulk, etl_receipts
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA, DatasetPolicy, ReceiptContext, combine_receipts, exact_json, write_dataset,
)
from spicy_regs.transforms.build_lobbying_filings import _shape
from spicy_regs.transforms.government_receipts import POLICIES
from spicy_regs.transforms.government_source_shapes import map_subject

WITNESS = {"source_id": "s", "source_uri": None, "sha256": "ab" * 32, "locator": "row", "body_version": None}
UTC = dt.timezone.utc
TIMESTAMP = pa.timestamp("us", tz="UTC")


def _spell(values, dtype):
    """Each value's spelling, and whether ``needs_reference_sql`` sends it to the row functions."""
    with etl_bulk.bulk_connection() as (con, _):
        con.register("t", pa.table({"x": pa.array(values, dtype)}))
        spelling = etl_bulk.exact_json_sql("x", dtype)
        return con.execute(f"SELECT {spelling}, {etl_bulk.needs_reference_sql(spelling)} FROM t").fetchall()


def test_timestamp_spelling_matches_isoformat_in_any_host_zone(monkeypatch):
    rng = random.Random(20261018)
    values = [dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC), dt.datetime(2000, 2, 29, tzinfo=UTC),
              dt.datetime(1000, 1, 1, tzinfo=UTC), dt.datetime(9998, 12, 31, 23, 59, 59, 5, tzinfo=UTC), None]
    for _ in range(5000):
        year = rng.choice([1000, 1001, 1969, 1970, 2024, 9998, rng.randint(1000, 9998)])
        values.append(dt.datetime(year, 1, 1, tzinfo=UTC) + dt.timedelta(
            days=rng.randint(0, 364), seconds=rng.randint(0, 86399), microseconds=rng.choice([0, 5, 999999, rng.randint(0, 999999)])))
    monkeypatch.setenv("TZ", "America/Los_Angeles")
    assert _spell(values, TIMESTAMP) == [(exact_json(value), False) for value in values]
    with etl_bulk.bulk_connection() as (con, _):
        assert con.execute("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)


@pytest.mark.parametrize("value", [dt.datetime(1, 1, 1, tzinfo=UTC), dt.datetime(999, 12, 31, tzinfo=UTC),
                                   dt.datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)])
def test_timestamp_outside_the_proven_years_is_left_to_the_row_functions(value):
    [(text, routed)] = _spell([value], TIMESTAMP)
    assert routed and text != exact_json(value)


def test_float_spelling_matches_float_hex_over_random_doubles():
    rng = random.Random(20261018)
    values = [struct.unpack("<d", struct.pack("<Q", rng.getrandbits(64)))[0] for _ in range(200_000)]
    for exponent in range(-1022, 1024):
        power = 2.0**exponent
        values += [power, -power, math.nextafter(power, 0.0), math.nextafter(power, math.inf)]
    values += [0.0, -0.0, 0.1, 1e-300, 1.7976931348623157e308, 2.2250738585072014e-308]
    spelled = _spell(values, pa.float64())
    for value, (text, routed) in zip(values, spelled, strict=True):
        if math.isfinite(value) and (value == 0 or abs(value) >= 2.2250738585072014e-308):
            assert (text, routed) == (exact_json(value), False), value
        else:
            assert routed, value  # subnormal, infinite, NaN: the row functions decide
    assert _spell([0.5, -1.5], pa.float32()) == [(exact_json(0.5), False), (exact_json(-1.5), False)]


def _filing(**row):
    raw = {"filing_uuid": "F" + str(row.pop("n", 0)), "dt_posted": "2026-01-02T03:04:05.123456-05:00", **row}
    return map_subject("lobbying_filings", _shape(raw))


def _lobbying_bundle(tmp_path, *rows):
    policy = POLICIES["lobbying_filings"]
    records = [(row, ReceiptContext("g", f"a{i}", "p", [WITNESS])) for i, row in enumerate(rows)]
    subject, receipts = write_dataset(records, tmp_path / "bundle", policy, validation_backend="reference")
    return policy, {policy.dataset: [subject]}, [receipts]


def test_a_lobbying_bundle_validates_in_bulk_with_no_row_route(tmp_path):
    """Finding 1: ``dt_posted`` had no spelling, so 15.9M rows went to the row validator at INFO."""
    policy, subjects, receipts = _lobbying_bundle(tmp_path, _filing(n=1), _filing(n=2, dt_posted=None))
    messages = []
    sink = logger.add(messages.append, level="DEBUG")
    try:
        etl_bulk.validate_bundle(subjects, receipts, [policy], generation_id="g")
        etl_receipts.validate_receipt_bundle(subjects, receipts, [policy], generation_id="g")
    finally:
        logger.remove(sink)
    assert not [m for m in messages if "route=row" in str(m)]


def test_a_timestamp_outside_the_proven_years_is_still_decided_by_the_row_validator(tmp_path):
    ancient = _filing(n=1, dt_posted="0500-03-04T05:06:07+00:00")
    policy, subjects, receipts = _lobbying_bundle(tmp_path, ancient, _filing(n=2))
    etl_bulk.validate_bundle(subjects, receipts, [policy], generation_id="g")


def _unsupported(tmp_path):
    policy = DatasetPolicy("wide", pa.schema([("id", pa.string()), ("n", pa.decimal128(12, 8))]), ("id",), ())
    subject, receipts = write_dataset(
        [({"id": "a", "n": Decimal("1.50000000")}, ReceiptContext("g", "a", "p", [WITNESS]))], tmp_path / "wide", policy)
    return policy, {"wide": [subject]}, [receipts]


def test_a_row_route_warns_with_dataset_and_rows(tmp_path):
    policy, subjects, receipts = _unsupported(tmp_path)
    messages = []
    sink = logger.add(lambda m: messages.append(m.record), level="WARNING")
    try:
        etl_receipts.validate_receipt_bundle(subjects, receipts, [policy], generation_id="g")
    finally:
        logger.remove(sink)
    [record] = messages
    assert record["level"].name == "WARNING"
    assert "route=row dataset=wide rows=2 " in record["message"] and "receipt validation" in record["message"]


def test_required_bulk_refuses_a_row_route(tmp_path, monkeypatch):
    policy, subjects, receipts = _unsupported(tmp_path)
    monkeypatch.setattr(etl_receipts, "_validate_receipt_bundle_rows", lambda *a, **k: pytest.fail("row validator ran"))
    with etl_bulk.bulk_limits(threads=1, memory_limit="1GB", require_bulk=True):
        with pytest.raises(ValueError, match="Required bulk receipt validation failed; reference fallback disabled"):
            etl_receipts.validate_receipt_bundle(subjects, receipts, [policy], generation_id="g")


def _shard(path, rows):
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path)
    return path


def _receipt(n):
    return {name: None for name in RECEIPT_SCHEMA.names} | {
        "receipt_id": f"r{n}", "dataset": "d", "witnesses": [WITNESS], "processing_json": "x" * n}


def test_combine_receipts_copies_batches_in_order_and_refuses_another_schema(tmp_path):
    first, second = (_shard(tmp_path / name, [_receipt(n) for n in rows])
                     for name, rows in (("a.parquet", (1, 2, 3)), ("b.parquet", (4, 5))))
    combined = combine_receipts([first, second], tmp_path / "all.parquet")
    assert pq.read_table(combined).equals(pa.concat_tables([pq.read_table(first), pq.read_table(second)]))
    other = tmp_path / "other.parquet"
    pq.write_table(pa.table({"receipt_id": ["x"]}), other)
    with pytest.raises(ValueError, match="not the shared receipt schema"):
        combine_receipts([first, other], tmp_path / "refused.parquet")
    assert not (tmp_path / "refused.parquet").exists()


def test_write_dataset_can_leave_validation_to_the_generation_admission(tmp_path, monkeypatch):
    policy = DatasetPolicy("plain", pa.schema([("id", pa.string())]), ("id",), ())
    calls = []
    monkeypatch.setattr(etl_receipts, "validate_receipt_bundle", lambda *a, **k: calls.append(1))
    record = [({"id": "a"}, ReceiptContext("g", "a", "p", [WITNESS]))]
    write_dataset(record, tmp_path / "checked", policy)
    write_dataset(record, tmp_path / "unchecked", policy, validate=False)
    assert calls == [1]
