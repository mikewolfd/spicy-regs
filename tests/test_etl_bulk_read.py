"""Bulk replay agrees with the independent row reader before and across batch boundaries."""

import random
from contextlib import contextmanager
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    DatasetPolicy, ReceiptContext, observation_receipt, read_attempts, read_with_receipts, write_dataset,
)
from spicy_regs.parquet_rows import write_rows
from .test_etl_bulk_validate import Bundle, REFUSALS, WITNESS, build


def scoped_bundle(tmp_path, rows=7):
    bundle = build(tmp_path, rows=rows)
    return Bundle([bundle.policies[0]], {"things": bundle.subjects["things"]},
                  [[row for row in bundle.receipts[0] if row["dataset"] == "things"]])


def result(subjects, receipts, policy, *, bulk):
    yielded = []
    try:
        yielded.extend(read_with_receipts(subjects, receipts, policy, generation_id="g1", bulk=bulk))
    except Exception as error:
        return yielded, (type(error), str(error))
    return yielded, None


@pytest.mark.parametrize("opened", [False, True])
def test_reordered_receipts_split_subjects_and_exact_nested_values(tmp_path, opened):
    bundle = scoped_bundle(tmp_path)
    subjects, receipts, policies = bundle.write(tmp_path / "original")
    expected, error = result(subjects["things"], receipts, policies[0], bulk=False)
    assert error is None
    random.Random(12).shuffle(bundle.receipts[0])
    bundle.receipts = [bundle.receipts[0][:3], [], bundle.receipts[0][3:]]
    rows = bundle.subjects["things"][0]
    bundle.subjects["things"] = [rows[:2], [], rows[2:]]
    subjects, receipts, policies = bundle.write(tmp_path / "split", opened=opened)
    assert result(subjects["things"], receipts, policies[0], bulk=True) == (expected, None)


@pytest.mark.parametrize("fault", REFUSALS)
def test_same_refusal_before_any_row_is_yielded(tmp_path, fault):
    bundle = scoped_bundle(tmp_path)
    REFUSALS[fault][1](bundle, "things")
    subjects, receipts, policies = bundle.write(tmp_path / "damaged")
    row = result(subjects["things"], receipts, policies[0], bulk=False)
    assert row[0] == [] and row[1] is not None
    assert result(subjects["things"], receipts, policies[0], bulk=True) == row


def test_boundaries_column_collisions_and_no_whole_row_index(tmp_path, monkeypatch):
    schema = pa.schema([("id", pa.int64()), ("n", pa.string()), ("n_", pa.string()),
                        ("source", pa.string()), ("processing_json", pa.string())])
    policy = DatasetPolicy("items", schema, ("id",), ("raw",))
    rows = [{"id": i, "n": "literal", "n_": None, "source": "source", "processing_json": "subject text",
             "raw": {"value": [i, None, "\x1b" if i % 5 == 4 else "plain"]}} for i in range(4003)]
    subject, receipts = write_dataset(((row, ReceiptContext("g1", str(i), "test/1", [WITNESS]))
                                      for i, row in enumerate(rows)), tmp_path / "written", policy)
    assert subject is not None
    expected = list(read_with_receipts([subject], [receipts], policy, generation_id="g1", bulk=False))
    load = etl_bulk._load_receipts
    referred = []

    def reference(con, paths, *args, **kwargs):
        paths = list(paths)
        referred.append(sum(pq.ParquetFile(member()).metadata.num_rows for member in paths))
        return load(con, paths, *args, **kwargs)

    monkeypatch.setattr(etl_bulk, "_load_receipts", reference)
    assert list(read_with_receipts([subject], [receipts], policy, generation_id="g1")) == expected == rows
    # Deep/raw escape text belongs to processing decoding; it must not cause all receipts to use SQLite.
    assert sum(referred) < len(rows)


def test_unproven_subject_type_uses_the_reference(tmp_path):
    schema = pa.schema([("id", pa.string()), ("amount", pa.float64())])
    policy = DatasetPolicy("items", schema, ("id",), ("raw",))
    row = {"id": "a", "amount": -0.0, "raw": {"decimal": Decimal("0.00"), "literal": "\x1b"}}
    subject, receipts = write_dataset([(row, ReceiptContext("g1", "a", "test/1", [WITNESS]))], tmp_path / "written", policy)
    assert result([subject], [receipts], policy, bulk=True) == result([subject], [receipts], policy, bulk=False)


def test_truncated_replay_cannot_replace_a_complete_processing_file(tmp_path, monkeypatch):
    schema = pa.schema([("id", pa.int64())])
    policy = DatasetPolicy("items", schema, ("id",), ())
    subject, receipts = write_dataset((({"id": i}, ReceiptContext("g1", str(i), "test/1", [WITNESS]))
                                      for i in range(4003)), tmp_path / "written", policy)
    assert subject is not None
    target = tmp_path / "processing.parquet"
    pq.write_table(pa.table({"id": [999]}, schema=schema), target)
    prior = target.read_bytes()
    numbered = etl_bulk._numbered
    calls = 0

    def shortened(*args, **kwargs):
        nonlocal calls
        calls += 1
        reader = numbered(*args, **kwargs)
        if calls == 3:  # Receipt admission, subject admission, then replay.
            return pa.RecordBatchReader.from_batches(reader.schema, [next(iter(reader))])
        return reader

    monkeypatch.setattr(etl_bulk, "_numbered", shortened)
    with pytest.raises(ValueError, match="reconstruct every subject"):
        write_rows(read_with_receipts([subject], [receipts], policy, generation_id="g1"), target, schema)
    assert target.read_bytes() == prior


@pytest.mark.parametrize("outcomes", [None, frozenset({"accepted", "observed"}), frozenset(), frozenset({"refused"})])
def test_attempts_keep_exact_payloads_failures_order_and_selection(tmp_path, outcomes):
    bundle = scoped_bundle(tmp_path)
    random.Random(9).shuffle(bundle.receipts[0])
    subjects, receipts, policies = bundle.write(tmp_path / "written")
    expected = list(read_attempts(receipts, policies[0], generation_id="g1", outcomes=outcomes, bulk=False))
    assert list(read_attempts(receipts, policies[0], generation_id="g1", outcomes=outcomes)) == expected


def test_attempts_check_ignored_later_corruption_before_yielding(tmp_path):
    bundle = scoped_bundle(tmp_path)
    bundle.receipts[0][-1]["receipt_id"] = "sha256:" + "0" * 64
    _, receipts, policies = bundle.write(tmp_path / "damaged")
    for bulk in [False, True]:
        read = read_attempts(receipts, policies[0], generation_id="g1", outcomes=frozenset({"accepted"}), bulk=bulk)
        with pytest.raises(ValueError, match="Receipt digest differs"):
            next(iter(read))


def test_subject_changed_after_admission_cannot_use_cached_identity(tmp_path, monkeypatch):
    bundle = scoped_bundle(tmp_path)
    subjects, receipts, policies = bundle.write(tmp_path / "written")
    original = etl_bulk._validated_bundle

    @contextmanager
    def changed(*args, **kwargs):
        with original(*args, **kwargs) as con:
            table = pq.read_table(subjects["things"][0])
            rows = table.to_pylist()
            rows[0]["n"] = 100
            pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), subjects["things"][0])
            yield con

    monkeypatch.setattr(etl_bulk, "_validated_bundle", changed)
    read = read_with_receipts(subjects["things"], receipts, policies[0], generation_id="g1")
    with pytest.raises(ValueError, match="Missing, ambiguous or reused subject receipt"):
        next(iter(read))


def test_empty_inputs_preserve_reference_behavior():
    policy = DatasetPolicy("items", pa.schema([("id", pa.string())]), ("id",), ())
    assert result([], [], policy, bulk=True) == result([], [], policy, bulk=False) == ([], None)


def test_observation_only_generation_with_unproven_subject_schema(tmp_path):
    policy = DatasetPolicy("items", pa.schema([("id", pa.string()), ("value", pa.float64())]), ("id",), ("raw",))
    context = ReceiptContext("g1", "empty-observation", "test/1", [WITNESS])
    observation = observation_receipt(policy, context, processing_fields={"raw": {"empty": True}})
    _, receipts = write_dataset([], tmp_path / "written", policy, failures=[observation])
    assert result([], [receipts], policy, bulk=True) == result([], [receipts], policy, bulk=False) == ([], None)
