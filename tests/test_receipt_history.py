"""Immutable history uses exact occurrence matching and only direct predecessor references."""
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import (
    DatasetPolicy, ReceiptContext, RECEIPT_SCHEMA, decode_exact_json, exact_json,
    split_record, failure_receipt, observation_receipt, _digest,
)
from spicy_regs.receipt_history import carry_receipt_history


@pytest.fixture
def policy():
    return DatasetPolicy("example", pa.schema([("id", pa.string()), ("sub", pa.string()), ("text", pa.string())]),
                         ("id", "sub"), ("raw",), nullable_identity_fields=("sub",))


@pytest.fixture
def context():
    return ReceiptContext("old", "attempt", "test", [{"source_id": "test", "source_uri": "https://source.test",
                                                       "sha256": "a" * 64, "locator": "/"}])


def write(path, rows):
    pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path, row_group_size=2)
    return path


def accepted(policy, context, identity="1", text="same"):
    return split_record(policy, {"id": identity, "sub": None, "text": text, "raw": "a\x1b\x00"}, context)[1]


def test_all_outcomes_fifo_reorder_delete_and_new_occurrence(tmp_path, policy, context):
    rows = [accepted(policy, context), accepted(policy, replace(context, attempt_id="deleted"), "deleted")]
    for outcome in ("rejected", "refused", "error", "observed"):
        for ordinal in range(2):
            ctx = replace(context, attempt_id=f"{outcome}-{ordinal}", diagnostics={"reason": "same"})
            rows.append(observation_receipt(policy, ctx, processing_fields={"raw": "same"}) if outcome == "observed"
                        else failure_receipt(policy, ctx, outcome=outcome, raw_fields={"raw": "same"}))
    prior = write(tmp_path / "old.parquet", rows)
    fresh = []
    for old in [rows[0], *rows[2:]]:
        row = dict(old, generation_id="new", attempt_id="new-" + old["attempt_id"])
        row["receipt_id"] = _digest({k: v for k, v in row.items() if k != "receipt_id"})
        fresh.append(row)
    # Reorder outcomes without changing duplicate occurrence order within each match.
    fresh = [fresh[0], *fresh[7:], *fresh[1:7]]
    extra = failure_receipt(policy, replace(context, generation_id="new", attempt_id="extra",
                                          diagnostics={"reason": "same"}), outcome="rejected", raw_fields={"raw": "same"})
    fresh.append(extra)
    current = write(tmp_path / "current.parquet", fresh)
    result = carry_receipt_history(current, [prior], tmp_path / "result.parquet")
    expected = [rows[0], *rows[8:], *rows[2:8], extra]
    assert pq.read_table(result).to_pylist() == expected
    assert pq.read_table(prior).to_pylist() == rows
    assert pq.read_table(current).to_pylist() == fresh


def test_changed_row_direct_predecessor_does_not_copy_history_or_witnesses(tmp_path, policy, context):
    first = accepted(policy, context)
    current = write(tmp_path / "current.parquet", [accepted(policy, replace(context, generation_id="new"), text="changed")])
    prior = write(tmp_path / "prior.parquet", [first])
    second = pq.read_table(carry_receipt_history(current, [prior], tmp_path / "second.parquet")).to_pylist()[0]
    assert decode_exact_json(second["diagnostic_json"]) == {"prior_receipt": {
        "receipt_id": first["receipt_id"], "generation_id": "old",
        "processing_sha256": _digest(decode_exact_json(first["processing_json"]))}}
    assert second["witnesses"] == pq.read_table(current).to_pylist()[0]["witnesses"]
    third_raw = accepted(policy, replace(context, generation_id="third"), text="again")
    current = write(tmp_path / "third-raw.parquet", [third_raw])
    third = pq.read_table(carry_receipt_history(current, [tmp_path / "second.parquet"], tmp_path / "third.parquet")).to_pylist()[0]
    diagnostics = decode_exact_json(third["diagnostic_json"])
    assert set(diagnostics) == {"prior_receipt"}
    assert diagnostics["prior_receipt"]["receipt_id"] == second["receipt_id"]
    assert len(exact_json(diagnostics)) < 400


def test_nonaccepted_diagnostic_change_is_new_attempt(tmp_path, policy, context):
    old = failure_receipt(policy, context, outcome="refused", raw_fields={"raw": "same"})
    fresh = failure_receipt(policy, replace(context, generation_id="new", diagnostics={"new": True}),
                            outcome="refused", raw_fields={"raw": "same"})
    result = carry_receipt_history(write(tmp_path / "new.parquet", [fresh]), [write(tmp_path / "old.parquet", [old])],
                                  tmp_path / "result.parquet")
    assert pq.read_table(result).to_pylist() == [fresh]


def test_empty_and_partitioned_prior(tmp_path, policy, context):
    one, two = accepted(policy, context, "1"), accepted(policy, context, "2")
    prior = [write(tmp_path / "part0.parquet", [one]), write(tmp_path / "part1.parquet", [two])]
    current = write(tmp_path / "current.parquet", [two, one])
    assert pq.read_table(carry_receipt_history(current, prior, tmp_path / "result.parquet")).to_pylist() == [two, one]
    empty = write(tmp_path / "empty.parquet", [])
    assert pq.read_table(carry_receipt_history(empty, prior, tmp_path / "gone.parquet")).num_rows == 0
    with pytest.raises(ValueError, match="Conflicting selected prior"):
        carry_receipt_history(current, [prior[0], prior[0]], tmp_path / "refused.parquet")
    assert not (tmp_path / "refused.parquet").exists()
