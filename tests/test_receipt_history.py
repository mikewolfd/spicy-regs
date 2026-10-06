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


@pytest.mark.parametrize("outcome", ["accepted", "refused"])
def test_candidate_collision_refuses_before_replacing_destination(tmp_path, policy, context, monkeypatch, outcome):
    from spicy_regs import receipt_history

    if outcome == "accepted":
        old = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "old"}, context)[1]
        fresh = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "new"},
                             replace(context, generation_id="new"))[1]
    else:
        old = failure_receipt(policy, context, outcome=outcome, raw_fields={"raw": "same"})
        fresh = failure_receipt(policy, replace(context, generation_id="new", diagnostics={"different": True}),
                                outcome=outcome, raw_fields={"raw": "same"})
    prior = write(tmp_path / "prior.parquet", [old])
    current = write(tmp_path / "current.parquet", [fresh])
    destination = write(tmp_path / "destination.parquet", [old])
    before = destination.read_bytes()
    monkeypatch.setattr(receipt_history, "_processing_key", lambda column: "unhex('00')")
    with pytest.raises(ValueError, match="candidate keys differ from exact values"):
        carry_receipt_history(current, [prior], destination)
    assert destination.read_bytes() == before
    assert pq.read_table(current).to_pylist() == [fresh]
    assert not list(tmp_path.glob("receipt-history-*"))


def test_valid_new_attempts_preserve_fifo_across_prior_files_and_output_batches(tmp_path, policy, context):
    old = [accepted(policy, context, str(i), text="value-" + str(i)) for i in range(2011)]
    old += [failure_receipt(policy, replace(context, attempt_id=f"refused-{i}"), outcome="refused",
                            raw_fields={"raw": "same"}) for i in range(7)]
    prior = []
    for number, rows in enumerate((old[:1000], old[1000:2014], old[2014:])):
        path = tmp_path / f"prior-{number}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=RECEIPT_SCHEMA), path, row_group_size=127)
        prior.append(path)
    new_context = replace(context, generation_id="new", attempt_id="new",
                          witnesses=[dict(context.witnesses[0], sha256="b" * 64)])
    fresh = [accepted(policy, new_context, str(i), text="value-" + str(i)) for i in reversed(range(2011))]
    fresh += [failure_receipt(policy, replace(new_context, attempt_id=f"new-refused-{i}"), outcome="refused",
                              raw_fields={"raw": "same"}) for i in range(8)]
    current = tmp_path / "current.parquet"
    pq.write_table(pa.Table.from_pylist(fresh, schema=RECEIPT_SCHEMA), current, row_group_size=113)
    before = current.read_bytes()
    result = carry_receipt_history(current, prior, current)
    assert pq.read_table(result).to_pylist() == [*reversed(old[:2011]), *old[2011:], fresh[-1]]
    assert current.read_bytes() != before
    assert not list(tmp_path.glob("receipt-history-*"))


@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt])
def test_partial_write_or_interruption_preserves_in_place_current(tmp_path, policy, context, monkeypatch, error_type):
    current = write(tmp_path / "current.parquet", [accepted(policy, replace(context, generation_id="new"))])
    prior = write(tmp_path / "prior.parquet", [accepted(policy, context)])
    before = current.read_bytes()

    class FailingWriter:
        def __init__(self, path, *args, **kwargs):
            self.path = path

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def write_table(self, table):
            self.path.write_bytes(b"partial owned output")
            raise error_type("injected write failure")

    monkeypatch.setattr(pq, "ParquetWriter", FailingWriter)
    with pytest.raises(error_type):
        carry_receipt_history(current, [prior], current)
    assert current.read_bytes() == before
    assert not list(tmp_path.glob("receipt-history-*"))


def test_absent_prior_preserves_every_current_outcome(tmp_path, policy, context):
    rows = [accepted(policy, context)]
    rows += [failure_receipt(policy, context, outcome=outcome, raw_fields={"raw": outcome})
             for outcome in ("rejected", "refused", "error")]
    rows.append(observation_receipt(policy, context, processing_fields={"raw": "observed"}))
    current = write(tmp_path / "current.parquet", rows)
    assert pq.read_table(carry_receipt_history(current, [], tmp_path / "result.parquet")).to_pylist() == rows


def test_wide_changed_attempt_keeps_current_witnesses_and_only_direct_predecessor(tmp_path, policy, context):
    old = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "a" * 131072}, context)[1]
    new_context = replace(context, generation_id="new", attempt_id="new",
                          witnesses=[dict(context.witnesses[0], sha256="b" * 64)],
                          diagnostics={"current": "kept", "prior_receipts": [old], "retained_processing": {"old": True},
                                       "prior_receipt": {"stale": True}})
    fresh = split_record(policy, {"id": "1", "sub": None, "text": "same", "raw": "b" * 131072}, new_context)[1]
    result = carry_receipt_history(write(tmp_path / "current.parquet", [fresh]),
                                   [write(tmp_path / "prior.parquet", [old])], tmp_path / "result.parquet")
    [actual] = pq.read_table(result).to_pylist()
    expected = dict(fresh, diagnostic_json=exact_json({"current": "kept", "prior_receipt": {
        "receipt_id": old["receipt_id"], "generation_id": old["generation_id"],
        "processing_sha256": _digest(decode_exact_json(old["processing_json"]))}}))
    expected["receipt_id"] = _digest({k: v for k, v in expected.items() if k != "receipt_id"})
    assert actual == expected
