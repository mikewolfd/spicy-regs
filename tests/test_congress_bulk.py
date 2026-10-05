"""A flat Congress dataset's bulk bundle and restored table equal the row writer's, row for row."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import congress_bulk
from spicy_regs.congress_receipts import restore_processing_input, write_congress_dataset
from spicy_regs.congress_subjects import INPUT_COLUMNS, map_record
from spicy_regs.etl_receipts import RECEIPT_SCHEMA

GENERATION = "generation-1"
WITNESS = {"source_id": "capture", "source_uri": "https://example.test/votes", "sha256": "ab" * 32, "locator": "page 1"}


def _member_votes(count: int) -> list[dict]:
    rows = [
        {name: None for name in INPUT_COLUMNS["member_votes"]}
        | {"vote_id": f"101-house-1-{i // 400}", "member_key": f"name:Member {i}", "congress": "101", "chamber": "house",
           "member_name": ["Smith (OR)", "O'Brien", 'Quote "Q"', "Peña"][i % 4], "position": "Yea" if i % 3 else None}
        for i in range(count)
    ]
    rows[7]["member_name"] = "Escape \x1b here"  # SQL spells this escape differently: the row code's
    rows[11]["vote_id"] = None                   # no identity: the row code refuses it
    rows[2001] = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v", "member_key": "k"}
    return rows


def _member_vote_terms(count: int) -> list[dict]:
    rows = []
    for i in range(count):
        matched = i % 3 != 0
        rows.append({"vote_id": f"101-house-1-{i // 400}", "member_key": f"name:Member {i}", "chamber": "house",
                     "bioguide_id": f"B{i:06d}" if matched else None, "vote_day": "1990-01-23",
                     "term_match": "half_open" if matched else "unresolved_member", "term_index": str(i % 7) if matched else None,
                     "term_start": "1989-01-03" if matched else None, "term_end": "1991-01-03" if matched else None})
    rows[4]["term_index"] = "007"       # digits with leading zeros read as 7
    rows[5]["term_index"] = "x1"        # not an integer: the row code refuses it
    rows[10]["chamber"] = "house\x0b"   # the row code's spelling
    rows[13]["term_match"] = None       # no term and no reason: a subject with a NULL term
    rows[13]["term_index"] = None
    return rows


def _source(tmp_path, dataset, rows, metadata=None):
    schema = pa.schema([(name, pa.string()) for name in INPUT_COLUMNS[dataset]], metadata=metadata)
    path = tmp_path / f"{dataset}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def _both(tmp_path, monkeypatch, dataset, rows, **kwargs):
    """The row writer's bundle and the bulk writer's, from one source file."""
    source = _source(tmp_path, dataset, rows, kwargs.pop("metadata", None))
    bulk = write_congress_dataset(source, tmp_path / "bulk", dataset=dataset, generation_id=GENERATION, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(congress_bulk, "eligible", lambda *_: False)
        row = write_congress_dataset(source, tmp_path / "row", dataset=dataset, generation_id=GENERATION, **kwargs)
    return source, row, bulk


@pytest.mark.parametrize(("dataset", "build"), [("member_votes", _member_votes), ("member_vote_terms", _member_vote_terms)])
def test_bulk_bundle_equals_the_row_writers(tmp_path, monkeypatch, dataset, build) -> None:
    rows = build(4500)
    source, (row_subject, row_receipts), (bulk_subject, bulk_receipts) = _both(
        tmp_path, monkeypatch, dataset, rows, witnesses=[WITNESS], metadata={b"scope": b"101"})
    assert congress_bulk.eligible(dataset, pq.read_schema(source))
    expected = pq.read_table(row_receipts).to_pylist()
    assert pq.read_table(bulk_receipts).to_pylist() == expected
    assert {receipt["outcome"] for receipt in expected} >= {"accepted", "refused", "observed"}
    assert pq.read_table(bulk_subject).to_pylist() == pq.read_table(row_subject).to_pylist()
    written = pq.ParquetFile(bulk_receipts)
    assert written.schema_arrow.equals(RECEIPT_SCHEMA)
    assert [written.metadata.row_group(i).num_rows for i in range(written.metadata.num_row_groups)] == [2000, 2000, 501]


def test_rows_without_a_subject_are_the_mappers(tmp_path) -> None:
    """NO_SUBJECT restates map_record's rule; hold it to the mapper's answer on every row shape it distinguishes."""
    import duckdb

    rows = _member_vote_terms(60)
    source = _source(tmp_path, "member_vote_terms", rows)
    stated = [flag for (flag,) in duckdb.sql(
        f"SELECT ({congress_bulk.NO_SUBJECT['member_vote_terms']}) FROM read_parquet('{source}', file_row_number = true) ORDER BY file_row_number"
    ).fetchall()]
    mapped = []
    for row in rows:
        try:
            mapped.append(map_record("member_vote_terms", row).subject is None)
        except ValueError:
            mapped.append(False)  # refused, not subjectless: the row code's
    assert stated == mapped and True in stated and False in stated


@pytest.mark.parametrize(("dataset", "build"), [("member_votes", _member_votes), ("member_vote_terms", _member_vote_terms)])
def test_bulk_restore_equals_the_row_restore(tmp_path, monkeypatch, dataset, build) -> None:
    rows = build(4500)
    source, (row_subject, row_receipts), _ = _both(tmp_path, monkeypatch, dataset, rows, metadata={b"scope": b"101"})
    bulk = restore_processing_input(row_subject, row_receipts, tmp_path / "bulk.parquet", dataset=dataset, generation_id=GENERATION)
    with monkeypatch.context() as patch:
        patch.setattr(congress_bulk, "eligible", lambda *_: False)
        row = restore_processing_input(row_subject, row_receipts, tmp_path / "row.parquet", dataset=dataset, generation_id=GENERATION)
    restored, expected = pq.read_table(bulk), pq.read_table(row)
    assert restored.to_pylist() == expected.to_pylist()
    assert restored.schema.equals(expected.schema, check_metadata=True)
    assert 0 < restored.num_rows < len(rows)  # the refused conversions are left out, as the row restore leaves them


@pytest.mark.parametrize("dataset", ["roll_call_votes", "nominations", "bill_family_archives", "public_activity_events", "unknown"])
def test_a_dataset_with_other_rules_stays_with_the_row_code(dataset) -> None:
    names = INPUT_COLUMNS.get(dataset, ["a"])
    assert not congress_bulk.eligible(dataset, pa.schema([(name, pa.string()) for name in names]))


def test_a_typed_or_unknown_source_column_stays_with_the_row_code() -> None:
    names = INPUT_COLUMNS["member_votes"]
    assert congress_bulk.eligible("member_votes", pa.schema([(name, pa.string()) for name in names[:5]]))
    assert not congress_bulk.eligible("member_votes", pa.schema([(names[0], pa.int64())]))
    assert not congress_bulk.eligible("member_votes", pa.schema([("not_declared", pa.string())]))
