"""``write_split``: the one loop behind every bulk receipt writer."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import RECEIPT_SCHEMA

SUBJECT = pa.schema([("id", pa.string())])
WITNESS = "CAST([] AS STRUCT(source_id VARCHAR, source_uri VARCHAR, sha256 VARCHAR, locator VARCHAR, body_version VARCHAR)[])"


def _query(*, descending=False):
    """Every fifth row is the row code's; every row but those proven is accepted."""
    strings = ", ".join(f"CAST(NULL AS VARCHAR) AS {name}" for name in RECEIPT_SCHEMA.names
                        if name not in {"witnesses", "receipt_id", "outcome", "attempt_id"})
    return (f"SELECT CAST(file_row_number AS VARCHAR) AS id, 'r' || file_row_number AS receipt_id, 'accepted' AS outcome, "
            f"'a' || file_row_number AS attempt_id, {strings}, {WITNESS} AS witnesses, file_row_number AS _ordinal, "
            f"file_row_number % 5 = 4 AS _reference FROM {etl_bulk.SOURCE_BATCH}"
            + (" ORDER BY file_row_number DESC" if descending else ""))


def _source(tmp_path, rows):
    path = tmp_path / "source.parquet"
    pq.write_table(pa.table({"v": [str(i) for i in range(rows)]}), path, row_group_size=7)
    return path


def _split(tmp_path, rows=23, *, query=None, expected=None, **options):
    seen = []

    def attempt(table, index):
        ordinal = table["_ordinal"][index].as_py()
        seen.append(ordinal)
        receipt = {name: None for name in RECEIPT_SCHEMA.names} | {
            "receipt_id": f"row{ordinal}", "outcome": "refused" if ordinal % 10 == 9 else "accepted",
            "witnesses": []}
        return ({"id": f"row{ordinal}"} if receipt["outcome"] == "accepted" else None), receipt

    source = _source(tmp_path, rows)
    with etl_bulk.bulk_connection() as (con, _):
        routed = etl_bulk.write_split(
            con, query or _query(), source=source, batch_rows=5, expected_rows=rows if expected is None else expected,
            subject_schema=SUBJECT, subject_path=tmp_path / "s.parquet", receipt_path=tmp_path / "r.parquet",
            row_attempt=attempt, label="items", **options)
    return routed, seen


def _ids(path, column):
    return pq.read_table(path)[column].to_pylist()


def test_rows_arrive_in_order_across_source_batches_and_the_row_code_decides_its_own(tmp_path):
    routed, seen = _split(tmp_path)
    assert routed == 4 and seen == [4, 9, 14, 19]
    # Subjects: the proven rows and the accepted decided ones, each at its own position.
    expected = [f"row{i}" if i % 5 == 4 else str(i) for i in range(23) if i % 10 != 9]
    assert _ids(tmp_path / "s.parquet", "id") == expected
    assert _ids(tmp_path / "r.parquet", "receipt_id") == [f"row{i}" if i % 5 == 4 else f"r{i}" for i in range(23)]


def test_refusals_follow_every_accepted_receipt_after_the_closing_receipts(tmp_path):
    _split(tmp_path, refusals_last=True, last_receipts=[{name: None for name in RECEIPT_SCHEMA.names} | {
        "receipt_id": "footer", "witnesses": []}])
    accepted = [f"row{i}" if i % 5 == 4 else f"r{i}" for i in range(23) if i % 10 != 9]
    assert _ids(tmp_path / "r.parquet", "receipt_id") == [*accepted, "footer", "row9", "row19"]


def test_an_out_of_order_stream_is_refused(tmp_path):
    with pytest.raises(RuntimeError, match="items: source rows did not arrive in file order"):
        _split(tmp_path, query=_query(descending=True))


def test_a_stream_cut_short_is_refused(tmp_path):
    with pytest.raises(RuntimeError, match="items: read 23 of the source's 24 rows"):
        _split(tmp_path, expected=24)


@pytest.mark.parametrize("failure, invalid", [(duckdb.OutOfMemoryException("injected"), False),
                                              (duckdb.InvalidInputException("injected"), True)])
def test_a_bulk_failure_is_never_retried_by_row(tmp_path, monkeypatch, failure, invalid):
    from tests.duckdb_faults import assert_raised_as, fail_streams_after

    delivered = fail_streams_after(monkeypatch, failure)
    with pytest.raises(etl_bulk.BulkValidationError) as caught:
        _split(tmp_path)
    assert delivered and delivered[0] > 0
    assert_raised_as(caught, failure, invalid)
