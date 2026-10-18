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

    def attempt(table, index, row):
        ordinal = table["_ordinal"][index].as_py()
        assert row == {"v": str(ordinal)}
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


def test_a_routed_row_gets_the_source_row_not_the_encoded_text(tmp_path):
    """A typed value SQL does not own (a year-999 timestamp) spells as a marker in ``_raw``; the row code never sees it."""
    import datetime as dt

    ancient = dt.datetime(999, 1, 2, tzinfo=dt.timezone.utc)
    source = tmp_path / "typed.parquet"
    pq.write_table(pa.table({"t": pa.array([ancient, dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)], pa.timestamp("us", tz="UTC"))}),
                   source)
    raw = etl_bulk.record_json_sql([("t", pa.timestamp("us", tz="UTC"))])
    query = (_query().replace("file_row_number % 5 = 4 AS _reference", f"{etl_bulk.needs_reference_sql(raw)} AS _reference")
             .replace("FROM " + etl_bulk.SOURCE_BATCH, f", {raw} AS _raw FROM " + etl_bulk.SOURCE_BATCH))
    rows = []

    def attempt(table, index, row):
        assert "route" in table["_raw"][index].as_py()  # the marker is in the query's text, as the review probed
        rows.append(row)
        return {"id": "x"}, {name: None for name in RECEIPT_SCHEMA.names} | {"receipt_id": "x", "outcome": "accepted", "witnesses": []}

    with etl_bulk.bulk_connection() as (con, _):
        assert etl_bulk.write_split(
            con, query, source=source, batch_rows=5, expected_rows=2, subject_schema=SUBJECT, subject_path=tmp_path / "s.parquet",
            receipt_path=tmp_path / "r.parquet", row_attempt=attempt, label="typed") == 1
    assert rows == [{"t": ancient}]


def test_an_isolated_row_is_its_own_receipt_row_group(tmp_path):
    def isolating(table):
        for index in range(len(table)):
            yield table.slice(index, 1)

    query = _query().replace("file_row_number % 5 = 4 AS _reference", "file_row_number % 5 = 4 AS _reference, "
                                                                        "file_row_number = 11 AS _big")
    _split(tmp_path, query=query, segments=isolating, isolate="_big")
    groups = [pq.ParquetFile(tmp_path / "r.parquet").metadata.row_group(i).num_rows for i in range(
        pq.ParquetFile(tmp_path / "r.parquet").metadata.num_row_groups)]
    assert 1 in groups and sum(groups) == 23
    assert len(groups) <= 4  # 11 rows before it, it alone, 11 after: not 23 one-row groups


def test_closing_receipts_stream_in_bounded_groups_before_generator_finishes(tmp_path, monkeypatch):
    original = pq.ParquetWriter
    written = []

    class RecordingWriter:
        def __init__(self, path, *args, **kwargs):
            self.writer = original(path, *args, **kwargs)
            self.record = path == tmp_path / "r.parquet"

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.writer.close()

        def __getattr__(self, name):
            return getattr(self.writer, name)

        def write_table(self, table, **kwargs):
            if self.record:
                written.append(table.num_rows)
            self.writer.write_table(table, **kwargs)

    def closing():
        for i in range(4001):
            if i == 2000:
                assert written  # materializing the whole iterable would fail here
            yield {"receipt_id": f"closing-{i}", "outcome": "refused", "witnesses": []}

    monkeypatch.setattr(pq, "ParquetWriter", RecordingWriter)
    _split(tmp_path, last_receipts=closing())
    assert _ids(tmp_path / "r.parquet", "receipt_id")[-4001:] == [f"closing-{i}" for i in range(4001)]
    assert max(written) <= etl_bulk.RECEIPT_ROW_GROUP


def test_unmapped_eligibility_warns_with_count_while_mapped_fallback_still_refuses():
    from loguru import logger

    messages = []
    sink = logger.add(messages.append, format="{message}")
    try:
        with etl_bulk.bulk_limits(threads=1, memory_limit="64MB", require_bulk=True):
            etl_bulk.log_row_eligibility("court write", dataset="small", rows=17, reason="no bulk mapping")
            with pytest.raises(ValueError, match="reference fallback disabled"):
                etl_bulk.log_row_fallback("court write", etl_bulk.NotBulkEligible("schema differs"),
                                          dataset="mapped", rows=19)
    finally:
        logger.remove(sink)
    assert any("dataset=small rows=17 reason=no bulk mapping" in message for message in messages)
