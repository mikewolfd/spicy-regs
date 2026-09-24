"""Full-comment migration must not silently discard unpartitionable source rows."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.migrate_comments_partitioned import migrate


def write_parent(output_dir: Path, rows: list[dict]) -> Path:
    path = output_dir / "comments.parquet"
    schema = pa.schema([(column, pa.string()) for column in ("comment_id", "agency_code", "docket_id", "posted_date")])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


ROW = {
    "comment_id": "EPA-HQ-OAR-2001-0002-0023",
    "agency_code": "EPA",
    "docket_id": "EPA-HQ-OAR-2001-0002",
    "posted_date": "2022-01-21T20:29:10Z",
}


@pytest.mark.parametrize(
    "field,value",
    [
        ("posted_date", "not-a-date"),
        ("posted_date", "infinity"),
        ("agency_code", None),
        ("agency_code", "../EPA"),
        ("agency_code", "__HIVE_DEFAULT_PARTITION__"),
        ("comment_id", None),
        ("comment_id", " "),
        ("docket_id", "__HIVE_DEFAULT_PARTITION__"),
        ("docket_id", ""),
        ("docket_id", "../EPA-2001-0002"),
    ],
)
def test_invalid_coordinates_refuse_before_any_replacement(tmp_path, field, value):
    parent = write_parent(tmp_path, [ROW, {**ROW, "comment_id": "other", field: value}])
    partition = tmp_path / "comments/retained/part-0.parquet"
    partition.parent.mkdir(parents=True)
    partition.write_bytes(b"existing partition")
    index = tmp_path / "comments_index.parquet"
    index.write_bytes(b"existing index")
    before = {path: path.read_bytes() for path in (parent, partition, index)}

    with pytest.raises(ValueError, match="1 rows have missing or invalid coordinates"):
        migrate(tmp_path)

    assert {path: path.read_bytes() for path in before} == before
    assert set(tmp_path.rglob("*")) == {parent, partition.parent.parent, partition.parent, partition, index}


@pytest.mark.parametrize("docket", [ROW["docket_id"], None])
@pytest.mark.parametrize("date", [ROW["posted_date"], None])
def test_null_coordinates_have_null_index_values_and_hive_partition_paths(tmp_path, docket, date):
    row = {**ROW, "posted_date": date, "docket_id": docket}
    write_parent(tmp_path, [row])
    migrate(tmp_path)
    partition = tmp_path / (
        f"comments/agency_code=EPA/docket_id={docket if docket is not None else '__HIVE_DEFAULT_PARTITION__'}/"
        f"year={2022 if date is not None else '__HIVE_DEFAULT_PARTITION__'}/"
        f"month={1 if date is not None else '__HIVE_DEFAULT_PARTITION__'}/part-0.parquet"
    )
    assert pq.ParquetFile(partition).read().to_pylist() == [row]
    assert pq.read_table(tmp_path / "comments_index.parquet").to_pylist() == [
        {"agency_code": "EPA", "docket_id": docket, "year": 2022 if date else None,
         "month": 1 if date else None, "row_count": 1}
    ]


def test_valid_rows_retain_source_dates_and_reconcile_index(tmp_path):
    rows = [ROW, {**ROW, "comment_id": "other", "posted_date": "2022-02-01T00:01:00+02:00"}]
    write_parent(tmp_path, rows)
    migrate(tmp_path)
    files = sorted((tmp_path / "comments").rglob("part-0.parquet"))
    output = [row for file in files for row in pq.ParquetFile(file).read().to_pylist()]
    assert output == rows
    index = pq.read_table(tmp_path / "comments_index.parquet").to_pylist()
    assert {(row["year"], row["month"], row["row_count"]) for row in index} == {(2022, 1, 1), (2022, 2, 1)}
    assert sum(row["row_count"] for row in index) == len(rows)
