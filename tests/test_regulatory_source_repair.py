"""An explicit native reread repairs equal dates without losing other evidence."""

from copy import deepcopy
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import pytest

from spicy_regs.pipelines.repair_regulations import repair_records
from spicy_regs.schemas import RECORD_TYPES
from spicy_regs.transforms.update_comments_index import update_comments_index


FIXTURES = Path(__file__).parent / "fixtures/regulatory_recovery"


def raw(identity="ACF-2006-0058-0001"):
    return json.loads((FIXTURES / f"{identity}.json").read_text())


def write(path, rows, table="documents"):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in RECORD_TYPES[table].schema])), path
    )


def shaped(value, table="documents"):
    return {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in RECORD_TYPES[table].extract(value).items()}


@pytest.fixture(autouse=True)
def no_remote(monkeypatch):
    monkeypatch.setattr(
        "spicy_regs.sources.r2.download_from_r2", lambda *a, **k: pytest.fail("local repair tried remote prior")
    )


@pytest.mark.parametrize(
    "prior_date,fresh_wins",
    [
        ("2001-01-01T00:00:00Z", True),
        ("2011-06-11T16:54:52Z", True),
        ("2011-06-11T12:54:52-04:00", True),
        ("2026-09-21T00:00:00Z", False),
        (None, True),
    ],
)
def test_source_recency_equal_date_correction_and_enrichment(tmp_path, prior_date, fresh_wins):
    value = raw()
    expected = shaped(value)
    prior = {
        **expected,
        "modify_date": prior_date,
        "fr_doc_num": None,
        "attachments_json": None,
        "text_content": "retained extracted text",
        "text_extraction_status": "ok",
        "pdf_extraction_results_json": '[{"source":"prior"}]',
    }
    unrelated = {**prior, "document_id": "unrelated", "modify_date": "legacy unknown date"}
    path = tmp_path / "documents.parquet"
    write(path, [prior, unrelated])
    (tmp_path / "manifest.parquet").write_bytes(b"acquisition state must not change")
    repair_records([value], table="documents", output_dir=tmp_path)
    rows = {r["document_id"]: r for r in pq.read_table(path).to_pylist()}
    winner = rows[expected["document_id"]]
    assert winner["fr_doc_num"] == ("06-04731" if fresh_wins else None)
    assert winner["attachments_json"] == (expected["attachments_json"] if fresh_wins else None)
    for c in ("text_content", "text_extraction_status", "pdf_extraction_results_json"):
        assert winner[c] == prior[c]
    assert rows["unrelated"] == unrelated
    assert (tmp_path / "manifest.parquet").read_bytes() == b"acquisition state must not change"
    first = pq.read_table(path).to_pylist()
    repair_records([value], table="documents", output_dir=tmp_path)
    assert pq.read_table(path).to_pylist() == first


def test_source_cleared_values_clear_stale_mapped_facts(tmp_path):
    value = raw("ACF-2019-0005-0243")
    prior = shaped(value)
    assert prior["withdrawn"] == "true" and prior["reason_withdrawn"] == "duplicate document"
    write(tmp_path / "documents.parquet", [prior])
    for name in ("withdrawn", "reasonWithdrawn", "fileFormats", "frDocNum"):
        value["data"]["attributes"][name] = None
    repair_records([value], table="documents", output_dir=tmp_path)
    [result] = pq.read_table(tmp_path / "documents.parquet").to_pylist()
    assert all(
        result[c] is None for c in ("withdrawn", "reason_withdrawn", "attachments_json", "file_url", "fr_doc_num")
    )


def test_undated_source_does_not_replace_dated_prior(tmp_path):
    value = raw()
    prior = shaped(value)
    write(tmp_path / "documents.parquet", [prior])
    value["data"]["attributes"].update(modifyDate=None, title="undated changed title")
    repair_records([value], table="documents", output_dir=tmp_path)
    assert pq.read_table(tmp_path / "documents.parquet").to_pylist() == [prior]


@pytest.mark.parametrize("fault", ["date", "duplicate", "late_failure", "corrupt_prior"])
def test_failed_input_preserves_prior_and_can_retry(tmp_path, fault):
    value = raw()
    path = tmp_path / "documents.parquet"
    prior = {**shaped(value), "fr_doc_num": None}
    write(path, [prior])
    if fault == "corrupt_prior":
        path.write_bytes(b"damaged retained file")
    before = path.read_bytes()
    values = [deepcopy(value)]
    if fault == "date":
        values[0]["data"]["attributes"]["modifyDate"] = "not-a-date"
    if fault == "duplicate":
        values.append(value)

    def records():
        yield from values
        if fault == "late_failure":
            raise ValueError("source read unresolved")

    with pytest.raises(ValueError):
        repair_records(records(), table="documents", output_dir=tmp_path)
    assert path.read_bytes() == before
    assert not (tmp_path / "manifest.parquet").exists()
    write(path, [prior])
    repair_records([value], table="documents", output_dir=tmp_path)
    assert pq.read_table(path).to_pylist()[0]["fr_doc_num"] == "06-04731"


def test_comment_repair_preserves_local_partition_and_enrichment(tmp_path):
    value = raw("ACF-2009-0004-0002")
    expected = shaped(value, "comments")
    path = tmp_path / "comments/agency_code=ACF/docket_id=ACF-2009-0004/year=2009/month=3/part-0.parquet"
    prior = {**expected, "attachments_json": None, "organization": None, "text_content": "retained text"}
    write(path, [prior], "comments")
    repair_records([value], table="comments", output_dir=tmp_path)
    [row] = pq.ParquetFile(path).read().to_pylist()
    assert row["attachments_json"] == expected["attachments_json"]
    assert row["organization"] == "FLORIDA DEPARTMENT OF REVENUE, CSE"
    assert row["text_content"] == "retained text"
    assert (tmp_path / "comments_index.parquet").exists()
    repair_records([value], table="comments", output_dir=tmp_path)
    assert pq.ParquetFile(path).read().to_pylist() == [row]


@pytest.mark.parametrize("coordinate,invalid", [("agencyId", None), ("docketId", "../unsafe")])
def test_comment_with_invalid_partition_coordinate_refuses(tmp_path, coordinate, invalid):
    value = raw("ACF-2009-0004-0002")
    value["data"]["attributes"][coordinate] = invalid
    with pytest.raises(ValueError, match="partition"):
        repair_records([value], table="comments", output_dir=tmp_path)
    assert not list(tmp_path.rglob("*.parquet"))


def test_comment_relocation_refuses_before_creating_duplicate(tmp_path):
    value = raw("ACF-2009-0004-0002")
    old_path = tmp_path / "comments/agency_code=ACF/docket_id=ACF-2009-0004/year=2009/month=3/part-0.parquet"
    write(old_path, [shaped(value, "comments")], "comments")
    before = old_path.read_bytes()
    value["data"]["attributes"]["postedDate"] = "2009-04-11T04:00:00Z"
    with pytest.raises(ValueError, match="relocate"):
        repair_records([value], table="comments", output_dir=tmp_path)
    assert old_path.read_bytes() == before
    assert list((tmp_path / "comments").rglob("*.parquet")) == [old_path]


def test_comment_in_noncanonical_part_refuses_before_creating_duplicate(tmp_path):
    value = raw("ACF-2009-0004-0002")
    old_path = tmp_path / "comments/agency_code=ACF/docket_id=ACF-2009-0004/year=2009/month=3/part-1.parquet"
    write(old_path, [shaped(value, "comments")], "comments")
    before = old_path.read_bytes()
    with pytest.raises(ValueError, match="relocate"):
        repair_records([value], table="comments", output_dir=tmp_path)
    assert old_path.read_bytes() == before
    assert list((tmp_path / "comments").rglob("*.parquet")) == [old_path]


def test_comment_failed_replacement_keeps_prior_and_remains_retryable(tmp_path, monkeypatch):
    value = raw("ACF-2009-0004-0002")
    path = tmp_path / "comments/agency_code=ACF/docket_id=ACF-2009-0004/year=2009/month=3/part-0.parquet"
    write(path, [{**shaped(value, "comments"), "attachments_json": None}], "comments")
    before = path.read_bytes()
    replace = Path.replace

    def fail_partition_replace(self, target):
        if Path(target) == path:
            raise OSError("injected partition replacement failure")
        return replace(self, target)

    with monkeypatch.context() as context:
        context.setattr(Path, "replace", fail_partition_replace)
        with pytest.raises(OSError, match="injected"):
            repair_records([value], table="comments", output_dir=tmp_path)
    assert path.read_bytes() == before
    assert path.with_suffix(".tmp.parquet").exists()
    repair_records([value], table="comments", output_dir=tmp_path)
    assert pq.ParquetFile(path).read().to_pylist()[0]["attachments_json"] is not None


@pytest.mark.parametrize("failure_kind", ["write", "replace"])
def test_comment_index_failure_preserves_prior_and_remains_retryable(tmp_path, monkeypatch, failure_kind):
    value = raw("ACF-2009-0004-0002")
    path = tmp_path / "comments/agency_code=ACF/docket_id=ACF-2009-0004/year=2009/month=3/part-0.parquet"
    write(path, [{**shaped(value, "comments"), "attachments_json": None}], "comments")
    index = update_comments_index(tmp_path, [path])
    before = index.read_bytes()
    replace = Path.replace
    write_parquet = pl.DataFrame.write_parquet

    def fail_index_write(self, target, *args, **kwargs):
        if Path(target).name.startswith("comments_index."):
            Path(target).write_bytes(b"injected incomplete index")
            raise OSError("injected index write failure")
        return write_parquet(self, target, *args, **kwargs)

    def fail_index_replace(self, target):
        if Path(target) == index:
            raise OSError("injected index replacement failure")
        return replace(self, target)

    with monkeypatch.context() as context:
        if failure_kind == "write":
            context.setattr(pl.DataFrame, "write_parquet", fail_index_write)
        else:
            context.setattr(Path, "replace", fail_index_replace)
        with pytest.raises(OSError, match="injected"):
            repair_records([value], table="comments", output_dir=tmp_path)
    assert index.read_bytes() == before
    assert pq.ParquetFile(path).read().to_pylist()[0]["attachments_json"] is not None
    repair_records([value], table="comments", output_dir=tmp_path)
    assert pq.ParquetFile(path).read().to_pylist()[0]["attachments_json"] is not None
    assert pl.read_parquet(index)["row_count"].to_list() == [1]


def test_literal_rin_and_placeholders_are_not_rewritten(tmp_path):
    value = raw("ACF-2015-0001")
    repair_records([value], table="dockets", output_dir=tmp_path)
    assert pq.read_table(tmp_path / "dockets.parquet").to_pylist()[0]["rin"] == "0970-AC47"
    value["data"]["attributes"]["rin"] = "Not Assigned"
    repair_records([value], table="dockets", output_dir=tmp_path)
    assert pq.read_table(tmp_path / "dockets.parquet").to_pylist()[0]["rin"] == "Not Assigned"


def test_unknown_date_comment_repair_preserves_nulls_and_retries(tmp_path):
    value = raw("ACF-2009-0004-0002")
    value["data"]["attributes"]["postedDate"] = None
    path = tmp_path / (
        "comments/agency_code=ACF/docket_id=ACF-2009-0004/"
        "year=__HIVE_DEFAULT_PARTITION__/month=__HIVE_DEFAULT_PARTITION__/part-0.parquet"
    )
    expected = shaped(value, "comments")
    write(path, [{**expected, "organization": None, "text_content": "prior enrichment"}], "comments")
    repair_records([value], table="comments", output_dir=tmp_path)
    [row] = pq.ParquetFile(path).read().to_pylist()
    assert row["organization"] == expected["organization"]
    assert row["posted_date"] is None
    assert row["text_content"] == "prior enrichment"
    repair_records([value], table="comments", output_dir=tmp_path)
    assert pq.ParquetFile(path).read().to_pylist() == [row]
    index = pq.read_table(tmp_path / "comments_index.parquet").to_pylist()
    assert len(index) == 1 and index[0]["year"] is None and index[0]["month"] is None
    assert index[0]["row_count"] == 1

    # A later observed date changes the partition; bounded repair must refuse
    # rather than create a second copy of the retained identity.
    before = path.read_bytes()
    value["data"]["attributes"]["postedDate"] = "2009-03-01T00:00:00Z"
    with pytest.raises(ValueError, match="relocate"):
        repair_records([value], table="comments", output_dir=tmp_path)
    assert path.read_bytes() == before
    assert list((tmp_path / "comments").rglob("part-0.parquet")) == [path]


@pytest.mark.parametrize(
    "fresh_text,expected",
    [
        # A pre-A6 reread carrying its own text and status replaces all three, provenance included.
        (("old derived text", "ok", None), ("old derived text", "ok", None)),
        # A PDF outcome without text is still a fill: its status and results replace the prior's.
        ((None, "empty", '[{"status":"empty"}]'), (None, "empty", '[{"status":"empty"}]')),
        # Neither text nor status: the prior's fill is kept whole.
        ((None, None, '[{"stray":"results"}]'), ("repaired text", "derived", '{"tool":"pypdf"}')),
    ],
)
def test_correction_takes_the_text_columns_together(fresh_text, expected):
    import duckdb

    from spicy_regs.transforms.regulations_correction import correction_query

    columns = ["comment_id", "modify_date", "text_content", "text_extraction_status", "pdf_extraction_results_json"]
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE prior AS SELECT 'C1' comment_id, '2026-01-01T00:00:00Z' modify_date, "
        "'repaired text' text_content, 'derived' text_extraction_status, '{\"tool\":\"pypdf\"}' pdf_extraction_results_json"
    )
    con.execute(
        "CREATE TABLE fresh AS SELECT 'C1' comment_id, '2026-01-01T00:00:00Z' modify_date, "
        "?::VARCHAR text_content, ?::VARCHAR text_extraction_status, ?::VARCHAR pdf_extraction_results_json",
        list(fresh_text),
    )
    query = correction_query(
        con, fresh_sql="SELECT * FROM fresh", prior_sql="SELECT * FROM prior", columns=columns, key="comment_id"
    )
    row = con.execute(query).fetchone()
    assert row is not None and row[2:] == expected
