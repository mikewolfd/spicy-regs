"""An explicit native reread repairs equal dates without losing other evidence."""

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.repair_regulations import COMMENT_RECEIPT, repair_records
from spicy_regs.schemas import COMMENT, RECORD_TYPES
from spicy_regs.sources import iceberg
from tests.test_backfill_derived_text import _seed_catalog


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


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """The Iceberg tests' local stand-in catalog; each replace commits one new snapshot."""
    database = tmp_path / "catalog.duckdb"
    state = {"snapshot": 1}

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{database}' AS {iceberg._CATALOG_ALIAS}")
        return con

    real_replace = iceberg.replace_rows

    def replace(con, record_type, source):
        real_replace(con, record_type, source)
        state["snapshot"] += 1

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setattr(iceberg, "_read_snapshot", lambda con, rt: iceberg.CatalogSnapshot("local", state["snapshot"], 0))
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, snapshot: f"SELECT * FROM {iceberg._qualified(rt)}")
    monkeypatch.setattr(iceberg, "replace_rows", replace)

    def seed(rows):
        with connect() as con:
            _seed_catalog(con, rows)

    def rows():
        with connect() as con:
            return con.execute(f"SELECT * FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id").pl().to_dicts()

    return SimpleNamespace(seed=seed, rows=rows)


COMMENT_ID = "ACF-2009-0004-0002"
UNRELATED = {**dict.fromkeys(COMMENT.schema), "comment_id": "ACF-2009-0004-0001", "agency_code": "ACF",
             "docket_id": "ACF-2009-0004", "modify_date": "2009-03-10T00:00:00Z", "text_content": "untouched"}


def comment_prior(**changes):
    """The fixture comment as the catalog holds it, with stale mapped facts and a retained text fill."""
    return {**shaped(raw(COMMENT_ID), "comments"), "organization": None, "attachments_json": None,
            "category": "stale category", "text_content": "retained text", "text_extraction_status": "derived",
            "pdf_extraction_results_json": '{"tool":"pypdf"}', **changes}


def receipt(path):
    return json.loads((path / COMMENT_RECEIPT).read_text())


def test_comment_dry_run_writes_only_a_receipt(tmp_path, catalog):
    catalog.seed([comment_prior(), UNRELATED])
    before = catalog.rows()
    output = tmp_path / "out"
    result = repair_records([raw(COMMENT_ID)], table="comments", output_dir=output, source_pins={"logical_id": "L"})
    assert catalog.rows() == before
    assert [p.name for p in output.iterdir()] == [COMMENT_RECEIPT]
    written = receipt(output)
    assert (written["mode"], written["source"], written["catalog_snapshot"]["snapshot_id"]) == ("dry-run", {"logical_id": "L"}, 1)
    assert written["identities"] == [COMMENT_ID] and written["missing_identities"] == []
    [change] = written["changes"]
    assert change["cells"]["category"] == {"before": "stale category", "after": None}
    assert change["cells"]["organization"]["after"] == "FLORIDA DEPARTMENT OF REVENUE, CSE"
    assert written["rows"][0]["text_content"] == "retained text"
    assert written["applied_snapshot"] is None and result["changed_rows"] == 1


def test_comment_apply_corrects_at_an_equal_timestamp_and_keeps_enrichment(tmp_path, catalog):
    catalog.seed([comment_prior(), UNRELATED])
    repair_records([raw(COMMENT_ID)], table="comments", output_dir=tmp_path, apply=True, expected_snapshot=1)
    expected = shaped(raw(COMMENT_ID), "comments")
    [row] = [row for row in catalog.rows() if row["comment_id"] == COMMENT_ID]
    assert row == {**expected, "text_content": "retained text", "text_extraction_status": "derived",
                   "pdf_extraction_results_json": '{"tool":"pypdf"}'}
    assert row["category"] is None  # a fresh NULL clears the stale value
    assert UNRELATED in catalog.rows()
    assert receipt(tmp_path)["applied_snapshot"]["snapshot_id"] == 2
    # A rerun finds nothing left to correct and commits nothing.
    repair_records([raw(COMMENT_ID)], table="comments", output_dir=tmp_path, apply=True)
    assert receipt(tmp_path)["rows"] == [] and receipt(tmp_path)["applied_snapshot"] is None


def test_comment_repair_keeps_a_newer_prior(tmp_path, catalog):
    catalog.seed([comment_prior(modify_date="2026-09-21T00:00:00Z")])
    before = catalog.rows()
    repair_records([raw(COMMENT_ID)], table="comments", output_dir=tmp_path, apply=True)
    assert catalog.rows() == before
    assert receipt(tmp_path)["changes"] == []


@pytest.mark.parametrize("moved", ["concurrent writer", "reviewed pin"])
def test_comment_apply_refuses_a_moved_snapshot(tmp_path, catalog, monkeypatch, moved):
    catalog.seed([comment_prior()])
    before = catalog.rows()
    if moved == "concurrent writer":
        snapshots = iter([1, 2])
        monkeypatch.setattr(iceberg, "_read_snapshot", lambda con, rt: iceberg.CatalogSnapshot("local", next(snapshots), 0))
    with pytest.raises(RuntimeError, match="moved since" if moved == "concurrent writer" else "not the reviewed"):
        repair_records([raw(COMMENT_ID)], table="comments", output_dir=tmp_path, apply=True,
                       expected_snapshot=7 if moved == "reviewed pin" else None)
    assert catalog.rows() == before


def test_comment_apply_fails_loudly_when_the_delete_leaves_the_prior_row(tmp_path, catalog, monkeypatch):
    """The catalog's DELETE has left rows behind before; the repair checks its own write."""
    catalog.seed([comment_prior()])
    columns = ", ".join(f'"{c}"' for c in COMMENT.schema)
    monkeypatch.setattr(iceberg, "replace_rows", lambda con, rt, source: con.execute(
        f"INSERT INTO {iceberg._qualified(rt)} ({columns}) SELECT {columns} FROM {source}"))
    with pytest.raises(RuntimeError, match="DELETE left rows behind"):
        repair_records([raw(COMMENT_ID)], table="comments", output_dir=tmp_path, apply=True)
    assert receipt(tmp_path)["applied_snapshot"] is None


def test_comment_apply_refuses_an_identity_missing_from_the_catalog(tmp_path, catalog):
    catalog.seed([UNRELATED])
    with pytest.raises(ValueError, match="not in the catalog"):
        repair_records([raw(COMMENT_ID)], table="comments", output_dir=tmp_path, apply=True)
    assert catalog.rows() == [UNRELATED]
    assert receipt(tmp_path)["missing_identities"] == [COMMENT_ID]


def test_literal_rin_and_placeholders_are_not_rewritten(tmp_path):
    value = raw("ACF-2015-0001")
    repair_records([value], table="dockets", output_dir=tmp_path)
    assert pq.read_table(tmp_path / "dockets.parquet").to_pylist()[0]["rin"] == "0970-AC47"
    value["data"]["attributes"]["rin"] = "Not Assigned"
    repair_records([value], table="dockets", output_dir=tmp_path)
    assert pq.read_table(tmp_path / "dockets.parquet").to_pylist()[0]["rin"] == "Not Assigned"


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
