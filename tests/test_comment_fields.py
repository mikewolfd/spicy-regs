"""The comment-field fill: a resumable read that holds no lock, and a catalog write that fills only what is unread."""

import json
from pathlib import Path

import duckdb
import polars as pl
import pytest

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.schemas import COMMENT
from tests.test_regulations_pipeline import _FakeS3Resource, _FlakyResource

FIXTURES = Path(__file__).parent / "fixtures/comments_submitter_fields"
PFAS = "EPA-HQ-OW-2022-0114"


def _key(comment_id: str, copy: str = "") -> str:
    docket = comment_id.rsplit("-", 1)[0]
    agency = docket.split("-")[0]
    return f"raw-data/{agency}/{docket}/text-{docket}/comments/{comment_id}{copy}.json"


def _store() -> dict[str, bytes]:
    return {_key(path.name.removesuffix(".source.json")): path.read_bytes() for path in FIXTURES.glob("*.source.json")}


def _manifest(tmp_path: Path, keys, name="manifest.parquet") -> Path:
    path = tmp_path / name
    pl.DataFrame({"key": [*keys, "raw-data/EPA/EPA-X/text-EPA-X/documents/EPA-X-0001.json"]}).write_parquet(path)
    return path


def _parts(workdir: Path) -> list[dict]:
    with duckdb.connect() as con:
        return con.execute(f"SELECT * FROM read_parquet('{workdir}/parts/*/*.parquet', hive_partitioning=false) "
                           "ORDER BY key").pl().to_dicts()


def test_plan_chunks_comment_keys_per_agency_and_a_newer_manifest_adds_only_new_keys(tmp_path):
    keys = sorted(_store())
    first = cf.plan(tmp_path, _manifest(tmp_path, keys[:3]), chunk_keys=1)
    assert cf.plan(tmp_path, _manifest(tmp_path, keys[:3]), chunk_keys=1) == first  # same manifest: no new plan
    rows = pl.read_parquet(first).to_dicts()
    assert [r["key"] for r in rows] == keys[:3]  # documents are not planned
    assert {(r["agency"], r["chunk"]) for r in rows} == {("CMS", 0), ("EPA", 0), ("EPA", 1)}
    second = cf.plan(tmp_path, _manifest(tmp_path, keys, "newer.parquet"), chunk_keys=2)
    assert pl.read_parquet(second)["key"].to_list() == keys[3:]
    assert cf.status(tmp_path)["planned_keys"] == len(keys)


def test_read_keeps_the_thin_row_the_body_digest_and_every_other_stated_attribute(tmp_path):
    import hashlib

    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    totals = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    assert (totals["chunks"], totals["keys"], totals["rows"], totals["deferred_chunks"]) == (2, 4, 4, 0)
    rows = {row["comment_id"]: row for row in _parts(tmp_path)}
    campaign = rows[f"{PFAS}-1811"]
    assert list(campaign) == list(cf.PART_SCHEMA) and "comment" not in campaign
    raw = json.loads(store[_key(f"{PFAS}-1811")])
    attributes = raw["data"]["attributes"]
    assert (campaign["subtype"], campaign["duplicate_comments"]) == ("Mass Mail Campaign", 15851)
    assert json.loads(campaign["attachments_json"]) == json.loads(COMMENT.extract(raw)["attachments_json"])
    assert campaign["comment_sha256"] == hashlib.sha256(attributes["comment"].encode()).hexdigest()
    assert campaign["comment_length"] == len(attributes["comment"])
    assert campaign["size"] == len(store[_key(f"{PFAS}-1811")])
    assert json.loads(campaign["attributes_json"]) == {
        key: value for key, value in attributes.items() if key not in cf.EXTRACTED_ATTRIBUTES and value is not None
    }
    assert "trackingNbr" in json.loads(campaign["attributes_json"])
    assert rows["CMS-2016-0123-0993"]["duplicate_comments"] == 0  # a stated zero stays zero
    assert cf.status(tmp_path)["read_keys"] == 4


def test_extracted_attributes_are_the_keys_the_extract_reads():
    """Derived, not listed: a key is extracted exactly when changing its value changes the extract's row."""
    reads = set()
    for path in FIXTURES.glob("*.source.json"):
        raw = json.loads(path.read_text())
        base = COMMENT.extract(raw)
        for key in raw["data"]["attributes"]:
            changed = json.loads(json.dumps(raw))
            changed["data"]["attributes"][key] = "\u2063probe"
            if COMMENT.extract(changed) != base:
                reads.add(key)
    assert reads == cf.EXTRACTED_ATTRIBUTES


def test_the_read_stops_before_a_chunk_when_the_disk_is_short(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    stopped = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store), min_free_gb=10**9)
    assert stopped["chunks"] == 0 and "free" in stopped["stopped"]
    assert not (tmp_path / "parts").exists()


def test_a_chunk_with_transport_failures_is_not_written_and_a_rerun_reads_only_the_rest(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    failing = {_key(f"{PFAS}-0017")}
    active = {"active": True}
    first = cf.read(tmp_path, workers=2, resource=_FlakyResource(store, failing, active))
    assert (first["chunks"], first["deferred_chunks"]) == (1, 1)  # CMS written; EPA deferred whole
    assert {row["agency_code"] for row in _parts(tmp_path)} == {"CMS"}
    active["active"] = False
    second = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    assert (second["chunks"], second["keys"]) == (1, 3)  # only the deferred chunk
    assert len(_parts(tmp_path)) == 4
    journal = [json.loads(line) for line in (tmp_path / "journal.jsonl").read_text().splitlines()]
    assert sorted((j["agency"], j["chunk"]) for j in journal) == [("CMS", 0), ("EPA", 0)]  # each chunk once
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["chunks"] == 0


def test_an_unreadable_object_is_journaled_not_retried_forever(tmp_path):
    store = {**_store(), _key(f"{PFAS}-9999"): b"{not json"}
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    [epa] = [json.loads(line) for line in (tmp_path / "journal.jsonl").read_text().splitlines() if '"EPA"' in line]
    assert epa["unresolved"] == [{"key": _key(f"{PFAS}-9999"), "status": "unreadable"}]
    assert epa["rows"] == 3


def test_the_read_stops_cleanly_past_its_memory_cap_and_resumes(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    stopped = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store), max_rss_mb=1)
    assert stopped["chunks"] == 1 and stopped["stopped"]
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["chunks"] == 1


def test_the_part_holds_every_fill_column_typed_as_the_table():
    assert set(cf.FILL_COLUMNS) <= set(cf.PART_SCHEMA)
    assert cf.FILL_COLUMNS[-3:] == ("subtype", "duplicate_comments", "attachments_json")
    assert cf.PART_SCHEMA["duplicate_comments"] == COMMENT.schema["duplicate_comments"] == pl.Int32


def test_shards_split_chunks_so_every_chunk_is_read_exactly_once(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=1)
    read = [cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store), shard=(i, 3))["keys"] for i in range(3)]
    assert sum(read) == len(store) and len(_parts(tmp_path)) == len(store)
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["chunks"] == 0


# --------------------------------------------------------------------------- #
# Write phase, against a local DuckDB catalog standing in for Iceberg.
# --------------------------------------------------------------------------- #
from spicy_regs.sources import iceberg  # noqa: E402


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """A local catalog file; its snapshot is a digest of its rows, so any write moves it and a no-op does not."""
    path = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{path}' AS {iceberg._CATALOG_ALIAS}")
        return con

    def snapshot(con, record_type):
        digest = con.execute(f"SELECT coalesce(sum(hash(t)), 0) FROM {iceberg._qualified(record_type)} t").fetchone()[0]
        return iceberg.CatalogSnapshot("local", int(digest % 2**62), 1)

    monkeypatch.setattr(iceberg, "_connect", connect)
    monkeypatch.setattr(iceberg, "_read_snapshot", snapshot)
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, _: f"SELECT * FROM {iceberg._qualified(rt)}")
    monkeypatch.setattr(cf, "FILE_COLUMN", "agency_code")  # plain DuckDB has no data-file column; one file per agency
    monkeypatch.setenv("R2_CATALOG_NAMESPACE", "default")
    monkeypatch.setenv(iceberg.CATALOG_LOCK_ENV, "test")
    with connect() as con:
        con.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
        con.execute(f"CREATE TABLE {iceberg._qualified(COMMENT)} (" + ", ".join(f'"{c}" VARCHAR' for c in COMMENT.schema) + ")")
    return connect


def _catalog_row(comment_id, agency="EPA", modify="2020-01-01T00:00:00Z", **values):
    return {**dict.fromkeys(COMMENT.schema), "comment_id": comment_id, "agency_code": agency, "modify_date": modify,
            "comment": f"body of {comment_id}", "title": "t", **values}


def _seed(connect, rows):
    with connect() as con:
        con.register("seed", pl.DataFrame(rows, schema=dict.fromkeys(COMMENT.schema, pl.Utf8)).to_arrow())
        con.execute(f"INSERT INTO {iceberg._qualified(COMMENT)} SELECT * FROM seed")


def _read_rows(tmp_path, rows, agency="EPA"):
    part = tmp_path / "parts" / f"agency={agency}" / "p.parquet"
    part.parent.mkdir(parents=True, exist_ok=True)
    full = [{**dict.fromkeys(cf.PART_SCHEMA), "agency_code": agency, **row} for row in rows]
    pl.DataFrame(full, schema=cf.PART_SCHEMA).write_parquet(part)


def _rows(connect) -> dict[str, dict]:
    with connect() as con:
        return {r["comment_id"]: r for r in con.execute(f"SELECT * FROM {iceberg._qualified(COMMENT)}").pl().to_dicts()}


def _fill(tmp_path, connect, **options):
    prepared = cf.prepare(tmp_path)
    with connect() as con:
        written = cf.write(tmp_path, con=con, **options)
    return prepared, written


def test_fill_writes_only_null_cells_and_keeps_a_stated_zero_distinct_from_null(tmp_path, catalog):
    _seed(catalog, [_catalog_row("A", subtype="Kept"), _catalog_row("B"), _catalog_row("C")])
    _read_rows(tmp_path, [
        {"key": "a", "comment_id": "A", "modify_date": "2020-01-01T00:00:00Z", "subtype": "Read", "duplicate_comments": 3,
         "attachments_json": '[{"title": "x"}]'},
        {"key": "b", "comment_id": "B", "modify_date": "2020-01-01T00:00:00Z", "subtype": "Public Comment",
         "duplicate_comments": 0},
        {"key": "c", "comment_id": "C", "modify_date": "2020-01-01T00:00:00Z", "duplicate_comments": None},
    ])
    prepared, written = _fill(tmp_path, catalog)
    rows = _rows(catalog)
    assert rows["A"]["subtype"] == "Kept"  # a value the catalog holds is never replaced
    assert (rows["A"]["duplicate_comments"], rows["A"]["attachments_json"]) == ("3", '[{"title": "x"}]')
    assert rows["B"]["duplicate_comments"] == "0"  # a stated zero is written
    assert rows["C"]["duplicate_comments"] is None  # nothing stated: stays unread NULL
    assert rows["A"]["comment"] == "body of A"  # other columns untouched
    assert (prepared["rows_to_fill"], written["rows_changed"]) == (2, 2)
    assert prepared["cells_by_column"]["subtype"] == 1 and prepared["cells_by_column"]["attachments_json"] == 1


def test_a_read_of_another_version_fills_nothing_and_is_counted(tmp_path, catalog):
    _seed(catalog, [_catalog_row("A", modify="2020-01-01T00:00:00Z")])
    _read_rows(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2021-06-01T00:00:00Z", "subtype": "Read"}])
    prepared, written = _fill(tmp_path, catalog)
    assert _rows(catalog)["A"]["subtype"] is None
    assert (prepared["other_version_only"], prepared["rows_to_fill"], written["rows_changed"]) == (1, 0, 0)


def test_copies_that_disagree_fill_nothing_and_are_listed(tmp_path, catalog):
    _seed(catalog, [_catalog_row("A"), _catalog_row("B")])
    _read_rows(tmp_path, [
        {"key": "a", "comment_id": "A", "modify_date": "2020-01-01T00:00:00Z", "subtype": "One"},
        {"key": "a(1)", "comment_id": "A", "modify_date": "2020-01-01T00:00:00Z", "subtype": "Other"},
        {"key": "b", "comment_id": "B", "modify_date": "2020-01-01T00:00:00Z", "subtype": "Same"},
        {"key": "b(1)", "comment_id": "B", "modify_date": "2020-01-01T00:00:00Z", "subtype": "Same"},
    ])
    prepared, _ = _fill(tmp_path, catalog)
    rows = _rows(catalog)
    assert (rows["A"]["subtype"], rows["B"]["subtype"]) == (None, "Same")  # agreeing copies are one version
    conflicts = pl.read_parquet(tmp_path / "fill" / "conflicts.parquet").to_dicts()
    assert [(c["comment_id"], list(c["keys"])) for c in conflicts] == [("A", ["a", "a(1)"])]
    assert prepared["conflicted"] == 1


def test_a_rerun_writes_nothing_twice_even_after_losing_its_journal(tmp_path, catalog):
    _seed(catalog, [_catalog_row("A"), _catalog_row("B", agency="CMS")])
    _read_rows(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2020-01-01T00:00:00Z", "subtype": "S"}])
    _read_rows(tmp_path, [{"key": "b", "comment_id": "B", "modify_date": "2020-01-01T00:00:00Z", "subtype": "S"}],
               agency="CMS")
    _, first = _fill(tmp_path, catalog, batch_rows=1)
    assert (first["batches"], first["rows_changed"]) == (2, 2)
    after = _rows(catalog)
    with catalog() as con:
        again = cf.write(tmp_path, con=con, batch_rows=1)
    assert (again["batches"], again["skipped_verified"]) == (0, 2)
    # A crash after COMMIT, before the journal line: the write cannot tell its own commit from another writer's,
    # so it refuses; preparing again finds every cell filled, and the rerun changes nothing.
    (tmp_path / "fill" / "write-journal.jsonl").unlink()
    with catalog() as con, pytest.raises(RuntimeError, match="another writer"):
        cf.write(tmp_path, con=con, batch_rows=1)
    prepared, replay = _fill(tmp_path, catalog, batch_rows=1)
    assert (prepared["rows_to_fill"], replay["batches"], replay["rows_changed"]) == (0, 0, 0)
    assert _rows(catalog) == after


def test_the_write_refuses_without_the_lock_and_when_another_writer_moved_the_catalog(tmp_path, catalog, monkeypatch):
    _seed(catalog, [_catalog_row("A")])
    _read_rows(tmp_path, [{"key": "a", "comment_id": "A", "modify_date": "2020-01-01T00:00:00Z", "subtype": "S"}])
    cf.prepare(tmp_path)
    monkeypatch.delenv(iceberg.CATALOG_LOCK_ENV)
    with catalog() as con, pytest.raises(RuntimeError, match="lock"):
        cf.write(tmp_path, con=con)
    monkeypatch.setenv(iceberg.CATALOG_LOCK_ENV, "test")
    _seed(catalog, [_catalog_row("Z")])  # an ETL write lands after prepare
    with catalog() as con, pytest.raises(RuntimeError, match="another writer"):
        cf.write(tmp_path, con=con)
    assert _rows(catalog)["A"]["subtype"] is None


def test_batches_are_whole_data_files_packed_to_the_row_budget(tmp_path):
    fill = tmp_path / "fill.parquet"
    pl.DataFrame({"_file": ["f1"] * 3 + ["f2"] * 1 + ["f3"] * 5, "comment_id": list("abcdefghi")}).write_parquet(fill)
    assert cf._batches(fill, 4, by_file=True) == [["f1", "f2"], ["f3"]]
    assert cf._batches(fill, 4, by_file=False) == [["f1", "f2", "f3"]]

