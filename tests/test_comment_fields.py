"""The comment-field fill: a resumable read that holds no lock, and a catalog write that fills only what is unread."""

import json
from pathlib import Path

import duckdb
import polars as pl

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


def test_read_keeps_only_the_fill_columns_as_stated(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    totals = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    assert (totals["chunks"], totals["keys"], totals["rows"], totals["deferred_chunks"]) == (2, 4, 4, 0)
    rows = {row["comment_id"]: row for row in _parts(tmp_path)}
    assert set(rows[f"{PFAS}-1811"]) == set(cf.PART_SCHEMA)  # no body, no raw JSON
    assert (rows[f"{PFAS}-1811"]["subtype"], rows[f"{PFAS}-1811"]["duplicate_comments"]) == ("Mass Mail Campaign", 15851)
    assert rows["CMS-2016-0123-0993"]["duplicate_comments"] == 0  # a stated zero stays zero
    assert rows[f"{PFAS}-0002"]["comment_on_document_id"] is not None
    assert cf.status(tmp_path)["read_keys"] == 4


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


def test_part_columns_are_the_fill_columns_plus_identity():
    assert tuple(cf.PART_SCHEMA)[4:] == cf.FILL_COLUMNS
    assert cf.FILL_COLUMNS[-2:] == ("subtype", "duplicate_comments")
    assert cf.PART_SCHEMA["duplicate_comments"] == COMMENT.schema["duplicate_comments"] == pl.Int32
