"""The comment-field fill: a resumable read that holds no lock, and a catalog write that fills only what is unread."""

import json
from hashlib import sha256
from glob import glob
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest
from botocore.exceptions import ClientError

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines.comment_fields_evidence import checkpoint_path
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


@pytest.mark.parametrize("http_status,expected", [(None, "read"), (404, "source-missing"), (412, "source-changed"),
                                                  (503, "transport")])
def test_pinned_chunk_commits_verified_body_evidence_or_retains_failure(tmp_path, http_status, expected):
    key, body = next(iter(_store().items()))
    etag = '"source-etag"'
    pins = tmp_path / "pins.parquet"
    pl.DataFrame({"key": [key], "etag": [etag], "size": [len(body)], "version_id": ["source-version"]}).write_parquet(pins)
    plan = cf.plan(tmp_path, _manifest(tmp_path, [key]), source_pins=pins)
    agency = key.split("/")[1]
    part = cf.part_path(tmp_path, plan.stem, agency, 0)
    requests = []

    def get(**options):
        requests.append(options)
        if http_status is not None:
            raise ClientError({"ResponseMetadata": {"HTTPStatusCode": http_status}}, "GetObject")
        return {"ETag": etag, "ContentLength": len(body), "VersionId": "source-version", "Body": BytesIO(body)}

    resource = SimpleNamespace(Object=lambda bucket, key: SimpleNamespace(get=get))
    first = cf.read(tmp_path, workers=1, resource=resource)
    assert requests == [{"IfMatch": etag, "VersionId": "source-version"}] * (2 if http_status == 503 else 1)
    [evidence] = (part.parent / "source-evidence").glob("*.parquet")
    [observed] = pl.read_parquet(evidence).to_dicts()
    assert observed["status"] == expected
    assert observed["sha256"] == (sha256(body).digest() if expected == "read" else None)
    if expected == "read":
        assert first["rows"] == 1
        assert cf.stage(tmp_path, tmp_path / "stage", profile="attachments")["rows"] == 1
        assert cf.read(tmp_path, workers=1, resource=resource)["chunks"] == 0
        assert len(requests) == 1
        checkpoint_path(part).unlink()
        assert cf.read(tmp_path, workers=1, resource=resource)["rows"] == 1
        assert len(requests) == 2
        selected = json.loads(checkpoint_path(part).read_text())
        (part.parent / selected["evidence_file"]).write_bytes(b"tampered")
        with pytest.raises(ValueError, match="checkpoint differs"):
            cf.read(tmp_path, workers=1, resource=resource)
    else:
        assert first["rows"] == 0
        assert checkpoint_path(part).is_file() == (expected != "transport")
        with pytest.raises(ValueError, match="resolved checkpoint"):
            cf.stage(tmp_path, tmp_path / "stage", profile="attachments")


def test_pinned_chunk_uses_verified_central_original_without_fetching(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    key, body = next(iter(_store().items()))
    digest = sha256(body).hexdigest()
    raw = tmp_path / "raw"
    CentralRawArchiveStore(raw).put_blob(f"sha256:{digest}", len(body), [body])
    pins = tmp_path / "pins.parquet"
    pl.DataFrame({
        "key": [key], "etag": ['"retained-etag"'], "size": [len(body)], "version_id": [None], "raw_sha256": [digest],
    }).write_parquet(pins)
    cf.plan(tmp_path, _manifest(tmp_path, [key]), source_pins=pins)
    resource = SimpleNamespace(Object=lambda *_: pytest.fail("Retained source must not issue GET"))
    with pytest.raises(ValueError, match="raw-root"):
        cf.read(tmp_path, workers=1, resource=resource)
    assert cf.read(tmp_path, workers=1, resource=resource, raw_root=raw)["rows"] == 1
    [evidence] = list((tmp_path / "parts").glob("*/source-evidence/*.parquet"))
    [observation] = pl.read_parquet(evidence).to_dicts()
    assert (observation["custody"], observation["sha256"]) == ("central-raw-archive", bytes.fromhex(digest))


def test_frozen_key_plan_can_capture_the_version_from_a_single_get(tmp_path):
    key, body = next(iter(_store().items()))
    plan = cf.plan(tmp_path, _manifest(tmp_path, [key]), observe_source=True)
    requests = []

    def get(**options):
        requests.append(options)
        return {"ETag": '"observed"', "ContentLength": len(body), "VersionId": "v1", "Body": BytesIO(body)}

    resource = SimpleNamespace(Object=lambda bucket, key: SimpleNamespace(get=get))
    assert cf.read(tmp_path, workers=1, resource=resource)["rows"] == 1
    assert cf.read(tmp_path, workers=1, resource=resource)["chunks"] == 0
    assert requests == [{}]
    part = cf.part_path(tmp_path, plan.stem, key.split("/")[1], 0)
    checkpoint = json.loads(checkpoint_path(part).read_text())
    [observed] = pl.read_parquet(part.parent / checkpoint["evidence_file"]).to_dicts()
    assert (observed["expected_etag"], observed["etag"], observed["version_id"], observed["sha256"]) == (
        None, '"observed"', "v1", sha256(body).digest(),
    )


def test_attachment_profile_reuses_r2_original_and_refuses_all_fields(tmp_path):
    from contextlib import contextmanager

    key, body = next(iter(_store().items()))
    digest = sha256(body).hexdigest()
    pins = tmp_path / "pins.parquet"
    pl.DataFrame({
        "key": [key], "etag": ['"retained-etag"'], "size": [len(body)], "version_id": [None],
        "raw_sha256": [digest], "raw_custody": ["r2"],
    }).write_parquet(pins)
    plan = cf.plan(tmp_path, _manifest(tmp_path, [key]), source_pins=pins, profile="attachments")

    @contextmanager
    def open_original(object_key):
        assert object_key == "blobs/sha256/" + digest
        yield BytesIO(body)

    resource = SimpleNamespace(Object=lambda *_: pytest.fail("R2-held source must not issue publisher GET"))
    assert cf.read(tmp_path, workers=1, resource=resource, evidence_store=SimpleNamespace(open=open_original))["rows"] == 1
    part = cf.part_path(tmp_path, plan.stem, key.split("/")[1], 0)
    assert pl.read_parquet(part).columns == list(cf.read_columns("attachments"))
    checkpoint = json.loads(checkpoint_path(part).read_text())
    assert checkpoint["profile"] == "attachments"
    [observation] = pl.read_parquet(part.parent / checkpoint["evidence_file"]).to_dicts()
    assert (observation["custody"], observation["sha256"]) == ("r2-source-evidence", bytes.fromhex(digest))
    with pytest.raises(ValueError, match="all-fields"):
        cf.stage(tmp_path, tmp_path / "stage")
    with pytest.raises(ValueError, match="seed comment attributes"):
        cf.build_attributes(tmp_path)
    assert cf.stage(tmp_path, tmp_path / "stage", profile="attachments")["rows"] == 1


def test_staging_source_gaps_quarantines_every_copy_of_the_comment(tmp_path):
    store = _store()
    key, body = next(iter(store.items()))
    refetch = key.removesuffix(".json") + "(1)(2).json"
    cf.plan(tmp_path, _manifest(tmp_path, [*store, refetch]), chunk_keys=1, observe_source=True)

    def get(object_key):
        if object_key == refetch:
            raise ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        content = store[object_key]
        return {"ETag": '"observed"', "ContentLength": len(content), "Body": BytesIO(content)}

    resource = SimpleNamespace(Object=lambda _, object_key: SimpleNamespace(get=lambda: get(object_key)))
    assert cf.read(tmp_path, workers=1, resource=resource)["rows"] == len(store)
    assert cf.status(tmp_path)["unresolved_keys"] == 1
    with pytest.raises(ValueError, match="resolved checkpoint"):
        cf.stage(tmp_path, tmp_path / "stage", profile="attachments")
    staged = cf.stage(tmp_path, tmp_path / "stage", profile="attachments", allow_source_gaps=True)
    assert staged["rows"] == len(store) - 1
    assert staged["source_gaps"]["unresolved_keys"] == staged["source_gaps"]["withheld_comment_ids"] == 1
    assert key not in pl.read_parquet(tmp_path / "stage/reads.parquet")["key"].to_list()


@pytest.mark.parametrize("earlier", ["s2", "s3", "s4"])
def test_a_new_record_shape_reads_every_key_again_beside_an_earlier_shapes_parts(tmp_path, monkeypatch, earlier):
    """A re-read on an earlier shape's workdir (s2: 2026-09-28; s3: 2026-10-03) plans and reads every key again."""
    store, shape = _store(), cf.RECORD_SHAPE
    assert shape not in {"s2", "s3", "s4"}
    monkeypatch.setattr(cf, "RECORD_SHAPE", earlier)
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["keys"] == len(store)
    monkeypatch.setattr(cf, "RECORD_SHAPE", shape)
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["keys"] == len(store)
    assert len(list(tmp_path.glob("parts/*/*.parquet"))) == 4  # two chunks per shape
    assert len(glob(str(cf.shape_parts(tmp_path)))) == 2


def test_planning_or_reading_the_shape_needs_an_extract_that_keeps_withheld_attachments_and_ids(tmp_path, monkeypatch):
    dropped = lambda record: {"attachments_json": None}  # noqa: E731 (SpicyDocs before b19b092)
    unidentified = lambda record: {"attachments_json": '[{"title": "withheld", "formats": null}]'}  # noqa: E731 (to 0.84.0)
    with pytest.raises(RuntimeError, match="b19b092"):
        cf.require_record_shape(dropped)
    with pytest.raises(RuntimeError, match="0.85.0"):
        cf.require_record_shape(unidentified)
    def old_id_only(record):
        return {"attachments_json": json.dumps([{"attachment_id": record["included"][0]["id"]}])}

    with pytest.raises(RuntimeError, match="attachment metadata"):
        cf.require_record_shape(old_id_only)
    cf.require_record_shape()

    def refuse(extract=None):
        raise RuntimeError("refused")

    monkeypatch.setattr(cf, "require_record_shape", refuse)
    for command in (["plan", "--workdir", str(tmp_path), "--manifest", str(_manifest(tmp_path, _store()))],
                    ["read", "--workdir", str(tmp_path)]):
        with pytest.raises(RuntimeError, match="refused"):
            cf.app(command)
    assert not (tmp_path / "plan").exists()


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
            try:
                if COMMENT.extract(changed) != base:
                    reads.add(key)
            except ValueError:  # read, and refused for the probe's type (duplicateComments)
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


def test_a_record_the_extract_refuses_is_journaled_and_the_chunk_still_completes(tmp_path):
    """A malformed duplicateComments is never coerced (SpicyDocs B1): that object is refused, the rest are kept."""
    bad = json.loads(next(iter(_store().values())))
    bad["data"]["id"] = f"{PFAS}-9998"
    bad["data"]["attributes"]["duplicateComments"] = "5"
    store = {**_store(), _key(f"{PFAS}-9998"): json.dumps(bad).encode()}
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    totals = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    assert (totals["chunks"], totals["rows"], totals["deferred_chunks"]) == (2, 4, 0)
    lines = [json.loads(line) for line in (tmp_path / "journal.jsonl").read_text().splitlines()]
    refused = [u for line in lines for u in line["unresolved"] if u["status"] == "refused"]
    assert [u["key"] for u in refused] == [_key(f"{PFAS}-9998")] and "duplicateComments" in refused[0]["reason"]
    assert f"{PFAS}-9998" not in {row["comment_id"] for row in _parts(tmp_path)}


def test_the_read_stops_cleanly_past_its_memory_cap_and_resumes(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    stopped = cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store), max_rss_mb=1)
    assert stopped["chunks"] == 1 and stopped["stopped"]
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["chunks"] == 1


def test_every_table_column_a_part_holds_is_a_fill_column_typed_as_the_table():
    """Parity both ways: a table column the parts hold that the fill skips would stay NULL on rows the extract
    predated (the submitter fields did until 2026-10-03), and a fill column the parts lack cannot be read."""
    from spicy_regs.pipelines.comment_fields_write import FILL_COLUMNS, KEY_COLUMNS

    assert set(FILL_COLUMNS) == (set(COMMENT.schema) & set(cf.PART_SCHEMA)) - set(KEY_COLUMNS)
    assert {"first_name", "last_name", "organization", "category", "subtype", "attachments_json"} <= set(FILL_COLUMNS)
    assert cf.PART_SCHEMA["duplicate_comments"] == COMMENT.schema["duplicate_comments"] == pl.Int32

def test_shards_split_chunks_so_every_chunk_is_read_exactly_once(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=1)
    read = [cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store), shard=(i, 3))["keys"] for i in range(3)]
    assert sum(read) == len(store) and len(_parts(tmp_path)) == len(store)
    assert cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))["chunks"] == 0


def test_the_fill_workflow_is_manual_holds_the_writers_lock_and_always_restores_compaction():
    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/fill-comment-fields.yml").read_text())
    assert list(workflow[True]) == ["workflow_dispatch"]  # PyYAML reads the key `on` as True: no schedule, no push
    assert workflow["concurrency"] == {"group": "comments-catalog-write", "cancel-in-progress": False, "queue": "max"}
    assert workflow["env"]["SPICY_REGS_CATALOG_LOCK"] == "comments-catalog-write"
    steps = workflow["jobs"]["fill"]["steps"]
    names = [step["name"] for step in steps]
    order = [next(i for i, n in enumerate(names) if n.startswith(prefix)) for prefix in (
        "Download and verify", "Earlier runs' journals", "Disable R2 compaction", "Prepare", "Write", "Keep the journals",
        "Keep the journal, receipts", "Re-enable R2 compaction")]
    assert order == sorted(order)
    for step in steps[order[-3]:]:
        assert step["if"].startswith("always()"), step["name"]
    assert "--sha256" in steps[order[0]]["run"] and "fetch" in steps[order[0]]["run"]
    from spicy_regs.pipelines.comment_fields_write import PROFILES

    profile = workflow[True]["workflow_dispatch"]["inputs"]["profile"]
    assert (profile["default"], profile["options"]) == ("all", list(PROFILES))
    assert '--profile "$PROFILE"' in steps[order[3]]["run"] and steps[order[3]]["env"]["PROFILE"] == "${{ inputs.profile }}"


def test_the_attributes_seed_projects_every_copy_and_keeps_the_newest(tmp_path):
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    counts = cf.build_attributes(tmp_path)
    table = pl.read_parquet(tmp_path / "comment_attributes.parquet")
    assert (counts["rows"], counts["refused"]) == (4, 0)
    from spicy_docs.schemas import TABLE_CONTRACTS

    assert table.columns == list(TABLE_CONTRACTS["comment_attributes"].columns)
    campaign = table.filter(pl.col("comment_id") == f"{PFAS}-1811").to_dicts()[0]
    stated = json.loads(store[_key(f"{PFAS}-1811")])["data"]["attributes"]
    assert (campaign["tracking_nbr"], campaign["page_count"], campaign["withdrawn"]) == (
        stated["trackingNbr"], stated["pageCount"], stated["withdrawn"])
    assert "email" not in table.columns and "phone" not in table.columns


def _exclusion(monkeypatch, payload):
    import hashlib

    from spicy_regs.transforms import reviewed_comments

    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    real_init = reviewed_comments.ExcludeReviewedComments.__init__

    def init(self, *, keyed=False):
        real_init(self, keyed=keyed)
        self.decisions = {payload["data"]["id"]: {"canonical_sha256": digest, "reason": "reviewed"}}

    monkeypatch.setattr(reviewed_comments.ExcludeReviewedComments, "__init__", init)


def test_the_seed_excludes_a_reviewed_comment_only_as_reviewed(tmp_path, monkeypatch):
    """The ETL's check: the reviewed bytes are excluded; a changed reviewed comment refuses the seed."""
    store = _store()
    cf.plan(tmp_path, _manifest(tmp_path, store), chunk_keys=10)
    cf.read(tmp_path, workers=2, resource=_FakeS3Resource(store))
    reviewed = json.loads(store[_key(f"{PFAS}-0002")])
    _exclusion(monkeypatch, reviewed)
    fetched = []
    fetch = lambda key: fetched.append(key) or json.loads(store[key])  # noqa: E731
    counts = cf.build_attributes(tmp_path, fetch=fetch)
    assert counts["rows"] == 3 and fetched == [_key(f"{PFAS}-0002")]
    changed = {**reviewed, "data": {**reviewed["data"], "attributes": {**reviewed["data"]["attributes"], "title": "x"}}}
    _exclusion(monkeypatch, changed)
    with pytest.raises(ValueError, match="changed"):
        cf.build_attributes(tmp_path, fetch=fetch)
