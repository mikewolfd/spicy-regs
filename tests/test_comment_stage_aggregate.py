"""An aggregate accepts only complete, identity-disjoint groups from the frozen source plan."""

import json
from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace

import polars as pl
import pytest
from botocore.exceptions import ClientError
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines import comment_fields_groups, comment_source_groups, comment_stage_aggregate
from spicy_regs.pipelines.comment_fields_evidence import file_sha256
from tests.test_comment_fields import _store


def _json(path, value):
    path.write_text(json.dumps(value))


@pytest.fixture
def completed(tmp_path):
    source = _store()
    local, kept, *_ = sorted(source)
    missing = local.replace(".json", "(1)(2).json")
    digest = sha256(source[local]).hexdigest()
    raw = tmp_path / "raw"
    CentralRawArchiveStore(raw).put_blob(f"sha256:{digest}", len(source[local]), [source[local]])
    manifest, pins = tmp_path / "manifest.parquet", tmp_path / "pins.parquet"
    pl.DataFrame({"key": [local, missing, kept]}).write_parquet(manifest)
    pl.DataFrame({
        "key": [local], "raw_custody": ["local"], "etag": ['"pin"'], "size": [len(source[local])],
        "version_id": ["v1"], "raw_sha256": [digest],
    }).write_parquet(pins)
    master = tmp_path / "groups" / "groups.json"
    groups = comment_source_groups.split(manifest, pins, master.parent)["groups"]
    master_sha256 = file_sha256(master)
    reads = tmp_path / "reads"

    def get(key):
        assert key != local, "The locally held original must never fetch"
        if key == missing:
            raise ClientError({"ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        body = source[key]
        return {"ETag": '"observed"', "ContentLength": len(body), "Body": BytesIO(body)}

    resource = SimpleNamespace(Object=lambda _, key: SimpleNamespace(get=lambda **__: get(key)))
    for group in groups:
        name = group["group"]
        inputs, work = master.parent / name, reads / name
        cf.plan(work, inputs / "manifest.parquet", chunk_keys=1,
                source_pins=inputs / "source-pins.parquet" if group["source_pins"] else None,
                observe_source=True, profile="attachments")
        cf.read(work, raw_root=raw, resource=resource, workers=1, min_free_gb=0)
        read = cf.read(work, raw_root=raw, resource=resource, workers=1, min_free_gb=0)
        assert read["keys"] == 0  # The last invocation need not have read any new keys.
        stage = cf.stage(work, work / "stage", profile="attachments", allow_source_gaps=True)
        if name.startswith("runner-"):
            result = {"group": name, "groups_sha256": master_sha256, "group_descriptor": group,
                      "stage_file": stage["file"], "status": "complete", "reader_sha256": cf._reader_sha256()}
        else:
            result = {"group": name, "master_receipt_sha256": master_sha256, "inputs": group, "stage": stage["file"]}
        _json(work / "result.json", result | {"read": read, "source_gaps": stage.get("source_gaps")})
    return master, master_sha256, reads, kept


def test_aggregate_checks_quarantine_and_feeds_native_group_planner(completed, tmp_path):
    master, master_sha256, reads, kept = completed
    result = comment_stage_aggregate.aggregate(master, master_sha256, reads, tmp_path / "merged")
    path, checked = comment_fields_groups._input(tmp_path / "merged")
    assert result == checked
    assert [row["key"] for row in pl.read_parquet(path).to_dicts()] == [kept]
    assert result["distinct_keys"] == result["distinct_comment_ids"] == 1
    assert result["source_gaps"] == {
        "unresolved_keys": 1, "quarantined_keys": 2, "withheld_comment_ids": 1, "statuses": {"source-missing": 1},
    }
    assert len(result["group_stages"]) == 2


@pytest.mark.parametrize("alteration,message", [
    ("missing", "pinned read plan receipt"),
    ("profile", "frozen group"),
    ("reader", "frozen group"),
    ("stage", "differs from its pin"),
    ("transient", "transient"),
    ("cross-group", "routed to another group"),
])
def test_aggregate_refuses_changed_incomplete_or_overlapping_inputs(completed, tmp_path, alteration, message):
    master, master_sha256, reads, _ = completed
    local = next(reads.glob("local-*"))
    runner = next(reads.glob("runner-*"))
    stage = runner / "stage" / "staging.json"
    if alteration == "missing":
        runner.rename(reads / "absent")
    if alteration == "profile":
        _json(stage, json.loads(stage.read_text()) | {"profile": "all"})
    if alteration == "reader":
        [plan] = runner.glob("plan/pinned-*.json")
        _json(plan, json.loads(plan.read_text()) | {"reader_sha256": "wrong"})
    if alteration == "stage":
        (runner / "stage" / "reads.parquet").write_bytes(b"changed")
    if alteration == "transient":
        path = local / "stage" / "source-gaps.parquet"
        pl.read_parquet(path).with_columns(pl.lit("access-refused").alias("status")).write_parquet(path)
        stage = local / "stage" / "staging.json"
        receipt = json.loads(stage.read_text())
        receipt["source_gaps"]["sha256"] = file_sha256(path)
        _json(stage, receipt)
        result = local / "result.json"
        _json(result, json.loads(result.read_text()) | {"source_gaps": receipt["source_gaps"]})
    if alteration == "cross-group":
        group_file = master.parent / runner.name / "manifest.parquet"
        duplicate = pl.read_parquet(master.parent / local.name / "manifest.parquet")["key"][0]
        pl.DataFrame({"key": [duplicate]}).write_parquet(group_file)
        receipt = json.loads(master.read_text())
        group = next(group for group in receipt["groups"] if group["group"] == runner.name)
        group["manifest"].update({"bytes": group_file.stat().st_size, "sha256": file_sha256(group_file)})
        _json(master, receipt)
        master_sha256 = file_sha256(master)
    out = tmp_path / "merged"
    with pytest.raises(ValueError, match=message):
        comment_stage_aggregate.aggregate(master, master_sha256, reads, out)
    assert not out.exists()
