"""Decision 36: keep each family's last three generations and everything cited, and plan the rest for deletion."""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import generation_retention as retention
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import publication as pub, r2

LATER = datetime.now(timezone.utc) + timedelta(days=2)


def _table(path: Path) -> Path:
    pq.write_table(pa.table({"id": [path.parent.name]}), path, store_schema=False)
    return path


class _Base(RollupPipeline):
    name: ClassVar[str] = "base"
    output: ClassVar[str] = "base.parquet"

    def build(self, output_dir: Path) -> Path:
        return _table(output_dir / self.output)


class _Derived(RollupPipeline):
    name: ClassVar[str] = "derived"
    inputs: ClassVar[tuple[str, ...]] = ("base.parquet",)
    output: ClassVar[str] = "derived.parquet"

    def build(self, output_dir: Path) -> Path:
        return _table(output_dir / self.output)


def _publish(pipeline: type[RollupPipeline], tmp_path: Path, store, runs: int) -> list[str]:
    """Run ``pipeline`` ``runs`` times and return each generation's digest, oldest first."""
    digests = []
    for run in range(runs):
        pipeline(output_dir=tmp_path / f"{pipeline.name}-{run}", skip_upload=False).run()
        entry = pub.parse_index(store.objects[pub.INDEX_KEY])["families"][pipeline.name]
        digests.append(entry["artifactDigest"].removeprefix("sha256:"))
    return digests


def _plan(store, *, notes=(), docspec=None, now=LATER) -> dict:
    return retention.plan(store, "spicy-regs", notes=notes, docspec=docspec or {}, now=now)


def _kept(record: dict, family: str) -> dict[str, list[str]]:
    return {g["digest"]: g["keep"] for g in record["families"][family]["generations"] if g["keep"]}


def test_a_family_keeps_its_current_and_two_predecessors_along_the_prior_chain(tmp_path, remote):
    digests = _publish(_Base, tmp_path, remote, 5)
    record = _plan(remote)
    assert set(_kept(record, "base")) == set(digests[2:])
    assert record["delete"] == sorted(f"generations/base/{d}" for d in digests[:2])
    assert record["totals"]["delete_bytes"] == sum(
        len(raw) for key, raw in remote.objects.items() if key.startswith(tuple(record["delete"])))


def test_a_note_or_docspec_pin_keeps_an_older_generation(tmp_path, remote):
    digests = _publish(_Base, tmp_path, remote, 5)
    notes = [("docs/research/ledger.md", f"qualified at `{digests[0][:8]}…` (2026-09-26)")]
    record = _plan(remote, notes=notes, docspec={digests[1]: "DocSpec evidence pin (a receipt)"})
    kept = _kept(record, "base")
    assert kept[digests[0]] == ["named in docs/research/ledger.md"]
    assert kept[digests[1]] == ["DocSpec evidence pin (a receipt)"]
    assert record["delete"] == []


def test_a_kept_generation_keeps_the_parent_it_read(tmp_path, monkeypatch, remote):
    def download(remote_key, local_path):
        location = pub.single_member(pub.parse_index(remote.objects[pub.INDEX_KEY]), remote_key).path
        local_path.write_bytes(remote.objects[location])
        return True

    monkeypatch.setattr(r2, "download", download)
    read = _publish(_Base, tmp_path, remote, 1)[0]
    _publish(_Derived, tmp_path, remote, 1)
    newer = _publish(_Base, tmp_path / "later", remote, 4)
    record = _plan(remote)
    assert _kept(record, "base")[read] == [f"parent of derived/{record['families']['derived']['current'][7:15]} "
                                           "(base.parquet)"]
    assert record["delete"] == [f"generations/base/{newer[0]}"]


def test_a_generation_stays_for_the_grace_after_it_stops_being_current(tmp_path, remote):
    digests = _publish(_Base, tmp_path, remote, 5)
    within = _plan(remote, now=datetime.now(timezone.utc) + retention.GRACE - timedelta(minutes=5))
    assert within["delete"] == []
    assert [reason[:13] for reason in _kept(within, "base")[digests[0]]] == ["current until"]


def test_an_upload_off_the_chain_is_kept_only_while_newer_than_current(tmp_path, remote):
    """A publish that has not swapped yet may be in flight; one older than current lost its swap."""
    first = _publish(_Base, tmp_path / "first", remote, 1)[0]
    pointers = {key: remote.objects[key] for key in (pub.INDEX_V2_KEY, pub.INDEX_KEY)}
    pending = _publish(_Base, tmp_path / "pending", remote, 1)[0]
    remote.objects.update(pointers)  # as if its swap had not happened yet
    assert _kept(_plan(remote), "base")[pending] == ["written after the current generation: a publish may be in flight"]

    current = _publish(_Base, tmp_path / "after", remote, 1)[0]
    record = _plan(remote)
    assert set(_kept(record, "base")) == {first, current}
    assert record["delete"] == [f"generations/base/{pending}"]


def test_docspec_pins_refuse_a_document_outside_the_stated_shape():
    pin = {"family": "documents", "artifactDigest": "sha256:" + "a" * 64, "role": "evidence", "cites": "r.json"}
    document = {"format": retention.DOCSPEC_PINS_FORMAT, "version": 1, "pins": [pin]}
    assert retention.docspec_pins(json.dumps(document).encode()) == {"a" * 64: "DocSpec evidence pin (r.json)"}
    for broken in ({**document, "version": 2}, {**document, "pins": [{**pin, "artifactDigest": "aaaa"}]}, {}):
        with pytest.raises(pub.PublicationError, match="DocSpec pins"):
            retention.docspec_pins(json.dumps(broken).encode())


def test_execution_deletes_only_what_a_fresh_plan_agrees_on_and_keeps_each_root(tmp_path, remote):
    digests = _publish(_Base, tmp_path, remote, 5)
    approved = _plan(remote)
    cited = [("docs/research/ledger.md", f"`{digests[1][:8]}…`")]  # cited after the review
    record = retention.execute(remote, "spicy-regs", approved, _plan(remote, notes=cited))

    assert record["deleted"] == [f"generations/base/{digests[0]}"]
    assert record["spared"] == [f"generations/base/{digests[1]}"]
    left = [key for key in remote.objects if key.startswith(record["deleted"][0])]
    assert left == [f"{record['deleted'][0]}/artifact.json"], "the root stays as the chain's link"
    assert any(key.startswith(record["deleted"][0]) for key in remote.deletes)
    assert any(key.startswith(record["spared"][0] + "/base.parquet") for key in remote.objects)
    stored = json.loads(remote.objects[f"retention/{LATER.isoformat()}.json"])
    assert stored["deleted"] == record["deleted"]

    after = _plan(remote, notes=cited)
    assert after["delete"] == []
    assert after["totals"]["pruned_generations"] == 1
    assert [g["pruned"] for g in after["families"]["base"]["generations"] if g["digest"] == digests[0]] == [True]


def test_execution_derives_version_1_again_before_deleting_a_generation_it_names(tmp_path, remote):
    """Publishes that stopped between their two writes left version 1 naming a generation the plan deletes."""
    digests = _publish(_Base, tmp_path, remote, 2)
    stale = remote.objects[pub.INDEX_KEY]
    digests += _publish(_Base, tmp_path / "later", remote, 3)
    remote.objects[pub.INDEX_KEY] = stale
    assert pub.parse_index(stale)["families"]["base"]["artifactDigest"] == f"sha256:{digests[1]}"
    order = []
    remote.before_put = lambda key: order.append((key, len(remote.deletes)))

    plan = _plan(remote)
    record = retention.execute(remote, "spicy-regs", plan, plan)

    assert f"generations/base/{digests[1]}" in record["deleted"]
    assert order[0] == (pub.INDEX_KEY, 0), "version 1 is written before anything is deleted"
    assert pub.parse_index(remote.objects[pub.INDEX_KEY]) == pub.derive_v1(pub.parse_index(remote.objects[pub.INDEX_V2_KEY]))


def test_execution_refuses_a_plan_for_another_bucket(remote):
    with pytest.raises(pub.PublicationError, match="approved plan"):
        retention.execute(remote, "spicy-regs", {"format": retention.PLAN_FORMAT, "version": 1, "bucket": "other"},
                          {})


def test_a_generation_holding_a_rulemaking_snapshot_input_is_kept(tmp_path, remote):
    """The snapshot names its inputs by table sha256, not generation; any generation holding those bytes stays."""
    digests = _publish(_Base, tmp_path, remote, 5)
    manifest = "materialized/rulemaking/snapshots/snapshot_abc/manifest.json"
    table = hashlib.sha256(remote.objects[f"generations/base/{digests[0]}/base.parquet"]).hexdigest()
    remote.objects[manifest] = json.dumps({"inputs": {"sources": {"base.parquet": {"sha256": table}}}}).encode()
    remote.objects[pub.SNAPSHOT_POINTER] = json.dumps(
        {"snapshot_id": "snapshot_abc", "manifest_key": manifest}).encode()
    record = _plan(remote)
    assert _kept(record, "base")[digests[0]] == ["input of rulemaking snapshot_abc (base.parquet)"]
    assert record["delete"] == [f"generations/base/{digests[1]}"]
