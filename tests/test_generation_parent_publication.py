"""Managed parents stay current through admission, uploads and pointer races."""

from copy import deepcopy

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import canonical_json_bytes

from spicy_regs.generations import build_generation
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_generation_publication import build, publish


def candidate(tmp_path, snapshot):
    source = tmp_path / "derived.parquet"
    pq.write_table(pa.table({"id": ["derived"]}), source, store_schema=False)
    directory = tmp_path / "derived-generation"
    artifact = build_generation(
        directory, family="derived", files=[source], expected_keys=[source.name], read_snapshot=snapshot,
        parents={"base.parquet": pub.table_pin(snapshot, "base.parquet")},
    )
    return directory, artifact


@pytest.mark.parametrize("window", ["before-publication", "during-upload", "pointer-retry"])
@pytest.mark.parametrize("split", [False, True])
def test_changed_managed_parent_never_publishes_stale_analysis(tmp_path, window, split):
    store = Store()
    if split:
        source = tmp_path / "base" / "congress=119"
        source.mkdir(parents=True)
        pq.write_table(pa.table({"id": ["base"], "congress": [119]}), source / "part-000000.parquet", store_schema=False)
        base = tmp_path / "base-generation"
        build_generation(base, family="base", files=[source.parent], expected_keys=["base.parquet"],
                         partitioned={"base.parquet": ["congress"]})
    else:
        base, _ = build(tmp_path, "base", family="base", keys=("base.parquet",))
    snapshot = publish(store, base)
    directory, _ = candidate(tmp_path, snapshot)
    rival = deepcopy(snapshot)
    # Even byte-identical table members belong to the captured parent generation.
    rival["families"]["base"].update(artifactDigest="sha256:" + "f" * 64,
                                    prefix="generations/base/" + "f" * 64)
    rival_bytes = canonical_json_bytes(rival)
    attempts = []

    def change_parent(key):
        if key == pub.INDEX_V2_KEY:
            attempts.append(key)
        trigger = key == pub.INDEX_V2_KEY if window == "pointer-retry" else key.endswith("derived.parquet")
        if trigger:
            store.before_put = None
            store.objects[pub.INDEX_V2_KEY] = rival_bytes

    if window == "before-publication":
        store.objects[pub.INDEX_V2_KEY] = rival_bytes
    else:
        store.before_put = change_parent
    writes_before = list(store.writes)
    with pytest.raises(pub.PublicationError, match="Managed parent changed"):
        publish(store, directory, snapshot)
    assert store.objects[pub.INDEX_V2_KEY] == rival_bytes
    assert "derived" not in pub.parse_index(store.objects[pub.INDEX_V2_KEY])["families"]
    assert attempts == ([pub.INDEX_V2_KEY] if window == "pointer-retry" else [])
    if window == "before-publication":
        assert store.writes == writes_before


def test_unrelated_family_race_preserves_unchanged_managed_parents(tmp_path):
    base, _ = build(tmp_path, "base", family="base", keys=("base.parquet",))
    store = Store()
    snapshot = publish(store, base)
    directory, artifact = candidate(tmp_path, snapshot)
    other, _ = build(tmp_path, "other", family="other", keys=("other.parquet",))
    sibling = publish(Store(), other)["families"]["other"]

    def race(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            current = pub.parse_index(store.objects[key])
            current["families"]["other"] = sibling
            store.objects[key] = canonical_json_bytes(current)

    store.before_put = race
    current = publish(store, directory, snapshot)
    assert current["families"]["derived"]["artifactDigest"] == artifact.pin.artifact_digest
    assert current["families"]["other"] == sibling
    assert pub.table_pin(current, "base.parquet") == pub.table_pin(snapshot, "base.parquet")


def test_missing_managed_parent_refuses_before_upload(tmp_path):
    base, _ = build(tmp_path, "base", family="base", keys=("base.parquet",))
    store = Store()
    snapshot = publish(store, base)
    directory, _ = candidate(tmp_path, snapshot)
    store.objects[pub.INDEX_V2_KEY] = canonical_json_bytes(pub.empty_index())
    before = list(store.writes)
    with pytest.raises(pub.PublicationError, match="Managed parent changed"):
        publish(store, directory, snapshot)
    assert store.writes == before
