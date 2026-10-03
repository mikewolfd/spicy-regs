"""Actual Parquet families, interrupted writes and immutable reader snapshots."""

import json
from contextlib import contextmanager
from datetime import datetime, timezone

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from loguru import logger
from rulespec_artifacts import ArtifactVerificationError, canonical_json_bytes

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.sources import publication as pub, r2
from tests.generation_fakes import Store, error


def build(tmp_path, name="one", family="test", keys=("a.parquet", "b.parquet"), value="one"):
    source = tmp_path / (name + "-source")
    source.mkdir()
    files = []
    for key in keys:
        path = source / key
        pq.write_table(pa.table({"id": [value]}), path, store_schema=False)
        files.append(path)
    target = tmp_path / name
    artifact = build_generation(target, family=family, files=files, expected_keys=keys)
    return target, artifact


def publish(store, path, prior=None):
    return pub.publish_generation(path, client=store, bucket="test", prior_index=prior or pub.empty_index())


def test_complete_generation_then_pointer_and_exact_readback(tmp_path):
    directory, artifact = build(tmp_path)
    store = Store()
    index = publish(store, directory)
    assert store.writes[-2:] == [pub.INDEX_V2_KEY, pub.INDEX_KEY]
    assert index["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    for key in ("a.parquet", "b.parquet"):
        location, info = pub.single_member(index, key).path, pub.table_descriptor(index, key)
        assert info is not None
        assert store.objects[location] == (directory / key).read_bytes()
        assert info["rows"] == 1
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == index
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(index)


@pytest.mark.parametrize("failure", ["a.parquet", "b.parquet", "artifact.json", "members.json", pub.INDEX_V2_KEY])
def test_interruption_never_changes_previous_complete_generation(tmp_path, failure):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    old_pointers = {key: store.objects[key] for key in (pub.INDEX_V2_KEY, pub.INDEX_KEY)}
    old_members = {key: store.objects[pub.single_member(prior, key).path] for key in ("a.parquet", "b.parquet")}

    def interrupt(key):
        if key.endswith(failure):
            raise OSError("interrupted transfer")

    store.before_put = interrupt
    with pytest.raises(OSError, match="interrupted"):
        publish(store, new, prior)
    assert {key: store.objects[key] for key in old_pointers} == old_pointers
    for key, raw in old_members.items():
        assert store.objects[pub.single_member(prior, key).path] == raw


def test_corrupt_uploaded_bytes_never_become_published(tmp_path):
    directory, _ = build(tmp_path)
    store = Store()
    store.corrupt_key = "b.parquet"
    with pytest.raises(ArtifactVerificationError, match="digest"):
        publish(store, directory)
    assert pub.INDEX_KEY not in store.objects


def test_late_shrink_refusal_uploads_nothing(tmp_path):
    directory, _ = build(tmp_path)
    store = Store()
    store.objects["b.parquet"] = b"x" * 100_000
    with pytest.raises(RuntimeError, match="shrink"):
        publish(store, directory)
    assert not store.writes


def test_persistent_contention_refuses_and_preserves_concurrent_writer(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    rivals = []

    def concurrent(key):
        if key == pub.INDEX_V2_KEY:  # semantically same, a new object version on every attempt
            rivals.append(json.dumps(prior, indent=len(rivals)).encode())
            store.objects[key] = rivals[-1]

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="concurrently"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_V2_KEY] == rivals[-1]
    assert len(rivals) == pub._POINTER_ATTEMPTS


def test_concurrent_sibling_family_is_merged_not_overwritten(tmp_path):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    rival = publish(Store(), sibling)["families"]["other"]

    def concurrent(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            index = pub.parse_index(store.objects[key])
            index["families"]["other"] = rival
            store.objects[key] = canonical_json_bytes(index)

    store.before_put = concurrent
    published = publish(store, new, prior)
    assert published["families"]["other"] == rival
    assert published["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == published
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(published)


def test_conditional_request_conflict_is_retried_like_a_refused_pointer_write(tmp_path):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)

    def conflict(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            raise error("ConditionalRequestConflict")

    store.before_put = conflict
    assert publish(store, new, prior)["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest


def test_concurrent_same_family_commit_refuses_as_stale(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    third, _ = build(tmp_path, "three", value="three")
    store = Store()
    prior = publish(store, old)
    other = Store()
    other.objects = dict(store.objects)
    publish(other, third, prior)
    rival = other.objects[pub.INDEX_V2_KEY]

    def concurrent(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            store.objects[key] = rival

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_V2_KEY] == rival


def test_stale_family_build_and_cross_family_takeover_refuse(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    other, _ = build(tmp_path, "other", family="other")
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior)
    before = list(store.writes)
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, old, prior)
    with pytest.raises(pub.PublicationError, match="already belongs"):
        publish(store, other)
    assert store.writes == before


def test_independent_family_update_is_preserved(tmp_path):
    one, _ = build(tmp_path)
    two, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    first = publish(store, one)
    second = publish(store, two)
    assert second["families"]["test"] == first["families"]["test"]
    assert set(second["families"]) == {"test", "other"}


def test_missing_extra_changed_and_invalid_outputs_refuse(tmp_path):
    directory, _ = build(tmp_path)
    files = list(directory.glob("*.parquet"))
    with pytest.raises(ValueError, match="declared complete"):
        build_generation(tmp_path / "missing", family="test", files=files[:1], expected_keys=[p.name for p in files])
    with pytest.raises(ValueError, match="declared schema"):
        build_generation(
            tmp_path / "bad-schema",
            family="test",
            files=files,
            expected_keys=[p.name for p in files],
            schemas={"a": [("wrong", "VARCHAR")]},
        )
    (directory / "extra.parquet").write_bytes(files[0].read_bytes())
    with pytest.raises(ArtifactVerificationError, match="undeclared"):
        verify_generation(directory)


def test_successful_empty_table_is_a_member(tmp_path):
    path = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"id": pa.array([], type=pa.string())}), path)
    artifact = build_generation(tmp_path / "empty", family="test", files=[path], expected_keys=[path.name])
    assert artifact.root["spec"]["tables"][path.name]["rows"] == 0


def test_multipart_completion_is_conditional_and_replayable(tmp_path, monkeypatch):
    """Replaying an already-published generation returns the prior index and uploads nothing."""
    monkeypatch.setattr(pub, "PART_BYTES", 100)
    directory, _ = build(tmp_path)
    store = Store()
    prior = publish(store, directory)
    assert not store.uploads
    assert publish(store, directory, prior) == prior
    assert not store.uploads


def test_snapshot_keeps_all_downloads_on_one_generation(tmp_path, monkeypatch):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    calls = []

    @contextmanager
    def stream(method, url, **kwargs):
        key = url.removeprefix("https://test/")
        calls.append(key)
        if key not in store.objects:
            yield httpx.Response(404, request=httpx.Request(method, url))
            return
        if key == pub.INDEX_V2_KEY:
            raw = store.objects[key]
            publish(store, new, prior)
        else:
            raw = store.objects[key]
        yield httpx.Response(200, content=raw, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    with pub.snapshot("https://test"):
        for key in ("a.parquet", "b.parquet"):
            target = tmp_path / key
            assert r2.download(key, target)
            assert pq.read_table(target).to_pylist() == [{"id": "one"}]
    assert calls.count(pub.INDEX_V2_KEY) == 1


def test_managed_download_refuses_corruption_without_replacing_prior(tmp_path, monkeypatch):
    directory, _ = build(tmp_path)
    store = Store()
    index = publish(store, directory)
    monkeypatch.setattr(pub, "load_index", lambda url: index)

    @contextmanager
    def stream(method, url, **kwargs):
        yield httpx.Response(200, content=b"corrupt", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx, "stream", stream)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://test")
    target = tmp_path / "held.parquet"
    target.write_bytes(b"prior")
    with pytest.raises(RuntimeError, match="differs from its pin"):
        r2.download("a.parquet", target)
    assert target.read_bytes() == b"prior"


@pytest.mark.parametrize("raw", [b"{}", b'{"format":1,"format":2}', b"null", b"[]", b"x" * (pub.INDEX_LIMIT + 1)])
def test_invalid_index_refuses(raw):
    with pytest.raises(pub.PublicationError):
        pub.parse_index(raw)


def test_readable_footer_with_corrupt_body_is_refused(tmp_path):
    path = tmp_path / "a.parquet"
    pq.write_table(pa.table({"id": ["original", "data", "values"]}), path)
    raw = bytearray(path.read_bytes())
    footer_bytes = int.from_bytes(raw[-8:-4], "little")
    raw[4 : len(raw) - 8 - footer_bytes] = b"\0" * (len(raw) - 12 - footer_bytes)
    path.write_bytes(raw)
    assert pq.read_metadata(path).num_rows == 3
    with pytest.raises((OSError, pa.ArrowInvalid)):
        build_generation(tmp_path / "bad", family="test", files=[path], expected_keys=[path.name])


def test_rollup_ignores_stale_cached_prior_and_keeps_it_as_evidence(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    from spicy_regs.transforms.table_merge import merge_table
    from spicy_regs.sources import cloudflare

    class Ingest(RollupPipeline):
        name = "test"
        output = "a.parquet"

        def build(self, output_dir):
            # This custom path reproduces the cache idiom used by ingest
            # builders outside the shared published_table helper.
            prior = output_dir / "_a_prior.parquet"
            assert prior.exists() or r2.download(self.output, prior)
            return merge_table(
                output_dir,
                name="a",
                columns=("id",),
                identity=("id",),
                version_column=None,
                rows=[],
                remote_key=self.output,
            )

    old, _ = build(tmp_path, "current", keys=("a.parquet",), value="B")
    store = Store()
    index = publish(store, old)
    retained = tmp_path / "run"
    retained.mkdir()
    stale = retained / "_a_prior.parquet"
    pq.write_table(pa.table({"id": ["A"]}), stale)
    stale_bytes = stale.read_bytes()
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(pub, "load_index", lambda url: index)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)

    def download(key, path):
        location = pub.single_member(pub.current_index("https://example.test"), key).path
        path.write_bytes(store.objects[location])
        return True

    monkeypatch.setattr(r2, "download", download)
    Ingest(output_dir=retained, skip_upload=False).run()
    current = pub.parse_index(store.objects[pub.INDEX_KEY])
    location = pub.single_member(current, "a.parquet").path
    from io import BytesIO

    assert pq.read_table(BytesIO(store.objects[location])).to_pylist() == [{"id": "B"}]
    assert stale.read_bytes() == stale_bytes


def test_unchanged_members_are_copied_not_reuploaded(tmp_path):
    old, _ = build(tmp_path, value="one")
    # A partial-writer-shaped new generation: a changes, b is byte-identical,
    # so the new prefix differs but b's bytes already exist under the old one.
    source = tmp_path / "two-source"
    source.mkdir()
    pq.write_table(pa.table({"id": ["two"]}), source / "a.parquet", store_schema=False)
    pq.write_table(pa.table({"id": ["one"]}), source / "b.parquet", store_schema=False)
    new = tmp_path / "two"
    build_generation(
        new, family="test", files=[source / "a.parquet", source / "b.parquet"], expected_keys=("a.parquet", "b.parquet")
    )
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior=prior)
    index = pub.parse_index(store.objects[pub.INDEX_KEY])
    b_loc, b_info = pub.single_member(index, "b.parquet").path, pub.table_descriptor(index, "b.parquet")
    assert b_info is not None
    assert b_info["sha256"] == prior["families"]["test"]["tables"]["b.parquet"]["sha256"]
    assert store.objects[b_loc] == (new / "b.parquet").read_bytes()
    # b was copied server-side from the prior prefix, not uploaded as bytes;
    # a was uploaded under the new prefix.
    assert store.copies == [b_loc]
    a_loc = pub.single_member(index, "a.parquet").path
    assert a_loc in store.writes and a_loc not in store.copies


def test_a_changed_member_still_uploads_bytes(tmp_path):
    old, _ = build(tmp_path, value="one")
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior=prior)
    index = pub.parse_index(store.objects[pub.INDEX_KEY])
    for key in ("a.parquet", "b.parquet"):
        assert store.objects[pub.single_member(index, key).path] == (new / key).read_bytes()
    assert store.copies == []


def test_partial_writer_carries_exact_siblings_and_refuses_cold_start(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.base import RollupPipeline
    from spicy_regs.sources import cloudflare

    class Partial(RollupPipeline):
        name = "partial"
        publication_family = "test"
        output = "a.parquet"

        def build(self, output_dir):
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["changed"]}), path)
            return path

    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    store = Store()
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr(pub, "load_index", lambda url: pub.empty_index())
    monkeypatch.setattr(cloudflare, "purge_urls", lambda urls: None)
    with pytest.raises(pub.PublicationError, match="complete test"):
        Partial(output_dir=tmp_path / "cold", skip_upload=False).run()
    assert not store.writes
    Partial(output_dir=tmp_path / "candidate", skip_upload=True).run()
    candidate = next((tmp_path / "candidate" / "generations").iterdir())
    assert verify_generation(candidate).root["spec"]["publicationStatus"] == "local-partial"
    with pytest.raises(pub.PublicationError, match="partial candidate"):
        publish(store, candidate)
    assert not store.writes and not store.objects
    old, _ = build(tmp_path)
    index = publish(store, old)
    monkeypatch.setattr(pub, "load_index", lambda url: index)

    def download(key, path):
        path.write_bytes(store.objects[pub.single_member(pub.current_index("https://example.test"), key).path])
        return True

    monkeypatch.setattr(r2, "download", download)
    Partial(output_dir=tmp_path / "warm", skip_upload=False).run()
    current = pub.parse_index(store.objects[pub.INDEX_KEY])
    assert set(current["families"]) == {"test"}
    assert store.objects[pub.single_member(current, "b.parquet").path] == (old / "b.parquet").read_bytes()
    generation = next((tmp_path / "warm" / "generations").iterdir())
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["carriedForward"] == {"b.parquet": index["families"]["test"]["artifactDigest"]}


def test_partial_writer_carries_a_split_sibling_member_for_member(tmp_path, monkeypatch):
    """A family with a split table (the bill family's ``bill_sections``) takes a partial writer too."""
    from spicy_regs.pipelines.rollups.base import RollupPipeline

    class Partial(RollupPipeline):
        name = "partial"
        publication_family = "test"
        output = "a.parquet"

        def build(self, output_dir):
            path = output_dir / self.output
            pq.write_table(pa.table({"id": ["changed"]}), path)
            return path

    source = tmp_path / "source"
    (source / "s" / "congress=118").mkdir(parents=True)
    (source / "s" / "congress=119").mkdir(parents=True)
    pq.write_table(pa.table({"id": ["one"]}), source / "a.parquet")
    for congress in ("118", "119"):
        pq.write_table(pa.table({"id": [congress], "congress": [congress]}),
                       source / "s" / f"congress={congress}" / "part-000000.parquet")
    old = tmp_path / "old"
    build_generation(old, family="test", files=[source / "a.parquet", source / "s"],
                     expected_keys=("a.parquet", "s.parquet"), partitioned={"s.parquet": ("congress",)})
    store = Store()
    index = publish(store, old)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://example.test")
    monkeypatch.setattr(pub, "load_index", lambda url: index)

    def download_members(key, directory):
        paths = []
        for member in pub.table_members(pub.current_index("https://example.test"), key):
            path = directory / member.key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(store.objects[member.path])
            paths.append(path)
        return paths

    monkeypatch.setattr(r2, "download_members", download_members)
    Partial(output_dir=tmp_path / "warm", skip_upload=True).run()
    artifact = verify_generation(next((tmp_path / "warm" / "generations").iterdir()))
    assert artifact.root["spec"]["carriedForward"] == {}, "a split table is carried member for member, not whole"
    assert artifact.root["spec"]["tables"]["s.parquet"]["partitionColumns"] == ["congress"]
    members = {path.relative_to(old).as_posix(): path.read_bytes() for path in (old / "s").rglob("*.parquet")}
    generation = next((tmp_path / "warm" / "generations").iterdir())
    assert {key: (generation / key).read_bytes() for key in members} == members, "each member's bytes, unchanged"


@pytest.mark.parametrize("failure", [OSError("interrupted transfer"), error("PreconditionFailed")])
def test_a_failed_version_1_write_is_logged_and_repaired_by_the_next_publish(tmp_path, failure):
    """Version 2 is the pointer, so the publish stands; version 1 stays behind until any writer derives it again.

    Both a transport failure and a version 1 that keeps moving (every conditional write refused) are logged.
    """
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    stale = store.objects[pub.INDEX_KEY]

    def interrupt(key):
        if key == pub.INDEX_KEY:
            raise failure

    store.before_put = interrupt
    messages = []
    sink = logger.add(lambda message: messages.append(message.record["message"]), level="ERROR")
    try:
        current = publish(store, new, prior)
    finally:
        logger.remove(sink)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == current
    assert current["families"]["test"] != prior["families"]["test"] and store.objects[pub.INDEX_KEY] == stale
    assert [m for m in messages if "publication.json is behind publication.v2.json; the next publish rederives it" in m]

    store.before_put = None
    published = publish(store, sibling, current)
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(published)
    assert published["families"]["test"] == current["families"]["test"]


def _v1_only(store, index: dict) -> dict:
    """Leave ``store`` as publishing before version 2 did: version 1 alone, in canonical bytes; return that index."""
    del store.objects[pub.INDEX_V2_KEY]
    store.objects[pub.INDEX_KEY] = canonical_json_bytes(pub.derive_v1(index))
    return pub.parse_index(store.objects[pub.INDEX_KEY])


def test_the_first_publish_after_the_merge_creates_version_2_from_version_1(tmp_path):
    old, _ = build(tmp_path)
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = _v1_only(store, publish(store, old))
    assert prior["version"] == 1 and pub.INDEX_V2_KEY not in store.objects

    published = publish(store, sibling, prior)
    assert set(published["families"]) == {"test", "other"}
    assert published["families"]["test"] == prior["families"]["test"]
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == published
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(published)


def test_two_writers_creating_version_2_at_once_both_publish(tmp_path):
    """Both read version 1 while version 2 is absent; the second to create it loses that write, rereads and merges."""
    old, _ = build(tmp_path)
    first, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    second, _ = build(tmp_path, "third", family="third", keys=("d.parquet",))
    store = Store()
    prior = _v1_only(store, publish(store, old))

    def rival(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            publish(store, second, prior)

    store.before_put = rival
    published = publish(store, first, prior)
    assert set(published["families"]) == {"test", "other", "third"}
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == published
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(published)


def test_a_version_1_only_publish_during_the_bootstrap_is_folded_into_version_2(tmp_path):
    """A writer predating version 2 publishes to version 1 alone after this writer created version 2 from it.

    Deriving version 1 from version 2 would erase that publish from both pointers; it is folded into version 2 first.
    """
    old, _ = build(tmp_path)
    newer, _ = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = _v1_only(store, publish(store, old))
    legacy = Store()
    legacy.objects = dict(store.objects)
    moved = pub.derive_v1(publish(legacy, newer, prior))["families"]["test"]

    def predating(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            store.objects.update({k: v for k, v in legacy.objects.items() if k.startswith(moved["prefix"])})
            store.objects[pub.INDEX_KEY] = canonical_json_bytes({**prior, "families": {"test": moved}})

    store.before_put = predating
    publish(store, sibling, prior)
    final = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert set(final["families"]) == {"test", "other"} and final["families"]["test"] == moved
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(final)


def test_folding_moves_only_families_version_1_alone_changed():
    """``g`` appeared in version 1 alone, so a writer predating version 2 published it. ``f`` changed in version 1
    because a writer derived it from version 2, which has moved on since; folding it would revert version 2."""
    def index(version, **families):
        return {"format": "spicy-regs-publication", "version": version, "families": {name: {
            "prefix": f"generations/{name}/{digit * 64}", "logicalId": f"urn:x:{name}", "artifactDigest": "sha256:" + digit * 64,
            "tables": {f"{name}.parquet": {"sha256": "sha256:" + digit * 64, "byteSize": 1, "rows": 1,
                                           "columns": [["id", "VARCHAR"]]}}} for name, digit in families.items()}}

    store = Store()
    store.objects[pub.INDEX_V2_KEY] = canonical_json_bytes(index(2, f="3"))
    pub._fold_v1(store, "test", canonical_json_bytes(index(1, f="0")), canonical_json_bytes(index(1, f="1", g="2")))
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == index(2, f="3", g="2")


def test_a_raced_version_1_write_rederives_from_the_newer_version_2(tmp_path, monkeypatch):
    """A rival publishing after this writer read version 2 for its version-1 write leaves version 1 at the newer state.

    The rival runs once this writer's read of version 2 has been answered, the interleaving that would let a writer
    reading version 2 before version 1's token write an older state over a newer one.
    """
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    armed = []
    get_object = store.get_object

    def get(*, Bucket, Key):
        response = get_object(Bucket=Bucket, Key=Key)
        if Key == pub.INDEX_V2_KEY and armed:
            armed.clear()
            store.before_put = None  # the rival's own version-2 write must not re-arm this
            publish(store, sibling, pub.parse_index(store.objects[pub.INDEX_V2_KEY]))
        return response

    store.before_put = lambda key: armed.append(key) if key == pub.INDEX_V2_KEY else None
    monkeypatch.setattr(store, "get_object", get)
    publish(store, new, prior)
    final = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert set(final["families"]) == {"test", "other"}
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(final)


def test_version_1_omits_split_tables_and_a_family_left_without_tables():
    single = {"sha256": "sha256:" + "c" * 64, "byteSize": 1, "rows": 1, "columns": [["id", "VARCHAR"]]}
    split = {"byteSize": 1, "rows": 1, "columns": [["id", "VARCHAR"]], "partitionColumns": ["id"], "members": []}
    family = {"prefix": "p", "logicalId": "urn:x", "artifactDigest": "sha256:" + "d" * 64}
    index = {"format": "spicy-regs-publication", "version": 2, "families": {
        "mixed": {**family, "tables": {"one.parquet": single, "many.parquet": split}},
        "split": {**family, "tables": {"all.parquet": split}}}}
    assert pub.derive_v1(index) == {"format": "spicy-regs-publication", "version": 1, "families": {
        "mixed": {**family, "tables": {"one.parquet": single}}}}


_INSTANT = "%Y-%m-%dT%H:%M:%SZ"


def _stamp(index: dict, family: str = "test") -> datetime:
    return datetime.strptime(index["families"][family]["publishedAt"], _INSTANT).replace(tzinfo=timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_the_pointer_write_stamps_when_it_moved_the_pointer_and_version_1_omits_it(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    stamps = []
    prior = None
    for directory in (old, new):
        before = _now().replace(microsecond=0)
        prior = publish(store, directory, prior)
        stored = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
        assert stored == prior
        assert before <= _stamp(stored) <= _now()
        assert "publishedAt" not in pub.parse_index(store.objects[pub.INDEX_KEY])["families"]["test"]
        assert pub.parse_index(store.objects[pub.INDEX_KEY]) == pub.derive_v1(stored)
        stamps.append(_stamp(stored))
    assert stamps[0] <= stamps[1]


def test_a_build_that_read_a_stamped_or_unstamped_index_still_publishes(tmp_path):
    """``publishedAt`` says when the pointer moved, not what a build read, so it never makes a family look changed.

    The build reads the index through ``parse_index`` (stamp kept), or before a backfill stamped it (stamp absent).
    """
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    third, latest = build(tmp_path, "three", value="three")
    store = Store()
    publish(store, old)
    read = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert "publishedAt" in read["families"]["test"]
    assert publish(store, new, read)["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    unstamped = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    del unstamped["families"]["test"]["publishedAt"]
    assert publish(store, third, unstamped)["families"]["test"]["artifactDigest"] == latest.pin.artifact_digest
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, new, read)


def test_replaying_the_current_generation_keeps_when_the_pointer_moved_to_it(tmp_path, monkeypatch):
    directory, _ = build(tmp_path)
    store = Store()
    prior = publish(store, directory)
    monkeypatch.setattr(pub, "_instant", lambda seconds=None: "2030-01-01T00:00:00Z")
    assert publish(store, directory, prior) == prior
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == prior


def _unstamped(store, *families: str) -> dict:
    """Leave ``store``'s version 2 as written before the writer stamped ``families``; return that index."""
    index = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    for family in families:
        del index["families"][family]["publishedAt"]
    store.objects[pub.INDEX_V2_KEY] = canonical_json_bytes(index)
    return index


def test_backfill_stamps_only_entries_lacking_the_instant_from_their_generation_s_latest_write(tmp_path):
    one, _ = build(tmp_path)
    other, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    publish(store, one)
    published = publish(store, other)
    index = _unstamped(store, "other")
    prefix = index["families"]["other"]["prefix"]
    for key in store.objects:
        if key.startswith(prefix + "/"):
            store.modified[key] = datetime(2026, 9, 1, 12, 0, 0, tzinfo=timezone.utc)
    # The root is written first; the pointer moves after the last member and its read-back.
    store.modified[f"{prefix}/c.parquet"] = datetime(2026, 9, 1, 12, 15, 2, 750_000, tzinfo=timezone.utc)
    v1, writes = store.objects[pub.INDEX_KEY], list(store.writes)

    planned = pub.backfill_published_at(store, "test")
    assert planned["stamps"] == {"other": "2026-09-01T12:15:02Z"} and not planned["applied"]
    assert store.writes == writes

    applied = pub.backfill_published_at(store, "test", apply=True)
    assert applied["stamps"] == planned["stamps"] and applied["applied"]
    stored = pub.parse_index(store.objects[pub.INDEX_V2_KEY])
    assert stored["families"]["other"] == {**index["families"]["other"], "publishedAt": "2026-09-01T12:15:02Z"}
    assert stored["families"]["test"] == published["families"]["test"]
    assert store.objects[pub.INDEX_KEY] == v1

    writes = list(store.writes)
    again = pub.backfill_published_at(store, "test", apply=True)
    assert again["stamps"] == {} and not again["applied"] and store.writes == writes


def test_backfill_refuses_when_the_pointer_moved_since_it_read_it(tmp_path):
    one, _ = build(tmp_path)
    two, _ = build(tmp_path, "two", value="two")
    store = Store()
    _unstamped(store, *publish(store, one)["families"])
    prior = pub.parse_index(store.objects[pub.INDEX_V2_KEY])

    moved = []

    def rival(key):
        if key == pub.INDEX_V2_KEY:
            store.before_put = None
            moved.append(publish(store, two, prior))

    store.before_put = rival
    with pytest.raises(pub.PublicationError, match="moved since"):
        pub.backfill_published_at(store, "test", apply=True)
    assert pub.parse_index(store.objects[pub.INDEX_V2_KEY]) == moved[0]
