"""Actual Parquet families, interrupted writes and immutable reader snapshots."""

import json
from contextlib import contextmanager

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactVerificationError, canonical_json_bytes

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.sources import publication as pub, r2
from tests.generation_fakes import Store


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
    assert store.writes[-1] == pub.INDEX_KEY
    assert index["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    for key in ("a.parquet", "b.parquet"):
        location, info = pub.table_location(index, key)
        assert info is not None
        assert store.objects[location] == (directory / key).read_bytes()
        assert info["rows"] == 1
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == index


@pytest.mark.parametrize("failure", ["a.parquet", "b.parquet", "artifact.json", "members.json", pub.INDEX_KEY])
def test_interruption_never_changes_previous_complete_generation(tmp_path, failure):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    old_pointer = store.objects[pub.INDEX_KEY]
    old_members = {key: store.objects[pub.table_location(prior, key)[0]] for key in ("a.parquet", "b.parquet")}

    def interrupt(key):
        if key.endswith(failure):
            raise OSError("interrupted transfer")

    store.before_put = interrupt
    with pytest.raises(OSError, match="interrupted"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_KEY] == old_pointer
    for key, raw in old_members.items():
        assert store.objects[pub.table_location(prior, key)[0]] == raw


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
        if key == pub.INDEX_KEY:  # semantically same, a new object version on every attempt
            rivals.append(json.dumps(prior, indent=len(rivals)).encode())
            store.objects[key] = rivals[-1]

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="concurrently"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_KEY] == rivals[-1]
    assert len(rivals) == pub._POINTER_ATTEMPTS


def test_concurrent_sibling_family_is_merged_not_overwritten(tmp_path):
    old, _ = build(tmp_path)
    new, artifact = build(tmp_path, "two", value="two")
    sibling, _ = build(tmp_path, "other", family="other", keys=("c.parquet",))
    store = Store()
    prior = publish(store, old)
    rival = publish(Store(), sibling)["families"]["other"]

    def concurrent(key):
        if key == pub.INDEX_KEY:
            store.before_put = None
            index = pub.parse_index(store.objects[key])
            index["families"]["other"] = rival
            store.objects[key] = canonical_json_bytes(index)

    store.before_put = concurrent
    published = publish(store, new, prior)
    assert published["families"]["other"] == rival
    assert published["families"]["test"]["artifactDigest"] == artifact.pin.artifact_digest
    assert pub.parse_index(store.objects[pub.INDEX_KEY]) == published


def test_concurrent_same_family_commit_refuses_as_stale(tmp_path):
    old, _ = build(tmp_path)
    new, _ = build(tmp_path, "two", value="two")
    third, _ = build(tmp_path, "three", value="three")
    store = Store()
    prior = publish(store, old)
    other = Store()
    other.objects = dict(store.objects)
    publish(other, third, prior)
    rival = other.objects[pub.INDEX_KEY]

    def concurrent(key):
        if key == pub.INDEX_KEY:
            store.before_put = None
            store.objects[key] = rival

    store.before_put = concurrent
    with pytest.raises(pub.PublicationError, match="changed since"):
        publish(store, new, prior)
    assert store.objects[pub.INDEX_KEY] == rival


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
        if key == pub.INDEX_KEY:
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
    assert calls.count(pub.INDEX_KEY) == 1


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
        location, _ = pub.table_location(pub.current_index("https://example.test"), key)
        path.write_bytes(store.objects[location])
        return True

    monkeypatch.setattr(r2, "download", download)
    Ingest(output_dir=retained, skip_upload=False).run()
    current = pub.parse_index(store.objects[pub.INDEX_KEY])
    location, _ = pub.table_location(current, "a.parquet")
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
    b_loc, b_info = pub.table_location(index, "b.parquet")
    assert b_info is not None
    assert b_info["sha256"] == prior["families"]["test"]["tables"]["b.parquet"]["sha256"]
    assert store.objects[b_loc] == (new / "b.parquet").read_bytes()
    # b was copied server-side from the prior prefix, not uploaded as bytes;
    # a was uploaded under the new prefix.
    assert store.copies == [b_loc]
    a_loc, _ = pub.table_location(index, "a.parquet")
    assert a_loc in store.writes and a_loc not in store.copies


def test_a_changed_member_still_uploads_bytes(tmp_path):
    old, _ = build(tmp_path, value="one")
    new, _ = build(tmp_path, "two", value="two")
    store = Store()
    prior = publish(store, old)
    publish(store, new, prior=prior)
    index = pub.parse_index(store.objects[pub.INDEX_KEY])
    for key in ("a.parquet", "b.parquet"):
        location, info = pub.table_location(index, key)
        assert store.objects[location] == (new / key).read_bytes()
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
        path.write_bytes(store.objects[pub.table_location(pub.current_index("https://example.test"), key)[0]])
        return True

    monkeypatch.setattr(r2, "download", download)
    Partial(output_dir=tmp_path / "warm", skip_upload=False).run()
    current = pub.parse_index(store.objects[pub.INDEX_KEY])
    assert set(current["families"]) == {"test"}
    assert store.objects[pub.table_location(current, "b.parquet")[0]] == (old / "b.parquet").read_bytes()
    generation = next((tmp_path / "warm" / "generations").iterdir())
    artifact = verify_generation(generation)
    assert artifact.root["spec"]["carriedForward"] == {"b.parquet": index["families"]["test"]["artifactDigest"]}
