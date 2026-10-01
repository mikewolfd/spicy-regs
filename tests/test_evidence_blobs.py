"""Shared content-addressed evidence blobs: stored once, verified once, refused when damaged."""

from io import BytesIO
import tempfile

import pytest
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_source_evidence import candidate, capture

BODY = b"retained source bytes" * 64


def evidence_run(tmp_path, name, *bodies):
    evidence = CaptureEvidence(tmp_path / name, "test")
    for body in bodies:
        evidence.capture(capture(body), stage="used")
    directory, _ = candidate(tmp_path / name, evidence)
    return directory, evidence


def publish(store, directory, evidence, prior=None):
    return pub.publish_generation(directory, client=store, bucket="test", prior_index=prior or pub.empty_index(),
                                  evidence_directories=(evidence.artifact_dir,))


def blob_keys(evidence):
    return sorted("source-evidence/blobs/sha256/" + path.name
                  for path in (evidence.artifact_dir / "blobs" / "sha256").iterdir())


def test_new_blob_is_uploaded_once_and_read_back_once(tmp_path):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    store = Store()
    publish(store, directory, evidence)
    blobs = blob_keys(evidence)
    assert blobs and all(store.writes.count(key) == 1 and store.reads[key] == 1 for key in blobs)
    prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
    assert not any(key.startswith(prefix + "/blobs/") for key in store.objects)


def test_identical_evidence_uploads_and_reads_back_no_blob_bytes(tmp_path):
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
    first_dir, first = evidence_run(tmp_path, "one", BODY)
    store = Store()
    prior = publish(store, first_dir, first)
    second_dir, second = evidence_run(tmp_path, "two", BODY)
    assert second.artifact.pin != first.artifact.pin
    blobs = blob_keys(second)
    assert blobs == blob_keys(first)
    sent, reads = dict(store.sent), dict(store.reads)
    publish(store, second_dir, second, prior)
    assert all(store.sent[key] == sent[key] and store.reads[key] == reads[key] for key in blobs)
    assert store.writes[-1] == pub.INDEX_KEY


def test_only_the_new_blob_of_a_mixed_run_is_sent_and_verified(tmp_path):
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
    first_dir, first = evidence_run(tmp_path, "one", BODY)
    store = Store()
    prior = publish(store, first_dir, first)
    second_dir, second = evidence_run(tmp_path, "two", BODY, b"a new response")
    [new] = sorted(set(blob_keys(second)) - set(blob_keys(first)))
    publish(store, second_dir, second, prior)
    assert store.writes.count(new) == 1 and store.reads[new] == 1
    assert all(store.reads[key] == 1 for key in blob_keys(first))


@pytest.mark.parametrize("damage", ["corrupt", "missing"])
def test_damaged_shared_blob_refuses_before_the_pointer_moves(tmp_path, damage):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    store = Store()
    [blob] = blob_keys(evidence)
    if damage == "corrupt":  # same size, different bytes: only the stored identity can tell
        store.objects[blob] = bytes([BODY[0] ^ 1]) + BODY[1:]
    else:

        def delete(key):
            if key.endswith("/members.json"):
                store.objects.pop(blob, None)

        store.before_put = delete
    with pytest.raises(ArtifactVerificationError):
        publish(store, directory, evidence)
    assert pub.INDEX_KEY not in store.objects
    assert not any(key.startswith("generations/") for key in store.writes)


def test_upload_refuses_bytes_that_differ_from_their_content_address(tmp_path):
    path = tmp_path / "blob"
    path.write_bytes(BODY)
    store = Store()
    with pytest.raises(pub.PublicationError, match="differ"):
        pub._put_immutable(store, "test", "source-evidence/blobs/sha256/x", path, sha256="sha256:" + "0" * 64)
    assert not store.objects


def test_blob_requests_run_concurrently(tmp_path):
    """1,810 serial round trips took 23.5 minutes (bill family, 2026-09-26); two requests must overlap."""
    import threading

    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY, b"a second response", b"a third response")
    both, arrivals, lock = threading.Barrier(2, timeout=5), [], threading.Lock()

    class Overlapping(Store):
        def head_object(self, *, Bucket, Key):
            with lock:
                arrivals.append(Key)
                first_two = "/blobs/" in Key and len([k for k in arrivals if "/blobs/" in k]) <= 2
            if first_two:
                try:
                    both.wait()
                except threading.BrokenBarrierError:
                    pass
            return super().head_object(Bucket=Bucket, Key=Key)

    store = Overlapping()
    publish(store, directory, evidence)
    assert not both.broken, "no two blob requests were in flight together"
    assert all(store.writes.count(key) == 1 and store.reads[key] == 1 for key in blob_keys(evidence))


@pytest.mark.parametrize("already_present", [False, True])
def test_untrusted_blobs_stream_into_admission_without_local_scratch(tmp_path, monkeypatch, already_present):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY * 1024, b"a second response" * 80_000)
    local_files = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file())
    bodies, active, peak = [], 0, 0

    class Bounded(BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 1024 * 1024, "Admission must request bounded chunks"
            return super().read(size)

        def close(self):
            nonlocal active
            if not self.closed:
                active -= 1
            super().close()

    class Streaming(Store):
        def get_object(self, *, Bucket, Key):
            nonlocal active, peak
            result = super().get_object(Bucket=Bucket, Key=Key)
            if Key in blob_keys(evidence):
                result["Body"].close()
                result["Body"] = Bounded(self.objects[Key])
                bodies.append(result["Body"])
                active += 1
                peak = max(peak, active)
            return result

        def put_object(self, **kwargs):
            if kwargs["Key"] in blob_keys(evidence):
                assert kwargs["IfNoneMatch"] == "*"
            return super().put_object(**kwargs)

    store = Streaming()
    if already_present:
        for key in blob_keys(evidence):
            store.objects[key] = (evidence.artifact_dir / key.removeprefix("source-evidence/")).read_bytes()
            store.etags[key] = '"different-etag-requires-readback"'

    def no_scratch(*args, **kwargs):
        raise AssertionError("Publication must not create a verification scratch directory")

    monkeypatch.setattr(tempfile, "TemporaryDirectory", no_scratch)
    publish(store, directory, evidence)
    assert peak == 1 and active == 0 and all(body.closed for body in bodies)
    assert all(store.reads[key] == 1 for key in blob_keys(evidence))
    assert all(store.writes.count(key) == (0 if already_present else 1) for key in blob_keys(evidence))
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file()) == local_files


@pytest.mark.parametrize("failure", ["interrupted-body", "extra-metadata"])
def test_remote_stream_or_membership_failure_closes_body_and_never_publishes(tmp_path, failure):
    (tmp_path / "one").mkdir()
    directory, evidence = evidence_run(tmp_path, "one", BODY)
    opened = []

    class Interrupted(BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise OSError("interrupted read")
            return super().read(min(size, 3))

    class Failing(Store):
        def get_object(self, *, Bucket, Key):
            result = super().get_object(Bucket=Bucket, Key=Key)
            if failure == "interrupted-body" and Key in blob_keys(evidence):
                result["Body"].close()
                result["Body"] = Interrupted(self.objects[Key])
                opened.append(result["Body"])
            return result

    store = Failing()
    if failure == "extra-metadata":
        prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
        store.objects[prefix + "/undeclared.json"] = b"undeclared"
    with pytest.raises((ArtifactVerificationError, OSError)):
        publish(store, directory, evidence)
    assert all(body.closed for body in opened)
    assert pub.INDEX_KEY not in store.objects and pub.INDEX_V2_KEY not in store.objects
    assert not any(key.startswith("generations/") for key in store.writes)
