"""Shared content-addressed evidence blobs: stored once, verified once, refused when damaged."""

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
