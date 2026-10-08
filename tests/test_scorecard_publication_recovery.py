"""An uncertain scorecard write is observed without repeating publication."""

from types import SimpleNamespace
import json

import pytest

from spicy_regs.scorecards.operations import publish


def setup_candidate(tmp_path, monkeypatch, *, current=True, status="prepared_not_published", family="scorecards"):
    pin = {"artifactDigest": "sha256:" + "a" * 64, "logicalId": "example"}
    snapshot = {"families": {family: {"artifactDigest": pin["artifactDigest"] if current else "sha256:" + "b" * 64}}}
    previous = publish.publication.empty_index()
    (tmp_path / "index.json").write_text(json.dumps(previous))
    prep = tmp_path / "preparation.json"
    prep.write_text(json.dumps({"status": status, "generation": pin, "generation_directory": str(tmp_path),
                               "read_snapshot_path": str(tmp_path / "index.json")}))
    artifact = SimpleNamespace(pin=SimpleNamespace(as_dict=lambda: pin, artifact_digest=pin["artifactDigest"]),
                               root={"spec": {"family": family, "readSnapshot": previous}})
    monkeypatch.setattr(publish, "verify_generation", lambda path: artifact)
    monkeypatch.setattr(publish, "load_dotenv", lambda path: None)
    monkeypatch.setattr(publish, "implementation_id", lambda: "current-verifier")
    monkeypatch.setattr(publish.r2, "get_r2_client", lambda: SimpleNamespace(meta=SimpleNamespace(
        endpoint_url="https://174055408ff1560e60601c4d12c561c4.r2.cloudflarestorage.com")))
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")
    monkeypatch.setattr(publish.publication, "_stored_index", lambda *args: (snapshot, None, None))

    def no_write(*args, **kwargs):
        raise AssertionError("Observe-only must never upload or write a pointer")

    monkeypatch.setattr(publish.publication, "publish_generation", no_write)
    return prep, artifact, snapshot


def test_observe_current_candidate_recovers_authenticated_receipt_without_writes(tmp_path, monkeypatch):
    prep, _, _ = setup_candidate(tmp_path, monkeypatch)
    output = tmp_path / "recovery"
    publish.main(["--preparation", str(prep), "--output", str(output), "--observe-only"])
    receipt = json.loads((output / "receipt.json").read_bytes())
    assert receipt["status"] == "published_authenticated_readback"
    assert receipt["publication_write_requested"] is False
    assert receipt["verifier_implementation_id"] == "current-verifier"


@pytest.mark.parametrize("fault", ["not-current", "status", "family", "snapshot", "generation"])
def test_observation_cannot_certify_a_wrong_or_unprepared_candidate(tmp_path, monkeypatch, fault):
    prep, artifact, _ = setup_candidate(tmp_path, monkeypatch, current=fault != "not-current",
                                       status="failed" if fault == "status" else "prepared_not_published",
                                       family="unrelated" if fault == "family" else "scorecards")
    if fault == "snapshot":
        artifact.root["spec"]["readSnapshot"] = {}
    elif fault == "generation":
        artifact.pin.as_dict = lambda: {"artifactDigest": "different"}
    output = tmp_path / "recovery"
    with pytest.raises(ValueError):
        publish.main(["--preparation", str(prep), "--output", str(output), "--observe-only"])
    assert not (output / "receipt.json").exists()
