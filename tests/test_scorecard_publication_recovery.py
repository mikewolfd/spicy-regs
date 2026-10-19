"""An uncertain scorecard write is observed without repeating publication."""

from types import SimpleNamespace
from collections import OrderedDict
import json

import pytest
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs import generations
from spicy_regs.scorecards.operations import publish
from spicy_regs.generations import build_generation
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, write_family
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import edition, tables


def setup_candidate(tmp_path, monkeypatch, *, current=True, status="prepared_not_published", family="scorecards",
                    observe=True):
    previous = publish.publication.empty_index()
    source = tmp_path / "source"
    files = write_family(source, tables(edition("2025")))
    directory = tmp_path / "generation"
    artifact = build_generation(directory, family=family, files=files, expected_keys=[p.name for p in files],
                                read_snapshot=previous, **generation_options(source, SOURCE_NAMES))
    pin = artifact.pin.as_dict()
    snapshot = publish.publication.publish_generation(directory, client=Store(), bucket="test", prior_index=previous)
    if not current:
        snapshot["families"][family]["artifactDigest"] = "sha256:" + "b" * 64
    (tmp_path / "index.json").write_text(json.dumps(previous))
    prep = tmp_path / "preparation.json"
    prep.write_text(json.dumps({"status": status, "generation": pin, "generation_directory": str(directory),
                               "read_snapshot_path": str(tmp_path / "index.json")}))
    monkeypatch.setattr(publish, "load_dotenv", lambda path: None)
    monkeypatch.setattr(publish, "implementation_id", lambda: "current-verifier")
    store = Store()
    monkeypatch.setattr(store, "meta", SimpleNamespace(
        endpoint_url="https://174055408ff1560e60601c4d12c561c4.r2.cloudflarestorage.com"), raising=False)
    monkeypatch.setattr(publish.r2, "get_r2_client", lambda: store)
    monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")

    def no_write(*args, **kwargs):
        raise AssertionError("Observe-only must never upload or write a pointer")

    if observe:
        monkeypatch.setattr(publish.publication, "_stored_index", lambda *args: (snapshot, None))
        monkeypatch.setattr(publish.publication, "publish_generation", no_write)
    return prep, artifact, snapshot


@pytest.fixture
def semantic_checks(monkeypatch):
    calls = []
    verify = generations._verify_receipts

    def counted(*args, **kwargs):
        calls.append(True)
        return verify(*args, **kwargs)

    monkeypatch.setattr(generations, "_verify_receipts", counted)
    return calls


def test_observe_current_candidate_recovers_authenticated_receipt_without_writes(tmp_path, monkeypatch, semantic_checks):
    prep, _, _ = setup_candidate(tmp_path, monkeypatch)
    before = len(semantic_checks)
    output = tmp_path / "recovery"
    publish.main(["--preparation", str(prep), "--output", str(output), "--observe-only"])
    assert len(semantic_checks) == before + 1
    receipt = json.loads((output / "receipt.json").read_bytes())
    assert receipt["status"] == "published_authenticated_readback"
    assert receipt["publication_write_requested"] is False
    assert receipt["verifier_implementation_id"] == "current-verifier"


@pytest.mark.parametrize("cold", [False, True])
def test_publication_admits_unseen_candidate_once_and_reuses_owner_completion(tmp_path, monkeypatch, semantic_checks, cold):
    prep, _, _ = setup_candidate(tmp_path, monkeypatch, observe=False)
    if cold:
        monkeypatch.setattr(generations, "_OWNER_ADMISSIONS", OrderedDict())
    before = len(semantic_checks)
    output = tmp_path / "publication"
    publish.main(["--preparation", str(prep), "--output", str(output)])
    assert len(semantic_checks) == before + int(cold)
    receipt = json.loads((output / "receipt.json").read_bytes())
    assert receipt["status"] == "published_authenticated_readback"
    assert receipt["publication_write_requested"] is True
    assert publish.publication.INDEX_V2_KEY in publish.r2.get_r2_client().writes


@pytest.mark.parametrize("fault", ["pin", "bytes"])
def test_publication_refuses_wrong_pin_or_changed_bytes_before_remote_writes(tmp_path, monkeypatch, fault):
    prep, _, _ = setup_candidate(tmp_path, monkeypatch, observe=False)
    prepared = json.loads(prep.read_bytes())
    if fault == "pin":
        prepared["generation"]["artifactDigest"] = "sha256:" + "f" * 64
        prep.write_text(json.dumps(prepared))
    else:
        path = tmp_path / "generation" / "scorecards.parquet"
        path.write_bytes(path.read_bytes() + b"changed")
    output = tmp_path / "publication"
    with pytest.raises((ValueError, ArtifactVerificationError)):
        publish.main(["--preparation", str(prep), "--output", str(output)])
    store = publish.r2.get_r2_client()
    assert not store.writes
    assert not store.reads
    assert not (output / "receipt.json").exists()


@pytest.mark.parametrize("fault", ["not-current", "status", "family", "snapshot", "generation"])
def test_observation_cannot_certify_a_wrong_or_unprepared_candidate(tmp_path, monkeypatch, fault):
    prep, _, snapshot = setup_candidate(tmp_path, monkeypatch, current=fault != "not-current",
                                       status="failed" if fault == "status" else "prepared_not_published",
                                       family="unrelated" if fault == "family" else "scorecards")
    if fault == "snapshot":
        (tmp_path / "index.json").write_text(json.dumps(snapshot))
    elif fault == "generation":
        prepared = json.loads(prep.read_bytes())
        prepared["generation"]["artifactDigest"] = "different"
        prep.write_text(json.dumps(prepared))
    output = tmp_path / "recovery"
    with pytest.raises(ValueError):
        publish.main(["--preparation", str(prep), "--output", str(output), "--observe-only"])
    assert not (output / "receipt.json").exists()


@pytest.mark.parametrize("field", ["logicalId", "tables", "etlReceipts"])
def test_same_digest_cannot_certify_different_candidate_descriptors(tmp_path, monkeypatch, field):
    prep, _, snapshot = setup_candidate(tmp_path, monkeypatch)
    current = snapshot["families"]["scorecards"]
    if field == "logicalId":
        current[field] = "urn:different"
    elif field == "tables":
        current[field]["scorecards.parquet"]["sha256"] = "sha256:" + "f" * 64
    else:
        current[field]["generationId"] = "different-generation"
    output = tmp_path / "recovery"
    with pytest.raises(ValueError, match="not the currently published family"):
        publish.main(["--preparation", str(prep), "--output", str(output), "--observe-only"])
    assert not (output / "receipt.json").exists()
