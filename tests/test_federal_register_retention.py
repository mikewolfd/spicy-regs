"""Exact FR pages survive the scheduled rollup and runner/store interruptions."""

import hashlib
import json
import shutil
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pytest
from rulespec_artifacts import admit_artifact, iter_member_descriptors
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts
from spicy_docs.transport import acquisition

from spicy_regs.pipelines.rollups.federal_register import FederalRegisterRollup
from spicy_regs.source_evidence import EvidenceMemberSource, JOURNAL, SourceEvidenceError, verify_evidence
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_federal_register import _RAW_DOC


def runner(tmp_path, monkeypatch, raw):
    output = tmp_path / "runner" / "output"
    archive = output / "raw-source-originals"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(archive))
    monkeypatch.setenv("FR_SINCE", date.today().isoformat())

    @contextmanager
    def fetcher(_):
        yield lambda url: raw

    monkeypatch.setattr(acquisition, "federal_register_fetcher", fetcher)
    return FederalRegisterRollup(output_dir=output), archive


def valid_page():
    return json.dumps({"count": 1, "total_pages": 1, "results": [
        dict(_RAW_DOC, publication_date=date.today().isoformat())
    ]}).encode()


@pytest.mark.parametrize("interrupt", [False, True])
def test_real_rollup_publishes_original_and_receipt_with_safe_retry(tmp_path, monkeypatch, interrupt):
    raw = valid_page()
    pipeline, _ = runner(tmp_path, monkeypatch, raw)
    pipeline.run()
    evidence = pipeline.source_evidence
    generation = next((pipeline.output_dir / "generations").iterdir())
    proof = json.loads(next((pipeline.output_dir / "generation-inputs").rglob("inputs.json")).read_bytes())
    selected = proof["generations"][0]["selected_inputs"]
    assert selected["complete"] is False
    assert len(selected["used"]) == 1
    assert selected["used"][0]["sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest()

    remote = Store()
    if interrupt:
        def fail_metadata(key):
            if key.startswith("source-evidence/") and key.endswith("members.json"):
                raise OSError("interrupted after original upload")
        remote.before_put = fail_metadata
        with pytest.raises(OSError, match="interrupted"):
            pub.publish_generation(generation, client=remote, bucket="test", prior_index=pub.empty_index(),
                                   evidence_directories=(evidence.artifact_dir,))
        assert pub.INDEX_V2_KEY not in remote.objects
        remote.before_put = None

    pub.publish_generation(generation, client=remote, bucket="test", prior_index=pub.empty_index(),
                           evidence_directories=(evidence.artifact_dir,))
    digest = hashlib.sha256(raw).hexdigest()
    blob = "blobs/sha256/" + digest
    assert remote.writes.count("source-evidence/" + blob) == 1
    assert remote.objects["source-evidence/" + blob] == raw
    prefix = "source-evidence/" + evidence.artifact.pin.artifact_digest.removeprefix("sha256:")
    blobs = {m.object_key for m in iter_member_descriptors(evidence.artifact, EvidenceMemberSource(evidence.artifact_dir))
             if m.object_key is not None and m.object_key.startswith("blobs/sha256/")}
    pin = evidence.artifact.pin
    # Remote admission must work after every runner-local original and receipt disappears.
    shutil.rmtree(pipeline.output_dir)
    remote_source = pub._EvidenceMembers(remote, "test", prefix, None, blobs, set())
    assert verify_evidence(remote_source, expected_pin=pin).pin == pin
    journal = remote.objects[prefix + "/" + JOURNAL]
    retained = [json.loads(line) for line in journal.splitlines()
                if json.loads(line).get("event") == "retained-file"]
    assert len(retained) == 1
    assert retained[0]["sha256"] == "sha256:" + digest
    assert retained[0]["byte_size"] == len(raw)
    assert retained[0]["requested_url"].startswith("https://www.federalregister.gov/")
    assert "resolved_url" not in retained[0]  # The byte-only fetcher exposes no redirect/HTTP metadata.
    assert "observed_at" not in retained[0]
    assert remote.writes[-2:] == [pub.INDEX_V2_KEY, pub.INDEX_KEY]


@pytest.mark.parametrize("raw", [b"not JSON", b'{"count":"invalid","results":[]}'])
def test_failed_rollup_original_and_receipt_survive_runner_artifact_restore(tmp_path, monkeypatch, raw):
    pipeline, archive = runner(tmp_path, monkeypatch, raw)
    with pytest.raises(ValueError):
        pipeline.run()
    evidence = pipeline.source_evidence
    assert not list((pipeline.output_dir / "generations").glob("*"))
    assert evidence.artifact.root["spec"]["outcome"] == "failed"
    journal = (evidence.artifact_dir / JOURNAL).read_bytes()
    assert not any(json.loads(line).get("event") == "selected-inputs" for line in journal.splitlines())
    receipt = evidence.directory.relative_to(pipeline.output_dir) / "artifact"
    # Match the workflow's existing output/ failure artifact, then lose the runner.
    packed = shutil.make_archive(str(tmp_path / "failure"), "zip", root_dir=pipeline.output_dir)
    shutil.rmtree(pipeline.output_dir.parent)
    restored = tmp_path / "restored-output"
    shutil.unpack_archive(packed, restored)
    restored_archive = restored / archive.name
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(restored_archive))
    assert admit_artifact(EvidenceMemberSource(restored / receipt),
                          expected_pin=evidence.artifact.pin).pin == evidence.artifact.pin
    stored = list(iter_acquisition_receipts(restored_archive, verify_objects=True))
    digest = hashlib.sha256(raw).hexdigest()
    assert any(file["sha256"] == digest and file["bytes"] == len(raw)
               for _, entry in stored for file in entry["files"])
    assert any(entry["receipt"].get("outcome") == "failed" for _, entry in stored)
    with CentralRawArchiveStore(restored_archive, create=False).open(digest) as body:
        assert body.read() == raw


def test_raw_retention_failure_prevents_generation(tmp_path, monkeypatch):
    pipeline, _ = runner(tmp_path, monkeypatch, valid_page())
    def unavailable(*args, **kwargs):
        raise OSError("raw storage unavailable")
    monkeypatch.setattr(CentralRawArchiveStore, "put_blob", unavailable)
    with pytest.raises(SourceEvidenceError):
        pipeline.run()
    assert not list((pipeline.output_dir / "generations").glob("*"))
    assert json.loads((pipeline.source_evidence.directory / "run-outcome.json").read_bytes())["outcome"] == "failed"


def test_hosted_fr_archive_is_inside_existing_failure_artifact():
    # The executed shell branch is intentionally limited to FR; other callers keep their archive setting.
    import subprocess
    import yaml
    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/_rollup.yml").read_text())
    steps = workflow["jobs"]["rollup"]["steps"]
    run = next(step["run"] for step in steps if step["name"] == "Run rollup")
    branch = run[run.index("          if ["):] if "          if [" in run else run
    branch = branch[branch.index("if [ \"$COMMAND\" = 'run-rollup-federal-register' ]; then"):]
    branch = branch[:branch.index("\nfi") + len("\nfi")]
    env = {"COMMAND": "run-rollup-federal-register", "GITHUB_WORKSPACE": "/runner/work/repo"}
    result = subprocess.run(["bash", "-c", branch + '\nprintf "%s" "$SPICYGOV_RAW_ROOT"'],
                            env=env, check=True, text=True, capture_output=True)
    assert result.stdout == "/runner/work/repo/output/raw-source-originals"
    failure = next(step for step in steps if step["name"] == "Retain build-only outputs and failures")
    assert "failure()" in failure["if"]
    assert failure["with"]["path"] == "output/"
