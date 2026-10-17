"""Owner admission reuse renews bytes and leaves independent admission intact."""

import shutil

import pyarrow as pa
import pytest
from rulespec_artifacts import ArtifactVerificationError

from spicy_regs import generations
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, write_dataset
from spicy_regs.sources import publication
from tests.generation_fakes import Store


@pytest.fixture
def counted(monkeypatch):
    calls = {"joins": 0, "pages": 0}
    receipts, info = generations._verify_receipts, generations._table_info

    def check(*args, **kwargs):
        calls["joins"] += 1
        return receipts(*args, **kwargs)

    def pages(*args, **kwargs):
        calls["pages"] += 1
        return info(*args, **kwargs)

    monkeypatch.setattr(generations, "_verify_receipts", check)
    monkeypatch.setattr(generations, "_table_info", pages)
    return calls


def family(tmp_path):
    selected = DatasetPolicy("things", pa.schema([("id", pa.string())]), ("id",), ("raw",))
    context = ReceiptContext("g", "a", "test", [{"source_id": "x", "sha256": "a" * 64}])
    subject, receipts = write_dataset([({"id": "one", "raw": "retained"}, context)], tmp_path / "writer", selected)
    assert subject is not None
    target = tmp_path / "generation"
    generations.build_generation(target, family="test", files=[subject], expected_keys=["things.parquet"],
                                 receipt_path=receipts, receipt_policies=[selected], receipt_generation_id="g")
    return target


def test_build_publish_joins_once_and_independent_reader_checks_again(tmp_path, counted):
    target = family(tmp_path)
    assert counted == {"joins": 1, "pages": 2}
    # Renaming/copying a completed generation does not require semantic rescanning.
    relocated = tmp_path / "relocated"
    shutil.copytree(target, relocated)
    publication.publish_generation(relocated, client=Store(), bucket="test", prior_index=publication.empty_index())
    assert counted == {"joins": 1, "pages": 2}
    generations.verify_generation(relocated)
    assert counted == {"joins": 2, "pages": 4}


@pytest.mark.parametrize("member", ["things.parquet", "etl_receipts.parquet", "artifact.json"])
def test_owner_completion_does_not_admit_changed_bytes(tmp_path, counted, member):
    target = family(tmp_path)
    path = target / member
    path.write_bytes(path.read_bytes() + b"changed")
    store = Store()
    with pytest.raises((ValueError, ArtifactVerificationError)):
        publication.publish_generation(target, client=store, bucket="test", prior_index=publication.empty_index())
    assert not store.writes


def test_validator_change_forces_fresh_semantic_admission(tmp_path, monkeypatch, counted):
    target = family(tmp_path)
    identity = generations.validation_identity
    monkeypatch.setattr(generations, "validation_identity", lambda: {**identity(), "changed": "validator/policy"})
    generations.admit_generation_for_publication(target)
    assert counted["joins"] == 2


def test_current_policy_change_still_refuses_owner_hit(tmp_path, monkeypatch, counted):
    from spicy_regs import etl_policy_registry

    target = family(tmp_path)
    selected = DatasetPolicy("things", pa.schema([("id", pa.string())]), ("id",), ("raw",), policy_version="new")
    monkeypatch.setattr(etl_policy_registry, "installed_policies", lambda: {"things": selected})
    store = Store()
    with pytest.raises(ValueError, match="installed field policy"):
        publication.publish_generation(target, client=store, bucket="test", prior_index=publication.empty_index())
    assert not store.writes


def test_mutation_during_admission_never_records_completion(tmp_path, monkeypatch, counted):
    target = family(tmp_path)
    generations._OWNER_ADMISSIONS.clear()
    verify = generations._verify_admitted_generation

    def changed(*args, **kwargs):
        admitted = verify(*args, **kwargs)
        path = target / "things.parquet"
        path.write_bytes(path.read_bytes() + b"changed")
        return admitted

    monkeypatch.setattr(generations, "_verify_admitted_generation", changed)
    with pytest.raises((ValueError, ArtifactVerificationError)):
        generations.admit_generation_for_publication(target)
    assert not generations._OWNER_ADMISSIONS


@pytest.mark.parametrize("changed_file", ["validator.py", "policy.json"])
def test_source_or_policy_bytes_invalidate_owner_completion(tmp_path, monkeypatch, counted, changed_file):
    sources = tmp_path / "validator-sources"
    sources.mkdir()
    (sources / "validator.py").write_text("version = 1")
    (sources / "policy.json").write_text('{"policy":1}')
    monkeypatch.setattr(generations, "__file__", str(sources / "validator.py"))
    target = family(tmp_path)
    (sources / changed_file).write_text("changed exact validator or policy bytes")
    generations.admit_generation_for_publication(target)
    assert counted["joins"] == 2
