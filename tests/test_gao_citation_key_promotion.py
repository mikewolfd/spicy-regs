"""The small GAO recipe preserves literal older records and real lifecycle guards."""
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactInput, LocalMemberSource, iter_member_descriptors

from scripts.prepare_gao_citation_key_promotion import prepare
from spicy_regs import etl_policy_registry
from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, failure_receipt, observation_receipt, write_dataset
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.rollups.gao_reports import GaoReportsRollup
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.transforms.government_receipts import EARLIER_POLICIES, POLICIES
from spicy_regs.transforms.government_source_shapes import map_subject
from tests.generation_fakes import Store


def digest(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def prior(tmp_path, monkeypatch):
    policies = (EARLIER_POLICIES["gao_reports"][0], POLICIES["gao_decisions"])
    raw = {
        # These heterogeneous literal rows deliberately omit later nullable columns.
        "gao_reports": [{"report_id": "gao-27-108551", "report_number": None},
                        {"report_id": "different-slug", "report_number": "GAO-23-105520"},
                        {"report_id": "gao-alias", "report_number": "GAO-23-105520", "source": None},
                        {"report_id": "blank-number", "report_number": ""}],
        "gao_decisions": [{"decision_number": "B-123456", "decision_date": "2025-01-01", "outcome_rule": "retained:original-rule", "url": "https://www.gao.gov/products/b-123456", "b_numbers_json": '["B-123456","B-123456"]'}],
    }
    outputs, shards = [], []
    for policy in policies:
        witness = {"source_id": "fixture:GAO", "source_uri": "held:fixture", "sha256": "sha256:" + "a" * 64,
                   "locator": "original", "body_version": "v1"}
        context = ReceiptContext("prior", "source-attempt", policy.policy_version, [witness], {"source_detail": "held"})
        failures = [failure_receipt(policy, context, outcome="refused", raw_fields={"raw_record": {"unknown": "held"}}),
                    observation_receipt(policy, ReceiptContext("prior", "empty", policy.policy_version, [witness]),
                                        processing_fields={"raw_record": None})]
        records = []
        for i, original in enumerate(raw[policy.dataset]):
            subject = map_subject(policy.dataset, original)
            subject = {name: subject[name] for name in policy.subject_schema.names}
            records.append(({**subject, "raw_record": original}, ReceiptContext(
                "prior", f"row:{i}", policy.policy_version, [witness, dict(witness)], {"source_detail": "held"})))
        subject, shard = write_dataset(records, tmp_path / "writer" / policy.dataset, policy, failures=failures)
        assert subject is not None
        outputs.append(subject)
        shards.append(shard)
    combined = combine_receipts(shards, tmp_path / "writer/etl_receipts.parquet")
    old_evidence = CaptureEvidence(tmp_path / "evidence", "gao-reports")
    installed = etl_policy_registry.installed_policies()
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, "installed_policies", lambda: installed | {p.dataset: p for p in policies})
        artifact = build_generation(tmp_path / "native", family="gao-reports", files=outputs,
            expected_keys=GaoReportsRollup.outputs, receipt_path=combined, receipt_policies=policies,
            receipt_generation_id="prior", inputs=old_evidence.inputs())
    members = list(iter_member_descriptors(artifact, LocalMemberSource(tmp_path / "native")))
    index = publication.empty_index()
    index["families"]["gao-reports"] = publication.generation_entry(artifact, members)
    captured = tmp_path / "captured.json"
    captured.write_text(json.dumps(index))
    controls = tmp_path / "native"
    bindings = tmp_path / "bindings.json"
    bindings.write_text(json.dumps({"selected_artifact": artifact.pin.artifact_digest,
        "prior_controls_directory": str(controls), "bindings": [{"key": key,
        "canonical_path": str(controls / key), "bytes": (controls / key).stat().st_size,
        "raw_sha256": digest(controls / key).removeprefix("sha256:")}
        for key in ("artifact.json", "members.json", "etl_receipts.parquet")]}))
    return {"captured_index": captured, "member_bindings": bindings, "reports": controls / "gao_reports.parquet",
        "decisions": controls / "gao_decisions.parquet", "expected_prior": artifact.pin.artifact_digest,
        "expected_index": digest(captured), "expected_bindings": digest(bindings)}, raw, old_evidence


def test_exact_native_family_preserves_literal_records_and_all_outcomes(prior, tmp_path, monkeypatch):
    args, raw, _ = prior
    def prohibited(*args, **kwargs):
        pytest.fail("Preparation must not acquire, fetch or publish")
    monkeypatch.setattr(publication, "fetch_member", prohibited)
    monkeypatch.setattr(publication, "current_index", prohibited)
    monkeypatch.setattr(publication, "publish_generation", prohibited)
    monkeypatch.setattr("spicy_regs.pipelines.rollups.gao_reports.build_gao_reports", prohibited)
    held = {p: p.read_bytes() for p in args["reports"].parent.iterdir()}
    result = prepare(**args, output=tmp_path / "promotion")
    assert result["preservedMainRows"] == {"gao_reports": 4, "gao_decisions": 1}
    assert result["outcomes"] == {"accepted": 5, "refused": 2, "observed": 2}
    assert result["remoteGets"] == 0 and result["published"] is False
    from spicy_regs.conversion_reads import ConversionReadOperation
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(tmp_path / "promotion/generation")
        for dataset, originals in raw.items():
            restored = list(reader.read_subjects(dataset))
            assert [r["raw_record"] for r in restored] == originals
    main = pq.read_table(tmp_path / "promotion/generation/gao_reports.parquet").to_pylist()
    assert [r["citation_product_id"] for r in main] == ["GAO-27-108551", "GAO-23-105520", "GAO-23-105520", ""]
    assert all(path.read_bytes() == content for path, content in held.items())
    assert (tmp_path / "promotion/generation/gao_decisions.parquet").read_bytes() == args["decisions"].read_bytes()
    assert result["carriedMainBytes"] == ["gao_decisions.parquet"]
    assert (tmp_path / "promotion/preparation.json").is_file()


@pytest.mark.parametrize("problem", ["index", "bindings", "prior", "wrong_receipt_pin", "wrong_control_pin", "missing_binding", "symlink", "corrupt_payload", "nested_output"])
def test_inexact_custody_or_inputs_refuse_without_success(prior, tmp_path, problem):
    args, _, _ = prior
    args = dict(args)
    output = tmp_path / "refused"
    if problem in {"index", "bindings", "prior"}:
        args["expected_" + problem] = "sha256:" + "f" * 64
    elif problem in {"wrong_receipt_pin", "wrong_control_pin", "missing_binding"}:
        value = json.loads(args["member_bindings"].read_text())
        if problem == "missing_binding":
            value["bindings"].pop()
        else:
            value["bindings"][0 if problem == "wrong_control_pin" else -1]["raw_sha256"] = "f" * 64
        args["member_bindings"].write_text(json.dumps(value))
        args["expected_bindings"] = digest(args["member_bindings"])
    elif problem == "symlink":
        alias = tmp_path / "reports-link"
        alias.symlink_to(args["reports"])
        args["reports"] = alias
    elif problem == "corrupt_payload":
        body = bytearray(args["reports"].read_bytes())
        body[12] ^= 1
        args["reports"].write_bytes(body)
    else:
        output = args["reports"].parent / "refused"
    with pytest.raises((ValueError, RuntimeError)):
        prepare(**args, output=output)
    assert not (output / "preparation.json").exists()


def selected_store(args):
    selected = json.loads(args["captured_index"].read_text())
    store = Store()
    store.objects[publication.INDEX_V2_KEY] = args["captured_index"].read_bytes()
    prefix = selected["families"]["gao-reports"]["prefix"]
    for path in args["reports"].parent.iterdir():
        store.objects[prefix + "/" + path.name] = path.read_bytes()
    return store


def test_promoted_family_passes_actual_publisher_lineage_gate(prior, tmp_path):
    args, _, old_evidence = prior
    result = prepare(**args, output=tmp_path / "promotion")
    selected = json.loads(args["captured_index"].read_text())
    store = selected_store(args)
    index = publication.publish_generation(tmp_path / "promotion/generation", client=store, bucket="fixture",
        prior_index=selected, evidence_directories=(Path(result["sourceEvidence"]["directory"]),), exact_prior=True)
    assert index["families"]["gao-reports"]["artifactDigest"] == result["candidate"]["artifactDigest"]
    assert old_evidence.artifact is not None
    assert result["sourceEvidence"]["artifactDigest"] != old_evidence.artifact.pin.artifact_digest


def test_old_evidence_fails_actual_publisher_before_any_write(prior, tmp_path):
    args, _, old_evidence = prior
    result = prepare(**args, output=tmp_path / "promotion")
    assert old_evidence.artifact is not None
    selected = json.loads(args["captured_index"].read_text())
    root = json.loads((tmp_path / "promotion/generation/artifact.json").read_text())
    entry = selected["families"]["gao-reports"]
    build_generation(tmp_path / "wrong-evidence", family="gao-reports",
        files=[tmp_path / "promotion/generation" / key for key in GaoReportsRollup.outputs],
        expected_keys=GaoReportsRollup.outputs, read_snapshot=selected,
        receipt_path=tmp_path / "promotion/generation/etl_receipts.parquet",
        receipt_policies=GaoReportsRollup.receipt_policies,
        receipt_generation_id=root["spec"]["etlReceipts"]["generationId"],
        inputs=[ArtifactInput("prior-generation", entry["logicalId"], entry["artifactDigest"]),
                ArtifactInput("source-evidence", old_evidence.artifact.pin.logical_id, old_evidence.artifact.pin.artifact_digest)])
    store = selected_store(args)
    with pytest.raises(publication.PublicationError, match="different inherited inputs"):
        publication.publish_generation(tmp_path / "wrong-evidence", client=store, bucket="fixture",
            prior_index=selected, evidence_directories=(old_evidence.artifact_dir,), exact_prior=True)
    assert not store.writes
    assert result["sourceEvidence"]["artifactDigest"] != old_evidence.artifact.pin.artifact_digest
