"""An exact older native family can gain its literal namespace without acquisition."""
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactInput, LocalMemberSource, iter_member_descriptors

from scripts.prepare_citation_namespace_promotion import prepare
from spicy_regs import etl_policy_registry, legislative_receipts as receipts
from spicy_regs.etl_receipts import DatasetPolicy
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.rollups.print_citations import PrintCitationsRollup
from spicy_regs.sources import publication
from tests.test_legislative_receipts import retained, row


@pytest.fixture
def prior(tmp_path, monkeypatch, request):
    history = Path(receipts.__file__).with_name("citation_navigation_policy_history.json")
    old_policy = DatasetPolicy.from_descriptor(json.loads(history.read_text())["document_citations"])
    citations = [row("document_citations", document_kind="budget_volume", document_key="BUDGET-2027",
                     text_sha256="sha256:" + "c" * 64, cite_kind="bill_number", target_key="119-hr-1",
                     span_start=str(span), span_end=str(span + 2), matched_text="H.R. 1",
                     target_table=namespace, target_resolved="true" if namespace == "bills" else "false",
                     target_rule="bill_number:stated", stated_by_index="false", rule_version="fixture")
                 for span, namespace in [(12, "bills"), (4, "unknown_source_namespace"),
                                         (14, "bills"), (20, None)]]
    reads = [row("document_citation_reads", document_kind="budget_volume", document_key="EMPTY",
                 text_sha256="sha256:" + "d" * 64, citation_rows="0", rule_set_version="fixture",
                 read_at="2026-10-13T00:00:00Z")]
    originals = [retained(tmp_path / "original", key.removesuffix(".parquet"),
                         citations if key == "document_citations.parquet" else
                         reads if key == "document_citation_reads.parquet" else [],
                         {b"fixture-original": key.encode()})
                 for key in PrintCitationsRollup.source_outputs]
    current_policy, current_mapped = receipts.policy, receipts.mapped_record

    def mapped(dataset, value):
        result = current_mapped(dataset, value)
        if dataset == "document_citations":
            result.pop("target_table")
        return result

    # Build the actual declared earlier policy, without changing any production guards.
    with monkeypatch.context() as patch:
        patch.setattr(receipts, "policy", lambda dataset: old_policy if dataset == "document_citations"
                      else current_policy(dataset))
        patch.setattr(receipts, "mapped_record", mapped)
        receipts.write_legislative_outputs(originals, tmp_path / "bundle", generation_id="fixture-prior",
                                          file_outcomes_table="print_citations_document_file_outcomes")
    policies = tuple(old_policy if item.dataset == "document_citations" else item
                     for item in PrintCitationsRollup.receipt_policies)
    evidence = {"logicalId": "urn:spicy:artifact:spicy-regs-source-evidence:" + "e" * 64,
                "artifactDigest": "sha256:" + "e" * 64,
                "role": "source-evidence"}
    installed = etl_policy_registry.installed_policies()
    with monkeypatch.context() as patch:
        # Simulate the historical writer's exact installed declaration, not a bypass.
        patch.setattr(etl_policy_registry, "installed_policies",
                      lambda: installed | {"document_citations": old_policy})
        artifact = build_generation(tmp_path / "native", family="print-citations",
            files=[tmp_path / "bundle" / key for key in PrintCitationsRollup.outputs],
            expected_keys=PrintCitationsRollup.outputs,
            receipt_path=tmp_path / "bundle/etl_receipts.parquet", receipt_policies=policies,
            receipt_generation_id="fixture-prior", publication_status=getattr(request, "param", "complete-family"),
            inputs=[ArtifactInput(
                evidence["role"], evidence["logicalId"], evidence["artifactDigest"])])
    members = list(iter_member_descriptors(artifact, LocalMemberSource(tmp_path / "native")))
    index = publication.empty_index()
    index["families"]["print-citations"] = publication.generation_entry(artifact, members)
    captured = tmp_path / "captured.json"
    captured.write_text(json.dumps(index))
    bindings = tmp_path / "bindings.json"
    bindings.write_text(json.dumps({"bindings": [{"family": "print-citations",
        "artifactDigest": artifact.pin.artifact_digest, "objectKey": item.object_key,
        "sha256": item.sha256, "bytes": item.byte_size,
        "canonical_path": str(tmp_path / "native" / item.object_key)} for item in members]}))
    return {"captured_index": captured, "prior_controls": tmp_path / "native",
            "member_bindings": bindings, "expected_prior": artifact.pin.artifact_digest,
            "expected_index": "sha256:" + hashlib.sha256(captured.read_bytes()).hexdigest()}, originals, evidence


def test_complete_native_prior_promotes_literals_and_preserves_originals(prior, tmp_path, monkeypatch):
    args, originals, evidence = prior
    monkeypatch.setenv("R2_PUBLIC_URL", "https://must-not-fetch.invalid")

    def prohibited(*args, **kwargs):
        pytest.fail("This operation must not acquire, fetch or publish")

    monkeypatch.setattr(publication, "current_index", prohibited)
    monkeypatch.setattr(publication, "fetch_member", prohibited)
    monkeypatch.setattr(publication, "publish_generation", prohibited)
    monkeypatch.setattr("spicy_regs.pipelines.rollups.print_citations.build_print_citations", prohibited)
    held = {path: path.read_bytes() for path in args["prior_controls"].iterdir()}
    result = prepare(**args, output=tmp_path / "promotion")
    assert result["literalNamespacesChecked"] == 4
    assert result["preservedMainRows"]["document_citations"] == 4
    assert result["remoteGets"] == 0 and result["published"] is False
    assert result["priorStagingPayloadCopies"] == 0
    assert "privateWorkingCopies" in result and "existingSourcePayloadCopies" not in result
    candidate = tmp_path / "promotion/generation"
    assert pq.read_table(candidate / "document_citations.parquet")["target_table"].to_pylist() == [
        "bills", "unknown_source_namespace", "bills", None]
    root = json.loads((candidate / "artifact.json").read_text())
    assert root["spec"]["readSnapshot"] == json.loads(args["captured_index"].read_text())
    assert root["inputs"] == [{"logicalId": root["spec"]["readSnapshot"]["families"]["print-citations"]["logicalId"],
                               "artifactDigest": args["expected_prior"], "role": "prior-generation"}, evidence]
    assert len(result["originalChecks"]) == 5
    for path in originals:
        # The maintained selector chooses its own private layout; find only the tiny fixture's output.
        [restored] = list((tmp_path / "promotion/priors").rglob(path.name))
        assert pq.read_table(restored).equals(pq.read_table(path), check_metadata=True)
    for path, content in held.items():
        assert path.read_bytes() == content
        assert path.stat().st_ino == (tmp_path / "promotion/prior" / path.name).stat().st_ino
    assert (tmp_path / "promotion/preparation.json").is_file()


@pytest.mark.parametrize("problem", ["index_pin", "prior_pin", "missing_member", "duplicate_member",
                                     "wrong_member_pin", "corrupt_payload", "symlink_member"])
def test_mismatched_or_incomplete_binding_refuses_without_success(prior, tmp_path, problem):
    args, _, _ = prior
    args = dict(args)
    if problem == "index_pin":
        args["expected_index"] = "sha256:" + "f" * 64
    elif problem == "prior_pin":
        args["expected_prior"] = "sha256:" + "f" * 64
    else:
        value = json.loads(args["member_bindings"].read_text())
        if problem == "missing_member":
            value["bindings"].pop()
        elif problem == "duplicate_member":
            value["bindings"].append(dict(value["bindings"][0]))
        elif problem == "wrong_member_pin":
            value["bindings"][0]["artifactDigest"] = "sha256:" + "f" * 64
        elif problem == "symlink_member":
            member = Path(value["bindings"][0]["canonical_path"])
            link = tmp_path / "held-alias.parquet"
            link.symlink_to(member)
            value["bindings"][0]["canonical_path"] = str(link)
        else:
            member = Path(value["bindings"][0]["canonical_path"])
            body = bytearray(member.read_bytes())
            body[12] ^= 1
            member.write_bytes(body)
        args["member_bindings"].write_text(json.dumps(value))
    with pytest.raises((ValueError, RuntimeError)):
        prepare(**args, output=tmp_path / "refused")
    assert not (tmp_path / "refused/preparation.json").exists()


def test_output_inside_held_prior_refuses_before_staging(prior):
    args, _, _ = prior
    with pytest.raises(ValueError, match="separate"):
        prepare(**args, output=args["prior_controls"] / "promotion")


@pytest.mark.parametrize("prior", ["local-partial"], indirect=True)
def test_partial_prior_cannot_qualify_complete_family(prior, tmp_path):
    args, _, _ = prior
    with pytest.raises(ValueError, match="partial prior"):
        prepare(**args, output=tmp_path / "refused")
    assert not (tmp_path / "refused/preparation.json").exists()


@pytest.mark.parametrize("problem", ["already_promoted", "incomplete_family", "missing_receipts"])
def test_ineligible_captured_entry_refuses_before_payload_staging(prior, tmp_path, problem):
    args, _, _ = prior
    value = json.loads(args["captured_index"].read_text())
    entry = value["families"]["print-citations"]
    if problem == "already_promoted":
        entry["tables"]["document_citations.parquet"]["columns"].append(["target_table", "VARCHAR"])
    elif problem == "incomplete_family":
        entry["tables"].pop("budget_volumes.parquet")
    else:
        entry.pop("etlReceipts")
    args["captured_index"].write_text(json.dumps(value))
    args["expected_index"] = "sha256:" + hashlib.sha256(args["captured_index"].read_bytes()).hexdigest()
    with pytest.raises((ValueError, RuntimeError)):
        prepare(**args, output=tmp_path / "refused")
    assert not (tmp_path / "refused").exists()
