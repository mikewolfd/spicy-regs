"""Future build proofs preserve exact native bytes without inventing coverage."""

import hashlib
import json
from copy import deepcopy

import httpx
import pytest
from rulespec_artifacts import LocalMemberSource
from spicy_docs.sources.legislators import LegislatorsBudget

from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.members import MembersRollup
from spicy_regs.source_catalog.input_proof import input_proof, save_input_proof, validate_selected_inputs
from spicy_regs.source_catalog.publication import import_publications
from spicy_regs.source_catalog.storage import Inputs, connect, schema
from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource
from spicy_regs.sources import publication as pub
from spicy_regs.sources.retained import RetainedLegislatorsAcquirer
from tests.generation_fakes import Store
from tests.test_generation_publication import build
from tests.test_source_evidence import NOW, capture, journal, roster


def selected(rows):
    return {"format": "spicy-selected-inputs", "version": 1, "scope": {"id": "fixture-rosters", "finite": True},
            "complete": True, "used": deepcopy(rows), "available": deepcopy(rows), "expected": deepcopy(rows)}


def source_row(body=b"original", key="roster-current"):
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    return {"source_id": key, "source_version": digest, "version_kind": "sha256", "asset_kind": "json-body",
            "sha256": digest, "bytes": len(body), "location": "https://source.test/" + key}


@pytest.mark.parametrize("fault", ["duplicate", "conflict", "version", "unbounded", "bool-size", "no-version"])
def test_selection_refuses_ambiguous_identity_or_completeness(fault):
    value = deepcopy(selected([source_row()]))
    if fault == "duplicate":
        value["used"].append(dict(value["used"][0]))
    elif fault == "conflict":
        value["available"][0]["bytes"] += 1
    elif fault == "version":
        value["used"][0]["source_version"] = "sha256:" + "0" * 64
    elif fault == "unbounded":
        value["scope"]["finite"] = False
    elif fault == "bool-size":
        value["used"][0]["bytes"] = True
    else:
        value["used"][0].pop("source_version")
    with pytest.raises(ValueError):
        validate_selected_inputs(value)


def test_capture_is_not_automatically_used_and_unbacked_selection_refuses(tmp_path):
    evidence = CaptureEvidence(tmp_path, "test", archive_root=tmp_path / "central-raw")
    evidence.capture(capture(b"original"), stage="fetched")
    evidence.selected_inputs(selected([source_row(b"not captured")]))
    directory, _ = build(tmp_path)
    # Bind the existing generation builder to the actual admitted source input.
    from spicy_regs.generations import build_generation
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = tmp_path / "output.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    artifact = build_generation(tmp_path / "selected", family="test", files=[path],
                                expected_keys=[path.name], inputs=evidence.inputs())
    with pytest.raises(ValueError, match="lacks an admitted capture"):
        input_proof(artifact, LocalMemberSource(tmp_path / "selected"),
                    ((EvidenceMemberSource(evidence.artifact_dir), evidence.artifact),))
    plain = verify_generation(directory)
    receipt, _ = input_proof(plain, LocalMemberSource(directory))
    generation = receipt["generations"][0]
    assert generation["complete"] is False and generation["sources"] == []
    assert generation["selected_inputs"] is None


def test_observed_original_stays_separate_from_used_set(tmp_path):
    from spicy_regs.generations import build_generation
    import pyarrow as pa
    import pyarrow.parquet as pq

    evidence = CaptureEvidence(tmp_path, "test", archive_root=tmp_path / "central-raw")
    evidence.capture(capture(b"original"), stage="fetched")
    path = tmp_path / "output.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    directory = tmp_path / "generation"
    artifact = build_generation(directory, family="test", files=[path], expected_keys=[path.name],
                                inputs=evidence.inputs())
    receipt, _ = input_proof(artifact, LocalMemberSource(directory),
                            ((EvidenceMemberSource(evidence.artifact_dir), evidence.artifact),))
    generation = receipt["generations"][0]
    assert generation["selected_inputs"] is None
    assert generation["sources"][0]["provenance"] == "capture-observation"
    assert generation["sources"][0]["source_version"] == source_row()["sha256"]
    assert generation["complete"] is False


@pytest.mark.parametrize("fault", [None, "location", "source_id", "source_version"])
def test_selected_native_identity_location_and_version_must_match_capture(tmp_path, fault):
    from spicy_regs.generations import build_generation
    import pyarrow as pa
    import pyarrow.parquet as pq

    evidence = CaptureEvidence(tmp_path, "test", archive_root=tmp_path / "central-raw")
    original = capture(b"original")
    evidence.capture(original, stage="used", native_id="native-record-1", source_version="v1", version_kind="etag")
    row = {**source_row(), "source_id": "native-record-1", "source_version": "v1", "version_kind": "etag",
           "location": original.resolved_url}
    if fault:
        row[fault] = "invented-value"
    evidence.selected_inputs(selected([row]))
    path = tmp_path / "table.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    artifact = build_generation(tmp_path / "generation", family="test", files=[path],
                                expected_keys=[path.name], inputs=evidence.inputs())
    arguments = (artifact, LocalMemberSource(tmp_path / "generation"),
                 ((EvidenceMemberSource(evidence.artifact_dir), evidence.artifact),))
    if fault:
        with pytest.raises(ValueError, match="identity, location and version"):
            input_proof(*arguments)
    else:
        receipt, _ = input_proof(*arguments)
        assert receipt["generations"][0]["sources"][0]["source_version"] == "v1"


def test_selected_inventory_pin_is_checked_without_retaining_derived_parquet(tmp_path):
    from spicy_regs.source_evidence import SourceEvidenceError
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "inventory.parquet"
    pq.write_table(pa.table({"key": ["native-key"], "etag": ["version-one"]}), path)
    inventory = {"role": "expected", "format": "parquet", "path": path.name,
        "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size,
        "columns": {"source_id": "key", "source_version": "etag"}, "version_kind": "etag",
        "asset_kind": "json-body", "complete": True}
    value = selected([])
    value["coverage_inventories"] = [inventory]
    invalid = deepcopy(value)
    invalid["complete"] = False
    invalid["coverage_inventories"][0]["scope"] = {"id": "unbounded", "finite": False}
    with pytest.raises(ValueError, match="own named finite scope"):
        validate_selected_inputs(invalid)
    evidence = CaptureEvidence(tmp_path, "test", archive_root=tmp_path / "central-raw")
    evidence.selected_inputs(value, base=tmp_path)
    retained = journal(evidence)[-1]["manifest"]["coverage_inventories"][0]
    assert retained["path"] == "blobs/sha256/" + inventory["sha256"].removeprefix("sha256:")
    assert not list((tmp_path / "central-raw/objects").rglob("*.parquet"))
    from spicy_regs.generations import build_generation
    output = tmp_path / "table.parquet"
    pq.write_table(pa.table({"id": [1]}), output)
    artifact = build_generation(tmp_path / "generation", family="test", files=[output],
                                expected_keys=[output.name], inputs=evidence.inputs())
    path.unlink()  # the acquired processing inventory is durable independently
    receipt = save_input_proof(tmp_path / "proof", artifact, LocalMemberSource(tmp_path / "generation"),
                              ((EvidenceMemberSource(evidence.artifact_dir), evidence.artifact),))
    item = json.loads(receipt.read_bytes())["generations"][0]["selected_inputs"]["coverage_inventories"][0]
    assert pq.read_table(receipt.parent / item["path"]).to_pylist() == [{"key": "native-key", "etag": "version-one"}]
    assert json.loads(receipt.read_bytes())["generations"][0]["sources"] == []
    store = Store()
    pub.publish_generation(tmp_path / "generation", client=store, bucket="test", prior_index=pub.empty_index(),
                           evidence_directories=(evidence.artifact_dir,))
    prefix = f"generation-inputs/test/{artifact.pin.artifact_digest.removeprefix('sha256:')}/"
    assert store.objects[prefix + item["path"]] == (receipt.parent / item["path"]).read_bytes()
    path.write_bytes(b"changed")
    changed = CaptureEvidence(tmp_path, "test", archive_root=tmp_path / "central-raw")
    with pytest.raises(SourceEvidenceError, match="differs from its declared pin"):
        changed.selected_inputs(value, base=tmp_path)


def test_exact_managed_parent_and_remote_version_remain_distinct(tmp_path):
    from spicy_regs.generations import build_generation
    import pyarrow as pa
    import pyarrow.parquet as pq

    parent, _ = build(tmp_path, keys=("parent.parquet",))
    index = pub.publish_generation(parent, client=Store(), bucket="test", prior_index=pub.empty_index())
    path = tmp_path / "child.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    directory = tmp_path / "child"
    artifact = build_generation(directory, family="child", files=[path], expected_keys=[path.name],
        read_snapshot=index, parents={"parent.parquet": pub.table_pin(index, "parent.parquet"),
        "external.parquet": {"etag": "accepted-etag", "byteSize": 100}})
    receipt, _ = input_proof(artifact, LocalMemberSource(directory))
    [proof] = receipt["generations"]
    assert proof["parents"] == [{"table": "parent", "object_key": "parent.parquet", "family": "test",
        "artifact_digest": index["families"]["test"]["artifactDigest"],
        "sha256": pub.table_pin(index, "parent.parquet")["sha256"],
        "bytes": pub.table_pin(index, "parent.parquet")["byteSize"]}]
    assert proof["other_parents"] == [{"table": "external.parquet", "etag": "accepted-etag", "byteSize": 100,
        "identity_basis": "storage-etag-and-size", "fresh_cryptographic_read": False}]
    assert proof["sources"] == [] and proof["complete"] is False


def test_real_members_builder_saves_and_publishes_selected_proof(tmp_path, monkeypatch):
    """Exercise the maintained source adapter, builder, receipt writer and publisher."""
    from spicy_regs.sources import r2, cloudflare

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "central-raw"))
    monkeypatch.setenv("R2_PUBLIC_URL", "https://source.test")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fixture")
    monkeypatch.setattr(pub, "load_index", lambda _: pub.empty_index())
    monkeypatch.setattr(cloudflare, "purge_urls", lambda _: None)
    store = Store()
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    current = roster()
    historical = roster(lis=False).replace(b"A000001", b"B000001")

    def respond(request):
        body = current if "current" in request.url.path else historical
        return httpx.Response(200, stream=httpx.ByteStream(body), headers={"content-type": "application/json"})

    class SelectedMembers(MembersRollup):
        def build(self, directory):
            with RetainedLegislatorsAcquirer(evidence=self.source_evidence,
                    budget=LegislatorsBudget(4, 1024 * 1024, 10.0, 0.0), clock=lambda: NOW,
                    transport=httpx.MockTransport(respond)) as owner:
                outputs = self.rebuild_complete_rosters(directory, acquirer=owner)
            return outputs

    pipeline = SelectedMembers(output_dir=tmp_path / "output", skip_upload=False)
    pipeline.run()
    [directory] = list((tmp_path / "output/generations").iterdir())
    artifact = verify_generation(directory)
    prefix = f"generation-inputs/members/{artifact.pin.artifact_digest.removeprefix('sha256:')}"
    saved = tmp_path / "output" / prefix / "inputs.json"
    assert saved.read_bytes() == store.objects[prefix + "/inputs.json"]
    proof = json.loads(saved.read_bytes())
    generation = proof["generations"][0]
    assert generation["artifact_digest"] == artifact.pin.artifact_digest
    assert generation["complete"] is False  # generic transitive closure remains conservative
    assert len(generation["sources"]) == len(generation["selected_inputs"]["used"]) == 2
    assert len(generation["selected_inputs"]["expected"]) == 2
    assert generation["selected_inputs"]["scope"] == {
        "id": "unitedstates-legislators:current-and-historical", "finite": True}
    assert all(row["provenance"] == "build-consumption" for row in generation["sources"])
    assert {row["sha256"] for row in generation["sources"]} == {
        source_row(current)["sha256"], source_row(historical)["sha256"]}
    assert any(row["event"] == "selected-inputs" for row in journal(pipeline.source_evidence))
    assert store.writes.index(prefix + "/inputs.json") < store.writes.index(pub.INDEX_V2_KEY)
    for pin in generation["evidence_inputs"]:
        raw = (saved.parent / pin["path"]).read_bytes()
        assert len(raw) == pin["bytes"] and "sha256:" + hashlib.sha256(raw).hexdigest() == pin["sha256"]
    # The actual catalog consumer can read the automatic proof unchanged.
    inputs = Inputs(tmp_path)
    index = tmp_path / "index.sqlite"
    public = tmp_path / "publication.json"
    public.write_bytes(store.objects[pub.INDEX_V2_KEY])
    def pin(path):
        return {"path": str(path), "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
                "bytes": path.stat().st_size}
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"format": "spicy-publication-reconciliation", "version": 1,
        "publication": pin(public), "receipts": [pin(saved)]}))
    with connect(index) as db:
        schema(db)
        report = import_publications(db, inputs, selection)
    assert report["currentTables"] == len(artifact.root["spec"]["tables"])
    assert set(report["statuses"]) == {"missing_provenance"}


@pytest.mark.parametrize("corrupt", [False, True])
def test_failed_or_corrupted_proof_cannot_advance_pointer(tmp_path, corrupt):
    directory, _ = build(tmp_path)
    store = Store()
    if corrupt:
        store.corrupt_key = "inputs.json"
    else:
        def fail(key):
            if key.startswith("generation-inputs/"):
                raise OSError("proof write failed")
        store.before_put = fail
    with pytest.raises((OSError, pub.PublicationError)):
        pub.publish_generation(directory, client=store, bucket="test", prior_index=pub.empty_index())
    assert pub.INDEX_V2_KEY not in store.objects
