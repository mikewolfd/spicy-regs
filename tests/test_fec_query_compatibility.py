"""The narrow legacy bridge refuses changed data, lineage and serving claims."""

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin, stamp_root

from spicy_regs import fec_query_compatibility as bridge
from spicy_regs.fec_release_verification import digest
from spicy_regs.generations import _table_footer_info
from tests.test_fec_serving_decisions import population, NEW


@pytest.fixture
def prepared(tmp_path):
    scope = json.loads(Path(bridge.__file__).with_name("fec_query_scope.json").read_text())
    old_digest = scope["source_generation_pin"]
    old_pin = dict(logicalId="urn:spicy:artifact:spicy-regs-rollup-generation:" + "a" * 64, artifactDigest=old_digest)
    old: dict[str, Any] = dict(
        old_pin,
        prefix="generations/fec-observations/" + old_digest[7:],
        tables={
            t + ".parquet": dict(
                sha256="sha256:" + "a" * 64, byteSize=100, rows=1, columns=[[k, "VARCHAR"] for k in fields]
            )
            for t, fields in {**bridge.PRESERVED, "fec_relationships": ["record_id"]}.items()
        },
    )
    snapshot: dict[str, Any] = dict(format="spicy-regs-publication", version=2, families={"fec-observations": old})
    proof = tmp_path / "observations.json"
    proof.write_text('{"fixture":"already-verified-observations"}')
    observations = SimpleNamespace(
        root={"spec": {"readSnapshot": snapshot}},
        pin=ArtifactPin("urn:spicy:artifact:spicy-regs-rollup-generation:" + "b" * 64, NEW),
        proof={"priorPin": old_pin},
        _bindings=[{"path": str(proof)}],
        check=lambda: None,
    )
    filings, headers = population()
    for row in headers:
        row["source_generation_pin"] = old_digest
    rows = dict(
        fec_filings=filings,
        fec_filing_header_associations=headers,
        fec_filing_definitions=[{"record_id": "definition"}],
        fec_filing_definition_evidence=[{"record_id": "evidence", "witness_generation_pin": old_digest}],
        fec_collection_selection=[{"record_id": "selection", "source_generation_pin": old_digest}],
    )
    inputs, tables, members = {}, {}, []
    for table, values in rows.items():
        path = tmp_path / (table + ".parquet")
        pq.write_table(pa.Table.from_pylist(values), path)
        inputs[table] = path
        tables[path.name] = _table_footer_info(path)
        members.append(
            dict(
                objectKey=path.name,
                role="table",
                mediaType="application/vnd.apache.parquet",
                byteSize=path.stat().st_size,
                recordCount=len(values),
                sha256=digest(path.read_bytes()),
            )
        )
    manifest = tmp_path / "members.json"
    manifest.write_text(json.dumps({"members": members}))
    counts = dict(
        memberCount=len(members),
        totalMemberByteSize=sum(m["byteSize"] for m in members),
        totalRecordCount=sum(m["recordCount"] for m in members),
    )
    parents = {
        key: dict(family="fec-observations", artifactDigest=old_digest, sha256=v["sha256"], byteSize=v["byteSize"])
        for key, v in old["tables"].items()
    }
    root = stamp_root(
        dict(
            format="spicy-artifact",
            formatVersion="1.0",
            kind="spicy-regs-rollup-generation",
            producer=dict(
                implementationId="urn:test:implementation:sha256:" + "f" * 64,
                product="test",
                verifierId="urn:test:verifier",
                verifierImplementationId="urn:test:implementation:sha256:" + "f" * 64,
                verifierVersion="1",
            ),
            inputs=[],
            counts=dict(manifestCount=1, **counts),
            memberManifests=[
                dict(
                    objectKey="members.json",
                    manifestId="global:fec-query",
                    scopeKind="global",
                    scopeId="fec-query",
                    byteSize=manifest.stat().st_size,
                    sha256=digest(manifest.read_bytes()),
                    **counts,
                )
            ],
            spec=dict(
                family="fec-query",
                publicationStatus="complete-family",
                tables=tables,
                readSnapshot=deepcopy(snapshot),
                parents=parents,
            ),
        )
    )
    entry = dict(
        bridge._pin(root),
        prefix="generations/fec-query/" + root["artifactDigest"][7:],
        publishedAt="2026-10-03T21:51:37Z",
        tables={
            m["objectKey"]: dict(tables[m["objectKey"]], sha256=m["sha256"], byteSize=m["byteSize"]) for m in members
        },
    )
    snapshot["families"]["fec-query"] = entry
    controls = {}
    for name, value in [("publication", snapshot), ("artifact", root)]:
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(value))
        controls[name] = path
    controls["members"] = manifest
    definitions = {}
    for i, (namespace, pin) in enumerate(scope["namespace_evidence"].items()):
        path = tmp_path / f"definition-{i}.json"
        path.write_text(
            json.dumps(
                dict(
                    source_namespace=namespace,
                    native_field="FILE_NUM",
                    observedDefinitionLabel="FILE_NUM",
                    context_sha256=pin,
                    cells=[{"text": "Unique report id"}],
                )
            )
        )
        definitions[namespace] = path
    bridge.prepare_retained_query_compatibility(
        observations, entry, controls=controls, inputs=inputs, definitions=definitions, output=tmp_path / "out"
    )
    report = tmp_path / "out/compatibility.json"
    return observations, entry, report


def admitted(prepared):
    observations, entry, path = prepared
    return bridge.verify_retained_query_compatibility(
        observations, entry, dict(path=str(path), sha256=digest(path.read_bytes()))
    )


def test_checked_result_retains_exact_entry_and_detects_mutation(prepared):
    result = admitted(prepared)
    assert result.entry == prepared[1]
    assert result.observations_pin == prepared[0].pin
    assert digest(result.descriptor_path.read_bytes()) == result.descriptor_sha256
    result.check()
    result.entry["publishedAt"] = "2026-10-04T00:00:00Z"
    with pytest.raises(ValueError, match="changed"):
        result.check()


@pytest.mark.parametrize("change", ["query", "observations", "parent", "lookup", "program", "code"])
def test_wrong_pair_lineage_payload_or_semantics_refuses(prepared, change):
    obs, entry, path = prepared
    report = json.loads(path.read_text())
    if change == "query":
        entry["publishedAt"] = "2026-10-04T00:00:00Z"
    elif change == "observations":
        obs.pin = ArtifactPin(obs.pin.logical_id, "sha256:" + "c" * 64)
    elif change == "parent":
        p = Path(report["controls"]["artifact"]["path"])
        root = json.loads(p.read_text())
        root["spec"]["parents"]["fec_collections.parquet"]["sha256"] = "sha256:" + "c" * 64
        p.write_text(json.dumps(root))
        report["controls"]["artifact"] = bridge._bound(p)
    elif change == "lookup":
        p = Path(report["inputs"]["fec_filings"]["path"])
        p.write_bytes(b"changed")
        report["inputs"]["fec_filings"] = bridge._bound(p)
    elif change == "program":
        p = Path(report["serving"]["path"])
        data = json.loads(p.read_text())
        data["decisionProgram"]["outputs"]["filing_key"] = ["literal", "fabricated"]
        p.write_text(json.dumps(data))
        report["serving"] = bridge._bound(p)
    else:
        report["implementation"]["fec_serving_decisions.py"] = "sha256:" + "c" * 64
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError):
        admitted(prepared)


def test_changed_descriptor_refuses_before_publication(prepared):
    result = admitted(prepared)
    result.descriptor_path.write_text("{}")
    with pytest.raises(ValueError, match="changed"):
        result.check()
