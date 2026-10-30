"""Publication-year population, retained body failures and the ordinary SQL consumer."""

from copy import deepcopy
from datetime import date
import hashlib
import json
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_docs.sources.federal_register.body_acquisition import FederalRegisterBodyAcquirer, FederalRegisterBodyBudget
from spicy_docs.sources.federal_register.body_captures import capture_body, load_body_capture
from spicy_docs.sources.federal_register.native import federal_register_source_record_id
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.pipelines import executive_orders as pipeline
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.transforms import build_executive_orders as module
from tests.generation_fakes import Store


class Reader:
    page_captures: list[dict[str, Any]]

    def __init__(self, rows):
        self.body = json.dumps(rows).encode()
        self.ref = "sha256:" + hashlib.sha256(self.body).hexdigest()
        self.pin = ArtifactPin("urn:test:executive-orders", self.ref)
        self.records: list[dict[str, Any]] = [
            {"sourceRecordId": federal_register_source_record_id(row), "record": row} for row in rows
        ]
        self.collection_outcome: dict[str, Any] = {
            "requestedScope": {
                "publishedFrom": "2025-01-01",
                "publishedThrough": "2025-12-31",
                "presidentialDocumentType": "executive_order",
            },
            "failedRecordCount": 0,
            "discardedObservationCount": 0,
            "publishedRecordCount": len(rows),
            "discoveredRecordCount": len(rows),
        }

    def iter_records(self):
        yield from deepcopy(self.records)

    def iter_record_evidence(self):
        for index, record in enumerate(self.records):
            yield {
                "sourceRecordId": record["sourceRecordId"],
                "evidenceBlobRef": self.ref,
                "jsonPointer": f"/results/{index}",
            }

    def read_evidence(self, ref, *, max_bytes):
        assert ref == self.ref and len(self.body) <= max_bytes
        return self.body


def record(number="2025-00001", eo="14133"):
    return {
        "document_number": number,
        "publication_date": "2025-01-03",
        "signing_date": "2024-12-30",
        "type": "Presidential Document",
        "subtype": "Executive Order",
        "executive_order_number": eo,
        "title": "Example order",
        "president": {"identifier": "example", "name": "Example President"},
        "disposition_notes": "Amends prior order.\r\nPublisher typo stays.",
        "executive_order_notes": "",
        "pdf_url": "https://www.govinfo.gov/content/pkg/FR-2025-01-03/pdf/" + number + ".pdf",
    }


def captured(tmp_path, reader, *, fail_last=False):
    paths = {}
    for index, item in enumerate(reader.records):
        row = item["record"]
        marker = "wrong-document" if fail_last and index == len(reader.records) - 1 else row["document_number"]
        body = f"<PRESDOCU><P>Clean energy coordination.</P><FRDOC>[FR Doc. {marker} Filed 1-2-25; 8:45 am]</FRDOC></PRESDOCU>".encode()
        path = tmp_path / row["document_number"] / "result.json"
        with FederalRegisterBodyAcquirer(
            budget=FederalRegisterBodyBudget(2, 4096, 4096, 1, 0),
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers={"Content-Type": "application/xml"}, stream=httpx.ByteStream(body)
                )
            ),
        ) as client:
            capture_body(
                path,
                record=row,
                source_pin={"logicalId": reader.pin.logical_id, "artifactDigest": reader.pin.artifact_digest},
                acquirer=client,
            )
        paths[item["sourceRecordId"]] = path
    return paths


def test_ordinary_publication_lookup_text_history_and_failed_original(tmp_path, monkeypatch):
    second = record("2025-00002", "14134")
    del second["president"]
    second["disposition_notes"] = None
    reader = Reader([record(), second])
    paths = captured(tmp_path / "bodies", reader, fail_last=True)
    output = tmp_path / "build"
    artifact, evidence = pipeline.build(reader, output, year=2025, body_paths=paths)
    index = publication.publish_generation(
        output / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["executive-orders"]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        assert connection.execute(
            f"SELECT executive_order_number, signing_date, president_name, disposition_notes FROM {module.TABLE} WHERE executive_order_number='14133' AND body_text ILIKE '%energy%'"
        ).fetchall() == [
            ("14133", date(2024, 12, 30), "Example President", reader.records[0]["record"]["disposition_notes"])
        ]
        assert connection.execute(
            f"SELECT executive_order_number, body_status, body_text FROM {module.TABLE} WHERE body_status='source_failed'"
        ).fetchall() == [("14134", "source_failed", None)]
    rows = pq.read_table(output / "generation" / module.OUTPUT).to_pylist()
    assert rows[0]["source_release_digest"] == reader.pin.artifact_digest
    assert json.loads(rows[1]["metadata_field_states_json"]) == {
        "president": "missing",
        "disposition_notes": "null",
        "executive_order_notes": "empty",
    }
    assert json.loads(rows[1]["raw_metadata_json"]) == second
    assert json.loads(rows[1]["offered_renditions_json"]) == {"pdf_url": second["pdf_url"]}
    refusal = load_body_capture(paths[reader.records[1]["sourceRecordId"]], record=second).refused_response
    assert refusal is not None and refusal.response_bytes is not None
    digest = hashlib.sha256(refusal.response_bytes).hexdigest()
    from spicy_regs.source_evidence import EvidenceMemberSource
    with EvidenceMemberSource(evidence).open("blobs/sha256/" + digest) as original:
        assert original.read() == refusal.response_bytes
    assert module.TABLE in data_dictionary.expected_schemas()


@pytest.mark.parametrize(
    "defect",
    [
        "partial_scope",
        "failed_metadata",
        "late_count",
        "missing_body",
        "extra_body",
        "wrong_type",
        "wrong_year",
        "wrong_locator",
    ],
)
def test_refusal_preserves_previous_table(tmp_path, monkeypatch, defect):
    reader = Reader([record()])
    paths = captured(tmp_path / "bodies", reader)
    output = tmp_path / "build"
    output.mkdir()
    evidence = CaptureEvidence(output, "executive-orders")
    path = module.build_executive_orders(
        reader, output, year=2025, body_paths=paths, load_body=load_body_capture, evidence=evidence
    )
    before = path.read_bytes()
    if defect == "partial_scope":
        reader.collection_outcome["requestedScope"]["publishedThrough"] = "2025-12-30"
    elif defect == "failed_metadata":
        reader.collection_outcome["failedRecordCount"] = 1
    elif defect == "late_count":
        reader.collection_outcome["publishedRecordCount"] = reader.collection_outcome["discoveredRecordCount"] = 2
    elif defect == "missing_body":
        paths.clear()
    elif defect == "extra_body":
        paths["other"] = next(iter(paths.values()))
    elif defect == "wrong_type":
        reader.records[0]["record"]["subtype"] = "Proclamation"
    elif defect == "wrong_year":
        reader.records[0]["record"]["publication_date"] = "2024-01-03"
    else:
        original = list(reader.iter_record_evidence())
        original[0]["sourceRecordId"] = "wrong"
        monkeypatch.setattr(reader, "iter_record_evidence", lambda: iter(original))
    with pytest.raises(ValueError):
        module.build_executive_orders(
            reader, output, year=2025, body_paths=paths, load_body=load_body_capture, evidence=evidence
        )
    assert path.read_bytes() == before
    evidence.finish()
