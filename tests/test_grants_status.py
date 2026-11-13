"""Source acquisition, explicit status history, valid empty, failure receipts and ordinary SQL."""

import hashlib
import json
import os

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.grants_gov_status import StatusSelection
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.pipelines import grants_status as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_grants_status import QUERY_TABLE, STATUS_TABLE
from tests.generation_fakes import Store
from tests.published_paths import local_paths
from tests.test_grants_opportunities import Reader, synopsis


def hit(native="00123", status="posted", **extra):
    return {"id": native, "number": "EXAMPLE", "agencyCode": "EX", "oppStatus": status,
            "docType": "forecast", "openDate": "", "closeDate": "rolling", **extra}


def transport(rows, *, fail_second=False):
    def serve(request):
        body = json.loads(request.content)
        offset, size = body["startRecordNum"], body["rows"]
        if offset and fail_second:
            value = {"errorcode": 9, "msg": "Failed page", "data": {}}
        else:
            value = {"errorcode": 0, "data": {"searchParams": body, "hitCount": len(rows),
                                              "startRecord": offset, "oppHits": rows[offset:offset + size],
                                              "errorMsgs": []}}
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(value).encode()),
                              headers={"Content-Type": "application/json"})
    return httpx.MockTransport(serve)


def acquire(tmp, rows, **kwargs):
    return pipeline.capture(StatusSelection("EXAMPLE", agencies="EX", eligibilities="25", page_size=2), tmp,
                            transport=transport(rows, **kwargs))["releases"][0]


def test_native_release_generation_history_empty_and_ordinary_query(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_grants_opportunities import build_grants_opportunities

    missing = hit("003")
    del missing["oppStatus"]
    first = acquire(tmp_path / "old", [hit(), hit("002", ""), missing, hit("004", None), hit("005", "Future State")])
    later = acquire(tmp_path / "later", [hit(status="closed")])
    empty = acquire(tmp_path / "empty", [])
    output = tmp_path / "build"
    artifact, evidence = pipeline.build([first, later, empty], output)
    rows = pq.read_table(output / "generation" / (STATUS_TABLE + ".parquet")).to_pylist()
    assert len(rows) == 6
    assert {row["status_value_state"] for row in rows} == {"value", "blank", "missing", "null"}
    assert {row["lifecycle_status"] for row in rows if row["opportunity_id"] == "00123"} == {"posted", "closed"}
    assert all(row["document_type"] == "forecast" and row["close_date_raw"] == "rolling" for row in rows)
    assert all(row["lifecycle_status"] != "withdrawn" for row in rows)
    populations = pq.read_table(output / "generation" / (QUERY_TABLE + ".parquet")).to_pylist()
    assert [(row["declared_count"], row["observed_count"], row["page_count"], row["outcome"]) for row in populations] == [
        (5, 5, 3, "observed-complete"), (1, 1, 1, "observed-complete"), (0, 0, 1, "observed-empty"),
    ]
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture", prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"][pipeline.FAMILY]["artifactDigest"] == artifact.pin.artifact_digest
    assert local_paths(output / "generation", evidence, also=(str(tmp_path),)) == []
    consumer = tmp_path / "consumer"
    consumer.mkdir()
    for path in (output / "generation").glob("*.parquet"):
        os.link(path, consumer / path.name)
    build_grants_opportunities([Reader([synopsis()])], consumer)
    monkeypatch.setattr(mcp_server, "DATA_DIR", consumer)
    with mcp_server._build_connection() as connection:
        result = connection.execute("""
            SELECT g.opportunity_id, s.lifecycle_status, s.document_type, s.close_date_raw,
                   json_extract_string(s.source_locator_json,'$.json_pointer'), q.observed_count
            FROM grants_opportunity_observations g
            JOIN grants_status_observations s USING (opportunity_id)
            JOIN grants_status_queries q ON q.source_release_digest=s.source_release_digest
            WHERE g.agency_code='EX' AND json_contains(g.eligibility_codes_json,'"25"')
              AND g.close_date >= DATE '2026-10-01'
            ORDER BY s.lifecycle_status
        """).fetchall()
        assert connection.execute("SELECT outcome FROM grants_status_queries WHERE declared_count=0").fetchall() == [
            ("observed-empty",),
        ]
    assert result == [("00123", status, "forecast", "rolling", "/data/oppHits/0", count)
                      for status, count in [("closed", 1), ("posted", 5)]]
    metadata = data_dictionary.build_mcp_metadata(data_dictionary.load_descriptions(), data_dictionary.expected_schemas())
    assert metadata[STATUS_TABLE]["columns"] and metadata[QUERY_TABLE]["columns"]


def test_failed_page_retained_and_incomplete_selection_cannot_replace_valid_generation(tmp_path):
    valid = acquire(tmp_path / "good", [hit()])
    out = tmp_path / "valid"
    pipeline.build([valid], out)
    before = hashlib.sha256((out / "generation/artifact.json").read_bytes()).hexdigest()
    failed = tmp_path / "failed-acquisition"
    with pytest.raises(ValueError):
        acquire(failed, [hit(), hit("002"), hit("003")], fail_second=True)
    events = [json.loads(line) for path in failed.rglob("journal.jsonl") for line in path.read_text().splitlines()]
    assert any(event["event"] == "refusal" for event in events)
    assert any(event["event"] == "capture" and event.get("source_validation") == "refused" for event in events)
    assert not (failed / "generation").exists()
    complete = acquire(tmp_path / "complete", [hit(), hit("002"), hit("003")])
    incomplete = dict(complete, captures=complete["captures"][:1])
    with pytest.raises(ValueError):
        pipeline.build([incomplete], tmp_path / "incomplete")
    assert not (tmp_path / "incomplete/generation").exists()
    assert hashlib.sha256((out / "generation/artifact.json").read_bytes()).hexdigest() == before
    with pytest.raises(ValueError, match="fresh output"):
        pipeline.build([complete], out)
