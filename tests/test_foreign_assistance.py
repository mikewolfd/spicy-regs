"""Summary comparisons, explicit revision semantics, and ordinary local serving.

The small generated captures below are planted controls, not evidence about a
publisher cohort. The lane's retained-source run qualifies the genuine cohort.
"""

import asyncio
import csv
import hashlib
import io
import json
from datetime import date
from decimal import Decimal

import httpx
import pyarrow.parquet as pq
import pytest
from mcp.types import CallToolResult

from spicy_regs import mcp_server
from spicy_regs.pipelines import foreign_assistance as pipeline
from spicy_regs.pipelines.raw_releases import open_raw, snapshot_readers
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.transforms import build_foreign_assistance as module
from tests.generation_fakes import Store


def captures(tmp, rows, monkeypatch, *, observed="2026-11-01T00:00:00Z"):
    from spicy_docs.sources.foreign_assistance import COLUMNS, DICTIONARY_URL, SUMMARY_URL
    from spicy_docs.sources.foreign_assistance_profile import FOREIGN_ASSISTANCE_PROFILE, iter_retained_pages

    target = io.StringIO(newline="")
    writer = csv.writer(target)
    writer.writerow(COLUMNS)
    writer.writerows(rows)
    csv_body, pdf_body = target.getvalue().encode(), b"%PDF-1.4\nsynthetic-control\n%%EOF\n"
    monkeypatch.setattr(module, "REVIEWED_DICTIONARY", "sha256:" + hashlib.sha256(pdf_body).hexdigest())
    raw_root = tmp / "raw"
    evidence = CaptureEvidence(tmp / "capture", pipeline.FAMILY, archive_root=raw_root)
    scope = {}
    for key, url, body in (("capture", SUMMARY_URL, csv_body), ("dictionary_capture", DICTIONARY_URL, pdf_body)):
        receipt = evidence.retain_bytes(body, stage="test-original", raw_source=True, source_url=url)
        scope[key] = {"request_url": url, "response_sha256": receipt["sha256"],
                      "observed_at": observed, "byte_size": len(body)}
    evidence.finish()
    readers, archives = snapshot_readers(
        FOREIGN_ASSISTANCE_PROFILE, iter_retained_pages, [scope], tmp / "releases", store=open_raw(raw_root),
    )
    return readers, archives, raw_root


def row(code="UKR", name="Ukraine", year="2024", measure_id="2", measure="Obligations", current="100", constant="125"):
    return [code, name, "2", "Europe and Eurasia", "NULL", "NULL", measure_id, measure, year, current, constant]


def serve(monkeypatch, directory):
    monkeypatch.setattr(mcp_server, "DATA_DIR", directory)
    return mcp_server._build_connection()


def test_real_release_generation_and_country_regional_query(tmp_path, monkeypatch):
    readers, archives, raw_root = captures(tmp_path / "inputs", [
        row(current="24816840516", constant="25470645195"),
        row(measure_id="3", measure="Disbursements", current="10162100658"),
        row("SSN", "Sub-Saharan Africa Region", current="1505594297"),
        row("WLD", "World", current="14966263213"), row("CS-KM", "Kosovo", "1976tq", current="-1000"),
        row(year="2027", current="", constant="NULL"),
    ], monkeypatch)
    output = tmp_path / "build"
    artifact, evidence = pipeline.build(readers, output, archives=archives, archive_root=raw_root)
    assert artifact.pin.artifact_digest.startswith("sha256:")
    assert (evidence / "journal.jsonl").exists()
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw_root))
    index = publication.publish_generation(
        output / "generation", client=Store(), bucket="fixture", prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"][pipeline.FAMILY]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: connection)
    with serve(monkeypatch, output / "generation") as connection:
        actual = connection.execute("""
            SELECT recipient_name, measure, amount, dollar_basis, reporting_status
            FROM foreign_assistance_latest WHERE recipient_code='UKR' AND fiscal_period='2024'
            AND dollar_basis='current' ORDER BY measure_id
        """).fetchall()
        assert actual == [
            ("Ukraine", "Obligations", Decimal("24816840516"), "current", "completeness_not_asserted"),
            ("Ukraine", "Disbursements", Decimal("10162100658"), "current", "completeness_not_asserted"),
        ]
        assert connection.execute("SELECT DISTINCT recipient_scope FROM foreign_assistance_latest "
                                  "WHERE recipient_code='SSN'").fetchall() == [("regional",)]
        assert connection.execute("SELECT DISTINCT period_start, period_end FROM foreign_assistance_latest "
                                  "WHERE fiscal_period='1976tq'").fetchall() == [(date(1976, 7, 1), date(1976, 9, 30))]
        assert connection.execute("SELECT dollar_basis, amount_status, reporting_status FROM foreign_assistance_latest "
                                  "WHERE fiscal_period='2027' ORDER BY dollar_basis").fetchall() == [
            ("constant", "publisher_null", "open_fiscal_period"), ("current", "blank", "open_fiscal_period"),
        ]
        server = mcp_server.build_server()
        reply = asyncio.run(server.call_tool("query_sql", {
            "sql": "SELECT amount FROM foreign_assistance_latest WHERE recipient_code='UKR' "
                   "AND fiscal_period='2024' AND measure_id='2' AND dollar_basis='current'",
        }))
        assert isinstance(reply, CallToolResult)
        result = reply.structured_content
        assert result is not None
        assert result["rows"] == [["24816840516.000000"]]


def test_revisions_empty_and_absence_never_mean_deletion_or_zero(tmp_path, monkeypatch):
    a, _, _ = captures(tmp_path / "a", [row(), row("SSN", "Sub-Saharan Africa Region")], monkeypatch,
                       observed="2026-10-01T00:00:00Z")
    b, _, _ = captures(tmp_path / "b", [row(current="120", constant="150")], monkeypatch,
                       observed="2026-11-01T00:00:00Z")
    c, _, _ = captures(tmp_path / "c", [], monkeypatch, observed="2026-11-02T00:00:00Z")
    paths = module.build_foreign_assistance([*a, *b, *c], tmp_path / "tables")
    with serve(monkeypatch, paths[0].parent) as connection:
        assert connection.execute("SELECT count(*) FROM foreign_assistance_latest").fetchone()[0] == 0
        actual = connection.execute("SELECT dollar_basis, observation_change, current_usd_change "
                                    "FROM foreign_assistance_revisions WHERE recipient_code='UKR' "
                                    "AND observed_at='2026-11-01T00:00:00Z' ORDER BY dollar_basis").fetchall()
        assert actual == [("constant", "changed", None), ("current", "changed", Decimal("20"))]
        assert connection.execute("SELECT DISTINCT observation_change FROM foreign_assistance_revisions "
                                  "WHERE recipient_code='SSN'").fetchall() == [("now_unobserved",)]
    assert pq.read_table(paths[1]).column("source_row_count").to_pylist() == [2, 1, 0]


@pytest.mark.parametrize("raw,expected,status", [
    ("0", Decimal("0"), "numeric"), ("-10.25", Decimal("-10.25"), "numeric"),
    ("", None, "blank"), ("NULL", None, "publisher_null"), ("#ERROR", None, "unparsed"),
    ("1.0000001", None, "unrepresentable"), ("1e6", None, "unparsed"),
])
def test_exact_amounts_and_missing_states(raw, expected, status):
    assert module.amount_cell(raw) == (expected, status)


def test_incomplete_unknown_dictionary_duplicate_and_equal_clock_refuse(tmp_path, monkeypatch):
    readers, _, _ = captures(tmp_path / "a", [row()], monkeypatch)
    module.build_foreign_assistance(readers, tmp_path / "valid")
    before = (tmp_path / "valid/foreign_assistance_observations.parquet").read_bytes()
    with pytest.raises(ValueError, match="twice"):
        module.build_foreign_assistance([*readers, *readers], tmp_path / "valid")
    assert (tmp_path / "valid/foreign_assistance_observations.parquet").read_bytes() == before
    class Incomplete:
        collection_outcome = {**readers[0].collection_outcome, "failedRecordCount": 1}

    with pytest.raises(ValueError, match="complete accepted"):
        module.build_foreign_assistance([Incomplete()], tmp_path / "incomplete")
    monkeypatch.setattr(module, "REVIEWED_DICTIONARY", "sha256:" + "f" * 64)
    with pytest.raises(ValueError, match="dictionary requires"):
        module.build_foreign_assistance(readers, tmp_path / "unknown")
    other, _, _ = captures(tmp_path / "b", [row(current="200")], monkeypatch)
    with pytest.raises(ValueError, match="ambiguous"):
        module.build_foreign_assistance([*readers, *other], tmp_path / "ambiguous")


def test_source_access_refusal_is_retained_without_a_successful_selection(tmp_path, monkeypatch):
    from spicy_docs.transport.credentials import CredentialRefusedError

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    requested = []

    def refused(request):
        requested.append(str(request.url))
        return httpx.Response(403, content=b"Access denied")

    with pytest.raises(CredentialRefusedError):
        pipeline.acquire(tmp_path, transport=httpx.MockTransport(refused))
    assert len(requested) == 1
    events = [json.loads(line) for path in tmp_path.glob("source-evidence/*/artifact/journal.jsonl")
              for line in path.read_text().splitlines()]
    assert any(event["event"] == "refusal" for event in events)
    assert not any(event["event"] == "capture" and event.get("status_code") == 200 for event in events)
