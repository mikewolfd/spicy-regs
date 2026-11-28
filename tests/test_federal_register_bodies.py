"""Hermetic unit tests for ``federal-register-bodies`` (`ss-fmhm`).

Covers declared v1 population & exclusions, XML-preferred capture, GovInfo HTML fallback,
explicit ``refused`` / ``missing_locator`` rows, prior carry-forward across ``max_new``
batches, monotonic row growth, ``CaptureEvidence`` raw blob retention, rollup execution,
workflow/CLI wiring, and ODCS Arrow schema parity.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs import data_dictionary, mcp_server, odcs_contracts as odcs
from spicy_regs.local_publication import LOCAL_FAMILIES
from spicy_regs.pipelines.rollups import federal_register_bodies as rollup_mod
from spicy_regs.pipelines.rollups.base import Unchanged
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication
from spicy_regs.transforms import build_federal_register_bodies as mod
from tests.generation_fakes import Store
from tests.published_paths import local_paths

REPO_ROOT = Path(__file__).resolve().parents[1]

_FR_INPUT_SCHEMA = pa.schema(
    [
        ("document_number", pa.string()),
        ("publication_date", pa.string()),
        ("document_type", pa.string()),
        ("title", pa.string()),
        ("significant", pa.bool_()),
        ("source_sha256", pa.string()),
        ("body_html_url", pa.string()),
    ]
)


def _fr_row(
    document_number: str,
    publication_date: str,
    *,
    document_type: str = "Rule",
    title: str | None = None,
    significant: bool | None = True,
    source_sha256: str | None = None,
    body_html_url: str | None = "AUTO",
) -> dict[str, Any]:
    year, month, day = publication_date.split("-")
    url = (
        f"https://www.federalregister.gov/documents/full_text/html/{year}/{month}/{day}/{document_number}.html"
        if body_html_url == "AUTO"
        else body_html_url
    )
    return {
        "document_number": document_number,
        "publication_date": publication_date,
        "document_type": document_type,
        "title": title if title is not None else f"Significant {document_type} {document_number}",
        "significant": significant,
        "source_sha256": source_sha256,
        "body_html_url": url,
    }


def _write_fr_parquet(path: Path, rows: list[dict[str, Any]]) -> Path:
    pq.write_table(pa.Table.from_pylist(rows, schema=_FR_INPUT_SCHEMA), path)
    return path


def _rule_xml(document_number: str, *, root: str = "RULE", body_sentence: str = "Regulatory text.") -> bytes:
    return (
        f"<{root}>"
        "<PREAMB>"
        "<AGENCY TYPE=\"S\">ENVIRONMENTAL PROTECTION AGENCY</AGENCY>"
        f"<SUBJECT>Rule {document_number}</SUBJECT>"
        f"<SUM><HD SOURCE=\"HED\">SUMMARY:</HD><P>{body_sentence}</P></SUM>"
        "<EFFDATE><HD SOURCE=\"HED\">DATES:</HD><P>Effective January 1, 2027.</P></EFFDATE>"
        "</PREAMB>"
        "<SUPLINF>"
        "<HD SOURCE=\"HED\">SUPPLEMENTARY INFORMATION:</HD>"
        f"<P>Official text for {document_number}.</P>"
        "<REGTEXT TITLE=\"40\" PART=\"60\">"
        "<PART><HD SOURCE=\"HED\">PART 60&#x2014;STANDARDS OF PERFORMANCE</HD>"
        "<SECTION><SECTNO>&#xA7; 60.1</SECTNO><SUBJECT>Applicability.</SUBJECT>"
        "<P>This section applies to affected facilities.</P></SECTION>"
        "</PART>"
        "</REGTEXT>"
        "</SUPLINF>"
        f"<FRDOC>[FR Doc. {document_number} Filed 11-26-26; 8:45 am]</FRDOC>"
        f"</{root}>"
    ).encode("utf-8")


def _govinfo_html(document_number: str, *, sentence: str = "Fallback GovInfo rule text.") -> bytes:
    return (
        "<html><head><title>Federal Register</title></head><body><pre>\n"
        f"[FR Doc No: {document_number}]\n\n"
        f"SUMMARY: {sentence}\n\n"
        "DATES: Comments due December 1, 2026.\n\n"
        f"[FR Doc. {document_number} Filed 11-25-26; 8:45 am]\n"
        "</pre></body></html>"
    ).encode("utf-8")


def test_population_filter_xml_html_fallback_refused_and_missing_locator(tmp_path):
    fr_path = _write_fr_parquet(
        tmp_path / "federal_register.parquet",
        [
            # Included v1 population (5 rows):
            _fr_row("2026-24227", "2026-11-27", document_type="Rule", source_sha256="sha256:" + "1" * 64),
            _fr_row("2026-24100", "2026-11-20", document_type="Proposed Rule", source_sha256="sha256:" + "2" * 64),
            _fr_row("2025-10001", "2025-05-15", document_type="Rule", source_sha256="sha256:" + "3" * 64),
            _fr_row("2024-09001", "2024-06-10", document_type="Rule", source_sha256="sha256:" + "4" * 64),
            _fr_row("2023-08001", "2023-03-01", document_type="Rule", body_html_url=None),
            _fr_row("2022-07001", "2022-02-01", document_type="Rule", source_sha256="sha256:" + "5" * 64),
            # Explicit exclusions (must never appear in v1 population):
            _fr_row("2026-90001", "2026-11-27", document_type="Notice", significant=True),
            _fr_row("2026-90002", "2026-11-27", document_type="Presidential Document", significant=True),
            _fr_row("2026-90003", "2026-11-27", document_type="Rule", significant=False),
            _fr_row("2026-90004", "2026-11-27", document_type="Proposed Rule", significant=None),
            _fr_row("2020-29999", "2020-12-31", document_type="Rule", significant=True),
        ],
    )

    xml_rule = _rule_xml("2026-24227", root="RULE", body_sentence="Clean air standards.")
    xml_prorule = _rule_xml("2026-24100", root="PRORULE", body_sentence="Proposed efficiency rule.")
    html_fallback = _govinfo_html("2025-10001", sentence="Recovered from GovInfo after XML 404.")
    wrong_root_xml = _rule_xml("2022-07001", root="NOTICE", body_sentence="Wrong root element.")
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested_urls.append(url)
        if url.endswith("/2026-24227.xml"):
            return httpx.Response(200, headers={"Content-Type": "application/xml"}, content=xml_rule)
        if url.endswith("/2026-24100.xml"):
            return httpx.Response(200, headers={"Content-Type": "application/xml"}, content=xml_prorule)
        if url.endswith("/2025-10001.xml"):
            return httpx.Response(404, headers={"Content-Type": "text/html"}, content=b"not found")
        if url.endswith("/2025-10001.htm"):
            return httpx.Response(200, headers={"Content-Type": "text/html"}, content=html_fallback)
        if url.endswith("/2024-09001.xml"):
            return httpx.Response(404, headers={"Content-Type": "text/html"}, content=b"no xml")
        if url.endswith("/2024-09001.htm"):
            return httpx.Response(404, headers={"Content-Type": "text/html"}, content=b"no html")
        if url.endswith("/2022-07001.xml"):
            return httpx.Response(200, headers={"Content-Type": "application/xml"}, content=wrong_root_xml)
        raise AssertionError(f"Unexpected URL requested: {url}")

    build_dir = tmp_path / "build"
    evidence = CaptureEvidence(build_dir, mod.FAMILY)
    out = mod.build_federal_register_bodies(
        build_dir,
        fr_path=fr_path,
        min_request_interval_seconds=0.0,
        download_prior=lambda *_: False,
        transport=httpx.MockTransport(handler),
        evidence=evidence,
    )
    evidence.finish()

    table = pq.read_table(out)
    assert table.schema.names == mod.SCHEMA.names
    rows = {row["document_number"]: row for row in table.to_pylist()}
    assert set(rows) == {"2026-24227", "2026-24100", "2025-10001", "2024-09001", "2023-08001", "2022-07001"}

    # XML-preferred Rule & Proposed Rule
    rule_row = rows["2026-24227"]
    assert rule_row["publication_date"] == date(2026, 11, 27)
    assert rule_row["document_type"] == "Rule"
    assert rule_row["significant"] is True
    assert rule_row["source_record_id"] == "2026-24227@2026-11-27"
    assert rule_row["source_sha256"] == "sha256:" + "1" * 64
    assert rule_row["body_status"] == "acquired"
    assert rule_row["body_format"] == "xml"
    assert rule_row["body_url"] == "https://www.federalregister.gov/documents/full_text/xml/2026/11/27/2026-24227.xml"
    assert rule_row["body_sha256"] == "sha256:" + hashlib.sha256(xml_rule).hexdigest()
    assert rule_row["text_sha256"] == "sha256:" + hashlib.sha256(rule_row["body_text"].encode("utf-8")).hexdigest()
    assert rule_row["text_derivation"] == "markup-reader"
    assert rule_row["text_derivation_version"]
    assert rule_row["body_observed_at"]
    assert rule_row["body_failure_reason"] is None
    assert "Clean air standards." in rule_row["body_text"]
    assert "<RULE>" not in rule_row["body_text"]  # derived text only, raw XML stays in R2 blobs

    prorule_row = rows["2026-24100"]
    assert prorule_row["body_status"] == "acquired"
    assert prorule_row["body_format"] == "xml"
    assert "Proposed efficiency rule." in prorule_row["body_text"]

    # GovInfo HTML fallback on XML 404
    html_row = rows["2025-10001"]
    assert html_row["body_status"] == "acquired"
    assert html_row["body_format"] == "html"
    assert html_row["body_url"] == "https://www.govinfo.gov/content/pkg/FR-2025-05-15/html/2025-10001.htm"
    assert html_row["body_sha256"] == "sha256:" + hashlib.sha256(html_fallback).hexdigest()
    assert "Recovered from GovInfo after XML 404." in html_row["body_text"]

    # Explicit refused rows (both 404/404 and mismatched XML root)
    refused_404 = rows["2024-09001"]
    assert refused_404["body_status"] == "refused"
    assert refused_404["body_text"] is None
    assert refused_404["body_sha256"] is None
    assert refused_404["body_failure_reason"]

    refused_root = rows["2022-07001"]
    assert refused_root["body_status"] == "refused"
    assert refused_root["body_text"] is None
    assert "NOTICE" in (refused_root["body_failure_reason"] or "")

    # Explicit missing_locator row (never dropped, zero network requests made for it)
    missing_row = rows["2023-08001"]
    assert missing_row["body_status"] == "missing_locator"
    assert missing_row["body_text"] is None
    assert missing_row["body_failure_reason"]
    assert not any("2023-08001" in u for u in requested_urls)

    # Raw XML/HTML bytes retained in central-raw evidence store (never duplicated in Parquet)
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    retained_raw = {e["sha256"] for e in events if e.get("blob_store") == "central-raw"}
    assert {rule_row["body_sha256"], prorule_row["body_sha256"], html_row["body_sha256"]} <= retained_raw

    # Coverage KV metadata records exact scope and completion state
    coverage = json.loads(pq.read_schema(out).metadata[mod.COVERAGE_KEY.encode()])
    assert coverage == {
        "since": "2021-01-01",
        "documentTypes": ["Rule", "Proposed Rule"],
        "significantOnly": True,
        "declaredPopulationCount": 6,
        "publishedRowCount": 6,
        "deferredCount": 0,
        "maxNew": None,
        "complete": True,
    }


def _stream_response(status: int, *, content_type: str, body: bytes) -> httpx.Response:
    return httpx.Response(status, headers={"Content-Type": content_type}, stream=httpx.ByteStream(body))


def test_max_new_batches_carry_forward_grow_monotonically_and_refresh_metadata(tmp_path):
    fr_rows = [
        _fr_row("2026-00004", "2026-04-01", source_sha256=None),
        _fr_row("2026-00003", "2026-03-01", source_sha256=None),
        _fr_row("2026-00002", "2026-02-01", source_sha256=None),
        _fr_row("2026-00001", "2026-01-01", source_sha256=None),
    ]
    fr_path = _write_fr_parquet(tmp_path / "federal_register.parquet", fr_rows)
    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        doc = request.url.path.rsplit("/", 1)[-1].removesuffix(".xml")
        fetched.append(doc)
        return _stream_response(200, content_type="application/xml", body=_rule_xml(doc))

    transport = httpx.MockTransport(handler)

    # Batch 1: max_new=2 -> acquires newest 2 rows only (2026-00004, 2026-00003)
    b1_dir = tmp_path / "batch1"
    b1 = mod.build_federal_register_bodies(
        b1_dir,
        fr_path=fr_path,
        max_new=2,
        min_request_interval_seconds=0.0,
        download_prior=lambda *_: False,
        transport=transport,
    )
    assert fetched == ["2026-00004", "2026-00003"]
    b1_rows = {r["document_number"]: r for r in pq.read_table(b1).to_pylist()}
    assert set(b1_rows) == {"2026-00004", "2026-00003"}
    cov1 = json.loads(pq.read_schema(b1).metadata[mod.COVERAGE_KEY.encode()])
    assert (cov1["publishedRowCount"], cov1["deferredCount"], cov1["complete"]) == (2, 2, False)

    # Batch 2: max_new=2 over Batch 1 prior -> carries forward first 2 without refetch, fetches next 2 (2 -> 4 rows)
    fetched.clear()
    b2_dir = tmp_path / "batch2"
    b2 = mod.build_federal_register_bodies(
        b2_dir,
        fr_path=fr_path,
        max_new=2,
        min_request_interval_seconds=0.0,
        prior_path=b1,
        transport=transport,
    )
    assert fetched == ["2026-00002", "2026-00001"]
    b2_rows = {r["document_number"]: r for r in pq.read_table(b2).to_pylist()}
    assert len(b2_rows) == 4
    assert b2.stat().st_size >= b1.stat().st_size
    assert b2_rows["2026-00004"] == b1_rows["2026-00004"]
    assert b2_rows["2026-00003"] == b1_rows["2026-00003"]
    cov2 = json.loads(pq.read_schema(b2).metadata[mod.COVERAGE_KEY.encode()])
    assert (cov2["publishedRowCount"], cov2["deferredCount"], cov2["complete"]) == (4, 0, True)

    # Batch 3: nothing changed -> raises Unchanged with zero HTTP calls
    fetched.clear()
    with pytest.raises(Unchanged):
        mod.build_federal_register_bodies(
            tmp_path / "batch3",
            fr_path=fr_path,
            min_request_interval_seconds=0.0,
            prior_path=b2,
            transport=transport,
            raise_if_unchanged=True,
        )
    assert fetched == []

    # Backfill updates source_sha256 on federal_register.parquet -> updates metadata without re-fetching bodies
    updated_rows = [{**row, "source_sha256": "sha256:" + "a" * 64} for row in fr_rows]
    fr_updated = _write_fr_parquet(tmp_path / "federal_register_updated.parquet", updated_rows)
    fetched.clear()
    b4 = mod.build_federal_register_bodies(
        tmp_path / "batch4",
        fr_path=fr_updated,
        min_request_interval_seconds=0.0,
        prior_path=b2,
        transport=transport,
        raise_if_unchanged=True,
    )
    assert fetched == []
    b4_rows = {r["document_number"]: r for r in pq.read_table(b4).to_pylist()}
    assert all(r["source_sha256"] == "sha256:" + "a" * 64 for r in b4_rows.values())
    assert b4_rows["2026-00004"]["body_observed_at"] == b1_rows["2026-00004"]["body_observed_at"]
    assert b4_rows["2026-00004"]["body_sha256"] == b1_rows["2026-00004"]["body_sha256"]


def test_lowering_since_expands_population_with_zero_schema_change(tmp_path):
    fr_path = _write_fr_parquet(
        tmp_path / "federal_register.parquet",
        [
            _fr_row("2021-00010", "2021-01-15"),
            _fr_row("2020-29000", "2020-11-10"),
        ],
    )
    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        doc = request.url.path.rsplit("/", 1)[-1].removesuffix(".xml")
        fetched.append(doc)
        return _stream_response(200, content_type="application/xml", body=_rule_xml(doc))

    transport = httpx.MockTransport(handler)
    v1 = mod.build_federal_register_bodies(
        tmp_path / "v1",
        fr_path=fr_path,
        since=date(2021, 1, 1),
        min_request_interval_seconds=0.0,
        download_prior=lambda *_: False,
        transport=transport,
    )
    assert fetched == ["2021-00010"]
    assert pq.read_table(v1).num_rows == 1

    fetched.clear()
    v2 = mod.build_federal_register_bodies(
        tmp_path / "v2",
        fr_path=fr_path,
        since=date(2020, 1, 1),
        min_request_interval_seconds=0.0,
        prior_path=v1,
        transport=transport,
    )
    assert fetched == ["2020-29000"]
    assert pq.read_table(v2).schema.equals(pq.read_table(v1).schema, check_metadata=False)
    assert {r["document_number"] for r in pq.read_table(v2).to_pylist()} == {"2021-00010", "2020-29000"}


def test_http_403_access_refusal_aborts_loudly_without_bypass(tmp_path):
    fr_path = _write_fr_parquet(tmp_path / "federal_register.parquet", [_fr_row("2026-24227", "2026-11-27")])
    evidence = CaptureEvidence(tmp_path / "build", mod.FAMILY)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, headers={"Content-Type": "text/html"}, content=b"Forbidden")

    with pytest.raises(CredentialRefusedError):
        mod.build_federal_register_bodies(
            tmp_path / "build",
            fr_path=fr_path,
            min_request_interval_seconds=0.0,
            download_prior=lambda *_: False,
            transport=httpx.MockTransport(handler),
            evidence=evidence,
        )
    evidence.finish()


def test_rollup_end_to_end_generation_publication_and_odcs_parity(tmp_path, monkeypatch):
    out_dir = tmp_path / "run1"
    out_dir.mkdir()
    _write_fr_parquet(
        out_dir / "federal_register.parquet",
        [
            _fr_row("2026-24227", "2026-11-27", document_type="Rule", source_sha256="sha256:" + "1" * 64),
            _fr_row("2026-24100", "2026-11-20", document_type="Proposed Rule", source_sha256="sha256:" + "2" * 64),
        ],
    )
    fetched: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        doc = request.url.path.rsplit("/", 1)[-1].removesuffix(".xml")
        fetched.append(doc)
        root = "PRORULE" if doc == "2026-24100" else "RULE"
        return httpx.Response(200, headers={"Content-Type": "application/xml"}, content=_rule_xml(doc, root=root))

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(mod, "published_prior", lambda *_: False)
    monkeypatch.setattr(rollup_mod.FederalRegisterBodiesRollup, "transport", httpx.MockTransport(handler))
    monkeypatch.setattr(rollup_mod.FederalRegisterBodiesRollup, "min_request_interval_seconds", 0.0)

    rollup_mod.FederalRegisterBodiesRollup(output_dir=out_dir, skip_upload=True).run()

    assert sorted(fetched) == ["2026-24100", "2026-24227"]
    (gen_dir,) = (out_dir / "generations").iterdir()
    (ev_dir,) = (out_dir / "source-evidence").iterdir()
    ev_artifact = ev_dir / "artifact"
    assert local_paths(gen_dir, ev_artifact, also=(str(tmp_path),)) == []

    # Admitted generation publishes cleanly to R2 Store (receipts-free)
    index = publication.publish_generation(
        gen_dir,
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(ev_artifact,),
    )
    entry = index["families"][mod.FAMILY]
    assert list(entry["tables"]) == [mod.OUTPUT]
    assert "etlReceipts" not in entry

    # Registered across dictionary, MCP, local families, and ODCS with exact Arrow parity
    assert mod.TABLE in data_dictionary.expected_schemas()
    assert mod.TABLE in mcp_server.TABLES
    assert mod.FAMILY not in LOCAL_FAMILIES

    odcs_dir = tmp_path / "odcs"
    odcs.export_odcs_contracts(odcs_dir, families=[mod.FAMILY])
    loaded = odcs.load_enrolled_family_contract(mod.FAMILY, contracts_dir=odcs_dir)
    assert loaded is not None
    contract, _ = loaded
    doc_num_prop = next(
        p for p in contract["schema"][0]["properties"] if p["name"] == "document_number"
    )
    refspec = {cp["property"]: cp["value"] for cp in doc_num_prop["customProperties"] if cp["vendor"] == "refspec"}
    assert refspec["urn"] == "urn:rkaf:us:frdoc:"
    parity = odcs.verify_parquet_schema_parity(
        {mod.TABLE: contract["schema"][0]},
        {mod.TABLE: gen_dir / mod.OUTPUT},
    )
    assert parity[mod.TABLE]["files_checked"] == 1
    assert parity[mod.TABLE]["columns"] == mod.SCHEMA.names

    # Re-run against the published table -> Unchanged exits green with zero fetches and no new generation
    fetched.clear()
    out_dir2 = tmp_path / "run2"
    out_dir2.mkdir()
    (out_dir2 / "federal_register.parquet").write_bytes((out_dir / "federal_register.parquet").read_bytes())
    published_parquet = gen_dir / mod.OUTPUT

    def download_prior(_key: str, dest: Path) -> bool:
        dest.write_bytes(published_parquet.read_bytes())
        return True

    monkeypatch.setattr(mod, "published_prior", download_prior)
    rollup_mod.FederalRegisterBodiesRollup(output_dir=out_dir2, skip_upload=True).run()
    assert fetched == []
    assert not (out_dir2 / "generations").exists()


def test_console_script_and_workflow_wiring():
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["run-rollup-federal-register-bodies"] == "spicy_regs.pipelines.rollups.federal_register_bodies:app"

    workflow = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "rollup-federal-register-bodies.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["schedule"] == [{"cron": "37 21 * * 1-5"}]
    assert set(triggers["workflow_dispatch"]["inputs"]) == {"max_new", "since", "retry_failed", "skip_upload"}
    assert workflow["permissions"] == {"contents": "read", "actions": "read"}
    run = workflow["jobs"]["run"]
    assert run["uses"] == "./.github/workflows/_rollup.yml"
    assert run["with"]["command"] == "run-rollup-federal-register-bodies"
    assert run["with"]["timeout_minutes"] == 45

    reusable = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "_rollup.yml").read_text())
    declared_inputs = set(reusable[True]["workflow_call"]["inputs"])
    assert set(run["with"]) <= declared_inputs
