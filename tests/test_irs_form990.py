"""IRS Form 990 streaming transform, multi-year sync-build, carry-forward, and CI workflow checks."""

from __future__ import annotations

import csv
from decimal import Decimal
import hashlib
from io import BytesIO, StringIO
import json
from pathlib import Path
from zipfile import ZipFile

import httpx
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_docs.sources import irs_form990 as source
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs import data_dictionary, local_publication, mcp_server
from spicy_regs.pipelines import irs_form990 as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_irs_form990 as transform

OID = "202300019349300001"
OID_AMENDED = "202300019349300002"
OID_RUTH = "202330199349301313"
OID_TRUSTEE = "202300129349300400"

INDEX = {
    "RETURN_ID": "0001",
    "FILING_TYPE": "EFILE",
    "EIN": "012345678",
    "TAX_PERIOD": "202112",
    "SUB_DATE": "2023",
    "TAXPAYER_NAME": "Synthetic control",
    "RETURN_TYPE": "990",
    "DLN": "0012",
    "OBJECT_ID": OID,
}


def _make_xml(
    *,
    return_version: str = "2021v4.2",
    ein: str = "012345678",
    tax_yr: str = "2021",
    period_begin: str = "2021-01-01",
    period_end: str = "2021-12-31",
    doc_cnt: str | None = "2",
    amended: str | None = "true",
    revenue: str | None = "00100",
    revenue_part8: str | None = "100",
    expenses: str | None = "-50",
    expenses_part9: str | None = "-50",
    grants_paid: str | None = "0",
    part_vii: str = (
        "<Form990PartVIISectionAGrp><PersonNm>A</PersonNm><TitleTxt>Trustee</TitleTxt>"
        "<ReportableCompFromOrgAmt>0</ReportableCompFromOrgAmt><ReportableCompFromRltdOrgAmt/>"
        "</Form990PartVIISectionAGrp>"
        "<Form990PartVIISectionAGrp><PersonNm>A</PersonNm><TitleTxt>Trustee</TitleTxt>"
        "</Form990PartVIISectionAGrp>"
    ),
    schedule_i: str | None = (
        "<IRS990ScheduleI>"
        "<RecipientTable><RecipientBusinessName><BusinessNameLine1Txt>Recipient</BusinessNameLine1Txt>"
        "</RecipientBusinessName><RecipientEIN>001234567</RecipientEIN>"
        "<CashGrantAmt>9007199254740993</CashGrantAmt><NonCashAssistanceAmt>0</NonCashAssistanceAmt>"
        "<PurposeOfGrantTxt>Program</PurposeOfGrantTxt></RecipientTable>"
        "<RecipientTable><RecipientBusinessName><BusinessNameLine1Txt>Recipient</BusinessNameLine1Txt>"
        "</RecipientBusinessName><CashGrantAmt>5</CashGrantAmt></RecipientTable>"
        "</IRS990ScheduleI>"
    ),
) -> bytes:
    doc_attr = f' documentCnt="{doc_cnt}"' if doc_cnt is not None else ""
    amended_xml = f"<AmendedReturnInd>{amended}</AmendedReturnInd>" if amended is not None else ""
    rev_xml = f"<CYTotalRevenueAmt>{revenue}</CYTotalRevenueAmt>" if revenue is not None else ""
    exp_xml = f"<CYTotalExpensesAmt>{expenses}</CYTotalExpensesAmt>" if expenses is not None else ""
    gp_xml = f"<CYGrantsAndSimilarPaidAmt>{grants_paid}</CYGrantsAndSimilarPaidAmt>" if grants_paid is not None else ""
    p8_xml = f"<TotalRevenueGrp><TotalRevenueColumnAmt>{revenue_part8}</TotalRevenueColumnAmt></TotalRevenueGrp>" if revenue_part8 is not None else ""
    p9_xml = f"<TotalFunctionalExpensesGrp><TotalAmt>{expenses_part9}</TotalAmt></TotalFunctionalExpensesGrp>" if expenses_part9 is not None else ""
    sched_xml = schedule_i or ""
    return (
        f'<Return xmlns="http://www.irs.gov/efile" returnVersion="{return_version}">'
        f"<ReturnHeader><Filer><EIN>{ein}</EIN></Filer><ReturnTypeCd>990</ReturnTypeCd>"
        f"<TaxYr>{tax_yr}</TaxYr><TaxPeriodBeginDt>{period_begin}</TaxPeriodBeginDt>"
        f"<TaxPeriodEndDt>{period_end}</TaxPeriodEndDt></ReturnHeader>"
        f"<ReturnData{doc_attr}><IRS990>{amended_xml}{rev_xml}{exp_xml}{gp_xml}{p8_xml}{p9_xml}{part_vii}</IRS990>"
        f"{sched_xml}</ReturnData></Return>"
    ).encode("utf-8")


XML = _make_xml()


def record(body: bytes = XML, *, streaming: bool = True, oid: str = OID, submission_year: int = 2023, index_row: dict | None = None):
    filing = {
        "object_id": oid,
        "submission_year": submission_year,
        "index_row": 1,
        "native_index": index_row or {**INDEX, "OBJECT_ID": oid},
    }
    locator = {"archive_sha256": "sha256:" + "a" * 64, "member_name": oid + "_public.xml"}
    fn = source.scan_form990_return if streaming else source.parse_return
    return fn(body, filing=filing, locator=locator)


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(INDEX.keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _zip_bytes(members: dict[str, bytes]) -> bytes:
    buf = BytesIO()
    with ZipFile(buf, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return buf.getvalue()


@pytest.mark.parametrize("streaming", [True, False])
def test_money_reconciliation_repetitions_and_missing_are_distinct(tmp_path, streaming):
    paths = transform.build_irs_form990([record(streaming=streaming)], tmp_path)
    filings, officers, grants = [pq.read_table(p).to_pylist() for p in paths]
    assert "native_xml_json" not in filings[0]
    assert filings[0]["document_cnt_status"] == "matched"
    assert filings[0]["revenue_raw"] == "00100" and filings[0]["revenue_usd"] == Decimal(100)
    assert filings[0]["expenses_usd"] == Decimal(-50)
    assert filings[0]["revenue_reconciliation"] == filings[0]["expenses_reconciliation"] == "matched"
    assert filings[0]["amended_raw"] == "true" and filings[0]["amended_status"] == "reported"
    assert len(officers) == len(grants) == 2
    assert officers[0]["submission_year"] == 2023 and grants[0]["submission_year"] == 2023
    assert officers[0]["business_name"] is None
    assert officers[0]["compensation_usd"] == Decimal(0) and officers[1]["compensation_status"] == "absent"
    assert officers[0]["related_compensation_status"] == "empty" and officers[0]["related_compensation_usd"] is None
    assert grants[0]["cash_usd"] == Decimal("9007199254740993") and grants[0]["recipient_ein"] == "001234567"
    assert grants[1]["recipient_ein"] is None
    assert officers[0]["source_xpath"] != officers[1]["source_xpath"]
    assert all(pq.read_schema(p).equals(s) for p, s in zip(paths, transform.SCHEMAS.values()))


@pytest.mark.parametrize(
    "version",
    [
        "2020v4.0",
        "2020v4.1",
        "2020v4.2",
        "2021v4.0",
        "2021v4.1",
        "2021v4.2",
        "2022v4.1",
        "2022v5.0",
        "2023v5.0",
        "2023v5.1",
        "2023v6.0",
        "2024v5.0",
        "2024v5.1",
        "2024v5.2",
        "2024v5.5",
        "2025v4.0",
    ],
)
def test_qualified_version_specs_admit_all_visually_reviewed_cohorts(version):
    assert transform.is_qualified_version(version)
    filing, officers, grants = transform.interpret(record(_make_xml(return_version=version)))
    assert filing["return_version"] == version
    assert len(officers) == 2 and len(grants) == 2


@pytest.mark.parametrize("unqualified", ["2018v3.0", "2019v4.0", "2025v5.0", "2026v1.0", "2026v4.0", "2023v6.bad", ""])
def test_unqualified_versions_refuse_in_interpret(unqualified):
    assert not transform.is_qualified_version(unqualified)
    rec = record(_make_xml(return_version=unqualified or "2026v1.0"))
    rec["return_version"] = unqualified
    with pytest.raises(transform.UnsupportedReturnVersionError):
        transform.interpret(rec)


def test_unknown_version_quarantines_within_bound_and_fails_above_ratio(tmp_path):
    good = [
        record(_make_xml(return_version="2023v6.0"), oid=f"20230001934930{i:04d}")
        for i in range(10)
    ]
    bad = record(_make_xml(return_version="2026v1.0"), oid="202300019349399999")

    # 1/11 (~9.1%) exceeds default max_quarantined_ratio=0.001 -> fails closed atomically with no Parquet written.
    with pytest.raises(ValueError, match="quarantined anomaly ratio"):
        transform.build_irs_form990_year(2023, [*good, bad], tmp_path / "fail")
    assert not list((tmp_path / "fail").rglob("*.parquet"))

    # Raising ratio bound admits the 10 good filings and writes quarantined-anomalies.json for the 1 unknown version.
    q_path = tmp_path / "ok" / "quarantined.json"
    summary = transform.build_irs_form990_year(
        2023,
        [*good, bad],
        tmp_path / "ok",
        max_quarantined_ratio=0.1,
        quarantined_out=q_path,
    )
    assert summary["rows"][transform.FILINGS] == 10
    assert summary["quarantined_count"] == 1
    assert summary["quarantined_by_reason"] == {"unsupported_return_version": 1}
    q_doc = json.loads(q_path.read_text(encoding="utf-8"))
    assert q_doc["count"] == 1 and q_doc["rows"][0]["return_version"] == "2026v1.0"


def test_document_cnt_status_corporate_trustee_amendments_and_ruth_asawa_gap(tmp_path):
    # 1. Ruth Asawa $8,000 unallocated gap witness + redacted_fewer documentCnt (decl=3, obs=2)
    ruth_index = {
        **INDEX,
        "EIN": "943231120",
        "TAXPAYER_NAME": "RUTH ASAWA LANIER INC",
        "TAX_PERIOD": "202209",
        "OBJECT_ID": OID_RUTH,
    }
    ruth_xml = _make_xml(
        return_version="2021v4.2",
        ein="943231120",
        tax_yr="2021",
        period_begin="2021-10-01",
        period_end="2022-09-30",
        doc_cnt="3",
        amended=None,
        revenue="42610",
        revenue_part8="42610",
        expenses="30070",
        expenses_part9="30070",
        grants_paid="28000",
        part_vii=(
            "<Form990PartVIISectionAGrp><PersonNm>PAUL LANIER</PersonNm><TitleTxt>PRESIDENT</TitleTxt>"
            "<ReportableCompFromOrgAmt>0</ReportableCompFromOrgAmt></Form990PartVIISectionAGrp>"
        ),
        schedule_i=(
            "<IRS990ScheduleI><RecipientTable>"
            "<RecipientBusinessName><BusinessNameLine1Txt>FRIENDS OF SCRAP INC</BusinessNameLine1Txt></RecipientBusinessName>"
            "<RecipientEIN>942566798</RecipientEIN>"
            "<CashGrantAmt>20000</CashGrantAmt><NonCashAssistanceAmt>0</NonCashAssistanceAmt>"
            "<PurposeOfGrantTxt>GENERAL SUPPORT</PurposeOfGrantTxt>"
            "</RecipientTable></IRS990ScheduleI>"
        ),
    )

    # 2. Corporate Part VII trustee with <BusinessName> and no <PersonNm> (2021v4.1) + absent documentCnt
    trustee_index = {
        **INDEX,
        "EIN": "436936657",
        "TAXPAYER_NAME": "EL SAIGH PUB SCH DIST OF CUBA MO TR",
        "TAX_PERIOD": "202208",
        "OBJECT_ID": OID_TRUSTEE,
    }
    trustee_xml = _make_xml(
        return_version="2021v4.1",
        ein="436936657",
        tax_yr="2021",
        period_begin="2021-09-01",
        period_end="2022-08-31",
        doc_cnt=None,
        amended=None,
        revenue="177663",
        revenue_part8="177663",
        expenses="180625",
        expenses_part9="180625",
        grants_paid="150000",
        part_vii=(
            "<Form990PartVIISectionAGrp>"
            "<BusinessName><BusinessNameLine1Txt>COMMERCE TRUST COMPANY</BusinessNameLine1Txt>"
            "<BusinessNameLine2Txt>INSTITUTIONAL DIVISION</BusinessNameLine2Txt></BusinessName>"
            "<TitleTxt>CO-TRUSTEE</TitleTxt>"
            "<ReportableCompFromOrgAmt>12500</ReportableCompFromOrgAmt>"
            "</Form990PartVIISectionAGrp>"
        ),
        schedule_i=None,
    )

    # 3. Original + Amended filing for SAME (EIN, TAX_PERIOD) with distinct OBJECT_IDs -> NEVER collapsed!
    orig_xml = _make_xml(return_version="2025v4.0", amended=None, revenue="50000", revenue_part8="50000")
    amend_xml = _make_xml(return_version="2025v4.0", amended="X", revenue="52000", revenue_part8="52000")

    records = [
        record(ruth_xml, oid=OID_RUTH, index_row=ruth_index),
        record(trustee_xml, oid=OID_TRUSTEE, index_row=trustee_index),
        record(orig_xml, oid=OID),
        record(amend_xml, oid=OID_AMENDED),
    ]
    summary = transform.build_irs_form990_year(2023, records, tmp_path / "out")
    assert summary["rows"][transform.FILINGS] == 4
    assert summary["document_cnt_status_counts"] == {"absent": 1, "matched": 2, "redacted_fewer": 1}

    filings = {
        r["filing_id"]: r
        for r in pq.ParquetFile(tmp_path / "out/irs990_filings/submission_year=2023/part-000000.parquet").read().to_pylist()
    }
    officers = pq.ParquetFile(tmp_path / "out/irs990_officers/submission_year=2023/part-000000.parquet").read().to_pylist()
    grants = pq.ParquetFile(tmp_path / "out/irs990_grants/submission_year=2023/part-000000.parquet").read().to_pylist()

    # Ruth Asawa witness + explicit $8,000 unallocated gap
    ruth_f = filings[OID_RUTH]
    ruth_grants = [g for g in grants if g["filing_id"] == OID_RUTH]
    assert ruth_f["revenue_usd"] == Decimal(42610)
    assert ruth_f["expenses_usd"] == Decimal(30070)
    assert ruth_f["grants_paid_usd"] == Decimal(28000)
    assert ruth_f["document_cnt_status"] == "redacted_fewer"
    assert len(ruth_grants) == 1
    assert ruth_grants[0]["recipient_ein"] == "942566798"
    assert ruth_grants[0]["cash_usd"] == Decimal(20000) and ruth_grants[0]["noncash_usd"] == Decimal(0)
    assert ruth_f["grants_paid_usd"] - ruth_grants[0]["cash_usd"] == Decimal(8000)

    # Corporate trustee witness
    corp_officers = [o for o in officers if o["filing_id"] == OID_TRUSTEE]
    assert len(corp_officers) == 1
    assert corp_officers[0]["person_name"] is None
    assert corp_officers[0]["business_name"] == "COMMERCE TRUST COMPANY INSTITUTIONAL DIVISION"
    assert corp_officers[0]["compensation_usd"] == Decimal(12500)
    assert filings[OID_TRUSTEE]["document_cnt_status"] == "absent"
    assert filings[OID_TRUSTEE]["schedule_i_status"] == "absent"

    # Both original and amended survive as distinct rows
    assert filings[OID]["amended_status"] == "absent" and filings[OID]["revenue_usd"] == Decimal(50000)
    assert filings[OID_AMENDED]["amended_raw"] == "X" and filings[OID_AMENDED]["amended_status"] == "reported"
    assert filings[OID_AMENDED]["revenue_usd"] == Decimal(52000)


def test_duplicate_identical_oid_deduplicates_while_conflicting_bytes_quarantines(tmp_path):
    r1 = record(_make_xml(revenue="100", revenue_part8="100"), oid=OID)
    r1_identical = record(_make_xml(revenue="100", revenue_part8="100"), oid=OID)
    summary = transform.build_irs_form990_year(2023, [r1, r1_identical], tmp_path / "dedup")
    assert summary["rows"][transform.FILINGS] == 1
    assert summary["duplicate_identical_oids"] == 1
    assert summary["quarantined_count"] == 0

    r1_conflicting = record(_make_xml(revenue="999", revenue_part8="999"), oid=OID)
    # Conflicting bytes for same OBJECT_ID fails above default 0.1% ratio
    with pytest.raises(ValueError, match="quarantined anomaly ratio"):
        transform.build_irs_form990_year(2023, [r1, r1_conflicting], tmp_path / "conflict-fail")
    # Within ratio cap, quarantined with reason=conflicting_duplicate_oid
    q_path = tmp_path / "conflict-ok" / "q.json"
    summary2 = transform.build_irs_form990_year(
        2023,
        [r1, r1_conflicting],
        tmp_path / "conflict-ok",
        max_quarantined_ratio=0.5,
        quarantined_out=q_path,
    )
    assert summary2["rows"][transform.FILINGS] == 1
    assert summary2["quarantined_by_reason"] == {"conflicting_duplicate_oid": 1}


@pytest.mark.parametrize("defect", ["version", "form", "amount", "duplicate"])
def test_unsupported_change_refuses_before_any_table(tmp_path, defect):
    r = record()
    if defect == "version":
        r["return_version"] = "2026v1.0"
    if defect == "form":
        r["native_index"] = {**r["native_index"], "RETURN_TYPE": "990EZ"}
    if defect == "amount":
        r = record(XML.replace(b">00100<", b">1.50<"))
    records = [r, r] if defect == "duplicate" else [r]
    with pytest.raises(ValueError):
        transform.build_irs_form990(records, tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


def test_absent_schedule_and_summary_and_mismatch_are_explicit():
    body = _make_xml(expenses=None, revenue_part8="99", schedule_i=None)
    f, _, g = transform.interpret(record(body))
    assert f["schedule_i_status"] == "absent" and g == []
    assert f["expenses_status"] == "absent" and f["expenses_reconciliation"] == "missing_field"
    assert f["revenue_reconciliation"] == "different"


def test_discover_sequential_pacing_and_immediate_403_abort():
    url_idx = source.BASE_URL + "2025/index_2025.csv"
    url_zip_a = source.BASE_URL + "2025/2025_TEOS_XML_01A.zip"
    url_zip_b = source.BASE_URL + "2025/2025_TEOS_XML_02A.zip"
    html = (
        f'<html><body><a href="{url_idx}">idx</a>'
        f'<a href="{url_zip_b}">b</a><a href="{url_zip_a}">a</a></body></html>'
    ).encode()

    calls: list[tuple[str, str, str | None]] = []
    sleeps: list[float] = []
    now = [0.0]

    def fake_clock() -> float:
        return now[0]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    def ok_handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, str(request.url), request.headers.get("user-agent")))
        if str(request.url) == source.DOWNLOADS_PAGE_URL:
            return httpx.Response(200, content=html)
        return httpx.Response(
            200,
            headers={
                "content-length": "12345",
                "etag": f'"{Path(request.url.path).name}"',
                "last-modified": "Wed, 05 Mar 2025 15:12:42 GMT",
            },
        )

    manifest = pipeline.discover(
        [2025],
        transport=httpx.MockTransport(ok_handler),
        min_interval=1.5,
        clock=fake_clock,
        sleeper=fake_sleep,
    )
    assert [c[:2] for c in calls] == [
        ("GET", source.DOWNLOADS_PAGE_URL),
        ("HEAD", url_idx),
        ("HEAD", url_zip_a),
        ("HEAD", url_zip_b),
    ]
    assert all(ua == pipeline.DEFAULT_USER_AGENT for _, _, ua in calls)
    assert sleeps == [1.5, 1.5, 1.5]
    assert len(manifest["years"]["2025"]["archives"]) == 2

    # HTTP 403 aborts immediately without retrying or probing remaining URLs.
    refused_calls: list[str] = []

    def deny_handler(request: httpx.Request) -> httpx.Response:
        refused_calls.append(f"{request.method} {request.url}")
        if str(request.url) == source.DOWNLOADS_PAGE_URL:
            return httpx.Response(200, content=html)
        return httpx.Response(403, content=b"Access Denied")

    with pytest.raises(CredentialRefusedError):
        pipeline.discover([2025], transport=httpx.MockTransport(deny_handler), min_interval=0)
    assert refused_calls == [f"GET {source.DOWNLOADS_PAGE_URL}", f"HEAD {url_idx}"]


def test_sync_build_dual_tier_hash_only_carry_forward_and_mcp_query(tmp_path, monkeypatch):
    """End-to-end sync-build: full CSV + hash_only ZIP + dry-run publish + closed-year carry-forward."""
    shared_raw = tmp_path / "shared-raw"
    monkeypatch.delenv("SPICYGOV_RAW_ROOT", raising=False)
    monkeypatch.setattr("spicy_docs.storage.raw_archive.default_raw_archive_root", lambda: shared_raw)

    # Year 2024: 1 Form 990 (2023v6.0) + 1 Form 990T (fast-skipped!)
    oid_2024 = "202400019349300001"
    oid_2024_t = "202400019349300002"
    idx_2024_rows = [
        {**INDEX, "OBJECT_ID": oid_2024, "TAX_PERIOD": "202312", "SUB_DATE": "2024", "RETURN_TYPE": "990"},
        {**INDEX, "OBJECT_ID": oid_2024_t, "TAX_PERIOD": "202312", "SUB_DATE": "2024", "RETURN_TYPE": "990T"},
    ]
    csv_2024 = _csv_bytes(idx_2024_rows)
    xml_2024 = _make_xml(return_version="2023v6.0", tax_yr="2023", period_begin="2023-01-01", period_end="2023-12-31")
    zip_2024 = _zip_bytes({
        f"2024_TEOS_XML_01A/{oid_2024}_public.xml": xml_2024,
        f"2024_TEOS_XML_01A/{oid_2024_t}_public.xml": b"<NotEvenParsed990T/>",
    })

    # Year 2025: 1 Form 990 (2025v4.0)
    oid_2025 = "202500019349300001"
    csv_2025 = _csv_bytes([{**INDEX, "OBJECT_ID": oid_2025, "TAX_PERIOD": "202412", "SUB_DATE": "2025"}])
    xml_2025 = _make_xml(return_version="2025v4.0", tax_yr="2024", period_begin="2024-01-01", period_end="2024-12-31")
    zip_2025 = _zip_bytes({f"2025_TEOS_XML_01A/{oid_2025}_public.xml": xml_2025})

    url_idx_24 = source.BASE_URL + "2024/index_2024.csv"
    url_zip_24 = source.BASE_URL + "2024/2024_TEOS_XML_01A.zip"
    url_idx_25 = source.BASE_URL + "2025/index_2025.csv"
    url_zip_25 = source.BASE_URL + "2025/2025_TEOS_XML_01A.zip"
    payloads = {
        url_idx_24: (csv_2024, '"idx24"'),
        url_zip_24: (zip_2024, '"zip24"'),
        url_idx_25: (csv_2025, '"idx25"'),
        url_zip_25: (zip_2025, '"zip25"'),
    }
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        body, etag = payloads[str(request.url)]
        return httpx.Response(200, content=body, headers={"etag": etag, "content-length": str(len(body))})

    manifest_24 = {
        "format": "spicy-irs-form990-discovery",
        "version": 1,
        "years": {
            "2024": {
                "submission_year": 2024,
                "index": {"url": url_idx_24, "content_length": len(csv_2024), "etag": '"idx24"'},
                "archives": [{"url": url_zip_24, "content_length": len(zip_2024), "etag": '"zip24"'}],
            }
        },
    }

    out1 = tmp_path / "run1"
    art1, ev1, summary1 = pipeline.sync_build(
        manifest_24,
        out1,
        archive_root=shared_raw,
        min_interval=0,
        transport=httpx.MockTransport(handler),
    )
    assert "etlReceipts" not in art1.root["spec"]
    assert all(
        t["partitionColumns"] == ["submission_year"] for t in art1.root["spec"]["tables"].values()
    )
    assert summary1["rows"] == {"irs990_filings": 1, "irs990_officers": 2, "irs990_grants": 2}
    assert summary1["years"]["2024"]["excluded_by_return_type"] == {"990T": 1}
    assert (out1 / "checkpoints" / "2024.json").is_file()
    # Temp ZIP spool was cleaned immediately and ZIP blob is NOT stored in CentralRawArchiveStore.
    assert not list(out1.glob(".zip-spool-*"))
    csv24_sha = "sha256:" + hashlib.sha256(csv_2024).hexdigest()
    zip24_sha = "sha256:" + hashlib.sha256(zip_2024).hexdigest()
    assert local_publication._cited_originals(ev1) == {csv24_sha}
    assert zip24_sha not in local_publication._cited_originals(ev1)

    # Local publish dry-run passes against shared raw archive (NOT --scratch-raw) with mixed full + hash_only evidence!
    live_path = tmp_path / "live.v2.json"
    live_path.write_text(json.dumps(publication.empty_index()))
    local_publication.main([
        "--generation", str(out1 / "generation"),
        "--family", "irs-form990",
        "--evidence", str(ev1),
        "--output", str(tmp_path / "dry1"),
        "--dry-run",
        "--live-index", str(live_path),
    ])
    receipt1 = json.loads((tmp_path / "dry1/dry-run.json").read_text())
    assert receipt1["status"] == "dry_run_accepted" and receipt1["raw_archive"] == "shared"

    # Run 2: request 2024 + 2025 with carry-forward from Run 1 -> 2024 carried forward (ZERO HTTP for 2024!), 2025 built!
    requested_urls.clear()
    manifest_24_25 = {
        "format": "spicy-irs-form990-discovery",
        "version": 1,
        "years": {
            **manifest_24["years"],
            "2025": {
                "submission_year": 2025,
                "index": {"url": url_idx_25, "content_length": len(csv_2025), "etag": '"idx25"'},
                "archives": [{"url": url_zip_25, "content_length": len(zip_2025), "etag": '"zip25"'}],
            },
        },
    }
    out2 = tmp_path / "run2"
    art2, _ev2, summary2 = pipeline.sync_build(
        manifest_24_25,
        out2,
        archive_root=shared_raw,
        carry_forward_generation=out1 / "generation",
        carry_forward_summary=out1 / "irs990-population-summary.json",
        min_interval=0,
        transport=httpx.MockTransport(handler),
    )
    assert requested_urls == [url_idx_25, url_zip_25]
    assert summary2["submission_years"] == [2024, 2025]
    assert summary2["years"]["2024"]["mode"] == "carried_forward"
    assert summary2["years"]["2025"]["mode"] == "built"
    assert summary2["rows"]["irs990_filings"] == 2

    # Verify MCP DuckDB SQL reads both split partitions cleanly through a published download selection
    serve_dir = _mount_split_generation_for_mcp(tmp_path / "serve2", out2 / "generation", _ev2)
    monkeypatch.setattr(mcp_server, "DATA_DIR", serve_dir)
    with mcp_server._build_connection() as con:
        years = con.execute("SELECT submission_year, return_version FROM irs990_filings ORDER BY submission_year").fetchall()
        assert years == [(2024, "2023v6.0"), (2025, "2025v4.0")]


def _mount_split_generation_for_mcp(dest_dir: Path, generation_dir: Path, evidence_dir: Path) -> Path:
    """Publish a split generation to a private DiskStore and materialize a verified local download.json batch."""
    import shutil
    from spicy_regs.local_data import selection_record
    from spicy_regs.sources.disk_store import DiskStore

    store = DiskStore(dest_dir / ".store", bucket="local")
    published = publication.publish_generation(
        generation_dir,
        client=store,
        bucket="local",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence_dir,),
    )
    batch_dir = dest_dir / "batch"
    batch_dir.mkdir(parents=True)
    selected = {}
    for table_name in transform.SCHEMAS:
        selected[table_name] = selection_record(published, table_name)
        shutil.copytree(generation_dir / table_name, batch_dir / table_name)
    (batch_dir / "download.json").write_text(
        json.dumps({"version": 1, "status": "complete", "publication": published, "selected": selected}) + "\n",
        encoding="utf-8",
    )
    return batch_dir


def originals(tmp_path):
    store = CentralRawArchiveStore(tmp_path / "raw")
    bodies = [_csv_bytes([INDEX]), _zip_bytes({OID + "_public.xml": XML})]
    descriptors = []
    for kind, body in zip(("index", "archive"), bodies):
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        store.put_blob(digest, len(body), [body])
        descriptors.append(
            {
                "request_url": source.BASE_URL
                + "2023/"
                + ("index_2023.csv" if kind == "index" else "2023_TEOS_XML_01A.zip"),
                "response_sha256": digest,
                "observed_at": "2026-11-13T00:00:00+00:00",
                "byte_size": len(body),
            }
        )
    return {"submission_year": 2023, "index": descriptors[0], "archive": descriptors[1], "object_ids": [OID]}


def test_ordinary_local_generation_and_consumer_lookup(tmp_path, monkeypatch):
    selection = originals(tmp_path)
    out = tmp_path / "build"
    artifact, evidence = pipeline.build(selection, out, archive_root=tmp_path / "raw")
    assert artifact.root["spec"]["family"] == "irs-form990"
    assert "etlReceipts" not in artifact.root["spec"]
    assert all(
        table["partitionColumns"] == ["submission_year"] for table in artifact.root["spec"]["tables"].values()
    )
    assert evidence.joinpath("artifact.json").exists()
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    serve_dir = _mount_split_generation_for_mcp(tmp_path / "serve-local", out / "generation", evidence)
    monkeypatch.setattr(mcp_server, "DATA_DIR", serve_dir)
    with mcp_server._build_connection() as con:
        rows = con.execute(
            "SELECT f.ein,f.filing_id,f.submission_year,f.revenue_usd,g.recipient_ein,g.cash_usd "
            "FROM irs990_filings f JOIN irs990_grants g USING(filing_id,ein,submission_year) "
            "WHERE f.ein=? ORDER BY g.entry_ordinal",
            ["012345678"],
        ).fetchall()
        assert rows == [
            ("012345678", OID, 2023, Decimal(100), "001234567", Decimal("9007199254740993")),
            ("012345678", OID, 2023, Decimal(100), None, Decimal(5)),
        ]
        assert con.execute("SELECT * FROM irs990_filings WHERE ein=?", ["999999999"]).fetchall() == []
    assert set(transform.SCHEMAS) <= data_dictionary.MCP_QUERYABLE


def test_changed_selected_original_refuses_before_generation(tmp_path):
    selection = originals(tmp_path)
    selection["index"]["response_sha256"] = "sha256:" + "b" * 64
    with pytest.raises(FileNotFoundError):
        pipeline.build(selection, tmp_path / "build", archive_root=tmp_path / "raw")
    assert not (tmp_path / "build/generation").exists()


def test_credential_refusal_is_retained_and_aborts_remaining_acquisitions(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    requests = []

    def deny(request):
        requests.append(str(request.url))
        return httpx.Response(403, content=b"Forbidden")

    selected = [
        {"url": source.BASE_URL + "2023/index_2023.csv", "expected_size": 50, "etag": '"pin"'},
        {"url": source.BASE_URL + "2023/2023_TEOS_XML_01A.zip", "expected_size": 100, "etag": '"pin"'},
    ]
    with pytest.raises(CredentialRefusedError):
        pipeline.acquire(selected, tmp_path / "capture", transport=httpx.MockTransport(deny))
    assert requests == [selected[0]["url"]]
    roots = list((tmp_path / "capture/source-evidence").glob("*/artifact/artifact.json"))
    assert len(roots) == 1
    assert '"failed"' in roots[0].read_text()


def test_refresh_irs_form990_workflow_structure_and_safety():
    root = Path(__file__).parents[1]
    wf_path = root / ".github/workflows/refresh-irs-form990.yml"
    assert wf_path.is_file()
    workflow = yaml.safe_load(wf_path.read_text(encoding="utf-8"))
    on_block = workflow.get("on", workflow.get(True))
    inputs = on_block["workflow_dispatch"]["inputs"]
    assert inputs["years"]["default"] == "2026"
    assert inputs["max_zips"]["default"] == "0"
    assert inputs["expect_prior"]["default"] == ""
    assert inputs["skip_upload"]["default"] is True

    steps = workflow["jobs"]["run"]["steps"]
    names = [step.get("name") for step in steps]
    free_step = next(step for step in steps if step.get("name") == "Free preinstalled toolchains on Linux runners")
    discover_step = next(step for step in steps if step.get("name") == "Discover official IRS TEOS index and archive URLs")
    build_step = next(step for step in steps if step.get("name") == "Stream build and validate candidate generation")
    publish_step = next(step for step in steps if step.get("name") == "Publish candidate generation")

    assert free_step["if"] == "runner.os == 'Linux'"
    assert "/usr/share/dotnet" in free_step["run"] and "/usr/local/lib/android" in free_step["run"]
    assert "secrets." not in json.dumps(free_step) and "R2_" not in json.dumps(free_step)
    assert names.index(free_step["name"]) < names.index(discover_step["name"]) < names.index(build_step["name"]) < names.index(publish_step["name"])

    assert publish_step["if"] == "inputs.skip_upload == false"
    assert "unset SPICYGOV_RAW_ROOT R2_ALLOW_SHRINK R2_MIN_SIZE_RATIO" in publish_step["run"]
    assert "live-before-publish.v2.json" in publish_step["run"]
    assert "publish-local-generation" in publish_step["run"]
    assert "--carry-forward-generation" in build_step["run"]
    assert "sync-build" in build_step["run"]
