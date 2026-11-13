"""Exact money, repeated native relationships, source changes and ordinary local consumer admission."""

from decimal import Decimal
from io import BytesIO
import csv
import hashlib
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources import irs_form990 as source
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs import mcp_server, data_dictionary
from spicy_regs.pipelines import irs_form990 as pipeline
from spicy_regs.transforms import build_irs_form990 as transform

OID = "202300019349300001"
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
XML = b"""<Return xmlns="http://www.irs.gov/efile" returnVersion="2021v4.2"><ReturnHeader><Filer><EIN>012345678</EIN></Filer><ReturnTypeCd>990</ReturnTypeCd><TaxYr>2021</TaxYr><TaxPeriodEndDt>2021-12-31</TaxPeriodEndDt></ReturnHeader><ReturnData><IRS990><AmendedReturnInd>true</AmendedReturnInd><CYTotalRevenueAmt>00100</CYTotalRevenueAmt><CYTotalExpensesAmt>-50</CYTotalExpensesAmt><CYGrantsAndSimilarPaidAmt>0</CYGrantsAndSimilarPaidAmt><TotalRevenueGrp><TotalRevenueColumnAmt>100</TotalRevenueColumnAmt></TotalRevenueGrp><TotalFunctionalExpensesGrp><TotalAmt>-50</TotalAmt></TotalFunctionalExpensesGrp><Form990PartVIISectionAGrp><PersonNm>A</PersonNm><TitleTxt>Trustee</TitleTxt><ReportableCompFromOrgAmt>0</ReportableCompFromOrgAmt><ReportableCompFromRltdOrgAmt/></Form990PartVIISectionAGrp><Form990PartVIISectionAGrp><PersonNm>A</PersonNm><TitleTxt>Trustee</TitleTxt></Form990PartVIISectionAGrp></IRS990><IRS990ScheduleI><RecipientTable><RecipientBusinessName><BusinessNameLine1Txt>Recipient</BusinessNameLine1Txt></RecipientBusinessName><RecipientEIN>001234567</RecipientEIN><CashGrantAmt>9007199254740993</CashGrantAmt><NonCashAssistanceAmt>0</NonCashAssistanceAmt><PurposeOfGrantTxt>Program</PurposeOfGrantTxt></RecipientTable><RecipientTable><RecipientBusinessName><BusinessNameLine1Txt>Recipient</BusinessNameLine1Txt></RecipientBusinessName><CashGrantAmt>5</CashGrantAmt></RecipientTable></IRS990ScheduleI></ReturnData></Return>"""


def record(body=XML):
    return source.parse_return(
        body,
        filing={"object_id": OID, "submission_year": 2023, "index_row": 1, "native_index": INDEX},
        locator={"archive_sha256": "sha256:" + "a" * 64, "member_name": OID + "_public.xml"},
    )


def test_money_reconciliation_repetitions_and_missing_are_distinct(tmp_path):
    paths = transform.build_irs_form990([record()], tmp_path)
    filings, officers, grants = [pq.read_table(p).to_pylist() for p in paths]
    assert filings[0]["revenue_raw"] == "00100" and filings[0]["revenue_usd"] == Decimal(100)
    assert filings[0]["expenses_usd"] == Decimal(-50)
    assert filings[0]["revenue_reconciliation"] == filings[0]["expenses_reconciliation"] == "matched"
    assert filings[0]["amended_raw"] == "true"
    assert len(officers) == len(grants) == 2
    assert officers[0]["compensation_usd"] == Decimal(0) and officers[1]["compensation_status"] == "absent"
    assert officers[0]["related_compensation_status"] == "empty" and officers[0]["related_compensation_usd"] is None
    assert grants[0]["cash_usd"] == Decimal("9007199254740993") and grants[0]["recipient_ein"] == "001234567"
    assert grants[1]["recipient_ein"] is None
    assert officers[0]["source_xpath"] != officers[1]["source_xpath"]
    assert all(pq.read_schema(p).equals(s) for p, s in zip(paths, transform.SCHEMAS.values()))


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


def test_absent_schedule_and_summary_and_mismatch_are_explicit(tmp_path):
    r = record()
    data = source.child(r["native_xml"], "ReturnData")
    data["children"] = data["children"][:1]
    form = source.child(data, "IRS990")
    form["children"] = [c for c in form["children"] if not c["tag"].endswith("}CYTotalExpensesAmt")]
    source.child(source.child(form, "TotalRevenueGrp"), "TotalRevenueColumnAmt")["text"] = "99"
    f, _, g = transform.interpret(r)
    assert f["schedule_i_status"] == "absent" and g == []
    assert f["expenses_status"] == "absent" and f["expenses_reconciliation"] == "missing_field"
    assert f["revenue_reconciliation"] == "different"


def originals(tmp_path):
    store = CentralRawArchiveStore(tmp_path / "raw")
    text = __import__("io").StringIO()
    w = csv.DictWriter(text, INDEX.keys())
    w.writeheader()
    w.writerow(INDEX)
    archive = BytesIO()
    with ZipFile(archive, "w") as z:
        z.writestr(OID + "_public.xml", XML)
    bodies = [text.getvalue().encode(), archive.getvalue()]
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
    assert evidence.joinpath("artifact.json").exists()
    monkeypatch.setattr(mcp_server, "DATA_DIR", out / "generation")
    with mcp_server._build_connection() as con:
        rows = con.execute(
            "SELECT f.ein,f.filing_id,f.revenue_usd,g.recipient_ein,g.cash_usd FROM irs990_filings f JOIN irs990_grants g USING(filing_id,ein) WHERE f.ein=? ORDER BY g.entry_ordinal",
            ["012345678"],
        ).fetchall()
        assert rows == [
            ("012345678", OID, Decimal(100), "001234567", Decimal("9007199254740993")),
            ("012345678", OID, Decimal(100), None, Decimal(5)),
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
    import httpx
    from spicy_docs.transport.credentials import CredentialRefusedError

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
