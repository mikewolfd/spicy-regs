"""Executive disclosure transforms, streaming multi-part extraction, pipeline, MCP, and dry-run tests."""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import date
import hashlib
from io import BytesIO
import json
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_docs.sources import executive_disclosures as source_mod
from spicy_docs.sources.executive_disclosures import CoverMetadata, ExecutiveDisclosureError, ExecutiveDisclosureResult
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs import local_publication, mcp_server, table_joins
from spicy_regs.pipelines import executive_disclosures as pipeline
from spicy_regs.source_evidence import JOURNAL, verify_evidence
from spicy_regs.sources import publication
from spicy_regs.transforms.build_executive_disclosures import (
    SCHEMAS,
    TABLES,
    amount_range,
    build_tables,
    parse_transaction_date,
    resolve_income_amount,
    validate_selection,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SELECTION = json.loads((Path(__file__).parent / "fixtures/executive_disclosures/selection.json").read_text())
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "refresh-executive-disclosures.yml"

HARRIS_2024_SHA256 = "sha256:e5e9308b0443d0a7f96348172f795e76b3f433f09c0b016446562fd33af70089"
HARRIS_2021_SHA256 = "sha256:4811d4c9e69e74730fd2354883907745f0939fc28875645ce816c6e447f61a08"
DEFAULT_RAW_ARCHIVE = Path.home() / "Work" / "corpora" / "spicyregs" / "raw"
HARRIS_2024_RAW = DEFAULT_RAW_ARCHIVE / "objects" / "sha256" / "e5" / HARRIS_2024_SHA256.removeprefix("sha256:")
HARRIS_2021_RAW = DEFAULT_RAW_ARCHIVE / "objects" / "sha256" / "48" / HARRIS_2021_SHA256.removeprefix("sha256:")
HAS_REAL_HARRIS_ORIGINALS = HARRIS_2024_RAW.is_file() and HARRIS_2021_RAW.is_file()

MINIMAL_PDF = (
    b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 100 100]>>endobj\n"
    b"xref\n0 4\n0000000000 65535 f \n0000000009 00000 n \n"
    b"0000000052 00000 n \n0000000101 00000 n \n"
    b"trailer<</Size 4/Root 1 0 R>>\nstartxref\n163\n%%EOF\n"
)


class EvidenceStub:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self.retained: list[dict] = []

    def retain_bytes(self, body: bytes, **kwargs) -> None:
        self.retained.append({"size": len(body), **kwargs})

    def event(self, name: str, **kwargs) -> None:
        self.events.append((name, kwargs))


def _seed_store(raw_root: Path, bodies: list[bytes]) -> CentralRawArchiveStore:
    store = CentralRawArchiveStore(raw_root)
    for body in bodies:
        store.put_stream([body], max_bytes=max(len(body), 1024))
    return store


def _synthetic_multipart_result(sha256: str) -> ExecutiveDisclosureResult:
    """Controlled multi-part Form 278e result covering Parts 2, 5, 6, 7, endnotes, and per-page resets."""
    endnote = {
        "section": "Part 2: Filer's Employment Assets & Income and Retirement Accounts",
        "section_code": "part2",
        "page": 3,
        "source_locator": "#page=3",
        "table_row_index": 21,
        "text_raw": "Endnote for Lines 5 & 9: Gross royalties.",
        "bbox_json": "[0, 0, 100, 10]",
        "raw_cells_json": '["Endnote for Lines 5 & 9: Gross royalties.", null]',
    }
    pages = (
        {
            "page": 3,
            "section": "Part 2: Filer's Employment Assets & Income and Retirement Accounts",
            "section_code": "part2",
            "layout_profile": "excel_278e",
            "form_revision": "OGE Form 278e (Updated Nov. 2021)",
            "revision_inherited": False,
            "page_text": "Part 2 page text",
            "filer_identity_json": "{}",
            "source_locator": "#page=3",
            "table_bbox_json": "[]",
            "source_slot_count": 3,
            "populated_slot_count": 3,
            "blank_slot_count": 0,
            "endnotes": (endnote,),
            "endnotes_json": json.dumps([endnote]),
        },
        {
            "page": 6,
            "section": "Part 5: Spouse's Employment Assets & Income and Retirement Accounts",
            "section_code": "part5",
            "layout_profile": "excel_278e",
            "form_revision": "OGE Form 278e (Updated Nov. 2021)",
            "revision_inherited": False,
            "page_text": "Part 5 page text",
            "filer_identity_json": "{}",
            "source_locator": "#page=6",
            "table_bbox_json": "[]",
            "source_slot_count": 2,
            "populated_slot_count": 1,
            "blank_slot_count": 1,
            "endnotes": (),
            "endnotes_json": "[]",
        },
        {
            "page": 8,
            "section": "Part 6: Other Assets and Income",
            "section_code": "part6",
            "layout_profile": "excel_278e",
            "form_revision": "OGE Form 278e (Updated Nov. 2021)",
            "revision_inherited": False,
            "page_text": "Part 6 page text",
            "filer_identity_json": "{}",
            "source_locator": "#page=8",
            "table_bbox_json": "[]",
            "source_slot_count": 2,
            "populated_slot_count": 2,
            "blank_slot_count": 0,
            "endnotes": (),
            "endnotes_json": "[]",
        },
        {
            "page": 9,
            "section": "Part 7: Transactions",
            "section_code": "part7",
            "layout_profile": "excel_278e",
            "form_revision": "OGE Form 278e (Updated Nov. 2021)",
            "revision_inherited": False,
            "page_text": "Part 7 page 9",
            "filer_identity_json": "{}",
            "source_locator": "#page=9",
            "table_bbox_json": "[]",
            "source_slot_count": 2,
            "populated_slot_count": 2,
            "blank_slot_count": 0,
            "endnotes": (),
            "endnotes_json": "[]",
        },
        {
            "page": 10,
            "section": "Part 7: Transactions",
            "section_code": "part7",
            "layout_profile": "excel_278e",
            "form_revision": "OGE Form 278e (Updated Nov. 2021)",
            "revision_inherited": True,
            "page_text": "Part 7 page 10",
            "filer_identity_json": "{}",
            "source_locator": "#page=10",
            "table_bbox_json": "[]",
            "source_slot_count": 2,
            "populated_slot_count": 2,
            "blank_slot_count": 0,
            "endnotes": (),
            "endnotes_json": "[]",
        },
    )
    rows = (
        {
            "section": "Part 2: Filer's Employment Assets & Income and Retirement Accounts",
            "section_code": "part2",
            "page": 3,
            "slot_index": 1,
            "row_key": "part2:p3:5",
            "row_number_raw": "5.",
            "description_raw": "Penguin Random House book royalty",
            "eif_raw": "N/A",
            "value_raw": "None (or less than $1,001)",
            "income_type_raw": "Royalties",
            "income_amount_raw": "$234.13",
            "income_exact_raw": "$234.13",
            "source_locator": "#page=3",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 2: Filer's Employment Assets & Income and Retirement Accounts",
            "section_code": "part2",
            "page": 3,
            "slot_index": 2,
            "row_key": "part2:p3:9",
            "row_number_raw": "9.",
            "description_raw": "Book advance (July 2020 single-column exact style)",
            "eif_raw": "N/A",
            "value_raw": "",
            "income_type_raw": "Royalties",
            "income_amount_raw": "$321,500.00",
            "income_exact_raw": "",
            "source_locator": "#page=3",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 2: Filer's Employment Assets & Income and Retirement Accounts",
            "section_code": "part2",
            "page": 3,
            "slot_index": 3,
            "row_key": "part2:p3:10",
            "row_number_raw": "10.",
            "description_raw": "Split col-6 exact only",
            "eif_raw": "N/A",
            "value_raw": "Over $50,000,000",
            "income_type_raw": "Royalties",
            "income_amount_raw": "",
            "income_exact_raw": "$8,254",
            "source_locator": "#page=3",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 5: Spouse's Employment Assets & Income and Retirement Accounts",
            "section_code": "part5",
            "page": 6,
            "slot_index": 1,
            "row_key": "part5:p6:1",
            "row_number_raw": "1.",
            "description_raw": "(Spouse) Retirement fund",
            "eif_raw": "Yes",
            "value_raw": "$15,001 - $50,000",
            "income_type_raw": "",
            "income_amount_raw": "None (or less than $201)",
            "income_exact_raw": "",
            "source_locator": "#page=6",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 5: Spouse's Employment Assets & Income and Retirement Accounts",
            "section_code": "part5",
            "page": 6,
            "slot_index": 2,
            "row_key": "part5:p6:slot2",
            "row_number_raw": "",
            "description_raw": "",
            "eif_raw": "",
            "value_raw": "",
            "income_type_raw": "",
            "income_amount_raw": "",
            "income_exact_raw": "",
            "source_locator": "#page=6",
            "blank_slot": True,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 6: Other Assets and Income",
            "section_code": "part6",
            "page": 8,
            "slot_index": 1,
            "row_key": "part6:p8:1",
            "row_number_raw": "1.",
            "description_raw": "(Self) U.S. bank #1 (cash)",
            "eif_raw": "N/A",
            "value_raw": "$50,001 - $100,000",
            "income_type_raw": "Interest",
            "income_amount_raw": "$201 - $1,000",
            "income_exact_raw": "",
            "source_locator": "#page=8",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 6: Other Assets and Income",
            "section_code": "part6",
            "page": 8,
            "slot_index": 2,
            "row_key": "part6:p8:4",
            "row_number_raw": "4.",
            "description_raw": "The KDH/DCE Family Trust",
            "eif_raw": "No",
            "value_raw": "",
            "income_type_raw": "",
            "income_amount_raw": "",
            "income_exact_raw": "",
            "source_locator": "#page=8",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
    )
    # Per-page row-number reset: 1. and 2. appear on BOTH page 9 and page 10!
    transactions = (
        {
            "section": "Part 7: Transactions",
            "section_code": "part7",
            "page": 9,
            "slot_index": 1,
            "row_key": "part7:p9:1",
            "row_number_raw": "1.",
            "description_raw": "(Spouse) Bond ETF (HYLB)",
            "transaction_type_raw": "sale",
            "transaction_date_raw": "02/23/2023",
            "amount_raw": "$1,001 - $15,000",
            "source_locator": "#page=9",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 7: Transactions",
            "section_code": "part7",
            "page": 9,
            "slot_index": 2,
            "row_key": "part7:p9:2",
            "row_number_raw": "2.",
            "description_raw": "(Joint) Treasury Note",
            "transaction_type_raw": "purchase",
            "transaction_date_raw": "06/30/2023",
            "amount_raw": "Over $1,000,000",
            "source_locator": "#page=9",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 7: Transactions",
            "section_code": "part7",
            "page": 10,
            "slot_index": 1,
            "row_key": "part7:p10:1",
            "row_number_raw": "1.",
            "description_raw": "(Self) Index Fund",
            "transaction_type_raw": "purchase",
            "transaction_date_raw": "11/14/2023",
            "amount_raw": "$15,001 - $50,000",
            "source_locator": "#page=10",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
        {
            "section": "Part 7: Transactions",
            "section_code": "part7",
            "page": 10,
            "slot_index": 2,
            "row_key": "part7:p10:2",
            "row_number_raw": "2.",
            "description_raw": "Unparsed date sample",
            "transaction_type_raw": "sale",
            "transaction_date_raw": "Various",
            "amount_raw": "$50,001 - $100,000",
            "source_locator": "#page=10",
            "blank_slot": False,
            "raw_cells_json": "[]",
            "cell_boxes_json": "[]",
        },
    )
    cover = CoverMetadata(
        status="readable",
        reason=None,
        cover_page=1,
        filer_last="Harris",
        filer_first="Kamala",
        filer_mi="D.",
        filer_name="Kamala D. Harris",
        position_raw="Vice President of the United States of America",
        agency_raw="",
        report_type="Annual",
        report_year=2023,
        report_period_raw="2023",
        appointment_or_termination_date_raw="January 20, 2021",
        form_revision="OGE Form 278e (Updated Nov. 2021)",
        oge_digital_sig_date="2024-05-15",
        cover_fields=(),
    )
    return ExecutiveDisclosureResult(
        source_sha256=sha256,
        parser_revision=source_mod.PARSER_REVISION,
        backend_version="test-double",
        layout_profile="excel_278e",
        form_revision="Updated Nov. 2021",
        cover=cover,
        pages=pages,
        rows=rows,
        transactions=transactions,
        endnotes=(endnote,),
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", ("blank", None, None, None)),
        ("   \n\t ", ("blank", None, None, None)),
        ("None (or less than $201)", ("below_threshold", 0, 200, None)),
        ("None (or less than\n$201)", ("below_threshold", 0, 200, None)),
        ("None (or less than $1,001)", ("below_threshold", 0, 1000, None)),
        ("$15,001 - $50,000", ("range", 15001, 50000, None)),
        ("$50,001 - $100,000", ("range", 50001, 100000, None)),
        ("Over $50,000,000", ("over_threshold", 50000000, None, None)),
        ("over $1,000,000", ("over_threshold", 1000000, None, None)),
        ("$234.13", ("exact_amount", None, None, "234.13")),
        ("$8,254", ("exact_amount", None, None, "8254")),
        ("$321,500.00", ("exact_amount", None, None, "321500.00")),
        ("$1655.92", ("exact_amount", None, None, "1655.92")),
        ("$100,000 - $50,001", ("unrecognized", None, None, None)),
        ("None (or less than $500)", ("unrecognized", None, None, None)),
        ("Over $0", ("unrecognized", None, None, None)),
        ("$12.3", ("unrecognized", None, None, None)),
        ("Not disclosed", ("unrecognized", None, None, None)),
    ],
)
def test_amount_range_classifies_all_six_statuses_without_truncating_cents(raw, expected):
    assert amount_range(raw) == expected


def test_resolve_income_amount_handles_july_2020_nov_2021_and_refuses_conflicts():
    # July 2020: exact amount in income_amount_raw, income_exact_raw == ""
    assert resolve_income_amount("$321,500.00", "") == ("$321,500.00", "exact_amount", None, None, "321500.00")
    # Nov 2021 coalesced (real read_excel_278e output): both hold "$234.13"
    assert resolve_income_amount("$234.13", "$234.13") == ("$234.13", "exact_amount", None, None, "234.13")
    # Nov 2021 uncoalesced split: income_amount_raw blank, exact in income_exact_raw
    assert resolve_income_amount("", "$234.13") == ("$234.13", "exact_amount", None, None, "234.13")
    # Conflicting non-empty values MUST fail closed
    with pytest.raises(ValueError, match="conflicting income_amount_raw"):
        resolve_income_amount("$201 - $1,000", "$234.13")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", (None, "blank")),
        ("02/23/2023", (date(2023, 2, 23), "parsed")),
        ("2/5/2023", (date(2023, 2, 5), "parsed")),
        ("2023-02-23", (date(2023, 2, 23), "parsed")),
        ("02/30/2023", (None, "unparsed")),
        ("02/23/23", (None, "unparsed")),
        ("Various", (None, "unparsed")),
    ],
)
def test_parse_transaction_date(raw, expected):
    assert parse_transaction_date(raw) == expected


def test_requested_unavailable_expired_and_refused_remain_distinct(tmp_path):
    selected = deepcopy(SELECTION)
    selected["filings"] = []
    states = ["not_requested", "requested", "unavailable", "expired", "access_refused"]
    for status in states:
        item = {
            **SELECTION["filings"][0],
            "filing_id": status,
            "access_status": status,
            "source_sha256": None,
            "byte_size": None,
            "access_reason": "controlled access outcome",
        }
        selected["filings"].append(item)
    paths = build_tables(selected, tmp_path, evidence=EvidenceStub(), store=None)
    assert [p.stem for p in paths] == list(TABLES)
    rows = pq.read_table(paths[0]).to_pylist()
    assert [row["access_status"] for row in rows] == states
    assert all(row["facts_status"] == "not_available" for row in rows)
    assert all(row["qualified_sections_json"] == "[]" for row in rows)
    assert pq.read_table(paths[1]).num_rows == 0
    assert pq.read_table(paths[2]).num_rows == 0
    assert pq.read_table(paths[3]).num_rows == 0


def test_source_version_mismatch_writes_no_tables(tmp_path):
    class Store:
        def open(self, _digest):
            return BytesIO(b"different original")

    with pytest.raises(ValueError, match="differs from selected source version"):
        build_tables(SELECTION, tmp_path, evidence=EvidenceStub(), store=Store())
    assert not list(tmp_path.glob("*.parquet"))


def test_access_claim_and_person_identity_controls():
    for field, value in [
        ("access_basis", "form201_required"),
        ("access_status", "unknown"),
        ("official_id", SELECTION["filings"][0]["position_raw"]),
        ("source_url", "https://extapps2.oge.gov/201/Presiden.nsf/201+Request?OpenForm"),
    ]:
        selected = deepcopy(SELECTION)
        selected["filings"][0][field] = value
        with pytest.raises(ValueError):
            validate_selection(selected)


def test_streaming_batch_equivalence_and_mcp_4_table_joins(tmp_path, monkeypatch):
    body = MINIMAL_PDF
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    selected = deepcopy(SELECTION)
    for item in selected["filings"]:
        item.update(source_sha256=digest, byte_size=len(body))

    class Store:
        def open(self, _digest):
            return BytesIO(body)

    monkeypatch.setattr(source_mod, "read_excel_278e", lambda _b, **kw: _synthetic_multipart_result(digest))

    out_small_batch = tmp_path / "batch-2"
    out_default_batch = tmp_path / "batch-2000"
    paths_small = build_tables(selected, out_small_batch, evidence=EvidenceStub(), store=Store(), batch_size=2)
    paths_default = build_tables(selected, out_default_batch, evidence=EvidenceStub(), store=Store(), batch_size=2_000)

    for p_small, p_default in zip(paths_small, paths_default, strict=True):
        t_small = pq.read_table(p_small)
        t_default = pq.read_table(p_default)
        assert t_small.schema == SCHEMAS[p_small.stem]
        assert t_small.to_pylist() == t_default.to_pylist()

    # Verify page-scoped unique IDs, exact cents, below/over thresholds, endnotes, and MCP joins.
    monkeypatch.setattr(mcp_server, "DATA_DIR", out_default_batch)
    with mcp_server._build_connection() as con:
        filings_status = con.execute(
            "SELECT filing_id, facts_status, qualified_sections_json FROM executive_disclosure_filings ORDER BY filing_id"
        ).fetchall()
        assert filings_status == [
            ("whitehouse-harris-2021-annual", "unsupported", "[]"),
            ("whitehouse-harris-2024-annual", "qualified_parts_2_5_6_7", '["part2", "part5", "part6", "part7"]'),
        ]
        exact_rows = con.execute(
            """SELECT section_code, row_number_raw, income_status, income_exact_usd, value_status, value_lower, value_upper
               FROM executive_disclosure_facts
               WHERE section_code = 'part2'
               ORDER BY row_number_raw"""
        ).fetchall()
        assert exact_rows == [
            ("part2", "10.", "exact_amount", "8254", "over_threshold", 50000000, None),
            ("part2", "5.", "exact_amount", "234.13", "below_threshold", 0, 1000),
            ("part2", "9.", "exact_amount", "321500.00", "blank", None, None),
        ]
        # Blank trust cells stay blank/NULL, never zero.
        assert con.execute(
            "SELECT value_raw, value_status, value_lower, currency FROM executive_disclosure_facts WHERE row_number_raw='4.'"
        ).fetchone() == ("", "blank", None, None)

        # Part 7 transactions join cleanly to filings with distinct page-scoped transaction_ids.
        txns = con.execute(
            """SELECT f.official_name, t.transaction_id, t.page, t.row_number_raw, t.ownership_label,
                      t.transaction_date, t.transaction_date_status, t.amount_status, t.amount_lower, t.amount_upper
               FROM executive_disclosure_filings f
               JOIN executive_disclosure_transactions t USING (filing_id)
               ORDER BY t.page, t.row_number_raw"""
        ).fetchall()
        assert len(txns) == 4
        assert len({row[1] for row in txns}) == 4
        assert txns[0][2:10] == (9, "1.", "Spouse", date(2023, 2, 23), "parsed", "range", 1001, 15000)
        assert txns[1][2:10] == (9, "2.", "Joint", date(2023, 6, 30), "parsed", "over_threshold", 1000000, None)
        assert txns[2][2:10] == (10, "1.", "Self", date(2023, 11, 14), "parsed", "range", 15001, 50000)
        assert txns[3][2:10] == (10, "2.", None, None, "unparsed", "range", 50001, 100000)

        # Endnote retained on page 3.
        endnotes_raw = con.execute(
            "SELECT endnotes_json FROM executive_disclosure_pages WHERE page = 3"
        ).fetchone()
        assert endnotes_raw is not None
        assert len(json.loads(endnotes_raw[0])) == 1

    # Verify all 3 intra-family joins are declared in table_joins.
    incoming = table_joins.joins_for("executive_disclosure_filings")["incoming"]
    assert {join["child"] for join in incoming} == {
        "executive_disclosure_facts",
        "executive_disclosure_transactions",
        "executive_disclosure_pages",
    }


def test_v2_cohort_manifest_cover_extraction_override_and_mismatch_refusal(tmp_path, monkeypatch):
    body = MINIMAL_PDF
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    store = _seed_store(tmp_path / "raw", [body])

    readable_cover = CoverMetadata(
        status="readable",
        reason=None,
        cover_page=1,
        filer_last="Harris",
        filer_first="Kamala",
        filer_mi="D.",
        filer_name="Kamala D. Harris",
        position_raw="Vice President of the United States of America",
        agency_raw="",
        report_type="Annual",
        report_year=2023,
        report_period_raw="2023",
        appointment_or_termination_date_raw="January 20, 2021",
        form_revision="OGE Form 278e (Updated Nov. 2021)",
        oge_digital_sig_date="2024-05-15",
        cover_fields=(),
    )
    unreadable_cover = CoverMetadata(
        status="unreadable",
        reason="image_only_or_missing_cover_text",
        cover_page=2,
        filer_last=None,
        filer_first=None,
        filer_mi=None,
        filer_name=None,
        position_raw=None,
        agency_raw=None,
        report_type=None,
        report_year=None,
        report_period_raw=None,
        appointment_or_termination_date_raw=None,
        form_revision=None,
        oge_digital_sig_date=None,
        cover_fields=(),
    )

    def fake_cover(_body, *, cover_page=1):
        return readable_cover if cover_page == 1 else unreadable_cover

    monkeypatch.setattr(source_mod, "read_form278e_cover", fake_cover)
    monkeypatch.setattr(source_mod, "read_excel_278e", lambda _b, **kw: _synthetic_multipart_result(digest))

    manifest_v2: dict[str, Any] = {
        "format": "executive-disclosures-cohort",
        "version": 2,
        "cohort_id": "v2-test-cohort",
        "coverage_note": "Compact v2 manifest test.",
        "filings": [
            {
                "filing_id": "readable-2024",
                "official_id": "cohort:kamala-d-harris",
                "official_name": "Kamala D. Harris",
                "source_url": SELECTION["filings"][0]["source_url"],
                "source_sha256": digest,
                "byte_size": len(body),
                "acquired_at": "2026-11-28T00:00:00Z",
                "access_status": "available",
                "access_basis": "public_posting",
                "access_reason": "Direct official public posting",
                "access_evidence_ref": "access.md",
            },
            {
                "filing_id": "scanned-2021",
                "official_id": "cohort:kamala-d-harris",
                "official_name": "Kamala D. Harris",
                "source_url": SELECTION["filings"][1]["source_url"],
                "source_sha256": digest,
                "byte_size": len(body),
                "acquired_at": "2026-11-28T00:00:00Z",
                "access_status": "available",
                "access_basis": "public_posting",
                "access_reason": "Direct official public posting",
                "access_evidence_ref": "access.md",
                "amendment_status": "included_unreconciled",
                "amendment_details_json": '[{"page": 1, "note": "amendment"}]',
                "cover_page": 2,
                "metadata_review_ref": "raw-visual-review.md#harris-2021",
                "cover_override": {
                    "position_raw": "Vice President of the United States of America",
                    "agency_raw": "",
                    "report_type": "Annual",
                    "report_period_raw": "2020",
                    "report_year": 2020,
                    "form_type": "278e",
                },
            },
        ],
    }

    paths = build_tables(manifest_v2, tmp_path / "v2-out", evidence=EvidenceStub(), store=store)
    filings = {row["filing_id"]: row for row in pq.read_table(paths[0]).to_pylist()}
    assert filings["readable-2024"]["position_raw"] == "Vice President of the United States of America"
    assert filings["readable-2024"]["report_year"] == 2023
    assert filings["readable-2024"]["oge_certification_date"] == "2024-05-15"
    assert filings["readable-2024"]["metadata_review_ref"].startswith("native-cover:p1:")
    assert filings["readable-2024"]["facts_status"] == "qualified_parts_2_5_6_7"
    assert filings["scanned-2021"]["report_year"] == 2020
    assert filings["scanned-2021"]["facts_status"] == "unsupported"

    # Identity mismatch between declared official_name and native cover MUST fail closed!
    mismatch = deepcopy(manifest_v2)
    mismatch["filings"] = [mismatch["filings"][0]]
    mismatch["filings"][0]["official_name"] = "Different Official"
    with pytest.raises(ValueError, match="differs from native cover filer_name"):
        build_tables(mismatch, tmp_path / "v2-mismatch", evidence=EvidenceStub(), store=store)

    # Unreadable cover without cover_override MUST fail closed!
    missing_override = deepcopy(manifest_v2)
    missing_override["filings"] = [missing_override["filings"][1]]
    del missing_override["filings"][0]["cover_override"]
    with pytest.raises(ValueError, match="requires a complete cover_override"):
        build_tables(missing_override, tmp_path / "v2-no-override", evidence=EvidenceStub(), store=store)


def test_pipeline_read_index_hash_only_vs_full_and_dry_run_added_table(tmp_path, monkeypatch):
    body = MINIMAL_PDF
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    raw_root = tmp_path / "raw-archive"
    _seed_store(raw_root, [body])

    selected = deepcopy(SELECTION)
    for item in selected["filings"]:
        item.update(source_sha256=digest, byte_size=len(body))
    monkeypatch.setattr(source_mod, "read_excel_278e", lambda _b, **kw: _synthetic_multipart_result(digest))

    # Build a real prior 3-table generation + mirrored read_index snapshot (simulating 3-table live pilot).
    from spicy_regs.contract_types import described_schema
    from spicy_regs.generations import build_generation

    prior_src = tmp_path / "prior-src"
    prior_src.mkdir()
    prior_tables = ("executive_disclosure_filings", "executive_disclosure_facts", "executive_disclosure_pages")
    prior_files = []
    for name in prior_tables:
        p = prior_src / f"{name}.parquet"
        pq.write_table(SCHEMAS[name].empty_table(), p)
        prior_files.append(p)
    prior_gen_dir = tmp_path / "prior-gen"
    prior_art = build_generation(
        prior_gen_dir,
        family="executive-disclosures",
        files=prior_files,
        expected_keys=tuple(f"{name}.parquet" for name in prior_tables),
        schemas={name: described_schema(SCHEMAS[name]) for name in prior_tables},
    )
    prior_root_bytes = (prior_gen_dir / "artifact.json").read_bytes()
    prior_digest = prior_art.pin.artifact_digest
    prior_prefix = f"generations/executive-disclosures/{prior_digest.removeprefix('sha256:')}"

    mirror_root = tmp_path / "mirror"
    (mirror_root / prior_prefix).mkdir(parents=True)
    (mirror_root / prior_prefix / "artifact.json").write_bytes(prior_root_bytes)
    live_index = publication.empty_index()
    live_index["families"]["executive-disclosures"] = {
        "prefix": prior_prefix,
        "logicalId": prior_art.pin.logical_id,
        "artifactDigest": prior_digest,
        "publishedAt": "2026-11-27T03:00:25Z",
        "tables": {
            "executive_disclosure_filings.parquet": {
                "rows": 2,
                "byteSize": 1000,
                "sha256": "sha256:" + "1" * 64,
                "columns": [list(c) for c in described_schema(SCHEMAS["executive_disclosure_filings"])],
            },
            "executive_disclosure_facts.parquet": {
                "rows": 5,
                "byteSize": 1000,
                "sha256": "sha256:" + "2" * 64,
                "columns": [list(c) for c in described_schema(SCHEMAS["executive_disclosure_facts"])],
            },
            "executive_disclosure_pages.parquet": {
                "rows": 1,
                "byteSize": 1000,
                "sha256": "sha256:" + "3" * 64,
                "columns": [list(c) for c in described_schema(SCHEMAS["executive_disclosure_pages"])],
            },
        },
    }
    read_index_path = mirror_root / "publication.v2" / "snapshot.json"
    read_index_path.parent.mkdir(parents=True)
    read_index_path.write_text(json.dumps(live_index), encoding="utf-8")

    # 1. Default hash_only build with --read-index
    hash_out = tmp_path / "build-hash-only"
    artifact_h, ev_h = pipeline.build(
        selected,
        hash_out,
        archive_root=raw_root,
        read_index=read_index_path,
    )
    gen_root = json.loads((hash_out / "generation" / "artifact.json").read_text())
    assert gen_root["spec"]["readSnapshot"]["families"]["executive-disclosures"]["artifactDigest"] == prior_digest
    assert any(inp["role"] == "prior-generation" for inp in gen_root["inputs"])
    verify_evidence(ev_h, archive_root=raw_root)

    journal_h = [json.loads(line) for line in (ev_h / JOURNAL).read_text().splitlines() if line.strip()]
    policy_events = [e for e in journal_h if e.get("event") == "source-policy"]
    assert len(policy_events) == 1
    assert policy_events[0]["publisher_id"] == "executive-disclosures"
    assert policy_events[0]["evidence_policy"] == "hash_only"
    assert policy_events[0]["policy_decision_id"] == "ss-rhsd-public-executive-disclosures"
    replay_events = [e for e in journal_h if e.get("stage") == "executive-filing-replay"]
    assert len(replay_events) == 2
    assert all(e["evidence_policy"] == "hash_only" and e["body_retained"] is False and "blob_member" not in e for e in replay_events)

    # 2. Explicit opt-in policy="full" build
    full_out = tmp_path / "build-full"
    _, ev_f = pipeline.build(
        selected,
        full_out,
        archive_root=raw_root,
        evidence_policy="full",
    )
    verify_evidence(ev_f, archive_root=raw_root)
    journal_f = [json.loads(line) for line in (ev_f / JOURNAL).read_text().splitlines() if line.strip()]
    replay_full = [e for e in journal_f if e.get("stage") == "executive-filing-replay"]
    assert all(e["evidence_policy"] == "full" and e["body_retained"] is True and e.get("blob_store") == "central-raw" for e in replay_full)

    # 3. publish-local-generation --dry-run --added-table executive_disclosure_transactions.parquet
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw_root))
    monkeypatch.setattr("shutil.disk_usage", lambda _p: SimpleNamespace(total=200 * 1024**3, used=50 * 1024**3, free=150 * 1024**3))
    dry_out = tmp_path / "dry-run"
    local_publication.main(
        [
            "--generation",
            str(hash_out / "generation"),
            "--evidence",
            str(ev_h),
            "--family",
            "executive-disclosures",
            "--added-table",
            "executive_disclosure_transactions.parquet",
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(read_index_path),
            "--output",
            str(dry_out),
        ]
    )
    receipt = json.loads((dry_out / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["etlReceipts"] is None
    assert receipt["other_family_changes"] == []
    assert receipt["added_tables"] == ["executive_disclosure_transactions.parquet"]
    assert receipt["artifactDigest"] == artifact_h.pin.artifact_digest
    assert set(receipt["tables"]) == {f"{name}.parquet" for name in TABLES}


def test_acquire_manifest_reuses_retained_originals_and_refuses_route_b_by_default(tmp_path):
    raw_root = tmp_path / "raw"
    held_body = MINIMAL_PDF
    held_sha = "sha256:" + hashlib.sha256(held_body).hexdigest()
    new_body = MINIMAL_PDF + b"% second pdf\n"
    new_sha = "sha256:" + hashlib.sha256(new_body).hexdigest()
    _seed_store(raw_root, [held_body])

    url_held = "https://bidenwhitehouse.archives.gov/wp-content/uploads/2024/05/Held-2024.pdf"
    url_new = "https://bidenwhitehouse.archives.gov/wp-content/uploads/2024/05/New-2024.pdf"
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        if str(request.url) == url_new:
            return httpx.Response(200, stream=httpx.ByteStream(new_body), headers={"content-type": "application/pdf"})
        return httpx.Response(404, stream=httpx.ByteStream(b"not found"))

    manifest: dict[str, Any] = {
        "format": "executive-disclosures-cohort",
        "version": 2,
        "cohort_id": "batch-acquire-test",
        "coverage_note": "Batch acquisition test.",
        "filings": [
            {
                "filing_id": "held-filing",
                "official_id": "cohort:test-1",
                "official_name": "Test Official One",
                "source_url": url_held,
                "source_sha256": held_sha,
                "byte_size": len(held_body),
                "acquired_at": "2026-11-28T00:00:00Z",
                "access_status": "available",
                "access_basis": "public_posting",
                "access_reason": "Already retained",
                "access_evidence_ref": "access.md",
            },
            {
                "filing_id": "new-filing",
                "official_id": "cohort:test-2",
                "official_name": "Test Official Two",
                "source_url": url_new,
                "source_sha256": None,
                "byte_size": None,
                "acquired_at": None,
                "access_status": None,
                "access_basis": "public_posting",
                "access_reason": "Pending public posting",
                "access_evidence_ref": "access.md",
            },
            {
                "filing_id": "form201-excluded",
                "official_id": "cohort:test-3",
                "official_name": "Test Official Three",
                "source_url": "https://www.oge.gov/web/OGE.nsf/publicresources_disclosure-faq",
                "source_sha256": None,
                "byte_size": None,
                "acquired_at": None,
                "access_status": "not_requested",
                "access_basis": "form201_required",
                "access_reason": "Excluded Form 201 agency-gated filing",
                "access_evidence_ref": "access.md",
            },
        ],
    }

    updated, summary = pipeline.acquire_manifest(
        manifest,
        tmp_path / "acq-out",
        archive_root=raw_root,
        max_requests=4,
        transport=httpx.MockTransport(handler),
    )
    assert requested_urls == [url_new]
    assert summary == {"cohort_id": "batch-acquire-test", "filings": 3, "reused": 1, "acquired": 1, "skipped": 1}
    assert updated["filings"][1]["source_sha256"] == new_sha
    assert updated["filings"][1]["byte_size"] == len(new_body)
    assert updated["filings"][1]["access_status"] == "available"
    assert updated["filings"][2]["source_sha256"] is None

    # Route B OGE Level I/II PDF MUST fail closed when accepted_terms=False (default).
    route_b_url = (
        "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
        "0123456789abcdef0123456789abcdef/$FILE/Cabinet-Annual-2024.pdf"
    )
    route_b_manifest = deepcopy(manifest)
    route_b_manifest["filings"] = [
        {
            **manifest["filings"][1],
            "filing_id": "route-b-filing",
            "source_url": route_b_url,
        }
    ]
    with pytest.raises(ExecutiveDisclosureError, match="accepted_terms=True"):
        pipeline.acquire_manifest(
            route_b_manifest,
            tmp_path / "acq-route-b-refused",
            archive_root=raw_root,
            transport=httpx.MockTransport(handler),
        )


@pytest.mark.skipif(not HAS_REAL_HARRIS_ORIGINALS, reason="Real retained Harris-2024/2021 PDFs absent from local archive")
def test_real_harris_2024_and_2021_v1_and_v2_builds(tmp_path):
    """Replay exact retained Harris-2024 + Harris-2021 PDFs under both v1 selection and v2 cohort manifest."""
    out_v1 = tmp_path / "real-v1"
    artifact_v1, ev_v1 = pipeline.build(
        SELECTION,
        out_v1,
        archive_root=DEFAULT_RAW_ARCHIVE,
        evidence_policy="hash_only",
    )
    verify_evidence(ev_v1, archive_root=DEFAULT_RAW_ARCHIVE)
    gen_v1 = out_v1 / "generation"

    filings = {r["filing_id"]: r for r in pq.read_table(gen_v1 / "executive_disclosure_filings.parquet").to_pylist()}
    facts = pq.read_table(gen_v1 / "executive_disclosure_facts.parquet").to_pylist()
    txns = pq.read_table(gen_v1 / "executive_disclosure_transactions.parquet").to_pylist()
    pages = pq.read_table(gen_v1 / "executive_disclosure_pages.parquet").to_pylist()

    assert len(filings) == 2
    assert filings["whitehouse-harris-2024-annual"]["facts_status"] == "qualified_parts_2_5_6_7"
    assert json.loads(filings["whitehouse-harris-2024-annual"]["qualified_sections_json"]) == [
        "part2",
        "part5",
        "part6",
        "part7",
    ]
    assert filings["whitehouse-harris-2021-annual"]["facts_status"] == "unsupported"
    assert all(r["filing_id"] == "whitehouse-harris-2024-annual" for r in facts)
    assert all(r["filing_id"] == "whitehouse-harris-2024-annual" for r in txns)

    assert len(facts) == 60
    assert Counter(r["section_code"] for r in facts) == {"part2": 17, "part5": 38, "part6": 5}
    exact_map = {r["row_number_raw"]: r["income_exact_usd"] for r in facts if r["income_exact_usd"] is not None}
    assert exact_map == {"5": "234.13", "9": "8254"}
    assert all(r["income_status"] != "unrecognized" for r in facts)

    assert len(txns) == 103
    assert len({r["transaction_id"] for r in txns}) == 103
    assert all(r["transaction_date_status"] == "parsed" and r["transaction_date"] is not None for r in txns)
    assert all(r["amount_status"] == "range" and r["currency"] == "USD" for r in txns)

    assert len(pages) == 10
    part2_endnotes = [e for p in pages if p["page"] == 3 for e in json.loads(p["endnotes_json"])]
    assert len(part2_endnotes) == 1
    assert "Endnote for Lines 5 & 9" in part2_endnotes[0]["text_raw"]

    # Compact v2 cohort manifest extracts Harris-2024 cover natively & requires cover_override only for Harris-2021.
    manifest_v2: dict[str, Any] = {
        "format": "executive-disclosures-cohort",
        "version": 2,
        "cohort_id": SELECTION["cohort_id"],
        "coverage_note": SELECTION["coverage_note"],
        "filings": [
            {
                "filing_id": "whitehouse-harris-2024-annual",
                "official_id": "cohort:kamala-d-harris",
                "official_name": "Kamala D. Harris",
                "source_url": SELECTION["filings"][0]["source_url"],
                "source_sha256": HARRIS_2024_SHA256,
                "byte_size": SELECTION["filings"][0]["byte_size"],
                "acquired_at": SELECTION["filings"][0]["acquired_at"],
                "access_status": "available",
                "access_basis": "public_posting",
                "access_reason": SELECTION["filings"][0]["access_reason"],
                "access_evidence_ref": "access-and-reuse.md",
            },
            {
                "filing_id": "whitehouse-harris-2021-annual",
                "official_id": "cohort:kamala-d-harris",
                "official_name": "Kamala D. Harris",
                "source_url": SELECTION["filings"][1]["source_url"],
                "source_sha256": HARRIS_2021_SHA256,
                "byte_size": SELECTION["filings"][1]["byte_size"],
                "acquired_at": SELECTION["filings"][1]["acquired_at"],
                "access_status": "available",
                "access_basis": "public_posting",
                "access_reason": SELECTION["filings"][1]["access_reason"],
                "access_evidence_ref": "access-and-reuse.md",
                "amendment_status": "included_unreconciled",
                "amendment_details_json": SELECTION["filings"][1]["amendment_details_json"],
                "cover_page": 2,
                "metadata_review_ref": "raw-visual-review.md#harris-2021",
                "cover_override": {
                    "position_raw": "Vice President of the United States of America",
                    "agency_raw": "",
                    "report_type": "Annual",
                    "report_period_raw": "2020",
                    "report_year": 2020,
                    "form_type": "278e",
                    "filer_signed_date": "2021-05-17",
                    "agency_certification_date": "2021-05-17",
                    "oge_certification_date": "2021-05-17",
                },
            },
        ],
    }
    out_v2 = tmp_path / "real-v2"
    pipeline.build(manifest_v2, out_v2, archive_root=DEFAULT_RAW_ARCHIVE, evidence_policy="hash_only")
    gen_v2 = out_v2 / "generation"
    v2_filings = {r["filing_id"]: r for r in pq.read_table(gen_v2 / "executive_disclosure_filings.parquet").to_pylist()}
    assert v2_filings["whitehouse-harris-2024-annual"]["position_raw"] == "Vice President of the United States of America"
    assert v2_filings["whitehouse-harris-2024-annual"]["agency_raw"] == ""
    assert v2_filings["whitehouse-harris-2024-annual"]["report_year"] == 2023
    assert v2_filings["whitehouse-harris-2024-annual"]["oge_certification_date"] == "2024-05-15"
    assert pq.read_table(gen_v2 / "executive_disclosure_facts.parquet").num_rows == 60
    assert pq.read_table(gen_v2 / "executive_disclosure_transactions.parquet").num_rows == 103
    assert pq.read_table(gen_v2 / "executive_disclosure_pages.parquet").num_rows == 10
    assert artifact_v1.pin.artifact_digest.startswith("sha256:")


def test_refresh_executive_disclosures_workflow_structure_hygiene_and_cli_flags():
    assert WORKFLOW_PATH.is_file()
    raw_text = WORKFLOW_PATH.read_text(encoding="utf-8")
    doc = yaml.safe_load(raw_text)
    triggers = doc.get("on", doc.get(True, {}))
    assert set(triggers) == {"workflow_dispatch"}
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["skip_upload"]["type"] == "boolean"
    assert inputs["skip_upload"]["default"] is True
    assert inputs["evidence_policy"]["default"] == "hash_only"

    assert doc["permissions"] == {"contents": "read", "actions": "read"}
    assert doc["concurrency"] == {"group": "refresh-executive-disclosures", "cancel-in-progress": False}
    assert "executive-disclosures" in local_publication.LOCAL_FAMILIES

    # Zero paid routes, zero Form 201 / terms bypass, zero shrink override.
    assert "R2_ALLOW_SHRINK=" not in raw_text
    assert "R2_MIN_SIZE_RATIO=" not in raw_text
    assert "secrets.ZYTE_TOKEN" not in raw_text
    assert "secrets.FIRECRAWL_API_KEY" not in raw_text
    assert "--accepted-terms" not in raw_text

    steps = {step["name"]: step for step in doc["jobs"]["run"]["steps"] if "name" in step}
    for name, step in steps.items():
        if "run" in step:
            subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)
            if name not in {"Install pinned dependencies", "Retain invocation scope"}:
                assert 'TMPDIR="$RUNNER_TEMP"' in step["run"]

    publish = steps["Publish candidate generation"]
    assert publish["if"] == "inputs.skip_upload == false"
    assert '-H "User-Agent: $WORKFLOW_USER_AGENT"' in publish["run"]
    assert "unset SPICYGOV_RAW_ROOT R2_ALLOW_SHRINK R2_MIN_SIZE_RATIO" in publish["run"]

    # Verify CLI flags referenced by the workflow exist on live argparse parsers.
    base = [sys.executable, "-m", "spicy_regs.pipelines.executive_disclosures"]
    acq_help = subprocess.run([*base, "acquire-manifest", "--help"], capture_output=True, text=True, check=True).stdout
    bld_help = subprocess.run([*base, "build", "--help"], capture_output=True, text=True, check=True).stdout
    acq_flags = set(re.findall(r"--[a-z][a-z0-9-]*", acq_help))
    bld_flags = set(re.findall(r"--[a-z][a-z0-9-]*", bld_help))
    assert {"--manifest", "--manifest-out", "--max-requests", "--output-dir", "--archive-root"} <= acq_flags
    assert {"--selection", "--output-dir", "--archive-root", "--read-index", "--evidence-policy"} <= bld_flags
