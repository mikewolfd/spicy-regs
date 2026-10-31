"""FEC tables rebuilt from retained originals admit only receipt-backed, successful captures (ss-j4vz B7-B9)."""

from datetime import date
from decimal import Decimal
import hashlib
from io import BytesIO
import json
from zipfile import ZipFile

import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.transforms.fec_original_tables import TABLES, _meetings, _Original, _source_page, rebuild, selection

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def cell(ref, value, *, style=None, kind=None):
    s = f' s="{style}"' if style is not None else ""
    if isinstance(value, str) and kind is None:
        return f'<c r="{ref}" t="inlineStr"{s}><is><t>{value}</t></is></c>'
    return f'<c r="{ref}"{s}><v>{value}</v></c>'


def ie_workbook(detail_rows):
    """Fixture: one 'Table 1' sheet with the qualified header, period and methodology rows.

    Style 1 is a custom "$" currency format (id 164); style 2 is built-in date id 14.
    """
    header = ["ID #", "Committee/Individual", "Filer", "Date", "Amount", "Image", "Support / Oppose", "Candidate"]
    rows = [
        f'<row r="1">{cell("A1", "Independent expenditures")}</row>',
        f'<row r="2">{cell("A2", "All filers")}</row>',
        f'<row r="3">{cell("A3", "from January 1, 1977 through December 31, 1978")}</row>',
        f'<row r="5">{cell("A5", "Amendments replace originals")}</row>',
        '<row r="6">' + "".join(cell(chr(65 + i) + "6", h) for i, h in enumerate(header)) + "</row>",
        *detail_rows,
    ]
    files = {
        "[Content_Types].xml": "<Types/>",
        "xl/workbook.xml": f'<workbook xmlns="{MAIN}" xmlns:r="{REL}"><sheets>'
        '<sheet name="Table 1" sheetId="1" r:id="rId1"/></sheets></workbook>',
        "xl/_rels/workbook.xml.rels": '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
        "xl/styles.xml": f'<styleSheet xmlns="{MAIN}"><numFmts count="1">'
        '<numFmt numFmtId="164" formatCode="&quot;$&quot;#,##0"/></numFmts>'
        '<cellXfs count="3"><xf numFmtId="0"/><xf numFmtId="164"/><xf numFmtId="14"/></cellXfs></styleSheet>',
        "xl/worksheets/sheet1.xml": f'<worksheet xmlns="{MAIN}"><sheetData>{"".join(rows)}</sheetData></worksheet>',
    }
    out = BytesIO()
    with ZipFile(out, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    return out.getvalue()


DETAIL = (
    '<row r="7">'
    + cell("A7", "C00000001")
    + cell("B7", "Committee A")
    + cell("C7", "PAC")
    + cell("D7", "28400", style=2, kind="n")
    + cell("E7", "1500.25", style=1, kind="n")
    + cell("F7", "77012345678", kind="n")
    + cell("G7", "Support")
    + cell("H7", "H8CA01001")
    + "</row>"
)
TOTAL = '<row r="8">' + cell("D8", "Total") + cell("E8", "1500.25", style=1, kind="n") + "</row>"


@pytest.fixture(name="archive")
def raw_archive(tmp_path):
    """Fixture: an empty central raw archive in tmp_path."""
    return CentralRawArchiveStore(tmp_path / "raw")


def admit(store, table, body, *, status=200, complete=True, receipt=True, **extra):
    """Store the original, record its capture-import receipt, and return its selection entry."""
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    entry = dict(
        table=table,
        source_sha256="sha256:" + sha,
        bytes=len(body),
        source_url=extra.pop("source_url", "https://www.fec.gov/original"),
        source_authority="www.fec.gov",
        collection_ids=["retained-collection"],
        acquisition_receipt=None,
        **extra,
    )
    if receipt:
        context = dict(http_status=status, response_complete=complete, acquired_at="2026-09-11T16:00:00+00:00")
        written = store.record_receipt(
            files=[dict(sha256=sha, bytes=len(body), receipt_context=context)], receipt=dict(event="capture-import")
        )
        entry["acquisition_receipt"] = written["object"]
    return entry


def test_committed_selection_names_only_rebuilt_tables_and_receipts_or_explicit_gaps():
    entries = selection()
    assert {e["table"] for e in entries} == set(TABLES)
    assert len({(e["table"], e["source_sha256"]) for e in entries}) == len(entries)
    # The one source page without a capture sidecar stays an explicit gap (ss-wfop), refused on rebuild.
    assert [e["source_sha256"][7:15] for e in entries if e["acquisition_receipt"] is None] == ["58bf2215"]


def test_historical_ie_rows_come_from_the_workbook_with_typed_amount_date_and_unit(archive):
    entry = admit(archive, "fec_historical_ie_statistics", ie_workbook([DETAIL, TOTAL]))
    result = rebuild("fec_historical_ie_statistics", store=archive, originals=[entry])
    assert result.refusals == []
    detail, total = result.rows
    assert (detail["worksheet_name"], detail["worksheet_row"], detail["row_grain"]) == ("Table 1", 7, "published_detail")
    assert (detail["amount"], detail["amount_status"], detail["unit"]) == (Decimal("1500.25"), "exact", "USD")
    assert (detail["reported_date"], detail["reported_date_status"]) == (date(1977, 10, 2), "native_typed_date")
    assert (detail["reporting_native_id"], detail["image_number"]) == ("C00000001", "77012345678")
    assert (detail["period_start"], detail["period_end"]) == (date(1977, 1, 1), date(1978, 12, 31))
    assert (total["row_grain"], total["reported_date_status"]) == ("published_total", "not_applicable_total")
    assert not {"context_sha256", "source_context_pointer", "native_cells_json"} & detail.keys()
    # Identity is the original's digest and native position: another collection id names the same row.
    moved = rebuild(
        "fec_historical_ie_statistics", store=archive, originals=[{**entry, "collection_ids": ["elsewhere"]}]
    )
    assert [r["record_id"] for r in moved.rows] == [r["record_id"] for r in result.rows]


@pytest.mark.parametrize(
    ("receipt_args", "reason"),
    [
        (dict(status=500), "http_status_500"),
        (dict(status=403), "http_status_403"),
        (dict(complete=False), "incomplete_response"),
        (dict(receipt=False), "no_acquisition_receipt"),
    ],
)
def test_original_without_a_successful_complete_receipt_is_refused(archive, receipt_args, reason):
    entry = admit(archive, "fec_historical_ie_statistics", ie_workbook([DETAIL]), **receipt_args)
    result = rebuild("fec_historical_ie_statistics", store=archive, originals=[entry])
    assert result.rows == []
    assert result.refusals == [(entry["source_sha256"], reason)]


def test_original_that_differs_from_its_selection_is_an_error(archive):
    entry = admit(archive, "fec_historical_ie_statistics", ie_workbook([DETAIL]))
    with pytest.raises(ValueError, match="differs from its selection"):
        rebuild("fec_historical_ie_statistics", store=archive, originals=[{**entry, "bytes": entry["bytes"] - 1}])


def rss(*items):
    body = "".join(
        f"<item><title>{t}</title><link>https://docquery.fec.gov/{g}</link><guid>{g}</guid>"
        f"<pubDate>Mon, 14 Sep 2026 10:00:00 GMT</pubDate>"
        f"<description>CommitteeId: C00000001 | FilingId: {g} | FormType: F3</description></item>"
        for t, g in items
    )
    return f'<?xml version="1.0"?><rss version="2.0"><channel><title>FEC</title>{body}</channel></rss>'.encode()


def test_feed_rows_are_every_item_of_the_whole_original_keyed_by_guid(archive):
    entry = admit(archive, "fec_reference_filing_feed_items", rss(("A", "101"), ("B", "102")))
    rows = rebuild("fec_reference_filing_feed_items", store=archive, originals=[entry]).rows
    assert [(r["guid"], r["filing_id"], r["committee_id"], r["parsing_status"]) for r in rows] == [
        ("101", "101", "C00000001", "parsed"),
        ("102", "102", "C00000001", "parsed"),
    ]
    assert len({r["record_id"] for r in rows}) == 2


def test_feed_guid_repeated_within_one_capture_is_refused(archive):
    entry = admit(archive, "fec_reference_filing_feed_items", rss(("A", "101"), ("B", "101")))
    with pytest.raises(ValueError, match="guid repeats"):
        rebuild("fec_reference_filing_feed_items", store=archive, originals=[entry])


def test_meeting_rows_keep_table_position_dates_and_cancellation(archive):
    html = (
        b"<html><body><h2>Open meetings</h2><table>"
        b'<tr><td>January 5, 2026</td><td><a href="/agenda.pdf">Agenda</a></td></tr>'
        b"<tr><td>March 2 and 4, 2026 (Canceled)</td><td></td></tr>"
        b"</table></body></html>"
    )
    entry = admit(archive, "fec_research_meeting_observations", html, source_url="https://www.fec.gov/meetings/")
    rows = rebuild("fec_research_meeting_observations", store=archive, originals=[entry]).rows
    assert [(r["table_ordinal"], r["table_row"], r["date_status"], r["reported_status"]) for r in rows] == [
        (0, 0, "source_single_date", "not_stated"),
        (0, 1, "source_listed_dates", "canceled"),
    ]
    assert json.loads(rows[0]["links_json"])[0]["url"] == "https://www.fec.gov/agenda.pdf"


def test_source_page_whose_title_says_it_failed_is_refused_even_with_a_successful_receipt(archive):
    page = b"<html><head><title>Server error</title></head><body><h1>Server error</h1></body></html>"
    entry = admit(archive, "fec_reference_source_pages", page, page_type="official_legal_document_reference")
    result = rebuild("fec_reference_source_pages", store=archive, originals=[entry])
    assert result.rows == []
    assert result.refusals == [(entry["source_sha256"], "failed_page")]


BANNER = b"<p>Federal Election Commission | United States of America</p>"
FOOTER = b"<p>About\nCareers\nPress\nContact\nPrivacy and security policy</p>"


def test_source_page_is_one_row_per_whole_original(archive):
    page = b"<html><head><title>MUR 1</title></head><body>" + BANNER * 2 + b"<h1>MUR 1</h1><p>Text</p>" + FOOTER + b"</body></html>"
    entry = admit(archive, "fec_reference_source_pages", page, page_type="official_legal_document_reference")
    (row,) = rebuild("fec_reference_source_pages", store=archive, originals=[entry]).rows
    assert (row["title"], row["content_scope"], row["page_type"], row["content_status"]) == (
        "MUR 1",
        "whole_original",
        "official_legal_document_reference",
        "body_extracted",
    )


def test_source_page_without_known_body_boundaries_is_refused_not_published(archive):
    """Only an extracted body is a page; the writer never sees another status (ss-j4vz re-review low)."""
    page = b"<html><head><title>MUR 1</title></head><body><h1>MUR 1</h1><p>Text</p></body></html>"
    entry = admit(archive, "fec_reference_source_pages", page, page_type="official_legal_document_reference")
    result = rebuild("fec_reference_source_pages", store=archive, originals=[entry])
    assert result.rows == []
    assert result.refusals == [(entry["source_sha256"], "unsupported_body_boundaries")]


def test_committed_selection_states_a_positive_row_count_for_every_rebuilt_table():
    from spicy_regs.transforms.fec_original_tables import expected_rows

    assert {table: expected_rows(table) for table in TABLES} == {
        "fec_historical_ie_statistics": 1086,
        "fec_research_meeting_observations": 60,
        "fec_reference_filing_feed_items": 794,
        "fec_reference_source_pages": 14,
    }


@pytest.mark.parametrize("stated", [0, -1, "20", None])
def test_selection_without_a_positive_expected_count_refuses(tmp_path, stated):
    from spicy_regs.transforms.fec_original_tables import SELECTION_PATH, expected_rows

    document = json.loads(SELECTION_PATH.read_text())
    document["expected_rows"]["fec_reference_source_pages"] = stated
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="positive row count"):
        expected_rows("fec_reference_source_pages", path)
    del document["expected_rows"]["fec_reference_source_pages"]
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="expected rows of every rebuilt table"):
        expected_rows("fec_historical_ie_statistics", path)


def test_collection_id_is_the_first_capture_part_by_number_not_by_spelling(archive):
    page = b"<html><head><title>MUR 1</title></head><body>" + BANNER * 2 + b"<h1>MUR 1</h1>" + FOOTER + b"</body></html>"
    entry = admit(archive, "fec_reference_source_pages", page, page_type="official_legal_document_reference")
    entry["collection_ids"] = ["capture-10", "capture-9", "capture-0011"]
    (row,) = rebuild("fec_reference_source_pages", store=archive, originals=[entry]).rows
    assert row["collection_id"] == "capture-9"


def facts_ctx(table, facts, url):
    """Fixture: the shared row rules over native events of one selected original."""
    entry = dict(source_url=url, source_sha256="sha256:" + "c" * 64, collection_ids=["capture"], source_authority="www.fec.gov")
    ctx = _Original(table, entry, "2026-09-11T00:00:00Z")
    ctx.parsing = {"facts": facts}
    return ctx


def event(kind, name=None, text=None, attrs=None):
    return dict(kind=kind, name=name, text=text, attributes=attrs or [])


def meetings(title="February 11 and 13, 2025  (Canceled)"):
    facts = [
        event("start", "h2"),
        event("text", text="Executive sessions"),
        event("end", "h2"),
        event("start", "table"),
        event("start", "tr"),
        event("start", "td"),
        event("start", "a", attrs=[["href", "/updates/session/"]]),
        event("text", text=title),
        event("end", "a"),
        event("end", "td"),
        event("start", "td"),
        event("start", "a", attrs=[["href", "/notice.pdf"]]),
        event("text", text="Sunshine Act Notice"),
        event("end", "a"),
        event("end", "td"),
        event("end", "tr"),
        event("end", "table"),
    ]
    return facts_ctx("fec_research_meeting_observations", facts, "https://www.fec.gov/meetings/")


def test_meeting_listing_preserves_cancellation_multiple_dates_and_notice_deferral():
    ctx = meetings()
    _meetings(ctx)
    (meeting,) = ctx.rows
    assert meeting["reported_status"] == "canceled" and meeting["meeting_type"] == "Executive sessions"
    assert json.loads(meeting["dates_json"]) == ["2025-02-11", "2025-02-13"]
    assert meeting["date_status"] == "source_listed_dates"
    assert json.loads(meeting["links_json"])[1]["body_status"] == "deferred_pdf"
    assert json.loads(meeting["links_json"])[0]["url"] == "https://www.fec.gov/updates/session/"


def test_meeting_date_range_is_not_collapsed_to_one_day():
    ctx = meetings("June 27-28, 2018 Public Hearing")
    _meetings(ctx)
    (meeting,) = ctx.rows
    assert meeting["date_status"] == "source_date_range"
    assert json.loads(meeting["dates_json"]) == ["2018-06-27", "2018-06-28"]
    assert meeting["reported_status"] == "not_stated"


def test_truncated_meeting_table_refuses():
    ctx = meetings()
    ctx.parsing["facts"].pop()
    with pytest.raises(ValueError, match="truncated"):
        _meetings(ctx)


def test_court_page_retains_native_text_and_deferred_links_without_legal_identity():
    facts = [
        event("text", text="Your web browser is not supported"),
        event("text", text="Federal Election Commission | United States of America"),
        event("text", text="Menu"),
        event("text", text="Federal Election Commission | United States of America"),
        event("start", "h1"),
        event("text", text="Fieger v. FEC"),
        event("end", "h1"),
        event("start", "script"),
        event("text", text="ignore this script"),
        event("end", "script"),
        event("start", "p"),
        event("text", text="The court issued its opinion."),
        event("end", "p"),
        event("start", "a", attrs=[["href", "/opinion.pdf"]]),
        event("text", text="Court opinion"),
        event("end", "a"),
        event("text", text="About\nCareers\nPress\nContact\nPrivacy and security policy"),
    ]
    ctx = facts_ctx("fec_reference_source_pages", facts, "https://www.fec.gov/legal-resources/court-cases/fieger-v-fec/")
    _source_page(ctx, "official_court_case_reference")
    (page,) = ctx.rows
    assert page["title"] == "Fieger v. FEC" and page["page_type"] == "official_court_case_reference"
    assert "The court issued its opinion." in page["text"] and "ignore this script" not in page["text"]
    assert json.loads(page["links_json"])[0]["body_status"] == "deferred_pdf"
    assert page["content_scope"] == "whole_original"
