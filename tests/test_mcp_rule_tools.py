"""Hermetic unit tests for ``prepare_comment`` and ``get_rule_text`` MCP rule tools."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone

import duckdb
import httpx
import pytest
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult

from spicy_regs import mcp_rule_tools, mcp_server
from spicy_regs.runtime_bounds import RequestBound

_NOW = datetime(2026, 10, 12, 16, 0, tzinfo=timezone.utc)

_GOVINFO_PAGE = """<html><head><title>Federal Register</title></head><body><pre>
[Federal Register Volume 91, Number 174 (Thursday, September 10, 2026)]
[Proposed Rules]
From the Federal Register Online via the Government Publishing Office [<a href="http://www.gpo.gov">www.gpo.gov</a>]
[FR Doc No: 2026-18481]

SUMMARY: The Census Bureau is proposing to add two new parts.

DATES: Comments must be received by October 13, 2026.

SUPPLEMENTARY INFORMATION:

    The preamble discusses the residence rule in
Sec.  60.4 at length, and &ldquo;usual residence&rdquo; generally.

[[Page 57529]]

PART 60--RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS OF THE
AMERICAN POPULATION

Sec.
60.1 Purpose.
60.4 Foreign citizens in the United States.

Sec.  60.1   Purpose.

    This part sets forth the criteria.


Sec.  60.4  Foreign citizens in the United States.

    (a) Citizens or lawful permanent residents. Counted at their usual
residence in accordance with

[[Page 57530]]

Sec.  60.2.
    (b) All other citizens of foreign countries. Not counted for
apportionment.

0
2. Amend Sec.  70.2 by revising paragraph (a) to read as follows:


Sec.  70.2  Restrictions on the inclusion of certain demographic
questions.

    (a) No question about race shall appear.

[FR Doc. 2026-18481 Filed 9-9-26; 8:45 am]
</pre></body></html>"""

_FR_BODY_HTML = """
<h1 id="h-1">SUMMARY:</h1>
<p id="p-1">The Census Bureau is proposing to add two new parts.</p>
<h1 id="h-2">DATES:</h1>
<p id="p-2">Comments must be received by October 13, 2026.</p>
<h1 id="h-3">SUPPLEMENTARY INFORMATION:</h1>
<p id="p-3">The preamble discusses &ldquo;usual residence&rdquo; and <a href="#p-7">§ 60.4</a> at length.</p>
<span class="printed-page-details unprinted-element">Start Printed Page 57529</span>
<h2 id="p-4">PART 60—RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS</h2>
<h3 id="p-5">§ 60.1 Purpose.</h3>
<p id="p-6">This part sets forth the criteria.</p>
<h3 id="p-7">§ 60.4 Foreign citizens in the United States.</h3>
<p id="p-8">(a) Citizens or lawful permanent residents. Counted at their usual residence in accordance with § 60.2.</p>
<p id="p-9">(b) All other citizens of foreign countries. Not counted for apportionment.</p>
<p id="p-10">2. Amend § 70.2 by revising paragraph (a) to read as follows:</p>
<h3 id="p-11">§ 70.2 Restrictions on the inclusion of certain demographic questions.</h3>
<p id="p-12">(a) No question about race shall appear.</p>
<script>console.log("ignored");</script>
"""


@pytest.fixture(autouse=True)
def _reset_caches(monkeypatch):
    mcp_server._reset_connection_cache()
    mcp_rule_tools._fetch_rule_text_with_source.cache_clear()
    monkeypatch.setattr(mcp_rule_tools, "_sleep", lambda _seconds: None)
    yield
    mcp_server._reset_connection_cache()
    mcp_rule_tools._fetch_rule_text_with_source.cache_clear()


_REAL_HTTPX_CLIENT = httpx.Client


def _patch_httpx(monkeypatch, handler):
    def client(*args, **kwargs):
        return _REAL_HTTPX_CLIENT(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(mcp_rule_tools.httpx, "Client", client)


# --- prepare_comment fixtures & tests ----------------------------------------


def _native_comment_connection(*, with_attributes: bool = True, as_view: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE dockets AS SELECT * FROM (VALUES "
        "('USBC-2026-0628', 'USBC', 'Decennial Census residence criteria'), "
        "('EPA-HQ-OAR-2001-0001', 'EPA', 'An old rule'), "
        "('DOE-2026-0010', 'DOE', 'Bare-date rule')) t(docket_id, agency_code, title)"
    )
    con.execute(
        "CREATE TABLE documents ("
        "document_id VARCHAR, docket_id VARCHAR, document_type VARCHAR, title VARCHAR, "
        "comment_start_date VARCHAR, comment_end_date VARCHAR, withdrawn BOOLEAN, publisher_status VARCHAR)"
    )
    con.executemany(
        "INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("USBC-2026-0628-0001", "USBC-2026-0628", "Proposed Rule", "Residence criteria",
             "2026-09-10T04:00:00Z", "2026-11-03T04:59:59Z", False, "listed"),
            ("USBC-2026-0628-0003", "USBC-2026-0628", "Notice", "Extension notice",
             "2026-09-20T04:00:00Z", "2026-10-20T03:59:59Z", None, None),
            ("USBC-2026-0628-0004", "USBC-2026-0628", "Notice", "Withdrawn boolean notice",
             "2026-09-20T04:00:00Z", "2026-12-01T04:59:59Z", True, "listed"),
            ("USBC-2026-0628-0005", "USBC-2026-0628", "Notice", "Not open yet",
             "2026-11-01T04:00:00Z", "2026-12-01T04:59:59Z", False, "listed"),
            ("USBC-2026-0628-0006", "USBC-2026-0628", "Proposed Rule", "Removed posting",
             "2026-09-10T04:00:00Z", "2026-12-15T04:59:59Z", False, "removed"),
            ("USBC-2026-0628-0002", "USBC-2026-0628", "Supporting & Related Material", "Impact analysis",
             "2026-09-10T04:00:00Z", None, False, "listed"),
            ("EPA-HQ-OAR-2001-0001-0001", "EPA-HQ-OAR-2001-0001", "Proposed Rule", "Old rule",
             "2001-01-01T05:00:00Z", "2001-03-01T04:59:59Z", False, None),
            ("DOE-2026-0010-0001", "DOE-2026-0010", "Proposed Rule", "Bare date window",
             "2026-10-01", "2026-10-12", False, None),
        ],
    )
    if with_attributes:
        kind = "VIEW" if as_view else "TABLE"
        con.execute(
            f"CREATE {kind} document_attributes AS SELECT * FROM (VALUES "
            "('USBC-2026-0628-0001', 'Notice of Proposed Rulemaking (NPRM)'), "
            "('USBC-2026-0628-0003', 'Extension of Comment Period'), "
            "('DOE-2026-0010-0001', NULL)"
            ") t(document_id, subtype)"
        )
    return con


def _legacy_string_comment_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE dockets AS SELECT 'USBC-2026-0628' AS docket_id, 'USBC' AS agency_code, 'Legacy' AS title"
    )
    con.execute(
        "CREATE TABLE documents AS SELECT * FROM (VALUES "
        "('USBC-2026-0628-0001', 'USBC-2026-0628', 'Proposed Rule', 'Open', "
        " '2026-09-10T04:00:00Z', '2026-11-03T04:59:59Z', 'false'), "
        "('USBC-2026-0628-0002', 'USBC-2026-0628', 'Notice', 'Withdrawn true', "
        " '2026-09-10T04:00:00Z', '2026-12-01T04:59:59Z', 'true'), "
        "('USBC-2026-0628-0003', 'USBC-2026-0628', 'Notice', 'Withdrawn 1', "
        " '2026-09-10T04:00:00Z', '2026-12-01T04:59:59Z', '1'), "
        "('USBC-2026-0628-0004', 'USBC-2026-0628', 'Notice', 'Withdrawn yes', "
        " '2026-09-10T04:00:00Z', '2026-12-01T04:59:59Z', 'YES')"
        ") t(document_id, docket_id, document_type, title, comment_start_date, comment_end_date, withdrawn)"
    )
    return con


@pytest.mark.parametrize("as_view", [False, True])
def test_prepare_comment_native_schema_enriches_subtype_and_filters_withdrawn_and_removed(as_view):
    con = _native_comment_connection(with_attributes=True, as_view=as_view)
    result = mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", today=_NOW)

    assert result["status"] == "open"
    assert [d["document_id"] for d in result["open_documents"]] == [
        "USBC-2026-0628-0001",
        "USBC-2026-0628-0003",
    ]
    first, second = result["open_documents"]
    assert first["subtype"] == "Notice of Proposed Rulemaking (NPRM)"
    assert second["subtype"] == "Extension of Comment Period"
    assert first["comment_url"] == "https://www.regulations.gov/commenton/USBC-2026-0628-0001"
    assert first["days_left"] == 21
    assert "draft_check" not in result
    assert "does not submit" in result["how_to_submit"]


def test_prepare_comment_omits_subtype_when_document_attributes_absent():
    con = _native_comment_connection(with_attributes=False)
    result = mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", today=_NOW)
    assert result["status"] == "open"
    assert "subtype" not in result["open_documents"][0]


def test_prepare_comment_handles_legacy_string_withdrawn_without_publisher_status():
    con = _legacy_string_comment_connection()
    result = mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", today=_NOW)
    assert result["status"] == "open"
    assert [d["document_id"] for d in result["open_documents"]] == ["USBC-2026-0628-0001"]


def test_prepare_comment_handles_bare_dates_and_date_today():
    con = _native_comment_connection(with_attributes=True)
    # On closing day 2026-10-12 at 16:00Z, bare end date 2026-10-12 stays open through 23:59:59Z.
    result = mcp_rule_tools.prepare_comment(con, "DOE-2026-0010", today=_NOW)
    assert result["status"] == "open"
    assert result["open_documents"][0]["document_id"] == "DOE-2026-0010-0001"
    assert result["open_documents"][0]["subtype"] is None
    assert result["open_documents"][0]["days_left"] == 0

    closed = mcp_rule_tools.prepare_comment(con, "DOE-2026-0010", today=date(2026, 10, 13))
    assert closed["status"] == "closed"
    assert closed["last_comment_deadline"] == "2026-10-12T23:59:59+00:00"


def test_prepare_comment_reports_closed_docket_with_last_deadline():
    con = _native_comment_connection()
    result = mcp_rule_tools.prepare_comment(con, "EPA-HQ-OAR-2001-0001", today=_NOW)

    assert result["status"] == "closed"
    assert result["open_documents"] == []
    assert result["last_comment_deadline"] == "2001-03-01T04:59:59+00:00"
    assert "comment_url" not in result["how_to_submit"]


def test_prepare_comment_unknown_docket():
    con = _native_comment_connection()
    result = mcp_rule_tools.prepare_comment(con, "NOPE-2026-0001", today=_NOW)
    assert "not in the Spicy Regs mirror" in result["error"]


@pytest.mark.parametrize("bad", ["", "   ", "x", "USBC'; DROP TABLE dockets; --", "USBC 2026 0628"])
def test_prepare_comment_rejects_malformed_ids(bad):
    con = _native_comment_connection()
    result = mcp_rule_tools.prepare_comment(con, bad, today=_NOW)
    assert "Not a docket ID" in result["error"]
    assert con.execute("SELECT count(*) FROM dockets").fetchone() == (3,)


def test_prepare_comment_checks_draft_length_chars_and_utf8_bytes():
    con = _native_comment_connection()
    fits = mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", "a" * 5000, today=_NOW)["draft_check"]
    assert fits == {"characters": 5000, "utf8_bytes": 5000, "limit": 5000, "fits_comment_box": True}

    too_long = mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", "a" * 5001, today=_NOW)["draft_check"]
    assert too_long["fits_comment_box"] is False
    assert "attached file" in too_long["advice"]

    multibyte = mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", "é" * 2600, today=_NOW)["draft_check"]
    assert multibyte["characters"] == 2600
    assert multibyte["utf8_bytes"] == 5200
    assert multibyte["fits_comment_box"] is False


def test_prepare_comment_respects_request_bound():
    con = _native_comment_connection()
    bound = RequestBound(30.0, "expired test bound")
    with pytest.raises(TimeoutError, match="expired test bound"):
        with bound.scope():
            bound.cancel()
            mcp_rule_tools.prepare_comment(con, "USBC-2026-0628", today=_NOW)


def test_prepare_comment_tool_runs_through_server_with_pinned_clock(monkeypatch):
    con = _native_comment_connection()
    monkeypatch.setattr(mcp_rule_tools, "_utc_now", lambda: _NOW)
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    server = mcp_server.build_server()

    result = asyncio.run(server.call_tool("prepare_comment", {"docket_id": "USBC-2026-0628", "draft": "Hello"}))
    assert isinstance(result, CallToolResult)
    data = result.structured_content
    assert isinstance(data, dict)
    assert data["status"] == "open"
    assert data["open_documents"][0]["subtype"] == "Notice of Proposed Rulemaking (NPRM)"
    assert data["open_documents"][0]["comment_url"] == "https://www.regulations.gov/commenton/USBC-2026-0628-0001"
    assert data["draft_check"]["fits_comment_box"] is True


# --- get_rule_text fixtures & tests ------------------------------------------


def _native_rule_connection(*, with_attributes: bool = True) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE documents AS SELECT * FROM (VALUES "
        "('USBC-2026-0628-0001', 'USBC-2026-0628', 'Proposed Rule', 'Residence criteria', "
        " '2026-09-10T04:00:00Z', '2026-18481', 'listed'), "
        "('USBC-2026-0628-0099', 'USBC-2026-0628', 'Notice', 'Comment extension', "
        " '2026-10-01T04:00:00Z', '2026-19999', 'listed'), "
        "('USBC-2026-0628-0098', 'USBC-2026-0628', 'Proposed Rule', 'Removed duplicate', "
        " '2026-10-05T04:00:00Z', '2026-19998', 'removed'), "
        "('USBC-2026-0628-0002', 'USBC-2026-0628', 'Supporting & Related Material', 'Analysis', "
        " '2026-09-10T04:00:00Z', NULL, 'listed'), "
        "('FDA_FRDOC_0001-0001', 'FDA_FRDOC_0001', 'Rule', 'Feed additive', "
        " '2026-10-14T04:00:00Z', '2026-20979', 'listed')"
        ") t(document_id, docket_id, document_type, title, posted_date, fr_doc_num, publisher_status)"
    )
    if with_attributes:
        con.execute(
            "CREATE TABLE document_attributes AS SELECT * FROM (VALUES "
            "('USBC-2026-0628-0001', 'Notice of Proposed Rulemaking (NPRM)'), "
            "('USBC-2026-0628-0099', 'Extension of Comment Period')"
            ") t(document_id, subtype)"
        )
    con.execute(
        """
        CREATE TABLE federal_register AS
        SELECT
            '2026-18481' AS document_number,
            'Decennial Census residence criteria' AS title,
            'Proposed Rule' AS document_type,
            '2026-09-10' AS publication_date,
            '2026-10-13' AS comments_close_on,
            'The Census Bureau is proposing to add two new parts.' AS abstract,
            [{'title': 15::BIGINT, 'part': '60', 'chapter': NULL::VARCHAR},
             {'title': 15::BIGINT, 'part': '70', 'chapter': NULL::VARCHAR}] AS cfr_references,
            ['0607-AA75'] AS regulation_id_numbers,
            'https://www.federalregister.gov/d/2026-18481' AS html_url,
            'https://www.federalregister.gov/documents/full_text/html/2026/09/10/2026-18481.html' AS body_html_url
        UNION ALL
        SELECT
            '2026-19999',
            'Comment extension',
            'Notice',
            '2026-10-01',
            NULL,
            NULL,
            []::STRUCT(title BIGINT, part VARCHAR, chapter VARCHAR)[],
            []::VARCHAR[],
            'https://www.federalregister.gov/d/2026-19999',
            NULL
        UNION ALL
        SELECT
            '2026-20979',
            'Feed additive',
            'Rule',
            '2026-10-14',
            NULL,
            'Feed additive amendment.',
            [{'title': 21::BIGINT, 'part': '573', 'chapter': 'I'}],
            ['0910-AI00'],
            'https://www.federalregister.gov/d/2026-20979',
            'https://www.federalregister.gov/documents/full_text/html/2026/10/14/2026-20979.html'
        """
    )
    return con


def _legacy_json_rule_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE documents AS SELECT * FROM (VALUES "
        "('USBC-2026-0628-0001', 'USBC-2026-0628', 'Proposed Rule', 'Residence criteria', "
        " '2026-09-10T04:00:00Z', '2026-18481')"
        ") t(document_id, docket_id, document_type, title, posted_date, fr_doc_num)"
    )
    con.execute(
        "CREATE TABLE federal_register AS SELECT * FROM (VALUES "
        "('2026-18481', 'Decennial Census residence criteria', 'Proposed Rule', '2026-09-10', '2026-10-13', "
        " 'The Census Bureau is proposing to add two new parts.', "
        ' \'[{"title": 15, "part": "60"}, {"title": 15, "part": "70"}]\', \'["0607-AA75"]\', '
        " 'https://www.federalregister.gov/d/2026-18481')"
        ") t(document_number, title, document_type, publication_date, comments_close_on, abstract, "
        "cfr_references_json, regulation_id_numbers_json, html_url)"
    )
    return con


def _fixture_fetch(calls: list[tuple[str, str]] | None = None):
    def fetch(number: str, published: str) -> str:
        if calls is not None:
            calls.append((number, published))
        return mcp_rule_tools._govinfo_text(_GOVINFO_PAGE)

    return fetch


def test_get_rule_text_outline_from_docket_prefers_proposed_rule_and_enriches_subtype():
    calls: list[tuple[str, str]] = []
    result = mcp_rule_tools.get_rule_text(
        _native_rule_connection(with_attributes=True),
        "USBC-2026-0628",
        fetch=_fixture_fetch(calls),
    )

    assert calls == [("2026-18481", "2026-09-10")]
    assert result["mode"] == "outline"
    assert result["fr_document_number"] == "2026-18481"
    assert result["rins"] == ["0607-AA75"]
    assert [c["part"] for c in result["cfr_references"]] == ["60", "70"]
    assert result["comments_close_on_in_notice"] == "2026-10-13"
    assert result["summary"] == "The Census Bureau is proposing to add two new parts."
    assert result["dates"] == "Comments must be received by October 13, 2026."
    assert result["parts"] == [
        {"part": "60", "title": "RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS OF THE AMERICAN POPULATION"}
    ]
    assert [(s["section"], s["title"]) for s in result["sections"]] == [
        ("60.1", "Purpose."),
        ("60.4", "Foreign citizens in the United States."),
        ("70.2", "Restrictions on the inclusion of certain demographic questions."),
    ]
    assert result["other_notices_in_docket"] == [
        {
            "document_id": "USBC-2026-0628-0099",
            "document_type": "Notice",
            "subtype": "Extension of Comment Period",
            "title": "Comment extension",
            "posted_date": "2026-10-01T04:00:00Z",
            "fr_doc_num": "2026-19999",
        }
    ]
    assert result["source"] == "https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm"
    assert "source_fallback" not in result


def test_get_rule_text_supports_legacy_json_federal_register_schema():
    result = mcp_rule_tools.get_rule_text(
        _legacy_json_rule_connection(),
        "USBC-2026-0628",
        fetch=_fixture_fetch(),
    )
    assert result["fr_document_number"] == "2026-18481"
    assert result["cfr_references"] == [{"title": 15, "part": "60"}, {"title": 15, "part": "70"}]
    assert result["rins"] == ["0607-AA75"]
    assert result["mode"] == "outline"


@pytest.mark.parametrize("ref", ["USBC-2026-0628-0001", "2026-18481", "FR Doc. 2026-18481"])
def test_get_rule_text_accepts_document_ids_and_fr_numbers(ref):
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), ref, fetch=_fixture_fetch())
    assert result["fr_document_number"] == "2026-18481"
    assert "other_notices_in_docket" not in result


def test_get_rule_text_allows_specific_document_in_catchall_docket_but_refuses_docket():
    con = _native_rule_connection()
    refused = mcp_rule_tools.get_rule_text(con, "FDA_FRDOC_0001", fetch=_fixture_fetch())
    assert "catch-all docket" in refused["error"]

    allowed = mcp_rule_tools.get_rule_text(con, "FDA_FRDOC_0001-0001", fetch=_fixture_fetch())
    assert allowed["fr_document_number"] == "2026-20979"
    assert allowed["mode"] == "outline"


@pytest.mark.parametrize("section", ["60.4", "§ 60.4", "Sec. 60.4", "section 60.4"])
def test_get_rule_text_returns_one_section(section):
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), "USBC-2026-0628", section, fetch=_fixture_fetch())
    text = result["text"]
    assert text.startswith("Sec.  60.4  Foreign citizens in the United States.")
    assert text.endswith("Not counted for\napportionment.")
    assert "[[Page" not in text
    assert "Amend" not in text
    assert "70.2" not in text


def test_get_rule_text_ignores_inline_section_mentions_and_strips_markup():
    con = _native_rule_connection()
    outline = mcp_rule_tools.get_rule_text(con, "USBC-2026-0628", fetch=_fixture_fetch())
    sections = [s["section"] for s in outline["sections"]]
    assert sections.count("60.4") == 1
    assert "60.2" not in sections

    full = mcp_rule_tools.get_rule_text(con, "2026-18481", "full", max_chars=20000, fetch=_fixture_fetch())["text"]
    assert "<a" not in full and "&ldquo;" not in full
    assert "“usual residence”" in full
    assert "[[Page" not in full


def test_get_rule_text_returns_part_and_pages_full_text():
    con = _native_rule_connection()
    part_text = mcp_rule_tools.get_rule_text(con, "2026-18481", "part 60", fetch=_fixture_fetch())["text"]
    assert part_text.startswith("PART 60--RESIDENCE CRITERIA")
    assert "No question about race" in part_text
    assert "[FR Doc." not in part_text

    first = mcp_rule_tools.get_rule_text(con, "2026-18481", "full", max_chars=500, fetch=_fixture_fetch())
    second = mcp_rule_tools.get_rule_text(
        con, "2026-18481", "full", offset=first["next_offset"], max_chars=500, fetch=_fixture_fetch()
    )
    assert len(first["text"]) == 500
    assert first["next_offset"] == 500
    assert second["offset"] == 500
    assert (
        first["text"] + second["text"]
        == mcp_rule_tools.get_rule_text(con, "2026-18481", "full", max_chars=1000, fetch=_fixture_fetch())["text"]
    )
    last = mcp_rule_tools.get_rule_text(
        con, "2026-18481", "full", offset=first["total_chars"] - 10, max_chars=500, fetch=_fixture_fetch()
    )
    assert last["next_offset"] is None


@pytest.mark.parametrize(
    "ref, message",
    [
        ("x'; DROP TABLE documents; --", "Not a docket, document, or Federal Register number"),
        ("FDA_FRDOC_0001", "catch-all docket"),
        ("EPA_FRDOC_0001", "catch-all docket"),
        ("NOPE-2026-0001", "No Federal Register notice is linked"),
        ("2001-12345", "not in the mirror"),
    ],
)
def test_get_rule_text_explains_unresolvable_references(ref, message):
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), ref, fetch=_fixture_fetch())
    assert message in result["error"]


def test_get_rule_text_lists_valid_sections_and_validates_args():
    con = _native_rule_connection()
    missing_sec = mcp_rule_tools.get_rule_text(con, "2026-18481", "99.9", fetch=_fixture_fetch())
    assert "No section 99.9" in missing_sec["error"] and "60.4" in missing_sec["error"]

    missing_part = mcp_rule_tools.get_rule_text(con, "2026-18481", "part 99", fetch=_fixture_fetch())
    assert "No part 99" in missing_part["error"] and "60" in missing_part["error"]

    bad_section = mcp_rule_tools.get_rule_text(con, "2026-18481", "preamble", fetch=_fixture_fetch())
    assert "section must be empty" in bad_section["error"]

    for kwargs in ({"max_chars": 10}, {"max_chars": 50_000}, {"offset": -1}):
        assert "error" in mcp_rule_tools.get_rule_text(con, "2026-18481", "full", fetch=_fixture_fetch(), **kwargs)


# --- HTTP resilience, retries, fallback, and safety guards -------------------


def test_govinfo_502_retries_then_succeeds(monkeypatch):
    attempts = 0
    seen_ua: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        seen_ua.append(request.headers.get("user-agent", ""))
        if attempts < 3:
            return httpx.Response(502)
        return httpx.Response(200, text=_GOVINFO_PAGE)

    _patch_httpx(monkeypatch, handler)
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), "2026-18481", "60.4")

    assert attempts == 3
    assert all(ua == mcp_rule_tools.USER_AGENT for ua in seen_ua)
    assert "Not counted for" in result["text"]
    assert result["source"] == "https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm"
    assert "source_fallback" not in result


def test_govinfo_transport_error_retries_then_succeeds(monkeypatch):
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("transient network drop", request=request)
        return httpx.Response(200, text=_GOVINFO_PAGE)

    _patch_httpx(monkeypatch, handler)
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), "2026-18481")

    assert attempts == 2
    assert result["mode"] == "outline"
    assert "source_fallback" not in result


def test_govinfo_persistent_502_falls_back_to_body_html_url(monkeypatch):
    seen_hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_hosts.append(request.url.host)
        if request.url.host == "www.govinfo.gov":
            return httpx.Response(502)
        if request.url.host == "www.federalregister.gov":
            return httpx.Response(200, text=_FR_BODY_HTML)
        raise AssertionError(f"Unexpected host: {request.url.host}")

    _patch_httpx(monkeypatch, handler)
    con = _native_rule_connection()

    outline = mcp_rule_tools.get_rule_text(con, "2026-18481")
    assert seen_hosts == ["www.govinfo.gov", "www.govinfo.gov", "www.govinfo.gov", "www.federalregister.gov"]
    assert outline["mode"] == "outline"
    assert outline["source"] == "https://www.federalregister.gov/documents/full_text/html/2026/09/10/2026-18481.html"
    assert outline["source_fallback"] == "federal_register_body_html"
    assert outline["dates"] == "Comments must be received by October 13, 2026."
    assert outline["parts"] == [{"part": "60", "title": "RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS"}]
    assert [s["section"] for s in outline["sections"]] == ["60.1", "60.4", "70.2"]

    # Subsequent section read hits the LRU cache without re-fetching.
    sec = mcp_rule_tools.get_rule_text(con, "2026-18481", "60.4")
    assert len(seen_hosts) == 4
    assert sec["source_fallback"] == "federal_register_body_html"
    assert sec["text"].startswith("§ 60.4 Foreign citizens in the United States.")
    assert "Not counted for apportionment." in sec["text"]
    assert "Amend" not in sec["text"]
    assert "Start Printed Page" not in sec["text"]
    assert "console.log" not in sec["text"]


def test_govinfo_404_does_not_retry_and_falls_back_to_body_html_url(monkeypatch):
    govinfo_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal govinfo_calls
        if request.url.host == "www.govinfo.gov":
            govinfo_calls += 1
            return httpx.Response(404)
        return httpx.Response(200, text=_FR_BODY_HTML)

    _patch_httpx(monkeypatch, handler)
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), "2026-18481", "60.4")
    assert govinfo_calls == 1
    assert result["source_fallback"] == "federal_register_body_html"
    assert "Not counted for apportionment." in result["text"]


def test_fallback_rejects_federal_register_captcha_page(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.govinfo.gov":
            return httpx.Response(404)
        return httpx.Response(
            200,
            text="<html><head><title>Federal Register :: Request Access</title></head><body>CAPTCHA</body></html>",
        )

    _patch_httpx(monkeypatch, handler)
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), "2026-18481")
    assert "error" in result
    assert "CAPTCHA" in result["error"]
    assert result["federal_register_url"] == "https://www.federalregister.gov/d/2026-18481"


def test_govinfo_404_without_body_html_url_returns_readable_error_without_retry(monkeypatch):
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    _patch_httpx(monkeypatch, handler)
    result = mcp_rule_tools.get_rule_text(_legacy_json_rule_connection(), "2026-18481")
    assert calls == 1
    assert "no text" in result["error"]
    assert result["federal_register_url"] == "https://www.federalregister.gov/d/2026-18481"


def test_fetch_refuses_redirect_off_host_without_contacting_target(monkeypatch):
    contacted_hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        contacted_hosts.append(request.url.host)
        if request.url.host == "www.govinfo.gov":
            return httpx.Response(302, headers={"Location": "https://evil.example/steal"})
        raise AssertionError("Off-host redirect target must never be contacted")

    _patch_httpx(monkeypatch, handler)
    with pytest.raises(mcp_rule_tools.RuleTextError, match="off its own host"):
        mcp_rule_tools._fetch_url_text("https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm")
    assert contacted_hosts == ["www.govinfo.gov"]


def test_fallback_refuses_off_host_redirect_and_untrusted_body_html_url(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "www.govinfo.gov":
            return httpx.Response(404)
        if request.url.host == "www.federalregister.gov":
            return httpx.Response(302, headers={"Location": "https://evil.example/x"})
        raise AssertionError("Off-host target contacted")

    _patch_httpx(monkeypatch, handler)
    result = mcp_rule_tools.get_rule_text(_native_rule_connection(), "2026-18481")
    assert "off its own host" in result["error"]

    # Non-federalregister.gov body_html_url must never be fetched.
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE federal_register AS SELECT "
        "'2026-18481' AS document_number, 'Title' AS title, 'Rule' AS document_type, "
        "'2026-09-10' AS publication_date, NULL AS comments_close_on, NULL AS abstract, "
        "'[]' AS cfr_references_json, '[]' AS regulation_id_numbers_json, "
        "'https://www.federalregister.gov/d/2026-18481' AS html_url, "
        "'https://evil.example/doc.html' AS body_html_url"
    )
    mcp_rule_tools._fetch_rule_text_with_source.cache_clear()
    seen: list[str] = []

    def handler2(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.host)
        return httpx.Response(404)

    _patch_httpx(monkeypatch, handler2)
    result2 = mcp_rule_tools.get_rule_text(con, "2026-18481")
    assert seen == ["www.govinfo.gov"]
    assert "no text" in result2["error"]


def test_fetch_caps_download_size(monkeypatch):
    monkeypatch.setattr(mcp_rule_tools, "RULE_TEXT_MAX_BYTES", 100)
    _patch_httpx(monkeypatch, lambda _request: httpx.Response(200, content=b"x" * 1000))
    with pytest.raises(mcp_rule_tools.RuleTextError, match="larger than"):
        mcp_rule_tools._fetch_url_text("https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm")


def test_fetch_retries_stop_on_cancelled_request_bound(monkeypatch):
    bound = RequestBound(30.0, "retry bound exceeded")
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        bound.cancel()
        return httpx.Response(502)

    _patch_httpx(monkeypatch, handler)
    with pytest.raises(TimeoutError, match="retry bound exceeded"):
        with bound.scope():
            mcp_rule_tools._fetch_url_text("https://www.govinfo.gov/content/pkg/FR-2026-09-10/html/2026-18481.htm")
    assert attempts == 1


def test_get_rule_text_tool_runs_through_server_and_enforces_strict_bounds(monkeypatch):
    con = _native_rule_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    _patch_httpx(monkeypatch, lambda _request: httpx.Response(200, text=_GOVINFO_PAGE))
    server = mcp_server.build_server()

    result = asyncio.run(server.call_tool("get_rule_text", {"document": "USBC-2026-0628", "section": "60.4"}))
    assert isinstance(result, CallToolResult)
    data = result.structured_content
    assert isinstance(data, dict)
    assert "Not counted for" in data["text"]

    with pytest.raises(ToolError, match="max_chars"):
        asyncio.run(server.call_tool("get_rule_text", {"document": "2026-18481", "max_chars": 10}))
    with pytest.raises(ToolError, match="offset"):
        asyncio.run(server.call_tool("get_rule_text", {"document": "2026-18481", "offset": -1}))


# --- Offline-first get_rule_text (federal_register_document_bodies + EOs) ----


def _offline_body_connection() -> duckdb.DuckDBPyConnection:
    con = _native_rule_connection()
    con.execute(
        """
        INSERT INTO federal_register
        SELECT
            '2025-01901',
            'Executive Order on Regulatory Review',
            'Presidential Document',
            '2025-01-28',
            NULL,
            'Establishes regulatory review principles.',
            []::STRUCT(title BIGINT, part VARCHAR, chapter VARCHAR)[],
            []::VARCHAR[],
            'https://www.federalregister.gov/d/2025-01901',
            'https://www.federalregister.gov/documents/full_text/html/2025/01/28/2025-01901.html'
        """
    )
    # Single-newline XML-derived body text matching spicy_docs.extraction.body_text output
    xml_derived_rule_text = (
        "DEPARTMENT OF COMMERCE\n"
        "Decennial Census residence criteria\n"
        "SUMMARY:\n"
        "The Census Bureau is proposing to add two new parts.\n"
        "DATES:\n"
        "Comments must be received by October 13, 2026.\n"
        "SUPPLEMENTARY INFORMATION:\n"
        "Preamble text discussing the proposed rule.\n"
        "1. Add part 60 to read as follows:\n"
        "PART 60—RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS\n"
        "§ 60.1\n"
        "Purpose.\n"
        "This part sets forth the criteria.\n"
        "§ 60.4\n"
        "Foreign citizens in the United States.\n"
        "(a) Citizens or lawful permanent residents.\n"
        "[FR Doc. 2026-18481 Filed 9-9-26; 8:45 am]"
    )
    con.execute(
        """
        CREATE TABLE federal_register_document_bodies (
            document_number VARCHAR,
            publication_date DATE,
            document_type VARCHAR,
            title VARCHAR,
            significant BOOLEAN,
            source_record_id VARCHAR,
            source_sha256 VARCHAR,
            body_status VARCHAR,
            body_format VARCHAR,
            body_url VARCHAR,
            body_sha256 VARCHAR,
            text_sha256 VARCHAR,
            body_observed_at VARCHAR,
            text_derivation VARCHAR,
            text_derivation_version VARCHAR,
            body_failure_reason VARCHAR,
            body_text VARCHAR
        )
        """
    )
    con.executemany(
        "INSERT INTO federal_register_document_bodies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "2026-18481",
                date(2026, 9, 10),
                "Proposed Rule",
                "Decennial Census residence criteria",
                True,
                "2026-18481@2026-09-10",
                "sha256:" + "1" * 64,
                "acquired",
                "xml",
                "https://www.federalregister.gov/documents/full_text/xml/2026/09/10/2026-18481.xml",
                "sha256:" + "a" * 64,
                "sha256:" + "b" * 64,
                "2026-11-28T00:00:00Z",
                "markup-reader",
                "006",
                None,
                xml_derived_rule_text,
            ),
            (
                "2026-20979",
                date(2026, 10, 14),
                "Rule",
                "Feed additive",
                True,
                "2026-20979@2026-10-14",
                "sha256:" + "2" * 64,
                "refused",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "GovInfo returned 404",
                None,
            ),
        ],
    )
    eo_text = (
        "Executive Order 14199 of January 20, 2025\n"
        "Executive Order on Regulatory Review\n"
        "By the authority vested in me as President by the Constitution.\n"
        "[FR Doc. 2025-01901 Filed 1-27-25; 8:45 am]"
    )
    con.execute(
        """
        CREATE TABLE executive_order_observations (
            document_number VARCHAR,
            publication_date DATE,
            body_status VARCHAR,
            body_url VARCHAR,
            body_sha256 VARCHAR,
            text_sha256 VARCHAR,
            body_text VARCHAR
        )
        """
    )
    con.execute(
        "INSERT INTO executive_order_observations VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "2025-01901",
            date(2025, 1, 28),
            "acquired",
            "https://www.federalregister.gov/documents/full_text/xml/2025/01/28/2025-01901.xml",
            "sha256:" + "c" * 64,
            "sha256:" + "d" * 64,
            eo_text,
        ],
    )
    return con


def test_get_rule_text_reads_significant_rule_offline_with_zero_http(monkeypatch):
    def forbid_http(*_args, **_kwargs):
        raise AssertionError("Outbound HTTP must not be called for offline acquired rule")

    monkeypatch.setattr(mcp_rule_tools.httpx, "Client", forbid_http)
    con = _offline_body_connection()

    outline = mcp_rule_tools.get_rule_text(con, "2026-18481")
    assert outline["mode"] == "outline"
    assert outline["source_mode"] == "offline_table"
    assert outline["source"] == "https://www.federalregister.gov/documents/full_text/xml/2026/09/10/2026-18481.xml"
    assert outline["body_sha256"] == "sha256:" + "a" * 64
    assert outline["text_sha256"] == "sha256:" + "b" * 64
    assert outline["dates"] == "Comments must be received by October 13, 2026."
    assert outline["parts"] == [{"part": "60", "title": "RESIDENCE CRITERIA FOR THE DECENNIAL CENSUS"}]
    assert [(s["section"], s["title"]) for s in outline["sections"]] == [
        ("60.1", "Purpose."),
        ("60.4", "Foreign citizens in the United States."),
    ]
    assert "source_fallback" not in outline

    sec = mcp_rule_tools.get_rule_text(con, "2026-18481", "60.4")
    assert sec["source_mode"] == "offline_table"
    assert "Citizens or lawful permanent residents." in sec["text"]
    assert "60.1" not in sec["text"]

    full = mcp_rule_tools.get_rule_text(con, "2026-18481", "full")
    assert full["source_mode"] == "offline_table"
    assert "DEPARTMENT OF COMMERCE" in full["text"]


def test_get_rule_text_reads_executive_order_offline_with_zero_http(monkeypatch):
    def forbid_http(*_args, **_kwargs):
        raise AssertionError("Outbound HTTP must not be called for offline acquired executive order")

    monkeypatch.setattr(mcp_rule_tools.httpx, "Client", forbid_http)
    con = _offline_body_connection()

    outline = mcp_rule_tools.get_rule_text(con, "2025-01901")
    assert outline["mode"] == "outline"
    assert outline["source_mode"] == "offline_table"
    assert outline["source"] == "https://www.federalregister.gov/documents/full_text/xml/2025/01/28/2025-01901.xml"
    assert outline["body_sha256"] == "sha256:" + "c" * 64
    assert outline["text_sha256"] == "sha256:" + "d" * 64

    full = mcp_rule_tools.get_rule_text(con, "2025-01901", "full")
    assert full["source_mode"] == "offline_table"
    assert "authority vested in me as President" in full["text"]


def test_get_rule_text_falls_back_live_for_out_of_scope_notice_and_refused_body(monkeypatch):
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return httpx.Response(200, text=_GOVINFO_PAGE)

    _patch_httpx(monkeypatch, handler)
    con = _offline_body_connection()

    # Out-of-scope Notice (not in federal_register_document_bodies) -> live GovInfo fallback
    notice = mcp_rule_tools.get_rule_text(con, "2026-19999", "full")
    assert "source_mode" not in notice
    assert "body_sha256" not in notice
    assert notice["source"] == "https://www.govinfo.gov/content/pkg/FR-2026-10-01/html/2026-19999.htm"
    assert any("2026-19999" in u for u in seen_urls)

    # Refused row (body_status='refused', body_text=NULL) -> live GovInfo fallback
    refused = mcp_rule_tools.get_rule_text(con, "2026-20979", "full")
    assert "source_mode" not in refused
    assert any("2026-20979" in u for u in seen_urls)


def test_mcp_server_get_rule_text_requests_offline_body_tables_and_serves_zero_http(monkeypatch):
    con = _offline_body_connection()
    observed_relations: list[set[str] | None] = []

    def get_con():
        observed_relations.append(mcp_server._requested_relations.get())
        return con

    def forbid_http(*_args, **_kwargs):
        raise AssertionError("Outbound HTTP must not be called when offline table holds body")

    monkeypatch.setattr(mcp_server, "_get_connection", get_con)
    monkeypatch.setattr(mcp_rule_tools.httpx, "Client", forbid_http)
    server = mcp_server.build_server()

    result = asyncio.run(server.call_tool("get_rule_text", {"document": "2026-18481", "section": "full"}))
    assert isinstance(result, CallToolResult)
    data = result.structured_content
    assert isinstance(data, dict)
    assert data["source_mode"] == "offline_table"
    assert "DEPARTMENT OF COMMERCE" in data["text"]
    assert observed_relations and observed_relations[0] is not None
    assert {"federal_register_document_bodies", "executive_order_observations"} <= observed_relations[0]


def test_get_rule_text_xml_preamble_inline_mentions_do_not_hijack_sections(monkeypatch):
    """BLOCKER 2 regression: real SpicyDocs XML body_text preamble <E>/<PRTPAGE/> mentions never hijack sections."""
    from types import SimpleNamespace

    from spicy_docs.extraction.body_text import body_text
    from spicy_docs.transport.captured import CapturedBodyResponse

    xml_bytes = (
        b"<PRORULE>"
        b"<PREAMB>"
        b'<AGENCY TYPE="S">DEPARTMENT OF COMMERCE</AGENCY>'
        b"<SUBJECT>Decennial Census residence criteria</SUBJECT>"
        b'<SUM><HD SOURCE="HED">SUMMARY:</HD><P>The Census Bureau is proposing to add two new parts.</P></SUM>'
        b'<EFFDATE><HD SOURCE="HED">DATES:</HD><P>Comments must be received by October 13, 2026.</P></EFFDATE>'
        b"</PREAMB>"
        b"<SUPLINF>"
        b'<HD SOURCE="HED">SUPPLEMENTARY INFORMATION:</HD>'
        b'<P>In the preamble we discuss <E T="03">&#xA7; 60.4</E> and its effect.</P>'
        b'<P>See also <PRTPAGE P="57529"/>&#xA7; 60.2 for cross reference.</P>'
        b'<REGTEXT TITLE="15" PART="60">'
        b"<AMDPAR>1. Add part 60 to read as follows:</AMDPAR>"
        b'<PART><HD SOURCE="HED">PART 60&#x2014;RESIDENCE CRITERIA</HD>'
        b"<SECTION><SECTNO>&#xA7; 60.1</SECTNO><SUBJECT>Purpose.</SUBJECT>"
        b"<P>This part sets forth the criteria.</P></SECTION>"
        b"<SECTION><SECTNO>&#xA7; 60.4</SECTNO><SUBJECT>Foreign citizens in the United States.</SUBJECT>"
        b"<P>(a) Citizens or lawful permanent residents. Counted at usual residence.</P></SECTION>"
        b"</PART>"
        b"</REGTEXT>"
        b"</SUPLINF>"
        b"<FRDOC>[FR Doc. 2026-18481 Filed 9-9-26; 8:45 am]</FRDOC>"
        b"</PRORULE>"
    )
    url = "https://www.federalregister.gov/documents/full_text/xml/2026/09/10/2026-18481.xml"
    capture = CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type="application/xml",
        observed_at="2026-11-28T00:00:00Z",
        content_encoding="identity",
        method="GET",
        body=xml_bytes,
    )
    derived = body_text(
        SimpleNamespace(
            format="xml",
            body=SimpleNamespace(media_type="application/xml", byte_size=capture.byte_size),
            body_capture=capture,
        )
    )
    assert "\n\n" not in derived.text  # exercises the single-newline XML body_text branch

    con = _offline_body_connection()
    con.execute(
        "UPDATE federal_register_document_bodies SET body_text = ? WHERE document_number = '2026-18481'",
        [derived.text],
    )

    def forbid_http(*_args, **_kwargs):
        raise AssertionError("Outbound HTTP must not be called for offline XML body")

    monkeypatch.setattr(mcp_rule_tools.httpx, "Client", forbid_http)

    outline = mcp_rule_tools.get_rule_text(con, "2026-18481")
    assert outline["source_mode"] == "offline_table"
    assert outline["dates"] == "Comments must be received by October 13, 2026."
    assert outline["parts"] == [{"part": "60", "title": "RESIDENCE CRITERIA"}]
    assert [(s["section"], s["title"], s["part"]) for s in outline["sections"]] == [
        ("60.1", "Purpose.", "60"),
        ("60.4", "Foreign citizens in the United States.", "60"),
    ]

    sec_60_4 = mcp_rule_tools.get_rule_text(con, "2026-18481", "60.4")
    assert sec_60_4["source_mode"] == "offline_table"
    assert "Foreign citizens in the United States." in sec_60_4["text"]
    assert "Counted at usual residence." in sec_60_4["text"]
    assert "and its effect" not in sec_60_4["text"]

    missing_60_2 = mcp_rule_tools.get_rule_text(con, "2026-18481", "60.2")
    assert "No section 60.2" in missing_60_2["error"]


def test_labeled_paragraph_preserves_wrapped_note_prose_in_govinfo_pre():
    """WARNING 1 regression: wrapped NOTE:/OMB: lines inside GovInfo <pre> paragraphs are never truncated."""
    govinfo_with_note = (
        "SUMMARY: The agency proposes an emissions standard.\n"
        "NOTE: Additional compliance detail continues the summary.\n"
        "OMB: Control number 2060-0001 applies to this collection.\n"
        "\n"
        "DATES: Comments are due October 13, 2026.\n"
        "NOTE: Late comments may be considered to the extent practicable.\n"
        "\n"
        "SUPPLEMENTARY INFORMATION:\n"
        "Further background."
    )
    summary = mcp_rule_tools._labeled_paragraph(govinfo_with_note, "SUMMARY")
    assert summary is not None
    assert "NOTE: Additional compliance detail continues the summary." in summary
    assert "OMB: Control number 2060-0001 applies to this collection." in summary
    assert "DATES:" not in summary

    dates = mcp_rule_tools._labeled_paragraph(govinfo_with_note, "DATES")
    assert dates is not None
    assert "NOTE: Late comments may be considered to the extent practicable." in dates
    assert "SUPPLEMENTARY INFORMATION:" not in dates


def test_get_rule_text_matches_composite_document_number_and_publication_date(monkeypatch):
    """Offline body lookup binds both document_number and publication_date for reused FR document numbers."""
    con = _offline_body_connection()
    con.execute(
        "INSERT INTO federal_register_document_bodies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            "2026-18481",
            date(2026, 1, 5),
            "Proposed Rule",
            "Older printing with same document number",
            True,
            "2026-18481@2026-01-05",
            "sha256:" + "0" * 64,
            "acquired",
            "xml",
            "https://www.federalregister.gov/documents/full_text/xml/2026/01/05/2026-18481.xml",
            "sha256:" + "e" * 64,
            "sha256:" + "f" * 64,
            "2026-01-06T00:00:00Z",
            "markup-reader",
            "006",
            None,
            "STALE OLD PRINTING TEXT THAT MUST NOT BE RETURNED",
        ],
    )

    def forbid_http(*_args, **_kwargs):
        raise AssertionError("Outbound HTTP must not be called")

    monkeypatch.setattr(mcp_rule_tools.httpx, "Client", forbid_http)
    full = mcp_rule_tools.get_rule_text(con, "2026-18481", "full")
    assert full["publication_date"] == "2026-09-10"
    assert full["source"] == "https://www.federalregister.gov/documents/full_text/xml/2026/09/10/2026-18481.xml"
    assert full["body_sha256"] == "sha256:" + "a" * 64
    assert "DEPARTMENT OF COMMERCE" in full["text"]
    assert "STALE OLD PRINTING" not in full["text"]


def test_get_rule_text_pre_publish_build_connection_falls_back_cleanly(tmp_path, monkeypatch):
    """Pre-publish: real _build_connection skips absent managed_only body tables and falls back to live GovInfo."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication as pub
    from tests.generation_fakes import Store
    from tests.test_generation_mcp import connection_fixture
    from tests.test_generation_publication import publish

    fr_table = pa.Table.from_pylist(
        [
            {
                "document_number": "2026-18481",
                "title": "Decennial Census residence criteria",
                "document_type": "Proposed Rule",
                "publication_date": "2026-09-10",
                "comments_close_on": "2026-10-13",
                "abstract": "The Census Bureau is proposing to add two new parts.",
                "cfr_references": [{"title": 15, "part": "60", "chapter": None}],
                "regulation_id_numbers": ["0607-AA75"],
                "html_url": "https://www.federalregister.gov/d/2026-18481",
                "body_html_url": "https://www.federalregister.gov/documents/full_text/html/2026/09/10/2026-18481.html",
            }
        ],
        schema=pa.schema(
            [
                ("document_number", pa.string()),
                ("title", pa.string()),
                ("document_type", pa.string()),
                ("publication_date", pa.string()),
                ("comments_close_on", pa.string()),
                ("abstract", pa.string()),
                (
                    "cfr_references",
                    pa.list_(pa.struct([("title", pa.int64()), ("part", pa.string()), ("chapter", pa.string())])),
                ),
                ("regulation_id_numbers", pa.list_(pa.string())),
                ("html_url", pa.string()),
                ("body_html_url", pa.string()),
            ]
        ),
    )
    src = tmp_path / "src"
    src.mkdir()
    fr_file = src / "federal_register.parquet"
    pq.write_table(fr_table, fr_file)
    gen_dir = tmp_path / "gen"
    build_generation(gen_dir, family="federal-register", files=[fr_file], expected_keys=["federal_register.parquet"])
    index = publish(Store(), gen_dir)
    fr_member = pub.single_member(index, "federal_register.parquet").path

    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    built = connection_fixture(
        monkeypatch,
        index,
        {fr_member: gen_dir / "federal_register.parquet"},
        tables=("federal_register", "federal_register_document_bodies", "executive_order_observations"),
    )
    _patch_httpx(monkeypatch, lambda _request: httpx.Response(200, text=_GOVINFO_PAGE))

    server = mcp_server.build_server()
    try:
        result = asyncio.run(server.call_tool("get_rule_text", {"document": "2026-18481", "section": "60.4"}))
        assert isinstance(result, CallToolResult)
        data = result.structured_content
        assert isinstance(data, dict)
        assert "error" not in data
        assert "source_mode" not in data
        assert "Not counted for" in data["text"]
        assert built
        assert not any("federal_register_document_bodies" in url for url in built[0].seen)
    finally:
        for con in built:
            con.close()
