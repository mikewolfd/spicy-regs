"""Offline unit tests for ``spicy_regs.pipelines.appropriations_deadlines``."""

from __future__ import annotations

import csv
from datetime import date
import hashlib
import json
from pathlib import Path
import socket

import httpx
import pytest

from spicy_regs.pipelines import appropriations_deadlines as ad


def _make_pdf(lines: list[str]) -> bytes:
    """Build a minimal valid single-page PDF with extractable Helvetica text."""
    escaped = [line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)") for line in lines]
    stream_lines = ["BT", "/F1 12 Tf", "72 720 Td"]
    for idx, line in enumerate(escaped):
        if idx > 0:
            stream_lines.append("0 -16 Td")
        stream_lines.append(f"({line}) Tj")
    stream_lines.append("ET")
    stream = "\n".join(stream_lines).encode("latin-1")
    objs = [
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n",
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n",
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >> endobj\n",
        f"4 0 obj << /Length {len(stream)} >> stream\n".encode("ascii") + stream + b"\nendstream endobj\n",
        b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objs:
        offsets.append(len(out))
        out.extend(obj)
    xref_pos = len(out)
    out.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode("ascii"))
    for off in offsets[1:]:
        out.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    out.extend(f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode("ascii"))
    return bytes(out)


@pytest.fixture(autouse=True)
def _block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def _deny(*args: object, **kwargs: object) -> None:
        raise AssertionError("Offline appropriations_deadlines tests must not open network sockets")

    monkeypatch.setattr(socket.socket, "connect", _deny)


def test_coldfusion_refresh_header_and_meta_cookie_redirect_chain() -> None:
    pdf_header_bytes = _make_pdf(["Senate OWT", "FY 2027", "All material must be received by May 20, 2026."])
    pdf_meta_bytes = _make_pdf(["Senate OWT", "FY 2027", "All material must be received by June 18, 2026."])
    seen_cookies: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27":
            return httpx.Response(
                200,
                headers=[
                    ("Content-Type", "text/html;charset=UTF-8"),
                    (
                        "Refresh",
                        "2; url=https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27&download=1",
                    ),
                    ("Set-Cookie", "CFID=111; Path=/; Secure; HttpOnly"),
                    ("Set-Cookie", "CFTOKEN=abc; Path=/; Secure; HttpOnly"),
                ],
                text="<html><body>Preparing download...</body></html>",
            )
        if url == "https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27&download=1":
            cookie = request.headers.get("cookie", "")
            seen_cookies.append(cookie)
            if "CFID=111" not in cookie or "CFTOKEN=abc" not in cookie:
                return httpx.Response(403, text="Missing CF cookies")
            return httpx.Response(
                302,
                headers={"Location": "/imo/media/doc/owt_guidelines_ag_fy27.pdf"},
            )
        if url == "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_ag_fy27.pdf":
            return httpx.Response(
                200,
                headers={"Content-Type": "application/pdf"},
                content=pdf_header_bytes,
            )
        if url == "https://www.appropriations.senate.gov/download/owt_guidelines_meta_fy27":
            return httpx.Response(
                200,
                headers=[
                    ("Content-Type", "text/html;charset=UTF-8"),
                    ("Set-Cookie", "CFID=222; Path=/; Secure"),
                    ("Set-Cookie", "CFTOKEN=xyz; Path=/; Secure"),
                ],
                text=(
                    '<html><head><meta http-equiv="refresh" '
                    'content="2; url=/download/owt_guidelines_meta_fy27?download=1"></head></html>'
                ),
            )
        if url == "https://www.appropriations.senate.gov/download/owt_guidelines_meta_fy27?download=1":
            cookie = request.headers.get("cookie", "")
            seen_cookies.append(cookie)
            if "CFID=222" not in cookie or "CFTOKEN=xyz" not in cookie:
                return httpx.Response(403, text="Missing CF meta cookies")
            return httpx.Response(
                302,
                headers={"Location": "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_meta_fy27.pdf"},
            )
        if url == "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_meta_fy27.pdf":
            return httpx.Response(
                200,
                headers={"Content-Type": "application/pdf"},
                content=pdf_meta_bytes,
            )
        return httpx.Response(404, text="Not found")

    transport = httpx.MockTransport(handler)
    with ad.build_http_client(transport=transport) as client:
        doc_header = ad.resolve_document_url(
            client, "https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27"
        )
        doc_meta = ad.resolve_document_url(
            client, "https://www.appropriations.senate.gov/download/owt_guidelines_meta_fy27"
        )

    assert len(seen_cookies) == 2
    assert doc_header.pdf_url == "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_ag_fy27.pdf"
    assert doc_header.pdf_sha256 == hashlib.sha256(pdf_header_bytes).hexdigest()
    assert doc_header.http_status == 200
    assert len(doc_header.hops) == 3

    assert doc_meta.pdf_url == "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_meta_fy27.pdf"
    assert doc_meta.pdf_sha256 == hashlib.sha256(pdf_meta_bytes).hexdigest()
    assert doc_meta.http_status == 200
    assert len(doc_meta.hops) == 3


def test_past_date_closed_open_omitted_year_and_wrapped_year() -> None:
    pdf_omitted_year = _make_pdf(
        [
            "House Committee on Appropriations - Agriculture",
            "Instructions for Submitting a Request to Testify at FY 2027 Members' Day",
            "The Subcommittee has scheduled a hearing on March 26 th at 10:00am.",
            "All requests to testify must be received by Thursday, March 19 th at 5:00pm.",
            "Please provide written testimony for the record by no later than Monday, March 23rd at 5:00pm.",
        ]
    )
    pdf_wrapped_year = _make_pdf(
        [
            "Subcommittee on National Security, Department of State, and Related Programs",
            "Requests to testify must be made by 6:00pm on March 13, 2026.",
            "Written testimony for the record must be submitted to the Subcommittee by close of business on Thursday, March 27,",
            "2026.",
        ]
    )

    text_omitted = ad.extract_pdf_text(pdf_omitted_year)
    milestones_omitted = ad.extract_deadline_milestones(text_omitted, default_fiscal_year=2027)
    assert [d for d, _ in milestones_omitted] == [date(2026, 3, 19), date(2026, 3, 23)]

    text_wrapped = ad.extract_pdf_text(pdf_wrapped_year)
    milestones_wrapped = ad.extract_deadline_milestones(text_wrapped, default_fiscal_year=2027)
    assert [d for d, _ in milestones_wrapped] == [date(2026, 3, 13), date(2026, 3, 27)]

    assert ad.classify_deadline(
        as_of=date(2026, 3, 20),
        deadline_date=date(2026, 3, 13),
        is_hearing_relative=False,
        hearing_matched=False,
        is_absent=False,
        is_captcha_gated=False,
    ) == ("closed", "explicit")
    assert ad.classify_deadline(
        as_of=date(2026, 3, 15),
        deadline_date=date(2026, 3, 19),
        is_hearing_relative=False,
        hearing_matched=False,
        is_absent=False,
        is_captcha_gated=False,
    ) == ("open", "explicit")


def _build_mock_site(
    *,
    thud_dot_hearing_mmddyy: str | None = "05/19/26",
    ag_pdf_version: int = 1,
    include_thud_hearing_postponed: bool = False,
) -> httpx.MockTransport:
    ag_pdf_v1 = _make_pdf(
        ["House Agriculture FY 2027 OWT", "Testimony must be emailed no later than May 1st, 2026 at 5:00pm."]
    )
    ag_pdf_v2 = _make_pdf(
        ["House Agriculture FY 2027 OWT (Revised)", "Testimony must be emailed no later than May 15th, 2026 at 5:00pm."]
    )
    ag_pdf = ag_pdf_v1 if ag_pdf_version == 1 else ag_pdf_v2

    house_prog_pdf = _make_pdf(["House FY 2027 General Guidance", "See subcommittee schedule."])
    senate_general_pdf = _make_pdf(
        [
            "General Guidance on Fiscal Year 2027 Appropriations Requests",
            "Submission Deadlines",
            "Subcommittee CDS Requests Programmatic Requests",
            "Agriculture, Rural Development, Food and Drug Administration (FDA), and Related Agencies April 7, 2026 April 14, 2026",
            "Defense (not accepting CDS requests) N/A May 12, 2026",
            "Transportation, Housing and Urban Development (HUD), and Related Agencies April 20, 2026 April 20, 2026",
        ]
    )
    senate_ag_uncertain_pdf = _make_pdf(
        [
            "Senate Subcommittee on Agriculture",
            "Fiscal Year 2027 Appropriations",
            "Submission deadline is to be determined following release of the budget.",
        ]
    )
    senate_thud_pdf = _make_pdf(
        [
            "Senate Subcommittee on Transportation, Housing and Urban Development, and Related Agencies",
            "Fiscal Year 2027 Appropriations",
            "Deadline: All material must be received by 5:00pm seven days after the respective Departmental budget hearing of interest.",
        ]
    )

    house_guidance_html = """
    <html><body><main>
      <h2>Subcommittee-Specific Guidance</h2>
      <h3>Agriculture, Rural Development, Food and Drug Administration</h3>
      <ul>
        <li>Programmatic and Language Request Guidance (<a href="/docs/prog.pdf">link</a>)
          <ul><li>Submission Deadline: Friday, March 13, 2026, at 6:00 p.m.</li></ul>
        </li>
        <li>Community Project Funding Request Guidance (<a href="/docs/ag-cpf.pdf">link</a>)
          <ul><li>Submission Deadline: Thursday, March 19, 2026, at 6:00 p.m.</li></ul>
        </li>
      </ul>
      <h3>Defense</h3>
      <ul>
        <li>Programmatic and Language Request Guidance (<a href="/docs/prog.pdf">link</a>)
          <ul><li>Submission Deadline: Friday, March 20, 2026, at 6:00 p.m.</li></ul>
        </li>
        <li>Community Project Funding Request Guidance
          <ul><li>Submission Deadline: Not applicable</li></ul>
        </li>
      </ul>
      <a href="/subcommittees/agriculture-rural-development-food-and-drug-administration-and-related-agencies">Ag</a>
      <a href="/subcommittees/financial-services-and-general-government-0">FSGG</a>
      <a href="/subcommittees/defense-0">Defense</a>
    </main></body></html>
    """

    house_ag_sub_html = """
    <html><body><main>
      <a href="/docs/ag-owt.pdf">FY27 Outside Witness Testimony Instructions</a>
    </main></body></html>
    """
    house_fsgg_empty_html = "<html><body><main><h1>Financial Services</h1><p>No current OWT notices.</p></main></body></html>"
    house_defense_captcha_html = """
    <html><body><main>
      <form id="witness-portal">
        <div class="g-recaptcha" data-sitekey="fake"></div>
      </form>
    </main></body></html>
    """
    house_dem_portal_html = """
    <html><body>
      <form id="webform-submission-webform-225-node-43-add-form">
        <div class="captcha captcha-type-challenge--hcaptcha"><div class="h-captcha" data-sitekey="test"></div></div>
      </form>
    </body></html>
    """

    senate_guidance_html = """
    <html><body><main>
      <a href="/imo/media/doc/fy_2027_full_committee_general_guidance.pdf">HERE</a>
      <a href="/subcommittees/agriculture-rural-development-food-and-drug-administration-and-related-agencies">Ag</a>
      <a href="/subcommittees/transportation-housing-and-urban-development-and-related-agencies">THUD</a>
      <a href="/subcommittees/defense">Defense</a>
    </main></body></html>
    """
    senate_ag_html = '<html><body><a href="/download/owt_guidelines_ag_fy27">Submit Written Testimony</a></body></html>'
    senate_thud_html = '<html><body><a href="/download/owt_guidelines_thud_fy27">Submit Written Testimony</a></body></html>'
    senate_empty_html = "<html><body><p>Subcommittee overview without OWT instructions.</p></body></html>"

    hearing_rows: list[str] = []
    if thud_dot_hearing_mmddyy is not None:
        prefix = "**Postponed** " if include_thud_hearing_postponed else ""
        hearing_rows.append(
            f"""
            <tr>
              <td><a href="/hearings/fy27-dot-budget">{prefix}Subcommittee Hearing: A Review of the President's Fiscal Year 2027 Budget Request for the Department of Transportation</a></td>
              <td>Dirksen 192</td>
              <td>{thud_dot_hearing_mmddyy} 03:30PM</td>
            </tr>
            """
        )
    senate_hearings_html = f"<html><body><table>{''.join(hearing_rows)}</table></body></html>"

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        routes: dict[str, httpx.Response] = {
            ad.HOUSE_GUIDANCE_URL: httpx.Response(200, headers={"Content-Type": "text/html"}, text=house_guidance_html),
            "https://appropriations.house.gov/docs/prog.pdf": httpx.Response(
                200, headers={"Content-Type": "application/pdf"}, content=house_prog_pdf
            ),
            "https://appropriations.house.gov/docs/ag-cpf.pdf": httpx.Response(
                200, headers={"Content-Type": "application/pdf"}, content=house_prog_pdf
            ),
            "https://appropriations.house.gov/docs/ag-owt.pdf": httpx.Response(
                200, headers={"Content-Type": "application/pdf"}, content=ag_pdf
            ),
            "https://appropriations.house.gov/subcommittees/agriculture-rural-development-food-and-drug-administration-and-related-agencies": httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=house_ag_sub_html
            ),
            "https://appropriations.house.gov/subcommittees/financial-services-and-general-government-0": httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=house_fsgg_empty_html
            ),
            "https://appropriations.house.gov/subcommittees/defense-0": httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=house_defense_captcha_html
            ),
            ad.HOUSE_DEM_PORTAL_URL: httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=house_dem_portal_html
            ),
            ad.SENATE_GUIDANCE_URL: httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=senate_guidance_html
            ),
            "https://www.appropriations.senate.gov/imo/media/doc/fy_2027_full_committee_general_guidance.pdf": httpx.Response(
                200, headers={"Content-Type": "application/pdf"}, content=senate_general_pdf
            ),
            "https://www.appropriations.senate.gov/subcommittees/agriculture-rural-development-food-and-drug-administration-and-related-agencies": httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=senate_ag_html
            ),
            "https://www.appropriations.senate.gov/subcommittees/transportation-housing-and-urban-development-and-related-agencies": httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=senate_thud_html
            ),
            ad.SENATE_HEARINGS_URL: httpx.Response(
                200, headers={"Content-Type": "text/html"}, text=senate_hearings_html
            ),
            f"{ad.SENATE_HEARINGS_URL}?PageNum_rs=2": httpx.Response(
                200, headers={"Content-Type": "text/html"}, text="<html><body><table></table></body></html>"
            ),
            "https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27": httpx.Response(
                200,
                headers=[
                    ("Content-Type", "text/html"),
                    (
                        "Refresh",
                        "2; url=https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27&download=1",
                    ),
                    ("Set-Cookie", "CFID=10; Path=/; Secure"),
                    ("Set-Cookie", "CFTOKEN=20; Path=/; Secure"),
                ],
                text="<html></html>",
            ),
            "https://www.appropriations.senate.gov/download/owt_guidelines_ag_fy27&download=1": httpx.Response(
                302, headers={"Location": "/imo/media/doc/owt_guidelines_ag_fy27.pdf"}
            ),
            "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_ag_fy27.pdf": httpx.Response(
                200, headers={"Content-Type": "application/pdf"}, content=senate_ag_uncertain_pdf
            ),
            "https://www.appropriations.senate.gov/download/owt_guidelines_thud_fy27": httpx.Response(
                200,
                headers=[
                    ("Content-Type", "text/html"),
                    ("Set-Cookie", "CFID=30; Path=/; Secure"),
                    ("Set-Cookie", "CFTOKEN=40; Path=/; Secure"),
                ],
                text='<html><head><meta http-equiv="refresh" content="2; url=/download/owt_guidelines_thud_fy27&download=1"></head></html>',
            ),
            "https://www.appropriations.senate.gov/download/owt_guidelines_thud_fy27&download=1": httpx.Response(
                302, headers={"Location": "/imo/media/doc/owt_guidelines_thud_fy27.pdf"}
            ),
            "https://www.appropriations.senate.gov/imo/media/doc/owt_guidelines_thud_fy27.pdf": httpx.Response(
                200, headers={"Content-Type": "application/pdf"}, content=senate_thud_pdf
            ),
        }
        if url in routes:
            return routes[url]
        if "/subcommittees/" in url:
            return httpx.Response(200, headers={"Content-Type": "text/html"}, text=senate_empty_html)
        return httpx.Response(404, text="Not found")

    return httpx.MockTransport(handler)


def _find_record(
    report: ad.TrackerReport, *, chamber: str, subcommittee: str, track: str
) -> dict[str, object]:
    matches = [
        r
        for r in report["records"]
        if r["chamber"] == chamber and r["subcommittee"] == subcommittee and r["track"] == track
    ]
    assert len(matches) == 1, f"Expected 1 record for {(chamber, subcommittee, track)}, got {len(matches)}"
    return matches[0]


def test_classifications_uncertain_absent_captcha_and_sha256_diff() -> None:
    report_v1 = ad.collect_appropriations_deadlines(
        as_of=date(2026, 3, 15),
        fiscal_year=2027,
        transport=_build_mock_site(ag_pdf_version=1),
    )

    # Past-date closed vs future open on House guidance
    house_ag_prog = _find_record(
        report_v1, chamber="house", subcommittee="agriculture", track="member_programmatic_language"
    )
    house_ag_cpf = _find_record(report_v1, chamber="house", subcommittee="agriculture", track="member_cpf")
    assert house_ag_prog["status"] == "closed"
    assert house_ag_prog["deadline_date"] == "2026-03-13"
    assert house_ag_cpf["status"] == "open"
    assert house_ag_cpf["deadline_date"] == "2026-03-19"

    # absent vs captcha_gated vs uncertain
    house_fsgg_sub = _find_record(
        report_v1, chamber="house", subcommittee="financial_services", track="subcommittee_notice"
    )
    house_def_cpf = _find_record(report_v1, chamber="house", subcommittee="defense", track="member_cpf")
    house_def_sub = _find_record(report_v1, chamber="house", subcommittee="defense", track="subcommittee_notice")
    house_dem_portal = _find_record(
        report_v1, chamber="house", subcommittee="democrats_portal", track="portal"
    )
    senate_ag_owt = _find_record(report_v1, chamber="senate", subcommittee="agriculture", track="owt")
    assert house_fsgg_sub["status"] == "absent"
    assert house_def_cpf["status"] == "absent"
    assert house_def_sub["status"] == "captcha_gated"
    assert house_dem_portal["status"] == "captcha_gated"
    assert senate_ag_owt["status"] == "uncertain"

    # Without --prior, updated is False
    house_ag_owt_v1 = _find_record(report_v1, chamber="house", subcommittee="agriculture", track="owt")
    assert house_ag_owt_v1["updated"] is False
    assert len(str(house_ag_owt_v1["pdf_sha256"])) == 64

    # Re-running against identical snapshot yields updated=False
    prior_records = report_v1["records"]
    report_same = ad.collect_appropriations_deadlines(
        as_of=date(2026, 3, 15),
        fiscal_year=2027,
        prior_records=prior_records,
        transport=_build_mock_site(ag_pdf_version=1),
    )
    house_ag_owt_same = _find_record(report_same, chamber="house", subcommittee="agriculture", track="owt")
    assert house_ag_owt_same["pdf_sha256"] == house_ag_owt_v1["pdf_sha256"]
    assert house_ag_owt_same["updated"] is False

    # Changing PDF bytes flips PDF SHA-256 and sets updated=True
    report_v2 = ad.collect_appropriations_deadlines(
        as_of=date(2026, 3, 15),
        fiscal_year=2027,
        prior_records=prior_records,
        transport=_build_mock_site(ag_pdf_version=2),
    )
    house_ag_owt_v2 = _find_record(report_v2, chamber="house", subcommittee="agriculture", track="owt")
    assert house_ag_owt_v2["pdf_sha256"] != house_ag_owt_v1["pdf_sha256"]
    assert house_ag_owt_v2["deadline_date"] == "2026-05-15"
    assert house_ag_owt_v2["updated"] is True


def test_moving_hearing_relative_dates_and_unresolved_fallback() -> None:
    # 1. Resolved against active DOT hearing on 2026-05-19 -> 2026-05-26
    report_may = ad.collect_appropriations_deadlines(
        as_of=date(2026, 5, 1),
        fiscal_year=2027,
        transport=_build_mock_site(thud_dot_hearing_mmddyy="05/19/26"),
    )
    thud_may = _find_record(report_may, chamber="senate", subcommittee="thud", track="owt")
    assert thud_may["status"] == "hearing_relative"
    assert thud_may["deadline_kind"] == "hearing_relative"
    assert thud_may["deadline_date"] == "2026-05-26"
    assert thud_may["hearing_date"] == "2026-05-19"
    assert thud_may["hearing_link"] == "https://www.appropriations.senate.gov/hearings/fy27-dot-budget"

    # 2. Hearing date moves from 05/19/26 to 06/02/26 -> updates automatically to 2026-06-09 & sets updated=True
    prior = report_may["records"]
    report_moved = ad.collect_appropriations_deadlines(
        as_of=date(2026, 5, 1),
        fiscal_year=2027,
        prior_records=prior,
        transport=_build_mock_site(thud_dot_hearing_mmddyy="06/02/26"),
    )
    thud_moved = _find_record(report_moved, chamber="senate", subcommittee="thud", track="owt")
    assert thud_moved["status"] == "hearing_relative"
    assert thud_moved["deadline_date"] == "2026-06-09"
    assert thud_moved["hearing_date"] == "2026-06-02"
    assert thud_moved["pdf_sha256"] == thud_may["pdf_sha256"]
    assert thud_moved["updated"] is True

    # 3. Relative to --as-of after deadline -> classified as closed while preserving hearing_relative basis
    report_expired = ad.collect_appropriations_deadlines(
        as_of=date(2026, 11, 24),
        fiscal_year=2027,
        transport=_build_mock_site(thud_dot_hearing_mmddyy="05/19/26"),
    )
    thud_expired = _find_record(report_expired, chamber="senate", subcommittee="thud", track="owt")
    assert thud_expired["status"] == "closed"
    assert thud_expired["deadline_kind"] == "hearing_relative"
    assert thud_expired["deadline_date"] == "2026-05-26"

    # 4. If hearing is postponed or missing on /hearings -> falls back cleanly to status="uncertain"
    report_postponed = ad.collect_appropriations_deadlines(
        as_of=date(2026, 5, 1),
        fiscal_year=2027,
        transport=_build_mock_site(thud_dot_hearing_mmddyy="05/19/26", include_thud_hearing_postponed=True),
    )
    thud_postponed = _find_record(report_postponed, chamber="senate", subcommittee="thud", track="owt")
    assert thud_postponed["status"] == "uncertain"
    assert thud_postponed["deadline_date"] == ""
    assert thud_postponed["hearing_link"] == ""


def test_deterministic_cli_json_and_csv_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    transport = _build_mock_site()
    monkeypatch.setattr(ad, "build_http_client", lambda **kw: httpx.Client(transport=transport, follow_redirects=False))

    json_a = tmp_path / "a.json"
    csv_a = tmp_path / "a.csv"
    json_b = tmp_path / "b.json"
    csv_b = tmp_path / "b.csv"

    assert ad.main(["--as-of", "2026-05-01", "--json-out", str(json_a), "--csv-out", str(csv_a)]) == 0
    assert ad.main(["--as-of", "2026-05-01", "--json-out", str(json_b), "--csv-out", str(csv_b)]) == 0

    assert json_a.read_bytes() == json_b.read_bytes()
    assert csv_a.read_bytes() == csv_b.read_bytes()

    parsed = json.loads(json_a.read_text(encoding="utf-8"))
    assert parsed["as_of"] == "2026-05-01"
    with csv_a.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert tuple(reader.fieldnames or ()) == ad.CSV_COLUMNS
        rows = list(reader)
    assert len(rows) == len(parsed["records"])
    assert any(r["status"] == "hearing_relative" and r["hearing_link"] for r in rows)
    assert any(r["status"] == "captcha_gated" for r in rows)
