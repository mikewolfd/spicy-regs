"""Hermetic tests for OIRA EO 12866 transforms, rollups, and raw evidence retention."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.oira import (
    AGENCY_LIST_FILE,
    REPORT_LIST_URL,
    REVIEWS_PENDING_FILE,
    REVIEWS_YTD_FILE,
    OiraMeetingsReader,
    OiraReviewsReader,
    OiraUnavailableError,
    xml_file_url,
)

from spicy_regs.pipelines.rollups.oira_meetings import OiraMeetingsRollup
from spicy_regs.pipelines.rollups.oira_reviews import OiraReviewsRollup
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_oira import (
    CONCLUDED,
    MEETING_COLUMNS,
    MEETING_SCHEMA,
    REVIEW_COLUMNS,
    REVIEW_SCHEMA,
    UNDER_REVIEW,
    build_oira_meetings,
    build_oira_reviews,
)

REPORT_LIST_HTML = b"""
<html><body>
  <a href="/public/do/XMLViewFileAction?f=AGY_AGENCY_LIST.xml">Agencies</a>
  <a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_1981.xml">1981</a>
  <a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_2025.xml">2025</a>
  <a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_30_DAYS.xml">30 Days</a>
  <a href="/public/do/XMLViewFileAction?f=EO_RULE_COMPLETED_YTD.xml">YTD</a>
  <a href="/public/do/XMLViewFileAction?f=EO_RULES_UNDER_REVIEW.xml">Under Review</a>
  <a href="/public/do/XMLViewFileAction?f=EO_12866_Meeting_Data_First_Half_2024.xml">Meetings H1</a>
  <a href="/public/do/XMLViewFileAction?f=EO_12866_Meeting_Data_Second_Half_2024.xml">Meetings H2</a>
</body></html>
"""

AGENCY_LIST_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA>
  <AGENCY>
    <AGENCY_CODE>0938</AGENCY_CODE>
    <ACRONYM>CMS</ACRONYM>
    <NAME>Centers for Medicare &amp; Medicaid Services</NAME>
  </AGENCY>
  <AGENCY>
    <AGENCY_CODE>2060</AGENCY_CODE>
    <ACRONYM>EPA</ACRONYM>
    <NAME>Environmental Protection Agency</NAME>
  </AGENCY>
</OIRA_DATA>
"""

# 1981 XML intentionally omits <?xml ...?> prolog, uses defunct agency 2527, and keeps historical DECISION phrasing.
REVIEWS_1981_XML = b"""<OIRA_DATA>
  <REGACT>
    <AGENCY_CODE>2527</AGENCY_CODE>
    <RIN>2527-YA01</RIN>
    <TITLE>Historical Defunct Agency Rule  </TITLE>
    <STAGE>Proposed Rule</STAGE>
    <ECONOMICALLY_SIGNIFICANT>No</ECONOMICALLY_SIGNIFICANT>
    <DATE_RECEIVED>1981-07-13</DATE_RECEIVED>
    <LEGAL_DEADLINE>None</LEGAL_DEADLINE>
    <DATE_COMPLETED>1981-07-21</DATE_COMPLETED>
    <DECISION>Consistent w/no change</DECISION>
  </REGACT>
</OIRA_DATA>
"""

REVIEWS_2025_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA>
  <REGACT>
    <AGENCY_CODE>0938</AGENCY_CODE>
    <RIN>0938-AV01</RIN>
    <TITLE>Medicare Update 2025</TITLE>
    <STAGE>Final Rule</STAGE>
    <ECONOMICALLY_SIGNIFICANT>Yes</ECONOMICALLY_SIGNIFICANT>
    <MAJOR>Yes</MAJOR>
    <LEGAL_DEADLINE>Statutory</LEGAL_DEADLINE>
    <DATE_RECEIVED>2025-11-01</DATE_RECEIVED>
    <DATE_COMPLETED>2025-12-01</DATE_COMPLETED>
    <DECISION>Consistent with Change</DECISION>
    <DATE_PUBLISHED>2025-12-15</DATE_PUBLISHED>
    <REVIEW_EXTENDED>Yes</REVIEW_EXTENDED>
    <UNFUNDED_MANDATES>No</UNFUNDED_MANDATES>
  </REGACT>
</OIRA_DATA>
"""

# YTD includes one overlapping 2025 row (must be dropped) and one 2026 row with whitespace-only <MAJOR>.
REVIEWS_YTD_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA>
  <REGACT>
    <AGENCY_CODE>0938</AGENCY_CODE>
    <RIN>0938-AV01</RIN>
    <TITLE>Medicare Update 2025</TITLE>
    <STAGE>Final Rule</STAGE>
    <DATE_RECEIVED>2025-11-01</DATE_RECEIVED>
    <DATE_COMPLETED>2025-12-01</DATE_COMPLETED>
    <DECISION>Consistent with Change</DECISION>
  </REGACT>
  <REGACT>
    <AGENCY_CODE>0938</AGENCY_CODE>
    <RIN>0938-AV44</RIN>
    <TITLE>Interoperability Standards</TITLE>
    <STAGE>Proposed Rule</STAGE>
    <ECONOMICALLY_SIGNIFICANT>No</ECONOMICALLY_SIGNIFICANT>
    <MAJOR> </MAJOR>
    <LEGAL_DEADLINE>None</LEGAL_DEADLINE>
    <DATE_RECEIVED>2026-01-16</DATE_RECEIVED>
    <DATE_COMPLETED>2026-04-10</DATE_COMPLETED>
    <DECISION>Consistent with Change</DECISION>
    <DATE_PUBLISHED />
    <TCJA>No</TCJA>
  </REGACT>
</OIRA_DATA>
"""

# Under-review XML carries EXPEDITED_REVIEW (live RegInfo tag) and omits DATE_COMPLETED/DECISION.
REVIEWS_PENDING_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<OIRA_DATA>
  <REGACT>
    <AGENCY_CODE>2060</AGENCY_CODE>
    <RIN>2060-AW99</RIN>
    <TITLE>Pending Air Standard</TITLE>
    <STAGE>Final Rule</STAGE>
    <ECONOMICALLY_SIGNIFICANT>Yes</ECONOMICALLY_SIGNIFICANT>
    <DATE_RECEIVED>2026-11-09</DATE_RECEIVED>
    <LEGAL_DEADLINE>Judicial</LEGAL_DEADLINE>
    <TCJA>Yes</TCJA>
    <EXPEDITED_REVIEW>No</EXPEDITED_REVIEW>
  </REGACT>
</OIRA_DATA>
"""

# First-half 2024 meetings: Cp1252 em-dash (\x97), double-escaped entities, no MEETING_ID.
MEETINGS_H1_XML = (
    b"<?xml version='1.0' encoding='Cp1252' ?>\r\n"
    b"<RESULTS>\r\n"
    b"  <ROW>\r\n"
    b"    <RIN>2060-AV50</RIN>\r\n"
    b"    <TITLE>Heavy-Duty Vehicles\x97Phase 3</TITLE>\r\n"
    b"    <AGENCYSUBAGENCYACRONYM>2060-EPA/OAR</AGENCYSUBAGENCYACRONYM>\r\n"
    b"    <RULE_STAGE_DESC>Final Rule Stage</RULE_STAGE_DESC>\r\n"
    b"    <MEETINGDATETIME>12-FEB-24</MEETINGDATETIME>\r\n"
    b"    <REQUESTOR>ADVION &amp;#040;Formerly NASC&amp;#041;</REQUESTOR>\r\n"
    b"    <REQUESTOR_NAME>Cynthia  Morton</REQUESTOR_NAME>\r\n"
    b"    <TYPE_CD>C</TYPE_CD>\r\n"
    b"    <MEETING_TYPE>Completed</MEETING_TYPE>\r\n"
    b"    <SUMMARY />\r\n"
    b"  </ROW>\r\n"
    b"</RESULTS>\r\n"
)

# Second-half 2024 meetings: has MEETING_ID and an agency not in AGENCY_LIST_XML (falls back to acronym segment).
MEETINGS_H2_XML = (
    b"<?xml version='1.0' encoding='Cp1252' ?>\r\n"
    b"<RESULTS>\r\n"
    b"  <ROW>\r\n"
    b"    <RIN>0581-AE30</RIN>\r\n"
    b"    <MEETING_ID>628973</MEETING_ID>\r\n"
    b"    <TITLE>Fed Cattle Markets</TITLE>\r\n"
    b"    <AGENCYSUBAGENCYACRONYM>0581-USDA/AMS</AGENCYSUBAGENCYACRONYM>\r\n"
    b"    <RULE_STAGE_DESC>Prerule Stage</RULE_STAGE_DESC>\r\n"
    b"    <MEETINGDATETIME>02-OCT-24</MEETINGDATETIME>\r\n"
    b"    <REQUESTOR>Family Farms Coalition</REQUESTOR>\r\n"
    b"    <REQUESTOR_NAME>Patty Lovera</REQUESTOR_NAME>\r\n"
    b"    <TYPE_CD>C</TYPE_CD>\r\n"
    b"    <MEETING_TYPE>Completed</MEETING_TYPE>\r\n"
    b"    <SUMMARY>Discussed price transparency.</SUMMARY>\r\n"
    b"  </ROW>\r\n"
    b"</RESULTS>\r\n"
)


def _mock_fetcher(mapping: dict[str, bytes]):
    def _fetch(url: str) -> bytes:
        if url not in mapping:
            raise OiraUnavailableError(f"unexpected URL: {url}")
        return mapping[url]

    return _fetch


def _all_urls() -> dict[str, bytes]:
    return {
        REPORT_LIST_URL: REPORT_LIST_HTML,
        xml_file_url(AGENCY_LIST_FILE): AGENCY_LIST_XML,
        xml_file_url("EO_RULE_COMPLETED_1981.xml"): REVIEWS_1981_XML,
        xml_file_url("EO_RULE_COMPLETED_2025.xml"): REVIEWS_2025_XML,
        xml_file_url(REVIEWS_YTD_FILE): REVIEWS_YTD_XML,
        xml_file_url(REVIEWS_PENDING_FILE): REVIEWS_PENDING_XML,
        xml_file_url("EO_12866_Meeting_Data_First_Half_2024.xml"): MEETINGS_H1_XML,
        xml_file_url("EO_12866_Meeting_Data_Second_Half_2024.xml"): MEETINGS_H2_XML,
    }


def _sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def test_build_oira_reviews_shapes_dedups_and_retains_evidence(tmp_path: Path):
    archive_root = tmp_path / "central-raw"
    out_dir = tmp_path / "out"
    evidence = CaptureEvidence(out_dir, "oira-reviews", archive_root=archive_root)
    captured_files: list[str] = []

    def _on_capture(captured):
        captured_files.append(captured.file_name)
        evidence.retain_bytes(
            captured.raw_bytes,
            stage="oira-reviews-xml",
            raw_source=True,
            file_name=captured.file_name,
            source_url=captured.url,
            fetched_at=captured.fetched_at,
        )

    reader = OiraReviewsReader(fetch_bytes=_mock_fetcher(_all_urls()), on_capture=_on_capture)
    parquet_path = build_oira_reviews(out_dir, reader=reader, evidence=evidence)
    evidence.finish()

    table = pq.read_table(parquet_path)
    assert table.schema == REVIEW_SCHEMA
    assert tuple(table.column_names) == REVIEW_COLUMNS
    rows = {r["rin"]: r for r in table.to_pylist()}
    # 4 distinct reviews: 1981, 2025 (YTD overlap dropped!), 2026 YTD, and under-review.
    assert set(rows) == {"2527-YA01", "0938-AV01", "0938-AV44", "2060-AW99"}

    # Pending review keeps EXPEDITED_REVIEW + TCJA in flags_json and leaves concluded fields NULL.
    pending = rows["2060-AW99"]
    assert pending["review_status"] == UNDER_REVIEW
    assert pending["agency_code"] == "EPA"
    assert pending["agency_name"] == "Environmental Protection Agency"
    assert pending["concluded_date"] is None
    assert pending["review_days"] is None
    assert pending["review_outcome"] is None
    assert pending["published_date"] is None
    assert json.loads(pending["flags_json"]) == {"expedited_review": "No", "tcja": "Yes"}
    assert pending["source_sha256"] == _sha(REVIEWS_PENDING_XML)

    # 2025 concluded review computes calendar review_days and separates review_extended from flags_json.
    r2025 = rows["0938-AV01"]
    assert r2025["review_status"] == CONCLUDED
    assert r2025["review_days"] == 30
    assert r2025["review_extended"] == "Yes"
    assert r2025["published_date"] == "2025-12-15"
    assert json.loads(r2025["flags_json"]) == {"unfunded_mandates": "No"}
    assert r2025["source_file"] == "EO_RULE_COMPLETED_2025.xml"
    assert r2025["source_sha256"] == _sha(REVIEWS_2025_XML)

    # 2026 YTD normalizes whitespace-only <MAJOR> </MAJOR> to None.
    r2026 = rows["0938-AV44"]
    assert r2026["major"] is None
    assert r2026["review_days"] == 84
    assert r2026["source_sha256"] == _sha(REVIEWS_YTD_XML)

    # 1981 defunct agency code 2527 preserved verbatim; historical DECISION wording untouched; flags_json is None.
    r1981 = rows["2527-YA01"]
    assert r1981["agency_code"] == "2527"
    assert r1981["agency_name"] is None
    assert r1981["review_outcome"] == "Consistent w/no change"
    assert r1981["review_days"] == 8
    assert r1981["flags_json"] is None
    assert r1981["source_sha256"] == _sha(REVIEWS_1981_XML)

    # Every raw XML file (including agency list) was retained in central raw archive.
    assert "EO_RULE_COMPLETED_30_DAYS.xml" not in captured_files
    assert set(captured_files) == {
        AGENCY_LIST_FILE,
        "EO_RULE_COMPLETED_1981.xml",
        "EO_RULE_COMPLETED_2025.xml",
        REVIEWS_YTD_FILE,
        REVIEWS_PENDING_FILE,
    }


def test_build_oira_meetings_decodes_cp1252_entities_and_retains_evidence(tmp_path: Path):
    archive_root = tmp_path / "central-raw"
    out_dir = tmp_path / "out"
    evidence = CaptureEvidence(out_dir, "oira-meetings", archive_root=archive_root)
    reader = OiraMeetingsReader(
        fetch_bytes=_mock_fetcher(_all_urls()),
        on_capture=lambda c: evidence.retain_bytes(
            c.raw_bytes,
            stage="oira-meetings-xml",
            raw_source=True,
            file_name=c.file_name,
            source_url=c.url,
            fetched_at=c.fetched_at,
        ),
    )
    parquet_path = build_oira_meetings(out_dir, reader=reader, evidence=evidence)
    evidence.finish()

    table = pq.read_table(parquet_path)
    assert table.schema == MEETING_SCHEMA
    assert tuple(table.column_names) == MEETING_COLUMNS
    rows = {r["rin"]: r for r in table.to_pylist()}
    assert set(rows) == {"2060-AV50", "0581-AE30"}

    h1 = rows["2060-AV50"]
    assert h1["meeting_id"] is None
    assert h1["url"] is None
    assert h1["title"] == "Heavy-Duty Vehicles\u2014Phase 3"
    assert h1["requestor_organization"] == "ADVION (Formerly NASC)"
    assert h1["requestor_name"] == "Cynthia Morton"
    assert h1["agency_code"] == "EPA"
    assert h1["agency_name"] == "Environmental Protection Agency"
    assert h1["meeting_date"] == "2024-02-12"
    assert h1["summary"] is None
    assert h1["source_sha256"] == _sha(MEETINGS_H1_XML)

    h2 = rows["0581-AE30"]
    assert h2["meeting_id"] == "628973"
    assert h2["url"] == "https://www.reginfo.gov/public/do/viewEO12866Meeting?viewRule=true&rin=0581-AE30&meetingId=628973"
    assert h2["agency_code"] == "AMS"
    assert h2["agency_name"] is None
    assert h2["meeting_date"] == "2024-10-02"
    assert h2["summary"] == "Discussed price transparency."
    assert h2["source_sha256"] == _sha(MEETINGS_H2_XML)


def test_build_oira_refuses_empty_result(tmp_path: Path):
    empty_xml = b'<?xml version="1.0" encoding="UTF-8"?><OIRA_DATA></OIRA_DATA>'
    urls = {
        REPORT_LIST_URL: REPORT_LIST_HTML,
        xml_file_url(AGENCY_LIST_FILE): AGENCY_LIST_XML,
        xml_file_url(REVIEWS_PENDING_FILE): empty_xml,
    }
    reader = OiraReviewsReader(files=[REVIEWS_PENDING_FILE], fetch_bytes=_mock_fetcher(urls))
    with pytest.raises(OiraUnavailableError, match="no EO 12866 review records"):
        build_oira_reviews(tmp_path, reader=reader)
    assert not (tmp_path / "oira_reviews.parquet").exists()


def test_oira_rollup_wiring():
    assert OiraReviewsRollup.name == "oira-reviews"
    assert OiraReviewsRollup.retain_source_evidence is True
    assert OiraReviewsRollup.inputs == ()
    assert OiraReviewsRollup.output == "oira_reviews.parquet"

    assert OiraMeetingsRollup.name == "oira-meetings"
    assert OiraMeetingsRollup.retain_source_evidence is True
    assert OiraMeetingsRollup.inputs == ()
    assert OiraMeetingsRollup.output == "oira_meetings.parquet"
