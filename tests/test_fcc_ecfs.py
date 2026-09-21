"""FCC output field mapping and credential resolution; window and refusal tests are in test_reference_source_failures."""

from __future__ import annotations

import json

from spicy_regs.sources.fcc_ecfs import (
    API_KEY_ENV_VARS,
    _resolve_api_key,
)
from spicy_regs.transforms.build_fcc_ecfs import (
    FILING_COLUMNS,
    PROCEEDING_COLUMNS,
    _shape_filing,
    _shape_proceeding,
)

_RAW_PROCEEDING = {
    "name": "17-108",
    "id_proceeding": 301759,
    "description": "Restoring Internet Freedom",
    "description_display": "Restoring Internet Freedom (display)",
    "bureau": {"code": "WC", "name": "Wireline Competition Bureau", "edocs_bureau_code": "WCB"},
    "flag_rulemaking_or_docket": "D",
    "filingStatus": "OPENALL",
    "date_proceeding_created": "2017-04-26T14:49:35.900Z",
    "date_closed": "2099-12-31T23:59:59.999Z",
    "comment_start_date": None,
    "comment_end_date": None,
    "comment_reply_start_date": None,
    "comment_reply_end_date": None,
    "filed_by": "Some.User",
    # Present in the payload but intentionally not published:
    "applicant_name": "",
    "flag_internet_file": "Y",
}

_RAW_FILING = {
    "id_submission": "26109947027",
    "proceedings": [
        {"name": "17-108", "bureau_code": "WC", "bureau_name": "Wireline Competition Bureau"},
        {"name": "23-320"},
    ],
    "submissiontype": {"description": "COMMENT", "short": "COMMENT", "id": 7, "type": "CO"},
    "express_comment": 1,
    "date_received": "2026-06-29T12:00:00.000Z",
    "date_submission": "2026-06-28T09:23:59.264Z",
    "date_disseminated": "2026-06-29T15:00:32.590Z",
    "filingstatus": {"description": "DISSEMINATED", "id": 30},
    "viewingstatus": {"description": "Unrestricted", "id": 10},
    "exparte_or_late_filed": "N",
    "filers": [{"name": "King"}],
    "authors": [],
    "lawfirms": [{"name": "Firm LLP"}],
    "bureaus": [],
    "text_data": "I support this rule.",
    "total_page_count": 3,
    "documents": [{"filename": "comment.pdf", "src": "https://www.fcc.gov/ecfs/document/1/x.pdf"}],
    # Present in the payload but intentionally not published:
    "_index": "filings.2026.6",
    "created": True,
}


# -- shaping: proceedings ------------------------------------------------------


def test_shape_proceeding_produces_exact_schema():
    row = _shape_proceeding(_RAW_PROCEEDING)
    assert set(row) == set(PROCEEDING_COLUMNS)
    assert len(PROCEEDING_COLUMNS) == 14


def test_shape_proceeding_maps_fields():
    row = _shape_proceeding(_RAW_PROCEEDING)
    assert row["name"] == "17-108"
    # Integer id stringifies (schema is all-VARCHAR).
    assert row["id_proceeding"] == "301759"
    # description_display wins over description.
    assert row["description"] == "Restoring Internet Freedom (display)"
    # Nested bureau object flattens.
    assert row["bureau_code"] == "WC"
    assert row["bureau_name"] == "Wireline Competition Bureau"
    assert row["rulemaking_or_docket"] == "D"
    assert row["filing_status"] == "OPENALL"
    assert row["date_created"] == "2017-04-26T14:49:35.900Z"
    assert row["filed_by"] == "Some.User"


def test_shape_proceeding_accepts_flat_bureau_fields():
    # Filings embed proceedings with flat bureau_code/bureau_name instead of a
    # nested bureau object; the shaper accepts both.
    row = _shape_proceeding({"name": "23-320", "bureau_code": "WC", "bureau_name": "Wireline"})
    assert row["bureau_code"] == "WC"
    assert row["bureau_name"] == "Wireline"


def test_shape_proceeding_handles_missing_fields():
    row = _shape_proceeding({"name": "96-45"})
    assert row["name"] == "96-45"
    assert row["id_proceeding"] is None
    assert row["bureau_code"] is None
    assert row["date_closed"] is None


# -- shaping: filings ----------------------------------------------------------


def test_shape_filing_produces_exact_schema():
    row = _shape_filing(_RAW_FILING)
    assert set(row) == set(FILING_COLUMNS)
    assert len(FILING_COLUMNS) == 18


def test_shape_filing_maps_and_serializes_fields():
    row = _shape_filing(_RAW_FILING)
    assert row["id_submission"] == "26109947027"
    # Nested description objects flatten to their descriptions.
    assert row["submission_type"] == "COMMENT"
    assert row["filing_status"] == "DISSEMINATED"
    assert row["viewing_status"] == "Unrestricted"
    # Integer flags/counts stringify (schema is all-VARCHAR).
    assert row["express_comment"] == "1"
    assert row["total_page_count"] == "3"
    # Array fields serialize to JSON strings of names.
    assert json.loads(row["proceeding_names_json"]) == ["17-108", "23-320"]
    assert json.loads(row["filers_json"]) == ["King"]
    assert json.loads(row["authors_json"]) == []
    assert json.loads(row["lawfirms_json"]) == ["Firm LLP"]
    # Documents keep only filename + src.
    docs = json.loads(row["documents_json"])
    assert docs == [{"filename": "comment.pdf", "src": "https://www.fcc.gov/ecfs/document/1/x.pdf"}]
    assert row["text_data"] == "I support this rule."
    assert row["filing_url"] == "https://www.fcc.gov/ecfs/filing/26109947027"


def test_shape_filing_handles_missing_fields():
    row = _shape_filing({"id_submission": 123})
    assert row["id_submission"] == "123"
    assert row["submission_type"] is None
    assert row["proceeding_names_json"] == "[]"
    assert row["documents_json"] == "[]"
    assert row["filing_url"] == "https://www.fcc.gov/ecfs/filing/123"


def test_shape_filing_without_id_has_no_url():
    row = _shape_filing({})
    assert row["id_submission"] is None
    assert row["filing_url"] is None


# -- API-key resolution --------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("FCC_API_KEY", "fcc-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_and_skips_blank(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "   ")
    monkeypatch.setenv("FCC_API_KEY", "fcc-key")
    assert _resolve_api_key() == "fcc-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for name in API_KEY_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    assert _resolve_api_key() is None
