"""FCC output field mapping and credential resolution; window and refusal tests are in test_reference_source_failures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from spicy_regs.transforms.build_fcc_ecfs import (
    API_KEY_ENV_VARS,
    FILING_COLUMNS,
    PROCEEDING_COLUMNS,
    _resolve_api_key,
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
    "date_nprm": "2017-05-23T00:00:00.000-04:00",
    "date_reporting_and_order": None,
    "rule_section": " Part 8",
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
    assert len(PROCEEDING_COLUMNS) == 17


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
    assert len(FILING_COLUMNS) == 21


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


_FIXTURES = Path(__file__).parent / "fixtures" / "fcc_native"


def test_shape_proceeding_keeps_nprm_order_and_rule_section_as_ecfs_writes_them():
    """Proceeding 96-82, copied verbatim from the retained ECFS /proceeding page whose decompressed sha256 is
    cf5ef628c7c6935523e9ca9b2b740d5b5e954ec1d822ed9bd7bf34413467d7d8, publishes its three ECFS fields as written."""
    from spicy_regs.transforms.government_source_shapes import map_subject

    fixture = json.loads((_FIXTURES / "proceeding_96_82.json").read_text())
    assert fixture["source"]["sha256"].startswith("cf5ef628")
    row = _shape_proceeding(fixture["record"])
    assert row["date_nprm"] == "1996-04-01T00:00:00.000-05:00"
    assert row["date_reporting_and_order"] == "1996-10-18T00:00:00.000-04:00"
    assert row["rule_section"] == "PART 80 AND 87"
    assert tuple(row) == PROCEEDING_COLUMNS
    map_subject("fcc_proceedings", row)


def test_rule_section_spacing_is_published_as_written():
    # ECFS staff type rule_section freely; " Part 8" stays as written, like filed_by.
    assert _shape_proceeding(_RAW_PROCEEDING)["rule_section"] == " Part 8"


def test_fcc_proceedings_admits_exactly_the_published_government_sources_1_policy():
    """gs/1 is the frozen descriptor fcc-proceedings published until ss-17vd (etl_policies/fcc_proceedings.json at
    028736f3, copied into navigation_policy_history.json); it must be today's schema less exactly the three new columns."""
    from importlib.resources import files

    from spicy_regs.transforms.government_receipts import EARLIER_POLICIES, POLICIES

    frozen = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())["fcc_proceedings"]
    current = POLICIES["fcc_proceedings"]
    assert current.policy_version == "government-sources/2"
    assert [policy.descriptor() for policy in EARLIER_POLICIES["fcc_proceedings"]] == [frozen]
    (earlier,) = EARLIER_POLICIES["fcc_proceedings"]
    assert earlier.policy_version == "government-sources/1"
    assert earlier.subject_schema.names == [
        name for name in current.subject_schema.names
        if name not in ("date_nprm", "date_reporting_and_order", "rule_section")
    ]


@pytest.mark.parametrize("columns", ["PROCEEDING_COLUMNS", "FILING_COLUMNS"])
def test_fcc_columns_have_one_owner(columns):
    from spicy_regs.transforms import build_fcc_ecfs
    from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS

    dataset = {"PROCEEDING_COLUMNS": "fcc_proceedings", "FILING_COLUMNS": "fcc_filings"}[columns]
    assert getattr(build_fcc_ecfs, columns) is LEGACY_COLUMNS[dataset]
