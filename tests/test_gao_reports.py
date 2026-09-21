"""GAO output field mapping; provider feed validation is tested in test_reference_source_failures."""

from __future__ import annotations

from spicy_regs.transforms.build_gao_reports import (
    COLUMNS,
    _published_date,
    _report_id,
    _shape,
)

_RAW_ITEM = {
    "title": "Navy Ship Modernization",
    "link": "https://www.gao.gov/products/gao-26-107974",
    "description": "What GAO Found. The Navy is behind schedule.",
    "pub_date": "Fri, 17 Jul 2026 07:10:42 -0400",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ITEM)
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 8


def test_shape_maps_fields():
    row = _shape(_RAW_ITEM)
    assert row["report_id"] == "gao-26-107974"
    assert row["title"] == "Navy Ship Modernization"
    assert row["report_type"] == "Report"
    assert row["published_date"] == "2026-07-17"
    assert row["abstract"].startswith("What GAO Found")
    # Reserved columns default to empty JSON arrays.
    assert row["agencies_json"] == "[]"
    assert row["topics_json"] == "[]"
    assert row["url"] == "https://www.gao.gov/products/gao-26-107974"


def test_report_id_extraction():
    assert _report_id("https://www.gao.gov/products/gao-26-107974") == "gao-26-107974"
    # Trailing slash and mixed case are normalized.
    assert _report_id("https://www.gao.gov/products/GAO-26-108520/") == "gao-26-108520"
    assert _report_id(None) is None
    assert _report_id("") is None


def test_published_date_parses_rfc822():
    assert _published_date("Fri, 17 Jul 2026 07:10:42 -0400") == "2026-07-17"
    # Unparseable / missing values degrade to None, not an exception.
    assert _published_date("not a date") is None
    assert _published_date(None) is None
