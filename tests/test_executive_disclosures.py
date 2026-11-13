"""Filing/access distinctions, exact-source binding and ordinary consumer lookup controls."""

from copy import deepcopy
from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_executive_disclosures import amount_range, build_tables, validate_selection

SELECTION = json.loads((Path(__file__).parent / "fixtures/executive_disclosures/selection.json").read_text())


class Evidence:
    def retain_bytes(self, *args, **kwargs):
        pass

    def event(self, *args, **kwargs):
        pass


@pytest.mark.parametrize("raw,expected", [
    ("", ("blank", None, None)),
    ("$50,001 - $100,000", ("range", 50001, 100000)),
    ("None (or less than $201)", ("unrecognized", None, None)),
    ("$100,000 - $50,001", ("unrecognized", None, None)),
    ("$12.34", ("unrecognized", None, None)),
])
def test_ranges_are_bounds_and_blanks_are_not_zero(raw, expected):
    assert amount_range(raw) == expected


def test_requested_unavailable_expired_and_refused_remain_distinct(tmp_path):
    selected = deepcopy(SELECTION)
    selected["filings"] = []
    states = ["not_requested", "requested", "unavailable", "expired", "access_refused"]
    for status in states:
        item = {**SELECTION["filings"][0], "filing_id": status, "access_status": status,
                "source_sha256": None, "byte_size": None, "access_reason": "controlled access outcome"}
        selected["filings"].append(item)
    paths = build_tables(selected, tmp_path, evidence=Evidence(), store=None)
    rows = pq.read_table(paths[0]).to_pylist()
    assert [row["access_status"] for row in rows] == states
    assert all(row["facts_status"] == "not_available" for row in rows)
    assert pq.read_table(paths[1]).num_rows == 0
    assert pq.read_table(paths[2]).num_rows == 0


def test_source_version_mismatch_writes_no_tables(tmp_path):
    class Store:
        def open(self, _digest):
            return BytesIO(b"different original")

    with pytest.raises(ValueError, match="differs from selected source version"):
        build_tables(SELECTION, tmp_path, evidence=Evidence(), store=Store())
    assert not list(tmp_path.glob("*.parquet"))


def test_access_claim_and_person_identity_controls():
    for field, value in [("access_basis", "form201_required"), ("access_status", "unknown"),
                         ("official_id", SELECTION["filings"][0]["position_raw"])]:
        selected = deepcopy(SELECTION)
        selected["filings"][0][field] = value
        with pytest.raises(ValueError):
            validate_selection(selected)


def test_lookup_through_ordinary_service_keeps_parent_blank_and_amendment(tmp_path, monkeypatch):
    import hashlib
    from spicy_docs.sources import executive_disclosures as source
    from spicy_regs import mcp_server

    body = b"controlled test input; source parsing independently tested against official PDF"
    selected = deepcopy(SELECTION)
    for item in selected["filings"]:
        item.update(source_sha256="sha256:" + hashlib.sha256(body).hexdigest(), byte_size=len(body))

    class Store:
        def open(self, _digest):
            return BytesIO(body)

    def read(_body, **kwargs):
        return SimpleNamespace(parser_revision=source.PARSER_REVISION, backend_version="test-double",
            source_sha256=selected["filings"][0]["source_sha256"],
            pages=({"page": 8, "page_text": "source page and footnote", "source_locator": "#page=8",
                    "table_bbox_json": "[]", "source_slot_count": 2},),
            rows=tuple({"page": 8, "source_locator": "#page=8", "row_number_raw": label,
                        "description_raw": description, "eif_raw": "N/A", "value_raw": value,
                        "income_type_raw": "", "income_amount_raw": "", "blank_slot": False,
                        "raw_cells_json": "[]", "cell_boxes_json": "[]"}
                       for label, description, value in [("1.", "(Self) U.S. bank #1 (cash)", "$50,001 - $100,000"),
                                                          ("4.", "The KDH/DCE Family Trust", "")]))
    monkeypatch.setattr(source, "read_part6", read)
    build_tables(selected, tmp_path, evidence=Evidence(), store=Store())
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    with mcp_server._build_connection() as con:
        row = con.execute("""SELECT f.official_name, f.report_year, v.value_raw, v.value_lower, v.value_upper,
                                    v.source_url || v.source_locator
             FROM executive_disclosure_filings f JOIN executive_disclosure_facts v USING(filing_id)
             WHERE f.filing_id='whitehouse-harris-2024-annual' AND v.row_number_raw='1.'""").fetchone()
        assert row[:5] == ("Kamala D. Harris", 2023, "$50,001 - $100,000", 50001, 100000)
        assert row[5].endswith(".pdf#page=8")
        assert con.execute("SELECT value_raw,value_lower FROM executive_disclosure_facts WHERE row_number_raw='4.'").fetchone() == ("", None)
        assert con.execute("SELECT facts_status FROM executive_disclosure_filings WHERE filing_id='whitehouse-harris-2021-annual'").fetchone() == ("unsupported",)
