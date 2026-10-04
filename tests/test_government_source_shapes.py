"""Native government values preserve meaning and refuse lossy conversions."""

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.government_source_shapes import (
    GovernmentShapeError,
    SUBJECT_SCHEMAS,
    map_subject,
)


def filing(native):
    text = json.dumps(native)
    return {
        "id_submission": "s",
        "native_fields_json": text,
        "native_fields_sha256": "sha256:" + hashlib.sha256(text.encode()).hexdigest(),
    }


def test_fcc_roles_nulls_duplicates_and_stated_fields_are_native(tmp_path):
    source = {
        "filers": [{"name": "A"}, None, {"name": "A"}],
        "authors": [],
        "lawfirms": None,
        "proceedings": [{"id_proceeding": 301759, "name": "17-108", "filingStatus": "OPENALL"}],
        "documents": [{"filename": "a.pdf", "description": "", "src": "https://example.test/a"}],
    }
    shaped = map_subject("fcc_filings", filing(source))
    assert shaped["filers"] == [{"name": "A"}, None, {"name": "A"}]
    assert shaped["authors"] == []
    assert shaped["lawfirms"] is None and shaped["bureaus"] is None
    assert shaped["proceedings"][0]["id_proceeding"] == "301759"
    assert shaped["proceedings"][0]["filing_status"] == "OPENALL"
    assert shaped["documents"] == [{"filename": "a.pdf", "description": ""}]
    assert "native_fields_json" not in shaped and "pdf_extraction_results_json" not in shaped
    path = tmp_path / "fcc.parquet"
    pq.write_table(pa.Table.from_pylist([shaped], schema=SUBJECT_SCHEMAS["fcc_filings"]), path)
    assert pq.read_table(path).to_pylist() == [shaped]


def test_fcc_source_null_does_not_fall_back_to_lossy_name_list():
    source = filing({"filers": None})
    source["filers_json"] = '["legacy"]'
    assert map_subject("fcc_filings", source)["filers"] is None
    assert map_subject("fcc_filings", {"id_submission": "x", "filers_json": '["A",null,"A"]'})["filers"] == [
        {"name": "A"},
        None,
        {"name": "A"},
    ]


@pytest.mark.parametrize("bad", [42, {}, [3], ["A"]])
def test_fcc_malformed_native_input_refuses(bad):
    with pytest.raises(GovernmentShapeError):
        map_subject("fcc_filings", filing({"filers": bad}))


def test_fcc_digest_mismatch_refuses():
    row = filing({"filers": []})
    row["native_fields_json"] = "{}"
    with pytest.raises(GovernmentShapeError, match="digest"):
        map_subject("fcc_filings", row)


def test_money_is_exact_and_domain_status_is_kept():
    recipient = map_subject(
        "usaspending_recipients",
        {
            "recipient_id": "x-R",
            "recipient_level": "R",
            "total_award_amount": "9007199254740993.120001",
            "observed_at": "old",
            "source_capture_sha256": "digest",
        },
    )
    assert recipient["total_award_amount"] == Decimal("9007199254740993.120001")
    assert not {"observed_at", "source_capture_sha256"} & recipient.keys()
    row = map_subject(
        "gao_recommendations",
        {"recommendation_id": "r", "status": "Open--Partially Addressed", "priority": "true", "listed_open": "false"},
    )
    assert row["status"] == "Open--Partially Addressed"
    assert row["priority"] is True and row["listed_open"] is False


@pytest.mark.parametrize("value", [1.1, True, "NaN", "Infinity", "1.0000001", "100000000000000000000000000000000"])
def test_lossy_money_refuses(value):
    with pytest.raises(GovernmentShapeError):
        map_subject("usaspending_recipients", {"recipient_id": "x", "total_award_amount": value})


def test_arrays_do_not_cross_product_or_deduplicate():
    row = map_subject(
        "lobbying_activities",
        {
            "filing_uuid": "f",
            "activity_index": "2",
            "government_entities_json": '[{"id":1,"name":"SENATE"},null,{"id":1,"name":"SENATE"}]',
        },
    )
    assert row["activity_index"] == 2
    assert row["government_entities"] == [{"id": "1", "name": "SENATE"}, None, {"id": "1", "name": "SENATE"}]
    assert map_subject("gao_reports", {"report_id": "g", "topics_json": "[]"})["topics"] == []
    assert map_subject("gao_reports", {"report_id": "g", "topics_json": None})["topics"] is None


def test_registration_nullable_key_and_source_status_survive():
    row = map_subject(
        "sam_entities",
        {"uei": "ABC", "entity_eft_indicator": None, "registration_status": "Active", "exclusion_status_flag": "N"},
    )
    assert row["entity_eft_indicator"] is None
    assert row["registration_status"] == "Active" and row["exclusion_status_flag"] == "N"


def test_timestamp_is_utc_and_crs_status_is_domain_data():
    row = map_subject(
        "crs_reports",
        {"report_id": "R1", "version": "2", "status": "Active", "update_date": "2026-10-03T04:00:00-04:00"},
    )
    assert row["update_date"] == datetime(2026, 10, 3, 8, tzinfo=timezone.utc)
    assert row["version"] == 2 and row["status"] == "Active"


def test_unknown_fields_and_invalid_source_values_refuse():
    with pytest.raises(GovernmentShapeError, match="unclassified"):
        map_subject("gao_reports", {"report_id": "g", "unknown": "data"})
    for row in ({"report_id": "g", "topics_json": "[3]"}, {"report_id": "g", "published_date": "2026-02-31"}):
        with pytest.raises(GovernmentShapeError):
            map_subject("gao_reports", row)
