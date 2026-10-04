import json
from datetime import date

import pytest

from spicy_regs.transforms.regulations_shape import (
    IDENTITIES,
    LEGACY_COLUMNS,
    RECEIPT_COLUMNS,
    RegulationsShapeError,
    restore_legacy_record,
    shape_record,
    subject_schema,
)


def test_null_empty_duplicates_and_raw_conversion_are_distinct():
    for value, native in [(None, None), ("null", None), ("[]", []), ('["A",null,"A"]', ["A", None, "A"])]:
        row = {"document_number": "2026-1", "publication_date": "2026-01-01", "docket_ids_json": value}
        shaped = shape_record("federal_register", row)
        assert shaped["docket_ids"] == native
        assert shaped["raw_conversion_inputs"]["docket_ids_json"] == value
        assert restore_legacy_record("federal_register", shaped)["docket_ids_json"] == value


def test_missing_conversion_is_not_invented_as_explicit_null():
    shaped = shape_record("documents", {"document_id": "D"})
    assert "attachments_json" not in shaped["raw_conversion_inputs"]
    assert "attachments_json" not in restore_legacy_record("documents", shaped)


@pytest.mark.parametrize("raw", ["", "{", "{}", '[{"part":1,"part":2}]', "NaN"])
def test_malformed_conversion_refuses_without_a_false_empty_list(raw):
    with pytest.raises(RegulationsShapeError):
        shape_record("federal_register", {"cfr_references_json": raw})


def test_unknown_nested_fields_refuse_instead_of_arrow_dropping_them():
    with pytest.raises(RegulationsShapeError):
        shape_record("documents", {"attachments_json": '[{"url":"u","unexpected":"source value"}]'})
    with pytest.raises(RegulationsShapeError):
        shape_record("dockets", {"unexpected": "source value"})


def test_attachment_order_repeated_url_sizes_and_restrictions_survive():
    formats = [{"url": "u", "format": "pdf", "size": 5}, None, {"url": "u", "format": "pdf", "size": 8}]
    row = {
        "document_id": "D",
        "attachments_json": json.dumps(formats),
        "attachment_records_json": json.dumps(
            [
                {
                    "id": "a",
                    "type": "attachments",
                    "attributes": {"fileFormats": None, "restrictReasonType": "Restricted"},
                },
                {"id": "a", "type": "attachments", "attributes": {"fileFormats": []}},
            ]
        ),
    }
    shaped = shape_record("documents", row)
    assert shaped["attachments"] == formats
    assert [x["attachment_id"] for x in shaped["attachment_records"]] == ["a", "a"]
    assert shaped["attachment_records"][0]["file_formats"] is None
    assert shaped["attachment_records"][1]["file_formats"] == []
    assert shaped["attachment_records"][0]["restrict_reason_type"] == "Restricted"


def test_timetable_keeps_literal_month_precision_and_non_dates():
    values = [
        {"action": "NPRM", "date": "04/00/2026", "fr_citation": None},
        {"action": "Final", "date": "To Be Determined", "fr_citation": "91 FR 100"},
    ]
    shaped = shape_record(
        "unified_agenda",
        {
            "rin": "1000-AA00",
            "agenda_edition": "202604",
            "rin_status": "Withdrawn",
            "rule_stage": "Completed",
            "timetable_json": json.dumps(values),
        },
    )
    assert shaped["timetable"] == values
    assert shaped["rin_status"] == "Withdrawn"
    assert shaped["rule_stage"] == "Completed"


def test_cfr_part_strings_do_not_lose_alpha_suffixes_or_printed_ranges():
    shaped = shape_record(
        "federal_register",
        {
            "cfr_references_json": json.dumps(
                [
                    {"title": 40, "part": 60, "chapter": None, "citation_url": "url"},
                    {"title": 14, "part": "1203a", "chapter": "I"},
                    {"title": 40, "part": "80-85", "chapter": None},
                ]
            )
        },
    )
    assert [x["part"] for x in shaped["cfr_references"]] == ["60", "1203a", "80-85"]
    assert "citation_url" in shaped["raw_conversion_inputs"]["cfr_references_json"]


def test_domain_status_and_dates_stay_but_extraction_status_moves():
    shaped = shape_record(
        "documents",
        {
            "document_id": "D",
            "modify_date": "2026-01-01",
            "withdrawn": "true",
            "reason_withdrawn": "Agency withdrew proposal",
            "text_content": "held text",
            "text_extraction_status": "error",
        },
    )
    schema = subject_schema("documents")
    assert shaped["withdrawn"] is True
    assert shaped["reason_withdrawn"] == "Agency withdrew proposal"
    assert shaped["modify_date"] == "2026-01-01"
    assert shaped["text_extraction_status"] == "error"
    assert "text_extraction_status" not in schema.names
    assert "text_content" in schema.names


def test_rule_targets_keep_witness_distinct_identity_without_exposing_source():
    base = {"docket_id": "D", "cfr_ref": "40-60", "rin": "1000-AA00"}
    a = shape_record("rule_targets", {**base, "source": "docket_rin"})
    b = shape_record("rule_targets", {**base, "source": "document_rin"})
    assert a["rule_target_id"] != b["rule_target_id"]
    assert "source" not in subject_schema("rule_targets").names


def test_statistical_cells_keep_null_agency_and_estimand_fields():
    values = {
        "agency_code": None,
        "stratum": "all",
        "rules": 31,
        "finals": 9,
        "withdrawals": 2,
        "censored": 20,
        "suppressed": False,
        "median_days": None,
        "q1_days": 120,
        "censor_date": date(2026, 9, 27),
    }
    shaped = shape_record("agency_lifecycle_stats", values)
    assert all(shaped[k] == v for k, v in values.items())
    assert "pre_2008_coverage" in subject_schema("rulemaking_lifecycles").names


def test_each_assigned_table_has_explicit_columns_schema_and_identity():
    assert set(LEGACY_COLUMNS) == set(RECEIPT_COLUMNS) == set(IDENTITIES)
    for table in LEGACY_COLUMNS:
        schema = subject_schema(table)
        assert set(IDENTITIES[table]) <= set(schema.names)
        assert not set(RECEIPT_COLUMNS[table]) & set(schema.names)
        assert not any(f.name.endswith("_json") for f in schema)
        assert len(set(schema.names)) == len(schema)


def test_wrong_native_values_do_not_coerce_to_strings_or_integers():
    with pytest.raises(RegulationsShapeError):
        shape_record("documents", {"attachments_json": '[{"size":"3"}]'})
    with pytest.raises(RegulationsShapeError):
        shape_record("regulatory_agenda_items", {"linked_proceeding_count": "1.0"})
    with pytest.raises(RegulationsShapeError):
        shape_record("dockets", {"docket_id": 42})


def test_restore_requires_a_qualified_receipt():
    with pytest.raises(RegulationsShapeError):
        restore_legacy_record("comments", {"comment_id": "C"})


def test_rule_target_dated_references_preserve_repeats_without_promoting_ambiguity():
    refs = [
        {"candidate_ids": ["2026-1@2026-01-01"], "status": "dated"},
        {"candidate_ids": ["2026-1@2026-01-01"], "status": "single_candidate_in_input"},
        {"candidate_ids": ["2026-2@2026-01-01", "2026-2@2026-01-02"], "status": "ambiguous"},
    ]
    shaped = shape_record(
        "rule_targets", {"docket_id": "D", "source": "document_fr_doc", "fr_references_json": json.dumps(refs)}
    )
    assert shaped["fr_document_ids"] == ["2026-1@2026-01-01", "2026-1@2026-01-01", None]
    assert shaped["fr_references_json"] == json.dumps(refs)
