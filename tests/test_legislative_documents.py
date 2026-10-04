"""Domain preservation at the legislative document/receipt boundary."""

import json
from decimal import Decimal

import pyarrow as pa
import pytest

from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_regs.legislative_documents import (
    LegislativeShapeError,
    body_version_id,
    field_registry,
    map_subject as optional_subject,
    subject_schema,
)


def required_subject(dataset, values):
    subject = optional_subject(dataset, values)
    assert subject is not None
    return subject


def row(dataset, **values):
    return {f["name"]: None for f in field_registry()[dataset]["fields"]} | values


def printing(dataset, **values):
    return row(dataset, bill_id="119-hr-1", version_code="ih", source="govinfo", **values)


def pair(dataset, **values):
    return row(dataset, bill_id="119-hr-1", from_version_code="ih", from_source="govinfo",
               to_version_code="enr", to_source="govinfo", **values)


@pytest.mark.parametrize("dataset", field_registry())
def test_every_source_owner_field_is_explicit(dataset):
    spec = field_registry()[dataset]
    if dataset in TABLE_CONTRACTS:
        assert {f["name"] for f in spec["fields"]} == set(TABLE_CONTRACTS[dataset].columns)
    schema = subject_schema(dataset)
    assert all(not name.endswith("_json") for name in schema.names)
    assert len(set(schema.names)) == len(schema.names)
    assert spec["processing_only"] == (len(schema) == 0)


def test_printing_keys_resolve_across_versions_sections_and_diff_sides():
    version = required_subject("bill_versions", printing("bill_versions"))
    section = required_subject("bill_sections", printing("bill_sections", seq="0", body="A", body_sha256="a" * 64))
    diff = required_subject("section_diff_items", pair("section_diff_items", seq="0", from_text_sha256="a" * 64))
    other = required_subject("bill_versions", printing("bill_versions") | {"source": "govinfo-pdf"})
    assert version["printing_id"] == section["printing_id"] == diff["from_printing_id"]
    assert other["printing_id"] != version["printing_id"]
    assert section["body_version_id"] == diff["from_body_version_id"]


def test_citation_occurrences_stay_distinct_by_body_and_offset():
    original = row("document_citations", document_key="x", document_kind="report", cite_kind="usc_section",
                   target_key="5-552", span_start="0", span_end="8", text_sha256="a" * 64,
                   matched_text="5 USC 552", rule_version="001", target_resolved="false")
    first = required_subject("document_citations", original)
    changed = required_subject("document_citations", original | {"text_sha256": "b" * 64})
    repeat = required_subject("document_citations", original | {"span_start": "20"})
    assert first["body_version_id"] != changed["body_version_id"]
    assert first["span_start"] != repeat["span_start"]
    assert "target_resolved" not in first and "rule_version" not in first


def test_native_lists_keep_order_repeats_nulls_and_empty():
    source = row("budget_volumes", package_id="BUDGET-2027-BUD", associated_laws_json='["a",null,"a"]',
                 associated_bills_json='[]', associated_usc_sections_json='[{"usc_key":"5-552","detail":null}]')
    subject = required_subject("budget_volumes", source)
    assert subject["associated_laws"] == ["a", None, "a"]
    assert subject["associated_bills"] == []
    assert subject["associated_cfr_parts"] is None
    assert subject["associated_usc_sections"] == [{"usc_key": "5-552", "detail": None}]


def test_cell_amounts_keep_cell_grouping_and_exact_signed_money():
    source = row("senate_expenditures", package_id="GPO-CDOC-119sdoc3", file_name="part.pdf", page="1",
                 table_ordinal="0", row_ordinal="2", text_sha256="a" * 64, cells_ruled="false",
                 amounts_json='[null,[],[{"amount":"-88657.02","text":"-88,657.02"},null]]')
    subject = required_subject("senate_expenditures", source)
    assert subject["amounts"] == [None, [], [{"amount": Decimal("-88657.02"), "text": "-88,657.02"}, None]]
    assert subject["cells_ruled"] is False
    table = pa.Table.from_pylist([subject], schema=subject_schema("senate_expenditures"))
    assert table.to_pylist()[0]["amounts"] == subject["amounts"]


def test_financial_delta_and_pairing_claim_remain_domain_data():
    subject = required_subject("financial_changes", pair("financial_changes", seq="0", amount_index="0",
                          from_amount="9007199254740993.01", to_amount="9007199254740993.02",
                          delta="0.01", pairing_claim="word_alignment"))
    assert subject["to_amount"] - subject["from_amount"] == subject["delta"] == Decimal("0.01")
    assert subject["pairing_claim"] == "word_alignment"


@pytest.mark.parametrize("invalid", ['[NaN]', '[1.001]', '[{"amount":"1.001","text":"1.001"}]', 'not json'])
def test_invalid_money_or_json_refuses_instead_of_rounding_or_empty(invalid):
    source = pair("section_diff_items", seq="0", financial_from_amounts_json=invalid)
    with pytest.raises(LegislativeShapeError):
        required_subject("section_diff_items", source)


def test_domain_status_and_publisher_absence_words_survive():
    law = required_subject("table3_records", row("table3_records", act_key="119-1", seq="0", status="repealed"))
    report = required_subject("committee_reports", row("committee_reports", package_id="CRPT-119hrpt1",
        part_id="CRPT-119hrpt1", report_states_estimate="false", estimate_absence_reason="The estimate was not received."))
    assert law["status"] == "repealed"
    assert report["estimate_absence_reason"] == "The estimate was not received."
    assert report["report_states_estimate"] is False


def test_native_candidates_keep_occurrences_without_lookup_diagnostics():
    candidate = {"cite_kind": "usc_section", "normalized_key": "5-552", "target_key": "5-552",
                 "span_start": 1, "span_end": 9, "target_status": "missing", "candidate_keys": []}
    source = row("native_legal_references", scope_id="scope", input_sha256="a" * 64, occurrence_index="0",
                 attributes_json='{"id":"section-a","href":""}', href="", text_runs_json='[]',
                 target_candidates_json=json.dumps([candidate, candidate]))
    subject = required_subject("native_legal_references", source)
    assert subject["href"] == "" and subject["text_runs"] == []
    assert subject["native_element_id"] == "section-a"
    assert len(subject["target_candidates"]) == 2
    assert "target_status" not in subject["target_candidates"][0]


def test_extra_top_level_and_nested_fields_require_an_audit():
    with pytest.raises(LegislativeShapeError, match="unreviewed input"):
        required_subject("bill_versions", printing("bill_versions", unexpected="new"))
    with pytest.raises(LegislativeShapeError, match="unreviewed struct"):
        required_subject("budget_volumes", row("budget_volumes", package_id="x", associated_bills_json='[{"extra":"x"}]'))


@pytest.mark.parametrize("dataset", ["committee_report_reads", "native_legal_reference_reads"])
def test_processing_checkpoints_never_create_subject_rows(dataset):
    assert optional_subject(dataset, row(dataset, **{k: "scope" for k in field_registry()[dataset]["identity_fields"]})) is None


def test_body_version_never_accepts_partial_hash():
    with pytest.raises(LegislativeShapeError):
        body_version_id("sha256:abc")


def test_native_diff_endpoints_and_held_citation_keys_resolve():
    import duckdb
    from spicy_regs.legislative_citation_sources import source_digests
    from spicy_regs.relationship_views.legislative_diffs import endpoints, version_endpoints

    section = required_subject("bill_sections", printing("bill_sections", seq="0", element_id="s1", body="hello", body_sha256="a" * 64))
    versions = [required_subject("bill_versions", printing("bill_versions", sha256="b" * 64)),
                required_subject("bill_versions", printing("bill_versions", sha256="c" * 64) | {"version_code": "enr"})]
    item = required_subject("section_diff_items", pair("section_diff_items", seq="0", from_element_id="s1", from_text_sha256="a" * 64))
    summary = required_subject("section_diffs", pair("section_diffs"))
    citation = required_subject("document_citations", row("document_citations", document_kind="bill_section",
        document_key='["119-hr-1","ih","govinfo","0"]', cite_kind="usc_section", target_key="5-552", span_start="0", text_sha256="a" * 64))
    with duckdb.connect() as cursor:
        for name, values in [("bill_sections", [section]), ("bill_versions", versions),
                             ("section_diff_items", [item]), ("section_diffs", [summary])]:
            cursor.register(name, pa.Table.from_pylist(values, schema=subject_schema(name)))
        results = cursor.execute(endpoints({})).to_arrow_table().to_pylist()
        assert {r["endpoint_side"]: r["target_status"] for r in results} == {"from": "found", "to": "unsupported"}
        assert next(r for r in results if r["endpoint_side"] == "from")["body_version_status"] == "matches"
        versions = cursor.execute(version_endpoints({})).to_arrow_table().to_pylist()
        assert all(isinstance(r["candidate_versions"], list) for r in versions)
        assert len(source_digests(cursor, "bill_section", citation["document_key"])) == 1


def test_repeated_object_properties_refuse_with_raw_input_retained():
    with pytest.raises(LegislativeShapeError, match="invalid retained JSON"):
        required_subject("budget_volumes", row("budget_volumes", package_id="x",
                    associated_bills_json='[{"bill_id":"a","bill_id":"b"}]'))


@pytest.mark.parametrize("dataset,values", [
    ("bill_summaries", {"summary": "A summary", "audience": "Readers", "top_provisions_json": '["A",null,"A"]'}),
    ("section_classifications", {"seq": "0", "label": "funding", "confidence": "0.9", "match_path": "a\x1fb"}),
])
def test_empty_held_model_tables_have_explicit_nonempty_conversion_controls(dataset, values):
    subject = required_subject(dataset, printing(dataset, **values))
    assert subject and "model" not in subject and "confidence" not in subject


def test_diff_summary_lists_are_independent_and_ordered():
    subject = required_subject("diff_summaries", pair("diff_summaries", headline="Changed",
                key_changes_json='["a","a"]', sections_added_json='[]', sections_removed_json=None,
                dollar_changes_json='["Increased by $0.01"]'))
    assert subject["key_changes"] == ["a", "a"]
    assert subject["sections_added"] == [] and subject["sections_removed"] is None
