"""Only recorded reads become typed main status rows; caches never invent them."""
import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import read_with_receipts
from spicy_regs.navigation_read_outcomes import POLICIES, recorded_rows, write_recorded_outcomes
from spicy_regs.selected_generations import SelectedDataset


def journal(tmp_path, events):
    path = tmp_path / "journal.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in events))
    return path


def test_independent_nomination_attempts_have_complete_native_keys_and_distinct_states(tmp_path):
    path = journal(tmp_path, [
        {"event": "capture", "requested_url": "https://example.test/committees", "resolved_url": "https://example.test/committees",
         "sha256": "sha256:" + "a" * 64, "capture_id": "c"},
        {"event": "congress-detail-result", "table": "nominations", "congress": "119", "citation": "PN3-02",
         "source_identity": {"congress": "119", "citation": "PN3-02", "number": "3", "part_number": "02"},
         "read_outcome": "read", "read_field": "committees", "source_url": "https://example.test/committees",
         "field_states": {"committees": "stated-empty"}},
        {"event": "congress-detail-result", "table": "nominations", "congress": "119", "citation": "PN3-02",
         "read_outcome": "failed", "read_field": "hearings", "error_type": "PagedJsonSourceError"},
        {"event": "congress-index-selection", "table": "nominations", "outcomes": {"held": 1, "deferred": 2}},
    ])
    subjects, receipts = write_recorded_outcomes(path, tmp_path / "out", generation_id="g1", tables=["nominations_detail_reads"])
    rows = pq.read_table(subjects[0]).to_pylist()
    assert len(rows) == 2 and rows[0]["congress"] == 119
    assert (rows[0]["citation"], rows[0]["number"], rows[0]["part_number"]) == ("PN3-02", "3", "02")
    assert rows[0]["field_states"] == [{"field": "committees", "state": "stated-empty"}]
    assert rows[1]["outcome"] == "failed" and rows[1]["field_states"] is None
    assert rows[0]["source_witnesses"][0]["sha256"] == "sha256:" + "a" * 64
    assert rows[1]["source_witnesses"] == []
    assert len(list(read_with_receipts(subjects, receipts, POLICIES["nominations_detail_reads"], generation_id="g1"))) == 2


def test_no_journal_and_cached_selection_produce_no_invented_attempts(tmp_path):
    subjects, _ = write_recorded_outcomes(None, tmp_path / "out", generation_id="g", tables=["committee_meetings_detail_reads"])
    assert pq.read_table(subjects[0]).num_rows == 0


def test_qualified_prior_attempt_survives_cached_build_with_its_original_generation(tmp_path):
    path = journal(tmp_path, [{"event": "congress-detail-result", "table": "nominations", "congress": "119",
                              "citation": "PN1", "read_outcome": "read"}])
    subjects, receipts = write_recorded_outcomes(path, tmp_path / "first", generation_id="first", tables=["nominations_detail_reads"])
    held = SelectedDataset("nominations_detail_reads", tuple(subjects), receipts[0], "first")
    next_subjects, next_receipts = write_recorded_outcomes(None, tmp_path / "next", generation_id="next",
        tables=["nominations_detail_reads"], priors={"nominations_detail_reads": held})
    [row] = read_with_receipts(next_subjects, next_receipts, POLICIES["nominations_detail_reads"], generation_id="next")
    assert row["generation_id"] == "first" and row["attempt_id"] == "journal:0"


def test_house_projection_only_links_digest_of_the_exact_printed_granule(tmp_path):
    path = journal(tmp_path, [{"event": "house-record-result", "communication_id": "119-ec-2", "congress": "119",
        "communication_type": "ec", "number": "2", "rule_version": "r", "complete_scope": True, "outcome": "matched",
        "qualified_occurrences": 1, "conflict_status": "no-conflict", "scope_packages": ["CREC-2026-01-01"],
        "witnesses": [{"entry": {"record_package_id": "CREC-2026-01-01", "record_granule_id": "A"}, "marker": "m",
            "input": {"generationId": "selected", "processing": {"sha256": "p"}},
            "bodies": [{"package_id": "CREC-2026-01-01", "granule_id": "B", "sha256": "wrong"},
                       {"package_id": "CREC-2026-01-01", "granule_id": "A", "sha256": "right"}]}]}])
    _, row, _ = next(recorded_rows(path, generation_id="g"))
    assert row["communication_id"] == "119-ec-2" and row["witnesses"][0]["body_sha256"] == "right"
    assert row["witnesses"][0]["input_generation"] == "selected"


@pytest.mark.parametrize("value", ["119.0", "1e2", "119\n", True, "１１９"])
def test_bad_congress_refuses_instead_of_guessing_a_key(tmp_path, value):
    path = journal(tmp_path, [{"event": "congress-detail-result", "table": "nominations", "congress": value,
                              "citation": "PN1", "read_outcome": "read"}])
    with pytest.raises(ValueError, match="native Congress"):
        list(recorded_rows(path, generation_id="g"))


def test_original_to_native_members_keep_separate_hashes_and_readable_schema(tmp_path):
    import hashlib
    from spicy_regs.legislative_receipts import write_legislative_outputs, restore_prior
    from tests.test_legislative_receipts import retained, row

    source = retained(tmp_path / "source", "laws", [row("laws", congress="119", law_type="public", number="1", law_id="119-public-1")],
                      {b"producer-context": b"preserve this footer"})
    manifest = write_legislative_outputs([source], tmp_path / "bundle", generation_id="g", file_outcomes_table="laws_document_file_outcomes")
    [result] = pq.read_table(tmp_path / "bundle/laws_document_file_outcomes.parquet").to_pylist()
    assert result["original_byte_sha256"] == "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    assert result["state_witness_sha256"] is None and result["state"] == "converted-member"
    [native] = result["native_members"]
    assert native["sha256"] == "sha256:" + hashlib.sha256((tmp_path / "bundle/laws.parquet").read_bytes()).hexdigest()
    assert native["sha256"] != result["original_byte_sha256"]
    assert result["relative_path"] == "laws.parquet"
    assert all(not str(value).startswith(str(tmp_path)) for value in result.values())
    assert any(field["name"] == "congress" for field in result["source_schema"]) and result["source_metadata"]
    assert manifest["outcome_subjects"]["laws_document_file_outcomes"] == ["laws_document_file_outcomes.parquet"]
    assert pq.read_table(restore_prior(tmp_path / "bundle", tmp_path / "restore")["laws"]).to_pylist()[0]["law_id"] == "119-public-1"


def test_empty_partition_has_a_state_witness_without_an_invented_file_digest(tmp_path):
    from spicy_regs.legislative_receipts import write_legislative_outputs
    source = tmp_path / "bill_sections"
    source.mkdir()
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g", file_outcomes_table="bill_family_document_file_outcomes")
    [result] = pq.read_table(tmp_path / "bundle/bill_family_document_file_outcomes.parquet").to_pylist()
    assert result["state"] == "successful-empty-partition" and result["row_count"] == 0
    assert result["original_byte_sha256"] is None and result["relative_path"] is None
    assert result["native_members"] == [] and result["state_witness_sha256"].startswith("sha256:")


def test_completed_zero_citation_findings_publish_native_body_and_document_identity(tmp_path):
    from spicy_regs.legislative_receipts import write_legislative_outputs
    from tests.test_legislative_receipts import retained, row
    from spicy_regs.legislative_documents import native_document_key

    original = row("document_citation_reads", document_kind="bill_section",
        document_key='["119-hr-1","ih","govinfo","0"]', text_sha256="sha256:" + "a" * 64,
        citation_rows="0", rule_set_version="r", input_generation="pinned")
    source = retained(tmp_path / "source", "document_citation_reads", [original])
    write_legislative_outputs([source], tmp_path / "bundle", generation_id="g", file_outcomes_table="print_citations_document_file_outcomes")
    [result] = pq.read_table(tmp_path / "bundle/document_citation_read_outcomes.parquet").to_pylist()
    assert result["citation_rows"] == 0 and result["body_version_id"] == "body:sha256:" + "a" * 64
    assert result["document_key"] == native_document_key(original["document_kind"], original["document_key"])
    assert result["input_generation"] == "pinned"
    assert not (tmp_path / "bundle/document_citation_reads.parquet").exists()  # Original checkpoint policy stays compatible.


@pytest.mark.parametrize("tamper", ["missing", "duplicate", "extra", "row_count", "absolute", "witness"])
def test_original_to_native_member_mapping_refuses_inconsistent_evidence(tmp_path, tamper):
    import hashlib
    import pyarrow as pa
    from spicy_regs.etl_receipts import ReceiptContext
    from spicy_regs.navigation_read_outcomes import file_rows

    source = tmp_path / "laws.parquet"
    schema = pa.schema([("law_id", pa.string())])
    pq.write_table(pa.Table.from_pylist([{"law_id": "119-public-1"}], schema=schema), source)
    digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    state = {"dataset": "laws", "relative_path": "laws.parquet", "partitioned": False, "rows": 1,
             "sha256": digest, "schema": schema.serialize().to_pybytes(), "metadata": []}
    members = {"laws": ["laws.parquet"]}
    witnesses = [{"source_id": "laws", "sha256": digest}]
    if tamper == "missing":
        members["laws"] = []
    elif tamper == "duplicate":
        members["laws"] *= 2
    elif tamper == "extra":
        members["laws"].append("unmatched.parquet")
    elif tamper == "row_count":
        state["rows"] = 2
    elif tamper == "absolute":
        state["relative_path"] = str(source)
    else:
        witnesses[0]["sha256"] = "sha256:" + "b" * 64
    context = ReceiptContext("g", "a", "p", witnesses)
    with pytest.raises(ValueError):
        list(file_rows([({"file_state": state}, context)], members, tmp_path, generation_id="g"))


def test_paginated_source_digest_witnesses_ignore_only_credentials_and_page_offset(tmp_path):
    path = journal(tmp_path, [
        {"event": "capture", "requested_url": "https://api.congress.gov/v3/nomination/119/1/committees?limit=250&offset=0&api_key=%3Credacted%3E",
         "resolved_url": "https://api.congress.gov/v3/nomination/119/1/committees", "sha256": "first"},
        {"event": "capture", "requested_url": "https://api.congress.gov/v3/nomination/119/1/committees?offset=250&limit=250&api_key=%3Credacted%3E",
         "resolved_url": "https://api.congress.gov/v3/nomination/119/1/committees", "sha256": "second"},
        {"event": "capture", "requested_url": "https://api.congress.gov/v3/nomination/118/1/committees?limit=250", "sha256": "wrong-Congress"},
        {"event": "congress-detail-result", "table": "nominations", "congress": "119", "citation": "PN1",
         "source_url": "https://api.congress.gov/v3/nomination/119/1/committees?limit=250", "read_outcome": "read"},
    ])
    _, row, _ = next(recorded_rows(path, generation_id="g"))
    assert [w["sha256"] for w in row["source_witnesses"]] == ["first", "second"]
