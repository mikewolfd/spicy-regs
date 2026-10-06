"""Declared-metadata repairs from the 2026-10-03 persona test (round 6, implementer B).

Hermetic: the subject field and the rewritten sentences, against the committed ``descriptions.yaml`` and the
in-code schema. The claims that need the published data are in ``test_chaos_r6_dictionary_live.py``; the joins are
in ``test_table_joins.py``.
"""

from __future__ import annotations

import pytest

from spicy_regs import data_dictionary as dd


def _entries() -> dict:
    return {table: dict(entry) for table, entry in dd.load_descriptions().items()}


def _prose(table: str, field: str = "data_quality") -> str:
    return " ".join((dd.load_descriptions()[table].get(field) or "").split())


def _column(table: str, column: str) -> str:
    from spicy_regs.fec_receipt_adapter import processing_declarations
    entry = dd.load_descriptions().get(table, {})
    prose = entry.get("columns", {}).get(column) or processing_declarations()[table]["descriptions"][column]
    return " ".join(prose.split())


# --------------------------------------------------------------------------- #
# The subject field (owner decision 8, 2026-10-03: group the table list by subject).
# --------------------------------------------------------------------------- #
def test_every_table_states_one_known_subject():
    entries = dd.load_descriptions()
    assert {table: entries[table].get("subject") for table in dd.TABLES if entries[table].get("subject")
            not in dd.SUBJECTS} == {}


@pytest.mark.parametrize("subject", [None, "", "elections"])
def test_the_check_refuses_a_missing_or_unknown_subject(subject):
    broken = _entries()
    broken["dockets"] = {**broken["dockets"], "subject": subject}
    errors = [error for error in dd.check_descriptions(dd.expected_schemas(), broken) if error.startswith("[dockets]")]
    assert len(errors) == 1 and "subject" in errors[0], errors


def test_the_subject_is_carried_into_the_server_metadata():
    metadata = dd.build_mcp_metadata(dd.load_descriptions(), dd.expected_schemas())
    assert {entry["subject"] for entry in metadata.values() if entry.get("subject")} == set(dd.SUBJECTS)
    assert metadata["rulemaking_lifecycles"]["subject"] == "rulemaking"
    assert metadata["gao_decisions"]["subject"] == metadata["crs_reports"]["subject"] == "oversight"
    assert metadata["sam_entities"]["subject"] == metadata["usaspending_recipients"]["subject"] == "spending"
    assert metadata["fec_receipts"]["subject"] == "campaign_finance"


@pytest.mark.parametrize(("subject", "tables"), [
    ("lobbying", {"lobbying_filings", "lobbying_activities", "lobbying_activity_lobbyists"}),
    ("courts", {"court_dockets", "court_docket_groups", "court_opinions", "court_opinion_pdf_extractions"}),
])
def test_a_source_family_shares_one_subject(subject, tables):
    entries = dd.load_descriptions()
    assert {entries[table]["subject"] for table in tables} == {subject}


def test_every_relationship_view_has_a_base_table_with_a_subject():
    """A relationship view takes its base table's subject: the first table it reads, which must be a dictionary table."""
    from spicy_regs.relationship_views import RELATIONSHIP_VIEWS, SQL_RELATIONSHIP_VIEWS

    entries = dd.load_descriptions()
    bases = {spec.source_table for spec in RELATIONSHIP_VIEWS} | {next(iter(spec.required)) for spec in SQL_RELATIONSHIP_VIEWS}
    from spicy_regs.fec_receipt_adapter import processing_declarations
    processing = processing_declarations()
    assert {base for base in bases if (entries.get(base, {}).get("subject") or
            processing.get(base, {}).get("subject")) not in dd.SUBJECTS} == set()


# --------------------------------------------------------------------------- #
# False or stale sentences out.
# --------------------------------------------------------------------------- #
def test_lobbyist_id_is_one_registrants_record_not_a_person():
    """H3 (wanjiru): LDA's own record for an id names one registrant; Stephen Holland is 146616 and 151303."""
    text = _column("lobbying_activity_lobbyists", "lobbyist_id")
    assert "stable identity of the person" not in text
    assert "one registrant's record of a lobbyist" in text and "146616" in text and "151303" in text


def test_covered_position_is_free_text_per_activity_with_no_crosswalk():
    text = _column("lobbying_activity_lobbyists", "covered_position")
    assert "null when none" not in text
    assert all(phrase in text for phrase in ("free text", "not parsed", "for this activity", "`committees`", "N/A"))


def test_gao_decisions_says_how_gaos_statistics_count_and_no_longer_promises_a_walk():
    """L1 and L3 (alasdair): the 42d9c8a5 sentence was false on the next generation; GAO counts B-numbers."""
    text = _prose("gao_decisions")
    assert "until a walk is read again" not in text and "42d9c8a5" not in text
    assert "not when it decided it" in text
    assert all(phrase in text for phrase in ("B-number", "merit decisions", "fiscal year", "B-423717"))
    assert "their figures are not held" in text


def test_sam_entities_no_longer_promises_a_uei_tie_to_commenters():
    text = _prose("sam_entities", "summary")
    assert "the same UEI ties" not in text and "name match" in text


def test_independent_expenditures_name_the_cover_route_that_works():
    """L5 (vikram): the header associations find no filing for any row; the cover by collection_id does."""
    summary, coverage = _prose("fec_independent_expenditures", "summary"), _prose("fec_independent_expenditures", "coverage")
    assert "Use the filing/header associations to locate supporting cover metadata" not in summary
    assert "fec_filing_report_observations" in summary and "`reported-form`" in summary
    assert "daily electronic-filing archives" in coverage and "not a cycle" in coverage
    assert "docquery.fec.gov" in _prose("fec_independent_expenditures")


def test_committee_meetings_points_at_the_mods_witnesses_and_the_meeting_documents():
    """L13, L14 (caetano): MODS witnesses are read since round 5; markups often omit their bills."""
    text = _prose("committee_meetings")
    assert "does not read MODS for witnesses" not in text
    assert "`hearing_transcripts.witnesses_json`" in text and "Senate and NoChamber meetings list no witnesses here" in text
    assert "the route this table reads" in text
    assert all(phrase in text for phrase in ("`meeting_documents_json`", "Bills and Resolutions", "no committee",
                                             "`Scheduled`", "338326"))
    assert "about six runs" not in text


@pytest.mark.parametrize(("table", "pin"), [
    ("bill_versions", "d380cdc0"), ("bill_versions", "82c8088d"), ("bill_sections", "55671b43"),
    ("section_diffs", "55671b43"),
])
def test_repair_notes_about_defects_a_later_generation_fixed_are_gone(table, pin):
    assert pin not in _prose(table)


def test_drifted_counts_point_at_their_query_instead():
    assert "1 of the 158" not in _prose("document_citations")
    assert "count distinct `document_key` by `document_kind`" in _prose("document_citations")
    assert "all 164 NULL rows" not in _prose("hearing_transcripts")
    assert "`committee_report_reads` says which" in _prose("hearing_transcripts")


def test_table3_names_the_law_the_publisher_left_out():
    assert "119-70" in _prose("table3_records")


def test_the_header_states_the_writing_rule_for_pinned_sentences():
    header = dd.DEFAULT_DESCRIPTIONS.read_text(encoding="utf-8").split("\ntables:\n", 1)[0]
    flat = " ".join(line.lstrip("# ") for line in header.splitlines())
    assert "a pin anchors a past measurement" in flat.lower()
    assert "deleted, not kept as history" in flat


# --------------------------------------------------------------------------- #
# Map-time status columns: one honest template, naming where the value is decided.
# --------------------------------------------------------------------------- #
#: The tables whose stored filing_link_status is `unresolved` on every row (footer sweep, 2026-10-03).
MAP_TIME_FILING_LINK = (
    "fec_account_transfers", "fec_allocated_disbursements", "fec_allocation_bases", "fec_api_response_controls",
    "fec_bundled_contributions", "fec_candidate_api_observations", "fec_committee_master_observations",
    "fec_committee_observations", "fec_communication_costs", "fec_contribution_aggregates",
    "fec_coordinated_party_expenditures", "fec_debts", "fec_disbursements", "fec_electioneering_communications",
    "fec_filing_report_observations", "fec_filing_text_observations", "fec_inaugural_donations",
    "fec_independent_expenditures", "fec_intercommittee_transactions", "fec_loan_guarantors", "fec_loan_terms",
    "fec_loans", "fec_lobbyist_registrations", "fec_postgres_committee_history_observations", "fec_quality_notices",
    "fec_receipts", "fec_reported_financial_summaries",
)


def test_filing_link_status_reads_from_one_template_that_says_it_is_set_when_mapped():
    from spicy_regs.fec_receipt_adapter import processing_declarations
    processing = processing_declarations()
    applicable = {table for table in MAP_TIME_FILING_LINK
                  if "filing_link_status" in processing[table]["descriptions"]}
    texts = {_column(table, "filing_link_status") for table in applicable}
    assert applicable and len(texts) == 1, texts
    (text,) = texts
    assert text.startswith("Always `unresolved`") and "before any filing is looked up" in text
    assert "_filing_associations" in text and "_native_filing_associations" in text


def test_the_other_map_time_statuses_name_where_they_are_decided():
    assert "`fec_filing_reference_resolution`" in _column("fec_filing_links", "target_resolution_status")
    assert "resolves in the retained filing population" not in _column("fec_filing_links", "target_resolution_status")
    entity = {_column(table, "entity_reference_status")
              for table in ("fec_reported_financial_summaries", "fec_contribution_aggregates")}
    assert len(entity) == 1 and all(name in next(iter(entity)) for name in ("`fec_committees", "`fec_candidate_history",
                                                                            "P00000001"))
    assert "`back_reference_transaction_id`" in _column("fec_loan_guarantors", "loan_link_status")


@pytest.mark.parametrize('table', ['committee_meetings', 'hearing_transcripts'])
@pytest.mark.parametrize('native', [False, True], ids=['legacy-json', 'native-list'])
def test_live_witness_claim_counts_stated_witnesses_and_preserves_null_and_empty(table, native):
    import duckdb
    import pyarrow as pa
    from spicy_regs.subject_catalog import policies
    from tests.test_chaos_r6_dictionary_live import _stated_witnesses

    if native:
        field = policies()[table].subject_schema.field('witnesses')
        schema = pa.schema([field])
        witness = {'name': 'Original witness'} if pa.types.is_struct(field.type.value_type) else 'Original witness'
        values = [None, [], [witness]]
        data = pa.Table.from_pylist([{'witnesses': value} for value in values], schema=schema)
    else:
        data = pa.table({'witnesses_json': [None, '[]', '[{"name":"Original witness"}]']})
    with duckdb.connect() as con:
        con.register('held', data)
        columns = {row[0]: row[1] for row in con.execute('DESCRIBE held').fetchall()}
        condition = _stated_witnesses(table, columns)
        assert con.execute(f'SELECT count(*) FROM held WHERE {condition}').fetchone() == (1,)


@pytest.mark.parametrize('columns', [{}, {'witnesses': 'VARCHAR'},
                                     {'witnesses_json': 'INTEGER'},
                                     {'witnesses': 'VARCHAR[]', 'witnesses_json': 'VARCHAR'}])
def test_live_witness_claim_refuses_missing_ambiguous_or_undeclared_representations(columns):
    from tests.test_chaos_r6_dictionary_live import _stated_witnesses

    with pytest.raises(ValueError):
        _stated_witnesses('committee_meetings', columns)
