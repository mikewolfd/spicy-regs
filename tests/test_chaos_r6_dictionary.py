"""Declared-metadata repairs from the 2026-10-03 persona test (round 6, implementer B).

Hermetic: the subject field, against the committed ``descriptions.yaml`` and the in-code schema.
"""

from __future__ import annotations

import pytest

from spicy_regs import data_dictionary as dd


def _entries() -> dict:
    return {table: dict(entry) for table, entry in dd.load_descriptions().items()}


def _prose(table: str, field: str = "data_quality") -> str:
    return " ".join((dd.load_descriptions()[table].get(field) or "").split())


def _column(table: str, column: str) -> str:
    return " ".join(dd.load_descriptions()[table]["columns"][column].split())


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
    assert {entry["subject"] for entry in metadata.values()} == set(dd.SUBJECTS)
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
    assert {base for base in bases if entries.get(base, {}).get("subject") not in dd.SUBJECTS} == set()
