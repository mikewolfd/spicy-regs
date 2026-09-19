#!/usr/bin/env python3
"""Generate and validate the Spicy Regs data dictionary.

The data dictionary has two layers:

* **Schema (source of truth, in code).** Column names and types come from
  :data:`spicy_regs.schemas.regulations.RECORD_TYPES` for the three core tables
  and from :data:`DERIVED_SCHEMAS` below for the four rollup tables. This keeps
  generation deterministic and offline.
* **Descriptions (curated prose).** Human descriptions live in
  ``data_dictionary/descriptions.yaml``, keyed by table and column.

``spicy-regs-dict check`` reconciles the two so they can't silently drift, and
``spicy-regs-dict generate`` renders one Markdown page per table for the MkDocs
site under ``docs/tables/``.

Usage::

    uv run spicy-regs-dict check                 # offline: descriptions vs in-code schema
    uv run spicy-regs-dict check --source r2     # also reconcile in-code schema vs live R2 parquet
    uv run spicy-regs-dict generate              # (re)write docs/tables/*.md
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from functools import lru_cache
import tempfile
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv

from spicy_regs.schemas.regulations import RECORD_TYPES

# Repo layout anchors (this file lives at src/spicy_regs/data_dictionary.py).
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DESCRIPTIONS = REPO_ROOT / "data_dictionary" / "descriptions.yaml"
DEFAULT_DOCS_TABLES_DIR = REPO_ROOT / "docs" / "tables"
DEFAULT_CATALOG_PATH = REPO_ROOT / "data_dictionary" / "catalog.json"
DEFAULT_CATALOG_DIGEST_PATH = REPO_ROOT / "data_dictionary" / "catalog.json.sha256"

#: Bumped when the catalog document's shape changes, so a reader can refuse a
#: shape it does not know rather than guess at a missing field.
CATALOG_FORMAT_VERSION = 3

#: The five kinds a coverage statement can be, as machine-readable tokens,
#: keyed by the prose prefix so the two cannot disagree.
#:
#: "Sampled" is the newest and the weakest claim: the table is filled from a
#: bounded slice of the publisher's archive chosen by a per-run cap, not by a
#: date window, so neither an end-to-end range nor a window describes it. The
#: bill-family and roll-call tables open this way until a full run has been
#: measured, because calling a capped first pass a "Window" would state a
#: density it does not have.
COVERAGE_KINDS: dict[str, str] = {
    "True range": "true_range",
    "Window": "window",
    "Sampled": "sampled",
    "Derived": "derived",
    "Not a range": "not_a_range",
}


def coverage_kind(coverage: str) -> str | None:
    """Return the machine-readable kind for a coverage statement, or None.

    Matches on the prose prefix, not the first whitespace-delimited token: the
    token carries a trailing period or comma depending on the sentence
    ("Derived." and "Derived," both occur), so a consumer splitting on a space
    would match neither.
    """
    text = coverage.strip()
    for prefix, kind in COVERAGE_KINDS.items():
        if text.startswith(prefix):
            return kind
    return None


DEFAULT_R2_BASE_URL = "https://data.spicy-regs.dev"

#: The tables hosted from spicy-docs' table contracts. Their columns and their
#: per-column prose both come from ``spicy_docs.schemas.TABLE_CONTRACTS``, so a
#: column added upstream fails ``spicy-regs-dict check`` here until the release
#: is adopted — it is never restated in this repository. ``congress_bills`` is
#: in this list and is also the one table that predates it: the contract keeps
#: its first ten columns in their published order and appends the rest.
CONTRACT_TABLES: tuple[str, ...] = (
    "congress_bills",
    "bill_actions",
    "bill_committees",
    "bill_publisher_summaries",
    "bill_versions",
    "bill_sections",
    "section_diffs",
    "section_diff_items",
    "financial_changes",
    "section_classifications",
    "bill_summaries",
    "diff_summaries",
    "public_activity_events",
    "amendments",
    "press_releases",
    "roll_call_votes",
    "member_votes",
    "members",
    "member_terms",
    "committee_reports",
    "report_sections",
    "hearing_transcripts",
    # A8/A9 (laws and rosters): the laws and committee-rosters rollups.
    "laws",
    "law_code_sections",
    "table3_records",
    "committees",
    "committee_assignments",
    # The Congress.gov index tables (gaps A5, A7, A10), each written by its own
    # rollup in pipelines/rollups/congress_index.py.
    "house_communications",
    "committee_meetings",
    "record_issues",
    "treaties",
    "nominations",
)


# Display order for the dictionary. The first three are the core record types;
# the rest are derived rollups. This is the full public R2 surface.
TABLES: tuple[str, ...] = (
    "dockets",
    "documents",
    "comments",
    "comments_index",
    "feed_summary",
    "agency_stats",
    "agency_monthly_volume",
    "rulemaking_lifecycles",
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "unified_agenda",
    "federal_register",
    "sam_entities",
    "lobbying_filings",
    "fec_committees",
    "org_committee_links",
    "gao_reports",
    "crs_reports",
    "court_dockets",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
    # The hosted tables, minus congress_bills — it predates them and keeps its
    # position above, so appending CONTRACT_TABLES wholesale would list it twice.
    *(name for name in CONTRACT_TABLES if name != "congress_bills"),
    # Published, but not contract tables: the bill-family rollup's own
    # processing state, and the vote references the roll-call rollup joins
    # against. Their columns are this repository's, so they live in
    # DERIVED_SCHEMAS and their prose is inline in descriptions.yaml.
    "bill_family_archives",
    "bill_vote_references",
    "bill_family_backfills",
    "bill_family_backfill_walks",
)

# Tables the MCP server (list_sources / describe_table / query_sql) exposes.
# This must equal spicy_regs.mcp_server.TABLES; a test enforces it so the docs'
# "queryable via MCP" flag can't drift from what the server actually serves.
MCP_QUERYABLE: frozenset[str] = frozenset(
    {
        "dockets",
        "documents",
        "comments",
        "comments_index",
        "feed_summary",
        "agency_stats",
        "agency_monthly_volume",
        "rulemaking_lifecycles",
        "fr_docket_links",
        "discovery_signals",
        "cfr_sections",
        "congress_bills",
        "unified_agenda",
        "federal_register",
        "sam_entities",
        "lobbying_filings",
        "fec_committees",
        "org_committee_links",
        "gao_reports",
        "crs_reports",
        "court_dockets",
        "usaspending_recipients",
        "fcc_proceedings",
        "fcc_filings",
        "bill_family_archives",
        "bill_vote_references",
        "bill_family_backfills",
        "bill_family_backfill_walks",
        *CONTRACT_TABLES,
    }
)

# Schemas for the derived rollups. These mirror the SQL/Polars schemas in
# src/spicy_regs/transforms/{build_feed_summary,build_agency_rollups,
# update_comments_index}.py. Types are DuckDB type names, matching what a
# DESCRIBE of the published parquet returns (see `check --source r2`).
DERIVED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "comments_index": [
        ("agency_code", "VARCHAR"),
        ("docket_id", "VARCHAR"),
        ("year", "BIGINT"),
        ("month", "BIGINT"),
        ("row_count", "BIGINT"),
    ],
    "feed_summary": [
        ("docket_id", "VARCHAR"),
        ("agency_code", "VARCHAR"),
        ("title", "VARCHAR"),
        ("docket_type", "VARCHAR"),
        ("modify_date", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("comment_count", "BIGINT"),
        ("comment_end_date", "VARCHAR"),
        ("date_created", "VARCHAR"),
    ],
    "agency_stats": [
        ("agency_code", "VARCHAR"),
        ("docket_count", "BIGINT"),
        ("document_count", "BIGINT"),
        ("comment_count", "BIGINT"),
    ],
    "agency_monthly_volume": [
        ("agency_code", "VARCHAR"),
        ("year", "BIGINT"),
        ("month", "BIGINT"),
        ("document_type", "VARCHAR"),
        ("document_count", "BIGINT"),
    ],
    # Ingested from GovInfo CFR (build_cfr_sections); section metadata + citations
    # only (not full text). All columns are stored as VARCHAR.
    "cfr_sections": [
        ("granule_id", "VARCHAR"),
        ("package_id", "VARCHAR"),
        ("cfr_ref", "VARCHAR"),
        ("title", "VARCHAR"),
        ("part", "VARCHAR"),
        ("section", "VARCHAR"),
        ("heading", "VARCHAR"),
        ("structure_level", "VARCHAR"),
        ("edition_year", "VARCHAR"),
        ("last_modified", "VARCHAR"),
        ("url", "VARCHAR"),
    ],
    # Ingested from the Congress.gov v3 API (build_congress_bills); list-level
    # fields only, all stored as VARCHAR.
    "congress_bills": [
        ("bill_id", "VARCHAR"),
        ("congress", "VARCHAR"),
        ("bill_type", "VARCHAR"),
        ("bill_number", "VARCHAR"),
        ("title", "VARCHAR"),
        ("origin_chamber", "VARCHAR"),
        ("latest_action_date", "VARCHAR"),
        ("latest_action_text", "VARCHAR"),
        ("update_date", "VARCHAR"),
        ("url", "VARCHAR"),
    ],
    # Built by build_rulemaking_lifecycles from documents.parquet. Two row shapes
    # discriminated by `kind`; bounded to proposed_date >= 2010-01-01.
    "rulemaking_lifecycles": [
        ("kind", "VARCHAR"),
        ("docket_id", "VARCHAR"),
        ("agency_code", "VARCHAR"),
        ("title", "VARCHAR"),
        ("proposed_date", "DATE"),
        ("final_date", "DATE"),
        ("days", "BIGINT"),
    ],
    # Built by build_fr_docket_links: federal_register.docket_ids_json exploded to
    # one row per (docket_id, FR document), carrying FR display columns.
    "fr_docket_links": [
        ("docket_id", "VARCHAR"),
        ("document_number", "VARCHAR"),
        ("title", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("document_type", "VARCHAR"),
        ("subtype", "VARCHAR"),
        ("publication_date", "VARCHAR"),
        ("effective_on", "VARCHAR"),
        ("comments_close_on", "VARCHAR"),
        ("signing_date", "VARCHAR"),
        ("agency_slugs", "VARCHAR"),
        ("docket_ids_json", "VARCHAR"),
        ("regulation_id_numbers_json", "VARCHAR"),
        ("html_url", "VARCHAR"),
        ("pdf_url", "VARCHAR"),
        ("executive_order_number", "VARCHAR"),
    ],
    # Built by build_discovery_signals from documents.parquet; CURRENT_DATE-relative,
    # so rows change on every rebuild. One row per spiking agency.
    "discovery_signals": [
        ("agency_code", "VARCHAR"),
        ("recent_30d", "BIGINT"),
        ("baseline", "DOUBLE"),
        ("ratio", "DOUBLE"),
    ],
    # Ingested from reginfo.gov (build_unified_agenda); all columns are stored as
    # VARCHAR, array fields serialized as JSON strings. Keyed by (rin, agenda_edition).
    "unified_agenda": [
        ("rin", "VARCHAR"),
        ("agency_code", "VARCHAR"),
        ("agency_name", "VARCHAR"),
        ("title", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("rin_status", "VARCHAR"),
        ("rule_stage", "VARCHAR"),
        ("priority_category", "VARCHAR"),
        ("agenda_edition", "VARCHAR"),
        ("major", "VARCHAR"),
        ("publication_id", "VARCHAR"),
        ("timetable_json", "VARCHAR"),
        ("cfr_references_json", "VARCHAR"),
        ("legal_authority_json", "VARCHAR"),
        ("first_action_date", "VARCHAR"),
        ("next_action_date", "VARCHAR"),
        ("url", "VARCHAR"),
    ],
    # Ingested from federalregister.gov (build_federal_register); all columns are
    # stored as VARCHAR, array fields serialized as JSON strings.
    "federal_register": [
        ("document_number", "VARCHAR"),
        ("title", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("document_type", "VARCHAR"),
        ("publication_date", "VARCHAR"),
        ("effective_on", "VARCHAR"),
        ("comments_close_on", "VARCHAR"),
        ("signing_date", "VARCHAR"),
        ("agencies_json", "VARCHAR"),
        ("agency_slugs", "VARCHAR"),
        ("docket_ids_json", "VARCHAR"),
        ("regulation_id_numbers_json", "VARCHAR"),
        ("cfr_references_json", "VARCHAR"),
        ("html_url", "VARCHAR"),
        ("pdf_url", "VARCHAR"),
        ("body_html_url", "VARCHAR"),
        ("volume", "VARCHAR"),
        ("start_page", "VARCHAR"),
        ("end_page", "VARCHAR"),
        ("subtype", "VARCHAR"),
        ("executive_order_number", "VARCHAR"),
        ("modify_date", "VARCHAR"),
        ("rin", "VARCHAR"),
    ],
    # Ingested from the SAM.gov Entity API v4 (build_sam_entities); list-level
    # fields only, all stored as VARCHAR. Keyed by uei (Unique Entity ID).
    "sam_entities": [
        ("uei", "VARCHAR"),
        ("cage_code", "VARCHAR"),
        ("legal_business_name", "VARCHAR"),
        ("dba_name", "VARCHAR"),
        ("entity_structure_desc", "VARCHAR"),
        ("entity_type_desc", "VARCHAR"),
        ("profit_structure_desc", "VARCHAR"),
        ("state", "VARCHAR"),
        ("city", "VARCHAR"),
        ("zip_code", "VARCHAR"),
        ("congressional_district", "VARCHAR"),
        ("primary_naics", "VARCHAR"),
        ("registration_status", "VARCHAR"),
        ("registration_date", "VARCHAR"),
        ("registration_expiration_date", "VARCHAR"),
        ("exclusion_status_flag", "VARCHAR"),
        ("purpose_of_registration_desc", "VARCHAR"),
        ("entity_url", "VARCHAR"),
    ],
    # Ingested from the Senate LDA REST API (build_lobbying_filings); all columns
    # are stored as VARCHAR, nested/array fields serialized as JSON strings.
    # Keyed by filing_uuid.
    "lobbying_filings": [
        ("filing_uuid", "VARCHAR"),
        ("filing_type", "VARCHAR"),
        ("filing_year", "VARCHAR"),
        ("filing_period", "VARCHAR"),
        ("dt_posted", "VARCHAR"),
        ("registrant_name", "VARCHAR"),
        ("registrant_id", "VARCHAR"),
        ("client_name", "VARCHAR"),
        ("client_id", "VARCHAR"),
        ("income", "VARCHAR"),
        ("expenses", "VARCHAR"),
        ("lobbying_activities_json", "VARCHAR"),
        ("government_entities_json", "VARCHAR"),
        ("url", "VARCHAR"),
    ],
    # Ingested from the OpenFEC /committees endpoint (build_fec_committees); a
    # committee/PAC reference dimension, all columns stored as VARCHAR with array
    # fields serialized as JSON strings. Keyed by committee_id.
    "fec_committees": [
        ("committee_id", "VARCHAR"),
        ("name", "VARCHAR"),
        ("committee_type", "VARCHAR"),
        ("committee_type_full", "VARCHAR"),
        ("designation", "VARCHAR"),
        ("designation_full", "VARCHAR"),
        ("party", "VARCHAR"),
        ("party_full", "VARCHAR"),
        ("state", "VARCHAR"),
        ("treasurer_name", "VARCHAR"),
        ("organization_type_full", "VARCHAR"),
        ("filing_frequency", "VARCHAR"),
        ("first_file_date", "VARCHAR"),
        ("last_file_date", "VARCHAR"),
        ("cycles_json", "VARCHAR"),
        ("candidate_ids_json", "VARCHAR"),
    ],
    # Derived by build_org_committee_links: commenter organization names from
    # comments.organization name-matched to fec_committees. One row per
    # (organization, committee_id); counts are BIGINT, everything else VARCHAR.
    "org_committee_links": [
        ("organization", "VARCHAR"),
        ("organization_norm", "VARCHAR"),
        ("organization_core", "VARCHAR"),
        ("name_source", "VARCHAR"),
        ("committee_id", "VARCHAR"),
        ("committee_name", "VARCHAR"),
        ("committee_type_full", "VARCHAR"),
        ("designation_full", "VARCHAR"),
        ("party_full", "VARCHAR"),
        ("organization_type_full", "VARCHAR"),
        ("committee_state", "VARCHAR"),
        ("match_method", "VARCHAR"),
        ("confidence", "VARCHAR"),
        ("committee_match_count", "BIGINT"),
        ("comment_count", "BIGINT"),
        ("docket_count", "BIGINT"),
        ("agency_codes_json", "VARCHAR"),
        ("first_comment_date", "VARCHAR"),
        ("last_comment_date", "VARCHAR"),
    ],
    # Ingested from the GAO reports RSS feed (build_gao_reports); an append-only
    # accumulator of recently published products. All columns are stored as
    # VARCHAR. Keyed by report_id.
    "gao_reports": [
        ("report_id", "VARCHAR"),
        ("title", "VARCHAR"),
        ("report_type", "VARCHAR"),
        ("published_date", "VARCHAR"),
        ("abstract", "VARCHAR"),
        ("agencies_json", "VARCHAR"),
        ("topics_json", "VARCHAR"),
        ("url", "VARCHAR"),
    ],
    # Ingested from the Congress.gov v3 API (build_crs_reports); list-level
    # fields only, all stored as VARCHAR. Keyed by report_id.
    "crs_reports": [
        ("report_id", "VARCHAR"),
        ("title", "VARCHAR"),
        ("report_type", "VARCHAR"),
        ("status", "VARCHAR"),
        ("published_date", "VARCHAR"),
        ("update_date", "VARCHAR"),
        ("version", "VARCHAR"),
        ("url", "VARCHAR"),
    ],
    # Ingested from the CourtListener v4 search API (build_courtlistener); APA /
    # agency-review litigation dockets (nature-of-suit 899). All columns stored as
    # VARCHAR, array fields serialized as JSON strings. Keyed by cl_docket_id.
    "court_dockets": [
        ("cl_docket_id", "VARCHAR"),
        ("case_name", "VARCHAR"),
        ("case_name_full", "VARCHAR"),
        ("court_id", "VARCHAR"),
        ("court", "VARCHAR"),
        ("court_citation_string", "VARCHAR"),
        ("docket_number", "VARCHAR"),
        ("date_filed", "VARCHAR"),
        ("date_terminated", "VARCHAR"),
        ("date_argued", "VARCHAR"),
        ("nature_of_suit", "VARCHAR"),
        ("cause", "VARCHAR"),
        ("jurisdiction_type", "VARCHAR"),
        ("jury_demand", "VARCHAR"),
        ("assigned_to", "VARCHAR"),
        ("referred_to", "VARCHAR"),
        ("parties_json", "VARCHAR"),
        ("attorneys_json", "VARCHAR"),
        ("firms_json", "VARCHAR"),
        ("pacer_case_id", "VARCHAR"),
        ("date_created", "VARCHAR"),
        ("absolute_url", "VARCHAR"),
    ],
    # Ingested from the USASpending.gov /api/v2/recipient/ endpoint
    # (build_usaspending_recipients); a federal-award recipient reference
    # dimension bounded to the top-N recipients by all-time award amount, all
    # columns stored as VARCHAR. Keyed by recipient_id.
    "usaspending_recipients": [
        ("recipient_id", "VARCHAR"),
        ("uei", "VARCHAR"),
        ("duns", "VARCHAR"),
        ("name", "VARCHAR"),
        ("recipient_level", "VARCHAR"),
        ("total_award_amount", "VARCHAR"),
    ],
    # Ingested from the FCC ECFS public API (build_fcc_proceedings); the FCC's
    # docket equivalent — the FCC does not participate in regulations.gov. All
    # columns stored as VARCHAR. Keyed by name (the docket number).
    "fcc_proceedings": [
        ("name", "VARCHAR"),
        ("id_proceeding", "VARCHAR"),
        ("description", "VARCHAR"),
        ("bureau_code", "VARCHAR"),
        ("bureau_name", "VARCHAR"),
        ("rulemaking_or_docket", "VARCHAR"),
        ("filing_status", "VARCHAR"),
        ("date_created", "VARCHAR"),
        ("date_closed", "VARCHAR"),
        ("comment_start_date", "VARCHAR"),
        ("comment_end_date", "VARCHAR"),
        ("reply_comment_start_date", "VARCHAR"),
        ("reply_comment_end_date", "VARCHAR"),
        ("filed_by", "VARCHAR"),
    ],
    # Ingested from the FCC ECFS public API (build_fcc_filings); the FCC's
    # comment equivalent. All columns stored as VARCHAR, array fields
    # serialized as JSON strings. Keyed by id_submission.
    "fcc_filings": [
        ("id_submission", "VARCHAR"),
        ("proceeding_names_json", "VARCHAR"),
        ("submission_type", "VARCHAR"),
        ("express_comment", "VARCHAR"),
        ("date_received", "VARCHAR"),
        ("date_submission", "VARCHAR"),
        ("date_disseminated", "VARCHAR"),
        ("filing_status", "VARCHAR"),
        ("viewing_status", "VARCHAR"),
        ("exparte_or_late_filed", "VARCHAR"),
        ("filers_json", "VARCHAR"),
        ("authors_json", "VARCHAR"),
        ("lawfirms_json", "VARCHAR"),
        ("bureaus_json", "VARCHAR"),
        ("text_data", "VARCHAR"),
        ("total_page_count", "VARCHAR"),
        ("documents_json", "VARCHAR"),
        ("filing_url", "VARCHAR"),
    ],
    # The bill-family rollup's own processing state (build_bill_family):
    # one retained GovInfo bulkdata listing entry per BILLSTATUS folder, which
    # the next run compares the live listing against to decide whether the zip
    # needs downloading at all. Listed literally, like every other entry here;
    # test_hosted_rollups pins it equal to the transform's ARCHIVE_COLUMNS, the
    # same way mcp_server.TABLES is pinned to this module's TABLES.
    "bill_family_archives": [
        ("name", "VARCHAR"),
        ("link", "VARCHAR"),
        ("formatted_last_modified_time", "VARCHAR"),
        ("modified_at", "VARCHAR"),
        ("size", "VARCHAR"),
        ("congress", "VARCHAR"),
        ("bill_type", "VARCHAR"),
        ("observed_at", "VARCHAR"),
    ],
    # The bill family's second non-contract output (build_bill_family): the
    # roll calls each bill's own actions record, which the roll-call rollup
    # reads back as a linkage index. Pinned to the transform's own
    # VOTE_REFERENCE_COLUMNS by test_hosted_rollups, like the entry above.
    "bill_vote_references": [
        ("bill_id", "VARCHAR"),
        ("chamber", "VARCHAR"),
        ("congress", "VARCHAR"),
        ("session", "VARCHAR"),
        ("roll_number", "VARCHAR"),
        ("action_index", "VARCHAR"),
        ("url", "VARCHAR"),
        ("date", "VARCHAR"),
        ("full_action_name", "VARCHAR"),
        ("observed_at", "VARCHAR"),
    ],
    # The bill family's pre-BILLSTATUS backfill state (build_bill_family):
    # per attempted bill, the list stamp it was attempted under and whether
    # it was filled or refused — a filled row whose stamp matches is skipped,
    # a refused row is retried first next run — and, per (congress, bill_type)
    # walked, the route's declared total against what was reached, so a capped
    # or refused walk cannot read as an empty unit. Pinned to the transform's
    # BACKFILL_COLUMNS / BACKFILL_WALK_COLUMNS by test_hosted_rollups, like
    # the two entries above.
    "bill_family_backfills": [
        ("congress", "VARCHAR"),
        ("bill_type", "VARCHAR"),
        ("number", "VARCHAR"),
        ("list_update_date_including_text", "VARCHAR"),
        ("refusal", "VARCHAR"),
        ("observed_at", "VARCHAR"),
    ],
    "bill_family_backfill_walks": [
        ("congress", "VARCHAR"),
        ("bill_type", "VARCHAR"),
        ("declared_count", "VARCHAR"),
        ("records_walked", "VARCHAR"),
        ("pages_walked", "VARCHAR"),
        ("list_completed", "VARCHAR"),
        ("unwalkable_count", "VARCHAR"),
        ("repeated_count", "VARCHAR"),
        ("backfilled_count", "VARCHAR"),
        ("observed_at", "VARCHAR"),
    ],
}

# Polars dtype -> DuckDB type label, so core-table types line up with what a
# DESCRIBE of the published parquet shows.
_POLARS_TYPE_LABELS: list[tuple[object, str]] = [
    (pl.Utf8, "VARCHAR"),
    (pl.Int64, "BIGINT"),
    (pl.Int32, "INTEGER"),
    (pl.Float64, "DOUBLE"),
    (pl.Boolean, "BOOLEAN"),
]


def _polars_type_label(dtype: object) -> str:
    for candidate, label in _POLARS_TYPE_LABELS:
        if dtype == candidate:
            return label
    return str(dtype)


@lru_cache(maxsize=1)
def _contracts() -> dict:
    """``TABLE_CONTRACTS`` from the installed spicy-docs wheel.

    Imported lazily, not at module import, so the base CLI and MCP installs
    keep working without the ``source-readers`` group — ``vendor/README.md``
    states that they do not require it. The dictionary commands do: the column
    tuples and the per-column prose for the hosted tables are the wheel's, and
    generating them from anything else would be this repository restating a
    contract it does not own.
    """
    try:
        from spicy_docs.schemas import TABLE_CONTRACTS
    except ModuleNotFoundError as exc:  # pragma: no cover - install-shape guard
        raise ModuleNotFoundError(
            "The data dictionary reads the hosted tables' columns and prose from "
            "spicy-docs. Install the source-readers group: uv sync --frozen"
        ) from exc
    return dict(TABLE_CONTRACTS)


def contract_schemas() -> dict[str, list[tuple[str, str]]]:
    """``{table: [(column, "VARCHAR"), ...]}`` for every contract-hosted table.

    Every published value in these tables is a string — the host publishes
    all-VARCHAR Parquet read through a DuckDB view — so the type is not a
    per-column decision and is not stored per column anywhere.
    """
    contracts = _contracts()
    return {name: [(column, "VARCHAR") for column in contracts[name].columns] for name in CONTRACT_TABLES}


def contract_column_prose(table: str) -> dict[str, str]:
    """The contract's own sentence per column, for a ``columns_from: spicy_docs`` entry."""
    return dict(_contracts()[table].descriptions)


def contract_grain(table: str) -> str:
    """The contract's one-sentence grain, used as the table summary's first clause."""
    return _contracts()[table].grain


def expected_schemas() -> dict[str, list[tuple[str, str]]]:
    """Return ``{table: [(column, type_label), ...]}`` for all tables (offline)."""
    schemas: dict[str, list[tuple[str, str]]] = {}
    from_contracts = contract_schemas()
    for name in TABLES:
        if name in from_contracts:
            # Checked before DERIVED_SCHEMAS on purpose: congress_bills is in
            # both, and the contract is the longer, current one.
            schemas[name] = list(from_contracts[name])
        elif name in RECORD_TYPES:
            rt = RECORD_TYPES[name]
            schemas[name] = [(col, _polars_type_label(dt)) for col, dt in rt.schema.items()]
        elif name in DERIVED_SCHEMAS:
            schemas[name] = list(DERIVED_SCHEMAS[name])
        else:  # pragma: no cover - guards against TABLES/registry drift
            raise KeyError(f"No schema known for table {name!r}")
    return schemas


# --------------------------------------------------------------------------- #
# Live schema discovery (DuckDB DESCRIBE over R2 or a local parquet directory).
# --------------------------------------------------------------------------- #
def discover_schemas(source: str, base: str | None = None) -> dict[str, list[tuple[str, str]]]:
    """Discover schemas from published parquet via DuckDB ``DESCRIBE``.

    ``source`` is ``"r2"`` (remote https bucket; needs the httpfs extension) or
    ``"local"`` (a directory of ``<table>.parquet`` files). Mirrors the
    connection recipe in :mod:`spicy_regs.mcp_server`.
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET home_directory='{tempfile.gettempdir()}'")
    if source == "r2":
        base_url = (base or DEFAULT_R2_BASE_URL).rstrip("/")
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")

        def url_for(name: str) -> str:
            return f"{base_url}/{name}.parquet"

    elif source == "local":
        base_dir = Path(base or "./spicy-regs-data")

        def url_for(name: str) -> str:
            return str(base_dir / f"{name}.parquet")

    else:
        raise ValueError(f"Unknown source {source!r}; expected 'r2' or 'local'")

    schemas: dict[str, list[tuple[str, str]]] = {}
    for name in TABLES:
        target = url_for(name).replace("'", "''")
        rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{target}')").fetchall()
        schemas[name] = [(row[0], row[1]) for row in rows]
    con.close()
    return schemas


# --------------------------------------------------------------------------- #
# Descriptions file.
# --------------------------------------------------------------------------- #
#: A descriptions.yaml entry naming this source reads its per-column prose from
#: the installed spicy-docs contract instead of listing it inline.
COLUMNS_FROM_SPICY_DOCS = "spicy_docs"


def load_descriptions(path: Path = DEFAULT_DESCRIPTIONS) -> dict:
    """The curated descriptions, with every ``columns_from`` marker resolved.

    ``columns_from: spicy_docs`` means "the per-column sentences for this table
    are the contract's". All four hundred and seven of them are, and copying
    them here would create a second copy to keep in step with no mechanism
    keeping it there. The label, coverage statement, ``measured_on`` and summary stay
    hand-written, because what this repository publishes and when it last
    measured it are its own facts, not the wheel's.

    Resolving here rather than at each call site means ``check``, ``generate``
    and ``catalog`` all see the same entry, and none of them has to know the
    marker exists.
    """
    import yaml

    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    tables = data.get("tables", {})
    for name, entry in tables.items():
        if not entry or entry.get("columns_from") != COLUMNS_FROM_SPICY_DOCS:
            continue
        inline = entry.get("columns") or {}
        if inline:
            raise ValueError(f"[{name}] declares columns_from: {COLUMNS_FROM_SPICY_DOCS} and also lists columns inline")
        entry["columns"] = contract_column_prose(name)
    return tables


# --------------------------------------------------------------------------- #
# Reconciliation.
# --------------------------------------------------------------------------- #
def _reconcile_columns(
    table: str,
    left_label: str,
    left_cols: list[str],
    right_label: str,
    right_cols: list[str],
) -> list[str]:
    """Return human-readable drift errors comparing two column-name lists."""
    errors: list[str] = []
    left, right = set(left_cols), set(right_cols)
    for col in sorted(left - right):
        errors.append(f"[{table}] column {col!r} in {left_label} but missing from {right_label}")
    for col in sorted(right - left):
        errors.append(f"[{table}] column {col!r} in {right_label} but missing from {left_label}")
    return errors


def check_descriptions(
    schemas: dict[str, list[tuple[str, str]]],
    descriptions: dict,
) -> list[str]:
    """Reconcile a schema map against the curated descriptions. Returns errors."""
    errors: list[str] = []
    schema_tables = set(schemas)
    desc_tables = set(descriptions)
    for table in sorted(schema_tables - desc_tables):
        errors.append(f"[{table}] table has a schema but no entry in descriptions.yaml")
    for table in sorted(desc_tables - schema_tables):
        errors.append(f"[{table}] described in descriptions.yaml but is not a known table")

    for table in sorted(schema_tables & desc_tables):
        schema_cols = [c for c, _ in schemas[table]]
        entry = descriptions[table] or {}
        if not (entry.get("summary") or "").strip():
            errors.append(f"[{table}] missing a 'summary' in descriptions.yaml")
        if not (entry.get("label") or "").strip():
            errors.append(f"[{table}] missing a 'label' in descriptions.yaml")
        if not (entry.get("coverage") or "").strip():
            errors.append(f"[{table}] missing a 'coverage' statement in descriptions.yaml")
        measured_on = str(entry.get("measured_on") or "").strip()
        if not measured_on:
            errors.append(f"[{table}] missing 'measured_on' beside its coverage statement")
        else:
            try:
                date.fromisoformat(measured_on)
            except ValueError:
                errors.append(f"[{table}] 'measured_on' is not an ISO date: {measured_on!r}")
        if "data_quality" in entry and not (entry.get("data_quality") or "").strip():
            errors.append(f"[{table}] has an empty 'data_quality' note in descriptions.yaml")
        desc_cols = list((entry.get("columns") or {}).keys())
        errors.extend(_reconcile_columns(table, "schema", schema_cols, "descriptions.yaml", desc_cols))
        for col in schema_cols:
            text = (entry.get("columns") or {}).get(col)
            if col in desc_cols and not (text or "").strip():
                errors.append(f"[{table}.{col}] has an empty description")
    return errors


def check_schema_drift(
    expected: dict[str, list[tuple[str, str]]],
    live: dict[str, list[tuple[str, str]]],
) -> list[str]:
    """Reconcile the in-code expected schema against a live (parquet) schema."""
    errors: list[str] = []
    for table in TABLES:
        exp_cols = [c for c, _ in expected.get(table, [])]
        live_cols = [c for c, _ in live.get(table, [])]
        errors.extend(_reconcile_columns(table, "in-code schema", exp_cols, "live parquet", live_cols))
    return errors


# --------------------------------------------------------------------------- #
# Markdown generation.
# --------------------------------------------------------------------------- #
_GENERATED_BANNER = (
    "<!-- Generated by `spicy-regs-dict generate`. Do not edit by hand. "
    "Edit data_dictionary/descriptions.yaml or the schema, then regenerate. -->"
)


def table_labels(descriptions: dict) -> dict[str, str]:
    """Return ``{table: short display label}``.

    The label names the collection for someone deciding whether it holds court
    cases, bills or comments. It lives here, beside the summary the table pages
    already render, so a catalog and a docs page cannot name the same class two
    different ways.
    """
    return {table: (entry or {}).get("label", "") for table, entry in descriptions.items()}


def table_coverage(descriptions: dict) -> dict[str, dict[str, str]]:
    """Return ``{table: {label, coverage, data_quality, summary}}`` for a catalog.

    ``coverage`` is a written statement, not a computed min/max, because a
    computed range lies for three of these classes in three different ways: a
    fifty-three-day ingest window reads as a coverage claim about the
    publisher's archive, a rotating-window ingest reads as even density it does
    not have, and two tables carry publisher dates in the year 0000. Each
    statement opens by naming which kind it is.

    This describes what spicy-regs *publishes*. Whether a class is in a search
    index is that consumer's fact, not ours, and must not be inferred from
    anything here — ``MCP_QUERYABLE`` in particular is every published table,
    so reading it as "searchable" would advertise classes no index holds.
    """
    return {
        table: {
            "label": (entry or {}).get("label", ""),
            "coverage": (entry or {}).get("coverage", ""),
            "measured_on": str((entry or {}).get("measured_on", "")),
            "data_quality": (entry or {}).get("data_quality", ""),
            "summary": (entry or {}).get("summary", ""),
        }
        for table, entry in descriptions.items()
    }


def _render_table_page(
    table: str,
    columns: list[tuple[str, str]],
    entry: dict,
) -> str:
    summary = (entry.get("summary") or "").strip()
    col_desc = entry.get("columns") or {}
    pk = None
    rt = RECORD_TYPES.get(table)
    if rt is not None:
        pk = rt.dedup_key

    label = (entry.get("label") or "").strip()
    lines = [_GENERATED_BANNER, "", f"# `{table}`", ""]
    if label:
        lines += [f"**{label}**", ""]
    if summary:
        lines += [summary, ""]
    coverage = (entry.get("coverage") or "").strip()
    if coverage:
        measured_on = str(entry.get("measured_on") or "").strip()
        stamp = f" *(measured {measured_on})*" if measured_on else ""
        lines += [f"**Coverage.** {coverage}{stamp}", ""]
    data_quality = (entry.get("data_quality") or "").strip()
    if data_quality:
        lines += [f"**Data quality.** {data_quality}", ""]
    queryable = "Yes" if table in MCP_QUERYABLE else "No (published to R2 only)"
    lines += [
        f"- **Parquet file:** `{table}.parquet`",
        f"- **Queryable via MCP `query_sql`:** {queryable}",
    ]
    if pk:
        lines.append(f"- **Primary / dedup key:** `{pk}`")
    lines += ["", "| Column | Type | Description |", "| --- | --- | --- |"]
    for col, col_type in columns:
        desc = (col_desc.get(col) or "").replace("|", "\\|").strip()
        marker = " 🔑" if col == pk else ""
        lines.append(f"| `{col}`{marker} | `{col_type}` | {desc} |")
    lines.append("")
    return "\n".join(lines)


def generate(
    descriptions: dict,
    schemas: dict[str, list[tuple[str, str]]],
    out_dir: Path = DEFAULT_DOCS_TABLES_DIR,
) -> list[Path]:
    """Render one Markdown page per table. Returns the written paths."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for table in TABLES:
        entry = descriptions.get(table, {}) or {}
        page = _render_table_page(table, schemas[table], entry)
        path = out_dir / f"{table}.md"
        path.write_text(page, encoding="utf-8")
        written.append(path)
    return written


# --------------------------------------------------------------------------- #
# CLI.
# --------------------------------------------------------------------------- #
def _schemas_for_source(source: str, base: str | None) -> dict[str, list[tuple[str, str]]]:
    if source == "schema":
        return expected_schemas()
    return discover_schemas(source, base)


#: Exit code for "the live source could not be read", distinct from drift.
#: A caller gating on this check needs to tell a transient unreachable bucket
#: from a real disagreement between what we declare and what we publish;
#: collapsing both into 1 makes the check either fail on outages or, with a
#: `|| true`, unable to fail at all.
EXIT_SOURCE_UNREACHABLE = 3


def cmd_check(args: argparse.Namespace) -> int:
    descriptions = load_descriptions(Path(args.descriptions))
    errors: list[str] = []

    if args.source == "schema":
        errors += check_descriptions(expected_schemas(), descriptions)
    else:
        try:
            live = discover_schemas(args.source, args.base)
        except (duckdb.IOException, duckdb.HTTPException, OSError) as exc:
            # Only the "could not read it" failures. A malformed parquet or a
            # bad query is a real problem and must not be reported as an outage.
            print(f"! Could not read the live {args.source} schema: {exc}", file=sys.stderr)
            print("  Drift was NOT checked. This is not a pass.", file=sys.stderr)
            return EXIT_SOURCE_UNREACHABLE
        # Descriptions must cover the live schema, and the in-code registry the
        # docs are generated from must match the live schema too.
        errors += check_descriptions(live, descriptions)
        errors += check_schema_drift(expected_schemas(), live)

    if errors:
        print(f"✗ Data dictionary check failed ({len(errors)} issue(s)):", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "\nUpdate data_dictionary/descriptions.yaml (and DERIVED_SCHEMAS / RECORD_TYPES "
            "if the schema changed) so they line up.",
            file=sys.stderr,
        )
        return 1
    print(f"✓ Data dictionary check passed ({len(descriptions)} tables, source={args.source}).")
    return 0


def build_catalog(descriptions: dict, schemas: dict[str, list[tuple[str, str]]]) -> dict:
    """Return the catalog document: one entry per published class, in TABLES order.

    This is the declaration side of a readable catalog, and it exists as a file
    because the consumer cannot import this package. It says what spicy-regs
    publishes, in words a person can read without knowing an identifier.

    It deliberately does **not** say whether a class is searchable. That is the
    serving side's fact, and a reader must derive it by aggregating over its own
    index rather than trusting this document — a catalog that certifies its own
    coverage is not a check. ``MCP_QUERYABLE`` is every published table, so it
    is not that fact either and is not exported here.

    ``measured_on`` is when the coverage statement was last checked against the
    published data. It does not update itself and nothing here claims it is
    current; it exists so a stale statement is *detectable* rather than
    indistinguishable from a fresh one.

    ``kind`` is the coverage statement's kind as a token. The prose still opens
    by naming it for a person, but a consumer must not have to parse a sentence
    to branch on it: the opening word carries a trailing period or comma
    depending on the phrasing, so a whitespace split fails on seven of the
    twenty-four classes.
    """
    coverage = table_coverage(descriptions)
    return {
        "format_version": CATALOG_FORMAT_VERSION,
        "declares": "what spicy-regs publishes; not what any index serves",
        "classes": [
            {
                "table": table,
                "label": coverage[table]["label"],
                "summary": coverage[table]["summary"],
                "coverage": coverage[table]["coverage"],
                "kind": coverage_kind(coverage[table]["coverage"]),
                "measured_on": coverage[table]["measured_on"],
                "data_quality": coverage[table]["data_quality"] or None,
                "columns": [name for name, _ in schemas[table]],
            }
            for table in TABLES
        ],
    }


def catalog_bytes(document: dict) -> bytes:
    """Serialize the catalog to its one canonical byte form.

    A vendored contract is pinned by digest, so there must be exactly one byte
    string for a given document. Key order is insertion order, which follows
    ``TABLES``; indent and separators are fixed here rather than at the call
    site so the digest cannot move because someone passed a different flag.
    """
    return json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"


def cmd_catalog(args: argparse.Namespace) -> int:
    descriptions = load_descriptions(Path(args.descriptions))
    schemas = _schemas_for_source(args.source, args.base)
    errors = check_descriptions(schemas, descriptions)
    if errors:
        print("✗ Refusing to write a catalog that disagrees with the schema.", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    out = Path(args.out) if args.out else DEFAULT_CATALOG_PATH
    document = build_catalog(descriptions, schemas)
    payload = catalog_bytes(document)
    out.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    digest_path = Path(args.digest) if args.digest else DEFAULT_CATALOG_DIGEST_PATH
    # Sidecar, not a field inside the document: a digest carried by the thing it
    # certifies proves nothing. A consumer verifies the bytes against this.
    digest_path.write_text(f"{digest}  {out.name}\n", encoding="utf-8")
    print(f"✓ Wrote {len(document['classes'])} class declaration(s) to {out}.")
    print(f"  sha256 {digest}")
    print(f"  digest recorded in {digest_path}")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    descriptions = load_descriptions(Path(args.descriptions))
    schemas = _schemas_for_source(args.source, args.base)
    # Fail rather than emit a dictionary that disagrees with itself.
    errors = check_descriptions(schemas, descriptions)
    if errors:
        print("✗ Refusing to generate: descriptions are out of sync with the schema.", file=sys.stderr)
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_DOCS_TABLES_DIR
    written = generate(descriptions, schemas, out_dir)
    print(f"✓ Wrote {len(written)} table page(s) to {out_dir} (source={args.source}).")
    for path in written:
        print(f"  - {path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path}")
    # The catalog is the same derivation from the same two inputs, and two tests
    # require the committed copy to equal a fresh build. Writing it here means a
    # contributor who edits descriptions.yaml and regenerates cannot leave it
    # stale by not knowing a third command exists.
    if out_dir == DEFAULT_DOCS_TABLES_DIR:
        payload = catalog_bytes(build_catalog(descriptions, schemas))
        DEFAULT_CATALOG_PATH.write_bytes(payload)
        DEFAULT_CATALOG_DIGEST_PATH.write_text(
            f"{hashlib.sha256(payload).hexdigest()}  {DEFAULT_CATALOG_PATH.name}\n", encoding="utf-8"
        )
        print(f"  - {DEFAULT_CATALOG_PATH.relative_to(REPO_ROOT)} (+ .sha256)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spicy-regs-dict",
        description="Generate and validate the Spicy Regs data dictionary.",
    )
    parser.add_argument(
        "--descriptions",
        default=str(DEFAULT_DESCRIPTIONS),
        help=f"Path to descriptions.yaml (default: {DEFAULT_DESCRIPTIONS})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_check = sub.add_parser("check", help="Reconcile descriptions and schema; exit non-zero on drift")
    p_check.add_argument(
        "--source",
        choices=["schema", "r2", "local"],
        default="schema",
        help="schema = in-code (offline, default); r2/local = live parquet via DuckDB DESCRIBE",
    )
    p_check.add_argument(
        "--base",
        default=None,
        help="R2 base URL (source=r2) or local parquet dir (source=local)",
    )
    p_check.set_defaults(func=cmd_check)

    p_gen = sub.add_parser("generate", help="Render docs/tables/*.md from the schema + descriptions")
    p_gen.add_argument(
        "--source",
        choices=["schema", "r2", "local"],
        default="schema",
        help="Where to read column names/types from (default: in-code schema, offline)",
    )
    p_gen.add_argument("--base", default=None, help="R2 base URL or local parquet dir")
    p_gen.add_argument("--out-dir", default=None, help=f"Output dir (default: {DEFAULT_DOCS_TABLES_DIR})")
    p_gen.set_defaults(func=cmd_generate)

    p_cat = sub.add_parser("catalog", help="Write the machine-readable class declaration (catalog.json)")
    p_cat.add_argument("--source", choices=["schema", "r2", "local"], default="schema")
    p_cat.add_argument("--base", default=None, help="R2 base URL or local parquet dir")
    p_cat.add_argument("--out", default=None, help=f"Output path (default: {DEFAULT_CATALOG_PATH})")
    p_cat.add_argument("--digest", default=None, help=f"Digest sidecar (default: {DEFAULT_CATALOG_DIGEST_PATH})")
    p_cat.set_defaults(func=cmd_catalog)
    return parser


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
