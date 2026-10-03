#!/usr/bin/env python3
"""Generate and validate the Spicy Regs data dictionary.

The data dictionary has two layers:

* **Schema (source of truth, in code).** Column names and types come from
  :data:`spicy_regs.schemas.regulations.RECORD_TYPES` for the core tables, the
  spicy-docs contracts for the hosted tables, a builder's own column list or
  Arrow schema where it has one, and :data:`DERIVED_SCHEMAS` below for the
  rollups whose SQL is their only declaration. This keeps generation
  deterministic and offline.
* **Descriptions (curated prose).** Human descriptions live in
  ``data_dictionary/descriptions.yaml``, keyed by table and column.

``spicy-regs-dict check`` reconciles the two so they can't silently drift, and
``spicy-regs-dict generate`` renders one Markdown page per table for the MkDocs
site under ``docs/tables/``. ``generate`` also bundles the output ledger's
per-table audits (``output_ledger``) and the declared cross-table joins
(``table_joins``) for the MCP server, and ``check`` refuses a stale copy of either.

Usage::

    uv run spicy-regs-dict check                 # offline: descriptions vs in-code schema
    uv run spicy-regs-dict check --source r2     # also reconcile in-code schema vs live R2 parquet
    uv run spicy-regs-dict generate              # (re)write docs/tables/*.md
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date
from functools import lru_cache
import tempfile
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from spicy_regs import output_ledger, table_joins
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.schemas.regulations import COMMENT_MIRROR_COLUMNS, RECORD_TYPES
from spicy_regs.sources.publication import SNAPSHOT_POINTER, PublicationError, parquet_scan

# Repo layout anchors (this file lives at src/spicy_regs/data_dictionary.py).
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DESCRIPTIONS = REPO_ROOT / "data_dictionary" / "descriptions.yaml"
DEFAULT_DOCS_TABLES_DIR = REPO_ROOT / "docs" / "tables"
DEFAULT_CATALOG_PATH = REPO_ROOT / "data_dictionary" / "catalog.json"
DEFAULT_CATALOG_DIGEST_PATH = REPO_ROOT / "data_dictionary" / "catalog.json.sha256"
DEFAULT_MCP_METADATA_PATH = Path(__file__).with_name("table_metadata.json")

#: Bumped when the catalog document's shape changes, so a reader can refuse a
#: shape it does not know rather than guess at a missing field.
CATALOG_FORMAT_VERSION = 3

#: The six kinds a coverage statement can be, as machine-readable tokens,
#: keyed by the prose prefix so the two cannot disagree.
#:
#: "Sampled" is the weakest population claim: the table is filled from a
#: bounded slice of the publisher's archive chosen by a per-run cap, not by a
#: date window, so neither an end-to-end range nor a window describes it. The
#: bill-family and roll-call tables open this way until a full run has been
#: measured, because calling a capped first pass a "Window" would state a
#: density it does not have. A capped table whose cap has caught up is
#: re-kinded only after the output ledger records the publisher's declared
#: count beside what is held (house_communications, 2026-10-03).
#:
#: "Empty" is a table that is declared and published with no rows, by owner
#: decision or because its producer is not enabled. The 2026-10-02 blind test
#: found six such tables reading "Sampled" or "Not a range", so a count over
#: them read as a thin sample rather than as zero by construction.
COVERAGE_KINDS: dict[str, str] = {
    "True range": "true_range",
    "Window": "window",
    "Sampled": "sampled",
    "Derived": "derived",
    "Not a range": "not_a_range",
    "Empty": "empty",
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


#: What coverage prose may not state, each named by what it is. Coverage prose
#: is pinned to no publication, so a count, snapshot id or pin written there
#: goes stale at the next publish and nothing notices: the 2026-09-28 blind
#: persona test found 13 of 56 stated row counts off by 1.5x to 30x, every
#: rulemaking note naming a replaced snapshot, and 25 notes calling live tables
#: unpublished. The 2026-10-02 test found the same smell one noun over: counts
#: in units the row-count rule did not name (registrations, documents, a
#: per-year rate), and 19 statements restating their own ``measured_on`` inside
#: the sentence, which readers took for the live end bound of a table rebuilt
#: daily. The live row count is describe_table's publication field, and a
#: measurement tied to a pin belongs in the output ledger, which names its pin.
COVERAGE_PROSE_REFUSALS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("a snapshot id", re.compile(r"snapshot_[0-9a-f]{6,}")),
    ("a digest or generation pin", re.compile(r"\b[0-9a-f]{16,}\b|\b[0-9a-f]{8,}…")),
    ("a publication-status claim", re.compile(r"not uploaded|not yet published", re.IGNORECASE)),
    ("a row count", re.compile(r"\d[\d,]*(?:\s+[A-Za-z-]+){0,2}\s+(?:rows|records)\b", re.IGNORECASE)),
    # A thousands-separated number in any other unit. A row count is left to the
    # rule above so it is reported once, and a partial number inside a longer one
    # (the "1,009" of "1,009,005") is not a match.
    ("a count", re.compile(
        r"(?<![\d,])\b\d{1,3}(?:,\d{3})+\b(?!,\d)(?!(?:\s+[A-Za-z-]+){0,2}\s+(?:rows|records)\b)", re.IGNORECASE
    )),
)

#: What these rules still cannot see: a number spelled with a suffix or in words
#: ("26.6M", "~20.7K", "two-thirds"), a share ("87%", "0.08%"), a count under
#: a thousand ("43 laws"), a dollar threshold ("over $1,000" has a comma and is
#: refused; "over $900" is not) and a date other than the entry's own
#: measured_on. Those are read by a person at review, not by this lint.

#: Exact coverage phrases allowed past COVERAGE_PROSE_REFUSALS, by table, each
#: with its reason. An exception whose phrase is no longer in the prose is an
#: error, so a fixed note cannot leave a stale permission behind. A per-run cap
#: or a cited threshold is a code constant quoted as a rule, not a measurement,
#: which is the one shape a count may take here.
COVERAGE_PROSE_EXCEPTIONS: dict[str, dict[str, str]] = {
    "bill_subjects": {"up to 2,000 a run": "the API request cap per run (build_bill_subjects), a rule"},
    "committee_meetings": {"at most 1,000 a run": "MAX_DETAILS_PER_RUN (build_congress_index), a rule"},
    "record_issues": {"up to 1,000 a run": "MAX_DETAILS_PER_RUN (build_congress_index), a rule"},
    "committees": {"Since 2026-09-26": "the day the walk scope changed (committee-fixes-2026-09-26), not a measurement"},
}


def _measurement_date_rule(measured_on: str) -> tuple[str, re.Pattern[str]]:
    """The entry's own ``measured_on`` as a standalone date; inside a receipt name it is a citation."""
    return (
        "the measured_on date it was measured on",
        re.compile(rf"(?<![\w/-]){re.escape(measured_on)}(?![\w/-])"),
    )


def coverage_prose_errors(table: str, coverage: str, measured_on: str = "") -> list[str]:
    """Refuse decaying facts in one table's coverage prose, less its declared exceptions.

    ``measured_on`` is the entry's own measurement date: restated in the
    sentence it reads as the live end bound, so it is refused like a count.
    """
    errors = []
    text = " ".join(coverage.split())
    for phrase in COVERAGE_PROSE_EXCEPTIONS.get(table, {}):
        if phrase not in text:
            errors.append(f"[{table}] coverage exception no longer matches its prose: {phrase!r}")
        text = text.replace(phrase, " ")
    refusals = COVERAGE_PROSE_REFUSALS + ((_measurement_date_rule(measured_on),) if measured_on else ())
    for what, pattern in refusals:
        errors.extend(
            f"[{table}] coverage states {what} ({match.group(0)!r}); a coverage statement names no "
            "measurement: the server states live counts and pins, and a dated measurement belongs in the "
            "output ledger or the data_quality note"
            for match in pattern.finditer(text)
        )
    return errors


#: A data-quality note may state a measurement, but only with what it was
#: measured at: a date, a pin, a snapshot id, a receipt or a generation. A bare
#: count there is the same decaying fact as one in coverage ("33,373 rows carry
#: a posted_date before 1990", on a table rebuilt daily, 2026-10-02).
_DATA_QUALITY_ANCHOR = re.compile(
    r"\b\d{4}-\d{2}-\d{2}\b|\b[0-9a-f]{8}…|snapshot_[0-9a-f]{6,}|\breceipt|\bgeneration", re.IGNORECASE
)
_DATA_QUALITY_RULES = {what: pattern for what, pattern in COVERAGE_PROSE_REFUSALS}


def data_quality_prose_errors(table: str, data_quality: str) -> list[str]:
    """Refuse a publication claim, and a count no date, pin, snapshot, receipt or generation anchors."""
    text = " ".join(data_quality.split())
    errors = [
        f"[{table}] data_quality states a publication-status claim ({match.group(0)!r}); publication is the "
        "server's fact"
        for match in _DATA_QUALITY_RULES["a publication-status claim"].finditer(text)
    ]
    if not _DATA_QUALITY_ANCHOR.search(text):
        errors.extend(
            f"[{table}] data_quality states {what} ({match.group(0)!r}) with no date, pin, snapshot, receipt or "
            "generation it was measured at"
            for what in ("a row count", "a count")
            for match in _DATA_QUALITY_RULES[what].finditer(text)
        )
    return errors


#: A data-quality note that stands in for a fix in the vendored spicy-docs wheel ends "(interim until
#: spicy-docs > <version>)". spicy-regs cannot override one contract column's sentence, so such a note
#: corrects it from the table level, and without an expiry it outlives the release that fixes the sentence
#: (round 4, 2026-10-03: three contract sentences the data contradicted, and the margin-note reader, were
#: fixed only on an unreleased spicy-docs branch). Once the installed wheel is past the stated version the
#: check refuses the note: delete it if the wheel fixed what it covers, or restate the marker if not.
INTERIM_MARKER = re.compile(r"\(interim until spicy-docs > ([^\s()]+)\)")
_INTERIM_WORDS = re.compile(r"\binterim until\b", re.IGNORECASE)


def installed_spicy_docs() -> str:
    """The spicy-docs version this environment installed, which ``uv run --frozen`` takes from the lock."""
    from importlib.metadata import version

    return version("spicy-docs")


def _wheel_past(installed: str, stated: str) -> bool:
    """Whether ``installed`` is a later spicy-docs than ``stated``.

    A local label names a branch build, not an order (``+laws.…`` and ``+billslane.…`` do not compare), so
    another local build of the same release counts as later, and a plain release does not.
    """
    from packaging.version import Version

    have, want = Version(installed), Version(stated)
    if have.public != want.public:
        return Version(have.public) > Version(want.public)
    return have.local is not None and have.local != want.local


def interim_note_errors(table: str, data_quality: str, installed: str) -> list[str]:
    """Refuse an interim note the installed wheel has moved past, and one whose expiry cannot be read."""
    from packaging.version import InvalidVersion

    text = " ".join(data_quality.split())
    stated = INTERIM_MARKER.findall(text)
    errors = []
    if len(stated) != len(_INTERIM_WORDS.findall(text)):
        errors.append(f"[{table}] data_quality has an interim note whose expiry does not read "
                      "'(interim until spicy-docs > <version>)'")
    for version in stated:
        try:
            past = _wheel_past(installed, version)
        except InvalidVersion:
            errors.append(f"[{table}] data_quality interim note names no valid spicy-docs version ({version!r})")
            continue
        if past:
            errors.append(f"[{table}] data_quality interim note expired: spicy-docs {installed} is installed, past "
                          f"{version}; delete the note if the wheel fixed it, or restate its marker")
    return errors


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
    "bill_cosponsors",
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
    "member_party_affiliations",
    "committee_reports",
    "report_sections",
    "hearing_transcripts",
    "hearing_bill_links",
    "cbo_cost_estimates",
    # A8/A9 (laws and rosters): the laws and committee-rosters rollups.
    "laws",
    "law_sections",
    "law_code_sections",
    "table3_records",
    "committees",
    "committee_assignments",
    # The Federal Register rollup (spicy-docs 0.42.0).
    "federal_register",
    # The FEC bulk committee master, every cycle (spicy-docs 0.43.0, decision 53).
    "fec_committee_history",
    # The FEC bulk candidate master, every cycle.
    "fec_candidate_history",
    # The Regulations.gov attributes the thin documents/dockets/comments lack, typed (decisions 65-67); the
    # regulations ETL writes them and the refresh's base families publish them. comment_attributes is seeded by
    # the comment re-read (fill-comment-fields attributes).
    "document_attributes",
    "docket_attributes",
    "comment_attributes",
    # The Congress.gov index tables (gaps A5, A7, A10), each written by its own
    # rollup in pipelines/rollups/congress_index.py.
    "house_communications",
    "committee_meetings",
    "record_issues",
    "treaties",
    "nominations",
    # The two PDF-only families (spicy-docs 0.23.0). ``document_citations`` is
    # the shared link table both GovInfo families write into, with one owner:
    # the print-citations rollup.
    "house_activity_reports",
    "budget_volumes",
    "bill_committee_actions",
    "document_citations",
    "senate_expenditures",
    # The native legal-reference tables (spicy-docs 0.52.0), their columns, identities and reading the wheel's.
    "native_legal_references",
    "native_legal_reference_reads",
    # GAO's open-recommendations export, accumulated daily (spicy-docs 0.53.0).
    "gao_recommendations",
)


#: The materialized rulemaking dataset's public tables, in its stage order. They
#: publish under their own snapshot pointer (``publication.SNAPSHOT_POINTER``),
#: not the publication index. Listed literally; test_generation_mcp pins them to
#: the pipeline's ``published_outputs``.
RULEMAKING_TABLES: tuple[str, ...] = (
    "rule_targets",
    "proceedings",
    "regulatory_agenda_items",
    "agenda_item_proceedings",
    "comment_periods",
    "rulemaking_lifecycles",
    "lifecycle_events",
    "agency_lifecycle_stats",
)

# These are supported retained observation tables, not a publication claim.
# Exact Arrow union bytes and DuckDB types are qualified together in the checked
# schema resource; the MCP consumes generated table_metadata.json only.
FEC_TYPED_TABLES: tuple[str, ...] = (
    "fec_account_transfers",
    "fec_agency_mapping_dispositions",
    "fec_agency_report_documents",
    "fec_agency_report_text",
    "fec_agency_reports",
    "fec_allocated_disbursements",
    "fec_allocation_bases",
    "fec_api_response_controls",
    "fec_audit_findings",
    "fec_bundled_contributions",
    "fec_candidate_api_observations",
    "fec_collection_selection",
    "fec_committee_master_observations",
    "fec_committee_observations",
    "fec_communication_costs",
    "fec_contribution_aggregates",
    "fec_coordinated_party_expenditures",
    "fec_debts",
    "fec_disbursements",
    "fec_electioneering_communications",
    "fec_filing_definition_evidence",
    "fec_filing_definitions",
    "fec_filing_header_associations",
    "fec_filing_links",
    "fec_filing_report_observations",
    "fec_filing_text_observations",
    "fec_filings",
    "fec_historical_ie_statistics",
    "fec_inaugural_donations",
    "fec_independent_expenditures",
    "fec_intercommittee_transactions",
    "fec_legal_documents",
    "fec_legal_events",
    "fec_legal_matters",
    "fec_legal_parties",
    "fec_loan_guarantors",
    "fec_loan_terms",
    "fec_loans",
    "fec_lobbyist_registrations",
    "fec_oversight_recommendations",
    "fec_postgres_committee_history_observations",
    "fec_quality_notices",
    "fec_receipts",
    "fec_record_evidence",
    "fec_registration_statements",
    "fec_report_metrics",
    "fec_reported_financial_summaries",
    "fec_research_context_dispositions",
    "fec_research_document_observations",
    "fec_research_filing_feed_items",
    "fec_research_meeting_observations",
    "fec_research_response_outcomes",
    "fec_research_source_pages",
    "fec_retained_csv_observations",
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
    "fr_docket_links",
    "discovery_signals",
    "cfr_sections",
    "congress_bills",
    "bill_subjects",
    "unified_agenda",
    *RULEMAKING_TABLES,
    "sam_entities",
    "lobbying_filings",
    "lobbying_activities",
    "lobbying_activity_lobbyists",
    "fec_committees",
    "fec_source_catalog",
    "fec_collections",
    "fec_source_records",
    "fec_relationships",
    *FEC_TYPED_TABLES,
    "org_committee_links",
    "gao_reports",
    "gao_decisions",
    "crs_reports",
    "court_dockets",
    "court_docket_groups",
    "court_opinion_clusters",
    "court_citations",
    "court_citation_map",
    "court_parentheticals",
    "court_opinions",
    "court_opinion_pdf_extractions",
    "member_vote_terms",
    "usaspending_recipients",
    "fcc_proceedings",
    "fcc_filings",
    # The hosted tables, minus congress_bills — it predates them and keeps its
    # position above, so appending CONTRACT_TABLES wholesale would list it twice.
    *(name for name in CONTRACT_TABLES if name != "congress_bills"),
    # Published, but not contract tables: the bill-family rollup's own
    # processing state, and the vote references the roll-call rollup joins
    # against. Their columns are this repository's, so expected_schemas()
    # takes them from the transform and their prose is inline in descriptions.yaml.
    "bill_family_archives",
    "bill_vote_references",
    "bill_family_backfills",
    "bill_family_backfill_walks",
    "committee_report_reads",
)

# Tables the MCP server (list_sources / describe_table / query_sql) exposes.
# This must equal spicy_regs.mcp_server.TABLES; a test enforces it so the docs'
# "queryable via MCP" flag can't drift from what the server actually serves.
MCP_QUERYABLE: frozenset[str] = frozenset(
    {
        "court_opinion_pdf_extractions",
        "native_legal_references",
        "native_legal_reference_reads",
        "dockets",
        "documents",
        "comments",
        "comments_index",
        "feed_summary",
        "agency_stats",
        "agency_monthly_volume",
        "fr_docket_links",
        "discovery_signals",
        "cfr_sections",
        "congress_bills",
        "bill_subjects",
        "unified_agenda",
        "federal_register",
        "sam_entities",
        "lobbying_filings",
        "lobbying_activities",
        "lobbying_activity_lobbyists",
        "fec_committees",
        "fec_source_catalog",
        "fec_collections",
        "fec_source_records",
        "fec_relationships",
        *FEC_TYPED_TABLES,
        "org_committee_links",
        "gao_reports",
        "gao_decisions",
        "crs_reports",
        "court_dockets",
        "court_docket_groups",
        "court_opinion_clusters",
        "court_citations",
        "court_citation_map",
        "court_parentheticals",
        "court_opinions",
        "usaspending_recipients",
        "fcc_proceedings",
        "fcc_filings",
        "bill_family_archives",
        "bill_vote_references",
        "bill_family_backfills",
        "bill_family_backfill_walks",
        "committee_report_reads",
        "member_vote_terms",
        *RULEMAKING_TABLES,
        *CONTRACT_TABLES,
    }
)

# Schemas for the derived rollups whose SQL is their only declaration: they mirror
# src/spicy_regs/transforms/{build_feed_summary,build_agency_rollups,build_fr_docket_links,
# build_discovery_signals}.py and sources/iceberg.py (_build_comments_index). A table whose
# transform declares its columns is read from that declaration in expected_schemas(), never
# restated here. Types are DuckDB type names, matching what a DESCRIBE of the published
# parquet returns (see `check --source r2`).
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
    # Built by build_fr_docket_links: federal_register.docket_ids_json exploded to
    # one row per (docket_id, document_number, publication_date), carrying FR display columns.
    "fr_docket_links": [
        ("docket_id", "VARCHAR"),
        ("docket_source_ordinal", "BIGINT"),
        ("normalized_docket_candidates_json", "VARCHAR"),
        ("docket_normalization_rule", "VARCHAR"),
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
}

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
    """``{table: [(column, DuckDB type), ...]}`` for every contract-hosted table.

    The type is the contract's (VARCHAR unless it states one, as the Regulations.gov attribute tables do,
    decision 67), spelled as DuckDB describes the written Parquet (``contract_types.DESCRIBED``).
    """
    from spicy_regs.contract_types import described_columns

    contracts = _contracts()
    return {name: described_columns(contracts[name]) for name in CONTRACT_TABLES}


def contract_column_prose(table: str) -> dict[str, str]:
    """The contract's own sentence per column, for a ``columns_from: spicy_docs`` entry."""
    return dict(_contracts()[table].descriptions)


def contract_grain(table: str) -> str:
    """The contract's one-sentence grain, used as the table summary's first clause."""
    return _contracts()[table].grain


def fec_typed_schemas() -> dict[str, list[tuple[str, str]]]:
    """Read the qualified retained union declaration without importing its producers.

    This resource retains exact Arrow schema bytes and their digest beside the
    DuckDB spelling measured from those bytes. The dictionary tests reconcile
    both and check producer schema compatibility. The generating receipt binds
    the complete selected union, including the declared bulk schemas. It makes
    no assertion that any table is currently published or fully populated.
    """
    path = REPO_ROOT / "data_dictionary" / "fec_typed_schemas.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("format_version") != 1 or set(document.get("tables", {})) != set(FEC_TYPED_TABLES):
        raise ValueError("Retained FEC schema declaration differs from the supported table set")
    result = {}
    for table, entry in document["tables"].items():
        columns = entry["columns"]
        if (not columns or any(not isinstance(pair, list) or len(pair) != 2
                               or not all(isinstance(value, str) and value for value in pair) for pair in columns)
                or len({pair[0] for pair in columns}) != len(columns)):
            raise ValueError(f"Invalid retained FEC declared columns: {table}")
        result[table] = [tuple(pair) for pair in columns]
    return result


def expected_schemas() -> dict[str, list[tuple[str, str]]]:
    """Return ``{table: [(column, type_label), ...]}`` for all tables (offline)."""
    schemas: dict[str, list[tuple[str, str]]] = {}
    from_contracts = contract_schemas()
    from_fec = fec_typed_schemas()
    from spicy_regs.contract_types import described_schema
    from spicy_regs.transforms.build_agency_lifecycle_stats import SCHEMA as AGENCY_LIFECYCLE_STATS_SCHEMA
    from spicy_regs.transforms.build_comment_periods import COLUMNS as COMMENT_PERIOD_COLUMNS
    from spicy_regs.transforms.build_lifecycles import EVENT_SCHEMA, LIFECYCLE_SCHEMA
    from spicy_regs.transforms.build_proceedings import COLUMNS as PROCEEDING_COLUMNS
    from spicy_regs.transforms.build_regulatory_agenda import ITEM_COLUMNS, RELATIONSHIP_COLUMNS
    from spicy_regs.transforms.build_rule_targets import COLUMNS as RULE_TARGET_COLUMNS
    from spicy_regs.transforms.build_fec_observations import COLLECTION_COLUMNS, RECORD_COLUMNS
    from spicy_regs.transforms.build_fec_source_catalog import COLUMNS as FEC_CATALOG_COLUMNS
    from spicy_regs.transforms.fec_relationships import COLUMNS as FEC_RELATIONSHIP_COLUMNS
    from spicy_regs.transforms.enrich_bill_subjects import COLUMNS as BILL_SUBJECT_COLUMNS
    from spicy_regs.transforms.build_court_opinion_clusters import COLUMNS as COURT_CLUSTER_COLUMNS
    from spicy_regs.transforms.build_courtlistener import PUBLISHED_COLUMNS as COURT_DOCKET_COLUMNS
    from spicy_regs.transforms.build_court_bulk_tables import CITATION_MAP, CITATIONS, OPINIONS, PARENTHETICALS
    from spicy_regs.transforms.build_court_pdf_extractions import COLUMNS as COURT_PDF_COLUMNS
    from spicy_regs.transforms.build_member_vote_terms import COLUMNS as MEMBER_VOTE_TERM_COLUMNS
    from spicy_regs.transforms.build_sam_entities import COLUMNS as SAM_COLUMNS
    from spicy_regs.transforms.build_lobbying_filings import (
        ACTIVITY_COLUMNS as LOBBYING_ACTIVITY_COLUMNS,
        COLUMNS as LOBBYING_COLUMNS,
        LOBBYIST_COLUMNS as LOBBYING_LOBBYIST_COLUMNS,
    )
    from spicy_regs.transforms.build_cfr_sections import COLUMNS as CFR_SECTION_COLUMNS
    from spicy_regs.transforms.build_unified_agenda import COLUMNS as UNIFIED_AGENDA_COLUMNS
    from spicy_regs.transforms.build_fec_committees import COLUMNS as FEC_COMMITTEE_COLUMNS
    # gao_reports types its three counts; its Arrow schema is the one declaration that says so.
    from spicy_regs.transforms.build_gao_reports import DECISION_COLUMNS as GAO_DECISION_COLUMNS
    from spicy_regs.transforms.build_gao_reports import _SCHEMA as GAO_REPORT_SCHEMA
    from spicy_regs.transforms.build_crs_reports import COLUMNS as CRS_REPORT_COLUMNS
    from spicy_regs.transforms.build_court_docket_groups import SCHEMA as COURT_DOCKET_GROUP_SCHEMA
    from spicy_regs.transforms.build_usaspending_recipients import COLUMNS as USASPENDING_COLUMNS
    from spicy_regs.transforms.build_fcc_ecfs import (
        FILING_COLUMNS as FCC_FILING_COLUMNS,
        PROCEEDING_COLUMNS as FCC_PROCEEDING_COLUMNS,
    )
    from spicy_regs.transforms.build_bill_family import (
        ARCHIVE_COLUMNS,
        BACKFILL_COLUMNS,
        BACKFILL_WALK_COLUMNS,
        VOTE_REFERENCE_COLUMNS,
    )
    from spicy_regs.transforms.committee_report_reads import READ_COLUMNS as COMMITTEE_REPORT_READ_COLUMNS
    from spicy_regs.transforms.build_org_committee_links import COLUMNS as ORG_COMMITTEE_LINK_COLUMNS

    builder_columns = {
        "fec_source_catalog": FEC_CATALOG_COLUMNS,
        "fec_collections": COLLECTION_COLUMNS,
        "fec_source_records": RECORD_COLUMNS,
        "fec_relationships": FEC_RELATIONSHIP_COLUMNS,
        "bill_subjects": BILL_SUBJECT_COLUMNS,
        "court_opinion_clusters": COURT_CLUSTER_COLUMNS,
        "court_dockets": COURT_DOCKET_COLUMNS,
        "court_citations": CITATIONS.columns,
        "court_citation_map": CITATION_MAP.columns,
        "court_parentheticals": PARENTHETICALS.columns,
        "court_opinions": OPINIONS.columns,
        "court_opinion_pdf_extractions": COURT_PDF_COLUMNS,
        "member_vote_terms": MEMBER_VOTE_TERM_COLUMNS,
        "sam_entities": SAM_COLUMNS,
        "lobbying_filings": LOBBYING_COLUMNS,
        "lobbying_activities": LOBBYING_ACTIVITY_COLUMNS,
        "lobbying_activity_lobbyists": LOBBYING_LOBBYIST_COLUMNS,
        "rule_targets": RULE_TARGET_COLUMNS,
        "proceedings": PROCEEDING_COLUMNS,
        "regulatory_agenda_items": ITEM_COLUMNS,
        "agenda_item_proceedings": RELATIONSHIP_COLUMNS,
        "comment_periods": COMMENT_PERIOD_COLUMNS,
        "cfr_sections": CFR_SECTION_COLUMNS,
        "unified_agenda": UNIFIED_AGENDA_COLUMNS,
        "fec_committees": FEC_COMMITTEE_COLUMNS,
        "gao_decisions": GAO_DECISION_COLUMNS,
        "crs_reports": CRS_REPORT_COLUMNS,
        "usaspending_recipients": USASPENDING_COLUMNS,
        "fcc_proceedings": FCC_PROCEEDING_COLUMNS,
        "fcc_filings": FCC_FILING_COLUMNS,
        "bill_family_archives": ARCHIVE_COLUMNS,
        "bill_vote_references": VOTE_REFERENCE_COLUMNS,
        "bill_family_backfills": BACKFILL_COLUMNS,
        "bill_family_backfill_walks": BACKFILL_WALK_COLUMNS,
        "committee_report_reads": COMMITTEE_REPORT_READ_COLUMNS,
    }
    # Writers that type their columns natively (DATE, INTEGER, BOOLEAN), spelled as DuckDB describes the file.
    builder_schemas = {
        "rulemaking_lifecycles": LIFECYCLE_SCHEMA,
        "lifecycle_events": EVENT_SCHEMA,
        "agency_lifecycle_stats": AGENCY_LIFECYCLE_STATS_SCHEMA,
        "gao_reports": GAO_REPORT_SCHEMA,
        "court_docket_groups": COURT_DOCKET_GROUP_SCHEMA,
    }
    # Writers whose SQL states the DuckDB type itself, as (column, type) pairs.
    builder_pairs = {
        "org_committee_links": ORG_COMMITTEE_LINK_COLUMNS,
    }
    for name in TABLES:
        if name in builder_columns:
            schemas[name] = [(column, "VARCHAR") for column in builder_columns[name]]
        elif name in builder_schemas:
            schemas[name] = described_schema(builder_schemas[name])
        elif name in builder_pairs:
            schemas[name] = list(builder_pairs[name])
        elif name in from_fec:
            schemas[name] = from_fec[name]
        elif name in from_contracts:
            schemas[name] = list(from_contracts[name])
        elif name in RECORD_TYPES:
            rt = RECORD_TYPES[name]
            schemas[name] = [(col, rt.sql_type(col)) for col in rt.schema]
            if name == "comments":
                schemas[name] += [(col, "VARCHAR") for col in COMMENT_MIRROR_COLUMNS]
        elif name in DERIVED_SCHEMAS:
            schemas[name] = list(DERIVED_SCHEMAS[name])
        else:  # pragma: no cover - guards against TABLES/registry drift
            raise KeyError(f"No schema known for table {name!r}")
    return schemas


# --------------------------------------------------------------------------- #
# Live schema discovery (DuckDB DESCRIBE over R2 or a local parquet directory).
# --------------------------------------------------------------------------- #
class SchemaDiscoveryError(OSError):
    """Incomplete live reads, retaining the schemas that could be checked."""

    def __init__(self, schemas: dict[str, list[tuple[str, str]]], failures: list[str]) -> None:
        self.schemas = schemas
        super().__init__("\n".join(failures))


def discover_schemas(source: str, base: str | None = None) -> dict[str, list[tuple[str, str]]]:
    """Discover schemas from published parquet via DuckDB ``DESCRIBE``.

    ``source`` is ``"r2"`` (remote https bucket; needs the httpfs extension) or
    ``"local"`` (a directory of ``<table>.parquet`` files). Mirrors the
    connection recipe in :mod:`spicy_regs.mcp_server`: a remote table resolves
    through the publication index, then the rulemaking snapshot pointer, then
    its bare legacy key.
    """
    import duckdb

    if source == "r2":
        from spicy_regs.sources.publication import published_urls

        base_url = (base or DEFAULT_R2_BASE_URL).rstrip("/")
        urls = published_urls(base_url)
        names = tuple(dict.fromkeys(("dockets", "documents", "comments", "comments_index", *urls)))

        def url_for(name: str) -> list[str]:
            return urls.get(name, [f"{base_url}/{name}.parquet"])

    elif source == "local":
        base_dir = Path(base or "./spicy-regs-data")
        names = TABLES

        def url_for(name: str) -> list[str]:
            return [str(base_dir / f"{name}.parquet")]

    else:
        raise ValueError(f"Unknown source {source!r}; expected 'r2' or 'local'")

    schemas: dict[str, list[tuple[str, str]]] = {}
    failures: list[str] = []
    with duckdb.connect() as con:
        con.execute(f"SET home_directory='{tempfile.gettempdir()}'")
        if source == "r2":
            load_public_http(con)
        for name in names:
            try:
                rows = con.execute(f"DESCRIBE SELECT * FROM {parquet_scan(url_for(name))}").fetchall()
                schemas[name] = [(row[0], row[1]) for row in rows]
            except (duckdb.IOException, duckdb.HTTPException, OSError) as exc:
                failures.append(f"[{name}] {exc}")
    if failures:
        raise SchemaDiscoveryError(schemas, failures)
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
    are the contract's". Every one of them is, and copying them here would
    create a second copy to keep in step with no mechanism
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
    installed = installed_spicy_docs()
    schema_tables = set(schemas)
    desc_tables = set(descriptions)
    for table in sorted(schema_tables - desc_tables):
        errors.append(f"[{table}] table has a schema but no entry in descriptions.yaml")
    for table in sorted(desc_tables - schema_tables):
        errors.append(f"[{table}] described in descriptions.yaml but is not a known table")
    for table in sorted(set(COVERAGE_PROSE_EXCEPTIONS) - set(TABLES)):
        errors.append(f"[{table}] has a coverage exception but is not a known table")

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
        errors.extend(coverage_prose_errors(table, entry.get("coverage") or "", measured_on))
        errors.extend(data_quality_prose_errors(table, entry.get("data_quality") or ""))
        errors.extend(interim_note_errors(table, entry.get("data_quality") or "", installed))
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
        identity = entry.get("identity_columns", [])
        if not isinstance(identity, list) or any(column not in schema_cols for column in identity):
            errors.append(f"[{table}] 'identity_columns' must list declared columns")
        errors.extend(_reconcile_columns(table, "schema", schema_cols, "descriptions.yaml", desc_cols))
        for col in schema_cols:
            text = (entry.get("columns") or {}).get(col)
            if col in desc_cols and not (text or "").strip():
                errors.append(f"[{table}.{col}] has an empty description")
    return errors


def published_row_counts(base_url: str) -> dict[str, int]:
    """Each published table's pinned row count: the index's families and the rulemaking snapshot's manifest."""
    from spicy_regs.sources.publication import load_index, load_rulemaking_snapshot

    base = base_url.rstrip("/")
    rows = {
        key.removesuffix(".parquet"): int(table["rows"])
        for family in load_index(base)["families"].values()
        for key, table in family["tables"].items()
    }
    snapshot = load_rulemaking_snapshot(base)
    if snapshot is not None:
        rows.update({key.removesuffix(".parquet"): int(table["rows"]) for key, table in snapshot["tables"].items()})
    return rows


def kind_index_errors(descriptions: dict, rows: dict[str, int]) -> list[str]:
    """Hold each declared coverage kind to the pinned index: no rows means Empty, and Empty means no rows.

    Where the index can be read, the kind is its fact, not the author's: six
    0-row tables read "Sampled" or "Not a range" on 2026-10-02, so a count over
    them read as a thin sample rather than as zero by construction. The prose
    prefix stays the committed source so the offline build is reproducible; this
    check is what refuses a prefix the index contradicts.
    """
    errors = []
    for table, count in sorted(rows.items()):
        entry = descriptions.get(table)
        if not entry:
            continue
        declared = coverage_kind(entry.get("coverage") or "")
        if (count == 0) != (declared == "empty"):
            expected = "Empty" if count == 0 else "a populated kind"
            errors.append(
                f"[{table}] coverage kind {declared!r} contradicts the published index ({count:,} rows); "
                f"the index says {expected}"
            )
    return errors


def check_schema_drift(
    expected: dict[str, list[tuple[str, str]]],
    live: dict[str, list[tuple[str, str]]],
) -> list[str]:
    """Reconcile the in-code expected schema against a live (parquet) schema: its columns, then each one's type.

    Names alone would pass a DATE column published as VARCHAR, so a typed
    declaration (the lifecycle and attribute tables) is held to the file too.
    """
    errors: list[str] = []
    for table in sorted(set(expected) | set(live)):
        exp_types, live_types = dict(expected.get(table, [])), dict(live.get(table, []))
        errors.extend(_reconcile_columns(table, "in-code schema", list(exp_types), "live parquet", list(live_types)))
        errors.extend(
            f"[{table}.{column}] in-code type {exp_types[column]} but live parquet {live_types[column]}"
            for column in exp_types
            if column in live_types and exp_types[column] != live_types[column]
        )
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

    These are statements about supported tables and retained measurements.
    Neither a declaration nor its measurement date establishes that an output
    is publicly available. Publication requires a separate artifact observation;
    inclusion in a search index is that consumer's own fact.
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
    """Render one table's docs page: banner, label, coverage stamp, column table."""
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
    queryable = "Configured" if table in MCP_QUERYABLE else "Not configured"
    where = f", in the snapshot that `{SNAPSHOT_POINTER}` names" if table in RULEMAKING_TABLES else ""
    lines += [
        f"- **Parquet file:** `{table}.parquet`{where}",
        f"- **MCP `query_sql` support:** {queryable}; requires an available artifact.",
        "- **Publication status:** Not established by this schema page or its measurement date.",
        "- **Row count:** Not stated here; the MCP `describe_table` reply gives the live count under `publication`.",
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
    """Reconcile descriptions against the chosen schema source and report drift.

    Exit 1 on drift; a live source that cannot be read exits EXIT_SOURCE_UNREACHABLE (3), not 1.
    """
    import httpx

    descriptions = load_descriptions(Path(args.descriptions))
    errors: list[str] = []
    unreadable = False

    if args.source == "schema":
        errors += check_descriptions(expected_schemas(), descriptions)
    else:
        try:
            live = discover_schemas(args.source, args.base)
        except SchemaDiscoveryError as exc:
            live = exc.schemas
            unreadable = True
            print(f"! Some live schemas could not be read:\n{exc}", file=sys.stderr)
        except (duckdb.IOException, duckdb.HTTPException, httpx.HTTPError, OSError, PublicationError) as exc:
            # Only the "could not read it" failures, and a pointer that cannot name
            # one publication. A malformed parquet or a bad query is a real
            # problem and must not be reported as an outage.
            print(f"! Could not read the live {args.source} schema: {exc}", file=sys.stderr)
            print("  Drift was NOT checked. This is not a pass.", file=sys.stderr)
            return EXIT_SOURCE_UNREACHABLE
        # Descriptions must cover the live schema, and the in-code registry the
        # docs are generated from must match the live schema too.
        # Supported but unpublished tables still receive offline checks. The
        # live check follows actual publication, including unknown active tables.
        errors += check_descriptions(live, {name: value for name, value in descriptions.items() if name in live})
        expected = {name: value for name, value in expected_schemas().items() if name in live}
        errors += check_schema_drift(expected, live)
        if args.source == "r2":
            # The kind is derived from the pinned index where the index can be read; offline it is the prose prefix.
            try:
                errors += kind_index_errors(descriptions, published_row_counts(args.base or DEFAULT_R2_BASE_URL))
            except (httpx.HTTPError, OSError, PublicationError) as exc:
                print(f"! Could not read the published row counts: {exc}", file=sys.stderr)
                unreadable = True

    stale = qualification_errors() + joins_errors()
    if errors or stale:
        print(f"✗ Data dictionary check failed ({len(errors) + len(stale)} issue(s)):", file=sys.stderr)
        for err in errors + stale:
            print(f"  - {err}", file=sys.stderr)
        if errors:
            print(
                "\nUpdate data_dictionary/descriptions.yaml (and the table's schema declaration "
                "if the schema changed; see expected_schemas) so they line up.",
                file=sys.stderr,
            )
        return 1
    if unreadable:
        print("Live schema verification is incomplete; this is not a pass.", file=sys.stderr)
        return EXIT_SOURCE_UNREACHABLE
    count = len(descriptions) if args.source == "schema" else len(live)
    print(f"✓ Data dictionary check passed ({count} tables, source={args.source}).")
    return 0


def qualification_bytes() -> bytes:
    """The MCP qualification record freshly built from the output ledger; OSError or ValueError when it cannot be."""
    return catalog_bytes(output_ledger.qualification_record(output_ledger.LEDGER.read_text(encoding="utf-8")))


def qualification_errors() -> list[str]:
    """Refuse a bundled qualification record that differs from a fresh build of the output ledger.

    The MCP server reports ledger dispositions from this copy, so a stale one
    would describe audits the ledger no longer states.
    """
    record = output_ledger.RECORD
    try:
        fresh = qualification_bytes()
    except (OSError, ValueError) as exc:
        return [f"{output_ledger.LEDGER_NAME} cannot build the qualification record: {exc}"]
    if not record.is_file() or record.read_bytes() != fresh:
        return [f"{record.name} is stale relative to {output_ledger.LEDGER_NAME}; run 'uv run spicy-regs-dict generate'"]
    return []


def joins_bytes() -> bytes:
    """The declared cross-table joins as the bundled ``table_joins.json`` states them."""
    return catalog_bytes(table_joins.joins_record())


def joins_errors() -> list[str]:
    """Refuse a join naming an undeclared table or column, or a bundled copy that differs from the declarations."""
    errors = table_joins.declaration_errors(expected_schemas())
    record = table_joins.RECORD
    if not record.is_file() or record.read_bytes() != joins_bytes():
        errors.append(f"{record.name} is stale relative to spicy_regs.table_joins; run 'uv run spicy-regs-dict generate'")
    return errors


def build_catalog(descriptions: dict, schemas: dict[str, list[tuple[str, str]]]) -> dict:
    """Return the schema catalog: one supported class per entry, in TABLES order.

    This is the declaration side of a readable catalog, and it exists as a file
    because the consumer cannot import this package. It describes the tables
    this checkout supports. A separate observation of the actual output is
    required to establish publication; local runs and declarations do not.

    It deliberately does **not** say whether a class is searchable. That is the
    serving side's fact, and a reader must derive it by aggregating over its own
    index rather than trusting this document — a catalog that certifies its own
    coverage is not a check. ``MCP_QUERYABLE`` identifies configured table
    support, not current availability, and is not exported here.

    ``measured_on`` is a manually maintained date attached to the coverage
    statement. Some statements describe source measurements, some describe
    local bounded runs, and some remain design statements. Read the associated
    prose and receipt to distinguish them. The date is neither a publication
    timestamp nor an automatic freshness or quality check.

    ``kind`` is the coverage statement's kind as a token. The prose still opens
    by naming it for a person, but a consumer must not have to parse a sentence
    to branch on it: the opening word carries a trailing period or comma
    depending on the phrasing, so a whitespace split fails on seven of the
    twenty-four classes.
    """
    coverage = table_coverage(descriptions)
    return {
        "format_version": CATALOG_FORMAT_VERSION,
        "declares": "supported table schemas; publication and search availability are verified separately",
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


def build_mcp_metadata(descriptions: dict, schemas: dict[str, list[tuple[str, str]]]) -> dict:
    """Bundle dictionary meaning for MCP without a runtime source-reader dependency.

    The public catalog keeps its existing format for downstream consumers. This
    package resource adds field prose, expected types and declared row identity
    from the same inputs. It describes supported output, not observed publication.
    """
    from spicy_regs.aggregate_checks import AGGREGATES

    classes = build_catalog(descriptions, schemas)["classes"]
    result = {}
    for entry in classes:
        table = entry["table"]
        description = descriptions[table]
        contract = _contracts().get(table) if table in CONTRACT_TABLES else None
        record_type = RECORD_TYPES.get(table)
        identity = (
            list(contract.identity)
            if contract is not None
            else [record_type.dedup_key]
            if record_type is not None
            else description.get("identity_columns", [])
        )
        result[table] = {
            **entry,
            "grain": contract.grain if contract is not None else description.get("grain"),
            "identity_columns": identity,
            "columns": [
                {
                    "column_name": name,
                    "column_type": dtype,
                    "description": description["columns"][name],
                }
                for name, dtype in schemas[table]
            ],
        }
        checks = [{"name": check.name, "role": "output" if table == check.output else "input",
                   "grain": list(check.grain), "population": check.population,
                   "selection_policy": "Matching declared input bytes or one materialized/catalog export snapshot; "
                   "fixed comment URLs require unchanged ETags around the read. Each check selects its own pins.",
                   "command": f"uv run --frozen python scripts/check_table_joins.py --aggregate {check.name}",
                   "status": "available_check_not_a_live_measurement"}
                  for check in AGGREGATES if table in (check.output, *check.inputs)]
        if checks:
            result[table]["aggregate_checks"] = checks
    return result


def catalog_bytes(document: dict) -> bytes:
    """Serialize the catalog to its one canonical byte form.

    A vendored contract is pinned by digest, so there must be exactly one byte
    string for a given document. Key order is insertion order, which follows
    ``TABLES``; indent and separators are fixed here rather than at the call
    site so the digest cannot move because someone passed a different flag.
    """
    return json.dumps(document, indent=2, ensure_ascii=False).encode("utf-8") + b"\n"


def cmd_catalog(args: argparse.Namespace) -> int:
    """Write catalog.json plus its sha256 sidecar, refusing (exit 1) when descriptions disagree with the schema."""
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
    """Render the table pages, refusing (exit 1) when descriptions are out of sync with the schema.

    Writing the default docs dir also refreshes catalog.json (+ .sha256), table_metadata.json,
    table_qualification.json (from the output ledger) and table_joins.json (from table_joins).
    """
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
        DEFAULT_MCP_METADATA_PATH.write_bytes(catalog_bytes(build_mcp_metadata(descriptions, schemas)))
        print(f"  - {DEFAULT_MCP_METADATA_PATH.relative_to(REPO_ROOT)}")
        try:
            output_ledger.RECORD.write_bytes(qualification_bytes())
        except (OSError, ValueError) as exc:
            print(f"✗ {output_ledger.LEDGER_NAME} cannot build the qualification record: {exc}", file=sys.stderr)
            return 1
        print(f"  - {output_ledger.RECORD.relative_to(REPO_ROOT)}")
        table_joins.RECORD.write_bytes(joins_bytes())
        print(f"  - {table_joins.RECORD.relative_to(REPO_ROOT)}")
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
