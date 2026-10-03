# FEC tables and child rows

**Status:** the implementation described below is intermediate. The October 3
repository-wide decision in [PLAN](../PLAN.md#dataset-and-etl-receipt-separation)
requires subject tables to contain domain data and stable keys only, with
processing details in a separate shared ETL receipt table and native lists in
place of JSON text. The existing cleanup has not yet completed that migration.

Use the existing FEC tables as the primary interface. Their producers interpret
source values before writing Parquet, so direct-file queries and MCP queries
receive the same typed facts. Do not introduce another table name merely to
hide columns or convert values from an existing table.

This follows the [FEC data model](fec-data-model.md#how-the-data-becomes-useful)
and the [delivery plan](fec-delivery-plan.md#scope-and-release-choice): preserve
source evidence, build useful typed subject records, and keep financial
qualification separate. Original source records and collection context remain
accessible through exact evidence references.

## Primary tables

| Existing table | Producer responsibility |
| --- | --- |
| `fec_candidate_api_observations`, `fec_committee_observations` | Typed identity attributes, independent arrays and interpretation statuses; no duplicated complete native objects or financial defaults. |
| `fec_committee_master_observations`, `fec_postgres_committee_history_observations` | Flat source attributes; known years, dates and flags typed once. Native spellings, capture descriptions and database extraction details remain in source evidence. |
| `fec_registration_statements`, `fec_lobbyist_registrations`, `fec_filings` | Typed supported values with raw/status fields. Filter registrations by `form_type`; uncertain two-digit dates remain unresolved. |
| `fec_filing_report_observations`, `fec_filing_text_observations` | Report facts and ordered measures or fragments, without a second copy of the complete native field array. |
| `fec_legal_matters`, `fec_legal_documents` | Matter and document facts. Useful legal scalars are typed columns; citation and subject structures retain their source hierarchy. |
| `fec_agency_report_text` | Text, source order and explicit `text_status`; blank and missing text remain diagnostic rows. |
| `fec_research_filing_feed_items` | Feed identifiers, labels, dates and timestamp interpreted at build time. Candidate IDs incorrectly labelled CommitteeId by the source retain that distinction. |
| `fec_research_source_pages` | Body text and `content_status`. Filter on `body_extracted` for substantive text; failure and unsupported-template rows remain in the same table. |
| `fec_research_document_observations`, `fec_retained_csv_observations` | Discovery leads and bounded samples, with their explicit source and financial-use limitations. |
| `fec_api_response_controls`, `fec_research_response_outcomes`, `fec_research_context_dispositions` | Typed capture/mapping diagnostics. Complete originals remain in source evidence. |
| `fec_collections` | Collection scope, context and numeric record/relationship counts. This remains an evidence table, not an additional subject dataset. |

## When a child view is justified

A child view must provide a different useful row meaning. It does not create
another physical table, qualify a financial total or replace its parent.

| Views | Why they exist |
| --- | --- |
| Candidate cycle, election-year, district and inactive-year views | Query independent repeated source attributes without pairing unrelated array positions. |
| Committee cycle, candidate-link and sponsor-candidate-link views | Query ordered membership/reference occurrences, preserving repeats. |
| `fec_filing_report_measures` | Query one reported measure with its exact value, label, definition coordinate, role and period basis. |
| `fec_filing_text` | Query one ordered narrative fragment, including future filings with multiple fragments. |
| `fec_legal_citations`, `fec_legal_subjects` | Query citation occurrences and subject hierarchy nodes with source pointers. |
| `fec_meeting_dates`, `fec_meeting_links` | Query independent date and link arrays; date ranges remain endpoints, not invented daily events. |
| `fec_context_output_counts` | Query one reported output-table count from a mapping outcome. |
| `fec_api_responses` | Group typed control observations into one captured-response summary, preserving conflicts and coverage limitations. |

## Discovery and field meanings

MCP `list_sources.tables` contains both primary tables and the useful child
views. A child entry declares `role: child_query` and `source_tables`. Ordinary
relationship navigation remains in `relationship_views`; there is no separate
FEC mirror-table discovery list.

`describe_table` returns field meanings, types, row identity, publication pins
and coverage limitations. Child fields inherit unchanged meanings from the
parent dictionary; new fields require explicit definitions. Dictionary
generation refuses undocumented child columns. Roles are explicit properties,
independent of rule-version strings.

Catalog format 4 adds dataset categories and a separate `views` declaration for
these logical children. `classes` continues to describe physical table schemas.
A view declaration never implies a downloadable Parquet file. Consumers of the
vendored catalog must refresh their copy when adopting this format.

## Evidence and release checks

Record IDs and source locators remain stable across these mapping revisions.
Mapping versions identify changed interpretation. Preserve array order and
repetition, source nulls, unsupported values, name-only references and partial
capture limitations. A typed value does not establish current records, complete
populations, verified identities or financial inclusion.

The supported schema declaration records the original union receipt as
`baseline_union_receipt_sha256`. That baseline does not qualify newly shaped
outputs. Rebuild and validate the selected data, seal a new generation and
qualify the matching consumer before publication. Updated filing-association
and quality-notice rules refuse unsupported mapping versions.

The cleanup changes producers and declarations locally; it does not mutate
previously published generations or delete original evidence. Publication and
verified source cleanup remain separate actions under the delivery plan.
