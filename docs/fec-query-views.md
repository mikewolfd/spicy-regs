# FEC tables and child rows

Since the October 3 [subject and receipt split](fec-subject-receipts.md), current
builds write each FEC table with domain values, stable keys and native lists
only. Raw values, interpretation statuses, mapping versions, source locators and
witnesses are in the generation's shared `etl_receipts.parquet`, selected by
`dataset`. Source evidence, collection scope, filing definitions and capture
diagnostics are receipt-only datasets there and create no tables. Generations
published before the split keep the earlier columns and tables; in the
publication index, a family with an `etlReceipts` member uses the receipt layout.

Use the existing FEC tables as the primary interface. Their producers interpret
source values before writing Parquet, so direct-file queries and MCP queries
receive the same typed facts. Do not introduce another table name merely to
hide columns or convert values from an existing table.

This follows the [FEC data model](fec-data-model.md#how-the-data-becomes-useful)
and the [delivery plan](fec-delivery-plan.md): preserve
source evidence, build useful typed subject records, and keep financial
qualification separate. Original source records and collection context remain
accessible through exact evidence references.

## Primary tables

| Existing table | Producer responsibility |
| --- | --- |
| `fec_candidate_api_observations`, `fec_committee_observations` | Typed identity attributes and native lists of cycles, election years, districts and candidate links; no duplicated complete native objects or financial defaults. Each value's raw spelling and interpretation status are in the receipt. |
| `fec_committee_master_observations`, `fec_postgres_committee_history_observations` | Flat source attributes; known years, dates and flags typed once. Native spellings, capture descriptions and database extraction details remain in the receipt. |
| `fec_registration_statements`, `fec_lobbyist_registrations`, `fec_filings` | Typed supported values; the raw value and its status are in the receipt. Filter registrations by `form_type`; uncertain two-digit dates remain unresolved. |
| `fec_filing_report_observations`, `fec_filing_text_observations` | Report facts with the ordered native lists `reported_measures` and `text_fragments`, without a second copy of the complete native field array. |
| `fec_legal_matters`, `fec_legal_documents` | Matter and document facts. Useful legal scalars are typed columns; citations and subjects are native lists and structures that keep their source hierarchy. |
| `fec_agency_report_text` | Text, its kind and its source order (`text_ordinal`). |
| `fec_research_filing_feed_items` | Feed identifiers, labels, dates and timestamp interpreted at build time. Candidate IDs incorrectly labelled CommitteeId by the source retain that distinction. |
| `fec_research_source_pages` | Body text, content scope and a native list of links. The extraction status (`content_status`) is in the receipt. |
| `fec_research_meeting_observations` | Meeting facts with native lists of dates and links; `date_kind` says whether the dates are one date, separately listed dates or the two endpoints of a range. |
| `fec_research_document_observations`, `fec_retained_csv_observations` | Discovery leads and bounded samples, with their explicit source and financial-use limitations. |

## Receipt-only datasets

These datasets are rows of `etl_receipts.parquet` with their name as `dataset`.
They are processing evidence, not tables, and have no Parquet file of their own.

| Dataset | What it records |
| --- | --- |
| `fec_source_catalog`, `fec_collections`, `fec_collection_selection`, `fec_source_records`, `fec_record_evidence` | The official source inventory, each selected collection's scope and counts, and the complete source records behind the typed rows. |
| `fec_filing_definitions`, `fec_filing_definition_evidence`, `fec_filing_header_associations`, `fec_agency_mapping_dispositions` | Filing layouts and their evidence, header association checks and agency mapping decisions. |
| `fec_api_response_controls`, `fec_research_response_outcomes`, `fec_research_context_dispositions` | Capture and mapping diagnostics. |

## When a child view is justified

A child view must provide a different useful row meaning. It does not create
another physical table, qualify a financial total or replace its parent. Each
gives one row per element of its parent's native lists.

| Views | Why they exist |
| --- | --- |
| `fec_filing_report_measures` | Query one reported measure with its exact value, label, role and period basis. |
| `fec_filing_text` | Query one ordered narrative fragment, including future filings with multiple fragments. |
| `fec_legal_citations`, `fec_legal_subjects` | Query citation occurrences and subject hierarchy nodes in source order. |

Repeated identity attributes (cycles, election years, candidate links) and
meeting dates and links are native list columns of their tables; `UNNEST` them
with `WITH ORDINALITY` to keep their positions. The earlier array-expansion,
output-count and captured-response views read columns that are now in the
receipt, and are not registered against these tables.

## Discovery and field meanings

MCP `list_sources` lists the primary tables and the useful child views by
subject. A child entry declares `role: child_query` and `source_tables`. Ordinary
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

Record IDs remain stable across these mapping revisions. Source locators and
the mapping versions that identify changed interpretation are in the receipt.
Preserve array order and
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
