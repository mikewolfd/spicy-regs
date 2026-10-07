# FEC subjects and ETL receipts

FEC family mapper outputs pass through `write_fec_subjects` before storage.
The subject file contains business values and stable keys. The shared
`etl_receipts.parquet` contains conversion inputs, source witnesses, parser
outcomes, private conversion evidence, and failed attempts. Main rows retain the source and qualification fields needed to inspect a relationship.

The explicit field policies are in `src/spicy_regs/fec_subject_fields.json`.
`fec_native_field_maps.json` declares names for flattened publisher properties.
`dataset_policy(table, input_schema)` derives the subject schema for the selected
mapper schema and refuses unclassified columns. It does not infer destinations
from suffixes at runtime.

Financial values stay exact decimals. Correction operations, reported measure
roles, period bases, legal business statuses, and the publisher's historical
amendment methodology remain business data. Source identity, authority, namespace, generation, definition and header context, and applicable qualification statuses are main columns. Exact amount parsing diagnostics remain separate from the published amounts.
The separate legal identifiers `rm_id` and `rm_number` retain their meanings.

Legal citations and subjects, agency organizations and dimensions, report
measures, and narrative fragments use native lists and structures. They preserve
order, repeats, and null elements. Subject hierarchy nodes retain their ordinal
paths. The receipt retains the original nested input, including missing versus
explicitly null properties and any unsupported structure.

## Write and assemble

`write_fec_subjects` accepts actual mapper rows, their declared Arrow schema, an
explicit generation ID, and a callback supplying a `ReceiptContext` for each
row. That callback must provide the selected source witnesses, including every
ordered repeated witness. Conversion failures produce refused receipts without
subject rows. An unknown column never disappears silently.

`assemble_subject_table` accepts qualified `TypedInput` members. It verifies
file digests, row membership, compatible schemas, and any declared source
namespace partition before splitting the rows. Subject output uses ordinary
Parquet membership. `source_namespace` stays in the main table where the mapper supplies it, including single-namespace populations. Each table's explicit policy states its fields. The assembler
restores and compares every input cell when all inputs are admitted. Refused
inputs make `full_rebuild_qualified` false.

Filing definitions, definition evidence, header association checks and agency mapping dispositions have main logical rows. Their `observation_ordinal` identifies each observation within the selected generation, including repeated observations. Definition bodies and conversion inputs stay in receipts; the ordinal never supplies a missing publisher record identifier.

For applicable financial/text inputs, pass `FilingAssociationInputs` to `assemble_subject_table`. It selects exact SHA-256 and row-count-pinned filing metadata and optional maintained header associations, plus source/target generation pins and admitted namespace-definition evidence. The maintained number/header rules produce one decision per source row. The assembly checks dependency bytes and compares every original mapper cell through receipt readback.

`filing_association_status` and its source/target pins state the decision. Every matching filing observation remains listed; unresolved, conflicting and missing-context decisions do not create edges. Header witnesses from a different generation or locator cannot resolve. No association selects a latest filing, applies corrections, or changes an amount. Catalog, agency, legal and collection rows gain no generic filing key. Without explicitly selected dependencies, applicable rows state `not_evaluated`.

## Read financial and source evidence

Public consumers read subject files. Internal financial, evidence, and filing
association consumers use `read_fec_with_receipts`, which restores the exact
prior mapper columns after the shared reader validates generation, stable
identity, content version, and receipt cardinality. Missing or ambiguous
receipts raise an error before a row is returned.

For existing SQL consumers, `register_internal_fec_table` creates a temporary
relation on a private connection after the same checks. Pass those restored
column names as `processing_schemas` to `fec_query_views`. Existing financial
and source evidence rules then run against the original values and continue
to enforce their existing restrictions. Subject columns alone cannot trigger
the old stored-evidence fallback.

Use `FEC_NATIVE_DOCUMENT_QUERY_VIEWS` for the existing document child views
when their parent tables use subject schemas. These views expand a single
native list per branch. Global catalog and MCP selection must choose these
specifications with the migrated schemas.

## Qualification boundary

The local family replay verifies held mapper rows, exact restoration, native
shapes, and receipt linkage. It does not reacquire sources or independently
reparse all original bytes. Prior published-versus-HEAD legal URL differences
are inherited from the earlier cleanup. A complete source rebuild, global
serving admission, and any publication require their own validation.

## Qualified serving

The MCP server keeps the existing qualified FEC view names. Its registered SQL
and release checks use packaged processing schemas. For a compatible release,
`ReceiptAdapter` reads only the captured generation's subject and shared receipt
members, checks their byte digests, and validates dataset, generation, identity,
content version, and receipt cardinality. It restores required processing values
in private tables and binds the registered SQL to those tables. Public subject
tables keep their native business schema.

The adapter and processing schema declarations are part of the measured release
identity. A prior release receipt cannot authorize changed interpretation code.
Missing, ambiguous, modified, or mismatched receipts fail the connection build;
there is no fallback to a different generation or to unqualified business rows.
Restoration is lazy: unavailable release configurations do not scan receipt rows.
`list_sources` and `describe_table` separately expose the shared receipt schema
and dataset selectors.

## Policy migration

`fec-subject-receipts/2` changes the main schema. Replay exact selected mapper inputs through the maintained assembler and publish subjects and receipts together. Old native generations keep their original policy pins; they cannot be relabeled as version 2. Navigation metadata and serving admission must select the matching new generation before these fields are advertised as available.
