# Explorer metadata

`explorer-metadata.v1.json` is a public, versioned companion to
`publication.v2.json` and the separate rulemaking and comments publications at
the configured data host. It lets an explorer pick up
new descriptions, source attribution and declared joins without rebuilding its
website. The publication index remains the authority for which tables exist,
their current files, columns, row counts and publication times.

## Contents and ownership

The bundle contains `format: spicy-regs-explorer-metadata`, `version: 1`,
`generatedAt`, `sourceRevision`, `publication`, `tables` and `joins`.
`generatedAt` is the metadata build time, not a source observation date.
`publication.sha256` hashes the canonical JSON of the index read for the build;
`publication.families` retains each family's artifact digest.

Each table retains the current data dictionary's label, summary, coverage,
quality notes and column descriptions. It also records:

- `family` and `publicationSchema`: the publication family and exact ordered
  `[column name, type]` pairs this metadata was checked against.
- `sources`: named original publishers, source kinds and notes. Saved historical
  scorecard publisher links may use HTTP; data access still requires HTTPS.
- `inputs`: documented input table IDs. An input outside the public index stays
  listed; consumers must label it unavailable instead of making a broken link.
- `transformation`, `modelGenerated` and optional `sourceEvidence`: how derived
  records were produced and where that statement was checked. Inputs describe
  the transformation, not a claim that its latest run consumed today's tables.
- `metadataStatus` and `sourceStatus`: separate known/unknown states. A new table
  remains discoverable even when its description or origin is undocumented.
- Optional `emptyReason` and `joinAudit`: documented empty-output reasons and
  relationship dispositions. Counts never establish completeness.

`data_dictionary/descriptions.yaml` owns curated table descriptions, generated
into `src/spicy_regs/table_metadata.json`. Still-published processing tables use
that same curated dictionary when absent from the subject-only generated file.
`explorer_sources.json` owns source attribution and derived inputs.
Source mappings are explicit: a shared column name never creates a relationship.
Scorecard publisher names and URLs are read from the current published
`scorecard_publishers` table, with size and digest checks. This is the only table
whose record bodies the metadata publisher reads.

The publisher also resolves `materialized/rulemaking/latest.json` and
`comments-publication.json` through the existing publication readers. Only
public files they name enter the bundle; main-index tables take precedence.
`extra_tables` records each separate file's family, exact schema, normalized
file descriptor and `publicationIdentity`. The identity is SHA-256 of the
sorted-key compact UTF-8 descriptor JSON, including the snapshot ID or mutable
file ETag. It does not change the main index's hash or family digests.

For a new separate file identity, bounded range requests read its Parquet
footer, checking the advertised byte size, ETag and row count. They inspect
metadata rather than scan records or independently recompute the file checksum.
The previous validated bundle supplies reusable schemas for unchanged file
identities. Mutable comments still require a matching ETag and size each run.

All table descriptions, including scorecards, come from the primary dictionary.
All relationships are authored in `table_joins.py` and generated into
`table_joins.json`. Both the explorer and MCP read this one generated registry.
There is no supplemental description or navigation registry. Detailed MCP
descriptions include full measurements on outgoing joins. Incoming joins retain
their keys, reasons and baseline counts, and point to the child description for
full measurement evidence so heavily connected tables stay within reply limits.

Every declared join uses the same checks. Pull requests measure added or changed
joins against published data; the scheduled check measures the whole registry.
CI checks complete composite keys, declared parent cardinality, and resolution
against each join's measured minimum. A `scope` join may have missing parent
keys for its documented reason; a measured rate does not establish that an
unexplained missing record is correct. Empty joins have no measured minimum;
newly populated keys require a baseline before the check passes. Native-key
connections marked `unmeasured` retain that uncertainty and do not borrow their
previous processing-key baseline. Existing lag
allowances remain in effect for independently published tables.

`join_measurements.json` packages full-input measurements and their source
pointers. The [join audit receipt](evidence/explorer-joins-2026-10-03.json) and
[scorecard navigation receipt](evidence/scorecard-navigation-2026-10-03.json)
retain the checked immutable inputs. The
[scorecard source receipt](evidence/scorecard-source-joins-2026-10-04.json)
establishes baselines for 25 populated source joins and records five with no
non-null keys. `join_audit.json` explains tables that
remain standalone or need special handling; it does not define executable joins.

Before publishing, the builder validates every declared join's table names,
columns, complete composite-key length, resolution kind and declared
cardinality. Measurements establish data cardinality; this structural check
does not invent it. A previously documented key missing from the current files
pauses that connection with the exact missing fields; unrelated connections and
descriptions still refresh. Malformed or never-documented keys fail validation.
Valid joins with an unpublished endpoint are retained in
`omittedJoins` with an explanation, and are excluded from actionable navigation.

## Refresh and deployment

The `Publish explorer metadata` workflow runs after dictionary changes on
`main`, after CI or the explicitly listed data workflows finish successfully, on
manual dispatch, and at minutes 7, 22, 37 and 52 each hour. The scheduled check
also catches workstation publications and new data workflows. GitHub may delay
scheduled runs. Every trigger first requires successful push CI for the exact
current `main` revision and checks out that verified revision. Pending or failed
CI leaves existing metadata in place; successful CI completion retries publication.
A newly added workflow can be listed for an immediate refresh;
it does not require a website change. Metadata failures do not fail a data
publication or replace the previous valid metadata object.

The workflow uses the existing `R2_*` secrets and the existing public URL/domain
configuration. It reads the current index and separate publication controls
directly from R2 and writes only
`explorer-metadata.v1.json`; it never changes the index, Parquet objects or MCP
container. The write is one complete JSON object, followed by an R2 readback.
It uses a 60-second cache lifetime and the existing best-effort Cloudflare URL
purge. Unchanged inputs leave `generatedAt` unchanged.

Read-only local validation:

```sh
uv run --frozen python scripts/publish_explorer_metadata.py \
  --base-url https://data.example.org --output output/explorer-metadata.v1.json
```

The default base URL follows `SPICY_REGS_R2_URL`, then `R2_PUBLIC_URL`, then
`SPICYREGS_DOMAIN` and its upstream fallback, as the rest of the project does.
`--index local-publication.v2.json` supplies a local index for validation; the
publisher lookup still reads the immutable publisher file that index names.
To write using configured credentials:

```sh
uv run --frozen python scripts/publish_explorer_metadata.py --publish
```

## Consumer safety

Fetch the publication index and metadata independently. Always show current
tables from the index, including unknown ones. Compare `publicationSchema`
before using descriptions or navigation for each table. For separate tables,
also match the current file descriptor and `publicationIdentity` before using
the verified footer schema. Reject joins unless
both endpoint schemas and all key columns match. A newer family digest with an
unchanged schema can keep its metadata, but must display its older-publication
status separately. A failed or invalid bundle must show an explicit unavailable
state while leaving the data browser usable. Do not silently substitute a
bundled old join dictionary.

Coverage text is documented scope, not an inferred date range. A missing or
invalid table publication date stays unavailable; it must not be replaced by
`generatedAt`, the source modification date or a backfill time.
