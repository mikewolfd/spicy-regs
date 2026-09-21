# FEC source discovery and retained data

SpicyRegs exposes source metadata, records and reported relationships for reuse
across investigations. SpicyDocs supplies the FEC source inventory, acquisition
and parsing. SpicyRegs builds queryable tables and serves their meaning through
MCP. Each collection retains its own selection, dates and evidence.

## Outputs

| Table | What one row means | How to use it |
| --- | --- | --- |
| `fec_source_catalog` | One official source family from the pinned SpicyDocs inventory | Find API routes, discovery indexes, bulk families and official references. Join collection evidence on `source_family`. |
| `fec_collections` | One explicitly selected, verified input collection | Read its requested scope, record and relationship counts, coverage limits and empty-result status before combining records. |
| `fec_source_records` | One source record within a collection | Query identifiers and complete native metadata; follow its digest and exact locator to evidence. Join on `(collection_id, source_record_id)`. |
| `fec_relationships` | One source-reported relationship value or explicit missing/empty observation | Filter `value_status`; join its locator's `collection_id` and `source_record_id` to the complete source record. Preserve name-only targets and conflicting observations. |
| `fec_committees` | One committee ID in the accumulated reference table | Use the existing 16-column identity table. Its default acquisition uses SpicyDocs and merges a completed unfiltered traversal with prior observations. |

`org_committee_links` continues to contain candidates produced by name matching.
Those matches have different evidence from source-reported FEC relationships.

The source catalog covers all 26 researched official families. A route in the
catalog does not establish that its records, full history or linked documents
have been acquired. The collection and record tables state the selected data
actually processed. API metadata can point to a document without acquiring its
body. Positional files retain literal fields and coordinates; their amounts do
not become current financial totals merely because they are queryable.

## Build and query

Build discovery metadata from the installed, pinned provider without a network
request:

```sh
uv run --frozen run-rollup-fec-source-catalog --output-dir output/fec-catalog
```

Build selected source records from an explicit input manifest:

```sh
uv run --frozen build-fec-observations --manifest retained-inputs.json --output-dir output/fec
```

The input manifest accepts supported SpicyDocs releases with expected artifact
digests, or explicit retained inputs. Input selection belongs to the caller;
the builder verifies that selection before making its generation available.
See [reported relationships](fec-relationships.md) for manifest details and
supported profiles. The default commands keep outputs local.

Read metadata through MCP's `list_sources` and `describe_table`. The former
distinguishes loaded tables from declared outputs. The latter returns field
meanings, row identity, coverage notes and differences between declared and
loaded columns. These descriptions travel with the installed server; querying
public tables does not require installing SpicyDocs.

For a local audit, place the selected generation's Parquet files and the source
catalog together in one directory, then run the same server against that directory:

```sh
SPICY_REGS_DATA_DIR=/absolute/path/to/fec-tables uv run --frozen spicy-regs-mcp
```

Local mode loads only that directory. It does not fill absent files from the
public service. Keep related tables from one generation together; the ordinary
R2 upload mechanism publishes files sequentially and does not provide an atomic
change across all tables.

```sql
SELECT s.source_family, s.title,
       c.collection_id, c.profile, c.record_count, c.record_outcome
FROM fec_source_catalog s
LEFT JOIN fec_collections c USING (source_family)
ORDER BY s.source_family, c.collection_id;
```

```sql
SELECT r.relationship_type, r.subject_id, r.object_id, r.object_name,
       r.value_status, s.metadata_json, s.source_url, s.source_locator_json
FROM fec_relationships r
JOIN fec_source_records s
  ON s.collection_id = json_extract_string(r.source_locator_json, '$.collection_id')
 AND s.source_record_id = json_extract_string(r.source_locator_json, '$.source_record_id')
WHERE r.value_status = 'reported'
LIMIT 20;
```

Do not sum across overlapping source collections or discard amendments without
an explicit source-specific rule. Query the native metadata and scope first.

## Completion checks

For each added collection:

1. Declare its source family, selection and complete input membership.
2. Verify raw digests and exact row, JSON Pointer or ZIP-member coordinates.
3. Reconcile record counts and every mapped field where practical.
4. Manually inspect raw/output pairs, including nulls, empty results, amendments,
   unusual IDs and any unsupported forms.
5. Exercise the actual table descriptions and joins through MCP.
6. Record local validation and public availability separately.

Failed collection builds leave earlier generations available. New attempts use
their own output generation. Committee acquisition retains each attempt's raw
responses and status; set `FEC_CAPTURE_DIR` to durable storage for scheduled
jobs. An interrupted or bounded traversal remains incomplete.
The existing committee workflow retains that evidence as a GitHub Actions
artifact for 30 days on successful and failed runs; longer retention needs
durable storage outside the disposable runner.

Further source coverage follows the same process across identities, financial
records, filings, legal materials, reference data and agency publications.
Historical backfills, source-specific correction rules and public refresh
operations remain visible work items; one example investigation does not set
the catalog's scope.

## Bulk coverage

The September 21, 2026 retained capture of the
[official bulk page](https://www.fec.gov/data/browse-data/?tab=bulk-data) contains
26 file groups and 331 grouped links, including data files, descriptions, headers
and directory indexes. Its group names and links match the September 11 research
inventory. These 26 file groups differ from the 26 broader official source
families in `fec_source_catalog`; the equal counts are coincidental.

Use bulk files for full-file and historical work. Retain API observations where
they supply additional fields, explicit query outcomes or other source coverage.
The existing committee reference pipeline still walks the API because its shape
contains fields not supplied by the committee master alone.

File metadata and parsed rows serve different needs. A `bulk` record exposes an
original file's URL, size, digest, acquisition time and verified ZIP member
inventory. It lets a caller select the native data before expanding millions of
records. A `positional` collection explicitly selects a file or archive member
for row-level access. Read `profile` and the collection scope when interpreting
record counts; a file count is not a transaction count.

```sql
SELECT collection_id, source_family, source_url, source_sha256,
       json_extract_string(metadata_json, '$.capture.objectKey') AS object_key,
       json_extract(metadata_json, '$.archive.members') AS archive_members
FROM fec_source_records
WHERE profile = 'bulk';
```

Named positional fields come from a retained source header or an explicitly
selected official HTML dictionary. The collection keeps the dictionary's literal
definitions and byte coordinates. Width mismatches refuse named mapping; the
caller can retain the complete positional record with the mismatch documented.
Correction-file insert/delete records remain separate observations until a
caller chooses and verifies an amendment policy.

Selected whole files can be complete within their declared period while the
family's history remains incomplete. Historical backfills of the individual,
intercommittee and operating-expenditure archives, full schedule database dumps,
and full daily-filing history need further acquisition and output checks.
Selected `pas2` rows do not establish coverage of the broader intercommittee
export. Financial totals also need source-specific amendment and memo rules.

See the dated [bulk expansion and audits](research/fec-bulk-continuation-2026-09-21.md)
for the selected 2026 transaction files, named summary mappings and unresolved
publisher-format and access issues. Those retained snapshots are separate from
complete historical backfills and public availability.
