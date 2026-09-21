# Local data available for fork generation

The local holdings can supply substantial parts of the fork without downloading
the sources again. The selected FEC generations are already sealed and audited.
Other holdings include source releases, corrected cohorts, older public tables
and bulk originals that need different preparation before publication.

This inventory was measured on September 21, 2026. It did not upload data or
dispatch generation workflows. The [fork generation plan](../fork-generation.md)
owns the complete producer/output sequence; finding a local file does not close
that work.

## Locations and verification

The main receipt root, abbreviated `R` below, is:

```text
/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts
```

The [inventory evidence directory][inventory] contains the discovery paths,
`local-prepared-table-inventory.json` (current Parquet footers and lengths),
`local-source-release-inventory.json` (exact release members) and its compact
`local-source-release-summary.json`. Searches covered
`Work` (including `corpora`, checkouts and preserved worktrees), `Documents`,
`Downloads`, `Desktop`, application caches, Codex/Claude working directories and
`/tmp`. Dependency trees and unrelated application caches were excluded. The
first broad search returned an error status without retained error details, so
this is not an exhaustive disk census. The separate temporary-directory search
completed without errors.

Verification here used live file presence, byte lengths, Parquet footers and
existing audit receipts. Large existing corpora were not rehashed or fully reread.
The smaller temporary legislative files preserved below received digest and ZIP
integrity checks. Prior raw/output comparisons remain dated evidence, not newly
repeated audits.
Sizes are apparent bytes; GB and MB below use decimal units. Do not add all rows
in these tables: originals, outputs, repairs and preserved copies can overlap.

## First publication candidates

| Holding under `R` | Verified local scope | Preparation |
| --- | --- | --- |
| `fec-generation-readiness-2026-09-21/generations/` | Two sealed families, **1,171,789,372 bytes** including metadata: 13,717,161 source records, 649 collections, 183,390 reported relationships and 26 broad source-catalog rows. | Reuse the exact sealed artifacts through managed publication, then verify remote downloads and MCP reads. The catalog describes source families; it does not claim all their records were acquired. |
| `fec-census-delivery-2026-09-12/consumer/fec_committees.parquet` | **27,311 rows; 1,038,794 bytes**. All 16 column names/types match the current registered schema. | Seal and admit the existing table. Preserve the explicit `cycle=2024&cycle=2026` traversal; this is not a fresh unfiltered census. |

The FEC generation pins are:

```text
fec-observations
sha256:11bcb620974dff661fe22a76b9e10f0d416ef54c334ecd010dccd0a6ba1898f8

fec-source-catalog
sha256:0570574b9ed0aff29880269f8d1e44b9ea5243014aec6cf8da71b2d7e8093d35
```

`fec-generation-readiness-2026-09-21/seal.json` identifies the exact directories
and confirms byte identity with the audited delivery. `completion.json` records
the successful local publisher → CLI download → stdio MCP roundtrip. Both still
record that external data publication had not happened.

## Regulatory source releases available for rebuild

`supply-2026-09-02/campaign/catalog-A-inputs.json` references 671 retained
releases. Their manifests reference **65,745 distinct blobs totaling
13,797,370,204 bytes**, plus 20,855,694 bytes of release-local members. Every
referenced blob and local member exists and matches its recorded byte length.

| Source selection | Releases | Native records in publication receipts | Distinct referenced blob bytes |
| --- | ---: | ---: | ---: |
| Federal Register, `releases/fr-full-1994-2026` | 1 | 1,007,156 | 4,988,949,808 |
| Regulations.gov dockets | 335 | 278,607 | 723,799,895 |
| Regulations.gov documents | 335 | 1,943,108 | 8,084,620,501 |

The bytes include originals, acquisition evidence and source-native companion
representations. They are not exclusively raw downloads. Counts come from
`publishedRecordCount`, not the manifest's larger count across companion
representations. The selected index contains no duplicate release names or
artifact pins. These are retained snapshot counts, not current source totals.

The old receipts report no failed records and passing release verification;
139 docket releases and 19 document releases are empty selections. Those old
verdicts do not supersede subsequently discovered field-recovery defects or
establish full agency absence. Rebuild with the corrected readers, retaining
the newer observations in the chosen public parent where the source snapshots
are older. Replacing the public parent wholesale with these older populations
would lose coverage.

## Prepared non-FEC data

| Holding | Local scope | Use and remaining qualification |
| --- | --- | --- |
| `R/data-validation-sprint-2026-09-21/public/` | **22 whole non-FEC public objects and one comments sample; 449,754,377 bytes**. Includes 279,085 dockets, 2,001,222 documents, 112,861 comments-index rows, 885,266 SAM entities and 287,606 lobbying filings. | Reusable retained parents. Read `reconciled-assessments.json` before selecting any table; these are not uniformly qualified final outputs. Preserve acquisition pins from `public-acquisition.jsonl` and `public-documents-refresh.json`. |
| `R/remediation-sprint-2026-09-21/fr-identity/verified-replay/public-plus-native/federal_register.parquet` | **803,997 rows; 123,857,394 bytes; 23 columns**. | Corrected retained public parent with a bounded dated-identity repair. Wider historical source-field recovery remains. |
| `R/remaining-gaps-wave1-2026-09-21/sr1/independent-second-review/native-bill-final/` | Five corrected 118th-Congress HR/S tables: **129,718 rows; 4,658,963 Parquet bytes**. | Reuse in a complete `bill-family` build. The sealed cohort is named `qualified-bill-status-118-hr-s`; publishing that family unchanged would claim five table keys needed by `bill-family`. Reconcile all 18 outputs before production publication. |
| `R/d1-measured-run-2026-09-19/output/` | Measured members/terms, House votes, current committee rosters, laws, amendments, meetings, nominations, record issues, treaties and press releases. | Current schema admission and complete-family sealing; preserve measured Congress/date limits. These are bounded runs, mostly for Congress 119. |
| `R/rollups-0-24-0-adoption-2026-09-20/` | Newer house communications: **4,969 rows**. Four print-citation outputs: **41 / 29 / 12,700 / 49,935 rows**. | Prefer these later prepared outputs over the corresponding older measured copies; qualify the current schema and family members. |
| `R/correction-lifecycle-2026-09-21/senate-final/run2/replay-output/senate_expenditures.parquet` | **3,272 rows; 234,172 bytes**. | Bounded retained corpus with one 357-row Part I correction. Other unavailable packages remain unrepaired. |
| `spicy-regs/output/court-data-2026-08-22/court_opinion_clusters.parquet` | **10,070,727 rows; 3,939,704,769 bytes**. | Retained materialization; paired raw cluster-dump agreement is not qualified. |
| Same directory, `court_opinion_bodies.parquet` | **250,000 rows; 1,735,931,994 bytes**. | Rebuild the old 19-column output: demonstrated native text variants were omitted. The corrected 25-column replay contains only 284 rows. |
| Same directory, `docket_courts-2026-06-30.parquet` | **71,677,647 rows; 332,460,420 bytes**. | Auxiliary two-column join input, not an additional hosted rollup. |
| Same directory, `court_cluster_scope.parquet` | **10,070,727 rows; 101,258,062 bytes**. | Five-column companion to the retained cluster population, not additional source coverage. |
| `spicy-regs/output/bill_subjects.parquet` | **20,013 rows; 403,212 bytes**, Congresses 116–119. | Older enrichment input; 5,624 overlapping rows matched a later native audit. Reconcile the selected bill population and publication schema. |

The five-table bill cohort's sealed pin is
`sha256:2bb61c8c8b4af24270a036d9d9e5a031b7803c56cb17bf2cbabfd6e0a51cbd73`.
Its review records 1,189,950 native-field comparisons and 2,206,611 stable-key
cell comparisons. This supports the selected cohort, not broader historical
coverage or the other thirteen family outputs.

### Retained Congress originals

Two official 118th-Congress BILLSTATUS ZIPs remain under
`R/cbo-routes-2026-09-20/blobs/`; the adjacent `requests.jsonl` records successful
acquisition from the official URLs:

| Scope | Blob name | XML members | Compressed bytes |
| --- | --- | ---: | ---: |
| 118 HR | `8e7ca7dab50a7b9b977f021ec1b3231f8fedf82c33494553857b892fadfdba98` | 10,564 | 35,522,726 |
| 118 S | `269261c0989db3ced789680ee2202747df9a7298f1ac8d2b074d3356b06e399c` | 5,649 | 14,410,894 |

The temporary-directory search found four additional ZIPs: `/tmp/bs119s.zip`
and `hr.zip`, `hres.zip`, `sres.zip` under
`/tmp/claude-501/-Users-mikewolfd-Work-spicy-docs/8a5a1a5d-bd44-4a09-a525-c269c5837b3d/scratchpad/`.
They contain **18,366 XML members / 50,395,149 compressed bytes**:
5,428 S, 10,503 HR, 1,566 HRES and 869 SRES members, all named for Congress 119.
They do not cover the four other resolution types.

These four archives and the scratchpad's **40 bill XML bodies with 40 acquisition
sidecars** (17,686,151 bytes together) were copied to the inventory directory's
`retained-legislative-inputs/blobs/sha256/`. Its `manifest.json` records original
paths, preserved paths, byte lengths, digests and archive member scopes. Copy
digests and all ZIP CRCs passed. Every body matched its acquisition sidecar's
digest, size and HTTP 200 status. No source was downloaded for this preservation.

Archive integrity and member naming do not prove archive source authenticity or
freshness; qualify their acquisition provenance before production use. The body
sample does have retained acquisition evidence, but remains a 40-body selection.
The measured D1 run's eight BILLSTATUS ZIPs were streamed rather than retained;
its archive-listing table is metadata, not another set of source bytes.

## Additional FEC originals and prepared sources

| Holding under `R` | Local size/scope | Next use |
| --- | --- | --- |
| `fec-cycle-2024-2026-09-12/tables/` | 15 prepared files; **8,915,803 bytes / 120,280 rows**. | Much is already represented in the sealed selection. Adopt only missing populations. |
| `fec-cycle-2026-2026-09-12/tables/` | 12 prepared files; **8,661,529 bytes / 101,432 rows**. | Masters, links, candidate/committee summaries and leadership remain available for builder adoption. Field audits are retained beside the tables. |
| `data-validation-sprint-2026-09-21/public/fec_committees.parquet` | Older wider public table: **89,643 rows; 2,680,520 bytes; 16 columns**. | Retain for population reconciliation. The independently source-qualified 27,311-row census above has a narrower explicit selection; it does not replace or prove this wider population. |
| `fec-bulk-expansion-2026-09-21/blobs/sha256/` | Five complete financial ZIP originals; **2,477,585,475 bytes**. | `indiv26.zip` alone is 2,196,803,354 bytes and has **32,034,987 main-member rows still awaiting expansion**. `oth26`, `oppexp26`, insert and delete streams already contribute selected rows to the sealed generation. |
| `fec-senate-discovery-2026-09-21/` | 598 live URL observations, **597 distinct stored originals / 122,507,816 bytes**. | Already adopted in the selected seed; one physical-line fallback remains explicit. The separate mirror is excluded. |
| `fec-postgres-inventory-2026-09-21/` | **14,190,177-byte committee-history dump**, plus README; assessed **262,276 rows / 73 fields**, cycles 1976–2022. | Reader and table adoption still needed; current seed describes the file inventory. |
| `fec-research-integration-2026-09-14/inaugural/` | 51 originals; **211,723,750 unique stored bytes**. | Reuse qualified retained originals; oversized PDFs and missing raw URLs remain recorded gaps. |
| Sibling `enforcement/` | 23 acquired URLs; 21 distinct originals; **203,444,543 bytes**. | Identity conflicts and wider legal/body adoption remain. |
| Sibling `agency/` | 13 acquired URLs; 11 distinct originals; **9,455,299 bytes**. | Selected FOIA/Oversight qualification exists; agency/OIG collections have not entered the sealed FEC tables. |

Also retain `fec-source-expansion-2026-09-13/bulk/`,
`fec-filings-2026-09-12/` and `fec-next-collections-2026-09-14/historical/`
for additional selected periods and filing originals.

The separate research repository at
`/Users/mikewolfd/Documents/Codex/fec-data-research-2026-09-11/` has 16,707
physical files and 1,535,832,195 apparent bytes excluding Git/bytecode/OS
metadata. Its 47,915 bulk-object and 134,739 legal-object inventory entries are
discovery records, not that many downloaded files. Some captures are prefixes or
failed responses. Do not upload that directory wholesale.

## Files that must not become full-corpus replacements

- The retained public `comments.parquet` is a **212,733-row sample** from a
  23,889,661-row published object. It cannot supply the full comments mirror.
  No full local comments corpus was found in the searched locations. The large
  catalog/Iceberg import contains documents and dockets; its catalog policy has
  `commentInput: null`. Older 50,000-row comments experiments do not fill the gap.
- The corrected regulatory cohort has **392 dockets, 547 documents and three
  comments**. Its 10,969 passing raw/output comparisons prove the repair path,
  not a full base dataset.
- The corrected court-body replay has **284 rows**; the corrected report-section
  replay has **11 rows**. Rebuild the wider selected populations with those fixes.
- The older CFR table contains the demonstrated part-ancestry defect. Older
  report sections have the defective representation, and older bill-family
  outputs precede the cosponsor repair; their three model tables are empty.
- Retained SAM, court dockets and USAspending tables lack paired source proof for
  the producing generation. Nonzero rows alone do not resolve that limitation.
- Search indexes, DocSpec working stores, preserved worktrees and catalog exports
  often duplicate these sources. Their combined disk size is not new coverage.
- `~/.claude/jobs/fcb997e0/tmp/` contains inventory JSONs pointing to receipt
  files, not additional Parquet data.

## Reuse sequence

1. Publish the two existing audited FEC generations and verify remote pins,
   downloads and MCP queries. Seal the schema-compatible committee seed.
2. Select compatible retained base parents; rebuild source-field repairs from
   pinned regulatory releases while preserving newer parent observations.
3. Admit scoped prepared independent families. Rebuild known defective outputs
   from retained raw sources; reconcile complete bill-family ownership and scope.
4. Generate dependent rollups from those exact parent versions.
5. Acquire only missing populations or required freshness deltas, then record
   the remaining gaps against the complete fork output inventory.

Uploading source releases for runner access and publishing queryable rollups are
separate steps. Transfer selected manifests and their referenced bytes with
provenance; do not turn an evidence-directory copy into an active dataset.

[inventory]: /Users/mikewolfd/Work/corpora/fork-rollup-generation-2026-09-21
