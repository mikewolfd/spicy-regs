# FEC data delivery to R2

Updated October 1, 2026. **Deliver useful retained non-PDF FEC data as
source-faithful, queryable Parquet tables in the `spicy-regs` R2 bucket.**
Keep the exact source evidence and enough metadata to explain each table.

The selected retained release is already published. Its
[publication checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01)
records complete remote byte verification and public table queries. The
[reviewed release](research/fec-reviewed-release-2026-10-01.md) records the
matching consumer and successful hosted queries.

This is the active checklist. The
[earlier roadmap](research/fec-delivery-roadmap-2026-10-01.md) preserves the full
historical acquisition plan and original FR/FH task IDs. The
[gap register](fec-gaps.md) preserves source limits and optional work; those
items become delivery tasks only when they affect the selected data's accuracy,
usability or availability on R2.

## Useful data and its shape

| User need | Published data to keep | Shape |
| --- | --- | --- |
| Identify candidates and committees over time | Candidate history, committee observations and master/history records | Source identity and cycle or snapshot, with reported attributes |
| Inspect reported money | Receipts, disbursements, intercommittee transactions, independent expenditures, loans, debts and other supported schedules | One reported record version with named amount/date fields, parties, filing links and interpretation flags |
| Understand filings | Filings, report observations, registration statements, filing links and definitions | Submitted versions and source identifiers remain distinct; supported links retain their evidence |
| Explore legal activity | Matters, parties, events, document metadata and audit findings | Case-type-qualified identifiers, reported roles, dates and document associations |
| Read agency activity | Agency reports, metrics, text, document metadata and oversight recommendations | Separate reports, dated measures and narrative observations, with source units and periods |
| Understand coverage and verify a result | Source catalog, collections, selection/mapping dispositions, source records and evidence links | Explicit scope and missing-data states; exact links to retained originals and definitions |

The published index and table dictionaries own the actual table membership and
column schemas. The [data model](fec-data-model.md) explains their meaning and
also retains proposals for later work.

Keep these rules:

- Use the existing logical tables and qualified partitions. Preserve stable
  record identities and the declared meaning of each row.
- Keep identifiers as strings. Use exact decimal amounts and source-specific
  dates where mappings are qualified. Preserve raw values and reasons when a
  value is missing, unsupported or ambiguous.
- Keep filing, amendment, memo, correction and interpretation flags needed to
  read a financial row correctly. Distinguish source observations from unique
  economic events; current/net totals remain unqualified.
- Keep shared definitions, full source records and detailed evidence in their
  existing tables or views. Reuse shared evidence rather than copying full
  native JSON into every subject table.
- Retain source-limited records with explicit dispositions. An unresolved
  field or missing body does not justify discarding the supported data.

The existing layout satisfies this release. Rewriting IDs, moving every evidence
column or redesigning storage requires a measured benefit and is outside this
checklist.

## R2 contents

| Content | Published location |
| --- | --- |
| Current family and table membership | [publication.v2.json](https://data.spicygov.ai/publication.v2.json) |
| Typed subject tables and their manifests | `generations/fec-query/<generation-digest>/` |
| Complete source observations, collections and reported relationships | `generations/fec-observations/<generation-digest>/` |
| Candidate history and source catalog | Their existing `fec-candidate-history` and `fec-source-catalog` generation paths |
| Selected original bytes and supporting evidence | `source-evidence/blobs/sha256/<digest>` and artifact manifests under `source-evidence/<artifact-digest>/` |

Use the captured publication index to select compatible generations and resolve
the exact object keys. Follow the existing
[publication procedure](generation-publication.md) for changes.

## Delivery checklist

| Task | Completion rule | Current status |
| --- | --- | --- |
| Account for retained inputs | Every selected non-PDF input contributes supported records or an explicit disposition | Complete for the selected retained corpus |
| Validate useful tables | Check source fields, types, row identity, joins, interpretation flags and evidence links | Complete at the recorded retained scope |
| Publish tables and recovery evidence | Upload immutable generations and selected originals; verify complete remote bytes before advancing the index | Complete |
| Read the delivered data from R2 | Verify generation pins, schemas, row counts and representative data against the accepted release | Complete; fresh public check passed |
| Leave a usable handoff | Record the index, table meanings, evidence locations and known limits | This page and the linked release receipts |

Completed work maps to FR01–FR10, FR12 and the positive public-read portion of
FR13. The current release's existing reviews, pins and deployment remain valid.
No additional upload or schema rewrite is identified by the accepted release.

## Next execution checkpoint

**The selected retained R2 delivery is complete.** The fresh public check passed
for every table in the selected families: generation pins and control bytes
match, schemas and footer counts agree, and representative data reads succeed.
No additional data upload or table-shape repair was identified.

Fresh check evidence:
[public-check.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/r2-delivery-scope-check-20261002T010948Z/public-check.json).
This read-only check completed October 1, 2026 at 9:10 p.m. Eastern
(October 2 at 01:10 UTC). It reused the publication verifier for all selected table
schemas, footer counts and sample data; full object hashes remain established
by the earlier publication receipts.

Resume delivery work when a concrete missing object, schema mismatch, broken
evidence link or useful retained mapping needs repair.

## Separate follow-up work

The following work stays outside the active R2 checklist:

- Historical acquisition and additional source families; PDFs remain deferred.
- Current/net financial totals, universal amendment handling and further
  relationship inference.
- Search indexing, legislative joins, scorecards and adjacent-source work.
- Recurring refresh, live deployment mutation/rollback drills and container
  lifecycle investigation.
- Storage optimization and local source cleanup.

If reclaiming disk space resumes, FR11 full restore/replay and FR14's exact
path/hash/remote-object/ownership checks still precede FR15 deletion. Preserve
shared or in-use inputs, deferred PDFs and the mixed `eFilingFormats.zip`
parent. Record removed paths and actual free-space change under FR16.
These checks protect local deletion; they are separate from R2 delivery.
