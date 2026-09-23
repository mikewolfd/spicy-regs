# Fork delivery: consolidated tasks and execution order

The delivery target is every existing rollup and its declared outputs generated
and verified for `mikewolfd/spicy-regs`, using the fork's own data destination.
FEC is one part of that delivery. A local sample, an upstream-hosted file, an
enabled schedule or a successful workflow is not completion evidence by itself.

This is the single execution backlog for fork generation and local data reuse.
It owns the order of work and what each task requires; each fact has one home
(decisions record, decision 16). The [output ledger](research/fork-output-ledger-2026-09-21.md)
owns every output's status, pins and verification, the
[execution log](research/fork-execution-log-2026-09-23.md) the dated narrative,
and the [decisions record](research/fork-delivery-decisions-2026-09-22.md) the
decisions. The [dated local inventory](research/local-data-reuse-2026-09-21.md) supplies
exact paths, sizes, artifact pins and qualification limits; it is supporting
evidence, not a second task list. Source-specific defects remain in the
[cross-repository gap register][master] and [FEC gap register](fec-gaps.md).

Reuse verified local outputs first, rebuild defective outputs from retained
sources, and acquire missing populations or freshness updates. Complete each
family's source, build and publication checks before starting its dependents.
An unrelated credential or source problem must not block ready families.

The original workflow/producer inventory was reconciled at `c418dc3`. The
execution update below supersedes its September 21, 22:28 UTC baseline.

## Execution update

The September 21 fork campaign is under way. Each line gives a task's current
state; the ledger holds its pins and evidence, and the execution log its history.

- **T01:** complete; SAM failures now refuse publication, and its initial load
  is T14. See the ledger.
- **T03/T18:** retained-input and catalog workflows, bounded transfer, invocation
  receipts and build-only artifacts are implemented, and real GitHub build-only
  runs passed for the catalog and a two-record retained fixture
  ([retained workflows](fec-retained-workflows.md)); snapshot-based freshness
  checks and SAM dispatch controls are in place. Per-family transfers and the
  operational gaps in the execution order remain. See the ledger.
- **T04:** complete for the selected seed: all five FEC tables, from the retained
  2024/2026 committee traversal, not all FEC history (T05). See the ledger and
  the [FEC gap register](fec-gaps.md).
- **T06:** dockets and documents delivered as a metadata repair; document bodies
  and text extraction remain open. See the ledger.
- **T07:** published for every comment-bearing agency, six plus ACF repaired from
  native source (decision 7); the other agencies, the dated partition tree, the
  agency mirror and bodies remain. See the ledger.
- **T08:** every selected family is source-qualified at its scope; wider history
  is open, and members/terms remain a community crosswalk, not official-roster
  completeness. See the checkpoint below and the ledger.
- **T08/T16 press:** the reviewed bounded link correction is published; newer
  scheduled capture metadata is not re-qualified. See the ledger.
- **T09:** the reconciled family is published (decision 1); unfinished bodies and
  XML pairs are retried within a per-run fetch budget (`MAX_VERSION_FETCHES`).
  Remaining work is in the checkpoint below; see the ledger.
- **T10:** complete for the selected packages; only the inherited hearing rows'
  capture times and read checkpoints are unqualified. See the ledger.
- **T11:** Federal Register and the retained Agenda edition (still reginfo's
  newest) are qualified. CFR part ancestry is wrong in titles 41, 43 and 14 vol 4;
  the fix (plan A8) reads each section's `PART` heading through SpicyDocs'
  annual scan, and the workflow stays off until it lands. See the ledger.
- **T12:** CRS, FCC, GAO, USAspending and court dockets are published and
  qualified at their selected scopes. Still required: raw-response retention in
  the ordinary scheduled readers for replayable audits, FCC crowded-single-day
  recovery, identity-set pooling (plan B6), court parties (decision 9) and
  catch-up, a scope decision on the APA selection's non-civil dockets, and wider
  populations. See the ledger.
- **T13:** clusters, citation tables and the text-free opinion index are
  published for the 2026-06-30 edition; bodies are withdrawn (decision 6). Bulk
  inputs advance only through `courtlistener-bulk/verified-manifest.json`
  (full-file SHA-256 plus exact source ETag; `ACQUISITION-GATE.md`). The space
  preflight checks the destination before parsing but reserves nothing for
  merges, publication copies, audit spill or concurrent work
  (`courtlistener-clusters-qualification/headroom-fix/`). See the ledger.
- **T15:** every summary, link table and docket search is delivered;
  `rulemaking_lifecycles` is withdrawn (decision 4). Docket search omits the 18
  dockets with neither title nor abstract; the other limits are in each table's
  dictionary entry. See the ledger.
- **T19:** dictionary/MCP metadata and fork coverage descriptions are in place
  for the delivered tables; representative cross-source joins remain. See the
  ledger.

### Current parallel work

*Superseded.* The September 22 checkpoint this section held (status snapshot,
index rechecks, workstreams, court bulk transfer and remote-output notes) is in
the ledger and the [execution log](research/fork-execution-log-2026-09-23.md).

### Congressional delivery checkpoint

*Updated 2026-09-23.* Status, pins and verification for each family are in the
ledger; scheduled runs publish new generations before audit (decision 3).

| Family | Remaining work |
| --- | --- |
| Members and terms | Official-roster reconciliation and finer within-term party history remain separate limits. |
| Nominations, treaties, reports/hearings and press | Qualify the inherited hearing rows' capture times and read checkpoints; audit the live scheduled nominations, treaties and press generations (decision 3). Broader history, detail and granule coverage remain open. |
| Laws, committee rosters and amendments | None beyond documented limits: seats without a publisher link stay unlisted (decision 5), and Rules Committee amendments have no member sponsor. |
| Bills, text and differences | Native-field qualification beyond the audited 118th HR/S cohort, uncaptured bodies, the four model tables (no `GEMINI_API_KEY` on the fork) and both backfill tables. |
| Votes | Portable public source evidence, full history and dynamic scorecards; the `bill_vote_references.date` day rule (plan A11). |
| Communications, meetings, record issues, print citations and Senate expenditures | Audit the newer scheduled generations (decision 3); the recorded post-publication drift stays explicit (`scheduled-published-qualification/`). |

[Execution receipts](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21)
retain withdrawals, generation pins, public downloads, MCP responses, byte
checks and independent reviews. The campaign remains open for the unfinished
tasks below. Storage success is separate from hosted MCP, custom-domain, catalog
and documentation deployment.

**Full host gate:** the installed SpicyDocs 0.26.3 adoption passed 2,426 host
tests with Ruff, types and dictionary validation, and the isolated source gate
7,330 (`native-vote-variants-adoption/`, `reviews/votes-0263-adoption-review.md`).
Earlier: 2,326 tests at `5028ed5`; dictionary checks and 108 focused tests at
`dd73a7d` with green [CI](https://github.com/mikewolfd/spicy-regs/actions/runs/35671496725);
source CI repairs `51c87a1` and `b0a8e6e` passed
([main](https://github.com/mikewolfd/spicy-docs/actions/runs/35679643578),
[branch](https://github.com/mikewolfd/spicy-docs/actions/runs/35679652261)); the
0.26.2 source adoption is in `congress-vote-press-adoption/`. Gates establish
implementation behavior, not completion of the open datasets.

## Baseline before execution

The checkout has 40 `run-rollup-*` commands and `build-fec-observations`, served
by 39 thin rollup workflows. All declared dictionary/MCP tables have producers.
At that observation, the catalog and retained-observation FEC commands had no workflow. Base
regulatory ingestion, comments publication and rulemaking materialization have
separate workflows and must be included in the delivery plan.

The initial complete bucket listing contained seven objects and no root base
tables. Its publication index selected only `sam-entities`. Subsequent scheduled
runs changed that index; do not treat the earlier listing as current coverage.

| Family selected at 22:28 UTC | Rows reported in the public index | Disposition |
| --- | ---: | --- |
| `federal-register` | 1,008,903 | Published by a scheduled job; superseded and source-audited 2026-09-23 (see the ledger). |
| `cfr-sections` | 319,507 | Published by a scheduled job; T11 must check the known ancestry defect as well as source/output agreement. |
| `unified-agenda` | 3,954 | Published by a scheduled job; T11 must audit this generation. |
| `sam-entities` | 0 | Invalid **575-byte** publication; T01 must withdraw it and repair failure handling. |

The [SAM run](https://github.com/mikewolfd/spicy-regs/actions/runs/35643620562)
reported no API key, yielded no source records and published that empty result.
Downloaded bytes and the Parquet footer confirm the count. This is an invalid
claim of acquired empty data and does not count as a generated dataset. The
earlier invalid lobbying family is withdrawn; its diagnostic objects remain.
The three later publication counts above were read from the live index, not
established by fresh full-file or native-field audits. At that baseline there was no FEC family in
the observed index. Local inventory and temporary-file preservation were done;
the local reuse campaign had not yet executed. The execution update above
supersedes these historical delivery statuses.

`DATA_GOV_API_KEY` and `ZYTE_TOKEN` are now installed in the fork's GitHub Secrets.
The shared API key passed a one-record OpenFEC request. A complete committee run
has not been verified after installation. Zyte is not yet wired to a workflow
or caller adapter. At that baseline the fork had no `SAM_API_KEY`, `GEMINI_API_KEY` or R2 catalog
secrets. `SAM_API_KEY` and `LDA_API_KEY` were added on 2026-09-23 (decision 10);
`GEMINI_API_KEY` and `R2_CATALOG_*` are still absent. Missing optional rate
limit credentials are different from missing required access.

The workflow inventory recorded 38 active thin rollup workflows; lobbying alone
was disabled. Recent
run conclusions span older source revisions and storage settings. Do not use
those green checks as proof that the new destination contains their outputs.

Evidence: [fork inventory receipts](/Users/mikewolfd/Work/corpora/fork-rollup-generation-2026-09-21),
including `publication-before.json`, `bucket-inventory.json`, `workflows.json`,
`workflow-runs.json`, `secrets.json` (names/timestamps only) and
`sam-empty-publication-audit.json`. The later `status-recheck.json`,
`status-publication-response.json` and `status-workflow-runs.json` preserve the
22:28 UTC index and matching scheduled runs.

## Consolidated task queue

**Status:** Ready means work can start; Review means a retained or published
candidate needs checks; Waiting names unfinished prerequisites; Follow-on means
further retained-source coverage after the first usable release. None of these
means the task is complete. Apply prerequisites per family, rather than waiting
for every source in a grouped row.

**Owners:** SpicyDocs owns acquisition, native parsing and source evidence.
SpicyRegs owns table construction, interpretation, schemas and consumer behavior.
Operations owns source access, fork storage, workflow execution and publication.
Use the existing generation publisher and retained-source readers; this plan does
not introduce a second pipeline or a new cross-source schema.

Source failures must abort publication. SAM's failure is confirmed; the known
failure-to-empty/partial paths in CFR, CRS, FCC, USAspending, GAO and court dockets
need source-specific qualification in T11/T12. They are not all confirmed bad
publications. FEC, Federal Register and corrected LDA already have stronger
refusal paths. A successful response establishing a selected empty population
is the evidence for valid emptiness; a generic nonempty-file check is insufficient.

### First: establish scope and prevent invalid publication

| ID / status | Task and owner | Inputs / prerequisites | Complete when |
| --- | --- | --- | --- |
| **T01 · Complete; initial load remains T14** | **Contain and repair SAM failure publication.** Operations + source owner. | Current invalid SAM pin and retained failure evidence. | Pause its faulty refresh, conditionally withdraw only the invalid family while preserving evidence and other families, and make missing access/failed extraction abort before output. Prove the former failure cases refuse publication. Resume only after T14 qualifies a real selection. |
| **T02 · Ledger recorded; scope gaps explicit** | **Record each producer's selected population and outputs.** SpicyRegs + operations. | Producer map below, dated local inventory and current publication index. | Each output has its intended Congress/cycle/date scope, current candidate or source location, missing work, owner, parent pins and receipt location. Keep broad existing populations when applying smaller repairs. Retain the already explicit FEC selection; do not invent a new approval step or shrink other sources to samples. |
| **T03 · Runner path qualified; per-family transfers remain** | **Prepare retained inputs and successful build artifacts for runner use.** SpicyRegs + operations. | Exact inventory paths and manifests; existing generation/admission tools. | Transfer only selected manifests and referenced bytes with verified digests. Preserve original acquisition metadata and record later transfers separately. Keep successful build-only artifacts and audits; GitHub runs now verify that path. Document the supported invocation for each retained input. Ready sealed FEC artifacts can use the existing publisher immediately. |

### In parallel: publish, qualify or rebuild independent families

| ID / status | Task and owner | Reuse route / dependencies | Complete when |
| --- | --- | --- | --- |
| **T04 · Complete for selected seed** | **Publish the audited FEC seed.** SpicyRegs + operations. | T02; the two sealed families in the inventory (**1.17 GB**) and the schema-compatible **27,311-row** committee table. T03 only for bytes that need runner transfer. | Reuse the exact sealed observation/catalog artifacts, seal the committee selection, and verify publication pins, remote bytes, CLI downloads and MCP reads. Preserve the committee cycle filter and reported-relationship limits. No reacquisition is needed for this seed. |
| **T05 · Follow-on** | **Expand FEC from other retained sources.** SpicyDocs + SpicyRegs. | T02/T03; audited 2024/2026 tables, complete financial ZIPs, committee-history dump and retained filing/legal/agency captures. | Reconcile what is already in T04; adopt missing selected tables, expand the **32,034,987-row** individual base, and implement/adopt the assessed committee history. Track correction streams separately. Gate further inaugural, enforcement and agency adoption on their documented parser/identity limits. Wider history remains explicit; this does not block publishing T04. |
| **T06 · Dockets/documents delivered; comments and index published; partitions open** | **Build corrected regulatory base parents.** SpicyDocs + SpicyRegs. | T02/T03; retained public dockets/documents/comments index plus the **13.8 GB** source-release set. | Apply the qualified repair paths to the intended populations, preserve newer parent observations and unrelated rows, and verify the fork's base `dockets`, `documents`, `comments_index` and selected partitions. The 392/547/3 repair cohort is evidence for the fix, not the full replacement. Use the existing base publication path; these outputs are outside managed rollup families. |
| **T07 · Published for all 133 comment-bearing agencies; six plus ACF re-verified and repaired from native source (decision 7); the other ~126, partitions, mirror and bodies open** | **Establish the full selected comments representation.** SpicyDocs + SpicyRegs + operations. | T02; a full pinned 23,889,661-row public parent is now retained. Repair its demonstrated native-field omissions and qualify the complete selected source population. Catalog configuration is required if using the current Iceberg mirror workflow. | Retain and audit the complete chosen population, publish its index/partitions and the public comments representation needed by consumers, and verify counts agree at their declared grains. The 212,733-row sample cannot stand in for the 23,889,661-row source object. Missing full comments blocks its consumers, not independent sources. |
| **T08 · Every selected family source-qualified; wider history open** | **Admit prepared independent legislative families.** SpicyRegs. | T02/T03; measured members/terms, rosters, laws, amendments, meetings, nominations, record issues, treaties; newer communications/print outputs; corrected Senate expenditures. | Current schemas, complete family membership and raw/output audits pass for each selected scope; publish and read back each family. Keep current-only, House-only and unavailable-package limits visible. Prepare votes and press releases here if useful; finalize optional bill links in T16. |
| **T09 · Reconciled family published (decision 1); bodies, models and backfills open** | **Build the complete bill family.** SpicyDocs + SpicyRegs. | T02/T03; corrected five-table 118th HR/S cohort; receipt-pinned 118th ZIPs; preserved 119th ZIPs and 40 body files. Qualify temporary ZIP provenance/freshness. T08 laws supplies optional links. | Reconcile all 18 family members under the ordinary `bill-family` owner, preserve intended coverage outside the repaired cohort, and acquire missing types/periods/bodies. Resolve the 600-body cap and unchanged-input skips so missing work is retried. Verify access for older backfills and models; keep uncomputed model outputs blocked, not falsely complete. Do not publish `qualified-bill-status-118-hr-s` as a competing owner. |
| **T10 · Complete for the selected 118-package family** | **Rebuild the report family with corrected sections.** SpicyDocs + SpicyRegs. | T02/T03; retained report/hearing inputs and the corrected section replay; acquire missing selected inputs as needed. | Rebuild and qualify reports, sections, hearings, bill links and read-status outputs together. The 11-section correction sample and the old defective family cannot substitute for the intended corpus. |
| **T11 · FR, Agenda and CFR parts qualified (A8 published)** | **Audit current Federal Register, CFR and Agenda generations.** SpicyDocs + SpicyRegs. | T02; current scheduled publication pins, retained public/corrected FR parents and source releases. | Verify actual remote bytes and source/output agreement. Qualify or repair CFR's failure-to-empty/partial paths before further unattended acquisition; failed sources must preserve prior valid data. Reuse passing current generations. Repair CFR's demonstrated part-ancestry mapping and recover required source fields before claiming qualified coverage. Do not replace the newer FR population wholesale with the older 803,997-row repair parent. Regenerate only affected representations or missing scope. |
| **T12 · CRS/FCC/USAspending/GAO/court dockets published and qualified; parties and wider populations open** | **Qualify FCC, CRS, GAO, USAspending and CourtListener docket families.** SpicyDocs + SpicyRegs. | T02/T03; retained public files and source-specific evidence. Court bulk selection, mapping and integrity receipts are linked above; the count fix is adopted and verified through the installed package. | Qualify source failure paths and complete selected populations. For courts, verify bulk bytes, map native fields and source/court selection, preserve prior observations, and acquire missing participant relationships separately. Resolve each family's field/grain defects before audited publication. A nonzero retained table is not source-completeness evidence. |
| **T13 · Clusters, citations and opinion index delivered; bodies withdrawn (decision 6)** | **Maintain qualified cluster, citation and opinion-index metadata.** SpicyDocs + SpicyRegs. | T02/T03; published qualified June 30 clusters and docket map, the verified opinions input. | Opinion text links out through each cluster's `absolute_url`; no bodies are built or hosted (decision 6). Audit joins against the same dated inputs and preserve the broader retained population. Treat newer catch-up and older corrections as separately qualified work. |
| **T14 · Bounded SAM and lobbying loads published (decision 10); wider years and schedules open** | **Complete SAM and lobbying initial loads.** SpicyDocs + operations + SpicyRegs. | T01 for SAM; T02/T03; retained nonempty tables as comparison candidates, not source proof. Verify SAM-specific authorization; use the repaired filtered LDA reader. | Explicit bounded scopes, rate budgets and resumable checkpoints produce source-proven outputs. Wire SAM year/mode/record controls into dispatch. Retain source codes, dates and field meanings. Qualify and publish each family before resuming its schedule; the existing lobbying pause remains until its initial load succeeds. |

### Then: generate dependent families from verified parents

| ID / status | Task and owner | Hard prerequisites | Complete when |
| --- | --- | --- | --- |
| **T15 · All summaries, links and search delivered; lifecycles withdrawn (decision 4)** | **Generate regulatory summaries, links and search.** SpicyRegs. | See exact producer map below: T06 base tables; T11 FR; T04 committees plus T07 public comments for organization links. | Build each eligible rollup from recorded parent pins; verify key coverage, join fan-out, source grain and date semantics. Publish the legacy `docket_search.json.gz` through its separate path. Do not carry old derivatives forward as though rebuilt against new parents. |
| **T16 · Bills, subjects and vote links delivered** | **Finish bill refresh, subjects, vote and press links.** SpicyRegs. | `congress-bills` requires T09's complete published family; `bill-subjects` requires its selected bills. Bill links for votes and press are optional dependencies. | The narrow writer preserves all sibling members and their pins; subjects reconcile the retained 20,013-row enrichment with the selected bills. Votes and press retain their source populations, distinguish missing from verified bill links, and pass join audits. Optional linking must not block their own-source acquisition. |
| **T17 · Bootstrapped `snapshot_0e799850…` against the five qualified parents; joins under-resolved (A7)** | **Materialize rulemaking relationships.** SpicyRegs + operations. | T06 dockets/documents; T11 FR/Agenda; T15 FR-docket links. | Bootstrap deliberately with `allow_bootstrap=true`, produce all five outputs, and verify references, counts and public reads against the exact parent generations. |

### Finish: consumer access and repeatable refresh

| ID / status | Task and owner | Dependencies | Complete when |
| --- | --- | --- | --- |
| **T18 · Wiring implemented; operational gaps remain** | **Complete workflow, access and monitoring wiring.** SpicyRegs + operations. | T03 retention; per-source access checks; published parents for live checks. | Add FEC catalog and explicit retained-observation workflow inputs; retain pinned input manifests and source failures. Forward Zyte only to an actually selected adapter and validate it there. Configure required catalog access if choosing Iceberg paths. Make parent completion the barrier, and make freshness checks resolve `publication.json` rather than bare table URLs. Reconcile body/model and bounded-backfill controls with their source tasks. |
| **T19 · Metadata implemented; wider data proof pending** | **Expose accurate catalog/MCP metadata for the fork.** SpicyRegs. | T02 output ledger and selected published families. | Add missing dictionary descriptions for `bill_subjects` and `court_opinion_clusters` (`court_opinion_bodies` is withdrawn); verify actual availability, columns, stable identifiers, join meanings, coverage and evidence links. Point the tested consumer at the fork, not an upstream default. Exercise representative cross-source joins without treating inferred links or money flows as source facts. |
| **T20 · Waiting on all intended outputs** | **Close the campaign and qualify refresh.** All owners. | T02 ledger and the completed family-specific tasks; T18/T19. | Every intended output has the completion evidence below or an explicit unresolved blocker. Verify public downloads and MCP reads, then repeat selected refresh/correction/failure scenarios without losing prior valid data. Keep the campaign open for missing intended outputs. Track catalog, custom domain, Worker and Pages deployment separately; storage/data success is not deployment success. |

### Immediate execution order

*As of 2026-09-23 the original steps below are largely done; see the
[execution log](research/fork-execution-log-2026-09-23.md). What remains, in order. Items A5–A12, B6 onward, C4 and D6 are in
SpicyDocs' consolidation plan (`spicy-docs/docs/research/consolidation-path-2026-09-22.md`),
with their evidence in `spicy-docs/docs/research/parsing-survey-2026-09-23.md`:*
- **Workflows.** The seven held for the 2026-09-23 push were re-enabled that day
  (decision 22). `cfr_sections` stays off until the CFR ancestry fix, and
  `rulemaking_lifecycles` by decision 4.
- **Next T07 cohorts.** Re-verify and repair the other ~126 agencies from native
  source, agency by agency. The table already holds their rows from the
  retained parent. Then build the dated partition tree and the
  `comments/agency/` mirror; the mirror needs `R2_CATALOG_*` secrets.
- **Parsing and metadata consolidation.** The quiet bugs A6–A12, the moves
  from B6 onward, C4 and D6, with the plan's rulings 5–9 (title 43 `cfr_ref`,
  rulemaking join changes, comment text provenance, the catalog file,
  search-field normalization).
- **Wider source populations.** SAM years, lobbying history (the key is now
  set), FCC filings history, USAspending beyond the top 10,000, court catch-up
  and parties (decision 9).
- **Bill family (T09).** Bodies, models (needs `GEMINI_API_KEY`) and backfills.
- **Operations and closure.** T18/T19 wiring (Pages is not enabled; the
  dictionary deploy fails), then T20.

The original order, kept for the record:

1. Continue **T07** native comments repair and index/partition reconciliation
   to release the remaining comment-dependent summaries and organization links.
   Keep **T15** lifecycle inference separate until its pairing and unknown-docket
   rules are qualified; its document parent is already delivered.
2. In parallel, continue **T09** complete bill-family qualification and the unfinished
   **T08/T11–T14** source-specific work. Each family keeps its own scope and
   access requirements; SAM, models and catalog access do not block independent
   families. The ledger and master gap register name their remaining defects.
3. Release remaining **T15–T17** producers as their own parents qualify. Finalize
   optional bill links after the chosen complete bill generation exists. Keep
   **T18/T19** workflow and consumer metadata consistent with actual delivery.
4. Close **T20** only across the whole intended output set. Carry **T05** broader
   retained FEC coverage as an explicit follow-on, and keep hosting/deployment
   separate from successful data publication.

Parallelize independent acquisition, local builds and audits within source rate
limits. Serialize publication-index updates where practical. A concurrency refusal
requires a fresh index and compatible parent pins before retry; unchanged local
artifacts are reusable only when the current publisher's checks accept them.
Use the [generation publication rules](generation-publication.md), including the
distinction between offline candidates, managed families and legacy/base paths.

### Catalog performance: partition the comments table by agency

The weekly dedupe rebuilds the 23.9M-row comments catalog with ~750 full-table
statements (per-agency hash-bucket INSERTs to bound each dedup window's memory,
then per-agency swap INSERTs), and the nightly sweep rebuilds the index with a
full GROUP BY per batch. The restructure, verified step first:

1. Run `uv run python scripts/probe_iceberg_partition.py` with the
   `R2_CATALOG_*` credentials — it creates a scratch partitioned table on the
   catalog, reads it back through an agency predicate, and drops it. Only a
   PASS authorizes the next steps (plain DuckDB does not reproduce catalog
   behavior; see `sources/iceberg.py` PR #117 note).
2. On PASS: create the `comments` and `comments_dedup` tables with
   `PARTITION BY ("agency_code")` (all three creation sites in
   `iceberg.dedupe_table`), so every per-agency statement — the bucket build,
   the swap, `upsert_comment_text`, the seed resume counts and the
   backfill candidates — prunes to that agency's files instead of scanning
   the table.
3. Replace the swap's per-agency INSERT loop with one `INSERT INTO ... SELECT`
   from the deduped sibling (a plain copy, no dedup window, so the bucket
   memory bound does not apply).
4. Leave the hash-bucket dedup build as-is (its window bound is the measured
   reason it exists) and the per-batch index rebuild as-is (with catalog
   DELETE unreliable, the index must keep deriving from the table, not from
   staging deltas).

The weekly dedupe then costs a few full scans plus pruned per-agency writes
instead of ~750 full scans; the nightly per-agency paths prune for free.

## Completion rule

Track every producer and every output with the selected source population,
source revision, input pins, code revision, run, output counts, publication pin
and public read result. Each output needs one explicit disposition:

- **Generated and verified:** successful acquisition/build, source/output audit,
  complete family publication and a verified public read.
- **Valid empty selection:** a successful source response establishes zero for
  the declared selection. Explain why; do not equate it with full-source absence.
- **Unproduced or blocked:** missing input, credential, reader, computation or
  unsuccessful source acquisition. Name the blocker and next action.

The full campaign stays open while an intended output remains blocked or
unproduced. Optional model tables left empty are not completed model work.
“All rollups” describes the producer/output set; historical completeness remains
a separate, explicit scope for each source. Use the existing intended source
populations, rather than substituting a tiny demonstration to mark a row done.

For each family, compare native records and output fields, manually inspect
representative raw/output pairs, verify parent keys and count/fan-out changes,
then verify remote bytes and queries. Preserve the acquisition and publication
receipts even when a run fails. The [cross-repository gap register][master]
owns the detailed source and interpretation limitations.

Perform these audits during each task, before publication and after remote
delivery. Include ordinary rows, missing/unknown values, corrections, duplicates
and source failures where applicable. Record source URL/digest/locator, the
corresponding output key and values, the verdict and any unresolved difference.
Reuse prior passing evidence only for identical inputs and transformations;
changed source bytes, mappings or selected populations require the affected
comparisons again. File presence, Parquet schema agreement and successful tests
do not replace these manual source/output checks.

## Producer and dependency coverage

The following accounts for every `run-rollup-*` command. Names omit that prefix;
outputs normally replace command hyphens with underscores. Multi-output families
are expanded below. Parallelize independent acquisitions within publisher quotas;
wait for verified parents before starting dependents.

| Task | Producers | Required inputs and execution conditions |
| --- | --- | --- |
| T06/T07 | `run-pipeline` | Produces base `dockets`, `documents`, `comments_index` and selected comments partitions/catalog. Manual non-Iceberg runs are supported; scheduled runs force Iceberg and need catalog configuration. |
| T07 | `publish-comments-mirror.yml` | Reads the populated Iceberg catalog and writes public `comments`/agency partitions. `org-committee-links` needs this public representation. |
| T04/T18 | `fec-committees`, `fec-source-catalog` | Reuse the qualified local seed/catalog first. Fresh committee traversal needs the configured API key and complete acquisition evidence; catalog refresh uses the pinned provider inventory. |
| T04/T05/T18 | `build-fec-observations` | Reuse sealed artifacts first. New builds consume an explicit manifest and exact retained releases/captures/dictionaries; this command does not acquire source data. |
| T08 | `members`, `laws`, `committee-rosters`, `house-communications`, `committee-meetings`, `record-issues`, `treaties`, `nominations`, `amendments`, `print-citations`, `senate-expenditures` | Independent source selections; reuse measured/adopted/corrected candidates from the inventory where qualified. Relevant fresh API readers receive `DATA_GOV_API_KEY`. |
| T09 | `bill-family` | Complete family acquisition/build. `laws` enriches law links but is optional. Bodies/older API backfills and models have separate access and processing limits. |
| T10 | `committee-reports` | Rebuild the complete report/hearing family with corrected sections and source-specific qualification. |
| T11 | `federal-register`, `cfr-sections`, `unified-agenda` | Audit the current scheduled generations first; reuse passing data and repair or rebuild the affected outputs. |
| T12 | `fcc-proceedings`, `fcc-filings`, `crs-reports`, `gao-reports`, `usaspending-recipients`, `courtlistener` | Own-source inputs and success checks; `courtlistener` produces `court_dockets`. Its dedicated token is optional. |
| T13 | `court-opinion-clusters`, `court-citations`, `court-opinions` | Native source qualification; retain cluster/scope/join inputs. Opinion bodies are withdrawn (decision 6). The citation tables and the text-free opinion index copy one whole export each; `court-opinions` runs only where the 54.6 GB original is retained. |
| T01/T14 | `sam-entities`, `lobbying-filings` | Source refusal repair/access and bounded resumable initial acquisition. SAM defaults to one rotating registration-year window; LDA schedule remains paused. |
| T15 | `feed-summary`, `agency-stats` | Require `dockets`, `documents`, `comments_index`. |
| T15 | `agency-monthly-volume`, `discovery-signals`, `lifecycles` | Require `documents`; `lifecycles` produces `rulemaking_lifecycles`. |
| T15 | `docket-search` | Requires `dockets`; produces legacy `docket_search.json.gz` outside managed Parquet-family publication. |
| T15 | `fr-docket-links` | Requires `federal_register`. |
| T15 | `org-committee-links` | Requires `fec_committees` and public `comments.parquet`; the transform's comments dependency is not declared in the rollup class. Make it explicit in the execution barrier. |
| T16 | `congress-bills` | Publishing requires an existing complete `bill-family`; carries sibling members forward unchanged. A local partial build does not satisfy publication. `laws` links are optional. |
| T08/T16 | `press-releases`, `roll-call-votes` | Own-source acquisition can stand alone. Optional enrichment uses `congress_bills` or `bill_vote_references`; missing links stay explicit. |
| T16 | `bill-subjects` | Requires selected `congress_bills`, then fetches missing per-bill subjects. |
| T17 | `materialize-rulemaking` | Requires `dockets`, `documents`, `federal_register`, `unified_agenda`, `fr_docket_links`; first publication requires `allow_bootstrap=true`. It does not depend on FEC or full comments. |

Catalog seed scripts migrate existing docket/comment snapshots. They do not
bootstrap an empty bucket. If the campaign starts with the supported Parquet
base path, complete and verify the comments representation needed by consumers
before declaring those dependents ready. Do not require unrelated Iceberg work
for a rollup that only needs verified Parquet inputs.
The current comments mirror also refuses fewer than **1,000,000 rows**
(`scripts/publish_comments_mirror.py:MIN_EXPECTED_ROWS`). A bounded catalog test
does not qualify the full mirror; preserve this protection while resolving T07.

### Complete multi-output families

| Producer | Outputs that must be accounted for together |
| --- | --- |
| `bill-family` | `congress_bills`, `bill_actions`, `bill_committees`, `bill_publisher_summaries`, `bill_versions`, `bill_sections`, `section_diffs`, `section_diff_items`, `financial_changes`, `section_classifications`, `bill_summaries`, `diff_summaries`, `cbo_cost_estimates`, `public_activity_events`, `bill_family_archives`, `bill_vote_references`, `bill_family_backfills`, `bill_family_backfill_walks` |
| `members` | `members`, `member_terms` |
| `roll-call-votes` | `roll_call_votes`, `member_votes` |
| `committee-reports` | `committee_reports`, `report_sections`, `hearing_transcripts`, `hearing_bill_links`, `committee_report_reads` |
| `print-citations` | `house_activity_reports`, `budget_volumes`, `bill_committee_actions`, `document_citations` |
| `laws` | `laws`, `law_code_sections`, `table3_records` |
| `committee-rosters` | `committees`, `committee_assignments` |
| `build-fec-observations` | `fec_source_records`, `fec_collections`, `fec_relationships` |
| `materialize-rulemaking` | `rule_targets`, `proceedings`, `regulatory_agenda_items`, `agenda_item_proceedings`, `comment_periods` |

`bill_subjects` and `court_opinion_clusters` have dictionary descriptions,
current schemas and MCP declarations as well as executable producers;
`court_opinion_bodies` was withdrawn in favour of linking out (decision 6) and
its registration deleted. Their publication state is in the ledger.

## Related plans and evidence

- [Fork configuration](https://github.com/mikewolfd/spicy-regs/blob/a4930a4/deploy/fork-setup.md)
  owns account, storage, catalog and hosting setup.
- [FEC gap register](fec-gaps.md) owns FEC-specific coverage and interpretation;
  its public delivery is one family group within this campaign.
- [Local data reuse](research/local-data-reuse-2026-09-21.md) identifies sealed
  publication candidates, retained source inputs and outputs requiring rebuild.
- [Cross-repository remaining work][master] owns the known source, transformation,
  reference and publication defects that generation must account for.
- [Inventory receipts](/Users/mikewolfd/Work/corpora/fork-rollup-generation-2026-09-21)
  preserve the exact live baseline; these are local evidence links, not public
  acquisition manifests.

[master]: /Users/mikewolfd/Work/spicy-stack/spicy-docs/docs/research/remaining-gaps-2026-09-21.md
