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

Updated September 25, 2026, through local mirror publication, public/catalog
readback and the [parallel source/output audits](research/parallel-rollup-audit-2026-09-25.md).
The campaign remains open. The ledger holds exact qualified pins and partial or
failed dispositions; the execution log records the dated work and receipts.

- **September 25 repairs (R1–R4 of the [repair plan](research/rollup-audit-repair-plan-2026-09-25.md)):**
  in code at `9818b2e` with spicy-docs 0.33.0, and `197e449`. They cover
  Table III and private-law fidelity, compiled hearing dates and PDF-only
  bodies, retained source evidence with per-row USAspending observation times,
  and held-vote link refresh. The affected families republish on their next
  runs. The first repaired generations are audited (see the ledger's
  September 25 repairs): all qualify except `committee_reports` and
  `hearing_transcripts`, whose one remaining defect is fixed in spicy-docs
  `7550f0e` and awaits release. R6 (model outputs) is out of scope by the
  owner's decision.
- **R7 (hosted MCP):** live at `https://spicy-regs-mcp.mdeeb.workers.dev/mcp`
  since 2026-09-25 (deployed from `f2df979`; `f51248f` had fixed the image, which
  had lacked a runtime package since 2026-09-22). The three tools and five
  cross-source joins pass against the fork's published data (FEC to legislators
  and votes, laws to Table III, compiled hearing dates, organization to
  committee, USAspending to SAM), and `describe_table` names each table's
  managed generation. Each output's ledger audit (pin, date, disposition) is
  now reported beside its live pin as `qualification`, bundled from the ledger
  by `spicy-regs-dict generate`; this is in code and not yet deployed. Receipts:
  `/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/r7-mcp-2026-09-25/`.
- **T01:** complete; SAM failures now refuse publication, and its initial load
  is T14. Earlier scheduled runs failed on the extract download's `Accept`
  header (HTTP 406). The repaired scheduled 2002 selection succeeded and retained
  the earlier 2026 population. Full qualification of the added extract remains
  T14 because its run-time source bytes were not retained. See the ledger.
- **T03/T18:** retained-input and catalog workflows, bounded transfer, invocation
  receipts and build-only artifacts are implemented, and real GitHub build-only
  runs passed for the catalog and a two-record retained fixture
  ([retained workflows](fec-retained-workflows.md)); snapshot-based freshness
  checks and SAM dispatch controls are in place. Per-family transfers and the
  operational gaps in the execution order remain. Workflow repairs are pushed:
  catalog recovery is journaled, weekly dedupe is audit-only, dependent refreshes
  wait for completed inputs, and health checks cover raw catalog and public data.
  The hosted regulatory completion chain once stopped during the mirror
  export; it now qualifies on hosted runners (see T06). Monitoring and the
  read-only duplicate audit are enabled again.
  See the [workflow review](research/workflow-review-2026-09-24.md).
- **T04:** complete for the selected seed: all five FEC tables, from the retained
  2024/2026 committee traversal, not all FEC history (T05). See the ledger and
  the [FEC gap register](fec-gaps.md).
- **T06:** metadata catch-up is complete. Every batch succeeded and published
  its checkpoint. Reviewed publisher test payloads remain excluded with raw
  evidence retained; genuine source-null docket relationships are preserved.
  The [comment-text refactor](research/comment-text-refactor-2026-09-24.md)
  adds shared concurrent reads, independent text retries and reusable local
  manifests; its batch-boundary handoff preserves the running batch's revision.
  The public comments mirror and raw catalog pass identity and coverage checks.
  Hosted export and scheduled ETL qualify ([hosted qualification](research/comments-publication-efficiency-2026-09-25.md#hosted-qualification)):
  run 36170694175 rebuilt, uploaded and read back the mirror at 4.30 GB peak
  (the earlier failure was at a 6 GB budget), 36175550193 skipped an unchanged
  snapshot, and 36177463432 completed the first full incremental sweep since
  before September 10 (8,251 new records, 26,311,037 comment rows, dependents
  and verify passing, 2 h 33 min end to end). ETL was re-enabled at 19:04 UTC on
  September 25. Document bodies and the retained text-read exceptions remain open. Follow the
  [catch-up runbook](etl-catalog-seed.md). The local
  [publication efficiency refactor](research/comments-publication-efficiency-2026-09-25.md)
  reuses sweep membership, builds the mirror agency-first and skips verified
  unchanged snapshots. Listing now dominates a sweep; SpicyDocs 0.33.1's
  concurrent docket-range listing and the retry-only 18:25 sweep address it
  (ledger operations checkpoint). Browser query-performance qualification for
  the new monolith order remains open.
- **T07:** the monolith, index and agency mirror are published and verified at
  the September 25 pins in the ledger. Local export recovered the hosted
  out-of-memory failure and retained every previously published ID. Earlier
  native-source and derived-text repairs remain evidenced at their selected
  scope; wider source audits, the dated partition tree and bodies remain open.
- **T08:** the audit advances the stated scopes for people, votes, rosters,
  meetings, nominations, treaties and record issues. Amendments and expanded
  communications remain partial. Private-law acquisition states and four omitted
  Table III rows require repair. Members/terms remain a community crosswalk;
  official-roster completeness and wider history remain open. See the ledger.
- **T08/T16 press:** the current bounded feed population qualifies with explicit
  capture/feed metadata limits; historical carried items and literal bill links
  retain their earlier scope. See the ledger.
- **T09:** the reconciled family is published (decision 1); unfinished bodies and
  XML pairs are retried within a per-run fetch budget (`MAX_VERSION_FETCHES`).
  Remaining work is in the checkpoint below; see the ledger.
- **T10:** current report metadata/bodies, hearing transcripts and read
  checkpoints qualify at their stated scope. All former detail refusals now
  have retained valid responses. Exact changed report-section decomposition
  remains partial; the compiled-hearing date is still wrong and the two
  historical part IDs remain unsupported. See the ledger.
- **T11:** Federal Register and the retained Agenda edition (still reginfo's
  newest) are qualified. CFR parts are placed from each volume's `PART` heading
  (plan A8), and the citation and rulemaking corrections have been adopted.
  Qualify later refreshes at their own pins; the ledger records the remaining
  join limits and the next rulemaking changes.
- **T12:** the frozen CRS, FCC and GAO generations qualify at their stated
  scopes; current USAspending and court dockets remain partial. CRS advanced
  after the audit freeze, so its newer generation is separately pending.
  Still required: raw-response retention in
  the ordinary scheduled readers for replayable audits, identity-set pooling
  (plan B6, released in SpicyDocs 0.31.0 and adopted at the re-vendor; FCC
  crowded-single-day recovery released in 0.34.0 as timestamp partitioning
  and adopted at `5a16d3a`, see the ledger), court parties (decision 9) and
  catch-up, a scope decision on the APA selection's non-civil dockets, and wider
  populations. See the ledger.
- **T13:** clusters, citation tables and the text-free opinion index are
  published for the 2026-06-30 edition; bodies are withdrawn (decision 6). Bulk
  inputs advance only through `courtlistener-bulk/verified-manifest.json`
  (full-file SHA-256 plus exact source ETag; `ACQUISITION-GATE.md`). The space
  preflight checks the destination before parsing but reserves nothing for
  merges, publication copies, audit spill or concurrent work
  (`courtlistener-clusters-qualification/headroom-fix/`). See the ledger.
- **T14:** lobbying's bounded refresh qualifies. SAM's repaired scheduled
  acquisition succeeded, but the new extract needs complete retained-source
  replay. Wider registration years and lobbying history remain open.
- **T15:** summaries, link tables and docket search have refreshed against the
  catch-up parents and pass independent complete transformation replays.
  Wider source qualification of those parents remains separate. `rulemaking_lifecycles` is
  withdrawn (decision 4), and its dispatch workflow has been removed. Docket
  search omits the 18 dockets with neither title nor abstract; the other limits
  are in each table's dictionary entry. See the ledger.
- **T17:** the current rulemaking snapshot passes public/input digest and
  reference checks. Full retirement and identity semantics against decisions
  32–33 remain partial; retired no-action shells may have no successor.
- **T19:** dictionary/MCP metadata and fork coverage descriptions are in place.
  Pages deployed, and live schema verification now passes for active published
  tables. Hosted MCP deployment and representative cross-source joins remain
  separate from those checks. See the ledger.

### Current parallel work

The September 25 audits ran in parallel at medium effort with independent
cross-reviews. The [audit report](research/parallel-rollup-audit-2026-09-25.md)
records findings, scope and receipts. Audit execution is complete; repairs and
the explicitly partial source/semantic checks remain in this backlog. Empty
model/backfill tables are verified as empty, not completed work.

### Congressional delivery checkpoint

*Updated 2026-09-25.* Status, pins and verification for each family are in the
ledger; scheduled runs publish new generations before audit (decision 3).

| Family | Remaining work |
| --- | --- |
| Members and terms | Official-roster reconciliation and finer within-term party history remain separate limits. |
| Nominations, treaties and press | Frozen scopes qualify; carried-source freshness, broader history and detail remain explicit limits. |
| Reports and hearings | Qualified at the ledger's pin; the compiled-volume date is fixed and both historical part IDs are admitted. The two 1946 parts stay refused over the 24 MiB body bound (312/373 MB PDF-only). Publisher HTML placeholders do not establish full content. |
| Laws and Table III | Private-law state and the four blank-act-section rows are fixed and verified live (2026-09-26). Table III must keep walking the outgoing Congress past 2027-02-17 (the cold-start scope risk; see the ledger). |
| Committee rosters and amendments | Rosters qualify within the stated publisher-link limits. Amendments need exact capture-time evidence to resolve later source differences; sponsor absence stays explicit. |
| Bills, subjects, text and differences | Audit expanded native metadata, bodies, differences and older-Congress subject additions. The larger subject population is published. Models and backfills remain uncomputed; wider acquisition and ordinary API freshness remain open. |
| Votes | Portable public source evidence, full history and dynamic scorecards. `roll_call_votes.vote_day` is backfilled at the ledger's qualified pin; consumers use it for the chamber's day. |
| Communications and print citations | Finish the unsampled detail fills and new document/action/citation-span audits; conservation and bounded native samples pass. |
| Meetings, record issues and Senate expenditures | Meetings and record issues advance to the audited frozen pins. Senate expenditures retain their earlier unchanged qualification; wider populations remain explicit. |

[Execution receipts](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21)
retain withdrawals, generation pins, public downloads, MCP responses, byte
checks and independent reviews. The campaign remains open for the unfinished
tasks below. Storage success is separate from hosted MCP, custom-domain, catalog
and documentation deployment.

**Full host gate:** workflow repairs at `e944365` passed the full test, lint,
type and dictionary jobs in [CI](https://github.com/mikewolfd/spicy-regs/actions/runs/36033160292).
The strict documentation build passed. After the report migration,
[live schema verification](https://github.com/mikewolfd/spicy-regs/actions/runs/36033160021/attempts/2)
also passed. Earlier adoption evidence remains in the execution log and
`native-vote-variants-adoption/`. These gates establish implementation and
schema behavior; source qualification remains specific to each output pin.

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
catalog credentials were added on September 24 and the seed is verified.
`GEMINI_API_KEY` remains absent. Missing optional rate-limit credentials are
different from missing required access.

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
| **T06 · Catch-up published; broader native audit and bodies open** | **Build corrected regulatory base parents.** SpicyDocs + SpicyRegs. | T02/T03; exact September 25 public parent digests and retained source releases in the ledger. | Preserve newer observations and unrelated rows, then complete native-source qualification beyond the bounded latest-record samples. Retain body/extraction gaps explicitly. Base publication and dependent transformation replay do not establish whole-source qualification. |
| **T07 · Mirror/index/agency files published; broader native repair and dated partitions open** | **Establish the full selected comments representation.** SpicyDocs + SpicyRegs + operations. | T02; the exact public monolith and catalog population are retained and reconciled in the September 25 receipts. | Complete broader native-field and body audits while preserving the verified population. The monolith, index and agency mirror already pass public-byte, unique-ID and coverage checks. Produce the separate dated partition layout if it remains in scope; do not substitute samples for the full selected source population. |
| **T08 · Law and hearing defects fixed; print citations repaired (partial on inline Congresses); expansion audits open** | **Admit prepared independent legislative families.** SpicyRegs. | T02/T03; the September 25 audit and exact native examples. The private-law state and Table III's four blank-act-section rows are fixed and verified live (2026-09-26). | Keep every native row and explicit missing field. Complete amendments, communications and print expansion audits; keep current-only, House-only and unavailable-package limits visible. The hearing-date correction is coordinated with T10. |
| **T09 · Family qualified (ledger); 108th–117th status backfilled; historical bodies and models open** | **Build the complete bill family.** SpicyDocs + SpicyRegs. | T02/T03; corrected five-table 118th HR/S cohort; receipt-pinned 118th ZIPs; preserved 119th ZIPs and 40 body files. Qualify temporary ZIP provenance/freshness. T08 laws supplies optional links. | Reconcile all 18 family members under the ordinary `bill-family` owner, preserve intended coverage outside the repaired cohort, and acquire missing types/periods/bodies. Resolve the 600-body cap and unchanged-input skips so missing work is retried. Verify access for older backfills and models; keep uncomputed model outputs blocked, not falsely complete. Do not publish `qualified-bill-status-118-hr-s` as a competing owner. |
| **T10 · Reports, sections, transcripts and checkpoints qualify; the hearing date is fixed** | **Rebuild the report family with corrected sections.** SpicyDocs + SpicyRegs. | T02/T03; the September 25 source artifact and conservation checks. All prior identities survive and the former detail refusals now have valid retained responses. The compiled hearing keeps all native dates and no ambiguous scalar; both historical part IDs are admitted, and their 312/373 MB PDFs stay refused over the body bound. | Make a size refusal final so the two parts stop being re-requested, and distinguish source-faithful HTML placeholders from complete content and bounded COVER links from wider history. |
| **T11 · FR, Agenda and CFR parts qualified (A8 published)** | **Audit current Federal Register, CFR and Agenda generations.** SpicyDocs + SpicyRegs. | T02; current scheduled publication pins, retained public/corrected FR parents and source releases. | Verify actual remote bytes and source/output agreement. Qualify or repair CFR's failure-to-empty/partial paths before further unattended acquisition; failed sources must preserve prior valid data. Reuse passing current generations. Repair CFR's demonstrated part-ancestry mapping and recover required source fields before claiming qualified coverage. Do not replace the newer FR population wholesale with the older 803,997-row repair parent. Regenerate only affected representations or missing scope. |
| **T12 · Frozen FCC/CRS/GAO scopes qualify; USAspending/court refreshes partial** | **Qualify FCC, CRS, GAO, USAspending and CourtListener docket families.** SpicyDocs + SpicyRegs. | T02/T03; retained public files and September 25 source checks. CRS has a newer post-freeze generation. USAspending and court dockets lack exact capture-time evidence for later drift. | Retain ordinary source responses, add recipient observation dates, qualify new generations and complete selected populations. Preserve prior observations; acquire court participant relationships separately. Resolve field/grain and source failure defects before wider claims. A nonzero retained table is not source-completeness evidence. |
| **T13 · Clusters, citations and opinion index delivered; bodies withdrawn (decision 6)** | **Maintain qualified cluster, citation and opinion-index metadata.** SpicyDocs + SpicyRegs. | T02/T03; published qualified June 30 clusters and docket map, the verified opinions input. | Opinion text links out through each cluster's `absolute_url`; no bodies are built or hosted (decision 6). Audit joins against the same dated inputs and preserve the broader retained population. Treat newer catch-up and older corrections as separately qualified work. |
| **T14 · SAM qualified (every active year from 1996, retirements reconciled); lobbying scope qualifies** | **Complete SAM and lobbying initial loads.** SpicyDocs + operations + SpicyRegs. | T01 for SAM; T02/T03; preserved qualified populations and September 25 audit receipts. SAM retains every extract it reads. | Retain and completely replay each added extract, preserving `(uei, entity_eft_indicator)` identity and native fields. Continue explicit bounded years, rate budgets and resumable acquisition. Lobbying's bounded refresh qualifies; wider years/history remain separate. |

### Then: generate dependent families from verified parents

| ID / status | Task and owner | Hard prerequisites | Complete when |
| --- | --- | --- | --- |
| **T15 · Catch-up refresh and independent transformation replays pass; lifecycles withdrawn** | **Generate regulatory summaries, links and search.** SpicyRegs. | Exact recorded parent pins: T06 base tables; T11 FR; T04 committees plus T07 public comments for organization links. | Preserve the verified computation and parent bindings on later refreshes. Keep broader parent source audits separate; organization-name links remain heuristics. The legacy search object is also published and replayed. |
| **T16 · Subjects expansion published and partially audited; wider metadata/freshness open** | **Finish bill refresh, subjects, vote and press links.** SpicyRegs. | T09 owns the bill family. The earlier 118th/119th timestamp/URL repair is qualified; the larger subjects generation is published and passes all retained native 118th/119th subject-set checks plus bounded additional samples. | Complete older-Congress and wider changed-field subject audits. Resolve ordinary API/BILLSTATUS freshness without losing later prior observations. Votes and press preserve their own populations and explicit missing links; optional bill enrichment must not block acquisition. Exact scope and pins are in the ledger. |
| **T17 · New snapshot publicly verified; full identity/retirement semantics partial** | **Materialize rulemaking relationships.** SpicyRegs + operations. | Exact T06 dockets/documents, T11 FR/Agenda and T15 FR-link parent digests. Public artifacts and reference checks pass. | Complete independent semantic accounting under decisions 32–33 while preserving evidence and valid references. Retired no-action shells may have no successor; explain removals against the rule instead of inferring defects from counts alone. |

### Finish: consumer access and repeatable refresh

| ID / status | Task and owner | Dependencies | Complete when |
| --- | --- | --- | --- |
| **T18 · Workflow repairs and the hosted regulatory refresh chain pass; failure paths and source access pending** | **Complete workflow, access and monitoring wiring.** SpicyRegs + operations. | T03 retention; per-source access checks; published parents for live checks. | Add FEC catalog and explicit retained-observation workflow inputs; retain pinned input manifests and source failures. Forward Zyte only to an actually selected adapter and validate it there. Catalog access, parent-completion barriers and publication-aware checks are implemented; qualify their full refresh and failure paths on the fork. Reconcile body/model and bounded-backfill controls with their source tasks. |
| **T19 · Metadata and live schema checks pass; hosted MCP live (R7); wider data proof pending** | **Expose accurate catalog/MCP metadata for the fork.** SpicyRegs. | T02 output ledger and selected published families. | Keep dictionary descriptions aligned with active publication; verify stable identifiers, join meanings, coverage and evidence links. The withdrawn opinion bodies remain excluded. Point the tested consumer at the fork, not an upstream default. Exercise representative cross-source joins without treating inferred links or money flows as source facts. |
| **T20 · Waiting on all intended outputs** | **Close the campaign and qualify refresh.** All owners. | T02 ledger and the completed family-specific tasks; T18/T19. | Every intended output has the completion evidence below or an explicit unresolved blocker. Verify public downloads and MCP reads, then repeat selected refresh/correction/failure scenarios without losing prior valid data. Keep the campaign open for missing intended outputs. Track catalog, custom domain, Worker and Pages deployment separately; storage/data success is not deployment success. |

### Immediate execution order

The [September 25 repair proposal](research/rollup-audit-repair-plan-2026-09-25.md)
supplies concrete fixes and acceptance checks for every audit disposition,
including checkpoint/schema migration and reuse of existing local work. It is
implementation guidance proposed for the tasks below, not a completion claim.

*Updated 2026-09-25; see the
[parallel audit](research/parallel-rollup-audit-2026-09-25.md) and
[execution log](research/fork-execution-log-2026-09-23.md). What remains, in order. Items A5–A12, B6 onward, C4 and D6 are in
SpicyDocs' consolidation plan (`spicy-docs/docs/research/consolidation-path-2026-09-22.md`),
with their evidence in `spicy-docs/docs/research/parsing-survey-2026-09-23.md`:*

- **Repair reproduced source/state defects.** Preserve blank act-section Table
  III rows, truthful captured-but-refused private-law state and every native
  date for compiled hearings. Invalidate affected checkpoints, independently
  replay the retained examples, preserve the broader population, then publish
  and read back corrected families. Historical hearing-part support remains
  open. The audit report records exact native proof and owner boundaries.
- **Hosted export and the incremental sweep: done** ([hosted qualification](research/comments-publication-efficiency-2026-09-25.md#hosted-qualification)).
  Hosted mirror publication, an unchanged-snapshot skip and a complete
  incremental sweep passed on September 25, and scheduled ETL runs again. Monitoring and the read-only duplicate audit are
  enabled. The source schedules remain independent; retain the reviewed
  exclusions, source-null relationships and exact input barriers. See the
  catch-up runbook and ledger.
- **Finish partial audits in parallel.** Expand checks for newly published bill
  metadata/bodies/differences, older-Congress subjects, print spans/actions,
  communications detail fills and exact report-section decomposition. Obtain
  capture-time evidence for amendments, SAM, USAspending and court dockets.
  Record recipient observation dates and retain native responses in ordinary
  refreshes. Audit post-freeze CRS separately. Source drift or green jobs alone
  do not establish qualification.
- **Complete regulatory source and identity checks.** Independently account for
  rulemaking retirements under decisions 32–33. Continue native-source audits
  of catch-up dockets/documents and comments beyond the already reviewed
  cohorts. The monolith, index, agency mirror and derivative computations have
  passed their recorded checks; the dated partition tree, broader native-field
  repairs and bodies remain separate work.
- **Wider source populations.** SAM years, lobbying history (the key is now
  set), FCC filings history, USAspending beyond the top 10,000, court catch-up
  and parties (decision 9).
- **Bill family (T09).** Bodies, models (needs `GEMINI_API_KEY`), historical
  backfills and the measured Congress.gov/BILLSTATUS freshness gap. The
  timestamp/URL repair is published; its source checks do not qualify new bodies.
- **Operations and closure.** Pages and live schema verification pass, the
  first resumed incremental sweep is qualified, and the MCP Worker is live (R7
  above). Representative consumer reads remain. Next in publication: `dockets`
  and `documents` as managed families, table members stored once, then comments
  (`PLAN.md`, "Make regulatory publication scale with change"). Complete the
  remaining T18/T19 source access and consumer work before T20.

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

### Catalog performance: evaluate agency partitioning

Agency partitioning remains an unqualified optimization. The weekly workflow
now audits without changing the catalog; rebuilding requires explicit `apply`.
The repair uses bounded candidate writes, a durable readiness journal and
verified replacement. Those recovery guarantees must survive any optimization.

1. Run the explicit scratch partition probe with catalog credentials, through
   `scripts/probe_iceberg_partition.py` or the workflow's `probe_partition` input.
   A successful scratch read establishes support, not production pruning or
   capacity.
2. Measure agency-filtered reads and bounded writes against a representative
   partitioned candidate. Prove identity conservation and interruption recovery
   before changing production table creation.
3. Keep the bounded replacement copy unless a real-catalog capacity test proves
   a larger copy safe. An ordinary `INSERT INTO ... SELECT` can still exhaust
   memory in the Iceberg writer; removing the dedup window does not establish
   a safe whole-table write.
4. Continue deriving the comments index from the catalog while DELETE behavior
   remains unreliable. Assess manifest-load and listing costs separately using
   the measurements in the catch-up runbook.

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
| T07 | `_comments-mirror.yml`, `publish-comments-mirror.yml` | The reusable exporter runs after successful ETL, or through the manual refresh entry. It validates and publishes public comments/agency partitions before the dependent regulatory jobs run. |
| T04/T18 | `fec-committees`, `fec-source-catalog` | Reuse the qualified local seed/catalog first. Fresh committee traversal needs the configured API key and complete acquisition evidence; catalog refresh uses the pinned provider inventory. |
| T04/T05/T18 | `build-fec-observations` | Reuse sealed artifacts first. New builds consume an explicit manifest and exact retained releases/captures/dictionaries; this command does not acquire source data. |
| T08 | `members`, `laws`, `committee-rosters`, `house-communications`, `committee-meetings`, `record-issues`, `treaties`, `nominations`, `amendments`, `print-citations`, `senate-expenditures` | Independent source selections; reuse measured/adopted/corrected candidates from the inventory where qualified. Relevant fresh API readers receive `DATA_GOV_API_KEY`. |
| T09 | `bill-family` | Complete family acquisition/build. `laws` enriches law links but is optional. Bodies/older API backfills and models have separate access and processing limits. |
| T10 | `committee-reports` | Rebuild the complete report/hearing family with corrected sections and source-specific qualification. |
| T11 | `federal-register`, `cfr-sections`, `unified-agenda` | Audit the current scheduled generations first; reuse passing data and repair or rebuild the affected outputs. |
| T12 | `fcc-proceedings`, `fcc-filings`, `crs-reports`, `gao-reports`, `usaspending-recipients`, `courtlistener` | Own-source inputs and success checks; `courtlistener` produces `court_dockets`. Its dedicated token is optional. |
| T13 | `court-opinion-clusters`, `court-citations`, `court-opinions` | Native source qualification; retain cluster/scope/join inputs. Opinion bodies are withdrawn (decision 6). The citation tables and the text-free opinion index copy one whole export each; `court-opinions` runs only where the 54.6 GB original is retained. |
| T01/T14 | `sam-entities`, `lobbying-filings` | Source refusal repair/access and bounded resumable acquisition. SAM's repaired scheduled selection succeeded; retain complete selected-year extracts for replay. The bounded lobbying refresh qualifies; wider histories remain open. |
| T15 | `feed-summary`, `agency-stats` | Require `dockets`, `documents`, `comments_index`. |
| T15 | `agency-monthly-volume`, `discovery-signals` | Require `documents`; run in the regulatory completion chain. The lifecycle research producer remains available locally, with its publication workflow removed. |
| T15 | `docket-search` | Requires `dockets`; produces legacy `docket_search.json.gz` outside managed Parquet-family publication. |
| T15 | `fr-docket-links` | Runs after successful `federal-register` refresh; a manual repair entry remains available. |
| T15 | `org-committee-links` | Requires the selected completed FEC family and public comments. The regulatory chain waits for mirror publication and verifies unchanged base ETags; the direct comments read still sits outside the rollup class's declared local inputs. |
| T08/T16 | `press-releases`, `roll-call-votes` | Own-source acquisition can stand alone. Optional enrichment uses `congress_bills` or `bill_vote_references`; missing links stay explicit. |
| T08 | `member-vote-terms` | The shared congressional refresh waits for both `members` and `roll-call-votes` to succeed; individual manual repair entries remain available. |
| T16 | `bill-subjects` | Requires selected `congress_bills`, then fetches missing per-bill subjects. |
| T17 | `materialize-rulemaking` | Requires `dockets`, `documents`, `federal_register`, `unified_agenda`, `fr_docket_links`; first publication requires `allow_bootstrap=true`. Scheduled invocation now follows the regulatory refresh, retaining the selected external-source generations. Its direct data requirements do not include FEC or full comments. |

Catalog seed scripts migrate existing docket/comment snapshots. They do not
bootstrap an empty bucket. If the campaign starts with the supported Parquet
base path, complete and verify the comments representation needed by consumers
before declaring those dependents ready. Do not require unrelated Iceberg work
for a rollup that only needs verified Parquet inputs.
The mirror verifies unique IDs, preservation of previously published IDs, and
exact index/partition coverage before upload. It also rejects a changing public
predecessor and keeps `MIN_EXPECTED_ROWS` as an early sanity check. The workflow
then reads public files and the raw catalog back at recorded base ETags. A
passing synthetic recovery probe establishes repair behavior; T07 still needs
the full public readback and source qualification of its selected population.

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
