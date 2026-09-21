# Fork delivery: consolidated tasks and execution order

The delivery target is every existing rollup and its declared outputs generated
and verified for `mikewolfd/spicy-regs`, using the fork's own data destination.
FEC is one part of that delivery. A local sample, an upstream-hosted file, an
enabled schedule or a successful workflow is not completion evidence by itself.

This is the single execution backlog for fork generation and local data reuse.
The [dated local inventory](research/local-data-reuse-2026-09-21.md) supplies
exact paths, sizes, artifact pins and qualification limits; it is supporting
evidence, not a second task list. Source-specific defects remain in the
[cross-repository gap register][master] and [FEC gap register](fec-gaps.md).

Reuse verified local outputs first, rebuild defective outputs from retained
sources, and acquire missing populations or freshness updates. Complete each
family's source, build and publication checks before starting its dependents.
An unrelated credential or source problem must not block ready families.

The workflow/producer inventory was reconciled at `c418dc3`; the latest status
below uses the September 21, 2026, 22:28 UTC observation at `4ef7150`. This
consolidation changes the plan; it does not dispatch jobs or publish data.

## Verified baseline

The checkout has 40 `run-rollup-*` commands and `build-fec-observations`, served
by 39 thin rollup workflows. All declared dictionary/MCP tables have producers.
The catalog and retained-observation FEC commands have no workflow yet. Base
regulatory ingestion, comments publication and rulemaking materialization have
separate workflows and must be included in the delivery plan.

The initial complete bucket listing contained seven objects and no root base
tables. Its publication index selected only `sam-entities`. Subsequent scheduled
runs changed that index; do not treat the earlier listing as current coverage.

| Family selected at 22:28 UTC | Rows reported in the public index | Disposition |
| --- | ---: | --- |
| `federal-register` | 1,008,903 | Published by a scheduled job; T11 must audit this generation before adoption as a verified parent. |
| `cfr-sections` | 319,507 | Published by a scheduled job; T11 must check the known ancestry defect as well as source/output agreement. |
| `unified-agenda` | 3,954 | Published by a scheduled job; T11 must audit this generation. |
| `sam-entities` | 0 | Invalid **575-byte** publication; T01 must withdraw it and repair failure handling. |

The [SAM run](https://github.com/mikewolfd/spicy-regs/actions/runs/35643620562)
reported no API key, yielded no source records and published that empty result.
Downloaded bytes and the Parquet footer confirm the count. This is an invalid
claim of acquired empty data and does not count as a generated dataset. The
earlier invalid lobbying family is withdrawn; its diagnostic objects remain.
The three later publication counts above were read from the live index, not
established by fresh full-file or native-field audits. There is no FEC family in
that observed index. Local inventory and temporary-file preservation are done;
the planned local reuse campaign has not been executed.

`DATA_GOV_API_KEY` and `ZYTE_TOKEN` are now installed in the fork's GitHub Secrets.
The shared API key passed a one-record OpenFEC request. A complete committee run
has not been verified after installation. Zyte is not yet wired to a workflow
or caller adapter. The fork has no configured `SAM_API_KEY`, `GEMINI_API_KEY` or
R2 catalog secrets in the observed secret-name inventory. Missing optional rate
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
| **T01 · Ready** | **Contain and repair SAM failure publication.** Operations + source owner. | Current invalid SAM pin and retained failure evidence. | Pause its faulty refresh, conditionally withdraw only the invalid family while preserving evidence and other families, and make missing access/failed extraction abort before output. Prove the former failure cases refuse publication. Resume only after T14 qualifies a real selection. |
| **T02 · Ready** | **Record each producer's selected population and outputs.** SpicyRegs + operations. | Producer map below, dated local inventory and current publication index. | Each output has its intended Congress/cycle/date scope, current candidate or source location, missing work, owner, parent pins and receipt location. Keep broad existing populations when applying smaller repairs. Retain the already explicit FEC selection; do not invent a new approval step or shrink other sources to samples. |
| **T03 · Ready** | **Prepare retained inputs and successful build artifacts for runner use.** SpicyRegs + operations. | Exact inventory paths and manifests; existing generation/admission tools. | Transfer only selected manifests and referenced bytes with verified digests. Preserve original acquisition metadata and record later transfers separately. Keep successful build-only artifacts and audits, which the shared workflow currently discards. Document the supported invocation for each retained input. Ready sealed FEC artifacts can use the existing publisher immediately. |

### In parallel: publish, qualify or rebuild independent families

| ID / status | Task and owner | Reuse route / dependencies | Complete when |
| --- | --- | --- | --- |
| **T04 · Ready** | **Publish the audited FEC seed.** SpicyRegs + operations. | T02; the two sealed families in the inventory (**1.17 GB**) and the schema-compatible **27,311-row** committee table. T03 only for bytes that need runner transfer. | Reuse the exact sealed observation/catalog artifacts, seal the committee selection, and verify publication pins, remote bytes, CLI downloads and MCP reads. Preserve the committee cycle filter and reported-relationship limits. No reacquisition is needed for this seed. |
| **T05 · Follow-on** | **Expand FEC from other retained sources.** SpicyDocs + SpicyRegs. | T02/T03; audited 2024/2026 tables, complete financial ZIPs, committee-history dump and retained filing/legal/agency captures. | Reconcile what is already in T04; adopt missing selected tables, expand the **32,034,987-row** individual base, and implement/adopt the assessed committee history. Track correction streams separately. Gate further inaugural, enforcement and agency adoption on their documented parser/identity limits. Wider history remains explicit; this does not block publishing T04. |
| **T06 · Ready** | **Build corrected regulatory base parents.** SpicyDocs + SpicyRegs. | T02/T03; retained public dockets/documents/comments index plus the **13.8 GB** source-release set. | Apply the qualified repair paths to the intended populations, preserve newer parent observations and unrelated rows, and verify the fork's base `dockets`, `documents`, `comments_index` and selected partitions. The 392/547/3 repair cohort is evidence for the fix, not the full replacement. Use the existing base publication path; these outputs are outside managed rollup families. |
| **T07 · Waiting: missing full input** | **Establish the full selected comments representation.** SpicyDocs + SpicyRegs + operations. | T02; no full local comments corpus was found. Use a qualified pinned full published object or acquire the missing source population. Catalog configuration is required if using the current Iceberg mirror workflow. | Retain and audit the complete chosen population, publish its index/partitions and the public comments representation needed by consumers, and verify counts agree at their declared grains. The 212,733-row sample cannot stand in for the 23,889,661-row source object. Missing full comments blocks its consumers, not independent sources. |
| **T08 · Review** | **Admit prepared independent legislative families.** SpicyRegs. | T02/T03; measured members/terms, rosters, laws, amendments, meetings, nominations, record issues, treaties; newer communications/print outputs; corrected Senate expenditures. | Current schemas, complete family membership and raw/output audits pass for each selected scope; publish and read back each family. Keep current-only, House-only and unavailable-package limits visible. Prepare votes and press releases here if useful; finalize optional bill links in T16. |
| **T09 · Ready; some outputs blocked by inputs/access** | **Build the complete bill family.** SpicyDocs + SpicyRegs. | T02/T03; corrected five-table 118th HR/S cohort; receipt-pinned 118th ZIPs; preserved 119th ZIPs and 40 body files. Qualify temporary ZIP provenance/freshness. T08 laws supplies optional links. | Reconcile all 18 family members under the ordinary `bill-family` owner, preserve intended coverage outside the repaired cohort, and acquire missing types/periods/bodies. Resolve the 600-body cap and unchanged-input skips so missing work is retried. Verify access for older backfills and models; keep uncomputed model outputs blocked, not falsely complete. Do not publish `qualified-bill-status-118-hr-s` as a competing owner. |
| **T10 · Ready** | **Rebuild the report family with corrected sections.** SpicyDocs + SpicyRegs. | T02/T03; retained report/hearing inputs and the corrected section replay; acquire missing selected inputs as needed. | Rebuild and qualify reports, sections, hearings, bill links and read-status outputs together. The 11-section correction sample and the old defective family cannot substitute for the intended corpus. |
| **T11 · Review** | **Audit current Federal Register, CFR and Agenda generations.** SpicyDocs + SpicyRegs. | T02; current scheduled publication pins, retained public/corrected FR parents and source releases. | Verify actual remote bytes and source/output agreement. Qualify or repair CFR's failure-to-empty/partial paths before further unattended acquisition; failed sources must preserve prior valid data. Reuse passing current generations. Repair CFR's demonstrated part-ancestry mapping and recover required source fields before claiming qualified coverage. Do not replace the newer FR population wholesale with the older 803,997-row repair parent. Regenerate only affected representations or missing scope. |
| **T12 · Review / missing paired sources** | **Qualify FCC, CRS, GAO, USAspending and CourtListener docket families.** SpicyDocs + SpicyRegs. | T02/T03; retained public files and source-specific evidence where available. | Verify or repair failure-to-empty/partial paths before acquisition. Locate or acquire missing paired originals, resolve each family's known field/grain defects from the master register, build only where needed, and publish audited complete selections. Source success and completeness must be explicit; a nonzero retained table is not enough. |
| **T13 · Review / rebuild required** | **Qualify court clusters and rebuild court bodies.** SpicyDocs + SpicyRegs. | T02/T03; 10.1M-cluster materialization, 250K old bodies, scope/join companions and 284-row corrected replay. | Find or reacquire required native originals, establish cluster/source agreement and rebuild the selected body population with all supported native text variants. Auxiliary joins are inputs, not extra hosted datasets. Do not promote the 284-row replay to full coverage. |
| **T14 · Waiting: source qualification/access** | **Complete SAM and lobbying initial loads.** SpicyDocs + operations + SpicyRegs. | T01 for SAM; T02/T03; retained nonempty tables as comparison candidates, not source proof. Verify SAM-specific authorization; use the repaired filtered LDA reader. | Explicit bounded scopes, rate budgets and resumable checkpoints produce source-proven outputs. Wire SAM year/mode/record controls into dispatch. Retain source codes, dates and field meanings. Qualify and publish each family before resuming its schedule; the existing lobbying pause remains until its initial load succeeds. |

### Then: generate dependent families from verified parents

| ID / status | Task and owner | Hard prerequisites | Complete when |
| --- | --- | --- | --- |
| **T15 · Waiting on selected parents** | **Generate regulatory summaries, links and search.** SpicyRegs. | See exact producer map below: T06 base tables; T11 FR; T04 committees plus T07 public comments for organization links. | Build each eligible rollup from recorded parent pins; verify key coverage, join fan-out, source grain and date semantics. Publish the legacy `docket_search.json.gz` through its separate path. Do not carry old derivatives forward as though rebuilt against new parents. |
| **T16 · Waiting for full bill publication where required** | **Finish bill refresh, subjects, vote and press links.** SpicyRegs. | `congress-bills` requires T09's complete published family; `bill-subjects` requires its selected bills. Bill links for votes and press are optional dependencies. | The narrow writer preserves all sibling members and their pins; subjects reconcile the retained 20,013-row enrichment with the selected bills. Votes and press retain their source populations, distinguish missing from verified bill links, and pass join audits. Optional linking must not block their own-source acquisition. |
| **T17 · Waiting on selected parents** | **Materialize rulemaking relationships.** SpicyRegs + operations. | T06 dockets/documents; T11 FR/Agenda; T15 FR-docket links. | Bootstrap deliberately with `allow_bootstrap=true`, produce all five outputs, and verify references, counts and public reads against the exact parent generations. |

### Finish: consumer access and repeatable refresh

| ID / status | Task and owner | Dependencies | Complete when |
| --- | --- | --- | --- |
| **T18 · Ready in parallel; final proof waits for data** | **Complete workflow, access and monitoring wiring.** SpicyRegs + operations. | T03 retention; per-source access checks; published parents for live checks. | Add FEC catalog and explicit retained-observation workflow inputs; retain pinned input manifests and source failures. Forward Zyte only to an actually selected adapter and validate it there. Configure required catalog access if choosing Iceberg paths. Make parent completion the barrier, and make freshness checks resolve `publication.json` rather than bare table URLs. Reconcile body/model and bounded-backfill controls with their source tasks. |
| **T19 · Ready in parallel; final proof waits for data** | **Expose accurate catalog/MCP metadata for the fork.** SpicyRegs. | T02 output ledger and selected published families. | Add missing dictionary descriptions for `bill_subjects`, `court_opinion_clusters` and `court_opinion_bodies`; verify actual availability, columns, stable identifiers, join meanings, coverage and evidence links. Point the tested consumer at the fork, not an upstream default. Exercise representative cross-source joins without treating inferred links or money flows as source facts. |
| **T20 · Waiting on all intended outputs** | **Close the campaign and qualify refresh.** All owners. | T02 ledger and the completed family-specific tasks; T18/T19. | Every intended output has the completion evidence below or an explicit unresolved blocker. Verify public downloads and MCP reads, then repeat selected refresh/correction/failure scenarios without losing prior valid data. Keep the campaign open for missing intended outputs. Track catalog, custom domain, Worker and Pages deployment separately; storage/data success is not deployment success. |

### Immediate execution order

1. Start **T01** containment and **T02** the output ledger. Ready families can
   proceed once their own scope and publication state are recorded.
2. Run **T04** FEC publication alongside **T03** transfer/retention,
   **T06** base repair preparation, **T08–T13** independent family work and
   **T18–T19** workflow/consumer fixes. Assess **T07/T14** missing inputs and access
   in parallel. Do not make every lane wait for SAM, comments, Gemini or Iceberg.
3. Release **T15–T17** per producer as each required parent is verified. Finalize
   optional bill-linked fields after the chosen bill generation exists.
4. Complete **T20** across the whole intended output set. Carry **T05** retained
   FEC expansion as an explicit follow-on; include any populations selected by
   T02 in the completion ledger rather than quietly omitting them.

Parallelize independent acquisition, local builds and audits within source rate
limits. Serialize publication-index updates where practical. A concurrency refusal
requires a fresh index and compatible parent pins before retry; unchanged local
artifacts are reusable only when the current publisher's checks accept them.
Use the [generation publication rules](generation-publication.md), including the
distinction between offline candidates, managed families and legacy/base paths.

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
| T13 | `court-opinion-clusters`, `court-opinion-bodies` | Native source qualification and complete selected body rebuild; retain cluster/scope/join inputs. |
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

`bill_subjects`, `court_opinion_clusters` and `court_opinion_bodies` have executable
producers even though the static dictionary lacks their declarations. They belong
in the generation inventory. Managed MCP discovery can find their published
tables; their detailed dictionary descriptions still need adoption.

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
