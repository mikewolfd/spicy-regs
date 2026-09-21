# Generate the complete rollup set on the fork

The delivery target is every existing rollup and its declared outputs generated
and verified for `mikewolfd/spicy-regs`, using the fork's own data destination.
FEC is one part of that delivery. A local sample, an upstream-hosted file, an
enabled schedule or a successful workflow is not completion evidence by itself.

This plan reconciles the workflow and producer inventory at `c418dc3` with the
fork's observed storage on September 21, 2026. It defines the full generation
campaign; the inventory check did not dispatch that campaign.

## Current fork state

The checkout has 40 `run-rollup-*` commands and `build-fec-observations`, served
by 39 thin rollup workflows. All declared dictionary/MCP tables have producers.
The catalog and retained-observation FEC commands have no workflow yet. Base
regulatory ingestion, comments publication and rulemaking materialization have
separate workflows and must be included in the delivery plan.

The complete bucket listing contained seven objects and no root base tables.
Its publication index selected only `sam-entities`: a **zero-row, 575-byte**
Parquet file. The [SAM run](https://github.com/mikewolfd/spicy-regs/actions/runs/35643620562)
reported no API key, yielded no source records and published that empty result.
Downloaded bytes and the Parquet footer confirm the count. This is an invalid
claim of acquired empty data and does not count as a generated dataset. The
earlier invalid lobbying family is withdrawn; its diagnostic objects remain.

`DATA_GOV_API_KEY` and `ZYTE_TOKEN` are now installed in the fork's GitHub Secrets.
The shared API key passed a one-record OpenFEC request. A complete committee run
has not been verified after installation. Zyte is not yet wired to a workflow
or caller adapter. The fork has no configured `SAM_API_KEY`, `GEMINI_API_KEY` or
R2 catalog secrets in the observed secret-name inventory. Missing optional rate
limit credentials are different from missing required access.

There were 38 active thin rollup workflows; lobbying alone was disabled. Recent
run conclusions span older source revisions and storage settings. Do not use
those green checks as proof that the new destination contains their outputs.

Evidence: [fork inventory receipts](/Users/mikewolfd/Work/corpora/fork-rollup-generation-2026-09-21),
including `publication-before.json`, `bucket-inventory.json`, `workflows.json`,
`workflow-runs.json`, `secrets.json` (names/timestamps only) and
`sam-empty-publication-audit.json`.

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

## Generate in dependency order

Start with the [local reuse inventory](research/local-data-reuse-2026-09-21.md).
The two audited FEC generations are already sealed, and extensive regulatory,
legislative and court inputs are retained locally. Transfer or admit qualified
existing inputs before reacquiring them. Rebuild known defective representations
from retained originals and acquire only missing populations or freshness deltas.
The inventory records which files are complete selections, bounded repairs,
samples or duplicates; publication must preserve those distinctions.

The following accounts for every `run-rollup-*` command. Names omit that prefix;
outputs normally replace command hyphens with underscores. Multi-output families
are expanded below. Parallelize independent acquisitions within publisher quotas;
wait for verified parents before starting dependents.

| Stage | Producers | Required inputs and execution conditions |
| --- | --- | --- |
| Base regulatory data | `run-pipeline` | Produce the fork's `dockets`, `documents`, `comments_index` and comments partitions/catalog. Manual non-Iceberg runs are supported. Scheduled runs force Iceberg and need catalog configuration. |
| Public comments | `publish-comments-mirror.yml` | Current mirror implementation reads the populated Iceberg catalog and writes public `comments`/agency partitions. `org-committee-links` needs the public comments representation. |
| Independent API/file sources | `cfr-sections`, `fcc-proceedings`, `fcc-filings`, `fec-committees`, `crs-reports`, `amendments`, `committee-reports`, `print-citations`, `senate-expenditures`, `laws`, `committee-rosters`, `house-communications`, `committee-meetings`, `record-issues`, `treaties`, `nominations` | Source-specific API/file acquisition; relevant readers receive `DATA_GOV_API_KEY`. Qualify successful scope and failure handling before accepting an initial dataset. These can run independently of regulatory base ingestion. |
| Other independent sources | `federal-register`, `unified-agenda`, `gao-reports`, `usaspending-recipients`, `members`, `court-opinion-clusters`, `court-opinion-bodies`, `courtlistener` | Source APIs, feeds, repositories or bulk files. CourtListener's dedicated token is optional. `courtlistener` produces `court_dockets`. |
| Sources with known initial-load blockers | `sam-entities`, `lobbying-filings` | SAM requires authorized access and failure refusal; its default reads one rotating registration-year window. LDA needs the bounded resumable initial acquisition described in the master register; its schedule remains paused. |
| FEC catalog | `fec-source-catalog` | Pinned provider inventory; add workflow wiring. |
| Retained FEC observations | `build-fec-observations` | Transfer an explicit manifest and exact retained releases/captures/dictionaries to the runner, then build its three outputs. This command does not acquire new source data. Add its distinct workflow inputs. |
| Base-dependent summaries | `feed-summary`, `agency-stats` | Require `dockets`, `documents`, `comments_index`. |
| Document-dependent derivations | `agency-monthly-volume`, `discovery-signals`, `lifecycles` | Require `documents`; `lifecycles` produces `rulemaking_lifecycles`. Audit source grain and time-window semantics separately. |
| Search object | `docket-search` | Requires `dockets`; produces legacy `docket_search.json.gz`, outside managed Parquet-family publication. |
| Federal Register links | `fr-docket-links` | Requires `federal_register`. |
| Organization links | `org-committee-links` | Requires `fec_committees` and public `comments.parquet`. The latter is read by the transform but not declared in the rollup class's `inputs`. |
| Complete bill family | `bill-family` | Acquires BILLSTATUS/GovInfo data. Run after `laws` for its optional law links. This must precede the narrow `congress-bills` writer. Bodies/older API backfills and model tables have separate credentials and processing limits. |
| Narrow bill refresh | `congress-bills` | Requires a complete published `bill-family`; carries its other members forward. Running it first can acquire/build data and then refuse publication. |
| Source records with optional bill links | `press-releases`, `roll-call-votes` | Own-source acquisition can stand alone. Run after the selected bill family/refresh to fill links from `congress_bills` or `bill_vote_references`. Missing links remain explicit. |
| Rulemaking materialization | `materialize-rulemaking` | Requires `dockets`, `documents`, `federal_register`, `unified_agenda`, `fr_docket_links`; its separate workflow requires `allow_bootstrap=true` for first publication. |
| Bill subjects | `bill-subjects` | Requires `congress_bills`, then fetches per-bill subjects. |

Catalog seed scripts migrate existing docket/comment snapshots. They do not
bootstrap an empty bucket. If the campaign starts with the supported Parquet
base path, complete and verify the comments representation needed by consumers
before declaring those dependents ready. Do not require unrelated Iceberg work
for a rollup that only needs verified Parquet inputs.

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

## Work needed before a trustworthy full run

| Item | Required change or verification |
| --- | --- |
| Source failure must stop publication | Repair SAM's confirmed missing-key/failure-to-empty path and withdraw the invalid empty family with its evidence preserved. Similar failure-to-empty/partial code paths exist in CFR, CRS, FCC, USAspending, GAO and court dockets; qualify and repair them before unattended initial loads. FEC, Federal Register and corrected LDA already have stronger refusal paths. A generic “nonempty” guard cannot replace a source-specific success check. |
| Required access | Verify each selected API with the configured key. SAM authorization is separate from a working OpenFEC key. Gemini is absent from the fork's observed secret inventory; the three model outputs remain unproduced without it. Zyte needs both environment forwarding and an explicitly selected public-source adapter; it does not replace OpenFEC authentication. |
| First-load scope and controls | Wire SAM's existing year/mode/record bounds into dispatch; current workflow exposes only `skip_upload`. Use explicit backfill windows and checkpoints for LDA, Congress, comments and other large populations. |
| Body/model completion | Bill-family caps body fetches at 600 per run. Its unchanged-input checks do not fully encode missing body/model work, so adding a credential and repeating an unchanged selection does not prove completion. Qualify explicit reprocessing and record unread/uncomputed outputs. |
| Known interpretation defects | Apply the master register's existing correctness gates. For example, CFR's `19-8.1`/Part `241` ancestry counterexample is not fixed merely by generating a newer file. Preserve raw source fields while unsupported interpretations remain explicit. |
| Parent availability | Use actual completed parent generations as barriers. Cron offsets are not dependencies. Pin compatible Congress/date scopes for bills, votes and reports; one selected parent cohort cannot silently replace a wider child population. |
| Concurrent publication | Independent family uploads share a conditional `publication.json` update. A concurrent change may safely refuse a publish. Retain the built artifact and retry using fresh publication state; serialize publication when practical without serializing independent acquisition. |
| Successful artifact retention | Shared workflow uploads output on failure, but normally discards successful dry-run outputs. Retain successful candidate artifacts and audits before relying on a build-only campaign. |
| Monitoring | `check_rollup_freshness.py` still reads bare `/<table>.parquet` paths. Resolve the publication index before treating it as a check of newly generated families. Verify legacy/base/partition paths separately. |
| Public completion | Validate every intended output at the fork's destination, then repeat CLI/MCP discovery and representative joins. Keep missing families visible; a declaration or green job does not establish a dataset. |

The immediate execution sequence is therefore: finish refusal/credential/input
checks; establish the base and independent source generations in parallel;
complete bill-family and the dependent waves; then reconcile all outputs and
qualify recurring refresh. Existing audit receipts can supply pinned inputs where
appropriate, but every published generation must identify what it actually used.

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
