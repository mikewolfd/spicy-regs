# SpicyRegs workflow review — September 24, 2026

**Verdict: keep the source coverage and shared runner; repair maintenance, sequencing and verification. Retire the withdrawn lifecycle workflow.** Most jobs produce distinct, still-useful datasets. The missing work is primarily reliable completion and publication checks, rather than more independent schedules.

This reviews the fork's GitHub Actions definitions at `cded33db60ab09ed4ebde4307f3f6c7c4c25de84`, their recent runs, producers and consumers, plus the configured Cloudflare target. The local definitions match GitHub's registered inventory. Run statuses below are a snapshot from **2026-09-24 16:13 UTC**; Wrangler checks followed at **16:25–16:27 UTC**. A successful run establishes execution success, not source qualification. No workflow, schedule, credential or remote resource was changed by this review.

## Implementation follow-up — September 24

The follow-up addresses W1–W5 and moves the historical remainder to a pinned
local checkout. The hosted sweep was cancelled after its completed batch 0;
batch 1 was still staging, so its uncommitted work is being repeated locally.
ETL, mirror publication, dedupe and comments monitoring are paused during that
local writer. The [catch-up runbook](../etl-catalog-seed.md) defines the return
to scheduled incremental processing.

| Finding | Implemented behavior | Verification |
| --- | --- | --- |
| W1 | Scheduled dedupe audits only and fails on duplicates; repair and the scratch probe require explicit inputs. An append-only journal marks the complete candidate before replacing live data. Normal writes and exports refuse unfinished repairs. | Local interruption tests cover candidate creation, readiness, live-table replacement and cleanup. A real R2 scratch catalog was interrupted after its first agency copy, then recovered both original IDs on retry; all probe tables were removed. |
| W2 | Checks read raw catalog rows and anonymous monolith/agency files, reconcile every docket/month count and verify unique IDs. The publisher preserves prior IDs before replacing the mirror. Freshness checks enumerate active published tables. | Regression tests reject duplicate IDs, same-month omissions, substituted IDs and changing predecessor ETags. Live rollup freshness passed against the current publication inventory. Full comments readback remains part of catch-up completion. |
| W3 | ETL completion triggers mirror publication, dependent regulatory outputs and readback under one writer queue. Vote terms wait for votes and members; Federal Register links wait for their source. Manual repair entry points remain available. | Reusable-workflow validation and scheduled-producer tests pass. Base ETags are retained in an Actions artifact and compared after dependent jobs finish. Actual execution of the new refresh chain remains a hosted check. |
| W4 | The purge credential check skips the uncached `r2.dev` serving path; custom-domain checks still require credentials. | Both cases pass regression tests; the existing Wrangler account/bucket observation still applies. |
| W5 | The withdrawn lifecycle workflow is removed. Live schema verification covers active publication and fails separately from documentation deployment. | Offline dictionary checks and generated pages agree. The live check now reports the pending report-part migration: `committee_reports.part_id`, `committee_reports.part_number` and `report_sections.part_id` await the next source refresh, as decision 29 already records. |

Lint, type checks and the full Python suite pass. The Actions linter passes
with one narrow compatibility exception: actionlint 1.7.12 does not yet recognize
GitHub's documented `concurrency.queue` field. Every catalog-writer group uses
`queue: max` with `cancel-in-progress: false`. The exception suppresses only
that unknown-field diagnostic.

Receipts added to the audit directory include `dedupe-real-catalog-recovery.json`,
`pytest-fixed.log`, `live-schema-fixed.log` and `live-freshness-fixed.log`.
These establish repair and checker behavior; the still-running catch-up has
not yet established a complete refreshed public dataset. Optional document
enrichment, retained court-edition automation and MCP deployment remain the
separate delivery choices listed below.

## Original findings

### W1 — High: scheduled deduplication is not safe to retry

The weekly [dedupe workflow](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/dedupe-comments-catalog.yml#L76) automatically applies a whole-table rebuild. In [the replacement code](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/src/spicy_regs/sources/iceberg.py#L681), an interruption after creating the replacement live table leaves both a partial live table and a complete sibling. Recovery recognizes only an *absent* live table. A later rebuild drops the sibling and starts from the partial live table.

A local DuckDB failure-injection reproduction used two distinct IDs, each duplicated. An interruption during the second agency's copy left one ID in the live table and both in the sibling. The duplicate audit then returned no duplicates, so [the script's clean-table branch](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/scripts/dedupe_comments_catalog.py#L83) would exit successfully. Retrying the core rebuild discarded the complete sibling and retained only the partial population. This proves the control-flow defect locally; it is **not evidence of live R2 data loss**. The earlier read-only catalog audit at 15:54 UTC found unique IDs throughout the then-current catalog.

**Recommendation:** make the weekly schedule audit-only before its next run. Make duplicate findings visible through a failing check or structured result. Keep repairs manual until recovery identifies whether a retained sibling is complete and preserves it through interrupted replacement. Qualify interruption and retry against an isolated real catalog before restoring automatic repair.

Manual dispatch also runs a scratch-table partition probe even when `apply=false`. Give that write probe its own explicit input or maintenance entry point so an audit is actually read-only. Skip mirror republication when a repair made no change.

### W2 — High: green health checks can conceal missing data

The [comments checker](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/scripts/check_comments_freshness.py#L90) compares the latest month and presence of rows; it computes index and actual row counts but does not use their disagreement to flag incomplete coverage. With catalog credentials, it reads [the MCP view](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/src/spicy_regs/mcp_server.py#L415), which already removes duplicate IDs. That view cannot establish physical catalog uniqueness, and it does not check what anonymous mirror readers receive.

A local reproduction supplied two raw rows for the same ID, one visible row after consumer deduplication, and an index advertising 100 rows in the same month. Both stale and duplicate flags were false. This is a reproduced checker blind spot, not a measurement of current public loss.

The [mirror publisher](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/scripts/publish_comments_mirror.py#L49) also accepts any export above a one-million-row floor while its workflow bypasses the byte-size shrink guard. That floor cannot protect the much larger currently published population against substantial partial export.

**Recommendation:** extend the existing checks to cover raw catalog uniqueness, expected ID and agency coverage, and anonymous mirror/index agreement at recorded input versions. Validate population conservation before publication and read back the published objects afterwards. Allow legitimate source corrections through explicit evidence rather than a blanket shrink override.

The [rollup freshness inventory](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/scripts/check_rollup_freshness.py#L64) also lags delivery: it still labels FCC tables and committee report reads as unpublished and omits newer families. Derive active table coverage from publication metadata, with source-specific freshness rules. An unchanged bulk edition may be healthy; a fresh maximum date alone does not prove complete coverage. Reuse the existing ledger-pin and source-domain-drift scripts for reporting. A changed pin should report that its source qualification is pending, as decision 3 allows, rather than silently inheriting the prior qualification.

### W3 — Medium: time offsets do not establish parent completion

Independent crons currently approximate dependency ordering:

| Consumer | Required parent data | Current exposure |
| --- | --- | --- |
| Comments mirror | Completed catalog writes | Its separate concurrency group allows overlap with ETL or dedupe. |
| Feed summary and agency statistics | Dockets, documents and comments index | Evening offsets do not guarantee the ETL sweep finished. |
| Monthly volume and discovery signals | Documents | Can consume the previous or an intermediate base publication. |
| Docket search | Dockets | Runs independently of the ingestion result. |
| Federal Register docket links | Federal Register | A later cron does not prove that the parent refresh succeeded. |
| Member vote terms | Member votes, members and terms | The 03:50 run may start while the 03:00/03:20 parents still run. |
| Organization–committee links | FEC committees and public comments mirror | The 20:30 schedule precedes the 21:30 mirror; the direct remote comments read is not declared among the normal rollup inputs. |
| Rulemaking dataset | Dockets, documents, Federal Register, agenda and docket links | Needs a coherent, recorded selection of completed parents. |

These jobs can succeed on older parents. That may be acceptable when recorded explicitly; it should not be mistaken for a completed refresh of the latest inputs. GitHub also documents that [scheduled runs can be delayed or dropped](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

**Recommendation:** add a bounded downstream workflow that waits for successful base publication, records the selected parent versions, then runs dependent jobs with explicit dependencies and verifies the result. Reuse the existing runner and producer functions. Keep independent source schedules and manual recovery entry points. Preserve optional joins as optional; a missing bill enrichment should not block acquisition of its independent source.

Use an explicit queue policy for shared writers. `cancel-in-progress: false` alone does not preserve every pending maintenance run: GitHub's default permits one running and one pending member of a concurrency group. [Current concurrency documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency) describes `queue: max` where retained pending work is needed. A chain of separate `workflow_run` triggers is not a general dependency graph: multiple named workflows are alternatives, and [chaining has a depth limit](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_run).

### W4 — Medium: the purge check fails for a feature this fork does not use

[Run 35935813847](https://github.com/mikewolfd/spicy-regs/actions/runs/35935813847) passed its published-table check and failed only the Cloudflare purge-credential step. Wrangler verified that the bucket has no custom domain and serves its enabled `r2.dev` URL. Cloudflare documents that [R2 caching requires a custom domain](https://developers.cloudflare.com/r2/buckets/public-buckets/).

**Recommendation:** condition the purge-credential check on an explicitly configured cached serving domain. Keep expiry/permission checks when purging applies. Creating a purge token would not improve this fork's current read path.

### W5 — Medium: retire the withdrawn lifecycle workflow and repair live schema checking

[Decision 4](fork-delivery-decisions-2026-09-22.md) keeps the heuristic lifecycle dataset unpublished and uses source-linked rulemaking data instead. The [lifecycle workflow](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-lifecycles.yml) is disabled, but remains dispatchable if re-enabled, with upload enabled by default. Remove that workflow or enforce candidate-only output if its research entry point is still needed. Keep `materialize-rulemaking.yml`.

The data dictionary's static table inventory still requests the withdrawn lifecycle object. Its 404 aborts live discovery; [the Pages job](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/deploy-docs.yml#L65) treats that incomplete-check exit as a warning. Thus a green Pages deployment does not establish a complete schema check.

**Recommendation:** check all active published tables using the publication inventory. Report a separate failing data-validation result when discovery is incomplete, while allowing independently valid documentation to build. Retired tables should not abort checks of active tables.

## What is missing, and what is intentionally manual

| Capability | Disposition |
| --- | --- |
| Completion-triggered downstream refresh and public readback | Add the bounded chain described in W3; extend the existing health checks rather than add a monitor per table. |
| Real catalog correction, interruption and recovery verification | Extend integration coverage with isolated scratch data and explicit cleanup. The current live-source integration workflow does not establish remote catalog replacement safety. Keep privileged remote checks out of untrusted pull-request execution. |
| Historical document PDF text enrichment | The `enrich-pdf-text --target documents` CLI exists, but only the comments target has a workflow. Add a bounded manual document path if completing T06 body coverage; first provide pinned inputs, preservation checks and publication through the owning pipeline. A local in-place enrichment command alone is not a complete publisher. |
| Court opinion metadata refresh | The metadata is already published and qualified for the June 30 edition. Its producer deliberately lacks a hosted-runner schedule because the retained source is 54.6 GB and the documented download takes about 8.6 hours. Provide a retained-source runner or verified input transfer, then coordinate metadata, clusters and citations by source edition. This is a refresh-automation gap, not an absent dataset or a reason for a daily cold download. |
| FEC retained observations and source catalog | Keep manual or tied to explicit provider-input changes. They consume selected retained evidence/inventory; a cron would not supply missing source acquisition. |
| Catalog seed and comment attachment backfill | Keep manual recovery/backfill utilities. Their usefulness does not depend on frequent runs. The existing manifest-seed runbook is sufficient until repeat bootstrap warrants a single coordinated manual workflow. |
| Model-generated bill outputs | Missing Gemini configuration is a capability/configuration gap inside the existing bill-family path, not a missing workflow. |
| Fork MCP deployment and smoke test | The configured Cloudflare Worker does not exist in the selected account. If this hosting target is adopted, add deployment plus an MCP handshake and representative data query. GitHub Pages serves documentation and does not deploy the MCP server. |
| Withdrawn opinion bodies and narrow bill writer | Keep retired. Decisions 6 and 31 deliberately chose source links for opinion bodies and one canonical bill-family writer. |

## Cloudflare verification with Wrangler

The installed and locked project version is **Wrangler 4.136.1**, invoked from `deploy/cloudflare` through `npm exec -- wrangler`. The active named profile is `spicy-regs-civictechdc`; `whoami --account 174055408ff1560e60601c4d12c561c4 --json` confirmed the configured account and an authenticated OAuth session.

Read-only remote checks established:

- `r2 bucket catalog get spicy-regs`: active; URI and warehouse match this account and bucket.
- `r2 bucket domain list spicy-regs`: no custom domains.
- `r2 bucket dev-url get spicy-regs`: public access enabled at the fork URL.
- `deployments list --name spicy-regs-mcp --json`: Cloudflare error 10007, the configured Worker does not exist in this account.

Wrangler authentication and catalog engine credentials are separate. The existing catalog setup works; no new credential is needed to resolve W4. Cloudflare requires [both catalog and storage permissions for engine access](https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/#authenticate-your-iceberg-engine). Its native [file compaction](https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/#enable-compaction) is worth assessing for file maintenance, but combining files does not establish comment-ID uniqueness or repair W1. No compaction, expiration, deployment or token change was applied.

## Interpreting recent failures

| Workflow/run | Observed cause | Current interpretation |
| --- | --- | --- |
| [Comments mirror](https://github.com/mikewolfd/spicy-regs/actions/runs/35922973837), [dedupe](https://github.com/mikewolfd/spicy-regs/actions/runs/35501408829) | Catalog credentials absent at the time | Configuration subsequently supplied; runtime and safety still need their own checks. |
| [Bill family](https://github.com/mikewolfd/spicy-regs/actions/runs/35946820267) | Shrink guard rejected loss of archive checkpoint rows | The checkpoint rule has since been repaired and adopted; qualify the next run. Keep the shrink guard. |
| [SAM entities](https://github.com/mikewolfd/spicy-regs/actions/runs/35907412992) | Source HTTP 406 | Source request repair adopted; successful scheduled execution remains the proof. |
| [FEC committees](https://github.com/mikewolfd/spicy-regs/actions/runs/35910579310) | Source HTTP 429 | Rate-budget/backoff/resumption work remains useful; the dataset and workflow remain relevant. |
| [Organization–committee links](https://github.com/mikewolfd/spicy-regs/actions/runs/35917126704) | Checkout certificate verification failed | Infrastructure failure before application work; preserve TLS verification. |
| [Nightly freshness](https://github.com/mikewolfd/spicy-regs/actions/runs/35935813847) | Missing purge credential | W4, rather than evidence that every dataset is stale. |

The [first seeded ETL sweep](https://github.com/mikewolfd/spicy-regs/actions/runs/36021389999) was still active during this review: setup and batch 0 succeeded, batch 1 was running. It does not yet establish a complete fork refresh. Source qualification and accepted generations remain in the [output ledger](fork-output-ledger-2026-09-21.md).

## Complete workflow disposition

Triggers and latest results are the audit snapshot, not a live dashboard. Scheduled workflows generally also allow manual dispatch. “Keep” means the role remains useful; it does not grant source qualification to the latest output.

| Workflow | Trigger (UTC cron where scheduled) | Purpose | Recommendation | Latest observed run |
| --- | --- | --- | --- | --- |
| [_rollup.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/_rollup.yml) | workflow_call | Common rollup execution, receipts and artifacts | Keep reusable runner | no separate run |
| [backfill-comment-attachment-text.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/backfill-comment-attachment-text.yml) | workflow_dispatch | Historical comment attachment text | Keep manual, bounded | no separate run |
| [check-comments-freshness.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/check-comments-freshness.yml) | `30 23 * * *` | Comments coverage and uniqueness checks | Repair checks (W2) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35934471660) |
| [check-rollup-freshness.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/check-rollup-freshness.yml) | `50 23 * * *` | Published dataset freshness and purge credentials | Repair coverage and applicability (W2, W4) | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35935813847) |
| [ci.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/ci.yml) | pull_request, push | Code and offline regression checks | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/36021702902) |
| [dedupe-comments-catalog.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/dedupe-comments-catalog.yml) | `0 9 * * 0` | Raw comments duplicate audit and rebuild | Make scheduled runs audit-only; repair recovery (W1) | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35501408829) |
| [deploy-docs.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/deploy-docs.yml) | push, workflow_run, workflow_dispatch | Data dictionary and GitHub Pages | Keep; separate schema-check result (W5) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/36021703441) |
| [etl-new-pipeline.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/etl-new-pipeline.yml) | `25 6 * * *`; `25 18 * * *` | Mirrulations incremental ingestion into catalog and public data | Keep; complete downstream chain (W3) | [queued](https://github.com/mikewolfd/spicy-regs/actions/runs/36021389999) |
| [integration.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/integration.yml) | `0 12 * * 1` | Live Mirrulations extract, staging and merge checks | Keep; add isolated catalog recovery coverage | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35598012534) |
| [materialize-rulemaking.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/materialize-rulemaking.yml) | `0 3 * * *` | Source-linked rulemaking tables | Keep; pin completed parents (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35949892228) |
| [publish-comments-mirror.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/publish-comments-mirror.yml) | `30 21 * * *` | Catalog export for anonymous Parquet readers | Keep; coordinate writers and validate conservation (W2, W3) | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35922973837) |
| [rollup-agency-monthly-volume.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-agency-monthly-volume.yml) | `50 22 * * *` | Document volume by agency and month | Keep; sequence after documents (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35930562385) |
| [rollup-agency-stats.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-agency-stats.yml) | `40 22 * * *` | Agency statistics from base data and comment index | Keep; sequence after base publication (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35929968080) |
| [rollup-amendments.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-amendments.yml) | `40 2 * * *` | Congressional amendments and relationships | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35948915726) |
| [rollup-bill-family.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-bill-family.yml) | `0 2 * * *` | Canonical bill family and derived bill outputs | Keep sole bill writer; verify fixed next run | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35946820267) |
| [rollup-bill-subjects.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-bill-subjects.yml) | `15 21 * * *` | Bill policy areas and subjects | Keep; consume completed bill family | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35933694138) |
| [rollup-cfr-sections.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-cfr-sections.yml) | `30 20 * * *` | Code of Federal Regulations sections | Keep edition-aware refresh | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35940291235) |
| [rollup-committee-meetings.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-committee-meetings.yml) | `30 6 * * *` | Congressional committee meetings | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35966423551) |
| [rollup-committee-reports.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-committee-reports.yml) | `40 3 * * *` | Committee reports and hearing family | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35952885353) |
| [rollup-committee-rosters.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-committee-rosters.yml) | `40 1 * * *` | Committees and roster assignments | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35945032241) |
| [rollup-court-citations.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-court-citations.yml) | `50 4 2,9 1,4,7,10 *` | Opinion citations, citation map and parentheticals | Keep; coordinate court edition refresh | no separate run |
| [rollup-court-opinion-clusters.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-court-opinion-clusters.yml) | `20 4 * * 1` | Court opinion clusters and source links | Keep; coordinate court edition refresh | [cancelled](https://github.com/mikewolfd/spicy-regs/actions/runs/35561659245) |
| [rollup-courtlistener.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-courtlistener.yml) | `30 18 * * *` | Court dockets and grouping metadata | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35903761395) |
| [rollup-crs-reports.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-crs-reports.yml) | `30 17 * * *` | Congressional Research Service reports | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35896233638) |
| [rollup-discovery-signals.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-discovery-signals.yml) | `20 23 * * *` | Signals derived from regulatory documents | Keep; sequence after documents (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35933303219) |
| [rollup-docket-search.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-docket-search.yml) | `0 23 * * *` | Browser docket search index | Keep; sequence after dockets (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35931938804) |
| [rollup-fcc-filings.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-fcc-filings.yml) | `40 18 * * *` | FCC proceeding filings | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35904925854) |
| [rollup-fcc-proceedings.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-fcc-proceedings.yml) | `10 18 * * *` | FCC proceeding inventory | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35902677861) |
| [rollup-fec-committees.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-fec-committees.yml) | `30 19 * * *` | Federal Election Commission committees | Keep; address rate limiting | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35910579310) |
| [rollup-fec-observations.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-fec-observations.yml) | workflow_dispatch | Observations from explicitly retained FEC inputs | Keep manual; acquisition remains separate | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35668878697) |
| [rollup-fec-source-catalog.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-fec-source-catalog.yml) | workflow_dispatch | Pinned SpicyDocs FEC source inventory | Keep manual or trigger on provider inventory update | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35668293889) |
| [rollup-federal-register.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-federal-register.yml) | `0 21 * * *` | Federal Register documents | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35920682059) |
| [rollup-feed-summary.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-feed-summary.yml) | `30 22 * * *` | Docket feed summary | Keep; sequence after base publication (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35928819178) |
| [rollup-fr-docket-links.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-fr-docket-links.yml) | `30 23 * * *` | Federal Register to docket links | Keep; sequence after Federal Register (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35934269024) |
| [rollup-gao-reports.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-gao-reports.yml) | `0 17 * * *` | Government Accountability Office reports | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35893426558) |
| [rollup-house-communications.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-house-communications.yml) | `40 6 * * *` | House communications | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35967007384) |
| [rollup-laws.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-laws.yml) | `0 1 * * *` | Laws and statutory source family | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35942931112) |
| [rollup-lifecycles.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-lifecycles.yml) | workflow_dispatch | Withdrawn heuristic rulemaking lifecycle table | Retire workflow; already disabled (W5) | [disabled; last success](https://github.com/mikewolfd/spicy-regs/actions/runs/35796811168) |
| [rollup-lobbying-filings.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-lobbying-filings.yml) | `0 19 * * *` | Bounded lobbying disclosure updates | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35906852371) |
| [rollup-member-vote-terms.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-member-vote-terms.yml) | `50 3 * * *` | Votes attributed to member terms | Keep; wait for votes and members (W3) | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35953351660) |
| [rollup-members.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-members.yml) | `20 3 * * *` | Congressional members and terms | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35952052955) |
| [rollup-nominations.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-nominations.yml) | `20 6 * * *` | Nominations and source detail | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35966095643) |
| [rollup-org-committee-links.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-org-committee-links.yml) | `30 20 * * *` | Comment organizations linked to FEC committees | Keep; declare mirror dependency (W3) | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35917126704) |
| [rollup-press-releases.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-press-releases.yml) | `20 2 * * *` | Congressional press releases | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35948126373) |
| [rollup-print-citations.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-print-citations.yml) | `0 7 * * *` | Congressional print citation family | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35969126987) |
| [rollup-record-issues.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-record-issues.yml) | `0 6 * * *` | Congressional Record issues | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35962957319) |
| [rollup-roll-call-votes.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-roll-call-votes.yml) | `0 3 * * *` | Roll-call votes and member votes | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35950660696) |
| [rollup-sam-entities.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-sam-entities.yml) | `0 19 * * *` | Bounded SAM entity inventory | Keep; verify repaired source request | [failure](https://github.com/mikewolfd/spicy-regs/actions/runs/35907412992) |
| [rollup-senate-expenditures.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-senate-expenditures.yml) | `30 7 * * 2` | Senate expenditure records | Keep periodic source refresh | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35700647260) |
| [rollup-treaties.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-treaties.yml) | `10 6 * * *` | Treaties and source detail | Keep | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35965369178) |
| [rollup-unified-agenda.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-unified-agenda.yml) | `0 20 * * *` | Retained Unified Agenda edition | Keep edition-aware refresh | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35914846843) |
| [rollup-usaspending-recipients.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/rollup-usaspending-recipients.yml) | `0 18 * * *` | USAspending recipient metadata | Keep; monitor population conservation | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/35901494454) |
| [seed-comments-catalog.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/seed-comments-catalog.yml) | workflow_dispatch | Initialize or recover comments catalog from published data | Keep manual recovery utility | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/36019107910) |
| [seed-dockets-catalog.yml](https://github.com/mikewolfd/spicy-regs/blob/cded33db60ab09ed4ebde4307f3f6c7c4c25de84/.github/workflows/seed-dockets-catalog.yml) | workflow_dispatch | Initialize or recover dockets catalog from published data | Keep manual recovery utility | [success](https://github.com/mikewolfd/spicy-regs/actions/runs/36018620120) |


## Decision and verification order

1. Make scheduled deduplication audit-only and isolate its write probe; prove recovery before restoring automatic repair.
2. Repair misleading health results, including the unnecessary purge failure and withdrawn-table schema abort.
3. Qualify the active ETL sweep and the next repaired source runs; record the actual public generations.
4. Connect dependent refreshes to completed parent publication and add anonymous readback.
5. Add bounded document enrichment, court edition refresh and MCP deployment only for their named delivery goals.

The existing separation of source jobs and the common rollup runner is worth preserving: deleting source jobs would reduce coverage, while a single giant scheduled workflow would couple unrelated source outages and credentials. Conversely, keeping every current schedule unchanged leaves the reproduced recovery and monitoring defects in place. The recommended changes preserve useful independent acquisition while making dependent results and maintenance verifiable.

The review follows the existing delivery decisions: scheduled publication and source qualification remain separate (decision 3), the lifecycle heuristic stays withdrawn (decision 4), opinion bodies remain source links (decision 6), and the bill family retains one owner (decision 31). It adds no automatic source-qualification gate.

## Retained evidence

Machine-local receipts are under [workflow-audit-2026-09-24](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/workflow-audit-2026-09-24/):

- `local-inventory.json`: parsed workflow events, jobs, commands and concurrency.
- `github-workflows.json`: registered workflows and recent run metadata.
- `failed-run-evidence.json` and per-job logs: failed steps and source errors.
- `dedupe-recovery-reproduction.json`: interruption, retained sibling, false clean audit and lossy core retry.
- `comments-monitor-reproduction.json`: raw duplicate plus same-month coverage gap missed by the checker.
- `wrangler-verification.json`: read-only checks of the account's catalog, public serving path and configured Worker.

The [earlier catalog-count receipt](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/ledger-continuation-2026-09-24/etl-review-catalog-counts.json) records the then-current raw catalog uniqueness audit. The same receipt directory retains `dictionary-live-check.log`, which shows the withdrawn-table 404. These receipts distinguish local control-flow reproductions, live service observations and recommendations.
