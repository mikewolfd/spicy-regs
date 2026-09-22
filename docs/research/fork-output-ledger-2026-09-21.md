# Fork output ledger — September 21, 2026

This dated ledger accounts for every declared rollup output plus base regulatory, comments and rulemaking outputs at code `ea64719`. It records 44 producer paths and 82 distinct output keys, including dynamic partition patterns. The narrow bills writer shares the ordinary bill family.

The [consolidated backlog](../fork-generation.md) remains the task list. [Machine-readable evidence](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/output-ledger.json) records each candidate path, measured size/count/schema, source assessment, scope, required and optional inputs, observed publication pins, next action and receipt. A missing pin or unresolved scope remains a blocker. Existing populations must survive narrower repairs.

Public data destination: `https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev`. Local candidates are not fork publications. Verified selections include FEC, members/terms, nominations, treaties, the five-member report family, repaired dockets/documents, docket search, two document-derived tables, Appropriations press-feed windows and the retained Agenda edition. CFR has a verified bounded correction; broader populations remain open as listed. Laws and committee rosters are published but still await source qualification.

| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T13 | `run-rollup-court-opinion-clusters` | `court_opinion_clusters.parquet` | verified bulk rebuild active; output qualification pending |
| T13 | `run-rollup-court-opinion-bodies` | `court_opinion_bodies.parquet` | bulk acquisition/verification active; rollup not qualified |
| T16 | `run-rollup-bill-subjects` | `bill_subjects.parquet` | waiting for qualified parents |
| T15 | `run-rollup-feed-summary` | `feed_summary.parquet` | waiting for qualified parents |
| T15 | `run-rollup-agency-stats` | `agency_stats.parquet` | waiting for qualified parents |
| T15 | `run-rollup-agency-monthly-volume` | `agency_monthly_volume.parquet` | generated and verified |
| T15 | `run-rollup-docket-search` | `docket_search.json.gz` | generated and verified |
| T15 | `run-rollup-lifecycles` | `rulemaking_lifecycles.parquet` | blocked on pairing semantics |
| T15 | `run-rollup-discovery-signals` | `discovery_signals.parquet` | generated and verified |
| T15 | `run-rollup-fr-docket-links` | `fr_docket_links.parquet` | published awaiting parent audit |
| T11 | `run-rollup-cfr-sections` | `cfr_sections.parquet` | published bounded correction verified |
| T16 | `run-rollup-congress-bills` | `congress_bills.parquet` | waiting for qualified parents |
| T11 | `run-rollup-unified-agenda` | `unified_agenda.parquet` | generated and verified for retained edition |
| T11 | `run-rollup-federal-register` | `federal_register.parquet` | published awaiting source audit |
| T12 | `run-rollup-fcc-proceedings` | `fcc_proceedings.parquet` | local candidate needs qualification |
| T12 | `run-rollup-fcc-filings` | `fcc_filings.parquet` | local candidate needs qualification |
| T14 | `run-rollup-sam-entities` | `sam_entities.parquet` | withdrawn blocked |
| T14 | `run-rollup-lobbying-filings` | `lobbying_filings.parquet` | local candidate needs qualification |
| T04 | `run-rollup-fec-committees` | `fec_committees.parquet` | generated and verified |
| T04 | `run-rollup-fec-source-catalog` | `fec_source_catalog.parquet` | generated and verified |
| T04 | `build-fec-observations` | `fec_source_records.parquet` | generated and verified |
| T04 | `build-fec-observations` | `fec_collections.parquet` | generated and verified |
| T04 | `build-fec-observations` | `fec_relationships.parquet` | generated and verified |
| T15 | `run-rollup-org-committee-links` | `org_committee_links.parquet` | waiting for qualified parents |
| T12 | `run-rollup-gao-reports` | `gao_reports.parquet` | local candidate needs qualification |
| T12 | `run-rollup-crs-reports` | `crs_reports.parquet` | local candidate needs qualification |
| T12 | `run-rollup-courtlistener` | `court_dockets.parquet` | bulk acquisition/verification active; rollup not qualified |
| T12 | `run-rollup-usaspending-recipients` | `usaspending_recipients.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `congress_bills.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_actions.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_committees.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_publisher_summaries.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_versions.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_sections.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `section_diffs.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `section_diff_items.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `financial_changes.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `section_classifications.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_summaries.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `diff_summaries.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `cbo_cost_estimates.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `public_activity_events.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_family_archives.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_vote_references.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_family_backfills.parquet` | local candidate partially source-qualified |
| T09 | `run-rollup-bill-family` | `bill_family_backfill_walks.parquet` | local candidate partially source-qualified |
| T08 | `run-rollup-press-releases` | `press_releases.parquet` | generated and verified for captured feed windows |
| T08 | `run-rollup-amendments` | `amendments.parquet` | local candidate needs qualification |
| T08 | `run-rollup-roll-call-votes` | `roll_call_votes.parquet` | local candidate needs qualification |
| T08 | `run-rollup-roll-call-votes` | `member_votes.parquet` | local candidate needs qualification |
| T08 | `run-rollup-members` | `members.parquet` | generated and verified |
| T08 | `run-rollup-members` | `member_terms.parquet` | generated and verified |
| T10 | `run-rollup-committee-reports` | `committee_reports.parquet` | generated and verified |
| T10 | `run-rollup-committee-reports` | `report_sections.parquet` | generated and verified |
| T10 | `run-rollup-committee-reports` | `hearing_transcripts.parquet` | generated and verified |
| T10 | `run-rollup-committee-reports` | `hearing_bill_links.parquet` | generated and verified |
| T10 | `run-rollup-committee-reports` | `committee_report_reads.parquet` | generated and verified |
| T08 | `run-rollup-print-citations` | `house_activity_reports.parquet` | local candidate needs qualification |
| T08 | `run-rollup-print-citations` | `budget_volumes.parquet` | local candidate needs qualification |
| T08 | `run-rollup-print-citations` | `bill_committee_actions.parquet` | local candidate needs qualification |
| T08 | `run-rollup-print-citations` | `document_citations.parquet` | local candidate needs qualification |
| T08 | `run-rollup-senate-expenditures` | `senate_expenditures.parquet` | local candidate needs qualification |
| T08 | `run-rollup-laws` | `laws.parquet` | published awaiting source audit |
| T08 | `run-rollup-laws` | `law_code_sections.parquet` | published awaiting source audit |
| T08 | `run-rollup-laws` | `table3_records.parquet` | published awaiting source audit |
| T08 | `run-rollup-committee-rosters` | `committees.parquet` | published awaiting source audit |
| T08 | `run-rollup-committee-rosters` | `committee_assignments.parquet` | published awaiting source audit |
| T08 | `run-rollup-house-communications` | `house_communications.parquet` | local candidate needs qualification |
| T08 | `run-rollup-committee-meetings` | `committee_meetings.parquet` | local candidate needs qualification |
| T08 | `run-rollup-record-issues` | `record_issues.parquet` | local candidate needs qualification |
| T08 | `run-rollup-treaties` | `treaties.parquet` | generated and verified |
| T08 | `run-rollup-nominations` | `nominations.parquet` | generated and verified |
| T06 | `run-pipeline` | `dockets.parquet` | generated and verified |
| T06 | `run-pipeline` | `documents.parquet` | generated and verified |
| T06 | `run-pipeline` | `comments_index.parquet` | local candidate needs qualification |
| T06 | `run-pipeline` | `comments/agency_code=<agency>/docket_id=<docket>/year=<year>/month=<month>/part-0.parquet` | local undated cohort; wider partitions unproduced |
| T07 | `publish-comments-mirror.yml` | `comments.parquet` | six-agency repair candidate; full parent not admitted |
| T07 | `publish-comments-mirror.yml` | `comments/by-agency/<agency>.parquet` | waiting for qualified comments parent |
| T17 | `materialize-rulemaking` | `rule_targets.parquet` | waiting for qualified parents |
| T17 | `materialize-rulemaking` | `proceedings.parquet` | waiting for qualified parents |
| T17 | `materialize-rulemaking` | `regulatory_agenda_items.parquet` | waiting for qualified parents |
| T17 | `materialize-rulemaking` | `agenda_item_proceedings.parquet` | waiting for qualified parents |
| T17 | `materialize-rulemaking` | `comment_periods.parquet` | waiting for qualified parents |

## Scope and evidence gaps

- FEC covers the pinned catalog, retained 2024/2026 committee traversal and 649 explicitly selected collections. Broader individual records, committee history and correction streams remain T05.
- Full docket/document repairs and docket search are published. All 670 selected source releases replayed; newer and unrelated prior observations survive. This is metadata repair: all document body-text, extraction-status and PDF-extraction-evidence values remain null; acquiring and extracting bodies remains open. The full 23,889,661-row comments object is now retained locally, but three raw witnesses still demonstrate omitted attachments/names/organizations; the current extraction-evidence column is missing. The separate 112,861-group index totals 23,888,128 comments, 1,533 below the independently pinned full object; a common generation is unproved. Comments remain unadmitted. Full reconciliation now identifies 1,555 native NULL-date rows omitted by the old partition rule, offset by 22 surplus counts in three older index groups. All 1,555 originals were acquired and match the parent. Standard Hive null partitions now retain those rows; the local full index conserves every parent row. Wider source-field repairs and the complete dated partition tree remain open.
- Nominations (2,204) and treaties (2) are published with public-byte and MCP verification at their declared 119th Congress scopes. Record issues still lack 360 paired detail bodies; the other T08 families retain their source/schema/field gaps.
- Bill-body retry/budget handling is implemented and manually audited across four retained-fixture runs. Independent review found and repaired missing-version/diff-child checkpoints and stale child rows after successful correction. Exact successful scopes now replace their children while failed, capped and unrelated scopes survive. Full 18-table coverage, older backfills and model retry/access remain open. All five report-family members are rebuilt and published for exactly 118 selected packages; one hearing is a fresh source observation with changed bytes.
- CFR failure paths refuse partial output. Its title 14, volume 4 correction preserves 318,063 unrelated rows and verifies 1,444 repaired rows; source ancestry and other packages remain open. CRS/FCC/GAO/USAspending refusal fixes do not qualify their full populations. CourtListener now uses the source-owned strict reader. A full keyless attempt stopped at HTTP 429 after 34 retained pages/680 records; an exact failure replay preserved prior output. Full scope, usable rate/access budgets and ordinary successful-source retention remain open. The retained Agenda edition now passes full native-field qualification; FR still needs it.
- SAM remains withdrawn and disabled pending a qualified initial load; lobbying remains paused. Missing source-specific access and resumable retained acquisition remain explicit.
- Derived tables need verified parent pins. Monthly volume and discovery are published from the exact repaired parent with accounting/as_of metadata. Discovery's reviewed UTC correction is published and passes independent full-parent counting plus both MCP read paths. Explicit source offsets survive; offset-free dates use UTC, with the policy stored in Parquet metadata. Lifecycles needs a defined pairing/unknown-docket policy: 19 prior null-docket groups collapsed unrelated proposals and 647 groups were lost when the earliest final predated the earliest proposal. The ledger adds the hidden public-comments dependency of organization links and the complete-family publication prerequisite of the narrow bill writer.

The continuation's [independent review reports](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/reviews) and exact raw/output receipts are linked from the machine ledger. See `discovery-utc/`, `bill-resume-fix-audit/`, `full-comments/reconciliation/` and `courtlistener-refusal/` under the execution receipt directory. These repairs do not mark the unfinished populations complete.

## Latest continuation

- **Members/terms:** generation `sha256:a0140c2f993e032ab53ceec6cd6e57de4b0dda8048ee3ece259d7e009ff8e758` publishes 12,770 legislators and 45,535 terms from both complete September 22 UTC community crosswalk captures. All 639,675 declared cells agree with raw JSON, prior identities/native values survive, and public bytes plus both actual MCP modes pass. The independent literal FEC join agrees on all 1,126 candidate/committee/member links, covering 889 legislators. This is not official-roster completeness; 29 native within-term party histories are not represented by the single-party term value. See `members-qualification/` and its independent review reports.
- **Laws/rosters:** scheduled generations `sha256:8a79810a8dfdfcd77928ff6a03b39f48c3e40caad09a6274695c77185d9b0f3d` and `sha256:bb48f2816894bf67ad7176aa6ec3ceb6d9f6aa4b2eab95db6e48b94bb61dd3d2` are publicly downloadable. Exact byte/digest/schema/count checks pass for 113 laws, 3,655 law/code rows, 65 Table III rows, 236 committees and 2,966 assignments. Native source/population audits remain open. These outputs are **published awaiting source audit**, not qualified deliverables. See `congressional-status/`.
- **Bill family:** the ordinary owner replay now produces a local 18-output candidate with 419,839 bill rows, retaining every prior identity and unrelated value, held body record and diff. It replays all 16,213 receipt-pinned 118th HR/S source records. Independent review accepts partial native qualification: 1,178,929 exact native comparisons pass; 11,021 URLs are absent in raw BILLSTATUS and inherited from the same prior bill identity, with proven lineage. The full-native equality verdict remains **FAIL**. Broader source provenance, original text retention, missing bodies, models and historical backfills remain open; nothing from this family is published. The prior 119th population has 22,064 printing records but only 600 marked captured, and retained 118th HR/S has 19,687 with four marked captured. These flags do not establish possession or new qualification of all original bodies: none of the 40 known retained XML originals matches those 604 marked-printing digests. See `bill-family-continuation/`.
- **Votes:** the local House candidates contain 577 roll calls and 249,099 member-vote rows; a fresh audit is active. A selection defect is confirmed in code: acquisition starts from bill references, so a missing legislative reference can exclude a procedural vote. The retained log's 676 listing entries do not yet prove 99 unique omitted votes. Raw listing/body retention, Senate scope and member/bill joins require qualification before publication or full-history scorecard claims.
- **Press releases:** generation `sha256:c3c056ff09697aca92a3c10ab6544114212862aa06a861420cbf4bbe51d759ec` serves 28 rows from complete September 19 and September 22 UTC House/Senate Appropriations feed windows. All 952 mapped-field comparisons, public bytes and both actual MCP modes pass. Three rotated-out items survive. Optional bill links remain NULL; the old possessive-`s 2027` false match remains a documented interpretation defect for T16.
- **Unified Agenda:** the existing `sha256:ea589343f8dcb5ffe134c5a2ac2fbf5d8856f105f296d838fe59e6330ee075b2` generation passes all 67,218 mapped-field comparisons against the complete retained 202510 XML. Actual replay is byte-identical; public and MCP reads agree. This qualifies the retained edition, without a latest-edition or historical-series claim.
- **Comments:** six complete agency source cohorts yield a local 23,889,665-row candidate: all original parent identities plus four BOP records. Every selected native field matches, all 23,862,187 unrelated rows survive unchanged, and the independently reconciled index conserves every row including 1,555 unknown dates. Wider source repair and physical partitions remain open. ACF's fully enumerated next cohort contains 129,052 source comment objects; it has not been acquired in this batch.
- **CourtListener acquisition:** initial loading prioritizes the complete June 30, 2026 main export plus unique supplements; historical duplicates remain catalogued. Of the 46 selected objects, 45 passed full-file SHA-256 and exact source ETag verification. The original downloader exited before finishing the 54.6 GB opinions object, and its verifier stopped with that input pending. The old multithread partial is retained. A detached sequential transfer resumes only its own partial with conditional requests; `opinions-resume-state.json` records progress. Its reviewed completion check requires the exact opinions entry, selection digest, size, SHA and source/computed ETags in `verified-manifest.json`; a verifier process exiting normally is insufficient. See `opinions-resume-launch-postfix.json` and the independent transfer review. No court rollup has been published from these bytes.
- **Court clusters:** a durable worker is streaming the complete verified docket file into a source-audited court map. Its dependent worker waits for that qualified map, then rebuilds the full cluster edition using pinned local cluster/court files. Local controls and both worker plans passed independent review; the host gate passes 2,403 tests. All 10,070,727 retained cluster identities and 36 old fields must be compared before qualification. The existing generic incremental merge has schema/freshness limitations; this edition build bypasses it. Full real output qualification and publication remain pending. See `courtlistener-clusters-qualification/HANDOFF.md`.
- **CourtListener source behavior:** the retained search walk stops at 55 pages/1,100 unique rows. Its reported count of 7,811 is approximate. The corrected provider and host preserve exact-count checks for small docket selections and opinions, and require a completed cursor walk for large docket selections. The independently approved correction is installed through the pinned SpicyDocs 0.26.1 wheel; full provider/host gates and offline raw/output replay pass. Bulk mapping preserves RECAP source bitmasks and descriptive nature-of-suit text. Standard exports omit party/attorney relationship tables; the dated bulk selection is not assumed equal to the current search index.

The [current parallel work](../fork-generation.md#current-parallel-work) assigns
integration, bills, court acquisition/builds, vote auditing and independent
review. Its September 22, 2026, 02:35 UTC checkpoint distinguishes completed
reviews from unfinished transfers and publication. Package integration remains
qualified at `b50a9eb`. Source CI fixes at `51c87a1` and isolated branch
`b0a8e6e` passed GitHub checks; they leave the installed wheel unchanged. The
machine ledger links the same receipts and records the next action per output.

Receipts: `members-qualification/`, `congressional-status/`, `bill-family-continuation/`, `press-release-qualification/`, `unified-agenda-qualification/`, `full-comments/source-campaign/`, `courtlistener-bulk/` and `courtlistener-clusters-qualification/` under the linked execution directory. Independent reviews state the exact approved scope and remaining limits.
