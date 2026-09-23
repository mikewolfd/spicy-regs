# Fork output ledger — September 21, 2026

This dated ledger accounts for every declared rollup output plus base regulatory, comments and rulemaking outputs at code `ea64719`. It records 44 producer paths and 82 distinct output keys, including dynamic partition patterns. The narrow bills writer shares the ordinary bill family.

The [consolidated backlog](../fork-generation.md) remains the task list. [Machine-readable evidence](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/output-ledger.json) records each candidate path, measured size/count/schema, source assessment, scope, required and optional inputs, observed publication pins, next action and receipt. A missing pin or unresolved scope remains a blocker. Existing populations must survive narrower repairs.

Public data destination: `https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev`. Local candidates are not fork publications. Retained qualified generations include FEC, members/terms, nominations, treaties, the five-member report family, repaired dockets/documents, docket search, two document-derived tables, Appropriations press-feed windows, the retained Agenda edition and complete June 30 court clusters. CFR has a verified bounded correction; broader populations remain open as listed. The scheduled bill family, laws, committee rosters and amendments are published but still await source qualification. Committee meetings, house communications, record issues, print citations, Senate expenditures and GAO reports were published by scheduled runs and are now source-qualified; the qualification receipts are in `scheduled-published-qualification/` under the execution receipts. Native votes and member votes are qualified for the captured selection; derived vote-to-bill fields still depend on an unqualified source parent. The latest press generation has a verified bounded relationship correction.

| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T13 | `run-rollup-court-opinion-clusters` | `court_opinion_clusters.parquet` | generated and verified for complete 2026-06-30 edition |
| T13 | `run-rollup-court-opinion-bodies` | `court_opinion_bodies.parquet` | withdrawn 2026-09-22 and removed 2026-09-23: builder, rollup, workflow and catalog/MCP registration deleted and the fork workflow disabled; text links out through the clusters' `absolute_url` |
| T13 | `run-rollup-court-citations` | `court_citations.parquet`, `court_citation_map.parquet`, `court_parentheticals.parquet` | added and published 2026-09-22 for the complete 2026-06-30 edition: generation `f1e2e523…`; public row counts verified |
| T13 | `run-rollup-court-opinions` | `court_opinions.parquet` | added and published 2026-09-22 for the complete 2026-06-30 edition (text-free): generation `f7cc67cc…`; public row counts verified; no scheduled workflow (54.6 GB source) |
| T16 | `run-rollup-bill-subjects` | `bill_subjects.parquet` | waiting for qualified parents |
| T15 | `run-rollup-feed-summary` | `feed_summary.parquet` | waiting for qualified parents |
| T15 | `run-rollup-agency-stats` | `agency_stats.parquet` | waiting for qualified parents |
| T15 | `run-rollup-agency-monthly-volume` | `agency_monthly_volume.parquet` | generated and verified |
| T15 | `run-rollup-docket-search` | `docket_search.json.gz` | generated and verified |
| T15 | `run-rollup-lifecycles` | `rulemaking_lifecycles.parquet` | withdrawn 2026-09-23 (decision 4): the 2026-09-22 scheduled run had published generation `2f001194…` (26,519 rows); the family was conditionally removed from the index, the workflow disabled and unscheduled; pairing semantics remain blocked |
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
| T12 | `run-rollup-gao-reports` | `gao_reports.parquet` | generated and verified |
| T12 | `run-rollup-crs-reports` | `crs_reports.parquet` | local candidate needs qualification |
| T12 | `run-rollup-courtlistener` | `court_dockets.parquet` | generated and verified: enriched APA selection published (11,459 rows) |
| T12 | `build_court_docket_groups` | `court_docket_groups.parquet` | republished 2026-09-23 with numeric parent order: generation `0f855eb1…`, 901 rows; raw-validated against the native docket edition and public readback identical |
| T12 | `run-rollup-usaspending-recipients` | `usaspending_recipients.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `congress_bills.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_actions.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_committees.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_publisher_summaries.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_versions.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_sections.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `section_diffs.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `section_diff_items.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `financial_changes.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `section_classifications.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_summaries.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `diff_summaries.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `cbo_cost_estimates.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `public_activity_events.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_family_archives.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_vote_references.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_family_backfills.parquet` | published awaiting source audit; broader local candidate retained |
| T09 | `run-rollup-bill-family` | `bill_family_backfill_walks.parquet` | published awaiting source audit; broader local candidate retained |
| T08 | `run-rollup-press-releases` | `press_releases.parquet` | published bounded relationship correction verified |
| T08 | `run-rollup-amendments` | `amendments.parquet` | published awaiting source audit |
| T08 | `run-rollup-roll-call-votes` | `roll_call_votes.parquet` | native fields verified; derived bill links await parent source audit |
| T08 | `run-rollup-roll-call-votes` | `member_votes.parquet` | generated and verified for frozen 119th Congress selection |
| T08 | `run-rollup-members` | `members.parquet` | generated and verified; fresh retained source observations |
| T08 | `run-rollup-members` | `member_terms.parquet` | generated and verified; fresh retained source observations |
| T10 | `run-rollup-committee-reports` | `committee_reports.parquet` | generated and verified for selected reports |
| T10 | `run-rollup-committee-reports` | `report_sections.parquet` | generated and verified; same bytes and report population |
| T10 | `run-rollup-committee-reports` | `hearing_transcripts.parquet` | published; native content verified, capture provenance open |
| T10 | `run-rollup-committee-reports` | `hearing_bill_links.parquet` | generated and verified for selected root MODS COVER scope |
| T10 | `run-rollup-committee-reports` | `committee_report_reads.parquet` | published; native content verified, capture provenance open |
| T08 | `run-rollup-print-citations` | `house_activity_reports.parquet` | generated and verified; replayed byte-identical |
| T08 | `run-rollup-print-citations` | `budget_volumes.parquet` | generated and verified; replayed byte-identical |
| T08 | `run-rollup-print-citations` | `bill_committee_actions.parquet` | generated and verified; replayed byte-identical |
| T08 | `run-rollup-print-citations` | `document_citations.parquet` | generated and verified; replayed byte-identical |
| T08 | `run-rollup-senate-expenditures` | `senate_expenditures.parquet` | generated and verified; replayed byte-identical |
| T08 | `run-rollup-laws` | `laws.parquet` | published awaiting source audit |
| T08 | `run-rollup-laws` | `law_code_sections.parquet` | published awaiting source audit |
| T08 | `run-rollup-laws` | `table3_records.parquet` | published awaiting source audit |
| T08 | `run-rollup-committee-rosters` | `committees.parquet` | published awaiting source audit |
| T08 | `run-rollup-committee-rosters` | `committee_assignments.parquet` | published awaiting source audit |
| T08 | `run-rollup-house-communications` | `house_communications.parquet` | generated and verified; 15 publisher-withdrawn identities |
| T08 | `run-rollup-committee-meetings` | `committee_meetings.parquet` | generated and verified; 4 rows carry post-publication publisher updates |
| T08 | `run-rollup-record-issues` | `record_issues.parquet` | generated and verified |
| T08 | `run-rollup-treaties` | `treaties.parquet` | generated and verified; new pin re-qualified |
| T08 | `run-rollup-nominations` | `nominations.parquet` | generated and verified; new pin re-qualified |
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

The [05:30 UTC status snapshot](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/status-20260922T0530-summary.json)
records 20 generated-and-verified outputs, two verified bounded corrections,
29 published outputs awaiting source/parent audit, 15 local candidates and 16
other unfinished outputs. These are 82 distinct output keys, not percentages
of work or historical coverage. The fork serves 51 outputs: 48 in the current
managed publication index and three base objects with retained public-byte audits.
The current index matches the newly reviewed member, report and vote generations.

The 17:13 UTC recheck against the live index supersedes those counts. The
index now serves 26 families with 57 table objects, plus the three base
objects. Six former local candidates were published without qualification
receipts: `gao_reports` (25 rows), `committee_meetings` (2,753),
`house_communications` (4,964), the four `print-citations` tables,
`record_issues` (363) and `senate_expenditures` (2,623). The later
qualification audit (18:10 UTC, `scheduled-published-qualification/`) now
qualifies all eleven objects: public-byte verification passes; fresh
source-owned captures reproduce every published cell that still exists in the
current source (treaties 48/48, record issues 6,171/6,171, nominations
28,652/28,652, GAO 200/200, house communications 158,368/158,368,
committee meetings 74,314/74,331); the two PDF rollups replay
byte-identically through the source-owned pipeline against the fork's own
prior tables; and actual stdio MCP reads pass in both download and direct-R2
modes for all eleven tables. Recorded drift: 15 house-communication
identities the publisher withdrew after the run, four meetings carrying
additive publisher updates, plus growth rows the publisher added since. The
re-published nominations (2,204) and treaties (2) pins are thereby
re-qualified. CourtListener bulk acquisition is complete:
all 46 selected objects passed full-file SHA-256 and source ETag verification
(67.3 GB), zero pending, acceptance audit passed. The private native docket
cache finished at 06:06 UTC with a PASS verdict — 71,677,647 rows, all IDs
unique, semantic digest matching the expected value. The later court-dockets
delivery (generation `sha256:cbb9924411268bae16211aa476004c565dc3a530d93de6741dfb517130a1db54`)
publishes the enriched APA-899 selection: all 7,766 retained search rows
cell-identical plus 3,693 bulk additions (APA-classified rows in all three
publisher spellings, and same-case sibling records of retained cases gated on
name/date agreement). 31 sealed and 18 docket-number-reuse rows are refused
and witnessed; 12 prior sealed rows are preserved as captured.
Parties/attorneys/firms stay NULL on bulk-derived rows. Public download and
both actual MCP modes verify 11,459 rows. A derived `court_docket_groups`
side table (generation `sha256:32fdbde6041d5aa6b2a5ca19e206a2f7d42beda8bd356c5d2225373c8a9b769f`,
901 rows) maps the same-case doppeldocket/refiling groups: 403 parents and
498 child links with a confidence tier, parent meant to be the lowest
pacer_case_id among published group members because the publisher's own
parent_docket_id is blank in the edition. That generation compared the ids as
strings, so 21 groups (67 rows) carry a different parent; the repository's
`build_court_docket_groups` compares them numerically and otherwise reproduces
every value (candidate `independent-rederivation/corrected-groups-candidate/`,
SHA-256 `54b38474…`), awaiting republication. Public download and both MCP
modes verify the published bytes; every parent id resolves in court_dockets.
About 730 selected rows are criminal, magistrate, petty-offense or
miscellaneous dockets the publisher codes 899 (383 from the prior search
selection, 382 added); excluding non-civil docket types is an open scope
decision. Receipts in `court-dockets-qualification/`; reviews in
`reviews/court-dockets-enrichment-review.md` (approve-with-findings, findings
fixed) and `reviews/court-dockets-independent-addendum.md` (independent
re-derivation: enrichment approved exactly, groups parent change requested).

- **Actual scheduled evidence:** ordinary fork runs `35689287614` and `35689357725` now pass complete source, public-byte and both actual MCP-mode audits, with independent approval. Members generation `sha256:c4b489004884825c499e885a6453290bf6c0957adbbaad5b9feb0faa1ad9b4a1` freshly qualifies all 12,770 members and 45,535 terms. Report generation `sha256:f5f169482bfd2903155ebe109268d52fa9be6081061193f980bcd4c537ab129a` qualifies all 105 reports, 1,246 sections and the empty root MODS COVER link selection across all 19 hearing parents. Original responses and evidence are retained on the fork. The 19 inherited hearing capture/checkpoint clocks remain unqualified; their native content remains verified. Earlier missing capture evidence is not recreated. See `scheduled-retention-live-qualification/final-qualification.json` and its independent reviews.
- **Remote court output:** the implementation passes focused/full host checks and an isolated real-storage input-to-publication-to-readback experiment. Independent review approves code integration and bounded experiments. Commit `413b3ab` includes bounded row/payload batching. A retained-row layout experiment and independent review verify reduced footer overhead and unchanged values; full host and dictionary gates pass. Opinion bodies were withdrawn on 2026-09-22, so no full opinion execution is planned; the reviewed remote path remains available for other large outputs. The experiment creates no real publication-index entry. See `court-body-remote-probe/` and `reviews/remote-generations-independent-review.md`.
- **Native docket cache:** the reviewed private build completed the full verified 71,677,647-row source while preserving all 54 literal columns, with a PASS verdict, all IDs unique and the semantic digest matching the expected value. Host mapping, prior-population reconciliation and public delivery remain open. See `court-dockets-qualification/`.

- **Earlier members/terms qualification:** generation `sha256:a0140c2f993e032ab53ceec6cd6e57de4b0dda8048ee3ece259d7e009ff8e758` publishes 12,770 legislators and 45,535 terms from both complete September 22 UTC community crosswalk captures. All 639,675 declared cells agree with raw JSON, prior identities/native values survive, and public bytes plus both actual MCP modes pass. The independent literal FEC join agrees on all 1,126 candidate/committee/member links, covering 889 legislators. This is not official-roster completeness; 29 native within-term party histories are not represented by the single-party term value. See `members-qualification/` and its independent review reports.
- **Historical scheduled refresh reconciliation:** at 03:56 UTC, new member and committee-report generations replaced the previously qualified current pins: `sha256:0b0588cc96e965a914094889e23e4d4b359439317d727de05cbccdb8790c7c05` and `sha256:925b245110c59ae3a605da56c0ac95fdb79a75aa74c0ecb21868af8398bb07b5`. The earlier generations retain their qualification. Full byte/identity/field comparisons pass: every member/term native value is unchanged, and only its capture timestamp differs. All 105 report values are preserved except capture time; 1,246 sections remain byte-identical for the same report parents. All thirteen prior hearings remain unchanged, with six added hearings now matched against 24 retained source responses. All nineteen non-timestamp fields, body/text digests and event IDs agree. Literal package-root MODS inspection proves zero COVER links for all six added parents, so the expanded empty-link selection is qualified within that root-only scope. Changed capture/checkpoint metadata remains outside the earlier qualification because workflow artifacts retain invocation metadata only. The section table keeps its qualification; other current native content and selected root-COVER results are verified, while scheduled capture/checkpoint provenance remains open. See `scheduled-members-reports-audit/full-comparison.json`, `native-content-audit.json` and `congressional-status/publication-0355.json`.
- **Retained source disagreements:** hearing packages `CHRG-119hhrg63968` and `CHRG-119hhrg64154` identify Congress 119 in structured metadata but print “ONE HUNDRED EIGHTEENTH CONGRESS” in their front matter. The output preserves the structured identity and retains both observations; no inferred correction is applied. Exact source hashes and locators are in `scheduled-members-reports-audit/native-content-audit.json` and `MANUAL-AUDIT.md`.
- **Laws/rosters:** scheduled generations `sha256:8a79810a8dfdfcd77928ff6a03b39f48c3e40caad09a6274695c77185d9b0f3d` and `sha256:bb48f2816894bf67ad7176aa6ec3ceb6d9f6aa4b2eab95db6e48b94bb61dd3d2` are publicly downloadable. Exact byte/digest/schema/count checks pass for 113 laws, 3,655 law/code rows, 65 Table III rows, 236 committees and 2,966 assignments. The fresh complete chamber files now reproduce every assignment identity and 56,354 comparisons across nineteen columns; the earlier `observed_at` remains outside this audit. Every member joins the qualified crosswalk, but 138 assignments across fourteen codes lack a committee-table match. Declared conversion equality does not prove canonical committee identity. Committee list/details and law source audits remain open. These families remain **published awaiting source audit**. See `congressional-status/` and `rosters-qualification/MANUAL-AUDIT.md`.
- **Amendments:** scheduled generation `sha256:d48c2567fd20bc911664042cb4b6aba8efa54b3505fb9eb3db2525df2be0b0b4` publishes 7,014 rows. Full public bytes, schema and count agree with the index; native-field/body coverage remains unqualified. See `congressional-status/scheduled-amendments-byte-audit.json`.
- **Bill family:** the ordinary owner replay now produces a local 18-output candidate with 419,839 bill rows, retaining every prior identity and unrelated value, held body record and diff. It replays all 16,213 receipt-pinned 118th HR/S source records. Independent review accepts partial native qualification: 1,178,929 exact native comparisons pass; 11,021 URLs are absent in raw BILLSTATUS and inherited from the same prior bill identity, with proven lineage. The full-native equality verdict remains **FAIL**. Broader source provenance, original text retention, missing bodies, models and historical backfills remain open; this broad candidate remains private. A separate scheduled 119th Congress generation now publishes all 18 outputs and 18,956 bills under `sha256:15d547a92e4433896ceb015d2f05e410634556be5accd5db5a525aa9bf782ca2`; full public-byte checks pass but native source qualification does not. Reconcile that newer generation before replacing any family member. The prior 119th population has 22,064 printing records but only 600 marked captured, and retained 118th HR/S has 19,687 with four marked captured. Those 604 records represent 601 unique packages/digests; three enrolled-bill/public-law aliases share source bodies. The known 40 XML originals are different printings with no package overlap. These flags do not establish possession or new qualification of all originals. See `bill-family-continuation/`.
- **Votes:** generation `sha256:80028c183a1daceee3ac5a6caa6c27b17000abf5781bd609255f49307609fcbf` now publishes all **1,573 selected votes** and **381,936 member rows**. Complete native-field, tally/roster and prior-identity audits pass; public downloads and both actual MCP modes pass independent review. This delivers the additional 86 votes and 20,922 member rows. All 7,865 derived-link cell comparisons match the pinned bill-reference table; its original BILLSTATUS evidence remains unqualified, so `roll_call_votes` retains that parent gap. `member_votes` is generated and verified within the frozen selection. Original vote evidence is retained locally but not yet attached to the public generation. Independent full join replay confirms all voter identities resolve, with 18 half-open term-date gaps; inclusive ends leave three gaps and create 1,855 ambiguous House matches. The three remaining literal source observations say “Not Voting.” No term policy or inferred crosswalk is silently adopted. See `votes-qualification/publication.json`, `mcp-audit/summary.json`, `complete-member-join-replay.json` and their independent reviews.
- **Press releases:** generation `sha256:c3c056ff09697aca92a3c10ab6544114212862aa06a861420cbf4bbe51d759ec` serves 28 rows from complete September 19 and September 22 UTC House/Senate Appropriations feed windows. All 952 mapped-field comparisons, public bytes and both actual MCP modes pass. Three rotated-out items survive. That generation had NULL bill links. A later scheduled generation repeated the possessive-`s 2027` false match. The reviewed bounded repair `sha256:f9bc0f4cedb002c87ece6fd92d1acc9f8b8d53663f402b470dbfef8c193f57e8` removes only that relation, preserves four literal H.R. links and every other current value, and passes full public-byte and both MCP readbacks. Newer scheduled capture metadata is not freshly raw-qualified; the full-source claim remains tied to the earlier captured window union. See `press-link-repair/`.
- **Unified Agenda:** the existing `sha256:ea589343f8dcb5ffe134c5a2ac2fbf5d8856f105f296d838fe59e6330ee075b2` generation passes all 67,218 mapped-field comparisons against the complete retained 202510 XML. Actual replay is byte-identical; public and MCP reads agree. This qualifies the retained edition, without a latest-edition or historical-series claim.
- **Comments:** six complete agency source cohorts yield a local 23,889,665-row candidate: all original parent identities plus four BOP records. Every selected native field matches, all 23,862,187 unrelated rows survive unchanged, and the independently reconciled index conserves every row including 1,555 unknown dates. Wider source repair and physical partitions remain open. ACF's fully enumerated next cohort contains 129,052 source comment objects; it has not been acquired in this batch.
- **CourtListener acquisition:** initial loading prioritizes the complete June 30, 2026 main export plus unique supplements; historical duplicates remain catalogued. All 46 selected objects (67.3 GB) passed full-file SHA-256 and exact source ETag verification; the detached sequential transfer recorded in `opinions-resume-state.json` finished the 54.6 GB opinions object, and `verified-manifest.json` holds zero pending keys. The qualified cluster metadata and the APA docket selection are published. Opinion bodies were withdrawn on 2026-09-22: readers reach the text through each cluster's `absolute_url`, so the body build and its capacity plan (about 73.3 GB beyond the 100 GiB floor) no longer apply; the retained original is kept. See `courtlistener-clusters-qualification/NEXT-WORK.md`.
- **Court clusters:** **generated and verified** for the complete June 30, 2026 edition. Generation `sha256:7a2cbdb72ad6a7b9c79aa7f25dfa03c4ae3fe752cdb33900cb5d6b346d299b7c` publishes 10,070,727 unique clusters in a 3,952,823,520-byte file, SHA-256 `c4189ac700bfa4610171a0ccf1642e9f27b7fdfc19c88f801e7c2c970bb927b6`. Every prior identity and all 36 old fields are preserved. The full native source agrees, and all three added court fields match the qualified 71,677,647-row docket map and native court reference. The reviewed preflight proves the one court with blank jurisdiction has no docket references; it retains that source row. Full public-download verification and actual MCP reads in both download/direct-fork modes pass, including the count, schema and all 39 fields of six native witnesses. Independent candidate, publication and final readback reviews approve this scope. Opinion bodies are withdrawn (text links out through `absolute_url`); newer catch-up and hosted MCP deployment remain separate. See `courtlistener-clusters-qualification/` and `reviews/courtlistener-clusters-public-readback-review.md`.
- **CourtListener source behavior:** the retained search walk stops at 55 pages/1,100 unique rows. Its reported count of 7,811 is approximate. The corrected provider and host preserve exact-count checks for small docket selections and opinions, and require a completed cursor walk for large docket selections. The independently approved correction was adopted through SpicyDocs 0.26.1 and remains in 0.26.3; full provider/host gates and offline raw/output replay pass. Bulk mapping preserves RECAP source bitmasks and descriptive nature-of-suit text. Standard exports omit party/attorney relationship tables; the dated bulk selection is not assumed equal to the current search index.

The [current parallel work](../fork-generation.md#current-parallel-work) assigns
integration, bills, court acquisition/builds, vote auditing and independent
review. Its September 22, 2026, 05:30 UTC checkpoint distinguishes completed
reviews from unfinished transfers and publication. Package integration remains
qualified at `b50a9eb`. Source CI fixes at `51c87a1` and isolated branch
`b0a8e6e` passed GitHub checks. The 0.26.3 adoption is committed at `c23a15d`: the isolated source gate passes 7,330 tests and the installed host passes 2,426 tests. Only the reviewed vote reader and schema change from the prior wheel. Exact installed-byte, raw regression and independent adoption checks pass. The
machine ledger links the same receipts and records the next action per output.

Receipts: `members-qualification/`, `congressional-status/`, `native-vote-variants-adoption/`, `bill-family-continuation/`, `press-release-qualification/`, `unified-agenda-qualification/`, `full-comments/source-campaign/`, `courtlistener-bulk/` and `courtlistener-clusters-qualification/` under the linked execution directory. Independent reviews state the exact approved scope and remaining limits.

## September 23 continuation

- **Court docket groups republished.** The repository builder's rebuild equals the
  reviewed candidate exactly and differs from generation `32fdbde6…` only in the 21
  groups (67 rows) whose parent had been chosen by string order. Checked against
  the raw 71,677,647-row native docket edition: all 901 members exist; each of the
  403 groups has one court and docket number and one caption; every parent is the
  numerically lowest PACER case id; and each of the 21 old parents was the
  string-lowest. Generation `sha256:0f855eb1b094d7405eac1321b276d20f8ca114165ee4c0f09409247126082d49`
  is live and its public bytes equal the validated build. See
  `court-dockets-qualification/republish-groups/`.
- **`rulemaking_lifecycles` withdrawn (decision 4).** Its daily workflow published
  generation `sha256:2f001194cff3819621847f4b5dadb48a9592414dceca2392ffbd184b6641c975`
  (26,519 rows) at 2026-09-22 23:19 UTC. The family was removed from
  `publication.json` by an If-Match write that left the other 31 families
  unchanged and the immutable objects in place; the workflow is disabled on the
  fork and no longer scheduled in code. See `lifecycles-withdrawal/`.
- **Opinion bodies removed (decision 6).** The builder, rollup, workflow, tests and
  catalog/MCP registration are deleted; the fork workflow is disabled.
  `court_opinions` (text-free) now carries the opinion-to-decision map, and the
  generic remote writer stays for other large outputs.
- **Court citation tables and opinion index published** (rows above): generations
  `f1e2e523…` (`court_citations`, `court_citation_map`, `court_parentheticals`) and
  `f7cc67cc…` (`court_opinions`), the 2026-06-30 edition, public row counts equal
  to the builds; receipts in `court-bulk-tables/`.

