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
| T16 | `run-rollup-bill-subjects` | `bill_subjects.parquet` | published 2026-09-23: generation `86137cc2…`, the retained 20,013-bill enrichment (GovInfo BILLSTATUS, enriched 2026-08-22); every bill exists in the reconciled family, its 5,965 BILLSTATUS-bearing bills agree on every policy area and all but one subject set, and 25 random others match raw BILLSTATUS exactly; later runs extend it |
| T15 | `run-rollup-feed-summary` | `feed_summary.parquet` | published 2026-09-23: generation `1a833d52…`, 279,124 dockets; every comment count and document date equals a direct recount from the parents |
| T15 | `run-rollup-agency-stats` | `agency_stats.parquet` | published 2026-09-23: generation `d4fa809a…`, 316 agencies; every count equals a direct recount |
| T15 | `run-rollup-agency-monthly-volume` | `agency_monthly_volume.parquet` | generated and verified |
| T15 | `run-rollup-docket-search` | `docket_search.json.gz` | generated and verified |
| T15 | `run-rollup-lifecycles` | `rulemaking_lifecycles.parquet` | withdrawn 2026-09-23 (decision 4): the 2026-09-22 scheduled run had published generation `2f001194…` (26,519 rows); the family was conditionally removed from the index, the workflow disabled and unscheduled; pairing semantics remain blocked |
| T15 | `run-rollup-discovery-signals` | `discovery_signals.parquet` | generated and verified |
| T15 | `run-rollup-fr-docket-links` | `fr_docket_links.parquet` | qualified 2026-09-23 against the audited parent: generation `92f99b00…` (899,227 rows) equals a separate re-derivation from `federal_register` `731984ca…` row for row |
| T11 | `run-rollup-cfr-sections` | `cfr_sections.parquet` | published bounded correction verified; the current code's rule (`cac7615`) would null part and citation on 253,758 of 319,507 rows, most of them correct, so it is not applied further (details below) |
| T16 | `run-rollup-congress-bills` | `congress_bills.parquet` | the narrow writer's table is the reconciled family's `congress_bills` (`a846cb44…`); its workflow is disabled until `6d34a1b` (url_source) is pushed |
| T11 | `run-rollup-unified-agenda` | `unified_agenda.parquet` | generated and verified for retained edition |
| T11 | `run-rollup-federal-register` | `federal_register.parquet` | source-audited 2026-09-23: generation `731984ca…` (1,009,005 rows, 1994-01-03 to 2026-09-22); every month's rows equal the publisher's facet and 74 whole days match in identity and every cell (details below) |
| T12 | `run-rollup-fcc-proceedings` | `fcc_proceedings.parquet` | published 2026-09-23 after a source replay validated against raw pages: generation `a1116f71…`, 21,683 docket names from 21,691 ECFS documents; walked whole, one row per docket (details below) |
| T12 | `run-rollup-fcc-filings` | `fcc_filings.parquet` | published 2026-09-23 after a source replay validated against raw pages: generation `c0c1aa4e…`, 5,137 filings received 2026-08-24 to 09-22 (the bounded first run); incremental from here |
| T14 | `run-rollup-sam-entities` | `sam_entities.parquet` | bounded initial load published 2026-09-23 (decision 10): generation `56dd0f65…`, all 147,254 active registrations dated 2026 from one retained bulk extract, cell-for-cell equal to it; the fork workflow stays disabled until the fixes are pushed and `SAM_API_KEY` is set |
| T14 | `run-rollup-lobbying-filings` | `lobbying_filings.parquet` | local candidate needs qualification |
| T04 | `run-rollup-fec-committees` | `fec_committees.parquet` | generated and verified |
| T04 | `run-rollup-fec-source-catalog` | `fec_source_catalog.parquet` | generated and verified |
| T04 | `build-fec-observations` | `fec_source_records.parquet` | generated and verified |
| T04 | `build-fec-observations` | `fec_collections.parquet` | generated and verified |
| T04 | `build-fec-observations` | `fec_relationships.parquet` | generated and verified |
| T15 | `run-rollup-org-committee-links` | `org_committee_links.parquet` | published 2026-09-23: generation `7dd4e95a…`, 1,114 name-matched links; every committee and comment count verified (the match itself is a stated heuristic with confidence tiers) |
| T12 | `run-rollup-gao-reports` | `gao_reports.parquet` | generated and verified |
| T12 | `run-rollup-crs-reports` | `crs_reports.parquet` | published 2026-09-23 after a source replay validated against raw pages: generation `6c15aac2…`, 14,137 reports (details below) |
| T12 | `run-rollup-courtlistener` | `court_dockets.parquet` | generated and verified: enriched APA selection published (11,459 rows) |
| T12 | `build_court_docket_groups` | `court_docket_groups.parquet` | republished 2026-09-23 with numeric parent order: generation `0f855eb1…`, 901 rows; raw-validated against the native docket edition and public readback identical |
| T12 | `run-rollup-usaspending-recipients` | `usaspending_recipients.parquet` | published by schedule; qualified 2026-09-23 with recorded source drift against a fresh capture (details below) |
| T09 | `run-rollup-bill-family` | `congress_bills.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_actions.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_committees.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_publisher_summaries.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_versions.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_sections.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `section_diffs.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `section_diff_items.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `financial_changes.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `section_classifications.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_summaries.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `diff_summaries.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `cbo_cost_estimates.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `public_activity_events.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_family_archives.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_vote_references.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_family_backfills.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T09 | `run-rollup-bill-family` | `bill_family_backfill_walks.parquet` | published 2026-09-23 (decision 1) in reconciled generation `a846cb44…`: the accepted 419,839-bill candidate plus the live scheduled 119th rows; conservation and row provenance verified (details below) |
| T08 | `run-rollup-press-releases` | `press_releases.parquet` | published bounded relationship correction verified |
| T08 | `run-rollup-amendments` | `amendments.parquet` | republished complete 2026-09-23: generation `52d60a8f…`, 7,066 = the source's declared 119th count; every list-route cell matches a clean replay; detail-only fields (sponsors, amended bill) remain unacquired |
| T08 | `run-rollup-member-vote-terms` | `member_vote_terms.parquet` | added and published 2026-09-23 (decision 2): generation `890481eb…`, 382,136 rows; exceptions equal the independent join replay row for row |
| T08 | `run-rollup-roll-call-votes` | `roll_call_votes.parquet` | native fields verified; derived bill links qualified 2026-09-23: in live generation `3204b8fc…` all 845 links are recorded votes in their bill's raw GovInfo BILLSTATUS (509 bills) and in the published references, no vote is claimed by two bills, and none of the 730 unmatched votes is named by any reference |
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
| T08 | `run-rollup-laws` | `laws.parquet` | qualified 2026-09-23 by clean source replay: all 113 laws match in every native cell; only capture times differ |
| T08 | `run-rollup-laws` | `law_code_sections.parquet` | qualified 2026-09-23 by clean source replay: all 3,655 rows match in every native cell |
| T08 | `run-rollup-laws` | `table3_records.parquet` | qualified 2026-09-23 by clean source replay: all 65 rows match in every native cell |
| T08 | `run-rollup-committee-rosters` | `committees.parquet` | qualified 2026-09-23 by clean source replay: all 236 committees match; only publisher activity counts grew since publication |
| T08 | `run-rollup-committee-rosters` | `committee_assignments.parquet` | qualified 2026-09-23 by clean source replay: all 2,966 assignments match in every native cell; decision 5: House select aliases live, 28 seats stay unlisted for want of a publisher link (documented) |
| T08 | `run-rollup-house-communications` | `house_communications.parquet` | generated and verified; 15 publisher-withdrawn identities |
| T08 | `run-rollup-committee-meetings` | `committee_meetings.parquet` | generated and verified; 4 rows carry post-publication publisher updates |
| T08 | `run-rollup-record-issues` | `record_issues.parquet` | generated and verified |
| T08 | `run-rollup-treaties` | `treaties.parquet` | generated and verified; new pin re-qualified |
| T08 | `run-rollup-nominations` | `nominations.parquet` | generated and verified; new pin re-qualified |
| T06 | `run-pipeline` | `dockets.parquet` | generated and verified |
| T06 | `run-pipeline` | `documents.parquet` | generated and verified |
| T06 | `run-pipeline` | `comments_index.parquet` | published 2026-09-23 with the comments cohorts: live `ba31f99f…`, 112,885 groups summing to all 23,890,403 comments |
| T06 | `run-pipeline` | `comments/agency_code=<agency>/docket_id=<docket>/year=<year>/month=<month>/part-0.parquet` | local undated cohort; wider partitions unproduced |
| T07 | `publish-comments-mirror.yml` | `comments.parquet` | six agencies plus ACF published 2026-09-23 (decision 7), each cohort after independent review: live `fca7afb7…`, 23,890,403 rows; further agencies continue by cohort |
| T07 | `publish-comments-mirror.yml` | `comments/by-agency/<agency>.parquet` | waiting for qualified comments parent |
| T17 | `materialize-rulemaking` | `rule_targets.parquet` | bootstrapped 2026-09-23 as snapshot `snapshot_0e799850…` (517,311 rows across 142,525 dockets) against the five audited parents; references, intervals and sampled source rows verified (details below) |
| T17 | `materialize-rulemaking` | `proceedings.parquet` | bootstrapped 2026-09-23 as snapshot `snapshot_0e799850…` (515,121 rows) against the five audited parents; references, intervals and sampled source rows verified (details below) |
| T17 | `materialize-rulemaking` | `regulatory_agenda_items.parquet` | bootstrapped 2026-09-23 as snapshot `snapshot_0e799850…` (38,403 items) against the five audited parents; references, intervals and sampled source rows verified (details below) |
| T17 | `materialize-rulemaking` | `agenda_item_proceedings.parquet` | bootstrapped 2026-09-23 as snapshot `snapshot_0e799850…` (155,628 relationships) against the five audited parents; references, intervals and sampled source rows verified (details below) |
| T17 | `materialize-rulemaking` | `comment_periods.parquet` | bootstrapped 2026-09-23 as snapshot `snapshot_0e799850…` (306,582 periods) against the five audited parents; references, intervals and sampled source rows verified (details below) |

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
- **Congress.gov source audits (laws, committee rosters, amendments).** Each family
  was rebuilt from its live source with no published prior (R2 unset), then
  compared with the live generation by contract identity, cell by cell. Laws
  (113), Code sections (3,655), Table III (65) and committee assignments (2,966)
  match in every native cell; only capture times differ. All 236 committees match
  except publisher activity counts that grew (15 bill counts; none fell).
  Amendments exposed a completeness defect: an offset walk over `updateDate`
  order repeats about as many records as it skips, so it met the declared count by
  rows while holding 7,013 distinct of 7,066; the live table was 52 short. The
  builder now pools alternating-order passes until the distinct count reaches the
  declared count (`00f1144`), and the complete table (generation `52d60a8f…`) is
  a strict superset of the old one with no shared cell changed; three added
  amendments were confirmed on the detail route. Detail-only fields (sponsors,
  amended bill) are empty on every row because the list route does not carry them.
  See `source-audit-2026-09-23/`.
- **USAspending recipients.** A fresh capture of the same top-100-page selection
  through the builder's reader (raw pages retained) against generation
  `18e51cf5…`, whose public bytes match the index: 9,740 of 10,000 recipients
  remain in today's ranking, with UEI and level identical for all, DUNS for
  9,731 and name for 9,730 (15 publisher updates: cleared DUNS, punctuation, one
  renamed recipient); 3,787 all-time amounts moved as awards accrued; 260 left
  the top ranks and 114 entered. The ranking itself drifts during a walk (146 ids
  seen twice on 2026-09-23 06:40 UTC), which the builder refuses rather than
  publishing. See `usaspending-qualification/`.
- **CRS reports and FCC proceedings and filings published (T12).** All three
  2026-09-22 scheduled runs had refused on the source's own data. Each table was
  replayed from its live source with no prior (R2 unset) and compared with raw
  pages walked separately, restating the column mapping from the publisher's
  field names:
  - **CRS reports.** All 14,137 ids and cells match, except 15 `update_date`
    values the publisher re-stamped between the two reads (03:08 and 03:23 UTC).
  - **FCC filings.** All 5,137 ids and cells match.
  - **FCC proceedings.** All 21,676 single-document docket names match cell for
    cell.
  - **Causes.**
    - CRS and filings repeated an identity within one offset walk; the list
      shifts while it is read, the amendments defect. `pool_passes` now pools
      passes until the distinct count reaches the publisher's count; amendments
      moved onto it.
    - ECFS states no total, but every response carries term aggregations. A
      window's count is its `express_comment` (filings) or `bureau_name`
      (proceedings) buckets plus the records lacking the field, which matched
      every walked window exactly.
    - ECFS proceedings are not unique by `name`. One 2017 document has no name
      and is left out. Seven dockets hold two documents: 13-84, 15-91, 15-94 and
      02-378 were re-created on 2026-09-21 as sparser copies, and 21-62, 24-89
      and 25-12 differ in closing or status. The table keeps the original
      docket, then the last-edited document; the validation confirms it chose
      the original for all seven.
    - Proceedings are now walked whole every run, because a creation-date
      increment never refreshed closings and would publish a lone re-created
      document over its original.
  - **Status.** Code `b2534aa`. Generations `6c15aac2…`, `a1116f71…` and
    `c0c1aa4e…` are live, with public bytes equal to the validated files. The
    fork's scheduled workflows run the pushed code, so they keep refusing until
    this commit is pushed. See `local-candidates-2026-09-23/`.
- **Federal Register source audit (T11) and FR docket links (T15).**
  - **Pin.** The live generation `731984ca…`'s public bytes match its pin
    (1,009,005 rows, 1994-01-03 to 2026-09-22).
  - **Completeness.** The publisher's facet endpoint states true counts where
    the list endpoint caps at 10,000. All 393 months' row counts equal the
    monthly facet, and the total equals it (1,009,005).
  - **Identity and cells.** 74 whole publication days, 60 random (seed 20260923)
    and the last 14, were fetched with raw pages kept and mapped from the
    publisher's field names: identity sets and every cell match, 8,683 documents
    with JSON strings byte-exact.
  - **Derived columns.** `rin` equals the array's first element on every row.
    `modify_date` is null on every row, because the REST API does not expose it.
    474 numbers appear on two dates, the builder's intended (number, date) key.
  - **FR docket links.** Generation `92f99b00…` equals a separate re-derivation
    from this parent (Python `json`, not DuckDB `UNNEST`) as a row multiset in
    both directions: 899,227 rows, all 16 columns.
  - **Dictionary.** The stale "2000 floor" coverage on both tables is corrected.
    See `fr-audit-2026-09-23/`.

- **Rulemaking dataset bootstrapped (T17).** With `federal_register` and
  `fr_docket_links` audited, all five parents qualified. The manifest pins them
  exactly: FR `47ad1212…`, links `2c5f941a…`, agenda `52775a37…`, and the
  qualified dockets `27a2ed4a…` and documents `7907ebe3…` base objects.
  - **Date defect fixed.** The first local build exposed one: the builder read
    regulations.gov comment-window instants by their UTC date, but every end
    stamp is 11:59:59 PM Eastern (03:59:59/04:59:59Z). Every document-sourced
    `close_date` was therefore one day late: over the 133,006 documents that
    also carry an FR `comments_close_on`, the Eastern day matched for 127,361
    and the UTC date for 118. Fixed in `0a898be` (actor `comment-periods:v5`).
  - **Stage profiled.** `rule_targets` took 8.5 minutes, quadratic
    re-serialization of each edge's references. `e8d3882` brings it to
    38 seconds with byte-identical output.
  - **Rebuild checks.** In the rebuild every check is zero: primary-id
    duplicates; unresolved docket, RIN, FR-record, proceeding and agenda-item
    references; FR evidence lacking the RIN it claims; inverted or unanchored
    periods. Each period's bounds equal its evidence's earliest open and latest
    close, recomputed in SQL with the Eastern rule.
  - **Live-source samples.** 30 of 30 document periods match regulations.gov,
    30 of 30 FR periods and 23 of 23 agenda-RIN links match federalregister.gov.
  - **Publication.** The validated files were published through the pipeline's
    own gate and publish step, with no rebuild. The public pointer, manifest
    and five artifacts equal them.
  - **Fork workflows.** `Materialize — rulemaking join surface` and
    `Rollup — fcc_proceedings` are disabled on the fork. Their pushed code
    predates `0a898be` and `b2534aa` and would republish the one-day-late
    closes, and the ECFS re-created dockets over their originals. Re-enable both
    after pushing. See `rulemaking-2026-09-23/`.
- **Vote terms (decision 2).** `member_vote_terms` assigns every `member_votes`
  row the term it counts toward: half-open `term_start <= vote_day < term_end`,
  then a unique inclusive end, with the term type following the chamber and a
  Senate LIS id resolved through `members`. Built from the published votes
  (`3204b8fc…`), members and terms, it has 382,136 rows: 382,118 half-open, 15
  inclusive-end and 3 unmatched (`G000578` on 2025-01-03, `S001157` twice on
  2026-04-22, all native `Not Voting`). The non-half-open rows equal the
  independent replay's 18 exceptions, and all 1,855 boundary-day House rows
  take the new term. Generation `890481eb…` equals the validated build (`c113329`).
  Its fork workflow exists only once pushed. See `vote-terms-2026-09-23/`.
- **Comments cohort and T15 rollups (decision 7).** The six-agency candidate
  (DOL, FMC, ITA, BOP, ONCD, PCSCOTUS) was approved by independent review
  (`reviews/comments-six-agency-review.md`). On 2026-09-23, 300 random rows
  re-matched their raw originals on all twelve native fields. The fork bucket
  held no comments objects, so this was a first publication. `comments.parquet`
  (`a72a08e9…`, 23,889,665 rows) and `comments_index.parquet` (`28e5cf9c…`) are
  live, with S3 read-back and public download digests equal to the reviewed
  files. The mirror workflow still fails daily for want of its catalog and does
  not overwrite them. That unblocked three T15 rollups, each rebuilt and
  recounted independently from the parents:
  - **`feed_summary`** (`1a833d52…`): comment counts per docket equal a direct
    count over `comments.parquet`, not the index, and document dates equal
    `documents`.
  - **`agency_stats`** (`d4fa809a…`): every docket, document and comment count
    matches; the totals are 279,124, 2,001,531 and 23,889,665.
  - **`org_committee_links`** (`7dd4e95a…`): every committee resolves with its
    stated name, and every organization's comment and docket counts match.

  80 comments in five dockets absent from `dockets` are counted by agency but
  cannot appear in `feed_summary`, which lists dockets. See
  `full-comments/publication-comments.json` and `t15-2026-09-23/`.
- **Bill family (decision 1).** Generation `a846cb44…` publishes all eighteen
  family tables and 419,866 bills.
  - **Inputs.** It reconciles the reviewed 419,839-bill candidate
    (`bill-family-continuation/`) with the live scheduled generation `83d20e77…`
    (18,998 mostly 119th-Congress bills). The reconciliation used the family's
    own merge helpers and parameters, with the candidate as prior and the live
    rows as fresh. At day grain, no live row is older than its candidate
    counterpart: 1,839 are newer and 17,132 same-day rows are identical in
    content. (A lexical comparison had flagged 132, which were timestamp against
    date-only spellings of the same day.)
  - **Checks, every table.** No duplicate keys. Every candidate and every live
    identity survives, and no row appears in neither input. Every live-keyed row
    equals the live row, and every candidate-only row equals the candidate's.
    The one exception is 1,752 `congress_bills` rows, where live NULLs were
    filled from the candidate by the column-wise merge (1,749 of them `url`).
  - **URL labels.** SpicyDocs 0.28.0 appends `congress_bills.url_source`, the
    inherited-provenance label the decision requires (`6d34a1b`). 12,767 URLs are
    labelled `inherited`: 11,018 of the 11,021 lineage bills, plus the 1,749 live
    rows above. The other three lineage bills were restated by the live narrow
    writer on 2026-09-18, so they are fresh statements. 5,005 are labelled
    `billstatus`, matching the raw archives. Rows that predate the label are NULL.
  - **Publication.** All 18 public members equal the reconciled files.
  - **Fork workflows.** `Rollup — bill family` and `Rollup — congress_bills`
    are disabled on the fork until `6d34a1b` is pushed. Their pushed contract
    lacks `url_source`, and their merge drops prior columns it does not know.
    See `bill-family-publication-2026-09-23/`.
- **SAM initial load (decision 10).** A one-record request confirmed that the
  workspace SAM key reaches the Entity API: 788,978 active registrations.
  Retained raw responses then exposed four defects in the bulk-extract path,
  each fixed in SpicyDocs and adopted here:
  - **Filters dropped.** httpx `params=` replaced the trigger's query, so every
    selection filter was lost and the API answered its unfiltered default page
    (0.28.0).
  - **Wrong trigger shape.** The trigger answers a plain-text sentence, not JSON
    with a count (0.28.0).
  - **In-progress read as refusal.** A generating file answers HTTP 400 with
    `errorCode` `FSP` (0.28.0).
  - **Wrong identity and count.** Registrations are keyed by UEI and EFT
    indicator: 187 UEIs in 2026 carry several. The file is written while
    registrations change, so its declared count is a floor: 147,250 declared,
    147,256 rows, 147,254 registrations, two held twice (0.28.2; host `ebf5c7e`
    adds `entity_eft_indicator` and a composite merge key).

  The bounded load is every active registration dated 2026: one retained
  71.5 MB extract (`sam-initial-load-2026-09-23/raw/`, plus a 508-record one-day
  extract that matched its paged count exactly). The table equals an
  independent mapping of the raw file in all 147,254 keys and every cell.
  Generation `56dd0f65…` is live, and its public bytes equal the build.
- **ACF comments cohort (decision 7).**
  - **Capture.** All 129,052 comment objects in the complete ACF listing were
    captured (380,321,010 bytes), and admitted as 128,965 records with 87
    discarded observations.
  - **Admission fix.** The first admission refused on
    `ACF-2026-0199-0536`: two byte-identical Mirrulations refetch files at one
    `modifyDate`. A census found 23 such groups, all identical and none
    differing, so SpicyDocs 0.28.1 (`6df4ae2`) extended the docket/document
    rule to comments: identical records collapse, differing ones still refuse.
  - **Repair.** The native source fills fields the parent held as NULL on
    108,369 existing rows, and adds 738 comments in `ACF-2026-0595`. No existing
    value changed, no row was lost, and every other row is preserved.
  - **Review.** Independent review (`reviews/comments-acf-review.md`) approved.
    It checked all 129,052 objects against S3's own checksums, all 23,761,438
    other rows by whole-row hash, and every ACF row against its newest original.
  - **F1, acted on.** 0.28.1 had changed the comment policy without moving its
    version, so current readers refused the published six-agency 1.2 releases
    with a bare digest error. 0.28.3 (`8474b5c`) moves comments to policy 1.3;
    replaying a 1.2 release needs the retained 0.28.0 wheel. Under 1.3, ACF
    re-admitted to byte-identical staging, and the rebuilt candidate and index
    equal the reviewed ones as whole-row hash multisets (merge output layout is
    not byte-stable).
  - **F2, recorded.** 161 ACF comments exist on Mirrulations only as
    `_UNAVAILABLE` markers, so the cohort is complete over the JSON Mirrulations
    serves.
  - **Publication.** `comments.parquet` (`fca7afb7…`) and `comments_index`
    (`ba31f99f…`) replaced the six-agency objects after confirming the live
    bytes were still those.
  - **T15 refresh.** The three T15 tables were rebuilt and recounted on the new
    parent, with zero mismatches: `feed_summary` `4c2e2cd9…`, `agency_stats`
    `a9b3d6fc…`, `org_committee_links` `e3d05e5b…` (1,186 links).
    `feed_summary`'s ordering broke `modify_date` ties arbitrarily, so equal
    inputs gave different bytes; `87cf257` orders by `docket_id` as well. See
    `full-comments/acf-campaign/`.
- **CFR part ancestry (T11): not extended; the current rule regresses.**
  - **What the rule does.** Since `cac7615`, the builder takes no part from a
    section granule id ("a section token alone does not establish its enclosing
    part"). Re-applying that rule to every row of the live `cfr_sections`
    (`8fb97150…`) changes 253,758 of 319,507 rows across 260 packages, nulling
    part and `cfr_ref`.
  - **Why that is wrong.** Most of those citations are correct. GovInfo's
    granule summary states the literal designation: `CFR-2026-title10-vol2-sec100-1`
    has `granuleNumber` `§ 100.1` (part 100), while
    `CFR-2025-title14-vol4-sec19-8-1` has `19-8.1`, no `§` and no part. The
    title-14 correction was right for that volume and wrong as a general rule.
  - **What a source-backed rule needs.** The granule list does not carry
    `granuleNumber`, and the id cannot tell the two shapes apart. Getting it
    means one summary request per granule (about 64 hours at the key's rate) or
    parsing each volume's XML section numbers (about 260 packages).
  - **Hazard.** Until then, a scheduled rebuild with the current code would
    publish nulls where the table now holds correct parts. `Rollup —
    cfr_sections` is therefore disabled on the fork: its pushed code includes
    `cac7615`, and its last run, on 2026-09-22, failed.
