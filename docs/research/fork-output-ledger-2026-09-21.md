# Fork output ledger — September 21, 2026

This dated ledger accounts for every declared rollup output plus base regulatory, comments and rulemaking outputs at code `ea64719`. It records 44 producer paths and 82 distinct output keys, including dynamic partition patterns. The narrow bills writer shares the ordinary bill family.

The [consolidated backlog](../fork-generation.md) remains the task list. [Machine-readable evidence](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/output-ledger.json) records each candidate path, measured size/count/schema, source assessment, scope, required and optional inputs, observed publication pins, next action and receipt. A missing pin or unresolved scope remains a blocker. Existing populations must survive narrower repairs.

Public data destination: `https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev`. Local candidates are not fork publications. Verified selections include FEC, nominations, treaties, the five-member report family, repaired dockets/documents, docket search and two document-derived tables. CFR has a verified bounded correction; broader populations remain open as listed.

| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T13 | `run-rollup-court-opinion-clusters` | `court_opinion_clusters.parquet` | local candidate needs qualification |
| T13 | `run-rollup-court-opinion-bodies` | `court_opinion_bodies.parquet` | local candidate needs qualification |
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
| T11 | `run-rollup-unified-agenda` | `unified_agenda.parquet` | published awaiting source audit |
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
| T12 | `run-rollup-courtlistener` | `court_dockets.parquet` | local candidate needs qualification |
| T12 | `run-rollup-usaspending-recipients` | `usaspending_recipients.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `congress_bills.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_actions.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_committees.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_publisher_summaries.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_versions.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_sections.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `section_diffs.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `section_diff_items.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `financial_changes.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `section_classifications.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_summaries.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `diff_summaries.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `cbo_cost_estimates.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `public_activity_events.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_family_archives.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_vote_references.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_family_backfills.parquet` | local candidate needs qualification |
| T09 | `run-rollup-bill-family` | `bill_family_backfill_walks.parquet` | local candidate needs qualification |
| T08 | `run-rollup-press-releases` | `press_releases.parquet` | local candidate needs qualification |
| T08 | `run-rollup-amendments` | `amendments.parquet` | local candidate needs qualification |
| T08 | `run-rollup-roll-call-votes` | `roll_call_votes.parquet` | local candidate needs qualification |
| T08 | `run-rollup-roll-call-votes` | `member_votes.parquet` | local candidate needs qualification |
| T08 | `run-rollup-members` | `members.parquet` | local candidate needs qualification |
| T08 | `run-rollup-members` | `member_terms.parquet` | local candidate needs qualification |
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
| T08 | `run-rollup-laws` | `laws.parquet` | local candidate needs qualification |
| T08 | `run-rollup-laws` | `law_code_sections.parquet` | local candidate needs qualification |
| T08 | `run-rollup-laws` | `table3_records.parquet` | local candidate needs qualification |
| T08 | `run-rollup-committee-rosters` | `committees.parquet` | local candidate needs qualification |
| T08 | `run-rollup-committee-rosters` | `committee_assignments.parquet` | local candidate needs qualification |
| T08 | `run-rollup-house-communications` | `house_communications.parquet` | local candidate needs qualification |
| T08 | `run-rollup-committee-meetings` | `committee_meetings.parquet` | local candidate needs qualification |
| T08 | `run-rollup-record-issues` | `record_issues.parquet` | local candidate needs qualification |
| T08 | `run-rollup-treaties` | `treaties.parquet` | generated and verified |
| T08 | `run-rollup-nominations` | `nominations.parquet` | generated and verified |
| T06 | `run-pipeline` | `dockets.parquet` | generated and verified |
| T06 | `run-pipeline` | `documents.parquet` | generated and verified |
| T06 | `run-pipeline` | `comments_index.parquet` | local candidate needs qualification |
| T06 | `run-pipeline` | `comments/agency_code=<agency>/docket_id=<docket>/year=<year>/month=<month>/part-0.parquet` | local undated cohort; wider partitions unproduced |
| T07 | `publish-comments-mirror.yml` | `comments.parquet` | full parent retained not admitted |
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
- CFR failure paths refuse partial output. Its title 14, volume 4 correction preserves 318,063 unrelated rows and verifies 1,444 repaired rows; source ancestry and other packages remain open. CRS/FCC/GAO/USAspending refusal fixes do not qualify their full populations. CourtListener now uses the source-owned strict reader. A full keyless attempt stopped at HTTP 429 after 34 retained pages/680 records; an exact failure replay preserved prior output. Full scope, usable rate/access budgets and ordinary successful-source retention remain open. Current FR/Agenda byte checks are not native-source qualification.
- SAM remains withdrawn and disabled pending a qualified initial load; lobbying remains paused. Missing source-specific access and resumable retained acquisition remain explicit.
- Derived tables need verified parent pins. Monthly volume and discovery are published from the exact repaired parent with accounting/as_of metadata. Discovery's reviewed UTC correction is published and passes independent full-parent counting plus both MCP read paths. Explicit source offsets survive; offset-free dates use UTC, with the policy stored in Parquet metadata. Lifecycles needs a defined pairing/unknown-docket policy: 19 prior null-docket groups collapsed unrelated proposals and 647 groups were lost when the earliest final predated the earliest proposal. The ledger adds the hidden public-comments dependency of organization links and the complete-family publication prerequisite of the narrow bill writer.

The continuation's [independent review reports](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/reviews) and exact raw/output receipts are linked from the machine ledger. See `discovery-utc/`, `bill-resume-fix-audit/`, `full-comments/reconciliation/` and `courtlistener-refusal/` under the execution receipt directory. These repairs do not mark the unfinished populations complete.
