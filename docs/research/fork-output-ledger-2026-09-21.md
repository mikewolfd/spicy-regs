# Fork output ledger — September 21, 2026

This dated ledger accounts for every declared rollup output plus base regulatory, comments and rulemaking outputs at code `ea64719`. It records 44 producer paths and 82 distinct output keys, including dynamic partition patterns.

The [consolidated backlog](../fork-generation.md) remains the task list. [Machine-readable evidence](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/output-ledger.json) records each candidate path, measured size/count/schema, source assessment, scope, required and optional inputs, observed publication pins, next action and receipt. A missing pin or unresolved scope remains a blocker. Existing populations must survive narrower repairs.

- **Pins.** A row records `qualified at <pin> (<date>)` for the generation its audit covered, not a claim that the pin is live; `scripts/check_ledger_pins.py` reads the public index and reports rows whose live generation differs. Withdrawn, unproduced, blocked and known-wrong outputs carry no such phrase, and base objects outside the index (dockets, documents, docket search, comments and its index) record `verified at table digest <digest> (<date>; ETag <etag>)`, whose ETag the script compares with a HEAD of the public object (a row without the ETag is reported unpinned).
- **Names.** A generation is named by the first 8 hex digits of its `artifactDigest`; a table digest is labelled "table"; units are named (rows, distinct keys, pairs).
- **Evidence.** Every load-bearing number cites a retained script or receipt; numbers copied from a publisher's statement cite the receipt.

Public data destination: `https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev`. Local candidates are not fork publications. Retained qualified generations include FEC, members/terms, nominations, treaties, the five-member report family, repaired dockets/documents, docket search, two document-derived tables, Appropriations press-feed windows, the retained Agenda edition and complete June 30 court clusters. As of 2026-09-23 the following are also source-qualified or validated against raw data:
- **Regulatory:** the Federal Register and its docket links; the rulemaking dataset; the comments table (all 133 comment-bearing agencies, with six plus ACF re-verified and repaired from native source) with its index and the three T15 summaries.
- **Legislative:** laws; committee rosters; amendments with their sponsor and amended-bill detail; the reconciled bill family (decision 1) and its subjects; member vote terms (decision 2); roll-call bill links.
- **Courts and other sources:** the court citation tables and opinion index; CRS reports; FCC proceedings and filings; bounded SAM and lobbying loads (decision 10).

CFR's parts are placed from each volume's `PART` heading since generation `de703ffe…` (see the CFR part ancestry entry in the [execution log](fork-execution-log-2026-09-23.md)). The September 23 parsing survey found join, text and key limits in some qualified tables; each affected row names them. Committee meetings, house communications, record issues, print citations, Senate expenditures and GAO reports were published by scheduled runs and source-qualified at the generations their rows name; the qualification receipts are in `scheduled-published-qualification/` under the execution receipts. Scheduled generations newer than a row's qualified pin await audit (decision 3). Both repositories were pushed on 2026-09-23 and the workflows held for the push were re-enabled that day (decisions record, decisions 11 and 22).

## September 25 parallel audit

The [parallel audit](parallel-rollup-audit-2026-09-25.md) supersedes older
current-state wording below at the exact September 25 pins recorded in each row.
Three medium-effort auditors plus the parent regulatory replay verified public
artifacts, compared prior populations and read native inputs. Qualified scopes,
partial audits, publication-only evidence and uncomputed empty outputs remain
separate in the [normalized results](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/audit-summary.json).
The audits reproduce a new four-row Table III omission and the existing private-law
state and compiled-hearing date defects. No production repairs or publication
were performed by this audit. CRS advanced after the frozen check; its newer
generation remains unaudited.

| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T13 | `run-rollup-court-opinion-clusters` | `court_opinion_clusters.parquet` | generated and verified for complete 2026-06-30 edition; qualified at `7a2cbdb7…` (2026-09-22) |
| T13 | `run-rollup-court-opinion-bodies` | `court_opinion_bodies.parquet` | withdrawn 2026-09-22 and removed 2026-09-23: builder, rollup, workflow and catalog/MCP registration deleted, and the push deleted the fork workflow; text links out through the clusters' `absolute_url` |
| T13 | `run-rollup-court-citations` | `court_citations.parquet`, `court_citation_map.parquet`, `court_parentheticals.parquet` | added and published for the complete 2026-06-30 edition: qualified at `f1e2e523…` (2026-09-22); public row counts verified |
| T13 | `run-rollup-court-opinions` | `court_opinions.parquet` | added and published for the complete 2026-06-30 edition (text-free): qualified at `f7cc67cc…` (2026-09-22); public row counts verified; no scheduled workflow (54.6 GB source) |
| T16 | `run-rollup-bill-subjects` | `bill_subjects.parquet` | current `1b067027…` is PARTIAL (2026-09-25): 98,482 identities, all prior identities retained. Every subject set and policy area in 38,376 retained 118th/119th native records matches, as do ten additional fresh samples; older-Congress additions and wider changed-field proof remain open. The larger candidate is now published. Earlier selection qualified at `86137cc2…` (2026-09-23); its 20,013-row scope remains historical evidence. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T15 | `run-rollup-feed-summary` | `feed_summary.parquet` | qualified at `71fb5c24…` (2026-09-25): all 279,336 rows and every output field equal an independent replay from exact catch-up parents. This qualifies the computation; broader native-source qualification of those parents remains separate. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T15 | `run-rollup-agency-stats` | `agency_stats.parquet` | qualified at `85ca895c…` (2026-09-25): every count for all 316 agencies equals an independent recount from exact catch-up parents. Parent native-source limits remain separate. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T15 | `run-rollup-agency-monthly-volume` | `agency_monthly_volume.parquet` | qualified at `f6a2c06c…` (2026-09-25): all 77,963 rows match an independent replay from documents table `ff502e4b…`; the stated date policy excludes 2,022 missing dates and eight year-zero dates. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T15 | `run-rollup-docket-search` | `docket_search.json.gz` | verified at table digest `167d3fb9…` (2026-09-25; ETag `4e413386…`): all 279,318 search documents match the independent field-by-field replay from dockets table `680b86ad…`. The 18 dockets with neither title nor abstract are explicitly omitted. Parent source qualification remains separate. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T15 | `run-rollup-lifecycles` | `rulemaking_lifecycles.parquet` | withdrawn 2026-09-23 (decision 4): the 2026-09-22 scheduled run had published generation `2f001194…` (26,519 rows); the family was conditionally removed from the index. Its workflow was deleted in `5715163` on September 24; the local research producer remains and pairing semantics remain blocked |
| T15 | `run-rollup-discovery-signals` | `discovery_signals.parquet` | qualified at `e33aa262…` (2026-09-25): all nine signals match an independent replay from documents table `ff502e4b…` at the retained UTC as-of instant, `2026-09-25T10:15:37.277533+00:00`. This is the stated moving-window computation, not additional source acquisition. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T15 | `run-rollup-fr-docket-links` | `fr_docket_links.parquet` | qualified at `1bd5402a…` (2026-09-25): all 899,520 rows equal an independent multiset expansion of Federal Register `fde0d30a…`, with zero missing or extra rows. Native docket labels and their downstream normalization limits remain explicit. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T11 | `run-rollup-cfr-sections` | `cfr_sections.parquet` | qualified at `60092f2a…` (2026-09-25): all 321,010 rows have table bytes identical to the previously qualified `08e585dd…`. This carries the retained edition and prior part-ancestry/citation qualification; it is not a fresh whole-source audit. `cfr_ref` remains the printed citation and is null on the documented 7,910 range, parenthesized and typo rows (earlier receipt `cfr-replace-all-2026-09-23/`). Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T16 | `run-rollup-congress-bills` (retired, A1) | `congress_bills.parquet` | current `5990abbb…` is PARTIAL (2026-09-25); `bill-family` remains the sole writer. All 419,978 prior bill identities survive; the expanded body/section/difference population is publicly verified but not fully source-qualified. The scoped native metadata repair was qualified at `4a1949a9…` (2026-09-24). The API/BILLSTATUS freshness gap and wider history remain open; see the complete family row. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T11 | `run-rollup-unified-agenda` | `unified_agenda.parquet` | qualified at `c63a3a10…` (2026-09-25): the retained edition's 3,954 rows have table bytes identical to the previously qualified `8a22c704…`. This preserves that edition's earlier scope, not a new claim about source freshness. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T11 | `run-rollup-federal-register` | `federal_register.parquet` | qualified at `fde0d30a…` (2026-09-25): 1,009,209 rows. Every one of the 1,009,103 previously qualified rows is unchanged; all 106 additions match the September 24 native issue JSON in identity and every field. Earlier historical scope/facet checks remain in `drift-audit-2026-09-23/` and the execution log. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `run-rollup-fcc-proceedings` | `fcc_proceedings.parquet` | qualified at `930e4716…` (2026-09-25): 21,683 docket rows, with table bytes identical to the qualified predecessor. Independently remapped all 21,691 retained native documents; every output field matches. The table remains one row per docket. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `run-rollup-fcc-filings` | `fcc_filings.parquet` | qualified at `18c85718…` (2026-09-25): 5,491 filings, retaining every prior row and adding 237. Every added native field matches; the fresh incremental window closes at 724 distinct filings. History before the initial August 24 window and crowded-day recovery remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T14 | `run-rollup-sam-entities` | `sam_entities.parquet` | current `464977e4…` is PARTIAL (2026-09-25): 167,965 registrations preserve the qualified 147,254-row 2026 population and add 20,711 dated 2002. Scheduled run 36046185903 succeeded after the request repair. Three new registrations match all 19 native fields, but the complete new extract was not retained; the run artifact contains input descriptions only. Earlier extract qualified at `56dd0f65…` (2026-09-23). Identity is `(uei, entity_eft_indicator)`; wider years and complete new-extract replay remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T14 | `run-rollup-lobbying-filings` | `lobbying_filings.parquet` | qualified at `fd1794b5…` (2026-09-25): 27,931 filings; every prior cell is unchanged, and all 35 additions match native responses. The fresh 131-record window closes after resolving one extra identity by direct read. This carries the bounded July 2026 onward selection; wider history remains open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T04 | `run-rollup-fec-committees` | `fec_committees.parquet` | generated and verified; qualified at `dfda14da…` (2026-09-21) |
| T04 | `run-rollup-fec-source-catalog` | `fec_source_catalog.parquet` | generated and verified; qualified at `0570574b…` (2026-09-21) |
| T04 | `build-fec-observations` | `fec_source_records.parquet`, `fec_collections.parquet`, `fec_relationships.parquet` | generated and verified; qualified at `11bcb620…` (2026-09-21) |
| T15 | `run-rollup-org-committee-links` | `org_committee_links.parquet` | qualified at `7d6f3b3b…` (2026-09-25): all 3,245 links and all 19 fields match an independent Python replay over exact FEC committee and public comment parents. The comments file is bound by its full digest to the public receipt. These are confidence-tiered name heuristics, not verified identities or financial relationships. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `run-rollup-gao-reports` | `gao_reports.parquet` | qualified at `d87cf97b…` (2026-09-25): 42 reports. All 11 additions and two abstract changes match native RSS; the other 29 rows are unchanged. `report_type=Report` is an explicit default, not a native feed value. This qualifies the bounded feed population; broader history and routine retained source inputs remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `run-rollup-crs-reports` | `crs_reports.parquet` | qualified at `35a43885…` (2026-09-25): 14,143 reports, preserving all prior identities. Business fields of all five additions and 30 updates match native responses; 22 later source timestamp restamps remain an explicit limit. The fresh 64-record window closes. At the 17:36 UTC end check, `7076f282…` had become public and remains unaudited. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `run-rollup-courtlistener` | `court_dockets.parquet` | current `5570c1b4…` is PARTIAL (2026-09-25): 11,474 dockets, all prior identities retained, four additions and eight updates. The fresh 40-record window closes, but docket 74842896 now has a native judge value absent at publication; no run-time raw response resolves that drift. Party ordering also differs while the set agrees. Earlier selection qualified at `95f2091d…` (2026-09-23); parties, catch-up and scope limits remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `build_court_docket_groups` | `court_docket_groups.parquet` | republished with numeric parent order and qualified at `0f855eb1…` (2026-09-23): 901 rows; raw-validated against the native docket edition and public readback identical |
| T12 | `run-rollup-usaspending-recipients` | `usaspending_recipients.parquet` | current `f033c5f4…` is PARTIAL (2026-09-25): 10,218 identities, preserving all prior identities and adding 101. Fresh samples match identity fields but amounts have moved; 218 carried rows retain older amounts without a per-row observation date. `total_award_amount` is trailing-12-month spending at each recipient's last read. Earlier bounded selection qualified at `380f5b32…` (2026-09-23); exact capture-time evidence, observation dates and wider coverage remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T09 | `run-rollup-bill-family` | `congress_bills.parquet`, `bill_actions.parquet`, `bill_committees.parquet`, `bill_publisher_summaries.parquet`, `bill_versions.parquet`, `bill_sections.parquet`, `section_diffs.parquet`, `section_diff_items.parquet`, `financial_changes.parquet`, `section_classifications.parquet`, `bill_summaries.parquet`, `diff_summaries.parquet`, `cbo_cost_estimates.parquet`, `public_activity_events.parquet`, `bill_family_archives.parquet`, `bill_vote_references.parquet`, `bill_family_backfills.parquet`, `bill_family_backfill_walks.parquet` | current `5990abbb…` is PARTIAL (2026-09-25): all public family bytes, schemas, counts and prior identities pass. There are still 419,978 bills; additions include 597 versions, 6,768 sections, 125 diffs and 1,301 diff items. Bill actions, vote references and CBO estimates pass their stated source/conservation checks; new bodies, broader metadata, summaries and differences need further raw replay. Six model/backfill outputs remain verified empty and uncomputed. The scoped native metadata repair was qualified at `4a1949a9…` (2026-09-24), including the known timestamp/URL corrections and preserved broader population. Ordinary API freshness, missing bodies, models and backfill coverage remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-press-releases` | `press_releases.parquet` | qualified at `c0f3cc22…` (2026-09-25): 32 rows, retaining every prior identity. All 25 current RSS items match native content; seven historical items carry forward. Capture time, feed position and source channel freshness are separate metadata limits. Literal bill links remain bounded; history and other committees are outside scope. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-amendments` | `amendments.parquet` | current `559fd5f4…` is PARTIAL (2026-09-25): 7,100 rows, all prior identities retained, five additions and 12 changed records. Fresh checks of those 17 records expose later source differences; no exact capture-time response resolves them. Earlier detail selection qualified at `af5ef4c3…` (2026-09-24); Rules Committee amendments without native member sponsors remain explicit. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-member-vote-terms` | `member_vote_terms.parquet` | qualified at `c85e578f…` (2026-09-25): all 382,536 identities and 2,677,752 nonidentity cells match an independent join replay over exact recorded inputs. The 15 unique inclusive-end fallbacks and three unmatched post-term Not Voting positions remain explicit. No prior row changes or losses; three new votes add 300 positions. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-roll-call-votes` | `roll_call_votes.parquet`, `member_votes.parquet` | qualified at `0255d5de…` (2026-09-25): 1,579 votes and 382,536 positions. Added Senate votes 242–244 and all 300 positions match official XML; prior rows are unchanged. All 845 bill links have native bill-action references; 734 votes remain unlinked. Chamber `vote_day` is distinct from the UTC bill-action instant. Senate 212/213 retain the documented older `match_action_index` values; wider history and scorecards remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-members` | `members.parquet`, `member_terms.parquet` | qualified at `47ac33e4…` (2026-09-25): 12,770 members and 45,535 terms. Only capture times change; all 581,370 substantive cells match native JSON, and retained captures bind the frozen run. The community crosswalk retains its Ed Case special-election-term and party-history limits. Three newly present native website URLs are omitted by the current table design; official-roster completeness is not established. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T10 | `run-rollup-committee-reports` | `committee_reports.parquet` | qualified at `b188e1b3…` (2026-09-25): 137 report parts; all prior identities survive and changed native body/summary fields match the admitted source artifact `d8c48125…`. `CRPT-119srpt35` reflects a genuine publisher refresh, but its HTML still directs readers to PDF for missing text. Identity is `(package_id, part_id)`; source-faithful HTML is not full-content completeness. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T10 | `run-rollup-committee-reports` | `report_sections.parquet` | current `b188e1b3…` is PARTIAL (2026-09-25): 1,758 sections; all prior identities survive. Three rows change and one is added for `CRPT-119srpt35`. Native coordinates, inclusion and lengths pass, but exact heading/body decomposition is not independently proved. Earlier scope qualified at `52325038…` (2026-09-24), with the documented empty-heading absorption rule; changed sections need an exact replay. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T10 | `run-rollup-committee-reports` | `hearing_transcripts.parquet` | qualified at `b188e1b3…` (2026-09-25): 137 hearings; 127 prior rows are exactly unchanged, and six refreshed plus four added rows match admitted GovInfo bodies/summaries and Congress.gov details. All six formerly refused details now have retained valid responses. Publisher HTML placeholders, broader history and the two unsupported historical part IDs remain limits. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T10 | `run-rollup-committee-reports` | `hearing_bill_links.parquet` | FAILED at `b188e1b3…` (2026-09-25): all 82 COVER relationships preserve the prior population and the four additions match native MODS. `CHRG-117shrg56721` → `117-s-2792` still publishes only the first of eleven native dates as `held_date`. Preserve every date and use a scalar only when unique; invalidate affected checkpoints before reprocessing. The earlier `ef08249e…` audit covered an empty root selection, not this relationship population. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T10 | `run-rollup-committee-reports` | `committee_report_reads.parquet` | qualified at `b188e1b3…` (2026-09-25): all 273 checkpoints are complete and reconcile to admitted native evidence or unchanged qualified prior rows. The six former detail refusals now have valid retained responses. Historical parts `CHRG-79jhrg79716p11` and `p19` remain unsupported before checkpointing; full history is not established. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-print-citations` | `house_activity_reports.parquet`, `budget_volumes.parquet`, `bill_committee_actions.parquet`, `document_citations.parquet` | current `d57faac3…` is PARTIAL (2026-09-25): 40 activity reports, 28 budget volumes, 12,699 actions and 52,674 citations. Every prior cell is unchanged; all parent counts reconcile. Existing 15,749 spans replay exactly against six retained texts. Two added PDF samples and nine citation phrases pass a second extractor, but the new normalized spans and action meanings need wider replay. The held-parent grammar repair was qualified at `a5d46aa6…` (2026-09-24); new coverage is separate. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-senate-expenditures` | `senate_expenditures.parquet` | generated and verified; replayed byte-identical; qualified at `acef9fcf…` (2026-09-22) |
| T08 | `run-rollup-laws` | `laws.parquet` | FAILED at `c9d35661…` (2026-09-25): all 113 prior identities survive, but private laws `119-private-1` and `119-private-2` still claim `uslm_outcome=not_requested` after captured-but-refused USLM reads. Digest-verified prior native XML and run 35942931112's acquisition log prove the defect. Today's HTTP 200 responses were HTML, not new XML proof. Preserve acquired evidence, available fields and truthful partial/refused state without inventing a Statutes citation. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-laws` | `law_code_sections.parquet` | qualified at `c9d35661…` (2026-09-25): all 3,655 identities survive. The page correction for `(119, 2, 6)` from 19 to 24 matches the native 42 USC 250a line; other source-value fields retain the earlier qualification. Acquisition timestamps are separate from source-value proof. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-laws` | `table3_records.parquet` | FAILED at `c9d35661…` (2026-09-25): the repaired walk has published 2,977 rows, retaining all 65 earlier rows. Act 119-37 has 110 native rows but only 106 published: four meaningful code-reference rows with blank act-section labels are dropped. All published source fields replay, but source-row conservation fails. Preserve explicit missing labels; wider freshness/history and a prior-Congress cold-start seed remain separate work. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-committee-rosters` | `committees.parquet`, `committee_assignments.parquet` | qualified at `9ab5e338…` (2026-09-25): 236 committees and 2,966 assignments preserve every substantive prior cell. All 450 Senate native seat identities and member fields match fresh XML; its later file-date restamp is an explicit capture-time limit. The declared/served committee discrepancy and 28 unlisted relationships remain; House seats carry the earlier qualification. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-house-communications` | `house_communications.parquet` | current `37625a89…` is PARTIAL (2026-09-25): all 4,992 identities survive and 1,000 more list-only rows gain detail; 992 remain list-only. Twenty additions match all 640 native cells, but the other 980 detail fills were not independently replayed. Earlier scope qualified at `333e422d…` (2026-09-24); source list drift does not establish lost records or completeness. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-committee-meetings` | `committee_meetings.parquet` | qualified at `f85be7b6…` (2026-09-25): 2,765 meetings, preserving all prior identities. Three additions and five changed records match native responses in all 216 checked cells. The corrected `NoChamber` population is retained; current list/carry-forward discrepancies remain explicit source-scope limits. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-record-issues` | `record_issues.parquet` | qualified at `6b1913da…` (2026-09-25): 367 issues; the added issue matches all 17 native fields and every prior row is unchanged. This carries the recorded source selection, not complete historical coverage. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-treaties` | `treaties.parquet` | qualified at `4ccf2d7a…` (2026-09-25): both rows are exactly unchanged from the previously qualified selection. No fresh whole-source walk was performed; broader history and detail remain outside this carried scope. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-nominations` | `nominations.parquet` | qualified at `d4622c60…` (2026-09-25): all 2,214 rows are exactly unchanged from the qualified predecessor. A fresh pooled list yields 2,212 native identities plus the two retained parent identities; later native changes remain explicit and do not establish frozen-output freshness. Prior population/part reconciliation remains evidenced in `ledger-continuation-2026-09-24/nominations/`. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T06 | `run-pipeline` | `dockets.parquet` | publication verified at table digest `680b86ad…` (2026-09-25; ETag `2466cee0…`): 279,336 rows. The latest five native records match every mapped field after locating their exact numbered source captures. This is bounded source evidence; whole-population source qualification after catch-up remains PARTIAL. Earlier T06 source qualification is retained in the execution receipts. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T06 | `run-pipeline` | `documents.parquet` | publication verified at table digest `ff502e4b…` (2026-09-25; ETag `9cac7d9b…`): 2,002,562 rows. The latest five native records match every mapped field after locating their exact numbered source captures. This is bounded source evidence; whole-population source qualification, document bodies and extraction evidence remain PARTIAL. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T06 | `run-pipeline` | `comments_index.parquet` | published and verified at table digest `2a050b5e…` (2026-09-25; ETag `97417edf…`): 143,367 groups sum to 26,303,691 comments. The public monolith, agency files and raw catalog pass exact coverage and unique-ID checks. Receipt: `comments-local-export-2026-09-25/` |
| T06 | `run-pipeline` | `comments/agency_code=<agency>/docket_id=<docket>/year=<year>/month=<month>/part-0.parquet` | unproduced on the fork (probed keys answer 404); the comments deliveries wrote `comments.parquet` and its index directly |
| T07 | `publish-comments-mirror.yml` | `comments.parquet` | published by the local recovery and verified at table digest `b90e1105…` (2026-09-25; ETag `45071b2b…`): 26,303,691 rows across 180 agencies, with every previously published ID retained. The catalog export adds 2,413,288 rows to the prior public population. Public bytes, unique IDs and index coverage are verified; wider native-source qualification remains open. Earlier native and attachment-text repairs are retained in `comment-text-repair-2026-09-23/`; current publication receipts are in `comments-local-export-2026-09-25/` |
| T07 | `publish-comments-mirror.yml` | `comments/agency/agency_code=<agency>/part-0.parquet` | published and verified 2026-09-25: the agency files contain the same 26,303,691 IDs as the monolith and agree with every index group. `comments-local-export-2026-09-25/public-object-verification.json` records each file’s digest, ETag and row count |
| T17 | `materialize-rulemaking` | `rule_targets.parquet`, `proceedings.parquet`, `regulatory_agenda_items.parquet`, `agenda_item_proceedings.parquet`, `comment_periods.parquet` | current `snapshot_6d3dc0f2…` is PARTIAL (2026-09-25): every public/input digest and all 20 recorded integrity checks pass. It contains 563,176 rule targets, 268,314 proceedings, 38,408 agenda items, 155,669 agenda links and 286,056 periods. Full semantic accounting of retired identities and decisions 32–33 remains open; retired no-action shells may legitimately have no successor. Earlier source/join scope was qualified at `snapshot_837754c3…` (2026-09-24); later bounded evidence is retained in `docket-lists-2026-09-24/published-step/`. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |

## September 24 report refresh

[Run 36032871739](https://github.com/mikewolfd/spicy-regs/actions/runs/36032871739)
at `cded33d` published generation `52325038…`. The 17:30 UTC anonymous readback
verified its declared schemas and counts: 137 rows describing report parts,
1,757 sections, 133 hearings, 78 hearing–bill links and 269 read checkpoints.
`CRPT-119hrpt494` has parts 1 and 2, and `CRPT-119hrpt811` is keyed to its
`-pt1` source. The receipt is
`workflow-audit-2026-09-24/report-refresh-public-readback.json`.

The independent source audit now qualifies reports, sections, hearing bodies
and read checkpoints at this generation. Every prior report body, section
content, hearing and checkpoint survives the migration. The audit compares
the admitted public bytes with retained native responses without calling the
production readers or shapers. Receipts, scripts and fresh source checks are in
`report-hearing-qualification-2026-09-24/`.

The hearing-link table keeps its earlier qualified pin. All current COVER
relationships match the source, but one compiled volume has no unique date:
`CHRG-117shrg56721` names 11 dates while its `117-s-2792` link publishes only
the first. Preserve the relationship and all native dates in SpicyDocs, expose
a scalar only when unique, then release/pin that change and invalidate CHRG
checkpoints before reprocessing. A dependency upgrade alone does not invalidate
their current relationship-rule checkpoints.

Two valid historical parts, `CHRG-79jhrg79716p19` and `CHRG-79jhrg79716p11`,
appear in native listings and fresh GovInfo summaries but fall outside the
reader's package grammar. They are not acquired or checkpointed. Six other
hearing details return 404 and remain correctly retryable; their GovInfo bodies
are retained. Discovery covers the recorded modification window and incomplete
prior reads, with no supported package deferred by the run cap. This audit
does not establish complete hearing history or agenda-link coverage.

## Open items

The dated narrative behind the rows (the September 21–22 status, the scope gaps and the September 23 continuation) is in the [fork execution log](fork-execution-log-2026-09-23.md); "plan" below is SpicyDocs' `docs/research/consolidation-path-2026-09-22.md`.

- **Audit reconciliation:** the September 25 workstreams advance only the qualified scopes above. Partial outputs retain earlier scoped pins; failed outputs carry no qualification phrase. `scripts/check_ledger_pins.py` reports generation equality, not semantic completion. CRS advanced after the frozen audit and remains a new pending generation. House communications has 992 list-only rows at its partial current pin.
- **Rulemaking joins:** the September 25 local refresh published `snapshot_6d3dc0f2…`, including the merged labelled-docket and decisions 32–33 corrections. Its public manifest and every output match the local bytes; it holds 268,314 proceedings after the completed metadata catch-up. Source and semantic qualification of this newer snapshot remain open; the earlier reviewed scope is retained in `docket-lists-2026-09-24/published-step/`.
- **Comment and document text:** the derived-text repair is published (plan A6, ruling 7; the 13 attachments whose derived text Mirrulations does not hold are recorded in the receipt); document bodies, extraction status and PDF-extraction evidence are still null (log: Scope and evidence gaps).
- **Other parsing-survey limits:** chamber vote days remain distinct from UTC bill-action instants. The larger subjects and print generations are now published but only partly source-audited; earlier key/grammar corrections retain their scoped evidence. See the September 25 rows and audit report.
- **Comments:** local catch-up and publication of the monolith, index and agency mirror are complete. Their population and public bytes are verified at the September 25 pins above. Native repair beyond the previously reviewed cohorts and the dated partition tree remain open. Three oversized derived texts and one empty FWS source response remain recorded for follow-up. See [Operations](#operations-checkpoint) and the [runbook](../etl-catalog-seed.md).
- **Bill family:** the scoped timestamp/URL repair is published and qualified at the pin above. Missing bodies and original text, models, historical backfills and ordinary source retention remain open. `119-hr-3446` and `119-hr-940` still have newer Congress.gov detail dates than current BILLSTATUS; the ordinary family has no API delta path. Six reserved House identities absent from the captured archives retain their prior values (receipt `bill-family-publication-2026-09-24/qualification.json`).
- **Wider populations:** SAM and lobbying beyond their bounded loads; FCC filings before the first window; USAspending beyond its top-100-page selection; CourtListener's full scope, rate budgets, newer catch-up and the non-civil docket scope decision (log: Scope and evidence gaps, Latest continuation).
- **Lifecycles:** a pairing and unknown-docket policy before `rulemaking_lifecycles` returns (decision 4).
- **Reports and hearings:** current report bodies/metadata, transcripts and checkpoints qualify at `b188e1b3…`; all six former detail refusals now have valid retained responses. Exact changed report-section decomposition remains partial. Correct the compiled-volume date and support the two native historical part IDs. The earlier [report refresh](#september-24-report-refresh) remains dated history.
- **Successful-run findings:** private laws still misreport captured-but-refused reads; USAspending still lacks per-row observation dates. The Table III traversal and larger subject selection have now published. Table III drops four meaningful native rows, and the broader subjects audit remains partial. SAM's repaired scheduled acquisition succeeded, but its new complete extract was not retained. Ordinary external readers need routine source-response retention under T18. See the [log audit](rollup-success-log-audit-2026-09-24.md) and [September 25 audit](parallel-rollup-audit-2026-09-25.md).
- **FEC (T05):** broader individual records, committee history and correction streams (log: Scope and evidence gaps).
- **Table III cold start:** a Congress whose first listed act has no OLRC page cannot start its chain (about 45% of acts have none); seed the 120th from the 119th's highest served act before January 2027 — its page is inferred (from 119-1 naming 118-273 as its prior act), not yet observed, to name the next Congress's first act.
- **Operations (T18–T20):** qualify the hosted mirror export within its runner
  limits, then qualify the first resumed incremental sweep. The
  [operations checkpoint](#operations-checkpoint) records completed repairs and
  the remaining deployment/access work.

## Operations checkpoint

### September 25 publication efficiency — local implementation

The [efficiency refactor](comments-publication-efficiency-2026-09-25.md) is
implemented locally: pinned snapshot reads, agency-first sorting, streaming
monolith assembly, one final index, verified no-op receipts and reused sweep
membership. Exact identity/coverage checks remain. Empty agency replacements
clear old public URLs after a move; partial uploads do not advance the completion
receipt. The runbook describes manual and scheduled finalization/recovery.

The repository checks pass. The full local build retained the exact published
population and passed schema, ID, count and all-column fingerprint comparisons.
The adopted 3 GB/one-thread settings completed in 527 seconds at 9.62 GB process
RSS; the configured budget is not a process cap. Receipts are retained in
`comments-efficient-publisher-2026-09-25/`; the linked record separates these
measurements from hosted qualification. This implementation did not change
public objects, dispatch CI or resume the ETL schedule.

### September 25 local mirror recovery

All catch-up batches completed at 01:41 UTC. The hosted completion run
[36083102561](https://github.com/mikewolfd/spicy-regs/actions/runs/36083102561)
then exhausted DuckDB’s 6 GB export budget and stopped before publication.

The same publisher ran locally with a 16 GB export budget and two threads.
It published the monolith, index and agency files by 10:11 UTC. The 10:16 UTC
public/catalog check passed unique-ID and exact agency/docket/month coverage
checks, including the three source-null docket relationships. Every previous
public ID remains present, and all published objects match the local files by
size and ETag. Full file digests and receipts are retained in
`comments-local-export-2026-09-25/`.

Scheduled ETL stays paused pending hosted export qualification. Local resource
controls do not establish that the smaller CI runner can complete the export.
Feed summary, agency statistics, monthly volume, docket search, discovery,
organization links and rulemaking refreshed successfully by 10:17 UTC, with
the base versions unchanged. Monitoring and the read-only duplicate audit are
enabled again; the manual mirror entry is available. The retained
`consumer-progress.json`, `refresh-inputs.json` and `completion.json` record
these results and the remaining hosted-export gate. The one
empty raw-source response and three texts above the 64 MiB read limit remain
in the published retry checkpoints. This is publication and integrity evidence;
wider source qualification remains separate.

### September 24 recovery history

Public counts checked September 24 at 17:32 UTC; failure state and unchanged
public ETags checked at 17:45 UTC. Catalog recovery was verified at 18:18 UTC
and catch-up restarted at 18:21 UTC. Publication measurements below remain
separate from the source-qualified pins in the output table.

- Catalog bootstrap is complete. The seed check proved every selected docket
  and comment ID present and unique, and verified the original input ETags.
  Manifest `28439568…` was published at 15:35 UTC; later batch publication has
  advanced that checkpoint. Receipts: `etl-seed-2026-09-24/` and
  `ledger-continuation-2026-09-24/`.
- Hosted sweep 36021389999 completed batch 0 and was cancelled at 16:36 UTC
  while batch 1 was staging. The local runner at `cded33d` stopped at 17:42 UTC:
  index validation found missing `docket_id` values on staged comments
  `CFTC-2026-0595-0003` and `CFTC-2026-0595-0005`. The log reports completed
  catalog merges to 279,129 dockets and 23,972,436 comments, followed by failure
  before public upload or manifest save. These are the failed attempt's counts;
  the comment identity check and cleanup below supersede them. Retained staging
  remains in `etl-local-catchup-2026-09-24/output/`, with first-attempt status and
  logs archived in `attempt-01/`. Full ingestion, source qualification, mirror
  publication and readback remain open.
- Source investigation at 17:55–18:00 UTC confirms both affected comments have
  explicit null docket IDs in the mirror JSON, and every mapped native field
  matches staging. They are archived test submissions; the current official API
  returns 404 for both, while their explicit parent document, docket and a
  neighboring comment return 200. Each affected ID appears once in the catalog.
  The application excludes these exact reviewed payloads at `3953175`, with
  raw evidence retained and changed payloads requiring another review. Missing
  docket IDs still refuse admission; staging is validated before persistent
  merges. See the [source investigation](fork-execution-log-2026-09-23.md#september-24-cftc-source-investigation)
  and `cftc-missing-docket-2026-09-24/` receipts.
- Recovery at 18:18 UTC retained the complete catalog rows, removed only the
  two reviewed source versions and verified through a reopened connection:
  23,972,434 comments, every ID unique, neither excluded ID present and no null
  docket IDs. Neither removed row was in the public comments file. The failed
  attempt's local manifest exactly matched the freshly downloaded public
  checkpoint (`db964dd4…` SHA-256). Retry began at 18:21 UTC using `3953175`,
  a fresh `output-retry-02/` directory and that verified manifest. Receipts:
  `cftc-test-exclusion-repair-2026-09-24/`; the
  [repair record](fork-execution-log-2026-09-23.md#september-24-reviewed-test-exclusions-and-catch-up-retry)
  explains the deletion checks and validation.
- The comment-text refactor shares a bounded comment worker pool between ETL
  and backfill, persists independent text retries and permits local manifest
  reuse. The fixed CFPB sample produced identical text/provenance and a 6.4-fold
  median text-fetch improvement with eight workers; this is not a whole-run
  estimate. Local validation and the pinned-checkout handoff are recorded in the
  [refactor evidence](comment-text-refactor-2026-09-24.md). The current batch
  keeps its existing revision until it commits; `handoff.json` and `progress.json`
  record adoption. Public mirror completion remains open.
- Public metadata now shows 279,126 dockets (ETag `91c7ee92…`), 2,001,540
  documents (`549ec0cc…`), and an index (`b2a78582…`) with 113,008 groups
  advertising 23,904,451 comments. The public monolith remains at 23,890,403
  rows (`cce5e386…`), trailing the index by 14,048 rows. The manifest has
  26,190,992 keys (`98336740…`). These are observed publication counts, not
  replacement source-qualified pins. Receipt:
  `workflow-audit-2026-09-24/base-public-readback-during-catchup.json`.
  The public ETags were unchanged after the failure; staging rows and public
  headers are retained in `local-catchup-batch-01-failure.json` in that directory.
- ETL, mirror publication, dedupe and comments monitoring remain paused during
  local catch-up. The completion watcher refused the failed attempt without
  dispatching the mirror or restoring schedules. Its retry requires successful
  batch receipts, CI and the reviewed fork revision, then publishes and verifies
  the mirror and dependent outputs before restoring incremental scheduling.
  `progress.json` and `completion.json` hold current supervisor state;
  `attempt-01/` retains the earlier failure.
- Repairs through `e944365` are pushed and [CI passes](https://github.com/mikewolfd/spicy-regs/actions/runs/36033160292).
  Dedupe is audit-only by schedule; explicit repair preserves its complete
  candidate through interrupted replacement, verified on a real scratch R2
  catalog. Publication now checks retained IDs and exact coverage. Dependent
  jobs wait for inputs, and base ETags are retained and rechecked. The full
  regulatory completion chain still awaits execution. See the
  [workflow review](workflow-review-2026-09-24.md).
- Pages deployment succeeded; active live schemas now pass (below). Wrangler
  verified the active catalog and uncached `r2.dev` endpoint with no custom
  domain; the configured MCP Worker was absent at that check. Purge credentials
  are unnecessary for this serving path. `GEMINI_API_KEY` remains absent and
  Zyte remains configured but unwired. SAM's request repair and the scheduled
  `bill_subjects` repair still await successful-run qualification.
- **Live dictionary verification — resolved:** active published schemas pass [run 36033160021, attempt 2](https://github.com/mikewolfd/spicy-regs/actions/runs/36033160021/attempts/2) after the report migration. The checker follows publication inventory, skips the withdrawn lifecycle object, continues past unreadable tables and fails incomplete verification separately from Pages deployment. This verifies schema agreement; source qualification remains specific to the pins above.
