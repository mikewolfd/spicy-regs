# MCP data opportunities, measured 2026-09-27

**All 91 advertised tables were readable: 85 populated, six empty.** Each was counted once; schemas matched declarations and publication pins agreed at discovery, count time and final discovery. See the [MCP experiment report](mcp-chaos-2026-09-27.md).

The subsequent [populated-table join audit](populated-table-joins-2026-09-27.md) maps every populated table to existing and proposed relationships, refreshes changed publications, and records live join measurements and extraction priorities.

## Evidence and completed findings

Local evidence, outside Git: `/Users/mikewolfd/.codex/artifacts/spicy-regs-full-data-inventory-20260927/`. Its `report.md`, `research-readiness-matrix.json`, `verification.json` and raw responses retain methods, schemas, identities, joins, coverage, queries and pins. The run used 181 requests, concurrency two and 35-second timeouts; no reads failed or counts repeated.

Availability is not qualification. Audit labels marked 73 generations newer than their audit, 11 current-generation-audited, two unpinned and five unspecified. Local-only coverage notes are historical; these counts establish readability. Some keys are prose-only: SAM needs UEI plus EFT indicator. Different row grains are not additive.

Earlier evidence: `/Users/mikewolfd/.codex/artifacts/spicy-regs-reference-chaos-20260927/`.

- RefSpec's co-occurrence crosswalk can map a child agency to its parent; confidence does not establish identity. Seven tested unmapped agencies had live documents.
- A follow-up diagnostic on ten frozen Federal Register documents joined 15 agency mentions to the SpicyDocs roster by ID with matching slug, versus one exact raw-name match. Four raw-only mentions remained unresolved.
- Unique-variant lookup added nothing for housing, unemployment and public participation; unemployment remained ambiguous. The extract was checked, not the PDF.
- Ten sampled bills with empty subject arrays still had policy areas. Some empty assignments cannot distinguish unassigned from not-held.

Population accuracy remains unmeasured.

## Table-by-table opportunities

`P` means populated; `E` means successfully counted as empty, not absence of the phenomenon. The last column distinguishes evidence, interpretation and collection state.

### Regulatory participation

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `dockets` | 279,429 | P | Define a Regulations.gov docket cohort. A docket is a folder, not necessarily one rule. |
| `documents` | 2,002,888 | P | Inspect agency documents within dockets. Upload dates can differ from publication dates. |
| `comments` | 26,314,331 | P | Study participation. Records are not unique people or representative opinion. |
| `comments_index` | 143,408 | P | Count comments by agency, docket and month. Rows are aggregate groups; inherit comment coverage. |
| `document_attributes` | 2,002,888 | P | Recover topics, dates and source attributes. Missing fields remain missing; avoid personal-field inference. |
| `docket_attributes` | 279,406 | P | Inspect program, keywords and status. Attribute coverage differs from the docket spine. |

### Regulatory activity summaries

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `feed_summary` | 279,429 | P | Select dockets by activity and comment windows. Derived from selected parent tables. |
| `agency_stats` | 316 | P | Compare recorded agency totals. A zero is within the contributing sources' scope. |
| `agency_monthly_volume` | 77,972 | P | Explore dated activity patterns. Omits unusable dates; backfills affect apparent trends. |
| `discovery_signals` | 8 | P | Find cases with output spikes. Rule-derived signal; source anomalies can create spikes. |

### Rule publication and authority

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `federal_register` | 1,009,313 | P | Study dated publication events. Use document number plus publication date. |
| `fr_docket_links` | 899,630 | P | Follow printed docket references. Printed labels need not resolve to Regulations.gov dockets. |
| `cfr_sections` | 321,010 | P | Locate codified regulatory units. Annual granule metadata; includes structural units, no body text. |
| `unified_agenda` | 233,250 | P | Track planned actions across editions. Use RIN plus edition; historical coverage note is narrower. |

### Rulemaking lifecycle interpretation

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `rule_targets` | 617,135 | P | Inspect stated docket/CFR/RIN relationships. Retain evidence class and unresolved targets. |
| `proceedings` | 172,742 | P | Group evidence for regulatory actions. Derived grouping; shared RIN alone does not merge actions. |
| `regulatory_agenda_items` | 52,092 | P | Study distinct agenda items. A RIN identifies an agenda item, not a proceeding. |
| `agenda_item_proceedings` | 148,011 | P | Trace agenda-to-proceeding evidence. Each row is supporting evidence, not a distinct action. |
| `comment_periods` | 281,635 | P | Measure observed comment windows. Merged intervals and reopenings follow explicit rules. |
| `rulemaking_lifecycles` | 75,270 | P | Study time from proposal to later action. Docketed selection; censoring and competing withdrawal matter. |
| `lifecycle_events` | 135,362 | P | Check the events behind lifecycle estimates. Upload-only anchors can misstate historical timing. |
| `agency_lifecycle_stats` | 344 | P | Inspect cumulative-incidence estimates. Derived estimator; small cells are suppressed, not zero. |

### Organizations and public funding

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `sam_entities` | 792,846 | P | Resolve public active registrations. UEI plus EFT indicator identifies a registration. |
| `usaspending_recipients` | 231,700 | P | Inspect observed award recipients. Accumulated top-N selection; amounts have mixed observation periods. |

### Lobbying disclosure

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `lobbying_filings` | 934,404 | P | Study disclosed clients, issues and amounts. Disclosure is not influence; retain filing/amendment scope. |
| `lobbying_activities` | 1,814,033 | P | Inspect issue-level disclosure detail. Multiple activities belong to one filing; avoid repeating amounts. |
| `lobbying_activity_lobbyists` | 4,735,056 | P | Link named lobbyists to activities. Activity positions define grain; people can recur. |

### Campaign-finance organizations

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `fec_committees` | 89,689 | P | Identify campaign-finance committees. Committee metadata is not a transaction census. |
| `fec_committee_history` | 298,395 | P | Track committee attributes by cycle. Use committee ID plus two-year cycle. |
| `org_committee_links` | 5,740 | P | Generate organization/committee candidates. Heuristic name matches are not verified affiliations. |

### FEC source inventory and evidence

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `fec_source_catalog` | 26 | P | Discover official FEC source routes. Catalog presence does not establish collection or acquisition. |
| `fec_collections` | 649 | P | Read selected FEC collection coverage. Collection scopes can overlap. |
| `fec_source_records` | 13,717,161 | P | Inspect native FEC records and evidence. Use collection plus record ID; amendment-safe sums need family rules. |
| `fec_relationships` | 183,390 | P | Trace source-stated associations. Check value and ID statuses; names never become IDs. |

### Oversight and policy research

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `gao_reports` | 42 | P | Find recent oversight products. RSS-derived selection, not GAO's complete archive. |
| `crs_reports` | 14,145 | P | Find policy research reports. Metadata availability does not prove retained report bodies. |

### Judicial records and citations

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `court_dockets` | 11,477 | P | Study retained agency-action litigation. Selected APA/nature-of-suit scope, not all litigation. |
| `court_docket_groups` | 901 | P | Avoid counting inferred duplicate cases. Same-case grouping is an interpretation. |
| `court_opinion_clusters` | 10,070,727 | P | Identify decisions. Bulk/search fields differ; docket join has narrower scope. |
| `court_citations` | 18,123,788 | P | Resolve publisher reporter citations. Cluster-level citations retain publisher spellings. |
| `court_citation_map` | 77,460,014 | P | Study opinion citation networks. Automated resolution; depth is mentions, not judicial treatment. |
| `court_parentheticals` | 6,408,887 | P | Inspect citing courts' descriptions. Automatically extracted; score is not reviewed correctness. |
| `court_opinions` | 10,798,347 | P | Bridge opinions to decisions. No opinion text; author strings are not resolved people. |

### FCC proceedings and filings

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `fcc_proceedings` | 21,684 | P | Extend docket research to ECFS. Docket-name deduplication; undated shells are excluded. |
| `fcc_filings` | 5,780 | P | Study FCC submissions. Accumulated bounded window, not the ECFS archive. |

### Legislative identity and history

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `congress_bills` | 421,465 | P | Define bill/resolution cohorts. Coverage varies by Congress; derived fields need their rules. |
| `bill_actions` | 930,779 | P | Trace ordered legislative actions. Action text is evidence; inferred stage is interpretation. |
| `bill_committees` | 286,100 | P | Trace bill referrals. Committee observations are not independent bills. |
| `bill_publisher_summaries` | 189,604 | P | Read publisher CRS summaries. Version/action-specific; separate from generated summaries. |
| `bill_subjects` | 175,095 | P | Select publisher policy/subject assignments. Missing and empty answers do not establish no policy subject. |
| `bill_versions` | 225,706 | P | Identify source-specific printings. Listed printing does not imply acquired body; keep reprint suffixes. |

### Legislative text change and interpretation

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `bill_sections` | 2,659,863 | P | Inspect structured bill text. Use printing plus sequence; missing trees are refusals. |
| `section_diffs` | 28,431 | P | Select compared printing pairs. Only pairs with usable trees; ordering/version rules matter. |
| `section_diff_items` | 1,150,597 | P | Inspect individual text correspondences. Diff text can be capped; inspect truncation flags. |
| `financial_changes` | 0 | E | Inspect aligned dollar-figure changes. Empty; word alignment would not establish fiscal meaning. |
| `section_classifications` | 0 | E | Use model-assigned section labels. Empty; vocabulary digest would define interpretation scope. |
| `bill_summaries` | 0 | E | Use generated printing summaries. Empty; publisher summaries are available separately. |
| `diff_summaries` | 0 | E | Use generated change summaries. Empty; source diff records are available separately. |

### Legislative activity and voting

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `public_activity_events` | 420,867 | P | Trace detected legislative changes. Detection time differs from publisher event time. |
| `amendments` | 7,106 | P | Follow amendment identities and targets. May amend another amendment; preserve target type. |
| `press_releases` | 36 | P | Find issuer statements about bills. Small feed selection; bill links are pattern-derived candidates. |
| `roll_call_votes` | 23,358 | P | Inspect publisher roll-call tallies. Use chamber/session/roll key and normalized vote day. |
| `member_votes` | 7,384,694 | P | Study member positions. Preserve publisher wording and normalized position separately. |
| `members` | 12,770 | P | Resolve legislator identifiers. Community crosswalk, not one government roster. |
| `member_terms` | 45,535 | P | Restrict comparisons to service periods. A person can switch chamber; terms remain separate. |
| `member_vote_terms` | 7,384,694 | P | Assign votes to dated terms. Derived interval match; unresolved matches remain visible. |
| `bill_vote_references` | 19,775 | P | Trace bills' source-stated vote references. Publisher statements, not operational checkpoints. |

### Congressional deliberation and fiscal evidence

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `committee_reports` | 146 | P | Inspect captured report parts. Use package plus part; CBO extraction needs cover evidence. |
| `report_sections` | 1,953 | P | Inspect report blocks and source spans. Agency identity fields remain unresolved. |
| `hearing_transcripts` | 145 | P | Identify captured hearing records. A hearing can concern several bills; bill_id stays NULL. |
| `hearing_bill_links` | 86 | P | Relate hearings to bills. Cover evidence means held_on; agenda evidence means noticed. |
| `cbo_cost_estimates` | 12,732 | P | Find indexed CBO publications per bill. An empty index does not prove no estimate exists. |
| `house_activity_reports` | 40 | P | Study committee activity reports. Includes Senate reports; print-derived findings have selected scope. |
| `budget_volumes` | 29 | P | Inspect budget documents. Bill numbers without Congress are not bill join keys. |
| `bill_committee_actions` | 12,695 | P | Find action statements in prints. Rule-derived with measured precision/recall limits; dates unscored. |
| `document_citations` | 52,674 | P | Trace exact citation occurrences. Keep text digest and span; mentions are not unique documents. |
| `senate_expenditures` | 2,623 | P | Inspect printed expenditure cells. Page-capped; many ruled cells contain whole payee blocks. |

### Enacted law and codification

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `laws` | 113 | P | Identify enacted laws. Missing USLM may reflect publisher lag; inspect outcome/reason. |
| `law_code_sections` | 3,655 | P | Trace per-Congress classification lines. Use source page/sequence; repeated lines are retained. |
| `table3_records` | 2,983 | P | Trace law-to-Code classification. Classification lags enactment; retain release point. |

### Congressional institutions and business

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `committees` | 817 | P | Resolve committee/subcommittee identities. Historical and current records mix; inspect currency. |
| `committee_assignments` | 2,966 | P | Inspect chamber roster seats. Capture-date membership; Senate Congress is caller-assigned. |
| `house_communications` | 5,006 | P | Trace executive communications and referrals. List-only rows have NULL detail fields, not negative findings. |
| `committee_meetings` | 6,096 | P | Find scheduled meetings and linked material. Listing is not attendance; distinguish unread detail from empty lists. |
| `record_issues` | 368 | P | Build a recorded chamber-day calendar. Chambers/package IDs require detail; issue metadata is not debate text. |
| `treaties` | 2 | P | Inspect treaty documents. Partitioned treaties remain list-only under current reader. |
| `nominations` | 2,214 | P | Trace nominated items and parts. Use Congress plus citation, including part number. |

### Acquisition operations and coverage auditing

| Table | Rows | State | Use / key limit |
| --- | ---: | :---: | --- |
| `bill_family_archives` | 96 | P | Audit bulk-folder reads. Checkpoint, not proof every member was processed. |
| `bill_family_backfills` | 0 | E | Audit attempted historical bill fills. Empty operational state; not absence of older bills. |
| `bill_family_backfill_walks` | 0 | E | Audit historical listing completion. Empty operational state; not a historical population count. |
| `committee_report_reads` | 292 | P | Inspect pending/completed document reads. Read state, not independent legislative activity. |

## Proposed bounded studies

These studies have not run. Preregister the cohort, comparison, missingness and stop bound. SpicyRegs supplies rows, SpicyDocs evidence, and RefSpec identities/vocabularies.

1. **Participation and interests:** Freeze a docket cohort; join comments and documents by source IDs. Compare identifier-supported organization links with name-only SAM/lobbying/FEC candidates. Check sampled records through SpicyDocs and RefSpec. Comments are not people; disclosures are not influence.
2. **Legislative scrutiny:** For one Congress and policy cohort, connect actions, publisher summaries, reports, CBO indexes, hearings and votes. Verify sampled SpicyDocs records; preserve RefSpec topic ambiguity. Compare hearing-cover evidence with agenda-only links: held and noticed remain distinct.
3. **Timing and judicial attention:** Select proceedings with source-backed events and censor dates; inspect legal references against opinions and citation records. Check source spans through SpicyDocs and identifier scope through RefSpec. Citation mentions are not adverse treatment; lifecycle estimates require competing-risk assumptions.
4. **Transparency gaps:** Compare selected report/FCC/FEC scopes with collection and read outcomes. Trace metadata to retained SpicyDocs evidence; retain RefSpec unresolved observations. Separate unattempted, pending, refused and completed where available. A URL does not prove body acquisition.
5. **Public money:** Check a small set of budget, expenditure and CBO records against SpicyDocs pages and cells. RefSpec cannot resolve congress-blind bill numbers. Compare matching units and fiscal periods; do not sum mixed recipient observation windows. Empty financial-change output adds no comparisons.
6. **Institutional business:** In a short chamber window, connect seats, meetings, communications, Record issues, nominations and treaties with dated terms. Check detail-read states and sampled SpicyDocs evidence; use explicit RefSpec identities where applicable. Compare scheduled with source-stated completed activity; listing does not prove attendance or disposition.

Retain pins, queries and missingness; validate joins and coverage before quantitative conclusions.
