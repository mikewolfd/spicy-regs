# Fork output ledger — September 21, 2026

This dated ledger accounts for every declared rollup output plus base regulatory, comments and rulemaking outputs at code `ea64719`. It records 44 producer paths and 82 distinct output keys, including dynamic partition patterns.

The [consolidated backlog](../fork-generation.md) remains the task list. [Machine-readable evidence](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/output-ledger.json) records each candidate path, measured size/count/schema, source assessment, scope, required and optional inputs, observed publication pins, next action and receipt. A missing pin or unresolved scope remains a blocker. Existing populations must survive narrower repairs.

- **Pins.** A row records `qualified at <pin> (<date>)` for the generation its audit covered, not a claim that the pin is live; `scripts/check_ledger_pins.py` reads the public index and reports rows whose live generation differs. Withdrawn, unproduced, blocked and known-wrong outputs carry no such phrase, and base objects record `verified at table digest <digest> (<date>; ETag <etag>)`, whose ETag the script compares with a HEAD of the public object (a row without the ETag is reported unpinned). Dockets and documents are also managed families since spicy-regs `1d283d5`; their rows keep the table-digest pin, which the MCP compares with the live family member's table digest. Docket search, comments and its index remain outside the index.
- **Names.** A generation is named by the first 8 hex digits of its `artifactDigest`; a table digest is labelled "table"; units are named (rows, distinct keys, pairs).
- **Evidence.** Every load-bearing number cites a retained script or receipt; numbers copied from a publisher's statement cite the receipt.

Public data destination: `https://pub-72e95c0c20a84508b42b03a6ff6d55f8.r2.dev`. Local candidates are not fork publications. Retained qualified generations include FEC, members/terms, nominations, treaties, the five-member report family, repaired dockets/documents, docket search, two document-derived tables, Appropriations press-feed windows, the retained Agenda edition and complete June 30 court clusters. As of 2026-09-23 the following are also source-qualified or validated against raw data:
- **Regulatory:** the Federal Register and its docket links; the rulemaking dataset; the comments table (all 133 comment-bearing agencies, with six plus ACF re-verified and repaired from native source) with its index and the three T15 summaries.
- **Legislative:** laws; committee rosters; amendments with their sponsor and amended-bill detail; the reconciled bill family (decision 1) and its subjects; member vote terms (decision 2); roll-call bill links.
- **Courts and other sources:** the court citation tables and opinion index; CRS reports; FCC proceedings and filings; bounded SAM and lobbying loads (decision 10).

CFR's parts are placed from each volume's `PART` heading since generation `de703ffe…` (see the CFR part ancestry entry in the [execution log](fork-execution-log-2026-09-23.md)). The September 23 parsing survey found join, text and key limits in some qualified tables; each affected row names them. Committee meetings, house communications, record issues, print citations, Senate expenditures and GAO reports were published by scheduled runs and source-qualified at the generations their rows name; the qualification receipts are in `scheduled-published-qualification/` under the execution receipts. Scheduled generations newer than a row's qualified pin await audit (decision 3). Both repositories were pushed on 2026-09-23 and the workflows held for the push were re-enabled that day (decisions record, decisions 11 and 22).

## September 26 drift qualification

Four medium-effort auditors (congress documents, bills and citations,
external sources, regulatory) re-qualified every row whose live generation had
drifted from its qualified pin; each row above records its disposition and
receipt under `drift-qualification-2026-09-26/`. `check_ledger_pins.py` and the
MCP `table_qualification` record read these rows directly.

- **Failed.** The bill family `5990abbb…` writes backward `section_diffs` when
  an enrolled BILLSTATUS version has an empty date (it sorts first in
  `build_bill_family.py`), and drops two `bill_sections` on identity
  collisions. Print citations `d57faac3…` stamp Senate activity reports that
  cover the 117th Congress as the 118th and misread `CLAUSE S 2(N)` and
  `S. Con. Res. 2022` as citations (receipt `bills-citations/print-check.json`).
  The scheduled `55671b43…`, the first bill family with retained BILLSTATUS
  evidence, still carries both defects. Both repairs are in code at `8041729`,
  adopting SpicyDocs 0.35.0 (`0ecac5c`):
  - bill printings pair by date, then a dateless enrolled one by its stage;
    `bill_sections` is keyed on `seq`, and the stale pairs are retired on the
    next run;
  - activity-report bills key in the covered Congress, and committees are read
    in the report's chamber, or in the chamber the print names.
  Bill family `d380cdc0…` shows both repaired, but it exposes repeated same-stage
  printings sharing one identity (119-hr-6644). SpicyDocs 0.37.0 gives a numbered
  reprint its own code, and 0.38.0 keys a bill printed under a Congress subheading in
  that Congress. The bill family now qualifies at `8ab2a2cf…`. Print citations `8f2502b6…`
  are repaired but stay partial on one wrong join and 31 inline-stated Congresses (their rows).
- **Partial.** `congress_bills` lacks bills that the retained 110th–113th
  BILLSTATUS lists (`bills-citations/congress-bills-missing-vs-billstatus-110-113.json`).
  The subjects, amendments and house-communications audits are partial as
  their rows state. FCC filings, SAM, lobbying, CRS and court dockets have no
  retained source responses in their producing runs, so only publication
  integrity is verified.
- **Rulemaking (R5 met at `snapshot_911969b4…`).** Snapshot `snapshot_dcda51db…` qualified with the
  limits its row names, but the R5 gate was not met: three dockets
  (`FAR-2018-0003`, `DHS-2004-0009`, `FDA-1999-F-0118`) are retired wrongly.
  D1: the SpicyDocs docket-name reader misses several native formats (the
  FAR-case form needs an owner ruling). D2: document numbers containing spaces
  do not join. D3: `identity_predecessors_json` looks back only one snapshot.
  D1 (owner ruling: read a docket named after prose) and D2 are in SpicyDocs
  0.35.0, adopted at `8041729`, where they read 432 of the 442 missed held pairs
  and all 39 missing references (receipt `regulatory/d1d2-fix/`). D3 is in code
  at `635c42c`: lineage now carries all recorded ancestry, and a proceeding no
  longer lists as a predecessor a sibling that shares only a cited notice
  (receipt `regulatory/d3-fix/`). All three closed at `snapshot_911969b4…`
  (2026-09-26, receipt `r5-reaudit-2026-09-26/`). An independent rebuild of
  decisions 32–33 with the pinned reader reproduces all 268,159 proceedings, and
  the three dockets are back in proceedings. No predecessor list names a live
  sibling. Open, but not blocking:
  - A docket that is an action docket only through its own documents' citation
    does not absorb the cited notice (572 dockets), as decision 33 reads; this
    needs a ruling or a recorded limit.
  - The 3,010 decisions 32–33 lineage links erased at `snapshot_47cca15e…` are
    not restored.
  - Ten docket values stay unread.
  D4: derived rollups record no parent digests (receipt `regulatory/results.json`);
  fixed at `4396f2e` and closed 2026-09-26. The refresh run 36224680634 published
  feed summary, agency stats, monthly volume, org committee links and discovery
  signals, and each records its parents. Every managed parent's pin equals its read
  snapshot and its member digest, and the streamed bytes match: dockets `e601dbf6…`,
  documents `7c98ef8c…` and FEC committees `4b1ca622…`. The bare comments index
  matches by sha256, and the in-place comments monolith by ETag and size (receipt
  `d4-parents-2026-09-26/`). This verifies lineage only; each table's own rows are
  audited in its row above.
- **Scaling.** `fec_committees` re-walks the whole registry with little
  deadline headroom, and its captures live only as an expiring workflow
  artifact. CHRG rule-token re-reads use most of the per-run cap. A package is
  re-read whole after a single-step failure. Derived jobs re-download their
  base tables (receipts `congress-docs/results.json`, `external/results.json`,
  `regulatory/results.json`).

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

## September 25 repairs

Work packages R1–R4 of the [repair plan](rollup-audit-repair-plan-2026-09-25.md)
are in code at spicy-regs `9818b2e` (adopting spicy-docs 0.33.0, `881f6f5`)
and `197e449`. The three FAILED rows (`laws`, `table3_records`,
`hearing_bill_links`) and the partial USAspending and roll-call-link findings
are repaired in code only. Their public generations are unchanged until the
next run re-reads under the new rule versions, and none is qualified until
that generation is audited. Offline replays against retained sources show the
intended corrections: Table III act 119-37 gains exactly its four native
rows, both private laws become `captured_partial`, all 82 COVER links survive,
and two stale Senate `match_action_index` values move from 19 to 15 and from
21 to 17. The receipts, reviews and replays are in
`/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/repair-execution-2026-09-25/`.
Every rollup the repair touches now retains its source evidence, SAM and the
bill-status families included, so the next generations can be qualified from
their own inputs.

The first repaired generations were published by dispatch at `8177fb0` and
audited against their own retained evidence
(`/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/repair-qualification-2026-09-25/`).
`laws`, `table3_records`, `hearing_bill_links`, `roll_call_votes` and
`usaspending_recipients` qualify at their new pins, and the rest of both
families qualify with stated limits. `committee_reports` and
`hearing_transcripts` fail on one newly found defect: the publisher's second
spelling of its PDF notice, fixed in spicy-docs `7550f0e` and awaiting release.
Each output's row states its pin and limits. Spicy-docs 0.33.2 then
recognised the second spelling, and the committee-reports rule moved to
`placeholder-pdf-002`. The family republished at `95810b26…`, where
CRPT-119hrpt649 and CHRG-119jhrg60491 are `pdf_extracted` from their PDFs
(259 and 89 pages). A full audit of that generation is still pending.

Found during the repair: `fec_committees` has failed every scheduled run
since 2026-09-22 (runs 35774968104, 35910579310, 36049323173 and
36180816422). Each dies on FEC's HTTP 429 after about 10,000 records, once
the transport's three quick retries are spent. Its qualified 2026-09-21
generation was therefore still the live one. `f2df979` paces the walk under the
gateway's stated limit of 60 requests per minute, and spicy-docs `3cfc6f6`
(carried into the 0.34.0 branch) waits out `Retry-After`. The dispatched run
then published the full registry at `4b1ca622…`; see its T04 row.

| Task | Producer | Output | Delivery state |
| --- | --- | --- | --- |
| T13 | `run-rollup-court-opinion-clusters` | `court_opinion_clusters.parquet` | generated and verified for complete 2026-06-30 edition; qualified at `7a2cbdb7…` (2026-09-22) |
| T13 | `run-rollup-court-opinion-bodies` | `court_opinion_bodies.parquet` | withdrawn 2026-09-22 and removed 2026-09-23: builder, rollup, workflow and catalog/MCP registration deleted, and the push deleted the fork workflow; text links out through the clusters' `absolute_url` |
| T13 | `run-rollup-court-citations` | `court_citations.parquet`, `court_citation_map.parquet`, `court_parentheticals.parquet` | added and published for the complete 2026-06-30 edition: qualified at `f1e2e523…` (2026-09-22); public row counts verified |
| T13 | `run-rollup-court-opinions` | `court_opinions.parquet` | added and published for the complete 2026-06-30 edition (text-free): qualified at `f7cc67cc…` (2026-09-22); public row counts verified; no scheduled workflow (54.6 GB source) |
| T16 | `run-rollup-bill-subjects` | `bill_subjects.parquet` | current `750784bd…` is PARTIAL (2026-09-26): 149,261 rows, all prior identities retained. Every added or changed row (50,790) replays against admitted evidence `78ddbfd7…` with zero mismatches, independently parsed from 32 BILLSTATUS zips and 1,236 API pages. The 26 null rows are reserved numbers absent from fully named zips. 47,558 rows added at `1b067027…` (114th–117th bulk; 643 API) retain no source; 48 fresh samples match. Receipt: `drift-qualification-2026-09-26/bills-citations/`. |
| T15 | `run-rollup-feed-summary` | `feed_summary.parquet` | qualified at `034a6ac6…` (2026-09-26): all 279,380 rows equal an independent replay from dockets `308b35c6…`, documents `d7487819…` and index `c43559bd…` (ETags recorded by run 36177463432 before it ran). The 44 additions and 267 changes each trace to a changed parent row. Parent source limits remain separate. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T15 | `run-rollup-agency-stats` | `agency_stats.parquet` | qualified at `e0a531e3…` (2026-09-26): every count for all 316 agencies equals an independent recount from the same exact parents; all 66 changed agencies trace to parent changes. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T15 | `run-rollup-agency-monthly-volume` | `agency_monthly_volume.parquet` | qualified at `baba33a2…` (2026-09-26): all 77,972 rows match an independent replay from documents `d7487819…`; the date policy excludes 2,023 missing and eight year-zero dates. Nine new and 61 changed buckets all trace to documents that were added or changed. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T15 | `run-rollup-docket-search` | `docket_search.json.gz` | Not built on the fork after `a6a14b1`: the refresh builds it only where `PUBLISH_DOCKET_SEARCH` is set, and the fork leaves it unset because its one reader, the web app, loads upstream's copy. On 2026-09-26 the object with ETag `cd719934…` (table `c761cc8c…`) matched an independent field-by-field replay of all 279,362 search documents from dockets `308b35c6…`, omitting the same 18 dockets without title or abstract. The fork's last regeneration (ETag `7421faa6…`) is unaudited and has no reader. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T15 | `run-rollup-lifecycles` | `rulemaking_lifecycles.parquet` | withdrawn 2026-09-23 (decision 4): the 2026-09-22 scheduled run had published generation `2f001194…` (26,519 rows); the family was conditionally removed from the index. Its workflow was deleted in `5715163` on September 24; the local research producer remains and pairing semantics remain blocked |
| T15 | `run-rollup-discovery-signals` | `discovery_signals.parquet` | qualified at `350e49f5…` (2026-09-26): built by the 2026-09-25 run, all 11 signals match an independent replay from documents `d7487819…` at the recorded as-of instant `2026-09-25T21:10:48.805987+00:00`. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T15 | `run-rollup-fr-docket-links` | `fr_docket_links.parquet` | qualified at `acc047d1…` (2026-09-26): all 899,630 rows equal the multiset expansion of Federal Register `40dfab8e…` (DuckDB two-way EXCEPT ALL and a Python multiset). The 110 additions are exactly the new documents' links. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T11 | `run-rollup-cfr-sections` | `cfr_sections.parquet` | qualified at `d063f76d…` (2026-09-26): all 321,010 rows have table bytes identical to the qualified `60092f2a…` (`5dc4e856…`). This carries the retained edition and earlier citation limits (`cfr_ref` null on the documented 7,910 rows). Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T16 | `run-rollup-congress-bills` (retired, A1) | `congress_bills.parquet` | current `8ab2a2cf…` is PARTIAL (2026-09-26): all 420,166 identities survive, byte-identical to `934c1da3…`. All 513 bills that retained 110th–113th BILLSTATUS lists are still absent, and the retired list walk left 1,299 missing across the 108th–117th (receipt `join-gaps-2026-09-26/d/`). A status-only backfill (`max_version_fetches=0`, one Congress per dispatch) would fill them; costing is in `print-subheading-2026-09-26/backfill-cost.json`. Scoped repair remains qualified at `4a1949a9…` (2026-09-24). |
| T11 | `run-rollup-unified-agenda` | `unified_agenda.parquet` | qualified at `649cfd28…` (2026-09-26): the edition backfill holds 58 editions, 199510–202510, in 233,250 rows over 46,247 RINs, with `(rin, agenda_edition)` unique. Every readable edition is present; 199504 and 201204 were never published, and SpicyDocs refuses 200404 and 200410 over an XML-forbidden control byte. The Fall 2012 file is stamped `201210`, as its records state. No prior row is removed. Three editions, the oldest, the legacy-named Fall 2012 and the newest, match independent parses of reginfo's export exactly: RIN sets, titles and priority categories (4,999, 4,063 and 3,954 RINs). The run retains no source evidence. Receipt: `unified-agenda-649cfd28-2026-09-26/`. |
| T11 | `run-rollup-federal-register` | `federal_register.parquet` | qualified at `40dfab8e…` (2026-09-26): 1,009,313 rows. Every one of the 1,009,209 previously qualified rows is unchanged; all 104 additions (the 2026-09-25 issue) match that complete native issue in identity and every field. The native issue was re-read at audit time; the producer retains no evidence. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T12 | `run-rollup-fcc-proceedings` | `fcc_proceedings.parquet` | qualified at `fc5c4bc5…` (2026-09-26): 21,683 docket rows, table bytes identical to `930e4716…`. Scheduled run 36173431595 (`943b4ae`, spicy-docs 0.32.1) walked 21,691 documents at exact counts and reused the unchanged member. It was not built by the spicy-docs 0.34.0 delegation (`9cd4658`), which no FCC run has used yet. Receipt: `drift-qualification-2026-09-26/external/`. |
| T12 | `run-rollup-fcc-filings` | `fcc_filings.parquet` | current `6bdd9eef…` is PARTIAL (2026-09-26): 5,705 filings, retaining every prior row unchanged and adding 214. All 214 additions match native fields, and all 1,450 rows in the run's window match a fresh read. Run 36175655150 (`a89476e`, spicy-docs 0.32.1) predates both evidence retention and the 0.34.0 delegation, so no run-time evidence exists. Of 71 later-seen window filings, 69 were disseminated after the run and 2 were absent from a pre-run read. Earlier window qualified at `18c85718…` (2026-09-25). Receipt: `drift-qualification-2026-09-26/external/`. |
| T14 | `run-rollup-sam-entities` | `sam_entities.parquet` | current `65edd788…` is PARTIAL (2026-09-26): 179,825 registrations keep all 167,965 rows of `464977e4…` unchanged, including the qualified 147,254-row 2026 population, and add 11,860 registered in 2003, the scheduled rotating year window. Ten sampled additions match all 19 native fields. Run 36177843463 (`4b345a5`) predates SAM evidence retention (`197e449`), so the 2003 extract was not retained. Earlier extract qualified at `56dd0f65…` (2026-09-23); identity is `(uei, entity_eft_indicator)`; other years remain open. Receipt: `drift-qualification-2026-09-26/external/`. |
| T14 | `run-rollup-lobbying-filings` | `lobbying_filings.parquet` | current `56a84791…` is PARTIAL (2026-09-26): 27,989 filings; every prior cell is unchanged and all 58 additions match native responses read before and after the run. Every in-window filing seen by two pre-run reads is present. Seventeen filings dated 2026-09-24 appeared at the source after 18:10 UTC, and 2026-09-25 filings fall outside the run's window by design; the 7-day overlap should admit both. Run 36177381824 (`4b345a5`) retained no evidence. Earlier selection qualified at `fd1794b5…` (2026-09-25); wider history remains open. Receipt: `drift-qualification-2026-09-26/external/`. |
| T04 | `run-rollup-fec-committees` | `fec_committees.parquet` | qualified at `4b1ca622…` (2026-09-26): the complete OpenFEC committee registry, 89,679 committees, from run 36201050523 (code `f2df979`). All 16 fields of every row (1,434,864 cells) replay from the run's 897 retained pages; the source's exact count matches 89,679 distinct ascending IDs, and three fresh pages are identical. All 27,311 committees of the `dfda14da…` selection remain: 24,634 unchanged, and 2,677 changed, each moving from its 2026-09-12 native value to the 2026-09-25 one. The captures are a GitHub run artifact expiring 2026-10-25, not a generation input; the receipt keeps a copy. Receipt: `drift-qualification-2026-09-26/external/`. |
| T04 | `run-rollup-fec-source-catalog` | `fec_source_catalog.parquet` | generated and verified; qualified at `0570574b…` (2026-09-21) |
| T04 | `build-fec-observations` | `fec_source_records.parquet`, `fec_collections.parquet`, `fec_relationships.parquet` | generated and verified; qualified at `11bcb620…` (2026-09-21) |
| T15 | `run-rollup-org-committee-links` | `org_committee_links.parquet` | qualified at `e12056b0…` (2026-09-26): all 3,250 links and 19 fields match an independent Python replay over comments (ETag `1970f15e…`) and FEC committees `dfda14da…`. There are five additions from two new organization strings, and the 15 changed counts only grew. Name heuristics, not identities; built before FEC committees advanced to `4b1ca622…`. Receipt: `drift-qualification-2026-09-26/regulatory/`. |
| T12 | `run-rollup-gao-reports` | `gao_reports.parquet` | qualified at `d87cf97b…` (2026-09-25): 42 reports. All 11 additions and two abstract changes match native RSS; the other 29 rows are unchanged. `report_type=Report` is an explicit default, not a native feed value. This qualifies the bounded feed population; broader history and routine retained source inputs remain open. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T12 | `run-rollup-crs-reports` | `crs_reports.parquet` | current `7076f282…` is PARTIAL (2026-09-26): 14,144 reports, preserving all prior identities, with one addition (R49360) and 36 updates. Every title, status and publish date among the 37 matches native reads taken 14 minutes before and 38 minutes after the run. 17 update dates match exactly; the other 20 are source restamps lying between those two reads. Later source changes (three Archived-to-Active statuses, R49361) postdate the run. Run 36167759334 (`03541c7`) retained no evidence. Earlier generation qualified at `35a43885…` (2026-09-25). Receipt: `drift-qualification-2026-09-26/external/`. |
| T12 | `run-rollup-courtlistener` | `court_dockets.parquet` | current `50a0b9fc…` is PARTIAL (2026-09-26): 11,475 dockets, all prior identities retained. Since `5570c1b4…` it adds one docket and updates two; since the qualified `95f2091d…`, five additions and eight updates. All 13 match a fresh native read in every field, party order included. Docket 74842896 now carries its native judge, which closes the prior drift finding. Every in-window docket seen before or after the run is present. Run 36174586704 (`943b4ae`) retained no evidence. Earlier selection qualified at `95f2091d…` (2026-09-23); catch-up and scope limits remain open. Receipt: `drift-qualification-2026-09-26/external/`. |
| T12 | `build_court_docket_groups` | `court_docket_groups.parquet` | republished with numeric parent order and qualified at `0f855eb1…` (2026-09-23): 901 rows; raw-validated against the native docket edition and public readback identical |
| T12 | `run-rollup-usaspending-recipients` | `usaspending_recipients.parquet` | qualified at `ab52206f…` (2026-09-25): 10,311 recipients, every prior identity retained. Each of the 10,000 rows read in the retained 100-page top-amount walk matches its page JSON, carries that page's capture digest and an `observed_at` bracketed by that capture (the reader's response-complete clock, not the journal's request-start time). The 311 carried rows, identified by ID, keep prior values with NULL observation fields; 47 of them come from the evidence-less `f033c5f4…` run. Schema equals the dictionary. Wider coverage and the trailing-12-month period mix remain explicit. Receipt: `repair-qualification-2026-09-25/votes-usaspending/`. |
| T09 | `run-rollup-bill-family` | `congress_bills.parquet`, `bill_actions.parquet`, `bill_committees.parquet`, `bill_publisher_summaries.parquet`, `bill_versions.parquet`, `bill_sections.parquet`, `section_diffs.parquet`, `section_diff_items.parquet`, `financial_changes.parquet`, `section_classifications.parquet`, `bill_summaries.parquet`, `diff_summaries.parquet`, `cbo_cost_estimates.parquet`, `public_activity_events.parquet`, `bill_family_archives.parquet`, `bill_vote_references.parquet`, `bill_family_backfills.parquet`, `bill_family_backfill_walks.parquet` | qualified at `8ab2a2cf…` (2026-09-26): SpicyDocs 0.37.0/0.38.0 (runs 36227172365 for the 119th and 36230043425 for the 118th, capped at 10 printing fetches) repair every defect the drift audit and its re-audits found. No pair runs backward; a dateless enrolled printing follows its dated predecessors. `bill_sections` is keyed on `seq` with no duplicate, and 119-hr-5334 enrolled, 119-hr-9022 reported and 119-hr-4060 introduced publish every section. A numbered reprint keeps its own code: 119-hr-6644's `eas` (198 sections) and `eas2` (276) are separate and chain placed-on-calendar → eas → engrossed-amendment-house → eas2 → enrolled, matching independent native parses of both documents; 118-hr-7643's `rh2` is separate, and no row keys a reprint's package under another code. Every diff item resolves to a published section. Each generation's removals are accounted for (32 stale pairs, then 119-hr-6644's 78 misfiled sections, stale row and 2 pairs; keyless tables differ only in re-observation times). The generation audit finds 0 failures, with evidence `18ea08d4…` admitted and bound. Limits: the four model outputs stay uncomputed, and historical bodies before the current Congress are only partly captured. Receipts: `bill-family-d380cdc0-2026-09-26/`, `bill-family-rerun-2026-09-26/`, `bill-family-8ab2a2cf-2026-09-26/`, `repeated-printings-2026-09-26/`. |
| T08 | `run-rollup-press-releases` | `press_releases.parquet` | qualified at `c0f3cc22…` (2026-09-25): 32 rows, retaining every prior identity. All 25 current RSS items match native content; seven historical items carry forward. Capture time, feed position and source channel freshness are separate metadata limits. Literal bill links remain bounded; history and other committees are outside scope. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-amendments` | `amendments.parquet` | current `559fd5f4…` is PARTIAL (2026-09-26): 7,100 rows, all 7,095 identities of `af5ef4c3…` retained, five additions and 12 changes (`update_date`, latest action). These match the run's logged window of 42 rows (2026-09-20 to 2026-09-25). The generation retained no source evidence (`inputs=[]`). In a post-publication sample of all 17 rows, 187 stable cells match. Of the latest-action pairs, 7 occur in native action history and nine equal a native NULL. SA 6782's published 2026-09-23 action is absent from today's history. Each `update_date` lies in the window and is monotone. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T08 | `run-rollup-member-vote-terms` | `member_vote_terms.parquet` | qualified at `c85e578f…` (2026-09-25): all 382,536 identities and 2,677,752 nonidentity cells match an independent join replay over exact recorded inputs. The 15 unique inclusive-end fallbacks and three unmatched post-term Not Voting positions remain explicit. No prior row changes or losses; three new votes add 300 positions. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-roll-call-votes` | `roll_call_votes.parquet`, `member_votes.parquet` | qualified at `45b8f6b8…` (2026-09-25): 1,579 votes and 382,536 positions, every prior identity retained. Only Senate 212/213 `match_action_index` change (21→17, 19→15) under `recorded-vote-first-numeric-action-v2`; all 845 links equal the first recorded reference in bill-family `5990abbb…`, 734 remain unmatched, and `member_votes` is byte-identical. All 50 retained overlap XMLs match every roll-call and member cell. Other votes inherit `0255d5de…` by exact equality; wider history and scorecards remain open. Receipt: `repair-qualification-2026-09-25/votes-usaspending/`. |
| T08 | `run-rollup-members` | `members.parquet`, `member_terms.parquet` | qualified at `47ac33e4…` (2026-09-25): 12,770 members and 45,535 terms. Only capture times change; all 581,370 substantive cells match native JSON, and retained captures bind the frozen run. The community crosswalk retains its Ed Case special-election-term and party-history limits. Three newly present native website URLs are omitted by the current table design; official-roster completeness is not established. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T10 | `run-rollup-committee-reports` | `committee_reports.parquet` | qualified at `95810b26…` (2026-09-26): all 142 rows were re-read under `body=placeholder-pdf-002` and replay against their evidence `12292d4f…`. `CRPT-119hrpt649` is now `pdf_extracted` from its retained 259-page PDF. pypdf and pdfium agree on the page count, and pdfium agrees on 99.6% of the words. The other 140 HTML bodies carry neither notice spelling and are `not_flagged`. Apart from `observed_at`, only that row changed. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T10 | `run-rollup-committee-reports` | `report_sections.parquet` | qualified at `95810b26…` (2026-09-26): 1,904 sections tile their texts contiguously, and each body is an exact slice. Only `CRPT-119hrpt649` changed: its five placeholder-notice sections are replaced by 24 PDF sections. Each cites pages, and pdfium confirms the cited pages hold the words (one heading word misses). No stale `seq` and no notice text remain. Every other section is unchanged. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T10 | `run-rollup-committee-reports` | `hearing_transcripts.parquet` | qualified at `95810b26…` (2026-09-26): all 145 rows replay against evidence `12292d4f…`. `CHRG-119jhrg60491` is `pdf_extracted` from its retained 89-page PDF. The production text keeps every pdfium word except the 227 GPO slug lines that its cleanup removes. Seventeen HTML digests moved only in Cloudflare `data-cfemail`, and their text is equal. Eight hearing-detail 404s are retained, and their `event_id` is NULL. `CHRG-106hhrg64097` and `CHRG-104hhrg26486` publish cover-only HTML (876 and 1,175 characters for 190 and 151 stated pages) as `not_flagged`. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T10 | `run-rollup-committee-reports` | `hearing_bill_links.parquet` | qualified at `95810b26…` (2026-09-26): table `474ec9d2…` is byte-identical to the one at `5e1e73ad…`. All 86 links replay against this run's retained MODS COVER entries under rule `d06e0bd80ca1`. `CHRG-117shrg56721` has a NULL `held_date` and keeps all 11 native dates. The other eight linked hearings each keep their single date. Every `event_id` is NULL: the retained details of eight hearings name no meeting, and `CHRG-119hhrg60291`'s four links follow a retained detail 404. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T10 | `run-rollup-committee-reports` | `committee_report_reads.parquet` | qualified at `95810b26…` (2026-09-26): 288 checkpoints equal their journal outcomes: 278 `complete`, 8 `detail_refused` and 2 `refused`. Every checkpoint carries `body=placeholder-pdf-002`, and all 288 were selected (141 CRPT, 147 CHRG) with none deferred. The two refused parts carry `last_modified` from their retained summaries. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T08 | `run-rollup-print-citations` | `house_activity_reports.parquet`, `budget_volumes.parquet`, `bill_committee_actions.parquet`, `document_citations.parquet` | current `8f2502b6…` is PARTIAL (2026-09-26): runs 36226295627 and 36227170848 re-read all 68 held parents under SpicyDocs 0.38.0 (bill_number 004, committee_name 002). The audited defects are repaired: every Senate report keys its bills in the Congress it covers, eight filed in the 118th covering the 117th and six filed in the 119th covering the 118th; Senate reports carry 1,904 Senate committee codes; `CLAUSE S 2(N)` and `S. Con. Res. 2022` are no longer bills; 116-hr-5119 and -5919 now resolve. Against the prior `ae51fc90…`, every removed row is accounted for: 12,684 rows changed only their rule versions, 3,148 re-keyed their target at the same span, and the 3 citations and 4 actions of the refused misreads dropped. The generation audit finds 0 failures, with evidence `54cb6f5f…` admitted and bound. Residuals keep it partial: 117-hr-1132 in 117hrpt705's `Prior Congresses` paragraph still joins the wrong bill, and 31 citations that state their own Congress inline (`H.R. 6752, 115th Cong.`) take the report's. Eight House codes in Senate reports are one correct `House Committee on Transportation` and 7 bare names only the House roster holds, a documented limit. bill_committee_actions → congress_bills misses 17 of 2,956 bills. Receipts: `print-citations-rerun-2026-09-26/`, `print-citations-8f2502b6-defect-check.txt`, `rerun-removals-2026-09-26.txt`. |
| T08 | `run-rollup-senate-expenditures` | `senate_expenditures.parquet` | generated and verified; replayed byte-identical; qualified at `acef9fcf…` (2026-09-22) |
| T08 | `run-rollup-laws` | `laws.parquet` | qualified at `43130abc…` (2026-09-25): all 113 identities survive, and every row replays against its admitted evidence `4d77e2fc…`. Private laws 119-1 and 119-2 are `captured_partial` with reason `statutes_citation_not_stated`; they keep their `citableAs` values, and their bytes equal the earlier hash-bound XML. No row claims `not_requested`, and the earlier citations are unchanged. 119-111 is newly captured at 140 Stat. 1026. Receipt: `repair-qualification-2026-09-25/congress-documents/`. |
| T08 | `run-rollup-laws` | `law_code_sections.parquet` | qualified at `43130abc…` (2026-09-25): all 3,655 rows keep every source-value cell of the `c9d35661…` audit. Only `observed_at` moves, to this run's retained classification captures. Their stated laws and prepared dates match; the pages were not re-parsed independently. Receipt: `repair-qualification-2026-09-25/congress-documents/`. |
| T08 | `run-rollup-laws` | `table3_records.parquet` | qualified at `43130abc…` (2026-09-25): 2,981 rows over 38 acts, all re-read under `table3-native-rows-v2` with capture-bound checkpoints. Every row replays cell for cell against its retained page. Act 119-37 has all 110 native rows, including the blank act-section rows at pages 511, 534, 534 and 563. Every earlier row survives in order. The walk ends at release point 119-73. Receipt: `repair-qualification-2026-09-25/congress-documents/`. |
| T08 | `run-rollup-committee-rosters` | `committees.parquet`, `committee_assignments.parquet` | qualified at `9ab5e338…` (2026-09-25): 236 committees and 2,966 assignments preserve every substantive prior cell. All 450 Senate native seat identities and member fields match fresh XML; its later file-date restamp is an explicit capture-time limit. The declared/served committee discrepancy and 28 unlisted relationships remain; House seats carry the earlier qualification. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-house-communications` | `house_communications.parquet` | current `37625a89…` is PARTIAL (2026-09-26): all 4,992 identities of `333e422d…` survive, and exactly 1,000 list-only rows gain detail. Those rows are the head of the 1,992-row queue under the builder's newest-first order and 1,000-per-run cap, as the run log states. All list columns are unchanged, and all 10,000 derived cells agree with their JSON and the RIN rule. A post-publication sample of 40 more fills matches all 1280 native cells (1920 with the earlier 20). No source evidence was retained. Receipt: `drift-qualification-2026-09-26/congress-docs/`. |
| T08 | `run-rollup-committee-meetings` | `committee_meetings.parquet` | qualified at `f85be7b6…` (2026-09-25): 2,765 meetings, preserving all prior identities. Three additions and five changed records match native responses in all 216 checked cells. The corrected `NoChamber` population is retained; current list/carry-forward discrepancies remain explicit source-scope limits. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-record-issues` | `record_issues.parquet` | qualified at `6b1913da…` (2026-09-25): 367 issues; the added issue matches all 17 native fields and every prior row is unchanged. This carries the recorded source selection, not complete historical coverage. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-treaties` | `treaties.parquet` | qualified at `4ccf2d7a…` (2026-09-25): both rows are exactly unchanged from the previously qualified selection. No fresh whole-source walk was performed; broader history and detail remain outside this carried scope. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T08 | `run-rollup-nominations` | `nominations.parquet` | qualified at `d4622c60…` (2026-09-25): all 2,214 rows are exactly unchanged from the qualified predecessor. A fresh pooled list yields 2,212 native identities plus the two retained parent identities; later native changes remain explicit and do not establish frozen-output freshness. Prior population/part reconciliation remains evidenced in `ledger-continuation-2026-09-24/nominations/`. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T06 | `run-pipeline` | `dockets.parquet` | publication verified at table digest `308b35c6…` (2026-09-26; ETag `50d8cfba…`): 279,380 rows with unique `docket_id`, published by hosted sweep run 36177463432 and read back anonymously (receipt `hosted-sweep-readback-2026-09-26/readback.json`). Managed family generation `69afafdc…` (run 36207823917 at `1d283d5`) holds these exact bytes. At the earlier table `680b86ad…` (279,336 rows), the latest five native records match every mapped field after locating their exact numbered source captures. This is bounded source evidence; whole-population source qualification after catch-up remains PARTIAL. Earlier T06 source qualification is retained in the execution receipts. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T06 | `run-pipeline` | `documents.parquet` | publication verified at table digest `d7487819…` (2026-09-26; ETag `f3324286…`): 2,002,831 rows with unique `document_id`, published by hosted sweep run 36177463432 and read back anonymously (receipt `hosted-sweep-readback-2026-09-26/readback.json`). Managed family generation `f0796ed9…` (run 36207823917 at `1d283d5`) holds these exact bytes. At the earlier table `ff502e4b…` (2,002,562 rows), the latest five native records match every mapped field after locating their exact numbered source captures. This is bounded source evidence; whole-population source qualification, document bodies and extraction evidence remain PARTIAL. Receipt: `parallel-rollup-audit-2026-09-25/` (workstream details in the [audit report](parallel-rollup-audit-2026-09-25.md)). |
| T06 | `run-pipeline` | `comments_index.parquet` | published and verified at table digest `c43559bd…` (2026-09-26; ETag `fb95b0e8…`): 143,397 groups sum to 26,311,037 comments, and every group equals a recount of the public monolith (`check_comments`, run on the downloaded bytes). Published by hosted sweep run 36177463432, whose verify job also passed the raw-catalog check. Receipt: `hosted-sweep-readback-2026-09-26/readback.json`. The earlier local-recovery index (`2a050b5e…`, 143,367 groups, 26,303,691 comments) is receipted in `comments-local-export-2026-09-25/`. |
| T06 | `run-pipeline` | `comments/agency_code=<agency>/docket_id=<docket>/year=<year>/month=<month>/part-0.parquet` | unproduced on the fork (probed keys answer 404); the comments deliveries wrote `comments.parquet` and its index directly |
| T07 | `publish-comments-mirror.yml` | `comments.parquet` | published by hosted sweep run 36177463432 (snapshot 3697835162427868969) and verified at table digest `0d11e204…` (2026-09-26; ETag `1970f15e…`): 26,311,037 rows with unique IDs across 180 agencies. Its publisher refuses the loss of any previously published ID, and every index group equals a recount (receipt `hosted-sweep-readback-2026-09-26/readback.json`; [hosted qualification](comments-publication-efficiency-2026-09-25.md#hosted-qualification)). The earlier local recovery (table `b90e1105…`, 26,303,691 rows) retained every previously published ID. The catalog export adds 2,413,288 rows to the prior public population. Public bytes, unique IDs and index coverage are verified; wider native-source qualification remains open. Earlier native and attachment-text repairs are retained in `comment-text-repair-2026-09-23/`; current publication receipts are in `comments-local-export-2026-09-25/`. |
| T07 | `publish-comments-mirror.yml` | `comments/agency/agency_code=<agency>/part-0.parquet` | republished by hosted sweep run 36177463432 with the 26,311,037-row snapshot; the publisher read back every file and the verify job's public check passed, but these files were not re-hashed independently. The 2026-09-25 local-recovery files held the same 26,303,691 IDs as that monolith and agreed with every index group; `comments-local-export-2026-09-25/public-object-verification.json` records each file’s digest, ETag and row count |
| T17 | `materialize-rulemaking` | `rule_targets.parquet`, `proceedings.parquet`, `regulatory_agenda_items.parquet`, `agenda_item_proceedings.parquet`, `comment_periods.parquet` | qualified at `snapshot_911969b4…` (2026-09-26): every public digest matches its manifest, and every input digest matches a published parent admitted against its own pin: dockets `e601dbf6…`, documents `7c98ef8c…`, federal-register `40dfab8e…`, fr-docket-links `acc047d1…` and unified-agenda `649cfd28…`. Actors are rule-targets v5, proceedings v8, comment-periods v8 and agenda v4, and no reference dangles. With SpicyDocs 0.38.0's docket reader, an independent SQL rebuild of decisions 32–33 reproduces all 268,159 proceedings exactly. Every difference from a relaxed scan traces to 105 reader-disagreement pairs: former identifiers, fused or broken labels, and the 10 values the D1 receipt leaves unread. FAR-2018-0003 and DHS-2004-0009 now hold their action notices, and FDA-1999-F-0118 is restored under its original id. Its notice 99-20888 names only `99F-0001`, so it stays FR-only, as decision 33 reads. From `snapshot_837754c3…`, 191,842 proceedings retired as no-action shells, 3,189 merged and 31 split, with the rest retained and none unexplained. No link pair is lost except one former identifier the D1 ruling fences. No predecessor list names a live sibling, and all 178 links trace. Limits: the 3,010 lineage links erased at `snapshot_47cca15e…` are not restored; 572 citation-only action dockets do not absorb the notices they cite. Receipt: `r5-reaudit-2026-09-26/`. |

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

- **Audit reconciliation:** the September 25 workstreams advance only the qualified scopes above. Partial outputs retain earlier scoped pins; failed outputs carry no qualification phrase. `scripts/check_ledger_pins.py` reports generation equality, not semantic completion. The [September 26 drift qualification](#september-26-drift-qualification) audited the generations that advanced afterwards, CRS included. House communications has 992 list-only rows at its partial current pin.
- **Rulemaking joins:** the September 25 local refresh published `snapshot_6d3dc0f2…`, including the merged labelled-docket and decisions 32–33 corrections. Its public manifest and every output match the local bytes; it holds 268,314 proceedings after the completed metadata catch-up. Source and semantic qualification of this newer snapshot remain open; the earlier reviewed scope is retained in `docket-lists-2026-09-24/published-step/`.
- **Comment and document text:** the derived-text repair is published (plan A6, ruling 7; the 13 attachments whose derived text Mirrulations does not hold are recorded in the receipt); document bodies, extraction status and PDF-extraction evidence are still null (log: Scope and evidence gaps).
- **Other parsing-survey limits:** chamber vote days remain distinct from UTC bill-action instants. The larger subjects and print generations are now published but only partly source-audited; earlier key/grammar corrections retain their scoped evidence. See the September 25 rows and audit report.
- **Comments:** local catch-up and publication of the monolith, index and agency mirror are complete. Their population and public bytes are verified at the September 25 pins above. Native repair beyond the previously reviewed cohorts and the dated partition tree remain open. Three oversized derived texts and one empty FWS source response remain recorded for follow-up. See [Operations](#operations-checkpoint) and the [runbook](../etl-catalog-seed.md).
- **Bill family:** the scoped timestamp/URL repair is published and qualified at the pin above. Missing bodies and original text, models, historical backfills and ordinary source retention remain open. `119-hr-3446` and `119-hr-940` still have newer Congress.gov detail dates than current BILLSTATUS; the ordinary family has no API delta path. Six reserved House identities absent from the captured archives retain their prior values (receipt `bill-family-publication-2026-09-24/qualification.json`).
- **Agency scrapers (spicy-docs 0.34.0, adopted at `5a16d3a`):** FCC filings now delegate their walk to `FccEcfsReader.iter_filings`, which partitions a crowded query by submission timestamp with reconciled counts (live: a 19,676-filing selection in 26 requests), so a day above the publisher's 10,000-result ceiling is a scope choice here, not a limit; the merge streams through `write_rows` and keeps the prior bytes on failure. The SEC, FERC, CFTC and USITC EDIS readers ship in the same release with no consumer here; each resumes by full re-walk and has no `publish` profile, so scheduling and recovery stay with this host when adopted. EDIS bulk ZIP jobs need a signed-in session (the token alone redirects to login) and have no client. The source-side open items, by package, are in spicy-docs `docs/research/agency-scraper-completion-2026-09-24.md` ("Open items after the 2026-09-25 validation"); the consumer-side ones are in `PLAN.md` (Next actionable work).
- **Wider populations:** SAM and lobbying beyond their bounded loads; FCC filings before the first window; USAspending beyond its top-100-page selection; CourtListener's full scope, rate budgets, newer catch-up and the non-civil docket scope decision (log: Scope and evidence gaps, Latest continuation).
- **Lifecycles:** a pairing and unknown-docket policy before `rulemaking_lifecycles` returns (decision 4).
- **Reports and hearings:** current report bodies/metadata, transcripts and checkpoints qualify at `b188e1b3…`; all six former detail refusals now have valid retained responses. Exact changed report-section decomposition remains partial. Correct the compiled-volume date and support the two native historical part IDs. The earlier [report refresh](#september-24-report-refresh) remains dated history.
- **Successful-run findings:** private laws still misreport captured-but-refused reads; USAspending still lacks per-row observation dates. The Table III traversal and larger subject selection have now published. Table III drops four meaningful native rows, and the broader subjects audit remains partial. SAM's repaired scheduled acquisition succeeded, but its new complete extract was not retained. Ordinary external readers need routine source-response retention under T18. See the [log audit](rollup-success-log-audit-2026-09-24.md) and [September 25 audit](parallel-rollup-audit-2026-09-25.md).
- **FEC (T05):** broader individual records, committee history and correction streams (log: Scope and evidence gaps).
- **Table III cold start:** a Congress whose first listed act has no OLRC page cannot start its chain (about 45% of acts have none); seed the 120th from the 119th's highest served act before January 2027 — its page is inferred (from 119-1 naming 118-273 as its prior act), not yet observed, to name the next Congress's first act.
- **Operations (T18–T20):** the hosted mirror export and the first resumed
  incremental sweep are qualified, and scheduled ETL runs again
  ([September 25 hosted qualification](#september-25-hosted-qualification)).
  `dockets` and `documents` publish as managed families at sweep finalization
  (run 36207823917 at `1d283d5`), so DocSpec can admit them by reference.
  Queued publication work: store table
  members once, since every generation publication still re-downloads its whole
  family; then the comments family; and qualify browser queries against the
  new monolith order. The [operations checkpoint](#operations-checkpoint)
  records the remaining deployment and access work.

## Operations checkpoint

### September 25 hosted qualification

Three hosted runs qualified the mirror export, the verified no-change skip and
the first complete incremental sweep since before September 10; their numbers
are in the [efficiency note](comments-publication-efficiency-2026-09-25.md#hosted-qualification).
ETL was re-enabled at 19:04 UTC. Changes made alongside:

- **Listing.** A sweep now spends most of its time listing Mirrulations
  (80.7 of the sweep's 107 ingestion minutes). SpicyDocs 0.33.1 (`4503f59`,
  adopted at `1f201b9` and carried into 0.34.0) lists each agency as concurrent
  contiguous docket ranges: four agencies took 133 s instead of 449 s with
  identical keys. The 18:25 sweep now runs only when no scheduled sweep has
  succeeded that day (`afe2274`). The 06:25 UTC sweep on September 26 is the
  first on the new listing.
- **Publication pointer.** Parallel dependent rollups share one compare-and-swap
  on `publication.json`; a lost race to another family now rereads and merges
  instead of failing the job (`a89476e`, logged by `4b345a5`, and R2's
  `ConditionalRequestConflict` counts as a lost race since `123cf59`).
- **Source evidence stored once.** Evidence blobs live at
  `source-evidence/blobs/sha256/<hex>`, uploaded create-only with Content-MD5
  and verified once (`123cf59`, [source evidence](../source-evidence.md)). A
  probe confirmed R2 refuses a wrong Content-MD5 (`BadDigest`). The first real
  uploads held 2,737 shared blobs (278.6 MiB) by 22:28 UTC, which let SAM and
  bill-status retention turn on (`197e449`).
- **Upstream.** `#198` is merged with the swap-insert retry: `a7237d9`, `37ce426`.
- **Local data.** 57.5 GB of superseded comment Parquet builds were deleted from
  `comments-efficient-publisher-2026-09-25/`, `comments-efficiency-review-2026-09-25/`,
  `comments-local-export-2026-09-25/`, `full-comments/` and
  `comment-text-repair-2026-09-23/`. Every receipt, digest record, log, script
  and raw capture remains, so the directory citations above resolve; the
  deleted builds can no longer be re-hashed locally.
- **Validation audit.** A stack-wide duplicate-validation audit is in
  `/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/dry-validation-audit-2026-09-25/`
  (`findings.md`, `findings.json`). Its top open item is the per-publication
  family re-download named in the Operations item above.

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

Scheduled ETL stayed paused pending hosted export qualification, because local
resource controls do not establish that the smaller CI runner can complete the
export. It resumed at 19:04 UTC the same day ([hosted qualification](#september-25-hosted-qualification)).
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
- ETL, mirror publication, dedupe and comments monitoring were paused during
  local catch-up; they resumed on 2026-09-25 after hosted qualification. The completion watcher refused the failed attempt without
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
