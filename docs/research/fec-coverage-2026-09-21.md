# FEC coverage census — September 21, 2026

This snapshot reconciles the final local Senate delivery with the source catalog,
the official bulk-page inventory and research tasks T01–T19. Use the
[gap register](../fec-gaps.md) for owners, completion checks and current remaining
work. Earlier September 21 reports describe intermediate selections; their
Senate and unnamed-summary gaps are superseded by the later audits.

The selected generation contains **649 collections, 13,717,161 source records,
183,390 relationship observations and 26 source-catalog rows**. It has selected
collections for **17 broad families**, leaving nine catalog-only in this batch.
All **26 official bulk groups** have selected outputs or file inventory. These
classifications are different; neither establishes complete FEC history.

## Broad source families

Counts below come from a left join of `fec_source_catalog` to `fec_collections`
in `fec-senate-discovery-2026-09-21/final-tables`, grouped by `source_family`.
They count selected collections, not files, records, API routes or completed
historical populations. Zero means absent from this delivery, not absent from
provider capabilities or retained research. Every family also needs the shared
evidence, refresh and publication work in FG01–FG02, FG16 and FG21.

| Source family | Selected collections | Remaining scope or applicable gaps |
| --- | ---: | --- |
| `fec_access` | 0 | API/export/bulk discovery metadata; no automatic requirement for an observation dataset (FG13) |
| `fec_committees` | 5 | Retained masters/history, broader periods, fresh credentialed census (FG03–FG04, FG12, FG16) |
| `fec_candidates` | 12 | Retained 2026 master/summary and entity history/detail (FG04, FG12) |
| `fec_candidate_links` | 3 | Retained 2026 linkages; broader history and interpretation (FG04, FG14) |
| `fec_reports` | 601 | Selected Senate, electronic/paper and filing records; full archive/layout/body coverage remains selective (FG06–FG07) |
| `fec_receipts` | 4 | Individual base table, further schedules and financial policy (FG04, FG08, FG15) |
| `fec_disbursements` | 3 | Operating field mismatch and broader Schedule B detail (FG05, FG08) |
| `fec_intercommittee` | 5 | Other cycles and explicit transfer/support/amendment semantics (FG08, FG14–FG15) |
| `fec_loans_debts` | 0 | Select native loan, guarantee and debt populations/releases (FG08, FG12–FG13) |
| `fec_party_allocation` | 0 | Select coordinated expenditure, allocation and special-account populations (FG08, FG12–FG13) |
| `fec_ie` | 1 | Other cycles and full Schedule E/report overlap (FG08, FG15) |
| `fec_electioneering` | 2 | Adopt other retained periods and qualify refresh (FG04, FG16) |
| `fec_communication_costs` | 1 | Adopt other retained periods and qualify refresh (FG04, FG16) |
| `fec_bundling` | 2 | Snapshot history/refresh; preserve report-level meaning (FG15–FG16) |
| `fec_leadership` | 1 | Retained 2026 counterpart and historical scope (FG04, FG14) |
| `fec_inaugural` | 1 | Missing/oversized originals and wider detail (FG07–FG08) |
| `fec_summaries` | 4 | Map detail exports, other retained summaries and periods (FG04, FG15) |
| `fec_public_funding` | 0 | Select public-funding collections and release identities (FG12–FG13) |
| `fec_results_calendar` | 0 | Select results, calendars and reporting-date collections (FG12–FG13) |
| `fec_legal` | 0 | Select statutes, regulations and rulemakings; raw acquisition differs from admission (FG12–FG13) |
| `fec_ao` | 1 | Further opinions/supporting originals and real pagination qualification (FG09, FG12–FG13) |
| `fec_enforcement` | 1 | Audit/MUR/ADR success, original acquisition and case identity review (FG09–FG10) |
| `fec_guidance_meetings` | 0 | Select guidance, Commission meetings and policy collections (FG12–FG13) |
| `fec_archive_quality` | 2 | Preserve source notices, history/refresh and inventory-only distinctions (FG03, FG15–FG16) |
| `fec_agency_reports` | 0 | Adopt retained evidence and choose broader agency/FOIA collections (FG11–FG13) |
| `fec_oig` | 0 | Adopt selected inspector-general evidence; broader years/status (FG11–FG13) |
| **Total** | **649** | **17 represented families; nine catalog-only** |

## Official bulk groups

The retained `bulk-coverage.json` classifies **25** groups as
`selected_outputs_audited` and **one** as `selected_file_inventory_audited`.
Every entry has `complete_history: false`. A group can have audited selected
rows while other retained members remain inventory-only.

For every row below, wider history remains explicit work under FG13/FG16:
choose periods, exhaust the selected discovery route, retain original/member
outcomes, audit fields, then publish the actual scope. This is additional source
coverage, not a defect in the completed selection. Source acquisition belongs
to SpicyDocs; output adoption belongs to SpicyRegs. Source-specific exceptions
refer to the [gap register](../fec-gaps.md).

| # | Official bulk group | Audited selected output/inventory | Remaining selection, blocker or interpretation |
| --- | --- | --- | --- |
| 1 | Candidate summary | Named `weball24` and `weball26` members | Wider history; source-compatible summary policies (FG15) |
| 2 | Candidate master | `cn24` plus its exact header | `cn26` retained but not adopted; source election year remains distinct from filename cycle (FG04) |
| 3 | Candidate-committee linkages | `ccl24` file metadata and positional member | `ccl26` retained but not adopted (FG04) |
| 4 | Presidential candidate map exports | Complete 2024 overall summary | 2024 contribution-by-size and three ZIP exports retained but unselected (FG04) |
| 5 | New statements of candidacy (Form 2) | Complete selected 2024/2026 CSV snapshots | Earlier snapshots/history; preserve election/report years and receipt dates |
| 6 | House/Senate current campaigns | Named `webl24` and `webl26` members | Wider history and financial interpretation (FG15) |
| 7 | All candidates | Complete `candidate_summary_2024` | 2026 counterpart retained but not adopted (FG04) |
| 8 | Committee master | `cm24` plus its exact header | `cm26` retained but not adopted; API reference census remains separate (FG04, FG16) |
| 9 | Committee summary | Complete 2024 CSV | 2026 counterpart retained but not adopted (FG04) |
| 10 | PAC summary | Named `webk24` and `webk26` members | Wider history and financial interpretation (FG15) |
| 11 | Leadership PACs and sponsors | Complete 2024 CSV | 2026 counterpart retained but not adopted (FG04) |
| 12 | New statements of organization (Form 1) | Complete selected 2024/2026 CSV snapshots | Earlier snapshots/history; names and `NONE` remain literal |
| 13 | Lobbyist/registrant committees | Complete all-period snapshot: 14,013 data rows plus header | Snapshot history/refresh; no selected-row parsing gap |
| 14 | Any transaction from one committee to another | `oth26`: 9,906,414 named data rows plus header | Older cycles; amendments, memos and financial interpretation (FG15) |
| 15 | Communication costs | Complete 2024 CSV | Other retained even-year snapshots from 2010–2026 not adopted (FG04) |
| 16 | Contributions by individuals | Base/insert/delete file/member inventories; 254,240 deletion and 394,630 insertion rows | Main base member's 32,034,987 rows not expanded; main/by-date overlap and correction policy unresolved (FG04, FG15) |
| 17 | Contributions from committees to candidates and independent expenditures | `pas224`: 703,597 data rows and official 22-column header | Other cycles; not full intercommittee or Schedule E coverage; explicit transaction semantics (FG08, FG15) |
| 18 | Electioneering communications | Complete selected 2024 and 2026 outputs | Other retained even-year snapshots from 2010–2026 not adopted (FG04) |
| 19 | Independent expenditures (24- and 48-Hour Reports) | Complete 2024 CSV: 73,449 data rows | Other cycles inventory-only; amendments and periodic-report overlap (FG08, FG15) |
| 20 | Lobbyist bundled filings | Complete all-period snapshot: 1,867 data rows plus header | Snapshot history/refresh; report totals do not establish individual lobbyist identity |
| 21 | Operating expenditures | `oppexp26`: 1,620,229 rows, all 26 positions preserved | Only 25 published header names; named mapping unavailable (FG05) |
| 22 | False and fictitious filings | Complete 330,672-byte snapshot: 2,164 data rows plus header | Snapshot history/refresh; retain publisher notice without automatic exclusions (FG15) |
| 23 | Electronically filed reports (.fec) | Selected complete originals; three daily archives retained/parsed; two original positional releases selected for delivery | Full daily archive populations not integrated; history/layout/body qualification (FG04, FG07) |
| 24 | Paper filed reports (.fec) | Original `1160672`, P3.3: 1,522 records including header | Other retained originals, daily archive and `nofiles` marker do not establish complete history (FG04, FG07) |
| 25 | Postgres database dump files | Two original-file records: committee-history dump and README | No decoded history rows or full A/B/E dump delivery (FG03, FG08) |
| 26 | Senate unofficial electronic filings | 598 live originals, 597,156 records; one separate excluded archive mirror | Complete inventory unproved; malformed native parse and semantic exceptions remain (FG06) |

## Research task reconciliation

The research repository's `integration-task-list.md` was reconciled through
September 14; it is not current product status. This table preserves all T01–T19
work packages while accounting for later completions. Provider execution remains
in SpicyDocs' existing FEC worklist; this repository tracks receiving work in
[PLAN.md](https://github.com/mikewolfd/spicy-regs/blob/main/PLAN.md) and the [gap register](../fec-gaps.md).

| Research task | Current disposition and remaining work |
| --- | --- |
| T01 — Source acquisition checklist | Official 26-family inventory implemented; wider discovery, route/terms/access maintenance and explicit population choices remain (FG13, FG24) |
| T02 — Bounded bulk acquisition | Shared acquisition, selected recovery and native delivery verified; portable evidence, wider qualification and capacity remain (FG02, FG07–FG08, FG17) |
| T03 — Short-retention sources | Adjacent advertising/FCC capture backlog; select time-sensitive collections and storage budgets independently (FG24) |
| T04 — FEC identity and summary bulk | Selected cycles/census and named summaries verified; retained counterparts, history and fresh census remain (FG03–FG04, FG16) |
| T05 — Original filings and amendments | Selected originals, native layouts and Senate recovery verified; further archives, dictionaries, attachments and bodies remain (FG06–FG07, FG20) |
| T06 — Full financial populations | Selected groups delivered; individual base, schedule gaps and large dumps remain (FG04, FG08) |
| T07 — Current and historical views | Literal source data available; amendment, memo, correction and inclusion policies remain consumer-specific (FG14–FG15) |
| T08 — FEC legal, policy and history | Raw readers and selected native parsers exist; real-query qualification, originals, identities and declared collections remain (FG09–FG13) |
| T09 — SpicyRegs delivery | Four selected FEC tables, source relationships, MCP discovery and verified local generation path work; queryable context, new profiles, portable evidence and external delivery remain (FG01–FG04, FG12, FG18, FG20–FG21) |
| T10 — IRS political organizations/nonprofits | Adjacent backlog: electronic 8871/8872 text, nonprofit masters and 990 XML/indexes; paper/public-donor visibility, amendment and NCCS provenance remain source-specific (FG24) |
| T11 — Lobbying, foreign agents and unions | Inspect existing LDA work before expansion; full LD-1/2/203, FARA originals and OLMS reporting families retain separate populations and histories (FG22, FG24) |
| T12 — Political advertising | Adjacent Google/Snap/FCC/Meta selections and access setup; preserve creatives, ranges, dates and sponsor evidence, distinguishing orders/airings/estimates from spending (FG24) |
| T13 — Financial/ethics disclosures | Adjacent House/Senate/OGE access and original-report work; request-only records, value ranges and personal assets stay distinct from campaign finance (FG24) |
| T14 — Corporate/award/legislative/geographic context | Reuse existing connectors and FEC/Bioguide bridge; audit combined queries, dates, geography and source authority before new matching or scores (FG18–FG19, FG24) |
| T15 — Representative state/local exports | Adjacent jurisdiction/format pilots; real enumeration, amendment and local/state coverage required for each export (FG24) |
| T16 — Every jurisdiction/filing authority | Adjacent discovery backlog; publisher-local catalogs, local custody, migration overlaps and exact vendor identifiers remain explicit (FG24) |
| T17 — Public third-party enrichment | Adjacent DIME/LittleSis/OpenSecrets/academic/repository sources; pin versions/codebooks, derived identities and use terms; paid-only alternatives are conditional (FG24) |
| T18 — Backfill and corrections | Selected refresh proofs exist; wider periods, replacement/deletion behavior, checkpoints and recurring operations remain (FG06, FG13, FG16) |
| T19 — Verification, cost and coverage | Selected literal audits, joins, local MCP roundtrip and synthetic real R2 checks pass; broader cases/capacity, evidence portability, source success and hosted delivery remain (FG01–FG02, FG09, FG17–FG23) |

The retained frontier's latest lanes are archive wave 13, academic wave 11,
repositories wave 13 and local-platforms wave 10. Earlier failures may have later
recoveries; use those latest dispositions. Remaining leads include academic and
repository candidates, county/city publishers, historical invitations and ad
airings, web-archive recovery, and license/schema/representation conflicts.
They are preserved research leads, not new prerequisites for the FEC tables.

## Reproduce and inspect

With DuckDB installed, run this against the retained `final-tables` directory:

```sql
SELECT s.source_family, count(c.collection_id) AS selected_collections
FROM read_parquet('fec_source_catalog.parquet') s
LEFT JOIN read_parquet('fec_collections.parquet') c USING (source_family)
GROUP BY s.source_family
ORDER BY s.source_family;
```

Evidence locations:

- [Final selected tables and bulk inventory](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21): `final-tables/`, `bulk-coverage.json`, `delivery.json`.
- [Full local generation audit](fec-generation-readiness-2026-09-21.md) and
  [Senate source/output audit](fec-senate-recovery-2026-09-21.md).
- [Research task list](/Users/mikewolfd/Documents/Codex/fec-data-research-2026-09-11/integration-task-list.md),
  [latest frontier](/Users/mikewolfd/Documents/Codex/fec-data-research-2026-09-11/inventory/expansion/frontier.json)
  and [provider worklist](/Users/mikewolfd/Work/spicy-stack/spicy-docs/docs/simplification-todo.md).

These are local evidence links. FG02 tracks portable evidence delivery; no new
source acquisition, full-history claim or deployment was performed for this
documentation census.

Operational follow-up: the scheduled committee job at `43c06b6` refused to start
acquisition because its API credential was missing. [FG16](../fec-gaps.md#fg16-operate-repeatable-refresh-and-durable-recovery)
records the confirmed blocker and [run evidence](https://github.com/mikewolfd/spicy-regs/actions/runs/35645971109).
That failed attempt does not change any retained coverage count above.
