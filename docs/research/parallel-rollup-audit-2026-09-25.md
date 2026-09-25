# Parallel rollup audit — September 25, 2026

The audits found a new Table III omission and confirmed the private-law state
and compiled-hearing date defects. The remaining outputs have different levels
of proof: some qualify at their frozen pins, some have only publication or
conservation checks, and others remain partly audited. The campaign is open.

The [repair proposal](rollup-audit-repair-plan-2026-09-25.md) maps these findings
to concrete changes, owners, reprocessing rules and acceptance evidence. Its
changes are proposed; this audit's dispositions remain unchanged.

Three independent auditors ran at **medium effort**, covering congressional
people and votes, congressional documents, and external sources. The parent
agent audited regulatory outputs. Each workstream followed the
[semi-formal code review skill](/Users/mikewolfd/.agents/skills/semi-formal-code-review/SKILL.md),
read native inputs and public outputs, and retained scripts and receipts.
Cross-reviews checked the congressional findings, external-source claims and
regulatory replay methods. This audit changed documentation and retained local
evidence; it did not repair or publish production data.

## Scope and results

The audit froze the public index at **17:13:48 UTC**, code
`03541c7d917b3ea5ed2eb88071590c13b45e34f6`. Its SHA-256 is
`0f7b0dc06a1f59d131de15d5ca679a4037c5dac1593712057e77aa2b9caa7301`.
[The normalized results][summary] preserve every workstream disposition and
the hashes of the underlying result files.

| Disposition | Audited output keys | Meaning |
| --- | ---: | --- |
| Qualified with limits | 34 | The stated source, transformation or carried-forward scope passes; broader completeness is not implied. |
| Partial | 27 | Public evidence and some source checks pass; specified source or semantic proof remains missing. |
| Verified publication | 3 | Comments, their index and the agency partition pattern pass publication/coverage checks; wider native-source qualification remains open. |
| Verified empty, uncomputed | 6 | Empty bill model/backfill outputs are unchanged. Processing and coverage remain unfinished. |
| Failed | 3 | The frozen output reproduces a specific defect described below. |

These are distinct audited output keys, including one agency partition pattern.
They are not producer counts, ledger-row counts or a campaign completion
percentage. Unchanged FEC and court bulk families retain their earlier audits;
this pass did not repeat their full source qualification.

At the **17:36:54 UTC** end check, the base object ETags and rulemaking pointer
were unchanged. CRS alone had advanced from the audited `35a43885…` to
`7076f282…`; that newer generation remains unaudited. The ledger retains the
qualified frozen pin. See [the end check][end].

## Confirmed defects

| Output | Evidence | Required correction |
| --- | --- | --- |
| `table3_records` at `c9d35661…` | Act 119-37 contains 110 native rows but only 106 published rows. Four meaningful code-reference rows lack an act-section label and are filtered out. Both retained and fresh HTML reproduce the omission. | SpicyDocs must preserve rows with missing act-section labels. SpicyRegs must retain their native absence and verify source-row conservation before publishing. |
| `laws` at `c9d35661…` | Private laws 119-1 and 119-2 still say `uslm_outcome=not_requested`, although retained native USLM and the acquisition log prove captured-but-refused reads. | Preserve captured bytes, available title/date/citation fields and a truthful partial/refused outcome. Do not invent the missing Statutes at Large citation. |
| `hearing_bill_links` at `b188e1b3…` | `CHRG-117shrg56721` links to `117-s-2792` with one `held_date`; native MODS lists eleven dates. | Preserve all dates and populate a scalar only when unique; invalidate affected read checkpoints and reprocess. |

The omitted Table III references are 7 USC 2254 (139 Stat. 511),
42 USC 1769g and 1758 (139 Stat. 534), and 2 USC 60a note
(139 Stat. 563, `Elim.`). The traversal repair did publish a larger population;
the old early-stop diagnosis no longer describes this generation. The current
defect is row loss within a successfully read page.

Today's attempted private-law XML requests returned HTTP 200 **HTML**, not USLM.
Independent review caught and corrected the initial interpretation. The finding
rests on separately parsed, digest-verified prior USLM captures and the
captured-but-refused run log. HTTP status alone did not qualify those new reads.

The [congressional document report][documents] traces each defect through source
parsing and publication and records the inspected tests. No production test
suite was run for this documentation-only reconciliation.

## What the passing checks establish

- **Regulatory transformations:** independent full replays reproduce feed
  summary, agency statistics, monthly volume, discovery signals, docket search,
  organization/committee links and Federal Register docket links. These prove
  the stated computations over exact parents. Organization names remain
  heuristic matches, not verified identities or money flows.
- **Regulatory sources:** every previously qualified Federal Register row is
  unchanged; all added September 24 issue rows match native JSON. CFR and the
  retained Agenda edition carry exactly identical table bytes, preserving their
  earlier qualification limits.
- **Congressional people and votes:** members/terms and committee assignments
  preserve their substantive populations. Added Senate votes and positions match
  official XML; the complete member-term join matches an independent replay.
  Meetings, record issues, press, nominations and treaties qualify at their
  recorded scope. The community crosswalk, party-history, stale vote-action
  index and carried-source freshness limits remain explicit.
- **External sources:** FCC proceedings and filings, the frozen CRS generation,
  GAO and lobbying qualify through complete carried-row conservation plus the
  documented native checks of new or changed records. This does not establish
  full history or eliminate later source restamps.
- **Congressional documents:** `law_code_sections`, report metadata and bodies,
  hearing transcripts and read checkpoints qualify at their stated scope.
  All six previously refused hearing detail reads now have retained valid
  responses. Report sections remain partial because the changed heading/body
  decomposition was not independently reconstructed exactly.

Workstream reports retain the exact counts, pins, cell comparisons, samples and
limitations: [regulatory][regulatory], [people and votes][people],
[documents][documents], and [external sources][external].

## Remaining work, in execution order

1. **Repair the three reproduced defects.** Preserve the broader public
   populations, add meaningful regression checks from the retained native
   examples, then independently replay, publish and read back each affected
   family. The source owner is SpicyDocs; SpicyRegs owns output/state handling.
2. **Finish the partial congressional audits.** Check new bill bodies, sections,
   differences and metadata beyond the sampled native cohort; older-Congress
   subject additions; new print documents/actions/citation spans; exact changed
   report-section decomposition; and the remaining communications detail fills.
   Amendments need capture-time source evidence to resolve later source drift.
   Empty models and backfills still require actual computation and acquisition.
3. **Retain missing source evidence for external refreshes.** SAM's scheduled
   2002 selection succeeded and preserved the qualified 2026 population, but its
   complete new extract was not retained. USAspending mixes observation times
   without per-row dates. Court dockets have later native field drift without
   capture-time evidence. Keep these current generations partial; retain raw
   responses in ordinary refreshes so later audits can reproduce them.
4. **Complete regulatory semantic and native-source checks.** The new dockets
   and documents pass bounded native samples, not a whole-population audit.
   Rulemaking passes all recorded digest/reference checks, but retirement and
   identity semantics require a complete explanation against decisions 32–33.
   Removed no-action shells may legitimately have no successor; their absence
   alone is not a defect. Wider comments source repair and the dated partition
   layout remain open.
5. **Qualify operations and consumers separately.** Local comments publication
   and dependent refreshes succeeded. Hosted export capacity, the resumed
   incremental sweep, hosted MCP and representative cross-source joins remain
   separate gates in the [execution plan](../fork-generation.md). This audit
   does not establish their completion.

The [output ledger](fork-output-ledger-2026-09-21.md) owns qualified pins and
delivery state. The [execution plan](../fork-generation.md) owns the backlog.
The receipt directory owns detailed audit measurements and exact source bytes.

Independent reconciliation review found no material overclaim in these status
changes. The [ledger pin check][pins] reports `OK=39`, `NO-PIN=7`, `DRIFT=12`,
`NOT-LIVE=0`, `MALFORMED=0`. Its nonzero exit reflects the explicitly pending
generations. These ledger-row checks compare pins or base ETags; an `OK` result
does not mean the corresponding source audit is complete.

[summary]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/audit-summary.json
[end]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/end.json
[regulatory]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/regulatory/REPORT.md
[people]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/congress-people/REPORT.md
[documents]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/congress-documents/REPORT.md
[external]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/external-sources/REPORT.md
[pins]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/ledger-pin-check.txt
