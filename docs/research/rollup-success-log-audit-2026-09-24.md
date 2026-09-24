# Successful rollup log audit — September 24, 2026

Successful workflows still contain acquisition gaps and misleading metadata.
The main unresolved findings are skipped historical hearing parts, an ambiguous
hearing date, private-law acquisition states and missing recipient observation
dates. Earlier nomination, meeting, citation and bill regressions have separate
repair evidence; a green historical run does not inherit those later repairs.

## What was read

The audit covers successful fork executions completed from **September 23,
18:44:20 UTC through September 24, 18:44:20 UTC**. Full pagination found 85
successful executions in that interval. All 34 exact `Run rollup` steps were
read in full: 2,489 lines covering acquisition, transformation, merges and
publication. The other 51 successes have no such step and are inventoried.
Completion time determines inclusion; failed, cancelled and later remediation
runs are separate.

The [complete report and per-run table][report] link every retained log. The
[coverage manifest][manifest] records all run IDs, attempts, executed revisions,
step times and hashes; [review dispositions][dispositions] cover every read
step. An [independent inventory and hash check][crosscheck] confirms the same
run population and log bytes. This audit follows concrete concerns into raw
sources and public files; it does not qualify every newly published generation.

## Issues that still need work

| Finding | Verified effect | Next action |
| --- | --- | --- |
| Private laws | [Laws run 35942931112](https://github.com/mikewolfd/spicy-regs/actions/runs/35942931112) acquired both 119th private-law XML files, then refused their missing Statutes at Large citations. The public rows retain their identities but still say `uslm_outcome=not_requested`, with no captured hash, title or approval date. Fresh source reads confirm valid XML and absent citations. | Represent the acquired source and explicit partial/refused state at the source/model boundary; preserve available fields and retry policy without inventing a citation. |
| Historical hearing parts | [Report run 36032871739](https://github.com/mikewolfd/spicy-regs/actions/runs/36032871739) rejects native IDs `CHRG-79jhrg79716p19` and `CHRG-79jhrg79716p11` before acquisition or checkpointing. Fresh GovInfo summaries confirm both exist. | Add source-qualified historical part identity support in SpicyDocs. |
| Compiled hearing date | The current link `(CHRG-117shrg56721, 117-s-2792, mods_cover)` selects the first of 11 native dates. The COVER relationship itself is correct. | Retain all native dates; populate `held_date` only when unique. Release/pin the source change and invalidate CHRG checkpoints before rebuilding. |
| USAspending freshness | [Run 36040206247](https://github.com/mikewolfd/spicy-regs/actions/runs/36040206247) refreshes 10,000 recipients and retains 10,218 identities. The 218 outside the fresh selection keep older trailing-12-month amounts without an observation date. | Add per-recipient observation timing. The incorrect “all-time” description is corrected in `150dabc`, with its required coverage label restored in `71b78b6`. |
| Routine source retention | Most ordinary external-source rollups do not opt into the shared response-retention path. Members and reports do; FEC has a separate mechanism. | Complete T18 retention for external reads. Manual audit captures do not preserve every future scheduled response. |

The report/hearing [source audit][hearing] qualifies the current report,
section, transcript and read-status tables. Hearing links retain their earlier
qualified pin because of the date defect. Six Congress.gov detail requests
still return 404: their GovInfo bodies survive and their `detail_refused`
checkpoints correctly request a retry. The package-refusal total and detail
refusal total describe different acquisition stages.

## Explicit backlogs and repaired defects

- **Table III:** the audited laws job acquired zero new rows and kept 65;
  its old traversal stopped with 106 numbered acts unvisited. That does not
  imply every unvisited act has a source page. The chain-based repair is
  already merged and live-tested; its next requirement is qualified public
  output.
- **Bill subjects:** [run 35933694138](https://github.com/mikewolfd/spicy-regs/actions/runs/35933694138)
  used `SKIP_UPLOAD=true`. Its 86,713-row candidate did not replace the public
  20,013-row table. The run deferred 54 bulk folders and 1,338 selected API
  bills at its deadline. Apply the merged source-key correction, qualify and
  publish before claiming a public refresh.
- **House communications and print citations:** the detail and discovery
  caps leave work for later runs. The qualified communications table retains
  1,992 list-only rows; the print selection defers 41 packages. These are
  logged acquisition bounds, with prior rows preserved.
- **Historical omissions:** later source-qualified publications recovered
  the four missed nominations (`f4d1a9a3…`), filled all 28 `NoChamber` meeting
  details (`7ad94916…`) and corrected held-parent citations (`a5d46aa6…`).
  The multipart report migration preserves every earlier body and section.
  The [output ledger](fork-output-ledger-2026-09-21.md) records the subsequent
  bill-family repair and its source/conservation checks.

The skipped Cloudflare purge is expected for the verified uncached `r2.dev`
serving path. Unchanged editions and byte-identical output reuse are also
expected. The committee count discrepancy, unmatched member-term joins,
textless search records and FCC document-to-docket reductions have explicit
source or selection explanations in the full report. None alone establishes
unintended row loss.

New public pins for GAO, CRS and court dockets still require their own source
qualification. No warning in a log can establish that every native field or
historical source record is represented correctly.

[report]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/rollup-success-log-audit-2026-09-24/findings.md
[manifest]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/rollup-success-log-audit-2026-09-24/coverage-manifest.json
[dispositions]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/rollup-success-log-audit-2026-09-24/review-dispositions.json
[crosscheck]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-delivery-2026-09-24/log-coverage-verified.json
[hearing]: /Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/report-hearing-qualification-2026-09-24/README.md
