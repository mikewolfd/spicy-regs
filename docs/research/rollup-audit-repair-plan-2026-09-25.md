# Proposed fixes for the September 25 rollup audit

**Proposal, not implemented.** Fix the reproduced source-fidelity defects,
retain the evidence ordinary refreshes already receive, then close the partial
audits against exact generations. Keep correct computations and documented
source limits. The [execution backlog](../fork-generation.md) remains the task
list; this document supplies implementation choices and acceptance evidence for
the [audit findings](parallel-rollup-audit-2026-09-25.md).
The [coverage map](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/parallel-rollup-audit-2026-09-25/repair-coverage.json)
assigns every audited output to the work packages below without changing its
audit disposition.

The checked trees are SpicyRegs `03541c7` and SpicyDocs `4310647`, with local
changes in both repositories. The three reproduced defects remain in those
trees. Comments publication efficiency and FCC traversal/streaming changes
already exist locally; reuse and qualify that work rather than implementing
competing versions.

## Ownership and order

SpicyDocs owns native acquisition, parsing and capture outcomes. SpicyRegs owns
table materialization, application relationships, checkpoints, publication and
catalog/MCP descriptions. Use existing source readers, `CaptureEvidence`,
generation admission and parent replacement; no new acquisition framework or
audit service is needed.

| Work package | Existing tasks | Owner | Can proceed independently |
| --- | --- | --- | --- |
| R1 — Law and Table III fidelity | T08 | SpicyDocs, then SpicyRegs laws | Yes, with one owner for the laws family |
| R2 — Hearing dates, historical parts and sections | T10 | SpicyDocs, then SpicyRegs reports | Yes, separate from laws |
| R3 — Retained evidence and observation dates | T03/T12/T14/T18 | Source adapters and SpicyRegs rollups | Yes; coordinate edits to the shared evidence helper |
| R4 — Congressional delta audits and vote links | T08/T09/T16 | SpicyRegs, source owner for demonstrated parser defects | Audits can start from retained bytes; new qualification runs depend on R3 |
| R5 — Regulatory source and identity proof | T06/T07/T17 | SpicyRegs with native SpicyDocs inputs | Yes, over frozen parent generations |
| R6 — Uncomputed outputs and wider coverage | T05/T09/T12–T14/T16 | Respective producer owners | Yes, within the recorded source selections and rate budgets |
| R7 — Hosted operation and consumer proof | T18–T20 | SpicyRegs operations/catalog | Qualify the existing local implementation; deploy after its own checks |
| R8 — Reusable audit checks | T02/T18/T20 | Audit tooling | Yes; no production source-model change needed |

R1 and R2 address known incorrect values first. R3 prevents further evidence
gaps and can run alongside them. R4 and R5 turn existing partial results into
bounded, repeatable proofs. Serialize dependency adoption and publication for
each family; independent builds and reviews can run in parallel.

## R1 — Preserve law evidence and every Table III row

### Missing Table III rows

The source parser currently requires a truthy `actsection`, and the host skips
already held acts. Changing only the parser would leave the public defect in
place. Its `(act_key, seq)` key also means adding omitted rows shifts later
positions. See [the parser](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/sources/uscode/table3.py:157),
the host walk (`src/spicy_regs/transforms/build_laws.py:432`) and
parent replacement (`src/spicy_regs/transforms/table_merge.py:168`).

Proposed change:

- Recognize a Table III data row from its native row structure and meaningful
  cells/links, not from one required text value. Keep missing act-section text
  explicitly null. Ignore purely decorative empty rows without inventing a
  section label or copying a neighboring label.
- Reuse successful-read checkpoint metadata keyed by act, capture digest and
  parser-rule version. An old or missing rule version requires reprocessing;
  a failed reread preserves the prior act and remains retryable.
- Queue stale held acts separately from forward chain discovery. Starting the
  walk at the highest held act cannot repair older pages. Charge rereads and
  new discoveries to the existing run budget and resume the unfinished queue.
- Replace each successfully reparsed act's complete row set using
  `replace_parents`, then enumerate its native row order. Do not append the
  newly found rows to the old filtered sequence. Bind the corrected sequence
  to the source capture and generation; document the positional identity
  correction for consumers.
- Replay all held acts with the corrected reader, not only 119-37. Keep failed
  and out-of-scope acts intact. For a new Congress, use the last verified
  served prior-Congress page as a traversal seed and accept the next act only
  when the native link states it; do not guess a future URL or equate a site
  template with absence.

Acceptance: the retained 119-37 page yields every native meaningful row,
including the four audited omissions, in source order. Every held act has a
source-row conservation result. A failed read, an empty successful selected
scope and a parser-version change exercise distinct replacement behavior.
No other act changes without source or rule evidence.

### False private-law acquisition state

[The law shaper](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/schemas/law_tables.py:184)
requires a Statutes citation for every captured USLM record. The
host refusal path (`src/spicy_regs/transforms/build_laws.py:242`) then
discards valid captured metadata and preserves `not_requested`.

Proposed change:

- Separate a successful acquisition from completeness of the projected
  citation. Add explicit `captured_partial` and `captured_refused` outcomes,
  plus a bounded reason field. `not_requested` means no attempted read;
  attempted transport failures get an error outcome in the read evidence and,
  when no prior valid body exists, `request_failed` in the law row.
- An identity-matched USLM file with no native Statutes citation becomes
  `captured_partial`, retaining its digest, observation time, title, approval
  date and literal native citations. Append `uslm_citable_as_json` so the
  private-law citation itself remains available. Keep the Statutes fields null
  with reason `statutes_citation_not_stated`.
- If parsing or identity checks fail, retain the capture and refusal reason;
  do not copy unvalidated law fields. Preserve an earlier valid body and its
  values on a later failed attempt, with the new attempt recorded separately.
  Keep `unavailable` reserved for supported source-absence evidence.
- Pass source capture evidence into the laws rollup. Include parsing-rule
  version in its completed-read decision; partial/refused reads remain in the
  bounded retry policy instead of being skipped by an unchanged list stamp.

Acceptance: both retained private-law USLM fixtures preserve their available
fields and never say `not_requested`. A valid public-law citation still maps
unchanged; wrong-identity XML, HTTP 200 HTML, unavailable sources and transport
failure produce distinct truthful outcomes. Update the existing test that
currently expects the incorrect state. Rebuild the complete laws family while
preserving unaffected classification rows and broader law coverage.

## R2 — Correct hearing dates and finish report qualification

### Compiled hearing dates

[The native reader](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/sources/govinfo/bodies.py:839)
takes the first `heldDate`. Its result propagates through the
[relationship model](/Users/mikewolfd/Work/spicy-stack/spicy-docs/src/spicy_docs/interpretation/hearing_bill_links.py:224)
and public schema.

Proposed change: retain the full source-ordered date list in the native model
and append `held_dates_json` to hearing links. Populate `held_date` only when
the source states one distinct date; otherwise publish null. Keep the existing
hearing/bill/source identity and COVER relationship. A compiled volume's list
of dates does not prove every bill was considered on every date, so do not
manufacture a bill-by-date cross-product. Agenda matching needs an explicit
event/date match; list membership alone cannot establish it.

Bump the hearing relationship rule version used by
`committee_report_reads` (`src/spicy_regs/transforms/committee_report_reads.py:26`),
adopt the source package, and reprocess the affected CHRG scope. Replace each
successfully evaluated package's full relationship set, including relationships
no longer emitted. A failed acquisition retains the prior rows/checkpoint.

Acceptance: the audited compiled volume retains all eleven dates and has no
false scalar date. Single-date hearings remain unchanged; no-date and repeated
identical-date fixtures are explicit. All prior valid COVER identities survive,
and the checkpoint version actually causes the correction to run.

### Historical parts, exact sections and body completeness

- Extend the GovInfo package grammar for the two evidenced historical part
  forms only, with native summary/MODS identity checks. Fetch and checkpoint
  those parts normally; do not accept arbitrary suffixes by loosening the
  grammar globally.
- For `CRPT-119srpt35`, independently reconstruct every changed section from
  the exact retained native text slice and the documented heading-absorption
  policy. Check order, contiguous offsets, exact heading/body values and full
  text coverage. Substring inclusion alone is insufficient. Fix the parser
  only if this exact replay shows a mismatch; otherwise advance the audit pin.
- Preserve HTML placeholder text faithfully, but expose its body-completeness
  status. For the affected selected reports/hearings, acquire the publisher's
  offered PDF and use the existing extraction path, retaining original bytes,
  extraction version and source-page references. Do not silently substitute
  extracted PDF text under the old HTML digest.

The formerly refused hearing details are already resolved at the audited pin;
they need regression coverage and routine retention, not another bespoke repair.

## R3 — Make each new generation reproducible

Use `CaptureEvidence` (`src/spicy_regs/source_evidence.py:41`) and the
`RollupPipeline` evidence hooks (`src/spicy_regs/pipelines/rollups/base.py:119`).
The report family already demonstrates this path. Wire captures before adapters
discard them to yield bare dictionaries. Enable retention in the affected laws,
congressional indexes/amendments, bill/subject/print and external-source rollups.

Retain the exact selection/request body, response bytes or streamed file,
source locator, observation time, digest, continuation/count evidence,
completion/refusal outcome and parser version. Scrub credentials using the
source owner's existing checks. Admit the source artifact and bind it into the
output generation. Carry unchanged rows through their qualified prior
generation; retention does not require reacquiring every unchanged record.

For large SAM extracts and bill archives, add a bounded file/stream ingestion
path to the existing evidence store. Do not funnel whole bulk files through
the helper's in-memory `capture.body` path. Verify size/digest and retain the
selection manifest before parsing; evidence retention failure must prevent a
new generation from claiming reproducibility.

| Current gap | Concrete fix | Acceptance |
| --- | --- | --- |
| SAM's added year lacks its complete extract | Locate an exact original capture if one survives; otherwise reacquire that selected year, retain it and publish a newly qualified generation. Keep the earlier year and `(uei, entity_eft_indicator)` identity. | Replay every selected extract record and every published field; reconcile identity/count sets and successful selection termination. Three samples cannot qualify the extract. |
| USAspending mixes observation times | Append `observed_at` and a source-capture digest to each newly read recipient row. Carry older values with their original time/digest; unknown historical times remain null. Record the ranking selection and response time rather than inventing exact trailing-window boundaries. | All refreshed fields match retained responses; carried rows never receive the new run's timestamp. Identify carried rows directly, not by count subtraction. Expand the merge/schema explicitly for old files. |
| Amendments and court dockets drift after publication | Retain exact list/detail/search responses during a new bounded refresh. Compare every changed/new row to its run's captures; preserve publisher order and distinguish order-only changes from changed sets. | Every changed field is explained by a captured source value or named transformation. A later fresh value does not retroactively prove or disprove the old one. |
| CRS advances while being audited | Freeze each audit to an immutable family generation, retain its raw inputs, then separately qualify any later generation. | The recorded qualified pin never silently moves to the latest public pin. Metadata restamps remain distinct from business-field changes. |
| HTTP 200 HTML mistaken for requested XML | Require expected native shape and identity in readers and independent audit probes. Retain unexpected bodies as refusals. | The retained HTML examples fail XML/USLM admission even with status 200; valid USLM succeeds. |

Missing historical response bytes cannot be recreated by relabeling a current
download. Keep the old generation's evidence gap explicit and close the selected
population through a new captured run. Apply routine retention to the already
qualified FCC, CRS, GAO and lobbying routes too; their scoped audit passes do
not remove this operational need.

## R4 — Close every partial congressional delta

Start with exact qualified predecessor and candidate generations. Independently
compare complete changed/new populations; unchanged records can inherit exact
prior proof. Reuse retained originals where available and use R3 for missing
captures. If old capture-time values cannot be proved, qualify a new generation.

| Audited outputs | Work needed before promotion |
| --- | --- |
| `congress_bills`, `bill_committees`, `bill_family_archives` | Replay every changed metadata field and archive descriptor/checkpoint against retained BILLSTATUS archives or captured native detail. Check archive digest, membership, completeness and source dates separately from local observation time. Preserve the previous timestamp/URL repairs and intended older population. |
| `bill_publisher_summaries`, `bill_versions`, `bill_sections`, `public_activity_events` | Check every added summary text and version identity against original bytes; verify body digests, extraction/refusal state, section boundaries and event references. A metadata match cannot qualify the body. |
| `section_diffs`, `section_diff_items` | First qualify both source versions and their ordering; independently apply/compare the diff items to reconstruct the target text. Check added/removed/moved sections and partial/refused inputs without using the production diff as its own oracle. |
| `bill_subjects` | Complete the older-Congress additions and changed-record replay, comparing policy area and complete subject sets. Reuse bulk BILLSTATUS before per-bill calls. Preserve explicit source refusal or absence; do not turn a cap into empty subjects. |
| `house_communications` | Replay all unsampled newly filled details and finish the remaining list-only queue under existing budgets/checkpoints. Prove list membership through stable pooled identity sets, retaining source discrepancies rather than assuming a single walk is complete. |
| `house_activity_reports`, `budget_volumes`, `bill_committee_actions`, `document_citations` | Retain and independently extract every new parent. Compare literal source spans, coordinates, normalized citations and native action context; recount all child totals. Use the second extractor to challenge discrepancies, not to approve all rows from a small phrase sample. |
| `report_sections` | Exact changed section decomposition under R2. |
| `amendments` | Complete changed-row capture-time proof under R3. |

Also repair the known stale `match_action_index` values in roll-call votes.
The current builder (`src/spicy_regs/transforms/build_roll_call_votes.py:387`)
skips held votes before refreshing the derived bill-reference fields. Recompute
only those relationship fields from the pinned current bill-reference input for
all held votes; preserve native tallies, dates and member positions. Record the
link rule/input version, including conflicts and unresolved cases. The two
audited Senate indices must match the current first-reference rule, with every
other native vote field unchanged. Never infer missing links from bill names.

Keep the API/BILLSTATUS freshness gap as separate source work: add the already
planned bounded API delta refresh under the sole bill-family writer, retain
per-field source provenance, and verify that a later native observation cannot
be overwritten by an older archive. Do not revive a competing bill-table writer.

## R5 — Prove regulatory source coverage and identity changes

The transformation replays passed. Keep those algorithms unless a new native
counterexample requires a change.

- **Dockets/documents:** index retained mirror captures by native identity,
  source version, locator and digest, including numbered versions. Replay the
  entire catch-up delta and carry unchanged qualified rows by exact equality.
  A plain filename is not necessarily the newest observation. Keep unresolved
  observations explicit; five matching samples are not complete proof.
- **Comments/index/agency mirror:** preserve the proved IDs, coverage and null
  relationships. Complete native-field and text checks for the unqualified
  source cohorts. Treat the empty response as a retryable refusal, never an
  empty comment. Route oversized texts through a bounded retained-file path
  or keep an explicit size refusal; do not simply raise an in-memory cap.
  Apply the same capture/extraction evidence to document bodies and PDF text.
- **Rulemaking:** independently reconstruct the action-docket and document
  union rules in decisions 32–33 over frozen parents. Classify every prior
  proceeding as retained, split, merged or legitimately retired, with the
  source evidence explaining that class. Check all affected agenda and comment
  period anchors and all target edges. A retired no-action shell needs a reason,
  not a fabricated successor. Publication requires zero unexplained removals,
  zero dangling references and complete evidence coverage for the selected run.
- **Dated comments partitions:** this is an unproduced output, not a broken
  monolith. Materialize the declared agency/docket/year/month layout from the
  pinned catalog through the existing bounded writer, preserving null-key policy
  and exact multiset coverage. Do not use it to delay the already verified agency
  mirror or duplicate its correctness logic. Any scope change belongs in the
  existing decisions record, not an undocumented omission.

## R6 — Finish uncomputed work and represent deliberate limits

The empty `financial_changes`, `section_classifications`, `bill_summaries`,
`diff_summaries`, `bill_family_backfills` and `bill_family_backfill_walks`
remain unfinished. Confirm model access, process qualified bodies/version pairs
through the existing model path, and retain model/version/prompt/input/output
evidence. Models produce derived claims, not source facts. Run resumable older
Congress backfills against explicit source membership. A missing dependency or
failed request must stay blocked/retryable instead of producing a completed
empty result. A successful run with genuinely no eligible findings is different
from never having run.

The following audited limitations require preserved metadata or bounded source
work, not fabricated records or broad rewrites:

| Limit | Proposed disposition |
| --- | --- |
| Community member crosswalk, missing Ed Case term, within-term party history | Reconcile native official term evidence; preserve a separately evidenced correction when supported. Represent party intervals independently of the final listed term. Do not invent dates or merge conflicting observations. Keep the current member/term identities. |
| Native member website URLs omitted | Add a source-faithful website list to the member schema when adopting those fields; bind it to the capture and keep missing values explicit. This is additive metadata, not evidence of roster completeness. |
| Unlisted committee seats and declared/served discrepancies | Retain complete native enumeration receipts, unresolved seat observations and explicit reasons. Add relationships only when the publisher supplies the missing identity/link. Do not force membership counts to match by inventing seats. |
| Inclusive-end member-term fallback and unmatched positions | Keep the documented unique fallback and explicit unmatched/ambiguous states. Require new native term evidence before changing those mappings. |
| Carried nominations/treaties/press and later metadata restamps | Preserve observation times and carried status; refresh through retained native reads. A new run timestamp must not imply each held record was reread. Keep list parent identities when only parts are currently listed unless the source proves retirement. |
| GAO default report label, CFR printed ranges/null citation keys, docket-search omissions and date exclusions | Keep the stated source/derived/default distinctions in the dictionary and MCP. Test the existing explicit rules; no automatic coercion, synthetic citations or made-up search content. |
| Organization-to-committee name matches | Keep heuristic method/confidence/evidence visible. Add source-ID-backed confirmations separately if acquired; no inferred donation flow or identity claim. |
| Bounded SAM, lobbying, FCC, CRS/GAO, APA court and legislative populations | Continue the existing explicit year/date/Congress/source selections with resume and terminal membership proof. Keep court bulk and APA search as different populations. Use the local FCC crowded-window traversal work after qualification. Wider data remains T12–T14/T16, not an implied result of a green audit. |
| Retained CFR/Agenda editions and unchanged FEC/court bulk seeds | Preserve the qualified pins and edition/cycle scope. New editions and T05 FEC expansion receive their own acquisition and source audits; no re-download of unchanged qualified originals is needed. |

## R7 — Qualify existing operational changes and the consumer path

The [comments efficiency implementation](comments-publication-efficiency-2026-09-25.md)
already addresses hosted memory costs locally. Review and qualify that work on
the actual hosted limits, including full publication/readback, interrupted retry,
empty-agency replacement, unchanged-input reuse, finalization and the browser
workload. Resume incremental ETL only after the existing hosted gate passes,
then qualify one complete resumed sweep. Keep successful local publication
distinct from hosted capacity.

Deploy the configured MCP consumer through its existing path and verify exact
fork generations, native source locators, query coverage and representative
cross-source joins. Expose published generation, qualified scope/pin and
unresolved/heuristic relationship states distinctly. A client must be able to
tell that a source can supply a relationship without being told that the
relationship has already been acquired or verified. Reuse current catalog and
dictionary metadata; no new graph service is required.

## R8 — Retain the independent checks in reusable audit scripts

Move the peer review's schema-type and duplicate/null-identity checks into the
audit scripts that originally checked only names or constructed dictionaries.
Check duplicates before indexing rows by identity and compare multisets where
source duplicates are meaningful. Add the retained HTTP 200 HTML examples to
native-shape checks. These weaknesses were independently closed for the frozen
population; this work prevents the same audit blind spots on later runs.

Keep frozen manifests, exact response digests, complete expected values and
per-output dispositions. Separate publication verification, source qualification,
empty/uncomputed state and deployment. `check_ledger_pins.py` compares live pins;
it is not a semantic quality gate. Link its result to the existing machine-readable
audit summary rather than interpreting `OK` as completion.

## Release and closure checks

For each affected family: retain source fixtures, implement in the source owner,
run meaningful source/refusal/identity tests, release and pin the dependency,
then run a build-only candidate through independent raw/output comparison.
Verify whole-family membership, intended parent replacement, unaffected-row
conservation, source evidence admission and checkpoint invalidation before
publication. Re-read anonymous public bytes and exercise the affected consumer
query afterward. Serialize against the latest family generation so a correction
does not overwrite newer observations.

Review law and hearing schema changes together with all local consumers, table
metadata and generated documentation. Do not apply fieldwise coalescing to hide
the deliberate removal of a false scalar date or to keep stale child rows.
Retain earlier immutable generations for diagnosis and recovery.

Close an item only at the boundary its evidence proves. A missing historical
capture stays documented even after a newly qualified generation supersedes it.
No audit result authorizes shrinking the intended population or marking
uncomputed outputs complete.
