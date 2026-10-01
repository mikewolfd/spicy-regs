# FEC delivery plan and tasks

Saved September 30, 2026; task status updated October 1, 2026. This plan finishes the retained Federal Election
Commission (FEC) data, makes useful tables available through SpicyRegs, and moves
recoverable originals to R2 before reclaiming local space. It also preserves the
later historical expansion plan for the 2007–2008 through 2025–2026 cycles.

The recommended sequence is to keep the existing evidence and identity tables,
add useful typed tables from the retained non-PDF data, qualify their meaning,
archive the complete inputs, publish the tables, and then delete only verified
local source copies. The [data model](fec-data-model.md) defines the row meanings,
keys, relationships, financial rules and proposed tables.

**This is the implementation design and task register. Retained-data execution is active.**
The implementation checkpoint is committed locally on `fec-publish`:

- `87f7d66` — streaming evidence retention and publication readback.
- `935b300` — retained FEC tables and query integration.
- `26d2761` — delivery design and execution checkpoint.
- `cccda24` — completed FEC query, evidence-parent and fixture corrections.
- `91dd82b` — deployment configuration for the release and image pins.
- `7df589a` — completed local validation records.

These commits have not been pushed. The SpicyDocs source work is already
committed through `bcdde5431fac`. The validated fixture, typing and source-parent
corrections are now committed.

**FR08 is complete at the selected retained non-PDF local boundary.** The
[combined receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/fr08-complete.json) binds the final composition and successful complete
key, evidence, financial and actual local MCP checks. The existing archive
owner accepts that receipt's metadata. The source-parent correction now refuses
missing or advanced evidence generations through real connection and refresh
checks; captured connections retain their original state and rollback works.
The separately sealed candidate history and retained catalog also pass local
MCP acceptance through their existing family owners. See the
[completion checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#complete-local-validation-2026-10-01) for exact coverage, test results and limits.

Current/net financial totals and unsupported amendment or correction claims
remain unqualified. Local acceptance uses explicit candidate image/archive/typed
generation descriptors; the final table generations are now sealed and published, while consumer image/receipt binding and deployed acceptance remain FR12–FR13. Source inputs and table values were preserved. PDF corpus processing
remains deferred.

**FR09 archive admission is complete.** The final selection includes the applied
consumer correction, completed acceptance receipts and separate identity-family
recovery inputs. Every selected source passed its exact hash, size, credential
disposition and staged-copy checks. See the [archive admission checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#complete-archive-admission-2026-10-01).
**FR10 upload and full public SHA-256 readback are complete.** See the
[verified upload checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#verified-r2-archive-upload-2026-10-01).
The requested [sample restore and reader smoke](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr11-smoke-20261001/receipt.json) passed. The owner then requested query-table publication without expanding that smoke into full recovery. The query generation is sealed and published with its required source generation, candidate history and catalog. [Public SQL and CLI checks passed](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01). Full restore, matching consumer deployment and verified source cleanup remain open. Follow the
[next execution checkpoint](#next-execution-checkpoint). The [FG gap register](fec-gaps.md)
continues to own source, semantic and operational gaps; successful local
validation does not close those separate boundaries.

## Scope and release choice

Treat the typed FEC application as greenfield work. Build the new tables and
queries directly; do not add legacy code compatibility, migration layers or
duplicate serving paths. Retained source formats remain inputs to the new
design, including older files already in the selected corpus when they deliver
user value. Prioritize questions about contributions, spending, filings, legal
matters and agency activity. Account for retained discovery and operational
metadata in the source inventory; do not invent new application tables solely
to reproduce old research machinery.

| Scope | Included | Completion means |
| --- | --- | --- |
| Immediate retained-data release | Already retained FEC originals, research captures, native outputs and their evidence; useful non-PDF mappings supported by those inputs | Every selected input has a verified result or explicit disposition; delivered tables have qualified meanings; remote inputs can be restored; public outputs work through the consumer |
| Historical expansion | Available official families covering cycle-ending years 2008 through 2026, plus associated calendar-year and fiscal-year material | Each family and period has reconciled source membership and a declared completion state; unavailable periods remain explicit |
| Deferred PDF work | Retained PDF originals, linked PDF bodies and any OCR or extraction | Resume only after the user lifts the processing deferral; retain metadata and existing receipts meanwhile |
| Adjacent work | Broader legislative coverage, lobbying recovery, IRS/state data and unrelated research | Track separately; these do not block a scoped FEC release |

The historical window is a selection rule, not a reason to discard older data
already retained. A cycle ending in 2008 normally covers 2007–2008; preserve the
publisher's actual period rather than assuming every family uses election
cycles. Filings, transactions, legal matters, agency fiscal years and publication
dates have different date meanings. Legal coverage must distinguish matters
opened during the window from older matters with events during the window.
The current 2025–2026 cycle is incomplete in time even if all available snapshot
files are captured.

**The execution goal selects typed query tables before publication.** Finish the
supported mappings across retained non-PDF data, then archive, publish and
verify the release before eligible cleanup. The earlier existing-tables-only
option is no longer the selected execution path. Restore checks remain required
before deletion.

## Starting evidence and status

The [retained qualification and research correction](research/fec-retained-corpus-qualification-2026-09-30.md)
own the completed local selection. Its completion and verification receipts
describe full accounting for the selected retained research, with explicit
source limits; they do not establish complete official FEC history or current
financial totals.

| Boundary | Status recorded on September 30 | Consequence |
| --- | --- | --- |
| Retained non-PDF source adoption | Qualified locally for the selected corpus, including the later research correction | Reuse the sealed baseline and receipts; do not rebuild it merely to save this plan |
| Candidate and committee histories | Candidate history has a qualified local output; the retained committee master and PostgreSQL observations still need their typed history output | Preserve cycle and snapshot identity; keep native PostgreSQL history distinct from a current registry |
| Financial interpretation | Native records retained; proposed typed tables and current-record policies remain work | Physical record counts must not be presented as unique transactions or summed indiscriminately |
| Missing historical body | One retained API receipt has no recoverable original body | Keep the unavailable disposition; a fresh request cannot replace the historical bytes |
| R2 access | A read-only bucket/index check was reported in the earlier session, but no separate access-check receipt is linked here | The blind review could not independently verify this claim. FR01 must refresh and retain the access/index evidence before any remote mutation. |
| Source-store inventory | Preliminary blob-store inventory saved | It is not complete reference closure, verified hashes, or an eligible deletion manifest |
| External delivery of the corrected local corpus | Pending | Existing public FEC tables are an older selection |
| Local cleanup | Pending | No source copies have been deleted by this handoff |

The corrected local observation generation is pinned as:

```text
sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0
```

Its measured output in the September 30 verification receipt contains
80,413,538 physical source observations, 951,374 relationship observations and
9,178 collection rows. These are different row populations. Relationship rows
include explicit absence states; collection rows include metadata dispositions.
They are not counts of donors, financial events or completed source families.
Use the receipt for subsequent comparisons rather than maintaining a second
live count in this document.

The source-store preflight measured approximately 3.12 GB of distinct declared
blob contents in the inspected stores. This excludes other originals,
derivatives and some referenced context. It does not establish the total FEC
footprint or reclaimable space. The earlier roughly 30 GB estimate must not
become a promised cleanup amount.

### Evidence locations

These are local handoff locations, not public download links. The archive task
must replace their operational dependence on this workstation with portable
paths and pinned remote objects. Preserve the original receipts unchanged.

| Evidence | Local location |
| --- | --- |
| Corrected completion and output verification | [completion.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/research-closure-20260930/completion.json), [verification.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/research-closure-20260930/verification.json) |
| Exact selected input manifest | [combined-inputs.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/research-closure-20260930/combined-inputs.json) |
| Local query verification | [mcp-check.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/research-closure-20260930/mcp-check.json) |
| Baseline preservation proof | [table-combination-proof.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/research-closure-20260930/table-combination-proof.json) |
| Available versus retained inventory | [inventory.json](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/availability-inventory-20260930/inventory.json) and the related files in that directory |
| Preliminary archive inventory | [source-store-inventory.json](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-r2-handoff-20260930/source-store-inventory.json) |

The completion receipt records source revision
`bcdde5431fac89799f0f3f76400dfacbc661399f`, consumer revision
`e257fcd2dec371f207a3ef230ef4930138dbaf2d`, and the qualification wheel
`spicy_docs-0.53.0+fec.bcdde5431fac-py3-none-any.whl` with SHA-256
`364a220975c868cc6ea67809f4064c09b9efd72ec54ba02166a95f38bdd29099`.
Those are the recorded build inputs, not a claim about a subsequently updated
branch. The receipt marks that work as neither pushed nor merged at completion.

## Owners and completion rules

| Owner | Responsibility |
| --- | --- |
| SpicyDocs | Source discovery and acquisition, native formats, complete native records, source releases and replayable source evidence |
| SpicyRegs | Useful query tables, source-reported relationships, financial interpretation policies, MCP discovery, generation publication and consumer checks |
| DocSpec | Shared document catalog and body adoption; evaluate its existing structures before adding FEC-specific document storage |
| RefSpec | Governed reference definitions and identifiers where applicable; preserve source authority in mappings |
| Publication operator | Storage, credentials, immutable archive transfer, restore, deployment, refresh and exact local cleanup |

These are repository and operational responsibilities, not new services or
assignments to named people. Source parsing must remain with the source owner;
consumer adapters should not duplicate native readers. Use existing generation,
evidence, publication and download code before adding another lifecycle.

Tasks move through planned, in progress, verified locally, verified remotely,
source-limited, or deferred. A source-limited result is acceptable only with the
exact missing population, reason and impact stated. It does not close a parser
defect or a missing implementation. Successful empty results, unknown counts,
missing bodies, refused inputs and unselected mappings remain distinct.

Each completed task must leave a durable receipt with the selected input pins,
implementation identity, output pins, checks and their scope, remaining limits,
and the next task. Preserve old receipts; append a new checkpoint instead of
rewriting an earlier completion claim.

## Immediate retained-data tasks

Execute these in order. Within a task, independent source families may run in
parallel after their shared schema and checks are fixed. Use separate outputs
and integrate centrally; do not allow concurrent writers to the publication
index or the same generation directory. Read each status at its stated boundary:
committed code, verified local components and public delivery are separate
results. Tasks with unfinished acceptance checks remain open.

| ID | Task and owner | Depends on | Current status and completion evidence |
| --- | --- | --- | --- |
| FR01 | Pin the release scope and baseline — SpicyRegs | None | **Verified starting checkpoint:** typed retained-data scope selected; baseline verified; expected R2 account/bucket reached; stored and public indices agree. Exact bytes, pins and conditional-write tokens are retained in the [execution evidence](research/fec-retained-delivery-execution-2026-09-30.md). Refresh mutable remote state again before publication. |
| FR02 | Enumerate dependencies and qualify capacity — SpicyDocs and operator | FR01 | **Complete for the admitted archive.** The [final admission preparation](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/candidate-v3/verification.json) reserves separate staging and restore copies, failure recovery, PostgreSQL scratch and replay metadata. Actual FR09 admission passed within that capacity reservation. Refresh capacity again before restore. |
| FR03 | Qualify the proposed schema against retained records — SpicyRegs and SpicyDocs | FR02 | **Selected retained schemas verified locally.** The collection register and source census account for the selected financial, legal, agency, registry, filing, context and history populations through qualified mappings or explicit source dispositions. Native-field and stored-cell bulk recovery, exact final composition and the complete local acceptance now pass. Retained definitions preserve unknown and extra positions. See the [completion checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#complete-local-validation-2026-10-01). |
| FR04 | Complete identity and filing joins — SpicyRegs | FR03 | **Retained joins verified locally; amendment replacement remains source-limited.** Full evidence checks verify filing targets, cardinality, exact source coordinates, physical-header associations and native file-number links. Unknown namespaces, unsupported mapper identities and unresolved joins remain explicit. Replacement scope, amendment completeness and deletion-by-omission claims remain unqualified where retained evidence cannot establish them. See the [completion checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#complete-local-validation-2026-10-01). |
| FR05 | Build typed financial tables from retained inputs — SpicyRegs | FR04 | **Complete at the retained bulk-output boundary.** The [V7 terminal receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-readback-recovery-v7/verification.json) proves complete native-field and stored-cell comparison of the main output, unchanged code and bytes, and exact reuse of the other completed collections. The process exited successfully. Final composition also passes; whole-release acceptance remains under FR08. |
| FR06 | Build non-PDF matter, document and agency queries — SpicyRegs and DocSpec | FR05 | **Selected local tables and combined local acceptance verified.** Legal, agency, filing-text and standalone context populations pass their selected replay, assembly and complete key/evidence checks; integration passes under FR08. Keep document, metric and narrative meanings separate. PDF processing remains deferred, and Word structure does not establish rendered-page fidelity. See the [completion checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#complete-local-validation-2026-10-01). |
| FR07 | Qualify current-record selection and aggregation — SpicyRegs | FR05, FR06 | **Reported-measure rules and local runtime verified; broader semantics source-limited.** The full selected populations pass decision/cardinality checks, every observed interpretation/source class is covered by the independent Python comparison, and real MCP binding/refusal works. Current/net totals, transaction deduplication, amendment replacement, cross-row spending aggregation and transfer pairing remain unqualified. Quality notices do not automatically exclude observations. See the [financial receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-v3-20261001/output/financial.json). |
| FR08 | Verify the complete retained release locally — SpicyRegs and SpicyDocs | FR07 | **Complete locally for the selected retained non-PDF release.** The [combined receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/fr08-complete.json) binds final composition and complete keys, evidence, financial and actual local MCP acceptance to the same source selection. The source-parent fix is integrated, exact view SQL is preserved, and missing/advanced parent, typed dependency, policy/image mismatch, refresh and rollback checks pass. Full baseline unit tests, focused post-change tests, lint/type checks, dictionary validation and generated pages pass at their recorded scopes. The existing archive owner accepts the metadata gate. Archive and final sealed/deployed acceptance are tracked separately in FR09–FR13. |
| FR09 | Prepare the portable archive and refresh the capacity estimate — operator | FR02, FR08 | **Complete.** Final code, completed validation controls and candidate/catalog recovery inputs are selected. Full source scanning, exact hashes/sizes, staged copies, artifact sealing and complete selected membership passed. [Archive admission checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#complete-archive-admission-2026-10-01). PDFs and the mixed PDF-bearing archive remain deferred. |
| FR10 | Upload immutable originals and context to R2 — operator | FR09 | **Complete.** Every selected object is stored in the `spicy-regs` bucket and passed fresh public SHA-256/size readback, including reused objects. The existing artifact owner verified the remote controls, exact membership and archive identity. [Verified upload checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#verified-r2-archive-upload-2026-10-01). |
| FR11 | Restore and replay from R2 — SpicyDocs and operator | FR10 | **Requested sample restore/replay smoke passed; full recovery remains open.** The [smoke receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr11-smoke-20261001/receipt.json) records fresh public downloads, exact hashes, CSV/ZIP/native filing replay and a catalog query. The owner requested this bounded smoke, then query-table publication. Full selected membership restoration and every special dependency route remain required before deleting source originals. |
| FR12 | Prepare compatible data and consumer releases — SpicyRegs | FR08, FR10, requested FR11 smoke | **Data generations sealed and published; consumer binding pending.** `fec-query` exactly matches the accepted composition and names the admitted archive and source-parent generation. Existing source, candidate and catalog seals were reused. See the [publication checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01). Bind the actual consumer image and final release receipt, then repeat acceptance with those identities. |
| FR13 | Publish deploy and verify the consumer — operator and SpicyRegs | FR12 | **Table publication and public/CLI checks complete; hosted qualified views pending.** Four FEC families passed full remote byte verification through the existing conditional publisher. Anonymous reads checked all published schemas, counts and sample data; managed CLI downloads checked single-file and partitioned tables. See the [completion receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/query-publication-20261001/completion.json). Deploy the matching image and receipt configuration, then verify hosted views, identity-bearing responses, dependency/policy refusal and rollback. |
| FR14 | Produce the exact local deletion manifest — operator | FR11, FR13 | **Planned.** For each eligible non-PDF source copy, record its unchanged local digest, remote object and restore receipt, size, inode/link information and ownership. Exclude shared, in-use, deferred, unarchived and unresolved files. |
| FR15 | Reclaim and measure local space — operator | FR14 | **Planned.** Recheck the deletion manifest immediately before removing only those copies. Record paths removed, preserved exceptions and actual free-space change; keep queryable tables, code, credentials, manifests and receipts. |
| FR16 | Save the release and recovery handoff — SpicyRegs and operator | FR15 | **Validated implementation and local acceptance committed; full release handoff pending.** See the commit list above. After delivery and eligible cleanup, record public pins, working queries, archive locations, restore instructions, deletion receipts, refresh policy and remaining historical/PDF work. Close FG items only at their verified boundaries. |

The selected release includes typed query tables, so FR03–FR08 remain required
before archive publication and cleanup. Existing evidence tables alone do not
complete the execution goal.

### Early resource checkpoint

FR02 establishes the resource budget before whole-selection mapping, join or
policy work. Bounded schema trials in FR03 may refine that estimate; any larger
run waits until the revised estimate fits. Record measured input sizes, sample
selection and scaling assumptions, the largest partitions, and the peak overlap
of retained generations, typed outputs, evidence/inclusion decisions, decoded
streams, spill, staging, sealing copies, readback and a fresh restore.

The receipt must give concrete byte limits for local scratch/output and memory,
a disk free-space floor, permitted concurrency and a stop rule. Reserve enough
space for failed-attempt recovery and FR11's restore; a logical file-size sum is
not a disk-allocation measurement. Refresh available-space measurements before
each long step. Stop at the declared limit without deleting unverified inputs.
If the bound does not fit, reduce concurrency/partition size, use views or
remote scratch, or narrow the first release's scope and record that choice.
FR09 updates this budget after measuring actual outputs; it is not the first
capacity check.

### Compatible release and deployment checkpoint

Follow the [model's release compatibility rule](fec-data-model.md#release-compatibility-rule).
The chosen default keeps existing source/identity families in place, seals the
selected new typed tables and stored inclusion/evidence outputs together as
`fec-query`, and enables a qualified view only when every dependency it uses
matches its exact qualified pins. A single captured index or a correct schema
does not satisfy this rule. Ad hoc raw-table queries remain available with their
own coverage and pins; they are not automatically qualified financial results.

FR08 uses a candidate dependency receipt during local checks. FR12 finalizes it
after its referenced generations and archive are sealed, then records the
consumer code/image, SQL policy, dictionary and definition identities. The
receipt is an immutable, content-addressed release artifact; the deployment
configuration pins its digest. Keep it outside the generations it references,
and inject its digest at deployment time rather than embedding the final
receipt digest in the image whose digest it records. These rules avoid circular
content hashes. The existing storage and deployment paths remain the owners;
this is a small release record and reader check, not another publication service.

FR13 records the exact running image/configuration and receipt digest before
acceptance. Its mandatory checks include:

1. Publish/read the complete compatible set and verify a qualified query returns
   the selected data, parent, policy, definition and release identities.
2. Advance a required parent after build but before publication, then separately
   before connection refresh. The new connection must refuse the affected
   qualified view when the pins differ, even if all byte/schema checks pass.
   Previously captured compatible connections may finish using their unchanged
   pins; they must not attach newer family rows to that result.
3. Change consumer SQL/policy/dictionary code without the matching receipt. The
   qualified view must refuse, with a specific mismatch reason, while unrelated
   views remain usable.
4. Roll back to a retained data/parent/image/policy/configuration set and repeat
   the query. Test the incompatible intermediate state as well: refuse the
   qualified view until the set matches; preserve unrelated publication families.
5. Verify the uploaded originals and CLI restore path independently of table
   publication. Confirm required parent generations, image and release receipts
   remain retained for the advertised recovery period.

The default design compares exact dependency pins. Serving older parent versions
under the qualified view could be a later explicit alternative, but the first
release must not silently substitute historical or latest parents. An
existing-tables-only release creates no `fec-query` family and makes no new
qualified-current-view promise; FR13 must still verify whether its deployed
consumer needs an update and retain the resulting deployment evidence.

### Mapping to the existing gap register

| Tasks | Related gaps |
| --- | --- |
| FR01–FR03, FR08 | FG01 coverage; FG03 retained PostgreSQL history; FG04 retained bulk adoption; FG05 field definitions; FG08 capacity; FG17 scale |
| FR04–FR07 | FG07 filing formats; FG10 legal identity; FG11 agency adoption; FG12 profiles; FG14 relationships; FG15 financial policies |
| FR09–FR11, FR14–FR15 | FG02 portable evidence; FG08 storage capacity |
| FR12–FR13 | FG16 compatible refresh/recovery; FG20 document/search adoption where selected; FG21 publication and consumer deployment; FG23 only if documentation hosting is explicitly selected |
| FR16 | FG16 refresh/recovery, plus the unresolved gaps carried forward |

## Archive publication and deletion design

The archive preserves recoverable inputs. Table publication serves query
outputs. Both belong in the existing publication system, but passing one does
not prove the other. A Parquet table alone cannot replace the original source,
its dictionaries or its capture evidence.

| Delivery target | Intended location |
| --- | --- |
| Existing R2 bucket | `spicy-regs`; recheck the configured account and bucket before mutation |
| Public data domain | [data.spicygov.ai](https://data.spicygov.ai) |
| Hosted MCP consumer | [mcp.spicygov.ai/mcp](https://mcp.spicygov.ai/mcp) |
| Publication index | Existing `publication.v2.json` and its compatibility index; refresh the current index and conditional-write state at publication time |

These are delivery destinations, not evidence that this new release is already
available there. Use the configured credential mechanism without copying secret
values into this plan, receipts or the archive.

Use the current immutable source-evidence layout in
`src/spicy_regs/sources/publication.py`: blobs addressed by SHA-256 beneath
`source-evidence/blobs/sha256/`, with artifact-specific manifests and evidence
under `source-evidence/<artifact-digest>/`. Use the existing completed evidence
artifact format and verification path. Consult
[generation publication](generation-publication.md) and
[retained-input workflows](fec-retained-workflows.md) for the implemented paths.
Do not build a separate archive service or assume the existing bundle utility
already relocates every newly discovered context path.

The archive manifest should identify:

- Each exact original, its digest and byte size, its retained capture URL/time
  and authority, its remote object key, and every selected collection that uses it.
- The original input manifest plus a separately pinned portable manifest whose
  changes are restricted to declared filesystem relocation. Preserve ordering,
  source selection, timestamps and content hashes.
- Complete source releases, referenced blob stores, dictionaries and definitions,
  acquisition failures/dispositions, manual audits and output-generation pins.
- Required derivation evidence, including PostgreSQL dump, schema, COPY stream,
  tool/version and arguments; archive context that lies outside normal blob roots.
- Explicit missing originals and PDF deferrals. A metadata row does not claim
  that the referenced body exists remotely.
- Remote verification and restore receipts, supported restore procedure and
  implementation/package pins sufficient to repeat it.

Inspect metadata and URLs for credentials before using a public bucket. Never
include `.env`, tokens or unrelated private files. If historical evidence
contains credentials, preserve the exact original in suitably restricted
storage and publish a separately identified safe derivative when needed; do
not silently modify bytes while claiming their original digest.

For cleanup, all of these conditions must hold for **each** deleted copy:

1. Its selected processing and validation are complete for the release scope.
2. Its full original bytes and required context are retained in durable remote
   storage and verified by byte size and SHA-256 after retrieval.
3. The portable manifest resolves it and its dependencies; restore qualification
   covers its format and any unusual dependency path. A sampled replay does not
   substitute for verifying every archived object's bytes.
4. Its local bytes still match the deletion manifest, and no active process,
   other retained release, shared checkout or deferred task requires that local
   path. Shared references need a validated replacement locator before deletion.
5. The release's public outputs and recovery instructions have passed FR13.
6. The path is an explicit eligible non-PDF source copy. Do not recursively
   delete a corpus root, virtual environment, worktree, receipt directory or
   unrelated dataset as a shortcut.

PDF originals stay local under this plan. Existing source and output receipts
remain immutable; new relocation and cleanup receipts explain the change. Keep
archive manifests and restore instructions both locally and remotely. Measure
allocated bytes and hard links as well as logical size, then report actual free
space before and after deletion. Do not promise to recover the apparent size of
every duplicate path.

## Full historical expansion sequence

This is the complete later roadmap preserved from the earlier discussion.
Start it only after the retained-data release has a durable recovery path and
the user chooses to resume expansion. Do not reacquire already qualified inputs
without a freshness or missing-field reason. Execute each step to a recorded
checkpoint before moving to the next; independent family work within a step
may run in parallel with isolated output ownership.

| ID | Work in order | Completion checkpoint |
| --- | --- | --- |
| FH01 | Fix the population and completion rules | A family-by-period matrix states date basis, official route, source authority, formats, target tables, known unavailable years, freshness and what complete means. Include a separate PDF deferral column. |
| FH02 | Reconcile the retained baseline | One manifest references the retained release, research holdings, package pins and public starting generation. Reuse the archive from FR tasks; do not erase older qualification or count overlap as new coverage. |
| FH03 | Resolve storage and processing capacity | Measure compressed originals, decoded streams, outputs, temporary files and recovery reserve. Choose bounded downloads/streaming and remote storage before acquiring large populations; record actual available capacity. |
| FH04 | Finalize schemas against actual records | Qualified family mappings define grains, keys, identifier namespaces, dates, decimals, missing values, conflicting fields and extra columns. The model remains source faithful across historical layouts. |
| FH05 | Expose queryable coverage and interpretation status | Users can distinguish discovered, retained, parsed, mapped, policy-qualified, published and searchable data by family and period, with evidence links and unknown counts preserved. |
| FH06 | Qualify historical format changes on bounded examples | Oldest available, current and intervening changed formats pass independent raw/output comparisons, including oversized IDs, encodings, nulls, amendment flags and the operating-expense mismatch. |
| FH07 | Acquire smaller bulk and identity families | Fill missing candidate/committee summaries, linkage, Forms 1/2, leadership, presidential summaries and smaller disclosure families, newest cycle to oldest. Check complete snapshot/member inventories and preserve existing broader histories. |
| FH08 | Build the filing and amendment foundation | Filing metadata and retained originals use separate source IDs, submitted-version identity and evidenced amendment/attachment links. Unavailable originals and deferred PDF attachments stay visible. |
| FH09 | Qualify financial inclusion rules | Reuse and extend FR07's correction-base, ordering, repeat-application, complete/partial amendment and omission cases for each historical layout. Independently check overlap, memos, refunds, signs, notices and balances. Preserve exact membership decisions and unexplained differences. |
| FH10 | Acquire missing large financial populations | Fill appropriate cycle ZIPs and selected Schedule A/B/E exports only after FH03. Retain hashes and exact partition selection; document when a whole-history dump must be downloaded to select the requested period. |
| FH11 | Expand original filings and supplemental schedules | Reconcile electronic, transcribed-paper and unofficial Senate inventories. Adopt supported C/D/F/H4, special-account and inaugural details, with explicit historical format coverage and unresolved variants. |
| FH12 | Publish useful local financial query outputs | Build typed record-version tables and qualified current views. Prove deduplication, join cardinality and evidence reachability before presenting totals. Retain rejected/unsupported records in coverage and source evidence. |
| FH13 | Add summaries, public funding, statistics and elections | Preserve source-defined populations, periods, units and summary types. Reconcile only comparable values; explain unitemized amounts and other legitimate differences. Keep reported totals distinct from computed totals. |
| FH14 | Complete selected non-PDF legal collections | Reconcile pagination and inventories for advisory opinions, enforcement, fines, ADR, audits, litigation and rulemaking. Preserve scoped case IDs, party roles, dated events and native text where available. |
| FH15 | Complete selected agency and reference material | Adopt non-PDF meetings, guidance, XML/HTML/Word/spreadsheets, FOIA, budgets, performance, privacy, procurement, operations and oversight. Preserve metric definitions, units, fiscal periods and source locators. |
| FH16 | Deliver useful queries and document discovery | MCP descriptions, example joins and selected document search answer real questions while exposing coverage. Reuse existing document/search owners and keep candidate-to-legislator joins within verified legislative coverage. |
| FH17 | Qualify refresh and recovery | Exercise unchanged/changed inputs, late amendments, deletions, interruption, failed credentials, partial listings and retry. Preserve the last good generation and record runtime, memory and storage bounds. |
| FH18 | Run complete local acceptance | Reconcile selected inventories, record membership, mapped fields, keys, joins, inclusion policies, comparable totals and coverage statuses. Use independent evidence checks; exclude PDF processing and PDF test fixtures. |
| FH19 | Integrate and prepare the release | Align source packages, code, documentation and the intended fork/upstream base. Reuse FR12's dependency receipt and consumer image/configuration pins with reproducible generations, portable evidence and rollback retention; complete appropriate gates without resuming PDFs. |
| FH20 | Publish, verify and activate refresh | Publish data and deploy the matching consumer/configuration; repeat FR13's compatibility, dependency-advancement, refusal and rollback checks plus remote byte/download verification. Verify documentation if selected. Activate only qualified refresh jobs after end-to-end acceptance. |

All FH tasks are planned expansion; local precursor work is recorded in the FR
and FG registers. Availability in the matrix below does not mark an FH task
complete. A family that starts after 2008 should state its first available
period rather than invent an earlier empty history.

## Available sources versus retained data

This matrix summarizes the September 30 availability inventory linked above.
“Available” means the official discovery material identified a route or object;
it is not a guarantee that a future download, historical traversal or semantic
mapping will succeed. “Retained” describes selected native observations, not
necessarily current or complete financial analysis. Refresh the machine-readable
inventory before acquisition. API route presence is not a coverage percentage.

### Bulk identities summaries and disclosures

| Data family | Retained selection | Available route or historical target | Model destination and user value |
| --- | --- | --- | --- |
| Candidate master | All target cycles, with older history also retained | Cycle files through 2008–2026 | Candidate history; find who ran, where, for which party and with which principal committee |
| Committee master | All target cycles, with older history also retained | Cycle files through 2008–2026 | Committee history; interpret committee type, affiliation and treasurer at the recorded cycle |
| Candidate to committee linkage | 2024 and 2026 cycles | 2008–2026 cycle files | Reported relationships; follow authorization and representation |
| Candidate financial summary | 2024 and 2026 | 2008–2026 cycle files | Reported financial summaries; compare fundraising, spending, cash and debt by defined period |
| House and Senate financial summaries | 2024 and 2026 | 2008–2026 cycle files | Reported summaries; compare candidates without reconstructing every itemized row |
| All candidates summary CSV | 2024 and 2026 | 2008–2026 cycle files | Reported summaries; preserve this format's definitions and overlap |
| Committee financial summary | 2024 and 2026 | 2008–2026 cycle files | Reported summaries by committee and period |
| PAC summary | 2024 and 2026 | 2008–2026 cycle files | Reported summaries; compare political action committees (PACs) |
| Presidential summaries and contribution maps | 2024 selected files | Presidential cycles 2008, 2012, 2016, 2020 and 2024; overall, amount-size and ZIP groups | Summaries and contribution aggregates; compare where and in what size bands money came from |
| Candidate statements of candidacy, Form 2 | 2024 and 2026 | 2008–2026 cycle files | Filings, candidate history and reported relationships |
| Committee statements of organization, Form 1 | 2024 and 2026 | 2010–2026 in the inventoried bulk route | Filings, committee identity and stated organization relationships |
| Leadership PAC sponsors | 2024 and 2026 | 2010–2026 in the inventoried bulk route | Reported sponsorship relationships |
| Lobbyist and registrant committees | Combined 2007–2026 snapshot | Combined bulk snapshot | Source-reported committee roles, retaining native authority and dates |
| Individual contributions | 2026 main and date-partition representations, plus retained correction streams | 2008–2026 cycle bulk files; API/Schedule A exports may add fields and other receipt types | Receipts; inspect contributors and exact reported amounts, with proven overlap handling |
| Committee transactions | 2026 | 2008–2026 cycle files | Intercommittee transactions; trace reported transfers and contribution relationships |
| Contributions to candidates and independent expenditures, pas2 | 2024 | 2008–2026 cycle files | Source-typed committee transactions or independent expenditures; distinguish row types before routing |
| Independent expenditure 24/48 hour reports | 2024 | 2010–2026 in the inventoried bulk route, plus API/Schedule E routes | Independent expenditures and candidate targets; inspect outside spending and its reporting timing |
| Communication costs | All inventoried 2010–2026 cycle snapshots | 2010–2026 in that route | Communication costs and targets |
| Electioneering communications | All inventoried 2010–2026 cycle snapshots | 2010–2026 in that route | Communication reports, child records and candidate targets |
| Operating expenditures | 2026; positional field-definition mismatch remains | 2008–2026 cycle files | Disbursements after field mapping qualifies; compare payees and purposes |
| Bundled contributions | Combined 2009–2026 snapshot | Combined bulk snapshot | Bundled contributions by bundler, recipient and reported period |
| False or fictitious contribution notices | Combined 2016–2026 snapshot | Combined bulk snapshot | Quality notices linked by source evidence; a notice is not an automatic exclusion or a proven allegation |

### Filings detailed schedules and historical exports

| Data family | Retained selection | Available route or remaining work | Model destination and user value |
| --- | --- | --- | --- |
| Electronic original filings | Selected daily archives for June 8, June 9 and July 2, 2026, plus individual originals | Historical daily archives and identified originals; reconcile expected members before claiming coverage | Filings and financial record versions; retrieve what was submitted and interpret amendments |
| Transcribed paper filings | Selected July 14, 2026 archive and individual originals | Historical archive listings and originals | Same logical filing tables, with format and transcription authority preserved |
| Unofficial Senate originals | Selected recovered originals, including an explicit fallback disposition | Historical 2008–2018 archive references; discovery/pagination remains source-limited | Filings with separate archive identifier namespace and unofficial authority |
| PostgreSQL committee history export | Native dump rows retained; data ends in 2022 despite later object timestamps | Recheck actual row periods before treating a current object as current data | History evidence and qualified typed fields; richer historical committee attributes |
| Schedule A historical export | Not acquired as a full dump | Whole-history dump listed; about 90.2 GB compressed in the dated inventory | Broader receipt details; select target periods after capacity planning |
| Schedule B historical export | Not acquired as a full dump | Whole-history dump listed; about 39.3 GB compressed in the dated inventory | Detailed disbursements and purpose/payee analysis |
| Schedule E historical export | Not acquired as a full dump | Whole-history dump listed; about 43.6 MB in the dated inventory | Independent expenditures; compare fields and overlap with other representations |
| Loans and guarantors, Schedule C | Small API captures or selected original records | API and original filings; no complete target population established | Loan states and guarantor links; distinguish outstanding balances from new borrowing |
| Debts, Schedule D | Small API captures or selected originals | API and original filings; completeness pending | Debt states by reporting period |
| Coordinated party spending, Schedule F | Small API captures or selected originals | API and originals; completeness pending | Coordinated expenditures and candidate links |
| Allocated disbursements, Schedule H4 | Small API captures or selected originals | API and originals; completeness pending | Federal/nonfederal allocation of reported payments |
| National party special accounts | Selected API/reference observations | API and relevant filing schedules; qualify account fields | Account-tagged receipts, disbursements and summaries |
| Inaugural donations, Form 13 | Filing metadata, small aggregate/reference observations; linked PDFs deferred | Original/API routes; itemized body completeness unestablished | Separate itemized donations from contributor aggregates |

The dump sizes above are measured compressed object sizes from
`official-dumps-vs-local.json`, not estimates of extraction space. The historical
exports do not establish a server-side 2008 cutoff or a cheap partial download.
The current API can add fields or select records; the user value of a historical
export is broader historical detail that can be queried locally without walking
the entire API. Its name alone does not establish fresher data or a complete
amendment policy.

### Legal agency reference and election material

| Data family | Retained selection | Remaining acquisition or qualification | Model destination and user value |
| --- | --- | --- | --- |
| Advisory opinions and administrative fines | Selected metadata and native response captures | Full target-window inventories, pagination and related non-PDF text | Legal matters, parties, events and documents; follow the issue and disposition |
| Enforcement matters, ADR, audits and litigation | Selected metadata, sometimes outside the target window | Complete inventories and case/document identity; ADR means alternative dispute resolution | Matter history, audit findings and parties; inspect enforcement and compliance evidence |
| Rulemaking, statutes and regulations | References and selected API observations | Reconcile existing stack holdings before new acquisition; qualify FEC-specific links | Reuse law/regulation structures and link legal matters/documents |
| Guidance and meetings | Selected HTML/RSS references and captures | Full selected inventories, editions and linked non-PDF bodies | Document discovery, meetings and cited matters |
| Public funding | Discovery/reference coverage | Acquire and qualify selected official payment/award data | Public funding records; inspect recipients, programs and amounts |
| Election results and calendars | Discovery/reference coverage | Acquire contest results, definitions, dates and filing/election calendars | Election results and calendar events; connect finance to the proper contest and deadlines |
| Campaign statistics | Selected discovery/summary material | Inventory publications and source-defined measures across the window | Campaign statistics; compare defined populations and time periods |
| FOIA reports | Native XML for 2010–2025, a 2009 Word original, selected FOIA.gov archives including 2008/2009/2025 | Qualify semantic metrics and remaining formats/periods; FOIA is the Freedom of Information Act | Agency reports and metrics; compare request handling, backlogs and timeliness |
| Oversight and inspector general reports | Selected report HTML and recommendation material | Reconcile selected report inventories and semantic recommendation/status fields; PDF bodies deferred | Reports and oversight recommendations; follow findings and reported corrective action |
| Budgets, performance, privacy, procurement and agency operations | Mixed retained references, captures and deferred bodies | Inventory each family and qualify non-PDF tables/metrics; do not infer complete adoption | Agency reports, metrics and documents with period and unit definitions |
| Dictionaries and quality notices | Retained headers, descriptions, references and notices | Version definitions by source layout and period, resolve contradictions | Reference definitions and notices; make code meanings and data limits inspectable |
| Third-party FEC research and crosswalks | Explicitly FEC-located retained research | Preserve publisher authority and qualify joins independently | Evidence and candidate-to-legislator/reference links; distinguish inferred matches from FEC statements |

## Acceptance queries and evidence

The following user questions guide acceptance; their table names and join rules
are specified in the [data model](fec-data-model.md). Each test must show its
coverage, selected generation and evidence route as well as its result.

| User question | What the check must prove |
| --- | --- |
| Who represented or sponsored this candidate or committee in a given cycle? | Exact source identifiers and cycle, source-reported role, conflicting/name-only assertions preserved, evidence reachable |
| Who gave or was paid, when and how much? | Exact amounts/dates, correct reporting party, supported receipt/payment type and source versions; no implicit person identity from a name |
| What is the current amount after amendments and source corrections? | Proven correction base/order, exact post-correction membership, complete-versus-partial replacement scope and omission handling; duplicates excluded once, unknown applicability/scope visible |
| Who spent to support or oppose a candidate? | Correct spend type and role; candidate links cannot multiply the full spend amount |
| What was outstanding in loans or debts at period end? | A selected balance snapshot, not a sum of recurring reported balances |
| What happened in a legal matter and where is the supporting document? | Matter namespace, dated events, party roles, document edition and body availability |
| How did FEC operational performance change? | Comparable metric definitions, periods, units and source references; structural extraction is not mistaken for a semantic metric |
| Can another machine reproduce the release after local cleanup? | Portable inputs, remote byte verification, restore/replay receipt and the same declared outputs or documented deterministic differences |
| Does the hosted result use the versions that were qualified together? | The running image, release receipt, parent/table pins and SQL/policy/dictionary digests match; dependency advancement or code drift refuses the affected qualified view; rollback restores a compatible set |

Useful external reference entry points are the
[FEC bulk index](https://www.fec.gov/data/browse-data/?tab=bulk-data),
[OpenFEC schema](https://api.open.fec.gov/swagger/),
[legal resources](https://www.fec.gov/legal-resources/), and
[reports about the FEC](https://www.fec.gov/about/reports-about-fec/).
Use retained, dated discovery evidence to justify a particular release's
selection; a live landing page is not its immutable inventory.

## Next execution checkpoint

The [FR08 completion receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/fr08-complete.json) closes complete local key, evidence,
financial and actual MCP acceptance. The integrated parent correction preserves
all registered SQL and the accepted table values. Full baseline unit tests and
focused post-change checks pass at their recorded scopes. The documentation-site
build remains unrun because MkDocs is absent from this environment.

The [query-table publication](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01) is complete. Remaining work:

1. **FR12–FR13 — Bind and activate the matching consumer.** The exact source,
   query, candidate and catalog generations are public and verified. Build or
   verify the consumer image, bind its actual digest and the final release
   receipt, deploy both Worker pins, and check hosted queries, dependency/policy
   refusal and rollback.
2. **FR11 — Complete recovery before source deletion.** The requested small
   restore/replay smoke passed. Full selected restoration and replay of every
   special route remain open. Preserve the mixed `eFilingFormats.zip` parent
   with deferred PDFs and refresh capacity before the full recovery check.
3. **FR14–FR16 — Reclaim eligible sources and record the release handoff.** After
   recovery and deployed acceptance, build and recheck the exact deletion
   manifest, remove only eligible copies, and measure recovered space.

PDF processing and historical acquisition remain deferred. Hosted qualified-view
activation and source cleanup still require their release, restore and deletion checks.
