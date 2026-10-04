# FEC coverage limits and optional backlog

Updated October 1, 2026. The active goal is useful retained non-PDF FEC data
on R2 in validated tables. The [R2 delivery checklist](fec-delivery-plan.md)
owns that work. This register preserves source limits, historical findings and
optional expansion; an open FG item is not automatically a release blocker.

The selected source, typed query, candidate-history and catalog generations are
published. Complete remote byte verification, public reads and the reviewed
consumer are recorded in the
[release checkpoint](research/fec-reviewed-release-2026-10-01.md).
Those results supersede older pending-publication and hosting statements below.

Only a concrete defect in the selected data's accuracy, useful shape, evidence
or remote availability belongs on the active checklist. Historical acquisition,
broader financial interpretation, refresh automation, search, legislative joins
and deployment drills remain separately scoped. PDFs remain deferred.
Full restore and deletion checks become prerequisites when local cleanup resumes.

The [earlier roadmap](research/fec-delivery-roadmap-2026-10-01.md) preserves FR/FH
tasks, and the [data model](fec-data-model.md) preserves row meanings and
relationship rules. FG identifiers and dated evidence below remain stable.

## Historical published selection — September 21, 2026

| Measure | Measured baseline result | Limit |
| --- | ---: | --- |
| Broad official source families catalogued | 26 | Discovery metadata, not acquisition completeness |
| Broad families with selected collection rows | 17 | Nine have no selected collection in this generation; an access category need not produce records |
| Collections | 649 | Each has its own selected input and period |
| Source records | 13,717,161 | Includes headers and file metadata; not a count of financial transactions |
| Relationship observations | 183,390 | Includes explicit missing/empty states; not all positive relationships |
| Official bulk-page groups represented | 26 | 25 have selected outputs; PostgreSQL has file inventory only; all decline complete-history claims |
| Senate originals | 598 live FEC originals | 597 native parses and one physical-line fallback; one archive mirror retained separately |
| External FEC publication | Selected seed published on the fork | Public byte checks, CLI downloads and direct fork-R2 MCP reads pass; hosted MCP deployment remains separate |

The [coverage census](research/fec-coverage-2026-09-21.md) lists every broad
family and every bulk group. These are two different classifications whose
counts happen to match. The [generation audit](research/fec-generation-readiness-2026-09-21.md)
records the complete local publication/download/MCP check. A separate
[Cloudflare R2 object-store rehearsal](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/deploy/fork-setup.md) qualified synthetic objects,
conditional publication and multipart transfer; it did not publish the FEC data.
The later [fork execution](fork-generation.md#execution-update) published all
five selected FEC tables and verified their public bytes, pins and actual MCP
queries. It supersedes the rehearsal's unpublished status without changing the
selected coverage limits above.

## How to use this register

Each `FG` identifier is stable. **Open** means missing implementation, adoption,
qualification or operations for the described work. **Source-limited** means
the retained publisher evidence is incomplete or inconsistent. **Expansion**
means an additional population or consumer choice, rather than a defect in the
completed selection. A selected dataset can be delivered with declared gaps;
“complete history” requires a separately declared and reconciled population.

SpicyDocs owns source acquisition, native parsing and source releases. SpicyRegs
owns useful tables, relationship interpretation, discovery through MCP and
table publication. DocSpec owns document catalog and body-processing adoption;
RefSpec owns governed identifiers and reference definitions. Operators own
credentials, refresh schedules, storage and deployments. Owners below name
responsibilities, not new services or assigned individuals.

For every data change, retain exact input bytes/digests and output pins. Compare
complete selected membership and fields where practical, then manually read raw
inputs alongside output witnesses. Include empty, malformed, missing, conflicting
and unsupported cases. Record acquisition, parsing, mapping, publication and
indexing separately; a successful test in one stage does not close the others.

## Evidence and queryability

### FG01 — Make coverage and interpretation status queryable

**Local implementation and retained qualification · SpicyRegs.** `fec_collections`
now accepts digest-pinned caller context and explicit metadata dispositions,
separately from provider scope and outcomes. The combined selection binds caller
discovery, authority, earlier refusals, relationship-mapping selection and the
bulk map without changing prior provider rows. Metadata-only entries emit zero
records while retaining an unknown source-record count. `retained_unparsed`
remains open work. For example, Senate `853` has an authority label;
`48` has a native-parser refusal followed by a successful physical-line reader.
Caller relationship context distinguishes an unselected mapping from a selected
mapper; its provider outcome still owns emitted counts. Bulk groups and
`source_family` remain distinct census axes. See the dated retained qualification
for actual local queries and its exact selection.

**Complete when:** existing metadata structures expose digest-pinned caller
context, mapping/version status and the bulk selection map. MCP can distinguish
source authority, incomplete discovery, parser refusal, fallback and genuine
empty results without decoding labels. Provider outcomes remain separate from
caller decisions. Evidence: [observation builder](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/src/spicy_regs/transforms/build_fec_observations.py),
[Senate audit](research/fec-senate-recovery-2026-09-21.md), receipts E1/E2 below.

### FG02 — Distribute recoverable input and audit evidence

**October 1 update:** archive admission/restore and local MCP preparation pass
synthetic checks, and bounded transfer code is committed. The candidate includes
the missing HTML classification witnesses. Restored-source replay preparation
subsequently found missing filing-body reference and qualification metadata;
the [expanded archive preparation](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-replay-closure-preparation-20261001/handoff.json)
now includes those dependencies and passes owner metadata/capacity checks. The
exact PostgreSQL tool and isolated replay environment are ready. Qualified
non-PDF members from `eFilingFormats.zip` are retained separately with derivation
evidence; the mixed parent stays preserved and ineligible for deletion.
[Actual FR09 admission](research/fec-retained-delivery-execution-2026-09-30.md#complete-archive-admission-2026-10-01) now passes for the final selection. [FR10 upload and full public hashes](research/fec-retained-delivery-execution-2026-09-30.md#verified-r2-archive-upload-2026-10-01)
also pass. The requested sample restore/replay smoke passed; full selected restoration and every special replay route remain open under FR11 before source deletion.

**Open distribution · Publication operator, SpicyRegs and SpicyDocs.** Table
generations bind output bytes, schemas and implementation identity. The retained
observation generation now also binds its exact selected manifest, including
source-capture and caller-context pins. The complete acquisition campaign,
original blobs and manual audit bundle are not distributed with it. Many
evidence locators are local paths; a current publisher URL cannot guarantee the
same bytes later. Direct-retained inputs correctly have no invented source-release
artifact digest.

**Complete when:** a portable pinned manifest associates each output with its
selection, originals, dictionaries, failures and audit receipts. A remote
consumer can retrieve representative originals and body ranges and verify their
digests. Preserve mirror provenance and missing evidence explicitly. Choose
durable storage and recovery checks; Actions' 30-day capture retention is not
the long-term evidence store. Evidence: [generation format](generation-publication.md),
[relationship provenance](fec-relationships.md), E1/E2 and the
[rollup workflow](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/.github/workflows/_rollup.yml).

## Source coverage and native interfaces

### FG03 — Adopt the assessed PostgreSQL committee history

**October 1 update:** typed committee-master and PostgreSQL history observations
also pass complete selected native-value/readback and local combined-table
checks. See the [reference/history closure](research/fec-retained-delivery-execution-2026-09-30.md#retained-reference-and-history-closure).
This extends local adoption; current-registry identity and remote delivery
remain separate.

**Retained native reader and local receiving path qualified · SpicyDocs and SpicyRegs.**
The September 30 selection includes the original dump's native committee-history
rows and literal README lines through the existing observation tables. The
source receipt verifies every native cell and regenerated COPY/schema pin.
Remote publication remains separate. The assessment
found **262,276 rows, 73 fields and 76,277 committee IDs**, covering cycles
1976–2022. The README describes 66 fields. A September 2026 object timestamp
does not make those records current.

**Complete when:** a small bounded reader uses maintained `pg_restore`/COPY
support and preserves original dump/table identity, tool identity, derived-stream
digest and exact row coordinates. Qualify null `\N`, empty text, empty arrays and
escaped values independently. Feed the existing source-record tables and audit
every selected field. A database restore is unnecessary for this table. Keep
arrays as native text until a typed decoder is qualified. Do not reuse the
`committee_api_current` relationship label for history: earlier cycle rows can
contain later candidate/sponsor/cycle information. Evidence: E3 and the
[assessment summary](research/fec-generation-readiness-2026-09-21.md).

### FG04 — Finish adoption of already retained bulk populations

**October 1 update:** the [V7 terminal recovery receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-readback-recovery-v7/verification.json)
passes complete native-field and stored-cell comparison of the main bulk output
and binds the other completed collections for exact reuse. This resolves the
earlier readback interruption at FR05's output boundary. Final composition and
complete keys, evidence, financial and actual local MCP acceptance now pass under
FR08. [Table publication and public SQL/CLI checks now pass](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01); matching hosted qualified-view activation now passes; the live mutation/rollback drill remains open under FR13. The [recovery checkpoint](research/fec-retained-delivery-execution-2026-09-30.md#bulk-readback-and-release-preparation-2026-10-01)
preserves earlier findings and subsequent evidence.

**Retained selection qualified locally; unacquired history remains expansion ·
SpicyDocs and SpicyRegs.** The September 30 manifest combines the previously
selected bulk populations with all retained native master snapshots, individual
main/date members, and selected daily/paper filing archive members. The source
row-hash oracle proves the retained individual main stream and date partitions
have equal row multiplicities. Both physical observations remain present;
summing them would double-count their overlap. Exact objects, members, dates,
source counts and source-limited dispositions are in the dated receipt.

**Bounded local completion, September 30, 2026:** `fec_candidate_history` now
builds every bulk candidate-master cycle from 1980 through 2026. The exact
retained-file selection, mixed acquisition dates, complete field comparison and
local MCP check are recorded in the [qualification receipt](research/fec-publish-qualification-2026-09-30.md#candidate-master).
The exact sealed candidate generation is now [published and verified](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01) through its existing family owner. It remains separate from the new typed-table family. The later [retained-corpus receipt](research/fec-retained-corpus-qualification-2026-09-30.md)
records the combined native adoption; neither selection claims unrelated
unacquired official history.

**Complete when:** each chosen object/member has bounded native rows, exact
field/member audits, source coordinates and a delivered selection record. Track
retained-but-unselected inputs separately from missing acquisitions. The
[26-group census](research/fec-coverage-2026-09-21.md#official-bulk-groups) gives
the exact dispositions and links to FG03/FG05/FG06 where another gap applies.

### FG05 — Resolve the operating-expenditure field definitions

**Named-field mapping qualified; extra position source-limited · SpicyDocs and SpicyRegs.**
The retained whole-population width census supports mapping the named native
positions while preserving the extra blank position without an invented name.
Complete typed bulk readback passes under FR05, and combined local acceptance
now passes under FR08. Publication remains separate. See the
[execution evidence](research/fec-retained-delivery-execution-2026-09-30.md).
All 1,620,229
selected `oppexp26` rows have 26 positions, including a final blank. The official
CSV header supplies 25 names; the HTML dictionary also has inconsistent row
width and repeated/missing positions. Literal positional rows remain retained.

**Complete when:** authoritative evidence establishes the matching field layout
and a whole-population audit supports a named mapping. Expose qualified named
fields separately from the unresolved extra position. Do not invent a name,
shift fields or drop a source
position. Evidence: E4 `oppexp-second-audit.json` and
[bulk audit](research/fec-bulk-continuation-2026-09-21.md).

### FG06 — Establish Senate archive coverage and preserve exceptions

**Source-limited · SpicyDocs discovery and qualification; SpicyRegs context.**
The selected recovery is complete for 598 live originals. The archive denominator
is unknown: `/senate/` returned 404, the explicit index worked, search returned
403, and `/posted/` returned a truncated 1,000-object listing while ignoring
continuation. Its listed SQL/image objects are inventory-only. Archived-page
failures remain recorded; `5.fec` is a separate Internet Archive mirror.

Native parsing still refuses malformed `48.fec`; its 19 physical lines are
preserved without quote repair. Fifteen legacy/differently spelled headers remain
unmapped in the date/reference audit. All 64 mapped `SEN-` references resolve,
but four `FEC-` occurrences have a different namespace. A closed selected
reference set does not establish a complete archive.

**Complete when:** a declared archive population and its denominator reconcile
against a complete publisher inventory or another independently qualified
population definition. Further bounded recovery can expand the selected set
without closing that archive-coverage gap. Exact form/version evidence must
support new mappings. Keep unresolved/refused cases queryable under FG01. An upstream issue draft is
retained but unsent; no publisher repair or root cause is claimed. Evidence:
[Senate recovery](research/fec-senate-recovery-2026-09-21.md) and E1.

### FG07 — Qualify further original filings, layouts and bodies

**Open qualification / expansion · SpicyDocs; DocSpec for processed bodies.**
Selected electronic/paper originals, legacy TEXT bodies and explicit encodings
are qualified. Literal `5.0` still lacks a genuine selected original; `5.00`
does not prove it. Further dictionary editions, encodings, amendments, attachments,
image-only PDFs and equal-text/distinct-filing associations remain selective.
For the retained Form 13 query, three PDFs exceeded the selected 32 MiB limit
and eight records had no raw URL. Negative file-number admission is fixed.

**Complete when:** each chosen filing/attachment has an acquisition disposition,
exact layout/encoding evidence, whole-field/body-range comparisons and independent
filing associations. Missing raw links stay unresolved unless an authoritative
alternative is found. PDF page-count checks do not establish OCR or visual
fidelity. Evidence: E5, SpicyDocs' [active FEC worklist][provider-worklist] and
[September 14 research][provider-research].

### FG08 — Reconcile full financial populations and acquisition capacity

**Expansion / unqualified capacity · SpicyDocs.** Selected ZIP/CSV groups do not
exhaust Schedule A/B/E, C/D/F/H4, C1/C2/H2/H5/H6/SL/SI, party special accounts
or inaugural detail. PostgreSQL financial dumps remain unacquired in this
delivery. The September 14 listing reported about 90.2 GB for A, 39.3 GB for B
and 43.4 MB for E; those are dated object observations, not current capacity
measurements.

**Complete when:** the selected schedule/period matrix reconciles bulk, raw filing
and API populations, unique fields and unavailable portions. Refresh source sizes
and measure transfer, decoded scratch and output capacity before acquisition.
Budget restore, write-ahead-log and index storage only if choosing an actual database restore.
Use existing transfers/readers rather than a new downloader or financial service.
Evidence: E5 financial preflight and research T06/population-gap matrices.

### FG09 — Qualify legal/audit release success and pagination with real inputs

**Open qualification · SpicyDocs.** The retained legal/audit interface has real
one-page advisory-opinion and administrative-fine success evidence. Successful
Alternative Dispute Resolution (ADR) and Matters Under Review (MUR) queries,
and complete audit pagination, remain synthetic; partial retained
audit pages prove refusal, not complete delivery.

**Complete when:** explicit complete native selections pass independent membership,
identity, field and pagination checks, including empty and refused outcomes.
Do not count working raw acquisition as qualified release admission. Evidence:
E6 and the provider's [FEC guide][provider-fec].

### FG10 — Complete selected enforcement acquisition and identity review

**Open / source-limited · SpicyDocs.** The retained 33-row difference set has
15 oversized originals and ten acquired rows without manual identity adjudication.
Case 2152 appears within 2189; 1829/2229 are related-only; 4307/3407 has a
filename/body conflict; 1999/1847 have equal bytes with conflicting metadata.
The truncated current 2035 original remains failed evidence even though a
complete historical alternative was found.

**Complete when:** every selected original has an outcome and the remaining
identity questions have source-specific adjudication or explicit unresolved
status. Equal bytes and related-case mentions do not merge legal identities.
Evidence: E5 enforcement `manual-review.json`, `failed-evidence.json` and
[provider research][provider-research]. Broader AO, enforcement, litigation and
supporting-document history remains a separate collection selection under FG13.

### FG11 — Adopt agency-report evidence and extend declared collections

**October 1 update:** selected typed agency report, metric, document and narrative
tables pass complete local replay, assembly and evidence checks. This advances
FR06; the later reviewed release also completes combined acceptance and public
delivery for the selected retained tables. The
[execution checkpoint](research/fec-retained-delivery-execution-2026-09-30.md)
records the selected scope and preserves deferred PDF and source limits.

**Open adoption / expansion · SpicyDocs, then receiving tables/catalogs.** Native
Freedom of Information Act (FOIA) XML parsers for the National Information
Exchange Model (NIEM) 1.02/1.03 and Oversight report parsers exist. Selected XML-linked FOIA
history and Oversight records are retained. The September 30 local qualification
admits all 119 successful selected agency releases through `profile: agency`
into the existing observation tables, preserving native fields, bodies, assets
and source positions. That earlier profile refused the selected 2009 Word Flat OPC original. The later
retained-corpus selection uses a separate Word reader, preserving package parts,
XML elements and literal text/control tokens, and adds retained FOIA.gov ZIP
members. See the [exact selection and receiver receipt](research/fec-publish-qualification-2026-09-30.md#agency-reports).
Remote publication remains unperformed; the agency-only qualification generation
must be combined with the intended existing collection selection before publication.
PDF-only years, other report types, public FOIA releases, broader Oversight years and current recommendation
status remain outside the qualified selection. Word text/control observations do not assert rendered-page or OCR fidelity;
paired XML creation dates and report/export dates remain separate. PDF processing
is explicitly deferred by the user. The exact combined selection and its
remaining source limitations supersede the earlier agency-only selection.

**Complete when:** selected releases are admitted by receivers, enumeration and
original outcomes reconcile, and native fields/associations survive. Define OCR,
visual fidelity, XSD validation or normalized statistics only when a consumer
needs them. Evidence: E5 agency qualification, E7 and [provider research][provider-research].

### FG12 — Add source-release identities for further API shapes

**Open interface work, as selected · SpicyDocs.** Existing raw API support is
broader than admitted releases. Remaining profiles include entity detail/history,
search/totals and relationship queries; legal detail and rulemaking `rm_id`;
ordinary/keyset/e-file financial and report observations; statutes, audit-category
references and publication indexes.

**Complete when:** each needed profile declares its native row unit, identity,
selection and completion rule and has retained success/empty/refusal replay.
Keep metadata and original bodies separate. Add receiving adapters to existing
tables only after those profiles are qualified. Evidence: provider worklist
FEC10 and research T09; [supported consumer inputs](fec-relationships.md).

### FG13 — Maintain the source catalog and declare missing populations

**Open maintenance / expansion · SpicyDocs source inventory, SpicyRegs discovery.**
All 26 researched families are catalogued, but inventory build time is not a live
route-health observation. Nine families have no collection in this generation;
some are metadata-only access categories. Loans/debts, party allocation, public
funding, results/calendars, statutes/rulemaking, guidance/meetings and agency/OIG
collections need explicit population choices and acquisition/adoption dispositions.
Do not infer that raw readers or retained research are absent.

**Complete when:** every promoted collection states source routes, historical
periods, native formats/IDs, access requirements, use terms, update behavior and
its acquire/equivalent/reference/unresolved disposition. Keep observed dates and
API credential requirements visible. The [family census](research/fec-coverage-2026-09-21.md#broad-source-families)
is the current output baseline; research T01/T08 and provider FEC07 own wider
collection discovery.

## Interpretation and reuse across sources

### FG14 — Extend only evidenced relationship mappings

**Expansion · SpicyRegs with SpicyDocs evidence.** Current mappings cover selected
bulk identity families, current committee API assertions and Form 1/2 variants
for versions 8.3/8.4. Other statement versions, history, legal associations and
financial transfers/support remain source rows without automatic relationship
interpretation.

**Complete when:** a selected mapping preserves source roles, namespace, qualifiers,
dates, absence states and parent coordinates and passes independent raw-to-output
checks. Validate fan-out and conflicting observations. Source-ID syntax does
not establish a valid entity; aggregate IDs, unverified filers and name-only
targets stay visible. Evidence: [relationship meanings](fec-relationships.md)
and [mapper](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/src/spicy_regs/transforms/fec_relationships.py).

### FG15 — Declare financial inclusion rules before deriving totals or paths

**October 1 update:** the retained reported-measure policy and SQL implementation
pass full-population decision/cardinality checks and independent Python
comparisons covering every observed interpretation and source class. Actual
local MCP acceptance also passes under [FR08](research/fec-retained-delivery-execution-2026-09-30.md#complete-local-validation-2026-10-01). Current/net economic
totals, transaction deduplication, correction/amendment applicability, cross-row
spending aggregation and transfer pairing remain unqualified. Quality notices
remain observations rather than automatic exclusion rules.

**Consumer policy / expansion · SpicyRegs financial-view owner.** Literal rows
do not settle amendments, insert/delete corrections, memo attribution, refunds,
negative values, duplicate representations, partial amendments, notices repeated
in periodic reports, or unitemized totals. A candidate-related payment or an
independent expenditure is not automatically a contribution to a candidate.

**Complete when:** each selected view names its population, time basis, amendment
and inclusion policy and reconciles source-compatible totals using retained hard
cases. Preserve details and excluded-row reasons. The false/fictitious-filing
list remains a publisher notice, not an independent allegation or automatic
filter. Raw acquisition and metadata delivery can proceed without adopting one
universal financial policy. Evidence: research T07 and [integration limits](fec-integration.md).

### FG16 — Operate repeatable refresh and durable recovery

**October 1 update:** the source-parent refusal gap is fixed and
[verified through actual local MCP](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-parent-bound-mcp-acceptance-v2-20261001/output/local-mcp-role.json).
Missing and advanced `fec-observations` parents disable qualified views before
new connections and refresh. Captured connections retain their original pins,
raw tables remain usable, and exact local rollback passes. Typed-dependency,
policy and image mismatch checks also pass. FR08 is complete locally, and the archive and table generations are now published. Actual consumer-image binding and hosted queries now pass. Full restore, deployed rollback and recurring source refresh remain open under FR11–FR13 and later operations.

**Open operations / wider qualification · Acquisition operator and SpicyDocs.**
The retained observation builder consumes a caller manifest; it does not download
or merge previous tables. The committee reference table has a daily workflow,
but the new selected campaign has no recurring acquisition/refresh workflow.
Communication-cost, electioneering and bundling refresh proofs do not establish
replacement/deletion behavior for every family. A fresh complete committee API
traversal still needs a retained complete-run receipt.

The September 21 scheduled
[committee run 35645971109](https://github.com/mikewolfd/spicy-regs/actions/runs/35645971109)
at `43c06b6` failed with `FEC committees require an API key; no complete acquisition
was attempted`. The workflow's evidence/debug-artifact steps succeeded. This
records a credential refusal before acquisition, not a failed or empty census.
The configuration gap was subsequently resolved: `DATA_GOV_API_KEY` was installed
at 19:44:43 UTC and passed a one-record OpenFEC request. No complete traversal
has been verified since installation. `ZYTE_TOKEN` was installed at 19:49:09 UTC
but is not wired to a workflow or caller adapter. The
[fork generation inventory](fork-generation.md) records these distinct states and
the [local reuse inventory](research/local-data-reuse-2026-09-21.md) identifies
an already qualified committee seed and sealed FEC observation families.

**Complete when:** each selected source has a cadence, bounded checkpoints,
credential setup where needed, durable original/evidence storage, failure
reporting and a qualified publication gate. Compare at least two observations
for changed, reused, absent and failed objects; do not infer deletion from a
partial listing. Recover interrupted work without duplicate rows or erasing prior
captures. Evidence: provider FEC09, [committee workflow](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/.github/workflows/rollup-fec-committees.yml),
[retained pipeline](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/src/spicy_regs/pipelines/rollups/fec_observations.py),
FG02 and E8 `fec-committees-workflow-failure.log`. The reusable workflow passes
the repository's `DATA_GOV_API_KEY` secret to the reader. Qualify a complete
traversal and its publication before claiming the fork's refresh is operational.

### FG17 — Measure and improve bulk scale where needed

**October 1 update:** the retained financial writer completed its output phase
within its memory bound, then stopped at the free-space floor during final
readback. Storage cleanup allowed local checks to run. The refreshed capacity
budget permitted complete V7 readback, which now passes within its memory and
free-space limits. Final composition has an explicit resource budget. The
archive preparation also budgets its staged copy separately from clean restore;
refresh free space before real admission. Broader historical-scale claims remain
unqualified.

**Open capacity qualification / conditional optimization · SpicyDocs and operators.**
The retained builder now uses session-scoped verified archive inventories.
Each opened original is still digest/size checked; ZIP member integrity and
identity are preserved. The completed selected
audits do not establish throughput or peak memory for full historical schedules,
all daily archives or large cross-source joins.

**Complete when:** representative selected runs record requests, repeated reads,
decoded/output bytes, time, peak memory and spill limits. Introduce multi-member
batching when measurements justify it, preserving digest and membership checks.
Use existing archive/Arrow/DuckDB paths, partitioned detail and manifests; avoid
per-transaction requests, Python objects or repeated whole-file hashing. Evidence:
provider FEC10, research T19 and E4/E5 capacity receipts.

### FG18 — Qualify an existing cross-source join end to end

**Open integration qualification · SpicyRegs.** The identity bridge already
exists: `members.fec_ids_json` maps candidate IDs to `bioguide_id`, which also
appears in `member_votes`. The four-table FEC batch does not include those
legislative tables or establish a combined query's coverage.

**Complete when:** a selected compatible bundle and reproducible SQL join FEC,
members, member terms and votes while preserving each generation pin. Audit
matched, unmatched and multiple-ID cases, join fan-out and date/chamber scope.
Identify the community crosswalk's authority separately from official FEC facts.
Keep name-derived `org_committee_links` distinct from source-reported relationship
observations.
Scorecards require a chosen scoring policy and source scope; they are optional
consumer work, not a missing catalog framework. Evidence:
[members](tables/members.md), [member terms](tables/member_terms.md),
[member votes](tables/member_votes.md).

### FG19 — Track legislative coverage limits separately

**Adjacent-source expansion · Legislative provider and rollup owner.** Delivered
roll-call/member-vote coverage is bounded and House-only. Senate-shaped schema
fields do not prove Senate acquisition; bill-reached votes are not a complete
session inventory. House refresh/backfill and native date interpretation also
need explicit qualification for a historical scoring use case.

**Complete when:** a selected Senate session index/traversal and House period set
reconcile listed, acquired, parsed and omitted votes, including member identity
and dates. Preserve these limits in cross-source examples. Evidence:
[roll-call coverage](tables/roll_call_votes.md) and [member positions](tables/member_votes.md).

### FG20 — Extend document and search adoption only to selected uses

**Open downstream adoption / optional indexing · DocSpec and SpicySearch/SpicyEngine.**
The selected committee census has a qualified DocSpec metadata catalog. That
does not establish broad FEC catalog admission, original-body processing or search
activation. Other retained families, filing/body associations and searchable
fields still need receiving-side qualification.

**Complete when:** each chosen catalog/body/search selection resolves to original
evidence, preserves equal-text/distinct-document identity, reports unindexed or
unsupported material, and distinguishes full companion metadata from indexed
fields. MCP table delivery does not require embedding every transaction or
building an FEC search service. Evidence: research T09/T19 and the
[census handoff][census-handoff].

## Publication and shared issues discovered during this work

### FG21 — Publish the selected FEC data and qualify the hosted reader

**October 1 update:** complete retained local validation now passes under
[FR08](research/fec-retained-delivery-execution-2026-09-30.md#complete-local-validation-2026-10-01), including evidence, financial decisions, actual MCP queries
and the applied source-parent correction. The expanded query tables, source parent, candidate history and catalog are now [sealed, published and publicly verified](research/fec-retained-delivery-execution-2026-09-30.md#query-table-publication-2026-10-01). The actual consumer image and release receipt are now bound and deployed; public qualified-view checks pass. See the [reviewed release checkpoint](research/fec-reviewed-release-2026-10-01.md). A live mutation/rollback drill remains separate; prior compatible image and receipt identities are retained.

**Selected data and hosted queries verified; operational drill open · Data/MCP operator.**
The September 21 fork execution published three complete selected families: `fec-observations`,
`fec-source-catalog` and `fec-committees`. Full public downloads passed digest,
size, schema and row-count checks. Actual stdio MCP passed both those downloads
and direct fork R2 queries, preserving generation pins. All selected relationship
parents resolve; nine raw-audited witnesses survive unchanged. This establishes
public selected-data delivery at that historical checkpoint. The current hosted
MCP service and canonical domain are verified in the newer release checkpoint;
documentation hosting remains a separate boundary. Evidence:
[fork execution receipts](/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21).
The larger September 30 retained selection and its research correction now have their own [October 1 publication receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/query-publication-20261001/completion.json). The September 21 evidence remains historical.

**Complete when:** publish the chosen families, including `fec-query` for the
retained typed release and its exact parent dependencies, through the existing
generation path. Verify remote bytes and one
captured publication index, and check externally observed table pins, schemas,
counts and raw-audited queries in the deployed reader. Make evidence accessibility
and scope limitations explicit. An absent family is unavailable, not an empty
dataset. Retain failed publication evidence and do not replace a good family
with a failed source run. Choose the fork's canonical data/MCP/docs addresses
and align landing-page/install examples before advertising them; existing upstream
defaults do not identify a new fork service. Catalog population, custom-domain
configuration and hosting/load qualification remain separate selected-path work.
Iceberg, paid hosting and Terraform adoption are conditional, not requirements
for direct Parquet use. Evidence: E2/E8, E8's `remaining-operational-gaps.md`
and [publication procedure](generation-publication.md).

### FG22 — Close the shared lobbying source-error incident

**Source repair pushed; backfill/publication/resume open · Lobbying source
owner and fork operator.** A Lobbying Disclosure Act (LDA) API HTTP 400 was converted into zero rows and
an empty family was published during fork setup. The family was conditionally
withdrawn, immutable evidence retained and only its workflow paused. This shows
why valid artifact bytes do not prove successful acquisition.

The separate repair at `7551f63`, pushed to the fork through `a4930a4`, requires the pagination filter and refuses
failed, malformed and incomplete/count-changing pages. Independent source/output
checks and the full 2,107-test gate pass. It has not established a complete seed
or a workable catch-up run within the schedule's time budget.

**Complete when:** run the reviewed code over a declared bounded initial population
with retry/watermark checks, inspect and publish
the validated output, and verify the public generation before resuming. Assess
anomalous native posting dates when selecting catch-up windows. Recheck the
separate repair task before marking operational recovery complete. This is shared operational
evidence, not an FEC parser defect. Evidence: E8 `invalid-family-withdrawal.json`,
`lda-reader-repair/` and `lda-final-host-gate.log`.

### FG23 — Resolve documentation hosting separately from data delivery

**Complete at the October 1 release checkpoint.** The strict documentation
site build, fork Pages deployment and public readback passed. The older failed
run below remains historical evidence.

**Historical check · Fork operator.** Core CI passed for the pushed
generation changes. The documentation deployment returned Pages HTTP 404 in
the retained run. A local docs build cannot establish hosted Pages availability.

**Complete when:** configure the intended Pages target and verify a successful
deployment and served content. Keep this distinct from FEC publication and MCP
deployment. Evidence: E2 `docs-deployment-status.json`,
[failed Pages run](https://github.com/mikewolfd/spicy-regs/actions/runs/35643179291)
and [successful core CI](https://github.com/mikewolfd/spicy-regs/actions/runs/35643179370).

### FG24 — Preserve the broader research frontier without making it a prerequisite

**Adjacent-source backlog · Source owners, selected independently.** Research
T03 and T10–T17 cover expiring advertising, IRS, lobbying/FARA/unions, ethics,
corporate/legislative/geographic context, state/local authorities and third-party
enrichment. Their detailed source/version/access dispositions remain in the
research repository. Existing SEC/SAM/USAspending/legislative and lobbying work
must be inspected before adding another connector.

The latest frontier retains academic/repository screening; county/city and
publisher-local catalogs; Party Time/Sunlight/ad-airing and web-archive recovery;
and representation/license/schema conflicts. Court PDFs and underlying FEC
survey responses cited by the research remain unacquired. Public bytes do not
by themselves establish redistribution permission; paid or access-restricted
alternatives remain conditional.

The research correction rechecks those directories by source identity. Official
FEC captures found there are part of the local FEC selection; explicitly
FEC-related third-party captures retain reference context with their authority.
The remaining adjacent-source inventory stays separate. See the correction's
`inventory.json` and `verification.json` for the complete classified denominator.

**Complete for a selected source when:** its declared population and every
enumerated object have source, access, acquisition and interpretation dispositions,
with version/authority/terms preserved. Unselected frontier candidates stay
visible without blocking official FEC delivery. See the
[task reconciliation](research/fec-coverage-2026-09-21.md#research-task-reconciliation)
and [retained frontier][research-frontier].

## Completed items and deliberate limits

Do not reopen these as missing implementation:

- All 26 researched official families have discovery metadata. All 26 official
  bulk groups have selected output or inventory dispositions.
- The default committee acquisition uses SpicyDocs, retains raw evidence and
  refuses incomplete traversal before replacing its output. The existing
  committee reference table and selected census catalog remain separate outputs.
- Six `weball`/`webl`/`webk` members have verified names. Selected intercommittee,
  committee-to-candidate, operating, correction and Form 1/2 rows are delivered.
- The 598-live-original Senate recovery, literal field/body audits, explicit
  malformed-file fallback and separate mirror retention are complete at that scope.
- Negative Form 13 numbers, selected candidate/legal/audit release profiles,
  native FOIA/Oversight readers and explicit filing dictionaries are implemented.
- Relationships join exact source parents; complete native metadata remains
  available. MCP exposes loaded availability, schemas, meaning and declared limits.
- Generation publication verifies complete family membership and remote bytes
  before a conditional index switch. Managed CLI downloads work directly with
  MCP, retain pins, refuse missing pins and avoid unselected files. Base MCP
  does not require a SpicyDocs runtime installation.
- The full selected local roundtrip and independent raw/output witnesses pass.
  Real synthetic R2 conditional/multipart qualification also passes.

MCP's 500-row response bound is deliberate; complete results require an explicit
query strategy or downloaded tables. A truncation indicator is a possible small
usability improvement, not a full-data acquisition gap. Local stat guards detect
ordinary file mutation but do not make writable storage immutable. The CLI's
other local commands do not promise a fresh full-file hash on every operation.
Base regulations.gov data, partitioned comments, Iceberg and docket-search gzip
publication have their own delivery paths; migrating all of them is not an FEC
completion criterion.

## Selecting further work

Use the [R2 delivery checklist](fec-delivery-plan.md#next-execution-checkpoint)
for the current selection. The earlier roadmap preserves historical acquisition
and operational tasks. Select those separately when they deliver an identified
user need; source limitations remain visible in the delivered tables meanwhile.

## Evidence index

The current retained-corpus selection, source wheel, receiving manifests,
source oracles, coverage ledger, resource receipts and local generation checks
live under [the September 30 evidence root](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930).
The dated qualification page distinguishes completed local boundaries from
user-deferred PDF work and unperformed publication.

These local receipts are retained evidence, not portable public downloads.
Their portability is part of FG02. The links identify exact audit roots; dated
observations must be refreshed before claiming current remote availability.

| ID | Retained evidence |
| --- | --- |
| E1 | [Senate recovery and final tables](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21): `delivery.json`, `bulk-coverage.json`, `combined-inputs.json`, `selected-publication/collection-context.json`, `raw-header-audit/round-2/summary.json`, `archive-discovery/` |
| E2 | [Generation readiness](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-generation-readiness-2026-09-21): `completion.json`, `seal.json`, `roundtrip-integrated/summary.json`, `mcp-download-audit-integrated/summary.json`, `manual-review.json`, `local-reader-review/scope-witnesses.json` |
| E3 | [PostgreSQL assessment](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-postgres-assessment-2026-09-21/ASSESSMENT.md) and adjacent `assessment.json` |
| E4 | [Bulk expansion](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-expansion-2026-09-21), including `oppexp-second-audit.json` |
| E5 | [September 14 research follow-up](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-research-integration-2026-09-14), with `inaugural/`, `enforcement/`, `agency/`; [financial preflight](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-next-collections-2026-09-14/financial/preflight.json) |
| E6 | [Legal/audit release qualification](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-legal-audit-release-2026-09-15/qualification.json) |
| E7 | [Agency native-interface qualification](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-agency-interfaces-2026-09-14/README.md) |
| E8 | [Fork setup evidence](/Users/mikewolfd/Work/corpora/fork-cloudflare-2026-09-21): `s3-generation-rehearsal.json`, `invalid-family-withdrawal.json` |

[provider-worklist]: /Users/mikewolfd/Work/spicy-stack/spicy-docs/docs/simplification-todo.md
[provider-research]: /Users/mikewolfd/Work/spicy-stack/spicy-docs/docs/research/fec-next-collections-2026-09-14.md
[provider-fec]: /Users/mikewolfd/Work/spicy-stack/spicy-docs/docs/sources/fec.md
[research-frontier]: /Users/mikewolfd/Documents/Codex/fec-data-research-2026-09-11/inventory/expansion/frontier.json
[census-handoff]: /Users/mikewolfd/Documents/Codex/fec-data-research-2026-09-11/integration/census-delivery-2026-09-12.md
