# FEC remaining work and discovered gaps

Reconciled September 21, 2026 against the FEC delivery at SpicyRegs `7b174f1`,
its pinned SpicyDocs 0.26.0 interface, retained research and the separate source
repair/runbook updates through `a4930a4`.
This is the canonical remaining-work register. Dated research reports preserve
what was known at the time; the newer evidence referenced here takes precedence
over their older open/closed status sentences.

The reusable path works for the selected inputs: source discovery, retained
records, reported relationships, verified table generations, CLI downloads and
local Model Context Protocol (MCP) queries. Remaining work concerns coverage,
interpretation, portable evidence, refresh and external delivery. The product
goal remains one place to discover and use relationships across many sources.
FEC financial analysis and legislative scorecards are possible consumers of that
foundation, each with its own inclusion and scoring rules.

## Verified baseline

| Measure | Current local result | Limit |
| --- | ---: | --- |
| Broad official source families catalogued | 26 | Discovery metadata, not acquisition completeness |
| Broad families with selected collection rows | 17 | Nine have no selected collection in this generation; an access category need not produce records |
| Collections | 649 | Each has its own selected input and period |
| Source records | 13,717,161 | Includes headers and file metadata; not a count of financial transactions |
| Relationship observations | 183,390 | Includes explicit missing/empty states; not all positive relationships |
| Official bulk-page groups represented | 26 | 25 have selected outputs; PostgreSQL has file inventory only; all decline complete-history claims |
| Senate originals | 598 live FEC originals | 597 native parses and one physical-line fallback; one archive mirror retained separately |
| External FEC publication | Not completed | Local output and code push do not publish the data |

The [coverage census](research/fec-coverage-2026-09-21.md) lists every broad
family and every bulk group. These are two different classifications whose
counts happen to match. The [generation audit](research/fec-generation-readiness-2026-09-21.md)
records the complete local publication/download/MCP check. A separate
[Cloudflare R2 object-store rehearsal](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/deploy/fork-setup.md) qualified synthetic objects,
conditional publication and multipart transfer; it did not publish the FEC data.

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

**Open · SpicyRegs.** `fec_collections` exposes provider scope and the selected
reader's outcome. Caller discovery, publisher authority and earlier refusals
remain in receipts or labels. For example, Senate `853` has an authority label;
`48` has a native-parser refusal followed by a successful physical-line reader.
Zero relationship rows also do not distinguish “unmapped,” “not selected” and
“mapped with no observations.” The bulk-group-to-collection mapping lives in an
external receipt; `source_family` is the broader classification.

**Complete when:** existing metadata structures expose digest-pinned caller
context, mapping/version status and the bulk selection map. MCP can distinguish
source authority, incomplete discovery, parser refusal, fallback and genuine
empty results without decoding labels. Provider outcomes remain separate from
caller decisions. Evidence: [observation builder](https://github.com/mikewolfd/spicy-regs/blob/7b174f1/src/spicy_regs/transforms/build_fec_observations.py),
[Senate audit](research/fec-senate-recovery-2026-09-21.md), receipts E1/E2 below.

### FG02 — Distribute recoverable input and audit evidence

**Open · Publication operator, SpicyRegs and SpicyDocs.** Table generations bind
output bytes, schemas and implementation identity. They do not yet bind the
complete acquisition campaign, original blobs and manual audit bundle. Many
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

**Open · SpicyDocs reader, then SpicyRegs adoption.** The original dump and README
are inventoried, but decoded rows are not in the delivered tables. The assessment
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

**Open adoption / expansion of selection · SpicyDocs and SpicyRegs.** Remaining
inputs include the **32,034,987-row individual-contribution main member**;
retained 2026 candidate/committee masters, candidate links, candidate/committee
summaries and leadership PACs; presidential map detail exports; other retained
communication-cost/electioneering periods; and daily/paper archive populations.
Individual insert/delete rows are already delivered. Main and `by_date` layouts
may overlap and cannot be summed without reconciliation.

**Complete when:** each chosen object/member has bounded native rows, exact
field/member audits, source coordinates and a delivered selection record. Track
retained-but-unselected inputs separately from missing acquisitions. The
[26-group census](research/fec-coverage-2026-09-21.md#official-bulk-groups) gives
the exact dispositions and links to FG03/FG05/FG06 where another gap applies.

### FG05 — Resolve the operating-expenditure field definitions

**Source-limited · SpicyDocs qualification, SpicyRegs mapping.** All 1,620,229
selected `oppexp26` rows have 26 positions, including a final blank. The official
CSV header supplies 25 names; the HTML dictionary also has inconsistent row
width and repeated/missing positions. Literal positional rows are delivered.

**Complete when:** authoritative evidence establishes the matching field layout
and a whole-population audit supports a named mapping. Until then, expose the
positional-only status. Do not invent a name, shift fields or drop a source
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

**Open adoption / expansion · SpicyDocs, then receiving tables/catalogs.** Native
Freedom of Information Act (FOIA) XML parsers for the National Information
Exchange Model (NIEM) 1.02/1.03 and Oversight report parsers exist. Selected XML-linked FOIA
history and Oversight records are retained, but the current FEC generation has
no agency-report or inspector-general collections. PDF-only years, other report
types, public FOIA releases, broader Oversight years and current recommendation
status remain outside the qualified selection. Word Flat OPC is a distinct
unsupported representation; paired XML creation dates and report/export dates
must remain separate.

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

**Open capacity qualification / conditional optimization · SpicyDocs and operators.**
Separate member releases repeat archive-wide verification. The completed selected
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

**Open deployment · Data/MCP operator.** Output sealing and the full local
publisher/CLI/stdio-MCP path pass. The fork's real R2 rehearsal also passes for
synthetic two-table data, stale conditional writes and a 6 MiB multipart object.
No full selected FEC upload or matching hosted-service run is claimed.

**Complete when:** publish the chosen `fec-observations` and `fec-source-catalog`
families through the existing generation path, verify remote bytes and one
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

**Open at the recorded check · Fork operator.** Core CI passed for the pushed
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

## Suggested execution order

1. Make current scope, mapping status and original evidence usable by remote
   consumers (FG01–FG02), then publish the selected audited families (FG21).
2. In parallel, adopt the assessed history and remaining selected retained bulk
   inputs (FG03–FG04), qualify the existing cross-source join (FG18), and finish
   the independently owned hosting/source repairs (FG22–FG23).
3. Select the next source populations, then resolve their native evidence,
   interfaces, scale and refresh requirements (FG05–FG13, FG16–FG17).
4. Add relationship interpretations, financial views, documents/search and
   adjacent sources as concrete consumer needs require (FG14–FG15, FG19–FG20,
   FG24). Preserve source observations throughout.

## Evidence index

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
