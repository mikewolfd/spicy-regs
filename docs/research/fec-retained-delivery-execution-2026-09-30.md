# Retained FEC delivery execution

Execution began September 30, 2026, against the reviewed
[delivery plan](../fec-delivery-plan.md) and [data model](../fec-data-model.md).
The execution goal selects useful typed tables before publication, keeps all
retained non-PDF data in scope, and defers new historical acquisition and PDF
processing. This checkpoint does not claim a completed release.

The October 1 direction is greenfield implementation: build the new tables and
queries directly, retaining older source data when it delivers user value.
Do not spend effort on old-code compatibility or reproducing research machinery.

## Verified starting point

The sealed observation generation passed fresh verification. The existing R2
credentials reached the expected account and `spicy-regs` bucket, and both
public publication indices matched their stored bytes. The public FEC tables
still represent the older selection; no new publication or deployment occurred.
See the pinned local files and retained index bytes in
[FR01 evidence](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr01/).

The input audit verified the selected direct originals, source-release members
and collection contexts without a pin mismatch. Historical audit citations
also name retired environments, old code versions, directory roots and copied
paths. Their dispositions preserve these differences instead of claiming that
present files reproduce every historical execution. PostgreSQL derivation
outputs were verified at their actual retained locations, along with the
matching `pg_restore` version. See the
[dependency checkpoint](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr02-closure/status.json)
and [derivation pins](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/postgres-dependencies.json).

The reference inventory is not an archive or a deletion manifest. Portable
path resolution, credential scanning, remote byte verification and a clean
restore remain required. Preserve PDF originals, including their enclosing
archives where a deferred PDF has no separate retained local copy.

## Typed mapping started

The first implementation maps the retained individual-contribution bulk layout
into `fec_receipts`. It preserves native identifier spelling, stores exact
`DECIMAL(38,9)` amounts and dates with explicit conversion states, and carries
the collection's snapshot/insertion/deletion role separately from amount signs
and amendment flags. Excess precision and unsupported values stay refused in
the typed fields, with the original values reachable through source evidence.
Filing associations and current-record applicability remain unqualified.

The shared evidence mapping distinguishes source-record witnesses from
collection-context dictionaries. Dictionary witnesses use
`collection_outcome_json` and `/tableFieldDefinitions`, with no invented
source-record identifier. Within-generation target links store `self` scope;
external source links carry an already sealed generation pin.

A bounded trial compared typed values and identifiers with retained native
positional rows, checked observation-key uniqueness, and verified every output
cell after a Parquet roundtrip. Its population and results are in the
[trial receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-receipt-trial/verification.json).
This validates the sampled mapping boundary; it does not establish full
population acceptance, correction applicability, current financial totals or
public availability.

## Collection register and additional mappings

The [collection census](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-layout-census/status.json)
reconciles every populated collection to the sealed source table. Its bounded
layout witnesses help choose mappings; they do not prove uniform fields across
whole collections. The
[mapping register](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-mapping-register-v6/collections.json)
accounts for all selected collection IDs, including dictionary evidence, API
controls, discovery listings, aliases, empty selections, statistical workbooks,
research text, partial sources and deferred PDFs. Proposed destinations identify
remaining work; a context needing semantic review is not a source limitation
that excuses implementation.

Additional consumer mappings cover other-committee and committee-to-candidate
transactions, operating expenses, independent expenditures, communication costs
and electioneering candidate/disbursement observations. The
[native-field trial](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-financial-trial-v5/verification.json)
passed on 40,554 selected observations from 22 collections. It compares every
mapped text field and exact decimal/date with the retained positional values,
checks every Parquet cell, and compares derived evidence with the stored oracle
in both directions while preserving duplicate associations. This remains a
bounded mapping result, not full-corpus financial qualification.

The operating-expense
[full width census](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-oppexp-audit/verification.json)
found one extra empty position after the named header fields on every retained
row. The consumer maps the named positions, links the retained ordered header,
and exposes the empty unlabelled position in its mapping result. A nonempty tail,
changed width or changed header refuses mapping. The
[FEC description](https://www.fec.gov/campaign-finance-data/operating-expenditures-file-description/)
corroborates the named column order and operating-expense meaning; the retained
header and native original remain the pinned witnesses.

The mappings keep financial meanings separate:

- An independent expenditure's amount and reported aggregate are distinct.
- An electioneering row's full disbursement and supplied candidate share are
  distinct. Event equivalence remains unresolved, so repeated full amounts do
  not become additive expenditures.
- Derived spending-target associations use supplied candidate fields and
  allocations. They do not infer equal shares or copy full spending amounts into
  an allocation field.
- Committee transaction direction remains unresolved until a supported policy
  interprets its native type. Amount signs and amendment flags do not select
  correction operations.
- Two-digit source years remain raw and unresolved unless explicit evidenced
  year bounds identify one century. No implicit date-library pivot is applied.

The retained bundling file contains recipient-period totals without individual
bundler identity. Its destination is reported financial summaries. It cannot
populate bundler/recipient records merely because its filename says bundling.
The existing SpicyDocs filing-layout API supplies pinned field definitions for
the next filing mappings; native version/form applicability still needs evidence.

Focused checks for the mapping, evidence views and existing FEC observation and
context boundaries passed. Ruff and changed-file type checking also passed;
PDF fixtures were excluded. These modules and views have not been registered as
public tables or deployed.

## Capacity and next action

### Complete selected summary and financial API mappings

The [complete mapping receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-summary-api-full-v2/verification.json)
records full field checks for the retained summary, aggregate and financial API
selection. The outputs contain 80,116 summary observations, 17,335 contribution
aggregate observations and 107 captured financial API results. Headers and API
response controls have separate counts. This proves the selected observations;
it does not claim complete API traversal, amendment selection or current totals.

Summary rows retain every source monetary measure with its literal field name,
raw value, exact decimal, unit and conversion state. A derived measure view
makes these queryable as rows without storing repeated source evidence. It keeps
quarterly and semiannual bundling totals, balances and period activity distinct.
Geographic/size aggregates retain native bins and leave unreported contributor
counts NULL. Candidate-like aggregate identifiers remain unresolved source
identifiers; labels such as “All candidates” do not become candidate identities.

Financial API adapters cover national-party account receipts, loans, debts,
allocated disbursements, coordinated expenditures and inaugural contributor
totals. Each captured result was compared with its hash-verified original JSON.
Loan principal, loan balance, debt-period activity, allocation components and
reported aggregates remain separate fields. API controls do not become money
rows. The selected loan results contain no supplied guarantor details to map.

The full run exposed valid decimal spellings such as `.01` and `-.99`. The
shared exact converter now accepts them and records
`fec-exact-financial-values/2`. The rerun accepted every retained monetary value
in this selection without rounding. Two-digit-year dates still require
evidenced century bounds, and the source's `99999999` date values remain
explicitly invalid. Raw values are preserved in both cases. Exact stored-cell
hashes, source-field comparisons and evidence multiset checks passed across the
complete output; this does not qualify broader financial interpretation.

### Filing preparation

The [full filing census](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-filing-layout-census/status.json)
reconciles every selected filing observation and every record's positions. It
retains literal format versions, including trailing spaces, and identifies the
version/form/width combinations to map. Header, record, text and malformed-file
fallback observations remain distinct. The retained workbook facts already
exist in collection contexts, so the next filing mappings can link the source
package's definitions to those witnesses instead of inventing dictionary rows.

The [financial filing trial](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-filing-financial-trial/verification.json)
checks every retained exact-version `8.5` financial form/width shape supported
by the new adapters. The trial checks physical positions, exact money and
date conversion, complete stored cells and derived evidence against a separate
row-level oracle. The [complete selected filing receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-filing-financial-full-v2/verification.json)
then verifies all 1,590,073 supported observations. Each output
passes full stored-cell sequence hashing, native-field checks, unique-ID checks
and evidence multiset comparison. Other forms in these files retain separate
counts and remain pending; older version strings were not silently remapped.

Schedules A and B preserve contributor/payee details, separate aggregates,
native transaction/back-reference IDs and memo flags. Loan principal, payments
and outstanding balance remain distinct; debt-period balances/activity,
guarantor references, independent expenditures and H4 allocation components
have separate fields. The adapters preserve short rows, body references and
extra positions with explicit dispositions. They do not trim version strings,
interpret a transaction ID as a report number, or turn bundled-contribution
schedules into ordinary itemized receipts.

Workbook labels and textual specification cells resolve to existing sealed
collection facts. The actual pointer is
`/receiverDisposition/callerContext/facts/parsing/facts` within
`collection_outcome_json`; the earlier preparation assumption that caller
context was at the root was incorrect. The normalized definition table and
its evidence retain the exact workbook/version and native cell locations.
Serving evidence joins those definitions plus the source file header to each
financial record. Tests exercise the real nesting and reject changed or
ambiguous workbook cells.

The first full filing run stopped at its 3 GiB RSS guard and remains saved
with a failed-attempt receipt. The successful retry checks evidence in batches
of 256 rows, closes each checking connection after use and keeps Parquet write
batches separate. Its complete output sizes and peak memory are retained in
the receipt. This is field-mapping qualification, not amendment/current-total
qualification.

### Additional retained filing formats

The [complete retained-format receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-retained-filing-full/verification.json)
qualifies field mapping for the supported financial schedules outside the exact
`8.5` selection above. It verifies 780,924 observations from the retained
electronic and transcribed-paper inputs. These add contribution, payment,
loan, guarantee, debt, allocation, coordinated-spending and inaugural queries.
Combined names stay combined, source-only reference codes remain explicit,
and amount positions follow the selected pinned definition.

All defined fields, source membership, stored cells, unique observation IDs and
derived evidence passed the full run. Its only value refusals are missing source
positions; raw values remain reachable. Boundary ASCII padding on filing amounts
has the explicit `exact_after_ascii_padding` state, while the unmodified text
stays in the raw field. A padded native format-version string remains literal
and requires its own explicit selection.

The source reader owns electronic, paper and comment-style header syntax.
The consumer uses the reader's declared version and cites the native header.
Workbook evidence selects a ZIP member's digest when the definition is retained
inside an archive, preserving the original archive identity in collection facts.
The initial trials exposed and corrected these two consumer assumptions; their
failed receipts remain alongside the successful trials.

The [focused checks](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-focused-checks-04.json)
and [checkpoint](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/checkpoint-04.json)
record the implementation pins, meaningful source-format tests and measured
output size. This is field qualification; financial policies, filing joins and
the complete typed release remain unfinished.

### Legal, agency and identity observations

The complete selected legal API population now maps to matter, party-role,
event, document-reference and audit-category observations. The
[legal qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-legal-qualification/qualification.json)
accounts for every selected source observation and response control. Native
matter identifiers retain their namespaces. Repeated party names do not establish
person identity, and linked PDF bodies remain deferred.

Retained FOIA XML, oversight HTML and Word reports now supply report editions,
metrics, recommendations, readable text and document references. The
[agency replay](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-agency-qualification-v2/qualification.json)
preserves metric definitions, periods, dimensions and unspecified units. Central
integration caught an emitted `bound_value` missing from the Parquet schema;
the superseded attempt is retained, and the new schema preserves the reported
`<1` bound without inventing an exact value. `fec_typed_batch.typed_batch` now
rejects emitted fields absent from a schema, including nested fields.

The [identity qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr04-identity-qualification/qualification.json)
maps registration statements, lobbyist registrations, quality notices and filing
metadata. Native file numbers identify filing observations; zero or absent file
numbers remain unresolved. Image numbers and names do not become filing keys.
Two-digit dates without independent century evidence remain raw and unresolved,
and a quality notice does not automatically exclude financial observations.

A separate [stored-value replay](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-typed-schema-integration/verification.json)
checks every original mapped legal and identity value against the existing
Parquet outputs. This closes the gap between testing an already-coerced Arrow
table and proving that no mapped fact disappeared during serialization.

### Individual-contribution representation and scoped selection

The [complete original-byte comparison](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr07-individual-representation-equivalence/verification.json)
proves that the retained `itcont.txt` is a concatenation of all date members,
in a different member order. Every exact-length main-file segment matches one
complete member by SHA-256; every member is used once. The comparison covers
all 5,930,347,790 decoded bytes and 32,034,987 rows, including separators and
duplicate row multiplicities. It used about 61 MB peak RSS and no decoded
scratch files. Counts alone were not the equivalence test.

The [bound representation policy](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr07-individual-representation-equivalence/selection-qualification.json)
checks the exact archive member identities, decoding rules, source-generation
pin and sealed collection counts. It selects the main representation and marks
the date representations as duplicates for this retained snapshot purpose.
Both physical sources remain in the source generation. The inclusion view
preserves one decision per observation and refuses ambiguous policy rows or
generation drift.

The retained insertion/deletion streams still lack proven predecessor snapshot,
window and ordering evidence. They remain inspectable with explicit operation
roles; this policy does not apply them or qualify a current financial total.
The new bounded `fec_financial_selection.select_scope` supports exact
scope/membership pins, complete replacement, partial amendment, explicit deletion,
proven already-applied insertions, equivalent representations and idempotent
operations. Its adversarial tests verify exact membership and refusal when
evidence, scope, ordering or completeness is missing. These tests establish the
selection machinery, not applicability to an unproven retained amendment chain.

### Complete retained filing and context accounting

The [final filing qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-remaining-filings-final/completion.json)
closes the remaining useful native filing mappings. Report covers, notices,
bundling, special accounts, allocation bases, transfers and loan terms retain
their separate meanings. Narrative observations retain exact body references,
native fields and readable text. A malformed unofficial Senate filing remains
literal physical text with `syntax_uninterpreted` status; its broken framing
does not justify inventing a filing identity or monetary interpretation.

The complete native filing census reconciles financial/report/narrative rows
with source headers and empty records. Seven historical F3 covers retain an
explicit source-dictionary ambiguity; their affected amounts and dates remain
uninterpreted. The final receipt pins all outputs and current shared header and
serialization checks. The [mapping register](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-mapping-register-v8/status.json)
integrates that accounting without claiming financial-current-state qualification.

The [context integrity replay](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-context-qualification-v3/integrity.json)
qualifies retained historical statistics, complete CSV preview rows, attributed
DocumentCloud metadata, meetings, court/form references, filing-feed events and
explicit source refusals. The software directory is reference documentation.
Partial preview tails and failed responses remain distinct from complete empty
results; third-party document metadata does not become official FEC content.

### Reviewed selection guards and filing-reference joins

An [independent follow-up review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr07-selection-independent-review/followup-review.json)
approves the corrected selection and serialization boundaries. Nested Arrow map
keys/values receive the same field-preservation checks as lists and structs;
unsupported shapes refuse conversion. Snapshot decisions expose their policy
version and reject unknown versions or statuses. Explicit deletion operations
now retain their own physical source-observation decisions separately from the
removed financial records, including when the entire scope remains unresolved.

The [complete filing-reference replay](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr04-reference-resolution/verification.json)
checks all retained references against an independent native-key index. It
resolves 579 references, preserves 14 missing native identifiers and 14 targets
outside the retained selection, and reproduces every other source-reference
field. Grouping target observations before joining preserves one output per
reference. A resolved native filing identity does not establish amendment
direction, replacement scope or complete affected membership.

### Additional committee observations and filing associations

The [committee completion receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr04-committee-observations/completion.json)
qualifies the additional retained API samples as separate observations and
response controls. It checks every native result value, array position and
written value. The accompanying reuse reconciliation matches older API captures
to existing releases by both exact response digest and request URL. Those aliases
do not create additional committee entities or replace the broader registry.
The remaining history check found retained committee master and PostgreSQL
observations without a qualified typed history output; their mapping is underway.

The [corrected filing-association receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr04-filing-associations/corrected-v2/completion.json)
binds physical headers through API-reported original URLs and bulk observations
through qualified native file-number fields. Joins preserve one decision per
input row. Review fixes preserve the correct filer field in union schemas,
refuse unknown policy metadata, and include the full source-observation identity
in association IDs. The last fix resolves a real collision between two retained
captures of the same original. Earlier attempts remain preserved. Archive member
names and unavailable target filings remain explicit unresolved associations.

### Financial meanings and combined-table preparation

The [financial-policy qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr07-financial-meaning/integrity.json)
checks source-specific memo treatment, signed reported values, spending before
candidate joins, single loan/debt states, payment allocations, publisher-calculated
electioneering shares, and defined summary equations. It separates these useful
reported measures from current economic totals. Itemized refund/attribution
links, correction applicability, cross-filer transfer pairing and event
equivalence remain unqualified where the retained evidence does not establish
them. Independent review found missing authority/version applicability checks; the corrected policy and serving integration remain in progress.

The [schema preflight](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-schema-preflight/verification.json)
finds no type conflicts among completed output schemas and the bulk build's
qualified target schemas. It reads footers, records exact expected member pins,
and applies strict unions without type promotion. It does not admit unfinished
bulk files or establish complete release membership.

The [unified filing definitions](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-filing-definitions/verification.json)
preserve source-layout identities separately from mapper selectors. Every held
definition is rebound to the SHA-verified workbook extraction, and every
non-null definition reference in the completed typed files resolves. The
verification reuses the qualified bounded context extraction; a redundant wide
catalog read exceeded the small-job memory guard and stopped before outputs.
The successful run stays within that guard. Combined generation construction,
identity/evidence checks and deployed query qualification still remain required.

The [serving-evidence check](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-serving-evidence/verification.json)
verifies real bulk and filing observations in one table. Header witnesses are
emitted only where header evidence exists; malformed partial witnesses remain
visible for rejection. The SQL module imports without optional source-processing
packages, and the shared policy identity reproduces the previously qualified
selection rows exactly.

### Resource limits and remaining work

Parquet footer measurements show about 9 GB of compressed source records and
213 GB of decoded columns. Full-memory or full-scratch decoding would exceed
the available capacity. The
[resource budget](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/resource-budget.json)
therefore requires bounded reads, one heavy process, separate build/seal/restore
phases and a fresh free-space check before each job. A separately measured
budget authorized the complete summary/API selection above. The later combined
native receipt measurement below qualifies the next full bulk field build.

The [evidence-storage trial](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-evidence-storage-trial-v2/verification.json)
proves that derived evidence reproduces the stored record/header/context oracle.
Keeping the exact source address on each typed row avoids storing its long
identity again for every witness. The receipt-family estimate falls from roughly
8.36 GB with stored evidence to 5.75 GB with derived evidence. Including the
additional financial mappings gives a sample-based estimate of about 6.71 GB.
These earlier estimates included duplicate individual date representations.
The complete byte-equivalence proof permits one typed representation with
explicit duplicate dispositions while retaining both physical sources.
Compression and row lengths can still vary across the full population.

The sealed collection catalog contains a roughly 632 MB decoded context column
chunk. Small Arrow batches do not bound that decompression. One shared selected
context extraction therefore used an isolated 1.5 GiB RSS ceiling; its measured
peak was about 584 MB. The small filing/context mapping jobs retain the 512 MiB
ceiling. Their combined evidence allowance increased to 192 MiB to retain exact
derived native filing members needed for narrative bodies, with shared files
counted once. The final filing receipt records actual use below that bound.

The [whole-build capacity receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-rowgroup-capacity/capacity.json)
uses measured strict schema unions and a combined native read/map/write test.
Parquet's accumulated row-group metadata makes very small write groups costly.
The selected 32,768-row groups reduce that overhead; the complete receipt union
and source footer fit the measured run at about 526 MiB peak RSS. The full job
uses a 3 GiB RSS stop, 8 GiB output cap, 20 GiB free-space floor, no mapping spill
and one heavy process. The capacity receipt reserves a separate sealing copy;
archive/restore capacity will be rechecked against the final input selection.

The [full bulk build](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-financial-full/resource-plan.json)
has exited with a free-space-floor refusal during final readback. The
[failure receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-financial-full/failure.json)
and completed-collection receipts remain beside that plan. All selected typed
rows were written, and the main Parquet file has a complete footer; its
`.partial` name remains because full verification did not finish. These files
are preserved for read-only recovery, not admitted as a complete build. The
field mapping does not apply unproven corrections or qualify current totals.

Next, verify the existing bulk output, assemble the qualified table inputs, and integrate
the financial SQL, release registry and final evidence checks. Committee history
and reference accounting are now recorded below. Whole-release local verification, portable R2
archive and restore, publication, MCP deployment and eligible source cleanup
remain unfinished. No source files were deleted, and no archive objects,
publication indices or deployments were changed by this execution checkpoint.


### Retained reference and history closure

The [source-lane completion receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-remaining-reference-reuse/completion.json)
qualifies every selected committee-master and PostgreSQL history observation,
the retained committee API census, the candidate API result and response controls.
It compares native values and complete stored outputs, including raw PostgreSQL
COPY spellings. Original captures, repeated observations and unknown current
identity remain distinct. Successful bounded workers stayed below the declared
memory ceiling; failed attempts remain evidence and are excluded from assembly.

The same receipt records exact reuse for candidate history, reference rows,
listing facts, aliases and retained contexts. Context reuse combines the prior
complete cell proof with fresh catalog/context hashes and exact collection
membership; it does not claim a new wide-column replay. Missing originals,
partial fragments and refused dictionary ordering remain visible source limits.
The [integrated work register](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr03-mapping-register-v10/status.json)
reconciles this closure against the prior register and preserves the separately
qualified sample, relationship and duplicate-representation decisions.

### Assembly and runtime release checks

The [assembly input manifest](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-assembly-inputs/verification.json)
freshly hashes completed table pieces, checks their row counts and every source
namespace, and builds strict combined schemas. The main bulk output remains
excluded until its terminal verification passes. The source lane's final admitted
parts match this draft; verified empty shard headers will be omitted from the
final assembly. Normalized filing definitions replace earlier duplicate copies.
Financial witnesses remain derived from exact source addresses, avoiding repeated
stored evidence without losing the underlying originals.

The new assembler compares every output cell against those selected inputs.
Independent review reproduced two failure classes: destination/staging overwrite
and accepting bytes changed after value readback. Exclusive file creation and
hard-link promotion prevent overwrite. Before/after byte pins bind the compared
values to the promoted files. Failed or partially promoted output has no success
receipt; the existing generation publisher owns atomic publication. The
[independent replay](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-assembly-review/attempt-v3/completion.json)
now confirms the refusal cases and complete comparison of the selected real
outputs, including nested summary measures and bounded agency values.

The [completed-table assembly](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-completed-table-assembly-v2/verification.json)
combines every selected table outside the pending bulk schemas and compares all
stored cells. An earlier API-control readback exceeded its worker memory bound;
that failed output remains excluded. Smaller batches passed under the same
ceiling. Previously completed tables were reused only after matching their input
selection, code and fresh output hashes. The
[whole-table checks](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-completed-table-checks/verification.json)
confirm non-null unique record IDs wherever that key exists, and exactly one
target for every stored evidence link. These are completed components; the full
financial assembly and sealed release remain pending.

The [release compatibility checks](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-release-compatibility/integrity.json)
verify exact dependency members, schemas, SQL and installed interpretation files.
Local MCP tests cover missing or changed receipts, dependency advancement,
consumer drift, unaffected raw queries, captured connections and rollback.
Synthetic test receipts establish checker behavior only. Actual registry wiring
is now prepared; its new tests and static-tool checks have not run because the
disk floor refuses execution. The
[registry handoff](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-qualified-query-views/registry/static-handoff.json)
pins the changed files and pending commands. Release and image pins remain unset,
so declared qualified views are not enabled. The release receipt and
independently verified running image remain outstanding.

[Financial policy `/2`](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr07-financial-meaning/corrected-v2/completion.json)
fixes the reviewed authority and mapper-applicability checks. Its retained
reported-measure decisions and negative probes pass. The
[filing-number SQL proof](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr04-filing-number-sql/corrected/completion.json)
compares complete decision cells against the Python mapper and refuses unknown
target identities without dropping observations. The
[financial-rule SQL comparison](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-financial-rule-sql/completion.json)
now matches the retained Python decisions, including refusals, exact amounts and
definition witnesses. Additional review found that a narrow SQL projection could
skip a record-ID guard and an undeclared column could shadow a policy helper.
The [corrected query-view qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-qualified-query-views/corrected/completion.json)
records the fixes, refusal checks and retained-decision comparisons. Runtime
registry verification remains separate and pending.
None of these results establishes amendment-qualified current financial totals.

### Bulk recovery and current capacity

The [bounded recovery comparison](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-readback-recovery-v2/bounded-trial-000016.json)
matched 95,184 main-output rows against the native source and complete mapper
output. This measured sample does not qualify the whole file. Independent review
then required held-file identity checks, exact completed-collection membership,
all native positional-field comparisons and metadata hashes derived from the
same bytes being parsed. The
[v3 review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-recovery-review/review-v3.json)
distinguishes passing memory-only probes from filesystem tests refused at the
capacity guard. A separate v4 runner addresses the remaining metadata-pinning
issue; it has not yet completed its executable qualification or full readback.
The original writer and earlier attempts remain unchanged.

The [assembly reuse plan](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-assembly-reuse/REUSE-PLAN.md)
uses the bulk outputs' existing column order for the relevant schemas. It changes
no field meanings, types or nullability, and preserves the completed non-bulk
schemas. Once admitted, whole bulk files can be hard-linked into final membership;
only smaller mismatched pieces need rewriting. Existing generation sealing still
requires its measured copy allowance.

Available space fell below the declared 20 GiB floor after the writer stopped.
No bulk or recovery process is currently running. The
[phase capacity estimate](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-assembly-reuse/capacity.json)
already showed that assembly followed by a clean restore did not fit its earlier,
larger free-space snapshot. Recheck actual free space and allocation before each
phase; do not use the older estimate as permission to run. Small code fixes and
status receipts can proceed, but disk-qualified execution remains held. No source
deletion, remote archive operation, publication or PDF processing has occurred.

### Portable archive and bounded transfer preparation

The [archive candidate](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-archive-preparation/completion.json)
enumerates digest-addressed originals and context, a portable input manifest and
the reader's actual restore dependencies. Descriptive paths inside source facts
remain unchanged. The candidate still requires final release acceptance and a
fresh capacity check before admission.

The [credential classification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-credential-classification/verification.json)
resolves the selected metadata flags against exact retained official FEC HTML
captures. Each flagged value appears in a browser-visible script assignment;
the pinned publisher templates corroborate those assignments. The receipt
records paths, hashes and variable names, never key values. Strict normalization
of bare versus prefixed SHA-256 text resolves the first classifier attempt's
failure; original bytes and context pins remain unchanged. This classification
does not replace full transfer-time integrity checks or authorize using the
publisher's browser keys for API requests.

[Streaming retention](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr10-preparation/handoff.json)
now hashes and checks local files in bounded chunks while preserving the existing
evidence journal. File mutations refuse successful retention. The
[publisher change](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr10-publisher-streaming/handoff.json)
streams untrusted remote blobs directly into artifact verification, removing
accumulating verification copies. Both changes pass focused checks, including
mutation, interrupted-stream and membership failures. The separate prepared
readback checks every remote blob's full SHA-256, including objects the publisher
reuses through its existing ETag check. Actual upload, full remote readback,
clean restore, public query acceptance and source cleanup have not run.

### Commit checkpoint — 2026-10-01

The retained-data execution remains paused. This checkpoint saves the current
implementation and documentation; it does not admit the bulk output or publish
the expanded FEC release. The SpicyDocs source worktree is already committed
through `bcdde5431fac`; the pending SpicyRegs work is grouped into streaming
evidence storage, typed FEC tables and query integration, and delivery notes.

The [commit-check evidence](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/commit-checkpoint-20261001/)
records fresh checks after storage cleanup restored enough space for local tests.
Repository-wide Ruff and offline dictionary checks passed. Every generated
table page matched its current descriptions and schema. The selected FEC, MCP,
generation, source-evidence and dictionary suite reported 1,278 passing tests,
three failures and one deselection in `pytest.log`. The failures are:

- `test_fec_release.py::test_release_import_does_not_load_source_processors`
- `test_mcp_server.py::test_an_unmoved_publication_keeps_the_connection_past_the_ttl`
- `test_mcp_server.py::test_other_callers_keep_the_connection_while_one_rebuilds`

The repository-wide type check also reported 32 diagnostics in the FEC research
context mapper and FEC tests; `ty.log` retains their exact locations. These
results supersede the earlier statement that registry execution checks were
still held by disk capacity. They do not establish a passing integration gate.
The MkDocs build was not run because that tool is absent from this environment.

Before release admission, resolve these check failures, qualify the corrected
bulk recovery runner, complete full readback and combined-release validation,
and refresh the capacity budget for sealing and restore. R2 transfer, clean
restore, deployment, public-query acceptance and eligible source deletion
remain outstanding. PDF processing and historical acquisition remain deferred.
