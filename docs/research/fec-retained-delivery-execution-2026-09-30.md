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

The retained-data execution was paused at this commit checkpoint. It saved the
implementation and documentation; it does not admit the bulk output or publish
the expanded FEC release. The SpicyDocs source worktree is already committed
through `bcdde5431fac`. SpicyRegs now records streaming evidence storage in
`87f7d66`, typed FEC tables and query integration in `935b300`, and delivery notes
in `26d2761`. These are local commits; they have not been pushed. The
[task register](../fec-delivery-plan.md#next-execution-checkpoint) records the
remaining fixes and resume order.

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

### Resumed validation 2026-10-01

Retained-data execution has resumed. The commit-time test failures were stale
fixtures: the import test expected an empty FEC view registry, while cache tests
omitted the release configuration now stored alongside publication metadata.
The corrected fixtures preserve lightweight imports, the populated registry,
connection reuse for unchanged releases and serving during refresh. Production
MCP behavior did not change for these fixes. The
[registry-fix receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-registry-fixes-20261001/completion.json)
records their focused validation and file pins.

Explicit internal types now describe context cells, links, headings and feed
items; test fixtures also preserve their heterogeneous value types. A fresh
[selected suite and static checks](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-local-checks-20261001/results.json)
pass: `pytest.log` reports 1,281 passed and one live test deselected. Repository
Ruff, ty and offline dictionary checks pass. The separate generated-page receipt
reports no mismatches. These results supersede the commit-time failures above;
the fixes remain local uncommitted changes. MkDocs remains unavailable, so the
documentation build is unverified.

Because the mapper file changed, the
[context replay](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-context-type-replay-20261001/integrity.json)
binds the new code pin and proves complete ordered field equality against the
previously qualified stored Parquet for the selected context population. It
preserves the prior immutable receipt and outputs. Selected unit tests and this
component replay do not establish full combined-release acceptance.

The independent [v4 recovery review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-recovery-v4-qualification-20261001/review.json)
reproduced three defects with bounded synthetic files: replacement of a reused
completed Parquet after hashing; reuse of journal checkpoints from an attempt
whose held file changed; and mutation of journal bytes after parsing. The v4
runner is rejected. The v5 runner holds completed files through hashing and
footer validation, binds file identities across resumes, checks those identities
before appending progress, and tracks exact journal bytes through its own writes.
The [v5 qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-recovery-v5-qualification-20261001/review.json)
confirms those failures now refuse and valid resume works, but also shows that
edited checkpoint counts can enter a success receipt. V6 validates counter types
and mapper states and atomically records each committed journal prefix by exact
length, digest and group count. It preserves an interrupted uncommitted tail
before replay and rejects changes within the committed prefix. Independent v6
qualification is in progress. Full bulk recovery has
not started, and no success receipt admits the existing bulk output.

The [capacity refresh](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr02-capacity-refresh-20261001/capacity.json)
measures the closed bulk files and available space. Readback, assembly, clean
restore and a conservative provisional sealing copy fit above the free-space
floor at that snapshot. This removes the previous capacity hold for those
bounded phases; phase guards and final member-size measurement remain required.

Next, qualify v6 and complete bulk readback, then assemble and verify the whole
retained release. Archive transfer, full remote hashes, clean restore, matching
deployment, public-query checks and eligible source deletion remain unfinished.
PDF processing and historical acquisition remain deferred.

### Bulk readback and release preparation 2026-10-01

This checkpoint supersedes the recovery status immediately above. The
[V7 qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-recovery-v7-qualification-20261001/review.json)
passes the previous recovery and journal-integrity probes plus the retained
operating-expense metadata cases. The only change from V6 permits a missing,
null or empty `field_mapping` to mean no header row, matching the retained
selection and producer receipt; invalid types still refuse. The
[bounded retained-data trial](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-readback-recovery-v7/bounded-trial-000016.json)
passes complete ordered comparison for its selected groups. Neither receipt
claims full-output acceptance.

Full readback is running with the same pinned V7 runner and committed journal.
The [14:32 UTC progress snapshot](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/task-list-refresh-20261001/readback-snapshot.json)
records 13,890,512 compared rows and 1,700 of 3,917 source groups; the
[live log](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-readback-recovery-v7/full-readback.log)
is the source for subsequent progress. No terminal success receipt exists at
that snapshot. Preserve the process and its proof files until it finishes.

The [final composition preparation](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-assembly-preparation-20261001/completion.json)
passes bounded synthetic qualification and metadata preflight. The
[key-checker qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-release-checks-preparation-20261001/synthetic-v1/qualification.json)
passes synthetic membership, identity, namespace and rejection cases; independent
review is in progress. These checks read no real corpus data pages. Real final
assembly and archive staging have not started. The archive will use the existing evidence owner's
normal staged copy; refresh capacity for both that copy and a separate clean
restore before staging. Complete keys, evidence, financial and local MCP checks
must bind the same exact final table-member receipt before archive admission.

Archive preparation also found a
[classification-evidence selection gap](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-archive-admission-preparation-20261001/classification-selection-gap.json):
the candidate omitted original HTML captures used to classify selected browser
configuration metadata. Add those exact witnesses to the supplementary selection
with their existing qualified digests, sizes and reason for inclusion. Preserve
the original candidate and gap receipt; final admission must rehash and scan the
complete expanded selection.

The [Worker preparation receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-deployment-preparation-20261001/completion.json)
records forwarding of `SPICY_REGS_FEC_RELEASE_SHA256` and
`SPICY_REGS_CONSUMER_IMAGE_DIGEST` into the container. Type checking and Worker
dry-run bundling pass. These values remain unset pending the admitted release
and actual image identities; no image build or deployment occurred.

The [public-index refresh](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr01-public-index-refresh-20261001/receipt.json)
retains both current public indices and their matching FEC family pins. The
required local `fec-observations` generation differs from the currently public
parent, so final publication must include the required parent as well as
`fec-query`. This read-only public check does not refresh authenticated R2 state
or conditional-write tokens; refresh those before publication. Archive upload,
remote full hashes, clean restore, public release acceptance and eligible source
deletion remain open. PDFs and historical acquisition remain deferred.

### Bulk recovery passed and final composition started 2026-10-01

Full V7 readback exited successfully. Its [terminal receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr05-bulk-readback-recovery-v7/verification.json)
records complete native-field and stored-cell comparison for the main
32,034,987-row file, with every selected source group checked. Its exact reuse
of the other completed collections accounts for 45,014,959 selected bulk rows.
Peak process RSS was 834,125,824 bytes. The main amount values all retain exact
decimal meaning; source-empty dates remain distinct from exact dates. This
receipt admits the bulk-output component, not a complete published release.

The [prepared final selection](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-assembly-20261001/selection.json)
binds the successful recovery receipt, qualified non-bulk outputs, canonical
schemas and ordered financial pieces. Metadata review accounts for 49,357,527
rows across the selected tables; these include evidence and association rows
and must not be interpreted as a count of distinct financial transactions.
The [composition launch](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-assembly-20261001/launch.json)
started only after recovery closed, with an explicit memory/new-output budget
and the free-space floor. Final composition is in progress at this checkpoint.

Independent key-checker probes reproduced parent-directory replacement during
SQL, late extra files, a success receipt left by a final resource refusal and
mismatched source-generation metadata. The fixed checker reads held file
descriptors, repeats complete member inventory, binds the exact reviewed source
scope and promotes success exclusively after checks close. Its
[final independent review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-assembly-preparation-20261001/checker-review-v3/completion.json)
passes the valid cases and rejects each reproduced failure, including receipt
collision. This qualifies the checker; it has not yet checked the actual final
tables.

The [archive-adapter handoff](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-archive-admission-preparation-20261001/handoff.json)
passes synthetic admission/readback/restore tests and metadata preflight. It
includes the missing original HTML witnesses and separately reserves staging,
fresh restore and failure-recovery space. Actual credential scanning, archive
admission, remote verification and restored-source replay remain pending.
Complete evidence/financial acceptance and local MCP checks are being prepared.
Local MCP checks will use explicitly labeled candidate release identities;
sealed generation, archive and image acceptance still belongs to FR12/FR13.

### Final composition and complete key checks passed 2026-10-01

The [final composition receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-assembly-20261001/output/verification.json)
passes with exact final membership, rechecked input/output hashes and complete
stored-cell comparison for schema-aligned pieces. Reused files retain their
previously qualified bytes. The composition created 173,757,848 new logical
payload bytes for alignment; linked files share original storage and are not
independent backup copies.

The [complete key-check receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-key-checks-20261001/output/verification.json)
passes across every selected final table and row. It binds the composition
digest `sha256:b38326f56bfd364fbd3e12c280a0abd378adadee7edcedf249e87ed158cab4d0`,
exact source-selection/generation pins, canonical schemas and every member hash.
Checks cover row counts, unique row identities or complete evidence-association
keys, actual namespace values and complete member inventory. Peak RSS was
1,947,205,632 bytes and peak temporary spill was 3,171,483,648 bytes, within the
explicit bounds. The process exited successfully and closed all held files.

This completes the key/membership component of FR08. Complete evidence endpoints,
financial decisions and draft local MCP acceptance remain required; neither
receipt admits the archive or establishes a public release. Their executable
checks are being prepared through existing table, query and policy owners.

The [read-only Wrangler account check](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-deployment-preparation-20261001/account-check-20261001.json)
matches the configured Cloudflare account and reports container/deployment
permissions. The Docker daemon responds locally. These readiness checks do not
establish R2 admission, a built consumer image or deployment. No source originals
were deleted, and PDF processing and historical acquisition remain deferred.

The subsequent [authenticated R2 refresh](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr01-authenticated-index-refresh-20261001/receipt.json)
confirms read access to the configured `spicy-regs` bucket. Both stored
publication indices match their public copies byte-for-byte and preserve the
previous FEC family versions. The receipt retains exact bytes and ETags. This
was read-only; publication must refresh conditional-write state when it runs.

### Acceptance and restore preparation task update 2026-10-01

This task update supersedes the earlier statements that bulk recovery or final
composition remain in progress. The [receipt snapshot](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/task-list-refresh-20261001/task-status-snapshot-after-mcp-preparation.json)
rehashes the successful bulk recovery, final composition and complete key
receipts. Their exact digests still match the completed checkpoints above.
The delivery plan and gap register now distinguish those completed checks from
whole-release evidence, financial and MCP acceptance.

The [evidence/financial runner handoff](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-evidence-financial-preparation-20261001/completion.json)
was frozen after that snapshot. Its [qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-evidence-financial-preparation-20261001/synthetic-qualified-v2/qualification.json)
passes synthetic positive/refusal probes and admits the exact composition/key
metadata. No production table payload was read and no acceptance receipt was
issued. The next full run checks evidence endpoints, filing associations,
representation choices and financial decisions through the existing owners.
Both successful role receipts must include the exact source and composition
pins and references to their evidence. Unsupported current totals and amendment
replacement remain explicitly unqualified.

The [local MCP handoff](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-local-mcp-preparation-20261001/handoff.json)
passes synthetic runner and existing-owner checks. It binds the final table
metadata but has not read or linked real payloads. Actual acceptance requires
the successful key, evidence and financial receipts and confirmation that all
checkers have closed their held files. It will verify member bytes, bind the
registered views, exercise representative queries, and check refusal, refresh
and local rollback. Its candidate descriptors support local checks only;
FR12/FR13 still require actual sealed generation, archive and image identities.
Any archived candidate receipts remain historical local-check evidence.

Restored-source replay preparation is in progress. It found that the earlier
archive candidate omitted filing-body reference metadata and associated
qualification evidence. Add those dependencies and the replay plan/code pins
to a newly pinned selection and recipe before staging; preserve the earlier
candidate. The replay must cover representative non-PDF formats and every
special dependency route, including PostgreSQL derivation and filing bodies
or ZIP members, with original workstation paths unavailable and no network
fallback. The [PostgreSQL runtime receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr11-runtime-readiness-20261001/pg-restore.json)
confirms that the exact required executable version and digest are installed.
Retained Word/RTF routes require the macOS runtime; this preparation does not
establish replay on arbitrary operating systems. Refresh capacity for replay
scratch as well as separate staging, fresh restore and failure recovery.

The replay agent subsequently identified another explicit dependency: the
candidate does not contain `eFilingFormats.zip`, whose contexts describe retained
spreadsheet and Word/RTF reference members. That parent also contains deferred
PDFs. Account for exact qualified non-PDF member bytes, parent/member identities
and derivation evidence in the expanded selection; preserve the mixed parent
and exclude it from deletion. Member-byte availability is still being checked.
This finding does not establish those reference-format replay checks as passed.

The [current container receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-deployment-preparation-20261001/current-container-20261001.json)
saves the deployed image identity for rollback planning. It does not verify a
matching new release or a completed rollback exercise. Actual archive staging,
upload, full remote readback, clean restore/replay, final sealing, expanded
publication, matching deployment and eligible FEC source deletion remain open.
PDF processing and historical acquisition remain deferred.

### Source availability and acceptance memory limit 2026-10-01

The first complete evidence/financial attempt stopped before table scans because
a retained reference original was a cloud placeholder whose read timed out.
The [namespace availability receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-source-availability-20261001/verification.json)
records restored local access and exact digest agreement for the required
originals. A subsequent [archive-selection check](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-source-availability-20261001/archive-selection-availability.json)
found further cloud placeholders, with no missing or size-mismatched selected
objects. Their [readback receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-source-availability-20261001/archive-placeholder-verification.json)
confirms restored local access and exact retained hashes. These checks restore
availability of existing evidence; they do not replace archive admission or R2
verification.

The [second full attempt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-retry-20261001/process-result.json)
reached the intercommittee evidence comparison, then stopped at DuckDB's memory
limit while joining the original locator values. It emitted no evidence or
financial success receipt. This resource failure does not establish a data
mismatch. The next checker retains exact comparisons and divides large joins
into exhaustive partitions; a bounded real-data probe will select the partition
size before complete acceptance resumes.

The [isolated replay environment](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr11-replay-runtime-20261001/verification.json)
now has the required Python and source/workbook readers. Its
[runtime identity](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr11-replay-runtime-20261001/environment.json)
pins the interpreter, installed distribution contents and external tools.
Repository dependency files remain unchanged. The initial provisioning attempt
encountered a missing shared cache entry; the successful retry used an independent
temporary cache and retained both logs.

The mixed reference ZIP also became readable and matched its retained digest.
The [selective materialization receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr11-nonpdf-member-materialization-20261001/nonpdf-member-materialization.json)
proves exact extraction of the selected non-PDF reference members, with the
parent held and rehashed before and after extraction. The process exited
successfully and the decoded files passed a separate digest check. It opened
only the selected spreadsheet, Word and RTF members; the parent ZIP remains
preserved and ineligible for deletion. Native semantic replay still awaits the
expanded archive, complete remote readback and fresh restore.

### Partitioned acceptance running and archive preparation ready 2026-10-01

The [revised checker qualification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-evidence-financial-preparation-v3-20261001/completion.json)
passes synthetic probes, including a reproduced memory failure in the old wide
join, exact partitioned comparisons under the same bound, and the actual archive
adapter's JSON receipt checks. Hashes assign every row to a partition; comparisons
inside each partition still use original IDs and complete field values. Separate
JSON detail receipts preserve the results within the downstream control budget.

Real-data readiness probes passed on intercommittee transactions and receipts
for both proposed partition sizes. The larger size reduced repeated scans with
similar time per comparison and stayed within the unchanged memory/spill limits.
The [measured configuration](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-v3-20261001/measured-config-selection.json)
pins those successful bounded probes. They prove readiness, not complete
evidence or financial acceptance. The [full launch](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-v3-20261001/launch.json)
started the complete run with that configuration. Follow its
[live log](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-v3-20261001/full-check.log)
and terminal process/role receipts before claiming completion or creating the
local MCP prior-check closure.

The [expanded archive handoff](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-replay-closure-preparation-20261001/handoff.json)
preserves the original selection and classified occurrences and adds the final
replay dependencies, actual reference-member proof and measured runtime evidence.
Existing owner metadata checks pass. Capacity includes separate staged and
restored copies, failure recovery, PostgreSQL scratch and replay metadata; the
measured free space fits that budget. Root independently loaded the final job
through the existing adapter and confirmed its canonical selection digest.
Actual source scanning, archive admission and staging still await successful
FR08 evidence, financial and local MCP receipts.

### Required source-parent refusal gap 2026-10-01

The full partitioned checker completed exact primary-evidence comparisons for
the intercommittee population, clearing the comparison that exhausted memory
in the previous attempt. Its [live log](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-v3-20261001/full-check.log)
records continued processing; this checkpoint does not establish terminal
evidence or financial acceptance.

The [consumer review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-final-release-preparation-20261001/parent-scope-review.json)
found that the actual view registry remained compatible after the required
`fec-observations` parent advanced or was removed from the captured index.
Existing checks compare SQL-table dependencies. These evidence routes emit or
filter an immutable source-generation pin without reading parent tables, so
their required evidence parent needs a separate application-declared check.
This finding leaves the explicit FR13 parent-advance requirement open; changing
only the typed-table family does not test that requirement.

Prepare the correction separately while the full checker holds its pinned
implementation files. After that process closes, integrate the correction,
verify unchanged SQL/data interpretation, and refresh local MCP acceptance to
cover the matching, missing, advanced and restored parent states. Preserve
earlier receipts at their actual scope. A change to generic publication policy
is not required by this finding.

A [bounded authenticated R2 check](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-required-parent-presence-20261001/receipt.json)
also found the required source-generation and associated evidence artifact roots
absent remotely. The current published source family remains the earlier
selection. Reuse and publish the exact already sealed local source generation;
resealing it would change the pin carried by typed evidence. This read checked
metadata presence only and made no remote changes.

### Parent check reviewed and retained identity delivery reconciled 2026-10-01

The [isolated parent-check handoff](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-parent-pin-fix-preparation-20261001/HANDOFF.md)
and its focused checks qualify application-declared evidence parents separately
from SQL dependencies. Each qualified view requires agreement between its
application declaration, release receipt and captured source-family pin, plus
recovery retention. Missing, advanced or malformed parents disable affected
views; existing captured connections, unrelated queries and rollback retain
their declared behavior.

The [root review](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-parent-pin-fix-preparation-20261001/root-review.json)
rechecked the actual live/isolated file hashes and patch applicability and
independently compared every before/after view definition. SQL bytes, required
tables/columns, meanings, population/as-of and non-factory interpretation pins
match exactly. The intended changes are the explicit evidence-parent declaration
and factory file digest. The patch remains unapplied while the full checker holds
the original implementation. Successful original value-check receipts may be
paired with this preservation proof; they do not replace fresh actual MCP
acceptance against the corrected consumer.

The [full inventory](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/inventory-20261001/inventory-workbook-verification.json)
also distinguishes useful retained outputs outside the new typed composition.
Candidate history already has a qualified sealed generation, but the captured
public index has no candidate-history family. The retained source-catalog member
also differs from the published member and requires explicit reconciliation.
FR12/FR13 must account for both through their existing publication owners.
Preserve the accepted typed composition and current checker inputs; this is
delivery closure for retained data, not historical acquisition.

The [fresh identity verification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-retained-identity-preparation-20261001/verification.json)
passes the existing generation/evidence owners, member hashes, decoded pages,
schemas, rows and query-bundle aliases for the sealed candidate and catalog
outputs. Candidate identities and cycle counts pass, and the earlier independent
whole-cell proof names the same unchanged member bytes. Catalog contents match
the retained provider declarations and pagination modes; different published
bytes alone are not claimed as a semantic change.

The [archive closure check](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-retained-identity-preparation-20261001/closure.json)
finds the original candidate-master payloads already selected, but additional
generation/evidence metadata, the candidate replay manifest/journal and small
query members are needed to recover these exact existing artifacts. Refresh the
final archive selection with the reviewed additive preservation list, final
consumer code and actual MCP evidence before admission. The closure inventory
also includes inspected proof/runtime references; it is not itself the minimum
addition list or a deletion manifest. No generation was resealed or published.

### Inventory publication comparison and identity acceptance preparation 2026-10-01

The [authenticated catalog comparison](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-source-catalog-comparison-20261001/comparison.json)
read the current stored index and small published catalog member, verified its
declared byte size and SHA-256, and compared every row and column with the
retained member. Only `provider_version` and `catalogued_at` differ. Source
families, routes, reference definitions, catalog digest and coverage text agree.
A retained-catalog publication therefore changes provenance, without adding
source coverage. The current index still lacks the candidate-history and typed
query families. This check made no remote changes.

The [additive MCP preparation](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-parent-bound-mcp-preparation-20261001/identity-revision/HANDOFF.md)
includes candidate history and the retained catalog through their separate
family owners. Its preservation proof keeps the accepted typed composition,
view definitions and actual source-parent descriptors unchanged. Synthetic
checks and metadata-only preparation pass; no actual payloads were linked or
real MCP role emitted. After the full checker closes and the parent correction
is integrated, prepare a fresh candidate from the installed runtime and execute
it against real successful prior-role receipts.

The [clean-restore supplement](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr12-retained-identity-preparation-20261001/RESTORED-PATHS.md)
provides existing-owner verification and the independent candidate comparison
using only restored paths. Preserve its pinned support scripts with the additive
archive selection. The commands retain the original manifest and make explicit
path-only substitutions in fresh scratch copies of the historical oracle.
Preparation checks do not establish clean-restore execution; that remains FR11.

### Complete local validation 2026-10-01

**FR08 is complete for the selected retained non-PDF local release.** The
[combined receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/fr08-complete.json) has SHA-256
`3be3b5685972f4ba7b95ab70d3a164e95c9428364a886f5fa1ce810ab768a8c5`.
The [existing-owner gate check](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/gate-verification.json)
accepts its source scope, exact table composition and all four role/evidence
pins. This is the local admission prerequisite, not full-source archive
admission, upload, restore or deployed acceptance.

| Measured check | Result and evidence |
| --- | --- |
| Complete typed composition and keys | The unchanged selected bundle contains 54 tables, 180 members and 49,357,527 physical rows. Existing exact composition and complete key receipts remain pinned in the combined receipt. |
| Complete evidence and financial checks | The [terminal run](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-evidence-financial-v3-20261001/process-result.json) exited successfully after 12,932 seconds. It completed all 46 derived evidence/association views, both stored evidence tables and 26 financial decision views. The independent Python comparison covered 363 representative decisions across every observed interpretation and source class. Peak RSS was 2,471,264,256 bytes and peak spill 2,276,392,960 bytes, within the declared bounds. |
| Applied source-parent correction | [Integration proof](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/parent-patch-integration.json) binds the exact reviewed patch. Every SQL digest in the completed value checks matches both the preserved view definitions and the fresh candidate release. Original value receipts retain their original code pins. |
| Product checks | [Full baseline unit suite](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/unit-tests-before-parent-patch.json): 4,553 passed, two skipped and 14 live integration tests deselected. [Post-patch checks](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/post-patch-checks.json): 133 focused tests passed; Ruff, type checks, dictionary validation, generated-page comparison and diff checks passed. The full suite preceded the parent patch; its affected release/view/MCP paths were rerun afterward. |
| Actual local MCP | [Successful role](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-parent-bound-mcp-acceptance-v2-20261001/output/local-mcp-role.json): all 56 selected tables described, all 72 qualified views bound, 15 representative row queries exercised, all 182 selected members verified by the existing owner. Candidate history has 130,562 unique candidate/cycle keys and the catalog has 26 distinct families. Source-parent payload links remain zero. |
| Actual compatibility scenarios | [Scenario receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-final-parent-bound-mcp-acceptance-v2-20261001/output/compatibility-scenarios.json): missing receipt, absent/advanced source parent, advanced typed dependency, policy and image mismatch all refuse affected qualified queries; raw queries remain usable, captured connections retain their pins and exact local rollback succeeds. |

The first MCP launch counted shared-parent outputs against its task budget and
stopped before opening data. The isolated retry reached the 512 MiB synthetic
RSS cap. Both failures remain in their original receipts. The
[revised runner](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-parent-bound-mcp-preparation-20261001/identity-revision-v2/verification.json)
retains every prior correctness check, adds explicit actual absent-parent
connection/refresh scenarios and progress logging, and raises only the process
RSS cap to 1 GiB. Its 27 synthetic checks pass; DuckDB remains limited to 160 MiB,
one thread, no spill and 60-second statements. The successful real run used
640,516,096 bytes peak RSS and completed in 55.93 seconds. Linked Parquet files
share existing storage; no source bytes were deleted or reclaimed.

Current/net totals, transaction deduplication, amendment replacement, cross-row
spending aggregation and transfer pairing remain unqualified. Quality notices
do not automatically exclude observations. Candidate image/archive/typed
identity descriptors remain explicit local test identities. FR09 must refresh
its selection with final code, validation controls and separate identity-family
recovery inputs; FR10–FR13 still require remote SHA verification, clean restore
and semantic replay, final seals/image and public consumer acceptance. No push,
remote mutation, deployment or source deletion occurred in this checkpoint.

The user's PDF inventory question was answered with a
[metadata-only presence refresh](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr08-validation-finish-20261001/pdf-presence-refresh.json):
777 locally present paths, 102 cloud placeholders and 10 recorded archive-member
references. These include duplicates, test/derived pages and partial captures;
unique official documents were not counted. No PDF bodies were opened by that
refresh, and corpus PDF processing remains deferred.

### Complete archive admission 2026-10-01

**FR09 passed full local admission for the final non-PDF archive.** The
[admission receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/source-evidence/e14ed13a37104bae99f5b34ef4bc9db9/archive-admission.json)
binds 15,681 distinct objects totaling 3,869,779,618 bytes. Its sealed evidence
artifact is `sha256:a287b2727bc56a7db652f40ab4b82733725c422f84d82d364533d299aa48adbf`;
the exact selected-object manifest is
`sha256:a668232d0cc7089f5133fdbd5b0d1841cffde73ba3ff0379153dfb7594f68b8e`.
The [final preparation](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/candidate-v3/verification.json)
records the replay recipe, actual FR08 gate, capacity reservation and source
scope. Final code and validation receipts, the existing candidate-history and
catalog seals, and their recovery dependencies are included. Previous source
objects and preparation records remain preserved. The admitted recipe restores
qualified consumer code at active paths and older code under historical paths.

The first admission attempt correctly stopped on credential-like Python names
in replay support code. The [exact support classification](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/support-classification-v3.json)
qualifies 17 matches in 15 pinned files as Python identifier expressions, using
Python parsing and token positions. It grants no allowance to string literals,
comments, numeric literals or other files. No configured-secret value matched.
The [adapter boundary checks](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/support-boundary-tests/verification.json)
verify exact selection/disposition binding, count-drift refusal and continued
configured-secret refusal. Existing source-browser classifications retain their
exact object hashes, sizes and occurrence counts. Full admission then scanned,
hashed, copied, sealed and verified every selected object successfully.

Commits `cccda24`, `91dd82b` and `7df589a` save the validated implementation,
deployment configuration and local validation records. R2 upload began through
the existing immutable evidence publisher; its [initial progress receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/upload/progress.json)
preserves that phase. The next checkpoint records completed public readback;
fresh restore and semantic replay remain pending. The archive does not activate the typed
tables or deployed consumer. PDF processing and source deletion remain deferred.

### Verified R2 archive upload 2026-10-01

**FR10 is complete for the admitted non-PDF archive.** The
[completion receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/completion.json) binds the local admission,
implementation commits, R2 destination and complete public readback. The
`spicy-regs` bucket holds 15,681 selected digest objects totaling
3,869,779,618 bytes, plus the archive's controls. Its
[public artifact root](https://data.spicygov.ai/source-evidence/a287b2727bc56a7db652f40ab4b82733725c422f84d82d364533d299aa48adbf/artifact.json)
binds `sha256:a287b2727bc56a7db652f40ab4b82733725c422f84d82d364533d299aa48adbf`.

The existing publisher completed every immutable object write. After transfer,
its serial readback was deliberately stopped and replaced by the
[qualified four-stream public reader](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/parallel-readback-v2-qualification.json).
The [switch receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/readback-switch.json) preserves the exact prior
progress and reason; the interrupted serial run is not claimed as completed.
Fourteen hermetic checks cover full coverage, same-size/same-ETag corruption,
missing data, selection mismatch, changed remote controls, local drift,
interrupted responses, bounded concurrency, transport retries and exact receipt
resumption. Basic lint checks pass. The first public attempt stopped on an HTTP
connection interruption; its completed per-object SHA receipts remain pinned.
The successful pass reused only exact complete records and re-read every
remaining object. Partial reads never qualify for reuse.

The [full public receipt](/Users/mikewolfd/Work/corpora/fec-corpus-completion-20260930/retained-delivery-20260930/fr09-final-admission-20261001/public-readback-v2/verification.json)
confirms fresh anonymous GETs, complete SHA-256 and size verification for every
selected blob, including reused objects. The existing artifact owner then
verified the remote control bytes and exact artifact identity against the
byte-identical admitted local blobs. This proves remote byte membership and
binding; it creates no payload scratch and does not substitute for a fresh
relocated source replay. The resumed public pass completed in
481.51 seconds with 227,033,088 bytes peak RSS.

No publication pointer was written by this operation, no typed query family
was activated, and no consumer deployment was performed. FR11 fresh restore
and semantic replay are next; final table/image publication and public consumer
acceptance remain FR12–FR13. Source files and deferred PDFs remain preserved.
The Git commits are local; this operation performed no Git push.
