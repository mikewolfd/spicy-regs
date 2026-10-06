# Scorecard domain rows and processing evidence

Publisher facts retain their stated meaning: ratings, grades, weights, eligibility,
methodology, disclosure, action text and result text. The source adapters still
return the frozen SpicyDocs acquisition shape for whole-edition validation.
SpicyRegs then maps that validated input into subject rows and shared ETL receipts.

See `src/spicy_regs/scorecards/subject_shapes.py` for the explicit classification
of every input field. An added provider field requires a classification before a
build can proceed. Aliases, identifiers, periods and item references use native
ordered lists. Repeated values stay repeated; null and an empty list remain
distinct. Reference and period locators remain in receipt evidence, alongside the
exact original JSON, so absent object properties remain distinguishable from
explicit null properties during replay.

Boolean flags retain unknown values as null. Under `scorecards-etl-ratings-v3`,
exact rating numbers retain validated decimal text in `VARCHAR`. Original
publisher JSON exceeds the earlier fixed decimal precision; increasing Arrow's
precision did not preserve those values in DuckDB. Weights and contributions
retain `DECIMAL(38,18)` and refuse values outside that bound. Publisher display
text remains independently available. Grade letters never become numbers.
See [exact rating numbers](research/scorecards/work/integration/rating_numbers.md)
for source evidence, SQL behavior and the supported local migration path.
The retained-row census and exact-roundtrip checks are in
`/Users/mikewolfd/Work/corpora/spicy-regs-etl-split-20261003/scorecards-report.json`.
The additional ILA precision census and exact Arrow/DuckDB comparisons are in
`receipts/scorecards-expansion-20261004/etl-conversion-review/diagnosis.json`.
Historical rating generations retain their original `DECIMAL(38,18)` or
`DECIMAL(38,19)` scale and their exact frozen receipt policy.
Reads admit only the exact known historical or current declarations and verify
the original schema, subject hashes and receipt joins before restoring source
strings. A subsequent build generates current rating subjects from those exact source
strings. An unchanged NULL rating can retain its original receipt under its
explicitly admitted historical policy. A changed numeric subject gets a current
receipt naming the direct predecessor; old receipt hashes are never reinterpreted.

A refresh compares the completed family through the shared
`carry_receipt_history` helper. Unchanged accepted rows retain their complete
original receipts, including their original generation and any historical notes.
Changed accepted rows retain only a direct predecessor reference alongside their
current source evidence. Repeated failed attempts match in source order when
both their processing values and diagnostics agree. The helper preserves current
row order and count; it does not trim historical files. Conversion refusal skips
prior carry and leaves the prior family untouched.

Snapshot records describe capture, parsing, completeness and evidence policy.
They belong entirely in receipts. Resolver receipts retain input pins, candidate
identities, ambiguity, refusal reasons, source context and rule versions. Only
resolved links belong in subject tables. Internal validation and incremental reads
must reconstruct the provider rows from verified receipts; missing or ambiguous
receipt joins must stop the read.

`read_family` uses the shared `read_receipt_bundle` API to reconstruct the selected
family with one receipt load and one subject join. The reader checks every receipt,
including failed attempts omitted from its result, and verifies all accepted
subjects before returning any rows. Callers explicitly select processing-only
observations and resolver refusals for internal replay. Exact decimals, nested
values, witnesses, generation identities and one-to-one joins use the same shared
validation rules as individual-dataset reads.

After writing a refresh, `verify_family_readback` uses the shared
`visit_receipt_bundle` reader to compare each persisted row with the validated
source rows already held by the build. Identity indexes reference those rows;
the check does not construct another complete source family. It compares every
literal source and evidence field, accepts reordered rows, and refuses missing,
extra, duplicated or changed rows. Processing-only snapshots receive the same
check. Earlier visits remain provisional until all receipt and subject joins
finish, so preservation callbacks and success events run only after complete
readback admission.

Unselected scopes use the same source identities and exact per-row comparison
for preservation. Prior and merged source rows still scale with corpus size;
these checks remove duplicate source populations and retained full-row JSON
sets rather than establishing a fixed memory bound for the whole build.
Member hashes stream from files, and Parquet verification and receipt selection
use bounded batches. Full-job resource qualification remains a separate gate.

The bounded LCV replay comparison and its original input hashes are retained in
`receipts/scorecards-expansion-20261004/receipt-read-optimization-benchmark/`.
Its baseline and candidate reports agree on the exact reconstructed row digest;
the measurement covers that captured edition rather than full-corpus throughput.

See `src/spicy_regs/etl_policies/scorecard*.json` for installed receipt requirements.
Both rollups use the shared `receipt_policies` path and its build identity.
Unchanged prior observations retain their complete original receipts through
the shared `carry_receipt_history` helper. New attempts remain specific to the new build;
earlier failures remain available in their retained immutable generations.
Official inputs with receipts use `CongressInput.materialize` before resolution.
The reader restores every selected partition under its generation-bound receipts.
Analysis preflight requires matching source generations and complete native receipt
declarations before downloading inputs.

For official tables published before native receipts, the preparation script
`docs/research/scorecards/work/integration/deployment/prepare_analysis.py` supports
the explicit `--convert-published-official-inputs` option. It verifies every pinned
published object, converts the complete shaped table through
`write_congress_dataset`, and verifies exact restoration of all original fields,
ordered rows, schema and footer metadata. The original table pins remain analysis
parents. Witnesses identify published shaped observations; they do not assert an
original congressional HTTP acquisition. Refused values stop preparation and
retain their attempts. Existing native inputs use their selected receipts, and
source scorecards always require their own receipts. The normal analysis rollup
does not enable this conversion implicitly.
Every original footer key/value must survive unchanged. Arrow may add its
generated `ARROW:schema` entry after exact decoded-schema verification; the proof
reports that addition separately as `generated_footer_keys_added`. Its
`exact_schema_and_authored_footer` field establishes retained source metadata,
without asserting byte-identical physical Parquet storage.

The resolver's exact-match and conflict rules remain authoritative. Moving
snapshot identifiers requires query integration to check that an analysis used the
currently selected scorecard generation before linking ratings to official people
or legislation. A stable publisher key alone does not establish a current link.

## Qualified live refresh

The source registry enables only readers qualified for unattended live acquisition.
See [the retained qualification](research/scorecards/work/integration/live_refresh_20261004.json)
for the named LCV and AFSCME editions, exact installed reader hashes, completeness
checks, source counts and private capture pins. Those readers require no private
PDF extraction assets. Other readers remain available for explicitly qualified
retained-input builds; registry enablement and published historical coverage are
separate decisions.

The scorecard workflow runs weekly and follows each enabled source's cadence.
Manual dispatch defaults to a local candidate; choosing publication still requires
qualified enabled sources. Complete reads replace only their named editions.
Failed or incomplete reads preserve prior rows and retain permitted hash-only
evidence. The workflow serializes its own runs; publication also checks the
selected source generation so a concurrent publisher cannot silently overwrite it.
