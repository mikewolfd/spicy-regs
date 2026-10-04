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

Boolean flags retain unknown values as null. Exact numeric text becomes
`DECIMAL(38,18)`; a value outside that bound refuses instead of rounding. Publisher
display text remains independently available. Grade letters never become numbers.
The retained-row census and exact-roundtrip checks are in
`/Users/mikewolfd/Work/corpora/spicy-regs-etl-split-20261003/scorecards-report.json`.

Snapshot records describe capture, parsing, completeness and evidence policy.
They belong entirely in receipts. Resolver receipts retain input pins, candidate
identities, ambiguity, refusal reasons, source context and rule versions. Only
resolved links belong in subject tables. Internal validation and incremental reads
must reconstruct the provider rows from verified receipts; missing or ambiguous
receipt joins must stop the read.

See `src/spicy_regs/etl_policies/scorecard*.json` for installed receipt requirements.
Both rollups use the shared `receipt_policies` path and its build identity.
Unchanged prior observations retain their original witnesses and attempt through
the shared `rebind_receipt` helper. New attempts remain specific to the new build;
earlier failures remain available in their retained immutable generations.
Official inputs with receipts use `CongressInput.materialize` before resolution.
The current congressional reader accepts a complete single-file table; native
partitioned inputs refuse until its owner supplies a complete-table reader.

The resolver's exact-match and conflict rules remain authoritative. Moving
snapshot identifiers requires query integration to check that an analysis used the
currently selected scorecard generation before linking ratings to official people
or legislation. A stable publisher key alone does not establish a current link.
