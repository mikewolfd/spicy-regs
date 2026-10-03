# Congressional scorecard source survey

This is the initial evidence baseline for the [integration plan](../../scorecard-integration.md).
The sampled source model is frozen as `scorecards-v1`; production ingestion
still requires adapter and publication qualification. The accepted
[freeze receipt](work/gate/freeze_receipt.json) pins the evidence and checks.
The generated [coverage summary](coverage_summary.json) records the dated counts
and exact input hashes. The census remains open.

Start with the [census](catalog_report.md), [shape matrix](shape_matrix.md),
and [sample dispositions](schema_fit.md). The [schema-breaker register](schema_breakers.json)
records every requested hunt, including negative weights, which this pass did
not establish. Signed point contributions are a different observation.

The [component reuse review](reuse_review.md) records inspected repositories,
APIs, licenses, integration decisions, a repeatable review method and the LCV
end-to-end qualification target. The [adapter task manifest](work/integration/adapter_tasks.json)
keeps every requested census candidate in scope; task creation does not imply
source verification or production support.

## What the files establish

| File | Meaning |
| --- | --- |
| [scorecard_source_catalog.json](scorecard_source_catalog.json) | One publisher/series candidate. Includes historical discovery leads that still need original-publisher verification. |
| [capture_receipts.json](capture_receipts.json) | Requested/resolved URL, response status, time, type, size and SHA-256 of bounded research responses; errors remain visible. |
| [shape_profiles.json](shape_profiles.json) | Named edition or methodology samples, literal examples, locators, observed features and limits. Multiple profiles may belong to one publisher. |
| [discovery_log.json](discovery_log.json) | Discovery inputs, merged aliases, exclusions and remaining searches. |
| [proposed_schema.json](proposed_schema.json) | Frozen V1 row meanings, keys, fields and invariants for implementation. Registration in SpicyDocs is a separate task. |
| [schema_examples.json](schema_examples.json) | Bounded logical row fragments linked to literal observations, plus explicit dispositions for historical grids without checked member cells. Research-local keys are not qualified production IDs. |
| [validation_receipt.json](validation_receipt.json) | Local revisions, input hashes, checks and remaining validation limits. |

`discovered` means a lead exists. `verified` means an original publisher response
identifies the federal scorecard series; it can still be a landing page.
`profiled` means a named sample has a recorded shape and disposition; the profile
depth distinguishes a member table from methodology alone. `blocked` records an
access problem. `supported` requires future adapter qualification. A missing page
does not establish that a publisher has retired.

`source_id` identifies a series independently of `publisher_id`. The current
census happens to retain one series candidate per publisher identity. Publisher
aliases prevent rebrands from inflating coverage. Joint publishers remain one
series; attribution must still name the participants. Candidate-only guides
remain visible with a scope warning and need a federal-incumbent filter before
admission. Discovery labels for unverified historical scorecards are descriptive,
not asserted original titles.

Year extrema and `congresses_available` record the years or Congresses witnessed
in a sampled index, edition or explicit publisher history. They do not imply
continuous coverage or successful acquisition of every listed edition. Missing
values mean unknown. `machine_readable` distinguishes an observed HTML table,
an offered export and an acquired export. The expanded sample pass acquired
LCV's CSV and AFL-CIO's XLSX exports and checked their disclosed scopes. See the
[sample handoff](work/samples/README.md) for measured counts and limitations.

## Evidence and reproducibility

Research used explicit public GETs, bounded to 20 MiB per response with a
20-second network timeout, a 45-second streaming budget and at most five redirects.
The response receipt states the exact limits and hash semantics. These are
research safety bounds, not recommended production limits. HTTP response EOF
does not establish complete scorecard membership. Every research receipt records
`source_snapshot_complete: false`.

Only hash receipts, brief literal examples and original observations belong in
this directory. Source HTML, PDFs, extracted full text and rendered pages were
temporary local inspection material and are not included. Redistribution rights
remain unreviewed. Hash-only evidence can identify bytes but cannot provide
offline replay if those bytes later disappear. Do not describe it as a replayable
source-native release. Public byte retention requires an explicit rights decision.

PDF text was read with PyMuPDF and checked against rendered sample pages for
C4IP, HRC and Humane World Action Fund. Sample locators distinguish physical PDF
pages from printed pages. This is shape inspection, not whole-document parser
qualification. For a new observation, fetch the receipt's `requested_url`, write
a new receipt rather than overwriting the old one, inspect the response shape,
and bind new profile claims to that receipt. Search snippets never establish a
complete original source.

Generate and verify the reports from the SpicyRegs directory:

```sh
uv run --frozen python scripts/build_scorecard_survey.py
uv run --frozen python scripts/build_scorecard_survey.py --check
```

The check rejects duplicate identities, dangling evidence references, failed
profile inputs, unexplained unsupported dispositions, changed literal examples,
orphan example rows and production-support claims. Acquisition readiness and
score-reproduction readiness remain separate from source-model fit. It validates research
bookkeeping; it cannot prove that a human read a
publisher correctly or that a model can reproduce a score. The raw source bytes
are deliberately absent, so original observations require a fresh capture for
independent source review.

## Coverage and the production gate

Every sampled profile has a fit or an explicit unsupported disposition. The
initial census, captures, shape matrix, schema-breaker analysis and proposed
model form a reviewable first-milestone packet for that sampled set. Bounded row
examples preserve observed cells and exercise proposed relationships; they do
not establish full scorecard completeness or validate every proposed table.

The census includes unprofiled candidates, and some profiles cover only
methodology. The expanded [census packet](work/census/) records the Vote Smart
directory and historical CRS enumeration separately from original-publisher
verification. A directory entry does not establish a federal scorecard.

The [schema review](work/operations/schema_review.md) found metric-free item
grids and cross-snapshot relationships that needed explicit rules. The revised
proposal addresses them, selects VARCHAR physical columns and preserves ordered
period/reference occurrences. Standalone numeric member adjustments remain
deferred because no qualified ledger fixture was observed. The
[gate decisions](work/gate/schema_decisions.json) record acceptance separately
from source acquisition and production support. The
[independent closure](work/operations/schema_closure.md) closes the schema
findings, with the unproved AFL-CIO column crosswalk explicitly deferred.
**The sampled schema is frozen; no source has production support yet.**
