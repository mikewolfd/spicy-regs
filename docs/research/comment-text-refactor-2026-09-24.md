# Comment text: concurrent reads and independent retries

The ETL and derived-text backfill now use one bounded comment worker pool.
A large docket can use every worker. All agencies in a run share the same
budget: `--text-workers` defaults to eight and is capped at sixteen. Backfill
uses its existing `--max-workers` option for the same pool.

Each worker owns its S3 resource. Resources are created serially before worker
startup, and a shared cache lists each docket once. SpicyDocs still owns source
selection, one-tool policy, numeric attachment order, conditional ETag reads,
size checks and byte hashes. The pool preserves record order and limits pending
results to twice its worker count, including completed results awaiting staging.
Staging also streams Parquet row groups, flushing at 4,096 rows or eight MiB of
string characters plus the current row. A complete source read atomically
replaces the agency staging file; interrupted reads expose no partial staging.

## Failure and restart behavior

`pending_comment_text.parquet` retains failed text reads independently of the raw
source manifest. Each failure records comment coordinates, failure phase,
reason, selected source metadata when available, processing-rule version and
attempt history. A fresh run checks the persisted comment and retries just its
text. It updates only text, status and provenance, preserving other fields and
completed PDF results. A valid blank or missing extraction clears the failure;
authentication refusal still aborts the run.

The retry checkpoint commits after the data merge and publishes before the raw
manifest. Empty retry state also publishes, so a fresh hosted runner cannot
resurrect resolved failures. Both normal and chunked ingestion follow this order.

Local catch-up can reuse one loaded `Manifest` across batches through
`RegulationsPipeline.run(manifest=manifest)`. Saving clears pending additions
while retaining membership, so later batches neither rebuild the Bloom filter
nor append the same newly committed keys again. Checkpoint files still commit
after every batch. Separate hosted jobs each load their own manifest.

Logs report text progress every thirty seconds, including derived, missing,
failed and skipped rows, and elapsed time for manifest loading, staging, merge
and publication. These distinguish text work from raw metadata download.

## Measured comparison

The predeclared comparison selected the first sixty-four comment identities in
`CFPB-2011-0002`. Each arm ran twice with fresh resources and listing caches;
the second pass reversed arm order. All outputs, statuses and provenance were
identical, with no failed reads. Timing includes resource creation and listing.

| Arm | Median seconds | Observed range, seconds |
| --- | ---: | ---: |
| Previous serial loop | 3.970 | 3.532–4.409 |
| Shared pool, one worker | 3.510 | 3.185–3.834 |
| Shared pool, eight workers | 0.620 | 0.585–0.655 |
| Shared pool, sixteen workers | 0.579 | 0.473–0.685 |

Eight workers improved this text-fetch sample by about 6.4 times. Sixteen did
not meet the predeclared additional twenty-percent improvement threshold, so
eight remains the default. This selected docket does not establish a whole-ETL
speedup or performance across the corpus.

Receipts live under
`/Users/mikewolfd/Work/corpora/fork-execution-2026-09-21/comment-text-refactor-2026-09-24/`:
`comparison.md`, `selection.json`, `benchmark.py`, `timings.json`, per-arm
outputs and validation logs. The unit suite checks shared worker limits,
resource ownership, bounded queues, source equality, fresh-host retries,
authentication refusal, failed writes and repeated manifest commits.

## Adoption

The local catch-up handoff uses a separate pinned checkout and preserves completed
batch receipts. The running batch finishes before the next batch adopts this
implementation. The operator's `handoff.json` and `progress.json` in
`etl-local-catchup-2026-09-24/` record the transition and actual running revision.
The public comment mirror still requires completion of catch-up and the existing
publication/readback gates. Local validation does not establish hosted CI or
public data freshness for this refactor.
