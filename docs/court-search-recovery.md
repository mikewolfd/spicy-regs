# Retained court Search catch-up

The existing `run-rollup-court-opinion-clusters catch-up` command adds a bounded
Search selection to an explicitly qualified **same-edition** native parent.
Use the default command for a new bulk edition or historical corrections.
The edition evidence must establish the selected parent's edition and the courts
original's edition; a filename or maximum cluster ID does not establish this.

This local route requires the accepted paired SpicyDocs source at `736855a3` (or
a later release containing its authenticated replay and status-selection fixes).
The currently vendored `spicy-docs==0.66.3` wheel alone is insufficient. Use the
existing immutable environment with both source paths explicitly selected;
`python -m spicy_regs.pipelines.rollups.court_opinion_clusters` invokes the same
app as the installed console script. Do not change a runtime serving another job.
For paired source execution, prefix that module invocation with
`PYTHONPATH=/accepted/docs/src:/accepted/regs/src uv run --frozen --no-sync --offline`.

```sh
run-rollup-court-opinion-clusters catch-up \
  --output-dir /owned/run \
  --prior-directory /retained/exact-native-generation \
  --expected-prior sha256:EXACT_CURRENT_COURT_GENERATION \
  --source-edition YYYY-MM-DD \
  --source-edition-evidence /retained/qualified-edition-evidence.json \
  --source-edition-sha256 sha256:EXACT_EVIDENCE_HASH \
  --courts-local-file /retained/same-edition-courts.csv \
  --courts-sha256 sha256:EXACT_COURTS_HASH \
  --search-through UPPER_CLUSTER_ID \
  --retained-search-directory /retained/this-selected-search
```

Set `R2_PUBLIC_URL` to the intended publication. Live Search dispatch also
requires `COURTLISTENER_API_TOKEN`. Upload remains off by default; selecting
`--no-skip-upload` uses ordinary complete admission and conditional publication.
`--max-requests` and `--max-wait-seconds` bound each traversal, including replay
attempts and live pacing/retry waits. They do not estimate a token's free quota.
Replayed prefix pages count toward `--max-requests` (default 50). A longer
retained walk needs an explicit sufficient allocation; resuming does not make
the traversal unlimited.

After an interruption, invoke the same command with the same selection and
retained directory. An exclusive lock protects the checkpoint. Each successful
page is retained before its rows leave the reader. Resume verifies those bytes,
replays their original observation times through the original count/identity
checks, then requests only the pending cursor. A saved `Retry-After` deadline
refuses live dispatch until it elapses; the command does not schedule itself.
Changed parent, edition/input pins, query/status selection, or missing/corrupt
captures refuse. Keep failed attempts and the retained search directory.

Only complete source traversal reaches delta conversion and prior append.
The native append preserves prior values/receipts and refuses overlapping IDs;
full generation admission and publication checks remain unchanged. Prior-file
hashing, the ceiling projection, full append/receipt output and admission still
cost time and disk. This change saves repeated successful HTTP acquisitions and
removes task-specific continuation wrappers; it does not eliminate those costs.
Search completion proves the selected indexed, all-status walk, not exhaustive
bulk coverage, historical corrections, or citation endpoint membership.

## Optional receipt members

`--split-receipts` keeps the prior receipt files and one new delta file as an
ordered selection. It is off by default. Enable it for publication only after
the selected reader supports split court clusters; a split-votes deployment
alone does not establish court compatibility. No other court family opts in.
The next catch-up and the ordinary full refresh can read the complete selection.
Receipt lookup keeps its aggregate scan bound and does not gain an index.

The first monolith-to-split transition changes receipt keys and uploads the
history. Later unchanged members retain their keys and byte pins, allowing the
normal publisher to copy them within the store instead of uploading them again.
Full native-subject rewriting, admission and remote byte readback still run.
The self-contained private bundle copies each receipt member, and generation
assembly copies it again: unlike monolith adoption, this route may use more
local copy space. It avoids receipt recombination and repeated uploads, not
all local copies or historical reads. Keep the normal disk reserve and record
local copies, uploads and readback separately on the next required run.
