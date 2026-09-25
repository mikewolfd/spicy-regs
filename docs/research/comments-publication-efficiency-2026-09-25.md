# Comments publication efficiency — September 25, 2026

**Implemented locally; hosted qualification remains open.** Iceberg remains the
write authority. The publisher builds agency files first, sorts each once for
docket reads, and streams them into the compatible monolith. It skips a verified
unchanged catalog snapshot and builds the comments index once at publication.
The scheduled sweep reuses one processed-key manifest.

This addresses the recurring cost without changing source ownership, dropping
fields or weakening the checks that protect the published population. A fully
incremental public dataset needs a separate consumer change: a fresh monolithic
file necessarily retains full-output work.

## Implementation and operation

[`pipelines/comments_mirror.py`](../../src/spicy_regs/pipelines/comments_mirror.py)
owns validation and publication; the CLI delegates to it. Catalog reads pin the
table UUID, snapshot ID and schema ID, including Iceberg deletes. One native
partitioned write produces agency staging. Each agency sorts by docket, posted
date and comment ID; the monolith streams those files in the original column
order. Connections close between phases and between agency sorts. Every phase
uses [`ExportResources`](../../src/spicy_regs/duckdb_settings.py).

The publisher retains exact IDs and group-count checks. If an agency loses its
last row, an empty file replaces its old public URL. This prevents an agency
move from leaving stale rows available at the old path. The completed receipt
includes these empty replacements in later object-version checks.

`comments-build.json` records a verified local build. After uploads, bounded
public reads compare every file's SHA-256 and size with local bytes and check its
storage ETag before and after readback. Only then does `comments-publication.json`
record the completed source version, exporter version, object versions, digests
and row counts. Matching receipts require both storage and public HEAD checks;
missing files or changed versions trigger a rebuild. Transport errors fail the
run. `--force` also permits rebuilding an unreadable receipt. This completion
record does not make the fixed public objects an atomic generation.

Comments ingestion now returns the number of winning rows without a full count
or index rebuild. Empty winners issue no catalog DELETE/INSERT. The existing
latest-modification policy, including its treatment of missing source versions,
is unchanged; matching still reads keys and modification dates rather than all
retained payload columns. Text repairs change snapshot identity independently
of source row counts.

`run-pipeline --sweep --batch-count …` discovers agencies and loads membership
once. Every batch retains its durable raw/text retry checkpoints. A failed batch
stops the sweep: continuing with uncommitted in-memory keys could retire them in
a later batch's manifest. A fresh runner resumes from the published checkpoint.
Manual single-batch CLI runs finalize the mirror after successful ingestion;
the workflow passes `--defer-comments-publication` and owns the one final refresh.
Full refresh remains an explicit single-run operation, separate from a reused
incremental sweep. Scheduled sweeps have a bounded hosted-job timeout.

The Bloom filter now uses a portable bytearray with accurate size accounting.
Its false-positive policy is unchanged. Manifest checkpoint I/O also remains:
saving new keys rewrites the file, and each successful batch publishes the
checkpoint. Reuse removes repeated Python membership construction, not that I/O.

Local qualification and resource measurements are retained in
`~/Work/corpora/fork-execution-2026-09-21/comments-efficient-publisher-2026-09-25/`.
The full unit suite, Ruff, type check and generated data-dictionary checks pass.
The remaining release gate is hosted publication/readback with the writer lock,
followed by the existing consumer refresh. No CI dispatch, public upload or
schedule change was made for this implementation.

### Complete local qualification

The final build read catalog snapshot `9028767677418325402` of table
`01a0d3fc-be18-70b0-abeb-e0f6e58d6cff`, schema `0`, and retained all 26,303,691
comments. Exact prior-ID, unique-ID and every agency/docket/month count check
passed. The public schema and per-agency fingerprints over every column match
the retained published parent. Fingerprints are additional non-cryptographic
evidence, not a new native-source audit.

| Complete catalog-to-validated-local-mirror run | Elapsed | Peak process RSS |
| --- | ---: | ---: |
| Shared connection; 6 GB DuckDB budget, two threads | 333 s | 12.88 GB |
| Separate phase/agency connections; same settings | 313 s | 12.74 GB |
| Adopted defaults: 3 GB budget, one thread | 527 s | 9.62 GB |

These are local sequential measurements, including remote input and predecessor
reads, without public uploads. Closing connections alone did not materially
reduce peak RSS on this host. The lower budget and concurrency trade runtime
for memory headroom; neither setting is a process-memory ceiling. The monolith
assembly-only comparison below has a narrower scope and must not stand in for
these complete-path measurements.

A read-only transport rehearsal checked storage and public versions for the
retained published objects in 5.6 seconds. It used an ephemeral receipt and did
not publish it or adopt the old layout as the new exporter format. A sample
public byte readback also matched its retained SHA-256. Normal no-op operation
still requires a real completion receipt from successful publication.

Sample local date-filter and large-docket queries returned identical results.
Their timings are retained, but cache/order effects and the absent browser
workload prevent treating them as an HTTP range-read performance guarantee.
The final output holds 4.259 GB in the monolith and 4.255 GB in agency files.

Receipts in `comments-efficient-publisher-2026-09-25/`: `qualification.json`,
`output-3GB/comments-build.json`, `layout-verification.json`,
`transport-verification.json`, the earlier resource diagnostics, replay scripts,
validation logs and the retained code patch. The final repository gates pass;
ETL remains manually disabled, verified through the workflow API. Hosted
publication/readback, consumer refresh and browser qualification remain open.

## Evidence and scope

The initial investigation reviewed SpicyRegs at `03541c7`, including the existing local export-resource
patch, and DocSpec at `3133850`. Production code and workflow settings were not
changed during this investigation. Local benchmark artifacts are under
`~/Work/corpora/fork-execution-2026-09-21/comments-efficiency-review-2026-09-25/`.
The completed publication is retained beside them in
`comments-local-export-2026-09-25/`.

The [failed hosted run](https://github.com/mikewolfd/spicy-regs/actions/runs/36083102561)
exhausted the 6 GB DuckDB budget inside `_export_parquet`. Its stack identifies
the full sorted `COPY`, not the later agency pass. The exception alone does not
distinguish every allocation inside that query.

`measurements.json` records the actual retained population and file footers:

- The monolith contains 26,303,691 rows and occupies 4.266 GB. Its encoded
  Parquet data before compression occupies 42.261 GB; this is a storage measure,
  not a direct measure of decoded Arrow or process memory.
- The largest original row group contains 990 MB before compression. A
  100,000-row setting therefore does not mean a small buffer.
- The agency files total 4.272 GB. The current publisher uploads both complete
  representations, about 8.54 GB, when it runs.
- The largest agency by population is FWS, with 2,637,195 rows. Agency-sized work
  is smaller than corpus-sized work but can still be substantial.

The completed local publication's historical timings were 267 seconds for
catalog export plus index, 58 seconds to split agencies, 59 seconds to sort them,
and 271 seconds to upload. These timings come from `export.log`; they are not
separate controlled benchmarks or forecasts for CI.

### Full-corpus local comparison

`experiment.md` records the hypothesis, settings, stop bounds and the amendment
that added the buffer diagnostic after the first two results. Each arm read the
same retained agency files in a fresh process, with DuckDB 1.5.5, two threads,
ZSTD and a 6 GB DuckDB setting. No catalog writes or public uploads occurred.

| Monolith assembly | Copy time | Peak process RSS | Peak spill | Output size |
| --- | ---: | ---: | ---: | ---: |
| A: global agency/modify-date sort; 100,000-row groups | 90.0 s | 19.73 GB | 31.49 GB | 4.266 GB |
| B: stream without global sort; same row-group setting | 43.2 s | 8.84 GB | 0 | 4.259 GB |
| C: stream; 20,000-row and 16 MB row-group targets | 37.6 s | 3.18 GB | 0 | 4.311 GB |

C used 84% less peak process memory than A and produced a file 1.05% larger.
Its exact ID retention, uniqueness, null-coordinate handling and every index
group passed the existing checks. Per-agency fingerprints over all columns also
matched the prior monolith. Those fingerprints are non-cryptographic evidence;
they are not an independent audit of the original source values.

These are single local runs in A/B/C order. Cache, platform and input-layout
effects limit timing comparisons. They test assembling the monolith from
existing agency files, not creating those files from the remote catalog. The
hosted full pipeline remains unqualified. Even C's largest row group was
195 MB before compression: byte settings are targets, not hard ceilings.

Receipts: `A-result.json`, `B-result.json`, `C-result.json`, the corresponding
DuckDB profiles, and `C-verification.json`. DuckDB documents that some allocations
can exceed its [managed memory setting](https://duckdb.org/docs/current/guides/performance/oom)
and provides a [row-group byte target](https://duckdb.org/docs/current/clients/python/relational_api#to_parquet).
Measure process RSS and scratch use as well as the configured budget.

## Current design and its costs

Let `N` be retained comments, `D` changed rows, `K` processed source keys,
`L` objects listed, `B` ingestion batches, `b` Arrow batch rows, `A` agencies,
and `n_a` the population of agency `a`. Let `S` denote payload bytes and `M`
rows in agencies touched by an update. Sorting costs below use the conventional
comparison-sort model; native radix/vector algorithms and string lengths affect
the actual cost. Byte movement and allocation matter alongside row counts.

| Work | Current cost | Proposed bound or limit |
| --- | --- | --- |
| Load processed-key membership | `O(BK)` key hashing across separate jobs | `O(K)` initial load per reused sweep, plus new keys |
| Save the processed-key checkpoint | Full manifest copy/upload on batches adding keys | Preserved initially; reusing memory does not eliminate this separate cost |
| Discover source changes | `O(L)` listing, then membership checks | Still `O(L)` with the present source interface |
| Download and parse unseen source records | Proportional to their bytes, plus pending text retries | Preserve the existing bounded pool and retry path |
| Find catalog upsert matches | Incoming work plus joins against the retained table | Do not claim `O(D)` without measured pruning |
| Rebuild comments index | `O(N)` per nonempty merge call, then again at export | One authoritative `O(N)` recount per successful publication |
| Build all public files | Global sort `O(N log N)`, batch agency sorts `O(N log b)`, and agency sorts `Σ O(n_a log n_a)` | One agency sort each, plus linear copying; worst-case sorting remains `O(N log N)` |
| Assemble compatible monolith | Another globally sorted full representation | `O(S)` streaming write; a full file remains unavoidable |
| Verify IDs and group coverage | Full scans of narrow columns | Keep exact checks; combine reusable scan results where practical |
| Publish an unchanged snapshot | Full build and upload | Metadata comparison and object checks, without payload scans |

Reducing `N log N` to `N` applies to **monolith assembly**, not the entire new
pipeline. Rebuilding changed agencies can reduce sorting to work over `M`, but
the monolith still contributes `O(S)` and global integrity checks can contribute
`O(N)`. A change to one FWS row may still require rewriting its agency file.

### Findings

1. **Reshape the export direction.**
   [`_export_parquet`](../../src/spicy_regs/sources/iceberg.py) sorts the full
   row, including long text. [`partition_comments`](../../src/spicy_regs/transforms/partition_comments.py)
   then sorts agency names within each batch, writes intermediate agency files,
   and sorts/recompresses each by docket and posted date. The latter ordering
   serves an identified consumer need; the preceding global modify-date sort
   does not supply that ordering.
2. **Make resource ownership consistent.** The exporter accepts a budget, while
   the agency sorter independently selects 16 GB and validation selects 4 GB.
   The Arrow splitter uses 500,000 rows irrespective of text width and keeps
   agency writers open together. Configuration alone cannot establish a
   process-memory bound; the measurements demonstrate that distinction.
3. **Move repeated aggregate work to its proper boundary.**
   [`merge_comments`](../../src/spicy_regs/sources/iceberg.py) rebuilds the
   complete index for each staged batch. The
   [refresh workflow](../../.github/workflows/_regulations-refresh.yml) exports
   it again after ingestion. Intermediate public indexes can also get ahead of
   the public comment files they describe.
4. **Preserve incremental state across execution batches.**
   [`Manifest.load`](../../src/spicy_regs/manifest.py) hashes the complete
   manifest in Python in every fresh job. The
   [runbook measurement](../etl-catalog-seed.md)
   records 183 seconds for the earlier seed, or about 46 minutes across the
   scheduled batch matrix. The pipeline already accepts `run(manifest=...)`.
   `array('L')` uses 8-byte items here while the filter allocates and reports
   them as 4-byte words; fix the representation/accounting together.
5. **Do not assume agency predicates produce cheap source reads.** The read-only
   `catalog-layout.json` probe found an empty Iceberg partition specification.
   File statistics may still help, but querying the catalog once for every
   agency could approach `O(AN)` scanning. Metadata also records position deletes;
   readers must use the active Iceberg snapshot, not glob its data files.

## Accepted design

### One bounded publisher

Put publication sequencing in a comments pipeline/module; keep snapshot access
in `sources/iceberg.py`, file shaping in `transforms/partition_comments.py`, checks
in `comments_health.py`, and transport in `sources/r2.py`. Keep the existing CLI
as a thin entry point. Docket export should retain its separate behavior.

The build sequence is:

1. Capture a table UUID, snapshot ID, schema ID and exporter-format version.
   Read that exact Iceberg snapshot. Retain the existing writer lock and dedupe
   recovery refusal. Verify supported snapshot reads with the installed engine.
2. Scan the selected snapshot once into bounded agency staging files. Prefer
   DuckDB's maintained partitioned writer with bounded open files and flush
   settings; prove its resource use before choosing it. A retained Arrow route
   must bound rows, estimated bytes and open writers and close them on failure.
   Do not issue a full catalog scan for each agency.
3. Sort each agency once by `(docket_id, posted_date, comment_id)`, writing its
   final file with explicit row and byte targets. The stable ID breaks ties.
   Preserve null dockets/dates and all columns. Start evaluation with the
   measured 20,000-row/16 MB targets; keep the same resource policy throughout.
4. Stream those final files into `comments.parquet` without another global
   sort, restoring `agency_code` explicitly and preserving the public schema.
   Derive the index once from narrow columns of the selected snapshot/final
   files and reconcile them. Keep identity, uniqueness, coverage and predecessor
   checks. An empty or partial catalog response must still refuse publication.
5. Publish the validated files and index, then a small completion receipt.
   Record snapshot identity, exporter version, output digests, ETags and counts.
   Advance that receipt only after successful readback. Current fixed public
   keys are not an atomic multi-object generation; this receipt must not claim
   otherwise. Keep recovery able to finish a partially uploaded mirror.

Extend the existing `duckdb_settings.py` for shared resource configuration;
do not add a second database abstraction. Use DuckDB/PyArrow's writers and
external sorting rather than implementing a sorter or Parquet concatenator.
The byte target must accommodate whole source values: never truncate a body
to satisfy a buffer target. Test outliers and account for the largest row.

### Skip work only when its inputs are unchanged

Use the successful receipt to compare the source snapshot/schema/exporter
identity and verify recorded object versions. With a matching receipt, skip
mirror scans, sorts and uploads. Missing receipts or changed output versions
force rebuild/recovery. This is metadata-sized work plus `O(A)` object checks,
not a claim that the complete pipeline is constant-time.

A text-only update must change the catalog snapshot and invalidate the receipt.
Do not use row count, the raw-source manifest, or `max(modify_date)` as a proxy:
same-count replacements and attachment repairs can leave those unchanged.
Have `_merge` report actual winning rows and return before catalog DML when
there are none; staged rows are not proof of a changed dataset. Preserve the
provider-specific update checks rather than assuming an empty or successful
statement establishes the expected physical result.
Keep independent health audits. Other consumer jobs retain their own input
requirements; discovery windows also depend on time, so a comments no-op does
not authorize skipping every downstream job.

Move index publication out of individual Iceberg ingestion batches and into
this completed mirror build. Keep per-batch raw/text retry checkpoints durable.
A failed final build must remain retryable even after ingestion keys commit.
Update explicit single-batch/manual invocations to use the same finalization
path; do not silently omit their publication requirement. Continue deriving
counts from actual stored rows while catalog update behavior needs qualification.

### Reuse the existing sweep and clean up the manifest

Add a supported sweep invocation that loads `Manifest` once and passes it to
the existing batch method. Preserve each batch's durable checkpoint and the
all-batches-success barrier. Have the scheduled workflow call that invocation
with an adequate bounded timeout; retain single-batch dispatch for recovery.
If a sweep cannot fit one hosted job, group batches with an explicit bound
instead of introducing a separate persistent membership service.

`Manifest.save` still streams and uploads the complete manifest when new keys
arrive. This proposal removes repeated Python membership construction, not that
checkpoint cost. Measure checkpoint transfer separately before changing its
format; immutable key segments would require explicit recovery and reader
changes, just as a partition inventory does for the comments mirror.

Use a portable byte bitset or an explicitly checked 32-bit array and calculate
actual allocated bytes. Correct the misleading size comments. Preserve the
accepted membership semantics in this change; changing the Bloom filter's
false-positive policy is a separate correctness decision.

Do not skip old docket-year prefixes to speed listing: new comments can arrive
on old dockets. Source change feeds/inventory comparisons belong in SpicyDocs
and require their own completeness evidence.

## What DocSpec already does

The current [DocSpec record-storage design](../../../DocSpec/docs/record-storage.md#snapshot-publication-and-maintenance)
separates logical state publication from a portable export. Its
[`apply_changes`](../../../DocSpec/src/docspec/adapters/storage/records.py)
registers the selected base snapshot, commits changed rows and positional
deletes together, and preserves base files. It sorts incoming rows rather than
resorting retained payloads. SQLite publishes the logical state after file
durability; [exact upsert retries](../../../DocSpec/src/docspec/application/core_ingestion.py)
reuse the published result.

Payload writes therefore track changed content. That does not make every
operation `O(D)`: match joins and physical checks may examine inherited files,
and some membership comparisons, sequence digests, compactions and
[portable exports](../../../DocSpec/docs/result-exports.md) require the full
selected population. DocSpec explicitly distinguishes bounded batches from
bounded total work and from a hard process-RSS ceiling.

Adopt those principles here: immutable source versions, unchanged-file reuse,
byte-aware batching, exact retries, and expensive full exports as an explicit
consumer requirement. SpicyRegs already uses Iceberg, so replacing the table
format does not remove its mirror costs. DocSpec's implemented record adapter
supports local filesystem storage, not a remote R2 profile. Direct adoption
would require that owner to implement and qualify remote storage and would
also require reconciling the public SpicyRegs row schema.

For a genuinely incremental public surface, consumers would read a versioned
inventory of immutable partitions and a small current pointer. Unchanged files
would carry forward; the single-file download would be an explicit export.
That follows DocSpec's separation and enables atomic generation selection.
It is a consumer migration, not a transparent optimization of today's fixed
`comments.parquet` URL. Keep it separate from the compatible publisher proposal.

## Commitments, alternatives and acceptance

| Existing commitment | Proposed treatment | Evidence / owner |
| --- | --- | --- |
| Iceberg owns stored comment rows | Preserve; read a pinned snapshot, including deletes | `sources/iceberg.py`; DocSpec record storage |
| UI scoped reads use agency files ordered for docket pruning | Preserve ordering and small row groups | `partition_comments.py`; generated comments documentation |
| CLI and anonymous readers can use the monolith | Preserve current file/schema; verify query effects of new physical order | `mcp_server.py`, `docs/tables/comments.md` |
| Public IDs survive and counts agree | Keep exact checks before publication and readback after | `comments_health.py`, publisher and freshness script |
| Retries do not retire unfinished work | Preserve per-batch checkpoints and final publication barrier | `pipelines/regulations.py`, `_regulations-refresh.yml` |
| Source parsing stays in SpicyDocs | Preserve; no new mirror-specific source reader | `pipelines/regulations.py`, Mirrulations reader |

The parent decision is the dual catalog/public-reader model described in
`sources/iceberg.py` and the [fork-generation dependencies](../fork-generation.md#producer-and-dependency-coverage).
The mirror restores the browser's public data after catalog ingestion. Removing
it immediately would leave that consumer stale. Keeping only a larger runner
would preserve repeated sorting and full uploads. Replacing Iceberg with DocSpec
without changing the public export would also preserve the linear/full-sort
cost. Querying every agency separately before proving pruning risks multiplying
reads. These alternatives do not address the measured causes.

Finish hosted qualification against these gates before resuming ETL:

- Run the complete pinned current catalog through the hosted path. Record total
  process RSS, native memory, peak scratch, rows and bytes read/written, runtime
  and upload volume. The local C result only qualifies monolith assembly on its
  retained inputs.
- Exercise no changes, one addition, one replacement at unchanged total count,
  a text-only repair, an agency move, null relationships, large text, duplicate
  IDs, changed exporter/schema, and an interrupted upload. A no-op must not scan
  or rewrite payloads; failed publication must not advance its success receipt.
- Verify exact identities and index groups, schema and all-value equivalence,
  including the preserved agency partitions. Fingerprints supplement those
  checks; they do not replace required source qualification.
- Measure representative docket and global date-filter queries. The browser
  implementation was not present in this checkout, so its full query workload
  remains a qualification gap. Preserve agency ordering and reject material
  regressions from changing monolith order.
- Prove that a hosted sweep reuses membership state while retaining batch
  recovery and that finalization runs exactly once after complete success.

**Verdict: reshape the current publisher; retain its integrity checks.** The
measured streaming assembly is a bounded improvement, not full CI qualification.
The coherent change is agency-first publication with shared resource controls,
snapshot-based no-op detection, one final index and reused sweep state. Further
agency-only refresh requires measured pruning and exact change accounting.
If these changes still cannot fit the hosted path, the next decision is the
public snapshot layout or runner capacity, with full-workload evidence.

Confidence is high in the traced repeated work and measured assembly result;
hosted publication and the browser workload still require the gates above.
