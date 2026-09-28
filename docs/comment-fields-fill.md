# Fill the comment fields a row predates

Owner decision, 2026-09-28: re-read every Mirrulations comment object once
(option a) and fill what existing `comments` rows lack. The ETL cannot do it:
it replaces a catalog row only when the source `modify_date` moves, and its
manifest skips keys it already read. So a column added to the extract never
reaches rows read before it existed. That is why these are NULL:

- the four comment-reference columns, on all but a few rows;
- `subtype` and `duplicate_comments`, new in SpicyDocs 0.52.0;
- `attachments_json`, wherever the row was read before 2026-03-15, when the
  extract first mapped comment attachments (`9a5955e`). The receipt
  `comments-full-reread-2026-09-28/attachments-root-cause/prove.json` shows
  it on the 5,945-object sample. For all 896 sampled rows that lack
  attachments the source lists, the plain key's object has them, the current
  extract maps them, and the row's `modify_date` equals the object's. None
  was posted after 2026-03-10. Refetch copies were not the cause: no sampled
  comment has attachments only in a `(n)` copy.


Receipts: `~/Work/corpora/supply-2026-09-02/receipts/comments-full-reread-2026-09-28/`.

## Commands

All run through `/opt/homebrew/bin/uv run --frozen fill-comment-fields …`.

| Step | Command | Touches the catalog |
| --- | --- | --- |
| Plan | `plan --workdir W --manifest manifest.parquet` | no |
| Read | `read --workdir W --shard i --shards n` (`run_read.zsh` runs eight) | no |
| Prepare | `prepare --workdir W [--docket D \| --agency A]` | reads one snapshot |
| Write | `write --workdir W [--no-by-file]` | writes, under the lock |

**Plan** splits the ETL manifest's comment keys into 20,000-key chunks per
agency. A plan is named by the manifest digest and the record shape, so a
newer manifest adds only the keys since and a new shape is a new plan.

**Read** writes one part per chunk. Each part row holds:

- the object key, and the GET's ETag and size;
- the extract's thin row, with the body kept as its SHA-256 and length;
- `attributes_json`: every other stated attribute, non-null only.

A part is written whole or not at all, so a rerun reads only the missing
chunks. Transport failures leave a chunk unwritten; unreadable or empty
objects are journaled. Each process stops cleanly past `--max-rss-mb`, or
before a chunk when the disk has less than `--min-free-gb`.

**Prepare** reads the catalog narrowly at its current snapshot: key, agency,
docket, `modify_date`, the fill columns and each row's data file. Measured
live at 28 s. It writes `fill/fill.parquet` and a counts-only
`fill/prepare.json`. A read row fills a catalog row only when:

- its `comment_id` and `modify_date` match, so a copy of another version is
  counted and skipped;
- the column is NULL in the catalog. A stated `0` is a value; unstated stays
  NULL;
- every copy read for that version agrees. Copies that disagree are listed in
  `fill/conflicts.parquet` and fill nothing.

**Write** applies the fill one batch of whole data files at a time. In each
batch's transaction it:

1. refuses unless the catalog is still at the snapshot this fill last saw;
2. captures the batch rows;
3. runs a MERGE that matches on `comment_id`, `modify_date` and `filename`
   and sets `COALESCE(catalog, read)` only where a fill column is NULL;
4. checks the count.

After commit, the rows in the snapshot's added files must equal the captured
rows with the fill applied, on every column. A journal line per batch lets a
rerun skip verified batches. A committed batch whose journal line was lost
refuses as "another writer"; preparing again then finds nothing left to fill.

## Cost

The live table is unpartitioned and unsorted, with about 46 data files and
26.3M rows (6.6 GB). Most rows sit in R2's compacted files, each spanning most
agencies (`catalog_files.json`). DuckDB writes Iceberg merge-on-read: a MERGE
never rewrites a data file. It writes the changed rows anew plus positional
deletes, and must fetch every column of every row group that holds a matched
row.

A batch per agency would read every row group once per agency in it, so it
costs O(agencies × table). A batch of whole data files, matched on
`filename`, reads each file a fixed number of times:

- once to capture its rows;
- once by the MERGE;
- the new rows once to verify.

That is O(table), and the table's rows are rewritten once.

Afterwards the old files are fully position-deleted, and storage holds about
two copies until R2's compaction and snapshot expiry reclaim them.

## Holding the lock locally

The catalog writers share the GitHub Actions concurrency group
`comments-catalog-write`:

- `etl-new-pipeline.yml`, which calls `_regulations-refresh.yml` and
  `_comments-mirror.yml`;
- `publish-comments-mirror.yml`;
- `dedupe-comments-catalog.yml`;
- `backfill-comment-attachment-text.yml`;
- `seed-comments-catalog.yml` and `seed-dockets-catalog.yml`;
- `check-comments-freshness.yml`.

A local process cannot join that group, so it excludes the workflows instead:

1. `gh workflow disable` each workflow above that has a schedule or can be
   dispatched.
2. Confirm that none has a run queued or in progress
   (`gh run list --workflow <file> --status in_progress`, then
   `--status queued`).
3. `export SPICY_REGS_CATALOG_LOCK=local-fill-<UTC time>`. `write` refuses
   without it.
4. Run `prepare`, review `prepare.json`, then run `write`. Every batch also
   refuses if the snapshot moved, which catches a writer the steps above
   missed.
5. Re-enable the workflows. The next mirror publication carries the filled
   rows.

The pilot is EPA-HQ-OW-2022-0114:
`prepare --docket EPA-HQ-OW-2022-0114`, then `write --no-by-file`, one batch.
Its key range is narrow, so the MERGE reads little.
