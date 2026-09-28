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
docket, `modify_date`, the fill columns and each row's data file (live, 37 s).
It refuses a catalog that lacks a fill column. It writes three files:

- `fill/fill.parquet`, holding only the values to fill;
- `fill/files.parquet`, each file's rows and bytes;
- a counts-only `fill/prepare.json`, whose `prepare_id` names this fill.

A read row fills a catalog cell only when all of these hold:

- its `comment_id` and `modify_date` match the catalog row's, so a copy of
  another version is counted and skipped;
- the cell is NULL. A stated `0` is a value; unstated stays NULL;
- every copy read for that version states the same value for that column, a
  stated null counting as one spelling. A column the copies disagree on is
  listed in `fill/conflicts.parquet` and fills nothing; the others still fill.

**Write** applies the fill in batches of whole data files, up to 512 MiB of
compressed files each. It never migrates the table and refuses if a fill
column is missing. Each batch runs in one transaction:

1. It checks that the catalog is still at the snapshot this fill last
   committed.
2. It writes the batch's rows, as they are, to `fill/preimage/`.
3. It journals the batch as `pending`.
4. It runs one MERGE on `comment_id`, `modify_date` and `filename` that fills
   only NULL cells, and requires the MERGE's count to equal the planned count.
5. It reads the batch's rows back inside the transaction, from its old files
   and from the files the MERGE wrote. They must equal the pre-image with the
   fill applied, on every column, and the new files must hold no other row.
   DuckDB's Iceberg transaction reads its own MERGE before COMMIT; see
   `bench/txn_readback.json`.

Any failure rolls back, so nothing is committed, and journals the batch as
`failed`. A `failed` line stops every later run until `--clear-failure`.

After COMMIT, the batch's snapshot must be the single child of the checked
one, adding exactly the changed records and position deletes; otherwise the
batch is journaled `failed` (committed) and must be rolled back (below).

The journal is per `prepare_id` and records files, so a changed batch budget
never redoes passed work. After a crash, a `pending` batch is handled in one
of three ways:

- if the catalog did not move, it is abandoned;
- if it committed (its snapshot is the child of the checked one and the counts
  match), it is checked against its stored pre-image as of its own snapshot and
  journaled;
- anything else stops the run.

Another commit between batches stops the run: an ETL write, a
`dedupe`, or R2's compaction. Preparing again then finds only the cells still
NULL.

## A committed bad batch has no undo yet

**Do not run `write` on the live catalog until an undo exists.** The first
`rollback` (moving the table's main ref back) was removed: after it, DuckDB
1.5.5 still read the newer snapshot and its next write failed with 409, so
it would have broken every ETL run. The replacement, due next session, is a
forward, checked commit from the batch's on-disk pre-image, written the way
`replace_rows` writes, and rehearsed on the fixture with a normal DuckDB read
and an ETL-shaped write afterwards.

Until then, a batch journaled `failed` with `committed: true` keeps its
pre-image under `fill/preimage/` and its prior snapshot in the journal line;
stop and restore by hand.

## Cost

The live table is unpartitioned and unsorted: 46 data files, 26.3M rows and
6.6 GB (`catalog_files.json`). Most rows sit in R2's compacted files, each
spanning most agencies. DuckDB writes Iceberg merge-on-read: a MERGE never
rewrites a data file. It writes the changed rows anew plus positional deletes,
and fetches every column of every row group holding a matched row.

- **A batch per agency** reads every row group once per agency in it:
  O(agencies × table).
- **A batch of whole data files** is matched on `filename` too, so DuckDB scans
  only those files (`bench/filename_pruning.json`: 100,000 of 600,000 rows). It
  reads each file a fixed number of times: the pre-image, the MERGE and the
  check. That is O(table).

The measurements behind the projection:

- Live, read-only, one 842,921-row file: 24.2 s matched on `filename`, 500.9 s
  on the key alone.
- Packed at 512 MiB, the table makes 15 batches of 270–506 MiB. The two
  largest files (435 and 377 MiB) are each a batch alone or nearly so.
- At the 4.5 MiB/s measured from a laptop while the read ran, that is about
  98 minutes under the lock (`write-review/lock_projection.json`), an upper
  bound. The GitHub runner's mirror export reads the table at about
  19 MiB/s, about 25 minutes.
- The pre-images take about the table's size on local disk; keep them until
  the run is done.

## Preconditions

1. This branch is deployed with its SpicyDocs release.
2. The catalog has the new columns: the first ETL data commit after the
   deploy adds them (`_connect_for_table`'s ALTERs). `prepare` checks for them.
3. At least one ETL data commit has landed since.

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

R2 also commits to the table on its own: managed compaction is enabled
(hourly, 128 MB target). Snapshot expiry is disabled. A local process cannot
join the group, so it excludes both:

1. `gh workflow disable` each workflow above that has a schedule or can be
   dispatched. Confirm none has a run queued or in progress
   (`gh run list --workflow <file> --status in_progress`, then
   `--status queued`).
2. Disable compaction for the table:
   `npx wrangler r2 bucket catalog compaction disable spicy-regs default comments`.
   Check the catalog-level settings read-only with:
   `curl -H "Authorization: Bearer $(npx wrangler auth token)" https://api.cloudflare.com/client/v4/accounts/<account>/r2-catalog/spicy-regs/maintenance-configs`
   (2026-09-28: compaction enabled, snapshot expiration disabled). Leave
   snapshot expiry off.
3. `export SPICY_REGS_CATALOG_LOCK=local-fill-<UTC time>`. `write` refuses
   without it.
4. Pilot first: `prepare --docket EPA-HQ-OW-2022-0114`, review
   `prepare.json`, then `write --no-by-file`, one batch.
5. Run `prepare`, review `prepare.json`, then `write`. Every batch also
   refuses if the snapshot moved, which catches a writer the steps above
   missed.
6. If a batch is journaled `failed` with `committed: true`, stop: there is no
   undo yet (above).
7. Re-enable compaction:
   `npx wrangler r2 bucket catalog compaction enable spicy-regs default comments --target-size 128 --token $R2_CATALOG_TOKEN`.
   Then re-enable the workflows. The next mirror publication carries the
   filled rows.
