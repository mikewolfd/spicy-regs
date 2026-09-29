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

All run through `uv run --frozen fill-comment-fields …` (locally,
`/opt/homebrew/bin/uv`). The live write runs on a GitHub runner, through
`.github/workflows/fill-comment-fields.yml` (below).

| Step | Command | Touches the catalog |
| --- | --- | --- |
| Plan | `plan --workdir W --manifest manifest.parquet` | no |
| Read | `read --workdir W --shard i --shards n` (`run_read.zsh` runs eight) | no |
| Fetch | `fetch --workdir W --key K --sha256 H` (the staged read) | no |
| Journals | `journals --workdir W --prefix P [--push]` | no |
| Prepare | `prepare --workdir W [--reads F] [--docket D \| --agency A]` | reads one snapshot |
| Write | `write --workdir W [--no-by-file] [--clear-failure]` | writes, under the lock |
| Undo | `undo --workdir W --batch B --expected-snapshot S` | writes, under the lock |

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

**The staged read.** A runner cannot see the parts, so one fill-only Parquet
file carries what `prepare` reads of them (`READ_COLUMNS`): each copy's object
key, `comment_id`, `modify_date` and the seven fill columns. It holds no
attribute JSON and none of the submitter's contact fields; every column it
holds goes public in `comments` anyway, which is why it may sit in the data
bucket. `fetch` refuses any bytes but the named sha256. Delete the staging
prefix (the file, its `.sha256` and `journals/`, after copying the journals
into the receipts) once the fill is done.

**Prepare** reads the catalog narrowly at its current snapshot: key, agency,
docket, `modify_date`, the fill columns and each row's data file, and counts
the table's rows. It checks the fill columns at that snapshot and refuses if
one is missing. It also refuses while any journal holds a batch that committed
and failed its check (below). It writes three files:

- `fill/fill.parquet`, holding only the values to fill;
- `fill/files.parquet`, each file's rows and bytes; a file whose size cannot
  be read refuses the prepare, since batches pack by size;
- a counts-only `fill/prepare.json`. Its `prepare_id` is the snapshot, the
  fill's digest and a random nonce, so a fresh prepare always starts a fresh
  journal.

A read row fills a catalog cell only when all of these hold:

- its `comment_id` and `modify_date` match the catalog row's, so a copy of
  another version is counted and skipped;
- the cell is NULL. A stated `0` is a value; unstated stays NULL;
- every copy read for that version states the same value for that column, a
  stated null counting as one spelling. A column the copies disagree on is
  listed in `fill/conflicts.parquet` and fills nothing; the others still fill.

**Write** applies the fill in batches of whole data files, up to 512 MiB of
compressed files each. It never migrates the table and refuses if a fill
column is missing. Each batch runs in one transaction. The transaction reads
one snapshot from `BEGIN` on, and the catalog refuses its `COMMIT` (409) if
another writer committed meanwhile:

1. It checks that the catalog is still at the snapshot this fill last
   committed, and lists the live data files from the table's manifests.
2. It writes the batch's rows, as they are, to `fill/preimage/`, and journals
   the batch as `pending` with the pre-image's digest.
3. It runs one MERGE on `comment_id`, `modify_date` and `filename` that fills
   only NULL cells, and requires the MERGE's count to equal the planned count.
4. It reads the batch's rows back inside the transaction, from its old files
   and from the files the MERGE wrote, and keeps each row's key and digest in
   `fill/preimage/…-written.parquet`. The rows must equal the pre-image with
   the fill applied, on every column; the new files must hold no row outside
   the batch; and the table must hold exactly the rows it held at prepare.
   DuckDB's Iceberg transaction reads its own MERGE before COMMIT
   (`bench/txn_readback.json`).

Any failure rolls back, so nothing is committed, and journals the batch as
`failed`. That stops later runs of this prepare until `--clear-failure`.

After COMMIT, the batch's snapshot must be the one child of the checked
snapshot, adding exactly the changed records and position deletes. Otherwise
the batch is journaled `failed` with `committed: true`. That stops every
`prepare` and `write`, in any journal, until the batch is undone (below) or,
after a repair by hand, cleared with `write --clear-failure`.

The journal records files, so a changed batch budget never redoes passed
work. After a crash, a `pending` batch is handled in one of three ways:

- if the catalog did not move, it is abandoned;
- if it committed (its snapshot is the child of the checked one and the counts
  match), it is checked against its stored pre-image as of its own snapshot,
  and journaled `verified`, or `failed` with `committed: true`;
- anything else stops the run.

Another commit between batches stops the run: an ETL write, a
`dedupe`, or R2's compaction. Preparing again then finds only the cells still
NULL.

## Undo a committed batch

`undo --batch B --expected-snapshot S` restores batch `B` from its pre-image
as a new commit, through `iceberg.replace_rows`, the ETL's own checked
replacement. Moving the table's ref back instead was tried and removed: after
it, DuckDB 1.5.5 still read the newer snapshot and its next write failed with
409, which would have broken every ETL run.

- `S` is the snapshot you reviewed (the journal's last `snapshot_after`, or
  the one the refusal names). The undo refuses unless the catalog is at `S`,
  both before it starts and inside its transaction.
- Every row the batch's commit wrote must still be in the table as written
  (the `-written` digests). A row changed since, by the ETL or anyone, refuses
  the whole undo, for a repair by hand.
- Only the rows the commit changed are rewritten, whole, so a damaged commit
  is undone on every column; a row it lost is inserted again.
- After COMMIT the undo's snapshot must be the one child of `S`, adding the
  restored rows. The journal gets an `undone` line, which clears that batch's
  failure. The fill's prepare is then spent: prepare again.

Rehearsed on the local Iceberg fixture
(`tests/test_comment_fields_iceberg.py`): an undo of one of three batches,
then a plain DuckDB read and the ETL's own `_merge` of a newer version and a
new comment; and a damaged commit found on recovery, refused, then undone
whole. On a runner, dispatch the workflow with `operation: undo`, the failed
run's id, the batch and `S`; it takes the pre-images from that run's artifact.

## Cost

The live table is unpartitioned and unsorted: 43 data files, 26.3M rows and
6.6 GB at 2026-09-29's snapshot (46 files the day before; R2's compaction
merges them). Most rows sit in R2's compacted files, each
spanning most agencies. DuckDB writes Iceberg merge-on-read: a MERGE never
rewrites a data file. It writes each changed row anew with a positional
delete.

- **A batch per agency** fetches every row group once per agency in it:
  O(agencies × table).
- **A batch of whole data files** is matched on `filename` too, so full rows
  are fetched only from its own files. A `filename` filter prunes no file,
  though: every scan still opens every data file and reads its key and file
  columns. So each batch reads those narrow columns of the whole table a fixed
  number of times (the pre-image, the MERGE, the read-back and one count):
  O(table) per batch and O(batches × table) in all, bounded by packing whole
  files up to 512 MiB.

Measured on live, read-only, at one pinned snapshot, from a laptop under
load (2026-09-29, receipts `write-review/live-timing/`). The table packs into
15 batches of 1–5 files and 270–508 MiB, 6.1 GiB in all. Per batch, for the
one with the most bytes (4 files, 4.5M rows) and the one with the most rows
(5 files, 5.5M rows):

| Step | 4.5M rows | 5.5M rows |
| --- | --- | --- |
| The live file list, from the manifests | 3.7 s | 3.7 s |
| The pre-image, streamed to disk (~0.5 GB) | 20.7 s | 27.6 s |
| The MERGE's read side (full rows, key and file match) | 21.1 s | 23.0 s |
| The row count and strays, one scan | 7.8 s | 1.0 s |
| The read-back digests and their comparison | 12.4 s | 14.9 s |

Comparing whole rows took 206–301 s per batch, so the check compares each
row's key and digest instead. The MERGE's write side (the batch's rows anew,
about 0.5 GB, plus position deletes) cannot be measured read-only. `prepare`
at full scale, against a local stand-in of the table, took 93 s and 7.8 GB
peak memory (`write-review/prepare-scale/`), plus the 19 s narrow scan.

The projection, scaled by rows (the MERGE and the check cost by row, the
pre-image by byte): about 255 s of pre-images, 116 s of MERGE reads, 71 s of
read-back, and 14 s of counting and 4 s of file listing per batch. That is
about 12 minutes of reading under the lock for all 15 batches on the laptop.
A runner scans the whole table in 0.85–1.11× the laptop's time (the mirror's
staging scan: 338 s and 439 s on runners, 396 s on the laptop), so about
10–13 minutes there. The MERGEs' writes, about 6.3 GB of new data files plus
position deletes, come on top; they are not measured. The run also holds the
group for `prepare` (about 2 minutes) and the artifact upload.

The runner has room: the check keeps digests, not whole rows. Comparing
whole rows spilled 42 GB per dense batch, more than a runner's disk.

## Running it on a GitHub runner

The owner's decision, 2026-09-29: the fill runs on a runner, one dispatch of
`fill-comment-fields.yml` at a time. The workflow joins the catalog writers'
concurrency group `comments-catalog-write`, so the ETL, the mirror, the dedupe
and the backfills queue behind it, and sets `SPICY_REGS_CATALOG_LOCK`. Each
run:

1. fetches the staged read and checks its sha256;
2. pulls every earlier run's journal from the staging prefix's `journals/`,
   so a committed failure from any run stops this one;
3. disables R2's compaction for the table and confirms it at table level
   (the catalog-level setting alone does not show a table override);
4. runs `prepare`, then `write` (or `undo`);
5. always pushes the journals back, uploads the journal, receipts and
   pre-images as the run's artifact `comment-fields-fill`, and re-enables
   compaction.

The staged read for this fill (2026-09-29, receipt
`comments-full-reread-2026-09-28/staging/staging.json`): key
`staging/comment-fields-fill-405b55d1c112/reads.parquet`, sha256
`405b55d1c1128d5f031b9b6a1fa5b7381208ea6e453eab45ccd2b50c4ff4bc96`,
179,560,661 bytes, 26,629,661 read copies of 26,314,480 comments. Its
journals go to `staging/comment-fields-fill-405b55d1c112/journals/`.

Order of dispatches:

1. `operation: prepare`: counts only, no write. Review its `prepare.json`
   (in the artifact): `rows_to_fill`, `cells_by_column`, the conflicts.
2. `operation: pilot` with `pilot_docket: EPA-HQ-OW-2022-0114`: one scoped
   batch. Check the docket's rows in the catalog.
3. `operation: fill`.
4. If a batch is journaled `failed` with `committed: true`, stop and undo it
   (above) before anything else.

The next mirror publication carries the filled rows.

## Preconditions

1. This branch is deployed with its SpicyDocs release.
2. The catalog has the new columns: the first ETL data commit after the
   deploy adds them (`_connect_for_table`'s ALTERs). `prepare` checks for them.
3. At least one ETL data commit has landed since.

## Holding the lock locally

Only if the fill must run from a workstation. A local process cannot join the
concurrency group, so it excludes the other writers by hand:

1. `gh workflow disable` each workflow in `comments-catalog-write` that has a
   schedule or can be dispatched (`etl-new-pipeline.yml` with
   `_regulations-refresh.yml` and `_comments-mirror.yml`,
   `publish-comments-mirror.yml`, `dedupe-comments-catalog.yml`,
   `backfill-comment-attachment-text.yml`, `seed-comments-catalog.yml`,
   `seed-dockets-catalog.yml`, `check-comments-freshness.yml`,
   `fill-comment-fields.yml`). Confirm none has a run queued or in progress
   (`gh run list --workflow <file> --status in_progress`, then `--status
   queued`).
2. Disable compaction for the table
   (`npx wrangler r2 bucket catalog compaction disable spicy-regs default comments`)
   and confirm it at table level, read-only:
   `curl -H "Authorization: Bearer $(npx wrangler auth token)" https://api.cloudflare.com/client/v4/accounts/<account>/r2-catalog/spicy-regs/namespaces/default/tables/comments/maintenance-configs`
   must show `result.maintenance_config.compaction.state` `disabled`
   (2026-09-29: `enabled`, hourly, 128 MB; at catalog level snapshot expiry
   is disabled, leave it so).
3. `export SPICY_REGS_CATALOG_LOCK=local-fill-<UTC time>`. `write` and `undo`
   refuse without it.
4. Prepare, pilot and fill as on the runner. Every batch also refuses if the
   snapshot moved, which catches a writer the steps above missed.
5. Re-enable compaction
   (`npx wrangler r2 bucket catalog compaction enable spicy-regs default comments --target-size 128`),
   then the workflows.
