# Seeding the ETL: catalog and manifest

The scheduled ETL (`etl-new-pipeline.yml`) has never completed on the fork.
Each batch found no `manifest.parquet` on R2, re-read its agencies' whole
Mirrulations history at about 280 keys/s, and was cancelled at 60 minutes with
nothing published. Batches that finished staging then failed because the
`R2_CATALOG_*` settings were absent. The fork's four base objects were written
by local deliveries.

The pipeline now refuses to start without a manifest. This runbook provisions
the catalog and loads the published Parquet into it. It then publishes a
manifest built from the IDs those objects hold. After that, the first sweep
reads only the records the fork lacks. Follow the steps in order; the
[current delivery state](research/fork-output-ledger-2026-09-21.md#open-items)
records catalog setup, seeding and ETL status separately.

## What is ready

- **Seed:** `~/Work/corpora/fork-execution-2026-09-21/etl-seed-2026-09-24/seed/`
  contains `manifest.parquet` and its receipt, `manifest-seed.json`. The file
  holds 26,171,058 keys (sha256 `28439568…`, 37.7 MB). They cover 279,124
  dockets, 2,001,531 documents and 23,890,403 comments.
- **Seed inputs:** the seed was built from the live objects below. The check in
  step 3 compares each object's current ETag with this table.

  | Object | ETag | sha256 |
  | --- | --- | --- |
  | `dockets.parquet` | `cae2e494…-2` | `27a2ed4a…` |
  | `documents.parquet` | `6c63cb08…-10` | `7907ebe3…` |
  | `comments.parquet` | `cce5e386…-303` | `c3f45f07…` |

- **Script:** `scripts/seed_manifest_from_published.py`
  builds the seed (the default), checks it against R2 and the catalog
  (`--check`), and uploads it (`--publish`).
- **Seed evidence:** `verification.json` and `mirror-listing/` in the same
  corpora directory. They compare the seed with the mirror's own listing and with
  upstream's manifest (see [First sweep](#first-sweep)).

Run each step in order, and do not skip a verification. Run local commands from
the repository root with a `.env` that holds the fork's R2 S3 keys,
`R2_PUBLIC_URL` and, after step 1, the catalog settings. Never paste a
credential into a command line.

## 0. Merge, then stop the schedule

Merge and push this change to `main` first. The workflows that step 2 dispatches
need its `source_key` input, and the ETL needs its refusal of a missing
manifest. Without that refusal, any run that starts before step 4 begins another
full re-read.

The seed workflows share the `comments-catalog-write` concurrency group with the
ETL, so they queue behind any running sweep. Pause the ETL while you seed:

```sh
gh workflow disable etl-new-pipeline.yml --repo mikewolfd/spicy-regs
gh run list --repo mikewolfd/spicy-regs --workflow etl-new-pipeline.yml --status in_progress
gh run list --repo mikewolfd/spicy-regs --workflow etl-new-pipeline.yml --status queued
gh run cancel <run-id> --repo mikewolfd/spicy-regs   # each run listed above
```

## 1. Provision the catalog and add three secrets

Enable the R2 Data Catalog on this account's `spicy-regs` bucket. Use the named
login from fork setup (`deploy/fork-setup.md`), and check `wrangler whoami`
first:

```sh
cd deploy/cloudflare
npx wrangler r2 bucket catalog enable spicy-regs
npx wrangler r2 bucket catalog get spicy-regs   # prints the catalog URI and warehouse
```

Use an existing R2 API token with **Admin Read & Write**, or create one on
this account. It must cover both the R2 Data Catalog and the bucket's storage. See
[Cloudflare's guide](https://developers.cloudflare.com/r2-data-catalog/manage-catalogs/#authenticate-your-iceberg-engine).
Enter each value at the prompt:

```sh
gh secret set R2_CATALOG_URI --repo mikewolfd/spicy-regs
gh secret set R2_CATALOG_WAREHOUSE --repo mikewolfd/spicy-regs
gh secret set R2_CATALOG_TOKEN --repo mikewolfd/spicy-regs
```

`R2_CATALOG_NAMESPACE` is optional; it defaults to `default`. Add the same
values to the local `.env`. The MCP Worker's catalog variables are a separate
step in fork setup.

## 2. Load the published Parquet into the catalog

**Dockets.** Run *Seed dockets catalog (manual)* (`seed-dockets-catalog.yml`)
twice: a dry run first, then the write.

```sh
gh workflow run seed-dockets-catalog.yml --repo mikewolfd/spicy-regs -f dry_run=true -f upload=false
gh workflow run seed-dockets-catalog.yml --repo mikewolfd/spicy-regs -f dry_run=false -f upload=false
```

- **Dry run:** the log should report an empty catalog table, a source of
  279,124 rows, and 279,124 `docket_id`s missing.
- **Write:** the log should end with `Backfilled 279,124 row(s); catalog dockets
  now holds 279,124`.
- **Keep `upload=false`:** republishing `dockets.parquet` changes its ETag,
  which makes step 3 refuse the seed.

**Comments.** The fork has no `comments/` partition tree; its comments exist
only as the single `comments.parquet` object. Set `source_key` to that object so
each agency is loaded from it. Run *Seed comments catalog (manual)*
(`seed-comments-catalog.yml`) as a one-agency smoke test, then as the full load:

```sh
gh workflow run seed-comments-catalog.yml --repo mikewolfd/spicy-regs \
  -f agency=OMB -f source_key=comments.parquet -f full_load=false -f append=false -f upload_index=false
gh workflow run seed-comments-catalog.yml --repo mikewolfd/spicy-regs \
  -f source_key=comments.parquet -f full_load=true -f append=true -f upload_index=false
```

- **Sort order:** the load reads the object once per agency, and each read
  skips the row groups whose `agency_code` range excludes that agency. The load
  therefore stays near one pass over the 2.5 GB file only because
  `comments.parquet` is sorted by `agency_code`. Its 195 row groups have one out
  of order, so the 133 agencies read 329 row groups, about 1.7 passes. The
  script checks this from the footer statistics and refuses a source that would
  take more than two passes. If a republished `comments.parquet` is refused,
  rewrite it ordered by `agency_code` first.
- **Full load:** it reports `Load complete: 23,890,403 rows in the catalog
  comments table`. It loads each agency separately, so an interrupted run can be
  dispatched again with the same inputs: agencies whose count already matches
  `comments_index.parquet` are skipped. The same job then runs
  `check_comments_freshness.py` against the catalog.
- **Duplicates:** the catalog does not reliably apply `DELETE`, so re-loading an
  agency can leave duplicate rows. If the freshness check reports any, dispatch
  `dedupe-comments-catalog.yml` with `apply=true` before step 3.
- **Keep `upload_index=false`:** the published index already describes these
  rows: 112,885 groups that sum to 23,890,403.

## 3. Check the catalog holds every seeded ID

```sh
uv run --frozen python scripts/seed_manifest_from_published.py \
  --output-dir ~/Work/corpora/fork-execution-2026-09-21/etl-seed-2026-09-24/seed --check
```

This step writes nothing. It passes only when all four conditions hold:

- the local manifest matches its receipt;
- the three published objects still have the ETags and sizes listed above;
- `manifest.parquet` is absent on R2;
- every docket and comment ID in the seed exists in the matching catalog table.

The last condition is an exact anti-join rather than a row count. The log
should report 0 absent IDs from 279,124 dockets and from 23,890,403 comments.

If a published object has changed, rebuild the seed from the current objects.
Then run `--check` again:

```sh
D=$(mktemp -d); for t in dockets documents comments; do curl -fsSo "$D/$t.parquet" "$R2_PUBLIC_URL/$t.parquet"; done
uv run --frozen python scripts/seed_manifest_from_published.py --output-dir <new-dir> \
  --dockets "$D/dockets.parquet" --documents "$D/documents.parquet" --comments "$D/comments.parquet"
```

## 4. Publish the manifest

```sh
uv run --frozen python scripts/seed_manifest_from_published.py \
  --output-dir ~/Work/corpora/fork-execution-2026-09-21/etl-seed-2026-09-24/seed --publish
```

This mode repeats every check from step 3. It refuses if `manifest.parquet`
already exists on R2, because the seed only bootstraps an absent checkpoint.
The upload keeps R2's size guard. The script then downloads the object from the
public URL, compares it with the local file, and writes `manifest-publish.json`
beside the seed. Afterwards, `curl -sI "$R2_PUBLIC_URL/manifest.parquet"`
should answer 200 with `content-length: 37697405`.

## 5. Re-enable the ETL and run the first sweep

Leave `allow_fresh_start` at its default of `false`; scheduled runs never set
it. Batches 13 and 14 need more than 60 minutes on the first sweep, and several
others might (see [First sweep](#first-sweep)). So run the whole first sweep as
one dispatch with a longer timeout. A job that finishes early costs nothing
extra. A disabled workflow cannot be dispatched, so enable it first:

```sh
gh workflow enable etl-new-pipeline.yml --repo mikewolfd/spicy-regs
gh workflow run etl-new-pipeline.yml --repo mikewolfd/spicy-regs \
  -f batch_number=all -f skip_upload=false -f use_iceberg=true -f timeout_minutes=240
```

- **Dispatch timing:** dispatch away from the 06:25 and 18:25 UTC crons. A
  scheduled sweep that starts first runs under the 60-minute limit, and batches
  13 and 14 time out in it. That wastes time but no data: the dispatch still
  reads their keys afterwards.
- **What `batch_number=all` does:** it runs all 15 batches in order, one at a
  time, each under the dispatch's timeout.
- **Resuming:** a batch appends its keys to the manifest before the next batch
  starts, so batches never repeat each other's work. The same applies when you
  re-dispatch one failed batch (`-f batch_number=14`).
- **Queued crons:** a cron that fires during the dispatch waits behind it. That
  run is an ordinary incremental sweep.

## Recognising success

- **Manifest:** each batch logs `Loaded manifest: 26,171,058 keys`, and the
  count grows as batches append. The words `No manifest found` never appear. A
  `MissingManifestError` means step 4 did not run.
- **Staging:** `[AGENCY] comments: staged N rows` reports the agency's
  remainder, not its whole history (FWS alone once listed 2,637,380 comments).
- **Catalog:** `iceberg: dockets now holds …` and `iceberg: comments now holds …`
  stay at or above 279,124 and 23,890,403.
- **Publication:** the batch ends with `Uploading manifest after all data files
  succeeded...`, and the ETag of `manifest.parquet` changes after every batch.
  `dockets.parquet`, `documents.parquet` and `comments_index.parquet` get new
  `Last-Modified` times, and `failed_keys.parquet` appears.
- **Duration:** first-sweep batch times track the table below. From the second
  sweep on, a batch spends most of its time loading the manifest (about 3
  minutes locally, likely more on a runner) and listing its agencies.
- **Comments mirror:** the ETL does not refresh the public `comments.parquet`;
  `publish-comments-mirror.yml` does, from the catalog.

## First sweep

**Measurement.** On 2026-09-24 the mirror's own listing covered 335 agencies:
33,856,521 objects, of which 29,075,432 are record keys (`mirror-listing/`,
`verify_seed.py`, `verification.json`).

- **Seed:** every one of the seed's 26,171,058 keys is in that listing.
- **Upstream:** all but two of the seed's keys are in upstream's
  29,071,734-key manifest. The two are WCPO keys, and upstream never read
  WCPO.
- **Remainder:** the first sweep reads 2,904,374 keys: 2,720,144 comments,
  99,238 docket files and 84,992 document files. Of these, 497,927 are the
  mirror's re-fetch copies (`{id}(1).json`), which cannot be derived from IDs.
  The sweep reads them and keeps only rows that are strictly newer.
- **Why not copy upstream:** 2,900,678 of upstream's keys are not in the seed.
  They include 387,550 for VA and 331,876 for USCIS, where the fork holds 11
  and 24,796 comments. A copy of upstream's manifest would have marked all of
  them as done.

**Rates.** The estimate combines four measured rates:

- **Per-agency download:** fork run 35828391248 downloaded comments at 31–85
  keys/s per agency with four agencies in flight. USCIS ran at 84 keys/s, HHS 61,
  VA 51, FWS 44 and USTR 31.
- **Listing:** about 4,000 objects/s per concurrent agency. One stream alone
  listed 5,500 objects/s; 24 threads in one process shared about 16,300/s.
- **Fixed cost:** about 10 minutes per batch for setup, the manifest load
  (183 s locally for 26.2M keys), merges and uploads.
- **Scheduling:** four agency workers run in parallel.

Each batch below has 23 agencies, except batch 14, which has 13.

| Batch | Agencies | Keys to read | Largest agency | Minutes at 85 / 60 / 40 keys/s |
| --- | --- | ---: | --- | --- |
| 0 | ABMC–ATF | 19,929 | AHRQ 10,655 | 16 / 17 / 18 |
| 1 | ATR–CFTC | 142,542 | CFPB 64,223 | 35 / 40 / 50 |
| 2 | CIA–DEPO | 259,893 | DEA 136,376 | 38 / 50 / 69 |
| 3 | DFC–ECSA | 209,633 | EBSA 148,070 | 43 / 55 / 76 |
| 4 | ED–FCA | 263,609 | EOIR 102,440 | 35 / 41 / 53 |
| 5 | FCC–FMCS | 201,522 | FHWA 112,932 | 35 / 42 / 58 |
| 6 | FMCSA–GPO | 340,242 | FMCSA 183,180 | 47 / 62 / 88 |
| 7 | GSA–MARAD | 188,341 | HUD 116,499 | 34 / 43 / 59 |
| 8 | MBDA–NEIGHBOR | 31,548 | MMS 16,701 | 13 / 15 / 17 |
| 9 | NHTSA–NTIA | 87,503 | NOAA 48,499 | 24 / 28 / 35 |
| 10 | NTSB–OSHA | 10,316 | OSHA 6,116 | 14 / 15 / 16 |
| 11 | OSHA_FRDOC_0001–RISC | 98,970 | PHMSA 76,278 | 26 / 32 / 43 |
| 12 | RITA–TRAIN | 204,445 | SSA 168,541 | 44 / 58 / 81 |
| 13 | TREAS–USMINT | 370,710 | USCIS 331,876 | 78 / 105 / 152 |
| 14 | USN–WHD | 475,171 | VA 387,550 | 88 / 119 / 173 |

**Which batches need more than 60 minutes:**

- **Batch 13:** above 60 minutes at every rate. At USCIS's measured 84 keys/s it
  takes about 80 minutes.
- **Batch 14:** above 60 minutes at every rate. VA's remainder alone takes about
  127 minutes at VA's measured 51 keys/s, so the batch takes about 140.
- **Batch 6:** above 60 minutes at the middle rate.
- **Batches 2, 3 and 12:** above 60 minutes only at the slowest rate.
  Batches 5 and 7 come within two minutes of it.

**Timeout:** a 240-minute timeout covers every batch at the slowest rate, and
no batch approaches the 360-minute hosted limit. A local run is therefore a
fallback, not a requirement. Pause the schedule first, then run for example
`uv run run-pipeline --batch-number 14 --batch-count 15 --no-skip-upload
--use-iceberg`, with R2 and catalog credentials in `.env`.

**Totals:** the first sweep takes about 9.5, 12 or 16.5 hours at the three
rates. Later sweeps read only a day's new keys. Each batch then spends 10–23
minutes, mostly loading the manifest and listing its agencies. A full sweep
takes about 4 hours.

**Manifest load cost (not yet addressed):** every batch rebuilds the Bloom
filter from the whole manifest. That took 183 s for the seed's 26.2M keys,
hashing each key in Python, so a sweep spends about 46 minutes (183 s × 15) on
it. The filter's bit array holds 241 MB, twice the ~120 MB that
`BloomFilter.size_bytes` reports and far above the "~34 MB" its comment claims:
the `array("L")` items are 8 bytes on 64-bit Linux and macOS, not 4. Both costs
grow with the manifest, and the next change to `manifest.py` should address
them.
