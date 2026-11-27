# Receipts-Free FEC Observations Splice & Query Rebuild Runbook

This runbook documents the maintained, restartable rebuild path for
`fec-observations` and `fec-query` after receipts retirement (`e5e94c8c7`) and
the `ss-42zk` / `ss-42nr` architecture reshapes.

## Overview

`fec_native_conversion.py`'s receipt-backed copy route is incompatible with
retired `fec-observations` policies (`etl_policy_registry.py`) and requires
tens of GiB of per-row `etl_receipts.parquet` scratch. The maintained rebuild
path replaces it with three receipts-free streaming phases exposed by
`spicy_regs.pipelines.fec_query_rebuild`:

1. **`splice-observations`** (`src/spicy_regs/transforms/fec_observations_splice.py`):
   Reads ONLY the 3 cp1252-corrected positional filings (`1981885.fec` inside
   `20260608.zip`, `1998705.fec`, `1998706.fec`) from
   `CentralRawArchiveStore(create=False)` inside `verified_archive_session()`,
   verifies `filing_encoding()` decides `"cp1252"`, splices them into verified
   prior `fec-observations` (`4cb0dffa`) Parquets in `4096`-row Arrow batches,
   proves bidirectional `EXCEPT ALL` parity on all untouched collections,
   passes `refuse_c1_controls()`, and seals receipts-free
   (`receipt_path=None`, `owned_output_root=build`).
2. **`build-query`** (`src/spicy_regs/transforms/fec_query_builder.py`):
   Streams all tables declared in `data_dictionary/fec_typed_schemas.json`
   (matching `src/spicy_regs/fec_processing_schemas.json` with zero diffs)
   directly from maintained repaired mappers (cross-source registration dedupe,
   archive-member header upgrades, filing-link resolution, `oppexp` trailing
   empty pipes, resolved `TEXT` narrative status, rollup pseudo-candidate
   separation, quality-notice candidate typing, agency SafeLinks & Word table
   rows, legal AF/audit typing & strict `api_key=<redacted>` MUR qualification,
   cover/schedule/bulk/history/API mappers, filing definitions, and the rebuilt
   original tables from `fec_original_tables.rebuild()`). Emits exact
   `DECIMAL(38,9)` values, table-level checkpoints for restart/resume,
   `id_mapping.parquet` (`table, old_record_id, new_record_id`), passes
   `refuse_c1_controls()`, and seals receipts-free with zero-copy
   `owned_output_root` adoption.
3. **`verify-parity`** (`src/spicy_regs/transforms/fec_query_builder.py`):
   Verifies `admit_generation_for_publication()` (no `etlReceipts`, no
   `etl_receipts.parquet`), `refuse_c1_controls()` (`0` hits), and bidirectional
   value parity against a prior `fec-query` generation under
   `id_mapping.parquet` one table at a time.

## Identity Rule (`fec-typed-observation/2`)

- Single source of truth: `IDENTITY_VERSION = "fec-typed-observation/2"` in
  `src/spicy_regs/fec_financial_rules.py`, imported by
  `src/spicy_regs/transforms/fec_query.py` and
  `src/spicy_regs/relationship_views/fec_filing_associations.py`
  (`legacy=True` preserves `"fec-typed-observation/1"` for historical reads).
- `fec_query.observation_id()` strips `"field_mapping"` and `"encoding"`
  (including `"encoding"` inside `locator["embedded_bodies"]`) so every query
  observation ID moves once under `/2` and future encoding fixes become pure
  value updates under stable IDs.
- `build-query` writes `(table, old_record_id, new_record_id)` to
  `id_mapping.parquet` so `verify-parity` proves zero unintended value drift
  against live `784b23da`.

## Resource Envelope & Hard Serialization Guards

Measured local envelope for full corpus execution (`4cb0dffa` / `784b23da`):

- **Single Heavy-ETL Mutex:** Run only one heavy phase at a time on the host
  (do not run concurrent Docker builds or full-repo pytest partitions while
  holding multi-GiB staging directories).
- **Pre-flight Disk Guard:** `fec_query_rebuild.py` requires `>= 25 GiB` free
  by default and hard-aborts below `20 GiB` free (`--min-free-gib`,
  `--abort-free-gib`).
- **Bounded Memory & Spill:** Arrow write batches stay at `4096` rows,
  partitioned financial tables roll at `1_048_576` rows (`32_768`-row row
  groups, zstd), `association_records()` streams in `512`-row batches over one
  DuckDB connection per table, and DuckDB is capped at `memory_limit='2GiB'`
  and `max_temp_directory_size='2GiB'`.
- **Zero-Copy Sealing:** Both builders resolve `output_dir` (eliminating macOS
  symlink ancestors) and pass `owned_output_root=build_root` to
  `build_generation()`, moving Parquet members into `build/generation/` via
  rename without duplicating bytes on disk.
- **Raw Archive Immutability:** Every raw read opens
  `CentralRawArchiveStore(create=False)` inside `verified_archive_session()`
  using `held_original()` (single read + single hash per archive). Nothing is
  written under `corpora/`. Note: agency-document ZIP spool reuse in
  `spicy-docs` is tracked independently under `ss-6fuk`.

## Known Caller-Context C1 Disposition Rows (`4cb0dffa`)

Live `4cb0dffa`'s `fec_collections.parquet` contains two third-party DOJ
press-release disposition rows (`research-capture-27549c221ab03f7ccfbd88ce-0000`
and `research-capture-d046d177f3390f09ab4b6ed1-0000`) whose retained
`callerContext` teaser text carries literal U+0092 (`\x92`) apostrophes.
`splice-observations` refuses any C1 match in `fec_collections.parquet` unless
explicitly addressed; pass `--repair-doj-c1-contexts` to apply the
pin-verified cp1252 punctuation repair with explicit provenance metadata while
preserving the original `pin.sha256`.

## Step-by-Step Execution

### Phase 1 — Splice `fec-observations`

Download verified `4cb0dffa` subject Parquets into a disposable staging folder
(e.g. `/path/to/work/prior-obs/`), then run:

```sh
uv run --frozen python -m spicy_regs.pipelines.fec_query_rebuild splice-observations \
  --prior-dir /path/to/work/prior-obs \
  --output-dir /path/to/work/obs-spliced \
  --repair-doj-c1-contexts \
  --cleanup-prior
```

Outputs under `/path/to/work/obs-spliced/`:
- `build/generation/` — sealed receipts-free `fec-observations` generation (`artifact.json` has no `etlReceipts`)
- `corrected_manifest.json` — canonical cp1252-corrected manifest
- `splice_receipt.json` — manifest diff, bidirectional `EXCEPT ALL` parity proof, and telemetry (`wall_seconds`, `peak_rss_bytes`, `peak_disk_bytes`, `zip_spool_opens`, `c1_control_matches=0`)

`--cleanup-prior` deletes `/path/to/work/prior-obs` only after parity and
generation admission succeed (and refuses any path inside the raw archive).

### Phase 2 — Stream-Build 50-Table `fec-query`

```sh
uv run --frozen python -m spicy_regs.pipelines.fec_query_rebuild build-query \
  --observations-dir /path/to/work/obs-spliced/build/generation \
  --output-dir /path/to/work/query-candidate \
  --source-generation-pin <G_obs_new_digest> \
  --selection-evidence-sha256 <corrected_manifest_sha256> \
  --full-source-records /path/to/work/full-source-records.parquet \
  --individual-snapshot-proof-path /path/to/work/indiv-snapshot-proof.json
```

If interrupted, re-run the exact same command with `--resume` (default `True`):
`load_verified_checkpoint()` verifies each completed table's SHA-256, byte
size, row count, and Arrow schema under `checkpoints/<table_name>.checkpoint.json`
and skips completed tables without re-reading their inputs. Corrupted or
incomplete `.partial` outputs are replaced cleanly on restart.

Outputs under `/path/to/work/query-candidate/`:
- `build/generation/` — sealed receipts-free 50-table `fec-query` generation
- `id_mapping.parquet` — `(table, old_record_id, new_record_id)` parity proof
- `refusals/<rebuilt_table>.refusals.json` — receipt-gated refusal lists for rebuilt original tables
- `refusals/<rebuilt_table>.dispositions.json` — validated per-SHA dispositions for excluded HTTP-200 source pages (`ss-j4vz`, when present)
- `build_receipt.json` — per-table row counts and measured telemetry

### Phase 3 — Verify Parity Table-by-Table

Stage prior `784b23da` tables one table (or bounded batch) at a time to cap
prior-table disk scratch, then run:

```sh
uv run --frozen python -m spicy_regs.pipelines.fec_query_rebuild verify-parity \
  --prior-dir /path/to/work/prior-query \
  --candidate-dir /path/to/work/query-candidate/build/generation \
  --id-mapping /path/to/work/query-candidate/id_mapping.parquet \
  --output-report /path/to/work/parity-report.json \
  --cleanup-prior
```

### Handoff to `ss-j9k3`

Before any R2 publish (`ss-j9k3`), unset `SPICYGOV_RAW_ROOT`,
`R2_ALLOW_SHRINK`, and `R2_MIN_SIZE_RATIO`, re-read live
`https://data.spicygov.ai/publication.v2.json` with an explicit `User-Agent`,
upload the immutable `fec-observations` and `fec-query` generations first,
verify SHA-256 readback, archive the compact JSON receipts and
`id_mapping.parquet`, and reclaim local multi-GiB Parquet staging before
building the reader container image.
