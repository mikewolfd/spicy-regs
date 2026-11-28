# Sync the local raw archive to R2

After a local acquisition commits its receipts, run
`scripts/sync_raw_archive.py` from the spicy-regs checkout. This keeps the
existing `s3://spicy-regs/spicyregs/raw/` mirror current. It is not a
publication step and does not read or remove generation `etl_receipts.parquet`.

Use the archive that the acquisition wrote: the default is
`~/Work/corpora/spicyregs/raw`; `SPICYGOV_RAW_ROOT` overrides it, and
`--raw-root` overrides both. The command only reads the local archive. The
mirror already serves public data, so do not point it at a scratch archive,
private captures, or receipts unfit for public release.

Load `R2_ENDPOINT`, `R2_ACCESS_KEY_ID`, and `R2_SECRET_ACCESS_KEY` through
your normal local secret setup. The endpoint must be the verified SpicyRegs
account; if set, `R2_BUCKET_NAME` must be `spicy-regs`. Dry run needs
authenticated object HEAD access that distinguishes missing keys from
permission errors. Execute also needs object write access.

```sh
uv sync --frozen
uv run --frozen python scripts/sync_raw_archive.py --dry-run
# Review the JSON receipt before enabling writes.
uv run --frozen python scripts/sync_raw_archive.py --execute
# Confirm missing_at_preflight is now zero in both groups.
uv run --frozen python scripts/sync_raw_archive.py --dry-run
```

Dry run is also the default; `--no-dry-run` is an alias for `--execute`.
Without a selection the command scans the committed acquisition journal
and issues a HEAD for each distinct object or receipt it names. When an
acquisition records its receipt keys, pass `--receipt-key
acquisitions/sha256/<shard>/<sha256>` once per new receipt on all three
invocations to limit that scan to the new captures. Use the full scan to
catch up after missed runs. This is not a full mirror audit: unclaimed
blobs, frozen base indexes, and base objects not named by selected receipts
are outside its scope.

The dry run validates receipt identities, checks R2 sizes, and verifies
every missing blob against its local acquisition descriptor (including the
original and stored SHA-256 and sizes for gzip wrappers). A matching remote
size skips the key; it is not a remote checksum check. A size difference,
bad local descriptor, or failed HEAD other than not-found aborts preflight
without writes. Investigate the named key; do not delete or overwrite it
to make the sync pass.

Execute creates missing originals before creating any receipts, with
[`If-None-Match: *` conditional writes](https://developers.cloudflare.com/r2/api/s3/conditional-operations/)
to prevent races from overwriting existing bytes. It rechecks each write
candidate by HEAD. The JSON receipt reports selected keys, size-match skips,
missing keys/bytes at preflight, and how many write candidates were confirmed
present afterward. That confirmation can include keys created by another
writer. A run interrupted during writes may have added some keys; fix the
failure and rerun the same command. Keep the JSON receipt with the acquisition
run's evidence outside the raw archive.
