# Convert an old-shape family to native, once

A scheduled rollup refuses a published family that carries no ETL receipts
(`selected_generations.py`: "selected native input requires one ETL receipt
generation"). A family whose builder reads its own prior therefore cannot
publish its first native generation by itself. The old-shape readers were
removed on purpose on 2026-10-03, and the owner's direction of 2026-10-04 is
to fix forward: no scheduled path reads an old shape again.

`scripts/convert_family_to_native.py` converts one named family, once. Owner
decision, 2026-10-05: convert the small families now; the large tables wait
for a cheaper writer. Evidence for everything below:
`~/Work/corpora/native-migration-dryrun-20261004/` (`dryrun-receipt.json`,
`state/family-inventory.json`).

## What the command does

1. Refuses unless this checkout is main, clean, and has the SpicyDocs wheel
   main pins, all equal to the values the run was started for
   (`--expect-main`, `--expect-spicy-docs`).
2. Captures the publication index, its ETag and the family's entry, and
   downloads the family's published tables by their pins.
3. Runs the family's own writer over those tables with upload skipped: the
   rollup with its builder replaced by "return the retained tables", or the
   court writer for a court family. Field policies are read at run time.
4. Refuses unless every row converts (no `refused` or `error` receipt) and a
   native read restores each table to exactly the retained rows, with the
   same columns and types.
5. With `--publish`: checks main and the wheel again, publishes through the
   standard conditional pointer write, and reads the result back without
   credentials: the entry carries `etlReceipts`, row counts match, and the
   read that refused before now passes.

Each run writes `conversion.json` in its `--work` directory. It holds the
captured entry, so `--rollback` can restore it.

The command refuses a family outside `--allow`, one that is already native,
one with a split table, and one with no subject/receipt rollup or court
writer (the regulations base tables, the derived rollups, FEC and government
families).

## One owner for each receipt dataset

The index gives every dataset one owning family, receipt-only datasets
included (`publication.py`, `_merge_family`). Two receipt-only logs are
declared by many rollups: `congress_acquisition` by every Congress.gov
family, and `legislative_document_file_states` by every GovInfo document
family. Publication accepts the first family that holds one and refuses the
rest: "['congress_acquisition'] already belongs to family amendments". This
is main's rule, so scheduled runs would meet it too. No such family is native
yet. The command checks it before converting.

Until that rule is decided, convert none of these families: whichever went
first would lock out the others, the bill family included.

## Families

Convertible now. Their receipts hold only their own datasets.

| Order | Family | Next scheduled run | After conversion |
|---|---|---|---|
| 1 | `courtlistener` | daily 18:30Z | `run-rollup-courtlistener` passed twice on the converted copy |
| 2 | `court-docket-groups` | none; built by hand | input `court_dockets` is native after step 1 |
| 3 | `court-opinion-pdf-extractions` | none; built by hand | |
| 4 | `unified-agenda` | daily 20:00Z | `run-rollup-unified-agenda` passed on the converted copy in 10 to 14 minutes of its 30 |
| 5 | `cfr-sections` | daily 20:30Z | limit raised to 60 minutes (`rollup-cfr-sections.yml` says why) |

Held for the receipt-dataset rule. Each converts cleanly in a dry run.

- Congress.gov: `amendments`, `committee-meetings`, `house-communications`,
  `nominations`, `record-issues`, `treaties`, `members`.
- GovInfo documents: `committee-reports`, `senate-expenditures`,
  `native-legal-references`.

A Congress.gov or GovInfo receipt also records the local path of the file it
was converted from. Run those conversions from a neutral `--work` path.

## Before a production run

- The owner has approved the run and the list.
- Tell spicy-stack-54, which replays the server's controls before and after:
  a converted table changes shape for readers (its field policy is
  `src/spicy_regs/etl_policies/<table>.json`).
- Check out main in a clean worktree and `uv sync --frozen`. Take the commit
  and the wheel version from that checkout; the command re-checks both
  against the remote immediately before it publishes.
- No run of the family's own rollup is in flight (`gh run list --workflow
  rollup-<name>.yml`). A concurrent publication is refused, not overwritten.
- Do not execute a generation-retention plan while a rollback may be wanted:
  it would delete the captured generation.

No concurrency group is involved: these conversions write only immutable
generation objects and the index pointer.

## Commands

```sh
cd <clean worktree at main>
MAIN=$(git rev-parse HEAD)
WHEEL=$(uv run --frozen python -c "from importlib.metadata import version; print(version('spicy-docs'))")
ALLOW=courtlistener,court-docket-groups,court-opinion-pdf-extractions,unified-agenda,cfr-sections
RUN=~/Work/corpora/native-conversion-<date>

# Dry: reads production anonymously, uploads nothing.
R2_PUBLIC_URL=https://data.spicygov.ai uv run --frozen python scripts/convert_family_to_native.py courtlistener \
  --allow $ALLOW --work $RUN/courtlistener-dry --expect-main $MAIN --expect-spicy-docs $WHEEL --remote fork

# Publish.
R2_PUBLIC_URL=https://data.spicygov.ai uv run --frozen python scripts/convert_family_to_native.py courtlistener \
  --allow $ALLOW --work $RUN/courtlistener --expect-main $MAIN --expect-spicy-docs $WHEEL --remote fork \
  --env-file <file with the R2 settings> --publish

# Roll back.
uv run --frozen python scripts/convert_family_to_native.py --rollback $RUN/courtlistener/conversion.json \
  --env-file <file with the R2 settings>
```

One family per invocation, in the order above. `--remote` names the git
remote whose `main` is checked (default `origin`). Exit status 1 and a line
starting `REFUSED:` mean nothing was published, unless the message says the
family is published and names the receipt to roll back with.

Settings, by name: a dry run needs only `R2_PUBLIC_URL`. Publishing and
rolling back need `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`
and `R2_BUCKET_NAME`. `CLOUDFLARE_API_TOKEN` with `CLOUDFLARE_ZONE_ID` purges
the two index URLs and is optional. Nothing is loaded from a `.env`
implicitly; `--env-file` names one, and a variable already set wins.

## Verify

- The command's last line is "published and read back", and
  `conversion.json` shows `rows_only_in_restored` and `rows_only_in_retained`
  both 0 for every table and `read_back.anonymous_read_rows` equal to the
  retained counts.
- `publication.v2.json` carries `etlReceipts` for the family.
- The family's next scheduled run succeeds and moves the generation. A
  failure there leaves the converted generation in place.

## Roll back

`--rollback` rewrites the family's index entry to the captured one. The
old-shape objects are still stored, and the command checks that first. The
converted objects stay, unreferenced. After a rollback the family refuses
its scheduled runs again, as before the conversion.

If a scheduled run has published since, the rollback refuses, because it
would discard that run's rows. `--discard-newer` says to do it anyway.

A family whose receipts are narrower than its rows (the FEC identity tables)
restores from the subject plus the receipt, never the receipt alone. None is
in this list.

## The regulations unit: pending the bulk writer

`dockets`, `documents`, the three attributes tables, the dockets and
comments catalogs and the comments mirror convert together, later, through
the bulk writer. The ETL restores all five tables every batch, so the per-row
writer cannot serve them (measured 2026-10-04: one batch's priming alone
would run 9 to 13 hours against a 6-hour limit). The public files stay as
published on 2026-10-03 until then.

The dry run fixed the order, which the bulk conversion keeps:

1. Tell spicy-stack-d0, take `comments-catalog-write`, create the native
   tables, and disable compaction on them and confirm it, as
   `fill-comment-fields.yml` does. `ensure_native` starts a table empty, so
   it must be populated before any scheduled writer reaches it.
2. Convert `dockets`, then `comments` one agency per commit, reading the old
   namespace at one pinned snapshot. A stopped run resumes at the next
   agency. The old namespace is never written.
3. Verify counts for each agency and exact rows in both directions, then
   re-enable compaction.
4. Convert the two old retry files (`failed_keys.parquet`,
   `pending_comment_text.parquet`) through `finish_checkpoints`.
5. Publish the five families last. That is what unblocks the ETL.
6. Let one sweep run, then the refresh.

Rollbacks rehearsed: an agency's rows through `replace_native(...,
delete_scope=True)`; the whole catalog by dropping the three native tables;
each family by its index entry.
