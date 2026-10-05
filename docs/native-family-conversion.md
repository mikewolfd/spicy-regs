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
   (`--expect-main`, `--expect-spicy-docs`). Clean includes untracked files
   under `src/` and `scripts/`, which the command could load. Main is read
   from the remote's URL; a local path is refused. The installed SpicyDocs
   files are hashed against the vendored wheel, and the wheel against its
   digest in `uv.lock`.
2. With `--publish`: checks the credentials and that `R2_BUCKET_NAME` is the
   `--expect-bucket`, and prints the bucket and endpoint host, before it
   converts anything.
3. Captures the publication index, its ETag and the family's entry, and
   downloads the family's published tables by their pins.
4. Runs the family's own writer over those tables with upload skipped: the
   rollup with its builder replaced by "return the retained tables", or the
   court writer for a court family. Field policies are read at run time.
5. Refuses unless every row converts (every receipt `accepted` or
   `observed`) and a native read restores each table to exactly the retained
   rows, with the same columns, types and file metadata.
6. Refuses unless a family that carries source evidence still does: the
   converted generation names the one it replaces and one evidence artifact,
   and journals again the events the next run reads back from its prior.
7. With `--publish`: checks main and the wheel again, saves
   `conversion.json`, then publishes through the standard conditional
   pointer write and reads the result back without credentials: the entry
   carries `etlReceipts`, row counts match, and the read that refused before
   now passes.

Each run writes `conversion.json` in its `--work` directory. It holds the
captured entry and the generation the run intends to publish, and it is
saved before the pointer write, so `--rollback` works even when the process
died or lost the write's response.

The command refuses a family outside `--allow`, one that is already native,
one with a split table, and one with no subject/receipt rollup or court
writer (the regulations base tables, the derived rollups, FEC and government
families).

### What the checks cannot see

- Row order, column order, `string` against `large_string`, nullability, a
  timestamp's timezone label, and `-0.0` against `0.0` are not compared.
- On the rollup path the restored table is rebuilt from the receipt's copy
  of each input row. A mapper defect in a public column is therefore
  invisible to the comparison: it proves the next run's prior, not the
  public table. The public table is checked only by row count and by the
  writer's own validation.
- On the court path the restored file takes the retained table's types, so
  the command holds those against the declared native schema instead. A
  column that moves to receipts has no declared type to hold it against.

## Shared logs

Every index reader gives each dataset an entry lists one owning family: the
hosted server, main's server and the library (`publication.py`,
`parse_index`, `_merge_family`). Two receipt-only logs are written by many
families: `congress_acquisition` by every Congress.gov family and
`legislative_document_file_states` by every GovInfo document family. Each is
declared a shared log (`"shared_log": true` in its `etl_policies/*.json`).

A family's rows of a shared log are its own run history. They are in its own
receipt member and its next run reads them back through its own tables. The
index entry does not list a shared log, so no family owns one and these
families convert in any order. The index format is unchanged.

A retry checkpoint or a read marker has the same policy shape and is not a
shared log: one family owns it and readers find it by name
(`committee_report_reads`, `document_citation_reads`). A test fails when a
dataset gains a second family without the marker, or carries the marker
with one.

## Families

A family converts only when every reader of its published table, other than
its own rollup, still works on the native shape. Two servers read each
table: the hosted one (branch `3a1d3140`, old layout) until the release, and
main's afterwards. Audit of 2026-10-05: main's code at `90dfa370`, DocSpec
and spicyengine by an independent read; the hosted build by the release
session. Server controls and saved user queries were not checked.

Safe to convert.

| Order | Family | Columns that leave the table | Outside readers and what they read | Next scheduled run |
|---|---|---|---|---|
| 1 | `courtlistener` | `parties_json`, `attorneys_json`, `firms_json`, `date_created`, `absolute_url` (`parties`, `attorneys`, `firms`, `blocked`, `date_blocked` arrive) | `build_court_docket_groups.py:141` (`cl_docket_id`, `court_id`, `docket_number`); `relationship_views/courts.py:44` and `table_joins.py:797` (`cl_docket_id`); `check_rollup_freshness.py:76` (`date_filed`). Hosted build: one view joins on `cl_docket_id`. | daily 18:30Z; passed on the converted copy, and its journal reads "Inherited pins" |
| 2 | `court-docket-groups` | `edition`, `rule_version`, `match_basis` | `table_joins.py:797` (`cl_docket_id`, `parent_cl_docket_id`). Hosted build: a plain view. | none; built by hand; its input `court_dockets` is native after step 1 |
| 3 | `cfr-sections` | `url`; `part_granule` becomes BOOLEAN | `citation_resolution.py:104` (`cfr_ref`, `package_id`, `granule_id`, `section`, and `part_granule = 'true'`, which gives the same rows on both shapes, on main and on the hosted build); `table_joins.py:345` (`title`, `part`); `check_rollup_freshness.py:74` (`last_modified`). Nothing reads `url`. | daily 20:30Z; limit raised to 60 minutes (`rollup-cfr-sections.yml` says why) |

User SQL on `cfr_sections.part_granule` keeps working with `= 'true'`;
`lower(part_granule)` and `LIKE` stop working on a BOOLEAN.

These seven hold a shared log and are also safe at any time: no reader on
main or on the hosted build needs a column that leaves (second audit,
2026-10-05, same scope). Convert them after the three above, each before its
next scheduled run.

| Order | Family | What leaves or changes | Next scheduled run | After conversion, on the loopback copy |
|---|---|---|---|---|
| 4 | `amendments` | `url` | daily 02:40Z | its scheduled run passed |
| 5 | `committee-reports` | `text_sha256` becomes `body_version_id`; file and rule columns leave; `committee_report_reads` moves to receipts | daily 03:40Z | its scheduled run passed; the read marker is still found by name |
| 6 | `record-issues` | four columns become lists; five rule and link columns leave | daily 06:00Z | dry conversion exact; scheduled run not rehearsed |
| 7 | `treaties` | four `*_json` become lists; `parts_json` becomes `parts_count`; four columns leave | daily 06:10Z | its scheduled run passed |
| 8 | `nominations` | `nomination_type_json`, `url` leave; `is_civilian` BOOLEAN; `is_military` arrives | daily 06:20Z | its scheduled run passed |
| 9 | `senate-expenditures` | `text_sha256` becomes `body_version_id`; three `*_json` become lists; nine columns typed | Tuesdays 07:30Z | its scheduled run passed, after a one-line fix to the rollup |
| 10 | `native-legal-references` | `input_sha256` becomes `body_version_id`; `attributes_json` becomes three columns; its reads table moves to receipts | none; built by hand | dry conversion exact |

Every live table of these families is text today, so each typed column is a
type change for user SQL: `= 'true'` still works on a BOOLEAN; `lower()`,
`trim()` and `<> ''` do not. `rebuild-record-issues`, the family's own manual
repair, reads `entire_issue_json` and would fail on the native table.

`court-docket-groups` names no parents after conversion (the conversion read
none), so `describe_table` shows no recorded inputs for it until its next
build. The replaced generation, named in the converted one's read snapshot,
still holds them.

Held.

| Family | Why | What releases it |
|---|---|---|
| `unified-agenda` | `materialize-rulemaking` downloads the table raw and requires `timetable_json`, `cfr_references_json`, `legal_authority_json` and `url` (`pipelines/materialized.py:253`, `rulemaking_dataset.py:133`), which all leave the native table. | That reader reads the native table. The same change raises the rollup's 30-minute limit: measured on loopback storage 10 to 14 minutes here and 862.9 s and 476.0 s by the reviewer, so a hosted runner at half the speed lands near it. |
| `court-opinion-pdf-extractions` | The hosted citation tool keys `court_opinion_derived_pdf` by `(opinion_id, source_sha256)`; natively the table is `opinion_body_id`, `opinion_id`, `cluster_id`, `text_content`. | Verified new hosted server, below. |
| `committee-meetings`, `house-communications`, `members`, `print-citations` | The hosted server reads columns that leave. | Verified new hosted server, below. |

Never put these in `--allow` yet. The command would convert each, and none
has been checked for outside readers.

- Too large for the per-row writer; they wait for the bulk writer:
  `court-citations` (102.0M rows), `court-opinions` (10.8M),
  `court-opinion-clusters` (10.1M), `roll-call-votes` (10.6M),
  `member-vote-terms` (10.6M), `federal-register` (1.0M).
- Not on an approved list: `bill-subjects` (also needs `congress_bills`
  native first), `committee-rosters`, `press-releases`, `laws`.

For `bill-family`, the first scheduled run after conversion must match the
pinned witness in `tests/fixtures/bill_family_enrolled/` and
`tests/test_bill_family_cbo.py`: 132 Senate and 2 House concurrent resolutions
move from `passed_both` to `cleared` under `enrolled_text_listed`, with 134
bills and 0 actions re-staged. The following run must report 0 and 0. A
different set of 134 fails. Reconcile a changed selected generation against
its own rows before asking for run approval; the bulk owner owns that proof.

For a bulk-written dataset's first production run, compare the row path
(`bulk=False`) on samples stratified by source row shape and every original
ordinal returned by `congress_bulk.routed_ordinals`. Record source,
accepted and rejected counts independently. A converter round trip through
the bulk writer and bulk restore alone does not establish correctness.

A Congress.gov or GovInfo receipt also records the local path of the file it
was converted from. Run those conversions from a neutral `--work` path.

## Hosted-server precondition

Deploy and verify the new hosted server before converting these families.
The server switch runs independently and keeps the selected data unchanged.
There is no conversion release window. Until each parent table converts,
the new citation reader reports that its native input is unavailable.

The historical reader audit permits the three canaries above before the
server switch, from merged converter code and an approved run package. The
coordinated execution of 2026-10-05 orders those canaries after the verified
first switch as well. Start with `courtlistener`, then its dependent
`court-docket-groups`; `cfr-sections` is independent.

| Order | Family | Hosted reader that needs the old shape | Main's reader that needs the native one |
|---|---|---|---|
| 1 | `print-citations` (65,674 rows; 55 s to convert, about 3 minutes with the read-back) | the citation tool orders by `rule_version` and reads `text_sha256`, `pages_read`, `rule_set_version` (`mcp_server.py:2110`, `:1535`) | the citation tool refuses until `document_citations` is native (`citation_receipts.py:28`) |
| 2 | `court-opinion-pdf-extractions` (3 rows) | kind `court_opinion_derived_pdf` keyed by `(opinion_id, source_sha256)` | the same kind keyed by `opinion_body_id` |
| 3 | `house-communications` (5,044 rows) | `relationship_views/regulatory.py:14` (`rin_occurrences_json`, `source_route`, `record_package_id`, `record_granule_id`, `detail_read`); `citation_sources.py:33` | the three `communication_*` citation kinds |
| 4 | `committee-meetings` (6,097 rows) | `relationship_views/congress.py:27` (`*_json`, `detail_read`) | none; it picks the old or native view by table |
| 5 | `members` (3 tables, 58,364 rows) | `relationship_views/congress.py:68`, `affiliations.py:5` | `member-vote-terms`, `roll-call-votes` and the scorecard analysis refuse an old-shape `members` |

`print-citations` shares only a log with the bill family and `laws`. It
reads its own priors and nothing of theirs, so it converts alone and first.
The citation tool needs each kind's own parent table native and nothing
else: `print-citations` alone covers 52,823 of 52,895 held rows.

Before these conversions, verify these checks against the new server:

- Its image holds what the citation inputs import. In an environment with
  only the image's packages plus pyarrow, the connection refuses once
  `document_citations` is native: `etl_receipts.select_receipts` imports
  `spicy_regs.transforms`, which imports `sources.r2` and so `boto3`.
- The two `document_citations` joins have a baseline. After conversion
  `scripts/check_table_joins.py` reports them `UNBASELINED`, a failing
  status.
- The held-citations manual reader (`transforms/held_citations.py:132`)
  reads `house_communications.record_entry_text`, which leaves the table. It
  is null in all 5,044 live rows, so nothing is lost today.

Held court-opinion citations are written under `[opinion_id,
source_sha256]`. The native mapper publishes them under `opinion_body_id`,
the key main's tool asks with (`legislative_documents.py`,
`HELD_KEY_TRANSLATIONS`). The other kinds are asked for as they are stored.

Rehearsal for these conversions: convert each family on the loopback copy, build
the server's own connection over the result, and call
`resolve_document_citations` once for each kind that holds rows. The drivers
and their output are in
`~/Work/corpora/native-migration-dryrun-20261004/` (`rehearse.py`,
`server_reader.py`).

Rollback uses the conversion receipt and the captured prior objects. Keep
serving and data compatible together: restoring an old-shape citation table
makes the new citation tool refuse that input; restoring the old server
while its dependent tables remain native breaks its readers. Verify the
chosen server/data pair on the rehearsal copy before either rollback.

## Before a production run

- The owner has approved the concrete merged-head run package and the list.
- Tell the server release session, which replays the server's controls
  before and after: a converted table changes shape for readers (its field
  policy is `src/spicy_regs/etl_policies/<table>.json`).
- Check out main in a clean worktree and sync it with the pinned lock. Take
  the commit and the wheel version from that checkout; the command re-checks
  both against the remote immediately before it publishes.
- No run of the family's own rollup is in flight (`gh run list --workflow
  rollup-<name>.yml`). A concurrent publication is refused, not overwritten.
- Record the prior objects, a restore proof, and when rollback recovery closes for the actual run date. `plan-generation-retention.yml` deletes on
  its own every Sunday at 05:45Z and keeps each family's last three
  generations. The conversion is one generation and each scheduled run adds
  one, so for a daily family the captured generation is the fourth by the
  third run after conversion, and the next Sunday's retention deletes it:
  recalculate the date from the actual conversion date, subsequent run cadence and next retention run. To keep a rollback open
  longer, disable that workflow's schedule first (`gh workflow disable
  plan-generation-retention.yml`) and enable it again once the rollback is
  no longer wanted.

No concurrency group is involved: these conversions write only immutable
generation objects and the index pointer.

## Commands

```sh
cd <clean worktree at main>
UV=/opt/homebrew/bin/uv   # the lock needs uv 0.11 or later; a bare `uv` may be an older shim that cannot read it
MAIN=$(git rev-parse HEAD)
WHEEL=$($UV run --frozen python -c "from importlib.metadata import version; print(version('spicy-docs'))")
ALLOW=courtlistener,court-docket-groups,cfr-sections   # then the next approved families, in the order above
RUN=~/Work/corpora/native-conversion-<date>
BUCKET=<the production bucket's name, typed by hand>

# Dry: reads production anonymously, uploads nothing.
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py courtlistener \
  --allow $ALLOW --work $RUN/courtlistener-dry --expect-main $MAIN --expect-spicy-docs $WHEEL --remote fork

# Publish.
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py courtlistener \
  --allow $ALLOW --work $RUN/courtlistener --expect-main $MAIN --expect-spicy-docs $WHEEL --remote fork \
  --env-file <file with the R2 settings> --expect-bucket $BUCKET --publish

# Roll back.
$UV run --frozen python scripts/convert_family_to_native.py --rollback $RUN/courtlistener/conversion.json \
  --env-file <file with the R2 settings> --expect-bucket $BUCKET
```

One family per invocation, in the order above. `--remote` names the git
remote whose `main` is checked (default `origin`).

Every exit that is not a success prints two lines. `REFUSED:` (exit 1) or
`FAILED:` (exit 2, or 130 when interrupted) says why. `STATE:` says what the
bucket holds: nothing was published; or the family is published, with the
rollback command; or a publish was attempted and its result is not known,
with the command that finds out. A publish whose response was lost is not a
refusal: the command rereads the stored index, and when it names this
conversion's generation it finishes the read-back and exits 0.

Settings, by name: a dry run needs only `R2_PUBLIC_URL`. Publishing and
rolling back need `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`
and `R2_BUCKET_NAME`; no bucket is assumed. `CLOUDFLARE_API_TOKEN` with
`CLOUDFLARE_ZONE_ID` purges the two index URLs and is optional. Nothing is
loaded from a `.env` implicitly; `--env-file` names one. A variable already
set in the shell that the file contradicts is refused, by name.

## Verify

- The command's last line is "published and read back", and
  `conversion.json` shows `rows_only_in_restored` and `rows_only_in_retained`
  both 0 for every table, no `type_changes` or `metadata_changes`, and
  `read_back.anonymous_read_rows` equal to the retained counts.
- `conversion.json` records the bucket and endpoint host under `target`, and
  under `lineage` the inputs the converted generation names.
- `publication.v2.json` carries `etlReceipts` for the family.
- The family's next scheduled run succeeds and moves the generation. A
  failure there leaves the converted generation in place.
- The table's notes follow the rule in the header of
  `data_dictionary/descriptions.yaml`: after a table's first native publish,
  add one sentence only if its receipt holds a link or a column people
  query, ending "is kept in its receipt; see etl_receipts."

If the command prints its last line and does not return, the work is done
and `conversion.json` is complete: interrupt it. Seen once in about thirty
loopback runs, in pyarrow's thread pool at interpreter exit.

## Roll back

`--rollback` reads the stored index and decides. If the family names this
conversion's generation, it rewrites the entry to the captured one. If it
still names the captured generation, there is nothing to do and it says so.
If it names anything else, a scheduled run or another writer has published
since: the rollback refuses, because it would discard that run's rows, and
`--discard-newer` says to do it anyway. `--rollback` takes no conversion
arguments.

The old-shape objects are still stored, and the command checks that first;
see "Before a production run" for when retention removes them. The converted
objects stay, unreferenced. After a rollback the family refuses its
scheduled runs again, as before the conversion.

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
delete_scope=True)`; the whole catalog requires an explicit reviewed recovery plan preserving native tables;
each family by its index entry.

### Materialized rulemaking snapshots

`materialized/rulemaking/latest.json` selects a separate snapshot. Converting a
flat `proceedings` family does not migrate that snapshot. The scheduled pipeline
requires native proceedings and their admitted receipts; an old snapshot
without receipts refuses before processing. Preserve proceedings IDs through
explicit migration rather than bootstrap an existing snapshot.

Prepare the migration with `scripts/prepare_rulemaking_snapshot_native.py`.
Pass retained `--pointer`, `--manifest`, and `--sources`, their exact
`--expected-pointer-sha256` and `--expected-manifest-sha256`, a fresh
`--destination`, `--generation-id`, and `--asserted-at`. The command checks
snapshot membership, each artifact's path, bytes, hash, and rows; it copies the
complete prior snapshot into `retained/`, classifies its existing outputs, and
checks restored rows in order, schema, and file metadata. It writes a candidate
native manifest, pointer, and `migration-checks.json` locally. It performs no
upload or pointer replacement.

Before production migration, qualify the complete retained snapshot and the
following scheduled materialization on merged readers and writers. Preserve the
old pointer and every old snapshot object, verify restore, and include the
actual retention deadline and compatible server rollback in the same concrete
run approval package. Scheduled materialization remains held until the native
snapshot is admitted and the verified consumers read it.
