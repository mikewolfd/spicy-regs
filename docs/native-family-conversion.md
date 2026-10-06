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
   downloads every published member by its hash and byte-size pins. Each
   member must match its declared physical columns and row count.
4. Runs the family's own writer over those tables with upload skipped: the
   rollup with its builder replaced by "return the retained tables", or the
   court writer for a court family. Field policies are read at run time.
5. Refuses unless every row converts (every receipt `accepted` or
   `observed`) and a native read restores each table to exactly the retained
   rows, with the same columns, types and file metadata.
   Split tables also retain exact relative member names, empty members,
   per-member row counts, and each member's full Arrow schema and metadata.
   A table-wide union cannot substitute for this member-by-member check.
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
died or lost the write's response. Updates write and sync a sibling temporary
file before replacing the receipt, preserving the previous complete record
if a write is interrupted. A confirmed version-2 publication is recorded
before repairing the derived version-1 index. If recovery cannot read the
stored index, the command reports an unknown outcome and keeps the rollback
record; it does not claim that nothing was published.

The command refuses a family outside `--allow`, one that is already native,
and one with no subject/receipt rollup or court writer (the regulations
base tables, the derived rollups, FEC and government families). A split
table requires a subject/receipt rollup with the same declared partition
columns. The court converter still refuses split tables.

### What the checks cannot see

- Row order and `-0.0` against `0.0` are not compared. For single-file
  tables, column order, `string` against `large_string`, nullability, and a
  timestamp's timezone label are not compared either.
- On the rollup path the restored table is rebuilt from the receipt's copy
  of each input row. A mapper defect in a public column is therefore
  invisible to the comparison: it proves the next run's prior, not the
  public table. The public table is checked only by row count and by the
  writer's own validation.
- On the court path the restored file takes the retained table's types, so
  the command holds those against the declared native schema instead. A
  column that moves to receipts has no declared type to hold it against.

## One owner for each receipt dataset

The index gives each subject or checkpoint dataset one owning family
(`publication.py`, `_merge_family`). Two explicitly marked receipt-only
logs are shared: `congress_acquisition` and
`legislative_document_file_states`. Each family retains its own log rows
inside its pinned receipt member, but does not claim the shared dataset in
the index. A reader must select the family's receipt member for these logs.

All other receipt datasets still require one owner. A source checkpoint
with a policy but no declared writer remains unavailable: shared-log
support does not assign it an owner or authorize a conversion. Family
reader checks and a full retained-data rehearsal remain release gates.

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

`court-docket-groups` names no parents after conversion (the conversion read
none), so `describe_table` shows no recorded inputs for it until its next
build. The replaced generation, named in the converted one's read snapshot,
still holds them.

Held.

| Family | Why | What releases it |
|---|---|---|
| `unified-agenda` | `materialize-rulemaking` downloads the table raw and requires `timetable_json`, `cfr_references_json`, `legal_authority_json` and `url` (`pipelines/materialized.py:253`, `rulemaking_dataset.py:133`), which all leave the native table. | That reader reads the native table. The same change raises the rollup's 30-minute limit: measured on loopback storage 10 to 14 minutes here and 862.9 s and 476.0 s by the reviewer, so a hosted runner at half the speed lands near it. |
| `court-opinion-pdf-extractions` | The hosted citation tool keys `court_opinion_derived_pdf` by `(opinion_id, source_sha256)`; natively the table is `opinion_body_id`, `opinion_id`, `cluster_id`, `text_content`. | Verified new hosted server, below. |
| `amendments`, `committee-meetings`, `house-communications`, `nominations`, `record-issues`, `treaties`, `members` | Shared-log ownership is implemented; the historical dry runs do not qualify the current release. | A current retained-data rehearsal and verified readers. |
| `committee-reports`, `senate-expenditures`, `native-legal-references` | Shared-log ownership is implemented; current writer declarations and outside readers still need qualification. | The same checks, with every published checkpoint declared by its source owner. |

Never put these in `--allow` yet. The command would convert each, and none
has been checked for outside readers.

- Too large for the per-row writer; they wait for the bulk writer:
  `court-citations` (102.0M rows), `court-opinions` (10.8M),
  `court-opinion-clusters` (10.1M), `roll-call-votes` (10.6M),
  `member-vote-terms` (10.6M), `federal-register` (1.0M).
- Are not on an approved list: `bill-subjects` (also
  needs `congress_bills` native first), `committee-rosters`,
  `press-releases`, `laws`, `print-citations`.

A Congress.gov or GovInfo receipt also records the local path of the file it
was converted from. Run those conversions from a neutral `--work` path.

## Hosted-server precondition

Deploy and verify the new hosted server before converting citation families.
The server switch keeps the selected data unchanged and runs independently.
There is no conversion release window. Until each parent table converts,
the new citation tool reports that its native input is unavailable.

`court-opinion-pdf-extractions` waits for this verification because the
hosted reader at `3a1d3140` keys its body by `(opinion_id, source_sha256)`;
the new reader keys by `opinion_body_id`. `print-citations`,
`committee-meetings`, `house-communications` and `members` also wait for the
new hosted reader. The shared-log branch qualifies those families separately.

The historical audit permits `courtlistener`, dependent
`court-docket-groups`, and independent `cfr-sections` before the server from
merged converter code and an approved run package. The coordinated execution
of 2026-10-05 orders these canaries after the first verified switch as well.

Rehearse each conversion on a pinned loopback copy, build the server's own
connection over it, and call the citation tool for each supported kind.
Keep rollback serving and data compatible together: restoring old-shape
citation tables makes the new tool refuse their input; restoring the old
server while its dependent tables remain native breaks its readers. Verify
the chosen server/data pair before either rollback.

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
- Record the prior objects, restore proof and rollback recovery date for the actual run. `plan-generation-retention.yml` deletes on
  its own every Sunday at 05:45Z and keeps each family's last three
  generations. The conversion is one generation and each scheduled run adds
  one. After two successful publications following conversion, the captured
  generation falls outside those three. The next weekly retention may delete
  its members unless docs, DocSpec, rulemaking inputs, managed parents, or the
  grace period still protect it. Recalculate the earliest eligible cleanup
  from the actual conversion date, successful subsequent publications, fresh
  retention plan, and next retention run. To keep a rollback open
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
ALLOW=courtlistener,court-docket-groups,cfr-sections
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

### Prepare, qualify, and publish the same build

A successful official dry run now seals its `conversion.json`. Run it from
the clean merged main and pinned environment that will publish the result:

```sh
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py bill-family \
  --allow bill-family --work "$RUN/bill-family" --expect-main "$MAIN" --expect-spicy-docs "$WHEEL" --remote fork
```

Before publishing, the release package must record that receipt's
`prepared.generationPin`, the full physical-key qualification, the exact
deployed reader image tested against this generation, and the coverage
preview's exact subject, receipt and supporting-table pins. Keep each proof
file and its SHA-256 beside the run package. Reports for another generation
do not qualify this one. These source and consumer checks remain separate
from the converter's receipt admission and restoration checks.

Publish those same bytes once the package is qualified:

```sh
R2_PUBLIC_URL=https://data.spicygov.ai $UV run --frozen python scripts/convert_family_to_native.py \
  --publish-prepared "$RUN/bill-family/conversion.json" --allow bill-family \
  --expect-main "$MAIN" --expect-spicy-docs "$WHEEL" --remote fork \
  --env-file <file with the R2 settings> --expect-bucket "$BUCKET"
```

Prepared publication reruns byte checks, native admission and exact input
restoration; it never runs the source writer. It refuses an altered or
incomplete receipt, changed source/runtime, missing or altered members,
different restoration results, or a changed captured family entry.
Timestamp-only changes to that entry also refuse, including during a
conditional pointer retry. Unrelated family updates remain intact.

Only a completed official dry run is eligible. Partial writer outputs,
old unsealed receipts, rehearsal reports, and already attempted or published
receipts are refused. A failed or interrupted publication retains its
rollback record. Reconcile the stored pointer read-only and inspect the
rollback record before any further write; the recovery procedure below
explains the available rollback. Do not remove the attempt journal or reseal
it to force another prepared publication. No report's success flag bypasses
verification.

Every exit that is not a success prints two lines. `REFUSED:` (exit 1) or
`FAILED:` (exit 2, or 130 when interrupted) says why. `STATE:` says what the
bucket holds: nothing was published; or the family is published, with the
rollback command; or a publish was attempted and its result is not known,
with the command that finds out. A publish whose response was lost is not a
refusal: the command rereads the stored index, and when it names this
conversion's generation it finishes the read-back and exits 0.

When prepared publication stops before saving a new attempt, `STATE:` says
that this invocation made no new attempt and calls for read-only
reconciliation of any previous outcome using the receipt. This also applies
when changed code, a changed wheel, or conflicting settings prevent the
receipt from being opened; it makes no claim about the current pointer.

Settings, by name: a dry run needs only `R2_PUBLIC_URL`. Publishing and
rolling back need `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT`
and `R2_BUCKET_NAME`; no bucket is assumed. `CLOUDFLARE_API_TOKEN` with
`CLOUDFLARE_ZONE_ID` purges the two index URLs and is optional. Nothing is
loaded from a `.env` implicitly; `--env-file` names one. A variable already
set in the shell that the file contradicts is refused, by name.

### Hosted execution

`convert-native-family.yml` runs the same maintained converter on a hosted
runner, one named family per manual dispatch. It supports the bulk regulatory,
court and votes families and `bill-family`. Their owners still coordinate
reader compatibility and concurrent ordinary writers.

Bind `expected_entry_sha256` to the complete observed family entry with
`scripts.native_conversion_invocation.entry_digest(entry)`. This digest
includes timestamps, schemas and every table member. The workflow checks it
before conversion, then checks the prepared or downloaded artifact's captured
entry against the same digest before publication. The first check does not
lock the publication index; a change before the converter captures it makes
the later check fail, retaining the candidate without publishing it.
The workflow explicitly checks out current main for the producer, even when
the manual dispatch selects a workflow repair branch. The invocation's
`revision` identifies the workflow revision; `producer-revision.txt` and the
conversion receipt identify the checked-out producer revision. The converter
receives that producer revision as `--expect-main` and separately checks clean
current main, the installed locked wheel, complete processing restoration,
source evidence and the exact stored predecessor before publication.

Leave `publish` false to prepare without publication credentials. The run
retains the sealed family under the `native-family-<family>` artifact, and its
invocation, logs and observed process resources in a separate evidence
artifact. Preparation has a 180-minute command timeout; publication has a
90-minute timeout inside the 300-minute job, leaving time to retain failed
outputs. The former 45-minute preparation bound interrupted normal work in
[court](https://github.com/mikewolfd/spicy-regs/actions/runs/37448262153),
[votes](https://github.com/mikewolfd/spicy-regs/actions/runs/37448264683),
[vote terms](https://github.com/mikewolfd/spicy-regs/actions/runs/37448267222)
and [bills](https://github.com/mikewolfd/spicy-regs/actions/runs/37448753043).
Publication also needs time for complete restoration and anonymous readback.
The runner, disk floor, thread limits and acceptance checks remain unchanged.
Hosted limits do not establish that a larger family fits the runner.

After checks against the actual reader, dispatch with `publish=true`, the
explicit `expected_bucket`, and `source_run_id` naming that preparation run.
The workflow downloads those same bytes and calls `--publish-prepared`;
it does not rerun the writer. Current main, runtime and complete prior entry
must still match. Alternatively, a cleared single dispatch can prepare and
publish immediately by leaving `source_run_id` empty. Always retain a failed
attempt and reconcile its pointer read-only; never clear its journal, reseal
it, or dispatch a blind retry. Ordinary producer acceptance remains a later
check after successful publication and anonymous readback.

The ordinary Federal Register, docket-link and rulemaking workflows also
declare finite limits that allow complete restoration of native priors.
These workflows explicitly check out main, so a workflow timing repair can
run before its merge without changing the producer. The shared rollup's
optional `producer_ref` leaves other callers at their original revision.
Its invocation records the actual `code_revision` and the separate
`workflow_revision`. See `rollup-federal-register.yml`,
`rollup-fr-docket-links.yml` and `materialize-rulemaking.yml` for their
limits; their source schedules, publication defaults and command arguments
stay the same.

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

Current rulemaking publication retains the full prior pointer and manifest
captured during priming. Before uploading the candidate, it checks both against
canonical storage. After uploading validated artifacts and the manifest, it
checks the same prior again and replaces `latest.json` conditionally on the
captured pointer's ETag. A changed pointer or manifest refuses publication;
an explicit bootstrap requires an absent canonical pointer. If the pointer
write loses its response, reconcile the canonical pointer before retrying.
The publisher does not retry or roll back that uncertain write automatically.
