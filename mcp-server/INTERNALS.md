# MCP server internals

> Engineering notes for contributors. Not user-facing — for how to *use* the
> server see <https://docs.spicy-regs.dev>; for deploying it see
> [`../deploy/cloudflare/`](../deploy/cloudflare/).

This document explains the canonical MCP server implementation
(`src/spicy_regs/mcp_server.py`) and its operating boundaries. The active public
endpoint is <https://mcp.spicygov.ai/mcp>: a Cloudflare Worker forwards requests
to the Python server in Cloudflare Containers.

> **History:** there used to be a second, hand-mirrored copy at
> `mcp-server/api/index.py` — a dependency-light parallel of the canonical server
> for a Vercel deployment, kept in sync by `test_vercel_copy_in_sync`. The server
> now runs from the **canonical** module directly; the copy and its sync test
> were removed. Cloud Run was an earlier deployment. The active Cloudflare
> Dockerfile installs the runtime dependencies without the ETL dependency set;
> see `deploy/cloudflare/Dockerfile` and `deploy/cloudflare/worker/index.ts`.

## What must never be deleted

**The tool docstrings are not documentation — do not delete them.**
MCPServer reflects over `fn.__doc__` to build the tool descriptions sent to every
client during `list_tools`; the `tool` wrapper in `_register_tools` copies each
signature with `functools.wraps` and registers the docstring through
`inspect.cleandoc` as the description. MCPServer sends `__doc__` as written, and
Claude Code cuts a description at 2,048 characters (`CLAUDE_CODE_MAX_MCP_DESCRIPTION_LENGTH`)
with "… [truncated]": round 4 found `query_sql`'s 2,132 characters, 225 of them
indentation, cut mid-word for three personas. The cap counts the compact input
schema too: round 5 saw `query_sql`'s 1,728 + 322 = 2,050 cut at character 1,711
for three personas while `describe_table`'s 1,832 + 190 = 2,022 was not
(`round5/audit-oyelaran.md`). `tests/test_chaos_r4_server.py` holds every
registered description plus its compact input schema to 2,048 characters, with
no indented line; a long field description or enum spends the same budget. `describe_table`'s docstring is how a client learns
which tables are valid; `query_sql`'s is how it learns the available views.
Strip them and the server still runs, but every client goes blind. Verify with
`asyncio.run(build_server().list_tools())`.

**Inline `# type: ignore` / `# noqa` are directives, not comments.** `ty` and
`ruff` gate merges and both read them.

## Connection setup (`_build_connection`, `_get_connection`, `_apply_security_settings`)

- A local directory (`SPICY_REGS_DATA_DIR`) replaces R2 for the whole
  connection and never falls back to remote files; every reply's `source` names
  which one it read.
- `SET home_directory` **must precede** `INSTALL`/`LOAD`. DuckDB writes
  extensions under `<home_directory>/.duckdb`, and the default home is read-only
  or undefined on serverless hosts — hence `_resolve_home_directory` defaulting
  to the temp dir (`SPICY_REGS_HOME_DIR` overrides).
- Keep `LocalFileSystem` enabled. Disabling it breaks httpfs reads. Instead,
  set `allowed_paths` to the exact local paths or HTTPS URLs selected for the
  published views and set `enable_external_access=false`. DuckDB then refuses
  unrelated file access, including reads hidden in nested queries.
- `allow_persistent_secrets=false` prevents httpfs from consulting an unrelated
  on-disk secrets directory. Set it immediately after connecting, before binding
  any remote views: httpfs initializes the secret manager, after which DuckDB
  refuses even setting the same value again. The final restriction step checks
  the value before setting it. Load required extensions before restrictions.
- Load httpfs through `load_public_http(con, INTERACTIVE_HTTP_RETRIES)` before
  any view binds, so the build's own reads retry, and before the lock: DuckDB
  refuses `SET` once `lock_configuration` is on. DuckDB 1.5.5 retries a 429 on
  HEAD and GET, the first retry at once and then `wait * backoff**(k - 2)`
  before retry k. Its defaults give up after 0.5 s; the interactive policy
  allows 7 s of backoff per request, and the batch policy
  (`PUBLIC_HTTP_RETRIES`) about four minutes. `tests/test_duckdb_settings.py`
  measures the schedule against a local server.
- A legacy table (one no pointer pins) is skipped only when its read answers
  HTTP 404 (`duckdb.HTTPException.status_code`). Any other failure, a 429
  included, refuses the build: skipping a throttled read served a connection
  without the table.
- Apply memory and spill-directory settings before disabling external access;
  DuckDB refuses changes to `temp_directory` after that boundary is locked.
  A configured spill directory is an engine resource, not an allowed SQL input.
- Apply `_apply_security_settings` after trusted view construction has collected
  selected member paths and before the connection reaches any user query.
  `lock_configuration=true` stays last. Local and HTTPS controls, path escapes,
  and the deployed `/etc/os-release` reproduction are covered by retained
  experiments and the MCP regression tests.
- The public SQL server refuses configured direct Iceberg reads before setup: its dynamically
  discovered manifest and data paths have not been scoped to this file boundary.
  The fork currently serves published Parquet and configures no catalog on its
  Worker. ETL ingestion and mirror publication continue to use Iceberg. See
  `docs/research/mcp-chaos-2026-09-27.md` for the catalog comparison and adoption
  criteria; enabling unrestricted reads to make a catalog query pass is not a fix.

## Connection lifecycle (`_get_connection`, `_refreshed`)

A build reads each published table's Parquet footer over HTTPS: 3 JSON GETs and
461 Parquet requests (345 HEAD, 116 footer GETs) against r2.dev, 43 to 56 s, on
2026-09-28. Each build is a new DuckDB instance, so nothing it cached survives
into the next one.

- **Polled, not rebuilt.** Past `SPICY_REGS_CONNECTION_TTL` (300 s), the first
  caller re-reads the publication index and the rulemaking pointer and manifest
  (3 GETs, 0.45 s), plus the comments export receipt (one GET of about 45 KB),
  and compares them with the pins the connection holds
  (`_pinned_publication`). The views name immutable URLs, so unchanged pointers
  keep the connection; an idle TTL went from 464 requests to 3.
- **Moved pointers rebuild** through `_build_connection(publication)`, which
  pins exactly the pointers the poll read. The rebuild is a whole new instance:
  the security settings lock `allowed_paths`, so a locked instance cannot admit a
  new generation's URLs, and `CREATE OR REPLACE` of only the changed views is
  not possible.
- **Only the refreshing caller waits.** Other callers keep the current
  connection while a rebuild runs (`_refreshing`). A failed refresh, such as a
  429 on a pointer or a member, logs and keeps the current connection until the
  next TTL. Only a cold start, with nothing to serve, raises.
- **Local mode rebuilds at every refresh**: its `current` link and member
  signatures are re-read at build.
- **Legacy comments files stay mutable.** Their views read the object current at
  each statement (the HEAD revalidates DuckDB's file cache), but the bound
  schema lasts until the pointers move. The build reads `comments-publication.json`
  and HEADs the two files that back views (`COMMENTS_EXPORT_TABLES`), not the 182
  the receipt lists; a moved receipt rebuilds the connection at the next poll.
  An invalid receipt reads as none (logged): it only labels two files' rows, so
  it does not refuse the connection. A failed read refuses it, as the index does.

## Concurrency: worker threads and a limiter

FastMCP (mcp 1.x) ran a sync tool on the event loop, so the server executed one
call at a time and a slow one, such as a 43 s build, stalled every request,
`GET /` included. MCPServer (mcp 2.x) runs sync tools on worker threads, and
`_register_tools` wraps each tool so that:

- it runs through `anyio.to_thread.run_sync` with one `CapacityLimiter` of
  `SPICY_REGS_TOOL_CONCURRENCY` tokens (default 2). A remote scan issues range
  requests from every DuckDB thread; one persona query peaked at 72 requests/s
  on four threads, and r2.dev throttles at hundreds per second across all the
  bucket's readers. Raise the limit once the bucket has a custom domain.
- each call takes its own `cursor()`, and the statement timer interrupts only
  that cursor (`tests/test_mcp_concurrency.py`).
- every failure is re-raised as a `ToolError`. MCPServer shows the client only a
  `ToolError`'s text; any other exception reaches it as a bare "Error executing
  tool". DuckDB's parser and binder messages and the read-only refusals are
  what a caller corrects a query from.

The Worker's `getContainer` has no instance name, so every request reaches one
container whatever `max_instances` says. More instances would each build their
own connection against the public bucket.

## The read-only statement guard

`query_sql` hands arbitrary SQL to `cursor.execute()`, so without a gate the
public endpoint accepts writes. `COPY ... TO`, `ATTACH`, and `EXPORT DATABASE`
all reach the container filesystem. Cloudflare gives this deployment disk-backed
spill space, but arbitrary writes could still exhaust it or corrupt local state.
The endpoint is public and unauthenticated, so that is an anonymous availability
lever, not a reason to relax the read-only guard.

Native file permissions constrain reads to selected data files. They do not
replace `_first_write_statement`, which classifies with DuckDB's own parser and
admits only `SELECT`. The outer `EXPLAIN` statement type hides its inner
statement: `EXPLAIN ANALYZE DELETE ...` executes the deletion, so all `EXPLAIN`
forms are refused until the inner statement can be reliably classified. The statement guard also refuses SQL that
changes configuration or writes permitted files.

**Do not swap the parser for a prefix regex.** The parser is what makes leading
comments (`/* c */ COPY ...`), `COPY` inside a string literal, and stacked
statements (`SELECT 1; DROP TABLE t`) classify correctly. The stacked case
matters most: `execute` runs every statement in the string but returns only the
last result, so a trailing write would otherwise land with nothing in the
response to show for it.

**The SELECT allowlist includes other read forms.** DuckDB folds
`DESCRIBE`, `SHOW`, `SUMMARIZE`, `VALUES`, `TABLE`, and the FROM-first
shorthand (`FROM comments LIMIT 1`) into `StatementType.SELECT`, so all of them
still run. `tests/test_mcp_server.py::test_read_forms_pass_the_guard` pins that
folding; if a DuckDB upgrade splits any of them into its own statement type,
that test fails rather than clients silently losing a query form.

Matching is on `StatementType.name`, not the enum member, because duckdb's type
stubs do not declare the members and `ty` gates merges — comparing names keeps
the check honest without a blanket type-ignore.

The two controls work together: `read_text`, `read_csv`, and `read_blob` are
SELECTs, but native file permissions refuse any file not selected for the views.
A result with duplicate column labels is rejected with alias guidance, because
JSON row objects would otherwise silently overwrite values. One-row lookahead
sets `truncated` only when `max_rows` actually omits a row. Validation refusals
raise tool errors so MCP reports `isError: true` consistently.

## The statement timeout

**DuckDB has no `statement_timeout` parameter.** `SET statement_timeout=...`
raises `Catalog Error: unrecognized configuration parameter` — this was a shipped
runtime crash once, and `tests/test_mcp_server.py` guards the regression. The cap
is a watchdog (`_statement_timeout`) that calls `cursor.interrupt()` and converts
DuckDB's `InterruptException` into a `TimeoutError` so the cause is unambiguous.

`SPICY_REGS_STATEMENT_TIMEOUT` defaults to `790s` in code. The active Cloudflare
Worker passes `600s` to the container (see `deploy/cloudflare/worker/index.ts`).
This is the application's cursor watchdog, not a measured platform or client
request deadline. A client or outer runner can stop waiting earlier. The timer
starts around the statement, after `query_sql` obtains its connection; it does
not measure client startup, transport initialization, limiter wait or a cold
connection build. The stdio entrypoint has no HTTP platform deadline.

Each tool call runs on its own worker thread and cursor, so the timer interrupts
that cursor only, never a sibling call on the same connection.

For serialized local audits, bound lock acquisition separately from the client
invocation and record attempt start, lock acquisition and terminal outcome in
the existing call log. The outer caller's timeout must cover both budgets plus
startup and log-delivery overhead: a `20000ms` Bash timeout cannot accommodate a
`120s` client deadline, even before lock waiting. Use the audit harness's
`600000ms` outer Bash budget, not a new production timeout setting. An outer
termination without a completed wire response establishes neither server query
duration nor whether server work continued or was cancelled. Test those caller
boundaries with a local held lock and fake transport, not a repeated heavy
public query.

## Iceberg and the comments mirror

Iceberg remains the ingestion and update store for comments. The MCP reads its
published Parquet mirror. Direct catalog configuration is refused before any
remote setup; dynamic manifest and data access has not been qualified against
the selected-file restrictions. The old best-effort attach and catalog view
were removed rather than retaining an unreachable, unrestricted fallback.

The 2026-09-27 comparison matched the catalog table UUID, snapshot and schema to
the mirror receipt, then matched the public object ETags and sizes to that
receipt. This supports current publication agreement, not a new source-content
audit. Legacy comments files still have mutable URLs and no immutable comments
generation is pinned; replies state the export receipt's identity and rows
beside the `legacy_unversioned` status (see Response size).

A table in `TABLES` whose Parquet is not published yet is skipped with a warning.
A missing managed generation member still refuses the connection.

## The rulemaking snapshot (`_spicy_rulemaking`)

The materialized rulemaking dataset (`rule_targets` through
`agency_lifecycle_stats`) is not in `publication.json`: it publishes all its
tables at once under `materialized/rulemaking/snapshots/<id>/` and then
replaces `materialized/rulemaking/latest.json`. `_build_connection` reads that
pointer and the manifest it names once, beside the index, through
`publication.load_rulemaking_snapshot`, and pins the result in
`_spicy_rulemaking` the way `_spicy_publication` pins the index. The same view
loop then points each rulemaking view at its immutable snapshot key; there is no
second view path.

- **How often.** At each connection build, and at each refresh past
  `SPICY_REGS_CONNECTION_TTL` (see Connection lifecycle). A moved pointer is
  therefore served from the next refresh, while cursors on the old connection
  keep the snapshot they started with.
- **Refused, not guessed.** As `pipelines.materialized` reads its prior
  generation, the pointer and manifest must be one readable format version, of
  this dataset, naming one snapshot, and every public artifact must sit at
  `<snapshot prefix>/<name>.parquet`: the key is inlined into
  `read_parquet('…')`. An artifact without `visibility: public` is never
  served. A snapshot member that cannot be read refuses the connection, as a
  managed member does.
- **Blast radius.** A pointer or manifest that fails those checks refuses the
  whole connection, every table with it, exactly as an invalid
  `publication.json` does through `load_index` today. Degrading to "rulemaking
  tables unavailable" would be possible but would make a broken publication look
  like an unpublished one; it is left as the index's behavior on purpose.
- **No schema pin.** The manifest records bytes, digest and rows but no columns,
  so the build cannot compare a view with a descriptor. `describe_table`
  compares it with the dictionary instead (`schema_matches_declared`), and
  `spicy-regs-dict check --source r2` holds each declared type to the file.
- **Local mode** never reads the pointer; a rulemaking table there is a loose
  Parquet file like any other.
- **Parsed once.** Each pinned record (`_spicy_publication`, `_spicy_rulemaking`,
  `_spicy_local_selection`) is JSON in the connection, read by several helpers
  per tool call; `_parsed_pin` parses each distinct document once and shares the
  result, which callers must not mutate.

## Declared joins (`_table_joins`)

`describe_table` lists the cross-table joins the table makes (`outgoing`) and
receives (`incoming`) from `table_joins.json`, which `spicy-regs-dict generate`
bundles from `spicy_regs.table_joins` and `check` refuses when stale. Each join
carries its kind (`complete`, `scope`, `design`, `empty`), the reason for a
partial one, and its measured baseline. `scripts/check_table_joins.py` holds the
live tables to each floor nightly in `check-rollup-freshness.yml`.

**The meaning text lives in the record (round 5).** `joins.basis` and
`qualification.basis` are the records' own top-level `basis`, which
`table_joins.joins_record()` and `output_ledger.qualification_record()` write
(`table_joins.BASIS`, `output_ledger.BASIS`); the server holds no copy. Round 5
found a persona reading a `complete` join as proof that the publisher paired
each key correctly (CHRG-118hhrg63377 with meeting 116369, the wrong hearing on
the same day), so the joins basis says `complete` checks only that each
non-null child key names a parent row; the qualification basis says a receipt
it names is the maintainer's retained evidence, not a public file. Editing
either text is a `spicy-regs-dict generate`, and `check` refuses a stale copy.

## Citation lookup kinds (`resolve_document_citations`)

The tool accepts exactly the keys of `citation_resolution.SOURCE_TABLES`: the
kinds a writer puts in `document_citations.document_kind` (the print-citations
rollup's `govinfo_package` and `budget_volume`, and every held-field kind in
`citation_sources.TEXT_SOURCES`). The server compares case-insensitively, and
any other kind is a tool error naming the supported kinds and the closest one.
Before round 4 an unknown kind ran its SELECT and answered
`complete_held_selection` with no findings, which read as "this document cites
nothing"; three aliases (`house_activity_report(s)`, `budget_volumes`) that no
writer emits did the same with `source_read: read`. Raw SQL over
`document_citations` is not checked. The `document_kind` field description,
built from `SOURCE_TABLES` (`kind: table`), lists each kind once with the table
whose key `document_key` takes, so a new kind names its table without a
docstring edit; `govinfo_package` covers only the reports
`house_activity_reports` holds (no CHRG package has citation rows). Round 5
dropped the round-4 enum: it listed the kinds a second time against the client
budget above, and as a schema hint it never validated (the server's own refusal
does).

**Order and paging (round 5).** Rows come in `cite_kind` order, then
`CAST(span_start AS BIGINT)`, `target_key`, `rule_version` and `text_sha256`.
`span_start` is VARCHAR, as every column of the spicy-docs contract is, so the
old `ORDER BY span_start` was text order: on CRPT-118hrpt964 the first bill
citation (offset 18732) came 17th, and its four public-law rows sat at
positions 167 to 170 behind the default cap of 100. `CAST`, not `TRY_CAST`: a
span that is not an integer refuses the call instead of sorting somewhere.
`target_key` separates the rows a range citation writes at one span (273 groups
in print-citations 685f2e27) and `text_sha256`, part of the row identity, a
re-read text's rows, so the order is total and `offset` pages neither skip nor
repeat a row; a legacy file without the digest column sorts without it.
`cite_kind` (case-insensitive) selects one kind, `offset` skips rows, and
`coverage.cite_kind_counts` states the document's rows per kind, so a page
says what it leaves out. A `cite_kind` outside `citation_resolution.CITE_KINDS`
(spicy-docs' `DOCUMENT_CITATION_KINDS`, held equal by
`tests/test_citation_parity.py`) is refused naming the kinds and the ones the
document holds; any of those kinds with no rows is a complete, empty
selection, `case_docket_number` (held, never routed) included. Until the DRY
scout's S1, the check read `ROUTES`, which carried three kinds no writer emits
(`federal_register_document`, `federal_register_number`, `bioguide_id`; deleted)
and lacked `case_docket_number`.
`occurrence_selection.status` is `capped`, `last_page` (an offset page that
reached the end) or `complete_held_selection`. MCPServer drops an argument a
tool does not declare, so a test of a new parameter asserts its effect, not
its acceptance.

**CFR part citations (round 6).** `2 CFR part 200` is keyed `2-200`, which
equals `cfr_ref` on every structural row of the part: 41 rows for 2-200
(subparts, subject groups, appendices, its table of contents), so every held
part citation read `ambiguous`. The `cfr_section` route's predicate keeps a
section row or the part's own granule (`cfr_sections.part_granule`, read from
the granule id by spicy-docs' grammar when the table is built), so the server
learns no CFR id grammar. A part held in two annual editions, or printed across
volumes (40 CFR part 60), still reads `ambiguous` with one candidate per
edition or volume, and the route's grain says so. `Route.predicate_columns`
names what a predicate reads: a selected table without one (a cfr-sections
generation built before the column, or a reverted pointer) is read as before,
every keyed row, instead of failing with `target_read_failure`. The probe is
the `SELECT * … LIMIT 0` this tool already uses for `text_sha256`, once per
batch.

**Pages and compaction (round 5, owner decision 2026-10-03).** `max_occurrences`
defaults to 25 and is at most 100; a larger request is refused with how to page
(the schema's `maximum` is only a hint to the client, so the refusal is the
server's own). The cap and the paging land together: before `cite_kind`,
`offset` and the counts, a smaller cap would have hidden more kinds. A reply
states a fixed list of occurrence fields once (`_compact_occurrences`, a reply
projection like `_query_reply_pins`): `OCCURRENCE_DOCUMENT_FIELDS`, fixed for a
document's text, in `occurrence_fields.shared`, and `OCCURRENCE_KIND_FIELDS`,
fixed for a kind's route and rule, in `occurrence_fields.by_cite_kind`; both
lists are in `occurrence_fields.hoisted`. `target_kind` or `normalized_key`
leaves an occurrence where it equals `cite_kind` or `target_key` (`same_as`).
An occurrence is `{**shared, **by_cite_kind[cite_kind], **occurrence}`;
`cite_kind` stays on every one. The list is fixed (coordinator answer 6), so a
page whose rows happen to agree on a per-occurrence field (all `found`) keeps
the same shape as the next. A listed field whose values differ in its scope (a
document holding rows of two texts) stays on each occurrence and is named in
`not_hoisted`: a stated-once value is never a guess. The acquisition queue is
built from the whole rows and then projected the same way (`_compact_queue`):
the fixed `QUEUE_ITEM_FIELDS` and `QUEUE_REQUEST_FIELDS` are stated once in
`acquisition_queue.shared_fields`. Nothing stored changes. Measured on the
live bucket (print-citations 685f2e27), characters per reply:

| Request | Deployed (d2b5f53) | Fixed lists (this tree) | Hoisting any agreeing field (794beed) |
|---|---|---|---|
| CRPT-118hrpt964, 100 rows | 113,534 | 47,041 | 31,644 |
| CRPT-118hrpt964, default page | 113,534 (100 rows) | 14,640 (25 rows) | 10,375 |
| CRPT-118hrpt964, all 222 rows | 283,615 (one 500-row call) | 127,422 (3 pages) | 97,989 |
| BUDGET-2025-APP, 100 rows | 148,361 | 80,933 | 65,838 |
| BUDGET-2025-APP, default page | 148,361 (100 rows) | 24,941 (25 rows) | 21,427 |
| BUDGET-2025-APP, all 4,456 rows | 893,849 for the first 500 | 4,151,031 (45 pages) | 2,998,213 |

The stable shape costs a third to a half more than hoisting any field a page
happens to agree on (`target_resolved`, `reason`, `rule_version`, ...). Without
the queue projection the 45 pages came to 5,461,495 under the earlier rule: the
queue repeated the document's pins on every requesting occurrence.

**Read statuses (round 5).** `source_read.status` separates three answers that
used to arrive as one `missing_digest` with no occurrences (`_source_read`):

- `not_held`: the kind's table has no row for the key. With citation rows (a
  document the table dropped) the occurrences read `unread_source`; with none,
  the call is refused (below).
- `not_read`: the table holds the document but records no read of it. A print
  kind's table records the read in the row: `text_sha256`, `pages_read`,
  `rule_set_version` and `citation_rows`. A held-field kind's table
  (`report_sections`, `bill_sections`, `comments`, ...) records none: the
  held-citations rollup reads only the fields an operator selects, at most 100
  a run. Its reads are recorded in `document_citation_reads` (`document_kind`,
  `document_key`, `text_sha256`, `rule_set_version`, `read_at`,
  `citation_rows`), which the held-citations pipeline publishes from its
  checkpoint beside `document_citations`. When that table is published, the
  record is the latest read (`read_at`) of the field's current text
  (`text_sha256` = `'sha256:' || sha256(field)`) that states its rule set;
  until then, or with no such row, a held field with no citation rows is
  `not_read`, never an answer that it cites nothing.
- `read_none_found`: the read record states `citation_rows` 0. A read stating
  rows that `document_citations` does not hold refuses: the publication
  disagrees with itself.

With citation rows the status names the digest they are checked against:
`read`, `missing_digest` (a held row without one) or `ambiguous`. A key that
neither the table nor any citation row of the kind holds is refused; the error
names the table, says keys are exact and case-sensitive, and names any
spelling `document_citations` holds that differs only in case, under any kind.
That lookup reads `document_citations` alone, never a parent such as
`comments`. Round 5 found `crpt-118hrpt964` and `CHRG-119hhrg64503` answering
`missing_digest` with a complete, empty selection: the third time an empty
selection passed as an answer, after unknown kinds in round 4.
`coverage.partial` is true when the page was capped or offset, an occurrence
was not looked up (`coverage.reason_counts`), or the document was not read.

## Ledger qualification (`_qualification`)

`describe_table` reports the output ledger's audit for its table beside the
live pin. (`list_sources` did too, for every table, until the response-size
repair below.) The ledger is Markdown under `docs/research/`, which
the image does not ship, so `spicy-regs-dict generate` bundles
`table_qualification.json` (built by `spicy_regs.output_ledger`) and `check`
refuses a stale copy. `_ledger` reads it once per process.

- **Only for the ledger's publisher.** The record names the destination the
  ledger audits. A server reading any other base URL, or a local directory,
  reports `unknown_for_publisher` and no pins: the same pin on another bucket
  proves nothing about this ledger.
- **Pins compare by kind.** A family pin compares with the snapshot's
  `artifactDigest`; a base object's `verified at table digest` pin compares with
  its managed table's `sha256`; a `snapshot_…` pin compares with the rulemaking
  snapshot the connection pinned. A table neither pointer names has no live
  pin, and the reply says it cannot compare rather than guessing from a bare URL.
- **Separate fields, never one verified flag.** `ledger_disposition` is the
  row's word (`qualified`, `PARTIAL`, `FAILED`, `verified`) for `ledger_pin`
  only. `generation` says whether the live pin is one the ledger audited, so a
  `FAILED` generation that is still live reads as "current generation audited"
  plus `FAILED`. With no match, the ledger's latest audit is shown.
- **Closed vocabulary.** "Published at" is a publication statement, not an
  audit. A new or misspelled audit word fails the build instead of being read as
  either an audit or its absence; add it to `output_ledger` deliberately.

## Response size: a reply carries what its caller asked about

On 2026-09-28 a blind persona test (`docs/research/mcp-chaos-2026-09-28.md`)
found the replies too large for the clients reading them. `list_sources`
returned 163,699 characters: every table's publication pin, ledger audit and
relationship-view metadata. Every `query_sql` returned about 54,000 characters,
`SELECT 1` included, because it attached every table's pin
(`connection_publication`). Claude Code spilled each reply to a single-line
file its reader cannot page, so two of five personas never read a row.

- **`list_sources`** lists each table's name, label and coverage kind, and
  lists each relationship family's views once, under their shared summary. Pins,
  audits and view dependencies live in `describe_table`.
  FEC release discovery reports the selected receipt, consumer identity and
  counts of the captured compatibility states. It retains the warning that raw
  financial qualification is not inferred. Available and unavailable view names
  remain discoverable; `describe_table` supplies a selected view's full release
  evidence and mismatch reasons. The summary is derived from the existing
  checks, not a separately maintained qualification model.
- **`query_sql`** echoes `sql` as its first key, so a reply read out of
  context, such as a client's spill file, names the statement it answers. It
  returns the pins of the tables the statement names. `_tables_named` walks
  DuckDB's unbound parse tree (`json_serialize_sql`). `cursor.get_table_names`
  binds the query and expands each view to the tables under it, which for these
  `read_parquet` views is none. The walk follows SQL scope: an unqualified name
  that a CTE in scope defines reads the CTE (a CTE body sees the CTEs before it,
  and itself only when recursive), names compare case-insensitively, and other
  schemas hold no published tables. Before this, a CTE named like a view pinned
  the view, and `FCC_FILINGS` pinned nothing.
- **Ordinary query provenance is compact, not weaker.** Preserve exact selected
  table/generation and release receipt pins, source population/as-of scope,
  named analytic purpose and essential interpretation warnings. Coverage kind
  alone does not explain financial eligibility: source-analysis eligibility,
  serving compatibility and a ledger audit are different checks. Keep the
  owner-defined limits; do not infer current/net money or group additivity in
  the server. Full dependency descriptors, consumer identity and acceptance
  evidence belong in the existing `describe_table` response, not every query.
  This is reply projection only: internal release admission, refusals and
  retained evidence are unchanged. No new evidence table or duplicate storage
  is needed.
- **Pins describe the selected connection**, not universal freshness. Managed
  `rows` come from the pinned index or snapshot manifest; snapshot `run_id` and
  `asserted_at` and dictionary coverage retain their own meanings. A later
  `describe_table` may run after a refresh: compare its exact generation and
  receipt pins with the query's before treating its detailed evidence as support
  for that earlier result. A mismatch is a different selection, not historical
  verification. Derived views embed `_publication_status` pins in
  `source_publication_json`, and `identity_candidates` hashes them into
  `candidate_id`; compact reply shaping must not change those internal pins.
- **`describe_table`** returns one column list. `declared_columns` repeated the
  column names and descriptions that `columns` already carried, which was about
  45% of the reply. DESCRIBE's null, key and default fields meant nothing for a
  Parquet view.
  Relationship metadata appears only in `metadata`. Full FEC release evidence
  appears once: under `publication.release_compatibility` for an available view,
  or `relationship.release_compatibility` for an unavailable view. Response
  shaping creates new dictionaries and leaves the stored relationship records,
  checks and publication provenance unchanged.
- **`describe_table(detail=false)` is the default since round 3 (2026-10-03).**
  The 2026-10-02 round-3 personas (`docs/research/mcp-chaos-2026-10-02.md`,
  round 3) found `bill_versions`' description at 24,600 bytes, 12,048 of them
  the joins' `measurement` records and 3,365 the bill family's ledger statements
  repeated on every table of the family. The default reply now omits exactly
  `joins[].measurement` and `qualification.ledger_statements` and names them in
  `detail.omitted`; every join still states its kind, reason, baseline counts
  and floor, and the qualification keeps its status, generation, pins,
  disposition and task ids. **A client that read `joins[].measurement` now
  needs `detail=true`**, which returns the whole record as before plus `detail`.
  Nothing else is truncated: a wide schema is still described whole.
- **A qualified FEC view's default description summarizes its release record
  (round 4, 2026-10-03).** The round-4 personas found
  `fec_receipts_net_receipts_decision` at 21,054 characters, 11,443 of them its
  dependency's whole storage descriptor (123 columns, 26 member files) and 1,703
  its 23 acceptance receipts. `fec_release.release_summary` keeps every pin and
  reason, each dependency's family and generation, and the receipts' count, and
  `detail.omitted` names `release_compatibility.dependencies[].descriptor` and
  `.acceptance_receipts` under `publication` or `relationship`. `detail=true`
  returns the stored record. Query replies use the same function narrowed to
  `QUERY_RELEASE_FIELDS`. This revises the 2026-10-02 "full release verification
  belongs in an explicit table description": the explicit description is now
  `detail=true`. Measured on the live 72 views: a median of 15,471 to 8,301
  characters (maximum 25,187 to 11,235; all 72, 1,130,695 to 609,034).
- **`query_sql(max_cell_chars=N)`** cuts every text, list or struct cell longer
  than N characters to its first N (a list or struct as its compact JSON text)
  and lists each cut cell in `truncated_cells` as `{row, column, chars}` with the
  cell's full length, the same honesty rule as `truncated` for rows: a shortened
  value is never mistaken for the whole. Unset, nothing is cut and
  `truncated_cells` is empty. There is no server-side `offset`: the docstring
  points at `ORDER BY … LIMIT n OFFSET m`, which DuckDB already does without the
  server re-executing a statement to skip rows. Round 3 measured a 25-row
  `SELECT *` on `committee_meetings` at 204,218 bytes, 99.6% of it rows, and a
  43-row GAO result at 81,801 bytes that the client spilled to a file it could
  not page.
- **Each reply is sent once as data and once as its compact JSON text.** MCPServer's
  default text block is the structured result indented (`indent=2`), so the wire
  carried each reply twice, the second copy 1.1 to 1.7 times the first. Claude
  Code reads `structuredContent` (its spill files are compact JSON), and the MCP
  specification asks only for the serialized JSON in a text block, so `tool`
  returns a `CallToolResult` whose text is the compact JSON: `list_sources` went
  from 86,603 to 75,403 bytes and a FEC view's description from 59,148 to
  17,168 with the release summary above.
- **A managed pin states `published_at` (round 5): when the publisher moved the
  family's pointer to this generation.** It is the index's optional
  `publishedAt` family field (a UTC instant; `parse_index` refuses any other
  spelling), read with the index the build already fetches, so it costs no
  request. It is not "data as of": a generation published today can hold a
  source read last week, and a derived family can lag the parent it was built
  from. It is null until the publisher records it; the writer lands only after
  an image that reads the field is live, because the earlier `parse_index`
  required the family key set exactly and refuses the new key (a refresh would
  keep the old connection and the next cold start would raise). `derive_v1`
  strips it, so version-1 readers never see it. `_reply_pins` adds it for
  `describe_table`, `query_sql` and the citation reply's `publication`; it is
  not in `_publication_status`, whose pins derived views embed, nor in
  `list_sources`. Reading each member's Parquet key-value metadata for a time
  was measured and rejected: only `discovery_signals` records an `as_of`, and
  the reads doubled a cold build's requests to the bucket (+519, +5.2 s).
- **A derived table's pin states the parent generations it was built from
  (round 5, owner decision 2026-10-03).** A generation root (`artifact.json`)
  records `spec.parents`: each parent table's family, generation and bytes, or
  a storage version (an ETag, or a local copy's digest) for a parent no family
  pins. `_input_lineage` adds `inputs`, one `{table, family, built_from, live,
  input_table_current}` per parent, and `inputs_current`. `built_from` and
  `live` are family generations; `input_table_current` compares the parent
  table's own `sha256` where both pins state one (else the generations), so a
  parent family that moved for another table does not mark this one stale.
  The name says so (coordinator answer 7): `built_from` can differ from `live`
  while `input_table_current` is true. A storage-version parent makes no lag
  claim (`input_table_current: null`, ignored by `inputs_current`, which is
  null when no parent can be compared). A family
  whose root records no parents gets no `inputs` key. The root is read on the
  first reply that pins the generation, never at build, and kept per artifact
  digest (`_ROOT_PARENTS`; roots are immutable); a read that fails states
  `inputs: null` with `inputs_status: root_unavailable`, and the root is not
  read again for 60 s (`ROOT_RETRY_SECONDS`, `_ROOT_FAILED_AT`), so a failing
  bucket costs one GET a minute per generation, not one per reply. The read is `publication.read_pinned_root`: the
  image does not install rulespec-artifacts, so the server checks that the
  root at the pinned prefix names the pin, as it trusts pinned member URLs,
  and does not recompute its digest (`load_family_root` does, for lineage).
  A local download holds no roots: its pins state `root_unavailable`. Not in
  `list_sources`. On 2026-10-03, 10 of 52 families recorded parents, and
  bill-subjects (`congress_bills`) and member-vote-terms (`members`,
  `member_terms`) were built from parent bytes no longer live. Tests keep root
  reads off the network (`tests/conftest.py::no_generation_roots`). The
  describe_table description is at 1,982 of its 2,000 characters.
- **`list_sources` states each table's pinned `rows`** (the index descriptor's or
  the snapshot manifest's count; null for a legacy table no pointer pins) so a
  declared table whose generation publishes no rows is visible at discovery
  without a describe call. It is the pinned generation's count, not a freshness
  claim: a later data run lands after the pin.
- **A comments export states its receipt's rows, labelled (round 4).** `comments`
  and `comments_index` are served from fixed URLs no pointer pins, so their
  `rows` were null while `comments-publication.json` stated them. A file whose
  ETag and size matched the receipt at build carries the receipt's `rows` with
  `rows_basis: comments_export_receipt`; a moved file carries `rows: null` with
  `rows_basis: export_receipt_does_not_match_object`. `describe_table` and
  `query_sql` pins add `export_receipt` (receipt digest, file digest, ETag,
  bytes, catalog snapshot). `catalog_snapshot_id` is a string: an Iceberg
  snapshot id passes 2^53, and a JavaScript client read 3797331542152182418 as
  a JSON number and cited 3797331542152182300 (round 5, oyelaran). The status
  stays `legacy_unversioned`: the facts
  join the reply in `_reply_pins` only, never `_publication_status`, whose pins
  the comment views embed in `source_publication_json`. A matching ETag proves
  the object is the one the receipt names, not that the receipt's count is
  right; that check belongs to the mirror export, before it writes the receipt.
  The match is measured at build: until the next poll sees a new receipt, a
  statement can read a newer file than the labelled count describes.

## Relationship-view column meanings (`view_columns`, `relationship_views.lineage`)

A derived view's column has one of three meanings in `describe_table`, in this
order: the spec's `column_descriptions` entry (an `SQLView` field; an
`ArrayRelationship.details` triple), the shared registry in `sql_views`
(`source_ordinal`, `target_key`, `source_table`, …), or the dictionary meaning
of the dependency column it projects unchanged. A column none of them covers has
`description: null`. The round-3 personas found 822 of 1,381 columns on the
non-FEC views carrying a sentence manufactured from the column name ("Term
match; its meaning and source grain are described by this view"); that fallback
is gone, and `tests/test_chaos_r3_server.py` describes every view over
dictionary-typed empty tables and refuses an undescribed column.

**Inheritance is by lineage, never by name.** `lineage.column_lineage` walks
DuckDB's own parse tree (`json_serialize_sql`) when a view is bound and records,
in the pinned relationship record's `column_lineage`, which output columns are a
bare projection (aliased or not, or a star expansion) of a dependency column,
through CTEs, subqueries, lateral joins and views over views. A UNION keeps a
position's origin only when every branch agrees; an aggregate, CASE, cast,
constant or struct field has none, so `fec_relationship_evidence.collection_id`
(computed from a locator) does not inherit `fec_source_records.collection_id`,
and `min(purpose)` does not inherit `purpose`. The serving connection runs no
EXPLAIN for this; the walk is a parse. A table function or VALUES list without
column aliases has unknown columns, and an unqualified name beside one resolves
to nothing rather than to a guess. The review of 2026-10-03 (`phase3-review.md`)
replaced an earlier name-based design with this one.

**Debt: the view meanings are not in `table_metadata.json`.** Generating the
relationship-view entries at `spicy-regs-dict generate` time would give one
dictionary file and let its decay lint cover view prose, but it needs the
generator (`data_dictionary.py`, another lane's file) to bind every view over
dictionary-typed tables, a placeholder FEC source pin for the 72 qualified
views, and a rule for text that lives in two places. Until then the specs hold
the view meanings and the server describes a view on request.

## DNS rebinding protection is off in `build_app`

Deliberate. The active deployment is reached via `mcp.spicygov.ai` and its
configured `workers.dev` address; the SDK's default localhost-only allowlist
would reject those hosts with **421**. `build_app` passes the setting to
`streamable_http_app`. This is a public, stateless, read-only endpoint, not a
privileged localhost service. The SQL and selected-file guards remain required.

## Other invariants

- `ICONS` uses a base64 `data:` URI, not an `https://` URL, so it works on both
  the stdio and HTTP transports. Generated by `scripts/gen_icon.py`.
- `_resolve_catalog_config` and `_resolve_r2_base_url` reject quotes,
  backslashes, and control characters outright rather than escaping them, because
  the values are inlined into `CREATE SECRET`/`ATTACH`/`read_parquet`, which take
  no bind parameters.
- `R2_CATALOG_NAMESPACE` uses `or DEFAULT` rather than `get`'s default argument so
  an env var set to the empty string falls back to `default` instead of `""`.
