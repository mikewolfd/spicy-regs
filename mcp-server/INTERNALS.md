# MCP server internals

> Engineering notes for contributors. Not user-facing — for how to *use* the
> server see <https://docs.spicy-regs.dev>; for deploying it see
> [`../deploy/cloudrun/`](../deploy/cloudrun/).

This is the rationale behind the MCP server implementation
(`src/spicy_regs/mcp_server.py`). The source is comment-free by project policy
(documentation lives in markdown), so this document explains what the code does
and why.

> **History:** there used to be a second, hand-mirrored copy at
> `mcp-server/api/index.py` — a dependency-light parallel of the canonical server
> for a Vercel deployment, kept in sync by `test_vercel_copy_in_sync`. The server
> now runs on Cloud Run (see `deploy/cloudrun/`) from the **canonical** module
> directly (importing it pulls only duckdb + mcp, no ETL deps), so the copy and
> its sync test were removed. Everything below is the single canonical server.

## What must never be deleted

**The three `@mcp.tool()` docstrings are not documentation — do not delete them.**
FastMCP reflects over `fn.__doc__` to build the tool descriptions sent to every
client during `list_tools`. `describe_table`'s docstring is how a client learns
which tables are valid; `query_sql`'s is how it learns the available views.
Strip them and the server still runs, but every client goes blind. Verify with
`asyncio.run(build_server().list_tools())`.

**Inline `# type: ignore` / `# noqa` are directives, not comments.** `ty` and
`ruff` gate merges and both read them.

## Connection setup (`_build_connection`, `_get_connection`, `_apply_security_settings`)

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

## The read-only statement guard

`query_sql` hands arbitrary SQL to `cursor.execute()`, so without a gate the
public endpoint accepts writes. `COPY ... TO`, `ATTACH`, and `EXPORT DATABASE`
all reach the container filesystem, and on Cloud Run that filesystem is
in-memory — a large enough `COPY` evicts the instance. The service is
`--allow-unauthenticated`, so that is an anonymous availability lever.

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

`SPICY_REGS_STATEMENT_TIMEOUT` defaults to `790s` in code; the Cloud Run
deployment sets it to `600s` via env (matching the service `--timeout`), so a
runaway query returns a clean `TimeoutError` rather than a platform-killed 5xx.
Keep the app timeout at or below the platform request timeout. In practice the
binding limit is usually the *MCP client*, which typically gives up at 60–120s.
The stdio entrypoint has no platform limit at all.

## Iceberg and the comments mirror

Iceberg remains the ingestion and update store for comments. The MCP reads its
published Parquet mirror. Direct catalog configuration is refused before any
remote setup; dynamic manifest and data access has not been qualified against
the selected-file restrictions. The old best-effort attach and catalog view
were removed rather than retaining an unreachable, unrestricted fallback.

The 2026-09-27 comparison matched the catalog table UUID, snapshot and schema to
the mirror receipt, then matched the public object ETags and sizes to that
receipt. This supports current publication agreement, not a new source-content
audit. Legacy comments files still have mutable URLs; the MCP does not yet
expose their publication receipt or pin an immutable comments generation.

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

- **How often.** Once per connection build: the first tool call, then whenever
  the cached connection is older than `SPICY_REGS_CONNECTION_TTL` (300 s by
  default). A moved pointer is therefore served from the next rebuild, while
  cursors on the old connection keep the snapshot they started with.
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
- **`query_sql`** returns the pins of the tables the statement names.
  `_tables_named` walks DuckDB's unbound parse tree (`json_serialize_sql`).
  `cursor.get_table_names` binds the query and expands each view to the tables
  under it, which for these `read_parquet` views is none. CTE names in the
  tree are dropped by intersecting with the published tables.
- **`describe_table`** returns one column list. `declared_columns` repeated the
  column names and descriptions that `columns` already carried, which was about
  45% of the reply. DESCRIBE's null, key and default fields meant nothing for a
  Parquet view.

## DNS rebinding protection is off in `build_app`

Deliberate. The deployment is reached via `mcp.spicy-regs.dev` and per-deploy Cloud Run
`*.run.app` hosts; FastMCP's default localhost-only allowlist would reject all
of them with **421**. The server is public, stateless, and read-only, so
rebinding protection buys nothing.

## Other invariants

- `ICONS` uses a base64 `data:` URI, not an `https://` URL, so it works on both
  the stdio and HTTP transports. Generated by `scripts/gen_icon.py`.
- `_resolve_catalog_config` and `_resolve_r2_base_url` reject quotes,
  backslashes, and control characters outright rather than escaping them, because
  the values are inlined into `CREATE SECRET`/`ATTACH`/`read_parquet`, which take
  no bind parameters.
- `R2_CATALOG_NAMESPACE` uses `or DEFAULT` rather than `get`'s default argument so
  an env var set to the empty string falls back to `default` instead of `""`.
