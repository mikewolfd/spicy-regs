# SpicyRegs plan

## Goal and boundary

Publish faithful source-native regulatory records as one immutable,
digest-pinned `spicyregs-source-native-release` that scales from local runs to
distributed execution. Rulespec owns the generic artifact bytes and admission;
SpicyRegs owns source acquisition and raw source meaning; DocSpec owns catalogs,
cross-source joins, and document processing; SpicySearch owns indexes and search.

The sole format authority is
[the source-native release specification](docs/superpowers/specs/2026-08-25-source-native-release-spec.md).
SpicyRegs keeps no private catalog format, duplicate artifact framework,
worktree hash, mandatory RefSpec runtime, or DocSpec policy.

## Implemented in the current worktree

- The common publisher, semantic verifier, and bounded reader use the injected
  `SourceNativeProfile` and the shared `rulespec-artifacts` API. Source profiles
  do not register themselves or import each other through the common core.
- The build caller cannot author a snapshot token or completeness scope.
  Federal Register releases are always `observed-crawl`; Regulations.gov may
  claim `complete-snapshot` only through its exact source enumeration proof.
- Federal Register acquisition uses bounded date windows, preserves every exact
  response, recursively splits a 10,000-result capped window, keeps capped
  probes as non-record evidence, refuses a capped single day, and accepts only
  two consecutive identical traversals.
- Regulations.gov documents and dockets publish as separate releases. The
  Mirrulations reader captures sorted membership plus source ETag, version ID,
  byte size, and ETag-pinned object bytes; the verifier refuses incomplete or
  changed evidence. Raw document and docket schemas preserve source join keys,
  withdrawal facts, file formats, topics, relationships, and explicit nulls
  while failing on unclassified fields or incompatible types.
- Regulations.gov comments publish as a third separate release with the same
  exact enumeration proof. Its closed raw schema preserves every classified
  comment and attachment field and every source-stated rendition. The injected
  profile publishes one row per `comment_id` by `modifyDate DESC NULLS LAST`,
  refuses version ties, and accounts for every older observation as discarded.
- One `spicy-regs-source-native` command publishes or verifies Federal Register,
  Regulations.gov documents, dockets, and comments. It accepts bounded dates,
  explicit Regulations.gov agencies, injected acquisition adapters, and
  machine-readable pins without adding catalog semantics.
- The package base path no longer needs RefSpec, RDFLib, DocSpec, or the former
  Rulespec conformance runtime for source-native publication.
- One injected public-table producer derives immutable, Rulespec-admitted
  Parquet generations for Federal Register, Regulations.gov documents, dockets,
  and comments. It preserves the established source columns, bounds batches and
  members, partitions comments by Hive `agency_code`, and refuses duplicates,
  schema drift, tamper, and destination replacement.
- The thin reader gives exact admitted members to DuckDB for local or anonymous
  range access. An injected new empty PyIceberg table can adopt those same files
  in one standard snapshot. SpicyRegs adds no table registry, latest-pointer
  algorithm, mutable upsert path, row hash, or file inventory.

## Remaining priorities

1. Publish one generation through the normal artifact/object store and a real
   Iceberg catalog, run live immutable HTTPS range and retained-data differential
   checks, then cut each known anonymous consumer over. The predecessor remains
   required until this happens; source-native document/comment views also leave
   derived text null, so DocSpec must supply that value before text-dependent
   consumers move.
2. Prove clean-wheel interoperability through Rulespec admission, the DocSpec
   source-native adapter, and the SpicySearch task-level fixture. Remove any
   remaining predecessor path only after the replacement reproduces its named
   user behavior.
3. Run the pinned Federal Register baseline comparison and the retained public
   data differential, then measure cadence, memory, temporary disk, and changed
   output bytes at the 3.9 GB scale target.
4. Close the predecessor only after the central roster assigns and verifies the
   active app, schema-docs site, MCP endpoint, rollups, and complementary source
   tables. Do not pull those cross-source and search surfaces into SpicyRegs just
   to make the old checkout disappear.

## Current gates

- Verified locally on 2026-08-25: 76 focused source-native tests and 11
  source-package boundary tests pass; the pinned one-day Federal Register live
  replay passes; targeted `ty`, repository-wide Ruff, lock consistency, and
  `git diff --check` pass. A freshly built wheel imports and publishes/verifies
  from an isolated environment with no RefSpec, RDFLib, Rulespec conformance,
  DocSpec, or sibling checkout on its import path.
- The focused public-profile gate covers all four stable schemas, bounded
  Parquet/Hive members, newest comments, local DuckDB reads, hermetic anonymous
  HTTP range reads, immutable publication, tamper and duplicate refusal, and an
  injected Iceberg first snapshot. A live object-store and catalog run remain
  cutover gates, not local claims.
- Focused source-native fixtures must cover success, tamper, incomplete
  acquisition, capped-window split, capped-day refusal, source-schema drift,
  exact enumeration, separated document/docket facts, CLI failure, and
  immutable no-replacement behavior.
- The live Federal Register status check is one bounded day. It verifies current
  source shape and replay only; it does not establish corpus-wide completeness.
- Run `uv run --frozen ruff check .`, focused `uv run --frozen ty check ...`,
  `uv lock --check`, `git diff --check`, and a freshly built wheel import and
  publish/verify check before calling the local slice complete.
- Do not publish, push, deploy, or retire the predecessor public path from this
  worktree. Git records completed work; this file carries only current state and
  remaining gates.
