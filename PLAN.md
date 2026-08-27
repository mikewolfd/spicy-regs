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
  explicit Regulations.gov agencies, one explicit persistent `--blob-store`,
  injected acquisition adapters, and machine-readable pins without adding
  catalog semantics or hidden sibling state.
- Source-native records, renditions, acquisition rows, and evidence use
  Rulespec external `blobRef` members in an injected shared content-addressed
  store. Fixed 64-way SHA-256 identity buckets keep unchanged payload refs
  exact, while the closed receipt accounts for payload bytes read, reused, and
  actually written plus local publication bytes. Publication verifies
  `EEXIST`, never replaces a root, and safely reuses verified orphan blobs.
  Directory-relative file operations keep the store inside its selected root
  even if an internal path is replaced. Acquisition-page inventories use page
  identity rather than shared evidence-byte identity.
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
2. Run the pinned Federal Register baseline comparison and the retained public
   data differential, then measure cadence, memory, temporary disk, and changed
   output bytes at the 3.9 GB scale target.
3. Close the predecessor only after the central roster assigns and verifies the
   active app, schema-docs site, MCP endpoint, rollups, and complementary source
   tables. Do not pull those cross-source and search surfaces into SpicyRegs just
   to make the old checkout disappear.

## Current gates

- Verified locally on 2026-08-26: 95 focused source-native and public-table
  tests pass, including exact unchanged-bucket reuse, changed-bucket-only
  writes, physical-only rebuilds, concurrent publication, orphan recovery,
  descriptor-relative containment guards, identical-evidence page ownership,
  and the fixed stream bound. Targeted `ty`, repository-wide Ruff, lock
  consistency, and `git diff --check` pass.
- Candidate SpicyRegs 0.1.7
  (`dist/spicy_regs-0.1.7-py3-none-any.whl`, 1,145,559 bytes,
  `sha256:b8c2f9ea3a7f44dbd1c373f4ba3902371ff1664ac55776d5f01372091f97f3ec`)
  and vendored Rulespec artifacts 1.0.9
  (`vendor/rulespec_artifacts-1.0.9-py3-none-any.whl`, 56,820 bytes,
  `sha256:67cb33bf63c11bc6812ad0e8f0a8b73e89501fa6d4242acf75a7cc6612f5d6c6`)
  are retained locally. The exact 95 focused tests and static gates pass against
  Rulespec 1.0.9. A fresh isolated Python 3.12 environment installed those wheel
  bytes, resolved versions 0.1.7 and 1.0.9, and passed the Federal Register CLI
  publish and independent-verify smoke test. DocSpec 0.2.5's installed-wheel
  catalog proof and SpicySearch 0.1.4's three-test installed-package and
  source-native proof now also pass against this exact SpicyRegs wheel. All of
  those package bytes and results remain local.
- Before the final Rulespec 1.0.9 repin, the same SpicyRegs 0.1.7 source passed
  the configured full suite except for 3 optional-model failures: it reported
  3,596 passes, 15 skips, 5 deselections, and 2 expected failures because
  `transformers`, `sentence-transformers`, and `torch` are absent from the base
  development environment. The final 1.0.9 dependency set is covered by the
  exact 95-test focused gate and installed-wheel smoke above; rerun the full
  suite before attributing its complete result to those final package bytes.
- The focused public-profile gate covers all four stable schemas, bounded
  Parquet/Hive members, newest comments, local DuckDB reads, hermetic anonymous
  HTTP range reads, immutable publication, tamper and duplicate refusal, and an
  injected Iceberg first snapshot. A live object-store and catalog run remain
  cutover gates, not local claims.
- Focused source-native fixtures must cover success, tamper, incomplete
  acquisition, capped-window split, capped-day refusal, source-schema drift,
  exact enumeration, separated document/docket facts, CLI failure, and
  immutable no-replacement behavior. Small-update fixtures also cover exact
  partition-ref reuse and truthful store-write accounting.
- The live Federal Register status check is one bounded day. It verifies current
  source shape and replay only; it does not establish corpus-wide completeness.
- Run `uv run --frozen ruff check .`, focused `uv run --frozen ty check ...`,
  `uv lock --check`, `git diff --check`, and a freshly built wheel import and
  publish/verify check before calling the local slice complete.
- Do not publish, push, deploy, or retire the predecessor public path from this
  worktree. Git records completed work; this file carries only current state and
  remaining gates.
