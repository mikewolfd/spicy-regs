# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.33.1`: built from SpicyDocs `main` at `4503f59`, with Rulespec Artifacts 1.1.1.
  Built September 25, 2026 UTC: **1,467,262 bytes**, SHA-256
  `4b2e99ea8dd57b9de4e2a4827f1e1d965f16b703b840cd4c073d59cf7893f482`, byte-identical across two
  rebuilds from a clean archive of that commit. Mirrulations agency listings run as concurrent
  contiguous docket ranges with listing timestamps left unparsed; the keys returned are identical
  to the serial listing (FAA checked per record type). Four agencies listed together took 133 s
  instead of 449 s on the public mirror. No rule version changes.

- `spicy_docs-0.33.0`: built from SpicyDocs `main` at `881f6f5`, with Rulespec Artifacts 1.1.1.
  Built September 25, 2026 UTC: **1,466,243 bytes**, SHA-256
  `9021a9468de3ddd501b6bf2477b0e1a3a0e954df839fb79a8f625a26f3fdeb3f`, byte-identical to a
  rebuild from a clean archive of that commit. It carries the September 25 audit repairs.
  Table III keeps a data row whose act-section label is blank (reader rule
  `table3-native-rows-v2`; act 119-37 had four such rows the old reader dropped). `laws`
  states every PLAW read (`captured_partial`, `captured_refused`, `request_failed`) and
  appends `uslm_citable_as_json`, `uslm_reason` and `uslm_reader_version`; a file stating
  another law raises `UslmIdentityError`. Hearings keep every native `heldDate`
  (`held_dates_json`; `held_date` only when one date is stated; link rule set
  `d06e0bd80ca1`), and `committee_reports`/`hearing_transcripts` append
  `body_completeness` and `text_derivation` for GovInfo PDF notices. The host re-reads
  under the new rule versions; the source gate and reviews are in
  `~/Work/corpora/fork-execution-2026-09-21/repair-execution-2026-09-25/`. This is a
  local package adoption; public tables require a subsequent run.

- `spicy_docs-0.32.1`: built from clean source commit `4310647`, with Rulespec Artifacts 1.1.1.
  Built September 24, 2026 UTC: **1,463,602 bytes**, SHA-256
  `09d96e176a2aa6bf82d15bb37aeff63e13db78354303798fafde732f396b6c85`.
  Every packaged source file equals the clean Git archive. The patch repairs
  numeric schedule labels before Public Law citations and wrapped CFR headings,
  collapses subpart-letter lists to one part link, rejects RIN-shaped fragments
  inside slash tokens, and refuses links to damaged U.S. Code prefixes while
  retaining their source occurrences. The changed citation rules are
  `public_law`, `cfr_section`, `usc_section` 003 and `rin` 004.
  The meeting-detail reader accepts the publisher's `nochamber` address and
  keeps that value in the table identity. The package also carries the already
  committed documentation clarifications for IRI ownership and lone `-pt1`
  reports. The source gate and retained-corpus comparisons are recorded in
  `~/Work/corpora/supply-2026-09-02/receipts/a10-grammar-followup-2026-09-24/`;
  the installed-reader Congress check is in
  `~/Work/corpora/fork-execution-2026-09-21/congress-index-fixes-2026-09-24/`.
  This is a local package adoption; public tables require a subsequent run.

- `spicy_docs-0.32.0`: built from SpicyDocs `main` at `549db06`, with Rulespec Artifacts 1.1.1.
  Built September 24, 2026 UTC: **1,462,674 bytes**, SHA-256
  `dffbcbfba8e0d857534539b803589163d42f601bc3635e6361062e07bdecd4f4`, byte-identical to a
  rebuild from a clean archive of that commit. A multi-part committee report is one row per
  part (B31, decision 29, confirmed 2026-09-24): `committee_reports` is keyed
  `(package_id, part_id)` with `part_number` beside it, `report_sections` is keyed
  `(package_id, part_id, seq)`, `GovInfoBodyAcquirer.acquire_parts` reads every part of a
  package or refuses it (`GovInfoPartsOverBudgetError` before any body request), and
  `REPORT_SECTION_READER_VERSION` moves to `report-headings-002`. The shapers refuse the old
  call, so a host adopts this in the release that vendors it: pass `part_id`, backfill prior
  rows, replace a package's part rows as a set, and size the body budget per part. Also the
  OLRC Table III chain walk (`sources.uscode.iter_table3_chain`; B32), which reads absence from
  the chain of pages rather than from a page's bytes, and `retain_dropped_body` on keyless
  routes, which keeps a dropped answer's bytes as `response-incomplete` refusal evidence.
- `spicy_docs-0.31.0`: built from SpicyDocs `main` at `7b65900`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,456,368 bytes**, SHA-256
  `87ef3880ca2b79727a0f25d7eedce699c0ec42e2d31527c46bcf424d631e7d5c`. Adds the canonical
  citation grammar, identifier shapes and IRI minters, moved from RefSpec
  (`interpretation.citation_grammar`, `identifier_shapes`, `iri_minting`; grammar changes
  move rule versions, pinned by a frozen specimen fixture, and the docket reader admits the
  `-RULE`-family suffixes); `usc_section_key` appended to `law_code_sections` and
  `table3_records` (decision 30); pooled enumeration by corroborated identity
  (`pool_walks`, `CongressListingReader.pooled`, typed `DeclaredCountMismatch`,
  `DeclaredCountChanged` and `IncompleteWalkError`; B6); the Unified Agenda field-path
  projection (`project_unified_agenda_edition`; B11); the Regulations.gov Eastern day
  (`regulations_gov_day`) and Congress.gov day windows (`utc_day_window`; A13); an annual
  CFR validator that admits the combined Title 34/35 and appendix-only volumes, and GovInfo
  discovery that refuses uncounted pages and repeated ids (A14); a double section sign
  before one number read as one section (A8); and a SAM extract download that asks for
  gzip and waits by wall clock (`max_wait` seconds replaces `poll_max`), without which every
  scheduled run had failed with HTTP 406.
- `spicy_docs-0.30.0`: built from SpicyDocs `main` at `4b13c0d`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,299,791 bytes**, SHA-256
  `02c824eb25ff171f55c3a23967f0833422eafa4b9e54bcae354ba7247f7e49c2`. Adds CFR section ancestry from annual
  volume headings (`scan_annual_cfr_sections`, `split_annual_cfr_section`), the roll-call
  vote day (`vote_day`, appended to `roll_call_votes`) and the Mirrulations derived-text
  reader with per-attachment provenance (`list_docket_derived_text`,
  `fetch_derived_text`). Otherwise identical to 0.29.0.
- `spicy_docs-0.29.0`: built from SpicyDocs `main` at `58a68b7`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,289,687 bytes**, SHA-256
  `78e31d490c238e47503377f3343c95ab852ce1ebb235a73fab9ea79b35e6978d`. Adds the
  Congress.gov amendment detail route (with an `amendment_type` parameter), which
  states each amendment's sponsor and amended bill or amendment. Otherwise identical
  to 0.28.3.

- Previous `spicy_docs-0.28.3`: built from SpicyDocs `main` at `8474b5c`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,289,422 bytes**, SHA-256
  `3740987ec6d195752c8f5080e12c1c2e335de630e0b1c7fcdfc585a2223467f0`. The comment
  acquisition policy is version 1.3 now that identical same-instant re-observations
  collapse; a published 1.2 comment release (the six-agency cohort) replays with
  0.28.0 or earlier, retained below. Otherwise identical to 0.28.2.

- Previous `spicy_docs-0.28.2`: built from SpicyDocs `main` at `6d655ed`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,289,256 bytes**, SHA-256
  `0916c0f98bc5857e88d67155ab3f4617d367fa827d6aad86975509a2f3fd2cdc`. SAM extract
  registrations are keyed by UEI and EFT indicator, a repeated registration keeps its
  newest version, and the file's declared count is a floor (measured on the first real
  registration-year extract). Otherwise identical to 0.28.1.

- Previous `spicy_docs-0.28.1`: built from SpicyDocs `main` at `ac102e4`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,288,625 bytes**, SHA-256
  `cc59ca71354a61b5a9e88726986a970ee0fd2e010904be87a48631615397e0f1`. Identical
  comment re-observations at one instant now collapse to one observation, as
  dockets and documents already did; the ACF comment census found 23 such pairs,
  all byte-identical. Otherwise identical to 0.28.0.

- Previous `spicy_docs-0.28.0`: built from SpicyDocs `main` at `767c945`, with Rulespec Artifacts 1.1.1.
  Built September 23, 2026 UTC: **1,288,534 bytes**, SHA-256
  `7456db35b358e3f648796b795b88817cb6ec63d23f2026292dbecd87ad4bc611`. Appends
  `congress_bills.url_source` (who stated `url`), the label delivery decision 1
  requires on inherited URLs, and fixes the SAM bulk extract so it keeps its
  selection filters, reads the publisher's sentence trigger and polls through
  its `FSP` in-progress answer (all measured against the live API). DocSpec,
  RefSpec, SpicySearch and SpicyEngine still pin 0.26.6. Qualified by the
  SpicyDocs and SpicyRegs gates.

- Previous `spicy_docs-0.27.0`: built from SpicyDocs `main` at `1b4285a69428364cef0ee3349f4b68aaaf4b50ef`, with
  Rulespec Artifacts 1.1.1. Built September 23, 2026 UTC: **1,287,487 bytes**,
  SHA-256 `c6133e34c646378bf9f133395f13f376ae80d2e2b77c79645bfbd6ab703e2029`. Its
  `courtlistener-local` extra (which this host installs) adds
  `CourtListenerLocalDump`, which parses a retained CourtListener export in
  record-aligned pieces with DuckDB under per-piece count and reference checks
  (the 54.6 GB opinions export in 28 minutes), plus parallel decompression and a
  faster strict decoder for the streaming reader; `us_reports_cite` now points at
  `court_citations`. DocSpec, RefSpec, SpicySearch and SpicyEngine still pin
  0.26.6; nothing they use changed. Qualified by the SpicyDocs and SpicyRegs gates.

- Previous `spicy_docs-0.26.6`: built from SpicyDocs `main` at `6673fa3413cb8887057ac2b262dc4b0b2bb88083`, with
  Rulespec Artifacts 1.1.1. Built September 22, 2026 UTC: **1,280,470 bytes**,
  SHA-256 `ce319f2a068881abc46cc0e6eb102b318c4150f081d8497daf002214fe75bddb`. It carries the SAM bulk-extract and adaptive windowed
  walks, caller headers on capture and the cached capture digests and parquet
  footers. Its 271 packaged source files are byte-identical to the previous
  `spicy_docs-0.26.5` entry below; only the version and the Rulespec Artifacts
  pin change. Retained for replay.

- `rulespec_artifacts-1.1.1`: exact dependency of SpicyDocs 0.26.6 and 0.27.0, built from
  Rulespec `a3acb04cbfe2cc32a89622a3523da48aa6958348`. **99,315 bytes**, SHA-256
  `63ad763f5e5f13ddba571225503c6ad7a4aa2bd7ea5d818e209fbf832285c8c8`. Identical code, schemas and canonical encoder to 1.1.0; it ships
  the platform fixtures regenerated by the pre.19 release-fixture repair.

- Previous `spicy_docs-0.26.5` (as vendored here): **1,280,470 bytes**, SHA-256
  `1c10096986228c3fe5a1b3e759643b85c04a1548b27b843b7cd83680c03cf32b`. Despite its label it is not the
  `1dc77011d79eeef26024396626a1200059a6e09a` release (SHA-256
  `c17453a827f45c514196ac8dace414a4f50b6f8859b5fb8f2d0153d1a32e3a5d`) other
  consumers pinned: every packaged source file matches SpicyDocs `main` at
  `54ddbea`, six commits later, including the SAM bulk-extract path this host
  adopted. SpicyDocs 0.26.6 now names that code. Retained for replay.

- Previous `rulespec_artifacts-1.1.0`: exact dependency of SpicyDocs 0.26.5, built from Rulespec `dba6c0a`.
  SHA-256 `3b2abcdcfa082f34baa3b03042c54fcc4e5e713901cd505cbd777dfd9bdf23cd`. Adds opt-in DocumentCapture v2; v1 parent,
  meta-schema and validator bytes are unchanged.

- Previous `spicy_docs-0.26.4`: locally built from isolated source commit
  `c9ce7c0f0d9f376bc8b2b71697e3e72867d28e5f`, extending the prior wheel with
  the reviewed House select-committee code correction. Built September 22,
  2026 UTC: **1,325,983 bytes**, SHA-256
  `626af49d42a208c58c3d6919ff03e064f88d249cf936702c7b7ce86a1eef51ac`.
  Only `sources/congress/committee_rosters.py` and `schemas/roster_tables.py`
  change inside the package. All other source/resources, entry points and
  dependency metadata remain unchanged; wheel RECORD hashes and sizes pass.

  Native House committee type and parent context determine the `hl` prefix
  for select committees and their subcommittees. Literal source codes survive.
  The full retained roster replay repairs 110 assignment codes and 52 parent
  codes while preserving every native identity and other source value. Joint
  committee aliases and the unmatched Senate inaugural observations remain
  explicit. Source main carries the same fix at `64bd903`; this wheel preserves
  the earlier adopted package's other bytes. Exact package, source and host
  qualification receipts are in
  `~/Work/corpora/fork-execution-2026-09-21/rosters-qualification/select-code-fix/`.
  This is a local package adoption, not a registry release or data publication.

- Previous `spicy_docs-0.26.3`: locally built from isolated source commit
  `8f5cddadb2a17e5b9c9c923bd6c520e1a5bd729f`, extending the prior wheel with
  the reviewed native vote variants. Built September 22, 2026 UTC:
  **1,325,593 bytes**, SHA-256
  `06123ae683001ad4051faf72c1d91b658d589856a1c4272ddddbdaeb4cb3a082`.
  Only `sources/congress/votes.py` and
  `schemas/congress_activity_tables.py` change inside the package; other
  packaged source and dependency metadata remain unchanged.

  Speaker elections preserve candidate tallies and literal member choices.
  Senate votes preserve every ordered document and amendment, including
  nomination identifiers. New nullable vote columns carry these source facts;
  older rows retain their existing values. Candidate totals never become
  ordinary yea/nay counts. Source, host, installed-package and retained-input
  checks are in
  `~/Work/corpora/fork-execution-2026-09-21/native-vote-variants-adoption/`.
  Complete acquisition and public rollup qualification remain separate work.
  This is a local package adoption, not a registry release.

- Previous `spicy_docs-0.26.2`: locally built from isolated source commit
  `aa2ad435df37d40e119106436d9300c4f14935ef`, extending the prior wheel source
  with the reviewed congressional vote identity and press mention fixes.
  Built September 22, 2026 UTC: **1,324,304 bytes**, SHA-256
  `a3f8f0c33125fcc962d23459b712702792bd974bc8b826fbd3106c05e8287f19`.
  Only `interpretation/vote_matching.py` and
  `interpretation/release_matching.py` change inside the package. Every other
  packaged source file is byte-identical to 0.26.1, including the court readers
  used by the running bulk audit. Metadata changes only its version/digests;
  dependencies are unchanged.

  House vote identity no longer requires a bill relationship. The host uses
  it to acquire procedural votes and adopts the existing Senate menu reader.
  Press matching rejects a possessive budget year while preserving quoted
  Senate bill citations. These code fixes do not establish complete vote
  acquisition or historical coverage. Exact source/wheel comparisons and
  installed-package checks are in
  `~/Work/corpora/fork-execution-2026-09-21/congress-vote-press-adoption/`.
  This is a local package adoption, not a registry release. Prior wheels remain
  retained for replay.

- Previous `spicy_docs-0.26.1`: locally built from isolated source commit
  `9f57934561e925dbc0447594534c0a583de727c2`, based on the prior wheel's
  `b618b922897a0b462e8d43737819288a369cf23c` source plus the reviewed CourtListener
  count fix (`11585b26f515148388b6751e9839b0f48fb2cc66` on source main).
  Built 2026-09-21: **1,324,207 bytes**, SHA-256
  `09888e7967c29fd4dea3bd48300f72e087e288f2747f929e42486631bd521d4a`.
  The only changed packaged source files are `reading/paged_json.py` and
  `sources/courtlistener/search.py`; all other packaged source is byte-identical
  to 0.26.0. Package metadata changes only the version and RECORD digests.

  Docket counts above 2,000 may be estimates; complete walks follow explicit
  terminal cursors. Smaller docket counts and opinion counts retain exact
  checks. Missing cursors/counts and empty searches with positive counts refuse.
  The host shares the source count policy and validates terminal pages before
  applying a requested record cap. Both source-reader pins and the wheel path
  move together; no dependency was added. The isolated source gate passed.
  Comparison, provenance and installed-wheel checks are retained in
  `~/Work/corpora/fork-execution-2026-09-21/courtlistener-count-adoption/`.
  This is a local package adoption; no registry release or new data publication
  occurs. Prior wheels remain retained for replay.

- Previous `spicy_docs-0.26.0`: locally built from isolated source commit `b618b92`,
  2026-09-21. **1,323,662 bytes**, SHA-256
  `31780875a7a9a5afd1b5ec93d7c987720e8e6a203365308ad8c13318df2ece23`.
  Adds the source-owned ordered FEC bulk dictionary reader. The caller selects
  a retained dictionary; the reader checks positions and preserves literal
  cells and byte evidence. No dependency was added.

  Compared with the previous wheel, 294 existing packaged source files are
  byte-identical. Three previously committed schema modules correct digest
  ordering descriptions and reject non-finite or circular JSON values. No
  unrelated uncommitted source edits entered this wheel. The isolated source
  checks passed **7,273 tests**, with four skipped and 46 deselected; lint and
  formatting checks passed. Receipts and the exact wheel comparison are in
  `receipts/fec-dictionary-adoption-2026-09-21/` under
  `~/Work/corpora/supply-2026-09-02/`. This is a local adoption, not a registry
  release. The prior wheel remains retained for replay.

- Previous `spicy_docs-0.25.0`: locally built from source commit `1c86f16`,
  2026-09-21. **1,321,425 bytes**, SHA-256
  `3bd52a8916e8c470111dd31bcdc6cb7fa7e41c5b70011b567b78a7c1b58a5745`.
  Both reader pins and the uv source move together; the lock holds this digest.
  This local package has not been released to an external registry.

  The installed registry contains **39 tables over 814 columns**. It adds the
  literal `report_sections.heading` column after the existing twelve and leaves
  unresolved agency identities NULL. `REPORT_SECTION_READER_VERSION` participates
  in the host's CRPT processing checkpoint alongside the CBO rule and installed
  package version. Corrected reads replace all old sections for that package;
  failed reads retain prior rows and remain retryable. This repairs unchanged
  publisher records without requiring a new publisher timestamp.

  The wheel also adopts the native BILLSTATUS cosponsor-count correction and
  the installed Rulespec capture validator. The source gate passed 7,255 tests;
  all 264 packaged source files matched the reviewed checkout. Native host
  replay and installed-wheel checks are retained at
  `receipts/remediation-sprint-2026-09-21/source-reader-adoption/` under
  `~/Work/corpora/supply-2026-09-02/`. Installing corrected readers does not
  backfill the full bill corpus or replace public data.

  It replaces 0.24.2 (`965776e`, 1,333,353 bytes,
  `cf84fb9f5ede4a53d1dca078759297b08ca30c63c82eb6343ccf53142caf72b1`).
  Prior source/API additions and provider choices remain in Git history and
  their source guides; no new acquisition family is enabled by this adoption.

- `rulespec_artifacts-1.0.14`: exact dependency of SpicyDocs 0.26.1.
  **82,881 bytes**, SHA-256
  `f09aaf4525af3ac243d04695a8700464f06019653ac6f8fb9ed235ff0694af0b`.
  Built from Rulespec `21693e0a`; byte-identical to the SpicyDocs vendored wheel.
  It ships the parent capture schema, profile meta-schema and shared invariant
  validator. The source consumer removes its copied schemas/checker and uses
  the installed owner, including parent ordering and unique-node checks.
  SpicyRegs keeps this as a transitive dependency; raw source strings do not
  become `DocumentCapture` objects without an explicit conversion.
- `deltatrack-0.1.0`: built with `uv build --wheel` from `civictechdc/DeltaTrack`
  commit `c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6`, 2026-09-19.
  SHA-256: `7f060e30af9702f4e45c305fa93c53d1e3e59bd70858d3c6717f6fc9cf825197`.
  DeltaTrack is a git dependency (SpicyDocs' `bill-diff` extra pins
  `git+https://github.com/civictechdc/DeltaTrack?rev=c636448…`), not a
  registry package, so it is vendored the same way as the other source-reader
  wheels here rather than resolved transitively. It exists for the bill-diff
  tables (`section_diffs`, `section_diff_items`, `financial_changes`) that
  `docs/research/table-contracts-2026-09-19.md` §5.4 describes, and the 0.21.x
  pin reaches them. `[tool.uv.sources]` alone was
  not enough: a uv source binds a dependency the project declares, and
  DeltaTrack arrives only through spicy-docs' `bill-diff` extra, so uv looked
  for it in the registry and failed the resolve. `deltatrack==0.1.0` is
  therefore also declared directly in both `source-readers` lists — the same
  shape `rulespec-artifacts` already had for the same reason.
- `uv.lock` records every wheel SHA-256 above. Replace the wheel and refresh the
  lock together, then run receiver tests against the installed wheel.

PDF enrichment (`transforms/pdf_text.py::extract_pdf_text`, the regulations.gov
attachment path) uses the narrow `pdf-pypdf` provider extra through
`source-readers`. SpicyRegs additionally pins pypdf `6.14.2` and checks that
version before parsing; package installs and checkouts therefore use the same
qualified PDF behavior.

The GovInfo *body* path is different: `extraction.body_text`'s PDF branch
(both `build_bill_family.py` and `build_committee_reports.py`) runs through
`body_text`'s default extractor, `DocumentExtractor(NativeText())`, whose
default reader opens PDFs with PyMuPDF — now installed via the `pdf` extra —
rather than through a pypdf adapter. This is not a licensing call; the user's
ruling is that results decide, not licensing (PyMuPDF is AGPL/commercial
dual-licensed; pypdfium2, which DeltaTrack itself uses, is BSD/Apache — the
same spicy-docs research doc measured it too and found it handles the two
genuinely GPO-numbered fixtures as well as PyMuPDF, but introduces false
hyphen rejoins on non-numbered layouts that spicy-docs's own extractor
declines; adopting it is a spicy-docs-side question, out of scope here). It is
a correctness one: `extraction/gpo_normalize.py`'s layout detector was derived
against PyMuPDF's line-grouped text, where a GPO gutter line number comes back
as its own physical line immediately after its content line. pypdf glues that
number onto the end of the content line instead (`Representa-1`), so the
detector's adjacency check never fires — measured on real GovInfo PDFs in
spicy-docs `docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`: 0 of 6,
then 0 of 12, real gutter numbers correctly detected under pypdf, against 6 of
6 and 12 of 12 under PyMuPDF, on the same two documents. Under pypdf, three of
the normalizer's rules — layout detection, hyphen rejoin, and the
gutter-evidenced half of the bare-digit strip gate — are structurally
unreachable on every PDF-derived GovInfo body, not just occasionally
degraded. Routing this path through spicy-docs's default extractor instead of
`transforms/pdf_text.py::PypdfPageExtractor` (removed) is what restores it.
`pdf-pypdf` stays in `source-readers` only because `extract_pdf_text` still
needs it for the unrelated attachment path above.

CourtListener listing, pins and raw rows use the shared provider directly. Run
`uv run pytest tests/test_courtlistener_bulk.py tests/test_courtlistener_shared.py tests/test_court_scope.py tests/test_cluster_court_scope_backfill.py`
for this receiving change. Before replacing the provider wheel, also run the
BILLSTATUS, Unified Agenda and PDF reader tests (`test_bill_subjects.py`,
`test_unified_agenda*.py`, `test_pdf_text*.py`).
