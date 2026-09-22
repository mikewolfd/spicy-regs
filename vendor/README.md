# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.26.3`: locally built from isolated source commit
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
