# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.25.0`: locally built from source commit `1c86f16`,
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

- `rulespec_artifacts-1.0.14`: exact dependency of SpicyDocs 0.25.0.
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
