# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.24.2`: SpicyDocs patch release (release commit `965776e`),
  2026-09-20. Verified before and after copying: **1,333,353 bytes**, SHA-256
  `cf84fb9f5ede4a53d1dca078759297b08ca30c63c82eb6343ccf53142caf72b1`.
  Both pins and the uv source move together; `uv.lock` records the same digest.
  Replaces 0.24.1 (`fc8171d`, 1,321,932 bytes, `ce25270b…1fa6`).

  No contract moves: the registry imported from this wheel is **39 contracts,
  813 columns**, the same as 0.24.1. The patch carries two fixes this
  repository consumes without a code change of its own. The shared retry now
  scrubs an exception before it logs it and before it truncates it, so a
  credential in a query string cannot stand in a log line that a later
  successful attempt leaves behind; and a Mirrulations object rejected on its
  advertised length has its body closed, which it did not before, so repeated
  oversized objects can no longer hold connections open. Both came from the
  duplication audit's correctness counterexamples in spicy-docs
  `docs/research/dry-audit-2026-09-20.md`.

  Independent imports from both wheel archives establish **39 contracts,
  813 columns**, with every contract identical: column order, identity,
  version column, grain and descriptions. The comparison and registry dumps
  are retained in
  `receipts/spicy-docs-0-24-1-adoption-2026-09-20/contracts-comparison.json`
  under `/Users/mikewolfd/Work/corpora/supply-2026-09-02/`.
  The Mirrulations reader now requires a nonblank `data.id` string for all
  three ingested types and rejects publisher error bodies. This matches the
  host's identity requirement and adds error rejection, so both ingestion
  paths use the supplier reader directly. Publisher reasons remain scrubbed;
  regression tests retain the missing-identity and retry cases.

  The preceding 0.24.0 adoption grew the registry from 37 contracts and 766
  columns. It added `hearing_bill_links` (12 columns, key
  `package_id, bill_id, link_source`) and `cbo_cost_estimates` (16, key
  `bill_id, publication_id`). Appended: `house_communications` +5 provenance
  columns, `committee_reports` +13 CBO letter columns, `congress_bills` +1
  estimate outcome. Existing prefixes, keys and version columns are unchanged.
  Prose also changes on `budget_volumes`, `hearing_transcripts` and
  `house_activity_reports`; every other contract is identical.

  Seven modules added and twelve changed; the complete wheel and registry
  comparison is retained in
  `receipts/rollups-0-24-0-adoption-2026-09-20/contracts-comparison.json`.
  The release supplies the Mirrulations refusal/retry reader, activity-report
  selection, PDF-first print preference, thirteen measured BUDGET parts,
  hearing cover/agenda rules and repository reader, CBO index and letter rule,
  and Record communication reconstruction. Hosting adopts these public
  functions; the Record acquisition backfill is a separate rollup.
  `shape_hearing_transcript` no longer accepts `bill_id`: hearing relationships
  are one-to-many and live in `hearing_bill_links`.

- `rulespec_artifacts-1.0.13`: required by spicy-docs 0.24.2, which pins it
  exactly; 1.0.12 no longer resolves. Nothing here imports it — it is a
  transitive pin vendored under the same discipline. Byte-identical to the
  wheel `rulespec` itself built (`dist/artifacts/`) and to the one spicy-docs
  vendors. SHA-256:
  `72d15ff9453bb819ab5945141dea92cae6c3ff5f76e1d26bb04849cf690bf377`.
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
