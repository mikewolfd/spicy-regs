# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.21.3`: built with `uv build` from SpicyDocs commit
  `083b536` (tag v0.21.3), 2026-09-20. SHA-256:
  `1f91bb70bc28a035fbeb7ca921dd3bb2b58fdc7e2fb353747dad2e365978f84b`
  (1,181,012 bytes). A narrow release: unzipping both wheels and diffing them
  shows exactly three modules changed and one data directory added, and
  **the thirty-two table contracts are identical** — same 617 columns, same
  identity, version column, grain and per-column prose, compared field by
  field rather than inferred from the release note. `spicy-regs-dict
  generate` accordingly moves nothing. What changed:
  - **The prompt-shape fix** (`interpretation/model_call.py`,
    `bill_summaries.py`, `section_classification.py`): each model-backed
    module declares its answer's keys, types and counts once as `AnswerField`
    records; `answer_shape_block` turns that declaration into the lines the
    prompt sends and the reader looks values up through the same records, so
    neither side can name a key the other does not. `PROMPT_VERSION` is `v2`
    on all three prompts. Aliases (`top_provisions`, `section_id`) and the
    `classifications` wrapper stay readable but are not offered. This exists
    because the `v1` summary prompt asked for its three items in prose and
    named none of the JSON keys: the first live call answered
    `most_affected_audience` and `notable_provisions`, and **a keyed
    production run here would have published zero `bill_summaries` rows**
    (this repository's own C1 measurement; PLAN.md records it).
  - **`schemas/document_capture/1.0/`**, a data directory of DocumentCapture
    v1 schemas, six capture profiles and a rulespec module. It registers no
    table contract and nothing here reads it.

  What this repository does with the fix: nothing in `transforms/model_call.py`
  moves — the adapter already asks for `responseMimeType: application/json`,
  which is the half that was right. `tests/test_bill_family.py` gains the
  stubbed client the readers were never run behind, so the `v2` stamp on a
  produced row and the refusal of the measured `v1`-era answer are both
  asserted here, and `transforms/build_bill_family.py` wraps the three seams
  so a refused answer costs its own rows rather than the run.

  Replaces 0.21.2 (`3642aa1`, SHA-256 `96eb4897…8ca3`, 1,152,040 bytes), which
  carried `PackageModsIdentity.bills`/`.primary_bill`, the ten new contracts
  this repository now hosts in full, and `uslm` in `BODY_PREFERENCE` /
  `DEFAULT_FORMAT_PREFERENCE`; all of that stands unchanged. The `reconstruct`
  extra (lxml) is still not installed.
- `rulespec_artifacts-1.0.13`: required by spicy-docs 0.21.3, which pins it
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
