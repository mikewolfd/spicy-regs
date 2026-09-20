# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.22.0`: built with `uv build` from SpicyDocs commit
  `b76a1f0` (tag v0.22.0), 2026-09-20. SHA-256:
  `782735d857b7ebe8cfa9e6fca1ad2dd265e0ab2e07c8dae9c7b1fe26474bafa1`
  (1,188,118 bytes). Unzipping both wheels and diffing them shows five
  modules changed and one added, all of them the model seam, and
  **the thirty-two table contracts are identical** — same 617 columns, same
  identity, version column, grain and per-column prose, compared field by
  field against both 0.21.3 and 0.21.2 rather than inferred from the release
  note. `spicy-regs-dict generate` accordingly moves nothing, twice over. What
  changed here:
  - **`interpretation/gemini_call.py`**, the Gemini-to-`ModelCall` adapter,
    now upstream. This repository's copy in `transforms/model_call.py` is
    deleted: 0.22.0 gave `ModelCall` a `response_schema` argument that every
    generator passes, so a local `call(*, model, prompt)` raised `TypeError`
    on every call. `transforms/model_call.py` keeps only `resolve_gemini_key`
    — which environment variable this host reads — because a hosting
    application supplies the key and spicy-docs never reads the environment
    for one. The adapter's own tests went with it
    (`tests/test_interpretation_gemini_call.py` upstream).
  - **The answer shape is on the request.** `model_call.answer_schema` derives
    a draft 2020-12 schema from the same `AnswerField` tuple the prompt and
    the reader use, and `gemini_call` sends it as Gemini's
    `responseJsonSchema` beside `responseMimeType: application/json` (one
    home, `extraction.gemini.json_generation_config`). This is the half of
    BillTrax's request the port had dropped. It is a request and not the
    contract: the readers still refuse, unchanged, because a provider may
    accept a schema and answer around it.
  - **A refused answer is a `FamilyRefusal`, not an exception.**
    `interpretation/bill_family._model_answer` runs all three generators
    inside the guard the row shapers already ran inside and files the
    reader's own message against the printing, while a credential refusal and
    a transport failure still abort. This closes what adopting 0.21.3 here
    found: a `ModelCallError` escaped `build_bill_family` and aborted the
    whole rollup, and the wrapper this repository added to survive it had to
    report a refused summary as a *declined* one — "its text is below the
    minimum" — which was false. Both that wrapper and the misstatement are
    gone; the diff path's twin misstatement is fixed upstream too.
  - **The classification prompt is `v3`** (`section_classification`): the
    `sectionId` field now asks for "the text inside the square brackets
    below, without the brackets". Under `v2` it said "copied exactly as given
    below" and the model copied the brackets too — measured live twice, zero
    rows stored both times. The two summary prompts stay `v2`: their schema
    rides in the generation config, so their prompt bytes did not move.

  What this repository does with it: `tests/test_bill_family.py` runs all
  three readers behind a stubbed client, dispatching on `response_schema`
  against the three schema constants the generators send, so the `v2`/`v3`
  stamps, the refusal of the measured `v1`-era answer and the diff reader on
  a changed pair are each asserted here. `transforms/build_bill_family.py`
  wires the seams unwrapped and logs each distinct `FamilyRefusal` reason
  with its count, so a refusal is actionable rather than only counted.

  Replaces 0.21.3 (`083b536`, SHA-256 `1f91bb70…f84b`, 1,181,012 bytes), which
  first stated each answer's keys in its prompt at `PROMPT_VERSION` v2 and
  added `schemas/document_capture/1.0/` (DocumentCapture v1 schemas and six
  capture profiles, registering no contract and unread here); and before it
  0.21.2 (`3642aa1`, `96eb4897…8ca3`), which carried
  `PackageModsIdentity.bills`/`.primary_bill`, the ten new contracts this
  repository now hosts in full, and `uslm` in `BODY_PREFERENCE` /
  `DEFAULT_FORMAT_PREFERENCE`. All of that stands unchanged. The `reconstruct`
  extra (lxml) is still not installed.
- `rulespec_artifacts-1.0.13`: required by spicy-docs 0.22.0, which pins it
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
