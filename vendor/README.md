# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.23.0`: built with `uv build` from SpicyDocs commit
  `73b10e5` (tag v0.23.0), 2026-09-20. SHA-256:
  `36d619a6744cf10d37bf466a3114b1958f59453b22d90ba42be61c6719be4f66`
  (1,259,241 bytes), verified against the built wheel after the copy and
  recorded identically in `uv.lock`. The resolve is one line — `Updated
  spicy-docs v0.22.0 -> v0.23.0` — with no refusal; the same
  `rulespec-artifacts==1.0.13` and DeltaTrack pins hold.

  **What moved, measured rather than read off a release note.** Unzipping both
  wheels and diffing them shows three modules changed
  (`schemas/__init__.py`, `sources/govinfo/bodies.py`, `sources/zyte.py`) and
  seven added (`schemas/document_citation_tables.py`,
  `schemas/budget_volume_tables.py`, `schemas/senate_expenditure_tables.py`,
  `schemas/bill_action_tables.py`, `interpretation/citations.py`,
  `interpretation/bill_actions.py`, `transport/zyte.py`). `TABLE_CONTRACTS`
  was then dumped from each wheel and compared field by field — columns,
  identity, version column, grain and per-column prose:

  - **The thirty-two contracts 0.22.0 shipped are identical**, all 617
    columns. Nothing this repository already hosts moved, so no hosted
    dictionary entry could move either.
  - **Five contracts are new, 149 columns**, taking the registry to **37
    contracts over 766 columns**: `document_citations` (17, keyed
    `document_key, text_sha256, cite_kind, target_key, span_start`),
    `house_activity_reports` (36, keyed `package_id`), `budget_volumes` (34,
    keyed `package_id`), `senate_expenditures` (35, keyed on package, file,
    page, table, row and the page text's digest) and
    `bill_committee_actions` (27, keyed `document_key, text_sha256, bill_id,
    print_phrasing, span_start`).

  So `UNHOSTED_CONTRACTS` in `tests/test_contract_tables.py` names those five
  rather than staying empty, and each leaves the set in the commit that gives
  it a writer. `spicy-regs-dict check`/`generate` move nothing on adoption:
  the dictionary is generated from `CONTRACT_TABLES`, which the adoption does
  not touch.

  What the release carries here, beyond the contracts:
  - **`interpretation/citations.py`** — `find_citations(text, *, pages, kinds,
    congress, committees)`, the rule set that reads cited keys out of a
    document's own text with their character spans, and
    `committee_vocabulary(house=…, senate=…)`, which turns roster records into
    the names those rules resolve against. `CITATION_RULE_SET_VERSION` and the
    per-rule versions in `CITATION_RULES_BY_NAME` are what the row shapers
    stamp, so a re-extraction is distinguishable from the one before it.
  - **`interpretation/bill_actions.py`** — `find_bill_actions(text, citations,
    *, committee_chamber)`, which pairs an action phrase a committee print
    states with the bill named in the same sentence, and states its own
    attachment class per row rather than publishing a pairing as a fact.
  - **The package-id grammar is widened** (`sources/govinfo/bodies.py`) to
    `BUDGET-{fy}-{part}` — six sealed parts, `APP`, `BALANCES`, `BUD`, `FCS`,
    `MSR`, `PER` — and `GPO-CDOC-{congress}sdoc{n}`. The same change records a
    publisher fact this repository must not assume away: **the collection a
    package id names is not always the `collectionCode` its records state**.
    Both `BUDGET` and the GPO-prefixed CDOC reprints state `GPO`, so each
    grammar entry carries the code its records state and the check compares
    against that.
  - **`transport/zyte.py`**, and `sources/zyte.py` grows a `mode` on its
    response — `httpResponseBody` is the publisher's own bytes,
    `browserHtml` is Zyte's browser's serialized DOM, which no publisher ever
    sent. The two are kept apart because they are different evidence. Nothing
    here reaches a walled route yet, so nothing in this repository calls it.

  Replaces 0.22.0 (`b76a1f0`, SHA-256 `782735d8…bafa1`, 1,188,118 bytes),
  which put each answer's declaration on the *request* as a JSON Schema, made
  a refused answer a `FamilyRefusal` rather than an exception out of the
  rollup, moved the classification prompt to `v3`, and brought
  `interpretation/gemini_call.py` upstream; and before it 0.21.3 (`083b536`,
  `1f91bb70…f84b`) and 0.21.2 (`3642aa1`, `96eb4897…8ca3`), which carried
  `PackageModsIdentity.bills`/`.primary_bill`, the ten contracts this
  repository hosts in full, and `uslm` in `BODY_PREFERENCE` /
  `DEFAULT_FORMAT_PREFERENCE`. All of that stands unchanged. The `reconstruct`
  extra (lxml) is still not installed.
- `rulespec_artifacts-1.0.13`: required by spicy-docs 0.23.0, which pins it
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
