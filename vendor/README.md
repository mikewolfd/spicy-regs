# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.21.2`: built with `uv build` from SpicyDocs commit
  `3642aa1` (tag v0.21.2), 2026-09-19. SHA-256:
  `96eb4897935998e903ed7e93ab26c120860baf3d677eeffd02599b760dbc8ca3`
  (1,152,040 bytes). Keeps everything 0.21.1 carried — the table-contract
  layer, the sealed body preference as `acquire`/`choose_format`'s default,
  `extraction/body_text.py`, the bulk listing skip and the GPO normalization
  fixes — and changes three things this repository reads:
  - **`PackageModsIdentity.bills` and `.primary_bill`**
    (`sources/govinfo/bodies.py`, `ModsBill`): every `<bill>` a package MODS
    names, in document order, and the one marked `PRIMARY`.
    `build_committee_reports.py` reads both from the package the acquirer
    already proved and deletes the interim `_mods_bills`/`_primary_bill` parse
    it carried. `bills` is a required field of the dataclass, so the test that
    built a `PackageModsIdentity` by hand now runs `validate_package_mods` over
    the fixture bytes, the same parse the acquirer runs.
  - **Thirty-two table contracts** (was twenty-two; 617 columns). Of the ten
    new ones, `laws`, `law_code_sections`, `table3_records`, `committees` and
    `committee_assignments` are hosted by the `laws` and `committee-rosters`
    rollups (A8/A9); `house_communications`, `committee_meetings`,
    `record_issues`, `treaties` and `nominations` are not hosted until their
    rollups land: `data_dictionary.CONTRACT_TABLES` enumerates the hosted
    tables by hand, and `tests/test_contract_tables.py::UNHOSTED_CONTRACTS`
    names the rest so a later wheel cannot add one silently. Two hosted
    contracts moved: `hearing_transcripts` appends `event_id` as its twentieth
    column (NULL here — the transform does not walk the Congress.gov hearing
    detail that states it), and `congress_bills.statutes_at_large_cite`'s
    prose now says the host fills it by joining `laws` at merge time, which
    `transforms/table_merge.py::fill_statutes_at_large_cite` does after every
    `congress_bills` merge from the published `laws` table; the dictionary
    prints the wheel's sentence verbatim.
  - **`BODY_PREFERENCE = ("xml", "uslm", "htm", "txt", "pdf")`** and
    `DEFAULT_FORMAT_PREFERENCE = ("xml", "uslm", "html", "txt", "pdf")`: the
    USLM rendition after XML, in both spellings. Nothing here passes a
    preference, so both callers take the new default.

  `CongressListRoute.single_record` and `paged_json.page()`/`pages()`'s
  `single_record` keyword are additive (default `False`); nothing here calls
  either positionally. The new `reconstruct` extra (lxml) is not installed.

  Replaces 0.21.1 (`6f8d20e`, SHA-256 `c519e231…68a6`).
- `rulespec_artifacts-1.0.13`: required by spicy-docs 0.21.2, which pins it
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
