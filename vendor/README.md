# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.20.0`: built from SpicyDocs commit `b96e083805019302fcf5b96bae962521b8a5ad7a` (tag v0.20.0). Address sweep to sources/uscode/*, spicy_docs.reading.* and sources/courtlistener/* in the same commit; receivers re-qualified below.
  SHA-256: `594a2f01f689c10f8c3b133a70b2e8ee9f644dd9234788a957b4f731e6d1c7e3`.
- `rulespec_artifacts-1.0.12`: unchanged from the prior source-reader pin.
- `deltatrack-0.1.0`: built with `uv build --wheel` from `civictechdc/DeltaTrack`
  commit `c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6`, 2026-09-19.
  SHA-256: `7f060e30af9702f4e45c305fa93c53d1e3e59bd70858d3c6717f6fc9cf825197`.
  DeltaTrack is a git dependency (SpicyDocs' `bill-diff` extra pins
  `git+https://github.com/civictechdc/DeltaTrack?rev=c636448…`), not a
  registry package, so it is vendored the same way as the other source-reader
  wheels here rather than resolved transitively. `[tool.uv.sources]` points at
  it already, but nothing depends on it yet — `uv lock` resolves 165 packages
  with the entry present and leaves `uv.lock` byte-identical, so it is inert
  until spicy-docs 0.21.0 pins `spicy-docs[...,bill-diff]` (`source-readers`),
  which pulls DeltaTrack transitively and puts this wheel in the lock. It
  exists for the bill-diff tables (`section_diffs`, `section_diff_items`,
  `financial_changes`) that `docs/research/table-contracts-2026-09-19.md` §5.4
  describes.
- `uv.lock` records both wheel SHA-256 values. Replace the wheel and refresh the
  lock together, then run receiver tests against the installed wheel.

PDF enrichment uses the narrow `pdf-pypdf` provider extra through `source-readers`.
SpicyRegs additionally pins pypdf `6.14.2` and checks that version before parsing;
package installs and checkouts therefore use the same qualified PDF behavior.

CourtListener listing, pins and raw rows use the shared provider directly. Run
`uv run pytest tests/test_courtlistener_bulk.py tests/test_courtlistener_shared.py tests/test_court_scope.py tests/test_cluster_court_scope_backfill.py`
for this receiving change. Before replacing the provider wheel, also run the
BILLSTATUS, Unified Agenda and PDF reader tests (`test_bill_subjects.py`,
`test_unified_agenda*.py`, `test_pdf_text*.py`).
