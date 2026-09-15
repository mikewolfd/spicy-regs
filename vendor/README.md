# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.17.0`: built from SpicyDocs commit `b9ff1fe16e8e5fc136d2282162293992fab5bbd3`.
  SHA-256: `917ca1ce62f139a2bae12e3cae6e3ef526118d533f0c71ebbc583daad4f337d2`.
- `rulespec_artifacts-1.0.12`: unchanged from the prior source-reader pin.
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
