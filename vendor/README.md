# Source reader wheels

`uv sync --frozen` installs these through the default `source-readers` group.
The optional `source-readers` extra enables the same readers for package installs;
its wheels must be supplied explicitly until they are published in a registry.
Base CLI and MCP installs do not require them.

- `spicy_docs-0.21.1`: built with `uv build` from SpicyDocs commit
  `6f8d20e` (tag v0.21.1), 2026-09-19. SHA-256:
  `c519e231b44a639c342fa857802869eca258bb4a7051a84b983956a2706e68a6`
  (1,025,308 bytes). Keeps the whole table-contract layer 0.21.0 carried —
  `spicy_docs.schemas`' twenty-two `TableContract`s with their columns,
  identity, version column, grain and per-column prose, the `shape_*` function
  per table, and `interpretation/bill_family.py`'s `build_bill_family`, which
  produces twelve of them in one pass — and adds four things this repository
  wires:
  - **The sealed body preference.** `sources/govinfo/bodies.py`'s
    `BODY_PREFERENCE = ("xml", "htm", "txt", "pdf")` is now
    `GovInfoBodyAcquirer.acquire`'s default, and
    `sources/congress/bill_versions.py`'s
    `DEFAULT_FORMAT_PREFERENCE = ("xml", "html", "txt", "pdf")` is
    `choose_format`'s. One order, spelled in each module's own format names;
    a package offered only as PDF now yields a body instead of a refusal.
  - **`extraction/body_text.py`.** `body_text(package_body) -> BodyText` —
    one text derivation per rendition (text, pages, rendition, media type,
    byte size, derivation name and the per-rendition cleanup record), so a
    caller stops writing its own decoder. Measured in
    `docs/sources/govinfo-bodies.md`: committee reports offer only `htm` and
    `pdf`, the `htm` is the text rendition, no `[[Page N]]` marker and no form
    feed exists in any GovInfo body, and PDF text is last.
  - **The bulk listing skip.** `sources/congress/bulk_status.py` gained
    `BulkListingEntry`, `BulkStatusAcquirer.list_archives(congress, bill_type)`
    and `acquire(..., unchanged_since=entry)`, which reads the folder listing
    first and skips the zip entirely when the retained entry's name, link,
    modified time and size all match (`BulkStatusAcquisition.skipped_unchanged`,
    `listing_entry`); `docs/sources/congress-bulk-status.md`.
  - **The GPO normalization fixes.** `extraction/gpo_normalize.py`'s
    `GpoPageCleanup` gained `running_footer_lines` and `content_lines`, which
    `bill_version_tables._page_cleanup` already serializes into
    `bill_versions.cleanup_json`.

  Replaces 0.21.0 (`ff92406`, SHA-256 `ca3f26c5…1705`).
- `rulespec_artifacts-1.0.13`: required by spicy-docs 0.21.1, which pins it
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

PDF enrichment uses the narrow `pdf-pypdf` provider extra through `source-readers`.
SpicyRegs additionally pins pypdf `6.14.2` and checks that version before parsing;
package installs and checkouts therefore use the same qualified PDF behavior.

CourtListener listing, pins and raw rows use the shared provider directly. Run
`uv run pytest tests/test_courtlistener_bulk.py tests/test_courtlistener_shared.py tests/test_court_scope.py tests/test_cluster_court_scope_backfill.py`
for this receiving change. Before replacing the provider wheel, also run the
BILLSTATUS, Unified Agenda and PDF reader tests (`test_bill_subjects.py`,
`test_unified_agenda*.py`, `test_pdf_text*.py`).
