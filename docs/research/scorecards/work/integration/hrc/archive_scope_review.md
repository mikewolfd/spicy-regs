# Extraction archive scope review

The archive failures exercise a reconstruction feature outside the selected
package scope. They are not failures of structured Gemini outcomes or scorecard
readers. Keep baseline reconstruction and narrow the test overlay explicitly;
do not adopt unrelated runtime changes merely to make the archive pass.

The entire new test file is not unrelated: it also covers adopted extraction
behavior. Retain the compatible tests below and record the omitted scope.

## Verified baseline and changes

- Baseline: `69964fe27c6ddc437e817041a3c5cca57a02723c`.
- Current checkout: `34a8d595e3d6cf19e67b8ca0b749b3fcc6043ffa`.
  The generalized reconstruction changes are committed in this checkout, not
  unstaged changes. They remain outside this scorecard package's chosen baseline.
- `tests/extraction/test_observations.py` does not exist in the baseline. Commit
  `34a8d59` added it alongside the generalized reconstruction path.
- Baseline and first Gemini candidate archive have identical
  `src/spicy_docs/reconstruction/evidence.py` bytes, SHA-256
  `db8e39b4922dfeb34fc4f68afc0e0ee01b1e53624ba64525ff6c9f154ec11003`.
  The current generalized implementation has SHA-256
  `d6e01598aa75523eb8253162d80e07e4b619cdc3a469ac7e8b4303c385cbedb3`.

Baseline `evidence_from_pages` calls `_assemble(_native_lines(page))` directly.
The retained PyMuPDF native observation is mandatory. The current `_page_lines`
function accepts arbitrary retained observations, preserves their order and
unknown geometry/style, and invokes native assembly only for native PyMuPDF
blocks. This is a distinct reconstruction capability, not an import required by
Gemini or the scorecard parsers. No reconstruction calls or imports were found in
`src/spicy_docs/extraction/` or `src/spicy_docs/sources/scorecards/`.

## Exact test boundary

| Test in `test_observations.py` | Selected-package disposition |
| --- | --- |
| `test_unplaced_ocr_reaches_evidence_without_inventing_geometry` | Omit with explicit scope note: requires generalized reconstruction and nullable style serialization. |
| `test_shared_evidence_preserves_provider_order_and_does_not_merge_columns` | Omit with explicit scope note: requires `_page_lines` for arbitrary observations. |
| `test_generic_native_provider_does_not_require_pymupdf_raw` | Omit with explicit scope note: deliberately exercises non-PyMuPDF reconstruction. |
| `test_provider_tables_preserve_unknown_empty_and_merged_cell_metadata` | Retain: tests adopted extraction observation types without reconstruction. |
| `test_regional_insertion_preserves_native_column_order` | Retain: tests adopted extraction strategy behavior without reconstruction. |
| `test_duplicate_native_text_geometry_keeps_distinct_observed_styles` | Retain: compatible native reconstruction regression, already passes against baseline runtime. |

The private archive log
`~/Work/corpora/supply-2026-09-02/receipts/scorecards-gemini-archive-check.log`
shows exactly the first three failures. The remaining archive tests passed or
were explicitly skipped/deselected by the existing gate. The failures arise at
the calls to `evidence_from_pages`, after extraction observation construction.
This is not a reason to suppress a structured-Gemini test, add an expected-failure
marker, or change runtime validation.

## Documentation correction required in the package

The full overlaid `docs/extraction/pdf-extraction-api.md` includes the current
checkout's statement that reconstruction consumes shared blocks without a
PyMuPDF dictionary. That statement does not describe the scoped baseline package.
Remove or qualify that specific statement in the archive recipe, while retaining
the extraction observation and structured-page documentation. The live checkout
guide remains accurate for its own reconstruction implementation.

VERDICT: narrow the archive overlay to the adopted capability, preserve the
compatible extraction tests, and rerun the archive gate. No runtime change is
required by these three failures.

Follow-up: independently inspected and executed the revised recipe's `overlay`
function in memory. Its AST selection emits exactly the three compatible tests
listed above and compiles successfully. The runtime reconstruction file is absent
from the overlay, and the packaged guide explicitly states the retained native
PyMuPDF limitation. The live source tests and guide are unchanged. This closes
the identified scope mismatch; the rebuilt archive gate remains the coordinator's
separate check.
