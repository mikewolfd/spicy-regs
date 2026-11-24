# Versioned CRS files

`spicy_regs.transforms.build_crs_report_files.build_crs_report_files` prepares an
explicit, bounded local `crs_report_files` candidate. Its maintained caller uses
SpicyDocs `CrsFileAcquirer.acquire_report` for the exact numbered PDF stated in a
qualified CRS detail original. `include_html=True` separately selects offered
HTML. HTML has no numbered version, so `publisher_version` and
`qualified_publisher_version` remain NULL even when its URL appeared beside a PDF.
The existing `crs_reports` metadata key remains `(report_id, update_date)`.

Pass qualified literal metadata rows with `detail_state='ok'`,
`detail_witness_json`, and `formats_json` (or native `formats`). The builder reads
the pinned original from `CentralRawArchiveStore`, checks the native report,
update instant, version and stated URLs, then resolves retained bodies before
acquisition. It never derives a family from the report id or guesses version
numbers. `retained_file_pin` translates a selected file-catalog occurrence into
the `retained_files` map keyed by exact source URL. Historical reconciliation
keeps unknown acquisition time, capture id and final URL unknown.
The catalog's receipt entry is a positive one-based ordinal. The adapter converts
it to the zero-based `capture_receipt_entry` index without changing the pinned
receipt key or digest; the original catalog evidence retains the source ordinal.

The central archive retains original bytes and immutable receipts. File subjects
keep the original digest, exact source link, capture/reference receipt and the
metadata association receipt. Metadata-only updates can associate the same PDF
with a later revision without creating another download observation. A different
numbered URL remains a different edition even if both files share a cover date.
An explicit printed `REPORT_ID · VERSION N · UPDATED` footer supplies an additional
byte identity check. A mismatch sets `body_state='content_mismatch'`, clears the
qualified version, and withholds usable text. An absent footer remains unknown.
`file_validation_basis` distinguishes observed final-URL checks from historical
source association checks. A historical PDF with unknown final URL and no
matching explicit footer retains the publisher-stated version while its
`qualified_publisher_version` remains NULL.

Text comes from the existing pinned pypdf reader for PDFs and the maintained HTML
markup reader for HTML. Each derivative names its exact input digest, output
digest and processor pin. Image-only PDFs can have successful empty embedded text;
OCR is outside this path. Raw originals remain retrievable with
`CentralRawArchiveStore.open(source_sha256)` and through the file catalog's normal
receipt/original lookup. Public `source_url` names the original publisher route;
mutable HTML at that route may subsequently change.

The existing shared receipt writer stores atomic body-complete and text-complete
phase bundles under the caller-owned `.crs-file-phases` directory. A retry reads
and admits those bundles before use. If the process stopped after original
capture but before its body phase committed, the builder reconciles its existing
bounded `CaptureEvidence` journals and reuses the completed archived capture.
The shared receipt reader refuses missing or corrupt original/receipt data.
Unchanged reruns reuse completed text. An explicit prior bundle retains earlier
file associations; it is a file subject history, not another metadata history.

The caller owns run directories through local acceptance. Keep them for recovery
while work is incomplete; existing run cleanup can remove disposable phases
after the accepted candidate and required evidence are retained. Raw originals
and their immutable receipts remain under the raw archive's existing owner.

The module CLI accepts a JSON object containing `reports` and optional
`retained_files`. It performs no network requests unless `--allow-network` is
explicit. Declare report, request, per-file, total decoded-body and inherited
subject/receipt limits. The prior-state limit also checks Parquet uncompressed
sizes and local recovery-journal bytes before decoding.

```sh
python -m spicy_regs.transforms.build_crs_report_files \
  --selection selection.json --output candidate \
  --max-reports 2 --max-requests 4 --max-file-bytes 8388608 \
  --max-total-bytes 33554432 --max-prior-bytes 134217728 --max-prior-rows 10000
```

The result names an admitted native generation and shared ETL receipts. It does
not publish or select a release. The maintained MCP `query_sql` path exposes
`crs_report_files` when that artifact is configured. For example:

```sql
SELECT f.report_id, f.metadata_update_date, f.publisher_version,
       f.qualified_publisher_version, f.rendition, f.body_state,
       f.source_url, f.source_sha256, f.capture_receipt_key,
       f.text_input_sha256, f.text_processor_sha256, f.text_content
FROM crs_report_files f
WHERE f.report_id = 'IN12418'
```

Join `f.report_id = r.report_id` and
`CAST(f.metadata_update_date AS TIMESTAMPTZ) = r.update_date` to query the exact
metadata revision in `crs_reports r`. File capture time and printed cover dates
have separate meanings and do not replace that revision identity.

For an earlier numbered PDF with no matching historical CRS API detail, use the
[receipt selection in the source catalog](shared-source-catalog.md#inspect-an-explicit-receipt-selection).
Keep the exact file, rendition, report number, proved edition and source
locator. Keep the literal cover date and acquisition or archive dates as
separate facts. The archive index or copy is a discovery or custody witness;
it does not supply the CRS API `update_date`. These holdings add queryable file
editions without manufacturing a `crs_reports` revision or a `crs_report_files`
association to the current revision.

## Prepare selections and inspect coverage offline

`python -m spicy_regs.crs_file_preparation` creates the collector's existing
`reports`/`retained_files` JSON input and a separate coverage report. Start with
an explicitly selected native `crs_reports.parquet` and its matching
`etl_receipts.parquet`. Supply their expected SHA256 digests and receipt
generation. Preparation checks both members' stored bytes, row counts and
Parquet uncompressed sizes before restoring any literal metadata rows through
the shared receipt reader. It then selects only the requested revisions. The
input pair retains its complete history and remains unchanged.

A bare `--report IN12418` requires `--latest`; latest means the newest revision
**in this admitted table**. Use `--report IN12418@2026-09-19T16:37:49Z` to select
an exact revision, and repeat `--report` for a bounded cohort. Exact comparisons
use timezone-aware instants. Unknown, duplicate and undated selections refuse.
Preparation never fetches missing details or files.

```sh
python -m spicy_regs.crs_file_preparation \
  --subject metadata/crs_reports.parquet --receipts metadata/etl_receipts.parquet \
  --generation-id "$metadata_generation" \
  --subject-sha256 "$subject_sha256" --receipt-sha256 "$receipt_sha256" \
  --report IN12418 --latest \
  --catalog-selection selected-catalog-rows.json \
  --catalog-selection-sha256 "$catalog_rows_sha256" \
  --max-input-bytes 1048576 --max-input-rows 100 \
  --max-reports 1 --max-file-bytes 8388608 --max-total-bytes 16777216 \
  --selection-output selection.json --coverage-output coverage.json

python -m spicy_regs.transforms.build_crs_report_files \
  --selection selection.json --output candidate \
  --max-reports 1 --max-requests 1 --max-file-bytes 8388608 \
  --max-total-bytes 16777216 --max-prior-bytes 1048576 --max-prior-rows 100
```

`--catalog-selection` is optional. Its input is a SHA256-pinned JSON array of
exact source-catalog occurrence rows, as returned by the catalog's existing
`query` command. Select by exact witness digest or immutable acquisition receipt;
check the row's `data.mapping.receipt_evidence_ref` before freezing the selection.
Keep only the intended occurrences when a source URL has several observations.
Preparation uses the existing `retained_file_pin` adapter, checks each selected
receipt entry and hashes its retained original. A conflicting body identity for
one URL refuses. The adapter preserves unknown capture times and final URLs.
The export and generated collector selection each have a 2 MiB byte ceiling.
This small export avoids a whole-catalog rebuild or hash. A missing CRS native-ID
match does not establish absence: catalog recognition can be incomplete. Exact
metadata-witness matches appear as `matched-by-witness`; an unmatched supplied
selection appears as `unmapped`; no supplied catalog evidence appears as
`unknown`.

For completed file evidence, supply `--prior-bundle` and both
`--prior-subject-sha256` and `--prior-receipt-sha256`. Preparation admits the pair
before using its outcomes, verifies relevant originals and refusal receipts, and
keeps earlier refusal outcomes visible beside later completions. Pass the same
`--prior-bundle` to the collector to retain its complete association history and
reuse completed extraction. Preparation does not extract text itself.

Coverage is limited to the requested metadata revisions and supplied evidence:

| Evidence | Reported meaning |
| --- | --- |
| List metadata without qualified detail | `metadata-only`; body and text coverage remain `unknown` |
| Unrequested detail | `detail-not-requested` |
| Detail answered for a different revision | `historical-detail-not-retained`; no source-unavailability claim |
| Failed detail attempt | `detail-attempt-failed`; the metadata outcome alone does not prove HTTP unavailability |
| Verified detail without an offered PDF | `qualified-detail`, with `no-pdf-offered` or unknown formats |
| A stated format URL without supplied original evidence | `format-offered`; body coverage remains `unknown` |
| Receipt-verified original bytes | `body-held` |
| Admitted completed file text | `completed-text` or `empty-text` |
| Exact retained collector refusal receipt | `source-unavailable` or `source-refused`, with the prior outcome preserved |

`empty-text` preserves a successful empty extraction. Image-only documents may
need optical character recognition (OCR); this path does not perform OCR. HTML
appears separately with a NULL `publisher_version`. Use `--include-html` in both
commands to select its offered mutable observation. Coverage lists an offered
HTML rendition even when it is not selected for collection.

A selection can contain no qualified reports while coverage still provides a
useful metadata-only result. The collector requires a nonempty report cohort;
qualify the intended detail revisions through the existing detail operation
before collecting their files. Neither command publishes, activates schedules,
or changes the selected source catalog.
