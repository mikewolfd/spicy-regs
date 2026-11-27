# Dockets, Docket Attributes, and Document Attributes Receipt Archive (`ss-vse3`)

`dockets`, `docket_attributes`, and `document_attributes` retire per-row `etl_receipts.parquet` publication (`"receipts_retired": true` in their `etl_policies/*.json` declarations).

## Why these three retire cleanly

1. Every parsed source column is already a public column on the published subject tables (`RECEIPT_COLUMNS` is empty for all three).
2. Full-table inspection of the pinned live generations confirmed `input_metadata = {}` across 100% of rows, zero `raw_source_record` values, zero `refused`/`error` outcomes, and zero `EXCEPT ALL` differences against full receipt round-trips.
3. `documents` remains on per-row receipts because its `attachment_records_json` input holds JSON:API fields absent from `documents.attachment_records` and `DocumentsFamily` still consumes witness evidence.

## Pinned Final Receipted Generations

Exact pinned generations, `publishedAt` timestamps, and `etl_receipts.parquet` paths live in [docs/pins/receipt-archive.json](pins/receipt-archive.json):

- `dockets`: `sha256:3b48c02f9f62fa29e71264ac9de3db0d7e3fddb6f6960b55478cfde50cd767f2`
- `docket_attributes`: `sha256:2ea07256f0257b910d4854d0967e1397f3ca125f67556cdf397fa015df6f440b`
- `document_attributes`: `sha256:d9604244b01eac6e99da933ce954efb72edaf59619f18d9af400685bf1554173`
