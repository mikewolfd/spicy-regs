# Find a nonprofit return and trace its finances

The `irs-form990` pipeline streams official multi-year IRS Tax Exempt Organization Search (TEOS)
annual indexes and bulk XML ZIP archives into three receipts-free, `submission_year`-partitioned
Parquet tables: `irs990_filings`, `irs990_officers`, and `irs990_grants`. SpicyDocs owns
streaming Expat XML parsing, archive bounds, and URL validation; SpicyRegs owns Form 990
financial interpretation, partitioned Parquet building, dual-tier evidence retention, and
R2/MCP publication.

## Scope & Explicit Form-Type Boundaries

- **Form 990 (`RETURN_TYPE = "990"`, `<IRS990>`):** Interpreted across all visually qualified
  schema families in `QUALIFIED_VERSION_SPECS` (`2020v4.*`, `2021v4.*`, `2022v4.*`, `2022v5.*`,
  `2023v5.*`, `2023v6.*`, `2024v5.*`, `2025v4.*`). Pre-2020 cohorts (`2018v3.*`, `2019v4.*`) and
  future schema releases (`2025v5.*`, `2026v*`) stay unqualified until raw visual review of
  rendered original XMLs; unrecognized versions are quarantined and fail the build above `0.1%`
  (`max_quarantined_ratio=0.001`) or `500` rows.
- **Form 990-EZ (`990EZ`), Form 990-PF (`990PF`), and Form 990-T (`990T`):** Present in the
  same IRS TEOS annual indexes and ZIP archives; explicitly recognized, fast-skipped without XML
  interpretation, and counted per year and archive in `irs990-population-summary.json`. They are
  never coerced into Form 990 columns.
- **Form 990-N (e-Postcard):** Published by the IRS as a separate non-XML bulk file
  (`data-download-epostcard.zip`) with no financial, officer, or grant schedules; out of scope.
- **IRS Section 527 (Forms 8871/8872):** Political organization filings live in the separate
  `irs-527` family (`irs527_*` tables) and are never conflated with `irs-form990`. Both families
  carry literal 9-digit `ein` strings for explicit cross-family joins.

## Streaming Architecture, Dual-Tier Evidence & Carry-Forward

1. **Discover official IRS URLs sequentially:**
   `python -m spicy_regs.pipelines.irs_form990 discover --years 2023,2024,2025,2026 --output manifest.json`
   Performs one `GET` against `https://www.irs.gov/charities-non-profits/form-990-series-downloads`
   followed by strictly sequential `HEAD` probes (`min_interval >= 1.5s`, explicit `User-Agent`,
   immediate `CredentialRefusedError` abort on HTTP `401`/`403` without retry or bypass).
2. **Stream build with closed-year carry-forward:**
   `python -m spicy_regs.pipelines.irs_form990 sync-build --manifest manifest.json --output-dir fresh-output`
   - **Dual-tier evidence:** Annual index CSVs are retained under `policy="full"` in
     `CentralRawArchiveStore` (reusing exact retained originals by SHA-256 or verified
     `(url, etag, byte_size)` receipt match). Multi-hundred-MB TEOS XML ZIPs are streamed strictly
     sequentially (`max_workers=1`) under `policy="hash_only"` into one private temporary file at a
     time, parsed via single-pass Expat `scan_form990_return()`, and unlinked immediately before the
     next ZIP. Already-retained local ZIPs (such as `2023_TEOS_XML_01A.zip`) are also reused in
     place under `policy="hash_only"` so bulk ZIPs are never uploaded to R2 evidence.
   - **Checkpoints & year-level resume:** `checkpoints/<year>.json` records per-ZIP progress audit
     state (`completed_zips`, `pending_zips`, dispositions, and peak RSS) for operational tracking.
     Restart/resume operates at the **submission-year** boundary via `--carry-forward-generation` +
     `--carry-forward-summary` (not intra-year mid-ZIP resume).
   - **Carry-forward & truncation guard:** Passing `--carry-forward-generation <mirrored-generation>`
     with `--carry-forward-summary <prior-summary.json>` copies unchanged closed-year
     `submission_year=<YYYY>/part-*.parquet` members verbatim (failing closed if prior year
     metadata/fingerprints are missing or schemas drift) so `publish-local-generation` reuses them
     via server-side R2 `copy_object`. Bounded `--max-zips > 0` runs seal as `local-partial` and
     are blocked from live publication.
   - **Provenance without DOM bloat:** Full XML trees (`native_xml_json`) are omitted from
     `irs990_filings`; every row retains `source_locator_json` (annual index + ZIP URL/SHA-256 +
     member name/SHA-256 + index row), `index_native_json`, and on child tables exact positional
     `source_xpath` and complete `native_entry_json`.
   - **Manual CI workflow:** `.github/workflows/refresh-irs-form990.yml` runs `discover` +
     `sync-build` + candidate validation (`skip_upload=true` dry-run default) and guarded
     `publish-local-generation` with live `publication.v2.json` re-read.

Offline single-slice replay (`acquire` and `build`) remains available for bounded fixtures and
explicitly selected object IDs.

## Querying & Witness Guarantees

Set `SPICY_REGS_DATA_DIR` to the resulting `generation/` to use normal local serving:

```sql
SELECT f.organization_name, f.ein, f.filing_id, f.tax_year,
       f.tax_period_end, f.submission_year, f.document_cnt_status,
       f.revenue_usd, f.expenses_usd, f.grants_paid_usd,
       g.recipient_name, g.recipient_ein, g.cash_usd, g.noncash_usd,
       g.purpose, g.source_xpath, g.source_locator_json
FROM irs990_filings f
LEFT JOIN irs990_grants g USING (filing_id, ein, submission_year)
WHERE f.ein = '943231120'
ORDER BY f.filing_id, g.entry_ordinal
```

- **Exact money & statuses:** All dollar columns use exact `DECIMAL(38,0)` reported whole US
  dollars with paired `_raw` and `_status` (`exact`, `absent`, `empty`). Missing fields remain
  `NULL` and never invent zero.
- **Unallocated grant gaps (Ruth Asawa Fund witness):** Ruth Asawa Fund (`ein = '943231120'`,
  `filing_id = '202330199349301313'`) reports `$42,610` revenue, `$30,070` expenses, and `$28,000`
  grants paid against a `$20,000` Schedule I domestic-organization grant to Friends of Scrap Inc
  (`recipient_ein = '942566798'`, `$0` noncash). The `$8,000` unallocated remainder stays explicit.
- **Corporate trustees in Part VII:** Institutional trustees/management firms reported with
  `<BusinessName>` and no `<PersonNm>` (e.g. `COMMERCE TRUST COMPANY` in `202300129349300400`)
  populate `irs990_officers.business_name` alongside nullable `person_name`.
- **IRS-redacted attachments:** `document_cnt_status` records `matched`, `redacted_fewer`
  (`observed < declared`), or `absent`; `observed > declared` still refuses.
- **Amendments & duplicates:** Distinct `OBJECT_ID` amended filings remain distinct rows with
  literal `amended_raw` and `amended_status` (never collapsed). Within a submission year,
  byte-identical reposted `OBJECT_ID` members are deduplicated and counted in
  `duplicate_identical_oids`; conflicting-bytes duplicates for the same `OBJECT_ID` are
  quarantined.
