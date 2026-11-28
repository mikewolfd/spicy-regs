# Find a public Executive filing and inspect supported facts and transactions

The `executive-disclosures` family exposes four receipts-free tables built from
digest-pinned OGE Form 278e / 278-T PDFs:

- `executive_disclosure_filings` — one row per declared report/access outcome (`filing_id` unique)
- `executive_disclosure_facts` — populated asset and income rows from qualified Form 278e **Parts 2, 5, and 6**
- `executive_disclosure_transactions` — populated securities transactions from qualified Form 278e **Part 7** (and future visually qualified Form 278-T periodic transaction reports)
- `executive_disclosure_pages` — per-page full text, filer identity provenance, slot reconciliation counts, layout profile, and structured `endnotes_json`

The cohort manifest records source URLs/digests, access/amendment outcomes, and
either readable native cover metadata (extracted deterministically via
`read_form278e_cover`) or reviewed `cover_override` metadata for scanned/amended
covers. It is a filing dataset, not a person profile or an estimate of total
wealth. `official_id` is scoped to the declared cohort, not a nominated office
or a general person crosswalk. Ethics agreements remain separate and outside
this family.

## Acquire and build from retained originals

Before acquiring anything over HTTP, check the selected catalog and local
`CentralRawArchiveStore`. `acquire-manifest` reuses already-retained digests
first and fetches only missing public postings within `--max-requests`:

```sh
uv run --frozen python -m spicy_regs.pipelines.executive_disclosures acquire-manifest \
  --manifest cohort-manifest.json \
  --manifest-out cohort-acquired.json \
  --max-requests 16 \
  --output-dir acquire-run
```

For a single explicit public URL:

```sh
uv run --frozen python -m spicy_regs.pipelines.executive_disclosures acquire \
  --url https://bidenwhitehouse.archives.gov/wp-content/uploads/2024/05/Harris-Kamala-D.-2024-Annual-278.pdf \
  --output-dir acquire-single
```

Build an immutable generation offline from originals retained in `CentralRawArchiveStore`:

```sh
uv run --frozen python -m spicy_regs.pipelines.executive_disclosures build \
  --selection cohort-acquired.json \
  --read-index "$LIVE_INDEX" \
  --evidence-policy hash_only \
  --output-dir candidate-run
```

Key pipeline controls:

- **Fresh output directory:** `build` refuses an existing `--output-dir`.
- **Streaming writes:** Filings are read, parsed, and flushed in 2,000-row Arrow
  batches to four `zstd` Parquet writers in a temporary directory, then replaced
  atomically only after the entire cohort succeeds.
- **Evidence policy (`--evidence-policy`):** Defaults to `hash_only` (bound via
  `CaptureEvidence.for_source("executive-disclosures", "hash_only", ..., policy_decision_id="ss-rhsd-public-executive-disclosures")`)
  so raw OGE PDFs stay in local `CentralRawArchiveStore` for replay and visual
  review while published R2 evidence records cryptographic SHA-256 and byte-size
  receipts without permanently re-hosting restricted/6-year-expiring OGE PDFs.
  Pass `--evidence-policy full` only when explicitly retaining small unrestricted
  archival cohorts in R2 source evidence.
- **Predecessor chain (`--read-index`):** Pass the pointer snapshot saved by
  `spicy-regs mirror` so `artifact.json` records `readSnapshot` and weekly
  `generation_retention.py` keeps the replaced generations.
- **Compact v2 cohort manifests:** Accepts both legacy v1 `selection.json` and
  compact v2 manifests (`format: "executive-disclosures-cohort"`, `version: 2`).
  On readable native covers, `read_form278e_cover()` derives `position_raw`,
  `agency_raw`, `report_type`, `report_period_raw`, `report_year`, `form_type`,
  and `oge_certification_date`, failing closed if declared `official_name`
  conflicts with the cover. Non-readable/image/amended covers require explicit
  `cover_override` and `metadata_review_ref`.

## Querying facts, transactions, and endnotes

Set `SPICY_REGS_DATA_DIR` to the candidate's `generation/` directory and query
through MCP `query_sql`:

```sql
SELECT f.official_name, f.report_year,
       v.section_code, v.row_number_raw, v.description_raw,
       v.value_raw, v.value_status, v.value_lower, v.value_upper,
       v.income_amount_raw, v.income_status, v.income_exact_usd,
       v.source_url || v.source_locator AS original_page
FROM executive_disclosure_filings f
JOIN executive_disclosure_facts v USING (filing_id)
WHERE f.filing_id = 'whitehouse-harris-2024-annual'
ORDER BY v.section_code, v.page, v.row_number_raw;
```

```sql
SELECT f.official_name, f.report_year,
       t.row_number_raw, t.description_raw, t.ownership_label,
       t.transaction_type_raw, t.transaction_date, t.amount_raw,
       t.amount_lower, t.amount_upper,
       t.source_url || t.source_locator AS original_page
FROM executive_disclosure_filings f
JOIN executive_disclosure_transactions t USING (filing_id)
WHERE f.filing_id = 'whitehouse-harris-2024-annual'
ORDER BY t.page, t.transaction_date;
```

## Ranges, exact amounts, identity, and refusal boundaries

- **Six amount classifications:** `amount_range()` classifies every printed cell
  into `blank`, `below_threshold` (`None (or less than $201)` -> `0..200`,
  `None (or less than $1,001)` -> `0..1000`), `range` (`$X - $Y` with `X <= Y`),
  `over_threshold` (`Over $X` as exclusive lower bound with `upper = NULL`),
  `exact_amount` (exact printed USD string in `value_exact_usd` / `income_exact_usd`,
  preserving cents such as `"234.13"` without integer truncation), or
  `unrecognized`. Blanks are never zero; midpoints and total net wealth are never
  computed.
- **Page-scoped row keys:** `fact_id` and `transaction_id` use
  `{filing_id}:{source_sha256}:{section_code}:p{page}:{row_number_raw}`, cleanly
  handling Part 7's per-page `1.`..`20.` resets without collisions.
- **Strict identity & amendment non-silencing:** Native cover fields and every
  section's `Filer's Name` must match declared `official_name` under
  whitespace-only normalization. Amended packages (such as `Harris-2021`) and
  scanned/image-only PDFs stay `facts_status='unsupported'` with `0` fabricated
  rows (`ss-jbw1`: zero paid OCR).
- **Zero Form 201 or paid proxy bypass:** Route A covers verified official
  zero-attestation White House archive hosts. Route B (OGE Level I/II direct PDF
  attachments) is disabled by default (`accepted_terms=False`). Form 201 agency
  requests, CAPTCHA walls, and paid scraping/proxy routes are always refused.
  See OGE's [public access FAQ](https://www.oge.gov/web/OGE.nsf/publicresources_disclosure-faq)
  for statutory 5 U.S.C. § 13107 use restrictions.
