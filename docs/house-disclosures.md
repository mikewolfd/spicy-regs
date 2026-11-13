# House filing lookup and transaction pilot

The House pipeline builds a complete selected index snapshot and a declared
selection of periodic transaction report (PTR) originals. It uses SpicyDocs for
capture and native source reading, the central archive for originals, and the
ordinary generation/evidence path for candidate admission. It does not select
shared publication state.

The filing lookup retains every category, including repeated DocIDs as separate
index-line observations. `P` selects PTRs, not all financial disclosures. An
ambiguous DocID refuses report selection. Other categories remain discovery
entries; annual income, liability dates and other sections have no extraction
claim. Constructed file URLs are labelled as unverified until captured separately.

The report and page tables retain successful and refused readings. The
transaction table contains qualified native cells only. Native blank owners
remain empty strings; amounts remain brackets, with an explicit exclusive status
for an open lower bound. Asset text stays literal. Printed native transaction ID,
New/Amended status, original dates, backend version and source page/word evidence
remain available. Scanned forms, image bundles, unfamiliar layouts and checkbox
interpretation remain outside this reader's qualified scope. A refused report
with no emitted rows is not an empty report.

## Use and bounded replay

Query the selected source catalog before acquisition. A selection JSON uses
`format: house-disclosures-captures`, `version: 1`, a plain-language `scope`, one
`index` and a `reports` list. The index names `year`, official `url`, `sha256`,
original `bytes` and supported `observed_at`. Each report additionally names its
`doc_id`. The raw archive holds those exact bytes; local build paths never become
source URLs. Missing capture times remain null.

```sh
uv run --frozen python -m spicy_regs.pipelines.house_disclosures acquire-index \
  --year 2025 --output-dir "$RUN/index-acquisition" \
  --captures-out "$RUN/selection.json"
uv run --frozen python -m spicy_regs.pipelines.house_disclosures acquire \
  --selection "$RUN/selection.json" --doc-id 20030591 \
  --output-dir "$RUN/acquisition" --captures-out "$RUN/captures.json"
uv run --frozen python -m spicy_regs.pipelines.house_disclosures build \
  --selection "$RUN/captures.json" --output-dir "$RUN/build"
```

Use `acquire-index` only when a refresh is required or the needed index version
is not retained. Otherwise begin with the exact retained index selection.
Acquisition reuses an explicitly selected exact capture after hash verification.
Only missing selected P files are requested, through `CaptureEvidence`; access
refusal aborts with failed evidence. Build is offline and requires a fresh output
directory. Rebuilding an unchanged selection produces the same Parquet table
bytes. Pass `--read-index` when comparing with an already published generation;
the coordinator owns shared integration, package release and publication.

The installed `spicy-docs` version must include the House readers. During a
coordinated local source/consumer review, use the exact two source heads through
an explicit Python import path; this does not establish installed-wheel or hosted
acceptance. No new source wheel is built by this lane.

## Inspect reported changes

This query compares the two retained Schneider filings without replacing either
row. The newer report prints Amended and a native transaction ID; it does not
print the earlier filing DocID, so the comparison is not a publisher-stated
replacement link.

```sql
SELECT f.first, f.last, f.filing_date, t.doc_id, t.filing_status_raw,
       t.native_transaction_id, t.owner_raw, t.asset_raw,
       t.transaction_date, t.notification_date, t.amount_raw, t.page_locator
FROM house_disclosure_transactions t
JOIN house_disclosure_filings f USING (filing_id)
WHERE t.doc_id IN ('20030591', '20030594')
ORDER BY f.filing_date;
```

`house_disclosure_amendment_links` separately holds explicit prior-filing-date
references reviewed on original images. A link requires both original hashes,
available page numbers, the same filer, a unique P entry on the stated earlier
date, the original statement, uncertainty and a hash-pinned review. Review images
can also be pinned and retained. This evidence establishes the reviewed reference,
not complete supersession, extracted scanned transactions or net money.

```sql
SELECT original_doc_id, amended_doc_id, source_statement, uncertainty,
       original_page_locator, amended_page_locator
FROM house_disclosure_amendment_links;
```

The House source-completion handoff retains its exact declared population,
original-image review, query results, failure controls and source pins. A local
candidate does not establish public publication or all-House coverage.
