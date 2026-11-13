# Find a public Executive filing and inspect supported facts

The local candidate exposes `executive_disclosure_filings` for report lookup,
`executive_disclosure_facts` for qualified Part 6 observations, and
`executive_disclosure_pages` for original page text and slot reconciliation.
The cohort selection records source URLs/digests, reviewed cover metadata and
access/amendment outcomes. It is a filing dataset, not a person profile or an
estimate of total wealth. `official_id` is scoped to this reviewed cohort, not
a nominated office or a general person crosswalk. Ethics agreements remain
separate and outside this cohort.

Build from selected originals already in `CentralRawArchiveStore`:

```sh
uv run --frozen python -m spicy_regs.pipelines.executive_disclosures build \
  --selection selection.json --output-dir candidate-run
```

Use a fresh directory. The builder validates the complete selection, reopens
bounded originals by digest, checks byte size/hash, retains source evidence and
creates an immutable generation through `build_generation`. It neither downloads
nor selects publication state. A missing original or mismatched source version
aborts instead of producing an empty filing. To acquire a missing explicit
public posting, use the same module's `acquire --url ... --output-dir ...` command;
check the selected catalog and archives first. The SpicyDocs route permits only
the qualified archive host/path and refuses Form 201/attestation routes.

The selected pilot is the public archived Kamala D. Harris annual packages
released in 2021 and 2024, whose cover years are 2020 and 2023. The 2024 package's
Part 6 is qualified. The 2021 package includes an amendment letter and an
unreadable embedded-text cover; it remains available for lookup with
`facts_status='unsupported'`. The amendment and original remain together in the
exact PDF. No corrected or stale Part 2 fact is emitted. Broader reports, years,
278-T, other sections and comprehensive amendment discovery need separate
qualification. The local candidate is not a hosted/public release.

For an ordinary local query, set `SPICY_REGS_DATA_DIR` to the candidate's
`generation/` directory and use the existing MCP `query_sql` tool:

```sql
SELECT f.official_name, f.position_raw, f.report_type, f.report_year,
       v.row_number_raw, v.description_raw, v.ownership_label,
       v.value_raw, v.income_amount_raw,
       v.source_url || v.source_locator AS original_page
FROM executive_disclosure_filings f
JOIN executive_disclosure_facts v USING (filing_id)
WHERE f.filing_id = 'whitehouse-harris-2024-annual'
  AND v.row_number_raw = '1.';
```

The source reports a self-owned cash account value of `$50,001 - $100,000` and
interest of `$201 - $1,000`, at original PDF page 8. The ranges are reported
categories, not exact amounts. The trust row's blank cells remain empty strings;
normalized bounds are NULL. Wholly blank source slots contribute only to page
reconciliation, not additional asset facts. The page's complete text keeps
footnotes; interpreting changed footnotes requires review.

Access status distinguishes `not_requested`, `requested`, `available`,
`unavailable`, `expired` and `access_refused`. Access outcomes require their
source/review evidence; tests for these states are controls, not claims that
these outcomes were observed in this pilot. Template expiry never sets record
expiration. Signed dates, agency/OGE certification dates and filing receipt date
are separate; receipt dates stay NULL when unknown. The source's blank agency
stays blank. The reader compares the declared name exactly with the native
labeled Part 6 Filer's Name and the cover's labeled name fields, permitting only
surrounding/repeated whitespace normalization. Spelling, case and punctuation
differences refuse; ownership labels and partial names cannot identify the filer.
`executive_disclosure_pages.filer_identity_json` retains the native labels and
values, cover fields and original page/section cell locators. An amendment state of `none_observed_in_retained_document` does not
assert global amendment completeness. Unsupported records retain their explicit
reason and source lookup; they are not empty disclosures.

OGE's [public access FAQ](https://www.oge.gov/web/OGE.nsf/publicresources_disclosure-faq)
describes request conditions and use restrictions. The implementation submits no
identity attestation, Form 201 or agency request. General requests or broader
coverage require a declared cohort and any required human access steps.

The source adapter requires the coordinated SpicyDocs Executive disclosure
reader on the Python path until the coordinator integrates/releases it. No
vendored dependency or shared package release is changed by this lane. Bounded
source fixtures live in SpicyDocs. Consumer tests use controlled source results;
the lane handoff records separate real-original replay and ordinary MCP evidence.
