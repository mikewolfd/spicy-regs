# Compare CMS Open Payments summaries

The pipeline delivers one explicitly selected year and publication to
`cms_open_payments_summaries`. It preserves one row per publisher summary
occurrence. Reporting entities, practitioner recipients, teaching hospitals,
states and national controls retain separate aggregation levels.

Capture or resume exact retained pages, then replay without network access:

```sh
uv run --frozen --extra source-readers python -m spicy_regs.pipelines.cms_open_payments capture --year 2024 --output /work/cms-capture
uv run --frozen --extra source-readers python -m spicy_regs.pipelines.cms_open_payments build --manifest /work/cms-capture/capture-manifest.json --output /work/cms-candidate
```

Choose a fresh capture directory for a refresh and a fresh build directory for
each candidate. The manifest records retained page identities after every
success. Resume verifies completed pages and fetches only missing pages. A count,
schema, year, release or ordering mismatch refuses before table replacement.
Partial input cannot establish deletion. The result is an observed complete
count-matching cohort, not a publisher-frozen snapshot. The pipeline does not
select shared publication state.

Amounts use exact `DECIMAL(38,2)` US dollars. Native counts use integers. Null
and empty cells remain distinct in `value_statuses_json` and `raw_row_json`.
No correction is inferred. The original fields remain available for authoritative
future corrections. `source_locator_json` links every row to its original
SHA-256, request URL, JSON pointer and native datastore record number.

`general_amount_usd`, `research_amount_usd`, `investment_amount_usd` and
`interest_amount_usd` are separate measures. `associated_research_amount_usd`
is an association measure, never added to research payments received. Multiple
source profile IDs and shared NPIs remain distinct. NPPES enrichment is optional.
Reporting-entity state is its address; recipient-profile state and the publisher's
state totals have separate meanings. Country totals with blank states remain.

Use the ordinary query path on the candidate or published generation:

```sql
SELECT name, state, general_amount_usd, source_locator_json
FROM cms_open_payments_summaries
WHERE program_year = 2024 AND release = 'P06302026_06032026'
  AND aggregation_level = 'entities'
ORDER BY general_amount_usd DESC, native_id
LIMIT 10;
```

For state comparisons, filter `aggregation_level = 'states'`,
`payment_category = 'General'` and exactly one `recipient_type`; use
`category_amount_usd`. Never add state, national, entity and recipient levels.
The maintained `compare_payments` consumer requires the exact year/release,
category and, for state/national summaries, recipient type. An unavailable
category refuses rather than producing an apparent zero.

The declared first delivery is Program Year 2024, release
`P06302026_06032026`. Local generation verification and coordinator integration
are separate from public publication and source acceptance. See the source
completion lane's HANDOFF for the exact tested population, raw visual review,
reconciliation and remaining acceptance gaps.

The SpicyDocs source guide records the reuse/license decision and the later
payment-level expansion requirements: Record_ID plus release/category,
deletions, and repeated product/investigator associations.

Large practitioner cohorts use the documented filtered CSV download after any
retained JSON prefix. This avoids repeating the API's full count/sort for each
small page. Resume exports only the missing suffix and then reuses the exact CSV
original. The source reader checks byte coordinates, ordering, schema and exact
count. CSV empty cells remain literal empty cells, with no inferred database-null
meaning. `source_locator_json.source_format` identifies CSV rows and supplies
byte offsets/lengths; retained JSON rows keep their JSON pointers.
