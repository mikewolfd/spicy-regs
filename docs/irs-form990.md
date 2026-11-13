# Find a nonprofit return and trace its finances

The `irs-form990` pipeline builds a local maintained generation containing
`irs990_filings`, `irs990_officers` and `irs990_grants`. It reads a selected annual
IRS index and monthly ZIP from the central raw archive. SpicyDocs retains and
reads source XML; this pipeline interprets selected Form 990 fields. Builds never
select a public generation or shared source catalog.

The initial qualified cohort is Form 990 `2021v4.2`. Form 990-EZ, 990-PF and the
separate Form 990-N notices have no qualified interpretation. Other XML schedules
remain native data in `native_xml_json`; their retention does not qualify financial
or relationship interpretation. Forms 8871/8872 and complete political-donor data
are outside scope.

Acquire explicit official URL/size/ETag selections through
`python -m spicy_regs.pipelines.irs_form990 acquire --originals originals.json
--output-dir capture --captures-out captures.json`. Capture uses the maintained
bounded HTTP reader and central original archive, and aborts on credential refusal.
Check retained holdings before acquiring. A replay selection binds `submission_year`,
`index`, `archive` and explicit `object_ids`; each original descriptor holds
`request_url`, `response_sha256`, `observed_at` and `byte_size`.

Build with `python -m spicy_regs.pipelines.irs_form990 build --selection selection.json
--output-dir fresh-output`. The selection is retained with the build evidence,
both originals are hash-checked, every selected index identity is checked against
its XML, and generation admission checks the exact output set and schemas. An
unsupported form/version, changed input, malformed amount or missing selected
filing refuses the build. Earlier outputs survive refused builds. Use `--read-index`
when preparing a replacement for an existing public family; the ordinary publisher
checks that predecessor. Integration and public selection remain separate steps.

Set `SPICY_REGS_DATA_DIR` to the resulting `generation/` to use normal local serving.
A useful ordinary `query_sql` example is:

```sql
SELECT f.organization_name, f.ein, f.filing_id, f.tax_year,
       f.tax_period_end, f.submission_year, f.revenue_usd, f.expenses_usd,
       f.grants_paid_usd, g.recipient_name, g.recipient_ein,
       g.cash_usd, g.noncash_usd, g.purpose, g.source_xpath,
       g.source_locator_json
FROM irs990_filings f
LEFT JOIN irs990_grants g USING (filing_id, ein)
WHERE f.ein = '943231120'
ORDER BY f.filing_id, g.entry_ordinal
```

The bounded official sample reports Ruth Asawa Fund's tax year 2021, period ending
2022-09-30, in the 2023 submission index: revenue $42,610, expenses $30,070 and
$28,000 grants paid. Its Schedule I names Friends of Scrap Inc, EIN 942566798,
with a $20,000 cash grant and reported zero noncash assistance. The $8,000
remainder is not assigned to an invented recipient: Schedule I has reporting
thresholds and the retained source does not explain this allocation. This is an
explicit source attribution gap; it does not change the reported totals.

Revenue and expense comparisons check Part I against the corresponding Part VIII
and Part IX totals and record `matched`, `different` or `missing_field`. All money
uses exact `decimal(38,0)` reported US dollars, with original spellings and
`exact`/`absent`/`empty` status. No field silently supplies zero. Part VII personnel
entries keep organization, related-organization and other compensation separate.
A missing Schedule I differs from a present schedule with no selected recipient
entries. Grant entries represent domestic organizations in Schedule I Part II,
not all assistance, Schedule I individual categories, foreign grants or donors.

EIN identifies an organization; `filing_id` is the literal IRS `OBJECT_ID`.
`return_id`, original DLN, tax year/period, submission year and capture time retain
their separate meanings. Amendment indicators retain their literal value and
presence status. The pipeline does not collapse amendments or infer which filing
supersedes another. Repeated names and recipients keep their source occurrence
and complete native entry, including role flags and unrecognized source fields.

The `ss-umeo` handoff records exact commits, original/member hashes, the tested
slice, bounded original visual review, replay/consumer/failure evidence and
local/public status. The local qualification is not a historical completeness
claim or proof of public release.
