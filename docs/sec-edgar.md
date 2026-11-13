# Find issuer filings and compare reported facts

The SEC EDGAR pipeline delivers a declared CIK cohort and filing-date window,
plus selected companyfacts concepts by actual period-end date. CIK is the SEC's
Central Index Key; tickers remain descriptive metadata. Tables are described in
`docs/tables/sec_*.md` and become available through the ordinary local
Parquet reader and SQL service. Publishing a generation remains a separate
operator action.

Acquire through the source-owned bounded SEC client with a real contact mailbox:

```sh
uv run --group source-readers python -m spicy_regs.pipelines.sec_edgar acquire \
  --cik 0000320193 --cik 0000789019 --start 2023-01-01 --end 2024-12-31 \
  --contact-email "$SPICY_DOCS_CONTACT_EMAIL" --output-dir /path/to/acquisition \
  --captures-out /path/to/captures.json
uv run --group source-readers python -m spicy_regs.pipelines.sec_edgar build \
  --captures /path/to/captures.json --concept us-gaap:Assets \
  --concept us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax \
  --concept us-gaap:EarningsPerShareDiluted \
  --fact-start 2023-01-01 --fact-end 2024-12-31 --output-dir /path/to/build
```

The source-reader package must include `spicy_docs.sources.sec_edgar`; the
coordinator releases and pins that package before production use. Originals use
the shared raw archive through `CaptureEvidence`. Builds read pinned originals
offline, verify digest/size and continuation inventory, and keep source evidence
as generation inputs. The acquisition API's `reuse` argument accepts exact
capture descriptors already held in that archive; it revalidates them without a
network request. The CLI's optional `--reuse-captures` provides the same path.
An acquisition refusal leaves failed-run evidence and produces no usable empty
issuer. Changing a selected digest, omitting an overlapping continuation or
repeating an accession refuses a generation.

For example, this ordinary SQL query finds the asset balance reported for each
annual filing's own report date. It does not add later comparative observations:

```sql
SELECT f.cik, f.accession, f.filing_date, f.report_date_raw,
       r.value_text, r.unit, r.source_sha256, r.source_pointer
FROM sec_filings f JOIN sec_reported_facts r USING (cik, accession)
WHERE f.cik = '0000320193' AND f.form = '10-K'
  AND r.taxonomy = 'us-gaap' AND r.concept = 'Assets' AND r.unit = 'USD'
  AND r.period_kind = 'instant' AND r.period_end = f.report_date_raw
ORDER BY f.filing_date;
```

The bounded official cohort yields USD 352583000000 at 2023-09-30 from accession
`0000320193-23-000106`, and USD 364980000000 at 2024-09-28 from
`0000320193-24-000123`. These are source-reported instant balances, not spending
flows. Later filings repeating those dates remain queryable in
`sec_reported_facts`.

`transforms.build_sec_edgar.select_fact` requires a CIK, taxonomy, concept, unit
and exact end/start dates. It returns `competing_observations` when multiple
positions match, even when their values agree. Selecting an accession can yield
`selected` or `reported_null`; missing facts yield `missing`,
`concept_not_reported`, or `no_observations_in_period` as applicable. Unacquired
or unselected scope yields `not_selected`. No units or periods are combined.
`context_json` preserves supplied/missing fields; original bytes plus JSON
Pointer preserve original number spelling. Decimal numbers in parsed context
JSON are represented as exact text. A missing `start` is an instant; explicit
null is unknown.

Amendments retain their own accession and `/A` form. Their prior accession is
not inferred. Companyfacts supplies whole-entity standard-taxonomy observations,
not original XBRL contextRef, segment dimensions, custom-taxonomy facts, or full
filing/ownership interpretation. A link is a retained locator, not proof that
its filing body has been downloaded. Initial source review and consumer evidence
are retained in the workspace's ss-h9qm source-completion lane.
