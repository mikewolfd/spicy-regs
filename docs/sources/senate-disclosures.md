# Trace Senate transaction filings

The `senate-disclosures` family provides native filing observations in
`senate_disclosure_reports` and each electronic filing's transactions in
`senate_disclosure_transactions`. The selected calendar year follows the Senate
search's submission dates. Transactions and report-title dates may fall outside
that year. Original and amended UUIDs remain separate observations.

Use the paired SpicyDocs Senate reader, which implements the supported public
agreement session and a qualified, attributed deterministic HTML parser. Its MIT
upstream revision is `2965e8796bd4cb9423a83b964a1f9dbd076a0ad6`.
The source guide in SpicyDocs states access restrictions and parsing limits.
The capture command requires explicit acceptance of those use restrictions.
Credentials and session tokens stay out of recorded search forms and source
receipts. Paper links stay visible as `unsupported_paper`; annual reports are
outside the selection. Failed/refused access never produces an empty year.

Run from this repository, with the paired SpicyDocs reader installed:

```sh
uv run python -m spicy_regs.pipelines.senate_disclosures capture \
  --year 2025 --accept-use-restrictions --output /new/run/capture
uv run python -m spicy_regs.pipelines.senate_disclosures build \
  --captures /new/run/capture/captures.json --output /new/run/tables
```

The capture retains exact originals through `CaptureEvidence` and the central raw
archive. Each response is bounded, protected reads use the shared transport,
and a run has request, byte and wall limits. A failure retains
`captures.partial.json` and failed evidence. A partial manifest can supply
already acquired reports to `capture --prior-captures`; the command obtains a
fresh complete year listing and retries every missing report. Reuse requires the
same UUID and literal listing row, plus renewed original-byte and parser checks.
A completed prior selection uses the same path for bounded refresh. It reuses
immutable reports and acquires new/changed filing identities.

`build` runs offline from original byte hashes, requires complete page/count and
electronic-report membership, and replays the source parser. Use a fresh output
directory. It writes an ordinary admitted generation and source evidence;
publication/catalog selection is a separate owner action. `--read-index` binds a
replacement candidate to the ordinary publication index it read.

For a consumer example, open the generation's Parquet files as DuckDB views named
`senate_disclosure_reports` and `senate_disclosure_transactions`, then ask which
transactions an amended filing reports and where to inspect the evidence:

```sql
SELECT r.report_title, r.submitted_date, r.amendment_link_status,
       r.original_candidate_ids, t.row_number, t.transaction_date,
       t.owner, t.ticker, t.asset_name, t.transaction_type, t.amount_raw,
       t.source_url, t.report_sha256, t.source_locator
FROM senate_disclosure_reports r
JOIN senate_disclosure_transactions t USING (filing_year, report_id)
WHERE r.report_id = '4b5c15fa-74ca-4e12-a0c4-3dd9ef5124b7'
ORDER BY t.row_position;
```

Amounts retain printed dollar ranges, with exact recognized lower/upper bounds.
Unknown amount formats retain their text and `unresolved` bounds. Blank owners
stay blank; blank/double-dash ticker and comment markers remain alongside their
interpreted null. No midpoint, asset enrichment, personal identity resolution,
or combined amendment total is created. The HTML caption describes transactions
“added to this report”; same-name/date candidate links establish neither
replacement nor cumulative amendment semantics. The native UUID, report hash and
HTML row selector let a researcher inspect each observation independently.
