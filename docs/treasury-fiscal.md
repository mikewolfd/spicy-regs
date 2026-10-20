# Treasury fiscal comparisons

`treasury_debt_observations` holds daily debt balances.
`treasury_statement_periods` holds Monthly Treasury Statement (MTS) Table 1
monthly and fiscal-year-to-date receipts, outlays and deficit. Debt is a stock;
the budget amounts are period flows. A change in debt is not equated to borrowing
from the public or the budget deficit.

The first retained source scope is United States federal debt on October 16,
2026 and the September 30, 2026 MTS issue, including its FY2025 and FY2026
Table 1 observations. Complete source Table 2 remains reconciliation evidence.
Forecasts, other MTS tables, inflation adjustment and historical backfills are
outside this typed delivery. This exact retained scope passed the source-release
reader, domain build, local SQL query and publication checks in an in-memory
store. Installed-package adoption and public publication remain pending; the
local check does not establish a current public selection.

## Amounts, dates and revisions

Amounts use exact `DECIMAL(38,2)` US dollars. Literal `"null"`, JSON null and empty
strings remain distinct statuses; none becomes zero. Original rows, metadata,
request selection and row locators remain alongside the typed amounts.

The [September 2026 report](https://fiscaldata.treasury.gov/static-data/published-reports/mts/MonthlyTreasuryStatement_202609.pdf)
was retained with SHA-256
`fcabeccdc7052960ffe8ec2c8a539aed9c1f49d47aedc2cbee39d50f0585e7db`.
Its page 5 budget actuals reconcile to the selected API Tables 1 and 2; Table 1
uses outlays minus receipts, while Table 2's surplus/deficit field reverses that
sign. Table 1's raw `gross` field names remain intact. The statement's
modified-cash methodology subtracts specified refunds and reimbursements; this
mapper does not invent a separate gross series. Table 2 forecasts have different
scale and missing-marker behavior, so its native cells stay untyped.

The source `record_date` and `record_fiscal_year` describe the report issue even
on prior-year rows. Economic years come from explicit `FY YYYY` parents; month
names determine calendar periods. Sequence codes such as `1.10` remain text.
The supported layout requires complete prior fiscal years and current-year
October-through-report-month rows, each with its own fiscal-year-to-date total.
Unsupported or missing periods refuse. Real-source qualification currently
covers the retained September issue, not every historical MTS layout.

The report's revision note describes an August outlay correction. The earlier
issue was not acquired, so no exact historical revision delta is claimed.
Different issues and same-issue refreshes remain separate observations keyed by
source release digest as well as report/economic period. A build includes all
explicitly supplied source releases; it neither deduplicates by economic month
nor automatically discovers prior releases.

## Retained replay and query

SpicyDocs owns the bounded API acquisition, pagination and source-release replay.
This command reads admitted local releases and pinned report controls only:

```sh
python -m spicy_regs.pipelines.treasury_fiscal \
  --selection "$TREASURY_SELECTION_JSON" --output-dir "$TREASURY_OUTPUT"
```

The selection JSON contains:

```json
{
  "sources": [
    {
      "release": "/local/source-release",
      "blobs": "/local/source-blobs",
      "logicalId": "<exact source logical ID>",
      "artifactDigest": "sha256:<exact digest>",
      "verifierImplementationId": "<accepted source verifier ID>"
    }
  ],
  "debt_dates": ["2026-10-16"],
  "report_dates": ["2026-09-30"],
  "reports": {
    "2026-09-30": {
      "path": "/local/retained-report.pdf",
      "sha256": "sha256:<exact report digest>",
      "url": "https://fiscaldata.treasury.gov/static-data/published-reports/mts/MonthlyTreasuryStatement_202609.pdf"
    }
  }
}
```

Supply one release per selected debt/Table 1/Table 2 dataset and issue, including
each revision to retain. The example shows the fields of one source entry, not
a complete set of releases. Missing selected dates or Table 2 controls refuse
without selecting a newer date. The report argument is an explicitly reviewed
qualification input: the build checks its bytes and retains it, but does not
rerun PDF interpretation.

The existing `CaptureEvidence` owner retains each source page once and records
every source release pin. `build_generation` produces both ordinary tables;
`sources.publication.publish_generation` accepts that generation with the
emitted source-evidence directory. Building locally does not publish it.
Normal SpicyDocs dependency packaging must adopt the paired source profile before
installed-package acceptance; a development `PYTHONPATH` is not that proof.

Point the existing local SQL consumer's `SPICY_REGS_DATA_DIR` at `generation/`.
This trend query retains the report issue and revision identity:

```sql
SELECT report_date, economic_fiscal_year, period_start,
       budget_receipts_usd, budget_outlays_usd, budget_deficit_usd,
       json_extract_string(source_locator_json, '$.request_url') AS source_url,
       qualification_report_url, source_release_digest
FROM treasury_statement_periods
WHERE period_kind = 'month' AND economic_fiscal_year = 2026
ORDER BY period_start, report_date, source_release_digest;
```

Query `treasury_debt_observations` separately for the selected daily balance.
Never sum monthly and fiscal-year-to-date rows together.

Source routes, field mappings and methodology reuse the
[official Fiscal Data repository](https://github.com/fedspendingtransparency/fiscal-data/tree/43be46fa6479de64f8a1a577cd761679a2f5cf7e)
at the reviewed revision. Its MIT notice is retained with the source adapter.
No website runtime, second HTTP client, new query engine or history cache is
introduced.
