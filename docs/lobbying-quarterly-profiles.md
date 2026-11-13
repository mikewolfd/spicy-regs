# Quarterly lobbying income and expenses

`lobbying_quarterly_profiles` compares reported LD-2 quarterly amounts for exact
native registrant/client pairs. It retains the selected filing and every competing
filing. Income and in-house expenses remain separate estimates in USD, serialized
as decimal text to preserve precision in browser clients.

The caller declares a filing year from 2008 onward, native quarterly periods,
registrant identifiers (or all held registrants), a timezone-aware posting cutoff,
and exact source generation and table SHA-256 pins. `client_id` means the source's
`client.id`; it identifies a client record within one registrant, never the same
organization across firms. Names establish no identity. Quarters without a retained
filing remain absent, not zero.

A unique original report can stand alone. One later amendment can restate that
original's quarter. Multiple originals, multiple amendments without predecessor
links, missing originals and anomalous posting times remain unresolved. A timestamp
alone never breaks a tie. Registrations are excluded. Termination reports and
no-activity suffixes retain their monetary disclosures. After-cutoff filings remain
in the witnesses but do not determine the as-of amount.

The [publisher's reporting guidance](https://lobbyingdisclosure.house.gov/ldaguidance.pdf),
section 6, describes good-faith quarterly estimates, the reporting threshold,
expense accounting methods and external firm fees. The API does not expose the
threshold checkbox. A source amount below $5,000 therefore stays in the witnesses
without becoming a qualified zero or an invented range. Null, empty, unsupported,
negative and conflicting amounts have separate statuses. Non-rounded reported
amounts are preserved as filed; this pipeline does not round or correct them.
An authoritative amendment can change the selected report without erasing its
original. An expense-method change prevents a comparable amendment difference.

Every output row is labeled `incomplete_retained_population`. A completed observed
API walk does not prove publisher-wide completeness or closure to future amendments.
Keep income and expenses separate: an organization's expenses may include fees
another firm reports as income. Do not add the two, combine accounting methods as
if they were equivalent, or allocate a filing's amount to each issue or lobbyist.

## Build a local candidate

Use an existing admitted native `lobbying-filings` generation and a JSON control
with exactly these fields (substitute its actual pins):

```json
{
  "filing_year": 2025,
  "periods": ["first_quarter", "second_quarter", "third_quarter", "fourth_quarter"],
  "cutoff": "2026-11-13T00:00:00+00:00",
  "registrant_ids": ["15997", "401106328"],
  "source_table_generation": "sha256:<source generation digest>",
  "source_table_sha256": "sha256:<native table digest>"
}
```

```sh
uv run --frozen python -m spicy_regs.pipelines.lobbying_quarterly_profiles \
  --selection selection.json \
  --source "$SOURCE_GENERATION/lobbying_filings.parquet" \
  --output-dir "$RUN/quarterly-profiles"
```

The existing artifact reader binds the table to the source root; the builder
hashes the actual table before reading it. DuckDB uses one thread, a 256 MB
execution limit and no spill; this is not a process RSS limit. The maintained
Arrow writer replaces its destination only after full consumption. The output
uses the ordinary generation, metadata, MCP and local-family publication paths.
`publish-local-generation --dry-run --family lobbying-quarterly-profiles` is a
private publication rehearsal; changing shared publication remains a separate action.
Each generation replaces this family's declared snapshot rather than appending
unrelated scopes or hiding a prior scope behind the same profile ID.

## Consumer queries

```sql
SELECT filing_period, income, expenses, expenses_method,
       income_amendment_change, expenses_amendment_change,
       selection_status, selection_reason, selected_filing_uuid,
       selected_source_url, coverage_status
FROM lobbying_quarterly_profiles
WHERE registrant_id = '15997' AND client_id = '121527'
ORDER BY period_start;
```

The bounded November 13 source verification retained all observed 2025 filings for
registrants `15997` and `401106328`. General Electric's first-quarter expense
amendment changes its reported estimate from $1,000,000 to $1,500,000. ASCEND's
client `57672` first-quarter income amendment changes $9,000 to $10,000, posted
in June 2026. These are source-specific, incomplete comparisons, not national
coverage. Qualification evidence lives in the workspace's
`output/source-completion-20261113/lanes/lda-lobbying/` handoff.

Count child records before joining them to financial profiles:

```sql
WITH activities AS (
  SELECT filing_uuid, count(*) AS activity_count
  FROM lobbying_activities GROUP BY filing_uuid
)
SELECT p.profile_id, p.income, p.expenses, a.activity_count
FROM lobbying_quarterly_profiles p
LEFT JOIN activities a ON p.selected_filing_uuid = a.filing_uuid;
```

For issue filters use `EXISTS` against the selected filing's activity rows. Do not
sum money after an occurrence-level join. The SpicyGov profile displays the
published selection, amendment effects, source links and unresolved witnesses;
it performs no financial selection or arithmetic.
