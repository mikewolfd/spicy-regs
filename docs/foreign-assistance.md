# Compare reported foreign assistance

`foreign_assistance_latest` answers recipient/fiscal-period/measure questions
from the latest retained complete country-summary file. The underlying
`foreign_assistance_observations` and `foreign_assistance_snapshots` tables keep
all selected observations and source identities. A complete file may still contain
partial financial reporting; its rows are never labeled final.

Filter measure and dollar basis explicitly. Country or area recipients remain
separate from regional and global programs. Historical names and territories are
source-designated recipients, not a verified present-day sovereign-state roster.

```sql
SELECT recipient_name, fiscal_period, measure, amount, unit, reporting_status,
       source_locator_json
FROM foreign_assistance_latest
WHERE recipient_code = 'UKR' AND fiscal_period IN ('2024', '2025', '2026')
  AND measure_id IN ('2', '3') AND dollar_basis = 'current'
ORDER BY fiscal_period, measure_id;

SELECT recipient_name, recipient_scope, fiscal_period, measure, amount,
       reporting_status, source_locator_json
FROM foreign_assistance_latest
WHERE recipient_code IN ('SSN', 'WLD') AND fiscal_period = '2024'
  AND measure_id = '2' AND dollar_basis = 'current';
```

Amounts use exact decimals and original USD units, without scaling or rounding.
Read `amount_status` before aggregation: blanks, publisher `NULL`, numeric zero,
unparsed errors and values beyond the exact decimal representation are distinct.
`raw_cells_json` preserves every original cell, while `source_locator_json` points
to the original URL, SHA-256 and record/physical line. No source correction is
applied without separate authoritative evidence.

The country summary aggregates all reporting agencies and reported programs but
supplies no agency/program identities. Those omissions are explicit in each row.
The retained dictionary does not state a constant-dollar base year, so it remains
unknown. Current and constant dollars and different financial measures must not
be added together. Historical periods retain their native labels, including the
1976 transition quarter.

## Capture and rebuild

Check the selected source catalog and retained capture selections before fetching.
Use exact existing originals when available. An explicit refresh fetches only the
summary and dictionary through `CaptureEvidence`; it never requests the complete
transaction corpus.

```sh
uv run --frozen python -m spicy_regs.pipelines.foreign_assistance acquire \
  --output-dir /owned/run/capture --captures-out /owned/run/captures.json
uv run --frozen python -m spicy_regs.pipelines.foreign_assistance build \
  --captures /owned/run/captures.json --output-dir /owned/run/build
```

Repeat `--captures` to retain earlier snapshots alongside a refresh. The build
opens central raw originals, admits each source release, checks the separately
pinned dictionary, writes bounded Arrow batches and admits an ordinary generation.
It does not select publication or catalog state. The coordinator can pass that
generation and its pinned evidence to the existing publication path; package
integration must include the coordinated SpicyDocs reader version.

`--archive-root` selects an explicit raw store for local tests/replay. `--read-index`
records an inherited publication input when applicable. `--selection` supports
previously admitted releases through the shared snapshot pipeline. Use a fresh
output directory for every candidate. No network is needed for a retained rebuild.
A changed dictionary refuses until its original has been reviewed and its unit and
geography rules accepted.

## Revisions and missing observations

```sql
SELECT recipient_name, fiscal_period, measure_id, dollar_basis,
       observation_change, previous_amount_raw, current_amount_raw,
       current_usd_change, previous_source_locator_json, current_source_locator_json
FROM foreign_assistance_revisions
WHERE recipient_code = 'UKR' AND fiscal_period = '2024'
ORDER BY observed_at, measure_id, dollar_basis;
```

The revision view compares adjacent held complete-file observations. It labels
changed values, unchanged values, newly observed keys and now-unobserved keys.
Now-unobserved does not establish deletion or zero. A later empty file stays
visible in the snapshot table and makes the latest query empty, rather than
silently carrying forward older rows. Constant-dollar cells remain visible, but
cross-version arithmetic stays null because their price base is unknown.
Capture order does not establish the publisher's release chronology. Equal capture
instants with different selections refuse instead of inventing an ordering.

A working local generation and synthetic failure controls establish their declared
scope only. Public availability, an independent final review, and an actual
changed publisher version require their own evidence.
