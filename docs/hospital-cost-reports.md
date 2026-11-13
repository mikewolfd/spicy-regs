# Compare selected hospital cost-report measures

The `hospital-cost-reports` family builds local candidate tables for the official
CMS-2552-10 fiscal-year-start 2010 extract. It uses SpicyDocs' retained-original
reader, RefSpec's existing CCN validator, the atomic Parquet writer and the
ordinary generation/evidence path. It does not select shared publication state.

## Inputs and interpretation

Supply a JSON selection with `original` (local retained ZIP), `sha256`,
`byte_size`, `form` (`CMS-2552-10`), `year` (`2010`), `source_url` and an explicit
`measures` array. The supported measures are:

| Measure | Native worksheet / line / column | Unit and scope |
| --- | --- | --- |
| `hospital_beds` | S300001 / 01400 / 00200 | Beds available at period end, hospital only |
| `total_costs` | C000001 / 20000 / 00300 | USD, worksheet C Part I total |
| `total_charges` | C000001 / 20000 / 00800 | USD, combined inpatient/outpatient gross charges, worksheet C Part I total |
| `net_patient_revenue` | G300000 / 00300 / 00100 | USD, financial statement reporting entity |

CMS's Hospital Cost Report Data Dictionary specifies these direct locations.
No ratios, annualization, settlement estimates, subsidiary totals or financial
margins are computed. Different worksheet scopes remain distinct. All other
numeric and alphanumeric cells survive without assigned units in the raw table.

The native RPT table retains status, transmittal and every literal source field.
Every filing in the pinned original survives. CMS distributes the highest report
status available for a facility/period; superseded submissions absent from that
extract remain unavailable. A later archive capture is a new source version.
No local rule ranks amended against reopened reports or collapses a CCN/year.
Reporting periods remain the actual start/end dates, with duration and explicit
short/annual/long/unknown flags. Unknown or out-of-cohort dates remain flagged.

`hospital_cost_reports` contains report facts; `hospital_cost_report_cells`
contains all native worksheet observations; `hospital_cost_report_measures`
contains one row per report and selected measure. Original hashes, member hashes,
record numbers and byte locations connect values to the retained CSV. Absent
cells, literal blanks, numeric zero, nonnumeric values and precision limits have
separate states. Duplicate raw observations survive; an ambiguous selected
measurement refuses the build. Unsupported measures, form/cohort or changed
native layouts also refuse. A failed fresh output has no admitted generation.

## Build and use

Until the coordinator releases the source reader and RefSpec dependency together,
run in the shared existing environment with the exact reviewed source paths on
`PYTHONPATH`. Do not silently use an older installed SpicyDocs wheel.

```sh
uv run --frozen --no-sync python -m spicy_regs.pipelines.hospital_cost_reports \
  --selection /path/to/pinned-selection.json --output-dir /path/to/new-candidate
```

Set `SPICY_REGS_DATA_DIR` to the resulting `generation` directory to use the
ordinary MCP server and its `query_sql` tool. The table metadata explains each
field and the source limits. For a facility/period comparison, use:

```sql
SELECT r.ccn, r.report_id, r.fiscal_begin, r.fiscal_end, r.period_status,
       r.report_status_raw, m.measure, m.value, m.value_status, m.unit,
       m.provider_scope, m.worksheet, m.line, m."column", m.source_member,
       m.source_record_number, m.source_byte_offset, m.source_archive_sha256
FROM hospital_cost_reports r
JOIN hospital_cost_report_measures m
  USING (source_archive_sha256, report_id)
WHERE r.ccn IN ('010001', '010005') AND r.cohort_status = 'in_cohort'
ORDER BY r.ccn, r.fiscal_begin, r.report_id, m.measure
```

Keep report identity and source hash in joins. A facility may have multiple
periods or alternate filings. Compare like periods and worksheet scopes; do not
sum rows across report statuses as though they were separate hospitals.

The source ticket and completion lane retain exact original pins, visual review,
source reconciliation, tested population and consumer/failure receipts. Schema
registration and fixtures establish capability; they do not establish live source
coverage, public publication or complete hospital-finance history.
