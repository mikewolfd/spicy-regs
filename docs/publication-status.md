# Verify publication separately from table definitions

The dictionary describes the table schemas this checkout supports. A successful
offline dictionary check establishes agreement between code and documentation.
It does not establish that those tables or columns are available publicly.
`measured_on` describes the associated coverage statement; read its receipt for
the input scope and whether the output was uploaded.

## Observation on 2026-09-21

A read-only audit at 14:07 UTC requested all 67 declared table URLs using bounded
HTTP ranges. It retained the exact Parquet footers and response headers, required
matching strong ETags across the two reads, and read metadata with PyArrow.

| Observation | Result |
| --- | --- |
| Declared schemas | 67 |
| Available public Parquet files | 24 |
| HTTP 404 at the documented URL | 43 |
| Other failures / unverified responses | 0 |
| Available files with the currently declared column names and order | 20 |
| Available files whose column list differs | `documents`, `comments`, `congress_bills`, `federal_register` |

The public `congress_bills` file has 419,571 rows and 10 columns. The local
49-column BILLSTATUS output is a separate artifact. The six tables previously
described as unproduced all exist publicly: `agency_stats`,
`agency_monthly_volume`, `discovery_signals`, `feed_summary`,
`rulemaking_lifecycles`, and `org_committee_links`. They remain declared.
`bill_subjects` has a local output but returned 404 at its public URL.

The availability set matches the 24-table declaration at upstream commit
`1f02a7f`. That commit is an ancestor of local `2b7b5e4`, with 111 commits
between them; the branches do not have unrelated histories. This ancestry and
the public files do not establish which job deployed them.

## Retained evidence and limits

The retained observation is in
`~/Work/corpora/supply-2026-09-02/receipts/publication-audit-2026-09-21/`:

- `catalog-input.json` is the exact declaration compared.
- `publication-status.json` contains URLs, timestamps, HTTP outcomes, ETags,
  sizes, row counts, column lists, footer paths and footer digests.
- `raw/*.footer` retains the bytes PyArrow inspected.
- `audit.py` records the measurement procedure. To repeat the observation,
  copy the script into a new, empty receipt directory, create its `raw/`
  subdirectory, and use `uv run --frozen python /absolute/path/to/audit.py`.
  Never run it in an existing receipt: although summary creation is exclusive,
  the raw footer writes are not.

Footer metadata checks availability and declared shape. It does not validate
row contents, qualify inferred fields, or establish one consistent generation
across independently observed files. A footer digest covers only its retained
bytes; an ETag is not asserted to be an artifact content digest.

For a complete readable deployment, the existing command below reconciles its
schemas with the dictionary. A missing file or outage makes that command fail;
it is not a substitute for the per-table observation above.

```sh
uv run --frozen spicy-regs-dict check --source r2
```

## Local correction and release work

[Correction handling](correction-lifecycle.md) covers rule changes and failed
reads. The separately retained `legislative-release-candidate-2026-09-21`
receipt contains a corrected five-table slice for the 118th Congress's House
and Senate bills. It was built offline against an explicitly pinned SpicyDocs
checkout; it is not a replacement for the broader public bills file or proof
that the normal vendored package contains that correction.
