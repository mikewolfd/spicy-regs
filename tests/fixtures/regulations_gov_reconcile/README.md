# Regulations.gov answers for the docket reconcile step

Recorded unchanged by the round-6 chaos audit (persona xiomara, 2026-10-03) from the keyed API,
`https://api.regulations.gov/v4/`, with `api_key=DEMO_KEY`. Government-source responses.

| File | Request | Status | SHA-256 |
| --- | --- | --- | --- |
| `documents-docket-FNA-2026-0301.json` | `documents?filter[docketId]=FNA-2026-0301` (default page size 25) | 200 | `435294e9eaeb25451e10f4774bb782b85f18f0dfc256c64c37297824f7f86602` |
| `document-FNA-2026-0301-0004.404.json` | `documents/FNA-2026-0301-0004` | 404 | `fd6f2fc62c86d802b95e050cda45022acf5a6938dec1dba78bf20cb053f64f9c` |

The docket lists `-0001`, `-0002`, `-0003` and `-0005`; the mirror still holds `-0004`, the Utah notice
(FR 2026-18893) the publisher moved to FNA-2026-0313-0006. The tests serve the listing for the step's own
`page[size]=250` request, so they restate `meta.pageSize` as 250 when they serve it; every other byte is as recorded.
