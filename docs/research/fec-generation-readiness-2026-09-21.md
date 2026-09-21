# FEC generation and local MCP audit — September 21, 2026

The [gap register](../fec-gaps.md) consolidates remaining source, evidence,
integration and deployment work, including the separate real R2 setup evidence.
The [coverage census](fec-coverage-2026-09-21.md) reconciles all source families
and bulk groups against this selected generation.

The selected FEC tables now pass the shared generation, download and MCP path
with their exact table identities intact. This continues the
[Senate recovery](fec-senate-recovery-2026-09-21.md); it adds no source records or
financial interpretation. The previous code and recovery notes were pushed to
`mikewolfd/spicy-regs:main` at `5c6ca0d` before this continuation.

## Downloaded data works directly with MCP

The CLI writes downloaded tables under `download-runs/<id>` and switches a
`current` link after the whole selected batch succeeds. Local MCP previously
looked only for loose Parquet files in the configured root, so the normal CLI
output directory exposed none of those tables. Pointing MCP at the batch itself
loaded rows but lost their generation pins.

The shared local selector now lets MCP accept the download root, `current`, or
an exact batch. It checks selection metadata, hashes managed files against the
captured publication index, and checks declared schemas. Discovery, descriptions
and query responses identify the selected directory and each managed table's
generation. Extra root files and unselected family siblings do not enter the
batch. Existing loose-file mode remains available and explicitly unversioned.

A connection holds resolved batch paths. Switching `current` affects a later
connection, while the existing one continues to use its selected batch. Ordinary
file replacement, removal or mutation is refused around tool statements. These
checks preserve ordinary local workflows; they do not make writable files an
immutable storage service. The reader needs no SpicyDocs installation.

## Verified data path

The existing generation API admitted all four previously audited Parquet files,
read every data page and checked schemas, member digests and row counts. The
three observation tables form `fec-observations`; the inventory forms
`fec-source-catalog`. Every table remains byte-identical to the Senate delivery.

The actual publisher API then wrote both families into a local test object store
and verified its stored bytes before each pointer switch. A loopback HTTP server
exposed those objects to the actual CLI downloader. The downloader captured one
publication index, verified all four files and selected the complete batch.
This exercises the local path, not a live R2 bucket or external deployment.

The running MCP stdio process read the ordinary download root and verified:

| Output | Verified rows |
| --- | ---: |
| `fec_source_records` | 13,717,161 |
| `fec_collections` | 649 |
| `fec_relationships` | 183,390 |
| `fec_source_catalog` | 26 |

All four generation pins survive discovery and queries. Every collection joins
its source family; every relationship observation joins its exact source record.
Nine previously raw-audited witnesses match every output cell after the
roundtrip, including native headers, a narrative body, nonbreaking spaces and the
malformed Senate file's first, blank and final physical lines.

From SpicyRegs, the retained downloaded batch is directly usable:

```sh
SPICY_REGS_DATA_DIR=/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-generation-readiness-2026-09-21/roundtrip-integrated/download \
  uv run --frozen spicy-regs-mcp
```

Evidence is retained outside the repository:

- [Generation identities, schemas and unchanged-byte checks](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-generation-readiness-2026-09-21/seal.json)
- [Local publication and HTTPS download receipt](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-generation-readiness-2026-09-21/roundtrip-integrated/summary.json)
- [Actual MCP calls, pins, counts and witness checks](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-generation-readiness-2026-09-21/mcp-download-audit-integrated/summary.json)
- [Source selections and coverage limits](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-discovery-2026-09-21/delivery.json)

The final audit includes the parallel fork-host and cache corrections merged
from `4d588d5`. It uses the configured HTTPS data-host selection, then exercises
the downloaded batch through MCP. The integrated repository suite passed
**2,085 tests**, with three live integration tests deselected. A subsequent
explicit missing-pin guard passed **116 focused reader tests**, type checking,
lint and formatting. The 71-table dictionary check and generated-file comparison
also passed. These are local checks; they do not report remote CI status.

## Next source gap: retained committee history

A parallel read-only assessment qualified the retained PostgreSQL custom archive
as a practical next metadata source: **262,276 rows, 73 fields and 76,277 committee
IDs**, with cycles **1976–2022**. Its 2026 archive date is not the date of the data.
The source README shows 66 columns; the dump's actual schema has seven more.
PostgreSQL's installed `pg_restore` emitted schema and selected COPY text without
executing SQL or connecting to a database. The entire COPY payload matched the
earlier retained export byte-for-byte.

The remaining provider work is a bounded COPY-text reader with original-dump,
table, row and decoded-stream evidence. It must preserve SQL null markers,
empty strings and escaped values rather than present a local export as a
publisher-downloaded original. Some arrays carry later information on earlier
cycle rows, so the current-API relationship mapper cannot be applied unchanged.
The [assessment and manual witnesses](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-postgres-assessment-2026-09-21/ASSESSMENT.md)
record this next step. Those history rows are not yet in the delivered tables.

Complete FEC history, a complete Senate inventory and external data publication
remain separate work. This continuation made no external data upload or service
deployment.
