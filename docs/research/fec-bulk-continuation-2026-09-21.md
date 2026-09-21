# FEC bulk expansion — September 21, 2026

This continues the [initial local integration](fec-delivery-2026-09-21.md).
SpicyDocs owns source acquisition and parsing. SpicyRegs exposes source metadata,
selected records and reported relationships through MCP. Files, parsed rows and
interpreted financial totals remain separate outputs.

The final local generation contains **13,120,005 source records**, **51 selected
collections** and **183,390 relationship observations**, alongside the 26-family
source catalog. Of the 26 official bulk groups, **24 have selected parsed
outputs**, **one has file-level inventory only** (PostgreSQL), and **one remains
unresolved** (the unofficial Senate directory). These are selected snapshots,
not complete historical coverage. Record counts include headers and file
metadata; relationship counts include missing and empty observations.

## Completed code and source checks

- The initial committee-reader and metadata/MCP work is committed as `125ab22`
  and `6b41977` in SpicyRegs.
- SpicyDocs `b618b92` adds an ordered bulk-dictionary reader. SpicyRegs `5437c86`
  adopts its local 0.26.0 wheel and maps fields from retained official HTML.
  Each collection keeps complete dictionary cells and coordinates; each record
  keeps its original fields and a dictionary reference.
- All six selected 2024/2026 `weball`, `webl` and `webk` members now have named
  fields. Independent checks matched **38,520 rows and 1,080,468 values**, plus
  **609 dictionary cells and 535 exact source text spans**. Manual inspections
  preserved leading zeros, blank values, negative amounts and decimal spelling.
- The isolated provider gate passed **7,273 tests**, with four skipped and 46
  deselected. The installed SpicyRegs host passed **1,960 tests**, with three
  deselected. Lint, formatting and the 71-table dictionary check passed.

The wheel was built from a clean isolated checkout. Concurrent uncommitted
source work did not enter it. Compared with 0.25.0, it also includes three already
committed schema corrections: digest-ordering descriptions and explicit refusal
of non-finite or circular JSON values. The wheel comparison records these changes.

## New bulk inputs and limits

The acquisition selected the complete current **2026** `oth`, `indiv`,
`indiv_delete`, `indiv_insert` and `oppexp` originals and their official header
companions. The preflight total was **2,477,585,475 compressed bytes**. File
metadata exposes original URLs, capture dates, exact digests and verified ZIP
members, allowing callers to select native data before expanding every row.

The five ZIPs contain **14,119,138,889 decoded bytes** in total. The individual
base archive contains `itcont.txt` and 18 `by_date/` members; the latter's sizes
sum to the main member's size. These are distinct archive layouts. Do not sum
their member counts into a contribution population; matching byte totals alone
does not prove record equivalence or a deduplicated population.

| Input | Selected treatment |
| --- | --- |
| `oth26.zip` | Complete member: 9,906,414 rows with 21 named literal fields. Transaction codes, amendments, memo fields and reported identifiers remain source values. |
| `indiv26.zip` | Complete original and member inventory; the main member has 32,034,987 native rows. Full expansion into individual row records is separate work. |
| `indiv26_delete.zip` | 254,240 separately labelled deletion-file records; no deletion policy applied to another population. |
| `indiv26_insert.zip` | 394,630 separately labelled insertion-file records; no inferred current contribution totals. |
| `oppexp26.zip` | 1,620,229 positional records. Every record has 26 positions; the official header names 25. |
| Retained PostgreSQL committee-history dump and README | Two file-level records, preserving their September 12 capture dates. This does not expose decoded database rows or establish current history. |
| Unofficial Senate filings | The current official bulk page still links a 2008–2018 directory, but the exact directory returned HTTP 404. No originals or successful-empty collection were created. |

The operating-expenditure mismatch is independently confirmed across the whole
member. Position 25 has a value on 411,233 rows; position 26 is always empty.
The current official HTML description also contains a duplicate position number
and an inconsistent row width. The output preserves every literal position and
leaves named-field mapping unavailable. No field was invented or dropped.

Negative amounts, source amendment flags, historical election indicators and
correction-file roles survive unchanged. These checks establish source fidelity,
not deduplicated financial totals, verified identity links or complete FEC history.

## Final output checks and use

The new financial row tables passed independent comparison of **263,786,985
literal fields** against raw inputs. The file inventory passed original-digest,
capture-field and member checks for five ZIPs, three headers and all 23 archive
members. The final combination preserves every cell of its four disjoint input
generations, with no conversion, filtering or inferred financial relationships.

The actual MCP server returned the final counts and field meanings. Every
collection joined exactly one catalog family, and every relationship observation
resolved to its exact source record. Manual raw/MCP checks covered 12 summary
witnesses and 12 financial witnesses, including negative amounts, blank values,
source identifiers, correction roles and the additional expenditure position.

The [delivery manifest](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-continuation-2026-09-21/delivery.json)
pins the combined input manifest, final table files, source versions and audits.
The [26-group coverage matrix](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-continuation-2026-09-21/BULK-COVERAGE.md)
lists the selected scope and remaining work for each group. Query this generation
from the repository with:

```sh
SPICY_REGS_DATA_DIR=/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-continuation-2026-09-21/final-tables uv run --frozen spicy-regs-mcp
```

## Evidence

- [Dictionary mapping and six-file audits](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-summary-mapping-2026-09-21/AUDIT.md)
- [Provider and installed-host checks](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-dictionary-adoption-2026-09-21/)
- [New financial originals and output audits](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-expansion-2026-09-21/)
- [Independent operating-expenditure discrepancy audit](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-expansion-2026-09-21/oppexp-second-audit.md)
- [Senate directory refusal](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-senate-bulk-expansion-2026-09-21/selection-result.json)

Historical backfills, decoded PostgreSQL history, full individual-row expansion,
source-specific correction rules, public refreshes and consistent multi-table
publication remain open. No data upload, registry publication or deployment was
performed in this pass.
