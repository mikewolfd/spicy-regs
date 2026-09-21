# Initial FEC integration audit — September 21, 2026

This records the initial selected delivery. See the subsequent
[bulk expansion](fec-bulk-continuation-2026-09-21.md) for newer acquisition,
named dictionary mappings and source limitations found later the same day.

The local implementation now connects SpicyDocs FEC readers to reusable SpicyRegs
metadata and relationship tables. MCP can describe those tables and query an
explicit local generation. Public service deployment and full historical FEC
acquisition are separate, unfinished work.

The authoritative local generation contains **944,479 source records**, **40
selected collections** and **183,390 relationship observations**, alongside the
26-family source catalog. Selected outputs represent **21 of the 26 official bulk
file groups**. The remaining five are listed below; the selected groups also have
historical coverage gaps.

## What changed

- The committee reference pipeline uses the installed SpicyDocs reader. Its
  existing 16 fields and merge behavior survive; raw pages and traversal status
  are retained, and failures preserve the previous output.
- The source catalog exposes all 26 official source families in the provider.
- The retained-input builder accepts verified API captures, bulk originals and
  selected archive members, or existing source releases. It keeps complete
  native records, selection evidence and reported relationships.
- Source headers can provide named bulk fields and literal identifiers for
  joins. Conflicting identifier aliases refuse the build. Aggregate identifiers,
  blank values and unusual filing numbers remain visible.
- MCP distinguishes available tables from declarations and returns field
  meanings, coverage limits, identifiers and schema differences. Its base wheel
  works without the source-reader package.

See [commands and query examples](../fec-integration.md) and
[input manifest details](../fec-relationships.md).

## Evidence and checks

The complete repository suite passed **1,954 tests**, with three deselected by
the existing test configuration. Focused lint and formatting checks, dictionary
validation across 71 tables, and diff whitespace checks passed.

The final bulk comparison matched **917,027 records and 20,844,236 literal fields**
against original bytes, including named fields, identifiers and byte coordinates.
Manual raw/output inspections checked representative values and unusual cases.
The actual MCP server loaded the final generation: all 40 collections joined
exactly one catalog family, and all 183,390 relationship observations resolved to
their exact parent records. This includes 12,311 Form 1 affiliation observations,
with explicit absence values preserved.

The first independent raw/output comparison verified all 27,367 selected API
metadata objects, two original-file inventories, 8,701 positional records with
63,128 literal fields, one narrative body reference, and 109,954 relationship
source-field observations. It compared actual output with original JSON pointers,
ZIP members and byte slices, without calling the production row mapper.

Manual inspections included negative and null F13 filing numbers, exact decimal
strings, candidate district `00` alongside numeric district zero, repeated cycle
arrays, blank electioneering candidate fields, negative expenditure amounts,
amendment references, and the presidential summary's `P00000001` aggregate row.
The F99 inspection found and repaired a distinction between ordinary row
coordinates and narrative body references. MCP inspection also corrected an
incorrectly documented empty-array status token to `empty_list`.

The source catalog audit independently reconciled the packaged source metadata
with its output. A fresh capture of the official bulk page produced the same
26 file groups and 331 grouped links as the prior research inventory. All these
file groups map to the provider's broader source families.

Audit commands, exact input pins, generated tables and raw/output witnesses are
retained outside the repository:

- [Integration receipts](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-spicyregs-integration-2026-09-21/)
- [Bulk-file receipts and family coverage](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-bulk-adoption-2026-09-21/BULK-COVERAGE.md)
- [Default committee reader audit](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-default-adoption-2026-09-21/AUDIT.md)

The [delivery manifest](/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-spicyregs-integration-2026-09-21/delivery.json)
pins the final input selection, table files and audit receipts. To query this
audited generation from the repository:

```sh
SPICY_REGS_DATA_DIR=/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/fec-spicyregs-integration-2026-09-21/mcp-delivery-final-tables uv run --frozen spicy-regs-mcp
```

## Remaining coverage

Selected files and queries are explicitly bounded observations. They do not
establish full source history, complete document bodies, verified organizational
identity, or a deduplicated financial view. Missing and empty relationship values
are included in observation counts; those counts are not positive-edge totals.

Large individual-contribution, intercommittee and operating-expenditure archives,
full financial database dumps, complete daily-filing history and unofficial
Senate filings still require acquisition and qualification. Source-specific
correction and financial inclusion rules remain necessary before derived totals.
The selected committee-history database dump is retained but is not integrated
into these tables. The other four missing bulk groups have catalog entries but
no acquired originals in the inspected scope.

Six selected candidate and PAC summary ZIP members retain positional fields;
their official HTML field dictionaries have been checked, but named-field mapping
remains unfinished. Older paper layouts likewise preserve literal records without
applying relationships from an incompatible newer format.

Legal, agency and reference routes in the inventory also need their own complete
collection runs beyond the selected records already inspected.

No recognized API credential was configured for a fresh full committee traversal.
Retained raw responses and provider transport tests qualified the new integration;
they do not prove a fresh live census. No public tables were uploaded, and no
service was deployed. Multi-table R2 publication remains sequential, so publication
must account for consumers seeing files from different generations.

## Next work

1. Acquire and qualify the four missing bulk groups, then finish named-field
   mapping and the retained database history. Keep each period and selection
   explicit instead of treating a family's presence as complete history.
2. Add repeatable refresh and backfill selections with raw/output audit receipts,
   prioritizing bulk files for large populations and APIs for additional fields.
3. Publish a tested generation with a consistent selection of related tables and
   verify discovery, field descriptions and cross-source joins through MCP.

SpicyDocs continues to own acquisition and parsing; SpicyRegs owns reusable
metadata, reported relationship interpretation and query access. Investigation
examples help test that access without defining the catalog's overall scope.
