# Explorer join audit — 2026-10-03

The public catalog contains 161 tables. The previous website snapshot left 81
without any declared join and 83 without a join to another currently published
table. These are different counts: a declaration whose other table is absent
cannot support navigation.

The refreshed metadata provides 204 usable navigation relationships across 149
tables. Twelve tables retain explicit dispositions: seven are standalone indexes
or processing records, and five require additional handling. No relationships
were inferred from matching column names, titles or person names.

## What changed

- Reuse the producer's 144 current declarations. This supplies relationships for
  50 of the 53 FEC tables that the old website left unconnected.
- Preserve the website's 36 scorecard relationships and their recorded origin.
- Add 40 documented navigation relationships in
  [`explorer_join_additions.json`](../src/spicy_regs/explorer_join_additions.json).
  These are separate from producer references and operational regression floors.
- Keep a disposition for every published table in
  [`join_audit.json`](../src/spicy_regs/join_audit.json).

There are 220 declarations in total. Sixteen whose endpoints are outside the
current publication index are omitted from usable navigation and explained by
the metadata publisher. A declaration does not make an unpublished table
available.

## What was checked

The [measurement receipt](evidence/explorer-joins-2026-10-03.json) records the
publication-index digest, immutable input URLs, SQL, distinct child keys,
missing keys, parent duplicates and resulting row counts for every addition.
All selected Parquet row groups were checked, reading only the relevant columns.
These are complete selected-input measurements, not samples of those inputs.

All 40 parent keys were unique in the measured inputs. Thirty-one relationships
resolved every non-null key; seven have documented partial scope; two had no
non-null child keys. The two empty-key paths are backfill attempts to bills and
House communications to Congressional Record issues. Neither is reported as a
successful live traversal.

The seven partial relationships are:

| Relationship | Missing distinct keys | Meaning |
| --- | ---: | --- |
| CBO estimates → feed publications | 3 / 14,800 | The selected feed lacks three IDs; this check does not establish why. |
| Citation map, citing opinion → opinions | 262 / 7,511,929 | Both opinion ID and export date must match. |
| Citation map, cited opinion → opinions | 1 / 4,519,538 | Both opinion ID and export date must match. |
| Parentheticals, describing opinion → opinions | 176 / 1,852,737 | Both opinion ID and export date must match. |
| Document citations → held-field reads | 67 / 79 | Print-derived citations have separate package parents. |
| Document citations → budget volumes | 53 / 79 | Only budget-body identities belong to this parent. |
| Document citations → committee activity reports | 38 / 79 | Only activity-report body identities belong to this parent. |

The additions preserve member-vote identity, service-term identity, export
edition and source-body digest where applicable. In particular, PDF extractions
use the opinion ID **and** native SHA-1; a changed source body cannot silently
become the extraction's input.

Five bounded live checks additionally follow a key in both directions for
definition evidence, PDF extractions, member votes, activity-report citations
and FEC source collections. Their precise sampling bounds are recorded
separately from the complete measurements. Offline tests add conflicting scope
values and NULL keys to verify that partial identifiers do not cross scopes.

These results describe the selected inputs on the audit date. They do not assert
complete source history, future uniqueness or a new minimum resolution rate.
The stored SQL can be replayed against the exact recorded URLs without moving
publication pointers.

## Tables that remain without usable navigation

| Tables | Disposition |
| --- | --- |
| `bill_family_archives`, `bill_family_backfill_walks` | Standalone processing checkpoints, not individual bill identities. |
| `crs_reports`, `gao_decisions`, `nominations`, `treaties` | Standalone published indexes without held structured reference keys. |
| `senate_expenditures` | Standalone extracted rows; no held body-parent table. Office labels are not person IDs. |
| `cfr_sections`, `comment_attributes` | Other endpoints of existing declarations are outside the current publication index. |
| `fcc_filings`, `fcc_proceedings` | Membership needs JSON-array expansion using source-stated proceeding names. |
| `fec_relationships` | Polymorphic IDs need entity-type and value-status conditions; source-record keys need JSON extraction. |

The last three groups remain explicit work rather than guessed scalar links.
Source pointers and table-specific explanations are included in the published
audit metadata.
