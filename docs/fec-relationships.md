# Reported FEC relationships

`transforms.fec_relationships` maps retained source rows into a local relationship
observation table. It covers candidate–committee links, principal and other
authorized committees, connected organizations, affiliation names, leadership
PAC sponsors and relationships stated on selected original Forms 1/2 and their
supplements. It does not publish a dataset or change `fec_committees` columns.

Use `bulk_relationships`, `api_relationships` or `statement_relationships` for
each verified input row, then stream their results to
`write_fec_relationship_rows(records, destination, batch_size=2000)`. Evidence
requires the original SHA-256, URL, observation time and exact source locator:
CSV data-row number and ZIP member, API JSON Pointer, or filing byte range.
The caller verifies each source once and retains the originals. The shared
Arrow writer keeps one bounded batch and replaces the destination after full
consumption; a late error preserves the previous output.

`value_status=reported` marks a populated source value. It does not establish
that the relationship is true or identify a name-only target. `reported_none`,
`empty_string`, `empty_list`, `null` and `missing_field` are observations, not
relationship edges. `source_id_shape` validates syntax only. Invalid candidate
IDs remain in their source role; a committee-shaped value is not retyped as a
committee. No name matcher runs and no row enters `org_committee_links`.

Bulk cycle and candidate election year remain separate. Current API rows have
`cycle=NULL`; the original `cycles`, dates and source fields remain in
`source_fields_json`. Candidate IDs alone are labeled `committee_candidate`;
authorization requires an explicit source designation or statement role.
Each reported array element and duplicate survives, including separate sponsor
ID and sponsor-name assertions. There is no deduplication or latest-wins policy
across different sources or amendments.

For statements, the supported versions are **8.3 and 8.4**. The positions come
from the FEC's `FEC_Format_v8.3.xlsx` and `FEC_Format_v8.4.xlsx` in the official
[format archive](https://www.fec.gov/files/bulk-downloads/electronic/eFilingFormats.zip).
F1 line 6 moves two fields in 8.4; F1S supplies additional affiliations and
joint-fundraising participants. The literal ORG/AFF/JFR/LPS code determines the
reported role. Unknown codes retain `reported_affiliation`; unsupported versions
refuse. F2 and F2S provide principal and other authorized committees. The original
byte ranges and source fields preserve spelling, dates, blank values and both
targets when committee and candidate IDs coexist. Other filing versions remain
available through SpicyDocs' literal reader without an assumed relationship map.

The current [qualification and coverage](/Users/mikewolfd/Documents/Codex/fec-data-research-2026-09-11/integration/relationships.md)
covers all selected 2024/2026 bulk relationship rows, the complete current
cycle-filtered committee census and retained gap observations, plus every
relationship record in the selected original statements. Full historical
statement/profile acquisition, immutable distribution admission and publication
remain separate work. The retained committee-history dump stops at 2022; its
recent object date does not make it a substitute for the selected cycles.
