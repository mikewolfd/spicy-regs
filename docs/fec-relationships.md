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
committee. The `subject_type` and `object_type` columns name the source field's
identifier or target role, not a legal entity classification. In particular,
`committee` covers FEC's C-prefixed filer role, including independent-expenditure
filers whose source label explicitly says "not a committee". API observations
retain `committee_type`, `committee_type_full`, `organization_type` and
`organization_type_full` when present, including a raw code with a null label.
Keep these qualifications with the observation. No name matcher runs and no row
enters `org_committee_links`.

Bulk cycle and candidate election year remain separate. Current API rows have
`cycle=NULL`; `cycles` stays in the complete source record, alongside fields
outside this narrow table. Candidate IDs alone are labeled `committee_candidate`;
authorization requires an explicit source designation or statement role.
Each reported array element and duplicate survives, including separate sponsor
ID and sponsor-name assertions. These arrays are independent: neither position
nor a null entry authorizes pairing or filling from the other array. Missing,
null and empty lists each emit an explicit observation for all three API arrays.
There is no deduplication or latest-wins policy across sources or amendments.

For API arrays, `source_locator_json.array_field` names the array and
`array_index` selects the element within the exact parent JSON Pointer.
`source_fields_json[array_field]` contains that literal element, including all
keys in a sponsor object. A null element retains its nonnull index; missing,
null or empty parent arrays have `array_index=null` and retain their original
absence/value state. Scalar observations have no `array_field`. This differs
from the earlier local output, which repeated the entire array on every row.
The top-level table columns are unchanged.

The original digest plus parent pointer locates the complete record. Keep its
complete metadata companion with the delivery so consumers can join directly to
`cycles`, detailed attributes and complete arrays. Do not treat the relationship
table as a complete entity record. The mapper copies selected array elements
once, for linear selected-field bytes as array length grows; it does not copy
`cycles` into every observation. It does not itself verify or package companions:
input completeness and their admission remain the caller's responsibility.

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

The [repaired selected output and verification](/Users/mikewolfd/Documents/Codex/fec-handoff-fixes/integration/fixes-2026-09-12/README.md)
cover the selected bulk relationship rows, current cycle-filtered census and
retained gap observations, plus the selected original statements. Required input
membership and independent source-role/state checks now qualify that output.
Full historical
statement/profile acquisition, immutable distribution admission and publication
remain separate work. The retained committee-history dump stops at 2022; its
recent object date does not make it a substitute for the selected cycles.
