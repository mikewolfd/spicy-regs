# Reported FEC relationships

`transforms.fec_relationships` maps retained source rows into a local relationship
observation table. It covers candidate–committee links, principal and other
authorized committees, connected organizations, affiliation names, leadership
PAC sponsors and relationships stated on selected original Forms 1/2 and their
supplements. It does not publish a dataset or change `fec_committees` columns.

The `build-fec-observations` command now delivers these observations with their
complete metadata companions through the existing rollup upload path. Local
builds are the default. A successful local build does not establish that these
tables are published on the public MCP server.

## Build selected retained inputs

```sh
uv run --frozen build-fec-observations \
  --manifest /path/to/retained-inputs.json \
  --output-dir /path/to/output
```

This creates a new `fec-observations-<run-id>` directory containing:

| Table | Row meaning |
| --- | --- |
| `fec_source_records` | One selected source observation, identified by `collection_id` and `source_record_id`; complete provider record and native metadata, source digest, URL, observation time and exact source coordinates. |
| `fec_collections` | One explicitly selected collection, including requested-empty results; requested scope, verified counts, coverage limits and source outcome. |
| `fec_relationships` | Source-reported assertions and absence states from supported committee API, original statement and explicitly mapped bulk inputs. |

The manifest is JSON with `version: 1` and a nonempty `collections` list. Each
collection names a unique `collection_id`, a `source_family` ID from the official
FEC source catalog, a `profile`, and a `blob_root`. Relative filesystem paths
resolve beside the manifest. Choose one input mode:

- **Retained API query:** `profile` is `committee`, `candidate`, `filing`, `legal`
  or `audit`; `captures` contains the exact SpicyDocs capture descriptors in
  request order. Existing provider parsers verify original digests, selected
  membership, pagination, counts and native identifiers. Missing pages refuse.
- **Retained original or ZIP member:** `profile` is `positional`; `scope` is the
  result of SpicyDocs `positional_row_scope`. This pins the original and explicitly
  selects format, encoding, delimiter, quoting and optional ZIP member ordinal
  and name. Complete source bytes are verified before parsing. Header rows,
  blank rows, duplicates, signed amounts and amendment flags survive.
- **Existing source release:** `release_path`, `artifact_sha256` and
  `verifier_implementation_id` pin an existing SpicyDocs release. Supported
  profiles additionally include `bulk`. The source reader checks artifact
  membership and replays source evidence. Releases with rejected or discarded
  observations refuse this complete companion delivery.

An existing release is optional. No input mode downloads source data. Explicit
source query scope remains visible; none establishes a complete historical FEC
population, complete legal case files or a current amendment view.

For positional data, an optional `field_mapping` associates a verified header
observation with its rows:

```json
{
  "header_collection_id": "linkage-header-2024",
  "header_row_ordinal": 0,
  "data_has_header": false,
  "relationship_family": "linkage",
  "cycle": 2024
}
```

The selected header collection must appear earlier in the manifest. An embedded
CSV header instead names its own collection and uses `data_has_header: true`.
Its exact nonempty, unique field names must match every nonblank data row's
field count. `metadata_json` then contains the named source fields, including
empty strings and literal numbers; `source_record_json` still contains every
original positional field and coordinate. Header provenance is retained in
`source_locator_json.field_mapping`, and collection metadata records the chosen
mapping. Header and blank rows keep their positional metadata.

`relationship_family` and `cycle` are optional together. The supported existing
maps are `linkage`, `candidate_master`, `committee_master`, `form1_bulk` and
`leadership`. Source candidate election year and selected bulk cycle remain
separate. Named source fields do not establish that a summary row represents an
individual entity; publisher aggregate and sentinel identifiers remain literal.

Mapped source ID fields also populate `committee_id` and `candidate_id` for
direct joins. Recognized publisher spellings include `CMTE_ID`, `COMMITTEE_ID`,
`Committee_Id`, `CAND_ID`, `CANDIDATE_ID`, `Cand_Id` and `cand_id`, alongside the
lowercase API spellings. These columns retain literal source roles and empty
strings. A source's aggregate placeholder remains an aggregate placeholder;
the column does not validate a person, committee or affiliation. Conflicting
aliases in one row refuse delivery instead of choosing one or filling a blank.
Every original alias and field remains in the named metadata and literal record.

Some summary ZIPs, including `weball`, `webl` and `webk`, publish their ordered
field definitions on an official HTML description page. Select that retained
dictionary directly instead of manufacturing a CSV header:

```json
{
  "dictionary": {
    "blob_root": "/path/to/retained-dictionaries/blobs",
    "capture": {
      "requestUrl": "https://www.fec.gov/campaign-finance-data/all-candidates-file-description/",
      "responseSha256": "sha256:<exact retained digest>",
      "byteSize": 32602,
      "observedAt": "2026-09-12T13:42:59.888472+00:00",
      "representation": "opaque"
    }
  },
  "data_has_header": false
}
```

Use this object as `field_mapping`; dictionary and header-collection modes are
mutually exclusive. SpicyDocs verifies the HTML and reads one explicit
`Column name` / `Field name` / `Position` table. Positions must run from one
without gaps, names must be unique and nonempty, and every data row must match
the dictionary's width. Surrounding HTML layout whitespace is removed from
field-name keys; literal cells, fragments and source spelling remain available.

Each data row retains the dictionary digest, URL and table locator. Complete
field definitions, cell coordinates and original text fragments appear once in
`fec_collections.collection_outcome_json.tableFieldDefinitions`; the row locator
identifies that collection and JSON location. `definitions_pointer` is relative
to the decoded `collection_outcome_json` value. Exact input rows remain in
`source_record_json`. Selecting a description for a historical file is explicit;
matching field counts does not prove historical semantic compatibility, establish
financial types or choose an amendment policy.

For API records, `metadata_json` follows SpicyDocs' metadata/body separation and
exact decimal-string representation. Original JSON numbers and embedded bodies
remain in the pinned original; `assets_json` and `embedded_bodies_json` retain
their source references. Full nested fields are accessible through DuckDB's JSON
functions without adding a new table schema for each source family.

The local builder stages and validates all three outputs, then installs the new
directory together. Any late input failure leaves earlier generations intact.
The Python `build_fec_observations(manifest, output_dir)` API requires a fresh
destination. The rollup creates one automatically and seals its complete output
set through the [generation publisher](generation-publication.md). Optional
`--no-skip-upload` checks shrink limits, uploads immutable members, verifies their
remote bytes and conditionally switches one publication pointer. A failed member
upload leaves the prior published family selected. Retain the input manifest and
pinned originals with each generation; the generation artifact verifies output
membership and bytes, not complete source coverage. Overlapping selections remain
separate observations.

Relationship locators include their exact companion identity. A consumer can
join without guessing from names or choosing a current amendment:

```sql
SELECT r.relationship_type, r.value_status, m.metadata_json
FROM fec_relationships AS r
JOIN fec_source_records AS m
  ON m.collection_id = json_extract_string(r.source_locator_json, '$.collection_id')
 AND m.source_record_id = json_extract_string(r.source_locator_json, '$.source_record_id')
 AND m.source_sha256 = r.source_sha256;
```

The native relationships mapper also remains available directly:

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
The [selected census delivery](/Users/mikewolfd/Documents/Codex/fec-handoff-fixes/integration/census-delivery-2026-09-12.md)
now admits current API metadata through a local SpicyDocs release, the existing
committee writer and a complete DocSpec metadata catalog. The retained-input
builder above delivers selected relationships and their companions without
requiring a new immutable release. Full historical acquisition and remote
publication remain separate work.
The retained committee-history dump stops at 2022; its
recent object date does not make it a substitute for the selected cycles.
