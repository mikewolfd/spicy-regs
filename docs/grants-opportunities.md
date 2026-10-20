# Grants.gov opportunities and forecasts

`grants_opportunity_observations` keeps every admitted record in each explicitly
selected Grants.gov v2 daily XML export. One row identifies a source release,
the XML notice kind and the native opportunity ID. These are opportunities,
not USAspending awards.

The XML distinguishes `synopsis` and `forecast` records. A synopsis does not
prove an opportunity is currently posted or open. Export membership, a past
deadline or an archive date does not establish closed, deleted or withdrawn
status. The retained October 20, 2026 export contains historical dates, so it
must not be described as active-only. Different selected snapshots remain
distinct even when their native opportunity IDs match.

## Source meaning and values

SpicyDocs owns the complete ZIP/XML traversal, native identity, original bytes,
record order and source release admission. The domain mapper streams the admitted
records through the existing atomic Parquet writer. It refuses an incomplete
or rejected source population and publishes no generation on failure.

The table retains every direct XML field occurrence, in order, with its
expanded name, decoded text, attributes and XPath. XML decoding occurs once;
embedded HTML is not unescaped again. Repeated eligibility codes, funding
categories, instruments and Assistance Listing references remain ordered JSON
arrays. Assistance Listing numbers remain strings, including zeros.

Eligibility and funding-category labels reuse RefSpec's
`registry/grants_gov_codes.py` reader at
`f8857c8669a412bd6c254aa0b3a23cf05a52e04d`, with the official
[code page](https://www.grants.gov/api/status-codes) captured August 3, 2026
and pinned in `reference/refspec/grants-gov-codes.json`. Unknown codes remain
literal and receive a null label. “Other” and “Unrestricted” codes still retain
their additional eligibility text. The code list does not define opportunity
status or funding instrument semantics.

Dates are parsed only from valid `MMDDYYYY` text. Missing, blank and unparsed
values remain distinct in `value_statuses_json`; their exact wording remains
in `raw_fields_json`. Forecast estimated dates are separate from synopsis
dates. Capture time, export date, publisher update date and version are separate.

Amounts use exact `DECIMAL(38,2)` when representable, with no rounding.
The literal `none`, blanks, missing values and unrecognized text remain
distinct. A value outside this decimal range or scale remains raw with
`unrepresentable` status. `amount_unit` is null because the selected XML controls
do not establish a unit for every amount; no USD assumption is added.

## Local build and query

The command acquires nothing and changes no public selection:

```sh
python -m spicy_regs.pipelines.grants_opportunities \
  --selection "$GRANTS_SELECTION_JSON" --output-dir "$GRANTS_OUTPUT"
```

The selection lists admitted source releases and their exact retained ZIPs:

```json
{
  "sources": [{
    "release": "/local/source-release",
    "blobs": "/local/source-blobs",
    "logicalId": "<exact source logical ID>",
    "artifactDigest": "sha256:<exact release digest>",
    "verifierImplementationId": "<accepted source verifier ID>"
  }],
  "archives": {
    "sha256:<original ZIP digest>": "/local/retained-export.zip"
  }
}
```

The ordinary source-evidence writer retains the compressed original once per
distinct digest and records each source release and its population outcome.
No full decoded XML file is created. `build_generation` then admits the table.
Normal `sources.publication.publish_generation` can publish the resulting
generation with its evidence directory. Source profile packaging must be adopted
before the command can run from an installed SpicyDocs wheel.

Point `SPICY_REGS_DATA_DIR` at the generated `generation/` directory for the
existing SQL consumer:

```sql
SELECT opportunity_id, opportunity_number, version, notice_type, agency_name,
       close_date, close_date_explanation, eligibility_text,
       json_extract_string(source_locator_json, '$.request_url') AS archive_url,
       json_extract_string(source_locator_json, '$.source_xpath') AS record_path,
       source_release_digest
FROM grants_opportunity_observations
WHERE agency_code = 'DOS-SA'
  AND json_contains(eligibility_codes_json, '"25"')
  AND close_date >= DATE '2014-09-01'
ORDER BY snapshot_date, opportunity_id;
```

Use forecast estimated dates explicitly when searching forecasts. Include
`source_release_digest` in comparisons of multiple snapshots; do not count
revisions as distinct opportunities. A changed-version fixture demonstrates
refresh behavior. An absent record remains absent from that snapshot without
creating a withdrawal event.

Focused local fixtures prove mapping, refusal, ordinary publication and SQL
behavior. Complete real-export qualification and public delivery are separate
acceptance steps; this implementation does not claim either is already complete.
