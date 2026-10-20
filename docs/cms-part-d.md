# Medicare Part D drug spending

`cms_part_d_drug_spending` exposes one explicitly selected benefit year from a
complete admitted CMS Part D spending release. SpicyDocs owns current catalog
discovery, the exact release and dictionary, bounded page acquisition, source
membership and replay. This mapper uses the existing atomic Parquet writer and
generation publisher. It acquires nothing.

The first selected scope is national drug/manufacturer observations for the
release's benefit year. Every original row remains available, including other
years and fields not described in the API dictionary. The table normalizes only
the explicitly selected year; it does not duplicate the full row for every year.
Source capture time and publication modification time do not establish current
drug prices or current utilization.

## Compare the same aggregation level

CMS supplies both `Overall` and named-manufacturer rows. For drug comparisons,
filter `aggregation_level = 'all_manufacturers'`. For manufacturer comparisons,
filter `aggregation_level = 'manufacturer'`. Adding both levels double-counts
spending. Source release identity remains part of every observation; different
releases of the same year must not be added together either.

Spending is gross spending before rebates and other price concessions, including
Medicare, plan and beneficiary payments. Beneficiaries are distinct within each
drug/year and are not additive across drugs. Dosage units can be tablets, grams,
milliliters or other lowest dispensable units. The source's weighted average
spending per unit is retained, not recomputed as total spending divided by units.
The original brand-name star indicates multiple administration routes.

Numeric fields use exact `DECIMAL(38,12)` only when representable without
rounding. Null, blank, nonnumeric and out-of-range/precision cells have separate
statuses; their original spellings remain. A blank is not automatically a
suppressed value or zero. The annual outlier flag remains visible and never
removes an observation.

## Local build and query

The command consumes a pinned local release and writes a fresh generation:

```sh
python -m spicy_regs.pipelines.cms_part_d \
  --selection "$CMS_PART_D_SELECTION" --output-dir "$CMS_PART_D_OUTPUT"
```

The selection names an explicit year and admitted source:

```json
{
  "benefit_year": 2024,
  "source": {
    "release": "/local/source-release",
    "blobs": "/local/source-blobs",
    "logicalId": "<exact source logical ID>",
    "artifactDigest": "sha256:<exact release digest>",
    "verifierImplementationId": "<accepted source verifier ID>"
  }
}
```

The ordinary source-evidence writer retains each original catalog, dictionary,
statistics response and data page once by digest. Normal
`sources.publication.publish_generation` can publish the resulting generation
with this evidence. No bare legacy table is selected for serving.

Point `SPICY_REGS_DATA_DIR` at the resulting `generation/` directory and use the
existing SQL consumer:

```sql
SELECT brand_name, generic_name, benefit_year, total_spending_usd,
       beneficiaries, outlier_flag_raw,
       json_extract_string(source_locator_json, '$.request_url') AS source_url,
       json_extract_string(source_locator_json, '$.json_pointer') AS row_pointer,
       source_release_digest
FROM cms_part_d_drug_spending
WHERE aggregation_level = 'all_manufacturers'
ORDER BY total_spending_usd DESC NULLS LAST
LIMIT 10;
```

The mapper rejects incomplete population counts, repeated source occurrences,
duplicate drug/manufacturer keys and manufacturer rows without an `Overall`
comparison row. A refusal preserves prior output. Focused fixtures establish
behavior for those inputs; real release reconciliation and public delivery
require their own evidence.
