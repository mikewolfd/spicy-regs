# Medicaid drug spending

`cms_medicaid_drug_spending` exposes the five reported measures for an explicitly
selected benefit year from one complete admitted CMS Medicaid release. The first
scope is national drug/manufacturer observations for 2024. The `program` column
is `Medicaid`; Part D populations and beneficiary measures do not apply.

SpicyDocs owns the catalog, versioned API selection, original dictionary, bounded
page acquisition and source replay. This domain path acquires nothing. It shares
exact-value parsing, population checks, original retention and ordinary atomic
publication with Part D. Each program owns its measure names and schema. Every
wide source row is retained once, including earlier years and unknown fields.
The normalized columns describe only the selected benefit year.

## What the amounts mean

The [CMS annual methodology linked by the selected release](https://data.cms.gov/sites/default/files/2025-05/DSD_MCD_RY25_Methodology_WDDSE%20508.pdf)
defines spending as reimbursement by Medicaid and non-Medicaid entities,
including federal and state reimbursement and dispensing fees, without state
rebate deductions. These are not net spending or current prices. Beneficiary
counts are unavailable and are not fabricated.

CMS reports `Overall` alongside individual manufacturers for a brand/generic
drug. Filter `aggregation_level = 'all_manufacturers'` for drug comparisons, or
`aggregation_level = 'manufacturer'` for manufacturer comparisons. Adding both
levels double-counts spending. Distinct source releases remain distinct as well.
Dosage units vary by drug. The reported average per unit is claims-weighted over
strengths, forms and routes; it is preserved rather than recomputed by dividing
spending by units. A brand-name `*` marks multiple administration routes, and
`^` marks an outlier requiring caution with unit spending. Both remain literal.
The annual outlier flag also remains visible; it never removes a row.

The methodology excludes over-the-counter drugs and drugs with fewer than 11
claims in the latest year, and redacts earlier-year information below that
threshold. The selected release is therefore not all Medicaid services or a
complete inventory of drugs used. An absent drug does not prove zero spending.
A particular blank has no source reason code, so it remains blank/unreported
without asserting that it was suppressed. Earlier-year blanks survive in
`raw_row_json` and must be queried with the year explicit.

Exact decimal values use `DECIMAL(38,12)` without rounding. Missing, blank,
nonnumeric and unrepresentable values have separate statuses and retain their
original cells. Null never means zero. The source date and capture time do not
establish freshness beyond the selected release. The API walk matches its
observed count; it is not a publisher-frozen membership snapshot.

## Local build and query

Use the ordinary source descriptor shape documented for [Part D](cms-part-d.md),
with the exact Medicaid release pins and `benefit_year: 2024`:

```sh
python -m spicy_regs.pipelines.cms_medicaid \
  --selection "$CMS_MEDICAID_SELECTION" --output-dir "$CMS_MEDICAID_OUTPUT"
```

The output includes the ordinary `generation/` and source-evidence artifact.
The writer retains every original catalog, dictionary, statistics response and
page once by digest, validates the selected dictionary before writing the table,
and refuses incomplete populations, duplicate drug/manufacturer identities or
manufacturer rows without their Overall comparison. Failed writes preserve a
prior table. A refresh uses a new explicitly selected source release and fresh
output directory; it does not silently replace a prior generation.

The normal publication path and SQL consumer accept this generation. For example:

```sql
SELECT program, benefit_year, brand_name, generic_name, total_spending_usd,
       claims, weighted_spending_per_unit_usd, outlier_flag_raw,
       json_extract_string(source_locator_json, '$.request_url') AS source_url,
       json_extract_string(source_locator_json, '$.json_pointer') AS row_pointer,
       source_release_digest
FROM cms_medicaid_drug_spending
WHERE aggregation_level = 'all_manufacturers'
ORDER BY total_spending_usd DESC NULLS LAST, observation_id
LIMIT 10;
```

Actual population, local qualification and public availability are established by
the selected generation and its evidence, separately from this schema's design.
