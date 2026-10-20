# Medicare Part B drug spending

`cms_part_b_drug_spending` retains one HCPCS billing-code observation for the
explicit benefit year of an admitted CMS Part B release. The main file has no
manufacturer field. `manufacturer_information_status` states that limit, and
`source_resources_url` points to the exact release's supporting resources.
The separately retained manufacturer workbook lists multiple associations
per HCPCS; a join must not repeat or allocate the spending. The nonadditive
association-table follow-up is `ss-mb8j`.

SpicyDocs owns exact catalog selection, dictionary/count controls, bounded API
pages and source admission/replay. This mapper uses that admitted release and
the existing CMS evidence and generation writer. It acquires nothing. Part B
has its own identity, measure names and dictionary requirements; Part D and
Medicaid keep their existing semantics.

The [publisher methodology](https://data.cms.gov/sites/default/files/2023-02/DSD_PTB_R22_20230125_Methodology%20508.pdf)
defines spending as Medicare payment plus deductible and coinsurance for
Part B fee-for-service claims. Medicare Advantage is excluded. CMS also
excludes specified claim types, non-primary-payer claims and drugs with fewer
than 11 claims in the latest year, and redacts older-year information below
that threshold. An absent drug is not zero spending. A particular blank has
no reason code and is not automatically labelled suppressed.

Dosage units equal the amount in one HCPCS billing unit. Different strengths
with different codes remain separate. Claims and unique beneficiaries are
distinct denominators; beneficiary counts across drugs are not additive.
Published averages remain exact source decimals rather than recomputed
quotients. The ASP payment amount is the source average of quarterly payment
amounts, including the statutory six percent or other applicable add-on.
It is neither a current market price nor spending.

Values use `DECIMAL(38,12)` without rounding, with separate null, blank,
nonnumeric and unrepresentable statuses. Original outlier flags and name
markers remain visible. A single star indicates multiple names; two stars
indicate unavailable names replaced by the HCPCS short description. The
complete wide row retains earlier years, trend fields and unknown fields.
Normalized columns describe only the explicitly selected latest benefit year.
Never combine Part B, Part D and Medicaid populations without independently
establishing compatibility.

Use the ordinary source descriptor documented for [Part D](cms-part-d.md),
with exact Part B source pins and `benefit_year: 2024`:

```sh
python -m spicy_regs.pipelines.cms_part_b \
  --selection "$CMS_PART_B_SELECTION" --output-dir "$CMS_PART_B_OUTPUT"
```

The writer checks the dictionary before the table, refuses repeated HCPCS
codes and incomplete populations, and preserves prior output on failure.
A refresh selects a new explicit release and fresh output directory.

```sql
SELECT hcpcs_code, benefit_year, brand_name, total_spending_usd,
       dosage_units, claims, beneficiaries, spending_per_dosage_unit_usd,
       outlier_flag_raw, value_statuses_json,
       json_extract_string(source_locator_json, '$.request_url') AS source_url,
       json_extract_string(source_locator_json, '$.json_pointer') AS row_pointer,
       source_release_digest
FROM cms_part_b_drug_spending
ORDER BY total_spending_usd DESC NULLS LAST, observation_id
LIMIT 10;
```

An observed count-matching source walk does not prove a frozen publisher
population or historical completeness. Source qualification, local generation
acceptance and public selection are separate evidence.
