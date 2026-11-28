# Medicare Part B drug spending

`cms_part_b_drug_spending` retains one HCPCS billing-code observation for the
explicit benefit year of an admitted CMS Part B release. The main file has no
manufacturer field. `manufacturer_information_status` states that limit, and
`source_resources_url` points to the exact release's supporting resources.
The separately retained manufacturer workbook lists multiple associations
per HCPCS; a join must not repeat or allocate the spending. Those literal
associations and HCPCS-level drug-use descriptions live in the separate
`cms-part-b-associations` family (`cms_part_b_manufacturer_associations` and
`cms_part_b_drug_use_information`, `ss-mb8j`).

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

## Manufacturer and drug-use associations (`cms-part-b-associations`)

The CMS Part B supporting workbook linked from `source_resources_url`
(`Medicare-Part-B-Drug-Dashboard-Manufacturer-Drug_use - 260602.xlsx` for
benefit year 2024) publishes two worksheets with distinct grains:

* `cms_part_b_manufacturer_associations` (`Manufacturer Summary`, many rows per
  HCPCS): preserves every literal `(hcpcs_code, drug_description, brand_name,
  generic_name, manufacturer_name)` row with exact 1-based `source_row_number`,
  `source_sheet`, `source_workbook_sha256`, and linked Part B release identity.
  CMS truncates `Manufacturer` at 15 characters on many rows (for example,
  `Grifols Therape`, `Adma Biologics,`) and leaves brand, generic, and
  manufacturer blank on some wound-dressing/skin-substitute codes; all strings
  remain literal and `spending_allocation_status = 'not_allocated_in_source'`.
* `cms_part_b_drug_use_information` (`Drug Use Information`, 1 row per HCPCS):
  preserves literal brand/generic display markers (`*`), CMS's clinical
  monograph text (`drug_uses`), explicit `drug_use_status` (`summary_provided`,
  `not_available`, `empty`), and CMS's `***` disclaimer text in
  `source_selection_json`. Upstream CMS text discrepancies (such as `Q9966` and
  `Q9967` carrying Iohexol prose under Iodixanol in the 2024 workbook) are
  retained verbatim rather than edited.

Build a candidate generation from retained central-archive originals without
touching `cms-part-b`:

```sh
python -m spicy_regs.pipelines.cms_part_b_associations build \
  --captures "$CMS_PART_B_ASSOC_CAPTURES" --output-dir "$CMS_PART_B_ASSOC_OUTPUT"
```

To inspect manufacturers and drug uses alongside spending without ever
multiplying `total_spending_usd`, aggregate `cms_part_b_manufacturer_associations`
to `(benefit_year, hcpcs_code)` before joining to `cms_part_b_drug_spending`:

```sql
WITH m AS (
  SELECT benefit_year, hcpcs_code,
         COUNT(*) AS association_row_count,
         LIST(brand_name ORDER BY source_row_number) AS brand_names,
         LIST(manufacturer_name ORDER BY source_row_number) AS manufacturers
  FROM cms_part_b_manufacturer_associations
  GROUP BY benefit_year, hcpcs_code
)
SELECT s.hcpcs_code, s.benefit_year, s.total_spending_usd,
       m.association_row_count, m.manufacturers,
       u.drug_use_status, u.drug_uses
FROM cms_part_b_drug_spending AS s
LEFT JOIN m USING (benefit_year, hcpcs_code)
LEFT JOIN cms_part_b_drug_use_information AS u USING (benefit_year, hcpcs_code)
WHERE s.hcpcs_code = '90371';
```
