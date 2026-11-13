# Medicare provider payments and utilization

`cms_medicare_provider` contains one rendering National Provider Identifier
(NPI) per calendar year from one explicitly selected CMS **Medicare Physician
& Other Practitioners - by Provider** release. The program label is Original
Medicare fee-for-service. The selected release, modification date, exact CSV,
dictionary, catalog and row locators remain attached to the observations.

SpicyDocs reuses the CMS DCAT-US 3 release resolver and reads the selected CSV
with Python's maintained `csv` reader. The complete file must match its SHA-256,
byte size, dictionary header and selected API population. SpicyRegs writes exact
`DECIMAL(38,12)` measures through the existing atomic table and generation path.
The source reader yields rows as it reads; callers must exhaust them into an
atomic destination. A failed final hash or count invalidates the entire candidate.
The raw-reader path does not claim an admitted SpicyDocs source release.

CMS's [dictionary](https://data.cms.gov/resources/medicare-physician-other-practitioners-by-provider-data-dictionary)
and [methodology](https://data.cms.gov/resources/medicare-physician-other-practitioners-methodology)
define the measures. Submitted charges, Medicare allowed amounts, actual Medicare
payments and geographically standardized payments have different meanings.
Allowed amounts include beneficiary and third-party responsibility. Standardized
payment removes geographic payment-rate differences; it is not risk adjustment.
Service units depend on the billed HCPCS code and are not interchangeable visits,
claims or clinical procedures. Unique beneficiaries and distinct HCPCS codes
cannot be added across providers or across the drug/medical subsets.

The drug and medical subsets remain columns beside provider totals. `*` means
suppression for fewer than 11 beneficiaries; `#` means counter-suppression to
prevent reconstruction. Those typed cells remain null with explicit statuses.
The mapper never subtracts totals to recover suppressed values. An unflagged
blank remains blank, and source zero remains zero. CSV empty cells do not encode
a distinct JSON null; the original CSV spelling remains in `raw_row_json`.
Beneficiary demographics, chronic-condition fields, risk scores and future fields
remain literal in that original row even when no normalized column selects them.

The source's provider summary is independently aggregated from Part B
non-institutional claims (excluding DMEPOS). It is not the sum of the redacted
provider/service detail. This delivery adds neither service detail nor geographic
aggregate rows. Provider location and specialty give comparison context; they do
not establish a beneficiary's location or a current directory match. Medicare
Advantage and non-Medicare activity are excluded. Payments do not measure provider
income, whole-practice activity, quality or fraud. Individual and organizational
NPIs can overlap in real practice without an available person-to-organization
allocation.

Build offline from a pinned local selection and exact retained original:

```sh
python -m spicy_regs.pipelines.cms_provider \
  --selection "$CMS_PROVIDER_SELECTION" --csv "$CMS_PROVIDER_ORIGINAL" \
  --output-dir "$CMS_PROVIDER_OUTPUT"
```

The selection contains `benefit_year`, `scope` from
`spicy_docs.sources.cms_provider.provider_scope` and `controls` descriptors
(`request_url`, `response_sha256`, `byte_size`, `observed_at`) for catalog,
dictionary and stats in that order. The CLI reads controls from the central raw
archive and verifies every pin. Use a fresh output directory for each revision.
Do not join multiple releases before selecting one `source_selection_digest`.
An optional provider-directory join must be a left join to a unique NPI relation,
so unmatched source providers survive and duplicate directory rows do not
multiply payment measures.

This comparison answers how selected providers' reported utilization and payments
differ within the same year, specialty and state. Ratios are derived USD per
source service unit, not source-published averages or quality measures:

```sql
SELECT npi, benefit_year, specialty, state, beneficiaries, services,
       submitted_charges_usd, allowed_amount_usd, medicare_payment_usd,
       standardized_payment_usd,
       medicare_payment_usd / NULLIF(services, 0) AS derived_paid_usd_per_service_unit,
       drug_suppression_indicator, medical_suppression_indicator,
       json_extract_string(source_locator_json, '$.request_url') AS source_url,
       json_extract_string(source_locator_json, '$.csv_record') AS csv_record,
       source_selection_digest
FROM cms_medicare_provider
WHERE benefit_year = 2024 AND specialty = 'Internal Medicine' AND state = 'MD'
ORDER BY medicare_payment_usd DESC NULLS LAST, npi
LIMIT 10;
```

A bounded sum may describe the reported payments for explicitly selected NPIs
within one release/year; it must name that cohort and report missing-value counts.
It does not establish statewide or national spending, population coverage, unique
beneficiary totals or a sum across alternative aggregation levels. Publication
selection and public acceptance remain separate from a qualified local candidate.
