# CMS NPPES Provider Directory (`nppes_provider_observations`)

`nppes_provider_observations` stores literal provider and reference sidecar
observations from explicitly digest-pinned CMS National Plan and Provider
Enumeration System (NPPES) Version 2 (V.2) monthly replacement and weekly
incremental CSV bundles (`https://download.cms.gov/nppes/NPI_Files.html`).

## Source qualification and V2 semantics

Qualified on 2026-11-27 against the live November 2026 monthly full replacement
bundle (`NPPES_Data_Dissemination_November_2026_V2.zip`), monthly deactivation
report (`NPPES_Deactivated_NPI_Report_110926_V2.zip`), and three contiguous
weekly incremental V2 bundles (`110226_110826`, `110926_111526`,
`111626_112226`), together with the bundled `NPPES_Data_Dissemination_Readme_v.2.pdf`
(updated May 12, 2026) and `NPPES_Data_Dissemination_CodeValues.pdf` (updated
February 1, 2025). See [nppes-v2-qualification-2026-11-27.md](evidence/nppes-v2-qualification-2026-11-27.md)
for exact URLs, headers, digests, Central Directory listing, and raw visual review.

Key qualified V2 behaviors:

- **Main file (`npidata_pfile_*.csv`, 330 columns):**
  - Active providers carry `Entity Type Code` `1` (Individual) or `2` (Organization).
  - Sparse deactivated rows disclose only `NPI` and `NPI Deactivation Date`
    (`MM/DD/YYYY`); all other 328 columns are blank, including
    `NPI Deactivation Reason Code` ("This is not publicly disseminated", V2 README p. 7).
  - Reactivated rows restore `Entity Type Code` (`1` or `2`) and descriptive
    fields while **retaining** their historical `NPI Deactivation Date` alongside
    `NPI Reactivation Date` (`reactivation_date >= deactivation_date`).
  - Whole-row replacement applies on weekly updates: a sparse deactivation clears
    prior descriptive fields in the current directory state without erasing
    historical observation rows.
- **Reference sidecars (`othername_pfile`, `pl_pfile`, `endpoint_pfile`):**
  - `othername` (4 columns, Type 2 NPIs; flagged by `Provider Other Organization Name Type Code = 6` in main),
    `practice_location` (10 columns, non-primary practice locations for Type 1/2),
    and `endpoint` (19 columns, endpoints for Type 1/2).
  - Every NPI present in a weekly sidecar CSV appears in that week's `npidata`
    main CSV (100% across all nine audited November 2026 weekly sidecar files).
  - For NPIs touched in a weekly `npidata` file, CMS emits the NPI's complete
    current sidecar set for each sidecar kind (so omission for a touched NPI
    removes prior rows for that kind, whereas absence of an NPI from a weekly
    `npidata` file leaves prior sidecars untouched).
  - **Deactivated NPIs still appear in CMS sidecar files:** CMS suppresses
    descriptive fields in `npidata` upon deactivation without stripping the
    NPI's historical rows from `othername`, `pl`, and `endpoint`. Consumers must
    always join main-row lifecycle state (`deactivation_date`, `reactivation_date`)
    rather than treating sidecar presence as active provider status.
  - Exact duplicate rows occur natively in `pl` and `endpoint`; every literal
    occurrence is retained with its exact `record_number` and byte coordinates.
- **Identifier and inference boundaries:**
  - `npi` is validated under the 45 CFR 162.406 Luhn check digit via RefSpec
    (`refspec.registry.nppes_npi_identifiers.validate_npi`).
  - **NPI is strictly distinct from CMS Certification Number (CCN):** join only
    when `identifier_kind = 'NPI'`, never by bare string match against a CCN even
    when a 10-character CCN shares characters with an NPI.
  - Always use a `LEFT JOIN` to a unique `(npi)` main-record snapshot so
    unmatched requests survive and repeated sidecar rows never multiply payment
    or utilization measures.
  - NPI issuance does not establish state licensure, clinical credentials,
    Medicare participation, or practice affiliation.

## Local build, lookup, and dry-run publication

`transforms.build_nppes_providers` exposes:

- `read_v2_zip_member` / `read_v2_csv_cohort`: atomic archive and member pin
  verification, exact V2 header check, RefSpec NPI validation, duplicate main
  NPI refusal, and `classify_v2_main_state`.
- `build_observations` / `build_observations_multi`: atomic Parquet writer for
  `nppes_provider_observations.parquet`.
- `apply_main_release` (main-only) and `apply_v2_release` (qualified main +
  sidecars): immutable monthly replacement and contiguous weekly update engine.
- `pipelines.nppes_providers.build`: packages `nppes_provider_observations.parquet`
  and `CaptureEvidence` into an admitted `nppes-providers` generation for
  `publish-local-generation --family nppes-providers`.

For an offline bounded excerpt lookup example, run:

```sh
PYTHONPATH="$DOCS/src:$REGS/src:$REFSPEC/src" \
  uv run --no-sync --project "$REGS_RUNTIME" python \
  "$REGS/examples/nppes_provider_lookup.py" \
  --csv "$ORIGINAL_EXCERPT" --header "$ORIGINAL_HEADER" \
  --sha256 "$EXCERPT_SHA256" --bytes "$EXCERPT_BYTES" \
  --source-url "$EXACT_ARCHIVE_URL" --member "$EXACT_MEMBER" \
  --requests "$REQUESTS_JSON" --output "$NEW_LOCAL_PARQUET"
```
