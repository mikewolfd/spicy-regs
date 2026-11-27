# CMS NPPES Version 2 (V.2) Live Source Qualification and Raw Visual Review (2026-11-27)

- **Ticket:** `ss-jknq` (Wave 3 Lane 3B-2: CMS NPPES Provider Directory)
- **Qualified on:** `2026-11-27T14:59Z`
- **Publisher listing page:** `https://download.cms.gov/nppes/NPI_Files.html` (`HTTP/2 200`, `Content-Length: 7374`, `Last-Modified: Mon, 23 Nov 2026 07:07:22 GMT`, `ETag: "8dff0129f8429d63217e684e43eed4db:1795417642.101411"`, server `AkamaiNetStorage`, requested with explicit `User-Agent: SpicyStack-NPPES-Qualification/1.0 (+https://github.com/mikewolfd/spicy-regs)`)
- **Boundary:** Local qualification, serving registration, SpicyGov coverage definitions, and verified local canary generation + MCP DuckDB readback are complete. **No live R2 upload or live MCP cutover was performed.**

---

## 1. Qualified Live CMS NPPES V2 Archive Pair and Series

The previous October 2026 weekly archive (`NPPES_Data_Dissemination_101226_101826_Weekly_V2.zip`) returned HTTP 404 on 2026-11-13. Live inspection of `https://download.cms.gov/nppes/NPI_Files.html` on `2026-11-27` established a currently available November 2026 V2 monthly baseline, companion monthly deactivation report, and three ordered contiguous weekly incremental V2 archives:

| Role | Live CMS URL | HTTP | Content-Length | Last-Modified (GMT) | ETag | Accept-Ranges | Local Acquisition & SHA-256 |
| --- | --- | --- | ---: | --- | --- | --- | --- |
| **Monthly V2 Full Replacement** | `https://download.cms.gov/nppes/NPPES_Data_Dissemination_November_2026_V2.zip` | 200 | `1,130,479,129` (~1.08 GB) | `Tue, 03 Nov 2026 03:29:06 GMT` | `"33d578345178502a44a8147e66542734:1793676464.781416"` | `bytes` | HTTP Range Central Directory + full fileheaders + CSV start slices (not downloaded in full to preserve disk headroom) |
| **Monthly Deactivation V2 Report** | `https://download.cms.gov/nppes/NPPES_Deactivated_NPI_Report_110926_V2.zip` | 200 | `2,693,475` (~2.69 MB) | `Mon, 09 Nov 2026 07:01:06 GMT` | `"2295fac33e4ff438783f6b892d6389ed:1794207666.35764"` | `bytes` | `sha256:3bc64ebb331b72d2fc80a15c4b4a89d57810c69991b33985c5bc31b763504190` |
| **Weekly V2 #1 (Nov 2–8)** | `https://download.cms.gov/nppes/NPPES_Data_Dissemination_110226_110826_Weekly_V2.zip` | 200 | `6,490,692` (~6.49 MB) | `Mon, 09 Nov 2026 07:07:56 GMT` | `"82642bf5063178831548c67ee48ed6d2:1794208076.311807"` | `bytes` | `sha256:080d7707861576c58d769796c5991904c7bd8a183c2cb7db76159c9a98ca1e5a` |
| **Weekly V2 #2 (Nov 9–15)** | `https://download.cms.gov/nppes/NPPES_Data_Dissemination_110926_111526_Weekly_V2.zip` | 200 | `6,274,061` (~6.27 MB) | `Tue, 17 Nov 2026 17:31:07 GMT` | `"c846aec7ec3c07f6446db19670748b4f:1794936667.022531"` | `bytes` | `sha256:cfdac80d3056c24428d8a96dc4b02c5a3cda336c44ea6016e416657cb2e35054` |
| **Weekly V2 #3 (Nov 16–22)** | `https://download.cms.gov/nppes/NPPES_Data_Dissemination_111626_112226_Weekly_V2.zip` | 200 | `6,373,117` (~6.37 MB) | `Mon, 23 Nov 2026 07:07:23 GMT` | `"21967220060d2cecd1d2fff0e94fc6d4:1795417642.55267"` | `bytes` | `sha256:d0e73f9d00ee816d1053d9c1005bd429183b13fce3ebf55bc2fde382274d2897` |

### Publisher Date-Boundary Finding (Monthly vs Weekly)

- `NPI_Files.html` labels the monthly archive `NPPES Data Dissemination V.2 (November 09, 2026)`, matching the `November 09, 2026` deactivation report (`NPPES Deactivated Records as of Nov 09 2026`, 360,673 deactivated NPIs spanning `05/23/2005` through `11/08/2026`).
- However, the live monthly ZIP's actual HTTP `Last-Modified` is `Tue, 03 Nov 2026 03:29:06 GMT` (Monday Nov 2 evening EST) and all eight CSV members inside the ZIP carry the filename span `20050523-20261102`.
- Meanwhile, Weekly #1 (`110226_110826`) spans `2026-11-02` through `2026-11-08` (including 6,227 rows with `Last Update Date = 11/02/2026`), followed with zero gap and zero overlap by Weekly #2 (`2026-11-09` through `2026-11-15`) and Weekly #3 (`2026-11-16` through `2026-11-22`).
- **Operational consequence:** Never trust the HTML page's display date to compute predecessor cutoffs. Always derive the baseline's covered date from the ZIP member filename span (`20050523-20261102`), apply `110226_110826` (handling the `2026-11-02` boundary overlap explicitly when chaining from monthly), and then chain `110926_111526` -> `111626_112226` contiguously.

---

## 2. Monthly V2 ZIP Central Directory and Byte-Identical Fileheaders

HTTP Range inspection of `NPPES_Data_Dissemination_November_2026_V2.zip` (ZIP32 Central Directory at offset `1,130,478,237`, size `870` bytes, 10 entries) confirmed the exact bundle layout:

| Member Filename | Uncompressed Bytes | Compressed Bytes | CRC-32 | Local Offset | Verified Header / First Row |
| --- | ---: | ---: | --- | ---: | --- |
| `npidata_pfile_20050523-20261102.csv` | `11,776,341,517` (~11.78 GB) | `1,048,041,781` | `5422f7b1` | `66,147,896` | 330 cols; row 1: `1679576722`, Type `1`, `WIEBE`, `DAVID`, `A` |
| `npidata_pfile_20050523-20261102_fileheader.csv` | `12,267` | `1,385` | `09b8631b` | `1,114,189,766` | 330 cols; `sha256:1f781040d7dae44496be1729250e79114b6dd03f17c10d7d8965486052177679` |
| `othername_pfile_20050523-20261102.csv` | `50,408,886` | `13,621,399` | `c15453a1` | `1,114,191,243` | 4 cols; row 1: `1023011053`, `Vine Discount Pharmacy & Medical Supply`, `3`, `09/23/2005` |
| `othername_pfile_20050523-20261102_fileheader.csv` | `101` | `64` | `f351851c` | `1,127,812,725` | 4 cols; `sha256:131707e64518721fb70e173b0357611e809c987fc5053fdd55e3402fc9a96b58` |
| `pl_pfile_20050523-20261102.csv` | `122,937,948` | `41,383,154` | `c1db56da` | `24,764,419` | 10 cols; row 1: `1154324382`, `500 N Hiatus Rd Ste 200`, `Pembroke Pines`, `FL`, `330265213` |
| `pl_pfile_20050523-20261102_fileheader.csv` | `578` | `160` | `111c4857` | `66,147,649` | 10 cols; `sha256:99a384a4ef38149817c390af29c0c8bf733dec96076c2abf7103214c944bbc93` |
| `endpoint_pfile_20050523-20261102.csv` | `126,953,621` | `24,764,090` | `b6ea0fbb` | `0` | 19 cols; row 1: `1154324382`, `DIRECT`, `Direct Messaging Address`, `rclose13800@MHSDIRECT.NET` |
| `endpoint_pfile_20050523-20261102_fileheader.csv` | `431` | `154` | `15923a95` | `24,764,172` | 19 cols; `sha256:ce2daf6615e2e30bfadd1adfa5c63399942515abfed9317c52e5371bb3e48de9` |
| `NPPES_Data_Dissemination_CodeValues.pdf` | `2,833,301` | `2,195,375` | `83a6e115` | `1,127,812,883` | `sha256:8510d05f062befe7eed8afa5e5b165ee83e7ebe507c4ce605db189252861e119` (Updated Feb 1, 2025, 26 pages) |
| `NPPES_Data_Dissemination_Readme_v.2.pdf` | `517,521` | `469,809` | `2336ac3b` | `1,130,008,343` | `sha256:3c70a24e4a90af3399badf1bc63fc15231101042b039b03e73aaa88e2dc9cbe7` (Updated May 12, 2026, 25 pages) |

All four fileheader CSVs and both PDFs have identical byte lengths, CRCs, and SHA-256 digests across the November 2026 monthly archive and all three weekly V2 archives (and the 330-column main fileheader is byte-identical to RefSpec's July/August 2026 V2 pin `sha256:1f781040d7dae44496be1729250e79114b6dd03f17c10d7d8965486052177679`).

---

## 3. Bundled V2 README and CodeValues Rules

Extracted and reviewed in full from `NPPES_Data_Dissemination_Readme_v.2.pdf` (Updated May 12, 2026, 25 pages) and `NPPES_Data_Dissemination_CodeValues.pdf` (Updated February 1, 2025, 26 pages):

1. **V2 extended lengths & V1 sunset:** Effective `03/03/2026`, CMS retired V1. V2 expands First Name (35 chars) and Legal Business Name / Other Organization Name / Parent Organization LBN (100 chars).
2. **CSV quoting & masking:** Every CSV cell is enclosed in double quotes; embedded double quotes inside data values are converted by CMS to single quotes (README §1.1). CMS masks inappropriately entered identifiers as SSN `$$$$$$$$$`, IRS ITIN `*********`, and EIN `=========`, and suppresses EIN and Parent Organization TIN (README §1.1 p. 3).
3. **Monthly vs Weekly definitions:** Monthly files replace the previous month's file with all FOIA-disclosable provider data (README §1.2). Weekly files contain new/updated FOIA-disclosable provider data since the previous weekly or monthly generation (README §1.3).
4. **Non-disseminated `NPI Deactivation Reason Code`:** README Exhibit 2-1 (p. 7) explicitly states for column 39 (`NPI Deactivation Reason Code`): **"This is not publicly disseminated."** (CodeValues Exhibit 1-5 lists internal reasons `Death`, `Disbandment`, `Fraud`, `Other`, none of which appear in the public CSVs.)
5. **Other Name pointer:** README Exhibit 2-1 footnote (p. 6): if `Provider Other Organization Name Type Code` is `6`, additional organization other names appear in `othername_pfile`.
6. **README-vs-CSV header discrepancies (follow CSV fileheaders strictly):**
   - `pl_pfile` column 3 contains a literal double space in both README and CSV header: `Provider Secondary Practice Location Address-  Address Line 2`.
   - `endpoint_pfile` column 19 is printed in README Exhibit 2-4 (p. 23) as `Affiliation Address Line Postal Code`, whereas the actual published CSV header is `Affiliation Address Postal Code`.
7. **Sidecar deletion silence in README:** The bundled V2 README specifies schemas for `othername_pfile` (4 cols), `pl_pfile` (10 cols), and `endpoint_pfile` (19 cols), but does **not** document weekly sidecar removal or deactivation interaction rules. Those semantics were established empirically below.

---

## 4. Raw Visual Review of Live November 2026 V2 Main and Sidecar Rows

All three downloaded weekly V2 bundles (`91,874` main rows, `8,891` othername rows, `34,773` practice-location rows, `6,340` endpoint rows) were audited in full:

### 4.1 Main File Counts and Lifecycle Invariants

| Weekly Archive | Main Rows | Unique NPIs | Type 1 (Ind) | Type 2 (Org) | Deactivated Only (`et=""`) | Reactivated Only (`dd=""`) | Both Dates (`dd` & `rd`) | `Deact Reason` Non-empty | Bad Width |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `110226_110826` (`w0`) | 31,562 | 31,562 | 24,434 | 6,417 | 711 | **0** | 92 (92 `rd > dd`) | 0 | 0 |
| `110926_111526` (`w1`) | 29,288 | 29,288 | 22,349 | 6,264 | 675 | **0** | 104 (104 `rd > dd`) | 0 | 0 |
| `111626_112226` (`w2`) | 31,024 | 31,024 | 23,616 | 6,627 | 781 | **0** | 112 (111 `rd > dd`, 1 `rd == dd`) | 0 | 0 |

**Verified V2 lifecycle rules (zero exceptions across 91,874 rows):**
1. **Sparse deactivation (`2,167 / 2,167` rows):** Every currently deactivated row populates **only** `NPI` and `NPI Deactivation Date` (`MM/DD/YYYY`). All other 328 columns (including `Entity Type Code`, names, addresses, `Last Update Date`, `NPI Deactivation Reason Code`, `NPI Reactivation Date`) are empty strings `""`.
   - Witness examples:
     - `w1` record 600: `NPI=1659275139`, `NPI Deactivation Date=11/09/2026` (all other 328 cells `""`).
     - `w2` record 41: `NPI=1073285235`, `NPI Deactivation Date=11/16/2026` (all other 328 cells `""`).
2. **Reactivation ALWAYS retains both dates (`308 / 308` rows, `0` reactivation-only rows):** Every reactivated provider carries **both** its historical `NPI Deactivation Date` and `NPI Reactivation Date` with `reactivation_date >= deactivation_date` (`307` strictly greater, `1` same-day), plus active `Entity Type Code` (`1` or `2`) and restored descriptive fields.
   - Witnessed live cross-week transitions (`28` total across `w0`/`w1`/`w2`):
     - `NPI 1225978380`: `w0` sparse deactivated (`dd=11/05/2026`, `rd=""`, `et=""`) -> `w1` reactivated (`et=1`, `SAMUELS/KEMAR`, `dd=11/05/2026`, `rd=11/09/2026`, `lu=11/09/2026`).
     - `NPI 1023950664`: `w1` sparse deactivated (`dd=11/09/2026`) -> `w2` reactivated (`et=1`, `HUDA/NOOR-UL`, `dd=11/09/2026`, `rd=11/17/2026`, `lu=11/17/2026`).
     - `NPI 1205337821` (Type 2 Org): `w1` sparse deactivated (`dd=11/10/2026`) -> `w2` reactivated (`et=2`, `CHIROS ON 54 LLC`, `dd=11/10/2026`, `rd=11/17/2026`, `lu=11/17/2026`).
     - `NPI 1174491484`: `w1` active (`et=1`, `SIEGEL/DANIELLE`, `lu=11/14/2026`) -> `w2` sparse deactivated (`dd=11/16/2026`, `et=""`).
     - `NPI 1245805423` (Type 2 Org): `w0` active (`et=2`, `TESTING MD PC`, `lu=11/03/2026`) -> `w2` sparse deactivated (`dd=11/17/2026`, `et=""`).
3. **Upstream `spicy-docs` wheel follow-up note:** The currently vendored wheel `spicy-docs 0.85.0+readers.20261125.3` (`spicy_docs/sources/nppes.py:main_state()`) still raises `NppesSourceError("Combined deactivation/reactivation dates need qualified V2 semantics")` when both dates are present. `spicy_regs.transforms.build_nppes_providers.classify_v2_main_state()` and `read_v2_csv_cohort()` implement the qualified V2 rule (`reactivation_date >= deactivation_date` with `Entity Type Code in {"1", "2"}`) immediately; `spicy-docs` should adopt the same 2-line update in its next wheel release.

### 4.2 Sidecar Counts, Replacement/Removal Rules, and Deactivated-NPI Leakage

| Archive / Sidecar | Rows | Unique NPIs | In Week Main | Deactivated NPIs in Sidecar | Multi-Row NPIs | Exact Duplicate Rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `w0` `othername` | 2,871 | 1,884 | `1,884 / 1,884` (100%) | **44** | 771 | 0 |
| `w0` `pl` | 12,136 | 5,446 | `5,446 / 5,446` (100%) | **34** | 2,135 | 53 |
| `w0` `endpoint` | 1,958 | 1,486 | `1,486 / 1,486` (100%) | **20** | 302 | 18 |
| `w1` `othername` | 2,773 | 1,840 | `1,840 / 1,840` (100%) | **44** | 690 | 0 |
| `w1` `pl` | 12,410 | 5,126 | `5,126 / 5,126` (100%) | **28** | 1,967 | 66 |
| `w1` `endpoint` | 2,143 | 1,736 | `1,736 / 1,736` (100%) | **14** | 275 | 9 |
| `w2` `othername` | 3,247 | 2,019 | `2,019 / 2,019` (100%) | **81** | 802 | 0 |
| `w2` `pl` | 10,227 | 5,000 | `5,000 / 5,000` (100%) | **27** | 1,763 | 39 |
| `w2` `endpoint` | 2,239 | 1,874 | `1,874 / 1,874` (100%) | **9** | 252 | 13 |

**Verified V2 sidecar rules:**
1. **`100% in_week_main` invariant (`0` orphan sidecar NPIs across all 9 files):** Weekly sidecars emit rows exclusively for NPIs present in that week's `npidata` main CSV. Absence of an NPI from a weekly sidecar file NEVER implies deletion if that NPI is absent from that week's `npidata` main CSV.
2. **Whole-set per-NPI replacement & removal when NPI appears in `npidata` (witnessed across `888` NPIs appearing in both `w1` and `w2` main):**
   - *Added/replaced:* `NPI 1013958446` (`othername`): `w1` 2 rows (`03/05/2019`, `06/01/2017`) -> `w2` 3 rows (`06/01/2017`, `11/20/2026`, `One Sky Health Billings Atrium Pharmacy 11/20/2026`, replacing `03/05/2019`).
   - *Subset removed:* `NPI 1144259052` (`pl`): `w1` 18 secondary locations -> `w2` 16 secondary locations.
   - *In-place field update:* `NPI 1023641727` (`pl`): 1 row in `w1` (fax `2487247489`) -> 1 row in `w2` (fax `2482096800`); `NPI 1366698789` (`endpoint`): `mhankina@piim.org` -> `mhankins@piim.org`.
   - *Full removal (0 rows in `w2` sidecar despite presence in `w2` main):* `NPI 1013616630` (2 `pl` rows in `w1`, dropped Kentucky license/taxonomy in `w2`, 0 `pl` rows in `w2`), `NPI 1063130482` (1 `pl` row in `w1` -> 0 in `w2`), `NPI 1366053589` (1 `endpoint` row `www.dudetherapy.ai` in `w1` -> 0 in `w2`).
3. **CRITICAL FINDING — Deactivated NPIs still emit historical sidecars:** CMS strips descriptive fields from `npidata` upon deactivation, but continues exporting historical rows for deactivated NPIs in `othername`, `pl`, and `endpoint`:
   - `w1` `NPI 1023642576`: deactivated `11/10/2026` in `npidata` (rec 7026), yet emits 2 rows in `othername` (`PNW CLINIC`, created `02/24/2020`).
   - `w1` `NPI 1043366925`: deactivated `11/12/2026` in `npidata` (rec 17669), yet emits 3 rows in `pl` and 4 rows in `endpoint`.
   - `w1` `NPI 1023765716`: deactivated `11/12/2026` in `npidata` (rec 17899), yet emits 1 row in `endpoint` (`lynette@rightchoicecc.com`).
   - `apply_v2_release()` / `v2_snapshot_sidecar_rows()` explicitly pairs every sidecar row with its NPI's current `main_lifecycle_state` (`active`, `reactivated`, or `deactivated`) so deactivated providers' sidecars never masquerade as active directory facts.
4. **Native duplicate rows:** Exact duplicate sidecar rows occur natively in `pl` (39–66/week) and `endpoint` (9–18/week); `nppes_provider_observations` preserves every literal occurrence keyed by `(source_sha256, source_member, record_number)`.

---

## 5. Local Canary Generation and MCP DuckDB Readback Receipt

A 15-NPI live canary cohort spanning all observed V2 transitions (active Type 1/Type 2, `w1->w2` deactivations, `w1->w2` combined-date reactivations, sidecar additions/updates/removals, and deactivated-NPI sidecar emissions) was built from the live downloaded `110926_111526` and `111626_112226` V2 ZIP archives through `spicy_regs.pipelines.nppes_providers.build` and verified through `local_publication._candidate`, `explorer_metadata.build_bundle`, and real MCP `describe_table` / `query_sql`:

- **Logical ID:** `urn:spicy:artifact:spicy-regs-rollup-generation:5cf6c147d62556ca8fe178d6f9a2ea764531897e1a17b3f1a886d89f3f15fba7`
- **Candidate Artifact Digest:** `sha256:e92e57c29b5cb600063db8d07b71665e3e6db0c43262809eb9e4c9661755ae23`
- **Member `nppes_provider_observations.parquet`:** `92` rows (`26` main across 15 NPIs, `41` practice_location, `13` othername, `12` endpoint), `16,373` bytes, `sha256:4a6f92d13e0ffc955deeb3a01ba3c46cc8b657edde1340f12259b828028bdf4d`
- **Explorer Metadata:** `metadataStatus = "documented"`, `sourceStatus = "documented"`, `sources[0].id = "cms-nppes"`, `recordIdentity.columns = ["source_sha256", "source_member", "record_number"]`
- **MCP `describe_table("nppes_provider_observations")`:** 17 columns matching declared Arrow `OBSERVATION_SCHEMA` (`schema_matches_declared = true`).
- **MCP `query_sql` readback:** Verified `1023950664` (`w1` deactivated `11/09/2026` -> `w2` reactivated `11/09/2026` + `11/17/2026`, `HUDA/NOOR-UL`), `1205337821` (`w1` deactivated `11/10/2026` -> `w2` reactivated `11/10/2026` + `11/17/2026`, `CHIROS ON 54 LLC`), `1174491484` (`w1` active `SIEGEL/DANIELLE` -> `w2` deactivated `11/16/2026`), and `LEFT JOIN` keeping `CCN` (`1205337821`, `010001`) strictly distinct from `NPI` while preserving unmatched requests (`1234567893`).

---

## 6. Ready-to-Run Live Cutover Steps (Not Executed Here)

Once parent blind review and coordinator sign-off approve live cutover and >= 40 GiB free volume headroom is confirmed on the shared raw archive volume:

```sh
# 1. Unset scratch raw root so originals enter the shared raw archive.
unset SPICYGOV_RAW_ROOT

# 2. Build the approved nppes-providers generation from pinned CMS V2 ZIP(s)
#    using spicy_regs.pipelines.nppes_providers.build(...) into $WORK/nppes-build.

# 3. Dry-run publication guards against the live publication index.
uv run --frozen publish-local-generation \
  --generation "$WORK/nppes-build/generation" \
  --evidence "$WORK/nppes-build/source-evidence/<run-id>/artifact" \
  --family nppes-providers \
  --dry-run \
  --output "$WORK/nppes-dry-run"

# 4. Live R2 publication (first publication of nppes-providers has no prior;
#    subsequent refreshes pass --expect-prior "$PRIOR_DIGEST").
uv run --frozen publish-local-generation \
  --generation "$WORK/nppes-build/generation" \
  --evidence "$WORK/nppes-build/source-evidence/<run-id>/artifact" \
  --family nppes-providers \
  --output "$WORK/nppes-publish-receipt"

# 5. Refresh explorer metadata and confirm live MCP describe_table / query_sql readback
#    against https://mcp.spicygov.ai (and update SpicyGov coverage-reviews/regulation.json
#    publication block with the live artifactDigest / publishedAt).
```
