# SAM.gov Contract Opportunities — Non-ASCII Encoding Audit & Owner Disposition (2026-11-27)

**Ticket:** `ss-5m6o` (Wave 3 Lane 3B-1)  
**Retained public extract:** `ContractOpportunitiesFullCSV.csv`  
**Source URL:** `https://sam.gov/api/prod/fileextractservices/v1/api/download/Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv?privacy=Public`  
**SHA-256:** `sha256:4ae861ce68327b18e8a2847487ae30e08d22408f82aec5f43acf170f81998150`  
**Byte size:** `222,056,693` bytes (captured `2026-11-13T11:49:48.599385+00:00`)  
**Population:** `72,318` physical CSV rows, `72,318` distinct `NoticeId` values, `PostedDate` range `2008-03-05` through `2026-11-12`, all `Active = Yes`.

---

## Executive Summary & Recommended Owner Disposition

**Recommended Disposition: Option A — Publish the full public-extract cohort (`{"population":"all-rows-in-public-extract"}`, 72,318 rows) under reversible `text_encoding = 'latin-1'` with explicit diagnostic columns (`text_interpretation_status`, `unqualified_text_fields_json`) preserved, and API-only attachments/version history explicitly omitted pending `ss-vf7b`.**

Byte-level inspection of both the original 118-row GSA pilot (`CGAC = 047`, `2026-11-01`..`2026-11-07`, containing the 47 non-ASCII `Description` records cited in `ss-5m6o`) and the full 72,318-row public extract proves three load-bearing facts:

1. **The public CSV is genuinely mixed-encoding — blind CP1252 or UTF-8 decoding corrupts or rejects valid notices.**
   - Strict UTF-8 (`utf-8-sig`) refuses the file at byte offset `3,923` (`0x92` in the first notice's `Description`).
   - Across the full 72,318-row extract, **11,887 rows (16.44%)** contain non-ASCII bytes across 8 free-text fields, including **236 field occurrences across 232 rows (229 rows where all non-ASCII fields decode as valid UTF-8, plus 3 rows combining a valid UTF-8 non-ASCII field with a non-UTF-8 C1 field)** (e.g., `38 U.S.C. \xc2\xa7\xc2\xa7` for `§§`, `EON\xc2\xae` for `EON®`, `\xc2\xbd` for `½`, `d\xc3\xa9cor` for `décor`) alongside **11,208 rows containing Windows-1252 C1 bytes (`0x80`–`0x9F`)** (`\x92`, `\x93`, `\x94`, `\x96`, `\x97`, `\x95`, `\x85`). Blindly decoding the file as `cp1252` would silently corrupt UTF-8 rows into mojibake (`Â§Â§`, `EONÂ®`, `dÃ©cor`).
2. **All structured discovery, agency, classification, set-aside, deadline, and URL fields are 100% ASCII-exact across all 72,318 rows.**
   - Zero non-ASCII bytes appear in `NoticeId`, `Sol#`, `CGAC`, `Department/Ind.Agency`, `Sub-Tier`, `AAC Code`, `PostedDate`, `Type`, `BaseType`, `ResponseDeadLine`, `SetASideCode`, `SetASide`, `NaicsCode`, `ClassificationCode`, `Active`, `ArchiveDate`, `Link`, or `AdditionalInfoLink`.
3. **Current `spicy-docs` and `spicy-regs` staging preserves raw bytes losslessly without silent repair.**
   - `spicy_docs.sources.sam_public.csv_records(..., encoding="utf-8-sig")` retains strict UTF-8 refusal by default.
   - Explicit `--encoding latin-1` maps every raw byte `0x00`–`0xFF` 1:1 to Unicode code points `U+0000`–`U+00FF` (`cell.encode("latin-1") == original_bytes`), stamps `text_encoding = 'latin-1'` into row and `snapshot_id` identity, marks strictly ASCII rows as `text_interpretation_status = 'decoded-exact'` (`unqualified_text_fields_json = '[]'`), and marks any row with non-ASCII bytes as `text_interpretation_status = 'byte-preserving-unqualified'` with the exact native CSV column names listed in `unqualified_text_fields_json`.

---

## Audit 1: The 47 Non-ASCII `Description` Records (GSA `CGAC 047`, `2026-11-01`..`2026-11-07`)

Within the 2026-11-13 bounded GSA pilot (`118` selected rows, `118` distinct `NoticeId`s):

| Metric | Count | Share |
|---|---:|---:|
| Selected GSA rows (`CGAC = 047`, `2026-11-01`..`2026-11-07`) | 118 | 100.00% |
| Strictly ASCII-exact (`decoded-exact`, `unqualified_text_fields_json = '[]'`) | 71 | 60.17% |
| Non-ASCII (`byte-preserving-unqualified`) | **47** | **39.83%** |
| Affected native column | `Description` only (47 / 47) | 100.00% |
| Valid strict UTF-8 among the 47 descriptions | 0 | 0.00% |
| Valid Windows-1252 (`cp1252`) among the 47 descriptions | 47 | 100.00% |
| Undefined Windows-1252 bytes (`0x81`, `0x8D`, `0x8F`, `0x90`, `0x9D`) | 0 | 0.00% |
| Non-ASCII bytes in any other column (`Title`, `Sol#`, `Office`, dates, codes, URLs) | 0 | 0.00% |

### Byte Distribution Across the 47 Records (229 Non-ASCII Bytes Total)

| Raw Byte | Occurrences | Windows-1252 Character | Observed Context in Retained CSV |
|---|---:|---|---|
| `0x92` | 86 | `’` (right single quote / apostrophe) | `vendor\x92s`, `GSA\x92s`, `project\x92s` |
| `0x94` | 43 | `”` (right double quote) | `\x93100-year\x94 floodplain` |
| `0x93` | 40 | `“` (left double quote) | `\x93100-year\x94 floodplain` |
| `0x96` | 34 | `–` (en-dash) | `January 1, 2027 \x96 April 30, 2030` |
| `0xb0` | 10 | `°` (degree sign) | `360\xb0 views` |
| `0x95` | 7 | `•` (bullet) | `Sallyport: \x95 Fencing for sallyports` |
| `0x97` | 5 | `—` (em-dash) | `products and services \x97 including` |
| `0xe7` | 3 | `ç` (Latin small letter c with cedilla) | `Exterior fa\xe7ade`, `historic fa\xe7ade` |
| `0xbc` | 1 | `¼` (vulgar fraction one quarter) | `walkable \xbc mile to a public transit stop` |

### Representative Sample Records (47-Row GSA Cohort)

| `native_id` | Physical Line | `Sol#` | Non-ASCII Bytes | Raw Escaped Byte Context | Visual CP1252 Reading (Unqualified) |
|---|---:|---|---|---|---|
| `584d9822a413468ba6f859a7452c6710` | 4905 | `1106202612072026` | `0x92`, `0x97` | `products and services \x97 including Rivest-Shamir-Adleman` | `products and services — including Rivest-Shamir-Adleman` |
| `a9ca6d1377064e46ba51bb5d9cac5e96` | 4984 | `2WA0663A` | `0x92`, `0x93`, `0x94` | `referred to as \x93100-year\x94 floodplain` | `referred to as “100-year” floodplain` |
| `cb98e41179054e28871423f01c9fb432` | 5116 | `6OR0247` | `0x92`, `0x93`, `0x94`, `0x96`, `0x97` | `Area 1 \x96 District Office 2 offices` | `Area 1 – District Office 2 offices` |
| `b186c45b95294ce7a599f20bb542fff2` | 6566 | `47PB5126R0038` | `0x92`, `0xe7` | `Exterior fa\xe7ade and building envelope` | `Exterior façade and building envelope` |
| `f3bf9a375c6841d8acbbcc2ba057a5c6` | 6570 | `47QMCA27N0004` | `0x92`, `0x93`, `0x94`, `0x96`, `0xb0` | `360\xb0 views, etc.` | `360° views, etc.` |
| `839fd925fdea49619b7724fea8b2b128` | 7985 | `0FL2354` | `0x92`, `0x93`, `0x94`, `0x95`, `0x96` | `Sallyport: \x95 Fencing for sallyports` | `Sallyport: • Fencing for sallyports` |
| `0117fdac98f74cccb8a300bcf5522846` | 8332 | `8TN2168` | `0x92`, `0x93`, `0x94`, `0xbc` | `walkable \xbc mile to a public transit stop` | `walkable ¼ mile to a public transit stop` |

<details>
<summary>Complete 47-record inventory (reconciles 1:1 with 2026-11-13 <code>consumer-evidence.json</code>)</summary>

| `native_id` | Physical Line | `Sol#` | `Type` | `ResponseDeadLine` | Non-ASCII Count | Distinct Bytes |
|---|---:|---|---|---|---:|---|
| `009f044a424f4794a3a708862103334c` | 6586 | `4CT0116` | Presolicitation | `2026-11-20T17:00:00-05:00` | 4 | `0x92`, `0x93`, `0x94` |
| `0117fdac98f74cccb8a300bcf5522846` | 8332 | `8TN2168` | Solicitation | `2026-11-30T17:00:00-05:00` | 10 | `0x92`, `0x93`, `0x94`, `0xbc` |
| `08eada21239c4518b8b9a60ebb4de82a` | 8254 | `6FL0613` | Solicitation | `2026-11-25T17:00:00-05:00` | 2 | `0x93`, `0x94` |
| `0da59422b8a34315a09a13cda746c44b` | 8256 | `6Ok0149` | Justification | `(blank)` | 10 | `0x92` |
| `1179001c77624b5496eea526a99a43a4` | 8236 | `5PA0528` | Presolicitation | `2026-11-16T23:59:00-05:00` | 3 | `0x92`, `0x93`, `0x94` |
| `1ac576e53b8141c0a248dc8df20eda37` | 6585 | `4CT0116` | Presolicitation | `2026-11-20T17:00:00-05:00` | 4 | `0x92`, `0x93`, `0x94` |
| `1c20c94b36e64f108a3e88a961087272` | 9616 | `47PB5126R0030` | Solicitation | `2026-12-15T15:00:00-04:00` | 1 | `0x92` |
| `2124a5be0a1c4e65be37be564320a008` | 6607 | `6MN0187` | Award Notice | `(blank)` | 1 | `0x92` |
| `21e6d14bdc064ce3b6ab2b99c129ff3d` | 10674 | `4MS0260` | Justification | `(blank)` | 3 | `0x92`, `0x96` |
| `345c594abb5844e3be9fbd415549d84d` | 8234 | `5PA0440` | Presolicitation | `2026-11-16T23:59:00-05:00` | 3 | `0x92`, `0x93`, `0x94` |
| `3675b7df1c8d44caab7ca525a7a59915` | 10806 | `FY27_NationalFineArtsIDIQ_TLC_11022026` | Sources Sought | `2026-11-30T17:00:00-05:00` | 2 | `0x92`, `0x96` |
| `3748da05a9f94d64a1616bff981a2087` | 10663 | `3MI0435` | Presolicitation | `2026-11-27T16:00:00-06:00` | 2 | `0x93`, `0x94` |
| `4bbf50953df2450e9d150a0ae572b765` | 9662 | `6FL0714` | Presolicitation | `2026-11-23T17:00:00-05:00` | 5 | `0x92`, `0x93`, `0x94` |
| `5629a5f4a5a0441fac26299da78c5d01` | 9622 | `47QMCA27N0004` | Sources Sought | `2026-11-24T14:00:00-06:00` | 12 | `0x92`, `0x93`, `0x94`, `0x96`, `0xb0` |
| `584d9822a413468ba6f859a7452c6710` | 4905 | `1106202612072026` | Sources Sought | `2026-12-07T23:59:00-05:00` | 5 | `0x92`, `0x97` |
| `59c4077b256a4523ba2d6708a1980bc1` | 8226 | `47QMCA27N0005` | Sources Sought | `2026-11-25T14:00:00-06:00` | 12 | `0x92`, `0x93`, `0x94`, `0x96`, `0xb0` |
| `650c7ab0f8cb4037939f8c96c30f36be` | 8223 | `47PB5126R0009` | Presolicitation | `2027-03-01T12:00:00-05:00` | 2 | `0x92`, `0xe7` |
| `65531a07ed09462a86ca08bc7f44d5c2` | 5415 | `RFQ_47QSSC-27-R-0007` | Combined Synopsis/Solicitation | `2026-11-12T15:30:00-06:00` | 1 | `0x92` |
| `7b284c343ab34380a995cc35d4126104` | 8225 | `47QMCA27N0004` | Sources Sought | `2026-11-24T14:00:00-06:00` | 12 | `0x92`, `0x93`, `0x94`, `0x96`, `0xb0` |
| `7d61264babe744f09a7c1ef510be2c48` | 9617 | `47PB5227R0003` | Presolicitation | `(blank)` | 4 | `0x92`, `0x96`, `0x97` |
| `7f3f06310b6a46bdab28982d6964dc67` | 10673 | `47QSSC-27-Q-0037` | Combined Synopsis/Solicitation | `2026-11-06T16:00:00-05:00` | 1 | `0x92` |
| `826f3d18a8504e6ab0a4b53f03d0ec02` | 9635 | `4IA0158` | Presolicitation | `2026-11-17T17:00:00-08:00` | 2 | `0x92` |
| `839fd925fdea49619b7724fea8b2b128` | 7985 | `0FL2354` | Presolicitation | `2026-11-30T17:00:00-05:00` | 25 | `0x92`, `0x93`, `0x94`, `0x95`, `0x96` |
| `9823f8bcd37b40f6b5abfd1041d806ee` | 8255 | `6OH0326` | Combined Synopsis/Solicitation | `2026-11-09T20:00:00-05:00` | 1 | `0x92` |
| `9adc6bb35996488da3f957b948d74b3a` | 9634 | `4CO0563` | Presolicitation | `2026-11-17T17:00:00-07:00` | 4 | `0x92`, `0x93`, `0x94` |
| `9ba7da1947bf4567bff898fbafefcd25` | 6593 | `5NJ0279` | Presolicitation | `2026-11-16T23:59:00-05:00` | 3 | `0x92`, `0x93`, `0x94` |
| `a4ca821173344d1ebdf7b47031c09260` | 8224 | `47PB5126R0018` | Solicitation | `2026-12-04T16:00:00-05:00` | 2 | `0x92`, `0x96` |
| `a71039782f8844faa24986da016435a7` | 10664 | `3NM0182` | Award Notice | `(blank)` | 4 | `0x96` |
| `a799b7fb719947448b5f1d8b9c9e9e40` | 8235 | `5PA0501` | Presolicitation | `2026-11-16T23:59:00-05:00` | 3 | `0x92`, `0x93`, `0x94` |
| `a9ca6d1377064e46ba51bb5d9cac5e96` | 4984 | `2WA0663A` | Combined Synopsis/Solicitation | `2026-12-04T17:00:00-08:00` | 5 | `0x92`, `0x93`, `0x94` |
| `b0c5a2a9ae15430ca65c4ac689a23a1b` | 6606 | `6MN0187` | Award Notice | `(blank)` | 1 | `0x92` |
| `b186c45b95294ce7a599f20bb542fff2` | 6566 | `47PB5126R0038` | Solicitation | `2026-11-23T16:00:00-05:00` | 4 | `0x92`, `0xe7` |
| `b2629ebae643498c9494f78e0ce2d0ed` | 10741 | `7NY2703_` | Presolicitation | `2026-11-12T17:00:00-05:00` | 4 | `0x92`, `0x93`, `0x94` |
| `b7941b2414cb4a3f9cc16733800845dc` | 9518 | `2LA0294` | Award Notice | `(blank)` | 4 | `0x96` |
| `b9e85521a2454072951b7213cb0ef349` | 9636 | `4NJ0303` | Presolicitation | `2026-11-09T23:59:00-05:00` | 3 | `0x92`, `0x93`, `0x94` |
| `cb98e41179054e28871423f01c9fb432` | 5116 | `6OR0247` | Combined Synopsis/Solicitation | `2026-11-09T20:00:00-05:00` | 11 | `0x92`, `0x93`, `0x94`, `0x96`, `0x97` |
| `d3a53318b6c84246bfd4107dfcd32e69` | 9638 | `5MD0499` | Presolicitation | `2026-11-24T17:00:00-05:00` | 5 | `0x92`, `0x93`, `0x94` |
| `d3c0baec97594639b96397b25fbe8497` | 10805 | `FY27_NationalFineArtsIDIQ_TLC_11022026` | Sources Sought | `2026-11-30T17:00:00-05:00` | 2 | `0x92`, `0x96` |
| `e0e5bf8c8137458584a3f250c7b1362f` | 9663 | `6FL0714` | Presolicitation | `2026-11-23T17:00:00-05:00` | 6 | `0x92`, `0x93`, `0x94` |
| `e376c97405254883aa274a70bb1c88de` | 9615 | `47PB5126R0030` | Solicitation | `2026-12-15T15:00:00-04:00` | 1 | `0x92` |
| `ea42676a12d349d6b75786a13a31878e` | 6324 | `10326` | Sources Sought | `2026-11-16T14:00:00-05:00` | 2 | `0x92` |
| `ea99411dd4a44f788526ea1ab45fe9fa` | 9637 | `4NJ0304` | Presolicitation | `2026-11-10T23:59:00-05:00` | 3 | `0x92`, `0x93`, `0x94` |
| `eeb3dc9d932a4f4f866cbf3cd0f2bd8b` | 6571 | `47QMCA27N0005` | Sources Sought | `2026-11-25T14:00:00-06:00` | 12 | `0x92`, `0x93`, `0x94`, `0x96`, `0xb0` |
| `f3bf9a375c6841d8acbbcc2ba057a5c6` | 6570 | `47QMCA27N0004` | Sources Sought | `2026-11-24T14:00:00-06:00` | 12 | `0x92`, `0x93`, `0x94`, `0x96`, `0xb0` |
| `f6944ca3bed5434685f7a86009f082b5` | 10677 | `4VA0772` | Presolicitation | `2026-11-13T17:00:00-05:00` | 4 | `0x92`, `0x93`, `0x94` |
| `f86a8027f2334f5288d80e8674bd77d9` | 6992 | `RFQ_47QSSC-27-R-0006` | Combined Synopsis/Solicitation | `2026-11-12T15:30:00-06:00` | 1 | `0x92` |
| `fc0b293ae9ae4134bc6e2770211f7e4b` | 8222 | `3MS0155` | Presolicitation | `2026-11-09T17:00:00-05:00` | 6 | `0x92`, `0x93`, `0x94` |

</details>

---

## Audit 2: Full Public Extract (`72,318` Rows) — Proof of Mixed Encoding

Inspecting all `72,318` rows of the same retained CSV establishes why heuristic `cp1252` repair must **not** be applied globally:

| Metric | Count | Share |
|---|---:|---:|
| Total physical rows (`distinct NoticeId = 72,318`) | 72,318 | 100.00% |
| Strictly ASCII-exact (`decoded-exact`) | 60,431 | 83.56% |
| Non-ASCII (`byte-preserving-unqualified`) | 11,887 | 16.44% |
| Undefined Windows-1252 bytes (`0x81`, `0x8D`, `0x8F`, `0x90`, `0x9D`) | 0 | 0.00% |
| Non-ASCII field occurrences across all columns | 12,388 | — |
| Non-ASCII field occurrences that are valid multi-byte UTF-8 | 236 field occurrences across 232 rows (229 rows where all non-ASCII fields decode as valid UTF-8; 3 rows also have a separate non-UTF-8 C1 field) | 1.91% of non-ASCII fields |
| Rows containing at least one Windows-1252 C1 byte (`0x80`–`0x9F`) | 11,208 | 94.29% of non-ASCII rows |
| Non-UTF-8 rows containing both a C1 byte (`0x80`–`0x9F`) and a UTF-8 lead pattern (`0xC2`, `0xC3`, or `0xE2 0x80`) | 79 | 0.66% of non-ASCII rows |

### Affected Native CSV Columns (Full 72,318-Row Extract)

| Native Column | Projected Column | Rows with Non-ASCII Bytes |
|---|---|---:|
| `Description` | `description_raw` | 11,702 |
| `Title` | `title_raw` | 562 |
| `Office` | `office_raw` | 44 |
| `SecondaryContactFullname` | `raw_fields_json` only | 37 |
| `PrimaryContactFullname` | `raw_fields_json` only | 23 |
| `PopStreetAddress` | `raw_fields_json` only | 16 |
| `Awardee` | `raw_fields_json` only | 2 |
| `PopCity` | `raw_fields_json` only | 2 |

All structured discovery and deadline columns (`NoticeId`, `Sol#`, `CGAC`, `Department/Ind.Agency`, `Sub-Tier`, `AAC Code`, `PostedDate`, `Type`, `BaseType`, `ResponseDeadLine`, `SetASideCode`, `SetASide`, `NaicsCode`, `ClassificationCode`, `Active`, `ArchiveDate`, `Link`, `AdditionalInfoLink`) have **0 non-ASCII rows** across all 72,318 observations.

### Mixed-Encoding Counterexamples (Why Blind CP1252 Silently Corrupts Rows)

| `native_id` | Line | Raw Escaped Bytes | True UTF-8 Reading | Corrupted CP1252 Mojibake |
|---|---:|---|---|---|
| `a657b79c96d2491a835c7d6118abc87f` | 150 | `38 U.S.C. \xc2\xa7\xc2\xa7 8127 8128` | `38 U.S.C. §§ 8127 8128` | `38 U.S.C. Â§Â§ 8127 8128` |
| `595a043c43e3421c9ce457df6614d29f` | 153 | `EON\xc2\xae Portable RO` | `EON® Portable RO` | `EONÂ® Portable RO` |
| `1772a7eaddbe41dcb477950d424ac80b` | 189 | `\xc2\xbd X 5 yd first aid tape` | `½ X 5 yd first aid tape` | `Â½ X 5 yd first aid tape` |
| `2a73fb61222641248bfb98254939e812` | 2336 | `holiday d\xc3\xa9cor, equipment` | `holiday décor, equipment` | `holiday dÃ©cor, equipment` |

---

## Verified Behavior in `spicy-docs` and `spicy-regs`

- `spicy_docs.sources.sam_public.csv_records(stream, kind="opportunities", encoding="utf-8-sig")` defaults to strict UTF-8 and raises `SamPublicError` on non-UTF-8 bytes; raw refusals are never coerced into empty tables.
- `spicy_regs.transforms.build_sam_sources.build_sam_source(..., text_encoding="latin-1")`:
  - Preserves every byte 1:1 (`cell.encode("latin-1") == original_bytes`) in both projected `*_raw` columns and `raw_fields_json`.
  - Records `text_encoding = 'latin-1'` on every observation row, in `sam_opportunity_snapshots.text_encoding`, and inside the deterministic `snapshot_id` digest.
  - Marks strictly ASCII rows `text_interpretation_status = 'decoded-exact'` and `unqualified_text_fields_json = '[]'`.
  - Marks any row with non-ASCII bytes `text_interpretation_status = 'byte-preserving-unqualified'` and records the exact native CSV field name(s) in `unqualified_text_fields_json` (e.g., `["Description"]`).
  - Records `unqualified_text_rows` in `sam_opportunity_snapshots`.

---

## Owner Disposition Options

### Option A (Recommended): Publish Full Public-Extract Cohort with Explicit Diagnostic Markers

- **Selection:** `{"population":"all-rows-in-public-extract"}` (`72,318` rows across all federal agencies, including `1,070` rows with blank `CGAC`; or owner may narrow to a declared agency/date window if preferred).
- **Encoding policy:** Stage with explicit `--encoding latin-1`, preserving `text_encoding`, `text_interpretation_status`, and `unqualified_text_fields_json` without guessed CP1252 or UTF-8 rewrites.
- **Consumer contract:**
  - All `72,318` rows have 100% ASCII-exact identifiers, solicitation numbers, agencies, NAICS/PSC codes, set-aside codes, posted dates, response deadlines, and SAM.gov links.
  - Consumers needing strictly qualified display text filter `WHERE text_interpretation_status = 'decoded-exact'` (`60,431` rows in the full extract; `71` of `118` in the GSA pilot).
  - Exact source bytes remain recoverable via `.encode('latin-1')` on any `byte-preserving-unqualified` cell.
- **Explicit API boundary (`ss-vf7b`):** API-only notice attachments and full historical notice versions are explicitly omitted from this public-CSV generation until SAM API credentials are renewed.

### Option B (Not Recommended): Quarantine Non-ASCII Rows

- Drop the `47` non-ASCII `Description` rows from the GSA pilot (or `11,887` rows from the full extract) before publication.
- **Why rejected as default:** Silently drops **39.83%** of the GSA pilot cohort (including active solicitations with upcoming deadlines such as `f6944ca3bed5434685f7a86009f082b5` and `ea42676a12d349d6b75786a13a31878e`) and **16.44%** of all active federal contract opportunities, even though 100% of structured discovery, deadline, set-aside, and SAM.gov URL fields on those rows are ASCII-exact and the diagnostic columns already prevent silent misinterpretation.
