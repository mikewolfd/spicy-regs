# Federal Register Agency Registry — Batch-2 Candidate Sheet (`ss-szdp`)

> **Status:** `candidate-pending-owner-adjudication`. **No identity (`sameEntityAs`) or succession assertion is emitted here.** Every item below requires an explicit decision by the RefSpec owner (`urn:ref:reviewer:refspec-owner`) before entering a RefSpec release.

## 1. Baseline & Coverage Summary

- **RefSpec baseline:** Agency Registry View `1.4` (`agency-registry-2026-09-26`, manifest `sha256:c89935debe320e35aaf748ea0519457c1618b49e13d79ddb33cbc30051b5a118`).
- **Measured live table:** `federal_register.parquet` (`generations/federal-register/65f6683e52de50a53fddc902e2346c7555b1fa71c7692e9f66a5f8e0ec023f3b/federal_register.parquet`, `1,013,492` rows, `sha256:1e438d61d04e747e773075ea59ee50a5e64f72c9600eb348b1940cdecf3dff87`).
- **Total FR agency mentions:** `1,593,417` (`1,562,770` with numeric FR ID across `449` distinct IDs; `30,647` ID-less mentions tracked separately in §4).
- **Already covered by View 1.4** (REF-038 projection + batch-1 bridges + succession events + recorded non-emissions): `1,558,260` of `1,562,770` ID-bearing mentions (**99.711%**).
- **Remaining unbridged FR IDs:** `131` IDs carrying `4,510` mentions (**0.289%** of ID-bearing mentions; **0.283%** of all mentions).
- **Bounded batch-2 coverage:**
  - Top 20 unbridged FR IDs: `3,377` / `4,510` (**74.88%**)
  - Top 25 unbridged FR IDs: `3,601` / `4,510` (**79.84%**)
  - Top 30 unbridged FR IDs (this sheet): `3,784` / `4,510` (**83.9%**)
- **Reverse-lookup safety:** `0` of the held-roster counterparts below carry a Regulations.gov code in REF-038, so none of these candidates introduces a Regulations.gov reverse-lookup ambiguity collision.
- **Stratification of top 30:** `11` cross-roster `sameEntityAs` bridge candidates (Group A), `7` defunct agencies already flagged for RefSpec's `later_succession_batch` (Group B), and `12` FR-only agencies with no exact counterpart in RefSpec's held eCFR/FH/OPM rosters (Group C).

## 2. Top-30 Candidate Overview Table

| Rank | Candidate ID | FR Agency | Mentions (Cum %) | Date Span | Parent (FR) | Category | Held-Roster Counterparts |
| ---: | --- | --- | ---: | --- | --- | --- | --- |
| 1 | `batch2:fr588` | FR 588: National Endowment for the Arts | 480 (10.64%) | 1994–2026 | FR 342 (National Foundation on the Arts and the Humanities) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:national-endowment-for-the-arts` (exactSlug); `urn:ref:federal-hierarchy-org:100241110` (nameKey); `urn:ref:federal-hierarchy-org:300000252` (nameKey); `urn:ref:opm-ehri-agency-subelement:AH01` (nameKey) |
| 2 | `batch2:fr223` | FR 223: Hearings and Appeals Office, Interior Department | 307 (17.45%) | 1994–2017 | FR 253 (Interior Department) | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 3 | `batch2:fr196` | FR 196: Bureau of the Fiscal Service | 306 (24.24%) | 2001–2026 | FR 497 (Treasury Department) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:bureau-of-the-fiscal-service` (exactSlug); `urn:ref:federal-hierarchy-org:100076578` (nameKey); `urn:ref:opm-ehri-agency-subelement:TRFD` (nameKey) |
| 4 | `batch2:fr591` | FR 591: Institute of Museum and Library Services | 302 (30.93%) | 1997–2026 | FR 342 (National Foundation on the Arts and the Humanities) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:institute-of-museum-and-library-services` (exactSlug); `urn:ref:federal-hierarchy-org:300000036` (nameKey); `urn:ref:federal-hierarchy-org:300000248` (nameKey); `urn:ref:opm-ehri-agency-subelement:AH03` (nameKey) |
| 5 | `batch2:fr495` | FR 495: Transportation Statistics Bureau | 243 (36.32%) | 1995–2026 | FR 492 (Transportation Department) | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 6 | `batch2:fr589` | FR 589: National Endowment for the Humanities | 195 (40.64%) | 1994–2026 | FR 342 (National Foundation on the Arts and the Humanities) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:national-endowment-for-the-humanities` (exactSlug); `urn:ref:federal-hierarchy-org:100171765` (nameKey); `urn:ref:federal-hierarchy-org:300000253` (nameKey); `urn:ref:opm-ehri-agency-subelement:AH02` (nameKey) |
| 7 | `batch2:fr609` | FR 609: Great Lakes St. Lawrence Seaway Development Corporation | 176 (44.55%) | 1994–2026 | FR 492 (Transportation Department) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:great-lakes-st-lawrence-seaway-development-corporation` (exactSlug); `urn:ref:federal-hierarchy-org:100159874` (nameKey) |
| 8 | `batch2:fr601` | FR 601: Unified Carrier Registration Plan | 166 (48.23%) | 2019–2026 | *(top-level)* | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 9 | `batch2:fr198` | FR 198: Food and Consumer Service | 160 (51.77%) | 1994–1998 | FR 12 (Agriculture Department) | `defunct-succession-candidate` | *(defer to succession: FR 200)* |
| 10 | `batch2:fr258` | FR 258: International Development Cooperation Agency | 148 (55.06%) | 1994–1999 | FR 6 (Agency for International Development) | `defunct-succession-candidate` | `urn:ref:ecfr-agency:united-states-international-development-cooperation-agency` (nameKey) |
| 11 | `batch2:fr351` | FR 351: National Institute of Justice | 127 (57.87%) | 1994–2025 | FR 268 (Justice Department) | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 12 | `batch2:fr140` | FR 140: Energy Research Office | 116 (60.44%) | 1994–1999 | FR 136 (Energy Department) | `defunct-succession-candidate` | *(defer to succession: none in FR roster)* |
| 13 | `batch2:fr33` | FR 33: Assassination Records Review Board | 108 (62.84%) | 1995–1998 | *(top-level)* | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 14 | `batch2:fr597` | FR 597: U.S. Codex Office | 96 (64.97%) | 2018–2026 | FR 12 (Agriculture Department) | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 15 | `batch2:fr372` | FR 372: National Technical Information Service | 94 (67.05%) | 1994–2026 | FR 54 (Commerce Department) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:national-technical-information-service` (exactSlug); `urn:ref:federal-hierarchy-org:100196017` (nameKey); `urn:ref:opm-ehri-agency-subelement:CM62` (nameKey) |
| 16 | `batch2:fr568` | FR 568: Ocean Energy Management, Regulation, and Enforcement Bureau | 94 (69.14%) | 2010–2011 | FR 253 (Interior Department) | `defunct-succession-candidate` | `urn:ref:opm-ehri-agency-subelement:IN23` (wordOrderKey) |
| 17 | `batch2:fr380` | FR 380: Northeast Dairy Compact Commission | 84 (71.0%) | 1996–2002 | *(top-level)* | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 18 | `batch2:fr448` | FR 448: Refugee Resettlement Office | 73 (72.62%) | 1994–2026 | FR 221 (Health and Human Services Department) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:refugee-resettlement-office` (exactSlug) |
| 19 | `batch2:fr598` | FR 598: Federal Council on the Arts and the Humanities | 52 (73.77%) | 1995–2026 | FR 342 (National Foundation on the Arts and the Humanities) | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 20 | `batch2:fr600` | FR 600: National Assessment Governing Board | 50 (74.88%) | 1998–2026 | FR 126 (Education Department) | `same-entity-bridge-candidate` | `urn:ref:federal-hierarchy-org:100525883` (nameKey); `urn:ref:federal-hierarchy-org:100525304` (nameKey); `urn:ref:opm-ehri-agency-subelement:EDEZ` (nameKey) |
| 21 | `batch2:fr143` | FR 143: Engraving and Printing Bureau | 48 (75.94%) | 1994–2023 | FR 497 (Treasury Department) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:engraving-and-printing-bureau` (exactSlug); `urn:ref:federal-hierarchy-org:100126434` (wordOrderKey); `urn:ref:opm-ehri-agency-subelement:TRAI` (wordOrderKey) |
| 22 | `batch2:fr618` | FR 618: Civil Rights Cold Case Records Review Board | 47 (76.98%) | 2024–2026 | *(top-level)* | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 23 | `batch2:fr509` | FR 509: United States Enrichment Corporation | 44 (77.96%) | 1994–1998 | *(top-level)* | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 24 | `batch2:fr452` | FR 452: Resolution Trust Corporation | 43 (78.91%) | 1994–1995 | *(top-level)* | `defunct-succession-candidate` | *(defer to succession: FR 164)* |
| 25 | `batch2:fr31` | FR 31: Arms Control and Disarmament Agency | 42 (79.84%) | 1994–1999 | *(top-level)* | `defunct-succession-candidate` | *(defer to succession: FR 476)* |
| 26 | `batch2:fr400` | FR 400: Panama Canal Commission | 42 (80.78%) | 1994–2004 | *(top-level)* | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 27 | `batch2:fr290` | FR 290: Mines Bureau | 40 (81.66%) | 1994–2002 | FR 253 (Interior Department) | `defunct-succession-candidate` | *(defer to succession: FR 136)* |
| 28 | `batch2:fr592` | FR 592: Office of Government Information Services | 36 (82.46%) | 2016–2026 | FR 304 (National Archives and Records Administration) | `register-only-no-held-counterpart` | *(none in held eCFR/FH/OPM)* |
| 29 | `batch2:fr604` | FR 604: Partnerships and Public Engagement Office | 33 (83.19%) | 2019–2024 | FR 12 (Agriculture Department) | `same-entity-bridge-candidate` | `urn:ref:opm-ehri-agency-subelement:AGAO` (wordOrderKey) |
| 30 | `batch2:fr581` | FR 581: Advocacy and Outreach Office | 32 (83.9%) | 2011–2018 | FR 12 (Agriculture Department) | `same-entity-bridge-candidate` | `urn:ref:ecfr-agency:advocacy-and-outreach-office` (exactSlug) |

## 3. Primary-Source Evidence by Adjudication Group

### Group A — Cross-Roster `sameEntityAs` Bridge Candidates (11 FR IDs)

Each row names an active or historical FR agency with one or more exact-slug / `name_key` / `word_order_key` counterparts in RefSpec's pinned eCFR, Federal Hierarchy, or OPM rosters. Finding-rule hits are search evidence only; owner adjudication is required on publisher evidence (`proposed_basis`).

#### `batch2:fr588` — FR 588: National Endowment for the Arts (480 mentions, 1994-02-08 to 2026-11-23)
- **FR record:** `urn:ref:federal-register-agency:588` | slug: `national-endowment-for-the-arts` | short_name: `NEA` | parent: `urn:ref:federal-register-agency:342` (National Foundation on the Arts and the Humanities)
- **FR URL:** https://www.federalregister.gov/agencies/national-endowment-for-the-arts | **Agency URL:** http://arts.gov/
- **FR roster description:** Established by Congress in 1965, the NEA is the independent federal agency whose funding and support gives Americans the opportunity to participate in the arts, exercise their imaginations, and develop their creative capacities. Through partnerships with state arts agencies, local leaders, other federal agencies, and t…
- **Proposed basis:** `exactPublisherNameEquality` | **Content digest:** `sha256:e580a2bb53d6a89304fa26dac693d67c04b841120c28cdca1dcfdc55532785f9`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:national-endowment-for-the-arts` (`ecfr-agencies`, found by `exactSlug`): **National Endowment for the Arts**, parent `urn:ref:ecfr-agency:national-foundation-on-the-arts-and-the-humanities` (National Foundation on the Arts and the Humanities); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100241110` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL ENDOWMENT FOR THE ARTS**, fhorgtype=Department/Ind. Agency, top-level; REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:300000252` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL ENDOWMENT FOR THE ARTS**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100241110` (NATIONAL ENDOWMENT FOR THE ARTS); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:AH01` (`opm-ehri-agency-subelement`, found by `nameKey`): **NATIONAL ENDOWMENT FOR THE ARTS**, code=AH01 (2005-02-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 588 (National Endowment for the Arts) against 4 held-roster counterpart(s) (urn:ref:ecfr-agency:national-endowment-for-the-arts, urn:ref:federal-hierarchy-org:100241110, urn:ref:federal-hierarchy-org:300000252, urn:ref:opm-ehri-agency-subelement:AH01): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr196` — FR 196: Bureau of the Fiscal Service (306 mentions, 2001-09-28 to 2026-08-17)
- **FR record:** `urn:ref:federal-register-agency:196` | slug: `bureau-of-the-fiscal-service` | short_name: `None` | parent: `urn:ref:federal-register-agency:497` (Treasury Department)
- **FR URL:** https://www.federalregister.gov/agencies/bureau-of-the-fiscal-service | **Agency URL:** https://home.treasury.gov/about/offices/domestic-finance/fiscal-service
- **FR roster description:** The Office of Fiscal Service helps formulate policy and develop systems for the collection, disbursement, management and security of public monies in the United States and abroad, and related government-wide accounting and reporting for those funds. Programs of interest in the Fiscal Service are cash and debt managemen…
- **Proposed basis:** `exactPublisherNameEquality` | **Content digest:** `sha256:390dd1f26ba22ca8ed9807130923b954d073099fef6f4c31afc0d4c7e0b6155e`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:bureau-of-the-fiscal-service` (`ecfr-agencies`, found by `exactSlug`): **Bureau of the Fiscal Service**, parent `urn:ref:ecfr-agency:treasury-department` (Department of Treasury); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100076578` (`federal-hierarchy-organizations`, found by `nameKey`): **BUREAU OF THE FISCAL SERVICE**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100013311` (TREASURY, DEPARTMENT OF THE); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:TRFD` (`opm-ehri-agency-subelement`, found by `nameKey`): **BUREAU OF THE FISCAL SERVICE**, code=TRFD (2013-10-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 196 (Bureau of the Fiscal Service) against 3 held-roster counterpart(s) (urn:ref:ecfr-agency:bureau-of-the-fiscal-service, urn:ref:federal-hierarchy-org:100076578, urn:ref:opm-ehri-agency-subelement:TRFD): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr591` — FR 591: Institute of Museum and Library Services (302 mentions, 1997-08-29 to 2026-11-02)
- **FR record:** `urn:ref:federal-register-agency:591` | slug: `institute-of-museum-and-library-services` | short_name: `None` | parent: `urn:ref:federal-register-agency:342` (National Foundation on the Arts and the Humanities)
- **FR URL:** https://www.federalregister.gov/agencies/institute-of-museum-and-library-services
- **Proposed basis:** `exactPublisherNameEquality` | **Content digest:** `sha256:4da51fd24c3a9d5440e4bbb38744731b01141442b4d8a74729c123fffeebc72c`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:institute-of-museum-and-library-services` (`ecfr-agencies`, found by `exactSlug`): **Institute of Museum and Library Services**, parent `urn:ref:ecfr-agency:national-foundation-on-the-arts-and-the-humanities` (National Foundation on the Arts and the Humanities); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:300000036` (`federal-hierarchy-organizations`, found by `nameKey`): **THE INSTITUTE OF MUSEUM AND LIBRARY SERVICES**, fhorgtype=Department/Ind. Agency, top-level; REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:300000248` (`federal-hierarchy-organizations`, found by `nameKey`): **THE INSTITUTE OF MUSEUM AND LIBRARY SERVICES**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:300000036` (THE INSTITUTE OF MUSEUM AND LIBRARY SERVICES); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:AH03` (`opm-ehri-agency-subelement`, found by `nameKey`): **INSTITUTE OF MUSEUM AND LIBRARY SERVICES**, code=AH03 (2005-02-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 591 (Institute of Museum and Library Services) against 4 held-roster counterpart(s) (urn:ref:ecfr-agency:institute-of-museum-and-library-services, urn:ref:federal-hierarchy-org:300000036, urn:ref:federal-hierarchy-org:300000248, urn:ref:opm-ehri-agency-subelement:AH03): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr589` — FR 589: National Endowment for the Humanities (195 mentions, 1994-06-02 to 2026-11-03)
- **FR record:** `urn:ref:federal-register-agency:589` | slug: `national-endowment-for-the-humanities` | short_name: `None` | parent: `urn:ref:federal-register-agency:342` (National Foundation on the Arts and the Humanities)
- **FR URL:** https://www.federalregister.gov/agencies/national-endowment-for-the-humanities | **Agency URL:** http://www.neh.gov/
- **Proposed basis:** `exactPublisherNameEquality` | **Content digest:** `sha256:6784cbe3950b7b60a506383f0ee44dd9f4af67074c0d6417bba3191339527b56`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:national-endowment-for-the-humanities` (`ecfr-agencies`, found by `exactSlug`): **National Endowment for the Humanities**, parent `urn:ref:ecfr-agency:national-foundation-on-the-arts-and-the-humanities` (National Foundation on the Arts and the Humanities); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100171765` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL ENDOWMENT FOR THE HUMANITIES**, fhorgtype=Department/Ind. Agency, top-level; REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:300000253` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL ENDOWMENT FOR THE HUMANITIES**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100171765` (NATIONAL ENDOWMENT FOR THE HUMANITIES); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:AH02` (`opm-ehri-agency-subelement`, found by `nameKey`): **NATIONAL ENDOWMENT FOR THE HUMANITIES**, code=AH02 (2005-02-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 589 (National Endowment for the Humanities) against 4 held-roster counterpart(s) (urn:ref:ecfr-agency:national-endowment-for-the-humanities, urn:ref:federal-hierarchy-org:100171765, urn:ref:federal-hierarchy-org:300000253, urn:ref:opm-ehri-agency-subelement:AH02): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr609` — FR 609: Great Lakes St. Lawrence Seaway Development Corporation (176 mentions, 1994-01-20 to 2026-11-19)
- **FR record:** `urn:ref:federal-register-agency:609` | slug: `great-lakes-st-lawrence-seaway-development-corporation` | short_name: `SAINT` | parent: `urn:ref:federal-register-agency:492` (Transportation Department)
- **FR URL:** https://www.federalregister.gov/agencies/great-lakes-st-lawrence-seaway-development-corporation | **Agency URL:** https://www.seaway.dot.gov/
- **FR roster description:** The Great Lakes St. Lawrence Seaway Development Corporation (GLS) is a wholly owned government corporation created by statute May 13, 1954, to construct, operate and maintain that part of the St. Lawrence Seaway between the Port of Montreal and Lake Erie, within the territorial limits of the United States. The mission …
- **Proposed basis:** `exactPublisherNameEquality` | **Content digest:** `sha256:4750729309c901031721e8f448474fe67ff310b810b4600a0f120874a1a52a64`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:great-lakes-st-lawrence-seaway-development-corporation` (`ecfr-agencies`, found by `exactSlug`): **Great Lakes St. Lawrence Seaway Development Corporation**, parent `urn:ref:ecfr-agency:transportation-department` (Department of Transportation); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100159874` (`federal-hierarchy-organizations`, found by `nameKey`): **GREAT LAKES ST. LAWRENCE SEAWAY DEVELOPMENT CORPORATION**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100000136` (TRANSPORTATION, DEPARTMENT OF); REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 609 (Great Lakes St. Lawrence Seaway Development Corporation) against 2 held-roster counterpart(s) (urn:ref:ecfr-agency:great-lakes-st-lawrence-seaway-development-corporation, urn:ref:federal-hierarchy-org:100159874): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr372` — FR 372: National Technical Information Service (94 mentions, 1994-01-03 to 2026-09-30)
- **FR record:** `urn:ref:federal-register-agency:372` | slug: `national-technical-information-service` | short_name: `NTIS` | parent: `urn:ref:federal-register-agency:54` (Commerce Department)
- **FR URL:** https://www.federalregister.gov/agencies/national-technical-information-service
- **FR roster description:** The National Technical Information Service (NTIS) operates a central clearinghouse of scientific and technical information that is useful to U.S. business and industry. NTIS collects scientific and technical information; catalogs, abstracts, indexes, and permanently archives the information; disseminates products in th…
- **Proposed basis:** `exactPublisherNameEquality` | **Content digest:** `sha256:98ebfe5830f4fdc45bfebf1daf949e9aa5c1d5d9510d4c29d670285502c81b1e`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:national-technical-information-service` (`ecfr-agencies`, found by `exactSlug`): **National Technical Information Service**, parent `urn:ref:ecfr-agency:commerce-department` (Department of Commerce); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100196017` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL TECHNICAL INFORMATION SERVICE**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100035122` (COMMERCE, DEPARTMENT OF); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:CM62` (`opm-ehri-agency-subelement`, found by `nameKey`): **NATIONAL TECHNICAL INFORMATION SERVICE**, code=CM62 (2005-02-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 372 (National Technical Information Service) against 3 held-roster counterpart(s) (urn:ref:ecfr-agency:national-technical-information-service, urn:ref:federal-hierarchy-org:100196017, urn:ref:opm-ehri-agency-subelement:CM62): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr448` — FR 448: Refugee Resettlement Office (73 mentions, 1994-03-14 to 2026-10-01)
- **FR record:** `urn:ref:federal-register-agency:448` | slug: `refugee-resettlement-office` | short_name: `ORR` | parent: `urn:ref:federal-register-agency:221` (Health and Human Services Department)
- **FR URL:** https://www.federalregister.gov/agencies/refugee-resettlement-office
- **FR roster description:** The Office of Refugee Resettlement (ORR) was established by Congress under The Refugee Act of 1980. This Act incorporates the definition of "refugee" used in the U.N. Protocol, and makes provision for regular flow as well as emergency admission of refugees, and authorizes federal assistance for the resettlement of refu…
- **Proposed basis:** `obviousPublisherNameVariant` | **Content digest:** `sha256:771af08c7edcba0220a3eef0260e8669bcaf371d43786bcdfacc1b31d7c35f75`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:refugee-resettlement-office` (`ecfr-agencies`, found by `exactSlug`): **Office of Administration for Children and Families Refugee Resettlement**, parent `urn:ref:ecfr-agency:health-and-human-services-department` (Department of Health and Human Services); REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 448 (Refugee Resettlement Office) against 1 held-roster counterpart(s) (urn:ref:ecfr-agency:refugee-resettlement-office): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr600` — FR 600: National Assessment Governing Board (50 mentions, 1998-09-15 to 2026-11-12)
- **FR record:** `urn:ref:federal-register-agency:600` | slug: `national-assessment-governing-board` | short_name: `NAGB` | parent: `urn:ref:federal-register-agency:126` (Education Department)
- **FR URL:** https://www.federalregister.gov/agencies/national-assessment-governing-board | **Agency URL:** https://www.nagb.gov
- **FR roster description:** The National Assessment Governing Board was created by Congress in 1988 as an independent, nonpartisan board to set policy for the National Assessment of Educational Progress (NAEP), also known as The Nation’s Report Card. In overseeing The Nation’s Report Card, the Governing Board identifies subjects to be tested, det…
- **Proposed basis:** `obviousPublisherNameVariant` | **Content digest:** `sha256:09283a92c9e9484d5129c834c8b9f0b4d60c4e6deff49808a5057a0b93b9325e`
- **Held-roster counterparts:**
  - `urn:ref:federal-hierarchy-org:100525883` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL ASSESSMENT GOVERNING BOARD**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100001616` (EDUCATION, DEPARTMENT OF); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100525304` (`federal-hierarchy-organizations`, found by `nameKey`): **NATIONAL ASSESSMENT GOVERNING BOARD**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100001616` (EDUCATION, DEPARTMENT OF); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:EDEZ` (`opm-ehri-agency-subelement`, found by `nameKey`): **NATIONAL ASSESSMENT GOVERNING BOARD**, code=EDEZ (2005-02-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 600 (National Assessment Governing Board) against 3 held-roster counterpart(s) (urn:ref:federal-hierarchy-org:100525883, urn:ref:federal-hierarchy-org:100525304, urn:ref:opm-ehri-agency-subelement:EDEZ): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr143` — FR 143: Engraving and Printing Bureau (48 mentions, 1994-05-04 to 2023-07-06)
- **FR record:** `urn:ref:federal-register-agency:143` | slug: `engraving-and-printing-bureau` | short_name: `BEP` | parent: `urn:ref:federal-register-agency:497` (Treasury Department)
- **FR URL:** https://www.federalregister.gov/agencies/engraving-and-printing-bureau | **Agency URL:** http://www.moneyfactory.gov/
- **FR roster description:** The Bureau of Engraving and Printing operates on basic authorities conferred by act of July 11, 1862 (31 U.S.C. 303), and additional authorities contained in past appropriations made to the Bureau that are still in force. Operations are financed by a revolving fund established in 1950 in accordance with Public Law 81-6…
- **Proposed basis:** `obviousPublisherNameVariant` | **Content digest:** `sha256:1b1bfbe621cb3a32030d4bc95e9ca4db84b01b177340e122aa873a54f0eb3b80`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:engraving-and-printing-bureau` (`ecfr-agencies`, found by `exactSlug`): **Bureau of Engraving and Printing**, parent `urn:ref:ecfr-agency:treasury-department` (Department of Treasury); REF-038 codes: `[]`
  - `urn:ref:federal-hierarchy-org:100126434` (`federal-hierarchy-organizations`, found by `wordOrderKey`): **BUREAU OF ENGRAVING AND PRINTING**, fhorgtype=Sub-Tier, parent `urn:ref:federal-hierarchy-org:100013311` (TREASURY, DEPARTMENT OF THE); REF-038 codes: `[]`
  - `urn:ref:opm-ehri-agency-subelement:TRAI` (`opm-ehri-agency-subelement`, found by `wordOrderKey`): **BUREAU OF ENGRAVING AND PRINTING**, code=TRAI (2005-02-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 143 (Engraving and Printing Bureau) against 3 held-roster counterpart(s) (urn:ref:ecfr-agency:engraving-and-printing-bureau, urn:ref:federal-hierarchy-org:100126434, urn:ref:opm-ehri-agency-subelement:TRAI): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr604` — FR 604: Partnerships and Public Engagement Office (33 mentions, 2019-10-25 to 2024-07-10)
- **FR record:** `urn:ref:federal-register-agency:604` | slug: `partnerships-and-public-engagement-office` | short_name: `OPPE` | parent: `urn:ref:federal-register-agency:12` (Agriculture Department)
- **FR URL:** https://www.federalregister.gov/agencies/partnerships-and-public-engagement-office
- **FR roster description:** The Secretary established the Office of Partnerships and Public Engagement (OPPE) to rapidly expand outreach to America’s agricultural community and facilitate greater access to USDA programs. Additionally, OPPE serves as the lead agent for USDA partnership and outreach activities with tasking and reporting authority t…
- **Proposed basis:** `obviousPublisherNameVariant` | **Content digest:** `sha256:1225e7b1b8a7e4e62802fcb4131ef008b85eb6f796cf8b3d23fd5452510c5bce`
- **Held-roster counterparts:**
  - `urn:ref:opm-ehri-agency-subelement:AGAO` (`opm-ehri-agency-subelement`, found by `wordOrderKey`): **OFFICE OF PARTNERSHIPS AND PUBLIC ENGAGEMENT**, code=AGAO (2020-04-01..PRESENT), top-level; REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 604 (Partnerships and Public Engagement Office) against 1 held-roster counterpart(s) (urn:ref:opm-ehri-agency-subelement:AGAO): accept sameEntityAs bridge, hold, or keep FR-only?

#### `batch2:fr581` — FR 581: Advocacy and Outreach Office (32 mentions, 2011-10-26 to 2018-03-26)
- **FR record:** `urn:ref:federal-register-agency:581` | slug: `advocacy-and-outreach-office` | short_name: `OAO` | parent: `urn:ref:federal-register-agency:12` (Agriculture Department)
- **FR URL:** https://www.federalregister.gov/agencies/advocacy-and-outreach-office | **Agency URL:** http://www.outreach.usda.gov/index.htm
- **FR roster description:** USDA’s Office of Advocacy and Outreach was established by the 2008 Farm Bill to improve access to USDA programs and to improve the viability and profitability of small farms and ranches, beginning farmers and ranchers, and socially disadvantaged farmers or ranchers. As such, OAO is split into five key program areas: So…
- **Proposed basis:** `obviousPublisherNameVariant` | **Content digest:** `sha256:3608b5db0cbc498f45bd3ff69e4c0a5421f7633f54441991b244f83b10fbff47`
- **Held-roster counterparts:**
  - `urn:ref:ecfr-agency:advocacy-and-outreach-office` (`ecfr-agencies`, found by `exactSlug`): **Office of Advocacy and Outreach**, parent `urn:ref:ecfr-agency:agriculture-department` (Department of Agriculture); REF-038 codes: `[]`
- **Owner question:** Adjudicate FR 581 (Advocacy and Outreach Office) against 1 held-roster counterpart(s) (urn:ref:ecfr-agency:advocacy-and-outreach-office): accept sameEntityAs bridge, hold, or keep FR-only?

### Group B — Defunct Agencies Flagged for Later Succession Batch (7 FR IDs)

Do **not** emit `sameEntityAs` for defunct agencies whose functions transferred or split; adjudicate these as dated `org:ChangeEvent` successions once their statutory/FR records are retained and read (per `plans/agency-registry-design.md` §3 & §7).

- **`batch2:fr198` — FR 198 (Food and Consumer Service)**: `160` mentions (1994-11-22..1998-02-26). Evidence kind: `publisherRoster`. Proposed successor(s): FR 200 (Food and Nutrition Service). Note: Roster: renamed the Food and Nutrition Service again, 63 FR 9721 (1998-02-26).
- **`batch2:fr258` — FR 258 (International Development Cooperation Agency)**: `148` mentions (1994-01-06..1999-12-28). Evidence kind: `statute`. Proposed successor(s): FR 476 (State Department), FR 6 (Agency for International Development), FR 397 (Overseas Private Investment Corporation). Note: FARRA secs. 1401, 1411-1412 and the roster: abolished, functions to State, USAID and OPIC -- a split.
- **`batch2:fr140` — FR 140 (Energy Research Office)**: `116` mentions (1994-01-07..1999-01-13). Evidence kind: `recordNotYetRead`. Proposed successor(s): none in held FR roster. Note: Renamed the Office of Science in 1998 (not in the held FR roster).
- **`batch2:fr568` — FR 568 (Ocean Energy Management, Regulation, and Enforcement Bureau)**: `94` mentions (2010-09-03..2011-09-30). Evidence kind: `publisherRoster`. Proposed successor(s): FR 575 (Ocean Energy Management Bureau), FR 576 (Safety and Environmental Enforcement Bureau). Note: Roster: divided into BOEM and BSEE on 2011-10-01 (ONRR, FR 586, split off 2010-10-01) -- a split.
- **`batch2:fr452` — FR 452 (Resolution Trust Corporation)**: `43` mentions (1994-01-19..1995-12-27). Evidence kind: `recordNotYetRead`. Proposed successor(s): FR 164 (Federal Deposit Insurance Corporation). Note: Remaining functions to the FDIC at its 1995 termination; the roster says only 'terminated'.
- **`batch2:fr31` — FR 31 (Arms Control and Disarmament Agency)**: `42` mentions (1994-02-22..1999-04-01). Evidence kind: `statute`. Proposed successor(s): FR 476 (State Department). Note: FARRA secs. 1201, 1211-1212: abolished, all functions to the Secretary of State (earlier of 1999-04-01 or the plan date).
- **`batch2:fr290` — FR 290 (Mines Bureau)**: `40` mentions (1994-02-15..2002-05-08). Evidence kind: `publisherRoster`. Proposed successor(s): FR 136 (Energy Department). Note: Roster: terminated 1996 (110 Stat. 32); certain functions to the Secretary of Energy (110 Stat. 1321-167) -- a split.

### Group C — Register-Only Agencies With No Held-Roster Counterpart (12 FR IDs)

No exact counterpart exists in RefSpec's held eCFR, Federal Hierarchy, or OPM rosters. Retain as FR-only records (already exposed via View 1.4 `agency-registry-organizations.parquet` and `lookup_agency`) unless the owner identifies a manual counterpart or retirement event.

- **`batch2:fr223` — FR 223 (Hearings and Appeals Office, Interior Department)**: `307` mentions (1994-01-11..2017-06-02, last published 2017), parent FR 253 (Interior Department).
- **`batch2:fr495` — FR 495 (Transportation Statistics Bureau)**: `243` mentions (1995-06-20..2026-11-18, active >=2024), parent FR 492 (Transportation Department).
- **`batch2:fr601` — FR 601 (Unified Carrier Registration Plan)**: `166` mentions (2019-05-20..2026-11-02, active >=2024), top-level.
- **`batch2:fr351` — FR 351 (National Institute of Justice)**: `127` mentions (1994-04-06..2025-01-21, active >=2024), parent FR 268 (Justice Department).
- **`batch2:fr33` — FR 33 (Assassination Records Review Board)**: `108` mentions (1995-01-09..1998-10-06, last published 1998), top-level.
- **`batch2:fr597` — FR 597 (U.S. Codex Office)**: `96` mentions (2018-09-06..2026-10-01, active >=2024), parent FR 12 (Agriculture Department).
- **`batch2:fr380` — FR 380 (Northeast Dairy Compact Commission)**: `84` mentions (1996-12-13..2002-05-08, last published 2002), top-level.
- **`batch2:fr598` — FR 598 (Federal Council on the Arts and the Humanities)**: `52` mentions (1995-07-06..2026-10-15, active >=2024), parent FR 342 (National Foundation on the Arts and the Humanities).
- **`batch2:fr618` — FR 618 (Civil Rights Cold Case Records Review Board)**: `47` mentions (2024-10-07..2026-11-09, active >=2024), top-level.
- **`batch2:fr509` — FR 509 (United States Enrichment Corporation)**: `44` mentions (1994-02-25..1998-08-07, last published 1998), top-level.
- **`batch2:fr400` — FR 400 (Panama Canal Commission)**: `42` mentions (1994-02-25..2004-12-16, last published 2004), top-level.
- **`batch2:fr592` — FR 592 (Office of Government Information Services)**: `36` mentions (2016-03-22..2026-10-06, active >=2024), parent FR 304 (National Archives and Records Administration).

## 4. Explicit Separate Gap — ID-Less Federal Register Agency Mentions

- **Magnitude:** `30,647` agency mentions in live `federal_register.parquet` carry `agencies[].id IS NULL` (**1.923%** of all `1,593,417` mentions — **6.8×** larger than the entire 131-ID unbridged pool of `4,510` mentions).
- **Why kept separate:** Federal Register agency entries without a numeric id cannot be bridged via sameEntityAs over FR agency IDs; most are generic department-level headings (e.g. 'Office of the Secretary', 24,015 mentions) or malformed pre-2001 OCR/heading fragments ('Rule', 'Proposed Rule', 'Development') that require document/parent-context disambiguation rather than ID-keyed identity bridges.

| `raw_name` | `name` | Mentions | First Date | Last Date |
| --- | --- | ---: | --- | --- |
| `Office of the Secretary` | `None` | 24,015 | 1994-01-03 | 2026-11-25 |
| `Office of Inspector General` | `None` | 407 | 1994-01-27 | 2026-09-11 |
| `Office Of Fossil Energy` | `None` | 223 | 1994-01-04 | 2008-08-13 |
| `Office of the Assistant Secretary for Community Planning and` | `None` | 191 | 1994-01-07 | 1999-11-08 |
| `Development` | `None` | 189 | 1994-01-07 | 1999-11-08 |
| `Rule` | `None` | 172 | 1994-01-31 | 2000-01-12 |
| `Commissioner` | `None` | 172 | 1994-01-11 | 2000-03-09 |
| `Proposed Rule` | `None` | 163 | 1994-01-27 | 2000-01-05 |
| `Office of the Assistant Secretary for Public and Indian Housing` | `None` | 161 | 1994-01-07 | 2000-12-26 |
| `Employment Standards Administration, Wage and Hour Division` | `None` | 134 | 1994-02-25 | 2004-09-03 |
| `Final Rule` | `None` | 110 | 1994-03-28 | 1999-12-27 |
| `Office of the Attorney General` | `None` | 110 | 1994-01-03 | 2026-09-25 |
| `Centers for Disease Control and Prevention (CDC)` | `None` | 107 | 2000-03-07 | 2017-05-22 |
| `Employment Standards Administration Wage and Hour Division` | `None` | 93 | 1994-01-07 | 2005-09-09 |
| `Bureau of Citizenship and Immigration Services` | `None` | 75 | 2003-04-01 | 2005-11-01 |

## 5. Out-of-Scope Tables — SAM.gov and USAspending Recipient UEI Tables

Verified directly against live schemas on `https://data.spicygov.ai`: neither table carries a federal agency identifier column, and **no recipient-to-agency mapping may be fabricated**:

| Table | Live Rows | Identifier Columns | Agency-ID Column | Disposition |
| --- | ---: | --- | --- | --- |
| `sam_entities` | 796,614 | `uei`, `entity_eft_indicator`, `cage_code` | None (`false`) | `outside-agency-id-scope` — SAM.gov entity registrations identify federal award/contract recipients (vendors, grantees, organizations) by UEI and CAGE code, not federal rulemaking agencies. Never map recipient UEIs onto the agency registry. |
| `usaspending_recipients` | 247,439 | `recipient_id`, `uei`, `duns` | None (`false`) | `outside-agency-id-scope` — USAspending recipient records identify spending recipients by recipient_id/UEI/DUNS, not awarding federal agencies. Awarding-agency identifiers (CGAC / TOPT_AGENCY / FH) belong to separate award/account tables if later scoped. |
