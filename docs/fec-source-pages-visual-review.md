# FEC Source Pages Bounded Raw Visual Review (`ss-j4vz`)

This record documents the mandatory raw visual review and explicit per-SHA disposition of all **6** HTTP-200 HTML pages refused by `source_page_body()` (`unsupported_body_boundaries`) in `fec_reference_source_pages` (`src/spicy_regs/transforms/fec_original_selection.json`).

## Review Method & Sample Limits

- **Scope & completeness:** **6 of 6 (100%)** of the HTTP-200 `fec_reference_source_pages` originals excluded with `unsupported_body_boundaries`. No sampling omission applies to the excluded HTTP-200 population.
- **Population accounting (all 29 selected `fec_reference_source_pages` originals):**
  - `14` admitted (`body_extracted`): 5 `www.fec.gov/legal-resources/court-cases/` pages (including 3 captures of the index), 2 `www.fec.gov/data/legal/matter-under-review/` pages (`MUR #206`, `MUR #2189`), 1 `docquery.fec.gov` `FEC FORM 3` (`C00341529/2761/`), and 6 `docquery.fec.gov` `FEC FORM 99` pages.
  - `8` refused by acquisition receipt (`http_status_500`): MUR pages `763`, `1366`, `1616`, `2243`, `2595`, `3000`, `4357`, `4499`.
  - `1` refused by acquisition receipt (`no_acquisition_receipt`): `sha256:58bf2215b395dcac74c009aa98701854e43cbe54a1cd3a95fee6a647ca9910d4` (`https://docquery.fec.gov/cgi-bin/senate_forms/C00484683/1678/f6S`, 118-byte 403 body without sidecar receipt, tracked as an explicit gap).
  - `6` refused after HTTP-200 receipt admission (`unsupported_body_boundaries`): reviewed below and explicitly dispositioned per SHA-256 in `src/spicy_regs/transforms/fec_original_selection.json`.
- **Raw source integrity (zero downloads):** Every page was read directly from the local `CentralRawArchiveStore(create=False)` at `SPICYGOV_RAW_ROOT` (`/Users/mikewolfd/Work/corpora/spicyregs/raw`), verified byte-for-byte against its selection `bytes` and `source_sha256` and its capture-import acquisition receipt SHA-256.
- **Offline visual rendering:** Each untouched original HTML payload was rendered to PNG at `1440x2400` using headless Google Chrome with `--host-resolver-rules="MAP * ~NOTFOUND"` and `--disable-background-networking` so zero external CSS, JavaScript, image, or PDF fetches occurred. Every rendered PNG was visually inspected directly.
- **Limits of offline rendering:** External stylesheets, logos, JavaScript bundles, and external PDF embeds (specifically `/pdf/873/202606089870450873/202606089870450873_000001.pdf` on `[18]`) intentionally do not load offline; the visual inspection therefore confirms the exact self-contained HTML structure and text present in the retained raw bytes. Overlap checks were measured directly against live R2 `fec-query` generation `sha256:784b23da0ea9ee7807e09715d38508cd8e0cd654af1047ae6a5b1fc63efb7300`.

## Six-Page Disposition Matrix

| Selection Idx | Source URL | Full SHA-256 | Bytes | Raw Archive Path (under `SPICYGOV_RAW_ROOT`) | Acquisition Receipt (`http_status=200`, `response_complete=true`) | Rendered Visual PNG | Disposition | Category / `reason_code` |
| ---: | --- | --- | ---: | --- | --- | --- | --- | --- |
| `[14]` | `https://www.oversight.gov/reports/recommendations` | `sha256:5b146c6dfecdb4454aa699d4b4c2645b2041f3f430004a59c62bd74c54784795` | 100,386 | `objects/sha256/5b/5b146c6dfecdb4454aa699d4b4c2645b2041f3f430004a59c62bd74c54784795.gz` (`gzip`, stored `16,259 B`) | `acquisitions/sha256/a4/a48b5988f4463032429ba0beac69081cb244229c82f3b11e4e2d8e468a378abe` (`2026-09-11T17:44:10Z`) | `visual-w2-j4vz/14_5b146c6d.png` | `excluded_with_reason` | `table_scope_refusal` / `out_of_scope_unfiltered_federal_search_portal` |
| `[17]` | `https://www.oversight.gov/reports/recommendations?submitting_oig_rec%5B%5D=236` | `sha256:7603682615cd9a23e4a9943aa2cb498eeb30b7610a0545a12e0e39654449faa4` | 88,791 | `objects/sha256/76/7603682615cd9a23e4a9943aa2cb498eeb30b7610a0545a12e0e39654449faa4.gz` (`gzip`, stored `15,659 B`) | `acquisitions/sha256/87/87cdfb74f20074ae126386718764033b50a8cb5863c1d03147dfb0c2978f8c59` (`2026-09-11T17:45:06Z`) | `visual-w2-j4vz/17_76036826.png` | `excluded_with_reason` | `accepted_coverage_limit` / `search_directory_covered_by_fec_oversight_recommendations` |
| `[18]` | `https://docquery.fec.gov/cgi-bin/fecimg/?202606089870450873` | `sha256:7777c3aba1990822596ac0c63f59d63207f3ef7c5fa01d120e8a7696309639c7` | 4,901 | `objects/sha256/77/7777c3aba1990822596ac0c63f59d63207f3ef7c5fa01d120e8a7696309639c7` (`raw`, stored `4,901 B`) | `acquisitions/sha256/d9/d98bd91ae7db269f73ff6ddf74610a327319a9b6661c6780ace2926d9a4fa6c4` (`2026-09-11T18:23:23Z`) | `visual-w2-j4vz/18_7777c3ab.png` | `excluded_with_reason` | `source_backed_refusal` / `image_viewer_shell_no_text_body` |
| `[20]` | `https://www.oversight.gov/reports/recommendations?agency-reviewe-investigated-rec%5B%5D=77&items_per_page=50` | `sha256:7c9e650a260305d313234a4bd40c18dbdcc9881153ccd3d103dd453f1fda1b33` | 95,075 | `objects/sha256/7c/7c9e650a260305d313234a4bd40c18dbdcc9881153ccd3d103dd453f1fda1b33.gz` (`gzip`, stored `16,117 B`) | `acquisitions/sha256/9e/9e558bc955c2967a3da1e03235864bef3b23ea3bd1abce7160b79d0f7c83d750` (`2026-09-11T17:49:14Z`) | `visual-w2-j4vz/20_7c9e650a.png` | `excluded_with_reason` | `accepted_coverage_limit` / `search_directory_covered_by_fec_oversight_recommendations` |
| `[21]` | `https://www.oversight.gov/reports/recommendations?submitting_oig_rec%5B%5D=236&items_per_page=50` | `sha256:85f89bc9151976bf4b4b95ddcf1f38f76da03758a403b40515e277344b811ccb` | 94,522 | `objects/sha256/85/85f89bc9151976bf4b4b95ddcf1f38f76da03758a403b40515e277344b811ccb.gz` (`gzip`, stored `15,952 B`) | `acquisitions/sha256/e7/e7fc5811cf906d46cd234d3f1dac4b2a619b03a914dd4eb6e69ca09acf96faa8` (`2026-09-11T17:47:09Z`) | `visual-w2-j4vz/21_85f89bc9.png` | `excluded_with_reason` | `accepted_coverage_limit` / `search_directory_covered_by_fec_oversight_recommendations` |
| `[27]` | `https://docquery.fec.gov/cgi-bin/forms/C00341529/` | `sha256:a2898228d9a542ff9ed855dafe9b42981ac81597afb51818a16960750e36db1a` | 43,090 | `objects/sha256/a2/a2898228d9a542ff9ed855dafe9b42981ac81597afb51818a16960750e36db1a.gz` (`gzip`, stored `4,871 B`) | `acquisitions/sha256/cb/cb3720c9c373906969c79fa4e3c33974a1ec2ef25e38db247b97bd31cdd4193e` (`2026-09-11T18:09:51Z`) | `visual-w2-j4vz/27_a2898228.png` | `excluded_with_reason` | `table_scope_refusal` / `committee_filing_directory_table_out_of_prose_scope` |

Screenshot directory for the local Wave 2 visual-review session: `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/`.
Raw archive root on the review host: `/Users/mikewolfd/Work/corpora/spicyregs/raw`.

## Detailed Visual Findings & Overlap Verification

### 1. `[14]` `sha256:5b146c6dfecdb4454aa699d4b4c2645b2041f3f430004a59c62bd74c54784795` — Unfiltered Oversight.gov Recommendations Portal

- **URL:** `https://www.oversight.gov/reports/recommendations`
- **Archive object:** `/Users/mikewolfd/Work/corpora/spicyregs/raw/objects/sha256/5b/5b146c6dfecdb4454aa699d4b4c2645b2041f3f430004a59c62bd74c54784795.gz` (`100,386 B` decoded, `stored_sha256: d14d2c6501a1c5469e23e3114b4484c6775054f2064a79fea9d86ac3aab19dce`)
- **Acquisition receipt:** `acquisitions/sha256/a4/a48b5988f4463032429ba0beac69081cb244229c82f3b11e4e2d8e468a378abe` (`http_status=200`, `response_complete=true`, `acquired_at=2026-09-11T17:44:10.078883+00:00`)
- **Rendered screenshot:** `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/14_5b146c6d.png`
- **Visual findings:** Renders the unfiltered federal-wide CIGIE/Oversight.gov `Open Recommendations` Drupal search form (`Date Issued`, `Report Type`, `Agency Reviewed`, `Submitting OIG`, `Priority Recommendation`, `Age`, `Items per page: 25`), global KPI counters (`12,257 Open Recommendations`, `2K Open Less Than 1 Year`, `6K Open Between 1-5 Years`, `2K Open More Than 5 Years`), and page 1 (`Displaying 1 - 25 of 12238`) showing exclusively **non-FEC** recommendations (starting with `09/09/2026 General Services Administration OIG — PBS Is Not Consistently Complying with Its Internal Guidance for the Building Assessment Tool`, followed by Commerce/NTIA and other agencies).
- **Table overlap:** `0 / 25` listed recommendations belong to the FEC or overlap `fec_oversight_recommendations` (which already contains all `31` structured FEC OIG recommendations parsed from the 6 individual FEC OIG report pages via `src/spicy_regs/transforms/fec_agency.py`).
- **Disposition:** `excluded_with_reason` (`table_scope_refusal` / `out_of_scope_unfiltered_federal_search_portal`).

### 2. `[17]` `sha256:7603682615cd9a23e4a9943aa2cb498eeb30b7610a0545a12e0e39654449faa4` — Paginated Oversight.gov FEC OIG Filter Page (1–25 of 31)

- **URL:** `https://www.oversight.gov/reports/recommendations?submitting_oig_rec%5B%5D=236`
- **Archive object:** `/Users/mikewolfd/Work/corpora/spicyregs/raw/objects/sha256/76/7603682615cd9a23e4a9943aa2cb498eeb30b7610a0545a12e0e39654449faa4.gz` (`88,791 B` decoded, `stored_sha256: 7ad9b703131c9eb7d1cb21dc303e9324c69d69b0fbfb64a1bf6e21c18145e5c2`)
- **Acquisition receipt:** `acquisitions/sha256/87/87cdfb74f20074ae126386718764033b50a8cb5863c1d03147dfb0c2978f8c59` (`http_status=200`, `response_complete=true`, `acquired_at=2026-09-11T17:45:06.364919+00:00`)
- **Rendered screenshot:** `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/17_76036826.png`
- **Visual findings:** Renders the same Drupal search/filter sidebar (`Submitting OIG` selected to `Federal Election Commission OIG`, `Items per page: 25`), retains the unfiltered federal-wide KPI widget (`12,257 Open Recommendations`) above `Filtered by Federal Election Commission OIG`, and lists only page 1 (`Displaying 1 - 25 of 31`) beginning with `07/27/2026 Federal Election Commission OIG — Evaluation of the FEC's DATA Act Compliance`. Recommendations 26–31 are omitted by default pagination.
- **Table overlap:** Exact **25 / 25** character-for-character match against `fec_oversight_recommendations` (`31` total rows across 6 FEC OIG reports) and exact prefix (`[:25]`) of `[20]` and `[21]`.
- **Disposition:** `excluded_with_reason` (`accepted_coverage_limit` / `search_directory_covered_by_fec_oversight_recommendations`).

### 3. `[18]` `sha256:7777c3aba1990822596ac0c63f59d63207f3ef7c5fa01d120e8a7696309639c7` — DocQuery PDF Image-Viewer Shell

- **URL:** `https://docquery.fec.gov/cgi-bin/fecimg/?202606089870450873`
- **Archive object:** `/Users/mikewolfd/Work/corpora/spicyregs/raw/objects/sha256/77/7777c3aba1990822596ac0c63f59d63207f3ef7c5fa01d120e8a7696309639c7` (`4,901 B` raw, `stored_sha256: 7777c3aba1990822596ac0c63f59d63207f3ef7c5fa01d120e8a7696309639c7`)
- **Acquisition receipt:** `acquisitions/sha256/d9/d98bd91ae7db269f73ff6ddf74610a327319a9b6661c6780ace2926d9a4fa6c4` (`http_status=200`, `response_complete=true`, `acquired_at=2026-09-11T18:23:23.262812+00:00`)
- **Rendered screenshot:** `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/18_7777c3ab.png`
- **Visual findings:** Renders a leaked CGI header line (`Content-type 1088: text/html`), breadcrumbs pointing to committee `C00871426`, heading `Page by Page Report Display (Page 1 of 2)`, page-navigation controls (`Goto Page #`, `Next >>`, `Last >>|`, `View Full Report`), a blank gray `<embed type='application/pdf'>` frame targeting `/pdf/873/202606089870450873/202606089870450873_000001.pdf`, and the FEC contact footer. **Zero** filing text or report body is present in the HTML document.
- **Table overlap:** `fec_filings` maps `pdf_url = https://docquery.fec.gov/pdf/873/202606089870450873/202606089870450873.pdf` directly to `html_url = https://docquery.fec.gov/cgi-bin/forms/C00871426/1981955/` (`F99`), which is **already admitted** in `fec_reference_source_pages` as selection entry `[28]` (`sha256:af750f5ceec7c5f72d017edb634cebf21574ffb602c915242f37fe9850d6c83c`). PDF/image extraction is explicitly deferred (`docs/fec-delivery-plan.md`).
- **Disposition:** `excluded_with_reason` (`source_backed_refusal` / `image_viewer_shell_no_text_body`).

### 4. `[20]` `sha256:7c9e650a260305d313234a4bd40c18dbdcc9881153ccd3d103dd453f1fda1b33` — Oversight.gov Agency-Reviewed Filter Directory (1–31 of 31)

- **URL:** `https://www.oversight.gov/reports/recommendations?agency-reviewe-investigated-rec%5B%5D=77&items_per_page=50`
- **Archive object:** `/Users/mikewolfd/Work/corpora/spicyregs/raw/objects/sha256/7c/7c9e650a260305d313234a4bd40c18dbdcc9881153ccd3d103dd453f1fda1b33.gz` (`95,075 B` decoded, `stored_sha256: 47146102f5da36fafcb391de7cb624f97eff5ba74a44bc48bc725bebca4d5886`)
- **Acquisition receipt:** `acquisitions/sha256/9e/9e558bc955c2967a3da1e03235864bef3b23ea3bd1abce7160b79d0f7c83d750` (`http_status=200`, `response_complete=true`, `acquired_at=2026-09-11T17:49:14.570394+00:00`)
- **Rendered screenshot:** `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/20_7c9e650a.png`
- **Visual findings:** Renders the Oversight.gov Drupal search/filter sidebar with `Agency Reviewed` selected to `Federal Election Commission` and `Items per page: 50`, preceded by the unfiltered federal-wide KPI widget (`12,256 Open Recommendations`), `Filtered by Federal Election Commission`, and listing `Displaying 1 - 31 of 31` FEC OIG open recommendations.
- **Table overlap:** Exact **31 / 31** character-for-character match against structured `fec_oversight_recommendations` (`31` rows across 6 FEC OIG reports) and identical recommendation sequence to `[21]`.
- **Disposition:** `excluded_with_reason` (`accepted_coverage_limit` / `search_directory_covered_by_fec_oversight_recommendations`).

### 5. `[21]` `sha256:85f89bc9151976bf4b4b95ddcf1f38f76da03758a403b40515e277344b811ccb` — Oversight.gov Submitting-OIG Filter Directory (1–31 of 31)

- **URL:** `https://www.oversight.gov/reports/recommendations?submitting_oig_rec%5B%5D=236&items_per_page=50`
- **Archive object:** `/Users/mikewolfd/Work/corpora/spicyregs/raw/objects/sha256/85/85f89bc9151976bf4b4b95ddcf1f38f76da03758a403b40515e277344b811ccb.gz` (`94,522 B` decoded, `stored_sha256: ab6f472ebe0527416035802f0eb41910929e130deda062877585986386616308`)
- **Acquisition receipt:** `acquisitions/sha256/e7/e7fc5811cf906d46cd234d3f1dac4b2a619b03a914dd4eb6e69ca09acf96faa8` (`http_status=200`, `response_complete=true`, `acquired_at=2026-09-11T17:47:09.233368+00:00`)
- **Rendered screenshot:** `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/21_85f89bc9.png`
- **Visual findings:** Renders the identical Oversight.gov Drupal search/filter page filtered by `Submitting OIG = Federal Election Commission OIG` with `Items per page: 50`, preceded by the unfiltered federal-wide KPI widget (`12,257 Open Recommendations`) and `Filtered by Federal Election Commission OIG`, listing `Displaying 1 - 31 of 31`.
- **Table overlap:** Exact **31 / 31** character-for-character match against structured `fec_oversight_recommendations` and exact duplicate recommendation list of `[20]`.
- **Disposition:** `excluded_with_reason` (`accepted_coverage_limit` / `search_directory_covered_by_fec_oversight_recommendations`).

### 6. `[27]` `sha256:a2898228d9a542ff9ed855dafe9b42981ac81597afb51818a16960750e36db1a` — DocQuery Committee Electronic-Filing Directory Table (`C00341529`)

- **URL:** `https://docquery.fec.gov/cgi-bin/forms/C00341529/`
- **Archive object:** `/Users/mikewolfd/Work/corpora/spicyregs/raw/objects/sha256/a2/a2898228d9a542ff9ed855dafe9b42981ac81597afb51818a16960750e36db1a.gz` (`43,090 B` decoded, `stored_sha256: b3a8d7c414ae38f74954ef75aaf15a542b7756ff5e904d40926dff823c04b139`)
- **Acquisition receipt:** `acquisitions/sha256/cb/cb3720c9c373906969c79fa4e3c33974a1ec2ef25e38db247b97bd31cdd4193e` (`http_status=200`, `response_complete=true`, `acquired_at=2026-09-11T18:09:51.126868+00:00`)
- **Rendered screenshot:** `/Users/mikewolfd/.gemini/antigravity/brain/349127dd-bda0-4fcc-a5db-e1eeeda7f78c/scratch/swarm-20261127/visual-w2-j4vz/27_a2898228.png`
- **Visual findings:** Renders a clean 6-column HTML filing directory table (`Form Type | FEC ID | Coverage Start Date | Coverage End Date | Report Type | Filed Date`) titled `Filings by HENRY E. BROWN JR. FOR CONGRESS - C00341529` (`1035 DOMINION DRIVE / HANAHAN`) with **178** electronic filing index rows spanning `01/01/1998–12/31/1998` (`F3N`, `FEC-1403`, filed `01/20/1999`) through `07/01/2019–09/30/2019` (`F3T`, `FEC-1359148`, `TERMINATION REPORT`, filed `10/16/2019`). Each `Form Type` cell links to an individual DocQuery form page (including row `176`: `F3A`, `FEC-2761`, `01/01/1999–06/30/1999`, `MID-YEAR`, filed `12/28/1999`, href `2761/`). The page contains **zero** narrative reference prose.
- **Table overlap & honest coverage limit:**
  - Every admitted `docquery.fec.gov` row in `fec_reference_source_pages` is an **individual filing form** (`FEC FORM 3` or `FEC FORM 99`), including `[15]` `https://docquery.fec.gov/cgi-bin/forms/C00341529/2761/` (`sha256:613e1365...`, `FEC FORM 3`, filing `FEC-2761`).
  - Live sampled `fec_filings` (`172` rows) currently holds **`0 / 178`** filings for `C00341529`, while `fec_filing_report_observations` holds `1` `F3A` report (`retained-filing-8836c55f05076e2e`) and `fec_reference_source_pages` admits `[15]` (`FEC-2761`).
  - Flattening 178 structured filing-directory rows into an unparsed newline string inside `fec_reference_source_pages` would mix tabular committee index listings into a flat prose reference table. Keeping `[27]` excluded preserves table grain (`expected_rows = 14`) while explicitly recording the sampled coverage gap in `fec_original_selection.json`; the untouched raw original remains retained in `CentralRawArchiveStore` for any future structured filing-index ingestion.
- **Disposition:** `excluded_with_reason` (`table_scope_refusal` / `committee_filing_directory_table_out_of_prose_scope`).

## Verification & Handoff to `ss-j9k3`

- **Exact `expected_rows` verified via `fec_original_tables.rebuild()` against `CentralRawArchiveStore`:**
  - `fec_historical_ie_statistics`: `1,086` rows (`0` refusals)
  - `fec_research_meeting_observations`: `60` rows (`0` refusals)
  - `fec_reference_filing_feed_items`: `794` rows (`0` refusals)
  - `fec_reference_source_pages`: `14` rows (`15` refusals: `8` `http_status_500`, `1` `no_acquisition_receipt`, `6` `unsupported_body_boundaries`, all `6` enforced against `excluded_page_dispositions`)
- **Declared schema & retirement alignment:**
  - All 4 `fec_original_tables.SCHEMAS` match `data_dictionary/fec_typed_schemas.json` exactly.
  - Zero `source_context_pointer` columns exist across all 50 declared `fec-query` tables in `data_dictionary/fec_typed_schemas.json`.
  - `fec_native_conversion.referenced_evidence()` strips all `fec_record_evidence` rows targeting any of the 8 retired or rebuilt tables (`fec_research_document_observations`, `fec_retained_csv_observations`, `fec_research_response_outcomes`, `fec_research_context_dispositions`, `fec_historical_ie_statistics`, `fec_research_meeting_observations`, `fec_research_filing_feed_items`, `fec_research_source_pages`).
- **Dictionary regeneration handoff (`w2-fec-cutover` / `ss-j9k3`):** Per Wave 2 dictionary merge discipline, `data_dictionary/descriptions.yaml`, `src/spicy_regs/table_metadata.json`, and `docs/tables/fec_reference_source_pages.md` are regenerated in a single coordinated `spicy-regs-dict generate` pass by `w2-fec-cutover` so prose notes that `fec_reference_source_pages` publishes `body_extracted` rows only and explicitly accounts for the 6 visually reviewed HTTP-200 exclusions recorded in `fec_original_selection.json`.
