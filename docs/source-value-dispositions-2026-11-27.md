# Cross-Family Source-Value Dispositions (`ss-qm55`, 2026-11-27)

This note records live-verified dispositions and storage measurements for the cross-family
source-value items audited under ticket `ss-qm55`. Every item was checked against the current
codebase, live `publication.v2.json` Parquet tables on R2, and retained raw originals where
applicable.

> Note: No R2 generation digests appear in this file so `generation_retention.py` does not
> unintentionally pin older generations.

## 1. CMS Drug Spending — Scientific Notation (`cms_drug_spending.py`)

**Disposition:** **Fixed in code** (`src/spicy_regs/transforms/cms_drug_spending.py`).

- **Root cause:** `_DECIMAL` previously rejected `[eE][+-]?[0-9]+` before `Decimal` parsing,
  misclassifying valid CMS scientific-notation cells as `"non_numeric"`.
- **Live & retained-original verification (2026-11-27):** Across all published 2024 CMS drug
  spending rows (`cms_medicaid_drug_spending`: 18,511 rows; `cms_part_b_drug_spending`: 799 rows;
  `cms_part_d_drug_spending`: 14,536 rows), exactly 2 rows in `cms_medicaid_drug_spending` had
  `"non_numeric"` statuses, both on `Avg_Spnd_Per_Dsg_Unt_Wghtd_2024` and both verified against
  the retained raw API JSON responses:
  - `Sodium Chloride*` / `Hospira` (`cms:98aa468b-130b-4c3d-897a-552a94f85750:15630`, JSON pointer
    `/630`): literal `"5.7405e-05"` -> exact decimal `0.000057405` (9 fractional digits <= 12).
    Now parsed as `Decimal("0.000057405")` with status `"value"`.
  - `Paricalcitol*` / `Hospira/Pfizer` (`cms:98aa468b-130b-4c3d-897a-552a94f85750:13317`, JSON
    pointer `/3317`): literal `"8.2132926e-06"` -> exact decimal `0.0000082132926` (13 fractional
    digits > 12). Now classified as `"unrepresentable"` (value `None`) rather than
    `"non_numeric"`, refusing silent rounding under declared `DECIMAL(38,12)`.
- Both original literals remain preserved verbatim in `raw_row_json`.
- Scanning `raw_row_json` across all supplied years confirms CMS routinely emits scientific
  notation in rate/change columns (42 cells in Medicaid, 1 in Part B, 21 in Part D).
- `build_cms_part_d.py` intentionally omits a `program` column (pinned by
  `tests/test_cms_medicaid.py::test_part_d_schema_and_program_boundary_remain_distinct`); no
  unrequested Part D schema change was made.

## 2. SCOTUS Capture-vs-Document Dates (and Other Retired Retained-Native Sample Families)

**Disposition:** **Retired / obsolete by owner decision (`ss-i1dy`, 2026-11-11).**

- On 2026-11-11, the owner unpublished all six `retained-*` sample families (Supreme Court,
  CFTC, SEC rulemakings, FERC, EDIS, and Inspectors General — 15 tables, 718 rows) from
  `publication.v2.json` and deleted `sources/retained_native_collections.py`,
  `retained_native_publication.py`, and their 15 ETL policies in PR #187.
- Live `publication.v2.json` verification on 2026-11-27 confirms 0 SCOTUS, CFTC, EDIS, FERC,
  IG, or `sec_rulemakings` tables remain published.
- Raw captures remain preserved in the shared raw archive; if a full Supreme Court family is
  built later, document identity vs. capture occurrences and ISO `YYYY-MM-DD` date normalization
  (retaining literal `M/D/YY` source dates) should be specified in that new family's contract.

## 3. GAO `-0` URL Pairs and HTML-Entity Titles (`gao_decisions`, `gao_reports`)

**Disposition:** **Intentional publisher fidelity — no destructive deduplication or silent title mutation.**

- **`-0` URL pairs in `gao_decisions` (6 pairs across 6,616 live rows):** Every one of the 6
  pairs sharing a `decision_number` is a **distinct GAO publication page** released on a
  different date — NOT a duplicate Drupal path alias:
  - `2020-02`: `/products/2020-02-0` (released `2022-10-21`, original CAB decision) vs.
    `/products/2020-02` (released `2023-01-20`, *Reconsideration of...*).
  - `B-333110`: `/products/b-333110` (released `2021-06-03`, *Update on Decision...*) vs.
    `/products/b-333110-0` (released `2021-06-15`, decided `2021-06-15`, full decision).
  - `B-330776` (`2019-09-05` vs. `2020-04-22`), `B-331093` (`2019-10-22` vs. `2020-06-30`),
    `B-331094` (`2019-09-05` vs. `2020-06-25`), and `B-331132` (`2019-12-19` vs. `2020-08-06`):
    each pair is released 7.5–9.5 months apart in different Annual Index scopes.
  - `spicy_docs.sources.gao.month_in_review.collect_listing()` already deduplicates true
    identical-field `-0` Drupal aliases for products (`gao_reports`) while intentionally keying
    legal decisions on `(decision_number, url)`. Dropping `-0` decision rows would destroy real
    decisions and reconsiderations.
- **HTML entities in titles:** Live `gao_decisions` holds 46 titles with literal entities
  (`&amp;`: 44, `&#151;`: 1, `&#8212;`: 1) and live `gao_reports` holds 48. The `spicy-docs`
  parser already runs `HTMLParser(convert_charrefs=True)`; surviving entities are literal
  double-escaped text in GAO's own HTML (`&amp;amp;`, `&amp;#151;`, `&amp;#8212;`). Both
  `spicy_docs.schemas.gao_decision_tables` and `docs/tables/gao_decisions.md` explicitly
  document this as publisher fidelity and instruct consumers to apply `html.unescape` when
  matching titles. Any decoded display title belongs in an explicit additive column rather than
  overwriting publisher text.

## 4. CourtListener Null/Empty Strings and Counsel Lists (`court_opinion_clusters`, `court_opinions`)

**Disposition:** **Intentional publisher-fidelity preservation — deferred from silent mutation.**

- **Live measurements (2026-11-27):**
  - `court_opinion_clusters.parquet` (10,211,354 rows; 3.97 GB; published 2026-11-27):
    - `ingest_source = 'bulk'` (10,174,585 rows): `attorneys = ['']` on 5,139,262 (50.51%),
      `attorneys IS NULL` on 34, `attorneys = []` on 0; `judges = ''` on 4,503,165 (25 NULL);
      `syllabus = ''` on 9,876,472 (27 NULL); `headmatter = ''` on 7,881,643 (34 NULL).
    - `ingest_source = 'search'` (36,769 rows): `attorneys = ['']` on 36,769 (100%),
      `judges = ''` on 36,005, `syllabus = ''` on 36,322, `headmatter IS NULL` on 36,769.
    - Combined: `attorneys = ['']` on 5,176,031 / 10,211,354 rows (50.69%).
  - `court_opinions.parquet` (10,903,928 rows; 651.9 MB): `author_str = ''` on 6,254,793
    (0 NULL), `joined_by_str = ''` on 10,903,877 (0 NULL), `sha1 = ''` on 520,243 (0 NULL),
    `download_url = ''` on 3,940, `local_path = ''` on 6,989,268.
- **Why not silently coerced here:**
  1. `build_court_opinion_clusters.py` (`_s`, `_counsel`), `court_subjects.py` (`_names`), and
     `court_bulk.py` deliberately preserve source `NULL` vs. `""` distinctions, matching
     `docs/tables/court_opinion_clusters.md`.
  2. Per-row ETL receipts on `court_opinion_clusters` were retired on 2026-11-26 (`ss-vse3`), so
     mutating `"" -> NULL` or `[""] -> []` in place would destroy the original source distinction
     without retained per-row conversion inputs.
  3. The daily Search catch-up (`docs/court-search-recovery.md`) appends onto the existing
     3.97 GB table without restoring prior rows; changing shaper semantics alone would split
     new daily rows from 10.21M existing rows mid-quarter. Any normalization to `NULL`/`[]`
     should be an explicit schema/policy decision executed with a quarterly bulk-export rebuild.

## 5. U.S. Code Duplicated Observations — Measured Byte Savings (`uscode-editions`)

**Disposition:** **Measured; no breaking schema move performed in this PR.**

Measured directly from live R2 `uscode-editions` (published `2026-11-11T12:33:40Z`, covering
2000 annual XHTML + release points `119-102` and `119-138`):

| Table | Rows | Total Parquet Size | `source_observation_json` Compressed | Share of File | `source_observation_json` Uncompressed Pages | Share of Uncompressed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `uscode_edition_sections.parquet` | 179,833 | 368,460,251 B (351.39 MiB) | 250,383,111 B (238.78 MiB) | 68.0% | 2,613,932,309 B (2.43 GiB) | 85.0% |
| `uscode_edition_structure.parquet` | 194,662 | 23,434,634 B (22.35 MiB) | 14,161,615 B (13.51 MiB) | 60.4% | 324,888,779 B (309.84 MiB) | 91.9% |
| **Combined** | **374,495** | **391,894,885 B (373.74 MiB)** | **264,544,726 B (252.29 MiB)** | **67.5%** | **2,938,821,088 B (2.74 GiB)** | **85.7%** |

- Decoded UTF-8 payload (`SUM(strlen(...))`) in `uscode_edition_sections.parquet`:
  - `body_text`: 436,212,838 B (annual: 106,497,269 B; release-point XML: 329,715,569 B).
  - `source_observation_json`: 2,610,717,139 B (annual: 492,607,519 B, 4.63× `body_text`;
    release-point XML: 2,118,109,620 B, 6.42× `body_text`), representing **84.53%** of total
    3,088,471,653 B decoded UTF-8 text across all 8 columns.
- **Recommendation:**
  - The earlier ">1 GB Parquet reduction" target is physically impossible (entire file is 368.5 MB),
    and the earlier "move to `etl_receipts.parquet`" design is obsolete because `uscode-editions`
    has retired per-row receipts (`uscode_editions.py`).
  - `source_observation_json` also carries non-duplicated structured metadata (`element`,
    `ancestors`, `fields`, `native_text`, `structure`, `issues`) that both independent auditors
    (`uscode_editions_audit.py`, `uscode_xml_audit.py`) verify verbatim. Meanwhile, Parquet
    columnar projection avoids downloading or decoding `source_observation_json` on queries that
    select `body_text` and identifier columns.
  - Keep the current schema intact now; if a future storage/decode optimization is prioritized,
    either move `source_observation_json` to an optional companion table or omit redundant
    `body_text`/`native_text` keys from the JSON payload alongside coordinated auditor updates.
- **Title zero-padding (`uscode_editions.parquet`, 177 rows):** Confirmed `annual` 2000 titles
  are unpadded (60 titles, 0 zero-padded) while `release-point-corpus` `119-102` and `119-138`
  each have 10 zero-padded titles (`01`–`09`, `05a`) matching `member_name` while `metadata_json`
  retains unpadded `doc_number`. Both independent auditors pin current `title` values; leave
  unchanged pending an explicit cross-edition schema decision.

## 6. Explicit Status of Remaining Agency and Lobbying Checks

Kept explicitly separate from this PR (no unrequested schema changes):

- **`sam_entities`** (796,614 live rows; currently paused on SAM API key refusal per `ss-b9ho`):
  1,090 `legal_business_name` values (0.14%) and 703 `dba_name` values (0.09%) carry leading or
  trailing whitespace (`city`: 0, `state`: 0).
- **`ofac_entity_observations`** (20,115 live rows, 33.83 MB): `raw_entity_json` accounts for
  14.06 MB compressed / 476.70 MB uncompressed.
- **`grants_opportunity_observations`** (83,733 live rows, 113.92 MB): `raw_fields_json` accounts
  for 68.17 MB compressed / 555.02 MB uncompressed.
- **`lobbying_filings`** (1,991,902 live rows): sampled nullable text columns
  (`registrant_address_2`, `posted_by_name`, `expenses_method`) show 0 empty strings (`""`) vs.
  362K–1.78M `NULL`s.
