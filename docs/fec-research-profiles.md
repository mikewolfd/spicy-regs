# FEC research profiles and election outputs

The `fec-research-profiles` family builds five receipts-free Parquet tables from
retained FEC originals in `CentralRawArchiveStore`:

- `fec_committee_relationship_profiles` (`fec-disclosed-relationships/1`)
- `fec_contribution_profiles` (`fec-selected-report-contributions/1`)
- `fec_contribution_decisions` (`fec-selected-report-contributions/1`)
- `fec_outside_spending_profiles` (`fec-publisher-outside-spending/1`)
- `fec_outside_spending_spenders` (`fec-publisher-outside-spending/1`)

These outputs extend existing FEC readers, relationship mapping, exact-money
field mappings, and financial membership selection without modifying Arrow
schemas or replacing whole-corpus `fec-query`. Builds stream all five tables in
bounded Arrow batches (`WRITE_BATCH_SIZE = 2000`) and publish through
`publish-local-generation` (`LOCAL_FAMILIES`).

## Committee disclosures

`fec_committee_relationship_profiles` preserves each selected native committee
occurrence from `/v1/committees/`. The maintained `api_relationships` mapping
retains every affiliation and sponsor field, missing/null/empty distinctions,
multiplicity, and unresolved labels. Sponsor-ID and sponsor-object arrays remain
separate source witnesses. Names never become IDs. Source retrieval and filing
dates do not establish a relationship effective date, ownership, a transfer, or
candidate authorization.

Scaled v2 selections declare `committee_population`:

- `complete_traversal`: contiguous pages `1..N` with exact publisher pagination
  (`is_count_exact == true`, constant `count`, and total rows reconciled to
  `count`).
- `bounded_page_prefix`: an explicit capped prefix (`--max-committee-pages > 0`)
  when fewer pages than the publisher total are requested.
- `sampled_pages`: legacy v1 pilot or explicitly hand-pinned page selections.

Existing retained `/v1/committees/` pages from a prior selection JSON or a
`build_fec_committees` `pages.jsonl` capture directory can be reused via
`--reuse-committees-from` without re-fetching unchanged pages.

## Contributions

`fec_contribution_profiles` and `fec_contribution_decisions` qualify recipient-side
Schedule A receipts and matching Schedule B refunds across reviewed electronic
**Form 3** (`SA11AI`/`SA11C`/`SB20A`/`SB20C`), **Form 3P**
(`SA17A`/`SA17C`/`SB28A`/`SB28C`), and **Form 3X**
(`SA11AI`/`SA11C`/`SB28A`/`SB28C`) effective reports across supported format
versions (including 8.5, 8.4, 8.2, 8.1, and 8.0 where reviewed cover and
schedule layouts exist).

Every admitted report enforces:

- Publisher effective-report binding: `most_recent == true`, `is_amended == false`,
  matching `file_number`, `most_recent_file_number`, and `amendment_chain[-1]`,
  plus exact cover match on `reporting_committee_id`, `period_start`,
  `period_end`, and `report_code`.
- Candidate association: Form 3 / 3P reports require explicit committee-history
  linkage and a pre-report candidate observation in the selected cycle; Form 3X
  non-candidate committees require `candidate_id = null` and empty
  `candidate_ids` in committee history.
- Exact official-report reconciliation: signed itemized receipts and refunds for
  both `individual` and `pac` families must reconcile to the penny against the
  official `/v1/committee/{id}/reports/` period measures, and
  `signed + unitemized == total_individual_contributions_period`. Legitimate
  zero-PAC or zero-individual effective reports are admitted only when the
  official report declares `$0.00` for that family's matching period measures.
- Separation of roles: memo entries stay visible and excluded; positive receipts,
  negative adjustments, and Schedule B refunds stay distinct; unitemized amounts
  are never attributed to named donors. Individual donor data remains subject to
  `52 U.S.C. 30111(a)(4)` sale-or-use restrictions.

## Independent spending

`fec_outside_spending_profiles` and `fec_outside_spending_spenders` consume
OpenFEC `/v1/schedules/schedule_e/by_candidate/` and
`/v1/schedules/schedule_e/totals/by_candidate/` across multi-cycle Presidential
(`P`), Senate (`S`), and House (`H`) cohorts. Each `(candidate_id, cycle)` scope
verifies exact pagination (up to `MAX_AGGREGATE_PAGES = 100` pages / 10,000 rows
per query) and penny-exact group-to-total reconciliation per `S`/`O` indicator.

Per [OpenFEC methodology](https://github.com/fecgov/openFEC/blob/develop/webservices/docs.py),
these publisher aggregates omit memo items and 24/48-hour notices, group all
elections in a two-year cycle (election-specific requests refuse), and preserve
Form 5 (`C9xxxxxxx` -> `fec_form5_filer`), committee (`Cxxxxxxxx` ->
`fec_committee`), and `unresolved` filer namespaces separately.

## Official Senate results

`fec_election_workbook_rows` and `fec_election_results` are built separately by
`build-fec-election-results` (`spicy_regs.pipelines.fec_election_results`). See
its module docstring and tests for workbook/sheet edition scope.

## Repeatable acquisition, build, and GitHub Actions runbook

### Secret and archive boundaries

- **Never place `DATA_GOV_API_KEY` or `API_GOV` in local `.env`.** OpenFEC API
  acquisition runs on GitHub Actions via `.github/workflows/refresh-fec-research-profiles.yml`
  using repository secrets (`DATA_GOV_API_KEY` or `API_GOV`).
- **No paid routes or proxies.** Acquisition hits only official
  `api.open.fec.gov` (credential via `X-Api-Key` header only) and keyless
  `docquery.fec.gov`.
- **Default dry-run safety.** `skip_upload` defaults to `true`, writing to a
  scratch archive in `$RUNNER_TEMP`. Real publishes (`skip_upload=false`) unset
  `SPICYGOV_RAW_ROOT`, `R2_ALLOW_SHRINK`, and `R2_MIN_SIZE_RATIO` and re-read
  live `publication.v2.json` with an explicit `User-Agent`.

### GitHub Actions dispatch (recommended for live scaling)

#### Default request-budget math (OpenFEC 60 req/min, 1,000 req/hr quota)

With default `workflow_dispatch` inputs (`cycles=2020,2022,2024,2026`,
`committee_cycles=2024,2026`, `max_committee_pages=0`,
`outside_candidates_per_cycle=15`, `contribution_reports_per_cycle=6`,
`contribution_forms=house-senate,presidential,pac-party`, `max_requests=900`):

- **Phase 1 — Outside spending:** `4 cycles × 2 discovery pages = 8` + `60 candidate-cycles × ~2.5 pages ≈ 150` -> **~160 requests**.
- **Phase 2 — Contributions:** `4 cycles × 3 entity types × 2 discovery pages = 24` + qualified Form 3 / 3P / 3X reports (`~12–18 × 4 reqs ≈ 48–72`) + fast-stopped probes on unmapped pre-v8.5 `F3`/`F3P` buckets (`v8.3` has no reviewed cover mappings; `v8.4` covers `F3X` only; `v8.5` covers `F3`, `F3PN`, and `F3X` — after 2 consecutive unmapped-cover refusals in one `(cycle, entity_type)`, auto-discovery stops probing that bucket cleanly) -> **~90–120 requests**.
- **Phase 3 — Committees:** Complete `2024,2026` `/v1/committees/` traversal (`~250–350 pages`) -> **~300–350 requests**.
- **Total:** **~550–630 requests** (~11–13 min at `1.1 s/req`), leaving **~270–350 requests of headroom** under `max_requests=900` and safely below OpenFEC's 1,000 req/hr standard key quota and the 120-minute workflow timeout. To expand further (e.g. `committee_cycles=all` at ~900 pages), either raise `max_requests` within a single multi-window run or reuse retained committee pages via `--reuse-committees-from`.

1. Dry-run verification (`skip_upload=true`):
   ```sh
   gh workflow run refresh-fec-research-profiles.yml \
     -f mode=acquire \
     -f cycles=2020,2022,2024,2026 \
     -f committee_cycles=2024,2026 \
     -f max_committee_pages=0 \
     -f outside_candidates_per_cycle=15 \
     -f contribution_reports_per_cycle=6 \
     -f contribution_forms=house-senate,presidential,pac-party \
     -f max_requests=900 \
     -f skip_upload=true
   ```
2. Inspect the uploaded `refresh-fec-research-profiles-audit-*` artifact
   (`selection.json`, `summary.json`, `candidate-validation.json`) to review
   admitted vs skipped scopes and exact row counts.
3. Live publication (`skip_upload=false`):
   ```sh
   gh workflow run refresh-fec-research-profiles.yml \
     -f mode=acquire \
     -f cycles=2020,2022,2024,2026 \
     -f committee_cycles=2024,2026 \
     -f max_committee_pages=0 \
     -f outside_candidates_per_cycle=15 \
     -f contribution_reports_per_cycle=6 \
     -f contribution_forms=house-senate,presidential,pac-party \
     -f max_requests=900 \
     -f skip_upload=false
   ```

### Offline replay from retained originals

Both v1 (pilot) and v2 (multi-cycle) selections build strictly offline:

```sh
TMPDIR=/private/tmp uv run --frozen build-fec-research-profiles \
  --selection /work/selection.json \
  --output-dir /work/new-profiles
```

Pass `--read-index` (from `spicy-regs mirror`) to bind the prior published
generation, and use `publish-local-generation --family fec-research-profiles`
for guarded local dry-runs or publication.

## Example queries

```sql
SELECT source_cycle, contribution_family, count(*) AS reports,
       sum(included_records) AS included_rows, sum(memo_records) AS memo_rows
FROM fec_contribution_profiles
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT source_cycle, substr(candidate_id, 1, 1) AS office, support_oppose,
       count(*) AS candidates, sum(reported_spender_count) AS spender_links
FROM fec_outside_spending_profiles
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

SELECT designation, count(*) AS committee_occurrences
FROM fec_committee_relationship_profiles
GROUP BY 1
ORDER BY 2 DESC;
```
