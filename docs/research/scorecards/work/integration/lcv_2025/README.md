# LCV 2025 local qualification

The installed SpicyDocs reader completed the original publisher edition, and SpicyRegs built, replaced, resolved, locally published, audited and queried it. [Source qualification](source_qualification.json) and [integration qualification](integration_qualification.json) contain exact artifact pins, receipt locations and measured results. Production enablement, remote publication and scheduling remain separate work.

The captured source contains 551 members, 1,102 literal rating cells, 66 items and 17,255 item results. Independent Python CSV/HTML readback reconciled every member, rating and item-result cell against the candidate Parquet. Both chamber catalogs and every item table agree. The publisher does not declare an independent aggregate count; completeness rests on the complete CSV/HTML catalog and cell reconciliation. Three blank annual cells and six `na` cells remain distinct. CSV scores such as Katie Britt's `0` and `2` stay literal; HTML `0%` and `2%` corroborate membership rather than replace the selected rating rendition.

Exact links resolve 489 members and all 66 items. The remaining 62 members retain their source fields, candidates and reasons. A prefixed-district diagnostic does not establish the cause of an unresolved result: the captured 2025 export includes Alan Armstrong and Analilia Mejia, whose pinned official terms begin in 2026. No fuzzy matches or overrides were applied. See the private `unresolved.private.json` named by the integration receipt.

Each item preserves its own CSV `Year`. The resolver checks that literal against actual official vote dates and canonical vote identifiers. Zeldin's item resolves to `119-senate-1-24`, whose retained official date is January 29, 2025. It does not infer a session from an edition year. The [independent resolver review](resolver_review.json) records a contradictory-year finding and its tested correction.

Original bodies remain in the private corpus. Public evidence contains hashes and metadata under `hash_only`, including failed-attempt journals; failed evidence cannot qualify a generation. Actual-corpus truncation, absent detail, HTTP 404 and layout drift preserve prior rows. A complete replacement advances the entire source family in the local object-store simulation; old pinned bytes remain unchanged, and a stale writer cannot move the pointer. Both source and analysis audits report zero failures and review findings. Generic MCP discovery, description and attributed SQL queries expose immutable local pins and original publisher links.

The selected bills match the retained October 3 publication index. Roll calls and amendments use explicitly selected, verified older local generations; members and historical terms use the separately qualified local rebuild. This is an exact-input qualification, not a claim of latest remote coverage. No congressional data was reacquired by these scripts. Methodology rows contain only the current annual/lifetime calculation paragraphs, marked partial; historical methodology effectiveness and score reproduction are not established.

## Reproduce

Run from `spicy-regs` with the installed pinned provider. The scripts reject changed retained bytes and use new output directories; they perform no network acquisition or remote writes. `select_official_inputs.py --help` lists explicit baseline, member receipt, bill file, roll generation and amendment generation arguments. Its retained output pins every selected input.

```sh
uv run --frozen --no-sync python docs/research/scorecards/work/integration/lcv_2025/qualify_lcv.py \
  --retained-dir /path/to/original-lcv-captures \
  --retained-dir /path/to/lcv-methodology-capture \
  --output-dir /path/to/new-private-source-run
uv run --frozen --no-sync python docs/research/scorecards/work/integration/lcv_2025/readback_sources.py \
  --retained-dir /path/to/original-lcv-captures \
  --retained-dir /path/to/lcv-methodology-capture \
  --source-report /path/to/new-private-source-run/qualification.json \
  --output /path/to/private-raw-readback.json
uv run --frozen --no-sync python docs/research/scorecards/work/integration/lcv_2025/integrate_lcv.py \
  --source-report /path/to/new-private-source-run/qualification.json \
  --official-inputs /path/to/verified-official-inputs.json \
  --output /path/to/new-private-integration-run
uv run --frozen --no-sync python docs/research/scorecards/work/integration/lcv_2025/check_preservation.py \
  --source-report /path/to/new-private-source-run/qualification.json \
  --integration-dir /path/to/new-private-integration-run \
  --output /path/to/private-preservation-receipt.json
```

The integration harness uses the repository's existing in-memory S3 test double and the real publication functions. Its isolated local registry enables LCV only inside the private qualification directory; the production registry stays disabled. The supplemental source-layout test now actually changes CSV chamber-group order, preserving semantic identifiers while locators change. This test-only improvement follows the packaged archive; runtime provider files are unchanged.
