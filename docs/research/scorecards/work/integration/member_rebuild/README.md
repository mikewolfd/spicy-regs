# Local member-family qualification

The existing `MembersRollup` produced a verified local generation with
`votesmart_id`, `other_names_json`, `bioguide_previous_json`, historical terms,
and nested party-affiliation occurrences. This is an installed-provider rebuild,
not a second congressional ingestion path. Nothing was published.

See [qualification.json](qualification.json) for measured coverage, exact table
and evidence pins, source observation times, and limitations. Private artifacts
are under
`/Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/scorecards-members-2026-10-03/`.
`analysis_inputs.json` there provides immutable table paths and pins for local
scorecard resolution. The generation ends in
`253ae5371141f436d7787608c7da3258b1a05a64b93c26822f227fe3754b251e`.

The existing reader fetched the original current and historical community JSON
routes once each. No previously retained JSON pair was established as a current
complete pair for this run. Acquisition and use receipts share each response's
hash and observation time; they are not additional HTTP requests. The existing
`full` evidence policy retains both complete bodies in an independently verified
artifact. Completeness means bounded HTTP EOF, a complete JSON array, and exact
reconciliation of source members, terms, and nested affiliations. The publisher
does not declare a separate total or a repository commit for these JSON routes.

`qualify.py` independently decodes the retained raw JSON rather than calling the
provider's parser or row shapers again. It compares source membership, IDs,
source names, literal dates, JSON absence/null distinctions, ordered histories,
affiliation objects, ordinals, and pointers with every output row. It verifies
both immutable artifacts, table schemas, primary-key uniqueness, and foreign
keys. Missing optional IDs remain NULL. No identifiers or dates are inferred.

Direct raw-byte readback also checked these source examples against Parquet:

| Source location | Checked fact |
| --- | --- |
| Current `/0/id/votesmart` | Maria Cantwell's numeric source ID `27122` is stored as text. |
| Historical `/3166/other_names` and `/3166/id/bioguide_previous` | David Yulee's `Levy` patch retains `middle: null`, end `1846-01-12`, and previous ID `L000266`. |
| Historical `/11046/other_names` | Jill Long Thompson's `Long` patch retains end `1995-09-03`. |
| Historical `/10269/terms/4/party_affiliations/0` | The Republican affiliation from `1971-01-21` to `1972-03-22` stays separate from the term-level Democrat assertion. |

Raw readback confirms three previous/current Bioguide collisions already
documented by the earlier source survey: Foot/Fallon (`F000246`), Greene
Waldholtz/Warnock (`W000790`), and Yulee/LaTurner (`L000266`). These remain distinct
source records and exact-resolution candidates; historical context can exclude
inapplicable terms. This is not a duplicate-primary-key finding or source drift.
Private `identity_diagnostics.json` lists missing IDs and all measured collisions.

To requalify the retained data without network reads, run from `spicy-regs`:

```sh
uv run --frozen --no-sync python docs/research/scorecards/work/integration/member_rebuild/qualify.py \
  --output-dir /Users/mikewolfd/Work/corpora/supply-2026-09-02/receipts/scorecards-members-2026-10-03 \
  --report-copy docs/research/scorecards/work/integration/member_rebuild/qualification.json
```

`--build` runs the existing rollup first and requires a new private directory,
the exact installed provider version, and an unset `R2_PUBLIC_URL`. It always
sets `skip_upload=True`. Requalification reads retained bytes only.
