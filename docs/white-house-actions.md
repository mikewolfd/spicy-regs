# Query retained White House action versions

The `white_house_action_versions` table exposes a retained White House article
version with its source URL, original HTML digest, source category, literal dates
and searchable body. The nominations/appointments reader adds White House source
versions absent from the selected official-document delivery. Related nomination
facts may already exist in GovInfo or other official records. Keep those records
under their existing identities; this table asserts no equivalent legal identity.

Acquire an explicit publication-date window through the central raw archive:

```sh
uv run python -m spicy_regs.pipelines.white_house acquire \
  --start 2026-11-01 --end 2026-11-13 --max-pages 4 \
  --output-dir /absolute/work/acquisition \
  --captures-out /absolute/work/captures.json
uv run python -m spicy_regs.pipelines.white_house build \
  --captures /absolute/work/captures.json --output-dir /absolute/work/build
```

Read retained originals before acquiring more. `build` performs no network
requests and renews each original's size and SHA-256. It reconciles current
article URLs against the captured index window, then produces an ordinary
source-evidenced generation. Refused, missing, corrupt or structurally changed
inputs produce no candidate generation. A successful empty window produces an
explicit empty table. The ordinary publication path accepts the generation;
building it alone does not publish it or select the shared catalog.

Refresh the same window with `acquire --prior-captures /absolute/work/captures.json`.
Unchanged exact originals retain the earlier observation. Changed HTML remains a
separate source version, including changes outside the article body. Previous
versions remain in `prior_actions` in the resulting selection. An index whose
continuation has not reached the date boundary refuses at the page cap. This is
an observed window, not historical completeness or an automatic freshness claim.

`build --existing-records /absolute/work/existing.json` accepts a JSON list of
proven representation identities from `federal-register`, `govinfo` or
`administration-policy`. Each needs `source_url`, `source_sha256`, `document_id`
and `route`. An exact URL-and-HTML-digest match reuses that existing route and
appears in `route-decisions.json` instead of another output row. Titles alone
and ambiguous mappings refuse. A different digest keeps the source version;
matching nominee names never establishes equivalent documents. Every emitted
row's `official_match_status` is always `unresolved`.

For example, an ordinary `query_sql` consumer can answer which retained White
House announcement withdrew David MacNeil's nomination:

```sql
SELECT title, document_type, published_time, modified_time,
       source_url, source_sha256, body_text
FROM white_house_action_versions
WHERE body_text ILIKE '%WITHDRAWALS SENT TO THE SENATE:%'
  AND body_text ILIKE '%David MacNeil%';
```

The original November 9, 2026 article places his entry under its withdrawals
heading. These are article versions rather than person-level nomination rows;
the query does not infer a person's status from a name elsewhere in a document.
Use `listing_url`, `listing_sha256` and `listing_record_index` to locate the
retained listing occurrence. `body_selector` locates the article body in the
original HTML; `body_html` is parser serialization rather than exact source bytes.
