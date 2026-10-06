# Column navigation architecture review — 2026-10-06

The review approves the reconciled integration. Two concerns were resolved:
the duplicate separate-publication reader was removed from the release change,
and category browsing now rejects structured fields and ambiguous Congress scopes.
The comments-to-dockets link keeps its measured missing destinations explicit.

## 1. Artifact summary

**Problem:** Readers could not follow several populated reference fields or
browse related tables by categories such as Congress. Sources also exposed
internal coverage labels without consistently explaining their meaning.

**Decision:** Add measured scalar links to the existing backend registry,
render self-links in both directions, and offer separately reviewed category
filters in the website. Keep source labels as display-only wording. Use the
current metadata publisher rather than introducing another publication reader.

**Category:** Product surface, supported by measurement evidence and CI checks.
The backend owns record relationships; the website owns browsing controls and
display labels. See `src/spicy_regs/table_joins.py:934-975`,
`spicygov/lib/catalog.ts:215-240`, `spicygov/lib/shared-browse.ts:7-59`, and
`spicygov/lib/source-labels.ts:6-46`.

Reviewed inputs: backend main `59214e5702dccef5b0f17e845ee7eaf65c145d7b`,
website main `b7e74a495dd0a97fc5ee915b16836fbf641a41b0`, and the local integration.
The workspace's `AGENTS.md` states the purpose: searchable applications backed
by public legislative and regulatory evidence. Neither repository has a
`GOAL.md`, `DEVELOPMENT-PHILOSOPHY.md`, or task-specific ratified ADR. The named
seams are documented in `docs/explorer-metadata.md` and the website README;
this review does not invent additional ratification requirements.

## 2. Lineage and relationships

| Prior artifact | Type | Relation | Citation |
| --- | --- | --- | --- |
| Canonical native join registry | Parent implementation | Preserves native key conversion and retired processing links | Commit `ddd1f4514664192321a02694d1ac57102085f140`; `src/spicy_regs/table_joins.py:996-1045` |
| Separate-publication metadata | Sibling implementation | Subsumes the locally proposed extra reader | Commit `29399306`; `src/spicy_regs/explorer_publications.py:95-148` |
| Public metadata rules | Parent documentation | Requires compatible schemas, exact separate identities, and explicit unavailable links | `docs/explorer-metadata.md`, Contents and ownership; Consumer safety |
| Earlier map visibility | Parent implementation | Keeps older measurements visible with their limitations | Website commit `b7e74a495dd0a97fc5ee915b16836fbf641a41b0` |
| Website data and coverage rules | Parent documentation | Distinguishes record identity, browsing filters, and measured coverage | `spicygov/README.md`, Data and joins; Coverage maps |

| Component or seam | Owner | Depends on | Used by | Named seam |
| --- | --- | --- | --- | --- |
| Canonical record joins | Backend | Schema declarations and measured keys | MCP and public metadata | `docs/explorer-metadata.md`, Contents and ownership |
| Separate file identity | Backend publisher | Validated receipts and exact Parquet footers | Website catalog | `docs/explorer-metadata.md`, Consumer safety |
| Metadata application | Website | Current publication descriptors and schemas | Records and Connections | `spicygov/README.md`, Data and joins; `spicygov/lib/catalog.ts:128-204` |
| Category browsing | Website | Reviewed scopes and compatible scalar fields | Normal Records filters | `spicygov/README.md`, Data and joins; `spicygov/lib/shared-browse.ts:7-59` |
| Coverage label display | Website | Recorded values and measurement labels | Sources coverage controls | `spicygov/README.md`, Coverage maps; `spicygov/lib/source-labels.ts:6-46` |

Exploration hypotheses and observations:

- **H1 confirmed:** Record relationships retain one owner. The website creates
  reverse views of self-links, not duplicate backend declarations. Evidence:
  `src/spicy_regs/table_joins.py:934-975`; `spicygov/lib/catalog.ts:215-240`.
- **H2 confirmed:** Current main already supplies receipt-bound separate-table
  metadata and exact schema reuse. Reusing it removes the proposed second reader.
  Evidence: commit `29399306`; `src/spicy_regs/explorer_publications.py:117-148`.
- **H3 confirmed:** The added declarations use complete keys and unique parents.
  The retained receipt supplies full-key results, and a fresh changed-join run
  passed all added declarations. Evidence:
  `docs/evidence/column-navigation-2026-10-06.json`, results;
  `scripts/check_table_joins.py:98-138`.
- **H4 refined and fixed:** A reviewed table name alone does not make a future
  structured column safe for category browsing. Scalar type qualification and
  one-Congress context are now required. Evidence:
  `spicygov/lib/shared-browse.ts:7-21,51-59`;
  `spicygov/tests/shared-browse.test.mjs`, structured category test.

## 3. Invariants and commitments

1. **One record-join registry — PRESERVED.** Stated in
   `docs/explorer-metadata.md`, Contents and ownership. Declarations and generated
   JSON remain paired; MCP reads that packaged JSON (`src/spicy_regs/mcp_server.py:1519-1555`).
   Violation would give browser and MCP users different relationship meanings.

2. **Complete identities — PRESERVED.** Stated in the website README, Data and
   joins. Vote links retain Congress, chamber, session, and roll number together
   (`src/spicy_regs/table_joins.py:960-962`). Missing, blank, or structured record
   keys cannot route (`spicygov/lib/catalog.ts:226-230`). Violation would navigate
   to unrelated votes or turn compound values into fabricated identifiers.

3. **Categories do not establish entity identity — PRESERVED.** Stated in
   `docs/explorer-metadata.md`, Contents and ownership. Congress, chamber, and
   other reviewed axes become ordinary exact filters, with source spellings
   retained as alternatives (`spicygov/lib/shared-browse.ts:22-48`). Violation
   would imply that equal Congress or year values identify one record.

4. **Publication compatibility — RELIED UPON.** Stated in
   `docs/explorer-metadata.md`, Consumer safety. Separate files require the exact
   descriptor and identity; current schemas qualify actionable links
   (`spicygov/lib/separate-publications.ts:20-32`; `spicygov/lib/catalog.ts:137-163`).
   Violation would apply stale descriptions or keys to changed data.

5. **Display changes preserve evidence — PRESERVED.** Stated in the website
   README, Coverage maps. Readable labels retain original values and unknown
   codes (`spicygov/lib/source-labels.ts:15-46`). Measurement definitions and
   scanner inputs are unchanged. Violation would hide uncertainty or invalidate
   costly measurements merely because wording changed.

6. **Exact-main CI before metadata publication — RELIED UPON.** Stated in
   `docs/explorer-metadata.md`, Refresh and deployment. The workflow selects a
   successful push-CI revision and checks out that revision
   (`.github/workflows/publish-explorer-metadata.yml:93-126`). A merge alone does
   not establish that metadata or the running MCP process has refreshed.

7. **Declared uncertainty stays visible — PRESERVED.** Stated in
   `docs/explorer-metadata.md`, Contents and ownership. The comments-to-dockets
   relationship remains `scope`, with its missing destinations stated
   (`src/spicy_regs/table_joins.py:969-971`). Native keys absent from a physical
   publication remain unavailable (`src/spicy_regs/explorer_metadata.py:173-198`).

## 4. User-value analysis

Readers can move from an amendment to its target bill or amendment, inspect
committee hierarchy, follow a fully identified vote, and browse tables using a
shared category. These are direct improvements to the data surface rather than
marketing or operational scaffolding. See the website README, Data and joins,
and `src/spicy_regs/table_joins.py:934-975`.

The design adds a small website-owned category policy. It pays down duplicate
publication-reader maintenance by retaining current main's implementation.
The category policy has a different purpose from the join registry: it defines
which scopes may be browsed together, not record equivalence.

Smaller shapes considered after tracing the current design:

- Matching names automatically is smaller code but loses namespace and
  session-within-Congress safeguards. Reject it.
- Treating categories as record joins would reuse one registry but assert the
  wrong identity. Reject it.
- Shipping another footer reader duplicates current main's validated reader.
  Remove it; reuse the maintained reader and cache.

The value claim is falsifiable: if people still need to copy reference IDs by
hand, if category links repeatedly misapply namespaces, or if either consumer
silently uses stale schemas, the integration has failed its stated purpose.

## 5. Counterfactual analysis

- **Kill criterion:** A reproducible wrong-record traversal, an incomplete vote
  identity, or stale separate-file metadata activating navigation invalidates
  the affected relationship. Requalification must precede restoration.
- **Opposite decision:** Automatic joins on all equal column names would
  simplify authoring but mix agency, publisher, cycle, and Congress namespaces.
  The explicit policy prevents that (`spicygov/lib/shared-browse.ts:3-20`).
- **Removal probe:** Removing the added canonical declarations removes the new
  record links from both consumers. Removing category browsing leaves records
  readable but forces manual filters. Removing label mappings exposes raw values
  without changing measurements (`spicygov/lib/source-labels.ts:6-46`).
- **Sibling subsumption:** The separate-publication reader is fully subsumed
  by commit `29399306`; this release adds no second implementation.
- **Six-month critic:** Check whether category policy still matches changed
  field types and meanings, whether measured missing destinations changed, and
  whether exact publication binding remains enforced. Tests cover structured
  types, ambiguous Congress scopes, URL alternatives, and self-link direction.

## 6. Findings

### F1 — CONCERN, sibling conflict — RESOLVED

The captured local change implemented separate-publication reading already
present on current main. Evidence: captured local commit `98bfe786` versus
commit `29399306` and `src/spicy_regs/explorer_publications.py:95-148`.
**RESHAPE:** retain the maintained reader and exact schema cache; remove the
duplicate reader and its source-reader dependency expansion from this release.
The reconciled change does so.

### F2 — CONCERN, conformance — RESOLVED

Category qualification originally checked field presence without excluding
structured types; session context could select one Congress from a multi-Congress
filter. Evidence: `spicygov/lib/shared-browse.ts:7-21,51-59` and the regression
test in `spicygov/tests/shared-browse.test.mjs`.
**RESHAPE:** qualify scalar category types and require one normalized Congress
for session browsing. Both guards are implemented.

### F3 — OBSERVATION, user value — KEEP

The comments-to-dockets link has a measured exception, not complete resolution:
28 of 60,296 distinct selected docket keys have no target. Evidence:
`docs/evidence/column-navigation-2026-10-06.json`, comments-index docket result.
**KEEP:** expose the useful navigation and its `scope` limitation; do not rename
the relationship complete or infer publisher completeness from the match rate.

### F4 — OBSERVATION, operational boundary — KEEP

MCP caches the packaged registry once per process
(`src/spicy_regs/mcp_server.py:1519-1522`). Metadata publication and website
deployment have their own workflows. **KEEP:** report commit/merge, CI,
publication, and running service state separately. No merge claim should imply
an MCP restart or successful deployment.

## 7. Verdict

**VERDICT: APPROVE** the reconciled integration.

- Intent versus shape: **MATCHES** — scalar identity links and category filters
  remain distinct, with visible source meanings.
- User value: **SUPPORTED** — navigable references and shared scopes serve
  readers directly.
- Commitments: **HONORED** — canonical ownership, full keys, uncertainty, and
  publication bindings remain intact.
- Conceptual debt: **PAYS DOWN** duplicate publication logic; the small category
  policy adds explicit meaning rather than a second record-join registry.
- Sibling subsumption: **PARTIAL** for the original proposal, fully removed from
  the final publication-reader change.
- Confidence: **HIGH** for this scoped architecture. Full hosted CI and deployment
  remain separate release observations.

Validation: current-main backend tests, metadata/publication tests, full-key
live checks, dictionary check and regeneration, Ruff, type checking, website
tests and production build. The live checks retain the documented docket
exceptions and found no duplicate parent keys among the added relationships.
