# Join delivery execution — 2026-09-27

Parallel work is implemented, pinned and deployed. The live server now exposes source-preserving relationship views and bounded citation target resolution; members, House communications, Federal Register links, partial FCC enrichment, GAO metadata and bounded citation additions are published and verified through public bytes and MCP. **The full proposal is not complete.** Remaining source qualification, wider adoption and backfills are listed per task below.

This ledger follows the [proposal](join-delivery-tasks-2026-09-27.md). The [machine-readable ledger](join-delivery-execution-2026-09-27.json) retains exact code paths, tests, publication pins, measurements and remaining acceptance. Paths in the task ledger are relative to the workspace root. SpicyDocs owns source behavior; SpicyRegs owns application behavior.

## Release and evidence boundary

| Boundary | Verified state |
| --- | --- |
| Provider | SpicyDocs 0.48.0 from `983463c`; two byte-identical wheel builds, vendored and tested in SpicyRegs. No registry upload. |
| Repository checks | Provider gate: 10,366 passed. Final consumer lint, types, dictionary and full suite: 3,307 passed. Logs: `provider-wave2-gate.log`, `consumer-wave2-gate-final.log`, `cloudflare-wave2-check.log`. |
| First corrective deployment | App `8380ac8`; Worker `ea8d54f5-a2f4-4871-b9b0-016ef595b518`; image `1d558a710949494a7b43e39be5027124f07259754e8ccb9ffd3ecd401020efb5`. New rule and metadata verified through the public endpoint. |
| Latest deployment | App `8082191`; Worker `1146ddba-bf85-4a71-8dd9-e87eaf8bf16f`; image `dac8a5518fc7aaeb40a6642e8dd6e53adfc62e9e065881ee1b548fbfca90c014`. All 14 bounded public checks passed; `wave2-smoke-summary.json`. |
| Members publication | Generation `00cad6cf…`: 59 affiliation occurrences; all prior member/term identities and native values preserved. All three public table hashes/counts match. |
| Communications publication | Generation `94d28167…`: 2,076 RIN occurrences on 2,046 rows; all 5,006 prior rows and 32 prior columns preserved. Public hash/counts match. |
| Comments | Nullable catalog migration and three-record read-only repair preview verified. Catalog row corrections and public export remain pending. |
| Git | Provider and application changes committed locally. No Git push or package-registry release is claimed. |

The serving endpoint is [the fork MCP server](https://spicy-regs-mcp.mdeeb.workers.dev/mcp). It reads selected public Parquet publications. Iceberg remains the ingestion/replay version boundary; this release does not migrate serving to Iceberg or claim an atomic snapshot across independently selected tables.

Primary execution evidence: `/Users/mikewolfd/.codex/artifacts/spicy-regs-join-implementation-20260927/`. The initial source review remains under `spicy-regs-join-tasks-20260927/`; members, communications, GAO, FEC benchmark and array reconciliations have separate evidence roots recorded in the JSON ledger. These local receipts are evidence paths, not public release links.

## Verified live behavior

The first release discovery measured 190 tables/views: 98 derived views available, four comment views unsupported against the older served schema, and none unavailable. Bounded reads passed for vote documents, related bills, member FEC identifiers, FR RINs, court endpoints, diff endpoints and pending identity candidates. The citation tool returned 20 found targets in an explicitly capped, partial selection. Local-file access and write attempts were rejected.

The first live FEC query exhausted 8.3 GiB. That failing receipt is retained in `fec-evidence-live.json`. The corrected view groups only referenced companion keys, keeps compact candidate digests and uses hash joins. Full companion evidence remains accessible through `fec_source_records` by collection/record identity. The same selected-input query completed in 6.67 seconds at a 512 MB local memory limit and in **3.864 seconds through the corrected public endpoint**. `final-smoke-summary.json` records all final checks passing. This is a measured query, not a general latency guarantee; the final cold description request took 33.089 seconds.

Container shutdown now executes Uvicorn directly so it receives stop signals. The local container proof completed graceful shutdown in 0.27 seconds. The deployment CLI confirms the rollout started, and live rule `fec-companion-location-v2` confirms the new runtime served the checks. Fleet-wide per-instance completion is not inferred from the CLI response.

Independent data checks under `spicy-regs-live-members-rins-20260927/` confirmed 59 affiliations and 2,076 communication RIN occurrences. Communication `119-ec-4554` returns `2125-AF80`, `2130-AD05` and `2132-AB51` with exact retained spans and field digest; target existence stays `not_checked`. Vote `119-house-1-1`, member `K000401`, on `2025-01-03` returns one Republican source interval. These are selected source assertions, not official roster completeness or complete historical coverage.

A separate retained-byte check verified FEC committee `C00000059` end to end: the complete 123,668-byte captured page matches its recorded SHA-256; `/results/0` matches companion metadata and its sponsor list is `[]`. The digest describes the complete page, not normalized record JSON. This qualifies one selected case; the live navigation tool still reports that it has not itself read or hashed raw source bytes. Receipt: `spicy-regs-fec-evidence-20260927/receipt.json`.

The earlier citation coverage report was also reproduced for all eight audited citation kinds over complete selected key columns with the source-text digest predicate. Exact SQL and pins are in `spicy-regs-join-delivery-t01-t02-20260927/citation-coverage.json`. This dated lookup measurement is distinct from the final capped live query and from extraction recall.

## Second parallel release

The final public check discovers 194 available tables/views, including 102 available derived views and four comment views unsupported against the older export. All fourteen bounded checks pass. FCC preserves 525 native-enriched rows among 5,780 total; 702 of 708 native proceeding occurrences find exact targets and six remain unsupported. FR candidates, GAO metadata/citation join, held-field source digest checks, unstated-Congress abstention, lifecycle routing and file/write refusals pass. Initial rollout verification hit the previous container; the final cold runtime check took 34.333 seconds. No fleet-wide completion or general latency claim follows from these checks.

Public readback verifies the full Federal Register link refresh, partial FCC native enrichment, one GAO target addition, and bounded held-field citation additions. All preserve prior rows/cells; the citation writer also preserves every sibling table byte. Exact pins and receipts are in `finalization.second_wave_publications` in the JSON ledger.

Native legal-reference pipelines now produce verified local candidates. Their retained fragments lack verified enclosing publisher editions, so they remain unpublished. The Senate parser likewise produces partial candidates, not qualified payment totals. Identity decisions and accepted agency lookups are available through local application APIs; no new identity decision was made. Four lineage-qualified aggregate comparisons pass; comments/lifecycle comparisons abstain until their different publication paths can supply equivalent pins.

## Full held-array reconciliation

Every selected source array-length total equals its occurrence-view count. Exact SQL, input pins, bytes and measurements are retained under `/Users/mikewolfd/.codex/artifacts/spicy-regs-array-population-20260927/`.

| Held array | Occurrences | Unsupported elements | Distinct source/target pairs |
| --- | ---: | ---: | ---: |
| Related bills | 142,698 | 0 | 142,698 |
| Member FEC IDs | 1,738 | 0 | 1,738 |
| Vote documents | 9,067 | 3,550 | 5,517 |
| Vote amendments | 9,067 | 0 | 0 |
| Federal Register RINs | 121,795 | 236 | 121,559 |
| Federal Register dockets | 899,632 | 2 | 899,630 |

All amendment objects remain deliberately unrouted. Their valid object shape does not qualify a target. The related-bill source also retains 248,469 SQL NULL arrays, separate from empty arrays. These checks prove preservation of held inputs; they do not establish publisher completeness, target existence or extraction recall.

## Comment repair boundary

The live catalog schema advanced from 0 to 4 without changing data snapshot `3490173674155493736`. A historical snapshot read still uses the earlier columns. Repair now checks current write-schema readiness separately and treats the four historically absent fields as unread NULL values. Export includes these nullable fields instead of silently omitting them.

Three retained native ODNI comments replayed in a read-only preview: only the four intended reference cells change per record; null dockets and all other cells remain unchanged. Both explicitly named parent documents exist once in documents generation `b5e567d1…`. Receipt: `comment-repair-validation.json`. No catalog data write or public export occurred. Applying the repair requires a qualified replacement path because the existing catalog `DELETE`/`INSERT` limitation is documented in `iceberg._merge`; no destructive whole-table rebuild was performed.

## Task ledger

### T01 — Correct join keys and publish measured cardinality

**Implemented:** Full-key references and measured cardinality. Full selected key columns measured at independent publication pins on 2026-09-27; version, report-part, hearing and both attribute joins report OK. Synthetic collisions cover full-key separation.

**Code:** `spicy-regs/src/spicy_regs/table_joins.py`; `spicy-regs/scripts/check_table_joins.py`; `spicy-regs/src/spicy_regs/join_measurements.json`; `spicy-docs/src/spicy_docs/schemas/bill_version_tables.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`; `spicy-docs/src/spicy_docs/schemas/committee_report_tables.py`.

**Validation:** `spicy-regs/tests/test_join_cardinality.py`; `spicy-regs/tests/test_table_joins.py`.

**Remaining:** None for the selected full-key delivery. Measurements are tied to independent table pins; broader source completeness and an atomic cross-table snapshot are not claimed.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Generated join metadata and serving code deployed in app 8380ac8; complete key-column measurements retained in join_measurements.json.

### T02 — Resolve citation targets against selected publications

**Implemented:** Bounded target resolver beside source findings. Found/missing/ambiguous/unsupported/unread states, target pins, dated FR routes, alternate identities, set-valued RIN matches, deduplicated batches, timeout outcomes and source-text digest qualification are tested. Earlier eight-kind target-coverage report reproduced over complete selected key columns with the source-text digest predicate; receipt citation-coverage.json captured 2026-09-27T20:22:35Z. Counts are scoped to its exact pins, not subsequent publications.

**Code:** `spicy-regs/src/spicy_regs/citation_resolution.py`; `spicy-regs/src/spicy_regs/mcp_server.py`.

**Validation:** `spicy-regs/tests/test_citation_resolution.py`; `spicy-regs/tests/test_mcp_relationships.py`.

**Remaining:** The selected eight-kind coverage replay is complete. Wider family/source interpretation qualification remains in T17/T24; published lookup coverage must be recomputed when selected inputs change.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T03 — Recover source-stated comment-to-document links

**Implemented:** Native comment fields, views and explicit repair. Native null-docket fixture preserves all three namespaces and absent/null/empty map states. Local migration/export tests retain old rows as unread; repair handles equal timestamps, newer priors, enrichment, pinned snapshots and read-only migration previews. Live fork Iceberg nullable-column migration verified schema 0 to 4 at unchanged data snapshot 3490173674155493736; no comment replay/export yet. Three retained native ODNI records previewed at the migrated schema/current data snapshot. All twelve new cells match source values, null dockets and other fields are preserved. Both explicit parent documents have one target under documents generation b5e567d1. Historical-schema reads now NULL-fill supported fields independently of current write-schema readiness; exporter regression is covered.

**Code:** `spicy-docs/src/spicy_docs/schemas/regulations.py`; `spicy-regs/src/spicy_regs/schemas/regulations.py`; `spicy-regs/src/spicy_regs/relationship_views/comments.py`; `spicy-regs/src/spicy_regs/sources/iceberg.py`; `spicy-regs/src/spicy_regs/pipelines/repair_regulations.py`.

**Validation:** `spicy-regs/tests/test_comment_references.py`; `spicy-regs/tests/test_regulatory_source_repair.py`.

**Remaining:** Apply corrections through a qualified catalog replacement path and publish corrected exports. Existing DELETE+INSERT reliability limitation remains documented; no destructive catalog rebuild was attempted. Wider retained replay remains.

**Delivery:** Live schema migration and bounded read-only source replay verified; catalog row corrections/public export pending. Comment views correctly unsupported on old served export; comment-schema-live.json verifies this limitation.

### T04 — Expose publisher-listed related bills

**Implemented:** Related-bill occurrences and distinct directed pairs. Duplicate occurrences, complete relationship-details objects, malformed elements and directional pair semantics are covered. Complete selected column replay reconciles 142698 held occurrences and distinct pairs against source arrays, preserving 248469 SQL NULL arrays separately from empty arrays.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`; `spicy-regs/src/spicy_regs/relationship_views/core.py`.

**Validation:** `spicy-regs/tests/test_relationship_views.py`.

**Remaining:** Held-array conservation and distinct-pair counts are verified at bill-family 72899ab3; source completeness and related-target existence are not established by these measurements.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T05 — Preserve and publish dated cosponsorship

**Implemented:** Source cosponsors and correction-safe output. Retained 49-entry block replayed; literal dates, original flag, party/state/district, raw XML and digest/ordinal retained. Absent/empty correction clears prior scope; v3 archive and bill checks reopen old readers and missing outputs.

**Code:** `spicy-docs/src/spicy_docs/sources/congress/bill_status.py`; `spicy-docs/src/spicy_docs/schemas/bill_tables.py`; `spicy-docs/src/spicy_docs/interpretation/bill_family.py`; `spicy-regs/src/spicy_regs/transforms/build_bill_family.py`.

**Validation:** `spicy-docs/tests/test_source_relationship_rows.py`; `spicy-regs/tests/test_bill_family.py`.

**Remaining:** Cosponsor data backfill/publication, retained positive withdrawal-date specimen and published count reconciliation remain required. Provider 0.47.0 is pinned and tested.

**Delivery:** Provider 0.47.0 adopted; bill_cosponsors source backfill/publication pending. Current serving implementation deployed; task-specific population/live acceptance not established.

### T06 — Expose meeting, hearing and publication references

**Implemented:** Independent meeting/hearing occurrence views. Retained meeting fixture checks jackets, witnesses and offered documents independently; full Congress/chamber/event keys, duplicate values and meeting status survive.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_retained.py`; `spicy-regs/tests/test_relationship_views.py`.

**Remaining:** Population reconciliation and jacket target checks. Earlier shapers lost source absent/null distinctions; held-array states cannot recover them.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Current serving implementation deployed; task-specific population/live acceptance not established.

### T07 — Expose vote document references without pairing unrelated lists

**Implemented:** Independent vote document/amendment observations. Retained Senate vote keeps all native document blocks alongside empty-ID amendment blocks; PN55-25 suffix and native Congress survive. No positional zip or current-Congress fallback. Complete held arrays reconcile 9067 document observations (3550 unsupported) and 9067 independently retained, deliberately unrouted amendment objects.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_retained.py`; `spicy-regs/tests/test_relationship_views.py`.

**Remaining:** Positive native amendment/treaty specimens before their routing; full missing-nomination coverage remains. Bounded live vote-document query succeeded.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T08 — Connect members to source-listed FEC candidate IDs

**Implemented:** Member FEC identifier occurrence/pair views. Candidate-ID shape including presidential IDs, source list order, duplicates, roster and crosswalk context tested. Complete member FEC array replay reconciles 1738 valid occurrences and pairs at members 00cad6cf.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

**Validation:** `spicy-regs/tests/test_relationship_views.py`.

**Remaining:** Pinned crosswalk conflicts and complete candidate/committee/cycle reconciliation remain. Bounded live member-FEC query succeeded; historical linkage is not authorization or a donation.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T09 — Make FEC relationship evidence directly inspectable

**Implemented:** FEC companion evidence navigation. Companion multiplicity, recorded digest equality, unresolved records and explicit empty-list observations tested without multiplying observations. Corrected public query completes in 3.864 seconds; same selected input benchmark completes at 512 MB in 6.67 seconds. Compact digest candidates preserve duplicates/nulls; full companion records remain accessible by collection_id/source_record_id. One end-to-end retained-page verification passed for C00000059: full 123,668-byte page digest matches observation/companion, /results/0 equals companion metadata, and sponsor_candidate_list is an empty list. The digest names whole page bytes, not normalized record JSON.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/fec.py`; `spicy-regs/src/spicy_regs/transforms/fec_relationships.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Wider selected companion cases and relationship kinds need byte-level qualification. One empty-list case is verified against retained page bytes; serving source_bytes_status remains not_checked because runtime navigation itself does not hash raw captures.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Passed on Worker ea8d54f5-a2f4-4871-b9b0-016ef595b518: found companion, rule fec-companion-location-v2, 3.864s. Initial failing receipt remains retained.

### T10 — Expose FCC filing membership and retain native role detail

**Implemented:** Native FCC fields and independent proceeding, participant and artifact observations. All 5,780 prior rows/18 columns preserved; 525 exact old-cell matches enriched from one verified retained page. Source states distinguish empty/absent/unread. 708 proceeding, 701 participant and 238 artifact observations conserve selected arrays.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/fcc_native.py`; `spicy-regs/src/spicy_regs/transforms/build_fcc_ecfs.py`.

**Validation:** `spicy-regs/tests/test_relationship_views.py`; `spicy-regs/tests/test_fcc_native.py`.

**Remaining:** Replay the remaining 5,255 unread rows and qualify full proceeding target coverage; no attachment body acquisition.

**Delivery:** Published with full public byte readback: sha256:34b970d51a425d8f57c5565f8ec9f540ebb73667f7ddf72bc878c862d190af87 Published FCC coverage and all native view counts verified live; 702 proceeding occurrences found,6unsupported. Worker1146ddba, app8082191; wave2-smoke-summary.json.

### T11 — Publish complete regulatory memberships and identifier candidates

**Implemented:** Owner docket normalization and source-routed lifecycle date evidence. Published full FR expansion has 899,630 rows, all old cells preserved, exact ordinals, 246,317 candidates across 243,615 occurrences. Lifecycle targets route by dated_by independently of stage source.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/agenda.py`; `spicy-regs/src/spicy_regs/transforms/build_fr_docket_links.py`; `spicy-regs/src/spicy_regs/relationship_views/lifecycle_dates.py`.

**Validation:** `spicy-regs/tests/test_relationship_views.py`; `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_regulatory_navigation.py`.

**Remaining:** Full lifecycle/agenda population reconciliation and wider dated native examples remain. Empty normalized candidate arrays mean no supported interpretation.

**Delivery:** Published with full public byte readback: sha256:b443a813335e8a7cd38f03fdcf199e6c3a93040ba23b4aad39dcc844f7aabfdf Published FR row/candidate totals and bounded lifecycle date evidence verified live. Worker1146ddba, app8082191; wave2-smoke-summary.json.

### T12 — Publish structured CFR references and authority observations

**Implemented:** Retained-input native CFR occurrence and read-state publication pipeline. Full local lifecycle verifies source evidence and generation; native XML AUTH/SOURCE occurrences preserve raw paths/attributes, unknown edition and unsupported part-only targets. Failed scans cannot replace prior; complete empty corrections clear their scopes.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/cfr/authority.py`; `spicy-regs/src/spicy_regs/transforms/native_legal_references.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/native_legal_references.py`.

**Validation:** `spicy-docs/tests/test_native_reference_rows.py`; `spicy-regs/tests/test_native_legal_references.py`.

**Remaining:** Public admission requires enclosing publisher input/edition association; retained fragments remain local. Positive PARAUTH/SECAUTH and full authority coverage remain unqualified.

**Delivery:** Verified local native-reference generation; public admission withheld for enclosing-input/edition association. Registration deployed; native fragment data remains local and unavailable in public MCP, pending enclosing publisher qualification.

### T13 — Publish native U.S. Code references and classification links

**Implemented:** Retained-input native US Code reference/source-credit pipeline and selected target resolution. Local native candidate has 33 total occurrences and two complete reads across US Code/CFR fragments. Exact source hashes/paths retained; changed target pins re-resolve without duplicating observations.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/uscode/references.py`; `spicy-regs/src/spicy_regs/transforms/native_legal_references.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/native_legal_references.py`.

**Validation:** `spicy-docs/tests/test_native_reference_rows.py`; `spicy-regs/tests/test_native_legal_references.py`.

**Remaining:** Qualify enclosing publisher editions and complete selected-edition classification population before public admission. Source credit is not current legal effect.

**Delivery:** Verified local native-reference generation; public admission withheld for enclosing-input/edition association. Registration deployed; native fragment data remains local and unavailable in public MCP, pending enclosing publisher qualification.

### T14 — Join spending recipients to SAM without multiplying money

**Implemented:** UEI identifiers and nonmultiplying recipient enrichment. Registrations aggregate before enrichment, preserving recipient grain, original amounts and registration multiplicity; literal/null EFT and UEI namespaces remain.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/entities.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Native retained and published cohort reconciliation of exact amounts/recipients; no inferred current registration or disjoint parent/child totals.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Current serving implementation deployed; task-specific population/live acceptance not established.

### T15 — Expose the existing court graph with typed endpoints

**Implemented:** Typed court graph endpoint views. Native endpoint kinds/direction and missing/duplicate target handling tested; no name-based substitution or snippet-to-body claim.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/courts.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Bounded population-wide graph validation and missing-body acquisitions remain; independent Supreme Court crosswalk is unqualified. A selected live court endpoint query succeeded.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T16 — Publish a source-artifact reference index

**Implemented:** FCC native asset alternatives and roles alongside existing offered-artifact views. Published partial FCC replay preserves 238 offered artifacts, including native file arrays and empty/read-state distinctions. Capture page and per-record pointers are retained.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`; `spicy-regs/src/spicy_regs/relationship_views/fcc_native.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_relationship_views_retained.py`; `spicy-regs/tests/test_fcc_native.py`.

**Remaining:** Every further enabled rendition needs native positive/empty/redirect/role qualification. Offered assets are not acquired bodies or a universal artifact index.

**Delivery:** Published with full public byte readback: sha256:34b970d51a425d8f57c5565f8ec9f540ebb73667f7ddf72bc878c862d190af87 238 offered artifact observations verified live; acquisition remains not_checked. Worker1146ddba, app8082191; wave2-smoke-summary.json.

### T17 — Extend citation extraction through source-specific adapters

**Implemented:** Bounded source-field adapters into shared citation publication and MCP resolution. Six reviewed bill/report/lobbying fields add 42 findings while preserving 52,674 prior citations and all sibling bytes. Spans and hashes exactly match retained field text; 20 bill mentions abstain without stated Congress. Native comment negatives, zero-result corrections, source-kind collisions, duplicate parents and failed/capped reads are tested.

**Code:** `spicy-docs/src/spicy_docs/interpretation/citations.py`; `spicy-docs/src/spicy_docs/schemas/document_citation_tables.py`; `spicy-regs/src/spicy_regs/citation_sources.py`; `spicy-regs/src/spicy_regs/transforms/held_citations.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/held_citations.py`; `spicy-regs/src/spicy_regs/mcp_server.py`.

**Validation:** `spicy-docs/tests/test_citation_context_qualification.py`; `spicy-docs/tests/test_citations.py`; `spicy-regs/tests/test_held_citations.py`.

**Remaining:** Wider reviewed recall, FCC body qualification, managed comment publication and scheduling remain. Committee names excluded by default after a line-wrap precision defect; executive orders/verbose FR references remain misses; USC note qualifiers do not identify exact note targets.

**Delivery:** Published with full public byte readback: sha256:a9fa600caf2081003f15cf04f8ed1721c5732c71d4cf273110bc3b699f0ea294 42 new held-field findings verified live; full-key source digest checks and missing-Congress abstention pass. Worker1146ddba, app8082191; wave2-smoke-summary.json.

### T18 — Preserve and expose structured report and communication references

**Implemented:** All communication RIN occurrences and retained repair. Positive retained multi-RIN specimen; compatibility scalar kept; array spans, field digest and route retained. Old held report_nature can be repaired without acquisition, preserving unread versus empty.

**Code:** `spicy-docs/src/spicy_docs/interpretation/communication_rin.py`; `spicy-docs/src/spicy_docs/schemas/congress_index_tables.py`; `spicy-regs/src/spicy_regs/transforms/build_congress_index.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

**Validation:** `spicy-docs/tests/test_communication_rin_occurrences.py`; `spicy-regs/tests/test_communication_rin_projection.py`; `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Published held report_nature RIN repair and exact public readback are complete. Full role-specific legal-authority/report-requirement/committee projections, repeated-edition reconciliation and wider population checks remain beyond this RIN route.

**Delivery:** Published generation sha256:94d28167db0a56a3670d0a3ceca17e610b0a54f7b3c1e0c6b91617affaa2a994; exact public hash/counts verified, zero origin requests; source fidelity inherited. Live count 2076 and communication 119-ec-4554 exact three RINs/spans/field digest verified at the published generation; target existence remains not_checked.

### T19 — Preserve party intervals and qualify dated roles

**Implemented:** Affiliation occurrence output and dated party candidates. Native T000254 intervals retained with raw JSON and source ordinals. Both rosters read before full affiliation replacement. Half-open downstream policy tests transitions, gaps, overlaps and missing ends; no term-party fallback.

**Code:** `spicy-docs/src/spicy_docs/sources/legislators.py`; `spicy-docs/src/spicy_docs/schemas/legislator_tables.py`; `spicy-regs/src/spicy_regs/transforms/build_members.py`; `spicy-regs/src/spicy_regs/pipelines/rollups/members.py`; `spicy-regs/src/spicy_regs/relationship_views/affiliations.py`.

**Validation:** `spicy-docs/tests/test_source_relationship_rows.py`; `spicy-regs/tests/test_source_evidence.py`; `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Provider pin, complete retained roster rebuild and publication/readback are complete; live affiliation schema and selected dated-party example are verified. Wider population checks and committee-assignment historical-date semantics remain separately unqualified.

**Delivery:** Published members generation sha256:00cad6cf5255d605f681a89705c22257e2975ed86c2dee61e90f3be278d51851; all three public hashes/counts verified. Live count 59 and vote 119-house-1-1 / member K000401 on 2025-01-03 verified: Republican, found, source_interval, target_count=1 at the published generation.

### T20 — Add namespaced agency and topic occurrence views

**Implemented:** Exact namespaced lookup over accepted vendored REF-038 agency mappings. Library lookup verifies pinned projection/manifest, retains evidence and contested/unmatched states. Native OPM/FAA/ARCTICGAS controls pass; dates abstain because succession/validity artifacts are not adopted.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/vocabulary_mapping.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_vocabulary_mapping.py`.

**Remaining:** Historical succession and topic mappings remain unqualified. This is an application-library API, not an additional deployed MCP tool.

**Delivery:** Local source-qualified scope only; broader publication or acceptance is not implied. Local API/CLI/reader checks passed; not a newly exposed MCP tool or accepted identity/payment publication.

### T21 — Anchor each text difference to both exact source versions

**Implemented:** Exact diff version and section endpoints. Both provider-specific endpoints, text-digest agreement, missing sides, engine revision and duplicate candidates stay separate without multiplication.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/diffs.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_join_cardinality.py`.

**Remaining:** Native insertion/deletion/renumbering/movement cohort and wider selected-publication reconciliation remain. A bounded live diff endpoint query succeeded.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T22 — Build an evidence-backed identity candidate queue

**Implemented:** Local evidence-backed acceptance/rejection/revocation workflow. Explicit CLI decisions bind full candidate bytes and source pin, reviewer, evidence, role and dates. Locked/fsynced hash-chain replay refuses stale candidates/heads and tampering; revocation is tested.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/identity_candidates.py`; `spicy-regs/src/spicy_regs/identity_review.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_identity_review.py`.

**Remaining:** Real same-name/rename/subsidiary truth sets and actual reviewed decisions remain; no mapping or money-attribution edge was accepted or published.

**Delivery:** Local source-qualified scope only; broader publication or acceptance is not implied. Local API/CLI/reader checks passed; not a newly exposed MCP tool or accepted identity/payment publication.

### T23 — Qualify Senate expenditure line extraction before publishing payments

**Implemented:** Bounded native Senate payment candidate reader. Native B-1243 candidates match DJST20250194 / ERAJ SHIRVANI amounts 13.20, 165.89, 291.72. Raw word/header boxes retained; B-1244 office attribution, summaries, unpriced text attachment and unsupported families refuse.

**Code:** `spicy-docs/src/spicy_docs/reading/senate_payment_review.py`; `spicy-docs/tests/fixtures/senate_expenditures/payment-review-2026-09-27.json`; `spicy-docs/src/spicy_docs/reading/senate_payment_candidates.py`.

**Validation:** `spicy-docs/tests/test_senate_payment_review.py`; `spicy-docs/tests/test_senate_expenditures.py`; `spicy-docs/tests/test_senate_payment_candidates.py`.

**Remaining:** Candidate output is partial and not payment-publication-qualified. Full group/section reconciliation, cross-page office origin, negative payment examples and C/D layouts remain.

**Delivery:** Local source-qualified scope only; broader publication or acceptance is not implied. Local API/CLI/reader checks passed; not a newly exposed MCP tool or accepted identity/payment publication.

### T24 — Acquire the missing targets and bodies that unlock useful links

**Implemented:** Explicit retained-first GAO target repair through source-owned metadata reader. GAO-17-317 page title and explicit 2017-02-15 date qualified; prior 42 rows/cells preserved and one target added. Original cited key resolves found against candidate; public bytes verified. Retained PDF/page evidence attached; initial credential refusals and explicit retry/replay tests retained.

**Code:** `spicy-regs/src/spicy_regs/acquisition_queue.py`; `spicy-docs/src/spicy_docs/sources/gao/files.py`; `spicy-regs/src/spicy_regs/transforms/build_gao_target.py`; `spicy-docs/src/spicy_docs/sources/gao/product_metadata.py`.

**Validation:** `spicy-regs/tests/test_acquisition_queue.py`; `spicy-regs/tests/test_gao_target.py`; `spicy-docs/tests/test_gao_product_metadata.py`.

**Remaining:** Metadata schema has no body-text/reference columns; PDF bytes are evidence, not a published extracted text row. Law/USC/court cohorts and historical edition coverage remain.

**Delivery:** Published with full public byte readback: sha256:566318c5740235cd9fb849f9281476fc6e3bb13120b3f03a1971196e33dd2737 New GAO metadata row and its citation join verified live. Worker1146ddba, app8082191; wave2-smoke-summary.json.

### T25 — Make aggregate joins and coverage claims reproducible

**Implemented:** Pin-aware aggregate checks in existing join-checker CLI. Four complete selected-file checks pass with zero differing groups: agency dockets, agency documents, monthly documents and feed dockets. Verified parent SHA lineage qualifies comparisons; capped/failed/parsed-only inputs cannot establish completeness.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/core.py`; `spicy-regs/src/spicy_regs/mcp_server.py`; `spicy-regs/src/spicy_regs/join_measurements.json`; `spicy-regs/src/spicy_regs/aggregate_checks.py`; `spicy-regs/scripts/check_table_joins.py`.

**Validation:** `spicy-regs/tests/test_mcp_relationships.py`; `spicy-regs/tests/test_relationship_views.py`; `spicy-regs/tests/test_aggregate_checks.py`.

**Remaining:** Comments/index and lifecycle totals abstain UNPINNED pending materialized/catalog lineage support; source checkpoint reconciliation and central MCP aggregate display remain.

**Delivery:** Local source-qualified scope only; broader publication or acceptance is not implied. Local API/CLI/reader checks passed; not a newly exposed MCP tool or accepted identity/payment publication.

## Remaining delivery work

The full proposal still requires the explicit remaining acceptance listed above: comments and cosponsor backfills, complete native-source and target cohorts, publisher-edition association for native legal fragments, historical vocabulary adoption, real identity decisions, Senate payment reconciliation and materialized/catalog lineage for the remaining aggregates. Code and bounded source proofs are delivered separately from these broader claims.

The Senate native review identifies `DJST20250194` / `ERAJ SHIRVANI`; its automated candidates match the reviewed amounts but do not qualify the whole ledger. GAO-17-317 metadata is now published and target-resolvable; its retained PDF is evidence rather than a newly published extracted text table.
