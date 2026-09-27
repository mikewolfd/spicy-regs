# Join delivery execution — 2026-09-27

Parallel work is implemented, pinned and deployed. The live server now exposes source-preserving relationship views and bounded citation target resolution; the members and House communication refreshes are published and verified through public bytes and MCP. **The full proposal is not complete.** Remaining source qualification, pipelines, review workflows and backfills are listed per task below.

This ledger follows the [proposal](join-delivery-tasks-2026-09-27.md). The [machine-readable ledger](join-delivery-execution-2026-09-27.json) retains exact code paths, tests, publication pins, measurements and remaining acceptance. Paths in the task ledger are relative to the workspace root. SpicyDocs owns source behavior; SpicyRegs owns application behavior.

## Release and evidence boundary

| Boundary | Verified state |
| --- | --- |
| Provider | SpicyDocs 0.47.0 from `7440dd4`; two byte-identical wheel builds, vendored and tested in SpicyRegs. No registry upload. |
| Repository checks | Provider gate: 10,354 passed. Final consumer lint, types, dictionary and full suite: 3,238 passed. Logs: `provider-gate-final.log`, `consumer-gate-final.log`, `cloudflare-check-final.log`. |
| Corrective deployment | App `8380ac8`; Worker `ea8d54f5-a2f4-4871-b9b0-016ef595b518`; image `1d558a710949494a7b43e39be5027124f07259754e8ccb9ffd3ecd401020efb5`. New rule and metadata verified through the public endpoint. |
| Members publication | Generation `00cad6cf…`: 59 affiliation occurrences; all prior member/term identities and native values preserved. All three public table hashes/counts match. |
| Communications publication | Generation `94d28167…`: 2,076 RIN occurrences on 2,046 rows; all 5,006 prior rows and 32 prior columns preserved. Public hash/counts match. |
| Comments | Nullable catalog migration and three-record read-only repair preview verified. Catalog row corrections and public export remain pending. |
| Git | Provider and application changes committed locally. No Git push or package-registry release is claimed. |

The serving endpoint is [the fork MCP server](https://spicy-regs-mcp.mdeeb.workers.dev/mcp). It reads selected public Parquet publications. Iceberg remains the ingestion/replay version boundary; this release does not migrate serving to Iceberg or claim an atomic snapshot across independently selected tables.

Primary execution evidence: `/Users/mikewolfd/.codex/artifacts/spicy-regs-join-implementation-20260927/`. The initial source review remains under `spicy-regs-join-tasks-20260927/`; members, communications, GAO, FEC benchmark and array reconciliations have separate evidence roots recorded in the JSON ledger. These local receipts are evidence paths, not public release links.

## Verified live behavior

Discovery measured 190 tables/views: 98 derived views available, four comment views unsupported against the older served schema, and none unavailable. Bounded reads passed for vote documents, related bills, member FEC identifiers, FR RINs, court endpoints, diff endpoints and pending identity candidates. The citation tool returned 20 found targets in an explicitly capped, partial selection. Local-file access and write attempts were rejected.

The first live FEC query exhausted 8.3 GiB. That failing receipt is retained in `fec-evidence-live.json`. The corrected view groups only referenced companion keys, keeps compact candidate digests and uses hash joins. Full companion evidence remains accessible through `fec_source_records` by collection/record identity. The same selected-input query completed in 6.67 seconds at a 512 MB local memory limit and in **3.864 seconds through the corrected public endpoint**. `final-smoke-summary.json` records all final checks passing. This is a measured query, not a general latency guarantee; the final cold description request took 33.089 seconds.

Container shutdown now executes Uvicorn directly so it receives stop signals. The local container proof completed graceful shutdown in 0.27 seconds. The deployment CLI confirms the rollout started, and live rule `fec-companion-location-v2` confirms the new runtime served the checks. Fleet-wide per-instance completion is not inferred from the CLI response.

Independent data checks under `spicy-regs-live-members-rins-20260927/` confirmed 59 affiliations and 2,076 communication RIN occurrences. Communication `119-ec-4554` returns `2125-AF80`, `2130-AD05` and `2132-AB51` with exact retained spans and field digest; target existence stays `not_checked`. Vote `119-house-1-1`, member `K000401`, on `2025-01-03` returns one Republican source interval. These are selected source assertions, not official roster completeness or complete historical coverage.

A separate retained-byte check verified FEC committee `C00000059` end to end: the complete 123,668-byte captured page matches its recorded SHA-256; `/results/0` matches companion metadata and its sponsor list is `[]`. The digest describes the complete page, not normalized record JSON. This qualifies one selected case; the live navigation tool still reports that it has not itself read or hashed raw source bytes. Receipt: `spicy-regs-fec-evidence-20260927/receipt.json`.

The earlier citation coverage report was also reproduced for all eight audited citation kinds over complete selected key columns with the source-text digest predicate. Exact SQL and pins are in `spicy-regs-join-delivery-t01-t02-20260927/citation-coverage.json`. This dated lookup measurement is distinct from the final capped live query and from extraction recall.

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

**Implemented:** Held FCC proceeding occurrences. Independent held proceeding-name membership preserves repeated and unsupported elements.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`.

**Validation:** `spicy-regs/tests/test_relationship_views.py`.

**Remaining:** Native numeric proceeding IDs and participant roles discarded upstream still need source repair and native fixtures; population replay remains pending.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Current serving implementation deployed; task-specific population/live acceptance not established.

### T11 — Publish complete regulatory memberships and identifier candidates

**Implemented:** Regulatory memberships and agenda editions. FR RIN/docket arrays use dated document identity; other held memberships expand independently; exact RIN/edition/URL targets retain duplicates and ambiguity. Full selected FR arrays reconcile 121795 RIN observations (236 unsupported) and 899632 docket observations (2 unsupported); valid distinct-pair counts are 121559 and 899630.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/regulatory.py`; `spicy-regs/src/spicy_regs/relationship_views/agenda.py`.

**Validation:** `spicy-regs/tests/test_relationship_views.py`; `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Free-text docket normalization, lifecycle date routing and their native qualification remain unimplemented; full lifecycle/agenda reconciliation remains. Bounded live FR-RIN query succeeded.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T12 — Publish structured CFR references and authority observations

**Implemented:** Reusable CFR AUTH/SOURCE source rows. Native part fragment preserves separate roles, literal text, text runs, XML path/ancestry and unknown title/edition. Existing scanner remains source owner.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/cfr/authority.py`.

**Validation:** `spicy-docs/tests/test_native_reference_rows.py`.

**Remaining:** Complete application write/refresh/resolution pipeline; positive PARAUTH/SECAUTH shapes and complete authority coverage are not qualified.

**Delivery:** Provider implementation pinned; wider application pipeline/output publication not established. Provider code delivery is not end-to-end application or live source qualification.

### T13 — Publish native U.S. Code references and classification links

**Implemented:** Reusable native USC reference/source-credit rows. Retained section-423 fragment yields 30 native href observations and one separate source credit; absent/empty/unknown hrefs and edition uncertainty survive.

**Code:** `spicy-docs/src/spicy_docs/schemas/native_reference_rows.py`; `spicy-docs/src/spicy_docs/sources/uscode/references.py`.

**Validation:** `spicy-docs/tests/test_native_reference_rows.py`.

**Remaining:** Full application occurrence pipeline, selected-edition classification links and population reconciliation; historical source credit is not current legal effect.

**Delivery:** Provider implementation pinned; wider application pipeline/output publication not established. Provider code delivery is not end-to-end application or live source qualification.

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

**Implemented:** Selected offered-artifact views. Held document/FCC artifact arrays and meeting offered documents preserve occurrences and native rendition fields; URLs do not imply acquired bytes.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`; `spicy-regs/src/spicy_regs/relationship_views/congress.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_relationship_views_retained.py`.

**Remaining:** Every enabled source rendition family needs native positive/empty/redirect/duplicate-role replay and retained receipt linkage; no universal artifact index or automatic acquisition.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Current serving implementation deployed; task-specific population/live acceptance not established.

### T17 — Extend citation extraction through source-specific adapters

**Implemented:** Citation context provenance and explicit abstention. Retained historical inline/subheading contexts distinguish their rule from document fallback; strict mode can refuse fallback while preserving source mention.

**Code:** `spicy-docs/src/spicy_docs/interpretation/citations.py`; `spicy-docs/src/spicy_docs/schemas/document_citation_tables.py`.

**Validation:** `spicy-docs/tests/test_citation_context_qualification.py`; `spicy-docs/tests/test_citations.py`.

**Remaining:** Broader bill/report/FCC/LDA/comment adapters, reviewed precision/recall, complete-read clearing, scheduling and bounded retained publication are not complete.

**Delivery:** Provider implementation pinned; wider application pipeline/output publication not established. Provider code delivery is not end-to-end application or live source qualification.

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

**Implemented:** Namespaced native agency/topic observations. FR agencies/topics, Congress subjects and LDA contacted-entity arrays retain native namespace and objects; empty contacts create no edge.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/artifacts_topics.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Accepted RefSpec mappings, historical validity, contested/unmatched mapping workflow and per-route native-ID/abstention evidence remain pending.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Current serving implementation deployed; task-specific population/live acceptance not established.

### T21 — Anchor each text difference to both exact source versions

**Implemented:** Exact diff version and section endpoints. Both provider-specific endpoints, text-digest agreement, missing sides, engine revision and duplicate candidates stay separate without multiplication.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/diffs.py`; `spicy-docs/src/spicy_docs/schemas/bill_diff_tables.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`; `spicy-regs/tests/test_join_cardinality.py`.

**Remaining:** Native insertion/deletion/renumbering/movement cohort and wider selected-publication reconciliation remain. A bounded live diff endpoint query succeeded.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T22 — Build an evidence-backed identity candidate queue

**Implemented:** Pending identity candidate view. Existing name matches preserve features, alternatives, repeats and source dates; publication-digest candidate IDs only for pinned sources. No accepted identity or money attribution.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/identity_candidates.py`.

**Validation:** `spicy-regs/tests/test_relationship_views_navigation.py`.

**Remaining:** Review/acceptance/revocation workflow and truth set for same-name entities, renames and parent/subsidiary cases remain unimplemented. Live pending-candidate query succeeded; no identity was accepted.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Bounded positive live case verified on Worker faa8b3ab-ed99-456a-a234-18b6b6ac3006; full population acceptance remains.

### T23 — Qualify Senate expenditure line extraction before publishing payments

**Implemented:** Pinned manual payment truth set and refusal gate. High-resolution native B-1243/B-1244 review corrects DJST20250194/ERAJ SHIRVANI; exact decimal strings, word boxes, raw cells, negative summary/total observations and unresolved continuation retained. Focused native Senate suite passed on 2026-09-27.

**Code:** `spicy-docs/src/spicy_docs/reading/senate_payment_review.py`; `spicy-docs/tests/fixtures/senate_expenditures/payment-review-2026-09-27.json`.

**Validation:** `spicy-docs/tests/test_senate_payment_review.py`; `spicy-docs/tests/test_senate_expenditures.py`.

**Remaining:** No automated payment parser or publication qualification. Complete cross-page grouping, office-origin/boundary proof, negative payment examples, payment reconciliation and C/D layouts remain unsupported.

**Delivery:** Provider implementation pinned; wider application pipeline/output publication not established. Provider code delivery is not end-to-end application or live source qualification.

### T24 — Acquire the missing targets and bodies that unlock useful links

**Implemented:** Bounded missing-target queue and one GAO acquisition. Queue requires qualified source/target pins and explicit retained inspection, deduplicates/ranks occurrences, names provider APIs and abstains on unselected USC editions. On 2026-09-27 GAO-17-317 acquired in one request, PDF validated and cover identity checked.

**Code:** `spicy-regs/src/spicy_regs/acquisition_queue.py`; `spicy-docs/src/spicy_docs/sources/gao/files.py`.

**Validation:** `spicy-regs/tests/test_acquisition_queue.py`.

**Remaining:** No acquired-body publication or target-table re-resolution; law/USC/court end-to-end cohorts, refusal/retry paths and historical edition coverage remain.

**Delivery:** One GAO PDF acquired and identity checked; not published or re-resolved. Queue code deployed; end-to-end acquisition/publication/re-resolution remains incomplete.

### T25 — Make aggregate joins and coverage claims reproducible

**Implemented:** Explicit per-input view coverage metadata. Views expose schema-derived columns, inherited population, grain, per-table publication metadata and unsupported/unread states; dated key-only join measurements retain independent pins.

**Code:** `spicy-regs/src/spicy_regs/relationship_views/core.py`; `spicy-regs/src/spicy_regs/mcp_server.py`; `spicy-regs/src/spicy_regs/join_measurements.json`.

**Validation:** `spicy-regs/tests/test_mcp_relationships.py`; `spicy-regs/tests/test_relationship_views.py`.

**Remaining:** Full comments/index, agency, lifecycle, feed and checkpoint aggregate reconciliation is not implemented; installation does not measure row counts or completeness.

**Delivery:** Views use selected existing publications; no new independently materialized task artifact claimed. Current serving implementation deployed; task-specific population/live acceptance not established.

## Remaining delivery work

Code existence and passing tests do not close source acceptance. The next work includes comment and cosponsor backfills, T11 normalization/date routing, T12/T13 native-reference application pipelines, T17 wider extraction adapters, T20 accepted vocabulary mappings, T22 reviewed identity decisions, T23 automated payment qualification, T24 acquired-body publication/re-resolution and T25 aggregate reconciliation. Each remains bounded by its task's native-source evidence requirements.

The Senate truth set corrects the earlier transcription to `DJST20250194` and `ERAJ SHIRVANI`, based on high-resolution native PDF review. It qualifies a manual review gate, not an automated payment parser. The GAO receipt proves one acquired and identified PDF; no extracted body publication or target re-resolution is claimed.
